"""Shared chain and group scratch: keys, allocation, exposure, nested authority and cleanup (#53)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import artifacts, backends, bridge_state, shared_scratch, state_ownership, store
from harflow_mcp.backends.base import Invocation
from harflow_mcp.backends.codex import UnsafeInvocationError
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry
from test_extra_path_backends import qualified  # noqa: F401 - fixture reuse


@pytest.fixture
def tmp_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    workspace = tmp_path_factory.mktemp("shared")
    root = workspace / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir(exist_ok=True)
        yield workspace


def log_dir_of(tmp_path: Path) -> Path:
    return tmp_path / "state" / "tasks"


def record(task_id: str, *, group=None, chain=None, status="completed", finished_at="2026-01-01T00:01:00+00:00",
           repo_path="/tmp", started_at="2026-01-01T00:00:00+00:00", **kwargs) -> store.TaskRecord:
    settled = status != "running"
    fields = {"enforcement": backends.get("claude").enforcement("write_in_repo").as_dict()} | kwargs
    if not settled:
        fields |= {"pid": 4242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": ["x"]}
    return store.TaskRecord(
        task_id=task_id, backend="claude", session_id=None, repo_path=repo_path, started_at=started_at,
        group=group, scratch_chain_id=chain, status=status, exit_code=0 if settled else None,
        finished_at=finished_at if settled else None, output_drained=settled, **fields)


# ---- keys ------------------------------------------------------------------------------------


def test_chain_keys_are_validated_task_ids_including_the_reserved_looking_ones():
    assert shared_scratch.key_for("chain", "chain") == "chain"
    assert shared_scratch.key_for("chain", "group") == "group"
    for bad in ("", "../x", "a/b", "a b", ".", ".."):
        with pytest.raises(store.InvalidTaskId):
            shared_scratch.key_for("chain", bad)


def test_group_keys_distinguish_groups_and_never_traverse():
    groups = ["MT-2691", "mt-2691", "MT 2691", "MT/2691", "../MT-2691", "a" * 300, "a" * 301, "é", "e", "..", ".", "", " "]
    keys = {}
    for group in groups:
        if not group:
            with pytest.raises(shared_scratch.SharedScratchError):
                shared_scratch.key_for("group", group)
            continue
        key = shared_scratch.key_for("group", group)
        assert "/" not in key and key not in ("", ".", "..") and not key.startswith(".")
        assert len(key) <= shared_scratch.SLUG_MAX + 17
        assert key not in keys, (group, keys[key])
        keys[key] = group
    assert shared_scratch.key_for("group", "MT-2691") == shared_scratch.key_for("group", "MT-2691")
    assert shared_scratch.key_for("group", "MT-2691").startswith("mt-2691-")


def test_paths_are_under_the_shared_root(tmp_path):
    log_dir = log_dir_of(tmp_path)
    assert shared_scratch.directory(log_dir, "chain", "abc") == tmp_path / "state" / "shared-scratch" / "chain" / "abc"
    path = shared_scratch.directory(log_dir, "group", "../escape")
    assert path.parent == tmp_path / "state" / "shared-scratch" / "group"


def test_effective_chain_falls_back_for_older_records():
    assert shared_scratch.effective_chain(record("c", chain="root")) == "root"
    assert shared_scratch.effective_chain(record("c", chain_root_task_id="cont-root")) == "cont-root"
    assert shared_scratch.effective_chain(record("solo")) == "solo"


# ---- allocation and metadata -----------------------------------------------------------------


def test_allocation_is_private_with_metadata_outside_the_folder(tmp_path):
    log_dir = log_dir_of(tmp_path)
    path = shared_scratch.allocate(log_dir, "group", "MT-1")
    assert path.is_dir() and path.stat().st_mode & 0o777 == 0o700
    meta = tmp_path / "state" / "shared-scratch" / ".meta" / "group" / f"{path.name}.json"
    assert json.loads(meta.read_text())["identity"] == "MT-1"
    assert list(path.iterdir()) == []
    assert shared_scratch.allocate(log_dir, "group", "MT-1") == path


def test_reopening_with_a_different_identity_fails_closed(tmp_path):
    log_dir = log_dir_of(tmp_path)
    path = shared_scratch.allocate(log_dir, "group", "MT-1")
    meta = tmp_path / "state" / "shared-scratch" / ".meta" / "group" / f"{path.name}.json"
    document = json.loads(meta.read_text())
    meta.write_text(json.dumps(document | {"identity": "someone-else"}))
    with pytest.raises(shared_scratch.SharedScratchError) as failure:
        shared_scratch.allocate(log_dir, "group", "MT-1")
    assert failure.value.code == "shared_scratch_mismatch"


def test_symlinked_namespace_or_folder_is_refused(tmp_path):
    log_dir = log_dir_of(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    base = tmp_path / "state" / "shared-scratch"
    base.mkdir()
    (base / "chain").symlink_to(outside, target_is_directory=True)
    with pytest.raises(shared_scratch.SharedScratchError):
        shared_scratch.allocate(log_dir, "chain", "abc")
    (base / "chain").unlink()
    shared_scratch.allocate(log_dir, "chain", "abc")
    (base / "chain" / "abc").rmdir()
    (base / "chain" / "abc").symlink_to(outside, target_is_directory=True)
    with pytest.raises(shared_scratch.SharedScratchError):
        shared_scratch.allocate(log_dir, "chain", "abc")
    assert list(outside.iterdir()) == []


def test_a_folder_without_metadata_is_adopted_by_its_derived_name(tmp_path):
    log_dir = log_dir_of(tmp_path)
    folder = tmp_path / "state" / "shared-scratch" / "chain" / "adopted"
    folder.mkdir(parents=True)
    (folder / "keep").write_text("x")
    assert shared_scratch.allocate(log_dir, "chain", "adopted") == folder.resolve()
    assert (folder / "keep").read_text() == "x"


def test_the_lock_is_bounded_and_exclusive(tmp_path):
    log_dir = log_dir_of(tmp_path)
    with shared_scratch.locked(log_dir, "chain", "abc"):
        with pytest.raises(shared_scratch.SharedScratchError) as failure:
            with shared_scratch.locked(log_dir, "chain", "abc", timeout=0.05):
                pass
        assert failure.value.code == "shared_scratch_busy"
        hold = shared_scratch.Hold()
        with pytest.raises(shared_scratch.SharedScratchError):
            asyncio.run(shared_scratch.lock_async(log_dir, "chain", "abc", hold, timeout=0.05))
    with shared_scratch.locked(log_dir, "chain", "abc", timeout=0.05):
        pass


# ---- bridge state ----------------------------------------------------------------------------


def test_bridge_state_accepts_only_allocated_shared_folders(tmp_path):
    log_dir = log_dir_of(tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    chain = shared_scratch.allocate(log_dir, "chain", "abc")
    group = shared_scratch.allocate(log_dir, "group", "MT-1")
    bridge_state.assert_state_outside_writes(log_dir, repo, (), (str(chain), str(group)))
    for bad in (tmp_path / "state" / "tasks", tmp_path / "state" / "shared-scratch", tmp_path / "state" / "shared-scratch" / "chain"):
        with pytest.raises(ValueError):
            bridge_state.assert_state_outside_writes(log_dir, repo, (), (str(chain), str(bad)))
    nested = chain / "inner"
    nested.mkdir()
    with pytest.raises(ValueError):
        bridge_state.assert_state_outside_writes(log_dir, repo, (), (str(nested),))


# ---- argv shapes -----------------------------------------------------------------------------


def build(name, tmp_path, freedom="write_in_repo", resume=False):
    backend = backends.get(name)
    method = backend.build_resume_argv if resume else backend.build_start_argv
    session = "11111111-1111-4111-8111-111111111111" if name == "claude" or resume else None
    return backend, method("hello", repo=tmp_path, freedom=freedom, session_id=session, model=None, max_turns=None, reasoning_effort=None)


@pytest.mark.parametrize("name", ["claude", "codex"])
@pytest.mark.parametrize("resume", [False, True])
@pytest.mark.parametrize("granted", [1, 2])
def test_shared_directories_are_repeated_add_dirs_after_the_task_scratch(name, resume, granted, tmp_path):
    backend, invocation = build(name, tmp_path, resume=resume)
    task = tmp_path / "task"
    shared = [tmp_path / "chain", tmp_path / "group"][:granted]
    for directory in (task, *shared):
        directory.mkdir(exist_ok=True)
    invocation = backend.with_writable_directory(invocation, task, "write_in_repo")
    invocation = backend.with_shared_directories(invocation, tuple(shared), "write_in_repo")
    backend.assert_safe(invocation, "write_in_repo")
    argv = invocation.argv
    values = [argv[i + 1] for i, token in enumerate(argv) if token == "--add-dir"]
    assert values == [str(task.resolve()), *(str(path.resolve()) for path in shared)]
    assert invocation.shared_scratch_directories == tuple(str(path.resolve()) for path in shared)
    # Every one of them sits before the positional boundary.
    assert max(i for i, token in enumerate(argv) if token == "--add-dir") < (argv.index("--") if "--" in argv else len(argv))


@pytest.mark.parametrize("name", ["claude", "codex"])
def test_shared_directory_shapes_are_exact(name, tmp_path):
    backend, invocation = build(name, tmp_path)
    task, chain = tmp_path / "task", tmp_path / "chain"
    task.mkdir()
    chain.mkdir()
    granted = backend.with_shared_directories(backend.with_writable_directory(invocation, task, "write_in_repo"), (chain,), "write_in_repo")
    backend.assert_safe(granted, "write_in_repo")
    with pytest.raises((RuntimeError, ValueError)):
        backend.assert_safe(replace(granted, shared_scratch_directories=()), "write_in_repo")
    with pytest.raises((RuntimeError, ValueError)):
        backend.assert_safe(replace(granted, shared_scratch_directories=(str(tmp_path / "other"),)), "write_in_repo")
    with pytest.raises((RuntimeError, ValueError)):
        backend.with_shared_directories(invocation, (chain,), "read_only")
    with pytest.raises((RuntimeError, ValueError)):
        backend.with_shared_directories(granted, (chain,), "write_in_repo")
    reordered = list(granted.argv)
    first = reordered.index("--add-dir")
    reordered[first + 1], reordered[first + 3] = reordered[first + 3], reordered[first + 1]
    with pytest.raises((RuntimeError, ValueError)):
        backend.assert_safe(replace(granted, argv=reordered), "write_in_repo")


@pytest.mark.parametrize("resume", [False, True])
def test_codex_shared_directories_sit_between_the_task_scratch_and_the_extra_grants(qualified, resume, tmp_path):
    from test_extra_path_backends import build as build_codex
    from harflow_mcp.extra_paths import normalize_paths
    grant = tmp_path / "grant"
    task, chain, group = tmp_path / "task", tmp_path / "chain", tmp_path / "group"
    for directory in (grant, task, chain, group):
        directory.mkdir()
    grants = normalize_paths([str(grant)])
    invocation = qualified.with_extra_writable_paths(build_codex(qualified, tmp_path, resume=resume), grants, "write_in_repo")
    invocation = qualified.with_writable_directory(invocation, task, "write_in_repo")
    invocation = qualified.with_shared_directories(invocation, (chain, group), "write_in_repo")
    argv = invocation.argv
    assert [argv[i + 1] for i, token in enumerate(argv) if token == "--add-dir"] == [
        str(task.resolve()), str(chain.resolve()), str(group.resolve()), grants[0].path]
    qualified.assert_extra_writable_paths(invocation, grants, "write_in_repo")
    with pytest.raises(UnsafeInvocationError):
        qualified.assert_safe(replace(invocation, shared_scratch_directories=(str(group.resolve()), str(chain.resolve()))), "write_in_repo")


# ---- running tasks -----------------------------------------------------------------------------


class Capture:
    """Replaces process creation with a long sleep, recording the argv and environment of each launch."""

    def __init__(self, monkeypatch):
        self.launches = []
        create = asyncio.create_subprocess_exec

        async def fake(*argv, **kwargs):
            self.launches.append(SimpleNamespace(argv=list(argv), env=dict(kwargs["env"])))
            return await create(sys.executable, "-c", "import time; time.sleep(60)", **kwargs)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake)


async def launch(registry, name, tmp_path, *, freedom="write_in_repo", group=None, scratch_chain_id=None, task_id=None, spawned_by=None, **kwargs):
    backend, invocation = build(name, tmp_path, freedom)
    return await registry._spawn(
        invocation, backend=backend, prompt="hello", repo_path=tmp_path, session_id=None, freedom=freedom, max_turns=None,
        model=None, reasoning_effort=None, group=group, task_id=task_id,
        **({"scratch_chain_id": scratch_chain_id} if scratch_chain_id else {}), spawned_by=spawned_by, **kwargs)


async def stop(registry, task):
    await registry.cancel(task)


BACKENDS = ["claude", "codex", "opencode", "vibe", "antigravity"]


@pytest.mark.parametrize("name", BACKENDS)
async def test_every_backend_gets_the_scratch_environment(name, tmp_path, monkeypatch):
    capture = Capture(monkeypatch)
    monkeypatch.setenv("HARFLOW_GROUP_SCRATCH", "/inherited/group")
    monkeypatch.setenv("HARFLOW_CHAIN_SCRATCH", "/inherited/chain")
    monkeypatch.setenv("HARFLOW_SCRATCH", "/inherited/task")
    registry = TaskRegistry(log_dir=log_dir_of(tmp_path), open_monitor=False)
    root = tmp_path / "state"
    grouped = await launch(registry, name, tmp_path, group="MT-2691", task_id="t-grouped")
    ungrouped = await launch(registry, name, tmp_path, task_id="t-plain")
    try:
        env = capture.launches[0].env
        assert env["HARFLOW_SCRATCH"] == env["HARFLOW_TASK_SCRATCH"] == str(root / "scratch" / "t-grouped")
        assert env["HARFLOW_CHAIN_SCRATCH"] == str(root / "shared-scratch" / "chain" / "t-grouped")
        assert env["HARFLOW_GROUP_SCRATCH"] == str(shared_scratch.directory(log_dir_of(tmp_path), "group", "MT-2691"))
        assert all(Path(env[name]).is_dir() for name in ("HARFLOW_SCRATCH", "HARFLOW_CHAIN_SCRATCH", "HARFLOW_GROUP_SCRATCH"))
        plain = capture.launches[1].env
        assert "HARFLOW_GROUP_SCRATCH" not in plain and plain["HARFLOW_CHAIN_SCRATCH"].endswith("/t-plain")
        scratch = grouped.snapshot()["scratch"]
        assert scratch["chain"] == env["HARFLOW_CHAIN_SCRATCH"] and scratch["group"] == env["HARFLOW_GROUP_SCRATCH"]
        assert grouped.scratch_chain_id == "t-grouped"
        if name in ("claude", "codex"):
            argv = capture.launches[0].argv
            values = [argv[i + 1] for i, token in enumerate(argv) if token == "--add-dir"]
            assert values == [env["HARFLOW_SCRATCH"], env["HARFLOW_CHAIN_SCRATCH"], env["HARFLOW_GROUP_SCRATCH"]]
            assert scratch["access"] == {"task": "writable", "chain": "writable", "group": "writable"}
            assert set(scratch["writable"]) <= set(grouped.enforcement["writable_roots"])
            assert scratch["chain"] in grouped.enforcement["writable_roots"] and scratch["group"] in grouped.enforcement["writable_roots"]
        else:
            assert not {env["HARFLOW_SCRATCH"], env["HARFLOW_CHAIN_SCRATCH"], env["HARFLOW_GROUP_SCRATCH"]} & set(capture.launches[0].argv)
            assert scratch["access"] == {"task": "no_additional_grant", "chain": "no_additional_grant", "group": "no_additional_grant"}
            assert scratch["writable"] == []
        assert "writable" in scratch["notice"] or "no write grant" in scratch["notice"]
    finally:
        for task in (grouped, ungrouped):
            await stop(registry, task)


@pytest.mark.parametrize("name", ["claude", "codex"])
async def test_read_only_tasks_get_the_environment_and_no_grant(name, tmp_path, monkeypatch):
    capture = Capture(monkeypatch)
    registry = TaskRegistry(log_dir=log_dir_of(tmp_path), open_monitor=False)
    task = await launch(registry, name, tmp_path, freedom="read_only", group="G", task_id="ro")
    try:
        env, argv = capture.launches[0].env, capture.launches[0].argv
        assert "--add-dir" not in argv and "HARFLOW_SCRATCH" not in env
        assert Path(env["HARFLOW_CHAIN_SCRATCH"]).is_dir() and Path(env["HARFLOW_GROUP_SCRATCH"]).is_dir()
        assert task.enforcement["writable_roots"] == [] or env["HARFLOW_CHAIN_SCRATCH"] not in task.enforcement["writable_roots"]
        scratch = task.snapshot()["scratch"]
        assert scratch["writable"] == [] and scratch["task"] is None
        expected = "sandbox_read_only" if name == "codex" else "no_additional_grant"
        assert scratch["access"] == {"chain": expected, "group": expected}
    finally:
        await stop(registry, task)


async def test_chain_and_group_are_shared_and_task_scratch_is_not(tmp_path, monkeypatch):
    capture = Capture(monkeypatch)
    registry = TaskRegistry(log_dir=log_dir_of(tmp_path), open_monitor=False)
    first = await launch(registry, "codex", tmp_path, group="X", task_id="first")
    # A resumed successor carries the first task's chain; a sibling starts its own chain in the same group.
    successor = await launch(registry, "codex", tmp_path, group="X", task_id="successor", scratch_chain_id="first")
    sibling = await launch(registry, "codex", tmp_path, group="X", task_id="sibling")
    try:
        envs = [launch_.env for launch_ in capture.launches]
        assert envs[0]["HARFLOW_CHAIN_SCRATCH"] == envs[1]["HARFLOW_CHAIN_SCRATCH"] != envs[2]["HARFLOW_CHAIN_SCRATCH"]
        assert envs[0]["HARFLOW_GROUP_SCRATCH"] == envs[1]["HARFLOW_GROUP_SCRATCH"] == envs[2]["HARFLOW_GROUP_SCRATCH"]
        assert len({env["HARFLOW_SCRATCH"] for env in envs}) == 3
        assert successor.scratch_chain_id == "first"
    finally:
        for task in (first, successor, sibling):
            await stop(registry, task)


async def test_the_acceptance_scenario_group_file_is_visible_to_resume_and_sibling(tmp_path, monkeypatch):
    """#53: a group X task writes $HARFLOW_GROUP_SCRATCH/a.png; a resumed successor and a sibling both see it."""
    script = (
        "import json,os,pathlib,sys\n"
        "group=pathlib.Path(os.environ['HARFLOW_GROUP_SCRATCH'])\n"
        "if os.environ['SCENARIO'] == 'write':\n"
        "    (group/'a.png').write_bytes(b'png')\n"
        "    print(json.dumps({'type':'thread.started','thread_id':'thread-1'}))\n"
        "print(json.dumps({'type':'item.completed','item':{'id':'i','type':'agent_message','text':(group/'a.png').read_text(errors='ignore') and str(group/'a.png')}}))\n"
        "print(json.dumps({'type':'turn.completed'}))\n")
    create = asyncio.create_subprocess_exec
    seen = []

    async def fake(*argv, **kwargs):
        env = dict(kwargs["env"], SCENARIO="write" if not seen else "read")
        seen.append(env)
        return await create(sys.executable, "-c", script, **{**kwargs, "env": env})

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake)
    registry = TaskRegistry(log_dir=log_dir_of(tmp_path), open_monitor=False)
    repo = tmp_path / "repo"
    repo.mkdir()
    backend = backends.get("codex")
    writer = await registry.start("write it", repo, backend=backend, group="X", freedom="write_in_repo")
    await asyncio.wait_for(writer.done.wait(), 30)
    group_dir = Path(seen[0]["HARFLOW_GROUP_SCRATCH"])
    assert (group_dir / "a.png").read_bytes() == b"png"
    record_ = store.read(log_dir_of(tmp_path), writer.task_id)
    assert record_.session_id == "thread-1" and record_.group == "X"
    successor = await registry.resume_record(record_, "again")
    sibling = await registry.start("sibling", repo, backend=backend, group="X", freedom="write_in_repo")
    for task in (successor, sibling):
        await asyncio.wait_for(task.done.wait(), 30)
    assert seen[1]["HARFLOW_GROUP_SCRATCH"] == seen[2]["HARFLOW_GROUP_SCRATCH"] == str(group_dir)
    assert seen[1]["HARFLOW_CHAIN_SCRATCH"] == seen[0]["HARFLOW_CHAIN_SCRATCH"] != seen[2]["HARFLOW_CHAIN_SCRATCH"]
    assert seen[1]["HARFLOW_SCRATCH"] != seen[0]["HARFLOW_SCRATCH"]
    assert successor.status == "completed" and sibling.status == "completed"
    for task in (successor, sibling):
        assert Path(task.snapshot()["scratch"]["group"]) / "a.png" == group_dir / "a.png"
    # Settlement and later task completion never touch shared folders.
    assert (group_dir / "a.png").exists() and Path(seen[0]["HARFLOW_CHAIN_SCRATCH"]).is_dir()
    # Settlement is recorded as the folders' last use.
    meta = json.loads((tmp_path / "state" / "shared-scratch" / ".meta" / "group" / f"{group_dir.name}.json").read_text())
    assert datetime.fromisoformat(meta["last_used_at"]) >= datetime.fromisoformat(record_.finished_at)
    # The successor's record keeps the same chain id through a second resume.
    again = await registry.resume_record(store.read(log_dir_of(tmp_path), successor.task_id), "third")
    await asyncio.wait_for(again.done.wait(), 30)
    assert store.read(log_dir_of(tmp_path), again.task_id).scratch_chain_id == writer.task_id


async def test_a_busy_folder_fails_the_launch_as_not_started(tmp_path, monkeypatch):
    Capture(monkeypatch)
    monkeypatch.setattr(shared_scratch, "LOCK_WAIT_SECONDS", 0.05)
    registry = TaskRegistry(log_dir=log_dir_of(tmp_path), open_monitor=False)
    with shared_scratch.locked(log_dir_of(tmp_path), "chain", "busy-task"):
        with pytest.raises(shared_scratch.SharedScratchError) as failure:
            await launch(registry, "codex", tmp_path, task_id="busy-task")
    assert failure.value.code == "shared_scratch_busy" and failure.value.harflow_not_started
    assert not (tmp_path / "state" / "scratch" / "busy-task").exists()
    task = await launch(registry, "codex", tmp_path, task_id="busy-task")
    await stop(registry, task)


async def test_the_lock_is_released_once_the_record_is_durable(tmp_path, monkeypatch):
    Capture(monkeypatch)
    registry = TaskRegistry(log_dir=log_dir_of(tmp_path), open_monitor=False)
    task = await launch(registry, "codex", tmp_path, task_id="released")
    try:
        with shared_scratch.locked(log_dir_of(tmp_path), "chain", "released", timeout=0.05):
            pass
    finally:
        await stop(registry, task)


async def test_unsupported_grants_still_expose_the_folders_for_native_free_dispatch(tmp_path, monkeypatch):
    """A failed exec cleans the task scratch but never removes the shared folders or leaves a lock."""
    async def refuse(*argv, **kwargs):
        raise OSError("executable unavailable")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", refuse)
    registry = TaskRegistry(log_dir=log_dir_of(tmp_path), open_monitor=False)
    with pytest.raises(OSError):
        await launch(registry, "codex", tmp_path, task_id="never-ran", group="G")
    assert not (tmp_path / "state" / "scratch" / "never-ran").exists()
    with shared_scratch.locked(log_dir_of(tmp_path), "chain", "never-ran", timeout=0.05):
        pass


# ---- access states ---------------------------------------------------------------------------


@pytest.mark.parametrize("name,freedom,scratch_allowed,expected", [
    ("claude", "write_in_repo", True, "writable"),
    ("claude", "read_only", True, "no_additional_grant"),
    ("codex", "write_in_repo", True, "writable"),
    ("codex", "read_only", True, "sandbox_read_only"),
    ("codex", "write_in_repo", False, "sandbox_read_only"),  # codex app-server with caller approvals
    ("opencode", "write_in_repo", True, "no_additional_grant"),
])
async def test_access_states_never_overclaim(name, freedom, scratch_allowed, expected, tmp_path):
    registry = TaskRegistry(log_dir=log_dir_of(tmp_path), open_monitor=False)
    backend, invocation = build(name, tmp_path, freedom)
    enforcement = backend.enforcement(freedom)
    _, _, allocation = await registry._allocate_shared_scratch(
        invocation, backend=backend, freedom=freedom, network=None, enforcement=enforcement, task_id="access",
        chain_id="access", group="G", spawned_by=None, max_depth=None, hold=shared_scratch.Hold(), task_scratch=None,
        task_granted=False, scratch_allowed=scratch_allowed, extra_grants=False)
    assert allocation.access == {"chain": expected, "group": expected}
    assert (allocation.writable != []) is (expected == "writable")


# ---- nested authority ------------------------------------------------------------------------


def seed_caller(tmp_path, task_id="caller", *, confined=True, roots=()):
    log_dir = log_dir_of(tmp_path)
    enforcement = backends.get("codex").enforcement("write_in_repo").as_dict()
    if not confined:
        enforcement["writes_confined"] = False
    enforcement["writable_roots"] = list(roots)
    assert store.write_landed(log_dir, record(task_id, enforcement=enforcement))
    return enforcement


def allows(registry, tmp_path, kind, identity, task_id="child", spawned_by="caller"):
    path = shared_scratch.directory(log_dir_of(tmp_path), kind, identity)
    return registry._caller_allows_shared(kind, path, identity, task_id, spawned_by, 5)


def test_an_unconfined_caller_restricts_nothing(tmp_path):
    seed_caller(tmp_path, confined=False)
    registry = TaskRegistry(log_dir=log_dir_of(tmp_path), open_monitor=False)
    assert allows(registry, tmp_path, "group", "Z") and allows(registry, tmp_path, "chain", "other-family")


def test_a_confined_caller_grants_only_what_it_holds_or_began(tmp_path):
    log_dir = log_dir_of(tmp_path)
    held = str(shared_scratch.directory(log_dir, "group", "X"))
    seed_caller(tmp_path, roots=(held,))
    # Chains: root -> caller -> child1 -> child2 (a grandchild chain), plus a sibling family.
    store.write_landed(log_dir, record("child1", spawned_by="caller"))
    store.write_landed(log_dir, record("child2", spawned_by="child1"))
    store.write_landed(log_dir, record("sibling", spawned_by="elsewhere"))
    store.write_landed(log_dir, record("elsewhere"))
    registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
    assert allows(registry, tmp_path, "group", "X")                      # already one of the caller's roots
    assert not allows(registry, tmp_path, "group", "Y")                  # another family's group by renaming
    assert allows(registry, tmp_path, "chain", "child", task_id="child")  # a brand-new chain
    assert allows(registry, tmp_path, "chain", "child1")                 # a chain the caller began
    assert allows(registry, tmp_path, "chain", "child2")                 # a grandchild's chain, by strict ancestry
    assert not allows(registry, tmp_path, "chain", "sibling")            # a sibling family's chain
    assert not allows(registry, tmp_path, "chain", "caller")             # the caller's own root chain it does not hold
    assert not allows(registry, tmp_path, "chain", "unknown-chain")      # unreadable denies
    assert not allows(registry, tmp_path, "chain", "child1", spawned_by="missing-caller")


def test_the_denial_persists_across_hops(tmp_path):
    """A -> X -> X: the middle task holds the group read-only, so the grandchild is denied too."""
    log_dir = log_dir_of(tmp_path)
    group = str(shared_scratch.directory(log_dir, "group", "X"))
    seed_caller(tmp_path, "middle", roots=())  # the middle task was given the group read-only: not a writable root
    registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
    assert not allows(registry, tmp_path, "group", "X", spawned_by="middle")
    seed_caller(tmp_path, "holder", roots=(group,))
    assert allows(registry, tmp_path, "group", "X", spawned_by="holder")


# ---- artifacts -------------------------------------------------------------------------------


def test_artifacts_in_a_granted_shared_folder_count_as_own_scratch(tmp_path):
    log_dir = log_dir_of(tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    chain = shared_scratch.allocate(log_dir, "chain", "own")
    group = shared_scratch.allocate(log_dir, "group", "G")
    (chain / "shot.png").write_bytes(b"x")
    (group / "log.txt").write_bytes(b"x")
    granted = record("own", group="G", repo_path=str(repo),
                     enforcement={**backends.get("claude").enforcement("write_in_repo").as_dict(), "writable_roots": [str(chain), str(group)]},
                     scratch={"chain": str(chain), "group": str(group), "chain_id": "own", "group_name": "G"})
    assert store.write_landed(log_dir, granted)
    for path in (chain / "shot.png", group / "log.txt"):
        artifacts._target(log_dir, granted, str(path))
    # Without the grant in the recorded roots the folder is bridge state and is refused.
    bare = replace(granted, enforcement=backends.get("claude").enforcement("write_in_repo").as_dict())
    with pytest.raises(ValueError):
        artifacts._target(log_dir, bare, str(chain / "shot.png"))


# ---- listing and cleanup ---------------------------------------------------------------------


def populate(tmp_path):
    log_dir = log_dir_of(tmp_path)
    chain = shared_scratch.allocate(log_dir, "chain", "c1")
    group = shared_scratch.allocate(log_dir, "group", "MT-1")
    (chain / "a").write_bytes(b"x" * 1000)
    (group / "sub").mkdir()
    (group / "sub" / "b").write_bytes(b"y" * 500)
    return log_dir, chain, group


def test_inventory_lists_identity_size_and_active_users(tmp_path, identities):
    log_dir, chain, group = populate(tmp_path)
    store.write_landed(log_dir, record("c1", group="MT-1", chain="c1", status="running"))
    entries = {(e["kind"], e["identity"]): e for e in shared_scratch.inventory(log_dir)}
    assert entries[("chain", "c1")]["bytes"] >= 1000 and entries[("group", "MT-1")]["bytes"] >= 500
    assert entries[("chain", "c1")]["path"] == str(chain.resolve())
    assert entries[("group", "MT-1")]["active_tasks"] == 1 and entries[("group", "MT-1")]["last_used_at"]
    assert all(not e["bytes_truncated"] for e in entries.values())


def test_directory_size_does_not_follow_symlinks(tmp_path):
    big = tmp_path / "big"
    big.mkdir()
    (big / "f").write_bytes(b"z" * 100000)
    folder = tmp_path / "folder"
    folder.mkdir()
    (folder / "link").symlink_to(big, target_is_directory=True)
    size, truncated = shared_scratch.directory_size(folder)
    assert size < 1000 and not truncated


@pytest.mark.parametrize("status", ["running"])
def test_clean_refuses_while_a_task_is_unsettled(tmp_path, identities, status):
    log_dir, chain, group = populate(tmp_path)
    store.write_landed(log_dir, record("c1", group="MT-1", chain="c1", status=status))
    for kind, identity, path in (("chain", "c1", chain), ("group", "MT-1", group)):
        with pytest.raises(shared_scratch.SharedScratchError) as failure:
            shared_scratch.clean(log_dir, kind, identity)
        assert failure.value.code == "shared_scratch_in_use" and "c1" in str(failure.value) and path.exists()


def test_clean_refuses_on_a_pending_launch_receipt(tmp_path):
    log_dir, chain, group = populate(tmp_path)
    receipt = {"task_id": "waiting", "state": "waiting_for_resources", "group": "MT-1", "scratch_chain_id": "c1", "chain_root_task_id": None}
    state = shared_scratch.State(log_dir)
    state.receipts = [receipt]
    assert [b["kind"] for b in state.blockers("chain", "c1")] == ["launch"]
    assert [b["kind"] for b in state.blockers("group", "MT-1")] == ["launch"]
    assert state.blockers("group", "other") == []


def test_clean_refuses_on_a_pinned_record_and_a_registered_artifact(tmp_path):
    log_dir, chain, group = populate(tmp_path)
    store.write_landed(log_dir, record("c1", group="MT-1", chain="c1"))
    state = shared_scratch.State(log_dir)
    assert state.blockers("chain", "c1", chain) == []
    state.pinned = {"c1"}
    assert [b["kind"] for b in state.blockers("chain", "c1", chain)] == ["pinned"]
    state.pinned = set()
    (log_dir / "c1.artifacts.json").write_text(json.dumps({"items": [{"path": str(chain / "a")}]}))
    assert [b["kind"] for b in state.blockers("chain", "c1", chain)] == ["artifact"]
    assert state.blockers("group", "MT-1", group) == []
    (log_dir / "c1.artifacts.json").write_text("not json")
    assert [b["kind"] for b in state.blockers("chain", "c1", chain)] == ["artifact"]


def test_clean_refuses_when_a_record_cannot_be_read(tmp_path):
    log_dir, chain, _group = populate(tmp_path)
    (log_dir / "broken.meta.json").write_text("{not json")
    with pytest.raises(shared_scratch.SharedScratchError) as failure:
        shared_scratch.clean(log_dir, "chain", "c1")
    assert failure.value.code == "shared_scratch_in_use" and chain.exists()


def test_clean_removes_a_settled_folder_through_trash_and_its_metadata(tmp_path):
    log_dir, chain, group = populate(tmp_path)
    store.write_landed(log_dir, record("c1", group="MT-1", chain="c1"))
    result = shared_scratch.clean(log_dir, "group", "MT-1")
    assert result["deleted"] and not group.exists() and chain.exists()
    assert not (tmp_path / "state" / "shared-scratch" / ".meta" / "group" / f"{group.name}.json").exists()
    assert list((tmp_path / "state" / "shared-scratch" / ".trash").iterdir()) == []
    with pytest.raises(shared_scratch.SharedScratchError) as failure:
        shared_scratch.clean(log_dir, "group", "MT-1")
    assert failure.value.code == "shared_scratch_unknown"


def test_clean_does_not_follow_a_symlink_inside_the_folder(tmp_path):
    log_dir, chain, _group = populate(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("safe")
    (chain / "link").symlink_to(outside, target_is_directory=True)
    shared_scratch.clean(log_dir, "chain", "c1")
    assert (outside / "keep").read_text() == "safe" and not chain.exists()


def test_clean_is_serialized_with_allocation(tmp_path):
    log_dir, chain, _group = populate(tmp_path)
    with shared_scratch.locked(log_dir, "chain", "c1"):
        with pytest.raises(shared_scratch.SharedScratchError) as failure:
            shared_scratch.clean(log_dir, "chain", "c1")
    assert failure.value.code == "shared_scratch_busy" and chain.exists()


async def test_clean_cannot_overtake_a_launch_between_allocation_and_its_record(tmp_path, identities):
    """The launch holds the lock from before the existence check until its record is durable."""
    log_dir = log_dir_of(tmp_path)
    registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
    backend, invocation = build("opencode", tmp_path)
    hold = shared_scratch.Hold()
    _, _, allocation = await registry._allocate_shared_scratch(
        invocation, backend=backend, freedom="write_in_repo", network=None, enforcement=backend.enforcement("write_in_repo"),
        task_id="launching", chain_id="launching", group=None, spawned_by=None, max_depth=None, hold=hold,
        task_scratch=None, task_granted=False, scratch_allowed=True, extra_grants=False)
    try:
        with pytest.raises(shared_scratch.SharedScratchError) as failure:
            shared_scratch.clean(log_dir, "chain", "launching")
        assert failure.value.code == "shared_scratch_busy" and Path(allocation.chain).is_dir()
    finally:
        hold.release()
    # Record durable: a cleaner now sees it as a reference instead.
    store.write_landed(log_dir, record("launching", chain="launching", status="running"))
    with pytest.raises(shared_scratch.SharedScratchError) as failure:
        shared_scratch.clean(log_dir, "chain", "launching")
    assert failure.value.code == "shared_scratch_in_use"


# ---- the age policy --------------------------------------------------------------------------


NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)


def backdate(log_dir, kind, identity, days):
    key = shared_scratch.key_for(kind, identity)
    meta_path = log_dir.parent / "shared-scratch" / ".meta" / kind / f"{key}.json"
    meta = json.loads(meta_path.read_text())
    meta["last_used_at"] = (NOW - timedelta(days=days)).isoformat()
    meta_path.write_text(json.dumps(meta))


def test_the_sweep_removes_only_old_unreferenced_folders(tmp_path):
    log_dir, chain, group = populate(tmp_path)
    backdate(log_dir, "chain", "c1", 30)
    backdate(log_dir, "group", "MT-1", 3)
    stats = shared_scratch.sweep(log_dir, 14, NOW)
    assert stats["removed"] == 1 and not chain.exists() and group.exists()


def test_the_sweep_age_counts_from_the_newest_referencing_record(tmp_path):
    log_dir, chain, _group = populate(tmp_path)
    backdate(log_dir, "chain", "c1", 30)
    store.write_landed(log_dir, record("c1", chain="c1", finished_at=(NOW - timedelta(days=2)).isoformat()))
    assert shared_scratch.sweep(log_dir, 14, NOW)["removed"] == 0 and chain.exists()


def test_the_sweep_keeps_referenced_folders_however_old(tmp_path, identities):
    log_dir, chain, group = populate(tmp_path)
    backdate(log_dir, "chain", "c1", 90)
    backdate(log_dir, "group", "MT-1", 90)
    store.write_landed(log_dir, record("c1", group="MT-1", chain="c1", status="running", started_at="2026-01-01T00:00:00+00:00"))
    stats = shared_scratch.sweep(log_dir, 14, NOW)
    assert stats["removed"] == 0 and chain.exists() and group.exists()


def test_the_sweep_is_serialized_with_a_resume(tmp_path):
    log_dir, chain, _group = populate(tmp_path)
    backdate(log_dir, "chain", "c1", 90)
    with shared_scratch.locked(log_dir, "chain", "c1"):
        stats = shared_scratch.sweep(log_dir, 14, NOW)
    assert stats["kept_busy"] == 1 and chain.exists()


def test_task_retention_cannot_shorten_the_shared_retention(tmp_path):
    from harflow_mcp import retention
    log_dir, chain, _group = populate(tmp_path)
    last = NOW - timedelta(days=5)
    backdate(log_dir, "chain", "c1", 90)
    store.write_landed(log_dir, record("c1", chain="c1", finished_at=last.isoformat()))
    retention._touch_shared_scratch(log_dir, store.read(log_dir, "c1"))
    meta = json.loads((tmp_path / "state" / "shared-scratch" / ".meta" / "chain" / "c1.json").read_text())
    assert datetime.fromisoformat(meta["last_used_at"]) >= last
    # The record is gone (task retention was shorter than shared retention); the folder still counts from it.
    (log_dir / "c1.meta.json").unlink()
    assert shared_scratch.sweep(log_dir, 14, NOW)["removed"] == 0 and chain.exists()
    assert shared_scratch.sweep(log_dir, 14, NOW + timedelta(days=30))["removed"] == 1


def test_the_sweep_uses_the_mtime_of_a_folder_with_no_metadata(tmp_path):
    log_dir = log_dir_of(tmp_path)
    orphan = tmp_path / "state" / "shared-scratch" / "chain" / "orphan"
    orphan.mkdir(parents=True)
    os.utime(orphan, (NOW.timestamp() - 40 * 86400,) * 2)
    assert shared_scratch.sweep(log_dir, 14, NOW)["removed"] == 1 and not orphan.exists()


def test_the_age_policy_is_configurable(monkeypatch):
    assert shared_scratch.days_retained({}) == 14
    assert shared_scratch.days_retained({"HARFLOW_SHARED_SCRATCH_DAYS": "3"}) == 3
    for disabled in ("0", "-1", "soon"):
        assert shared_scratch.days_retained({"HARFLOW_SHARED_SCRATCH_DAYS": disabled}) is None


def test_the_retention_sweep_runs_the_shared_policy(tmp_path, monkeypatch):
    from harflow_mcp import retention
    log_dir, chain, _group = populate(tmp_path)
    backdate(log_dir, "chain", "c1", 90)
    monkeypatch.setenv("HARFLOW_SHARED_SCRATCH_DAYS", "14")
    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc) + timedelta(days=100))
    assert stats["deleted_shared_scratch"] >= 1 and not chain.exists()
    monkeypatch.setenv("HARFLOW_SHARED_SCRATCH_DAYS", "0")
    other = shared_scratch.allocate(log_dir, "chain", "c2")
    backdate(log_dir, "chain", "c2", 90)
    retention.sweep(log_dir, 30, datetime.now(timezone.utc) + timedelta(days=100))
    assert other.exists()


def test_completion_restart_and_recovery_never_remove_shared_folders(tmp_path):
    from harflow_mcp import retention
    log_dir, chain, group = populate(tmp_path)
    store.write_landed(log_dir, record("c1", group="MT-1", chain="c1"))
    with state_ownership.state_session(resolve_layout(root=tmp_path / "state")):
        assert retention._delete_task_files(log_dir, "c1")
    assert chain.exists() and group.exists() and (chain / "a").exists()


# ---- ctl -------------------------------------------------------------------------------------


def test_ctl_scratch_list_and_clean(tmp_path, monkeypatch, capsys, identities):
    from harflow_mcp import ctl, lineage
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "state"))
    log_dir, chain, group = populate(tmp_path)
    store.write_landed(log_dir, record("c1", group="MT-1", chain="c1", status="running"))
    assert ctl.main(["scratch", "list", "--json"]) == 0
    listed = json.loads(capsys.readouterr().out.splitlines()[-1])["shared_scratch"]
    assert {(e["kind"], e["identity"]) for e in listed} == {("chain", "c1"), ("group", "MT-1")}
    assert all(e["bytes"] > 0 and e["active_tasks"] == 1 for e in listed)
    assert ctl.main(["scratch", "list"]) == 0
    assert "MT-1" in capsys.readouterr().out
    # A person is required, undecidable callers are refused, and an active task keeps the folder.
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None, "ps unreadable"))
    assert ctl.main(["scratch", "clean", "--group", "MT-1", "--json"]) == 1
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["error"]["code"] == "caller_undecidable"
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None))
    assert ctl.main(["scratch", "clean", "--group", "MT-1", "--json"]) == 1
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["error"]["code"] == "shared_scratch_in_use" and group.exists()
    store.write_landed(log_dir, record("c1", group="MT-1", chain="c1"))
    assert ctl.main(["scratch", "clean", "--group", "MT-1", "--json"]) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["result"]["deleted"] is True and not group.exists()
    assert ctl.main(["scratch", "clean", "--chain", "../escape", "--json"]) == 1
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["error"]["code"] == "invalid_chain_id"
    assert chain.exists()


def test_ctl_scratch_clean_needs_exactly_one_target():
    from harflow_mcp import ctl
    parser = ctl._build_parser()[0]
    for argv in (["scratch", "clean"], ["scratch", "clean", "--group", "a", "--chain", "b"], ["scratch"]):
        with pytest.raises(SystemExit):
            parser.parse_args(argv)
    assert parser.parse_args(["scratch", "clean", "--chain", "abc"]).chain == "abc"


# ---- chain derivation and receipts -------------------------------------------------------------


async def test_a_continuation_starts_in_its_sources_chain(tmp_path):
    from test_task_continuation import Registry, record as source_record
    from harflow_mcp.task_continuation import continue_task
    log_dir = log_dir_of(tmp_path)
    source = source_record(log_dir, scratch_chain_id="first-in-chain")
    registry = Registry(tmp_path)
    await continue_task(registry, source.task_id, "carry on")
    assert registry.start.await_args.kwargs["scratch_chain_id"] == "first-in-chain"
    # An older record with no chain field uses its continuation root.
    older = source_record(log_dir)
    assert shared_scratch.effective_chain(older) == older.task_id


def test_a_launch_receipt_carries_the_chain_and_group(tmp_path):
    from harflow_mcp import task_supervision
    log_dir = log_dir_of(tmp_path)
    info = log_dir.stat()
    document = {
        "format": task_supervision.FORMAT, "product": "harflow", "product_version": task_supervision.PRODUCT_VERSION, "v": 1,
        "task_id": "waiting", "generation": "c" * 32, "root": str(log_dir.parent), "directory_identity": [info.st_dev, info.st_ino],
        "state": "waiting_for_resources", "request_sha256": "d" * 64, "configuration_sha256": "e" * 64, "backend": "claude",
        "session_id": None, "parent_task_id": None, "chain_root_task_id": None, "scratch_chain_id": "chain-1", "group": "MT-1",
        "owner": None, "repo_path": str(tmp_path), "freedom": "write_in_repo"}
    task_supervision._write(log_dir / "waiting.launch.json", document)
    assert task_supervision.read_launch(log_dir, "waiting")["scratch_chain_id"] == "chain-1"
    state = shared_scratch.State(log_dir)
    assert [b["task_id"] for b in state.blockers("chain", "chain-1")] == ["waiting"]
    assert [b["task_id"] for b in state.blockers("group", "MT-1")] == ["waiting"]
    assert state.blockers("chain", "other") == []
    task_supervision._write(log_dir / "waiting.launch.json", document | {"scratch_chain_id": 7})
    with pytest.raises(store.TaskDocumentError):
        task_supervision.read_launch(log_dir, "waiting")


def test_the_new_record_fields_are_additive(tmp_path):
    log_dir = log_dir_of(tmp_path)
    older = record("older")
    store.write_landed(log_dir, older)
    document = json.loads((log_dir / "older.meta.json").read_text())
    for field in ("scratch_chain_id", "scratch"):
        document.pop(field, None)
    (log_dir / "older.meta.json").write_text(json.dumps(document))
    loaded = store.read(log_dir, "older")
    assert loaded.scratch_chain_id is None and loaded.scratch is None
    with state_ownership.state_session(resolve_layout(root=tmp_path / "state")):
        snapshot = store.snapshot(log_dir, loaded)
    assert "scratch" not in snapshot


async def test_recovered_status_reports_the_folders(tmp_path):
    log_dir = log_dir_of(tmp_path)
    scratch = {"task": "/a", "chain": "/b", "group": None, "chain_id": "c", "group_name": None,
               "access": {"task": "writable", "chain": "writable"}, "writable": ["/a", "/b"]}
    store.write_landed(log_dir, record("recovered", chain="c", scratch=scratch))
    loaded = store.read(log_dir, "recovered")
    assert loaded.scratch == scratch and loaded.scratch_chain_id == "c"
    assert store.snapshot(log_dir, loaded)["scratch"] == scratch
    assert store.brief(log_dir, loaded)["scratch"] == scratch


async def test_the_permission_preview_discloses_the_shared_roots_symbolically(tmp_path):
    from harflow_mcp import tasks
    registry = TaskRegistry(log_dir=log_dir_of(tmp_path), open_monitor=False)
    repo = tmp_path / "repo"
    repo.mkdir()
    for name in ("claude", "codex"):
        preview = await registry.preview_permissions(repo, backend=backends.get(name), freedom="write_in_repo")
        roots = preview["enforcement"]["writable_roots"]
        assert tasks.FUTURE_TASK_SCRATCH_ROOT in roots and tasks.FUTURE_CHAIN_SCRATCH_ROOT in roots and tasks.FUTURE_GROUP_SCRATCH_ROOT in roots
    read_only = await registry.preview_permissions(repo, backend=backends.get("claude"), freedom="read_only")
    assert tasks.FUTURE_CHAIN_SCRATCH_ROOT not in read_only["enforcement"]["writable_roots"]
    assert not (tmp_path / "state" / "shared-scratch").exists()
