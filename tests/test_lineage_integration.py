"""Lineage recording and the nested-dispatch caps, wired through `TaskRegistry` and the server.

`tests/test_lineage.py` covers `lineage.detect_caller` in isolation; `tests/test_caps.py` covers
the pure enforcement comparison. This file covers the parts that only exist once the two are
plumbed into `TaskRegistry.start`/`resume`/`resume_record` and `server.start_task`: lineage fields
landing on a spawned `Task`, its persisted record, its `task_started` event, and the caps actually
refusing (or allowing) a dispatch — plus the session lock that now wraps `resume`/`resume_record`.

Every test here either leaves `lineage.detect_caller` neutralised by the autouse
`no_caller_detected` fixture (root-task cases) or monkeypatches it directly to a fixed `Caller` —
never the real process-tree walk — so nothing depends on this machine's actual process tree.
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp import Client, MCPError

from polybridge import backends, control, lineage, server, store
from polybridge import tasks as tasks_module
from polybridge.backends import BACKENDS, NestedDispatchRefused
from polybridge.backends import Invocation
from polybridge.backends.base import Enforcement
from polybridge.backends.codex import CodexBackend
from polybridge.lineage import Caller
from polybridge.tasks import SessionBusyError, TaskRegistry


async def call(tool: str, **arguments):
    """Call one tool on an in-memory MCP client — same pattern as `tests/test_server.py`."""
    error: MCPError | None = None
    async with Client(server.mcp) as client:
        try:
            return await client.call_tool(tool, arguments)
        except MCPError as exc:
            error = exc
    raise error


class _FakeBackend:
    """A backend double that spawns a real, instant process, so `_spawn` runs end to end without
    a real agent CLI. `enforcement_kwargs` lets a test shape exactly the `Enforcement` fields the
    nested-dispatch caps compare."""

    def __init__(
        self,
        name: str = "fake",
        argv: list[str] | None = None,
        enforcement_kwargs: dict | None = None,
    ) -> None:
        self.name = name
        self._argv = argv or ["/bin/echo", "hi"]
        self.binary = self._argv[0]
        self.capabilities = SimpleNamespace(chooses_session_id=False)
        self._enforcement_kwargs = enforcement_kwargs or {}

    def build_start_argv(self, prompt, **kwargs):
        return Invocation(list(self._argv))

    def build_resume_argv(self, prompt, **kwargs):
        return Invocation(list(self._argv))

    def assert_safe(self, invocation, freedom, network=None):
        assert isinstance(invocation, Invocation)
        return None

    def encode_live_message(self, text):
        raise backends.UnsupportedCapability("no live input")

    def enforcement(self, freedom, network=None):
        return Enforcement(
            freedom=freedom,
            mechanism="fake",
            os_enforced=False,
            writes_confined=False,
            **self._enforcement_kwargs,
        )

    def ingest(self, event, acc):
        return None

    def normalize(self, event, acc):
        return []

    def classify(self, acc, exit_code):
        return "completed"


def make_lineage_record(
    repo_path: Path,
    task_id: str,
    *,
    backend: str = "claude",
    freedom: str = "write_in_repo",
    enforcement: dict | None = None,
    depth: int = 0,
    max_depth: int | None = 2,
    group: str | None = None,
    session_id: str | None = None,
    status: str = "running",
    root_task_id: str | None = None,
) -> store.TaskRecord:
    """A `TaskRecord` shaped for a `Caller`'s `.record`, or for a resumed task. Never written to
    disk here — a `Caller` is handed to `detect_caller`'s stub directly, so its record only needs
    to exist as a Python object."""
    return store.TaskRecord(
        task_id=task_id,
        backend=backend,
        session_id=session_id,
        repo_path=str(repo_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        freedom=freedom,
        enforcement=enforcement,
        depth=depth,
        max_depth=max_depth,
        group=group,
        root_task_id=root_task_id,
        status=status,
    )


# --- Root task: no caller detected ----------------------------------------------------------


async def test_root_task_gets_default_lineage_everywhere_it_is_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No caller detected (the autouse fixture's default) means a root task: no `spawned_by`, its
    own id as `root_task_id`, depth 0, `max_depth` from `PB_MAX_DEPTH`, no detection method. Must
    show up identically on the live `Task`, its persisted `TaskRecord`, and its `task_started`
    event."""
    monkeypatch.setenv("PB_MAX_DEPTH", "7")
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    fake = _FakeBackend()
    monkeypatch.setitem(backends.BACKENDS, fake.name, fake)

    task = await registry.start("hi", tmp_path, backend=fake)
    await task.done.wait()

    assert task.spawned_by is None
    assert task.root_task_id == task.task_id
    assert task.depth == 0
    assert task.max_depth == 7
    assert task.group is None
    assert task.lineage_detected is None

    brief = task.brief()
    for key in ("spawned_by", "root_task_id", "depth", "max_depth", "group", "lineage_detected"):
        assert key in brief
    assert brief["spawned_by"] is None
    assert brief["root_task_id"] == task.task_id
    assert brief["depth"] == 0
    assert brief["max_depth"] == 7
    assert brief["lineage_detected"] is None

    snap = task.snapshot()
    assert snap["root_task_id"] == task.task_id
    assert snap["max_depth"] == 7

    record = store.read(registry.log_dir, task.task_id)
    assert record is not None
    assert record.spawned_by is None
    assert record.root_task_id == task.task_id
    assert record.depth == 0
    assert record.max_depth == 7
    assert record.group is None
    assert record.lineage_detected is None

    record_brief = store.brief(registry.log_dir, record)
    assert record_brief["root_task_id"] == task.task_id
    assert record_brief["max_depth"] == 7

    record_snapshot = store.snapshot(registry.log_dir, record)
    assert record_snapshot["root_task_id"] == task.task_id
    assert record_snapshot["max_depth"] == 7

    events_lines = [
        json.loads(line)
        for line in (registry.log_dir / f"{task.task_id}.events.jsonl").read_text().splitlines()
    ]
    started = next(e for e in events_lines if e["kind"] == "task_started")
    assert started["spawned_by"] is None
    assert started["root_task_id"] == task.task_id
    assert started["depth"] == 0
    assert started["max_depth"] == 7
    assert started["group"] is None
    assert started["lineage_detected"] is None


async def test_max_depth_default_falls_back_when_env_is_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("PB_MAX_DEPTH", raising=False)
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    fake = _FakeBackend()
    monkeypatch.setitem(backends.BACKENDS, fake.name, fake)

    task = await registry.start("hi", tmp_path, backend=fake)
    await task.done.wait()

    assert task.max_depth == lineage.DEFAULT_MAX_DEPTH


# --- Inherited lineage: a caller is detected ------------------------------------------------


async def test_child_inherits_lineage_from_a_detected_caller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    fake = _FakeBackend(enforcement_kwargs={"writable_roots": ("/tmp",)})
    monkeypatch.setitem(backends.BACKENDS, fake.name, fake)

    parent_enforcement = fake.enforcement("write_in_repo").as_dict()
    parent = make_lineage_record(
        tmp_path,
        "parent-task",
        backend=fake.name,
        freedom="write_in_repo",
        enforcement=parent_enforcement,
        depth=0,
        max_depth=2,
        group="g1",
        root_task_id="parent-task",
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: Caller(parent, "session"))

    task = await registry.start("hi", tmp_path, backend=fake, freedom="write_in_repo")
    await task.done.wait()

    assert task.spawned_by == "parent-task"
    assert task.root_task_id == "parent-task"
    assert task.depth == 1
    assert task.max_depth == 2
    assert task.lineage_detected == "session"
    # No explicit group was given, so it is inherited from the caller.
    assert task.group == "g1"


async def test_explicit_group_overrides_the_callers_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    fake = _FakeBackend(enforcement_kwargs={"writable_roots": ("/tmp",)})
    monkeypatch.setitem(backends.BACKENDS, fake.name, fake)

    parent = make_lineage_record(
        tmp_path,
        "parent-task",
        backend=fake.name,
        freedom="write_in_repo",
        enforcement=fake.enforcement("write_in_repo").as_dict(),
        depth=0,
        max_depth=2,
        group="g1",
        root_task_id="parent-task",
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: Caller(parent, "session"))

    task = await registry.start("hi", tmp_path, backend=fake, freedom="write_in_repo", group="g2")
    await task.done.wait()

    assert task.group == "g2"


async def test_root_task_id_is_inherited_through_a_grandchild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A caller one level below the root reports its own `root_task_id` (inherited from *its*
    caller, not its own id) — the grandchild must carry that same root forward."""
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    fake = _FakeBackend(enforcement_kwargs={"writable_roots": ("/tmp",)})
    monkeypatch.setitem(backends.BACKENDS, fake.name, fake)

    parent = make_lineage_record(
        tmp_path,
        "mid-task",
        backend=fake.name,
        freedom="write_in_repo",
        enforcement=fake.enforcement("write_in_repo").as_dict(),
        depth=1,
        max_depth=3,
        root_task_id="root-task",
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: Caller(parent, "ancestry"))

    task = await registry.start("hi", tmp_path, backend=fake, freedom="write_in_repo")
    await task.done.wait()

    assert task.spawned_by == "mid-task"
    assert task.root_task_id == "root-task"
    assert task.depth == 2
    assert task.max_depth == 3


# --- Env passed to the spawned process -------------------------------------------------------


async def test_lineage_env_vars_reach_the_spawned_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out_path = tmp_path / "envs.txt"
    script = (
        "import os, sys\n"
        "with open(sys.argv[1], 'w') as f:\n"
        "    f.write('\\n'.join([\n"
        "        os.environ.get('PB_TASK_ID', ''),\n"
        "        os.environ.get('PB_ROOT_TASK_ID', ''),\n"
        "        os.environ.get('PB_DEPTH', ''),\n"
        "    ]))\n"
    )
    fake = _FakeBackend(
        name="env-capture",
        argv=[sys.executable, "-c", script, str(out_path)],
        enforcement_kwargs={"writable_roots": ("/tmp",)},
    )
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    monkeypatch.setitem(backends.BACKENDS, fake.name, fake)

    parent = make_lineage_record(
        tmp_path,
        "parent-task",
        backend=fake.name,
        freedom="write_in_repo",
        enforcement=fake.enforcement("write_in_repo").as_dict(),
        depth=1,
        max_depth=4,
        root_task_id="root-task",
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: Caller(parent, "pb_task_id"))

    task = await registry.start("hi", tmp_path, backend=fake, freedom="write_in_repo")
    await task.done.wait()

    assert task.depth == 2
    assert task.root_task_id == "root-task"

    written_task_id, written_root_task_id, written_depth = out_path.read_text().splitlines()
    assert written_task_id == task.task_id
    assert written_root_task_id == "root-task"
    assert written_depth == "2"


# --- Caps applied via start() ------------------------------------------------------------------


async def test_caps_refuse_a_weaker_start_on_writable_roots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    codex = BACKENDS["codex"]
    parent = make_lineage_record(
        tmp_path,
        "caller-task",
        backend="codex",
        freedom="read_only",
        enforcement=codex.enforcement("read_only").as_dict(),
        depth=0,
        max_depth=2,
        root_task_id="caller-task",
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: Caller(parent, "pb_task_id"))

    with pytest.raises(NestedDispatchRefused) as exc_info:
        await registry.start("hi", tmp_path, backend=codex, freedom="write_in_repo")

    assert exc_info.value.rule == "writable_roots"
    # No process must have been spawned — the check runs before `_spawn`.
    assert registry.list() == []


async def test_caps_refuse_when_the_depth_budget_is_exhausted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    fake = _FakeBackend()
    monkeypatch.setitem(backends.BACKENDS, fake.name, fake)

    parent = make_lineage_record(
        tmp_path,
        "caller-task",
        backend=fake.name,
        freedom="write_in_repo",
        enforcement=fake.enforcement("write_in_repo").as_dict(),
        depth=2,
        max_depth=2,
        root_task_id="caller-task",
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: Caller(parent, "ancestry"))

    with pytest.raises(NestedDispatchRefused) as exc_info:
        await registry.start("hi", tmp_path, backend=fake, freedom="write_in_repo")

    assert exc_info.value.rule == "depth"
    assert registry.list() == []


async def test_caps_allow_a_dispatch_no_weaker_than_its_caller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    fake = _FakeBackend(enforcement_kwargs={"writable_roots": ("/tmp",)})
    monkeypatch.setitem(backends.BACKENDS, fake.name, fake)

    parent = make_lineage_record(
        tmp_path,
        "caller-task",
        backend=fake.name,
        freedom="write_in_repo",
        enforcement=fake.enforcement("write_in_repo").as_dict(),
        depth=0,
        max_depth=2,
        root_task_id="caller-task",
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: Caller(parent, "session"))

    task = await registry.start("hi", tmp_path, backend=fake, freedom="write_in_repo")
    await task.done.wait()

    assert task.status == "completed"
    assert task.depth == 1


# --- Caps applied via resume_record -------------------------------------------------------------


async def test_caps_refuse_a_weaker_resume_record_on_writable_roots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    codex = BACKENDS["codex"]
    repo = tmp_path / "repo"
    repo.mkdir()
    resumed = make_lineage_record(
        repo,
        "resumed-task",
        backend="codex",
        freedom="write_in_repo",
        session_id="resume-session",
        status="completed",
    )

    parent = make_lineage_record(
        tmp_path,
        "caller-task",
        backend="codex",
        freedom="read_only",
        enforcement=codex.enforcement("read_only").as_dict(),
        depth=0,
        max_depth=2,
        root_task_id="caller-task",
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: Caller(parent, "pb_task_id"))

    with pytest.raises(NestedDispatchRefused) as exc_info:
        await registry.resume_record(resumed, "continue")

    assert exc_info.value.rule == "writable_roots"


# --- Caps applied via the server tool ------------------------------------------------------------


async def test_start_task_maps_nested_dispatch_refused_to_invalid_params(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    codex = BACKENDS["codex"]
    parent = make_lineage_record(
        git_repo,
        "caller-task",
        backend="codex",
        freedom="read_only",
        enforcement=codex.enforcement("read_only").as_dict(),
        depth=0,
        max_depth=2,
        root_task_id="caller-task",
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: Caller(parent, "pb_task_id"))

    with pytest.raises(MCPError, match="writable_roots"):
        await call(
            "start_task",
            prompt="x",
            repo_path=str(git_repo),
            backend="codex",
            freedom="write_in_repo",
        )

    # No task must have been persisted — the refusal happens before any process is spawned.
    assert server._reg().list() == []


async def test_resume_task_maps_nested_dispatch_refused_to_invalid_params(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    codex = BACKENDS["codex"]
    resumed = make_lineage_record(
        git_repo,
        "resumed-task",
        backend="codex",
        freedom="write_in_repo",
        session_id="resume-session-2",
        status="completed",
    )
    store.write(server._reg().log_dir, resumed)

    parent = make_lineage_record(
        git_repo,
        "caller-task-2",
        backend="codex",
        freedom="read_only",
        enforcement=codex.enforcement("read_only").as_dict(),
        depth=0,
        max_depth=2,
        root_task_id="caller-task-2",
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: Caller(parent, "pb_task_id"))

    with pytest.raises(MCPError, match="writable_roots"):
        await call("resume_task", task_id="resumed-task", followup_prompt="continue")


# --- group parameter validation on the server tool ------------------------------------------------


async def test_start_task_rejects_an_empty_group(git_repo: Path) -> None:
    with pytest.raises(MCPError, match="non-empty"):
        await call("start_task", prompt="x", repo_path=str(git_repo), group="   ")


async def test_start_task_rejects_an_overlong_group(git_repo: Path) -> None:
    with pytest.raises(MCPError, match="128"):
        await call("start_task", prompt="x", repo_path=str(git_repo), group="g" * 129)


# --- Session lock around resume_record's check-and-spawn --------------------------------------


async def test_session_lock_serialises_concurrent_resume_record_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    repo = tmp_path / "repo"
    repo.mkdir()
    record = store.TaskRecord(
        task_id="parent-task",
        backend="claude",
        session_id="shared-session",
        markers=["shared-session"],
        repo_path=str(repo),
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
    )

    release_first = asyncio.Event()
    entered_spawn = asyncio.Event()
    live_run_checks: list[bool] = []
    real_check = registry.session_has_live_run

    def tracking_check(session_id: str | None) -> bool:
        live_run_checks.append(release_first.is_set())
        return real_check(session_id)

    monkeypatch.setattr(registry, "session_has_live_run", tracking_check)

    async def slow_spawn(argv, **kwargs):
        entered_spawn.set()
        await release_first.wait()
        child = tasks_module.Task(
            task_id="child-1",
            backend="claude",
            session_id="shared-session",
            repo_path=repo,
            prompt="go",
            max_turns=None,
            log_path=repo / "child-1.jsonl",
            started_at=datetime.now(timezone.utc),
        )
        registry._tasks[child.task_id] = child
        registry.persist(child)
        return child

    monkeypatch.setattr(registry, "_spawn", slow_spawn)

    first_task = asyncio.create_task(registry.resume_record(record, "go"))
    await asyncio.wait_for(entered_spawn.wait(), timeout=5)

    second_task = asyncio.create_task(registry.resume_record(record, "also go"))
    # Give the event loop a turn: `second_task` must be blocked on the session lock, not racing
    # ahead of the first.
    await asyncio.sleep(0.05)
    assert not second_task.done()

    release_first.set()
    first_result = await asyncio.wait_for(first_task, timeout=5)
    assert first_result.task_id == "child-1"

    with pytest.raises(SessionBusyError):
        await asyncio.wait_for(second_task, timeout=5)

    # The first call's check ran before the lock was released (release_first still False at that
    # point); the second's only ran after — proving the lock, not luck, serialised them.
    assert live_run_checks == [False, True]


async def test_session_lock_timeout_becomes_a_session_busy_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path / "streams")
    repo = tmp_path / "repo"
    repo.mkdir()
    record = store.TaskRecord(
        task_id="parent-task",
        backend="claude",
        session_id="locked-session",
        markers=["locked-session"],
        repo_path=str(repo),
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
    )

    monkeypatch.setattr(tasks_module, "SESSION_LOCK_TIMEOUT_SECONDS", 0.05)

    lock_path = control.session_lock_path(registry.log_dir, record.session_id)
    fd = await control.acquire(lock_path, timeout=5.0)
    try:
        with pytest.raises(SessionBusyError):
            await registry.resume_record(record, "go")
    finally:
        control.release(fd)
