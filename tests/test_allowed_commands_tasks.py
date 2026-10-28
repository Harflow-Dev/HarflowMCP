"""Dispatch-time `allowed_commands` through the registry, server, store and ctl, against a scripted `claude` executable."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp import MCPError

import fake_claude_cli
from harflow_mcp import backends, ctl, lineage, server, store
from harflow_mcp.backends import UnsupportedCapability, get
from harflow_mcp.backends.base import NestedDispatchRefused
from harflow_mcp.command_grants import CommandGrantError
from harflow_mcp.lineage import Caller
from harflow_mcp.tasks import TaskRegistry
from harflow_mcp.task_continuation import continue_task as continuation

GRANTS = ["make test:*", "python3 -m pytest:*"]
ALLOWED = "Bash(make test:*),Bash(python3 -m pytest:*)"
PUBLISH = "Bash(git commit:*),Bash(git push:*)"


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, git_repo: Path, synthetic_permission_identity) -> SimpleNamespace:
    bin_dir = fake_claude_cli.install(tmp_path / "bin")
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    log = tmp_path / "argv.jsonl"
    monkeypatch.setenv("FAKE_CLAUDE_ARGV_LOG", str(log))
    repo = git_repo
    registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)

    def launches() -> list[list[str]]:
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    def flag(argv: list[str], name: str) -> list[str]:
        return [argv[i + 1] for i, token in enumerate(argv) if token == name]

    return SimpleNamespace(registry=registry, repo=repo, launches=launches, flag=flag, tmp=tmp_path, log_dir=registry.log_dir)


async def run(env, **kwargs):
    task = await env.registry.start("go", env.repo, backend=get("claude"), freedom=kwargs.pop("freedom", "write_in_repo"), **kwargs)
    await asyncio.wait_for(task.done.wait(), timeout=30)
    await asyncio.wait_for(task.monitor, timeout=30) if task.monitor is not None else None
    return task


# --- start ----------------------------------------------------------------------------------------

async def test_scripted_permissions_use_the_fixture_seed_without_host_probes(env, monkeypatch):
    from harflow_mcp.backends import qualification
    def unexpected_probe(*_args):
        raise AssertionError("scripted unit tests must not run installed-harness probes")
    monkeypatch.setattr(qualification, "_probe", unexpected_probe)
    identity = qualification.identify(get("claude"))
    assert identity.platform == "darwin"
    assert Path(identity.executable).is_relative_to(env.tmp.resolve())
    task = await run(env, allowed_commands=GRANTS)
    assert task.allowed_commands == GRANTS


async def test_start_records_grants_on_argv_task_record_and_snapshots(env):
    task = await run(env, allowed_commands=GRANTS)
    (argv,) = env.launches()
    assert env.flag(argv, "--allowedTools") == [ALLOWED]
    assert env.flag(argv, "--disallowedTools") == ["Bash(git commit:*),Bash(git push:*)"]
    assert task.allowed_commands == GRANTS and task.allowed_commands_version == "2.1.310"
    assert task.enforcement["extra_allowed_commands"] == GRANTS
    assert task.enforcement["direct_commit_commands_denied"] is True and task.enforcement["commit_push_blocked"] is False
    assert task.snapshot()["allowed_commands"] == GRANTS
    record = store.read(env.log_dir, task.task_id)
    assert record.allowed_commands == GRANTS and record.enforcement["extra_allowed_commands"] == GRANTS
    recovered = store.snapshot(env.log_dir, record)
    assert recovered["allowed_commands"] == GRANTS and recovered["enforcement"]["extra_allowed_commands"] == GRANTS


async def test_publish_carries_the_freedoms_entries_first(env):
    await run(env, freedom="publish", allowed_commands=["make test"])
    (argv,) = env.launches()
    assert env.flag(argv, "--allowedTools") == [f"{PUBLISH},Bash(make test)"]
    assert env.flag(argv, "--disallowedTools") == []


async def test_start_without_grants_is_unchanged(env):
    task = await run(env)
    (argv,) = env.launches()
    assert env.flag(argv, "--allowedTools") == []
    assert task.allowed_commands == [] and "extra_allowed_commands" not in task.enforcement
    assert task.snapshot()["allowed_commands"] == []
    assert store.read(env.log_dir, task.task_id).allowed_commands == []


async def test_an_empty_list_grants_nothing(env):
    task = await run(env, allowed_commands=[])
    assert env.flag(env.launches()[0], "--allowedTools") == [] and task.allowed_commands == []


@pytest.mark.parametrize("freedom", ["read_only", "unrestricted"])
async def test_other_freedoms_are_refused_before_spawn(env, freedom):
    with pytest.raises(UnsupportedCapability, match="only at write_in_repo and publish"):
        await env.registry.start("go", env.repo, backend=get("claude"), freedom=freedom, allowed_commands=["make test"])
    assert env.launches() == [] and env.registry.list() == []


@pytest.mark.parametrize("pattern", ["git commit:*", "bash -c x", "pytest*", "make test,x", "cp a ../b"])
async def test_a_refused_pattern_never_spawns(env, pattern):
    with pytest.raises(CommandGrantError, match="refused|comma"):
        await env.registry.start("go", env.repo, backend=get("claude"), allowed_commands=[pattern])
    assert env.launches() == []


@pytest.mark.parametrize("name", ["codex", "opencode", "vibe", "antigravity"])
async def test_unsupported_backends_refuse_before_spawn(env, name):
    with pytest.raises(UnsupportedCapability, match=name):
        await env.registry.start("go", env.repo, backend=get(name), allowed_commands=["make test"])
    assert env.registry.list() == []


async def test_workflow_builders_cannot_receive_grants(env):
    with pytest.raises(CommandGrantError, match="native workflows and workflow builders"):
        await env.registry.start("go", env.repo, backend=get("claude"), workflow_builder=True, allowed_commands=["make test"])


async def test_unseen_version_with_failed_behavior_probe_is_refused(env, monkeypatch):
    monkeypatch.setenv("FAKE_CLAUDE_VERSION", "2.1.311")
    with pytest.raises(UnsupportedCapability, match="allowed_commands qualification failed"):
        await env.registry.start("go", env.repo, backend=get("claude"), allowed_commands=["make test"])
    assert env.launches() == []
    task = await run(env)  # no grants: no permission self-test is required
    assert task.allowed_commands == []


# --- resume and continuation ----------------------------------------------------------------------

async def test_resume_inherits_saved_grants_and_requalifies(env, monkeypatch):
    first = await run(env, allowed_commands=GRANTS)
    second = await env.registry.resume(first, "again")
    await asyncio.wait_for(second.done.wait(), timeout=30)
    assert env.flag(env.launches()[1], "--allowedTools") == [ALLOWED]
    assert second.allowed_commands == GRANTS and second.enforcement["extra_allowed_commands"] == GRANTS
    assert store.read(env.log_dir, second.task_id).allowed_commands == GRANTS
    monkeypatch.setenv("FAKE_CLAUDE_VERSION", "2.1.311")
    with pytest.raises(UnsupportedCapability, match="allowed_commands qualification failed"):
        await env.registry.resume(second, "third")
    assert len(env.launches()) == 2


async def test_resume_replaces_with_an_explicit_list_and_can_clear(env):
    first = await run(env, allowed_commands=GRANTS)
    replaced = await env.registry.resume(first, "again", allowed_commands=["cargo build"])
    await asyncio.wait_for(replaced.done.wait(), timeout=30)
    assert env.flag(env.launches()[1], "--allowedTools") == ["Bash(cargo build)"] and replaced.allowed_commands == ["cargo build"]
    cleared = await env.registry.resume(replaced, "again", allowed_commands=[])
    await asyncio.wait_for(cleared.done.wait(), timeout=30)
    assert env.flag(env.launches()[2], "--allowedTools") == [] and cleared.allowed_commands == []
    assert "extra_allowed_commands" not in cleared.enforcement
    with pytest.raises(CommandGrantError, match="git is refused"):
        await env.registry.resume(cleared, "again", allowed_commands=["git push:*"])


async def test_resume_record_inherits_and_replaces(env):
    first = await run(env, allowed_commands=GRANTS)
    record = store.read(env.log_dir, first.task_id)
    inherited = await env.registry.resume_record(record, "again")
    await asyncio.wait_for(inherited.done.wait(), timeout=30)
    assert env.flag(env.launches()[1], "--allowedTools") == [ALLOWED]
    replaced = await env.registry.resume_record(record, "again", allowed_commands=["make build"])
    await asyncio.wait_for(replaced.done.wait(), timeout=30)
    assert env.flag(env.launches()[2], "--allowedTools") == ["Bash(make build)"]


async def test_a_tampered_saved_list_is_revalidated_on_resume(env):
    first = await run(env, allowed_commands=GRANTS)
    record = store.read(env.log_dir, first.task_id)
    for forged in (["git commit:*"], ["bash x"], ["make test"] * 2, [f"p{i}" for i in range(17)]):
        with pytest.raises(CommandGrantError):
            await env.registry.resume_record(_with(record, forged), "again")
    assert len(env.launches()) == 1


def _with(record, grants):
    from dataclasses import replace
    return replace(record, allowed_commands=grants)


async def test_continue_task_inherits_and_replaces(env):
    first = await run(env, allowed_commands=GRANTS)
    record = store.read(env.log_dir, first.task_id)
    assert record.output_drained
    inherited = await env.registry.continue_task(first.task_id, "next")
    await asyncio.wait_for(inherited.done.wait(), timeout=30)
    assert env.flag(env.launches()[1], "--allowedTools") == [ALLOWED] and inherited.allowed_commands == GRANTS
    replaced = await env.registry.continue_task(inherited.task_id, "next", allowed_commands=["cargo build"])
    await asyncio.wait_for(replaced.done.wait(), timeout=30)
    assert env.flag(env.launches()[2], "--allowedTools") == ["Bash(cargo build)"] and replaced.allowed_commands == ["cargo build"]


class _ContinuationRegistry:
    """Just enough registry to see what `continue_task` forwards to `start`."""

    def __init__(self, log_dir: Path):
        from unittest.mock import AsyncMock
        from harflow_mcp.state_layout import resolve_layout
        self.log_dir = log_dir
        self.state_layout = resolve_layout(log_dir=log_dir)
        self._mutation_caller = AsyncMock(return_value=None)
        self.children: dict = {}

        async def spawn(prompt, path, **kwargs):
            child = SimpleNamespace(task_id=kwargs["task_id"], spawned_by=None, root_task_id=None, depth=0, max_depth=None, latest_record_landed=True,
                                    persistence_error=None, backend="claude", session_id=None, repo_path=str(path), started_at="2026-01-01T00:02:00+00:00",
                                    prompt=prompt, freedom="write_in_repo", status="running")
            self.children[child.task_id] = child
            return child

        self.start = AsyncMock(side_effect=spawn)

    def get(self, task_id):
        return self.children.get(task_id)

    def persist(self, child):
        pass


async def test_continuation_forwards_only_an_explicit_list(tmp_path, monkeypatch, synthetic_permission_identity):
    monkeypatch.setattr("harflow_mcp.workflow_hooks.refuse_managed", lambda *args: None)
    monkeypatch.setattr(backends, "version", lambda backend, **kwargs: "2.1.310 (Claude Code)")
    state = tmp_path / "state" / "tasks"
    source = store.TaskRecord(task_id="a" * 32, backend="claude", session_id="s", repo_path=str(tmp_path), started_at="2026-01-01T00:00:00+00:00", prompt="p",
                              status="completed", exit_code=0, finished_at="2026-01-01T00:01:00+00:00", output_drained=True, allowed_commands=["make test"])
    assert store.write_landed(state, source)
    registry = _ContinuationRegistry(state)
    await continuation(registry, source.task_id)
    assert "allowed_commands" not in registry.start.call_args.kwargs
    with pytest.raises(ValueError, match="different caller request"):
        await continuation(registry, source.task_id, allowed_commands=["make build"])
    registry2 = _ContinuationRegistry(tmp_path / "state2" / "tasks")
    store.write_landed(registry2.log_dir, source)
    await continuation(registry2, source.task_id, allowed_commands=["make build"])
    assert registry2.start.call_args.kwargs["allowed_commands"] == ["make build"]
    with pytest.raises(CommandGrantError):
        await continuation(_ContinuationRegistry(tmp_path / "state3" / "tasks"), source.task_id, allowed_commands=["make build", "make build"])


async def test_automatic_continuation_inherits(tmp_path, monkeypatch, synthetic_permission_identity):
    monkeypatch.setattr("harflow_mcp.workflow_hooks.refuse_managed", lambda *args: None)
    monkeypatch.setattr(backends, "version", lambda backend, **kwargs: "2.1.310 (Claude Code)")
    state = tmp_path / "state" / "tasks"
    source = store.TaskRecord(task_id="b" * 32, backend="claude", session_id="s", repo_path=str(tmp_path), started_at="2026-01-01T00:00:00+00:00", prompt="p",
                              status="completed", exit_code=0, finished_at="2026-01-01T00:01:00+00:00", output_drained=True, allowed_commands=["make test"], auto_continue=1)
    store.write_landed(state, source)
    registry = _ContinuationRegistry(state)
    from harflow_mcp.backends import Accumulator
    live = SimpleNamespace(done=SimpleNamespace(is_set=lambda: True), proc=None, cancel_requested=False, acc=Accumulator(terminal={"subtype": "error_max_turns"}),
                           snapshot=lambda: store.snapshot(state, source))
    registry.get = lambda task_id: live if task_id == source.task_id else registry.children.get(task_id)
    await continuation(registry, source.task_id, automatic=True)
    assert "allowed_commands" not in registry.start.call_args.kwargs


async def test_an_invalid_continuation_list_poisons_no_reservation(env):
    first = await run(env, allowed_commands=GRANTS)
    for bad in (["git commit:*"], ["make test", "make test"], ["cp a /etc/x"]):
        with pytest.raises(CommandGrantError):
            await env.registry.continue_task(first.task_id, "next", allowed_commands=bad)
    assert not (env.log_dir / f"{first.task_id}.continuation.json").exists()
    fixed = await env.registry.continue_task(first.task_id, "next", allowed_commands=["cargo build"])
    await asyncio.wait_for(fixed.done.wait(), timeout=30)
    assert fixed.allowed_commands == ["cargo build"]
    second = await run(env, allowed_commands=GRANTS)
    with pytest.raises(CommandGrantError):
        await env.registry.continue_task(second.task_id, "next", allowed_commands=["bash x"])
    inherited = await env.registry.continue_task(second.task_id, "next")
    await asyncio.wait_for(inherited.done.wait(), timeout=30)
    assert inherited.allowed_commands == GRANTS


async def test_an_over_broad_inherited_list_is_refused_before_reserving(env):
    first = await run(env, allowed_commands=["make test:*", "make build"])
    registry = nested_registry(env, caller_record(env, grants=["make test:*"]))
    with pytest.raises(NestedDispatchRefused) as raised:
        await registry.continue_task(first.task_id, "next")
    assert raised.value.rule == "extra_allowed_commands"
    assert not (env.log_dir / f"{first.task_id}.continuation.json").exists()
    narrowed = await registry.continue_task(first.task_id, "next", allowed_commands=["make test:*"])
    await asyncio.wait_for(narrowed.done.wait(), timeout=30)
    assert narrowed.allowed_commands == ["make test:*"]


async def test_a_failed_qualification_is_refused_before_reserving(env, monkeypatch):
    first = await run(env, allowed_commands=GRANTS)
    monkeypatch.setenv("FAKE_CLAUDE_VERSION", "2.1.311")
    for kwargs in ({}, {"allowed_commands": ["cargo build"]}):
        with pytest.raises(UnsupportedCapability, match="allowed_commands qualification failed"):
            await env.registry.continue_task(first.task_id, "next", **kwargs)
    assert not (env.log_dir / f"{first.task_id}.continuation.json").exists()
    cleared = await env.registry.continue_task(first.task_id, "next", allowed_commands=[])
    await asyncio.wait_for(cleared.done.wait(), timeout=30)
    assert cleared.allowed_commands == []
    second = await run(env)  # unrelated, no grants
    monkeypatch.delenv("FAKE_CLAUDE_VERSION")
    third = await run(env, allowed_commands=GRANTS)
    again = await env.registry.continue_task(third.task_id, "next")
    await asyncio.wait_for(again.done.wait(), timeout=30)
    assert again.allowed_commands == GRANTS and second.allowed_commands == []


async def test_server_continue_validates_before_reserving(env, monkeypatch):
    monkeypatch.setattr(server, "_registry", env.registry)
    first = await run(env, allowed_commands=GRANTS)
    with pytest.raises(MCPError, match="git is refused"):
        await server.continue_task(first.task_id, "next", allowed_commands=["git push:*"])
    assert not (env.log_dir / f"{first.task_id}.continuation.json").exists()


async def test_a_capped_task_whose_prompt_names_the_flag_still_gets_its_grants(env):
    task = await env.registry.start("--allowedTools", env.repo, backend=get("claude"), max_turns=1, allowed_commands=GRANTS)
    await asyncio.wait_for(task.done.wait(), timeout=30)
    (argv,) = env.launches()
    options = argv[:argv.index("--")]
    assert argv[argv.index("--") + 1:] == ["--allowedTools"]
    assert env.flag(options, "--allowedTools") == [ALLOWED]


async def test_a_grants_only_control_request_before_init_stops_the_run(env):
    from harflow_mcp import approval_runtime
    from harflow_mcp.backends import Accumulator
    stopped = []
    registry = SimpleNamespace(log_dir=env.log_dir, state_layout=env.registry.state_layout, persist=lambda task: None, _stop_native_transport=stopped.append)
    task = SimpleNamespace(caller_approvals=False, caller_write_approvals=False, caller_approval_version=None, allowed_commands=["make test"],
                           allowed_commands_version="2.1.310", acc=Accumulator(), bridge_notices=[], task_id="t", approval_generation="g")
    event = {"type": "control_request", "request_id": "r", "request": {"subtype": "can_use_tool", "tool_name": "Bash", "input": {"command": "make test"}}}
    assert await approval_runtime.observe(registry, task, get("claude"), event) is False
    assert stopped == [task] and "allowed_commands were qualified on 2.1.310" in task.bridge_notices[0]
    plain = SimpleNamespace(**{**vars(task), "allowed_commands": [], "bridge_notices": []})
    stopped.clear()
    assert await approval_runtime.observe(registry, plain, get("claude"), event) is False and stopped == []


# --- init-version cross-check ----------------------------------------------------------------------

@pytest.mark.parametrize("scenario,message", [
    ({"FAKE_CLAUDE_INIT_VERSION": "2.1.311"}, "reported version 2.1.311 but allowed_commands were qualified on 2.1.310"),
    ({"FAKE_CLAUDE_INIT_VERSION": "none"}, "reported version none but allowed_commands were qualified on 2.1.310"),
    ({"FAKE_CLAUDE_EARLY_TOOL": "1"}, "none before this request but allowed_commands were qualified on 2.1.310"),
])
async def test_a_version_mismatch_stops_a_grant_run_with_a_bridge_notice(env, monkeypatch, scenario, message):
    for key, value in scenario.items():
        monkeypatch.setenv(key, value)
    task = await run(env, allowed_commands=GRANTS)
    assert any(message in notice for notice in task.bridge_notices), task.bridge_notices
    assert task.status != "completed"


async def test_a_mismatch_does_not_stop_a_run_without_grants(env, monkeypatch):
    monkeypatch.setenv("FAKE_CLAUDE_INIT_VERSION", "2.1.311")
    task = await run(env)
    assert task.status == "completed" and not any("qualified" in notice for notice in task.bridge_notices)


async def test_a_matching_version_completes(env):
    task = await run(env, allowed_commands=GRANTS)
    assert task.status == "completed" and not any("qualified" in notice for notice in task.bridge_notices)


# --- nested dispatch ------------------------------------------------------------------------------

def caller_record(env, *, grants: list[str], freedom: str = "write_in_repo", name: str = "c" * 32):
    enforcement = get("claude").enforcement(freedom)
    if grants:
        enforcement = get("claude").allowed_commands_enforcement(enforcement, tuple(grants))
    return store.TaskRecord(task_id=name, backend="claude", session_id="s", repo_path=str(env.repo), started_at="2026-01-01T00:00:00+00:00", freedom=freedom,
                            enforcement=enforcement.as_dict(), allowed_commands=grants, depth=0, max_depth=3, root_task_id=name)


def nested_registry(env, record):
    return TaskRegistry(log_dir=env.log_dir, open_monitor=False, caller_override=Caller(record, "session"))


async def test_a_child_may_grant_a_subset_of_its_callers_patterns(env):
    registry = nested_registry(env, caller_record(env, grants=GRANTS))
    task = await registry.start("go", env.repo, backend=get("claude"), allowed_commands=["make test:*"])
    await asyncio.wait_for(task.done.wait(), timeout=30)
    assert env.flag(env.launches()[0], "--allowedTools") == ["Bash(make test:*)"] and task.spawned_by == "c" * 32


async def test_a_child_cannot_widen_and_omission_does_not_inherit(env):
    registry = nested_registry(env, caller_record(env, grants=GRANTS))
    with pytest.raises(NestedDispatchRefused) as raised:
        await registry.start("go", env.repo, backend=get("claude"), allowed_commands=["make test:*", "make build"])
    assert raised.value.rule == "extra_allowed_commands"
    omitted = await registry.start("go", env.repo, backend=get("claude"))
    await asyncio.wait_for(omitted.done.wait(), timeout=30)
    assert omitted.allowed_commands == [] and env.flag(env.launches()[-1], "--allowedTools") == []
    assert len(env.launches()) == 1


async def test_a_caller_without_grants_cannot_grant(env):
    registry = nested_registry(env, caller_record(env, grants=[]))
    with pytest.raises(NestedDispatchRefused) as raised:
        await registry.start("go", env.repo, backend=get("claude"), allowed_commands=["make test"])
    assert raised.value.rule == "extra_allowed_commands" and env.launches() == []


async def test_an_unrestricted_caller_may_grant_anything(env):
    registry = nested_registry(env, caller_record(env, grants=[], freedom="unrestricted"))
    task = await registry.start("go", env.repo, backend=get("claude"), allowed_commands=["make test"])
    await asyncio.wait_for(task.done.wait(), timeout=30)
    assert task.allowed_commands == ["make test"]


async def test_resume_inheritance_is_subject_to_the_nested_subset(env):
    first = await run(env, allowed_commands=GRANTS)
    registry = nested_registry(env, caller_record(env, grants=["make test:*"]))
    with pytest.raises(NestedDispatchRefused) as raised:
        await registry.resume(first, "again")
    assert raised.value.rule == "extra_allowed_commands"
    narrowed = await registry.resume(first, "again", allowed_commands=["make test:*"])
    await asyncio.wait_for(narrowed.done.wait(), timeout=30)
    assert narrowed.allowed_commands == ["make test:*"]


async def test_an_undecidable_caller_fails_closed_when_grants_are_requested(env, monkeypatch):
    registry = TaskRegistry(log_dir=env.log_dir, open_monitor=False)
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None, "process table unreadable"))
    with pytest.raises(NestedDispatchRefused) as raised:
        await registry.start("go", env.repo, backend=get("claude"), allowed_commands=["make test"])
    assert raised.value.rule == "caller_undecidable" and env.launches() == []


async def test_preview_applies_the_nested_subset(env):
    registry = nested_registry(env, caller_record(env, grants=["make test:*"]))
    with pytest.raises(NestedDispatchRefused):
        await registry.preview_permissions(env.repo, backend=get("claude"), allowed_commands=["make build"])
    preview = await registry.preview_permissions(env.repo, backend=get("claude"), allowed_commands=["make test:*"])
    assert preview["allowed_commands"] == ["make test:*"] and preview["enforcement"]["extra_allowed_commands"] == ["make test:*"]


# --- preview --------------------------------------------------------------------------------------

async def test_preview_discloses_grants_without_launching(env):
    preview = await env.registry.preview_permissions(env.repo, backend=get("claude"), allowed_commands=GRANTS)
    assert preview["allowed_commands"] == GRANTS and preview["enforcement"]["extra_allowed_commands"] == GRANTS
    assert any("prefix approvals" in caveat for caveat in preview["enforcement"]["caveats"])
    assert preview["enforcement"]["direct_commit_commands_denied"] is True
    assert env.launches() == []
    plain = await env.registry.preview_permissions(env.repo, backend=get("claude"))
    assert plain["allowed_commands"] == [] and "extra_allowed_commands" not in plain["enforcement"]
    with pytest.raises(UnsupportedCapability):
        await env.registry.preview_permissions(env.repo, backend=get("claude"), freedom="read_only", allowed_commands=["make test"])


# --- server tools ---------------------------------------------------------------------------------

async def test_server_validates_before_dispatch(env, fake_backend_clis, monkeypatch):
    monkeypatch.setattr(server, "_registry", env.registry)
    monkeypatch.setenv("PATH", f"{env.tmp / 'bin'}{os.pathsep}{os.environ['PATH']}")
    with pytest.raises(MCPError, match="codex"):
        await server.start_task("go", str(env.repo), backend="codex", allowed_commands=["make test"])
    with pytest.raises(MCPError, match="at most 16"):
        await server.start_task("go", str(env.repo), backend="claude", allowed_commands=[f"p{i}" for i in range(17)])
    with pytest.raises(MCPError, match="git is refused"):
        await server.start_task("go", str(env.repo), backend="claude", allowed_commands=["git commit:*"])
    with pytest.raises(MCPError, match="only at write_in_repo and publish"):
        await server.start_task("go", str(env.repo), backend="claude", freedom="read_only", allowed_commands=["make test"])
    with pytest.raises(MCPError, match="unavailable for workflow launches"):
        await server.start_task("go", str(env.repo), workflow="anything", allowed_commands=["make test"])
    with pytest.raises(MCPError, match="codex"):
        await server.preview_task_permissions(str(env.repo), backend="codex", allowed_commands=["make test"])
    assert env.launches() == []


async def test_server_start_resume_continue_round_trip(env, monkeypatch):
    monkeypatch.setattr(server, "_registry", env.registry)
    started = await server.start_task("go", str(env.repo), backend="claude", allowed_commands=GRANTS)
    assert started["enforcement"]["extra_allowed_commands"] == GRANTS
    task = env.registry.get(started["task_id"])
    await asyncio.wait_for(task.done.wait(), timeout=30)
    await asyncio.wait_for(task.monitor, timeout=30)
    resumed = await server.resume_task(task.task_id, "again")
    assert resumed["enforcement"]["extra_allowed_commands"] == GRANTS
    child = env.registry.get(resumed["task_id"])
    await asyncio.wait_for(child.done.wait(), timeout=30)
    with pytest.raises(MCPError, match="duplicated"):
        await server.resume_task(child.task_id, "again", allowed_commands=["make test", "make test"])
    replaced = await server.resume_task(child.task_id, "again", allowed_commands=["make build"])
    assert replaced["enforcement"]["extra_allowed_commands"] == ["make build"]
    preview = await server.preview_task_permissions(str(env.repo), backend="claude", allowed_commands=GRANTS)
    assert preview["enforcement"]["extra_allowed_commands"] == GRANTS


async def test_list_backends_reports_the_capability_block():
    metadata = {entry["backend"]: entry["capabilities"]["allowed_commands"] for entry in backends.describe_all()}
    assert metadata["claude"]["supported"] is True and metadata["codex"]["supported"] is False


# --- ctl ------------------------------------------------------------------------------------------

def parse(*argv: str) -> argparse.Namespace:
    parsers = ctl._build_parser()
    return parsers[0].parse_args(list(argv))


def test_ctl_accepts_a_repeatable_allowed_command():
    run_args = parse("run", "--repo", "/r", "--prompt", "p", "--allowed-command", "make test:*", "--allowed-command", "cargo build")
    assert run_args.allowed_command == ["make test:*", "cargo build"]
    assert parse("run", "--repo", "/r", "--prompt", "p").allowed_command is None
    assert parse("resume", "t", "go", "--allowed-command", "make test").allowed_command == ["make test"]
    assert parse("continue", "t", "--allowed-command", "make test").allowed_command == ["make test"]
    preview = parse("permission-preview", "--repo", "/r", "--backend", "claude", "--allowed-command", "make test")
    assert preview.allowed_command == ["make test"]


def test_ctl_help_mentions_the_flag():
    help_text = ctl._build_parser()[0].format_help() + "".join(p.format_help() for p in ctl._build_parser()[1:])
    assert "--allowed-command" in help_text


async def test_ctl_run_and_resume_actions_forward_the_list(env, monkeypatch):
    monkeypatch.setattr(server, "_registry", env.registry)
    args = argparse.Namespace(prompt="go", repo=str(env.repo), backend="claude", freedom=None, model=None, max_turns=None, reasoning_effort=None,
                              network=None, group=None, title=None, auto_continue=0, caller_approvals=None, monitor=False, extra_writable_path=None,
                              allowed_command=GRANTS, background_idle_seconds=None)
    task = await ctl._run_action(args)(env.registry)
    await asyncio.wait_for(task.done.wait(), timeout=30)
    assert task.allowed_commands == GRANTS
    resume_args = argparse.Namespace(task_id=task.task_id, text="again", max_turns=None, network=None, allowed_command=["make build"], background_idle_seconds=None)
    resumed = await ctl._resume_action(resume_args)(env.registry)
    await asyncio.wait_for(resumed.done.wait(), timeout=30)
    assert resumed.allowed_commands == ["make build"]
    inherit_args = argparse.Namespace(task_id=resumed.task_id, text="again", max_turns=None, network=None, allowed_command=None, background_idle_seconds=None)
    inherited = await ctl._resume_action(inherit_args)(env.registry)
    await asyncio.wait_for(inherited.done.wait(), timeout=30)
    assert inherited.allowed_commands == ["make build"]


# --- compatibility --------------------------------------------------------------------------------

def test_old_records_default_to_no_grants(tmp_path):
    state = tmp_path / "state" / "tasks"
    record = store.TaskRecord(task_id="d" * 32, backend="claude", session_id="s", repo_path=str(tmp_path), started_at="2026-01-01T00:00:00+00:00",
                              enforcement=get("claude").enforcement("write_in_repo").as_dict())
    assert store.write_landed(state, record)
    path = store.record_path(state, record.task_id)
    document = json.loads(path.read_text())
    document.pop("allowed_commands", None)
    document["enforcement"].pop("extra_allowed_commands", None)
    path.write_text(json.dumps(document))
    loaded = store.read(state, record.task_id)
    assert loaded.allowed_commands == []
    assert store.snapshot(state, loaded)["allowed_commands"] == []


def test_a_malformed_saved_list_makes_the_document_unreadable(tmp_path):
    state = tmp_path / "state" / "tasks"
    record = store.TaskRecord(task_id="e" * 32, backend="claude", session_id="s", repo_path=str(tmp_path), started_at="2026-01-01T00:00:00+00:00")
    assert store.write_landed(state, record)
    path = store.record_path(state, record.task_id)
    document = json.loads(path.read_text())
    for bad in ("make test", [1], ["a", "a"], [f"p{i}" for i in range(17)]):
        path.write_text(json.dumps({**document, "allowed_commands": bad}))
        with pytest.raises(store.TaskDocumentError):
            store.read(state, record.task_id)
