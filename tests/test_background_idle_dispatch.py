"""Issue 23: background idle settings must survive real dispatch boundaries."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import backends, ctl, server, store, tasks as tasks_module
from harflow_mcp.tasks import TaskRegistry
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session

from conftest import ALIVE_OWNER
from test_live_input import (
    _FakeClaude,
    _Stdin,
    _settle,
    _stub_task,
    _task_dir,
)


@pytest.fixture
def fake_idle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    backend = _FakeClaude()
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    monkeypatch.setenv("FAKE_CLAUDE_LOG", str(tmp_path / "agent-stdin.jsonl"))
    root = tmp_path / "state"
    with state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir()
        registry = TaskRegistry(log_dir=root / "tasks", owner=ALIVE_OWNER)
        yield SimpleNamespace(backend=backend, registry=registry, repo=tmp_path)


async def test_start_uses_environment_default_and_persists_override(fake_idle, monkeypatch):
    monkeypatch.setenv("HARFLOW_LIVE_IDLE_SECONDS", "0.75")
    default = await fake_idle.registry.start("reply default", fake_idle.repo, backend=fake_idle.backend)
    await _settle(default)
    explicit = await fake_idle.registry.start(
        "reply explicit", fake_idle.repo, backend=fake_idle.backend, background_idle_seconds=1.25
    )
    await _settle(explicit)

    assert default.background_idle_seconds == 0.75
    assert explicit.background_idle_seconds == 1.25
    assert store.read(fake_idle.registry.log_dir, default.task_id).background_idle_seconds == 0.75
    assert store.read(fake_idle.registry.log_dir, explicit.task_id).background_idle_seconds == 1.25


async def test_resume_and_continue_inherit_or_override_the_persisted_bound(fake_idle):
    parent = await fake_idle.registry.start(
        "reply parent", fake_idle.repo, backend=fake_idle.backend, background_idle_seconds=0.4
    )
    await _settle(parent)

    resumed = await fake_idle.registry.resume(parent, "reply resumed")
    await _settle(resumed)
    overridden = await fake_idle.registry.resume(parent, "reply overridden", background_idle_seconds=0.8)
    await _settle(overridden)
    continued = await fake_idle.registry.continue_task(parent.task_id, "reply continued")
    await _settle(continued)

    assert resumed.background_idle_seconds == 0.4
    assert overridden.background_idle_seconds == 0.8
    assert continued.background_idle_seconds == 0.4
    assert continued.continuation_of_task_id == parent.task_id


def test_ctl_actions_forward_background_idle_seconds(monkeypatch, tmp_path):
    seen = {}

    class Registry:
        log_dir = tmp_path / "tasks"

        async def start(self, *args, **kwargs):
            seen["start"] = kwargs
            return {"task_id": "new"}

        async def resume_record(self, *args, **kwargs):
            seen["resume"] = kwargs
            return {"task_id": "resumed"}

        async def continue_task(self, *args, **kwargs):
            seen["continue"] = kwargs
            return {"task_id": "continued"}

    registry = Registry()
    run_args = SimpleNamespace(
        prompt="run", freedom="write_in_repo", network=None, backend="claude", max_turns=None,
        reasoning_effort=None, model=None, group=None, title=None, repo=str(tmp_path), auto_continue=0,
        caller_approvals=False, monitor=False, extra_writable_path=None, background_idle_seconds=2.5,
        json=False,
    )
    monkeypatch.setattr(server, "_backend", lambda _: backends.get("claude"))
    monkeypatch.setattr(server, "_check_freedom", lambda _: None)
    monkeypatch.setattr(server, "_check_turn_cap", lambda *args: None)
    monkeypatch.setattr(server, "_check_reasoning_effort", lambda *args: None)
    monkeypatch.setattr(server, "_check_model", lambda *args: None)
    monkeypatch.setattr(server, "_check_network", lambda *args: None)
    monkeypatch.setattr(server, "_check_group", lambda *args: None)
    monkeypatch.setattr(server, "_normalize_title", lambda value: value)
    monkeypatch.setattr(server, "_validate_repo_path", lambda value: asyncio.sleep(0, result=tmp_path))
    asyncio.run(ctl._run_action(run_args)(registry))
    assert seen["start"]["background_idle_seconds"] == 2.5

    record = SimpleNamespace(task_id="old", backend="claude", freedom="write_in_repo", network=None)
    monkeypatch.setattr(ctl.store, "read", lambda *args, **kwargs: record)
    monkeypatch.setattr(ctl.store, "record_process_alive", lambda *args, **kwargs: False)
    resume_args = SimpleNamespace(
        task_id="old", text="resume", max_turns=None, background_idle_seconds=2.75,
        network=None,
    )
    asyncio.run(ctl._resume_action(resume_args)(registry))
    assert seen["resume"]["background_idle_seconds"] == 2.75

    # The continuation action is a direct forwarding seam; resume_record's record lookup is
    # exercised by the registry integration tests above and is intentionally not duplicated here.
    continue_args = SimpleNamespace(task_id="old", notes_path=None, extra_prompt="follow", background_idle_seconds=3.5)
    asyncio.run(ctl._continue_action(continue_args)(registry))
    assert seen["continue"]["background_idle_seconds"] == 3.5


def test_workflow_agent_settings_validate_and_retain_background_idle_seconds():
    from harflow_mcp.workflows import _candidate, launch_definition

    candidate = _candidate({"backend": "claude", "background_idle_seconds": 4.5})
    assert candidate["background_idle_seconds"] == 4.5
    definition = {
        "name": "idle",
        "orchestrator": candidate,
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "worker", "type": "agent", "role": "task", "agent": candidate, "instructions": "work"},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "worker"},
            {"id": "finish", "source": "worker", "target": "end"},
        ],
    }
    launched = launch_definition(definition, None)
    assert launched["orchestrator"]["background_idle_seconds"] == 4.5
    assert launched["nodes"][1]["agent"]["background_idle_seconds"] == 4.5


async def test_idle_cutoff_diagnostic_identifies_successful_latest_result(fake_idle, monkeypatch):
    registry = fake_idle.registry
    task = _stub_task(registry.log_dir, _Stdin())
    task.background_idle_seconds = 0.01
    task.acc.result_count = 1
    task.acc.awaiting_input = False
    task.acc.background_open = {"job"}
    task.acc.latest_result = {"subtype": "success", "is_error": False, "result": "ok"}
    task.last_output_at -= 1
    monkeypatch.setattr(tasks_module, "PUMP_POLL_SECONDS", 0.001)
    await asyncio.wait_for(registry._pump(task), 5)

    diagnostic = task.acc.failure_diagnostic
    assert diagnostic["category"] == "background_abandoned_at_idle_bound"
    assert diagnostic["background_idle_seconds"] == 0.01
    assert diagnostic["successful_agent_result"] is True
    assert diagnostic["latest_result"] == task.acc.latest_result


async def test_idle_cutoff_diagnostic_identifies_error_latest_result(fake_idle, monkeypatch):
    registry = fake_idle.registry
    task = _stub_task(registry.log_dir, _Stdin())
    task.background_idle_seconds = 0.01
    task.acc.result_count = 1
    task.acc.awaiting_input = False
    task.acc.background_open = {"job"}
    task.acc.latest_result = {"subtype": "error_during_execution", "is_error": True, "result": "nope"}
    task.last_output_at -= 1
    monkeypatch.setattr(tasks_module, "PUMP_POLL_SECONDS", 0.001)
    await asyncio.wait_for(registry._pump(task), 5)

    diagnostic = task.acc.failure_diagnostic
    assert diagnostic["category"] == "background_abandoned_at_idle_bound"
    assert diagnostic["successful_agent_result"] is False
    assert "reported an error" in diagnostic["reason"]


async def test_background_work_finishing_before_bound_completes_without_diagnostic(fake_idle):
    task = await fake_idle.registry.start(
        "bg short 0.05", fake_idle.repo, backend=fake_idle.backend, background_idle_seconds=0.5
    )
    await _settle(task)

    assert task.status == "completed"
    assert task.acc.background_abandoned is False
    assert task.acc.failure_diagnostic is None
    assert task.acc.background_open == set()
