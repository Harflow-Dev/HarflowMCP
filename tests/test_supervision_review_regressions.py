"""Round-one regressions for supervision, native settlement, and proxy handles."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import approval_runtime, approvals, identity, server, store, task_supervision, workflows
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session
from harflow_mcp.task_attachment import AttachedTask, TaskObservationUnavailable
from harflow_mcp.workflow_native_observations import require_transport_settled
from harflow_mcp import workflow_native_observations as native_observations
from harflow_mcp import workflow_recovery, task_supervision as supervision


@pytest.fixture
def task_dir(tmp_path: Path) -> Path:
    root = tmp_path / "state"
    with state_session(resolve_layout(root=root)):
        directory = root / "tasks"
        directory.mkdir(parents=True)
        yield directory


def _record(task_id: str, **changes: object) -> store.TaskRecord:
    values: dict[str, object] = {
        "task_id": task_id,
        "backend": "claude",
        "session_id": "session-1",
        "repo_path": "/repo",
        "started_at": "2026-10-24T00:00:00+00:00",
        "status": "running",
        "owner": {"pid": 123, "start_time": "owner-start", "markers": []},
        "pid": 456,
        "start_time": "agent-start",
        "markers": ["agent"],
        "exit_code": None,
    }
    return store.TaskRecord(**(values | changes))


def _definition() -> dict:
    return {
        "name": "supervision regression",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "work", "type": "agent", "agent": {"backend": "codex"}, "instructions": "work"},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "work"},
            {"id": "finish", "source": "work", "target": "end"},
        ],
    }


def test_reconcile_keeps_unsettled_owner_task_running_and_preserves_observed_terminal(
    task_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = workflows.WorkflowStore(task_dir.parent)
    run = storage.create_run(workflows.validate_definition(_definition()), "run", tmp_path)
    activation = {"id": "activation", "node_id": "work", "role": "node", "status": "running", "created_at": 0.0, "tasks": [{"task_id": "worker", "status": "running"}]}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(activations=[activation], pending=[]), "fixture")
    record = _record("worker", status="failed", output_drained=False)
    store.write(task_dir, record)
    monkeypatch.setattr(identity, "identity_check", lambda _value: "alive")
    monkeypatch.setattr(task_supervision, "read_launch", lambda *_args: {"state": "spawned", "owner": record.owner})
    assert task_supervision.owner_holds_unsettled(task_dir, record)
    reconciled = storage.reconcile_run(run["workflow_run_id"])
    assert reconciled["activations"][0]["tasks"][0]["status"] == "running"
    assert "recovered_failed_result" not in reconciled["pending"]

    terminal = _record("worker", status="completed", output_drained=True, exit_code=0, finished_at="2026-10-24T00:00:01+00:00")
    store.write(task_dir, terminal)
    reconciled = storage.reconcile_run(run["workflow_run_id"])
    assert reconciled["activations"][0]["tasks"][0]["status"] == "completed"
    assert "recovered_failed_result" not in reconciled["pending"]


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "completed", "exit_code": 0, "finished_at": "2026-10-24T00:00:01+00:00", "output_drained": False},
        {"status": "completed", "exit_code": None, "finished_at": "2026-10-24T00:00:01+00:00", "output_drained": True},
        {"status": "completed", "exit_code": 0, "finished_at": None, "output_drained": True},
    ],
)
def test_supervised_native_settlement_requires_exit_and_drain_barrier(task_dir: Path, changes: dict) -> None:
    record = _record("transport", **changes)
    store.write(task_dir, record)
    supervisor = SimpleNamespace(registry=SimpleNamespace(execution_mode="supervised", _log_dir=task_dir))
    with pytest.raises(ValueError, match="exit and output drain"):
        require_transport_settled(supervisor, "transport")


@pytest.mark.asyncio
async def test_attached_wait_stops_when_owner_and_agent_are_dead(task_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    record = _record("lost", status="running")
    store.write(task_dir, record)
    attached = AttachedTask(task_dir, "lost")
    monkeypatch.setattr(identity, "identity_check", lambda _value: "dead")
    monkeypatch.setattr(identity, "task_identity", lambda *_args: {"pid": 456})
    started = time.monotonic()
    with pytest.raises(TaskObservationUnavailable):
        await asyncio.wait_for(attached.done.wait(), 1.0)
    assert time.monotonic() - started < 0.8


@pytest.mark.asyncio
async def test_approval_response_reserves_attached_request_without_wake_or_mutation(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = _record("approval", approval_generation="generation-1")
    store.write(task_dir, record)
    source = SimpleNamespace(task_id="approval", proc=SimpleNamespace(pid=456), start_time="agent-start", markers=("agent",), owner=record.owner, approval_generation="generation-1", session_id="session-1", repo_path=Path("/repo"), freedom="write_in_repo")
    row = approvals.admit(task_dir, source, {"native_request_id": "native", "tool_use_id": "tool", "action": {"tool_name": "Bash", "input": {"command": "true"}}}, {"interaction_owner": "caller", "workflow_run_id": None, "root_workflow_run_id": None, "workflow_node_id": None, "workflow_execution_id": None})
    attached = AttachedTask(task_dir, "approval")

    class Registry:
        log_dir = task_dir
        state_layout = resolve_layout(log_dir=task_dir)

        async def _mutation_caller(self):
            return None

        def get(self, _task_id):
            return attached

    monkeypatch.setattr(approval_runtime, "_reconcile", lambda *_args: record)
    monkeypatch.setattr(approval_runtime, "context", lambda *_args: {"interaction_owner": "caller", "workflow_run_id": None, "root_workflow_run_id": None, "workflow_node_id": None, "workflow_execution_id": None})
    monkeypatch.setattr(identity, "identity_check", lambda _value: "alive")
    reserve_calls = []
    real_reserve = approvals.reserve
    monkeypatch.setattr(approvals, "reserve", lambda *args, **kwargs: (reserve_calls.append(args), real_reserve(*args, **kwargs))[1])
    answered = await approval_runtime.respond_request(Registry(), "approval", row["request_id"], row["action_digest"], row["owner_revision"], "approve")
    assert answered["delivery"] == "reserved"
    assert len(reserve_calls) == 1
    with pytest.raises(TypeError, match="read-only"):
        attached.approval_view = {"interaction_state": "waiting_for_approval"}


@pytest.mark.asyncio
async def test_progress_and_duration_are_read_only_for_attached_task(task_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    record = _record("progress", caller_approvals=True)
    store.write(task_dir, record)
    attached = AttachedTask(task_dir, "progress")
    class Registry:
        log_dir = task_dir
    monkeypatch.setattr(server, "_reg", lambda: Registry())
    monkeypatch.setattr(approvals, "summary", lambda *_args: {"interaction_state": None})
    plan = server._wait_plan(0, "timeout", 55)
    assert await server._await_with_progress(attached, plan, None) == "timeout"
    assert attached.duration_seconds >= 0


@pytest.mark.asyncio
async def test_checkout_lease_adopts_only_qualified_recovered_native_transport(tmp_path: Path) -> None:
    storage = workflows.WorkflowStore(tmp_path / "state")
    definition = workflows.validate_definition(_definition())
    run = storage.create_run(definition, "native recovery", tmp_path)
    task_id = "native-transport"
    activation = {
        "id": "execution", "node_id": "work", "role": "node", "status": "uncertain", "created_at": 0.0,
        "tasks": [{"task_id": task_id, "status": "uncertain", "execution_kind": "native_subagent", "freedom": "write_in_repo"}],
    }
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", activations=[activation]), "native_recovery_fixture")
    storage.list_run_page()
    adopted = workflows.CheckoutLease(storage, str(tmp_path), True, wait_seconds=0.1, adopted_native_tasks={(run["workflow_run_id"], task_id)})
    assert adopted._orphan_owner() is None
    async with adopted:
        assert adopted.handle is not None

    foreign = workflows.CheckoutLease(storage, str(tmp_path), True, wait_seconds=0.1, adopted_native_tasks={("foreign-run", task_id)})
    assert foreign._orphan_owner()["task_id"] == task_id
    uncertain = workflows.CheckoutLease(storage, str(tmp_path), True, wait_seconds=0.1)
    assert uncertain._orphan_owner()["workflow_run_id"] == run["workflow_run_id"]


@pytest.mark.parametrize("mutation", ["child_link", "generation", "owner", "session", "nonce", "missing_context"])
def test_native_adoption_proof_requires_reciprocal_transport_and_child_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    transport_id, child_id, run_id = "transport", "child", "run"
    owner = {"pid": 11, "start_time": "owner-start", "markers": []}
    record = SimpleNamespace(owner=owner, backend="claude", parent_task_id="parent", session_id="session")
    child = {
        "task_id": child_id, "execution_kind": "native_subagent", "transport_task_id": transport_id,
        "owner_task_id": "parent", "owner_session_id": "session", "dispatch_nonce": "nonce",
        "native_replay_state": {"owner_session_id": "session"},
    }
    control = {
        "id": transport_id, "role": "native_control", "tasks": [{"task_id": transport_id, "native_owner_execution_ids": [child_id], "native_observation_generation": "generation"}],
    }
    run = {"workflow_run_id": run_id, "parent_link": {"root_workflow_run_id": run_id}, "activations": [control, {"id": "child-activation", "role": "node", "tasks": [child]}]}
    if mutation == "child_link":
        control["tasks"][0]["native_owner_execution_ids"] = ["foreign-child"]
    elif mutation == "generation":
        control["tasks"][0]["native_observation_generation"] = "changed"
    elif mutation == "owner":
        record.owner = {"pid": 99, "start_time": "different", "markers": []}
    elif mutation == "session":
        child["owner_session_id"] = "different"
    elif mutation == "nonce":
        child["dispatch_nonce"] = ""
    elif mutation == "missing_context":
        child.pop("native_replay_state")

    class Storage:
        def get_run(self, _run_id, **_kwargs):
            return run

    supervisor = SimpleNamespace(store=Storage(), registry=SimpleNamespace(_log_dir=tmp_path / "tasks"))
    monkeypatch.setattr(workflow_recovery, "inventory", lambda _storage, _root: [run])
    monkeypatch.setattr(store, "read", lambda *_args, **_kwargs: record)
    monkeypatch.setattr(supervision, "read_launch", lambda *_args: {"state": "spawned", "owner": owner, "backend": "claude", "parent_task_id": "parent", "generation": "generation"})
    monkeypatch.setattr(supervision, "attachable", lambda *_args: True)
    assert native_observations.native_adoption_tasks(supervisor, run, transport_id) == set()


def test_native_adoption_proof_returns_exact_reciprocal_child_set(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    transport_id, child_id, run_id = "transport", "child", "run"
    owner = {"pid": 11, "start_time": "owner-start", "markers": []}
    record = SimpleNamespace(owner=owner, backend="claude", parent_task_id="parent", session_id="session")
    child = {"task_id": child_id, "execution_kind": "native_subagent", "transport_task_id": transport_id, "owner_task_id": "parent", "owner_session_id": "session", "dispatch_nonce": "nonce", "native_replay_state": {"owner_session_id": "session"}}
    control = {"id": transport_id, "role": "native_control", "tasks": [{"task_id": transport_id, "native_owner_execution_ids": [child_id], "native_observation_generation": "generation"}]}
    run = {"workflow_run_id": run_id, "parent_link": {"root_workflow_run_id": run_id}, "activations": [control, {"id": "child-activation", "role": "node", "tasks": [child]}]}
    class Storage:
        def get_run(self, _run_id, **_kwargs): return run
    supervisor = SimpleNamespace(store=Storage(), registry=SimpleNamespace(_log_dir=tmp_path / "tasks"))
    monkeypatch.setattr(workflow_recovery, "inventory", lambda _storage, _root: [run])
    monkeypatch.setattr(store, "read", lambda *_args, **_kwargs: record)
    monkeypatch.setattr(supervision, "read_launch", lambda *_args: {"state": "spawned", "owner": owner, "backend": "claude", "parent_task_id": "parent", "generation": "generation"})
    monkeypatch.setattr(supervision, "attachable", lambda *_args: True)
    assert native_observations._qualified_native_adoption_tasks(supervisor, run, transport_id) == {(run_id, child_id)}
