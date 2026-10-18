"""Adversarial lifecycle tests for caller approval ownership and settlement."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from polybridge import approval_runtime, approvals, identity, store


def _task(directory: Path, *, generation: str = "generation-1") -> SimpleNamespace:
    return SimpleNamespace(
        task_id="task-1",
        proc=SimpleNamespace(pid=1234, returncode=None),
        start_time="task-start",
        markers=("task-marker",),
        owner={"pid": 5678, "start_time": "bridge-start", "markers": []},
        approval_generation=generation,
        session_id="session-1",
        repo_path=directory,
        freedom="write_in_repo",
    )


def _admit(directory: Path, index: int = 1, **context_overrides: object) -> dict:
    task = _task(directory)
    context = {
        "interaction_owner": "caller",
        "workflow_run_id": None,
        "root_workflow_run_id": None,
        "workflow_node_id": None,
        "workflow_execution_id": None,
    }
    context.update(context_overrides)
    return approvals.admit(
        directory,
        task,
        {
            "native_request_id": f"native-{index}",
            "tool_use_id": f"tool-{index}",
            "action": {"tool_name": "Bash", "input": {"command": f"printf {index}"}},
        },
        context,
    )


class _Registry:
    def __init__(self, directory: Path, *, caller=None):
        self.log_dir = directory
        self._caller = caller
        self.task = SimpleNamespace(status="running")

    async def _mutation_caller(self):
        return self._caller

    def get(self, _task_id):
        return self.task


@pytest.mark.asyncio
async def test_managed_worker_cannot_list_or_get_even_when_pb_task_id_is_cleared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    row = _admit(tmp_path)
    registry = _Registry(tmp_path, caller=object())
    monkeypatch.delenv("PB_TASK_ID", raising=False)

    with pytest.raises(ValueError, match="Workers and orchestrators"):
        await approval_runtime.list_requests(registry, task_id="task-1")
    with pytest.raises(ValueError, match="Workers and orchestrators"):
        await approval_runtime.get_request(registry, "task-1", row["request_id"])


@pytest.mark.asyncio
async def test_monitor_spoof_and_mismatched_root_owner_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    row = _admit(tmp_path, root_workflow_run_id="root-1")
    registry = _Registry(tmp_path)
    monkeypatch.setattr(approval_runtime, "context", lambda *_: {
        "interaction_owner": "caller",
        "root_workflow_run_id": "root-2",
        "workflow_run_id": "child",
        "workflow_node_id": None,
        "workflow_execution_id": None,
    })
    monkeypatch.setattr(approval_runtime, "_reconcile", lambda *_: SimpleNamespace(
        interaction_owner="caller", approval_generation="generation-1", status="running",
    ))
    monkeypatch.setattr(identity, "identity_check", lambda _value: "alive")

    with pytest.raises(ValueError, match="root interaction owner"):
        await approval_runtime.respond_request(
            registry, "task-1", row["request_id"], row["action_digest"], 1, "approve", "monitor"
        )


def test_delivery_unknown_stays_uncertain_until_observed_exit_and_output_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    row = _admit(tmp_path)
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "pending", delivery="sending")
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "sending", delivery="delivery_unknown")
    record = store.TaskRecord(
        task_id="task-1", backend="claude", session_id="session-1", repo_path=str(tmp_path),
        started_at="now", status="failed", exit_code=None, output_drained=False,
    )
    store.write(tmp_path, record)
    monkeypatch.setattr(identity, "identity_check", lambda _value: "dead")
    uncertain = approvals.summary(tmp_path, "task-1")
    assert uncertain["delivery_uncertain_count"] == 1
    assert uncertain["interaction_state"] == "approval_state_uncertain"

    store.write(tmp_path, record.__class__(**{**record.__dict__, "exit_code": 1, "output_drained": True}))
    settled = approvals.summary(tmp_path, "task-1")
    assert settled["delivery_uncertain_count"] == 0
    assert settled["interaction_state"] is None


@pytest.mark.parametrize("mutation", ["invalidate", "cancel"])
def test_process_invalidation_closes_old_generation_and_rejects_stale_reservation(
    tmp_path: Path, mutation: str
) -> None:
    row = _admit(tmp_path)
    if mutation == "invalidate":
        approvals.invalidate(tmp_path, "task-1", "invalidated", row["generation"])
    else:
        approvals.observe_cancellation(tmp_path, "task-1", row["request_id"], row["generation"])
    final = approvals.get(tmp_path, "task-1", row["request_id"])
    assert final["disposition"] in {"invalidated", "cancelled"}
    assert final["delivery"] == "failed"
    with pytest.raises(ValueError, match="Stale approval action"):
        approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], 1, "approve", row["generation"])
    with pytest.raises(ValueError, match="Stale"):
        approvals.observe_outcome(tmp_path, "task-1", row["request_id"], "old-generation", "succeeded")


@pytest.mark.parametrize("outcome_first", [True, False])
def test_expiry_preserves_caller_decision_but_forces_wire_reject_in_either_event_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome_first: bool
) -> None:
    row = _admit(tmp_path)
    reserved = approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], 1, "approve", row["generation"])
    monkeypatch.setattr(approvals.time, "time", lambda: reserved["deadline"] + 1)
    sending = approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    if outcome_first:
        approvals.observe_outcome(tmp_path, "task-1", row["request_id"], row["generation"], "succeeded")
    approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], True, "reject")
    final = approvals.get(tmp_path, "task-1", row["request_id"])
    assert final["decision"] == "approve"
    assert final["effective_decision"] == "reject"
    assert final["disposition"] == "expired"
    assert final["execution_outcome"] == ("succeeded" if outcome_first else "unknown")


def test_workflow_traversal_requires_reciprocal_root_owned_child_link(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from polybridge import workflows

    runs = {
        "root": {"workflow_run_id": "root", "activations": [{"id": "edge", "tasks": [], "invocation": {"child_workflow_run_id": "child"}}]},
        "child": {"workflow_run_id": "child", "parent_link": {"workflow_run_id": "other", "execution_id": "edge", "root_workflow_run_id": "root"}, "activations": []},
    }
    monkeypatch.setattr(workflows.WorkflowStore, "get_run", lambda _self, identifier, **_kwargs: runs[identifier])
    with pytest.raises(ValueError, match="ownership link"):
        approval_runtime.workflow_task_ids(tmp_path / "streams", "root")


def test_missing_caller_approvals_flag_keeps_legacy_compatibility() -> None:
    record = store.TaskRecord("t", "claude", None, "/tmp", "now")
    assert record.caller_approvals is False


@pytest.mark.asyncio
async def test_list_pages_active_requests_before_retained_history(tmp_path: Path) -> None:
    store.write(tmp_path, store.TaskRecord(
        task_id="task-1", backend="claude", session_id="session-1", repo_path=str(tmp_path),
        started_at="now", status="running", approval_generation="generation-1",
        start_time="task-start", owner={"pid": 5678, "start_time": "bridge-start", "markers": []},
    ))
    for index in range(1, 17):
        row = _admit(tmp_path, index)
        reserved = approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], 1, "reject", row["generation"])
        approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
        approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], True, reserved["decision"])
    pending = _admit(tmp_path, 17)
    raw = approvals.rows(tmp_path, "task-1")
    assert sum(r["disposition"] == "pending" for r in raw) == 1
    result = await approval_runtime.list_requests(_Registry(tmp_path), task_id="task-1", limit=1)
    assert result["requests"][0]["request_id"] == pending["request_id"]
    assert result["requests"][0]["disposition"] == "pending"
