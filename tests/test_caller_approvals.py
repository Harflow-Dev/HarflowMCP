"""Focused contract tests for Claude caller approval admission and ledger semantics."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from polybridge import approvals
from polybridge import approval_runtime
from polybridge.backends import get
from polybridge.backends.base import UnsupportedCapability
from polybridge.backends.claude_approvals import decode, encode, validate
from polybridge.backends import Accumulator


def _task(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        task_id="task-1",
        proc=SimpleNamespace(pid=os.getpid()),
        start_time="self-start",
        markers=("session-1",),
        owner={"pid": os.getpid(), "start_time": "bridge-start", "markers": []},
        approval_generation="generation-1",
        session_id="session-1",
        repo_path=tmp_path,
        freedom="write_in_repo",
    )


def _request(task: SimpleNamespace, index: int = 1) -> dict:
    return {
        "native_request_id": f"native-{index}",
        "tool_use_id": f"tool-{index}",
        "action": {"tool_name": "Bash", "input": {"command": f"printf {index}"}},
    }


def _admit(tmp_path: Path, index: int = 1) -> dict:
    task = _task(tmp_path)
    return approvals.admit(
        tmp_path,
        task,
        _request(task, index),
        {
            "interaction_owner": "caller",
            "workflow_run_id": None,
            "root_workflow_run_id": None,
            "workflow_node_id": None,
            "workflow_execution_id": None,
        },
    )


def _reserve_worker(directory: str, request_id: str, action_digest: str, generation: str, output) -> None:
    try:
        approvals.reserve(Path(directory), "task-1", request_id, action_digest, 1, "approve", generation)
    except Exception as exc:  # pragma: no cover - result is asserted by parent
        output.put(type(exc).__name__)
    else:
        output.put("won")


def test_claude_caller_approval_contract_is_strict() -> None:
    with pytest.raises(UnsupportedCapability):
        validate(True, "read_only", None)
    with pytest.raises(UnsupportedCapability):
        validate(True, "write_in_repo", 1)
    with pytest.raises(UnsupportedCapability):
        validate(True, "write_in_repo", None, native=True)
    validate(False, "read_only", 1)


@pytest.mark.parametrize(
    "event",
    [
        {"type": "assistant", "message": "please approve this"},
        {"type": "control_request", "request_id": "r", "request": {"subtype": "selection"}},
        {
            "type": "control_request",
            "request_id": "r",
            "request": {"subtype": "can_use_tool", "tool_name": "Bash", "tool_use_id": "t", "input": {"command": "x", "dangerouslyDisableSandbox": True}},
        },
        {
            "type": "control_request",
            "request_id": "r",
            "request": {"subtype": "can_use_tool", "tool_name": "Bash", "tool_use_id": "t", "input": {"command": "x"}, "parent_tool_use_id": "child"},
        },
    ],
)
def test_only_exact_root_bash_control_request_is_decodable(event: dict) -> None:
    if event.get("type") != "control_request":
        assert decode(event) is None
    else:
        with pytest.raises(ValueError):
            decode(event)


def test_claude_decode_and_encode_preserve_exact_action() -> None:
    event = {
        "type": "control_request",
        "request_id": "native-1",
        "request": {
            "subtype": "can_use_tool",
            "tool_name": "Bash",
            "tool_use_id": "tool-1",
            "input": {"command": "printf 'a  b\\n'", "timeout": 17},
        },
    }
    decoded = decode(event)
    assert decoded == {"native_request_id": "native-1", "tool_use_id": "tool-1", "action": {"tool_name": "Bash", "input": {"command": "printf 'a  b\\n'", "timeout": 17}}}
    import json
    payload = json.loads(encode(decoded, "approve"))
    assert payload["response"]["request_id"] == "native-1"
    assert payload["response"]["response"]["updatedInput"] == event["request"]["input"]
    reject = json.loads(encode(decoded, "reject"))
    assert reject["response"]["response"]["message"] == "Caller rejected this exact action"


def test_ledger_hides_action_from_metadata_and_requires_exact_cas(tmp_path: Path) -> None:
    row = _admit(tmp_path)
    metadata = approvals.metadata(row)
    assert "action" not in metadata
    assert "bridge_owner" not in metadata
    full = approvals.get(tmp_path, "task-1", row["request_id"])
    assert full["action"] == row["action"] and full["complete"] is True
    with pytest.raises(ValueError, match="Stale"):
        approvals.reserve(tmp_path, "task-1", row["request_id"], "wrong", row["owner_revision"], "approve", row["generation"])
    reserved = approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    with pytest.raises(ValueError, match="expired, settled"):
        approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], reserved["owner_revision"], "reject", row["generation"])


def test_ledger_rejects_duplicate_native_identity_and_bounds_pending(tmp_path: Path) -> None:
    first = _admit(tmp_path)
    with pytest.raises(ValueError, match="Repeated"):
        approvals.admit(tmp_path, _task(tmp_path), _request(_task(tmp_path)), {"interaction_owner": "caller"})
    for index in range(2, approvals.MAX_PENDING + 1):
        _admit(tmp_path, index)
    with pytest.raises(ValueError, match="count bound"):
        _admit(tmp_path, approvals.MAX_PENDING + 1)
    assert len(approvals.rows(tmp_path, "task-1")) == approvals.MAX_PENDING


def test_cross_process_reservation_has_one_winner(tmp_path: Path) -> None:
    row = _admit(tmp_path)
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    processes = [context.Process(target=_reserve_worker, args=(str(tmp_path), row["request_id"], row["action_digest"], row["generation"], output)) for _ in range(2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    assert sorted(output.get(timeout=2) for _ in processes) == ["ValueError", "won"]


def test_approval_answer_crossing_deadline_is_transmitted_as_deny(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    row = _admit(tmp_path)
    reserved = approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    monkeypatch.setattr(approvals.time, "time", lambda: reserved["deadline"] + 1)
    sending = approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    assert sending["decision"] == "approve"
    assert sending["effective_decision"] == "reject"
    approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], True, sending["effective_decision"])
    final = approvals.get(tmp_path, "task-1", row["request_id"])
    assert final["decision"] == "approve"
    assert final["effective_decision"] == "reject"
    assert final["delivered_decision"] == "reject"
    assert json.loads(encode(final, final["effective_decision"]))["response"]["response"]["behavior"] == "deny"


def test_large_control_request_is_rejected_before_action_is_exposed(tmp_path: Path) -> None:
    task = _task(tmp_path)
    oversized = _request(task)
    oversized["action"]["input"]["command"] = "x" * approvals.MAX_REQUEST_BYTES
    with pytest.raises(ValueError, match="64 KiB"):
        approvals.admit(tmp_path, task, oversized, {"interaction_owner": "caller"})


@pytest.mark.asyncio
async def test_runtime_admits_only_a_real_control_request_and_keeps_prose_out_of_ledger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    task = _task(tmp_path)
    task.caller_approvals = True
    task.interaction_owner = "caller"
    task.acc = Accumulator()
    task.approval_pending = False
    task.approval_receiving = False
    task.approval_view = {"pending_count": 0, "interaction_state": None, "requests": []}
    task.cancel_requested = False
    task.bridge_notices = []
    task.pump_wake = asyncio.Event()

    class Registry:
        log_dir = tmp_path

        def persist(self, value):
            return None

        def _stop_native_transport(self, value):
            value.cancel_requested = True

    registry = Registry()
    monkeypatch.setattr(approval_runtime, "context", lambda *_args: {"interaction_owner": "caller", "workflow_run_id": None, "root_workflow_run_id": None, "workflow_node_id": None, "workflow_execution_id": None})
    backend = get("claude")
    prose = {"type": "assistant", "message": {"content": [{"type": "text", "text": "please approve"}]}}
    assert await approval_runtime.observe(registry, task, backend, prose) is False
    assert not (tmp_path / "task-1.approvals.json").exists()

    event = {"type": "control_request", "request_id": "native-1", "request": {"subtype": "can_use_tool", "tool_name": "Bash", "tool_use_id": "tool-1", "input": {"command": "printf exact"}}}
    assert await approval_runtime.observe(registry, task, backend, event) is True
    records = approvals.rows(tmp_path, "task-1")
    assert len(records) == 1
    assert records[0]["action"]["input"]["command"] == "printf exact"
    assert task.approval_pending is True


@pytest.mark.asyncio
async def test_tool_result_racing_local_send_is_reconciled_without_stale_pump_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A fast Claude tool_result can arrive between send reservation and final ledger update."""
    task = _task(tmp_path)
    task.backend = "claude"
    task.caller_approvals = True
    task.interaction_owner = "caller"
    task.cancel_requested = False
    task.input_closed = False
    task.approval_pending = True
    task.approval_receiving = False
    task.approval_view = {"pending_count": 1, "interaction_state": "waiting_for_approval", "requests": []}
    task.pump_wake = asyncio.Event()
    task.proc.returncode = None

    class Registry:
        log_dir = tmp_path

        def get(self, _task_id):
            return task

    registry = Registry()
    row = _admit(tmp_path)
    row = approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    monkeypatch.setattr(approval_runtime, "context", lambda *_args: {"interaction_owner": "caller", "workflow_run_id": None, "root_workflow_run_id": None, "workflow_node_id": None, "workflow_execution_id": None})
    monkeypatch.setattr(approval_runtime.approvals, "summary", lambda *_args: {"pending_count": 0, "interaction_state": None, "requests": []})
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _identity: "alive")
    import polybridge.tasks
    monkeypatch.setattr(polybridge.tasks, "_write_stdin", lambda *_args: True)

    original_update = approvals.update
    raced = False

    def update_with_fast_outcome(directory, task_id, request_id, generation, expected_delivery=None, **changes):
        nonlocal raced
        result = original_update(directory, task_id, request_id, generation, expected_delivery, **changes)
        if changes.get("delivery") == "sending" and not raced:
            raced = True
            original_update(directory, task_id, request_id, generation, "sending", execution_outcome="succeeded", disposition="decided", delivery="sent")
        return result

    monkeypatch.setattr(approval_runtime.approvals, "update", update_with_fast_outcome)
    assert await approval_runtime.pump(registry, task) is False
    settled = approvals.rows(tmp_path, "task-1")[0]
    assert settled["execution_outcome"] == "succeeded"
    assert settled["disposition"] == "decided"
    assert settled["delivery"] == "sent"

