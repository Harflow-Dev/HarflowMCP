"""Workflow read failures are diagnostics, not evidence of an approval interaction."""
import json
from types import SimpleNamespace

import pytest

from harflow_mcp import approval_runtime, approvals, store
from test_caller_write_approvals import CONTEXT, task_stub, tmp_path  # noqa: F401
from test_approval_expiry_and_workflows import bash

pytestmark = pytest.mark.usefixtures("exact_ids")


def select(monkeypatch, ids):
    monkeypatch.setattr(approval_runtime, "workflow_task_ids", lambda *_: ids)


def write(directory, identifier, data):
    approvals._path(directory, identifier).write_bytes(data)


def test_absent_ledger_is_complete_empty(tmp_path, monkeypatch):
    select(monkeypatch, ["task-1"])
    view = approval_runtime.workflow_summary(tmp_path, "run-1")
    assert "read_status" not in view
    assert view["interaction_state"] is None
    assert "read_errors" not in view
    assert not approvals._path(tmp_path, "task-1").exists()


@pytest.mark.parametrize("raw", [b'{', b'', b'{}', b'x' * (approvals.MAX_LEDGER_BYTES + 1)])
def test_unreadable_ledgers_report_task_diagnostic(tmp_path, monkeypatch, raw):
    select(monkeypatch, ["task-1"])
    write(tmp_path, "task-1", raw)
    view = approval_runtime.workflow_summary(tmp_path, "run-1")
    assert view["interaction_state"] is None
    assert view["delivery_uncertain_count"] == 0
    assert view["read_status"] == "incomplete"
    assert view["read_errors"][0]["task_id"] == "task-1"
    assert view["read_errors"][0]["error_type"] == "ValueError"
    assert view["read_errors"][0]["message"]
    assert approvals.summary(tmp_path, "task-1")["interaction_state"] == "approval_state_uncertain"


def pending(tmp_path, monkeypatch):
    approvals.admit(tmp_path, task_stub(tmp_path), bash(), CONTEXT)
    monkeypatch.setattr(store, "read", lambda *_a, **_k: SimpleNamespace(exit_code=None, output_drained=False, status="running"))
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda *_: "alive")


def test_healthy_pending_sibling_survives_bad_ledger(tmp_path, monkeypatch):
    pending(tmp_path, monkeypatch)
    write(tmp_path, "task-bad", b'{')
    select(monkeypatch, ["task-bad", "task-1"])
    view = approval_runtime.workflow_summary(tmp_path, "run-1")
    assert view["pending_count"] == view["awaiting_decision_count"] == 1
    assert view["requests"][0]["task_id"] == "task-1"
    assert view["interaction_state"] == "waiting_for_approval"
    assert view["read_status"] == "incomplete"


@pytest.mark.parametrize("delivery", ["pending", "sending"])
def test_real_uncertainty_survives_bad_sibling(tmp_path, monkeypatch, delivery):
    pending(tmp_path, monkeypatch)
    path = approvals._path(tmp_path, "task-1")
    ledger = json.loads(path.read_bytes())
    ledger["requests"][0]["delivery"] = delivery
    path.write_bytes(approvals.canonical(ledger))
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda *_: "unknown")
    write(tmp_path, "task-bad", b'{')
    select(monkeypatch, ["task-bad", "task-1"])
    view = approval_runtime.workflow_summary(tmp_path, "run-1")
    assert view["interaction_state"] == "approval_state_uncertain"
    assert view["delivery_uncertain_count"] == (delivery == "sending")
    assert view["read_status"] == "incomplete"


def test_aggregate_budget_rejects_without_losing_readable_sibling(tmp_path, monkeypatch):
    pending(tmp_path, monkeypatch)
    ids = [f"bulk-{i}" for i in range(5)]
    for identifier in ids:
        raw = approvals.canonical(approvals._empty(identifier))
        write(tmp_path, identifier, raw + b' ' * (approvals.MAX_LEDGER_BYTES - len(raw)))
    select(monkeypatch, ids + ["task-1"])
    view = approval_runtime.workflow_summary(tmp_path, "run-1")
    assert view["read_error_count"] == 2
    assert "source budget" in view["read_errors"][0]["message"]
    assert view["interaction_state"] is None
    # With a little budget left, an over-budget source does not consume it.
    monkeypatch.setattr(approval_runtime, "WORKFLOW_APPROVAL_SOURCE_BYTES", approvals.MAX_LEDGER_BYTES + 16384)
    select(monkeypatch, ids[:2] + ["task-1"])
    view = approval_runtime.workflow_summary(tmp_path, "run-1")
    assert view["read_error_count"] == 1
    assert view["pending_count"] == 1


def test_oserror_and_diagnostic_bounds(tmp_path, monkeypatch):
    select(monkeypatch, [f"task-{i}" for i in range(40)])
    def fail(*_a, **_k):
        raise OSError("unreadable " + "x" * 10000)
    monkeypatch.setattr(approvals, "summary_strict", fail)
    view = approval_runtime.workflow_summary(tmp_path, "run-1")
    assert view["read_error_count"] == 40
    assert len(view["read_errors"]) == 32
    assert view["read_errors_truncated"]
    assert len(view["read_errors"][0]["message"]) == 512
    assert view["read_errors"][0]["error_type"] == "OSError"
    assert view["interaction_state"] is None


def test_traversal_failure_identifies_workflow(tmp_path, monkeypatch):
    def fail(*_):
        raise ValueError("child ownership link is inconsistent")
    monkeypatch.setattr(approval_runtime, "workflow_task_ids", fail)
    view = approval_runtime.workflow_summary(tmp_path, "run-1")
    assert view["read_errors"] == [{"workflow_run_id": "run-1", "error_type": "ValueError", "message": "child ownership link is inconsistent"}]
    assert view["interaction_state"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("known_uncertain", [False, True])
async def test_workflow_wait_read_error_is_not_attention(tmp_path, monkeypatch, known_uncertain):
    from harflow_mcp import server, wait_policy
    directory = server._reg().log_dir
    directory.mkdir(parents=True, exist_ok=True)
    write(directory, "task-bad", b'{')
    ids = ["task-bad"]
    if known_uncertain:
        # Use an actual ledger summary, with ownership uncertainty and zero unknown deliveries.
        approvals.admit(directory, task_stub(directory), bash(), CONTEXT)
        monkeypatch.setattr(store, "read", lambda *_a, **_k: SimpleNamespace(exit_code=None, output_drained=False, status="running"))
        monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda *_: "unknown")
        ids.append("task-1")
    select(monkeypatch, ids)
    async def status(*_a, **_k):
        return {"workflow_run_id": "run-1", "status": "running", "approval_summary": {}}
    monkeypatch.setattr(server, "_workflow_call", status)
    now = [0.0]
    monkeypatch.setattr(wait_policy, "monotonic", lambda: now[0])
    async def sleep(seconds):
        now[0] += seconds
    monkeypatch.setattr(server.asyncio, "sleep", sleep)
    result = await server._wait_workflow_full("run-1", 1)
    assert result["stop_reason"] == ("attention" if known_uncertain else "timeout")
    assert result["approval_summary"]["read_status"] == "incomplete"


@pytest.mark.parametrize("failure", [None, OSError("task record read denied")])
def test_known_pending_with_unavailable_task_record_remains_uncertain(tmp_path, monkeypatch, failure):
    pending(tmp_path, monkeypatch)
    def read(*_a, **_k):
        if failure:
            raise failure
        return None
    monkeypatch.setattr(store, "read", read)
    select(monkeypatch, ["task-1"])
    view = approval_runtime.workflow_summary(tmp_path, "run-1")
    assert view["interaction_state"] == "approval_state_uncertain"
    assert view["delivery_uncertain_count"] == 0
    assert view["read_status"] == "incomplete"
    assert view["read_errors"][0]["task_id"] == "task-1"
    assert view["read_errors"][0]["error_type"] == ("OSError" if failure else "ValueError")


@pytest.mark.parametrize("field,value", [("created_at", None), ("created_at", "yesterday"),
                                        ("created_at", True), ("request_id", None),
                                        ("created_at", "missing")])
def test_malformed_ordering_metadata_does_not_hide_healthy_sibling(tmp_path, monkeypatch, field, value):
    pending(tmp_path, monkeypatch)
    ledger = json.loads(approvals._path(tmp_path, "task-1").read_bytes())
    ledger["task_id"] = "task-bad"
    ledger["requests"][0]["task_id"] = "task-bad"
    if value == "missing":
        ledger["requests"][0].pop(field)
    else:
        ledger["requests"][0][field] = value
    write(tmp_path, "task-bad", approvals.canonical(ledger))
    select(monkeypatch, ["task-bad", "task-1"])
    view = approval_runtime.workflow_summary(tmp_path, "run-1")
    assert view["pending_count"] == view["awaiting_decision_count"] == 1
    assert view["requests"][0]["task_id"] == "task-1"
    assert view["interaction_state"] == "waiting_for_approval"
    assert view["read_errors"] == [{"task_id": "task-bad", "error_type": "ValueError",
                                    "message": "Invalid approval request ordering metadata"}]
