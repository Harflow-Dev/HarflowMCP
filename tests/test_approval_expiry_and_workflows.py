"""Caller-approval lifecycle: request deadlines and expiry delivery, workflow nodes waiting on approvals, and ctl end to end."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import approval_runtime, approvals, ctl, server, state_ownership, store
from harflow_mcp import workflows as w
from harflow_mcp.backends import get
from harflow_mcp.backends.claude_approvals import decode
from harflow_mcp.state_layout import resolve_layout
from test_caller_write_approvals import CONTEXT, Q310, Registry as LedgerRegistry, event, runtime, runtime_task, task_stub, tmp_path  # noqa: F401
from test_workflow_delegation import Registry as FlowRegistry, parallel, run_bounded, storage  # noqa: F401


def bash(index: int = 1) -> dict:
    return decode(event("Bash", {"command": f"echo {index}"}, index), Q310)


# --- request deadline -------------------------------------------------------------------------------

@pytest.mark.parametrize("hard_offset, expected", [(None, approvals.REQUEST_SECONDS), (3600, approvals.REQUEST_SECONDS), (90, 90), (-5, -5)])
def test_request_deadline_is_the_earlier_of_the_request_window_and_the_attempt_deadline(tmp_path, monkeypatch, hard_offset, expected):
    monkeypatch.setattr(approvals.time, "time", lambda: 1000.0)
    context = {**CONTEXT, **({} if hard_offset is None else {"hard_deadline": 1000.0 + hard_offset})}
    row = approvals.admit(tmp_path, task_stub(tmp_path), bash(), context)
    assert row["deadline"] == 1000.0 + expected


# --- expiry ---------------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unanswered_request_expires_into_a_harflow_rejection_and_does_not_touch_the_node(runtime, monkeypatch):
    directory, registry = runtime
    task = runtime_task(directory)
    task.proc, task.input_closed, task.backend = SimpleNamespace(pid=os.getpid(), returncode=None), False, "claude"
    row = approvals.admit(directory, task, bash(), {**CONTEXT, "hard_deadline": 10**12})
    assert row["deadline"] - row["created_at"] == approvals.REQUEST_SECONDS and row["deadline"] < 10**12
    writes: list[bytes] = []
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _i: "alive")
    monkeypatch.setattr("harflow_mcp.tasks._write_stdin", lambda _t, wire: writes.append(wire) or True)
    monkeypatch.setattr(approval_runtime, "context", lambda *_a: dict(CONTEXT))
    registry.get = lambda _id: task
    monkeypatch.setattr(approval_runtime.time, "time", lambda: row["deadline"] + 1)
    await approval_runtime.pump(registry, task)
    assert json.loads(writes[0])["response"]["response"] == {"behavior": "deny", "message": "Harflow refused this action: approval_expired"}
    (final,) = approvals.rows(directory, "task-1")
    assert (final["decision"], final["effective_decision"], final["delivered_decision"], final["delivery"], final["disposition"], final["expiry_rejection"]) == (None, "reject", "reject", "sent", "expired", True)
    assert final["execution_outcome"] == "unknown"
    # Expiry neither stops the run nor times the node out: that is the attempt-timeout mechanism's job alone.
    assert registry.stopped == 0 and task.cancel_requested is False


@pytest.mark.asyncio
async def test_failed_delivery_on_expiry_is_recorded_separately_and_never_approves(runtime, monkeypatch):
    directory, registry = runtime
    task = runtime_task(directory)
    task.proc, task.input_closed, task.backend = SimpleNamespace(pid=os.getpid(), returncode=None), False, "claude"
    row = approvals.admit(directory, task, bash(), CONTEXT)
    approvals.reserve(directory, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _i: "alive")
    monkeypatch.setattr("harflow_mcp.tasks._write_stdin", lambda _t, _wire: False)
    monkeypatch.setattr(approval_runtime, "context", lambda *_a: dict(CONTEXT))
    registry.get = lambda _id: task
    monkeypatch.setattr(approval_runtime.time, "time", lambda: row["deadline"] + 1)
    await approval_runtime.pump(registry, task)
    (final,) = approvals.rows(directory, "task-1")
    assert final["decision"] == "approve" and final["effective_decision"] == "reject" and final["expiry_rejection"] is True
    assert final["delivery"] == "delivery_unknown" and final["delivered_decision"] is None
    assert final["disposition"] == "expired" and final["execution_outcome"] == "unknown"
    # Nothing is resent automatically.
    writes: list[bytes] = []
    monkeypatch.setattr("harflow_mcp.tasks._write_stdin", lambda _t, wire: writes.append(wire) or True)
    await approval_runtime.pump(registry, task)
    assert writes == []


def test_a_stale_revision_after_expiry_is_refused_even_with_the_current_digest(tmp_path, monkeypatch):
    row = approvals.admit(tmp_path, task_stub(tmp_path), bash(), CONTEXT)
    monkeypatch.setattr(approvals.time, "time", lambda: row["deadline"] + 1)
    for revision in (row["owner_revision"], row["owner_revision"] + 1):
        with pytest.raises(ValueError, match="expired|Stale"):
            approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], revision, "approve", row["generation"])
    assert approvals.get(tmp_path, "task-1", row["request_id"])["decision"] is None


# --- workflow nodes waiting for approval --------------------------------------------------------------

OWNER = {"pid": 5678, "start_time": "bridge-start", "markers": []}


class ApprovalRegistry(FlowRegistry):
    """Worker nodes in `gated` ask for one approval and continue only after it is answered and delivered."""

    def __init__(self, root, gated):
        super().__init__(root)
        self.gated = set(gated)
        self.rows: dict[str, dict] = {}
        self.continued: dict[str, str] = {}

    async def _mutation_caller(self):
        return None

    def get(self, task_id):
        return SimpleNamespace(pump_wake=asyncio.Event()) if any(r["task_id"] == task_id for r in self.rows.values()) else super().get(task_id)

    async def start(self, prompt, repo, **kwargs):
        node = kwargs["title"].split(" · ")[-1]
        if node not in self.gated or self.decode_context(prompt) is not None:
            return await super().start(prompt, repo, **kwargs)
        task_id = kwargs["task_id"]
        store.write(self._log_dir, store.TaskRecord(task_id=task_id, backend="claude", session_id="session-" + task_id, repo_path=str(repo), started_at="now", status="running", caller_approvals=True,
                                                    approval_generation="gen-" + task_id, start_time="task-start", owner=OWNER))
        task = SimpleNamespace(task_id=task_id, proc=SimpleNamespace(pid=1234), start_time="task-start", markers=("m",), owner=OWNER, approval_generation="gen-" + task_id,
                               session_id="session-" + task_id, repo_path=repo, freedom="write_in_repo")
        context = await asyncio.to_thread(approval_runtime.context, self._log_dir, task_id, "caller")
        self.rows[node] = approvals.admit(self._log_dir, task, bash(len(self.rows) + 1), context)
        while (current := approvals.get(self._log_dir, task_id, self.rows[node]["request_id"]))["delivery"] != "reserved":
            await asyncio.sleep(0.01)
        generation, request_id = current["generation"], current["request_id"]
        approvals.update(self._log_dir, task_id, request_id, generation, "reserved", delivery="sending")
        effective = current["decision"]
        approvals.finish_delivery(self._log_dir, task_id, request_id, generation, True, effective)
        approvals.observe_outcome(self._log_dir, task_id, request_id, generation, "succeeded" if effective == "approve" else "failed")
        self.continued[node] = effective
        return await super().start(prompt, repo, **kwargs)


async def until(predicate, timeout=10.0):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout)


def respond(registry, node, decision, *, monitor=False):
    row = registry.rows[node]
    return approval_runtime.respond_request(registry, row["task_id"], row["request_id"], row["action_digest"], row["owner_revision"], decision, "monitor" if monitor else "caller")


@pytest.mark.asyncio
async def test_workflow_node_waits_for_approval_then_continues(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _i: "alive")
    registry = ApprovalRegistry(storage.root, {"left"})
    run = storage.create_run(w.validate_definition(parallel()), "ORIGINAL PRIVATE REQUEST", tmp_path)
    run_id = run["workflow_run_id"]
    flow = asyncio.create_task(w.WorkflowSupervisor(registry, storage).execute(run_id))
    try:
        await until(lambda: "left" in registry.rows)
        summary = approval_runtime.workflow_summary(registry.log_dir, run_id)
        assert summary["interaction_state"] == "waiting_for_approval" and summary["pending_count"] == 1 and summary["awaiting_decision_count"] == 1
        assert not flow.done() and storage.get_run(run_id)["status"] != "completed"
        listed = await approval_runtime.list_requests(registry, workflow_run_id=run_id)
        assert [r["request_id"] for r in listed["requests"]] == [registry.rows["left"]["request_id"]]
        answered = await respond(registry, "left", "approve", monitor=True)
        assert answered["decision"] == "approve" and answered["delivery"] == "reserved" and answered["responder"] == "monitor"
        await asyncio.wait_for(flow, 20)
    finally:
        if not flow.done():
            flow.cancel()
    assert storage.get_run(run_id)["status"] == "completed"
    final = approvals.get(registry.log_dir, registry.rows["left"]["task_id"], registry.rows["left"]["request_id"])
    assert (final["delivery"], final["delivered_decision"], final["execution_outcome"], final["disposition"]) == ("sent", "approve", "succeeded", "decided")
    assert approval_runtime.workflow_summary(registry.log_dir, run_id)["interaction_state"] is None


@pytest.mark.asyncio
async def test_parallel_sibling_requests_are_answered_independently(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _i: "alive")
    registry = ApprovalRegistry(storage.root, {"left", "right"})
    run = storage.create_run(w.validate_definition(parallel()), "ORIGINAL PRIVATE REQUEST", tmp_path)
    run_id = run["workflow_run_id"]
    flow = asyncio.create_task(w.WorkflowSupervisor(registry, storage).execute(run_id))
    try:
        await until(lambda: {"left", "right"} <= set(registry.rows))
        assert approval_runtime.workflow_summary(registry.log_dir, run_id)["pending_count"] == 2
        assert registry.rows["left"]["task_id"] != registry.rows["right"]["task_id"]
        await respond(registry, "right", "reject")
        await until(lambda: "right" in registry.continued)
        assert "left" not in registry.continued
        summary = approval_runtime.workflow_summary(registry.log_dir, run_id)
        assert summary["pending_count"] == 1 and summary["requests"][0]["request_id"] == registry.rows["left"]["request_id"]
        # A digest or revision taken from the sibling never answers this request.
        left = registry.rows["left"]
        with pytest.raises(ValueError, match="Stale"):
            await approval_runtime.respond_request(registry, left["task_id"], left["request_id"], registry.rows["right"]["action_digest"], left["owner_revision"], "approve")
        await respond(registry, "left", "approve")
        await asyncio.wait_for(flow, 20)
    finally:
        if not flow.done():
            flow.cancel()
    assert registry.continued == {"left": "approve", "right": "reject"}


# --- ctl end to end -------------------------------------------------------------------------------------

def test_ctl_approval_list_get_respond_end_to_end(tmp_path, monkeypatch, capsys):
    directory = tmp_path / "state" / "tasks"
    with state_ownership.state_session(resolve_layout(root=directory.parent)):
        directory.mkdir(parents=True)
    store.write(directory, store.TaskRecord(task_id="task-1", backend="claude", session_id="session-1", repo_path=str(directory), started_at="now", status="running", caller_approvals=True,
                                            approval_generation="generation-1", start_time="task-start", owner=OWNER))
    task = SimpleNamespace(task_id="task-1", proc=SimpleNamespace(pid=1234), start_time="task-start", markers=("task-marker",), owner=OWNER, approval_generation="generation-1",
                           session_id="session-1", repo_path=directory, freedom="write_in_repo")
    row = approvals.admit(directory, task, bash(), CONTEXT)

    class Registry:
        log_dir = directory
        state_layout = resolve_layout(log_dir=directory)
        task_ = SimpleNamespace(pump_wake=SimpleNamespace(set=lambda: None))

        async def _mutation_caller(self):
            return None

        def get(self, _task_id):
            return self.task_

    registry = Registry()
    monkeypatch.delenv("HARFLOW_TASK_ID", raising=False)
    monkeypatch.setattr(server, "_registry", registry)
    monkeypatch.setattr(server, "install_cli_registry", lambda: registry)
    monkeypatch.setattr("harflow_mcp.caller_policy.require_mutation", lambda *_a, **_k: None)
    monkeypatch.setattr(ctl, "default_log_dir", lambda: directory)
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _i: "alive")

    def call(*argv):
        code = ctl.main([*argv, "--json"])
        document = json.loads(capsys.readouterr().out)
        return code, document["result"] if "result" in document else document

    code, listed = call("approval-list", "--task-id", "task-1")
    assert code == 0 and [r["request_id"] for r in listed["requests"]] == [row["request_id"]] and "action" not in listed["requests"][0]
    code, full = call("approval-get", "task-1", row["request_id"])
    assert code == 0 and full["action"] == row["action"] and full["disposition"] == "pending"
    code, stale = call("approval-respond", "task-1", row["request_id"], "--action-digest", full["action_digest"], "--owner-revision", str(full["owner_revision"] + 5), "--decision", "approve")
    assert code != 0
    code, answered = call("approval-respond", "task-1", row["request_id"], "--action-digest", full["action_digest"], "--owner-revision", str(full["owner_revision"]), "--decision", "reject")
    assert code == 0 and answered["decision"] == "reject" and answered["delivery"] == "reserved"
    code, again = call("approval-respond", "task-1", row["request_id"], "--action-digest", full["action_digest"], "--owner-revision", str(full["owner_revision"]), "--decision", "approve")
    assert code != 0
