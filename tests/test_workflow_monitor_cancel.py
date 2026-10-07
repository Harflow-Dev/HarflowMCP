"""Monitor cancellation must not acquire the caller's other interaction powers."""
import copy

import pytest

from polybridge import workflows as w
from polybridge.workflow_cancellation import eligibility


@pytest.fixture
def run_store(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda _: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    storage = w.WorkflowStore(tmp_path)
    definition = w.validate_definition({"name": "cancel", "orchestrator": {"backend": "codex"}, "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}], "connections": [{"id": "done", "source": "start", "target": "end"}]})
    run = storage.create_run(definition, "cancel test", tmp_path)
    rid = run["workflow_run_id"]
    storage.update_run(rid, lambda r: r.update(status="paused", interaction_owner="caller"), "fixture")
    return storage, rid


@pytest.mark.parametrize("status", ["paused", "needs_input", "needs_attention", "stuck"])
def test_idle_external_cancellation_preserves_owner_and_checkpoint(run_store, status):
    storage, rid = run_store
    storage.update_run(rid, lambda r: r.update(status=status, input_question="Original question", instructions="Original answer", input_decision_id="checkpoint"), "fixture")
    assert eligibility(storage, storage.get_run(rid))[0]
    result = storage.control(rid, "cancel", interaction_owner="monitor")
    assert result["status"] == "cancelling"
    assert result["interaction_owner"] == "caller"
    assert result["input_question"] == "Original question"
    assert result["instructions"] == "Original answer"
    assert result["input_decision_id"] == "checkpoint"
    assert not eligibility(storage, result)[0]


@pytest.mark.parametrize("status", ["running", "starting", "paused", "needs_input", "needs_attention"])
def test_monitor_owner_can_cancel_active_or_idle_runs(run_store, status):
    storage, rid = run_store
    storage.update_run(rid, lambda r: r.update(status=status, interaction_owner="monitor", activations=[{"id": "work", "status": "running", "tasks": [{"task_id": "active", "status": "running"}]}]), "fixture")
    assert storage.control(rid, "cancel", interaction_owner="monitor")["status"] == "cancelling"


@pytest.mark.parametrize("status", ["reserved", "running", "uncertain", "cancelling"])
def test_idle_external_with_active_task_refused(run_store, status):
    storage, rid = run_store
    storage.update_run(rid, lambda r: r.update(activations=[{"id": "work", "status": "waiting", "tasks": [{"task_id": "active", "transport_task_id": "native", "status": status}]}]), "fixture")
    assert not eligibility(storage, storage.get_run(rid))[0]
    with pytest.raises(w.WorkflowError, match="settling|active"):
        storage.control(rid, "cancel", interaction_owner="monitor")


@pytest.mark.parametrize("stage", ["preparing", "created", "running"])
def test_empty_active_invocation_is_not_idle(run_store, stage):
    storage, rid = run_store
    storage.update_run(rid, lambda r: r.update(activations=[{"id": "child", "status": "waiting_for_child", "tasks": [], "invocation": {"stage": stage}}]), "fixture")
    with pytest.raises(w.WorkflowError, match="Nested"):
        storage.control(rid, "cancel", interaction_owner="monitor")


def test_missing_descendant_fails_closed(run_store):
    storage, rid = run_store
    storage.update_run(rid, lambda r: r.update(activations=[{"id": "child", "status": "waiting_for_child", "tasks": [], "invocation": {"stage": "waiting", "child_workflow_run_id": "missing"}}]), "fixture")
    assert not eligibility(storage, storage.get_run(rid))[0]
    with pytest.raises(w.WorkflowError, match="unavailable"):
        storage.control(rid, "cancel", interaction_owner="monitor")


def test_stale_poll_is_rechecked_and_late_reservation_is_refused(run_store):
    storage, rid = run_store
    assert eligibility(storage, storage.get_run(rid))[0]
    storage.update_run(rid, lambda r: r.update(status="running"), "resumed")
    with pytest.raises(w.WorkflowError, match="idle"):
        storage.control(rid, "cancel", interaction_owner="monitor")
    storage.update_run(rid, lambda r: r.update(status="paused"), "paused")
    storage.control(rid, "cancel", interaction_owner="monitor")
    with pytest.raises(w.DispatchNotStarted):
        storage.update_run(rid, lambda r: r.update(status="running"), "native_reserved")
    assert storage.get_run(rid)["status"] == "cancelling"


def test_child_reservation_cannot_race_root_cancel(run_store):
    storage, rid = run_store
    child = copy.deepcopy(storage.get_run(rid))
    child.update(workflow_run_id="child", status="running", parent_link={"workflow_run_id": rid, "root_workflow_run_id": rid, "execution_id": "invocation"})
    w._write(storage.runs / "child.json", child)
    storage.control(rid, "cancel", interaction_owner="monitor")
    with pytest.raises(w.DispatchNotStarted):
        storage.update_run("child", lambda r: r.update(activations=[]), "dispatch_reserved")
    assert not eligibility(storage, child)[0]


def test_descendant_reservation_serializes_with_cancel(run_store, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from polybridge import workflow_cancellation
    storage, rid = run_store
    child = copy.deepcopy(storage.get_run(rid))
    child.update(workflow_run_id="child", status="running", parent_link={"workflow_run_id": rid, "root_workflow_run_id": rid, "execution_id": "invocation"})
    w._write(storage.runs / "child.json", child)
    gate_entered, release_gate, reservation_started = Event(), Event(), Event()
    real_eligibility = workflow_cancellation.eligibility
    def gated(store, run):
        gate_entered.set()
        assert release_gate.wait(5)
        return real_eligibility(store, run)
    monkeypatch.setattr(workflow_cancellation, "eligibility", gated)
    def reserve():
        reservation_started.set()
        with pytest.raises(w.DispatchNotStarted):
            storage.update_run("child", lambda r: r.update(status="running"), "native_reserved")
    with ThreadPoolExecutor(max_workers=2) as pool:
        cancellation = pool.submit(storage.control, rid, "cancel", interaction_owner="monitor")
        assert gate_entered.wait(5)
        reservation = pool.submit(reserve)
        assert reservation_started.wait(5)
        assert not reservation.done()
        release_gate.set()
        assert cancellation.result(timeout=5)["status"] == "cancelling"
        reservation.result(timeout=5)


async def test_missing_descendant_cancellation_does_not_report_success(run_store):
    from polybridge.workflow_invocation import WorkflowTree
    storage, rid = run_store
    storage.update_run(rid, lambda r: r.update(status="cancelling", activations=[{"id": "invocation", "node_id": "work", "status": "failed", "role": "node", "tasks": [], "invocation": {"stage": "settled", "child_workflow_run_id": "missing"}}]), "fixture")
    class Registry:
        async def cancel_cascade(self, task_id, **kwargs):
            return {}
    supervisor = w.WorkflowSupervisor(Registry(), storage)
    await supervisor.execute(rid)
    result = storage.get_run(rid)
    assert result["status"] == "needs_attention"
    assert "Unresolved descendant links" in result["attention_reason"]


async def test_descendant_cascade_failures_are_reported(run_store):
    from polybridge.workflow_invocation import WorkflowTree
    storage, rid = run_store
    child = copy.deepcopy(storage.get_run(rid))
    child.update(workflow_run_id="child", status="failed", parent_link={"workflow_run_id": rid, "root_workflow_run_id": rid, "execution_id": "invocation"}, activations=[{"id": "native", "status": "running", "tasks": [{"task_id": "execution", "transport_task_id": "transport", "status": "running"}]}])
    w._write(storage.runs / "child.json", child)
    storage.update_run(rid, lambda r: r.update(activations=[{"id": "invocation", "status": "failed", "tasks": [], "invocation": {"stage": "settled", "child_workflow_run_id": "child"}}]), "fixture")
    class Registry:
        calls = []
        async def cancel_cascade(self, task_id, **kwargs):
            self.calls.append(task_id)
            return {"not_signalled": [{"task_id": task_id, "reason": "process identity uncertain"}]}
    registry = Registry()
    report = await WorkflowTree(storage).cancel_descendants(rid, registry)
    assert registry.calls == ["transport"]
    assert report["errors"][0]["task_id"] == "transport"
    assert "process identity uncertain" in report["errors"][0]["error"]


def test_monitor_cli_cancel_retains_verified_human_flag(monkeypatch, capsys):
    from polybridge import ctl, server
    import json
    observed = {}
    async def call(action, **kwargs):
        observed.update(action=action, **kwargs)
        return {"status": "cancelling"}
    monkeypatch.setattr(server, "_workflow_call", call)
    assert ctl.main(["workflow-cancel", "root", "--monitor", "--json"]) == 0
    assert observed == {"action": "cancel", "run_id": "root", "interaction_owner": "monitor"}
    assert json.loads(capsys.readouterr().out)["result"]["status"] == "cancelling"


async def test_agent_cannot_claim_monitor_cancellation(monkeypatch):
    from polybridge import server, takeover
    monkeypatch.setattr(takeover, "caller_refusal", lambda _: ("caller_detected", "Agent caller cannot claim human controls"))
    with pytest.raises(Exception, match="verified human Monitor"):
        await server._workflow_call("cancel", run_id="root", interaction_owner="monitor")


def test_empty_child_waiting_under_paused_root_is_idle(run_store):
    storage, rid = run_store
    child = copy.deepcopy(storage.get_run(rid))
    child.update(workflow_run_id="child", status="running", suspended_via_root=True, parent_link={"workflow_run_id": rid, "root_workflow_run_id": rid, "execution_id": "invocation"}, activations=[])
    w._write(storage.runs / "child.json", child)
    storage.update_run(rid, lambda r: r.update(activations=[{"id": "invocation", "status": "waiting_for_child", "tasks": [], "invocation": {"stage": "waiting", "child_workflow_run_id": "child"}}]), "fixture")
    assert eligibility(storage, storage.get_run(rid))[0]
    assert storage.control(rid, "cancel", interaction_owner="monitor")["status"] == "cancelling"


@pytest.mark.parametrize("outcome", [None, [], "cancelled", True, 0])
def test_missing_or_invalid_cascade_outcome_fails_closed(outcome):
    from polybridge.workflow_cancellation import cascade_error
    assert cascade_error(outcome) == "Cancellation transport returned no verifiable outcome"


@pytest.mark.parametrize("owner_still_settling", [[], ["lineage-only-child"]])
async def test_tracked_task_settlement_requires_cascade_descendant_owner_settlement(run_store, monkeypatch, owner_still_settling):
    """A lineage descendant has no workflow activation for reconciliation to inspect."""
    storage, rid = run_store
    tracked_id = "tracked-worker"
    storage.update_run(rid, lambda r: r.update(status="cancelling", activations=[{
        "id": "tracked-execution", "node_id": "start", "role": "orchestrator", "status": "running",
        "tasks": [{"task_id": tracked_id, "status": "running"}],
    }]), "fixture")

    class Registry:
        calls = []
        async def cancel_cascade(self, task_id, **kwargs):
            self.calls.append(task_id)
            # The named workflow task has settled, while its lineage-only child's
            # owner may still be recording the cancellation outcome.
            w.task_store.write(storage.root / "tasks", w.task_store.TaskRecord(
                task_id=task_id, backend="codex", session_id="tracked-session", repo_path=str(storage.root),
                started_at="2026-10-07T00:00:00Z", status="cancelled", exit_code=-15,
            ))
            return {"cancelled_descendants": [task_id], "owner_still_settling": owner_still_settling,
                    "sigkill_survivors": [], "not_signalled": [], "not_recorded": [],
                    "cascade_incomplete": False, "unconverged": []}

    registry = Registry()
    supervisor = w.WorkflowSupervisor(registry, storage)
    # Enter the active owner's cancellation loop directly; the real final
    # reconciliation still consumes the task record written by cancel_cascade.
    async def already_reconciled():
        return True
    monkeypatch.setattr(supervisor, "reconcile", already_reconciled)
    await supervisor.execute(rid)
    result = storage.get_run(rid)
    assert registry.calls == [tracked_id]
    assert result["activations"][0]["tasks"][0]["status"] == "cancelled"
    assert result["settling"] is False
    assert result["status"] == ("needs_attention" if owner_still_settling else "cancelled")
    if owner_still_settling:
        assert "owner_still_settling" in result["attention_reason"]
        assert "lineage-only-child" in result["attention_reason"]


def test_cascade_owner_still_settling_is_unresolved():
    from polybridge.workflow_cancellation import cascade_error
    assert "lineage-only-child" in cascade_error({"owner_still_settling": ["lineage-only-child"]})
    assert cascade_error({"owner_still_settling": []}) == ""
