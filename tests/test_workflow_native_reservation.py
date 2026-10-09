"""Atomic native reservation and candidate-specific fallback presentation."""
import asyncio

import pytest

from polybridge import workflows as w
from test_workflow_native import MODEL
from test_workflow_native_batch import BatchRegistry, graph as batch_graph
from test_workflow_native_fallback_runtime import RuntimeRegistry, fallback_graph


@pytest.fixture
def certified(monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda _: True)
    monkeypatch.setattr(w.backends, "version", lambda _: "2.1.295 (Claude Code)")


async def test_cancel_after_batch_preflight_refuses_atomic_reservation(tmp_path, monkeypatch, certified):
    storage, registry = w.WorkflowStore(tmp_path), BatchRegistry(tmp_path)
    run = storage.create_run(w.validate_definition(batch_graph()), "Review", tmp_path)
    supervisor = w.WorkflowSupervisor(registry, storage)
    update = storage.update_run
    intercepted = []

    def cancel_before_reserve(run_id, mutate, event, detail=None):
        if event == "native_batch_reserved":
            intercepted.append(event)
            # Preparation, capacity and the checkout lease have succeeded. The
            # cancellation wins immediately before the atomic reservation gate.
            update(run_id, lambda current: current.update(status="cancelling"), "cancel_won_race")
        return update(run_id, mutate, event, detail)

    monkeypatch.setattr(storage, "update_run", cancel_before_reserve)
    await asyncio.wait_for(supervisor.execute(run["workflow_run_id"]), 10)
    observed = storage.get_run(run["workflow_run_id"])
    assert intercepted
    assert observed["status"] == "cancelled"
    assert registry.batches == [] and registry.native_calls == []
    assert not any(activation["role"] == "native_control" for activation in observed["activations"])
    workers = [activation for activation in observed["activations"] if activation["role"] == "node"]
    assert workers and all(activation["tasks"] == [] for activation in workers)
    assert supervisor.tree.slots.free == run["definition"]["max_parallel"]
    assert supervisor.tree.control_slots.free == 1
    owned = {task["task_id"] for activation in observed["activations"] if activation["role"] == "orchestrator" for task in activation["tasks"]}
    assert {path.name.removesuffix(".meta.json") for path in registry._log_dir.glob("*.meta.json")} == owned


@pytest.mark.parametrize("batch", [False, True])
async def test_native_fallback_clears_reason_without_rewriting_failed_attempt(tmp_path, monkeypatch, certified, batch):
    from polybridge import workflow_native
    dispatch = workflow_native.dispatch_native
    local_reasons = []
    async def observe_local(supervisor, node, assignment, activation):
        handled, result = await dispatch(supervisor, node, assignment, activation)
        local_reasons.append((handled, activation.get("execution_fallback_reason")))
        return handled, result
    monkeypatch.setattr(workflow_native, "dispatch_native", observe_local)
    storage = w.WorkflowStore(tmp_path)
    definition = batch_graph() if batch else fallback_graph()
    for node in definition["nodes"]:
        if node["type"] == "agent":
            node["agent"] = {"backend": "opencode", "fallbacks": [{"backend": "claude", "model": MODEL}]}
    registry = BatchRegistry(tmp_path) if batch else RuntimeRegistry(tmp_path)
    registry.outputs = {node["id"]: {"status": "failed", "summary": "Primary is unavailable", "backend": "opencode"} for node in definition["nodes"] if node["type"] == "agent"}
    monkeypatch.setattr(w, "availability_failure", lambda snapshot: "Primary is unavailable" if snapshot.get("backend") == "opencode" else None)
    run = storage.create_run(w.validate_definition(definition), "Review", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 10)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed", observed.get("attention_reason")
    assert any(not handled and reason for handled, reason in local_reasons)
    assert any(handled for handled, reason in local_reasons)
    assert all(reason is None for handled, reason in local_reasons if handled)
    workers = [activation for activation in observed["activations"] if activation["role"] == "node"]
    for activation in workers:
        assert activation["execution_kind"] == "native_subagent"
        assert "execution_fallback_reason" not in activation
        primary, native = activation["tasks"]
        assert primary["candidate"]["backend"] == "opencode"
        assert primary["execution_kind"] == "headless"
        assert "differs" in primary["execution_fallback_reason"]
        assert native["candidate"]["backend"] == "claude"
        assert "execution_fallback_reason" not in native
