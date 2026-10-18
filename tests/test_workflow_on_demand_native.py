"""Native execution for standalone on-demand workflow nodes."""
import asyncio

from polybridge import store, workflows as w

from test_workflow_delegation import default_decision, graph
from test_workflow_native import MODEL, NativeRegistry


class OnDemandNativeRegistry(NativeRegistry):
    def __init__(self, root, policy):
        super().__init__(root)
        self.policy = policy


def native_on_demand_graph():
    definition = graph()
    definition["orchestrator"] = {"backend": "claude", "model": MODEL}
    definition["on_demand_nodes"] = [{
        "id": "research",
        "title": "API researcher",
        "instructions": "Return API evidence.",
        "call_condition": "Call when API behaviour needs verification.",
        "agent": {"backend": "claude", "model": MODEL},
        "freedom": "read_only",
        "session_mode": "fresh",
        "execution_mode": "prefer_subagent",
    }]
    return definition


async def test_standalone_prefer_subagent_uses_native_worker_and_returns_to_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w.backends, "version", lambda backend: "2.1.295 (Claude Code)")
    storage = w.WorkflowStore(tmp_path)
    definition = native_on_demand_graph()
    calls = {"count": 0}

    def policy(context, registry):
        if context["current_stage"]["node_id"] == "start" and not context.get("on_demand_results"):
            calls["count"] += 1
            return {
                "decision_id": context["decision_id"],
                "action": "call_on_demand",
                "node_id": context["on_demand_nodes"][0]["node_id"],
                "assignment": "Verify the API response",
                "reason": "Need specialist evidence",
            }
        return default_decision(context, registry)

    registry = OnDemandNativeRegistry(tmp_path, policy)
    run = storage.create_run(w.validate_definition(definition), "Review API", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)

    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed", observed.get("attention_reason") or observed.get("failure_reason")
    assert calls["count"] == 1
    checkpoints = [c for c in registry.contexts if c["current_stage"]["node_id"] == "start"]
    assert len(checkpoints) == 2
    assert checkpoints[0]["decision_id"] == checkpoints[1]["decision_id"]
    assert checkpoints[1]["on_demand_results"][0]["status"] == "succeeded"
    assert len(observed["on_demand_calls"]) == 1
    call = observed["on_demand_calls"][0]
    assert call["status"] == "succeeded"
    specialist = next(a for a in observed["activations"] if a.get("on_demand_call_id") == call["id"])
    attempt = specialist["tasks"][0]
    assert attempt["execution_kind"] == "native_subagent"
    assert attempt["native_child_id"] == "native-child"
    assert registry.native_calls[0]["native_subagent"] is True
    assert store.read(registry._log_dir, attempt["task_id"]) is None
    assert store.read(registry._log_dir, attempt["transport_task_id"]) is not None
    assert len([a for a in observed["activations"] if a.get("on_demand_call_id")]) == 1

