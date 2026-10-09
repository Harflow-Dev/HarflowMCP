"""Nested native transports retain the root tree's checkout exclusion."""
import pytest

from polybridge import workflows as w
from polybridge.workflow_invocation import tree_write_strength
from test_workflow_native import MODEL, native_graph
from test_workflow_native_batch import BatchRegistry, graph as batch_graph
from test_workflow_run_node_execution import parent_graph, run_tree, child_runs, workflow_policy


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("mode", ["current", "child"])
async def test_readonly_native_descendant_keeps_writer_root_lease(tmp_path, monkeypatch, batch, mode):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w.backends, "version", lambda backend: "2.1.295 (Claude Code)")
    storage = w.WorkflowStore(tmp_path)
    definition = batch_graph() if batch else native_graph()
    definition["name"] = "child"
    saved = storage.save("child", definition)
    parent = parent_graph(saved["workflow_id"], mode=mode, prep=True)
    parent["orchestrator"] = {"backend": "claude", "model": MODEL}
    parent["nodes"][1]["freedom"] = "write_in_repo"
    storage.save("parent", parent)
    registry = BatchRegistry(tmp_path)
    registry.policy = workflow_policy
    strengths = []
    original = w.CheckoutLease

    class InspectLease(original):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            strengths.append(self.write)

    monkeypatch.setattr(w, "CheckoutLease", InspectLease)
    run, _ = await run_tree(storage, tmp_path, "parent", registry)
    assert run["status"] == "completed", run.get("attention_reason")
    child = child_runs(storage, run)[0]
    assert tree_write_strength(child, "read_only") is False
    assert tree_write_strength(run, "read_only") is True
    workers = [activation for activation in child["activations"] if activation["role"] == "node"]
    assert workers and all(activation["execution_kind"] == "native_subagent" for activation in workers)
    owner_attempts = [task for activation in run["activations"] if activation["role"] == "orchestrator" for task in activation["tasks"]]
    assert all(task["freedom"] == "read_only" for task in owner_attempts)
    assert strengths and all(strengths)
    assert bool(registry.batches) is batch
