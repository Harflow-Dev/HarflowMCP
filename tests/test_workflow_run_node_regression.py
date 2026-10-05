"""N4 amendment regression: runs without workflow nodes behave exactly as before."""
import asyncio

from polybridge import workflows as w

from test_workflow_delegation import Registry, default_decision, graph, run_flow, storage


async def test_run_without_workflow_nodes_is_unchanged(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path)
    assert run["status"] == "completed", run.get("attention_reason")
    workers = [a for a in run["activations"] if a["role"] == "node"]
    assert len(workers) == 1
    assert workers[0]["node_result"]["status"] == "succeeded"


async def test_run_without_workflow_nodes_keeps_decision_and_dispatch_flow(storage, tmp_path):
    calls: list[str] = []

    def policy(context, registry):
        calls.append(context["current_stage"]["node_id"])
        return default_decision(context, registry)

    run, registry = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy))
    assert run["status"] == "completed", run.get("attention_reason")
    assert calls == ["start", "work", "end"]


async def test_no_tree_gate_blocks_ordinary_pause_and_resume(storage, tmp_path):
    run, _ = await run_flow(storage, tmp_path)
    assert run["status"] == "completed"
    supervisor = w.WorkflowSupervisor(Registry(storage.root), storage)
    assert supervisor.tree.tree_running(storage.get_run(run["workflow_run_id"])) is False
