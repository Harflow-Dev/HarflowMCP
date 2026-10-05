"""Guided structural traversal keeps individual assignments authoritative."""
from polybridge import workflows as w
from polybridge.workflow_traversal import automatic_edges
from test_workflow_delegation import Registry, default_decision, run_flow, storage  # noqa: F401


def definition():
    return {"name": "branches", "routing_mode": "explicit", "orchestrator": {"backend": "codex"}, "nodes": [{"id": "start", "type": "start"}, {"id": "split", "type": "parallel_start", "parallel_group_id": "pair"}, *[{"id": name, "type": "agent", "instructions": name, "agent": {"backend": "codex"}} for name in ("left", "right")], {"id": "join", "type": "parallel_end", "parallel_group_id": "pair"}, {"id": "end", "type": "end"}], "connections": [{"id": "enter", "source": "start", "target": "split"}, *[{"id": name, "source": "split", "target": name} for name in ("left", "right")], *[{"id": name + "-join", "source": name, "target": "join"} for name in ("left", "right")], {"id": "finish", "source": "join", "target": "end"}]}


def policy(context, registry):
    decision = default_decision(context, registry)
    for entry, choice in zip(decision.get("next", []), context["valid_continuations"]):
        if "branch_continuations" in choice:
            entry["branch_assignments"] = [{"continuation_id": c["continuation_id"], "prompt": "Distinct briefing " + c["node_id"], "session_mode": "fresh"} for c in choice["branch_continuations"]]
    return decision


async def test_guided_split_has_one_briefing_and_structural_end_skips_routing(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path, definition(), Registry(storage.root, policy=policy), guided=True)
    assert run["status"] == "completed"
    assert [c["current_stage"]["node_id"] for c in registry.contexts] == ["start", "left", "right", "end"]
    workers = [a for a in run["activations"] if a["role"] == "node"]
    assert {a["assignment_prompt"] for a in workers} == {"Distinct briefing left", "Distinct briefing right"}
    assert run["transitions"] == 6
    assert len(run["released_parallel_groups"]) == 1


async def test_missing_branch_assignment_dispatches_no_worker(storage, tmp_path):
    def missing(context, registry):
        decision = policy(context, registry)
        if context["current_stage"]["node_id"] == "start":
            decision["next"][0]["branch_assignments"].pop()
        return decision
    run, registry = await run_flow(storage, tmp_path, definition(), Registry(storage.root, policy=missing), guided=True)
    assert run["status"] == "needs_attention"
    assert not any(a["role"] == "node" for a in run["activations"])
    assert run["transitions"] == 0


def test_historical_and_conditional_structural_exits_keep_judgment():
    run = {"definition": w.validate_definition(definition()), "activations": [], "joins": {}, "transitions": 0}
    node = next(n for n in run["definition"]["nodes"] if n["id"] == "join")
    token = {"id": "t", "node_id": "join"}
    assert automatic_edges(run, node, token) is None
    run["runner_policy"] = "guided"
    assert automatic_edges(run, node, token)[0]["id"] == "finish"
    run["definition"]["connections"][-1]["condition"] = "Only if findings are addressed"
    assert automatic_edges(run, node, token) is None


async def test_successful_worker_end_asks_only_completion(storage, tmp_path):
    from test_workflow_delegation import graph
    run, registry = await run_flow(storage, tmp_path, graph(), guided=True)
    assert run["status"] == "completed"
    assert [c["current_stage"]["node_id"] for c in registry.contexts] == ["start", "end"]
    assert registry.contexts[-1]["input_results"][0]["status"] == "succeeded"
    assert run["transitions"] == 2


async def test_end_checkpoint_can_complete_only_assigned_implementation_evidence(storage, tmp_path):
    from test_workflow_delegation import graph
    g = graph("planning")
    g["nodes"].insert(2, {"id": "implement", "type": "agent", "role": "implementation", "instructions": "Implement", "agent": {"backend": "codex"}})
    g["connections"][1]["target"] = "implement"
    g["connections"].append({"id": "done", "source": "implement", "target": "end"})
    def assign(context, registry):
        value = default_decision(context, registry)
        if context["current_stage"]["node_id"] == "work":
            value["next"][0]["assigned_task_ids"] = ["one"]
        if context["current_stage"]["node_id"] == "end":
            value["task_updates"] = [{"task_id": "one", "status": "completed", "reason": "Observed successful assigned implementation"}]
        return value
    outputs = {"work": {"status": "succeeded", "result": {"tasks": [{"id": "one", "title": "Implement"}], "technical_plan": "Implement the bounded change"}, "evidence": []}, "implement": {"status": "succeeded", "result": {"completed_task_ids": ["one"]}, "evidence": []}}
    run, registry = await run_flow(storage, tmp_path, g, Registry(storage.root, assign, outputs), guided=True)
    assert run["status"] == "completed"
    assert run["tasks"][0]["status"] == "completed"
    assert [c["current_stage"]["node_id"] for c in registry.contexts] == ["start", "work", "end"]
