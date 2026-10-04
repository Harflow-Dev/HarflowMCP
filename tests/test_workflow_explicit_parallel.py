"""Explicit split/merge authoring and durable delegation execution."""
import copy

import pytest

from polybridge import workflows as w
from test_workflow_delegation import Registry, default_decision, run_flow, storage  # noqa: F401


def explicit(nested=False):
    nodes = [{"id": n, "type": t, **({"parallel_group_id": g} if g else {})} for n, t, g in [
        ("start", "start", None), ("split", "parallel_start", "outer"),
        ("left", "agent", None), ("left2", "agent", None), ("right", "agent", None),
        ("merge", "parallel_end", "outer"), ("end", "end", None)]]
    edges = [("start", "split"), ("split", "left"), ("split", "right"), ("left", "left2"), ("left2", "merge"), ("right", "merge"), ("merge", "end")]
    if nested:
        nodes += [{"id": "inner", "type": "parallel_start", "parallel_group_id": "inner"}, {"id": "inner_end", "type": "parallel_end", "parallel_group_id": "inner"}, {"id": "inner_a", "type": "agent"}, {"id": "inner_b", "type": "agent"}]
        edges.remove(("right", "merge"))
        edges += [("right", "inner"), ("inner", "inner_a"), ("inner", "inner_b"), ("inner_a", "inner_end"), ("inner_b", "inner_end"), ("inner_end", "merge")]
    return {"name": "explicit", "routing_mode": "explicit", "max_parallel": 1, "nodes": nodes, "connections": [{"id": a + "-" + b, "source": a, "target": b} for a, b in edges]}


def policy(context, registry):
    if context["current_stage"]["node_id"] == "start":
        context = copy.deepcopy(context)
        context["valid_continuations"] = context["valid_continuations"][:1]
    return default_decision(context, registry)


def test_ordinary_alternatives_require_one():
    graph = explicit()
    graph["connections"].append({"id": "skip", "source": "start", "target": "end"})
    graph = w.validate_definition(graph)
    node = graph["nodes"][0]
    choices = [e for e in graph["connections"] if e["source"] == "start"]
    with pytest.raises(w.WorkflowError, match="exactly one"):
        w.validate_selection(graph, node, choices, {}, {})
    assert w.validate_selection(graph, node, choices[:1], {}, {}) is None


def test_split_requires_all_paths():
    graph = w.validate_definition(explicit())
    node = next(n for n in graph["nodes"] if n["id"] == "split")
    edges = [e for e in graph["connections"] if e["source"] == "split"]
    with pytest.raises(w.WorkflowError, match="all forward branches"):
        w.validate_selection(graph, node, edges[:1], {}, {})
    assert w.validate_selection(graph, node, edges, {}, {}) == "merge"


@pytest.mark.parametrize("mutation", ["missing_end", "cross_branch", "escape", "external_entry", "unmarked", "one_branch", "nesting"])
def test_invalid_regions(mutation):
    graph = explicit(nested=mutation == "nesting")
    if mutation == "missing_end":
        next(n for n in graph["nodes"] if n["id"] == "merge")["parallel_group_id"] = "orphan"
    elif mutation == "cross_branch":
        graph["connections"].append({"id": "cross", "source": "left", "target": "right"})
    elif mutation == "escape":
        graph["connections"].append({"id": "escape", "source": "left", "target": "end"})
    elif mutation == "external_entry":
        graph["connections"].append({"id": "enter", "source": "start", "target": "left"})
    elif mutation == "unmarked":
        graph.pop("routing_mode")
    elif mutation == "one_branch":
        graph["connections"] = [e for e in graph["connections"] if e["id"] != "split-right"]
    elif mutation == "nesting":
        graph["connections"].append({"id": "bad_nesting", "source": "inner_a", "target": "merge"})
    with pytest.raises(w.WorkflowError):
        w.validate_definition(graph)


@pytest.mark.asyncio
@pytest.mark.parametrize("nested", [False, True])
async def test_serial_worker_cap_never_blocks_barriers(storage, tmp_path, nested):
    registry = Registry(storage.root, policy=policy)
    run, registry = await run_flow(storage, tmp_path, explicit(nested), registry)
    assert run["status"] == "completed", run.get("attention_reason")
    assert not run["joins"]
    assert len(run["released_parallel_groups"]) == (2 if nested else 1)
    assert all(len(g["branch_ids"]) == 2 for g in run["released_parallel_groups"].values())
    executions = [a["node_id"] for a in run["activations"] if a["role"] == "node"]
    assert len(executions) == len(set(executions)) == (5 if nested else 3)
    merge_decision = next(c for c in registry.contexts if c["current_stage"]["node_id"] == "merge")
    assert len(merge_decision["input_results"]) == (3 if nested else 2)


@pytest.mark.asyncio
async def test_required_retry_resolves_evidence_without_repeating_sibling(storage, tmp_path):
    attempts = 0
    def worker(prompt, kwargs):
        nonlocal attempts
        attempts += 1
        return {"status": "blocked" if attempts == 1 else "succeeded", "result": {"summary": "left2"}, "evidence": []}
    def retry_policy(context, registry):
        retries = [c for c in context["valid_continuations"] if c["kind"] == "retry_execution"]
        if retries:
            return {"decision_id": context["decision_id"], "action": "continue", "reason": "Supply missing context", "next": [{"continuation_id": retries[0]["continuation_id"], "prompt": "Corrected assignment", "session_mode": "fresh"}]}
        return policy(context, registry)
    graph = explicit()
    for node in graph["nodes"]:
        node["title"] = node["id"]
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, retry_policy, {"left2": worker}))
    assert run["status"] == "completed"
    assert attempts == 2
    workers = [a for a in run["activations"] if a["role"] == "node"]
    left = [a for a in workers if a["node_id"] == "left2"]
    assert left[0]["node_result"]["status"] == "blocked"
    assert left[0]["resolved_by_execution_id"] == left[1]["id"]
    assert sum(a["node_id"] == "right" for a in workers) == 1


@pytest.mark.asyncio
async def test_optional_blocked_branch_releases_group(storage, tmp_path):
    graph = explicit()
    next(n for n in graph["nodes"] if n["id"] == "right").update(optional=True, title="right")
    output = {"status": "blocked", "result": {"summary": "compatible harness unavailable", "blocker_category": "availability"}, "evidence": []}
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, policy, {"right": output}))
    assert run["status"] == "completed"
    activation = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "right")
    assert activation["optional_failure"]
    assert not run["joins"]


def test_duplicate_and_late_arrivals_are_idempotent(storage, tmp_path):
    definition = w.validate_definition(explicit())
    run = storage.create_run(definition, "test", tmp_path)
    gid = "generation"
    def seed(r):
        r.update(status="running", pending=[{"id": "first", "node_id": "merge", "stack": [gid], "branch_ids": {gid: "split-left"}, "context": {}}, {"id": "duplicate", "node_id": "merge", "stack": [gid], "branch_ids": {gid: "split-left"}, "context": {}}, {"id": "second", "node_id": "merge", "stack": [gid], "branch_ids": {gid: "split-right"}, "context": {}}])
        r["joins"][gid] = {"join_id": "merge", "split_id": "split", "stack": [], "expected": 2, "arrived": []}
    storage.update_run(run["workflow_run_id"], seed, "seed")
    supervisor = w.WorkflowSupervisor(Registry(storage.root), storage)
    supervisor.run_id = run["workflow_run_id"]
    for tid in ["first", "duplicate", "second"]:
        token = next(t for t in supervisor.run()["pending"] if t["id"] == tid)
        assert supervisor._arrive_join(token)
        if tid == "duplicate":
            assert len(supervisor.run()["joins"][gid]["arrived"]) == 1
    assert len(supervisor.run()["pending"]) == 1
    merged = supervisor.run()["pending"][0]
    assert merged["joined"]
    late = {"id": "late", "node_id": "merge", "stack": [gid], "branch_ids": {gid: "split-right"}, "context": {}}
    supervisor.update(lambda r: r["pending"].append(late), "late")
    assert supervisor._arrive_join(late)
    assert supervisor.run()["pending"] == [merged]


@pytest.mark.asyncio
@pytest.mark.parametrize("category", ["permission", "authority", "uncertain", "missing_context", "unspecified"])
async def test_unsafe_optional_blocker_cannot_bypass(storage, tmp_path, category):
    graph = explicit()
    next(n for n in graph["nodes"] if n["id"] == "right").update(optional=True, title="right")
    output = {"status": "blocked", "result": {"summary": "Must inspect", "blocker_category": category}, "evidence": []}
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, policy, {"right": output}))
    assert run["status"] != "completed"
    activation = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "right")
    assert not activation.get("optional_failure")


@pytest.mark.asyncio
async def test_conditioned_failed_arrival_cannot_be_laundered_by_successor(storage, tmp_path):
    graph = explicit()
    next(n for n in graph["nodes"] if n["id"] == "right").update(title="right", max_attempts=1)
    next(e for e in graph["connections"] if e["source"] == "right")["condition"] = "Continue after failed review"
    graph["nodes"].append({"id": "adjudicate", "type": "agent"})
    next(e for e in graph["connections"] if e["source"] == "merge")["target"] = "adjudicate"
    graph["connections"].append({"id": "adjudicate-end", "source": "adjudicate", "target": "end"})
    output = {"status": "failed", "result": {"summary": "Review execution failed"}, "evidence": []}
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, policy, {"right": output}))
    assert run["status"] == "failed"
    assert not any(a["node_id"] == "adjudicate" and a["role"] == "node" for a in run["activations"])
    assert run["joins"]
    assert any("needs recovery before Parallel end" in c["error"] for c in run["decision_errors"])


@pytest.mark.asyncio
async def test_closed_group_retry_creates_distinct_generations(storage, tmp_path):
    graph = explicit()
    graph["connections"].append({"id": "repeat-group", "source": "merge", "target": "split", "max_retries": 1, "condition": "Repeat entire review"})
    visits = 0
    def repeat_policy(context, registry):
        nonlocal visits
        if context["current_stage"]["node_id"] == "merge":
            visits += 1
            chosen = "repeat-group" if visits == 1 else "merge-end"
            return {"decision_id": context["decision_id"], "action": "continue", "reason": "Explicit closed group retry", "next": [{"continuation_id": chosen}]}
        return policy(context, registry)
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, repeat_policy))
    assert run["status"] == "completed"
    assert len(run["released_parallel_groups"]) == 2
    assert run["retry_counts"]["repeat-group"] == 1
    assert len([a for a in run["activations"] if a["role"] == "node"]) == 6


def test_local_retry_allowed_but_open_group_escape_rejected():
    graph = explicit()
    graph["connections"].append({"id": "retry-local", "source": "left2", "target": "left", "max_retries": 2})
    normalized = w.validate_definition(graph)
    assert next(e for e in normalized["connections"] if e["id"] == "retry-local")["backward"]
    graph["connections"].append({"id": "escape-retry", "source": "left2", "target": "split"})
    with pytest.raises(w.WorkflowError, match="escapes parallel branch"):
        w.validate_definition(graph)


def test_recovered_required_failure_arrival_cannot_release_barrier(storage, tmp_path):
    definition = w.validate_definition(explicit())
    run = storage.create_run(definition, "test", tmp_path)
    token = {"id": "failed-arrival", "node_id": "merge", "stack": ["generation"], "branch_ids": {"generation": "split-right"}, "input_result_refs": ["failed-execution"], "context": {}}
    def seed(r):
        r.update(status="running", pending=[token])
        r["activations"].append({"id": "failed-execution", "role": "node", "node_id": "right", "status": "failed", "tasks": [], "node_result": {"status": "failed", "result": {}, "evidence": []}})
        r["joins"]["generation"] = {"join_id": "merge", "split_id": "split", "stack": [], "expected": 2, "arrived": []}
    storage.update_run(run["workflow_run_id"], seed, "seed")
    supervisor = w.WorkflowSupervisor(Registry(storage.root), storage)
    supervisor.run_id = run["workflow_run_id"]
    assert supervisor._arrive_join(token)
    assert supervisor.run()["status"] == "needs_attention"
    assert supervisor.run()["joins"]["generation"]["arrived"] == []
    assert supervisor.run()["pending"] == [token]


@pytest.mark.asyncio
async def test_entire_fanout_reserved_before_worker_spawn(storage, tmp_path):
    definition = w.validate_definition(explicit())
    run = storage.create_run(definition, "test", tmp_path)
    assignments = {"split-left": {"assignment_prompt": "Left assignment", "execution_session_mode": "fresh"}, "split-right": {"assignment_prompt": "Right assignment", "execution_session_mode": "fresh"}}
    token = {"id": "split-token", "node_id": "split", "stack": [], "selected_connections": list(assignments), "assignments": assignments, "context": {}}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", pending=[token]), "seed")
    registry = Registry(storage.root)
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = run["workflow_run_id"]
    await supervisor._node(token)
    checkpoint = supervisor.run()
    assert not registry.calls
    assert len(checkpoint["pending"]) == 2
    assert len(checkpoint["joins"]) == 1
    group = next(iter(checkpoint["joins"].values()))
    assert group["expected"] == 2
    assert set(group["branch_ids"]) == set(assignments)
    assert {t["assignment_prompt"] for t in checkpoint["pending"]} == {"Left assignment", "Right assignment"}


@pytest.mark.asyncio
async def test_successful_different_node_does_not_resolve_required_failure(storage, tmp_path):
    graph = explicit()
    next(n for n in graph["nodes"] if n["id"] == "left").update(title="left", max_attempts=1)
    next(e for e in graph["connections"] if e["source"] == "left")["condition"] = "Write report even when reviewer failed"
    output = {"status": "failed", "result": {"summary": "Reviewer execution failed"}, "evidence": []}
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, policy, {"left": output}))
    assert run["status"] == "failed"
    first = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "left")
    report = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "left2")
    assert report["node_result"]["status"] == "succeeded"
    assert not first.get("resolved_by_execution_id")
    assert run["joins"]


@pytest.mark.asyncio
async def test_optional_suffix_cannot_erase_inherited_required_failure(storage, tmp_path):
    graph = explicit()
    next(n for n in graph["nodes"] if n["id"] == "left").update(title="left", max_attempts=1)
    next(n for n in graph["nodes"] if n["id"] == "left2").update(title="left2", optional=True)
    next(e for e in graph["connections"] if e["source"] == "left")["condition"] = "Run optional report after failure"
    failed = {"status": "failed", "result": {"summary": "Failed"}, "evidence": []}
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, policy, {"left": failed, "left2": failed}))
    assert run["status"] == "needs_attention"
    required = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "left")
    optional = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "left2")
    assert optional["optional_failure"]
    assert not required.get("resolved_by_execution_id")
    assert run["joins"]
