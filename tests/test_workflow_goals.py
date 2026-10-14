"""Goals are caller data and versioned completion judgments, never permissions."""
import copy

import pytest

from polybridge import workflows as w
from polybridge import workflow_goals as g
from polybridge import workflow_delegation as d
from polybridge import workflow_responses as responses
from test_workflow_delegation import graph, Registry, default_decision


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    return w.WorkflowStore(tmp_path)


def create(storage, tmp_path, goal="Finish with observed evidence"):
    return storage.create_run(w.validate_definition(graph()), "request", tmp_path, goal=goal)


@pytest.mark.parametrize("goal", ["", " \n", "é" * (32 * 1024 + 1), False])
def test_invalid_goals(storage, tmp_path, goal):
    with pytest.raises(w.WorkflowError):
        create(storage, tmp_path, goal)


def test_verbatim_and_legacy(storage, tmp_path):
    run = create(storage, tmp_path, "  Goal\nwith spaces  ")
    assert storage.get_run(run["workflow_run_id"])["goal"] == "  Goal\nwith spaces  "
    assert run["goal_history"][0]["goal"] == run["goal"]
    assert "goal" not in create(storage, tmp_path, None)


def test_revision_preserves_evidence_and_invalidates_decisions(storage, tmp_path):
    run = create(storage, tmp_path)
    rid = run["workflow_run_id"]
    storage.update_run(rid, lambda r: r.update(status="paused", pending=[{"id": "t", "node_id": "work", "decision_id": "stale", "assignments": {"old": {}}, "execution_complete": True}], tasks=[{"id": "done", "status": "completed"}], goal_assessment={"status": "satisfied"}), "test")
    revised = g.revise(storage, rid, "  new goal\n", 1)
    assert revised["goal_revision"] == 2 and len(revised["goal_history"]) == 2
    assert revised["tasks"][0]["status"] == "completed"
    assert revised["pending"][0]["execution_complete"] is True
    assert "decision_id" not in revised["pending"][0]
    assert revised["goal_assessment"] is None and revised["goal_replanning_required"]
    with pytest.raises(w.WorkflowError, match="stale"):
        g.revise(storage, rid, "other", 1)


@pytest.mark.parametrize("instructions", [None, "", "  Keep these spaces\r\nand this newline\n  "])
def test_revision_keeps_caller_instructions_separate_from_replan(storage, tmp_path, instructions):
    run = create(storage, tmp_path)
    rid = run["workflow_run_id"]
    def prepare(current):
        current.update(status="paused", tasks=[{"id": "done", "status": "completed"}])
        if instructions is None:
            current.pop("instructions", None)
        else:
            current["instructions"] = instructions
    storage.update_run(rid, prepare, "test")
    g.revise(storage, rid, "new goal", 1)
    reloaded_store = w.WorkflowStore(storage.root)
    g.revise(reloaded_store, rid, "second goal", 2)
    revised = reloaded_store.get_run(rid)
    if instructions is None:
        assert "instructions" not in revised
    else:
        assert revised["instructions"].encode("utf-8") == instructions.encode("utf-8")
    assert revised["tasks"] == [{"id": "done", "status": "completed"}]
    node = next(node for node in revised["definition"]["nodes"] if node["type"] == "start")
    token = {"id": "test-token", "node_id": node["id"], "decision_id": "issued-test", "context": {}}
    context = d.decision_context(revised, node, token, False, root=storage.root)
    assert context["recovery_instructions"] == (instructions or "")
    assert context["goal_replanning_required"] is True
    assert context["goal_revision"] == 3
    assert "before continuation or completion include goal_replan" in d.decision_prompt(context)


@pytest.mark.parametrize("status", ["running", "completed", "failed", "needs_attention"])
def test_revision_rejects_unsafe_checkpoint(storage, tmp_path, status):
    run = create(storage, tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status=status), "test")
    with pytest.raises(w.WorkflowError):
        g.revise(storage, run["workflow_run_id"], "changed", 1)


def test_revision_rejects_active_even_monitor(storage, tmp_path):
    run = create(storage, tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="paused", interaction_owner="monitor", activations=[{"id": "a", "status": "running", "tasks": []}]), "test")
    with pytest.raises(w.WorkflowError, match="active"):
        g.revise(storage, run["workflow_run_id"], "changed", 1, interaction_owner="monitor")


@pytest.mark.parametrize("assessment_status, expected", [("satisfied", "completed"), ("unmet", "needs_input"), ("unverifiable", "needs_input")])
async def test_completion_assessment(storage, tmp_path, assessment_status, expected):
    run = create(storage, tmp_path)
    def policy(context, registry):
        decision = default_decision(context, registry)
        if decision["action"] == "complete":
            decision["goal_assessment"] = {"revision": context["goal_revision"], "status": assessment_status, "reason": "Observed verification", "evidence_refs": [item["execution_id"] for item in context["settled_executions"]]}
        return decision
    registry = Registry(storage.root, policy=policy)
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    result = storage.get_run(run["workflow_run_id"])
    assert result["status"] == expected
    assert result["goal_assessment"]["status"] == assessment_status
    assert all(c["goal"] == run["goal"] for c in registry.contexts)
    assert any("Goal constraints:" in prompt for prompt, kwargs in registry.calls)


def test_assessment_requires_issued_evidence():
    run = {"goal": "x", "goal_revision": 2, "activations": []}
    assessment = {"revision": 2, "status": "satisfied", "reason": "done", "evidence_refs": []}
    with pytest.raises(w.WorkflowError, match="evidence"):
        g.validate_assessment(run, assessment)
    assessment["revision"] = 1
    with pytest.raises(w.WorkflowError, match="stale"):
        g.validate_assessment(run, assessment)
    assert not g.completion_allowed(run)
    assert g.completion_allowed({})


def test_large_goal_is_paged(storage, tmp_path):
    run = create(storage, tmp_path, "g" * (64 * 1024))
    compact = responses.monitor(run)
    assert "goal" not in compact
    assert compact["goal_revision"] == 1
    assert "goal" in compact["monitor_digests"]
    assert responses._view_value(run, "goal") == run["goal"]


def test_nested_tree_gate_and_constraint_refresh(storage, tmp_path):
    parent = create(storage, tmp_path)
    child = create(storage, tmp_path, None)
    pid, cid = parent["workflow_run_id"], child["workflow_run_id"]
    storage.update_run(cid, lambda r: r.update(status="paused", parent_link={"workflow_run_id": pid, "root_workflow_run_id": pid, "execution_id": "nested"}, parent_goal={"workflow_run_id": pid, "goal": parent["goal"], "revision": 1}, pending=[{"id": "child-token", "node_id": "work", "assignment_prompt": "Old scoped assignment", "decision_id": "old"}], activations=[{"id": "child-work", "role": "node", "status": "running", "tasks": [{"task_id": "active", "status": "running"}]}]), "test")
    storage.update_run(pid, lambda r: r.update(status="paused", activations=[{"id": "nested", "status": "waiting", "tasks": [], "invocation": {"stage": "waiting", "child_workflow_run_id": cid}}]), "test")
    with pytest.raises(w.WorkflowError, match="active|settling"):
        g.revise(storage, pid, "new parent constraint", 1)
    storage.update_run(cid, lambda r: r.update(activations=[]), "test_settled")
    g.revise(storage, pid, "new parent constraint", 1)
    updated = g.refresh_parent(storage, storage.get_run(cid))
    assert updated["prompt"] == "request"
    assert updated["parent_goal"]["goal"] == "new parent constraint"
    assert updated["parent_goal"]["revision"] == 2
    assert updated["goal_replanning_required"]
    assert updated["goal_replanning_revision"] == 2
    assert "decision_id" not in updated["pending"][0]
    assert "assignment_prompt" not in updated["pending"][0]


def test_revision_compare_and_swap(storage, tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    run = create(storage, tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="paused"), "test")
    def attempt(goal):
        try:
            return g.revise(storage, run["workflow_run_id"], goal, 1)["goal"]
        except w.WorkflowError:
            return None
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, ["first", "second"]))
    assert len([r for r in results if r is not None]) == 1
    assert storage.get_run(run["workflow_run_id"])["goal_revision"] == 2


async def test_revised_goal_requires_explicit_replan_before_dispatch(storage, tmp_path):
    run = create(storage, tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="paused"), "test")
    g.revise(storage, run["workflow_run_id"], "revised", 1)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running"), "test_resume")
    registry = Registry(storage.root)  # Intentionally omits required replan.
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    result = storage.get_run(run["workflow_run_id"])
    assert result["status"] == "needs_attention"
    assert not any("Goal constraints:" in prompt for prompt, kwargs in registry.calls)
    assert result["goal_replanning_required"]


async def test_missing_goal_assessment_cannot_complete(storage, tmp_path):
    run = create(storage, tmp_path)
    registry = Registry(storage.root)
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    result = storage.get_run(run["workflow_run_id"])
    assert result["status"] == "needs_attention"
    assert result["goal_assessment"] is None


@pytest.mark.parametrize("optimized", [False, True])
async def test_revised_goal_replan_and_complete(storage, tmp_path, optimized):
    definition = graph()
    if optimized:
        definition["context_delivery"] = "optimized_v1"
    run = storage.create_run(w.validate_definition(definition), "request", tmp_path, goal="original")
    rid = run["workflow_run_id"]
    storage.update_run(rid, lambda r: r.update(status="paused"), "test")
    g.revise(storage, rid, " revised\nverbatim ", 1)
    storage.update_run(rid, lambda r: r.update(status="running"), "test_resume")
    def policy(context, registry):
        decision = default_decision(context, registry)
        if context.get("goal_replanning_required"):
            decision["goal_replan"] = {"revision": 2, "plan": "Keep completed work and implement the revised verification."}
        if decision["action"] == "complete":
            decision["goal_assessment"] = {"revision": 2, "status": "satisfied", "reason": "Observed checks satisfy revised goal", "evidence_refs": [a["execution_id"] for a in context["settled_executions"]]}
        return decision
    registry = Registry(storage.root, policy=policy)
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    result = storage.get_run(rid)
    assert result["status"] == "completed"
    assert result["goal_replan"]["revision"] == 2
    assert result["goal_assessment"]["revision"] == 2
    assert all(c["goal"] == " revised\nverbatim " for c in registry.contexts)
    assert not result.get("goal_replanning_required")
    assert all("revised" in prompt for prompt, kwargs in registry.calls)


def test_unsettled_evidence_is_not_issued():
    run = {"goal_revision": 1, "activations": [{"id": "a", "role": "node", "status": "completed", "tasks": [{"status": "running"}], "node_result": {"status": "succeeded", "evidence": ["claimed"]}}]}
    with pytest.raises(w.WorkflowError, match="settled"):
        g.validate_assessment(run, {"revision": 1, "status": "satisfied", "reason": "done", "evidence_refs": ["a"]})


def seed_history(storage, tmp_path, goals):
    run = create(storage, tmp_path, goals[0])
    return storage.update_run(run["workflow_run_id"], lambda r: r.update(
        status="paused", goal=goals[-1], goal_revision=len(goals),
        goal_history=[{"revision": i + 1, "goal": goal, "created_at": i} for i, goal in enumerate(goals)],
        goal_assessment={"revision": len(goals), "status": "unmet"}, instructions="Retain caller prose",
    ), "test")


@pytest.mark.parametrize("goals, proposed", [
    (["x"] * g.MAX_GOAL_REVISIONS, "new"),
    (["é" * (g.MAX_GOAL_BYTES // 2)] * 16, "new"),
    (["x" * g.MAX_GOAL_BYTES] * 15 + ["é" * (g.MAX_GOAL_BYTES // 2 - 1)], "éx"),
])
def test_history_budget_rejection_is_atomic(storage, tmp_path, goals, proposed):
    run = seed_history(storage, tmp_path, goals)
    before = storage.get_run(run["workflow_run_id"])
    with pytest.raises(w.WorkflowError, match="history|revisions"):
        g.revise(storage, run["workflow_run_id"], proposed, len(goals))
    assert storage.get_run(run["workflow_run_id"]) == before


def test_exact_multibyte_history_limit(storage, tmp_path):
    goals = ["x" * g.MAX_GOAL_BYTES] * 15 + ["é" * (g.MAX_GOAL_BYTES // 2 - 1)]
    run = seed_history(storage, tmp_path, goals)
    assert g.revision_eligibility(storage, run)[0]
    revised = g.revise(storage, run["workflow_run_id"], "é", 16)
    assert sum(len(item["goal"].encode("utf-8")) for item in revised["goal_history"]) == g.MAX_GOAL_HISTORY_BYTES
    allowed, reason = g.revision_eligibility(storage, revised)
    assert not allowed and "1 MiB" in reason and "new run" in reason


def test_concurrent_last_revision_and_stale_edit(storage, tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    run = seed_history(storage, tmp_path, ["x"] * (g.MAX_GOAL_REVISIONS - 1))
    def attempt(goal):
        try:
            return g.revise(storage, run["workflow_run_id"], goal, 63)
        except w.WorkflowError as error:
            return str(error)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, ["first", "second"]))
    assert len([result for result in results if isinstance(result, dict)]) == 1
    assert any("stale" in result for result in results if isinstance(result, str))
    recovered = w.WorkflowStore(storage.root).get_run(run["workflow_run_id"])
    assert recovered["goal_revision"] == len(recovered["goal_history"]) == 64
    assert g.revision_eligibility(storage, recovered) == (False, "Goal history reached 64 revisions; start a new run")


@pytest.mark.parametrize("goals", [["x"] * 65, ["x" * g.MAX_GOAL_BYTES] * 17])
def test_recovered_overlimit_goals_remain_readable(storage, tmp_path, goals):
    run = seed_history(storage, tmp_path, goals)
    recovered_store = w.WorkflowStore(storage.root)
    recovered = recovered_store.get_run(run["workflow_run_id"])
    assert recovered["goal_history"] == run["goal_history"]
    assert not g.revision_eligibility(recovered_store, recovered)[0]
    with pytest.raises(w.WorkflowError, match="new run"):
        g.revise(recovered_store, run["workflow_run_id"], "new", len(goals))
    assert recovered_store.get_run(run["workflow_run_id"]) == recovered
