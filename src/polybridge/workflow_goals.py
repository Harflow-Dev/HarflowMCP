"""Caller-owned run goals; evidence judgments remain separate from harness status."""
from __future__ import annotations

import copy
import time
from typing import Any

MAX_GOAL_BYTES = 64 * 1024
MAX_GOAL_REVISIONS = 64
MAX_GOAL_HISTORY_BYTES = 1024 * 1024


def _history_budget(run: dict[str, Any], additional_goal: str | None = None) -> tuple[bool, str]:
    history = run.get("goal_history", [])
    # Count the revision too, so recovered records with incomplete history cannot
    # regain an exhausted budget. Existing oversized records remain readable.
    if max(len(history), run.get("goal_revision", 0)) >= MAX_GOAL_REVISIONS:
        return False, "Goal history reached 64 revisions; start a new run"
    size = sum(len(item["goal"].encode("utf-8")) for item in history)
    if size >= MAX_GOAL_HISTORY_BYTES:
        return False, "Goal history reached 1 MiB UTF-8; start a new run"
    if additional_goal is not None and size + len(additional_goal.encode("utf-8")) > MAX_GOAL_HISTORY_BYTES:
        return False, "Proposed goal exceeds the remaining 1 MiB UTF-8 history budget; use a smaller goal or start a new run"
    return True, ""


def validate_goal(goal: str | None) -> str | None:
    from .workflows import WorkflowError
    if goal is not None and (not isinstance(goal, str) or not goal.strip()):
        raise WorkflowError("A supplied goal must be nonblank text")
    if goal is not None:
        try:
            size = len(goal.encode("utf-8"))
        except UnicodeEncodeError:
            raise WorkflowError("Goal must be valid UTF-8 text") from None
        if size > MAX_GOAL_BYTES:
            raise WorkflowError("Goal exceeds 64 KiB UTF-8")
    return goal


def initialize(run: dict[str, Any], goal: str | None) -> None:
    validate_goal(goal)
    if goal is not None:
        run.update(goal=goal, goal_revision=1, goal_history=[{"revision": 1, "goal": goal, "created_at": time.time()}], goal_assessment=None)


def context(run: dict[str, Any], *, root: Any = None) -> dict[str, Any]:
    fields = {key: copy.deepcopy(run.get(key)) for key in ("goal", "goal_revision", "goal_assessment", "goal_replan", "goal_replanning_required", "goal_replanning_revision") if key in run}
    parent = copy.deepcopy(run.get("parent_goal"))
    if run.get("parent_link") and root is not None:
        from .workflows import WorkflowStore
        owner_id = run["parent_link"]["root_workflow_run_id"]
        owner = WorkflowStore(root).get_run(owner_id)
        if owner.get("goal") is not None:
            parent = {"workflow_run_id": owner_id, "goal": owner["goal"], "revision": owner["goal_revision"]}
    if parent:
        fields["parent_goal"] = parent
    return fields


GUIDANCE = " Saved purpose, caller request and run goal are complementary constraints; conflicts require focused caller clarification. Goals never expand permissions. After revision, before continuation or completion include goal_replan {revision: current goal_replanning_revision (or goal_revision), plan: nonempty revised implementation plan preserving completed work}. For design comparisons, planning must identify supplied references, screens, states and verification criteria; ask for missing references. At End with a goal, include goal_assessment {revision: current goal_revision, status: satisfied|unmet|unverifiable, reason: nonempty explanation, evidence_refs: [issued settled execution IDs]}. Satisfied requires evidence from successful settled executions; observations alone do not prove requested work is complete."


def validate_assessment(run: dict[str, Any], assessment: Any) -> dict[str, Any]:
    from .workflows import WorkflowError
    if not isinstance(assessment, dict) or set(assessment) != {"revision", "status", "reason", "evidence_refs"}:
        raise WorkflowError("Completion requires an explicit goal assessment")
    if type(assessment["revision"]) is not int or assessment["revision"] != run.get("goal_revision"):
        raise WorkflowError("Goal assessment revision is stale")
    if assessment["status"] not in {"satisfied", "unmet", "unverifiable"} or not isinstance(assessment["reason"], str) or not assessment["reason"].strip():
        raise WorkflowError("Invalid goal assessment status or reason")
    from .workflow_delegation import settled
    refs = assessment["evidence_refs"]
    known = {a["id"]: a for a in run.get("activations", []) if a.get("role") == "node" and a.get("node_result") and settled(a)}
    if not isinstance(refs, list) or any(not isinstance(ref, str) or ref not in known for ref in refs):
        raise WorkflowError("Goal evidence must reference settled execution IDs")
    if assessment["status"] == "satisfied" and (not refs or not any(known[ref]["node_result"].get("status") == "succeeded" and bool(known[ref]["node_result"].get("evidence")) for ref in refs)):
        raise WorkflowError("Satisfied goal requires successful execution evidence")
    return copy.deepcopy(assessment)


def completion_allowed(run: dict[str, Any]) -> bool:
    if run.get("goal_replanning_required"):
        return False
    if run.get("goal") is None:
        return True
    assessment = run.get("goal_assessment") or {}
    try:
        validate_assessment(run, assessment)
    except ValueError:
        return False
    return assessment.get("status") == "satisfied" and not run.get("goal_replanning_required")


def revise(store: Any, run_id: str, goal: str, expected_revision: int, *, interaction_owner: str = "caller") -> dict[str, Any]:
    from .workflows import WorkflowError
    from .workflow_cancellation import eligibility
    validate_goal(goal)
    if goal is None or type(expected_revision) is not int or expected_revision < 0:
        raise WorkflowError("Goal and a nonnegative expected revision are required")
    def change(run: dict[str, Any]) -> None:
        if run.get("kind") != "workflow" or run.get("parent_link"):
            raise WorkflowError("Revise the root workflow goal")
        if run.get("interaction_owner", "caller") != interaction_owner:
            raise WorkflowError("Workflow interaction belongs to its original caller")
        if run.get("status") not in {"paused", "needs_input"}:
            raise WorkflowError("Goal revision requires a paused or awaiting-input checkpoint")
        if run.get("goal_revision", 0) != expected_revision:
            raise WorkflowError("Goal revision is stale; re-read the run")
        allowed, reason = eligibility(store, {**run, "interaction_owner": "caller"})
        if not allowed:
            raise WorkflowError(reason)
        allowed, reason = _history_budget(run, goal)
        if not allowed:
            raise WorkflowError(reason)
        revision = expected_revision + 1
        run.update(goal=goal, goal_revision=revision, goal_assessment=None, goal_replanning_required=True)
        run.setdefault("goal_history", []).append({"revision": revision, "goal": goal, "created_at": time.time()})
        # Reissue every pending routing checkpoint; retained assignments cannot bypass replanning.
        for token in run.get("pending", []):
            for key in ("selected_connections", "assignments", "accepted_decision_id", "decision_id", "decision_attempts", "decision_error", "assignment_prompt"):
                token.pop(key, None)
            token["requires_assignment"] = True
        run["instructions"] = run.get("instructions", "") + "\nThe caller revised the goal. Replan against the current goal while retaining completed work and its evidence."
        run["sessions"] = {}
    return store.update_run(run_id, change, "goal_revised", {"expected_revision": expected_revision})


def revision_eligibility(store: Any, run: dict[str, Any]) -> tuple[bool, str]:
    from .workflow_cancellation import eligibility
    if run.get("kind") != "workflow" or run.get("parent_link"):
        return False, "Revise the root workflow goal"
    if run.get("status") not in {"paused", "needs_input"}:
        return False, "Goal revision requires a paused or awaiting-input checkpoint"
    allowed, reason = _history_budget(run)
    if not allowed:
        return False, reason
    return eligibility(store, {**run, "interaction_owner": "caller"})


def refresh_parent(store: Any, run: dict[str, Any]) -> dict[str, Any]:
    """Refresh inherited constraints before a suspended child's next scheduling step.

    Completed work and the scoped child request remain intact. Synchronization uses
    the same tree lock as dispatch reservations and goal revision.
    """
    if not run.get("parent_link"):
        return run
    constraints = context(run, root=store.root).get("parent_goal")
    if not constraints or constraints == run.get("parent_goal"):
        return run
    def change(current: dict[str, Any]) -> None:
        updated = context(current, root=store.root).get("parent_goal")
        if updated == current.get("parent_goal"):
            return
        current.update(parent_goal=updated, goal_replanning_required=True, goal_replanning_revision=updated["revision"], goal_assessment=None)
        for token in current.get("pending", []):
            for key in ("selected_connections", "assignments", "accepted_decision_id", "decision_id", "decision_attempts", "decision_error", "assignment_prompt"):
                token.pop(key, None)
            token["requires_assignment"] = True
        current["sessions"] = {}
    return store.update_run(run["workflow_run_id"], change, "parent_goal_refreshed")
