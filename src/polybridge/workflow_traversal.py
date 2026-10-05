"""Assignment-preserving structural traversal for guided workflow runs."""
from __future__ import annotations

import copy
from pathlib import Path
from typing import Any


def branch_choices(run: dict[str, Any], choice: dict[str, Any], token: dict[str, Any], *, root: Path | None = None) -> list[dict[str, Any]] | None:
    if run.get("runner_policy") != "guided" or run["definition"].get("routing_mode") != "explicit":
        return None
    split = next(n for n in run["definition"]["nodes"] if n["id"] == choice["node_id"])
    if split["type"] != "parallel_start":
        return None
    from .workflow_delegation import continuations
    # The incoming result becomes each branch's predecessor. No agent is run here.
    branch_token = {**token, "execution_complete": False}
    for key in ("result", "execution_activation_id"):
        branch_token.pop(key, None)
    return continuations(run, split, branch_token, False, root=root)


def validate_branch_assignments(run: dict[str, Any], choice: dict[str, Any], token: dict[str, Any], entry: dict[str, Any], *, root: Path | None = None) -> dict[str, Any] | None:
    choices = branch_choices(run, choice, token, root=root)
    if choices is None:
        if "branch_assignments" in entry or "selection_reason" in entry:
            from .workflows import WorkflowError
            raise WorkflowError("branch_assignments requires an issued Parallel start continuation")
        return None
    from .workflow_delegation import validate_decision
    split = next(n for n in run["definition"]["nodes"] if n["id"] == choice["node_id"])
    selectable = split.get("branch_selection", "all") == "orchestrator"
    reason = entry.get("selection_reason", "All configured branches apply")
    if selectable and (not isinstance(reason, str) or not reason.strip() or "selection_reason" not in entry):
        from .workflows import WorkflowError
        raise WorkflowError("Selectable parallel branches require a nonempty selection_reason explaining selection and exclusions")
    if not selectable and "selection_reason" in entry:
        from .workflows import WorkflowError
        raise WorkflowError("selection_reason requires an Orchestrator selects Parallel start")
    branch_token = {**token, "node_id": split["id"], "execution_complete": False}
    # The split itself has no execution. Prevent failed predecessor filtering from
    # converting a mandatory split into partial dispatch.
    branch_token.pop("result", None)
    branch_token.pop("execution_activation_id", None)
    decision = {"decision_id": token["decision_id"], "action": "continue", "reason": reason, "next": entry.get("branch_assignments", [])}
    selected, assignments, _ = validate_decision(run, split, branch_token, decision, False, root=root)
    from .workflows import WorkflowError
    transition_cost = len(selected) + sum(a.get("structural_dispatch", {}).get("structural_transition_cost", 0) for a in assignments.values())
    if run["transitions"] + 1 + transition_cost > run["definition"]["max_transitions"] + run.get("transition_grant", 0):
        raise WorkflowError("Transition limit reached for selected parallel group")
    return {"selected_connections": [edge["id"] for edge in selected], "assignments": copy.deepcopy(assignments), "structural_transition_cost": transition_cost, "selection_reason": reason, "selection_decision_id": token["decision_id"]}


def automatic_edges(run: dict[str, Any], node: dict[str, Any], token: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Skip only a structural, unconditional, single path with no assignment."""
    if run.get("runner_policy") != "guided":
        return None
    worker_end = node["type"] == "agent" and token.get("execution_complete") and token.get("result", {}).get("status") == "succeeded"
    if node.get("role") == "planning" and token.get("result", {}).get("result", {}).get("no_checklist_needed") is True:
        return None  # Accepting a no-checklist proposal is orchestrator judgment.
    if node["type"] not in {"parallel_end", "join"} and not worker_end:
        return None
    if worker_end:
        refs = set(token.get("failed_execution_refs", []))
        if any(a["id"] in refs and not a.get("resolved_by_execution_id") and not a.get("optional_failure") for a in run["activations"]):
            return None
    from .workflow_delegation import continuations
    choices = continuations(run, node, token, False)
    if len(choices) != 1 or choices[0]["requires_prompt"] or choices[0]["connection"].get("condition") or choices[0]["connection"].get("backward"):
        return None
    if worker_end and next(n for n in run["definition"]["nodes"] if n["id"] == choices[0]["node_id"])["type"] != "end":
        return None
    # A split needs branch assignments even though it has no worker itself.
    if branch_choices(run, choices[0], token) is not None:
        return None
    return [choices[0]["connection"]]
