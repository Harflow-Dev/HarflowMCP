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


def _clean_checkpoint(run: dict[str, Any], token: dict[str, Any]) -> bool:
    """Require positive local settlement without bypassing sibling scheduling."""
    if run.get("goal_replanning_required") or token.get("decision_error") or token.get("requires_assignment") or token.get("protocol_repair_of"):
        return False
    if any(token.get(key) for key in ("optional_failure_join", "recovery_return_checkpoint", "recovered_failed_result")):
        return False
    if token.get("decision_activation_id"):
        control = next((a for a in run["activations"] if a["id"] == token["decision_activation_id"]), None)
        if control is None or control.get("decision_disposition") != "accepted":
            return False
    refs = set(token.get("input_result_refs", []) + token.get("failed_execution_refs", []))
    if token.get("execution_activation_id"):
        refs.add(token["execution_activation_id"])
    indexed = {a["id"]: a for a in run["activations"]}
    from .workflow_delegation import settled
    for ref in refs:
        if ref.startswith("invocation:"):
            continue
        activation = indexed.get(ref)
        if activation and activation.get("resolved_by_execution_id"):
            continue
        if not activation or not settled(activation) or activation.get("node_result", {}).get("status") != "succeeded":
            return False
        if activation.get("optional_failure") or activation.get("result_error") or activation.get("pending_question_id") or activation.get("resume_question_id"):
            return False
        if any(q.get("status") not in {"answered", "resolved"} for q in activation.get("questions", [])):
            return False
        if any(task.get("status") in {"reserved", "running", "uncertain", "starting"} or task.get("result", {}).get("outcome_unknown") for task in activation.get("tasks", [])):
            return False
        result = activation["node_result"].get("result", {})
        if result.get("no_checklist_needed") is True or result.get("verdict") == "changes_needed":
            return False
        if activation.get("invocation"):
            outcome = result.get("child_outcome", {})
            if activation["invocation"].get("stage") != "settled" or outcome.get("kind") != "completed" or outcome.get("child_status") != "completed" or outcome.get("permission_evidence") or outcome.get("failure_reason") or outcome.get("child_session_refusal"):
                return False
    return True


def child_start_assignment(run: dict[str, Any], node: dict[str, Any], token: dict[str, Any], *, root: Path | None = None) -> dict[str, Any] | None:
    """Validate a fresh linked child's exact invocation assignment as a handoff."""
    if node["type"] != "start" or not run.get("parent_link") or run.get("orchestrator_mode") not in {"current", "child"}:
        return None
    if node.get("always_route") or run.get("transitions") or run.get("activations") or run.get("decisions") or token.get("stack") or token.get("decision_id"):
        return None
    if run.get("inherited_orchestrator_binding") or run.get("child_session_selection", {}).get("selected_mode") == "resume":
        return None  # The first inherited control turn must verify strict Resume before worker dispatch.
    if run.get("goal") is not None or run.get("parent_goal") or run.get("tasks") or run.get("invocation_artifact_refs") or not _clean_checkpoint(run, token):
        return None
    if any(item.get("descriptors") for item in run.get("invocation_inputs", [])):
        return None
    if not isinstance(run.get("prompt"), str) or not run["prompt"].strip():
        return None
    outgoing = [edge for edge in run["definition"]["connections"] if edge["source"] == node["id"]]
    if len(outgoing) != 1 or outgoing[0].get("condition") or outgoing[0].get("backward"):
        return None
    target = next(n for n in run["definition"]["nodes"] if n["id"] == outgoing[0]["target"])
    if target["type"] != "agent" or target.get("role") == "planning" or target.get("always_route"):
        return None
    if run.get("orchestrator_mode") == "child" and target.get("execution_mode") == "prefer_subagent":
        return None  # A fresh Child owner must establish its session before native worker preparation.
    from .workflow_delegation import continuations, validate_decision
    checkpoint = {**token, "decision_id": "runner:" + token["id"]}
    choices = continuations(run, node, checkpoint, False, root=root)
    if len(choices) != 1 or not choices[0].get("requires_prompt") or choices[0].get("available_sessions"):
        return None
    # No retained source exists in a newly published child. Let the ordinary
    # validator enforce fixed Fresh/Resume bootstrap and continue_previous rules.
    entry = {"continuation_id": choices[0]["continuation_id"], "prompt": run["prompt"]}
    if target.get("session_mode") == "agent_decides":
        entry["session_mode"] = "fresh"
    decision = {"decision_id": checkpoint["decision_id"], "action": "continue", "reason": "Harflow passed the invocation assignment verbatim to the sole child entry", "next": [entry]}
    try:
        edges, assignments, _ = validate_decision(run, node, checkpoint, decision, False, root=root)
    except (ValueError, KeyError):
        return None
    return {"connections": edges, "assignments": assignments, "reason": decision["reason"]}


def automatic_transition(run: dict[str, Any], node: dict[str, Any], token: dict[str, Any], *, root: Path | None = None) -> dict[str, Any] | None:
    """Offer only deterministic structural movement; executable routing stays owned."""
    if run.get("runner_policy") != "guided" or node.get("always_route"):
        return None
    child = child_start_assignment(run, node, token, root=root)
    if child is not None:
        return child
    worker_end = node["type"] in {"agent", "workflow"} and token.get("execution_complete") and token.get("result", {}).get("status") == "succeeded"
    if node["type"] not in {"parallel_end", "join"} and not worker_end:
        return None
    if not _clean_checkpoint(run, token):
        return None
    if worker_end and not token.get("execution_activation_id"):
        return None
    outgoing = [edge for edge in run["definition"]["connections"] if edge["source"] == node["id"]]
    if len(outgoing) != 1 or outgoing[0].get("condition") or outgoing[0].get("backward"):
        return None
    from .workflow_delegation import continuations
    choices = continuations(run, node, token, False, root=root)
    if len(choices) != 1 or choices[0]["requires_prompt"] or not choices[0].get("connection"):
        return None
    target = next(n for n in run["definition"]["nodes"] if n["id"] == choices[0]["node_id"])
    if target["type"] not in {"end", "parallel_end", "join"}:
        return None
    if target["type"] in {"parallel_end", "join"}:
        stack = token.get("stack", [])
        group = run.get("joins", {}).get(stack[-1]) if stack else None
        if not group or group.get("join_id") != target["id"]:
            return None
        selected = group.get("selected_connection_ids", group.get("branch_ids"))
        if selected is not None and token.get("branch_ids", {}).get(stack[-1]) not in selected:
            return None
    elif worker_end and token.get("stack"):
        return None  # A branch cannot escape its active join directly into End.
    return {"connections": [choices[0]["connection"]], "assignments": {},
            "reason": "Harflow advanced clean settled evidence along the sole unconditional structural path"}


def automatic_edges(run: dict[str, Any], node: dict[str, Any], token: dict[str, Any], *, root: Path | None = None) -> list[dict[str, Any]] | None:
    transition = automatic_transition(run, node, token, root=root)
    return transition["connections"] if transition is not None else None
