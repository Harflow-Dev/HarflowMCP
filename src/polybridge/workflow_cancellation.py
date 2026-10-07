"""Human Monitor cancellation is independent of the workflow's interaction owner."""
from __future__ import annotations

from typing import Any

TERMINAL = {"completed", "failed", "cancelled"}
ACTIVE = {"starting", "reserved", "running", "cancelling", "uncertain"}
IDLE = {"paused", "needs_input", "needs_attention", "stuck"}
DESCENDANT_READ_LIMIT = 100
DESCENDANT_METADATA_BYTES = 4 * 1024 * 1024
DESCENDANT_METADATA_BATCH_BYTES = 8 * 1024 * 1024


def eligibility(store: Any, run: dict[str, Any]) -> tuple[bool, str]:
    """Fail closed when a caller-owned tree cannot be proven idle.

    Control evaluates this under the tree mutation lock. Polling is advisory;
    the persisted status and every invocation are read again when cancelling.
    """
    if run.get("parent_link"):
        return False, "Cancel the root workflow to stop this child and its siblings"
    if run.get("status") in TERMINAL:
        return False, "Workflow is terminal"
    if run.get("status") == "cancelling":
        return False, "Cancellation is already in progress"
    if run.get("supervisor_identity"):
        from . import identity
        if identity.identity_check(run["supervisor_identity"]) == "undecidable":
            return False, "Supervisor identity is uncertain; cannot cancel"
    if run.get("interaction_owner") == "monitor":
        return True, ""
    if run.get("status") not in IDLE:
        return False, "Caller-owned workflows can be cancelled only while idle"
    from .bounded_io import ReadLimit
    from .workflow_references import MAX_WORKFLOW_NESTING_DEPTH
    stack, visited = [(run, 1)], set()
    reads, budget = 0, None
    while stack:
        current, depth = stack.pop()
        current_id = current.get("workflow_run_id")
        if current_id in visited:
            return False, "Workflow descendant links are cyclic or ambiguous"
        visited.add(current_id)
        if current.get("settling"):
            return False, "Workflow tasks are still settling"
        for activation in current.get("activations", []):
            if any(t.get("status") in ACTIVE or (t.get("result") or {}).get("outcome_unknown") for t in activation.get("tasks", [])):
                return False, "Workflow has active or unresolved tasks"
            invocation = activation.get("invocation")
            if not invocation:
                if activation.get("status") in ACTIVE:
                    return False, "Workflow execution is still active"
                continue
            if invocation.get("stage") in {"preparing", "created", "running"}:
                return False, "Nested workflow execution is still active"
            child_id = invocation.get("child_workflow_run_id")
            if not child_id:
                return False, "Workflow descendant record is unavailable"
            if depth >= MAX_WORKFLOW_NESTING_DEPTH:
                return False, "Workflow cancellation eligibility exceeds the nesting depth limit"
            if reads >= DESCENDANT_READ_LIMIT:
                return False, "Workflow cancellation eligibility exceeds the descendant count limit"
            try:
                if budget is None:
                    from .catalog import Catalog
                    budget = Catalog(store.runs, ".json")
                    budget.metadata_limit = DESCENDANT_METADATA_BATCH_BYTES
                reads += 1
                child = store.get_run(child_id, metadata_byte_limit=DESCENDANT_METADATA_BYTES, metadata_budget=budget)
                link = child.get("parent_link") or {}
                if child.get("workflow_run_id") != child_id or link.get("workflow_run_id") != current_id or link.get("execution_id") != activation.get("id") or link.get("root_workflow_run_id") != run.get("workflow_run_id"):
                    return False, "Workflow descendant link does not match"
            except ReadLimit:
                return False, "Workflow cancellation eligibility exceeds the bounded metadata budget; inspect its known runs directly"
            except (OSError, ValueError, KeyError, TypeError, AttributeError):
                return False, "Workflow descendant record is unavailable"
            suspended_child = child.get("status") == "running" and child.get("suspended_via_root") is True and invocation.get("stage") == "waiting"
            if child.get("status") not in IDLE | TERMINAL and not suspended_child:
                return False, "Nested workflow is not idle"
            stack.append((child, depth + 1))
    return True, ""


def monitor_projection(store: Any, run: dict[str, Any]) -> dict[str, Any]:
    result = dict(run)
    result["can_cancel_from_monitor"], result["monitor_cancel_reason"] = eligibility(store, run)
    return result


def cascade_error(outcome: Any) -> str:
    """Keep task cancellation transport refusals visible to workflow controls."""
    if not isinstance(outcome, dict):
        return "Cancellation transport returned no verifiable outcome"
    problems = {key: outcome[key] for key in ("sigkill_survivors", "owner_still_settling", "not_signalled", "not_recorded", "unconverged", "cascade_incomplete") if outcome.get(key)}
    if not problems:
        return ""
    import json
    return json.dumps(problems, ensure_ascii=True)[:2000]
