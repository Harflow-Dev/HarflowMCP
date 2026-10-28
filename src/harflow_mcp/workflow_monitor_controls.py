"""Advisory human controls; every mutation rechecks current engine authority."""
from __future__ import annotations

from typing import Any

ACTIONS = ("pause", "resume", "recover", "revise_goal", "skip_optional_review", "additional_attempts", "cancel")


def _continuation_reason(storage: Any, run: dict[str, Any]) -> str:
    from . import control, identity
    if run.get("settling"):
        return "Workflow tasks are still settling"
    if run.get("supervisor_identity") and identity.identity_check(run["supervisor_identity"]) == "undecidable":
        return "Supervisor identity is uncertain; cannot resume or recover"
    if any((a.get("status") in {"reserved", "uncertain", "running"} and not a.get("invocation"))
           or any(t.get("status") in {"reserved", "uncertain", "running"} for t in a.get("tasks", []))
           for a in run.get("activations", [])):
        return "Unresolved dispatches must be reconciled before resume"
    if run.get("status") in {"paused", "needs_input", "needs_attention", "failed"}:
        reservations = control.takeover_reservations(storage.root / "tasks")
        if any(t.get("task_id") in reservations for a in run.get("activations", []) for t in a.get("tasks", [])):
            return "Workflow sessions are reserved for human takeover; cannot resume or recover"
    return ""


def _scheduler_recovery_reason(storage: Any, run: dict[str, Any]) -> str:
    from types import SimpleNamespace
    from . import identity
    from .catalog import Catalog
    from .workflow_recovery import validate
    from .workflows import WorkflowError
    if not run.get("supervisor_identity") or identity.identity_check(run["supervisor_identity"]) != "dead":
        return "Workflow scheduler is live or uncertain; confirmed owner death is required for recovery"
    # Reuse the actual recovery qualification, with bounded descendant reads.
    budget = Catalog(storage.runs, ".json", layout=storage.state_layout)
    def read(identifier, **kwargs):
        return storage.get_run(identifier, **kwargs, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=budget)
    try:
        validate(SimpleNamespace(root=storage.root, get_run=read), run)
    except WorkflowError as exc:
        return str(exc)
    return ""


def projection(storage: Any, run: dict[str, Any]) -> dict[str, Any]:
    """Keep unavailable controls discoverable without conferring authority.

    Children name their root route, but require navigation to its current
    checkpoint so a child's answer cannot accidentally answer a root question.
    This projection never performs a handover or writes workflow state.
    """
    from .workflow_interactions import owner_state
    from .workflow_cancellation import eligibility
    from .workflow_goals import revision_eligibility
    target = (run.get("parent_link") or {}).get("root_workflow_run_id") or run["workflow_run_id"]
    controls = {action: {"allowed": False, "reason": "", "target_workflow_run_id": target} for action in ACTIONS}

    def set_action(action: str, reason: str = "") -> None:
        controls[action].update(allowed=not bool(reason), reason=reason)

    if run.get("parent_link"):
        for action in ACTIONS:
            set_action(action, "Open the root workflow to inspect its current checkpoint and control this child")
        return controls

    status = run.get("status")
    monitor_owned = run.get("interaction_owner", "caller") == "monitor"
    state, owner_reason = owner_state(run, storage)
    ownership_reason = "" if monitor_owned or state == "dead" else (
        "Original caller is alive" if state == "alive" else "Original caller ownership is uncertain: " + owner_reason)
    delegation_reason = "" if run.get("execution_contract") == "delegation" and run.get("kind") != "builder" else "This control requires a delegation workflow"
    common = delegation_reason or ownership_reason or _continuation_reason(storage, run)
    set_action("resume", common or ("Only paused/attention/input workflows can resume" if status not in {"paused", "needs_input", "needs_attention"} else ""))
    set_action("recover", common or ("Only failed delegation workflows can recover" if status != "failed" else ""))
    control_run = run
    source_id = (run.get("input_source") or {}).get("workflow_run_id")
    if status == "needs_input" and source_id not in (None, target) and run.get("input_decision_id"):
        # The engine routes an exact descendant answer before whole-root resume
        # gates. Unrelated live siblings must not disable that checkpoint.
        from .workflow_inputs import project
        cached: dict[str, Any] = {}
        pending = project(storage, run, _cached=cached)["pending_inputs"]
        control_run = cached[target]
        selected = next((item for item in pending if item["workflow_run_id"] == source_id
                         and item["decision_id"] == run["input_decision_id"]), None)
        source = cached.get(source_id)
        reason = "The selected child checkpoint changed; refresh the root workflow"
        if selected and source and control_run.get("status") not in {"completed", "failed", "cancelled", "cancelling"}:
            reason = _continuation_reason(storage, source)
        set_action("resume", delegation_reason or ownership_reason or reason)
    scheduler_recovery = control_run.get("status") in {"starting", "running", "cancelling"}
    if scheduler_recovery:
        set_action("recover", delegation_reason or ownership_reason or _scheduler_recovery_reason(storage, control_run))
    # Pause does not claim caller authority: only resume/recover/goal revision
    # (and explicit messaging) support the qualified dead-caller handover.
    set_action("pause", ("Workflow interaction belongs to its original caller" if not monitor_owned else "")
               or ("Workflow is not running" if status not in {"starting", "running"} else ""))
    allowed, reason = eligibility(storage, run)
    set_action("cancel", "" if allowed else reason)
    allowed, reason = revision_eligibility(storage, run)
    set_action("revise_goal", ownership_reason or ("" if allowed else reason))
    continuation = controls["recover" if status == "failed" else "resume"]
    set_action("additional_attempts", continuation["reason"])
    if scheduler_recovery and status in {"starting", "running", "cancelling"}:
        set_action("additional_attempts", "Scheduler recovery cannot grant additional attempts")
    set_action("skip_optional_review", controls["resume"]["reason"]
               or ("No optional review consent is available at this checkpoint" if not (
                   status == "needs_input" and run.get("optional_review_skip_available") is True
                   and run.get("input_decision_id")) else ""))
    return controls
