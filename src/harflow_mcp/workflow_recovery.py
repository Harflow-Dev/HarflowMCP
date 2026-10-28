"""Explicit recovery of a positively identified interrupted scheduler tree."""
from __future__ import annotations

import time
from typing import Any

ACTIVE = {"reserved", "running", "uncertain"}


def inventory(storage: Any, root: dict[str, Any]) -> list[dict[str, Any]]:
    """Follow only reciprocal durable invocation links; never infer descendants."""
    from .workflows import WorkflowError
    result, seen, pending = [], set(), [root]
    while pending:
        run = pending.pop()
        rid = run["workflow_run_id"]
        if rid in seen:
            raise WorkflowError("Workflow recovery found a repeated or cyclic child link")
        seen.add(rid)
        result.append(run)
        for activation in run["activations"]:
            invocation = activation.get("invocation")
            if not invocation:
                continue
            child_id = invocation.get("child_workflow_run_id")
            try:
                child = storage.get_run(child_id, refresh_atomic=True)
            except FileNotFoundError:
                if invocation.get("stage") == "preparing":
                    continue  # Existing reservation owns the not-yet-created child ID.
                raise WorkflowError("Workflow recovery cannot establish a linked child")
            link = child.get("parent_link") or {}
            if link.get("workflow_run_id") != rid or link.get("execution_id") != activation["id"] or link.get("root_workflow_run_id") != root["workflow_run_id"]:
                raise WorkflowError("Workflow recovery child ownership does not match its parent")
            pending.append(child)
    return result


def validate(storage: Any, root: dict[str, Any]) -> list[dict[str, Any]]:
    from . import identity, store
    from .task_supervision import attachable
    from .workflows import WorkflowError, TERMINAL
    if root.get("parent_link"):
        raise WorkflowError("Recover the root workflow; linked children share its scheduler")
    runs = inventory(storage, root)
    for run in runs:
        owner = run.get("supervisor_identity")
        if owner and identity.identity_check(owner) != "dead":
            raise WorkflowError("Workflow scheduler is live or uncertain; cannot recover")
        if run.get("supervisor_pid") and not owner:
            raise WorkflowError("Workflow scheduler lacks a qualified identity; cannot recover")
        for activation in run["activations"]:
            for task in activation["tasks"]:
                if task.get("status") not in ACTIVE:
                    continue
                if task.get("execution_kind") == "native_subagent":
                    from .workflow_native_observations import recoverable
                    if not recoverable(storage.root / "tasks", task):
                        raise WorkflowError("Native child ownership must be reconciled before scheduler recovery")
                    continue
                record = store.read(storage.root / "tasks", task["task_id"])
                if record is None and task.get("dispatch_stage") == "preparing":
                    continue
                if not attachable(storage.root / "tasks", task["task_id"]):
                    raise WorkflowError("Workflow task owner is lost or uncertain; reconcile before recovery")
        if run.get("status") in TERMINAL and any(t.get("status") in ACTIVE for a in run["activations"] for t in a["tasks"]):
            raise WorkflowError("Terminal child has unresolved execution ownership")
    return runs


def request(storage: Any, root: dict[str, Any], reason: str, interaction_owner: str | None) -> dict[str, Any]:
    from .workflows import WorkflowError, _launch, _supervisor_present, _require_delegation_control
    _require_delegation_control(root)
    if root.get("kind") == "builder" or root.get("parent_link"):
        raise WorkflowError("Interrupted scheduler recovery requires a delegation root")
    if not isinstance(reason, str) or not reason.strip():
        raise WorkflowError("Recovery requires a nonempty reason")
    from . import identity
    owner = root.get("supervisor_identity")
    verdict = identity.identity_check(owner) if owner else "undecidable"
    if verdict == "alive" and root.get("scheduler_recovery"):
        from .workflow_interactions import authorize
        import copy
        authorize(storage, copy.deepcopy(root), interaction_owner, "recover", pending=True)
        return root
    if verdict != "dead":
        raise WorkflowError("Workflow scheduler is live or uncertain; confirmed owner death is required for recovery")
    validate(storage, root)
    def reserve(run: dict[str, Any]) -> None:
        if _supervisor_present(run):
            raise WorkflowError("Workflow scheduler changed before recovery")
        if run.get("status") != root.get("status") or run.get("sequence") != root.get("sequence"):
            raise WorkflowError("Workflow changed before recovery; read it again")
        from .workflow_interactions import authorize
        authorize(storage, run, interaction_owner, "recover", pending=True)
        run["scheduler_recovery"] = {"reason": reason, "requested_at": time.time()}
    result = storage.update_run(root["workflow_run_id"], reserve, "scheduler_recovery_requested")
    _launch(storage, root["workflow_run_id"])
    return result


def restore_permits(supervisor: Any) -> None:
    """Charge each surviving attempt before any coroutine may acquire a new slot."""
    root = supervisor.run(refresh_atomic=True)
    runs = inventory(supervisor.store, root)
    supervisor.tree.recovered_run_ids = {run["workflow_run_id"] for run in runs}
    for run in runs:
        if run["workflow_run_id"] != root["workflow_run_id"]:
            from .workflows import _supervisor_present
            run = supervisor.store.reconcile_run(run["workflow_run_id"], resource_owner_dead=not _supervisor_present(run))
        for activation in run["activations"]:
            for task in activation["tasks"]:
                native_pending = False
                if task.get("execution_kind") == "native_subagent" and task.get("status") in {"completed", "failed"}:
                    transport = next((t for a in run["activations"] for t in a["tasks"] if t["task_id"] == task.get("transport_task_id")), {})
                    from .task_supervision import read_launch
                    native_pending = "native_observation_watermark" not in transport and read_launch(supervisor.registry._log_dir, task["transport_task_id"]) is not None
                if task.get("status") not in ACTIVE and not native_pending or task.get("dispatch_stage") == "preparing":
                    continue
                control = activation["role"] != "node"
                slots = supervisor.tree.control_slots if control and supervisor.tree.control_slots is not None else supervisor.tree.slots
                if slots is not None:
                    slots.free -= 1
                    supervisor.tree.hold_slot(run["workflow_run_id"], task["task_id"], control=control)
