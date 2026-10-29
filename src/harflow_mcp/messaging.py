"""Public queued messaging shared by MCP and CLI, independent of launch origin."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from . import followups, inbox, store, workflow_messages
from .state_access import task_operation


@task_operation
def queue(log_dir: Path, task_id: str, text: str, *, by=None, delivery="queue", interaction_owner="caller") -> dict[str, Any]:
    workflow_messages.validate_text(text)
    if delivery not in {"queue", "now"}:
        raise inbox.SendRefused("delivery must be queue or now", code="invalid_delivery")
    from .workflow_hooks import owner
    from .workflows import WorkflowStore
    association = owner(log_dir, task_id, strict=True)
    if association is not None:
        return workflow_messages.send(WorkflowStore(log_dir.parent), association["workflow_run_id"], text,
                                      task_id=task_id, delivery=delivery, interaction_owner=interaction_owner)
    record = store.read(log_dir, store.validate_task_id(task_id))
    if record is None:
        raise inbox.SendRefused(f"unknown task_id: {task_id}", code="unknown_task")
    from . import control
    if control.takeover_attempt_active(log_dir, task_id):
        raise inbox.SendRefused("Session is taken over or its takeover is uncertain", code="taken_over")
    result = followups.send(log_dir, task_id, text, by=by, allow_live=True, delivery=delivery)
    result["delivery"] = delivery
    fresh = store.read(log_dir, task_id)
    resolved = store.resolve_status(log_dir, fresh, detail=False)[0] if fresh is not None else None
    if delivery == "now" and resolved == "running":
        result["refusal"] = workflow_messages.refusal("interrupt_unsupported", "The message stays queued: this backend has no qualified safe interruption operation. Wait for settlement, then continue.")
    elif delivery != "now" and resolved in store.TERMINAL_RECORD_STATUSES:
        result["target_settled"] = True
        result["target_status"] = resolved
        result["note"] = (
            f"Task already finished ({resolved}); this message waits for the next resume_task / continue_task. "
            'Use delivery="now" or continue_task(consume_followups=True) to deliver it now.'
        )
    return result


async def send(registry, task_id: str, text: str, *, delivery="queue", interaction_owner="caller") -> dict[str, Any]:
    await _qualify_source(registry, task_id)
    result = await asyncio.to_thread(queue, registry.log_dir, task_id, text, by=registry._owner,
                                     delivery=delivery, interaction_owner=interaction_owner)
    if "workflow_run_id" in result:
        from .workflows import WorkflowStore
        return await workflow_messages.deliver_now(WorkflowStore(registry.log_dir.parent), result, interaction_owner=interaction_owner)
    if delivery != "now" or "refusal" in result:
        return result
    try:
        # Continue enforces observed settlement, pending approvals, takeover, exact successor
        # reservation and the original execution authority. A refused launch leaves the queue.
        successor = await registry.continue_task(task_id, consume_followups=True)
        result.update(status="delivering", successor_task_id=successor.task_id, note="Continuation reserved; inspect its status for process and delivery progress.")
    except (ValueError, RuntimeError, OSError) as exc:
        view = await asyncio.to_thread(followups.view, registry.log_dir, task_id)
        item = next((item for item in view.get("items", []) if item["id"] == result["message_id"]), None)
        if item is not None and item["status"] != "queued":
            # A launch error after reservation is not proof that nothing was delivered. Keep the
            # existing sealed/uncertain ledger authoritative and never promise it stayed queued.
            result["status"] = "delivered" if item["status"] == "consumed" else "delivering"
            result["note"] = "Continuation delivery requires reconciliation; do not resend automatically."
            result["refusal"] = workflow_messages.refusal("continuation_uncertain", "Continuation requires inspection before resending: " + str(exc))
        else:
            result["refusal"] = workflow_messages.refusal(getattr(exc, "code", "continuation_refused"), str(exc))
    return result


@task_operation
def cancel(log_dir: Path, task_id: str, message_id: str, *, interaction_owner="caller") -> dict[str, Any]:
    from .workflow_hooks import owner
    from .workflows import WorkflowStore
    association = owner(log_dir, task_id, strict=True)
    if association is not None:
        return workflow_messages.cancel(WorkflowStore(log_dir.parent), association["workflow_run_id"], message_id,
                                        interaction_owner=interaction_owner)
    return followups.cancel(log_dir, task_id, message_id)


def task_messages(task_id: str, view: dict) -> list[dict]:
    """Additive common lifecycle; the full legacy ledger remains available unchanged."""
    states = {"queued": "queued", "reserved": "delivering", "consumed": "delivered", "cancelled": "cancelled", "undeliverable": "undeliverable", "delivery_unknown": "delivering"}
    return [{**item, "status": states[item["status"]], "target": item.get("target", {"kind": "task", "task_id": task_id}), "delivery": item.get("delivery", "queue"), **({"reason": "Delivery is uncertain; inspect the continuation before resending"} if item["status"] == "delivery_unknown" else {})} for item in view.get("items", [])]


async def _qualify_source(registry, task_id: str) -> None:
    from .extra_paths import has_extra_path_authority
    record = await asyncio.to_thread(store.read, registry.log_dir, store.validate_task_id(task_id))
    if record is not None and has_extra_path_authority(record):
        from .backends import get as get_backend
        caller = await registry._mutation_caller()
        await registry._resolve_extra_paths_async(caller, None, get_backend(record.backend), record)


async def cancel_message(registry, task_id: str, message_id: str, *, interaction_owner="caller") -> dict[str, Any]:
    """Qualify saved extra-path sender authority before withdrawing a queued message."""
    await _qualify_source(registry, task_id)
    return await asyncio.to_thread(cancel, registry.log_dir, task_id, message_id, interaction_owner=interaction_owner)
