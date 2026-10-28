"""Workflow-owned caller messages; dispatch reservations own consumption atomically.

Messages supplement context, never execution contracts, decisions, goals or grants. A task target
follows its node's next issued attempt; it cannot itself authorize another attempt.
"""
from __future__ import annotations

import copy
import time
import uuid
from typing import Any

from . import inbox

MAX_BYTES = 32 * 1024
MAX_OUTSTANDING = 16
STATUSES = {"queued", "delivering", "delivered", "undeliverable", "cancelled"}


def validate_text(text: str) -> None:
    try:
        if not isinstance(text, str) or not text.strip():
            raise inbox.SendRefused("text must be a non-empty string", code="invalid_text")
        if len(text.encode("utf-8")) > MAX_BYTES:
            raise inbox.SendRefused("Messages are limited to 32 KiB of UTF-8", code="message_too_large")
    except UnicodeEncodeError:
        raise inbox.SendRefused("Message is not valid UTF-8", code="invalid_text") from None


def refusal(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _authorize(storage, run, responder, action):
    from .workflow_interactions import authorize
    from .workflows import WorkflowError
    try:
        authorize(storage, run, responder, action, pending=True)
    except WorkflowError as exc:
        raise inbox.SendRefused(str(exc), code="interaction_owned") from exc


def send(storage, run_id: str, text: str, *, task_id: str | None = None,
         delivery: str = "queue", interaction_owner: str = "caller") -> dict[str, Any]:
    validate_text(text)
    if delivery not in {"queue", "now"}:
        raise inbox.SendRefused("delivery must be queue or now", code="invalid_delivery")
    message = {"id": uuid.uuid4().hex, "status": "queued", "text": text,
               "delivery": delivery, "queued_at": time.time()}
    def queue(run):
        _authorize(storage, run, interaction_owner, "message")
        target = {"kind": "workflow", "workflow_run_id": run_id}
        if task_id is not None:
            found = next((a for a in run["activations"] if any(t["task_id"] == task_id for t in a["tasks"])), None)
            if found is None:
                raise inbox.SendRefused("Task does not belong to this workflow", code="unknown_target")
            if found["role"] == "native_control":
                raise inbox.SendRefused("Internal transport messages must target the workflow", code="internal_target")
            target.update(kind="task", task_id=task_id, node_id=found["node_id"], role=found["role"])
        messages = run.setdefault("messages", [])
        # Retain bounded terminal history. Never drop pending or uncertain deliveries.
        final = [m for m in messages if m["status"] in {"delivered", "cancelled", "undeliverable"}]
        for old in final[:-48]:
            messages.remove(old)
        if sum(m["status"] in {"queued", "delivering"} for m in messages) >= MAX_OUTSTANDING or sum(len(m.get("text", "").encode("utf-8")) for m in messages) + len(text.encode("utf-8")) > 256 * 1024:
            raise inbox.SendRefused("Workflow message queue is full; cancel or deliver pending messages", code="queue_full")
        message.update(seq=run.get("message_sequence", 0) + 1, target=target)
        run["message_sequence"] = message["seq"]
        messages.append(copy.deepcopy(message))
    run = storage.update_run(run_id, queue, "message_queued", {"message_id": message["id"]})
    published = next(m for m in run["messages"] if m["id"] == message["id"])
    result = {"workflow_run_id": run_id, "message_id": message["id"], "seq": message["seq"], "status": published["status"], "delivery": delivery}
    if published.get("reason"):
        result["reason"] = published["reason"]
    if task_id:
        result["task_id"] = task_id
    if delivery == "now" and published["status"] == "undeliverable":
        result["refusal"] = refusal("workflow_terminal", published["reason"])
    elif delivery == "now":
        # The backend seam has no qualified interrupt primitive. Do not signal a process or
        # synthesize a control decision from caller prose. The durable queue survives refusal.
        active = any(t["status"] in {"reserved", "running", "uncertain"} for a in run["activations"] for t in a["tasks"])
        code = "interrupt_unsupported" if active else "workflow_checkpoint_required"
        result["refusal"] = refusal(code, "The message stays queued. " + ("This backend has no qualified safe interruption operation." if active else "Resume or recover the workflow through its current checkpoint; messaging cannot authorize a node or answer a pending question."))
    return result


def cancel(storage, run_id: str, message_id: str, *, interaction_owner: str = "caller") -> dict[str, Any]:
    def withdraw(run):
        _authorize(storage, run, interaction_owner, "cancel_message")
        message = next((m for m in run.get("messages", []) if m["id"] == message_id), None)
        if message is None:
            raise inbox.SendRefused("Unknown workflow message", code="unknown_message")
        if message["status"] != "queued":
            raise inbox.SendRefused("Message is already reserved or settled", code="not_cancellable")
        message.update(status="cancelled", at=time.time())
        message.pop("text", None)
    storage.update_run(run_id, withdraw, "message_cancelled", {"message_id": message_id})
    return {"workflow_run_id": run_id, "message_id": message_id, "status": "cancelled"}


def claim(run: dict, activation: dict, task_id: str) -> list[dict]:
    """Called in the same atomic mutation as dispatch reservation, before any spawn."""
    for attempt in (t for a in run.get("activations", []) for t in a.get("tasks", [])):
        task_state(run, attempt.get("task_id", ""), attempt.get("status", ""))
    result = []
    for message in run.get("messages", []):
        target = message["target"]
        matches = (target["kind"] == "workflow" and activation["role"] in {"orchestrator", "builder"} or
                   target["kind"] == "task" and target["node_id"] == activation["node_id"] and target["role"] == activation["role"])
        if message["status"] == "queued" and matches:
            message.update(status="delivering", delivery_task_id=task_id, execution_id=activation["id"])
            result.append(copy.deepcopy(message))
    return result


def task_state(run: dict, task_id: str, status: str) -> None:
    for message in run.get("messages", []):
        if message.get("delivery_task_id") != task_id or message["status"] != "delivering":
            continue
        if status == "not_started":
            message["status"] = "queued"
            message.pop("delivery_task_id", None)
            message.pop("execution_id", None)
        elif status in {"running", "completed", "failed", "cancelled"}:
            message.update(status="delivered", at=time.time())
            message.pop("text", None)
        # An uncertain spawn keeps delivering and cannot be cancelled/reconsumed.


def render(messages: list[dict]) -> str:
    if not messages:
        return ""
    return "\n\nQueued caller messages (context only; existing workflow goal, permissions and issued assignment remain authoritative):\n" + "\n\n".join("Message " + m["id"] + ":\n" + m["text"] for m in messages) + "\nEnd of queued caller messages.\n"


def projection(run: dict, task_id: str | None = None) -> list[dict]:
    return [copy.deepcopy(m) for m in run.get("messages", []) if task_id is None or m["target"].get("task_id") == task_id or m.get("delivery_task_id") == task_id]


def reconcile(run: dict) -> None:
    """Use authoritative attempts only; ambiguity never releases a delivery."""
    for attempt in (t for a in run.get("activations", []) for t in a.get("tasks", [])):
        task_state(run, attempt.get("task_id", ""), attempt.get("status", ""))
    terminal = run.get("status") == "cancelled" or run.get("status") == "completed" and run.get("kind") != "builder"
    if terminal:
        for message in run.get("messages", []):
            if message["status"] == "queued":
                message.update(status="undeliverable", reason="Workflow ended before another matching dispatch was issued", at=time.time())


async def deliver_now(storage, result: dict, *, interaction_owner: str) -> dict:
    """Try only existing engine control routes; every refusal preserves the admitted message."""
    if result.get("delivery") != "now" or result.get("status") != "queued":
        return result
    from . import workflows
    run = storage.get_run(result["workflow_run_id"], refresh_atomic=True)
    active = any(t["status"] in {"reserved", "running", "uncertain"} for a in run["activations"] for t in a["tasks"])
    if active:
        return result
    try:
        if run.get("kind") == "builder" and run["status"] in {"completed", "needs_attention"}:
            # The public builder continuation route retains the builder's read-only authority.
            # Exact caller text stays in this ledger and is claimed by the issued builder turn.
            _authorize(storage, run, interaction_owner, "message")
            await workflows.followup_workflow_builder(run["workflow_run_id"], "Process queued caller messages within the existing builder scope.", root=storage.root, interaction_owner=interaction_owner, _require_idle=True)
        elif run["status"] in {"paused", "needs_attention"} and not run.get("pending_inputs") and not run.get("input_question"):
            # Control revalidates ownership, live/uncertain attempts, approvals, current checkpoint
            # and attempt budget. No new retry grant or fabricated answer is supplied.
            storage.control(run["workflow_run_id"], "resume", interaction_owner=interaction_owner)
        else:
            return result
        result.pop("refusal", None)
        result["delivery_started"] = True
    except (workflows.WorkflowError, inbox.SendRefused, OSError) as exc:
        result["refusal"] = refusal(getattr(exc, "code", "workflow_checkpoint_required"), str(exc))
    return result
