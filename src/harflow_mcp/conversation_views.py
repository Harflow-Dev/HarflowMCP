"""Bounded read-only task-status presentation for native conversation sources.

This module is a status view, never a control seam: it locates the same durable
ledger the engine writes (a standalone leaf ledger or a workflow binding through
the existing facade), reads it once, and returns only bounded shape. No message
prose, native session id, profile fingerprint or live process identity crosses
this boundary, and the view never launches, signals, qualifies or mutates
anything. Descriptive linkage locates state only: an exact source/ledger
mismatch fails the readback instead of presenting another actor's conversation,
and readback loss is reported as `available: False` with a stable code so the
physical task status stands unchanged. `StateOwnershipError` propagates because
it means the state root itself could not be used safely.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .conversation_descriptor import validate as validate_descriptor
from .conversation_documents import CONTRACT, ConversationError, identifier, validate
from .conversation_store import ConversationStore
from .state_access import task_layout, task_operation

_MESSAGE_STATUSES = ('queued', 'claimed', 'consumed', 'unknown', 'cancelled')
_OUTSTANDING_STATUSES = ('queued', 'claimed', 'unknown')
_WITNESS = ('conversation_id', 'head_task_id', 'generation', 'turn_id', 'ledger_epoch')


def _workflow_document_errors() -> tuple[type[BaseException], ...]:
    from .workflow_receipt_documents import WorkflowReceiptDocumentError
    from .workflow_run_documents import WorkflowRunDocumentError
    from .workflows import WorkflowError
    return (WorkflowError, WorkflowReceiptDocumentError, WorkflowRunDocumentError)


def _typed_code(exc: BaseException, default: str) -> str:
    code = getattr(exc, 'code', None)
    return code if isinstance(code, str) and code else default


def _unavailable(code: str, conversation_id: str | None) -> dict[str, Any]:
    return dict(contract=CONTRACT, available=False, code=code, conversation_id=conversation_id)


def _summary(state: dict[str, Any]) -> dict[str, Any]:
    counts = dict.fromkeys(_MESSAGE_STATUSES, 0)
    for item in state['messages']:
        counts[item['status']] += 1
    fault = state['runtime_fault']
    return dict(
        contract=CONTRACT,
        available=True,
        conversation_id=state['conversation_id'],
        expected={key: state[key] for key in _WITNESS},
        closed=state['closed'],
        progress=dict(state['native_progress']),
        accounting=dict(state['accounting']),
        runtime_fault=dict(fault) if fault is not None else None,
        message_counts=counts,
        outstanding_count=sum(counts[status] for status in _OUTSTANDING_STATUSES),
    )


@task_operation
def describe(log_dir: Path, source) -> dict[str, Any] | None:
    """Return bounded conversation status for one native source, or None.

    `log_dir` is the engine task directory and `source` the record or task whose
    `conversation` descriptor names the ledger. `None` (no descriptor) is not an
    error: the task simply has no native conversation presentation. A failure to
    read the exact ledger (missing, corrupt, uncertain, retired or owned by
    another source) is reported as `available: False`, never as a different
    actor's conversation and never by changing the source's own outcome.
    """
    descriptor = getattr(source, 'conversation', None)
    if descriptor is None:
        return None
    try:
        descriptor = validate_descriptor(descriptor)
        conversation_id = descriptor['conversation_id']
        task_id = identifier(getattr(source, 'task_id', None))
        session_id = getattr(source, 'session_id', None)
        from .workflow_hooks import owner
        association = owner(log_dir, task_id, strict=True)
    except (ConversationError, OSError, ValueError, KeyError, TypeError, *_workflow_document_errors()) as exc:
        return _unavailable(_typed_code(exc, 'workflow_ownership_unknown'), None)
    try:
        if association is not None:
            from .conversation_workflows import WorkflowConversationStore
            from .workflows import WorkflowStore
            state = WorkflowConversationStore(
                WorkflowStore(root=log_dir.parent),
                association['workflow_run_id'], association['activation_id'],
                conversation_id).read()
        else:
            from .conversation_live_owner import conversation_id as live_conversation_id
            # The standalone descriptor id is deterministic from the task; a
            # foreign id is another actor's ledger and is never read.
            if conversation_id != live_conversation_id(task_id):
                raise ConversationError('native_owner_mismatch')
            state = ConversationStore(task_layout(log_dir), conversation_id).read()
        state = validate(state)
    except (ConversationError, OSError, ValueError, KeyError, TypeError, *_workflow_document_errors()) as exc:
        return _unavailable(_typed_code(exc, 'ledger_unavailable'), conversation_id)
    if (session_id is not None and state['native_context_id'] is not None
            and session_id != state['native_context_id']):
        return _unavailable('native_owner_mismatch', conversation_id)
    return _summary(state)
