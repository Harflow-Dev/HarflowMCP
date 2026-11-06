"""Durable, one-message same-session resume driver.

Only explicitly qualified resume profiles enter this route. Native live-turn Now
belongs to conversation_transport; this module never interrupts or opens Fresh.
The ledger claim names the destination before the subprocess can be issued.
"""
from __future__ import annotations

import asyncio
import logging
import uuid

from . import conversation_delivery as delivery, store, control
from .backends import get as get_backend
from .conversation_documents import ConversationError, request_uuid
from .conversation_store import ConversationStore
from .state_access import storage_operation
from .state_ownership import owned_job

log = logging.getLogger(__name__)
_NAMESPACE = uuid.UUID('9e546fb0-9981-4bac-9fe8-55a52fe03edf')


def _identity(registry, source):
    if not source.session_id:
        raise ConversationError('native_context_unknown')
    # Only recorded same-session resume ancestry joins a conversation. A new
    # root that happens to name the same native session is never silently joined.
    root = source
    visited = set()
    while getattr(root, 'parent_task_id', None):
        if root.task_id in visited or len(visited) >= 256:
            raise ConversationError('conversation_lineage_unavailable')
        visited.add(root.task_id)
        parent = _record(registry, root.parent_task_id)
        if (parent.backend, str(parent.repo_path), parent.session_id) != (source.backend, str(source.repo_path), source.session_id):
            break
        root = parent
    return str(uuid.uuid5(_NAMESPACE, '\0'.join((source.backend, str(source.repo_path), source.session_id, root.task_id))))


def _qualified(source):
    if getattr(source, 'conversation', None):
        raise ConversationError('live_conversation_owned')
    backend = get_backend(source.backend)
    qualified = getattr(backend, 'conversation_resume_supported', None)
    if (not callable(qualified) or qualified(source) is not True
            or backend.capabilities.resume_may_start_fresh):
        raise ConversationError('queue_unsupported')
    if getattr(source, 'max_turns', None) is not None:
        raise ConversationError('queue_cap_unsupported')
    if getattr(source, 'native_subagent', False):
        raise ConversationError('native_child_unsupported')


def _record(registry, task_id):
    record = store.read(registry.log_dir, store.validate_task_id(task_id))
    if record is None:
        raise ConversationError('unknown_task')
    return record


def ledger_for(registry, source):
    descriptor = getattr(source, 'conversation', None)
    if descriptor:
        from .workflow_hooks import owner
        association = owner(registry.log_dir, source.task_id, strict=True)
        if association is not None:
            from .workflows import WorkflowStore
            from .conversation_workflows import WorkflowConversationStore
            return WorkflowConversationStore(WorkflowStore(registry.state_layout.root),
                association['workflow_run_id'], association['activation_id'], descriptor['conversation_id'])
        return ConversationStore(registry.state_layout, descriptor['conversation_id'])
    return ConversationStore(registry.state_layout, _identity(registry, source))


def _initial_state(registry, ledger, source):
    state = delivery.new_state(ledger.conversation_id, source.task_id, source.session_id, 0, source.task_id)
    if (source.status == 'completed' and source.exit_code is not None and source.output_drained
            and store.resolve_status(registry.log_dir, source, detail=False)[0] == 'completed'):
        task = registry.get(source.task_id)
        if task is None:
            from types import SimpleNamespace
            acc, _ = store.replay_log(registry.log_dir, source.task_id, source.backend)
            done = asyncio.Event(); done.set()
            task = SimpleNamespace(**vars(source), acc=acc, done=done,
                latest_record_landed=True, persistence_error=None)
        if _observed(task, source.session_id):
            state = delivery.record_response(state, generation=0, turn_id=source.task_id)
            state = delivery.observe_boundary(state, generation=0, turn_id=source.task_id)
    return state


def _enroll_locked(registry, task_id):
    """Enroll only an explicit positive profile; existing heads never move here."""
    source = _record(registry, task_id)
    _qualified(source)
    ledger = ledger_for(registry, source)
    initial = _initial_state(registry, ledger, source)
    def create(old):
        state = old if old is not None else initial
        return state, state
    return ledger.update(create)


def _admit_locked(registry, task_id, text, *, request_id, expected_epoch=None, mode='queue',
          expected_head_task_id=None, expected_generation=None, expected_turn_id=None):
    """Return a durable admission receipt; original and resolved heads are explicit.

    Replay is checked before current head/turn checks. Now on an active ordinary
    subprocess refuses before admission: it cannot silently become Queue.
    """
    request_uuid(request_id)
    source = _record(registry, task_id)
    _qualified(source)
    ledger = ledger_for(registry, source)
    initial = _initial_state(registry, ledger, source)
    def add(old):
        state = old if old is not None else initial
        existing = next((m for m in state['messages'] if m['request_id'] == request_id), None)
        if existing is not None:
            if (existing['text'], existing['delivery']) != (text, mode):
                raise ConversationError('request_conflict')
            if existing.get('original_target_task_id', existing['target_task_id']) != task_id:
                raise ConversationError('request_conflict')
            if any(value is not None and value != existing[key] for key, value in (
                    ('target_task_id', expected_head_task_id),
                    ('source_generation', expected_generation), ('source_turn_id', expected_turn_id),
                    ('source_epoch', expected_epoch))):
                raise ConversationError('request_conflict')
            return state, existing
        if None in (expected_head_task_id, expected_generation, expected_turn_id):
            raise ConversationError('expected_source_required')
        if (expected_head_task_id, expected_generation, expected_turn_id) != (state['head_task_id'], state['generation'], state['turn_id']):
            raise ConversationError('target_stale')
        if mode == 'now' and not state['boundary']:
            raise ConversationError('interrupt_unsupported')
        return delivery.admit(state, request_id=request_id, text=text,
            delivery=mode, target_task_id=state['head_task_id'], expected_epoch=expected_epoch,
            expected_generation=expected_generation, expected_turn_id=expected_turn_id,
            original_target_task_id=task_id)
    item = ledger.update(add)
    projection = ledger.read()
    return dict(conversation_id=ledger.conversation_id, message_id=request_id,
        task_id=task_id, resolved_task_id=item['target_task_id'], status=item['status'],
        delivery=mode, sequence=item['sequence'], **{key: projection[key]
            for key in ('head_task_id', 'generation', 'turn_id', 'ledger_epoch')})


@storage_operation
def enroll(registry, task_id):
    source = _record(registry, task_id)
    if not source.session_id:
        raise ConversationError('native_context_unknown')
    with control.session_lock_sync(registry.log_dir, source.session_id):
        return _enroll_locked(registry, task_id)


@storage_operation
def admit(registry, task_id, text, *, request_id, expected_epoch=None, mode='queue',
          expected_head_task_id=None, expected_generation=None, expected_turn_id=None):
    source = _record(registry, task_id)
    if not source.session_id:
        raise ConversationError('native_context_unknown')
    with control.session_lock_sync(registry.log_dir, source.session_id):
        return _admit_locked(registry, task_id, text, request_id=request_id,
            expected_epoch=expected_epoch, mode=mode,
            expected_head_task_id=expected_head_task_id,
            expected_generation=expected_generation, expected_turn_id=expected_turn_id)


@storage_operation
def cancel(registry, task_id, request_id):
    source = _record(registry, task_id)
    ledger = ledger_for(registry, source)
    ledger.update(lambda old: (delivery.cancel(old, request_id), None))
    return dict(message_id=request_id, status='cancelled')


def _observed(task, context_id):
    """A task-local parsed terminal plus actual exit/drain is response evidence."""
    acc = task.acc
    return (task.done.is_set() and task.exit_code is not None and task.output_drained
        and task.latest_record_landed and task.persistence_error is None
        and acc.session_id == context_id and not acc.normalize_errors
        and (acc.result_count > 0 or acc.terminal is not None)
        and not acc.turn_open and not acc.background_open)


@storage_operation
async def on_settled(registry, task):
    """Owner hook after Task.done, terminal persistence and physical pipe drain.

    Existing claims receive proof only for their predetermined destination.
    Unknown effects stay protected; an absent result never becomes consumption.
    At most one next message is issued by this invocation.
    """
    if getattr(task, 'conversation', None):
        return None  # Live owner already binds native turns; never resume-drain it.
    if not task.session_id:
        return None
    ledger = ledger_for(registry, task)
    if not ledger.path.exists():
        return None  # No enrollment means no changed meaning for legacy queues.
    def settle(state):
        if state is None or state['head_task_id'] != task.task_id:
            return state, False
        claim_item = next((m for m in state['messages'] if m['status'] in ('claimed', 'unknown')), None)
        if not _observed(task, state['native_context_id']):
            if claim_item is not None:
                state = delivery.mark_unknown(state, claim_id=claim_item['claim']['claim_id'])
            return state, False
        if claim_item is not None:
            claim = claim_item['claim']
            if claim['destination_task_id'] != task.task_id:
                return state, False
            state = delivery.record_receipt(state, receipt=dict(claim=claim,
                task_id=task.task_id, turn_id=claim['destination_turn_id'],
                generation=claim['destination_generation'], kind='input_consumed'))
        state = delivery.record_response(state, generation=state['generation'], turn_id=state['turn_id'])
        state = delivery.observe_boundary(state, generation=state['generation'], turn_id=state['turn_id'])
        return state, True
    observed = await asyncio.to_thread(ledger.update, settle)
    if not observed:
        return None
    if getattr(task, 'cancel_requested', False) or task.status != 'completed':
        return None
    return await drain(registry, ledger.conversation_id)


@storage_operation
async def drain(registry, conversation_id):
    """Claim one FIFO message, then issue same-session resume outside every lock."""
    ledger = ConversationStore(registry.state_layout, conversation_id)
    child_id, logical_turn, claim_id = (str(uuid.uuid4()) for _ in range(3))
    before = await asyncio.to_thread(ledger.read)
    if (not before['boundary'] or before['closed']
            or any(m['status'] in ('claimed', 'unknown') for m in before['messages'])):
        return None
    source = await asyncio.to_thread(_record, registry, before['head_task_id'])
    _qualified(source)
    resolved = await asyncio.to_thread(lambda: store.resolve_status(registry.log_dir, source, detail=False)[0])
    def claim(state):
        if state is None:
            raise ConversationError('conversation_missing')
        if (state['head_task_id'], state['generation'], state['turn_id']) != (
                before['head_task_id'], before['generation'], before['turn_id']):
            return state, None
        # No guessed/recovered terminal is an authorization to start a response.
        if resolved != 'completed' or source.exit_code is None or not source.output_drained:
            return state, None
        return delivery.claim_next(state, claim_id=claim_id,
            destination_task_id=child_id, destination_turn_id=logical_turn)
    item = await asyncio.to_thread(ledger.update, claim)
    if item is None:
        return None
    claimed = item['claim']
    try:
        parent_id = claimed['source_task_id']
        parent = registry.get(parent_id)
        kwargs = dict(task_id=child_id, consume_followups=False, require_unchanged_session=True)
        if parent is not None:
            successor = await registry.resume(parent, item['text'], **kwargs)
        else:
            parent = await asyncio.to_thread(_record, registry, parent_id)
            successor = await registry.resume_record(parent, item['text'], **kwargs)
        if successor.task_id != child_id or successor.session_id != claimed['native_context_id']:
            raise ConversationError('destination_mismatch')
        return successor
    except BaseException:
        # Even a launch refusal keeps its reserved destination protected. Only a
        # separate positive not-started reconciliation could release it safely.
        await asyncio.to_thread(ledger.update, lambda state: (
            delivery.mark_unknown(state, claim_id=claim_id), None))
        raise


@storage_operation
def owns(registry, task):
    """Enrollment changes continuation semantics even while its queue is empty."""
    if not task.session_id and not getattr(task, 'conversation', None):
        return False
    ledger = ledger_for(registry, task)
    if not getattr(task, 'conversation', None) and not ledger.path.exists():
        return False
    if hasattr(ledger,'owns'):
        if not ledger.owns():raise ConversationError('conversation_missing')
    else:
        ledger.read()  # Corruption refuses; it never falls through to Fresh.
    return True


@storage_operation
def schedule_settled(registry, task):
    """Call beside auto-continuation scheduling; True suppresses the Fresh path.

    The job waits for Task.done and is retained in registry._continuations, which
    detached owners already await. It must also run when auto_continue is zero.
    """
    if not owns(registry, task):
        return False
    if getattr(task, 'conversation', None):
        return True
    async def run():
        await task.done.wait()
        try:
            await on_settled(registry, task)
        except Exception:
            log.exception('Conversation settlement deferred for %s', task.task_id)
    pending = getattr(registry, '_continuations', None)
    if pending is None:
        pending = registry._continuations = set()
    job = owned_job(registry.state_layout, run(), name=f'harflow-conversation-{task.task_id}')
    pending.add(job)
    job.add_done_callback(pending.discard)
    return True


@storage_operation
async def reconcile(registry):
    """Startup/ctl primitive: resume queued-only observed boundaries, never claims.

    Existing claimed or unknown effects require exact owner receipt recovery.
    No legacy follow-up ledger is read, consumed or migrated by this pass.
    """
    directory = registry.state_layout.root / 'conversations'
    if not directory.exists():
        return []
    started = []
    for path in sorted(directory.glob('*.json')):
        ledger = ConversationStore(registry.state_layout, path.stem)
        state = await asyncio.to_thread(ledger.read)
        record = await asyncio.to_thread(store.read, registry.log_dir, state['head_task_id'])
        if record is None:
            continue  # Reserved/unknown destination without receipt is never replayed.
        if getattr(record, 'conversation', None):
            continue  # Native live recovery never becomes a resume replay.
        resolved = await asyncio.to_thread(lambda: store.resolve_status(registry.log_dir, record, detail=False)[0])
        if resolved not in store.TERMINAL_RECORD_STATUSES or record.exit_code is None or not record.output_drained:
            continue
        task = registry.get(record.task_id)
        if task is None:
            from types import SimpleNamespace
            acc, _ = await asyncio.to_thread(store.replay_log, registry.log_dir, record.task_id, record.backend)
            done = asyncio.Event()
            done.set()
            task = SimpleNamespace(**vars(record), acc=acc, done=done,
                latest_record_landed=True, persistence_error=None)
        successor = await on_settled(registry, task)
        if successor is not None:
            started.append(successor.task_id)
    return started



def protected_task_ids(layout):
    """Retention snapshot; repeat under the task's session lock before deletion.

    Admission/enrollment take that same session lock before ledger publication.
    Existing queued work already pins every ancestor before a claim can move its
    head, so no leaf ledger lock is held across filesystem deletion.
    Corrupt or unavailable state raises and must abort the retention pass.
    """
    from .state_ownership import assert_owned
    assert_owned(layout)
    directory = layout.root / 'conversations'
    if not directory.exists():
        return set()
    protected = set()
    paths = list(directory.glob('*.json'))
    if len(paths) > 4096:
        raise ConversationError('conversation_inventory_unavailable')
    for path in paths:
        state = ConversationStore(layout, path.stem).read()
        if not delivery.protects_retention(state):
            continue
        ids = {state['head_task_id']}
        for item in state['messages']:
            ids.add(item['target_task_id'])
            ids.add(item['original_target_task_id'])
            if item['claim']:
                ids.update((item['claim']['source_task_id'], item['claim']['destination_task_id']))
        frontier = list(ids)
        while frontier:
            task_id = frontier.pop()
            if task_id in protected:
                continue
            if len(protected) >= 16384:
                raise ConversationError('conversation_inventory_unavailable')
            store.validate_task_id(task_id)
            protected.add(task_id)
            record = store.read(layout.tasks_directory, task_id)
            # A pre-reserved destination legitimately has no TaskRecord yet.
            if record is not None and record.parent_task_id and record.parent_task_id not in protected:
                frontier.append(record.parent_task_id)
    return protected
