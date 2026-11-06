"""Shared caller boundary for independently qualified conversation delivery.

Ctl/MCP caller guards run first. Descriptive linkage locates state; it never
substitutes for current source/launch qualification or workflow authority.
"""
from __future__ import annotations

import asyncio

from . import conversation_runtime as runtime
from .conversation_documents import CONTRACT, ConversationError
from .conversation_requests import validate_send
from .state_access import storage_operation


def _monitor(interaction_owner):
    if interaction_owner not in ('caller', 'monitor'):
        raise ConversationError('invalid_interaction_owner')
    return interaction_owner == 'monitor'


def _live_binding(registry, source):
    from .conversation_descriptor import validate
    from .conversation_live_owner import ledger_binding
    descriptor = validate(source.conversation)
    binding = ledger_binding(registry.state_layout, source.task_id)
    if descriptor['conversation_id'] != binding.conversation_id:
        raise ConversationError('target_stale')
    return binding


@storage_operation
async def prepare_task(registry, task_id, *, interaction_owner='caller'):
    """Return the exact witness for this standalone task or owned activation."""
    monitor = _monitor(interaction_owner)
    from .messaging import _qualify_source
    from .workflow_hooks import owner
    await _qualify_source(registry, task_id)
    association = await asyncio.to_thread(owner, registry.log_dir, task_id, strict=True)
    if association is not None:
        from .workflows import WorkflowStore
        from .conversation_workflows import enroll_activation
        storage = WorkflowStore(registry.state_layout.root)
        state = await asyncio.to_thread(enroll_activation, storage, registry,
            association['workflow_run_id'], association['activation_id'], task_id, monitor=monitor)
        return {**projection(state), 'target': dict(kind='workflow_activation',
            workflow_run_id=association['workflow_run_id'], activation_id=association['activation_id'], task_id=task_id)}
    source = await asyncio.to_thread(runtime._record, registry, task_id)
    if source.conversation:
        from .conversation_task_transport import qualify_saved_source
        binding = _live_binding(registry, source)
        state = await asyncio.to_thread(binding.ledger.read)
        if not state['closed'] and state['runtime_fault'] is None:
            if not await asyncio.to_thread(qualify_saved_source, registry, source):
                raise ConversationError('native_source_unqualified')
    else:
        state = await asyncio.to_thread(runtime.enroll, registry, task_id)
    return projection(state)


def projection(state):
    """Public intent/progress only; native context and launch authority stay private."""
    def message(item):
        result = {key: item[key] for key in (
            'request_id', 'text', 'delivery', 'sequence', 'status', 'target_task_id',
            'original_target_task_id', 'source_generation', 'source_turn_id', 'source_epoch')}
        claim = item['claim']
        if claim is not None:
            result['destination'] = dict(task_id=claim['destination_task_id'],
                turn_id=claim['destination_turn_id'], generation=claim['destination_generation'],
                origin=claim['origin'])
        if item['receipt'] is not None:
            result['receipt'] = {key: item['receipt'][key] for key in (
                'kind', 'task_id', 'turn_id', 'generation')}
        if item['interrupt_intent'] is not None:
            result['interrupt_intent'] = {key: item['interrupt_intent'][key] for key in (
                'generation', 'logical_turn')}
        if item['promotion'] is not None:
            result['promotion_request_id'] = item['promotion']['request_id']
        if item['cancellation'] is not None:
            result['cancellation_request_id'] = item['cancellation']['request_id']
        return result
    return dict(contract=CONTRACT, expected={key: state[key] for key in (
        'conversation_id', 'head_task_id', 'generation', 'turn_id', 'ledger_epoch')},
        closed=state['closed'], cumulative_issuances=state['cumulative_issuances'],
        accounting=dict(state['accounting']),
        runtime_fault=dict(state['runtime_fault']) if state['runtime_fault'] is not None else None,
        **({'progress': dict(state['native_progress'])} if 'native_progress' in state else {}),
        messages=[message(item) for item in state['messages']])


def message_ack(conversation_id, task_id, item):
    """Compact native-task admission readback; no consumption is inferred."""
    return dict(contract=CONTRACT, conversation_id=conversation_id,
        message_id=item['request_id'], task_id=task_id, resolved_task_id=item['target_task_id'],
        status=item['status'], delivery=item['delivery'], sequence=item['sequence'])


def control_ack(command, task_id, receipt):
    """Control receipt is distinct from the prepare/status projection."""
    return dict(contract=CONTRACT, request_id=command['request_id'], message_id=command['message_request_id'],
        operation=command['operation'], status='cancelled' if command['operation'] == 'cancel' else 'promoted',
        task_id=task_id, resolved_task_id=receipt['target_task_id'], control=receipt)


@storage_operation
async def send_task(registry, command, *, interaction_owner='caller'):
    """Admit exact intent; durable issuance belongs to the settlement/native owner."""
    validate_send(command)
    monitor = _monitor(interaction_owner)
    task_id = command['target']['task_id']
    from .messaging import _qualify_source
    await _qualify_source(registry, task_id)
    if command['target']['kind'] == 'workflow_activation':
        from .workflows import WorkflowStore
        from .conversation_workflows import send_request, schedule_delivery
        storage = WorkflowStore(registry.state_layout.root)
        result = await asyncio.to_thread(send_request, storage, registry, command, monitor=monitor)
        schedule_delivery(storage, registry, command['target']['workflow_run_id'], command['target']['activation_id'])
        return {'contract': CONTRACT, **result}
    from .workflow_hooks import owner
    association = await asyncio.to_thread(owner, registry.log_dir, task_id, strict=True)
    if association is not None:
        raise ConversationError('workflow_activation_required')
    expected = command['expected']
    source = await asyncio.to_thread(runtime._record, registry, task_id)
    if source.conversation:
        from .conversation_task_transport import qualify_saved_source
        from .conversation_live_owner import admit
        binding = _live_binding(registry, source)
        state = await asyncio.to_thread(binding.ledger.read)
        if expected['conversation_id'] != binding.conversation_id:
            raise ConversationError('target_stale')
        replay = any(m['request_id'] == command['request_id'] for m in state['messages'])
        if not replay and not await asyncio.to_thread(qualify_saved_source, registry, source):
            raise ConversationError('native_source_unqualified')
        item = await asyncio.to_thread(admit, binding, command['text'], request_id=command['request_id'],
            mode=command['delivery'], expected=expected)
        live = registry.get(task_id)
        if live is not None and live.conversation_driver is not None:
            live.conversation_driver.wake()
        return message_ack(binding.conversation_id, task_id, item)
    ledger = runtime.ledger_for(registry, source)
    if expected['conversation_id'] != ledger.conversation_id:
        raise ConversationError('target_stale')
    result = await asyncio.to_thread(runtime.admit, registry, task_id, command['text'],
        request_id=command['request_id'], mode=command['delivery'], expected_epoch=expected['ledger_epoch'],
        expected_head_task_id=expected['head_task_id'], expected_generation=expected['generation'],
        expected_turn_id=expected['turn_id'])
    await runtime.drain(registry, ledger.conversation_id)
    return {'contract': CONTRACT, **result}


def _known_request(state, request_id):
    return any(item['request_id'] == request_id
        or (item['promotion'] and item['promotion']['request_id'] == request_id)
        or (item['cancellation'] and item['cancellation']['request_id'] == request_id)
        for item in state['messages'])


def _apply_control(state, command):
    from . import conversation_delivery as delivery
    expected = command['expected']
    if state['conversation_id'] != expected['conversation_id']:
        raise ConversationError('target_stale')
    function = delivery.cancel_control if command['operation'] == 'cancel' else delivery.promote_now
    return function(state, request_id=command['request_id'], message_request_id=command['message_request_id'],
        target_task_id=expected['head_task_id'], expected_generation=expected['generation'],
        expected_turn_id=expected['turn_id'], expected_epoch=expected['ledger_epoch'])


@storage_operation
def _control_standalone(registry, source, command):
    from . import control
    if source.conversation:
        from .conversation_task_transport import qualify_saved_source
        binding = _live_binding(registry, source)
        before = binding.ledger.read()
        # Detached qualification hashes/probes the executable. Complete it
        # before taking the leaf lock; the mutation only checks this result and
        # revalidates the exact current request/turn/epoch.
        qualified = (command['operation'] != 'promote_now'
            or _known_request(before, command['request_id'])
            or qualify_saved_source(registry, source))
        def mutate(state):
            if (command['operation'] == 'promote_now' and not _known_request(state, command['request_id'])
                    and not qualified):
                raise ConversationError('native_source_unqualified')
            return _apply_control(state, command)
        return binding.ledger.update(mutate)
    # The same session serialization as admission prevents adding a new pending
    # native effect to a source concurrently selected for age retention.
    with control.session_lock_sync(registry.log_dir, source.session_id):
        source = runtime._record(registry, source.task_id)
        ledger = runtime.ledger_for(registry, source)
        before = ledger.read()
        if command['operation'] == 'promote_now' and not _known_request(before, command['request_id']):
            runtime._qualified(source)
        def mutate(state):
            if command['operation'] == 'promote_now' and not _known_request(state, command['request_id']):
                if not state['boundary']:
                    raise ConversationError('interrupt_unsupported')
            return _apply_control(state, command)
        return ledger.update(mutate)


@storage_operation
async def control_task(registry, command, *, interaction_owner='caller'):
    """Cancel or promote one admitted message; caller prose and identity stay intact."""
    from .conversation_requests import validate_control
    validate_control(command)
    monitor = _monitor(interaction_owner)
    task_id = command['target']['task_id']
    from .messaging import _qualify_source
    await _qualify_source(registry, task_id)
    if command['target']['kind'] == 'workflow_activation':
        from .workflows import WorkflowStore
        from .conversation_workflows import control_request, schedule_delivery
        storage = WorkflowStore(registry.state_layout.root)
        result = await asyncio.to_thread(control_request, storage, registry, command, monitor=monitor)
        if command['operation'] == 'promote_now':
            schedule_delivery(storage, registry, command['target']['workflow_run_id'], command['target']['activation_id'])
        return {'contract': CONTRACT, **result}
    from .workflow_hooks import owner
    association = await asyncio.to_thread(owner, registry.log_dir, task_id, strict=True)
    if association is not None:
        raise ConversationError('workflow_activation_required')
    source = await asyncio.to_thread(runtime._record, registry, task_id)
    receipt = await asyncio.to_thread(_control_standalone, registry, source, command)
    if command['operation'] == 'promote_now':
        if source.conversation:
            task = registry.get(task_id)
            if task is not None and task.conversation_driver is not None:
                task.conversation_driver.wake()
        else:
            await runtime.drain(registry, command['expected']['conversation_id'])
    return control_ack(command, task_id, receipt)


@storage_operation
async def commit_task(registry, command, *, interaction_owner='caller'):
    """Explicit workflow commit through the existing capped, framed validator."""
    from .conversation_requests import validate_commit
    from .conversation_workflows import commit_conversation
    from .messaging import _qualify_source
    from .workflows import WorkflowStore
    validate_commit(command)
    monitor = _monitor(interaction_owner)
    target = command['target']
    if target['task_id'] != command['expected']['head_task_id']:
        raise ConversationError('target_stale')
    await _qualify_source(registry, target['task_id'])
    storage = WorkflowStore(registry.state_layout.root)
    receipt = await asyncio.to_thread(commit_conversation, storage, registry,
        target['workflow_run_id'], target['activation_id'], expected=command['expected'],
        steering_request=command['steering_request'], monitor=monitor)
    return dict(contract=CONTRACT, request_id=command['request_id'], operation='commit', receipt=receipt)
