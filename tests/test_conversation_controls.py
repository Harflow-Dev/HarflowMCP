"""Operation UUIDs bind cancellation/promotion without changing caller messages."""
from copy import deepcopy
import uuid

import pytest

from harflow_mcp import conversation_api as api, conversation_delivery as delivery
from harflow_mcp.conversation_documents import CONTRACT, ConversationError
from harflow_mcp.conversation_requests import validate_control, load_control
from test_conversation_tasks import conversation, tmp_path, finish_turn


def uid():
    return str(uuid.uuid4())


def state_with_message():
    state = delivery.new_state(uid(), 'task', 'context', 0, 'turn')
    return delivery.admit(state, request_id=uid(), text='  exact  message\n', delivery='queue',
        target_task_id='task', expected_epoch=0)


def test_cancel_control_replay_is_exact_and_global_uuid_namespace_conflicts():
    state, message = state_with_message()
    request_id = uid()
    args = dict(request_id=request_id, message_request_id=message['request_id'], target_task_id='task',
        expected_generation=0, expected_turn_id='turn', expected_epoch=0)
    cancelled, receipt = delivery.cancel_control(state, **args)
    assert cancelled['messages'][0]['text'] == message['text']
    assert cancelled['messages'][0]['request_id'] == message['request_id']
    assert cancelled['messages'][0]['cancellation'] == receipt
    assert delivery.cancel_control(cancelled, **args) == (cancelled, receipt)
    for changed in (dict(expected_generation=1), dict(expected_turn_id='other'),
                    dict(expected_epoch=1), dict(message_request_id=uid()), dict(target_task_id='other')):
        with pytest.raises(ConversationError, match='request_conflict'):
            delivery.cancel_control(cancelled, **{**args, **changed})
    with pytest.raises(ConversationError, match='request_conflict'):
        delivery.promote_now(cancelled, **args)
    with pytest.raises(ConversationError, match='request_conflict'):
        delivery.admit(cancelled, request_id=request_id, text='new', delivery='queue', target_task_id='task', expected_epoch=0)
    with pytest.raises(ConversationError, match='request_conflict'):
        delivery.cancel_control(state, **{**args, 'request_id': message['request_id']})


def test_promotion_control_uuid_conflicts_with_cancellation_and_preserves_prose():
    state, message = state_with_message()
    args = dict(request_id=uid(), message_request_id=message['request_id'], target_task_id='task',
        expected_generation=0, expected_turn_id='turn', expected_epoch=0)
    promoted, receipt = delivery.promote_now(state, **args)
    assert receipt['ledger_epoch'] == 0
    assert promoted['messages'][0]['text'] == message['text']
    assert promoted['messages'][0]['delivery'] == 'queue'
    assert delivery.promote_now(promoted, **args) == (promoted, receipt)
    with pytest.raises(ConversationError, match='request_conflict'):
        delivery.cancel_control(promoted, **args)
    with pytest.raises(ConversationError, match='request_conflict'):
        delivery.promote_now(promoted, **{**args, 'expected_epoch': 1})


def test_expired_cancellation_receipt_never_recreates_old_operation():
    state, first = state_with_message()
    args = dict(request_id=uid(), message_request_id=first['request_id'], target_task_id='task',
        expected_generation=0, expected_turn_id='turn', expected_epoch=0)
    state, _ = delivery.cancel_control(state, **args)
    for _ in range(65):
        state, message = delivery.admit(state, request_id=uid(), text='history', delivery='queue',
            target_task_id='task', expected_epoch=state['ledger_epoch'])
        state = delivery.cancel(state, message['request_id'])
    assert state['ledger_epoch'] > 0
    with pytest.raises(ConversationError, match='request_expired'):
        delivery.cancel_control(state, **args)


def command():
    return dict(contract=CONTRACT, request_id=uid(), operation='cancel', message_request_id=uid(),
        target=dict(kind='task', task_id='task'), expected=dict(conversation_id=uid(),
            head_task_id='task', generation=0, turn_id='turn', ledger_epoch=0))


@pytest.mark.parametrize('mutate', [
    lambda c: c.update(operation='kill'), lambda c: c.update(qualified=True),
    lambda c: c.update(message_request_id='invalid'), lambda c: c['expected'].update(generation=True),
    lambda c: c['target'].update(all_workers=True), lambda c: c.pop('expected'),
])
def test_control_document_refuses_ambiguous_or_authority_bearing_intent(mutate):
    value = command()
    mutate(value)
    with pytest.raises(ConversationError):
        validate_control(value)


def test_control_file_loader_uses_same_strict_bounded_json_boundary(tmp_path):
    import json
    path = tmp_path / 'control.json'
    value = command()
    path.write_text(json.dumps(value))
    assert load_control(path) == value
    path.write_text('{"request_id":"a","request_id":"b"}')
    with pytest.raises(ConversationError):
        load_control(path)


async def test_public_cancel_replays_without_delivering_or_replacing_original_message(conversation, tmp_path):
    registry, provider = conversation
    task = await registry.start('assignment', tmp_path, backend=provider, freedom='read_only')
    turn = await provider.next_turn()
    prepared = await api.prepare_task(registry, task.task_id)
    send = dict(contract=CONTRACT, request_id=uid(), delivery='queue', text='original caller prose',
        target=dict(kind='task', task_id=task.task_id), expected=prepared['expected'])
    await api.send_task(registry, send)
    control = dict(contract=CONTRACT, request_id=uid(), operation='cancel', message_request_id=send['request_id'],
        target=send['target'], expected=send['expected'])
    receipt = await api.control_task(registry, control)
    assert receipt['status'] == 'cancelled'
    assert await api.control_task(registry, control) == receipt
    assert (await api.send_task(registry, send))['status'] == 'cancelled'
    with pytest.raises(ConversationError, match='request_conflict'):
        await api.control_task(registry, {**control, 'operation': 'promote_now'})
    assert await finish_turn(registry, task, turn, 'done') is None
    assert len(provider.history) == 1
    assert await api.control_task(registry, control) == receipt


def test_cancel_control_cannot_retract_durable_native_interrupt_intent():
    state = delivery.new_state(uid(), 'task', 'native', 0, 'logical')
    state, message = delivery.admit(state, request_id=uid(), text='now', delivery='now',
        target_task_id='task', expected_epoch=0)
    state = delivery.record_interrupt_intent(state, request_id=message['request_id'],
        native_identity=dict(session_id='native', turn_id='native-turn', generation=0, logical_turn='logical'))
    with pytest.raises(ConversationError, match='interrupt_already_issued'):
        delivery.cancel_control(state, request_id=uid(), message_request_id=message['request_id'],
            target_task_id='task', expected_generation=0, expected_turn_id='logical', expected_epoch=0)
