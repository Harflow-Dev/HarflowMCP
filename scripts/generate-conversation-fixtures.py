#!/usr/bin/env python3
"""Canonical conversation specimens from production pure transitions/projection.

No process, provider, runtime qualification, engine state mutation or permission grant
is exercised. A consumed specimen supplies explicitly synthetic correlated proof;
it does not certify any real backend. Raw caller examples remain test/docs data
and are deliberately excluded from the wire-envelope fixture manifest.
"""
from __future__ import annotations

import argparse
from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import uuid

from harflow_mcp import conversation_delivery as delivery
from harflow_mcp.conversation_api import projection, message_ack, control_ack
from harflow_mcp.conversation_workflows import message_ack as workflow_message_ack, control_ack as workflow_control_ack
from harflow_mcp.conversation_views import _summary as status_summary, _unavailable as status_unavailable
from harflow_mcp.conversation_documents import CONTRACT, ConversationError, MAX_HISTORY, MAX_OUTSTANDING
from harflow_mcp.conversation_requests import validate_control, validate_send
from harflow_mcp.ctl import _fail
from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, envelope

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'
CAPABILITY = 'conversation-delivery.v1'
PRIVATE_NATIVE_CONTEXT = 'private-synthetic-native-context'


def uid(number):
    return str(uuid.UUID(int=number))


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()


def initial():
    return delivery.new_state(uid(1), uid(10), PRIVATE_NATIVE_CONTEXT, 1, uid(11))


def witness(state):
    return {key: state[key] for key in ('conversation_id', 'head_task_id', 'generation', 'turn_id', 'ledger_epoch')}


def requests():
    """Validated caller intent examples for docs/tests, not fictitious ctl replies."""
    expected = witness(initial())
    send = dict(contract=CONTRACT, request_id=uid(101), delivery='queue',
        text='Keep  two spaces.\nFollow-up prose stays verbatim.',
        target=dict(kind='task', task_id=uid(10)), expected=expected)
    now = {**deepcopy(send), 'request_id': uid(103), 'delivery': 'now', 'text': 'Handle this response first.'}
    cancel = dict(contract=CONTRACT, request_id=uid(201), operation='cancel', message_request_id=uid(101),
        target=deepcopy(send['target']), expected=deepcopy(expected))
    promote = {**deepcopy(cancel), 'request_id': uid(202), 'operation': 'promote_now'}
    return {'queue': validate_send(send), 'now': validate_send(now),
        'cancel': validate_control(cancel), 'promote_now': validate_control(promote)}


def admit(state, request):
    expected = request['expected']
    return delivery.admit(state, request_id=request['request_id'], text=request['text'],
        delivery=request['delivery'], target_task_id=expected['head_task_id'],
        original_target_task_id=request['target']['task_id'], expected_generation=expected['generation'],
        expected_turn_id=expected['turn_id'], expected_epoch=expected['ledger_epoch'])


def control(state, request):
    expected = request['expected']
    function = delivery.cancel_control if request['operation'] == 'cancel' else delivery.promote_now
    return function(state, request_id=request['request_id'], message_request_id=request['message_request_id'],
        target_task_id=expected['head_task_id'], expected_generation=expected['generation'],
        expected_turn_id=expected['turn_id'], expected_epoch=expected['ledger_epoch'])


def specimens():
    """name -> (real wire document, ctl exit code, controlled provenance)."""
    output = {}
    def put(name, state, *, proof='pure transitions and public projection'):
        output['ctl-conversation-' + name + '.json'] = (
            envelope(CTL_FORMAT, {'result': projection(state)}), 0,
            dict(producer='conversation_api.projection', controlled_facts=proof,
                synthetic=True, runtime_qualification=False))
    def refusal(name, operation):
        try:
            operation()
        except ConversationError as exc:
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = _fail(SimpleNamespace(json=True), exc.code, str(exc))
            output['ctl-conversation-refusal-' + name + '.json'] = (
                json.loads(stdout.getvalue()), code,
                dict(producer='conversation_delivery + ctl._fail', controlled_facts='pure transition refusal',
                    synthetic=True, runtime_qualification=False))
        else:
            raise AssertionError('Expected deterministic refusal: ' + name)
    examples = requests()
    state = initial()
    put('empty', state)
    for suffix, status, conversation, settlement in (
        ('available', 'running', status_summary(state), {'exit_code': None, 'output_drained': False}),
        ('unavailable', 'completed', status_unavailable('ledger_unavailable', state['conversation_id']),
         {'exit_code': 0, 'output_drained': True}),
    ):
        output['ctl-conversation-task-status-' + suffix + '.json'] = (
            envelope(CTL_FORMAT, {'task': {'task_id': state['head_task_id'], 'status': status,
                                         'conversation': conversation, **settlement}}), 0,
            dict(producer='conversation_views status presentation',
                 controlled_facts='synthetic minimal task envelope with production bounded status readback; physical settlement independent',
                 synthetic=True, runtime_qualification=False))
    queue, first = admit(state, examples['queue'])
    second_request = {**deepcopy(examples['queue']), 'request_id': uid(102), 'text': 'Second FIFO message.'}
    queue, second = admit(queue, second_request)
    def ack(name, result, producer):
        output['ctl-conversation-' + name + '.json'] = (
            envelope(CTL_FORMAT, {'result': result}), 0,
            dict(producer=producer, controlled_facts='synthetic command admission receipt; not consumption',
                 synthetic=True, runtime_qualification=False))
    ack('send-ack', message_ack(state['conversation_id'], uid(10), first),
        'conversation_api.message_ack')
    ack('workflow-send-ack', {'contract': CONTRACT, **workflow_message_ack(uid(20), uid(21),
        state['conversation_id'], uid(10), first)}, 'conversation_workflows.message_ack')
    put('queue-fifo', queue)
    replayed, replay_receipt = admit(queue, examples['queue'])
    assert replayed == queue and replay_receipt == first
    put('queue-replayed', replayed)
    now, selected = admit(queue, examples['now'])
    put('now-queued', now)
    refusal('now-busy', lambda: admit(now, {**deepcopy(examples['now']), 'request_id': uid(104)}))
    intent = delivery.record_interrupt_intent(now, request_id=selected['request_id'], native_identity=dict(
        session_id=PRIVATE_NATIVE_CONTEXT, turn_id='synthetic-native-turn', generation=1, logical_turn=uid(11)))
    intent = delivery.record_progress(intent, generation=1, logical_turn=uid(11), phase='interrupting')
    put('interrupt-intent', intent, proof='synthetic exact native interruption correlation before any I/O')
    boundary = delivery.record_response(now, generation=1, turn_id=uid(11))
    boundary = delivery.observe_boundary(boundary, generation=1, turn_id=uid(11))
    claimed, claim = delivery.claim_next(boundary, claim_id=uid(301),
        destination_task_id=uid(10), destination_turn_id=uid(12))
    assert claim['request_id'] == selected['request_id']
    put('now-claimed', claimed)
    unknown = delivery.mark_unknown(claimed, claim_id=uid(301))
    assert delivery.claim_next(unknown, claim_id=uid(302))[1] is None
    put('delivery-unknown', unknown)
    # Synthetic owner proof is deliberately correlated to the reserved destination.
    # No provider acceptance, tool settlement or OS process exit is invented here.
    synthetic_receipt = dict(claim=claim['claim'], task_id=uid(10), turn_id=uid(12), generation=2, kind='input_consumed')
    consumed = delivery.record_receipt(claimed, receipt=synthetic_receipt)
    consumed = delivery.observe_boundary(consumed, generation=2, turn_id=uid(12))
    consumed = delivery.record_progress(consumed, generation=2, logical_turn=uid(12), phase='boundary', native_terminal='completed')
    put('consumed-synthetic', consumed,
        proof='explicitly supplied synthetic correlated consumption receipt; usage unavailable; no runtime/provider qualification')
    reconciled = delivery.record_receipt(unknown, receipt=synthetic_receipt)
    put('unknown-reconciled-synthetic', reconciled,
        proof='synthetic exact reserved-destination receipt reconciles uncertainty without issuing again')
    cancelled, cancel_receipt = control(queue, examples['cancel'])
    ack('cancel-ack', control_ack(examples['cancel'], uid(10), cancel_receipt),
        'conversation_api.control_ack')
    ack('workflow-cancel-ack', {'contract': CONTRACT, **workflow_control_ack(uid(20), uid(21),
        state['conversation_id'], 'cancel', cancel_receipt)}, 'conversation_workflows.control_ack')
    put('cancelled', cancelled)
    replayed_cancel, repeated_cancel = control(cancelled, examples['cancel'])
    assert repeated_cancel == cancel_receipt and replayed_cancel == cancelled
    put('cancel-replayed', replayed_cancel)
    promoted, promotion_receipt = control(queue, examples['promote_now'])
    ack('promote-ack', control_ack(examples['promote_now'], uid(10), promotion_receipt),
        'conversation_api.control_ack')
    ack('workflow-promote-ack', {'contract': CONTRACT, **workflow_control_ack(uid(20), uid(21),
        state['conversation_id'], 'promote_now', promotion_receipt)}, 'conversation_workflows.control_ack')
    put('promoted-now', promoted)
    replayed_promotion, repeated_promotion = control(promoted, examples['promote_now'])
    assert repeated_promotion == promotion_receipt and replayed_promotion == promoted
    put('promotion-replayed', replayed_promotion)
    refusal('request-conflict', lambda: control(cancelled, {**deepcopy(examples['cancel']), 'operation': 'promote_now'}))
    refusal('cancel-issued', lambda: control(claimed, {**deepcopy(examples['cancel']),
        'message_request_id': selected['request_id'], 'expected': witness(claimed)}))
    waiting = delivery.record_progress(initial(), generation=1, logical_turn=uid(11), phase='waiting_tools',
        native_terminal='interrupted', pending_tools=1, unsettled_effects=1)
    put('waiting-tool-settlement', waiting, proof='synthetic native progress; protocol interruption is not OS/tool settlement')
    refusal('tool-interruption-unqualified', lambda: admit(waiting, examples['now']))
    faulted = delivery.mark_runtime_fault(initial(), code='synthetic_native_unknown', generation=1, turn_id=uid(11))
    put('runtime-unknown', faulted, proof='synthetic owner fault pins the conversation without admitting any message')
    refusal('conversation-unknown', lambda: admit(faulted, examples['queue']))
    closed = delivery.close(delivery.observe_boundary(initial(), generation=1, turn_id=uid(11)))
    put('closed', closed)
    refusal('target-stale', lambda: admit(closed, examples['queue']))
    window = initial()
    old_request = None
    for index in range(MAX_HISTORY + 1):
        command = {**deepcopy(examples['queue']), 'request_id': uid(1000 + index), 'text': 'Historical message ' + str(index), 'expected': witness(window)}
        if index == 0:
            old_request = deepcopy(command)
        window, item = admit(window, command)
        window = delivery.cancel(window, item['request_id'])
    refusal('request-expired', lambda: admit(window, old_request))
    for index in range(MAX_OUTSTANDING):
        command = {**deepcopy(examples['queue']), 'request_id': uid(2000 + index), 'text': 'Pending message ' + str(index), 'expected': witness(window)}
        window, _ = admit(window, command)
    assert len(window['messages']) == MAX_HISTORY + MAX_OUTSTANDING and window['ledger_epoch'] > 0
    put('bounded-window', window, proof='real bounded history compaction and maximum pending count; old epoch expires')
    refusal('queue-full', lambda: admit(window, {**deepcopy(examples['queue']), 'request_id': uid(3000), 'expected': witness(window)}))
    return output


def generate(*, bundle=BUNDLE, manifest=None):
    """Return proposed canonical bytes without touching any file."""
    bundle = Path(bundle)
    manifest = deepcopy(manifest if manifest is not None else json.loads((BUNDLE / 'manifest.json').read_text()))
    output = {}
    for name, (document, code, provenance) in specimens().items():
        output[bundle / name] = canonical(document)
        manifest['files'][name] = hashlib.sha256(output[bundle / name]).hexdigest()
        manifest['cases'][name] = dict(encoding='json', expected_product_version=PRODUCT_VERSION,
            format=CTL_FORMAT, required_capabilities=[CAPABILITY], valid=True, exit_code=code,
            provenance=provenance)
    output[bundle / 'manifest.json'] = (json.dumps(manifest, indent=2, ensure_ascii=False) + '\n').encode()
    output[bundle / 'manifest.sha256'] = (hashlib.sha256(output[bundle / 'manifest.json']).hexdigest() + '  manifest.json\n').encode()
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Compare canonical bytes without writing')
    parser.add_argument('--output', type=Path, default=BUNDLE, help='Staging directory for owned conversation specimens plus proposed manifest; unrelated fixtures are not copied')
    args = parser.parse_args()
    outputs = generate(bundle=args.output)
    if args.check:
        different = [str(path.name) for path, raw in outputs.items() if not path.exists() or path.read_bytes() != raw]
        if different:
            raise SystemExit('Conversation fixtures differ: ' + ', '.join(different))
    else:
        args.output.mkdir(parents=True, exist_ok=True)
        for path, raw in outputs.items():
            path.write_bytes(raw)


if __name__ == '__main__':
    main()
