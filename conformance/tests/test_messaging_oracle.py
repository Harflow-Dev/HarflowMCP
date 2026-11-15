"""Independent expectations for the messaging.v1 oracle; never import engine validators or test helpers."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from conformance.oracles import (
    ConformanceError, MESSAGING_FIXTURE_SHAPES, required_capabilities, validate_messaging,
)

BUNDLE = Path(__file__).resolve().parents[2] / 'contracts' / 'v1'


def load(name):
    return json.loads((BUNDLE / name).read_bytes())


def first(shape_name, shape):
    """A document that passes, for mutation."""
    return load(shape_name), shape


def entry_doc(name):
    """Decoded copy of a task-status fixture and a reference to its first message."""
    doc = load(name)
    return doc, doc['task']['messages']


@pytest.mark.parametrize('name,shape', sorted(MESSAGING_FIXTURE_SHAPES.items()))
def test_every_mapped_fixture_passes(name, shape):
    assert (BUNDLE / name).is_file()
    validate_messaging(load(name), shape, expected_version='0.0.14' if 'messaging-' in name and name not in
                       ('ctl-messaging-queued.json',) else '0.0.4')


def test_map_covers_the_new_fixtures_and_both_legacy_specimens():
    on_disk = {path.name for path in BUNDLE.glob('ctl-messaging-*.json')} | {'ctl-workflow-messages.json'}
    assert set(MESSAGING_FIXTURE_SHAPES) == on_disk
    assert len(MESSAGING_FIXTURE_SHAPES) == 31


def test_unknown_shape_and_wrong_version_are_refused():
    doc = load('ctl-messaging-workflow-send.json')
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'nonsense')
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'send', expected_version='0.1.0')


def test_return_values():
    assert validate_messaging(load('ctl-messaging-cancel.json'), 'cancel')['status'] == 'cancelled'
    assert validate_messaging(load('ctl-messaging-send-error-too-large.json'), 'error')['code'] == 'message_too_large'
    entries = validate_messaging(load('ctl-messaging-status-delivered.json'), 'task-status')
    assert [entry['status'] for entry in entries] == ['delivered']
    detail = validate_messaging(load('ctl-messaging-workflow-detail-messages.json'), 'workflow-detail')
    assert len(detail) >= 2


# Deviating documents

def test_unknown_status_is_refused():
    doc = load('ctl-messaging-queued-ack.json')
    doc['result']['status'] = 'consumed'
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'send')
    doc, entries = entry_doc('ctl-messaging-status-reserved.json')
    entries[0]['status'] = 'reserved'
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'task-status')


@pytest.mark.parametrize('status', ['delivered', 'cancelled'])
def test_text_on_delivered_or_cancelled_entry_is_refused(status):
    doc, entries = entry_doc('ctl-messaging-status-delivered.json')
    entries[0]['status'] = status
    entries[0]['text'] = 'body that must have been dropped'
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'task-status')


def test_non_string_text_is_refused():
    doc, entries = entry_doc('ctl-messaging-status-reserved.json')
    entries[0]['text'] = 5
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'task-status')


@pytest.mark.parametrize('refusal', [{'message': 'no code'}, {'code': '', 'message': 'x'}, {'code': 'x'},
                                     {'code': 3, 'message': 'x'}, 'continuation_refused', None])
def test_refusal_without_code_or_message_is_refused(refusal):
    doc = load('ctl-messaging-now-continuation-refused.json')
    doc['result']['refusal'] = refusal
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'send')


@pytest.mark.parametrize('value', ['later', 'NOW', '', None, 1])
def test_wrong_delivery_value_is_refused(value):
    doc = load('ctl-messaging-queued-ack.json')
    doc['result']['delivery'] = value
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'send')


@pytest.mark.parametrize('field', ['reason', 'note', 'target_status', 'successor_task_id', 'delivery_task_id'])
def test_non_string_text_fields_are_refused(field):
    doc = load('ctl-messaging-queued-ack.json')
    doc['result'][field] = 7
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'send')
    doc, entries = entry_doc('ctl-messaging-status-delivery-unknown.json')
    entries[0][field] = ['x']
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'task-status')


@pytest.mark.parametrize('shape,name', [('send', 'ctl-messaging-queued-ack.json'), ('cancel', 'ctl-messaging-cancel.json')])
def test_missing_or_empty_message_id_is_refused(shape, name):
    doc = load(name)
    del doc['result']['message_id']
    with pytest.raises(ConformanceError):
        validate_messaging(doc, shape)
    doc['result']['message_id'] = ''
    with pytest.raises(ConformanceError):
        validate_messaging(doc, shape)


def test_entry_without_id_or_integer_seq_is_refused():
    doc, entries = entry_doc('ctl-messaging-status-reserved.json')
    del entries[0]['id']
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'task-status')
    doc, entries = entry_doc('ctl-messaging-status-reserved.json')
    entries[0]['seq'] = True
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'task-status')


def test_non_boolean_flags_are_refused():
    for field, name in (('target_settled', 'ctl-messaging-queued-target-settled.json'),
                        ('delivery_started', 'ctl-messaging-workflow-now-delivery-started.json')):
        doc = load(name)
        doc['result'][field] = 'yes'
        with pytest.raises(ConformanceError):
            validate_messaging(doc, 'send')
    doc = load('ctl-messaging-workflow-status-truncated.json')
    doc['result']['messages_truncated'] = 1
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'workflow-messages')


def test_cancel_must_report_cancelled_without_text():
    doc = load('ctl-messaging-cancel.json')
    doc['result']['status'] = 'queued'
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'cancel')
    doc = load('ctl-messaging-cancel.json')
    doc['result']['text'] = 'leaked'
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'cancel')


def test_error_shape_requires_error_and_no_result():
    doc = load('ctl-messaging-send-error-too-large.json')
    del doc['error']['code']
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'error')
    doc = load('ctl-messaging-send-error-too-large.json')
    doc['result'] = {}
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'error')
    doc = load('ctl-messaging-send-error-too-large.json')
    doc['error']['data'] = 'text'
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'error')
    with pytest.raises(ConformanceError):
        validate_messaging(load('ctl-messaging-send-error-too-large.json'), 'send')


def test_task_status_without_messages_list_is_refused():
    doc = load('ctl-messaging-status-reserved.json')
    doc['task']['messages'] = {}
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'task-status')


def test_invalid_target_kind_is_refused():
    doc, entries = entry_doc('ctl-messaging-status-reserved.json')
    entries[0]['target']['kind'] = 'conversation'
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'task-status')


def test_detail_page_digest_and_shape_checks():
    doc = load('ctl-messaging-workflow-detail-messages.json')
    doc['result']['content_sha256'] = '0' * 64
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'workflow-detail')
    doc = load('ctl-messaging-workflow-detail-messages.json')
    doc['result']['view'] = 'executions'
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'workflow-detail')
    # An entry violating the lifecycle inside a correctly hashed chunk is still refused.
    doc = load('ctl-messaging-workflow-detail-messages.json')
    chunk = json.loads(doc['result']['chunk'])
    chunk[0]['status'] = 'bogus'
    raw = json.dumps(chunk, separators=(',', ':'))
    doc['result']['chunk'] = raw
    doc['result']['total_characters'] = len(raw)
    doc['result']['content_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
    with pytest.raises(ConformanceError):
        validate_messaging(doc, 'workflow-detail')


def test_partial_detail_page_is_not_decoded():
    doc = load('ctl-messaging-workflow-detail-messages.json')
    doc['result']['next_cursor'] = 'cursor'
    doc['result']['chunk'] = doc['result']['chunk'][:10]
    assert validate_messaging(doc, 'workflow-detail') is None


def test_unknown_optional_fields_are_tolerated():
    doc = load('ctl-messaging-queued-ack.json')
    doc['result']['future_field'] = {'nested': [1]}
    validate_messaging(doc, 'send')
    doc, entries = entry_doc('ctl-messaging-status-queued-cancelled.json')
    entries[0]['future_field'] = 'x'
    entries[1]['another'] = None
    validate_messaging(doc, 'task-status')
    doc = load('ctl-messaging-send-error-too-large.json')
    doc['error']['data'] = {'anything': 1}
    validate_messaging(doc, 'error')


# Exclusions: other vocabularies are not this oracle's business

def test_oracle_is_applied_only_to_named_fixtures():
    names = set(MESSAGING_FIXTURE_SHAPES)
    conversation = sorted(path.name for path in BUNDLE.glob('ctl-conversation-*.json'))
    assert conversation and not names & set(conversation)
    for name in conversation:
        assert 'conversation' not in MESSAGING_FIXTURE_SHAPES.get(name, '')


def test_conversation_delivery_vocabulary_and_queued_followups_would_not_pass_as_entries():
    """`consumed`, `claimed`, `unknown` are other ledgers' statuses; the map must never route them here."""
    for name in sorted(path.name for path in BUNDLE.glob('ctl-conversation-*.json')):
        doc = load(name)
        for item in (doc.get('result') or {}).get('messages', []) if isinstance(doc.get('result'), dict) else []:
            if isinstance(item, dict) and item.get('status') in {'claimed', 'consumed', 'unknown'}:
                with pytest.raises(ConformanceError):
                    from conformance.oracles import validate_message_entry
                    validate_message_entry(item)
    for name in ('ctl-messaging-status-reserved.json', 'ctl-messaging-status-delivered.json'):
        followups = load(name)['task']['queued_followups']['items']
        statuses = {item['status'] for item in followups}
        assert statuses & {'reserved', 'consumed'}


def test_status_fixtures_validate_messages_not_queued_followups():
    doc = load('ctl-messaging-status-delivered.json')
    doc['task']['queued_followups']['items'][0]['status'] = 'consumed-but-unrecognised'
    validate_messaging(doc, 'task-status')


# Capability gating

@pytest.mark.parametrize('command', ['workflow-send', 'workflow-cancel-message', 'send', 'cancel-message'])
def test_messaging_commands_require_messaging_capability(command):
    assert required_capabilities([command, '--x']) == ('ctl.v1', 'messaging.v1')
    assert required_capabilities([command]) == ('ctl.v1', 'messaging.v1')


def test_other_commands_do_not_require_messaging_capability():
    assert required_capabilities(['status']) == ('ctl.v1',)
    assert required_capabilities([]) == ('ctl.v1',)
