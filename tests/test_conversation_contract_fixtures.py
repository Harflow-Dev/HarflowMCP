"""Canonical proposed specimens use actual transitions and public projection."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import runpy

from harflow_mcp.backends import conversation_qualification
from harflow_mcp.conversation_documents import MAX_HISTORY, MAX_OUTSTANDING
from harflow_mcp.conversation_requests import validate_control, validate_send
from harflow_mcp.tasks import TaskRegistry
from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, validate_envelope

ROOT = Path(__file__).parents[1]
GENERATOR = ROOT / 'scripts/generate-conversation-fixtures.py'


def generator():
    return runpy.run_path(str(GENERATOR))


def documents():
    return {name: item[0] for name, item in generator()['specimens']().items()}


def test_proposed_bundle_is_byte_deterministic_without_mutating_canonical_contracts(tmp_path):
    module = generator()
    canonical_manifest = ROOT / 'contracts/v1/manifest.json'
    old = canonical_manifest.read_bytes()
    accepted = deepcopy(conversation_qualification.ACCEPTED_PROFILES)
    manifest = {'files': {}, 'cases': {}}
    first = module['generate'](bundle=tmp_path, manifest=manifest)
    second = module['generate'](bundle=tmp_path, manifest=manifest)
    assert first == second
    assert manifest == {'files': {}, 'cases': {}}
    assert not list(tmp_path.iterdir())
    assert canonical_manifest.read_bytes() == old
    assert conversation_qualification.ACCEPTED_PROFILES == accepted
    proposed = json.loads(first[tmp_path / 'manifest.json'])
    assert len(proposed['files']) == 33
    assert set(proposed['files']) == set(proposed['cases'])
    for name, digest in proposed['files'].items():
        assert hashlib.sha256(first[tmp_path / name]).hexdigest() == digest
    digest = hashlib.sha256(first[tmp_path / 'manifest.json']).hexdigest()
    assert first[tmp_path / 'manifest.sha256'] == (digest + '  manifest.json\n').encode()


def test_wire_envelopes_and_synthetic_provenance_never_claim_provider_qualification():
    module = generator()
    for name, (document, code, provenance) in module['specimens']().items():
        validate_envelope(document, CTL_FORMAT, live=True, expected_version=PRODUCT_VERSION)
        assert code == (1 if 'refusal-' in name else 0)
        assert provenance['synthetic'] is True
        assert provenance['runtime_qualification'] is False
        encoded = module['canonical'](document).decode()
        assert module['PRIVATE_NATIVE_CONTEXT'] not in encoded
        for private in ('"claim_id"', '"native_context_id"', '"session_id"', '"pid"', '"qualified"'):
            assert private not in encoded
        assert '/Users/' not in encoded and '/private/' not in encoded
    proof = module['specimens']()['ctl-conversation-consumed-synthetic.json'][2]
    assert 'synthetic correlated consumption receipt' in proof['controlled_facts']
    assert 'no runtime/provider qualification' in proof['controlled_facts']
    assert len(proof['controlled_facts'].encode()) <= 512


def test_raw_request_examples_are_validated_but_not_fictitious_manifest_responses():
    module = generator()
    examples = module['requests']()
    assert set(examples) == {'queue', 'now', 'cancel', 'promote_now'}
    for name in ('queue', 'now'):
        assert validate_send(examples[name]) is examples[name]
    for name in ('cancel', 'promote_now'):
        assert validate_control(examples[name]) is examples[name]
        assert examples[name]['message_request_id'] == examples['queue']['request_id']
    assert examples['queue']['text'] == 'Keep  two spaces.\nFollow-up prose stays verbatim.'
    assert not any('request-document' in name or name.startswith('request-') for name in module['specimens']())


def test_fifo_now_unknown_and_correlated_consumption_are_distinct_public_states():
    cases = documents()
    queued = cases['ctl-conversation-queue-fifo.json']['result']
    claimed = cases['ctl-conversation-now-claimed.json']['result']
    unknown = cases['ctl-conversation-delivery-unknown.json']['result']
    consumed = cases['ctl-conversation-consumed-synthetic.json']['result']
    assert [message['status'] for message in queued['messages']] == ['queued', 'queued']
    assert [message['text'] for message in claimed['messages'][:2]] == [message['text'] for message in queued['messages']]
    assert [message['status'] for message in claimed['messages']] == ['queued', 'queued', 'claimed']
    assert claimed['messages'][-1]['destination']['origin'] == 'now_delivery'
    assert 'receipt' not in claimed['messages'][-1]
    assert unknown['messages'][-1]['status'] == 'unknown'
    assert 'receipt' not in unknown['messages'][-1]
    assert consumed['messages'][-1]['status'] == 'consumed'
    assert consumed['messages'][-1]['receipt']['generation'] == consumed['expected']['generation'] == 2
    assert consumed['messages'][-1]['receipt']['task_id'] == consumed['messages'][-1]['destination']['task_id']
    assert consumed['accounting']['settled_responses'] == 2
    assert consumed['accounting']['usage_complete'] is False
    assert consumed['accounting']['observed_cost_usd'] is None


def test_replays_controls_bounds_and_stable_refusal_codes():
    cases = documents()
    for original, replay in (('queue-fifo', 'queue-replayed'), ('cancelled', 'cancel-replayed'), ('promoted-now', 'promotion-replayed')):
        assert cases['ctl-conversation-' + original + '.json'] == cases['ctl-conversation-' + replay + '.json']
    promoted = cases['ctl-conversation-promoted-now.json']['result']['messages'][0]
    assert promoted['delivery'] == 'queue' and promoted['promotion_request_id']
    cancelled = cases['ctl-conversation-cancelled.json']['result']['messages'][0]
    assert cancelled['status'] == 'cancelled' and cancelled['cancellation_request_id']
    bounded = cases['ctl-conversation-bounded-window.json']['result']
    assert len(bounded['messages']) == MAX_HISTORY + MAX_OUTSTANDING
    assert sum(item['status'] == 'queued' for item in bounded['messages']) == MAX_OUTSTANDING
    assert bounded['expected']['ledger_epoch'] > 0
    assert len(json.dumps(bounded).encode()) < 256 * 1024
    for suffix, code in (('now-busy', 'now_busy'), ('queue-full', 'queue_full'),
                         ('request-expired', 'request_expired'), ('target-stale', 'target_stale'),
                         ('request-conflict', 'request_conflict'), ('cancel-issued', 'already_claimed'),
                         ('conversation-unknown', 'conversation_unknown'),
                         ('tool-interruption-unqualified', 'tool_interruption_unqualified')):
        assert cases['ctl-conversation-refusal-' + suffix + '.json']['error']['code'] == code


def test_generator_has_no_agent_launch_or_runtime_discovery_seam(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('fixture generation cannot launch or qualify a runtime')
    monkeypatch.setattr(TaskRegistry, 'start', forbidden)
    monkeypatch.setattr(TaskRegistry, 'resume', forbidden)
    monkeypatch.setattr(TaskRegistry, 'resume_record', forbidden)
    monkeypatch.setattr(conversation_qualification, 'identify_runtime', forbidden)
    assert len(generator()['specimens']()) == 33


def test_command_acknowledgements_are_compact_receipts_not_prepare_projections():
    cases = documents()
    send = cases['ctl-conversation-send-ack.json']['result']
    assert set(send) == {'contract', 'conversation_id', 'message_id', 'task_id',
                         'resolved_task_id', 'status', 'delivery', 'sequence'}
    assert send['status'] == 'queued' and send['delivery'] == 'queue'
    for name, status in [('cancel', 'cancelled'), ('promote', 'promoted')]:
        ack = cases['ctl-conversation-' + name + '-ack.json']['result']
        assert ack['status'] == status and ack['control']['request_id'] == ack['request_id']
        assert ack['message_id'] == send['message_id']
        assert 'messages' not in ack and 'expected' not in ack
    workflow = cases['ctl-conversation-workflow-send-ack.json']['result']
    assert workflow['message_id'] == send['message_id'] and workflow['status'] == 'queued'
    assert {'workflow_run_id', 'activation_id'} <= set(workflow)
    for name, operation in [('cancel', 'cancel'), ('promote', 'promote_now')]:
        ack = cases['ctl-conversation-workflow-' + name + '-ack.json']['result']
        assert ack['operation'] == operation and ack['receipt']['message_request_id'] == send['message_id']
        assert 'messages' not in ack and 'control' not in ack


def test_task_status_readback_has_no_messages_and_cannot_erase_physical_settlement():
    cases = documents()
    available = cases['ctl-conversation-task-status-available.json']['task']
    assert available['conversation']['available'] is True
    assert available['conversation']['expected']['head_task_id'] == available['task_id']
    assert 'messages' not in available['conversation']
    unavailable = cases['ctl-conversation-task-status-unavailable.json']['task']
    assert unavailable['conversation']['available'] is False
    assert unavailable['conversation']['code'] == 'ledger_unavailable'
    assert unavailable['status'] == 'completed' and unavailable['exit_code'] == 0
    assert unavailable['output_drained'] is True
