"""messaging.v1 ctl specimens: regeneration, manifest, envelopes and the key facts each one shows."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, validate_envelope

ROOT = Path(__file__).parents[1]
BUNDLE = ROOT / 'contracts/v1'
LEGACY = {'ctl-messaging-queued.json', 'ctl-workflow-messages.json'}
RUN = 'c' * 32


def load_generator():
    spec = importlib.util.spec_from_file_location('messaging_fixtures', ROOT / 'scripts/generate-messaging-fixtures.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def generated():
    module = load_generator()
    first = module.generate()
    return module, first, module.generate()


@pytest.fixture(scope='module')
def manifest():
    return json.loads((BUNDLE / 'manifest.json').read_bytes())


def new_names():
    return sorted(path.name for path in BUNDLE.glob('ctl-messaging-*.json') if path.name not in LEGACY)


def doc(name):
    return json.loads((BUNDLE / ('ctl-messaging-' + name + '.json')).read_bytes())


def messages(name):
    return doc(name)['task']['messages']


def by_status(entries, status):
    return [entry for entry in entries if entry['status'] == status]


def test_regeneration_is_deterministic_and_byte_equal_to_checked_in_files(generated):
    module, first, second = generated
    assert first == second
    assert len(new_names()) == 29
    generated_fixtures = {path.name for path in first if path.name.startswith('ctl-messaging-')}
    assert generated_fixtures == set(new_names())
    for path, raw in first.items():
        assert path.read_bytes() == raw, path.name


def test_generator_check_mode_passes():
    done = subprocess.run([sys.executable, str(ROOT / 'scripts/generate-messaging-fixtures.py'), '--check'],
                          capture_output=True, text=True, cwd=ROOT)
    assert done.returncode == 0, done.stderr


def test_manifest_inventory_digest_and_file_hashes(manifest):
    assert len(manifest['files']) == 394 and set(manifest['files']) == set(manifest['cases'])
    raw = (BUNDLE / 'manifest.json').read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    assert digest == '7e8e151c137af933dd6bd000baa4ff2fd6afcab2dcb96614d48f8027eb42cfef'
    assert (BUNDLE / 'manifest.sha256').read_bytes() == (digest + '  manifest.json\n').encode()
    for name, expected in manifest['files'].items():
        assert hashlib.sha256((BUNDLE / name).read_bytes()).hexdigest() == expected, name


@pytest.mark.parametrize('name', new_names())
def test_manifest_case_and_envelope(name, manifest):
    case = manifest['cases'][name]
    assert case['encoding'] == 'json' and case['format'] == CTL_FORMAT
    assert case['expected_product_version'] == PRODUCT_VERSION == '0.0.14'
    assert case['required_capabilities'] == ['messaging.v1']
    assert case['valid'] is True
    assert case['exit_code'] == (1 if 'error' in json.loads((BUNDLE / name).read_bytes()) else 0)
    provenance = case['provenance']
    assert isinstance(provenance, dict)
    assert provenance['synthetic'] is True and provenance['runtime_qualification'] is False
    document = json.loads((BUNDLE / name).read_bytes())
    validate_envelope(document, CTL_FORMAT, live=True, expected_version=PRODUCT_VERSION)


def test_legacy_specimens_are_untouched(manifest):
    for name in LEGACY:
        case = manifest['cases'][name]
        assert case['expected_product_version'] == '0.0.4' and case['required_capabilities'] == []
        assert json.loads((BUNDLE / name).read_bytes())['product_version'] == '0.0.4'
        tracked = subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', 'contracts/v1/' + name], cwd=ROOT)
        assert tracked.returncode == 0


@pytest.mark.parametrize('name', new_names())
def test_no_fixture_contains_a_temp_or_home_path(name):
    text = (BUNDLE / name).read_text()
    for marker in ('/Users/', '/private/', '/var/folders', '/tmp/', '/home/', 'harflow-messaging-fixtures-'):
        assert marker not in text, marker


def test_fixtures_are_canonical_json(generated):
    module, _, _ = generated
    for name in new_names():
        document = json.loads((BUNDLE / name).read_bytes())
        assert (BUNDLE / name).read_bytes() == module.canonical(document)


# Standalone task sends and cancellation

def test_standalone_acknowledgements():
    ack = doc('queued-ack')['result']
    assert ack['status'] == 'queued' and ack['delivery'] == 'queue' and ack['seq'] == 1
    assert 'target_settled' not in ack
    settled = doc('queued-target-settled')['result']
    assert settled['status'] == 'queued' and settled['target_settled'] is True and settled['target_status'] == 'completed'


def test_cancel_and_not_cancellable_refusal():
    result = doc('cancel')['result']
    assert result['status'] == 'cancelled' and 'text' not in result
    refused = doc('cancel-refused-not-queued')
    assert refused['error']['code'] == 'not_cancellable' and 'result' not in refused


@pytest.mark.parametrize('name,code', [('send-error-superseded', 'superseded'),
                                       ('send-error-too-large', 'message_too_large'),
                                       ('send-error-too-many', 'queue_full')])
def test_send_errors_carry_stable_codes(name, code, manifest):
    document = doc(name)
    assert document['error']['code'] == code and document['error']['message']
    assert manifest['cases']['ctl-messaging-' + name + '.json']['exit_code'] == 1
    assert 'result' not in document


def test_error_exit_codes_are_one_for_every_error_fixture(manifest):
    errors = [name for name in new_names() if 'error' in json.loads((BUNDLE / name).read_bytes())]
    assert len(errors) == 4
    assert all(manifest['cases'][name]['exit_code'] == 1 for name in errors)


def test_now_refusals_and_started_continuation():
    started = doc('now-continuation-started')['result']
    assert started['status'] == 'delivering' and started['successor_task_id'] and 'refusal' not in started
    refused = doc('now-continuation-refused')['result']
    assert refused['status'] == 'queued' and refused['refusal']['code'] == 'continuation_refused'
    uncertain = doc('now-continuation-uncertain')['result']
    assert uncertain['status'] == 'delivering' and uncertain['refusal']['code'] == 'continuation_uncertain'
    checkpoint = doc('now-checkpoint-required')['result']
    assert checkpoint['status'] == 'queued' and checkpoint['refusal']['code'] == 'workflow_checkpoint_required'
    assert checkpoint['workflow_run_id'] == RUN
    for result in (started, refused, uncertain, checkpoint):
        assert result['delivery'] == 'now'


# Task status message lifecycles

def test_status_queued_and_cancelled_entries():
    entries = messages('status-queued-cancelled')
    assert [entry['status'] for entry in entries] == ['queued', 'cancelled']
    assert entries[0]['text'] and 'text' not in entries[1]


def test_status_reserved_is_delivering_with_successor():
    (entry,) = messages('status-reserved')
    assert entry['status'] == 'delivering' and entry['successor_task_id'] and entry['text']


def test_status_delivery_unknown_is_delivering_with_reason():
    (entry,) = messages('status-delivery-unknown')
    assert entry['status'] == 'delivering'
    assert isinstance(entry['reason'], str) and entry['reason']


def test_status_delivered_keeps_no_text():
    (entry,) = messages('status-delivered')
    assert entry['status'] == 'delivered' and 'text' not in entry and entry['successor_task_id']


def test_status_superseded_is_undeliverable_with_reason():
    (entry,) = messages('status-superseded')
    assert entry['status'] == 'undeliverable' and 'bbbbbbbb' in entry['reason']


def test_status_workflow_node_carries_delivery_task_id():
    (entry,) = messages('status-workflow-node')
    assert entry['status'] == 'delivering' and entry['delivery_task_id'] == 'e' * 32
    assert entry['target']['kind'] == 'task' and entry['target']['workflow_run_id'] == RUN


def test_status_workflow_orchestrator_has_delivered_task_and_workflow_targets():
    entries = messages('status-workflow-orchestrator')
    assert [entry['status'] for entry in entries] == ['delivered', 'delivered']
    assert [entry['target']['kind'] for entry in entries] == ['task', 'workflow']
    assert all('text' not in entry and entry['delivery_task_id'] for entry in entries)


# Workflow messages

def test_workflow_send_and_cancel():
    sent = doc('workflow-send')['result']
    assert sent['status'] == 'queued' and sent['delivery'] == 'queue' and sent['workflow_run_id'] == RUN
    cancelled = doc('workflow-cancel')['result']
    assert cancelled['status'] == 'cancelled' and 'text' not in cancelled and cancelled['workflow_run_id'] == RUN


@pytest.mark.parametrize('name,status', [('workflow-queued-target-settled-cancelled', 'cancelled'),
                                         ('workflow-queued-target-settled-completed', 'completed')])
def test_workflow_settled_runs_report_undeliverable_target_settled(name, status):
    result = doc(name)['result']
    assert result['status'] == 'undeliverable' and result['target_settled'] is True
    assert result['target_status'] == status and result['reason']


@pytest.mark.parametrize('name', ['workflow-queued-failed-run', 'workflow-queued-completed-builder'])
def test_workflow_failed_run_and_completed_builder_still_accept(name):
    result = doc(name)['result']
    assert result['status'] == 'queued' and 'target_settled' not in result and 'target_status' not in result


def test_workflow_now_terminal_and_delivery_started():
    terminal = doc('workflow-now-terminal')['result']
    assert terminal['status'] == 'undeliverable' and terminal['refusal']['code'] == 'workflow_terminal'
    started = doc('workflow-now-delivery-started')['result']
    assert started['status'] == 'queued' and started['delivery_started'] is True and 'refusal' not in started


def test_workflow_status_undeliverable_entry():
    result = doc('workflow-status-undeliverable')['result']
    (entry,) = result['messages']
    assert entry['status'] == 'undeliverable' and entry['reason'] and entry['target']['kind'] == 'task'


def test_workflow_status_truncated_flag_and_bounded_messages():
    result = doc('workflow-status-truncated')['result']
    assert result['messages_truncated'] is True
    assert 0 < len(result['messages']) < 10
    seqs = [entry['seq'] for entry in result['messages']]
    assert seqs == sorted(seqs) and seqs[-1] > len(seqs)
    assert all(entry['status'] == 'undeliverable' for entry in result['messages'])


def test_workflow_detail_page_is_a_complete_lossless_messages_page():
    result = doc('workflow-detail-messages')['result']
    assert result['view'] == 'messages' and result['encoding'] == 'json' and result['response_version'] == 1
    assert result['next_cursor'] is None and result['offset'] == 0 and result['workflow_run_id'] == RUN
    assert len(result['chunk']) == result['total_characters']
    assert hashlib.sha256(result['chunk'].encode()).hexdigest() == result['content_sha256']
    entries = json.loads(result['chunk'])
    assert [entry['seq'] for entry in entries] == sorted(entry['seq'] for entry in entries) and len(entries) >= 2
    assert {entry['target']['kind'] for entry in entries} == {'workflow', 'task'}
