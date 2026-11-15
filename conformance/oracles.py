"""Independent, standard-library wire and fixture checks. No engine imports."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from datetime import datetime

PINNED_MANIFEST_SHA256 = 'bc4d4cf5d8caaab46020188075e68854e50cd16bfc77bab56fb25bbeb51af8c8'

class ConformanceError(ValueError):
    """Evidence is missing, unsafe, or incompatible."""

def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ConformanceError('duplicate JSON key')
            result[key] = value
        return result
    try:
        return json.loads(data, object_pairs_hook=pairs,
                          parse_constant=lambda value: (_ for _ in ()).throw(ConformanceError('nonfinite JSON')))
    except (ValueError, UnicodeError) as exc:
        raise ConformanceError(str(exc)) from exc

def _version(value):
    if not isinstance(value, str) or len(value.encode()) > 256:
        raise ConformanceError('invalid product version')
    match = re.fullmatch(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:\+[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*)?', value)
    if not match or any(int(x) > 2147483647 for x in match.groups()):
        raise ConformanceError('invalid product version')
    return tuple(int(x) for x in match.groups())

def validate_envelope(doc, format, expected_version='0.0.13'):
    if not isinstance(doc, dict) or doc.get('product') != 'harflow' or doc.get('format') != format or type(doc.get('v')) is not int or doc['v'] != 1:
        raise ConformanceError('incompatible wire envelope')
    version = _version(doc.get('product_version'))
    if format != 'harflow.event' and expected_version is not None and version[:2] != _version(expected_version)[:2]:
        raise ConformanceError('incompatible product line')
    return doc

def validate_version_info(doc, root, required_capabilities=(), expected_os=None, expected_arch=None, expected_version='0.0.13'):
    validate_envelope(doc, 'harflow.version-info', expected_version)
    if doc.get('component') != 'engine' or doc.get('state_root') != str(root) or not os.path.isabs(str(root)) or 'error' in doc:
        raise ConformanceError('incompatible engine/root')
    if not isinstance(doc.get('contracts'), dict):
        raise ConformanceError('invalid contracts')
    for key, fmt in [('ctl', 'harflow.ctl'), ('setup', 'harflow.setup'), ('events', 'harflow.event')]:
        row = doc.get('contracts', {}).get(key, {})
        if not isinstance(row, dict) or row.get('format') != fmt or type(row.get('v')) is not int or row['v'] != 1:
            raise ConformanceError('incompatible contracts')
    caps = doc.get('capabilities')
    if not isinstance(caps, list) or any(not isinstance(x, str) for x in caps) or not set(required_capabilities).issubset(caps):
        raise ConformanceError('missing capabilities')
    for key, expected in [('os', expected_os), ('architecture', expected_arch)]:
        if not isinstance(doc.get(key), str) or not doc[key] or (expected is not None and doc[key] != expected):
            raise ConformanceError('incompatible platform')
    return doc

def read_regular(path):
    path = Path(path)
    for parent in [path, *path.parents]:
        if parent.is_symlink():
            raise ConformanceError('symlink source')
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        raise ConformanceError('unavailable regular source: ' + str(path)) from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ConformanceError('nonregular source')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            data = stream.read()
        after = os.fstat(fd)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) or path.stat().st_ino != after.st_ino:
            raise ConformanceError('source changed')
        return data
    finally:
        os.close(fd)

def verify_manifest(path, expected_sha256=PINNED_MANIFEST_SHA256):
    if expected_sha256 != PINNED_MANIFEST_SHA256:
        raise ConformanceError('manifest is not independently pinned')
    path = Path(path)
    data = read_regular(path)
    if hashlib.sha256(data).hexdigest() != expected_sha256:
        raise ConformanceError('manifest digest mismatch')
    manifest = strict_json(data)
    files = manifest.get('files')
    if not isinstance(files, dict) or len(files) != 381 or set(manifest.get('cases', {})) != set(files):
        raise ConformanceError('incomplete fixture inventory')
    for name, digest in files.items():
        if not isinstance(name, str) or Path(name).name != name or name in ('.', '..') or '\\' in name or not re.fullmatch('[a-f0-9]{64}', digest):
            raise ConformanceError('unsafe fixture entry')
        raw = read_regular(path.parent / name)
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ConformanceError('fixture digest mismatch: ' + name)
        # Invalid fixtures deliberately contain malformed JSON; their bytes still must match.
        case = manifest['cases'][name]
        if case.get('encoding') == 'json' and case.get('valid'):
            strict_json(raw)
    return manifest


CANCEL_REFUSAL_REASONS = frozenset({'workflow_native_control'})


def validate_cancel_refusal(doc, expected_version='0.0.14'):
    """Permissive shape of a refused `cancel` reply (an operation refusal, not an observation).

    `error.data` and `error.data.reason` are optional. An unknown reason is accepted as a generic
    refusal; consumers select special handling only from a known reason.
    """
    validate_envelope(doc, 'harflow.ctl', expected_version)
    error = doc.get('error')
    if 'result' in doc or not isinstance(error, dict) or not isinstance(error.get('code'), str) or not isinstance(error.get('message'), str):
        raise ConformanceError('invalid cancel refusal')
    if 'data' in error:
        data = error['data']
        if not isinstance(data, dict):
            raise ConformanceError('invalid cancel refusal data')
        if 'reason' in data and (not isinstance(data['reason'], str) or not data['reason']):
            raise ConformanceError('invalid cancel refusal reason')
    return error


def validate_cancel_observation(doc):
    validate_envelope(doc, 'harflow.ctl')
    value = doc.get('result', {}).get('cascade', {}).get('observation')
    if (not isinstance(value, dict) or set(value) != {'scope', 'observed_at', 'coverage', 'truncated', 'items'}
            or value['scope'] != 'recorded_tasks_and_pending_launches'
            or value['coverage'] not in {'complete', 'incomplete', 'unknown'}
            or type(value['truncated']) is not bool or not isinstance(value['items'], list)
            or len(value['items']) > 100 or len(json.dumps(value).encode('utf-8')) > 65536):
        raise ConformanceError('invalid cancellation observation')
    try:
        if datetime.fromisoformat(value['observed_at']).utcoffset() is None:
            raise ValueError('missing timezone')
    except (TypeError, ValueError) as exc:
        raise ConformanceError('invalid observation timestamp') from exc
    if value['coverage'] == 'unknown' and (value['items'] or value['truncated']):
        raise ConformanceError('invalid observation fallback')
    identifiers = []
    for row in value['items']:
        if (not isinstance(row, dict) or set(row) != {'task_id', 'state', 'basis', 'status'}
                or not isinstance(row['task_id'], str) or not row['task_id']
                or row['state'] not in {'stopped', 'running', 'unknown'}
                or row['basis'] not in {'observed_settlement', 'confirmed_live_identity', 'proven_not_started', 'insufficient_evidence'}
                or row['status'] not in {None, 'running', 'completed', 'failed', 'cancelled', 'timed_out'}):
            raise ConformanceError('invalid observation row')
        expected = {'observed_settlement': 'stopped', 'confirmed_live_identity': 'running',
                    'proven_not_started': 'stopped', 'insufficient_evidence': 'unknown'}[row['basis']]
        if row['state'] != expected:
            raise ConformanceError('inconsistent observation evidence')
        identifiers.append(row['task_id'])
    if (identifiers and identifiers[0] != doc['result']['task_id']
            or len(identifiers) != len(set(identifiers)) or identifiers[1:] != sorted(identifiers[1:])
            or value['truncated'] and value['coverage'] != 'incomplete'):
        raise ConformanceError('invalid observation ordering or coverage')
    return value


BACKEND_MODEL_STATES = {'available', 'not_installed', 'unsupported', 'unavailable'}
BACKEND_MODEL_SOURCES = {'cli_output', 'harness_cache', 'documented_aliases', 'config_aliases'}
BACKEND_MODEL_KEYS = {'backend', 'state', 'reason', 'source', 'scope', 'advisory', 'harness_version', 'source_observed_at',
                      'observed_at', 'truncated', 'dropped_count', 'models'}
BACKEND_MODEL_EFFORTS = ('low', 'medium', 'high', 'xhigh')
BACKEND_MODEL_LINE_LIMIT = 262144


def _rfc3339(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})', value):
        raise ConformanceError('invalid backend-models timestamp')
    try:
        if datetime.fromisoformat(value).utcoffset() is None:
            raise ValueError('missing timezone')
    except ValueError as exc:
        raise ConformanceError('invalid backend-models timestamp') from exc


BACKEND_MODEL_DETAILS = {'root_present_unmarked', 'lock_timeout', 'incompatible_store', 'store_unavailable',
                         'lineage_undecidable', 'detection_failed'}


def validate_backend_models_error(doc, expected_version='0.0.13'):
    """Shape of a refused `backend-models` reply: the caller gate's `agent_caller` / `caller_undecidable`."""
    validate_envelope(doc, 'harflow.ctl', expected_version)
    error = doc.get('error')
    if not isinstance(error, dict) or error.get('code') not in {'agent_caller', 'caller_undecidable'} or not isinstance(error.get('message'), str):
        raise ConformanceError('invalid backend-models refusal')
    data = error.get('data')
    if not isinstance(data, dict) or data.get('coverage') != 'unknown' or data.get('check') != 'direct_caller_required':
        raise ConformanceError('invalid backend-models refusal data')
    detail = data.get('detail')
    if set(data) - {'coverage', 'check', 'detail'}:
        raise ConformanceError('invalid backend-models refusal data')
    if error['code'] == 'agent_caller' and 'detail' in data or error['code'] == 'caller_undecidable' and (
            detail is not None and detail not in BACKEND_MODEL_DETAILS):
        raise ConformanceError('invalid backend-models refusal detail')
    return error


def validate_backend_models(doc, expected_version='0.0.13'):
    """Shape of a successful `backend-models` reply (`backend-models.v1`), checked without the engine."""
    validate_envelope(doc, 'harflow.ctl', expected_version)
    result = doc.get('result')
    if not isinstance(result, dict) or set(result) != BACKEND_MODEL_KEYS:
        raise ConformanceError('invalid backend-models result')
    if not isinstance(result['backend'], str) or not result['backend']:
        raise ConformanceError('invalid backend-models backend')
    state, reason, source = result['state'], result['reason'], result['source']
    if state not in BACKEND_MODEL_STATES:
        raise ConformanceError('invalid backend-models state')
    if (reason is not None and (not isinstance(reason, str) or not reason)) or (state == 'unavailable') != (reason is not None):
        raise ConformanceError('invalid backend-models reason')
    if source is not None and source not in BACKEND_MODEL_SOURCES:
        raise ConformanceError('invalid backend-models source')
    if result['scope'] != 'user' or result['advisory'] is not True or result['harness_version'] is not None:
        raise ConformanceError('invalid backend-models scope')
    if result['source_observed_at'] is not None:
        _rfc3339(result['source_observed_at'])
    _rfc3339(result['observed_at'])
    if type(result['truncated']) is not bool or type(result['dropped_count']) is not int or result['dropped_count'] < 0:
        raise ConformanceError('invalid backend-models counters')
    models = result['models']
    if not isinstance(models, list) or len(models) > 400 or (state != 'available' and (models or result['truncated'])):
        raise ConformanceError('invalid backend-models list')
    seen = set()
    for row in models:
        if not isinstance(row, dict) or set(row) != {'id', 'label', 'default', 'effort_levels'}:
            raise ConformanceError('invalid backend-models row')
        identifier, label = row['id'], row['label']
        if (not isinstance(identifier, str) or not identifier or len(identifier.encode('utf-8')) > 256
                or re.search(r'[\s\x00-\x1f\x7f]', identifier) or identifier in seen
                or not isinstance(label, str) or len(label.encode('utf-8')) > 256 or re.search(r'[\x00-\x1f\x7f]', label)):
            raise ConformanceError('invalid backend-models identity')
        seen.add(identifier)
        levels = row['effort_levels']
        if row['default'] not in (None, True, False) or (levels is not None and (
                not isinstance(levels, list) or len(set(levels)) != len(levels)
                or any(level not in BACKEND_MODEL_EFFORTS for level in levels)
                or levels != [level for level in BACKEND_MODEL_EFFORTS if level in levels])):
            raise ConformanceError('invalid backend-models row values')
    if len(json.dumps(doc).encode('utf-8')) > BACKEND_MODEL_LINE_LIMIT:
        raise ConformanceError('backend-models line exceeds its byte limit')
    return result


MESSAGE_STATUSES = frozenset({'queued', 'delivering', 'delivered', 'undeliverable', 'cancelled'})
MESSAGE_DELIVERIES = frozenset({'queue', 'now'})
MESSAGE_SHAPES = frozenset({'send', 'cancel', 'error', 'task-status', 'workflow-messages', 'workflow-detail'})

# The messaging.v1 fixtures, by name. The oracle is applied to exactly these, never selected by sniffing
# a `messages` key: conversation-delivery fixtures and `queued_followups` carry other vocabularies.
MESSAGING_FIXTURE_SHAPES = {
    'ctl-messaging-queued.json': 'send',
    'ctl-workflow-messages.json': 'workflow-messages',
    'ctl-messaging-queued-ack.json': 'send',
    'ctl-messaging-queued-target-settled.json': 'send',
    'ctl-messaging-cancel.json': 'cancel',
    'ctl-messaging-cancel-refused-not-queued.json': 'error',
    'ctl-messaging-status-queued-cancelled.json': 'task-status',
    'ctl-messaging-status-reserved.json': 'task-status',
    'ctl-messaging-status-delivery-unknown.json': 'task-status',
    'ctl-messaging-status-delivered.json': 'task-status',
    'ctl-messaging-status-superseded.json': 'task-status',
    'ctl-messaging-status-workflow-node.json': 'task-status',
    'ctl-messaging-status-workflow-orchestrator.json': 'task-status',
    'ctl-messaging-send-error-superseded.json': 'error',
    'ctl-messaging-send-error-too-large.json': 'error',
    'ctl-messaging-send-error-too-many.json': 'error',
    'ctl-messaging-now-continuation-started.json': 'send',
    'ctl-messaging-now-continuation-refused.json': 'send',
    'ctl-messaging-now-continuation-uncertain.json': 'send',
    'ctl-messaging-now-checkpoint-required.json': 'send',
    'ctl-messaging-workflow-send.json': 'send',
    'ctl-messaging-workflow-cancel.json': 'cancel',
    'ctl-messaging-workflow-queued-target-settled-cancelled.json': 'send',
    'ctl-messaging-workflow-queued-target-settled-completed.json': 'send',
    'ctl-messaging-workflow-queued-failed-run.json': 'send',
    'ctl-messaging-workflow-queued-completed-builder.json': 'send',
    'ctl-messaging-workflow-now-terminal.json': 'send',
    'ctl-messaging-workflow-now-delivery-started.json': 'send',
    'ctl-messaging-workflow-status-undeliverable.json': 'workflow-messages',
    'ctl-messaging-workflow-status-truncated.json': 'workflow-messages',
    'ctl-messaging-workflow-detail-messages.json': 'workflow-detail',
}


def _text_field(container, key, label):
    if key in container and not isinstance(container[key], str):
        raise ConformanceError('invalid messaging ' + label)


def _refusal(container):
    if 'refusal' in container:
        refusal = container['refusal']
        if not isinstance(refusal, dict) or not isinstance(refusal.get('code'), str) or not refusal['code'] or not isinstance(refusal.get('message'), str):
            raise ConformanceError('invalid messaging refusal')


def _lifecycle(container, label):
    if container.get('status') not in MESSAGE_STATUSES:
        raise ConformanceError('invalid messaging ' + label + ' status')
    if 'delivery' in container and container['delivery'] not in MESSAGE_DELIVERIES:
        raise ConformanceError('invalid messaging delivery')
    for key in ('message_id', 'reason', 'note', 'target_status', 'successor_task_id', 'delivery_task_id', 'task_id', 'workflow_run_id'):
        _text_field(container, key, key)
    for key in ('target_settled', 'delivery_started', 'text_truncated'):
        if key in container and type(container[key]) is not bool:
            raise ConformanceError('invalid messaging ' + key)
    _refusal(container)


def validate_message_entry(entry):
    """One message in a `messages` projection (task status or workflow run). Timestamps are not typed:
    task ledgers use ISO strings and workflow ledgers use numbers. Unknown optional fields are tolerated."""
    if not isinstance(entry, dict) or not isinstance(entry.get('id'), str) or not entry['id'] or type(entry.get('seq')) is not int:
        raise ConformanceError('invalid messaging entry')
    _lifecycle(entry, 'entry')
    if entry['status'] in {'delivered', 'cancelled'} and 'text' in entry:
        raise ConformanceError('delivered or cancelled messages keep no text')
    if 'text' in entry and not isinstance(entry['text'], str):
        raise ConformanceError('invalid messaging text')
    if 'target' in entry and (not isinstance(entry['target'], dict) or entry['target'].get('kind') not in {'task', 'workflow'}):
        raise ConformanceError('invalid messaging target')
    return entry


def validate_messaging(doc, shape, expected_version='0.0.14'):
    """Permissive shape of a messaging.v1 reply, for the explicitly named `shape` only.

    `send`: a send acknowledgement (lifecycle `status`, optional `refusal`, settled fields). `cancel`: a
    cancellation result. `error`: an error envelope (`error.code`/`error.message`; `error.data` tolerated).
    `task-status`: only the entries of `task.messages` (the task snapshot has no message lifecycle of its own).
    `workflow-messages`: only the entries of `result.messages` (the workflow's top-level `status` is the run's).
    `workflow-detail`: the `workflow-detail --view messages` page envelope and, for a complete single page,
    the decoded entries. `queued_followups` and conversation-delivery documents are out of scope.
    """
    if shape not in MESSAGE_SHAPES:
        raise ConformanceError('unknown messaging shape')
    validate_envelope(doc, 'harflow.ctl', expected_version)
    if shape == 'error':
        error = doc.get('error')
        if 'result' in doc or not isinstance(error, dict) or not isinstance(error.get('code'), str) or not error['code'] or not isinstance(error.get('message'), str):
            raise ConformanceError('invalid messaging error')
        if 'data' in error and not isinstance(error['data'], dict):
            raise ConformanceError('invalid messaging error data')
        return error
    if 'error' in doc:
        raise ConformanceError('unexpected messaging error')
    if shape == 'task-status':
        task = doc.get('task')
        messages = task.get('messages') if isinstance(task, dict) else None
        if not isinstance(messages, list):
            raise ConformanceError('invalid messaging task status')
        return [validate_message_entry(entry) for entry in messages]
    result = doc.get('result')
    if not isinstance(result, dict):
        raise ConformanceError('invalid messaging result')
    if shape in {'send', 'cancel'}:
        if not isinstance(result.get('message_id'), str) or not result['message_id']:
            raise ConformanceError('invalid messaging acknowledgement')
        _lifecycle(result, 'result')
        if 'seq' in result and type(result['seq']) is not int:
            raise ConformanceError('invalid messaging seq')
        if shape == 'cancel' and (result['status'] != 'cancelled' or 'text' in result):
            raise ConformanceError('invalid messaging cancellation')
        return result
    if shape == 'workflow-messages':
        messages = result.get('messages')
        if not isinstance(messages, list) or ('messages_truncated' in result and type(result['messages_truncated']) is not bool):
            raise ConformanceError('invalid messaging workflow messages')
        return [validate_message_entry(entry) for entry in messages]
    # workflow-detail: a lossless page; the message chunk is decoded only when it is the complete document.
    if (type(result.get('response_version')) is not int or result.get('view') != 'messages' or result.get('encoding') != 'json'
            or not isinstance(result.get('workflow_run_id'), str) or not re.fullmatch('[a-f0-9]{64}', str(result.get('content_sha256')))
            or type(result.get('offset')) is not int or result['offset'] < 0 or type(result.get('total_characters')) is not int
            or not isinstance(result.get('chunk'), str) or result.get('next_cursor') is not None and not isinstance(result['next_cursor'], str)):
        raise ConformanceError('invalid messaging detail page')
    if result['next_cursor'] is None and result['offset'] == 0:
        if len(result['chunk']) != result['total_characters'] or hashlib.sha256(result['chunk'].encode()).hexdigest() != result['content_sha256']:
            raise ConformanceError('messaging detail digest mismatch')
        entries = strict_json(result['chunk'].encode())
        if not isinstance(entries, list):
            raise ConformanceError('invalid messaging detail messages')
        return [validate_message_entry(entry) for entry in entries]
    return None


def validate_event_page(doc, expected_version='0.0.13'):
    validate_envelope(doc, 'harflow.ctl', expected_version)
    result = doc.get('result')
    if not isinstance(result, dict) or not isinstance(result.get('events'), list):
        raise ConformanceError('invalid event page')
    for event in result['events']:
        validate_envelope(event, 'harflow.event', None)
    return doc


def validate_fake_observation(observation, ledger):
    if not isinstance(observation, dict) or type(observation.get('pid')) is not int or observation.get('pid') != ledger.get('pid') or observation.get('nonce') != ledger.get('nonce'):
        raise ConformanceError('fake observation identity mismatch')
    # Standalone script sees argv after the script path, not Python switches.
    if observation.get('argv') != ledger.get('argv', [])[2:]:
        raise ConformanceError('fake observation argv mismatch')
    return observation


# Capabilities a fresh handshake must show before each public command is run (shared by the controlled profiles).
COMMAND_CAPABILITIES = {'task-events': 'task-event-page.v1', 'task-prompt': 'task-prompt.v1', 'send': 'messaging.v1',
                        'cancel-message': 'messaging.v1', 'workflow-send': 'messaging.v1',
                        'workflow-cancel-message': 'messaging.v1', 'backend-models': 'backend-models.v1'}


def required_capabilities(argv):
    caps = ['ctl.v1']
    if argv and argv[0] in COMMAND_CAPABILITIES:
        caps.append(COMMAND_CAPABILITIES[argv[0]])
    if argv and argv[0] == 'approval-respond' and '--monitor' in argv:
        caps.append('caller-approvals.monitor-respond.v1')
    return tuple(caps)


def validate_reply(argv, doc, expected_version='0.0.13'):
    """Envelope and, for task-events, the embedded event-page checks every controlled profile applies."""
    validate_envelope(doc, 'harflow.ctl', expected_version)
    if argv and argv[0] == 'task-events' and 'error' not in doc:
        validate_event_page(doc, expected_version)
    return doc
