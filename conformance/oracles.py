"""Independent, standard-library wire and fixture checks. No engine imports."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from datetime import datetime

PINNED_MANIFEST_SHA256 = '092299de1f3ed1ac91c55d168ce3f77c47e3d4f6859b9ebefb416393e149aea1'

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
    if not isinstance(files, dict) or len(files) != 350 or set(manifest.get('cases', {})) != set(files):
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
                        'cancel-message': 'messaging.v1', 'backend-models': 'backend-models.v1'}


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
