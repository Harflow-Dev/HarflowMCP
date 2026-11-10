"""Bounded direct-human launch discovery; receipts are display evidence, never task status."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from functools import wraps
from datetime import datetime, timezone
from pathlib import Path

from . import lineage, store, task_supervision, takeover
from .observation_budget import Budget, Exhausted, current
from .bounded_io import ReadLimit
from .state_access import task_operation
from .task_resources import is_pending_kind
from .resources import validate_names, InvalidResourceRequest

SCOPE = 'prerecord_launch_attempts_and_not_started'
MAX_ENTRIES = 65536
MAX_NAME_BYTES = 8 * 1024 * 1024
MAX_METADATA_BYTES = 8 * 1024 * 1024
MAX_SECONDS = 5
MAX_RESPONSE_BYTES = 128 * 1024
MAX_EXAMINED = 100
MAX_CURSOR_BYTES = 4096
SUFFIX = '.launch.json'


class PendingReadError(ValueError):
    def __init__(self, code, check):
        self.code, self.check = code, check
        super().__init__('Pending launch observation unavailable (' + check + '); coverage is unknown.')


def observed_at():
    return datetime.now(timezone.utc).isoformat()


def _authority(log_dir):
    refusal = takeover.caller_refusal(log_dir, candidates=_CallerCandidates(log_dir, current.get()))
    if refusal:
        raise PendingReadError(refusal[0], 'direct_caller_required')
    current.get().remaining()


def _identity(log_dir):
    root, tasks = log_dir.parent.stat(), log_dir.stat()
    return [root.st_dev, root.st_ino, tasks.st_dev, tasks.st_ino]


def _root(log_dir):
    return hashlib.sha256(os.fsencode(str(log_dir.parent.resolve()))).hexdigest()


def _source_namespace(log_dir, budget, suffix):
    if time.monotonic() >= budget.deadline:
        raise Exhausted("Observation deadline reached")
    names, entries, name_bytes = [], 0, 0
    try:
        with os.scandir(log_dir) as directory:
            for entry in directory:
                if time.monotonic() >= budget.deadline:
                    raise Exhausted("Observation deadline reached")
                entries += 1
                name_bytes += len(os.fsencode(entry.name))
                if entries > MAX_ENTRIES or name_bytes > MAX_NAME_BYTES:
                    raise PendingReadError('pending_observation_bound', 'namespace_bound')
                if entry.name.endswith(suffix):
                    names.append(entry.name)
    except OSError as exc:
        raise PendingReadError('pending_observation_unavailable', 'namespace_unavailable') from exc
    names.sort()
    digest = hashlib.sha256()
    candidates, invalid = [], 0
    for name in names:
        if time.monotonic() >= budget.deadline:
            raise Exhausted("Observation deadline reached")
        digest.update(os.fsencode(name) + b'\0')
        task_id = name[:-len(suffix)]
        if store.TASK_ID_PATTERN.fullmatch(task_id):
            candidates.append(task_id)
        else:
            invalid += 1
    return candidates, digest.hexdigest(), invalid


def _namespace(log_dir, budget):
    return _source_namespace(log_dir, budget, SUFFIX)


class _CallerCandidates(lineage.CandidateScan):
    """Complete bounded ordinary-record evidence for the unchanged direct-caller detector.

    No partial list escapes on read/namespace failure. Lineage converts these failures to
    undecidable, so an exhausted budget never becomes proof of a human caller.
    """

    def __init__(self, log_dir, budget):
        super().__init__(log_dir)
        self._budget = budget

    def resolve(self):
        if self._value is not None:
            return self._value
        identity = _identity(self._log_dir)
        names, revision, invalid = _source_namespace(self._log_dir, self._budget, store.RECORD_SUFFIX)
        if invalid:
            raise ValueError('Ordinary caller namespace has invalid task identities')
        records = []
        for task_id in names:
            self._budget.remaining()
            record = task_supervision.read_task_record(self._log_dir, task_id, include_prompt=False)
            if record is None:
                raise ValueError('Ordinary caller source disappeared during observation')
            if not (record.status in store.TERMINAL_RECORD_STATUSES and record.exit_code is not None):
                records.append(record)
        _, after_revision, _ = _source_namespace(self._log_dir, self._budget, store.RECORD_SUFFIX)
        if revision != after_revision or identity != _identity(self._log_dir):
            raise ValueError('Ordinary caller namespace changed during observation')
        # Preserve the existing detector's candidate ordering and selection behavior.
        self._value = sorted(records, key=lambda record: record.started_at)
        return self._value


def _encode(value):
    return base64.urlsafe_b64encode(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).decode()


def _decode(cursor):
    try:
        if not isinstance(cursor, str) or len(cursor.encode()) > MAX_CURSOR_BYTES:
            raise ValueError()
        value = json.loads(base64.b64decode(cursor, altchars=b'-_', validate=True))
        if (not isinstance(value, dict) or set(value) != {'v', 'scope', 'root', 'identity', 'revision', 'last', 'limit', 'uncertain'}
                or type(value['v']) is not int or value['v'] != 1
                or value['scope'] != SCOPE or not isinstance(value['last'], str)
                or not store.TASK_ID_PATTERN.fullmatch(value['last'])
                or type(value['limit']) is not int or not 1 <= value['limit'] <= 100
                or type(value['uncertain']) is not bool
                or not isinstance(value['identity'], list) or len(value['identity']) != 4
                or any(type(i) is not int for i in value['identity'])
                or any(not isinstance(value[k], str) or len(value[k]) != 64 or any(c not in '0123456789abcdef' for c in value[k]) for k in ('root', 'revision'))):
            raise ValueError()
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise PendingReadError('pending_cursor_invalid', 'cursor_shape') from exc


def _refused(task_id, error):
    # Do not expose paths, exception strings, or metadata from unqualified sources.
    return dict(task_id=task_id, disposition='refused', code=error.code,
                check=getattr(error, 'check', 'task_record_qualification'))


def _text(value, maximum=1024):
    if value is None:
        return None
    return value.encode('utf-8')[:maximum].decode('utf-8', errors='ignore')


def _lookup(log_dir, task_id):
    try:
        record = task_supervision.read_task_record(log_dir, task_id, include_prompt=False)
        if record is not None:
            return dict(task_id=task_id, disposition='recorded')
        receipt = task_supervision.read_launch(log_dir, task_id)
        # A qualified ordinary record that lands during receipt observation wins.
        if task_supervision.read_task_record(log_dir, task_id, include_prompt=False) is not None:
            return dict(task_id=task_id, disposition='recorded')
        if receipt is None:
            return dict(task_id=task_id, disposition='not_found')
        state = receipt['state']
        if state in ('spawned', 'settled'):
            return dict(task_id=task_id, disposition='unknown', code='pending_record_inconsistent', check='postrecord_source_without_record')
        metadata = {key: _text(receipt.get(key), 4096 if key == 'repo_path' else 1024)
                    for key in ('title', 'backend', 'repo_path', 'created_at', 'not_started_reason')}
        metadata.update(generation=receipt['generation'], state=state)
        metadata['lineage'] = {key: receipt[key] for key in
                               ('spawned_by', 'root_task_id', 'parent_task_id', 'chain_root_task_id')
                               if receipt.get(key) is not None and store.TASK_ID_PATTERN.fullmatch(receipt[key])}
        resource_names = (receipt.get('resources') or {}).get('names', [])
        if receipt.get('resources') is not None:
            try:
                validate_names(resource_names)
            except InvalidResourceRequest:
                return dict(task_id=task_id, disposition='refused', code='field_shape', check='resource_names')
        # The receipt is bounded, but metadata projection is independently bounded too.
        metadata['resource_names'] = [_text(name, 256) for name in resource_names[:100]]
        metadata['metadata_truncated'] = (len(resource_names) > 100 or any(len(name.encode()) > 256 for name in resource_names)
            or any(isinstance(receipt.get(key), str) and len(receipt[key].encode()) > (4096 if key == 'repo_path' else 1024)
                   for key in ('title', 'backend', 'repo_path', 'created_at', 'not_started_reason')))
        return dict(task_id=task_id, disposition='pending', launch=metadata,
                    cancel_support='supported' if state != 'not_started' and is_pending_kind(receipt) else 'unsupported')
    except store.TaskDocumentError as exc:
        return _refused(task_id, exc)
    except UnicodeError:
        return dict(task_id=task_id, disposition='refused', code='field_shape', check='display_text_encoding')


def _budgeted(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        budget = Budget(seconds=MAX_SECONDS, byte_limit=MAX_METADATA_BYTES)
        token = current.set(budget)
        try:
            return function(*args, budget=budget, **kwargs)
        except (Exhausted, ReadLimit) as exc:
            raise PendingReadError('pending_observation_bound', 'read_budget') from exc
        except OSError as exc:
            raise PendingReadError('pending_observation_unavailable', 'source_unavailable') from exc
        finally:
            current.reset(token)
    return wrapped


@task_operation
@_budgeted
def pending_status(log_dir: Path, task_id: str, *, budget=None):
    _authority(log_dir)
    try:
        store.validate_task_id(task_id)
    except ValueError as exc:
        raise PendingReadError('invalid_task_id', 'exact_task_id_required') from exc
    result = _lookup(log_dir, task_id)
    value = dict(scope=SCOPE, observed_at=observed_at(), **result)
    if len(json.dumps(value).encode()) > MAX_RESPONSE_BYTES - 8192:
        raise PendingReadError('pending_observation_bound', 'response_bytes')
    return value


@task_operation
@_budgeted
def pending_page(log_dir: Path, *, limit=50, cursor=None, budget=None):
    _authority(log_dir)
    if type(limit) is not int or not 1 <= limit <= 100:
        raise PendingReadError('invalid_pending_limit', 'limit_1_to_100')
    prior = _decode(cursor) if cursor is not None else None
    root, identity = _root(log_dir), _identity(log_dir)
    if prior and (prior['root'] != root or prior['limit'] != limit):
        raise PendingReadError('pending_cursor_invalid', 'cursor_scope')
    names, revision, invalid = _namespace(log_dir, budget)
    if prior and (prior['identity'] != identity or prior['revision'] != revision):
        raise PendingReadError('pending_cursor_expired', 'namespace_changed')
    if prior and prior['last'] not in names:
        raise PendingReadError('pending_cursor_invalid', 'cursor_position')
    selected = [name for name in names if prior is None or name > prior['last']]
    uncertain = bool(invalid) or bool(prior and prior['uncertain'])
    items, last, examined, bounded = [], prior['last'] if prior else None, 0, None
    for task_id in selected:
        if examined >= MAX_EXAMINED or len(items) >= limit:
            break
        try:
            row = _lookup(log_dir, task_id)
        except (Exhausted, ReadLimit):
            bounded = 'read_budget'
            break
        # Qualified recorded/postrecord history is outside the inventory scope.
        include = row['disposition'] not in ('recorded', 'unknown', 'not_found')
        if include and len(json.dumps(items + [row]).encode()) > MAX_RESPONSE_BYTES - 8192:
            bounded = 'response_bytes'
            break
        if include:
            items.append(row)
        if row['disposition'] in ('refused', 'not_found'):
            uncertain = True
        last, examined = task_id, examined + 1
    if bounded and examined == 0:
        raise PendingReadError('pending_observation_bound', bounded)
    # Re-enumeration detects name churn; content is qualified per row, never a snapshot.
    try:
        _, after_revision, _ = _namespace(log_dir, budget)
    except Exhausted:
        raise PendingReadError('pending_observation_bound', 'namespace_recheck_budget')
    if revision != after_revision or identity != _identity(log_dir):
        raise PendingReadError('pending_cursor_expired', 'namespace_changed')
    has_more = examined < len(selected)
    if bounded:
        uncertain = True
    next_cursor = _encode(dict(v=1, scope=SCOPE, root=root, identity=identity, revision=revision,
                               last=last, limit=limit, uncertain=uncertain)) if has_more else None
    result = dict(scope=SCOPE, observed_at=observed_at(), items=items, namespace_revision=revision,
                  namespace_complete=True, coverage='unknown' if uncertain or has_more else 'complete',
                  invalid_name_count=invalid, examined_count=examined, has_more=has_more,
                  next_cursor=next_cursor, bound=bounded)
    if len(json.dumps(result).encode()) > MAX_RESPONSE_BYTES - 1024:
        raise PendingReadError('pending_observation_bound', 'response_bytes')
    return result
