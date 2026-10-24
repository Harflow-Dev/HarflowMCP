"""Explicit, cooperative, crash-resumable backup of an incompatible state root.

The operation-tagged fresh store marker is the irreversible commit point. After
publication, retries only reconcile receipts and never inspect or move new data.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from .state_layout import StateLayout
from .state_migration import authoritative_format, read_regular
from . import state_ownership as ownership
from .wire_contracts import PRODUCT, PRODUCT_VERSION

JOURNAL_NAME = '.harflow-recovery.json'
JOURNAL_PENDING = JOURNAL_NAME + '.pending'
MANIFEST_NAME = 'manifest.json'
RECEIPT_NAME = 'receipt.json'
OPERATION = re.compile(r'[a-f0-9]{32}')
MAX_ENTRIES = 100_000


class StateRecoveryError(ownership.StateOwnershipError):
    code = 'state_recovery_failed'


class StateRecoveryStale(StateRecoveryError):
    code = 'state_recovery_stale'


class StateRecoveryUnsafe(StateRecoveryError):
    code = 'state_recovery_unsafe'


def _failure(message, reason, *, stale=False, unsafe=False):
    cls = StateRecoveryStale if stale else StateRecoveryUnsafe if unsafe else StateRecoveryError
    return cls(message, reason=reason)


def _sync(directory):
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write(path, document):
    """Reserved temporary publication; an existing temporary is never overwritten."""
    pending = path.with_name(path.name + '.pending')
    if pending.exists() or pending.is_symlink():
        previous = _load(pending)
        if previous.get('operation_id') != document.get('operation_id') or not document.get('operation_id'):
            raise _failure('Recovery publication temporary conflicts', 'backup_conflict', unsafe=True)
        os.replace(pending, path)
        _sync(path.parent)
    payload = json.dumps(document, sort_keys=True, separators=(',', ':'), allow_nan=False).encode() + b'\n'
    descriptor = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        with os.fdopen(descriptor, 'wb', closefd=False) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(pending, path)
    _sync(path.parent)


def _load(path):
    return json.loads(read_regular(path, 16 * 1024 * 1024))


def _safe_root(layout):
    ownership._validate_layout(layout)
    root = layout.root
    if root in (Path('/'), Path.home()) or root.parent == root:
        raise _failure('Recovery requires a dedicated state directory', 'unsafe_root', unsafe=True)
    current = root
    while True:
        try:
            info = current.lstat()
        except FileNotFoundError:
            if current == root:
                return False
            raise
        if not stat.S_ISDIR(info.st_mode):
            raise _failure('Recovery root and its parents must be directories without symlinks', 'unsafe_root', unsafe=True)
        if current.parent == current:
            break
        current = current.parent
    if root.stat().st_dev != root.parent.stat().st_dev:
        raise _failure('Recovery requires a sibling backup on the same filesystem', 'unsafe_root', unsafe=True)
    for name in (ownership.LOCK_NAME, ownership.MARKER_NAME, ownership.PENDING_NAME, JOURNAL_NAME, JOURNAL_PENDING):
        ownership._regular(root / name)
    if (root / JOURNAL_PENDING).exists():
        pending = _load(root / JOURNAL_PENDING)
        if (not isinstance(pending, dict) or pending.get('format') != 'harflow.state-recovery'
                or pending.get('state_root') != str(root)):
            raise _failure('An unqualified journal publication blocks recovery', 'recovery_interrupted')
    return True


def _entry(path, budget):
    budget[0] += 1
    if budget[0] > MAX_ENTRIES:
        raise _failure('Recovery inventory exceeds its safe bound', 'infrastructure_unavailable')
    before = path.lstat()
    value = [before.st_dev, before.st_ino, before.st_mode, before.st_size, before.st_mtime_ns]
    if stat.S_ISDIR(before.st_mode):
        children = {child.name: _entry(child, budget) for child in sorted(path.iterdir())}
        value.append(children)
    elif stat.S_ISLNK(before.st_mode):
        value.append(os.readlink(path))
    elif not stat.S_ISREG(before.st_mode):
        raise _failure('Recovery cannot archive special filesystem entries', 'unsafe_root', unsafe=True)
    after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_mode, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_mode, after.st_size, after.st_mtime_ns):
        raise _failure('State changed during recovery inspection', 'stale_confirmation', stale=True)
    return value


def _inventory(root):
    budget = [0]
    return {path.name: _entry(path, budget) for path in sorted(root.iterdir())
            if path.name not in (ownership.LOCK_NAME, JOURNAL_NAME, JOURNAL_PENDING)}


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _encode(value):
    return base64.urlsafe_b64encode(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).decode().rstrip('=')


def _decode(token):
    try:
        if not isinstance(token, str) or len(token) > 8192:
            raise ValueError('Invalid token')
        value = json.loads(base64.b64decode(token + '=' * (-len(token) % 4), altchars=b'-_', validate=True))
        if not isinstance(value, dict) or _encode(value) != token:
            raise ValueError('Invalid token')
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise _failure('Invalid recovery confirmation', 'stale_confirmation', stale=True) from exc


def _journal(root):
    path = root / JOURNAL_PENDING
    if not path.exists() and not path.is_symlink():
        path = root / JOURNAL_NAME
        if not path.exists() and not path.is_symlink():
            return None
    value = _load(path)
    if (not isinstance(value, dict) or value.get('product') != PRODUCT or value.get('format') != 'harflow.state-recovery'
            or type(value.get('v')) is not int or value.get('v') != 1
            or not isinstance(value.get('operation_id'), str) or OPERATION.fullmatch(value['operation_id']) is None
            or value.get('state_root') != str(root)):
        raise _failure('Unqualified recovery journal', 'recovery_interrupted', unsafe=True)
    proposal = value.get('proposal')
    inventory = value.get('inventory')
    if (value.get('phase') not in {'planned', 'prepared', 'backed_up', 'committed', 'completed'}
            or not isinstance(proposal, dict) or proposal.get('operation_id') != value['operation_id']
            or proposal.get('state_root') != str(root)
            or value.get('backup_path') != str(_backup_path(root, value['operation_id']))
            or value.get('root_identity') != proposal.get('root_identity')
            or not isinstance(inventory, dict)
            or any(not isinstance(name, str) or not name or name in {'.', '..', ownership.LOCK_NAME, JOURNAL_NAME, JOURNAL_PENDING}
                   or '/' in name or '\x00' in name for name in inventory)):
        raise _failure('Recovery journal paths or phase cannot be qualified', 'recovery_interrupted', unsafe=True)
    return value


def committed_journal(root, marker):
    """Pure startup gate: only the exact committed marker bypasses a journal."""
    try:
        value = _journal(root)
        return (value is not None and isinstance(marker, dict)
                and marker.get('recovery_operation_id') == value['operation_id'])
    except (OSError, ValueError, ownership.StateOwnershipError):
        return False


def _proposal(root, operation_id=None):
    operation_id = operation_id or uuid.uuid4().hex
    info = root.lstat()
    lock = ownership._regular(root / ownership.LOCK_NAME)
    inventory = _inventory(root)
    proposal = {'state_root': str(root), 'engine_version': PRODUCT_VERSION,
                'root_identity': [info.st_dev, info.st_ino],
                'lock_identity': [lock.st_dev, lock.st_ino] if lock else None,
                'inventory_sha256': _digest(inventory), 'operation_id': operation_id,
                'prior_journal_sha256': hashlib.sha256(read_regular(root / JOURNAL_NAME)).hexdigest() if (root / JOURNAL_NAME).exists() else None}
    return proposal, inventory


def _backup_path(root, operation_id):
    # Operation identity is opaque and collision-resistant; callers cannot choose a destination.
    return root.with_name(root.name + '.harflow-backup-' + operation_id)


def _recovery_info(root, proposal, *, phase='proposed', resumable=False):
    return {'operation_id': proposal['operation_id'], 'confirmation_token': _encode(proposal),
            'backup_path': str(_backup_path(root, proposal['operation_id'])),
            'phase': phase, 'resumable': resumable}


def inspect_state(layout: StateLayout) -> dict:
    """Inspect without mkdir, lock creation, ownership acquisition, or migration."""
    root = layout.root
    result = {'state_root': str(root), 'engine_version': PRODUCT_VERSION,
              'recovery_supported': False, 'cooperative_ownership_only': True}
    try:
        if not _safe_root(layout):
            return {**result, 'status': 'missing', 'reason': 'missing_root', 'message': 'State root has not been initialized'}
        journal = _journal(root)
        try:
            marker = ownership._read_marker(root)
        except ownership.StateIncompatible:
            marker = None
        if journal is not None and journal.get('phase') != 'completed' and not committed_journal(root, marker):
            return {**result, 'status': 'interrupted', 'reason': 'recovery_interrupted',
                    'message': 'Explicit recovery must be resumed', 'recovery_supported': True,
                    'recovery': _recovery_info(root, journal['proposal'], phase=journal['phase'], resumable=True)}
        try:
            marked = ownership._marker(root)
            if marked and ownership._regular(root / ownership.LOCK_NAME) is None:
                raise ownership._incompatible('marked root has no stable lock')
            if marked and ownership.parse_version(marker['product_version'])[:2] != ownership.parse_version(ownership.PRODUCT_VERSION)[:2]:
                from .state_migration import validate_promotion
                validate_promotion(root, target_version=ownership.PRODUCT_VERSION)
            if marked or ownership._admissible_empty(root):
                return {**result, 'status': 'ready', 'reason': 'compatible', 'message': 'State is compatible'}
            reason, message = 'unmarked_nonempty', 'Unmarked state root contains data'
        except ownership.StateIncompatible as exc:
            reason, message = exc.reason or 'invalid_marker', str(exc)
        proposal, _ = _proposal(root)
        result.update(status='incompatible', reason=reason, message=message,
                      recovery_supported=True, recovery=_recovery_info(root, proposal))
        if isinstance(marker, dict) and marker.get('product') == PRODUCT:
            result['observed_store'] = {key: marker[key] for key in ('product', 'format', 'v', 'product_version', 'layout_version') if key in marker}
        return result
    except (OSError, ValueError, ownership.StateOwnershipError, RecursionError) as exc:
        return {**result, 'status': 'incompatible', 'reason': getattr(exc, 'reason', None) or 'infrastructure_unavailable',
                'message': str(exc)}


@contextmanager
def _raw_exclusive(layout, proposal):
    root = layout.root
    with ownership._manager_until(time.monotonic() + ownership.DEFAULT_TIMEOUT):
        if not _safe_root(layout):
            raise _failure('Selected state root disappeared', 'stale_confirmation', stale=True)
        root_id = ownership._root_identity(root)
        if root_id in ownership._handles:
            raise ownership.StateBusy('Cannot recover while this process holds a state lease', reason='active_owner')
        if list(root_id) != proposal.get('root_identity'):
            raise _failure('Selected root was replaced', 'stale_confirmation', stale=True)
        lock = root / ownership.LOCK_NAME
        info = ownership._regular(lock)
        expected = proposal.get('lock_identity')
        if expected is not None and (info is None or [info.st_dev, info.st_ino] != expected):
            raise _failure('State ownership lock was replaced', 'stale_confirmation', stale=True)
        flags = os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
        if info is None:
            flags |= os.O_CREAT | os.O_EXCL
        descriptor = os.open(lock, flags, 0o600)
        try:
            lock_id = ownership._identity(os.fstat(descriptor))
            try:
                ownership._wait(descriptor, fcntl.LOCK_EX, time.monotonic() + ownership.DEFAULT_TIMEOUT)
            except ownership.StateBusy as exc:
                exc.reason = 'active_owner'
                raise
            if ownership._root_identity(root) != root_id or ownership._identity(lock.lstat()) != lock_id:
                raise _failure('State ownership identity changed', 'stale_confirmation', stale=True)
            _sync(root)
            yield root_id, lock_id, info is None
        finally:
            os.close(descriptor)


def _check_owners(root):
    """Refuse known live or uncertain pre-ownership task/scheduler evidence."""
    from .identity import identity_check

    def ambiguous():
        raise ownership.StateBusy('State process ownership is ambiguous', reason='ambiguous_owner')

    def complete_owner(value):
        if (not isinstance(value, dict) or not {'pid', 'start_time', 'markers'} <= value.keys()
                or type(value['pid']) is not int or value['pid'] <= 0
                or value['start_time'] is not None and not isinstance(value['start_time'], str)
                or not isinstance(value['markers'], list)
                or any(not isinstance(marker, str) for marker in value['markers'])):
            ambiguous()

    def identity(value):
        if (type(value.get('pid')) is not int or value['pid'] <= 0
                or value.get('start_time') is not None and not isinstance(value['start_time'], str)
                or not isinstance(value.get('markers', []), list)
                or any(not isinstance(marker, str) for marker in value.get('markers', []))):
            ambiguous()
        verdict = identity_check(value)
        if verdict != 'dead':
            raise ownership.StateBusy('State has an active or uncertain process owner',
                                      reason='active_owner' if verdict == 'alive' else 'ambiguous_owner')

    def visit(value):
        if isinstance(value, dict):
            if 'pid' in value and value['pid'] is not None:
                identity(value)
            if value.get('supervisor_pid') is not None and not value.get('supervisor_identity'):
                identity({'pid': value['supervisor_pid']})
            for key, item in value.items():
                if key in ('owner', 'supervisor_identity', 'controller') and item is not None:
                    complete_owner(item)
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    for name in ('tasks', 'workflow-runs', 'workflow-leases'):
        directory = root / name
        if not directory.exists() and not directory.is_symlink():
            continue
        if not stat.S_ISDIR(directory.lstat().st_mode):
            raise ownership.StateBusy('Known owner evidence is not safely readable', reason='ambiguous_owner')
        for path in directory.iterdir():
            format_name = authoritative_format(path.relative_to(root))
            if format_name in {'harflow.task', 'harflow.task-launch', 'harflow.workflow.run', 'harflow.task.control'}:
                try:
                    value = _load(path)
                    if format_name == 'harflow.task':
                        from .store import TERMINAL_RECORD_STATUSES
                        if not isinstance(value, dict):
                            ambiguous()
                        observed_terminal = (isinstance(value.get('status'), str)
                                             and value['status'] in TERMINAL_RECORD_STATUSES
                                             and type(value.get('exit_code')) is int)
                        # A dead dispatcher cannot establish that its detached
                        # agent also exited. Unobserved tasks need independent
                        # task-process evidence before their state can move.
                        if not observed_terminal and (type(value.get('pid')) is not int or value['pid'] <= 0):
                            ambiguous()
                    visit(value)
                except (OSError, ValueError, RecursionError) as exc:
                    raise ownership.StateBusy('Known owner evidence cannot be qualified', reason='ambiguous_owner') from exc
            if path.suffix == '.lock':
                descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
                try:
                    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                        raise ownership.StateBusy('Nonregular owner lock', reason='ambiguous_owner')
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError as exc:
                        raise ownership.StateBusy('An older engine owner lock is active', reason='active_owner') from exc
                finally:
                    os.close(descriptor)


def _evidence(root, journal):
    return {'state_root': str(root), 'engine_version': PRODUCT_VERSION,
            'operation_id': journal['operation_id'], 'backup_id': journal['operation_id'],
            'backup_path': journal['backup_path'], 'phase': journal['phase'], 'recovery_supported': True}


def _assert_locations(root, backup, journal):
    if list(ownership._root_identity(root)) != journal['root_identity']:
        raise _failure('Recovery root was replaced', 'stale_confirmation', stale=True)
    lock = ownership._regular(root / ownership.LOCK_NAME)
    if lock is None or [lock.st_dev, lock.st_ino] != journal['lock_identity']:
        raise _failure('Recovery ownership lock was replaced', 'stale_confirmation', stale=True)
    info = backup.lstat()
    if not stat.S_ISDIR(info.st_mode) or [info.st_dev, info.st_ino] != journal['backup_identity']:
        raise _failure('Recovery backup was replaced', 'backup_conflict', unsafe=True)
    info = (backup / 'data').lstat()
    if not stat.S_ISDIR(info.st_mode) or [info.st_dev, info.st_ino] != journal['data_identity']:
        raise _failure('Recovery backup data directory was replaced', 'backup_conflict', unsafe=True)


def _finish(root, backup, journal):
    """After commit, only operation metadata is reconciled; new state is untouched."""
    _assert_locations(root, backup, journal)
    result = {**_evidence(root, journal), 'status': 'completed', 'phase': 'completed'}
    journal = {**journal, 'phase': 'completed', 'result': result}
    for path, value in ((backup / RECEIPT_NAME, result), (backup / MANIFEST_NAME, journal), (root / JOURNAL_NAME, journal)):
        # A crash before replace leaves only our reserved temporary. Verify it
        # belongs to this operation before resuming publication.
        pending = path.with_name(path.name + '.pending')
        if pending.exists() or pending.is_symlink():
            saved = _load(pending)
            if saved.get('operation_id') != journal['operation_id']:
                raise _failure('Recovery publication temporary conflicts', 'backup_conflict', unsafe=True)
            os.replace(pending, path)
            _sync(path.parent)
        _write(path, value)
    return result


def _completed_result(root, proposal):
    """A durable receipt answers old-operation repeats without changing any store."""
    backup = _backup_path(root, proposal['operation_id'])
    receipt = backup / RECEIPT_NAME
    if not receipt.exists() and not receipt.is_symlink():
        return None
    info = backup.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise _failure('Recovery backup is unsafe', 'backup_conflict', unsafe=True)
    manifest, result = _load(backup / MANIFEST_NAME), _load(receipt)
    if (not isinstance(manifest, dict) or manifest.get('proposal') != proposal
            or manifest.get('backup_identity') != [info.st_dev, info.st_ino]
            or not isinstance(result, dict) or result.get('operation_id') != proposal['operation_id']
            or result.get('state_root') != str(root) or result.get('backup_path') != str(backup)
            or result.get('status') != 'completed' or result.get('phase') != 'completed'):
        raise _failure('Recovery receipt cannot be qualified', 'backup_conflict', unsafe=True)
    return result


def recover_state(layout: StateLayout, *, operation_id: str, confirmation_token: str) -> dict:
    """Explicit confirmation/resume. Never retries a different operation implicitly."""
    root = layout.root
    journal = None
    try:
        proposal = _decode(confirmation_token)
        if (not isinstance(operation_id, str) or OPERATION.fullmatch(operation_id) is None
                or proposal.get('operation_id') != operation_id or proposal.get('state_root') != str(root)
                or proposal.get('engine_version') != PRODUCT_VERSION):
            raise _failure('Confirmation does not match this engine, root, or operation', 'stale_confirmation', stale=True)
        # Receipt reads do not require exclusive ownership and never reset again.
        # Validate the selected root before considering a historical operation.
        if not _safe_root(layout) or list(ownership._root_identity(root)) != proposal.get('root_identity'):
            raise _failure('Selected root was replaced', 'stale_confirmation', stale=True)
        completed = _completed_result(root, proposal)
        if completed is not None:
            return completed
        with _raw_exclusive(layout, proposal) as (root_id, lock_id, lock_created):
            journal = _journal(root)
            previous_journal = None
            if journal is not None and journal['operation_id'] != operation_id and journal.get('phase') == 'completed':
                previous_journal = base64.b64encode(read_regular(root / JOURNAL_NAME)).decode()
                journal = None
            backup = _backup_path(root, operation_id)
            if journal is not None:
                if journal['operation_id'] != operation_id or journal['proposal'] != proposal:
                    raise _failure('Another recovery operation owns this root', 'stale_confirmation', stale=True)
                if journal['phase'] != 'planned':
                    _assert_locations(root, backup, journal)
                try:
                    marker = ownership._read_marker(root)
                except ownership.StateIncompatible:
                    marker = None
                if marker is not None and marker.get('recovery_operation_id') == operation_id:
                    return _finish(root, backup, journal)
                if journal['phase'] == 'completed':
                    raise _failure('Committed recovery marker changed', 'stale_confirmation', stale=True)
            else:
                current, inventory = _proposal(root, operation_id)
                # Creating the previously absent stable lock is infrastructure,
                # not a change to the inspected user data.
                current['lock_identity'] = proposal.get('lock_identity')
                if current != proposal:
                    raise _failure('State changed since inspection; inspect again', 'stale_confirmation', stale=True)
                inspection = inspect_state(layout)
                if (inspection['status'] not in {'incompatible', 'interrupted'} or not inspection['recovery_supported']) and not lock_created:
                    raise _failure('State no longer requires supported recovery', 'stale_confirmation', stale=True)
                _check_owners(root)
                journal = {'product': PRODUCT, 'format': 'harflow.state-recovery', 'v': 1,
                           'engine_version': PRODUCT_VERSION, 'state_root': str(root),
                           'operation_id': operation_id, 'proposal': proposal,
                           'backup_path': str(backup), 'root_identity': list(root_id),
                           'lock_identity': list(lock_id), 'inventory': inventory,
                           'created_at': datetime.now(timezone.utc).isoformat(), 'phase': 'planned',
                           'backup_identity': None, 'data_identity': None,
                           'previous_journal_bytes': previous_journal}
                if backup.exists() or backup.is_symlink():
                    raise _failure('Backup destination already exists; no entries were overwritten', 'backup_conflict', unsafe=True)
                # Establish durable in-root operation intent before creating the
                # sibling. An interrupted mkdir can then be resumed explicitly.
                _write(root / JOURNAL_NAME, journal)
            if journal['phase'] == 'planned':
                if not backup.exists() and not backup.is_symlink():
                    backup.mkdir(mode=0o700)
                    _sync(backup.parent)
                info = backup.lstat()
                if not stat.S_ISDIR(info.st_mode):
                    raise _failure('Backup destination is unsafe', 'backup_conflict', unsafe=True)
                data = backup / 'data'
                manifest = backup / MANIFEST_NAME
                pending_manifest = manifest.with_name(MANIFEST_NAME + '.pending')
                has_manifest = manifest.exists() or manifest.is_symlink()
                has_pending = pending_manifest.exists() or pending_manifest.is_symlink()
                if has_manifest or has_pending:
                    data_info = data.lstat()
                    if not stat.S_ISDIR(data_info.st_mode) or any(data.iterdir()):
                        raise _failure('Planned backup data is unsafe or unexpectedly populated', 'backup_conflict', unsafe=True)
                    expected_manifest = {**journal, 'phase': 'prepared',
                                         'backup_identity': [info.st_dev, info.st_ino],
                                         'data_identity': [data_info.st_dev, data_info.st_ino]}
                    for path, present in ((manifest, has_manifest), (pending_manifest, has_pending)):
                        if present and _load(path) != expected_manifest:
                            raise _failure('Initial backup manifest conflicts with recovery intent', 'backup_conflict', unsafe=True)
                    if has_pending:
                        os.replace(pending_manifest, manifest)
                        _sync(backup)
                    allowed = {'data', MANIFEST_NAME}
                    if journal.get('previous_journal_bytes') is not None:
                        previous = backup / 'previous-recovery.json'
                        if read_regular(previous) != base64.b64decode(journal['previous_journal_bytes']):
                            raise _failure('Previous recovery journal conflicts with recovery intent', 'backup_conflict', unsafe=True)
                        allowed.add(previous.name)
                    if {item.name for item in backup.iterdir()} != allowed:
                        raise _failure('Backup destination contains unqualified entries', 'backup_conflict', unsafe=True)
                if any(backup.iterdir()) and not (backup / MANIFEST_NAME).exists():
                    # Only our data directory may have been made before the
                    # first manifest publication; it must still be empty.
                    allowed = {'data'}
                    if journal.get('previous_journal_bytes') is not None:
                        previous = backup / 'previous-recovery.json'
                        if previous.exists() or previous.is_symlink():
                            if read_regular(previous) != base64.b64decode(journal['previous_journal_bytes']):
                                raise _failure('Previous recovery journal conflicts with recovery intent', 'backup_conflict', unsafe=True)
                            allowed.add(previous.name)
                    if {item.name for item in backup.iterdir()} != allowed or not stat.S_ISDIR(data.lstat().st_mode) or any(data.iterdir()):
                        raise _failure('Backup destination contains unqualified entries', 'backup_conflict', unsafe=True)
                if not data.exists() and not data.is_symlink():
                    data.mkdir(mode=0o700)
                    _sync(backup)
                if not stat.S_ISDIR(data.lstat().st_mode):
                    raise _failure('Backup data directory is unsafe', 'backup_conflict', unsafe=True)
                journal['backup_identity'] = [info.st_dev, info.st_ino]
                info = data.lstat()
                journal['data_identity'] = [info.st_dev, info.st_ino]
                if journal.get('previous_journal_bytes') is not None:
                    old_path = backup / 'previous-recovery.json'
                    if not old_path.exists():
                        descriptor = os.open(old_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                        try:
                            with os.fdopen(descriptor, 'wb', closefd=False) as stream:
                                stream.write(base64.b64decode(journal['previous_journal_bytes']))
                                stream.flush()
                                os.fsync(descriptor)
                        finally:
                            os.close(descriptor)
                        _sync(backup)
                journal['phase'] = 'prepared'
                _write(backup / MANIFEST_NAME, journal)
                _write(root / JOURNAL_NAME, journal)
            _assert_locations(root, backup, journal)
            _check_owners(root)
            data = backup / 'data'
            if not stat.S_ISDIR(data.lstat().st_mode):
                raise _failure('Backup data directory is unsafe', 'backup_conflict', unsafe=True)
            inventory = journal['inventory']
            expected = set(inventory)
            actual = {entry.name for entry in root.iterdir()} - {ownership.LOCK_NAME, JOURNAL_NAME, JOURNAL_PENDING}
            if ownership.PENDING_NAME in actual and ownership.PENDING_NAME not in expected and journal['phase'] == 'backed_up':
                pending_marker = _load(root / ownership.PENDING_NAME)
                if pending_marker == {'product': PRODUCT, 'format': 'harflow.store', 'v': 1, 'product_version': PRODUCT_VERSION, 'layout_version': 1, 'recovery_operation_id': operation_id}:
                    actual.remove(ownership.PENDING_NAME)
            if actual - expected or set(entry.name for entry in data.iterdir()) - expected:
                raise _failure('Unexpected entries appeared during recovery', 'stale_confirmation', stale=True)
            # Move marker before all other data. Rename of a directory preserves
            # its entire subtree and never traverses contained symlink targets.
            names = sorted(inventory, key=lambda name: (name != ownership.MARKER_NAME, name))
            for name in names:
                _assert_locations(root, backup, journal)
                source, target = root / name, data / name
                source_exists = source.exists() or source.is_symlink()
                target_exists = target.exists() or target.is_symlink()
                if source_exists == target_exists:
                    raise _failure('Recovery entry is missing or exists on both sides', 'backup_conflict', unsafe=True)
                path = source if source_exists else target
                if _entry(path, [0]) != inventory[name]:
                    raise _failure('Recovery entry changed; no replacement was moved', 'stale_confirmation', stale=True)
                if source_exists:
                    os.rename(source, target)
                    _sync(root)
                    _sync(data)
            journal['phase'] = 'backed_up'
            _write(root / JOURNAL_NAME, journal)
            _write(backup / MANIFEST_NAME, journal)
            _assert_locations(root, backup, journal)
            # THIS PUBLICATION IS THE IRREVERSIBLE COMMIT POINT.
            ownership._publish(root, {'product': PRODUCT, 'format': 'harflow.store', 'v': 1,
                                     'product_version': PRODUCT_VERSION, 'layout_version': 1,
                                     'recovery_operation_id': operation_id})
            journal['phase'] = 'committed'
            return _finish(root, backup, journal)
    except ownership.StateOwnershipError as exc:
        exc.data.update(state_root=str(root), engine_version=PRODUCT_VERSION, recovery_supported=not isinstance(exc, StateRecoveryUnsafe))
        if journal is not None:
            exc.data.update(_evidence(root, journal))
        raise
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as exc:
        error = _failure('State recovery did not complete: ' + str(exc), 'recovery_interrupted' if journal else 'infrastructure_unavailable')
        error.data.update(state_root=str(root), engine_version=PRODUCT_VERSION, recovery_supported=True)
        if journal is not None:
            error.data.update(_evidence(root, journal))
        raise error from exc
