"""Conservative, idempotent settlement of abandoned detached executions.

This mutator is deliberately separate from status and resource resolvers. It never signals a
process, reconstructs an exit code, emits a completion event or schedules a continuation.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path

from . import control, followups, identity, store, task_supervision as supervision
from .state_access import task_operation, task_layout
from .state_ownership import StateOwnershipError


_SCAN_AFTER: dict[str, str] = {}


class RecoveryRefused(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def _refuse(code, message):
    raise RecoveryRefused(code, message)


def _usable(value, *, agent=False):
    return (isinstance(value, dict) and type(value.get('pid')) is int and value['pid'] > 0
            and isinstance(value.get('start_time'), str) and bool(value['start_time'].strip())
            and isinstance(value.get('markers'), list) and (not agent or bool(value['markers']))
            and all(isinstance(marker, str) and bool(marker.strip()) for marker in value['markers']))


def _stamp(receipt, record):
    return {'generation': receipt['generation'], 'owner': receipt.get('owner'),
            'receipt_session_id': receipt.get('session_id'),
            'session_id': record.session_id if record is not None else receipt.get('session_id'), 'task_id': receipt['task_id'],
            'agent': identity.task_identity(record.pid, record.start_time, record.markers) if record is not None else None}


@task_operation
def recover(log_dir: Path, task_id: str, *, held_session: str | None = None, human=False):
    """Settle one unchanged dead generation, or return a stable refusal.

    ``held_session`` means the caller already owns that exact session lock. Recovery skips
    other conversations in that mode. Every new lock acquisition is nonblocking.
    ``human`` bypasses only persistent directory-binding mismatch.
    """
    from .task_resources import ledger_for, clear_cancel
    binding_mismatch = False
    try:
        receipt = supervision._read_launch(log_dir, task_id)
    except supervision.LaunchDocumentError as exc:
        if not human or exc.check != 'identity_mismatch':
            raise
        binding_mismatch = True
        receipt = supervision._read_launch(log_dir, task_id, allow_binding_mismatch=True)
    if receipt is None:
        _refuse('unknown_launch', 'No launch receipt exists')
    pending = receipt.get('reconciliation_pending') is True
    binding_only = (human and binding_mismatch and receipt['state'] in {'settled', 'not_started'}
                    and not pending and not receipt.get('recovery_intent'))
    if receipt['state'] not in {'claimed', 'waiting_for_resources', 'granted', 'spawned'} and not pending and not receipt.get('recovery_intent') and not binding_only:
        _refuse('launch_not_uncertain', 'Launch is not an abandoned execution candidate')
    initial_generation = receipt['generation']
    receipt_session = receipt.get('session_id')
    initial_record = supervision.read_task_record(log_dir, task_id)
    session = receipt_session
    if initial_record is not None:
        if initial_record.owner != receipt.get('owner') or (receipt_session is not None and initial_record.session_id != receipt_session):
            _refuse('identity_mismatch', 'Launch and task owner/session identity do not match')
        if session is None:
            session = initial_record.session_id
    if held_session is not None and session != held_session:
        _refuse('different_session', 'Candidate does not belong to the held session lock')
    lock_session = session or 'continuation-' + (receipt.get('chain_root_task_id') or task_id)
    try:
        with ExitStack() as locks:
            if held_session is None:
                locks.enter_context(control.session_lock_sync(log_dir, lock_session, timeout=0))
            locks.enter_context(control._flock_sync(log_dir / f'{task_id}.launch.lock', 0, 'launch recovery', layout=task_layout(log_dir)))
            locks.enter_context(control._flock_sync(log_dir / f'{task_id}.lock', 0, 'task recovery', layout=task_layout(log_dir)))
            receipt = supervision._read_launch(log_dir, task_id, allow_binding_mismatch=human)
            if (receipt is None or receipt['generation'] != initial_generation or receipt.get('session_id') != receipt_session
                    or (session or 'continuation-' + (receipt.get('chain_root_task_id') or task_id)) != lock_session):
                _refuse('source_changed', 'Launch generation or session changed before lock acquisition')
            if receipt['state'] not in {'claimed', 'waiting_for_resources', 'granted', 'spawned'} and not receipt.get('reconciliation_pending') and not receipt.get('recovery_intent') and not binding_only:
                _refuse('launch_not_uncertain', 'Launch settled before recovery acquired its locks')
            record = supervision.read_task_record(log_dir, task_id)
            evidence = _stamp(receipt, record)
            if not _usable(evidence['owner']):
                _refuse('durable_identity_missing', 'Launch owner lacks durable process-generation identity')
            intent = receipt.get('recovery_intent')
            pre_spawn = (record is None and (
                receipt['state'] == 'waiting_for_resources'
                or (receipt['state'] == 'granted' and (receipt.get('worktree') or {}).get('state') in {'planned', 'creating'})
                or (isinstance(intent, dict) and intent.get('reason') == 'owner_died_before_spawn')))
            # A retry uses the saved generation-bound intent as its pre-spawn witness. A Git
            # creation interrupted by the dead owner is reported as unknown after settlement.
            # The intent is independently validated below before any claim can be released.
            if record is None and not pre_spawn:
                _refuse('agent_identity_unavailable', 'Potentially spawned launch has no durable agent identity; settlement refused')
            if record is not None:
                if (record.owner != receipt.get('owner') or record.session_id != session
                        or record.task_id != task_id):
                    _refuse('identity_mismatch', 'Launch and task identity do not match')
                if not _usable(evidence['agent'], agent=True):
                    _refuse('durable_identity_missing', 'Task agent lacks durable process-generation identity')
            for name in ('owner', 'agent'):
                if evidence[name] is not None and identity.identity_check(evidence[name]) != 'dead':
                    _refuse(name + '_not_dead', f'The saved {name} process is live or undecidable')
            fresh = supervision._read_launch(log_dir, task_id, allow_binding_mismatch=human)
            fresh_record = supervision.read_task_record(log_dir, task_id)
            if fresh is None or _stamp(fresh, fresh_record) != evidence or fresh != receipt:
                _refuse('source_changed', 'Launch or process generation changed during inspection')
            if record is not None and asdict(fresh_record) != asdict(record):
                _refuse('source_changed', 'Task changed during inspection')
            if binding_only:
                if (record is None or record.status not in store.TERMINAL_RECORD_STATUSES
                        or receipt['state'] not in {'settled', 'not_started'}
                        or receipt.get('reconciliation_pending') or receipt.get('recovery_intent')):
                    _refuse('record_not_recoverable', 'Binding-only repair requires an unchanged terminal record and receipt')
                supervision._write(log_dir / f'{task_id}.launch.json', receipt | {
                    'directory_binding': {'path': str(log_dir.resolve()), 'inode': log_dir.stat().st_ino}})
                return {'task_id': task_id, 'outcome': 'binding_repaired', 'released_reservations': [], 'followups': None}
            if intent is not None or receipt.get('reconciliation_pending'):
                if (not isinstance(intent, dict) or intent.get('evidence') != evidence
                        or not isinstance(intent.get('recorded_at'), str) or not isinstance(intent.get('reason'), str)
                        or not isinstance(intent.get('source'), str)):
                    _refuse('recovery_intent_mismatch', 'Pending recovery lacks matching saved generation evidence')
            else:
                intent = {'evidence': evidence, 'recorded_at': datetime.now(timezone.utc).isoformat(),
                          'source': 'launch-settle' if human else 'maintenance',
                          'reason': 'owner_and_agent_confirmed_dead' if not pre_spawn else 'owner_died_before_spawn'}
                # Before touching outcomes or claims, retain the irreversible delivery uncertainty.
                receipt = receipt | {'recovery_intent': intent, 'followups_delivery_unknown': not pre_spawn}
            if human:
                # Explicit recovery, including retry of an existing intent, rebinds only after
                # both saved generations and the unchanged intent have been verified.
                receipt = receipt | {'directory_binding': {'path': str(log_dir.resolve()), 'inode': log_dir.stat().st_ino}}
            supervision._write(log_dir / f'{task_id}.launch.json', receipt)
            if record is not None and record.status == 'running' and record.exit_code is None:
                interrupted = replace(record, status='failed', finished_at=intent['recorded_at'],
                                      interruption={key: intent[key] for key in ('reason', 'source', 'recorded_at')} | {'evidence': evidence})
                if not store.write_landed(log_dir, interrupted, expected_record=record):
                    _refuse('record_write_failed', 'Interrupted record did not become durable; claims retained')
                record = interrupted
            elif record is not None and record.exit_code is None and (record.interruption is None
                    or record.interruption.get('evidence') != evidence):
                # An unobserved terminal projection is not a matching recovery write.
                _refuse('record_not_recoverable', 'No matching interruption record or observed terminal outcome')
            changes = {'state': 'not_started' if pre_spawn else 'settled', 'settled_reason': 'abandoned',
                       'reconciliation_pending': True}
            if pre_spawn:
                changes['not_started_reason'] = 'owner_died_before_spawn'
                worktree = receipt.get('worktree') or {}
                if worktree.get('state') == 'creating':
                    changes['worktree'] = worktree | {'state': 'unknown'}
            receipt = receipt | changes
            supervision._write(log_dir / f'{task_id}.launch.json', receipt)
            handed = receipt.get('followups')
            if handed:
                if not followups.finish(log_dir, handed['task_id'], task_id, handed['ids'],
                                        'released' if pre_spawn else 'delivery_unknown', intent['reason']):
                    _refuse('followup_write_failed', 'Follow-up reconciliation remains pending')
            # Ledger lock is a leaf and follows the follow-up transaction.
            ledger = ledger_for(log_dir)
            wanted = {(receipt.get('resources') or {}).get('reservation_id')}
            wanted.update(entry.reservation_id for entry in ledger.entries() if entry.owner.kind == 'task' and entry.owner.task_id == task_id)
            released = []
            for reservation_id in sorted(item for item in wanted if item):
                if ledger.release_owned_task(reservation_id, task_id) != 'absent':
                    released.append(reservation_id)
            clear_cancel(log_dir, task_id)
            supervision._write(log_dir / f'{task_id}.launch.json', receipt | {'reconciliation_pending': False})
            return {'task_id': task_id, 'outcome': 'never_started' if pre_spawn else 'interrupted',
                    'released_reservations': released, 'followups': ('released' if pre_spawn else 'delivery_unknown') if handed else None}
    except control.LockTimeout as exc:
        raise RecoveryRefused('recovery_locked', 'Launch generation is locked; try again after its owner exits') from exc


@task_operation
def recover_abandoned(log_dir: Path, *, held_session=None, limit=64):
    """Bounded best-effort maintenance; one damaged receipt never aborts the remaining work."""
    result = {'recovered': [], 'refusals': {}}
    scan = supervision.scan_launches(log_dir)
    result['refusals'].update({key: value.as_dict() for key, value in scan.refusals.items()})
    candidates = [value for value in scan.entries if value['state'] in {'claimed', 'waiting_for_resources', 'granted', 'spawned'} or value.get('reconciliation_pending')]
    if held_session is not None:
        selected = []
        for value in candidates:
            try:
                record = supervision.read_task_record(log_dir, value['task_id'], include_prompt=False)
                effective_session = value.get('session_id')
                if effective_session is None and record is not None and record.owner == value.get('owner'):
                    effective_session = record.session_id
                if effective_session == held_session:
                    selected.append(value)
            except (store.TaskDocumentError, OSError) as exc:
                result['refusals'][value['task_id']] = {'code': getattr(exc, 'code', 'recovery_unavailable'), 'reason': str(exc)}
        candidates = selected
    key = str(log_dir.resolve())
    after = _SCAN_AFTER.get(key, '') if held_session is None else ''
    candidates = [value for value in candidates if value['task_id'] > after] + [value for value in candidates if value['task_id'] <= after]
    for receipt in candidates[:limit]:
        if held_session is None:
            _SCAN_AFTER[key] = receipt['task_id']
        task_id = receipt['task_id']
        try:
            result['recovered'].append(recover(log_dir, task_id, held_session=held_session))
        except StateOwnershipError:
            raise
        except Exception as exc:
            result['refusals'][task_id] = {'code': getattr(exc, 'code', 'recovery_unavailable'), 'reason': str(exc)}
    return result
