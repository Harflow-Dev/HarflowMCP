"""Private, one-use authorization transfer to detached task owners.

Launch files reserve identities; they are deliberately insufficient to execute a task. Only the
inherited authorization pipe can provide the prepared invocation to a newly execed owner.
"""
from __future__ import annotations

import asyncio
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import stat
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from . import identity, store
from .state_access import task_operation, assert_task_owned
from .state_layout import child_environment
from .state_compatibility import persisted_compatible
from .wire_contracts import PRODUCT, PRODUCT_VERSION

FORMAT = 'harflow.task-launch'
MAX_REQUEST = 4 * 1024 * 1024
LAUNCH_HANDSHAKE_SECONDS = 30.0
# The owner moves through these in order. `waiting_for_resources` and `granted` exist only for a
# launch that asked for resources; `not_started` is reachable from every state before `spawned`
# (from `granted` only when the failure is proven to precede the spawn).
STATES = {'reserved', 'claimed', 'waiting_for_resources', 'granted', 'spawned', 'settled', 'not_started'}


def _write(path, document):
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('x') as handle:
            os.fchmod(handle.fileno(), 0o600)
            json.dump(document, handle, ensure_ascii=False, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _root_identity(log_dir):
    info = log_dir.stat()
    return [info.st_dev, info.st_ino]


@task_operation
def read_launch(log_dir: Path, task_id: str):
    """Read launch authority with the exact current task-directory binding."""
    return _read_launch(log_dir, task_id)


class LaunchDocumentError(store.TaskDocumentError):
    """A launch refusal with a stable qualification check and source path."""

    def __init__(self, log_dir, task_id, check, reason, *, unavailable=False):
        super().__init__(task_id, reason, code='task_unavailable' if unavailable else check)
        self.check, self.path = check, str(log_dir / f'{task_id}.launch.json')
        self.args = (f'{self.path} [{self.check}]: {self.reason}',)

    def as_dict(self):
        return super().as_dict() | {'path': self.path, 'check': self.check}


def _launch_source(log_dir, task_id):
    path = log_dir / f'{task_id}.launch.json'
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise LaunchDocumentError(log_dir, task_id, 'not_regular', str(exc)) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise LaunchDocumentError(log_dir, task_id, 'not_regular', 'Task launch source is not a regular file')
        with os.fdopen(descriptor, 'rb', closefd=False) as handle:
            raw = handle.read(65537)
        try:
            after = path.lstat()
        except FileNotFoundError as exc:
            raise LaunchDocumentError(log_dir, task_id, 'source_changed', 'Task launch disappeared while reading', unavailable=True) from exc
        if (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size) != (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_size):
            error = LaunchDocumentError(log_dir, task_id, 'source_changed', 'Task launch changed while reading', unavailable=True)
            error._source_changed_during_read = True
            raise error
    finally:
        os.close(descriptor)
    if len(raw) > 65536:
        raise LaunchDocumentError(log_dir, task_id, 'oversize', 'Task launch exceeds 65536 bytes')
    try:
        value = json.loads(raw)
    except (ValueError, TypeError) as exc:
        raise LaunchDocumentError(log_dir, task_id, 'format', 'Task launch is not a JSON object') from exc
    if not isinstance(value, dict):
        raise LaunchDocumentError(log_dir, task_id, 'format', 'Task launch is not a JSON object')
    return value


def _read_launch(log_dir: Path, task_id: str, *, allow_terminal_device_drift=False, allow_binding_mismatch=False):
    """Qualify persistent path/inode binding; device IDs remain runtime-only evidence."""
    assert_task_owned(log_dir)
    store.validate_task_id(task_id)
    value = _launch_source(log_dir, task_id)
    if value is None:
        return None
    def require(condition, check, reason):
        if not condition:
            raise LaunchDocumentError(log_dir, task_id, check, reason)
    require(isinstance(value, dict) and value.get('format') == FORMAT and value.get('product') == PRODUCT, 'format', 'Incompatible task launch format')
    require(type(value.get('v')) is int and value['v'] == 1 and persisted_compatible(value.get('product_version'), FORMAT), 'version', 'Incompatible task launch version')
    require(value.get('root') == str(log_dir.parent), 'root_mismatch', 'Task launch root differs from selected root')
    require(value.get('task_id') == task_id, 'task_id_mismatch', 'Task launch task ID differs from source name')
    require(isinstance(value.get('generation'), str) and re.fullmatch('[a-f0-9]{32}', value['generation']) is not None, 'generation', 'Invalid launch generation')
    owner = value.get('owner')
    require(owner is None or (isinstance(owner, dict) and type(owner.get('pid')) is int and owner['pid'] > 0 and (owner.get('start_time') is None or isinstance(owner['start_time'], str)) and isinstance(owner.get('markers'), list) and all(isinstance(marker, str) for marker in owner['markers'])), 'owner_shape', 'Malformed launch owner identity')
    if (False
                or not isinstance(value.get('request_sha256'), str) or re.fullmatch('[a-f0-9]{64}', value['request_sha256']) is None
                or not isinstance(value.get('backend'), str)
                or any(value.get(key) is not None and not isinstance(value[key], str) for key in ('session_id', 'parent_task_id', 'chain_root_task_id', 'scratch_chain_id'))
                or not isinstance(value.get('state'), str) or value['state'] not in STATES
                or any(value.get(key) is not None and not isinstance(value[key], str) for key in ('spawned_by', 'root_task_id', 'repo_path', 'freedom', 'title', 'group', 'created_at', 'not_started_reason'))
                or value.get('resources') is not None and (not isinstance(value['resources'], dict) or not isinstance(value['resources'].get('names'), list) or any(not isinstance(name, str) for name in value['resources']['names']))
                or value.get('worktree') is not None and (not isinstance(value['worktree'], dict) or not isinstance(value['worktree'].get('path'), str))
                or value.get('followups') is not None and (not isinstance(value['followups'], dict) or not isinstance(value['followups'].get('task_id'), str) or not isinstance(value['followups'].get('ids'), list) or any(not isinstance(item, str) for item in value['followups']['ids']))):
        raise LaunchDocumentError(log_dir, task_id, 'field_shape', 'Malformed task launch field')
    directory = value.get('directory_identity')
    require(type(directory) is list and len(directory) == 2 and all(type(part) is int for part in directory), 'field_shape', 'Malformed legacy directory identity')
    current = _root_identity(log_dir)
    binding = value.get('directory_binding')
    if binding is not None:
        require(isinstance(binding, dict) and isinstance(binding.get('path'), str) and Path(binding['path']).is_absolute() and os.path.normpath(binding['path']) == binding['path'] and type(binding.get('inode')) is int and binding['inode'] >= 0, 'field_shape', 'Malformed directory binding')
        matches = binding['path'] == str(log_dir.resolve()) and binding['inode'] == current[1]
    else:
        matches = directory[1] == current[1]
    require(matches or allow_binding_mismatch, 'identity_mismatch', 'Task launch directory binding differs from selected tasks directory')
    return value


@task_operation
def read_task_record(log_dir: Path, task_id: str, **read_options):
    """Retry only a positively observed metadata replacement, never a malformed source."""
    for attempt in range(3):
        try:
            return store.read(log_dir, task_id, **read_options)
        except store.TaskDocumentError as exc:
            if getattr(exc, '_source_changed_during_read', False) is not True or attempt == 2:
                raise


@task_operation
def read_task_records(log_dir: Path):
    """Reacquire complete history only when every refusal proves source replacement."""
    for attempt in range(3):
        result = store.scan(log_dir)
        if (attempt == 2 or not result.refusals or any(
                error.code != 'task_unavailable'
                or getattr(error, '_source_changed_during_read', None) is not True
                for error in result.refusals.values())):
            return result.require_complete()


@dataclass
class LaunchScan:
    entries: list[dict] = field(default_factory=list)
    refusals: dict[str, LaunchDocumentError] = field(default_factory=dict)
    claims: list[dict] = field(default_factory=list)
    opaque: set[str] = field(default_factory=set)

    def require_complete(self):
        if self.refusals:
            raise next(iter(self.refusals.values()))
        return self.entries


@task_operation
def scan_launches(log_dir: Path):
    """Isolate damaged sources; partial claims deny actions and never grant authority."""
    result = LaunchScan()
    for path in sorted(log_dir.glob('*.launch.json')):
        task_id = path.name.removesuffix('.launch.json')
        try:
            value = _read_launch(log_dir, task_id)
            if value is not None:
                result.entries.append(value)
        except (store.TaskDocumentError, store.InvalidTaskId, OSError) as exc:
            refusal = exc if isinstance(exc, LaunchDocumentError) else LaunchDocumentError(log_dir, task_id, 'field_shape', str(exc))
            result.refusals[task_id] = refusal
            claim = None
            malformed_associations = False
            try:
                value = _launch_source(log_dir, task_id)
                if (isinstance(value, dict) and value.get('format') == FORMAT and value.get('product') == PRODUCT
                        and value.get('root') == str(log_dir.parent) and value.get('task_id') == task_id):
                    claim = {'task_id': task_id, 'state': 'refused', 'deny_only': True}
                    malformed_associations = any(value.get(key) is not None and not isinstance(value[key], str)
                                                 for key in ('session_id', 'parent_task_id', 'chain_root_task_id', 'scratch_chain_id'))
                    for key in ('session_id', 'parent_task_id', 'chain_root_task_id', 'scratch_chain_id', 'repo_path'):
                        if isinstance(value.get(key), str):
                            claim[key] = value[key]
                    worktree = value.get('worktree')
                    if isinstance(worktree, dict) and isinstance(worktree.get('path'), str):
                        claim['worktree'] = {'path': worktree['path']}
            except (store.TaskDocumentError, OSError):
                pass
            try:
                record = read_task_record(log_dir, task_id, include_prompt=False)
            except (store.TaskDocumentError, ValueError, OSError):
                record = None
            if claim is not None:
                result.claims.append(claim)
            if record is not None:
                record_claim = {key: getattr(record, key) for key in ('task_id', 'session_id', 'parent_task_id', 'chain_root_task_id', 'scratch_chain_id', 'repo_path', 'worktree')}
                record_claim.update(state='refused', deny_only=True)
                # Independent deny claims are a union: conflicting receipt evidence cannot erase
                # the qualified record's session/path, nor can the record erase the receipt claim.
                if record_claim != claim:
                    result.claims.append(record_claim)
            elif claim is None or malformed_associations:
                result.opaque.add(task_id)
    return result


@task_operation
def reservations(log_dir: Path):
    return scan_launches(log_dir).entries


@task_operation
def unresolved(log_dir: Path):
    return [value for value in reservations(log_dir) if value['state'] not in {'settled', 'not_started'} or value.get('reconciliation_pending')]


@task_operation
def session_reservations(log_dir: Path):
    """Unsettled launches that can still write a conversation; ambiguous launches stay reserved.

    A crash may leave a spawned reservation forever. Only matching durable process identity,
    with both owner and agent confirmed dead, releases its busy-session claim. This does not
    authorize replaying the original launch or fabricate an exit receipt.
    """
    scan = scan_launches(log_dir)
    if scan.opaque:
        raise LaunchDocumentError(log_dir, sorted(scan.opaque)[0], 'associations_incomplete', 'Unreadable launch associations prevent safe session reuse')
    busy = []
    for value in scan.entries:
        if value['state'] in {'settled', 'not_started'} and not value.get('reconciliation_pending'):
            continue
        if value.get('session_id') is None:
            record = read_task_record(log_dir, value['task_id'], include_prompt=False)
            if record is not None and record.owner == value.get('owner'):
                value = value | {'session_id': record.session_id}
        busy.append(value)
    return busy + scan.claims


@task_operation
def mark(log_dir: Path, task_id: str, generation: str, **changes):
    value = read_launch(log_dir, task_id)
    if value is None or value['generation'] != generation:
        raise ValueError('Task launch generation changed')
    _write(log_dir / f'{task_id}.launch.json', value | changes)


async def _transfer_authorization(descriptor: int, raw: bytes, deadline: float, task_id: str):
    """Bounded pipe transfer without blocking an executor or retaining a pipe after timeout."""
    loop = asyncio.get_running_loop()
    try:
        os.set_blocking(descriptor, False)
        offset = 0
        while offset < len(raw):
            if loop.time() >= deadline:
                raise RuntimeError(f'Task launch authorization transfer uncertain: {task_id}; reservation retained, do not retry execution')
            try:
                written = os.write(descriptor, raw[offset:offset + 65536])
            except BlockingIOError:
                await asyncio.sleep(min(0.01, max(0, deadline - loop.time())))
                continue
            offset += written
    finally:
        os.close(descriptor)


def invocation_request(invocation):
    """The JSON-safe invocation mapping a supervised launch transfers; `control_pipe` is carried only when set,
    so every other launch request is byte-identical to what it was before the flag existed."""
    request_invocation = {name: getattr(invocation, name) for name in (
        'argv', 'stdin_mode', 'extra_writable_paths', 'native_subagent', 'caller_approvals', 'caller_write_approvals', 'caller_approval_version', 'allowed_commands', 'allowed_commands_version', 'native_settings')}
    if invocation.control_pipe:
        request_invocation['control_pipe'] = True
    if invocation.system_guidance is not None:
        request_invocation['system_guidance'] = invocation.system_guidance
    if invocation.system_guidance_root is not None:
        request_invocation['system_guidance_root'] = invocation.system_guidance_root
    if invocation.control_context is not None:
        request_invocation['control_context'] = invocation.control_context
    if invocation.identity_markers is not None:
        request_invocation['identity_markers'] = list(invocation.identity_markers)
    for name in ('group_report_directory', 'group_report_identity', 'group_report_runtime', 'group_report_configuration', 'group_report_qualification'):
        value = getattr(invocation, name, None)
        if value is not None:
            request_invocation[name] = value
    request_invocation['env'] = dict(invocation.env)
    request_invocation['initial_input'] = base64.b64encode(invocation.initial_input).decode('ascii') if invocation.initial_input is not None else None
    return request_invocation


async def launch(registry, invocation, kwargs):
    from .extra_paths import serialize_paths
    from .task_attachment import AttachedTask
    task_id = store.validate_task_id(kwargs.get('task_id') or str(uuid.uuid4()))
    kwargs = dict(kwargs, task_id=task_id, dispatch_origin=registry._owner)
    # The caller's computed enforcement for the child; it is reported while the launch is pending and
    # is not part of what the owner executes.
    planned_enforcement = kwargs.pop('planned_enforcement', None)
    backend = kwargs.pop('backend')
    # Resolve only registered backend keys in the child. Test object injection uses local mode.
    from .backends import get
    if type(get(backend.name)) is not type(backend):
        raise ValueError('Supervised launches require a registered backend')
    backend.assert_safe(invocation, kwargs['freedom'], kwargs.get('network'))
    if invocation.control_pipe:
        from .backends.base import control_pipe_problem
        problem = control_pipe_problem(backend)
        if problem is not None:
            raise ValueError(problem)
    grants = serialize_paths(kwargs.pop('extra_path_grants', ()))
    kwargs['repo_path'] = str(kwargs['repo_path'])
    request_invocation = invocation_request(invocation)
    generation = uuid.uuid4().hex
    # Consumption of queued follow-ups is finalized by the owner at spawn when the launch may wait.
    followup_source = kwargs.pop('followup_source_task_id', None)
    resource_request = kwargs.get('resource_request')
    request = {'task_id': task_id, 'generation': generation, 'root': str(registry.state_layout.root),
               'backend': backend.name, 'invocation': request_invocation, 'grants': grants, 'kwargs': kwargs}
    if followup_source is not None and kwargs.get('message_ids'):
        request['followups'] = {'task_id': followup_source, 'ids': list(kwargs['message_ids'])}
    configuration = dict(request)
    configuration.pop('generation')
    # Observer provenance does not change the authorized execution configuration.
    configuration['kwargs'] = {key: value for key, value in kwargs.items() if key != 'dispatch_origin'}
    configuration_hash = hashlib.sha256(json.dumps(configuration, ensure_ascii=False, allow_nan=False).encode()).hexdigest()
    raw = json.dumps(request, ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > MAX_REQUEST:
        raise ValueError('Authorized task launch exceeds private transfer bound')
    log_dir = registry.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    # A launch into a worktree takes its usage lock just long enough to write the receipt, so a
    # removal that already decided "nothing uses it" cannot be overtaken by this launch. The
    # worktree is recognised from what was recorded for it (the plan of a fresh launch, the
    # `worktree` of the record being resumed), never from the root configured right now, which
    # can have changed since it was created.
    from . import worktrees
    recorded = kwargs.get('worktree_request') or kwargs.get('worktree_info')
    lock_target = recorded.get('path') if isinstance(recorded, dict) and recorded.get('path') else None
    if lock_target is None:
        # A plain launch whose repo_path is itself a recorded worktree races a removal just the same.
        # The scan reads every record and receipt, so it runs off the event loop and before the launch lock.
        inside = await asyncio.to_thread(worktrees.containing_worktree, log_dir, Path(kwargs['repo_path']))
        lock_target = str(inside) if inside is not None else None
    lock = os.open(log_dir / f'{task_id}.launch.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        # Nonblocking: a competing owner is never joined by executing the same request again.
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        existing = read_launch(log_dir, task_id)
        if store.read(log_dir, task_id) is not None or existing is not None and (existing['state'] != 'not_started' or existing.get('configuration_sha256') != configuration_hash):
            raise ValueError(f'task_id already reserved: {task_id}')
        value = {'format': FORMAT, 'product': PRODUCT, 'product_version': PRODUCT_VERSION, 'v': 1,
                 'task_id': task_id, 'generation': generation, 'root': str(log_dir.parent),
                 'directory_identity': _root_identity(log_dir),
                 'directory_binding': {'path': str(log_dir.resolve()), 'inode': log_dir.stat().st_ino}, 'state': 'reserved',
                 'request_sha256': hashlib.sha256(raw).hexdigest(), 'configuration_sha256': configuration_hash, 'backend': backend.name,
                 'session_id': kwargs.get('session_id'), 'parent_task_id': kwargs.get('parent_task_id'),
                 'chain_root_task_id': kwargs.get('chain_root_task_id'), 'scratch_chain_id': kwargs.get('scratch_chain_id'), 'owner': None,
                 'spawned_by': kwargs.get('spawned_by'), 'root_task_id': kwargs.get('root_task_id'),
                 'repo_path': kwargs['repo_path'], 'freedom': kwargs.get('freedom'), 'network': kwargs.get('network'),
                 'title': kwargs.get('title'), 'group': kwargs.get('group'),
                 'created_at': datetime.now(timezone.utc).isoformat()}
        if planned_enforcement is not None:
            value['enforcement'] = planned_enforcement
        if resource_request is not None:
            value['resources'] = {'names': list(resource_request['names']), 'wait_seconds': resource_request['wait_seconds'],
                                  'scope': resource_request.get('scope'), 'state': 'requested'}
        if 'followups' in request:
            value['followups'] = request['followups']
        if kwargs.get('worktree_request') is not None:
            value['worktree'] = dict(kwargs['worktree_request'])
        if lock_target is not None:
            try:
                with worktrees.usage_lock(Path(lock_target)):
                    if kwargs.get('worktree_request') is None and not Path(lock_target).is_dir():
                        raise worktrees.WorktreeError(f"the worktree {lock_target} no longer exists", code='worktree_missing')
                    _write(log_dir / f'{task_id}.launch.json', value)
            except worktrees.WorktreeError as exc:
                exc.harflow_not_started = True  # nothing was written or started
                raise
        else:
            _write(log_dir / f'{task_id}.launch.json', value)
    finally:
        os.close(lock)
    read_fd, write_fd = os.pipe()
    try:
        # Only the private authorization descriptor is inherited, never session/chain/store locks.
        proc = subprocess.Popen([sys.executable, '-m', 'harflow_mcp.task_supervisor', str(read_fd), task_id, generation],
                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                pass_fds=(read_fd,), close_fds=True, start_new_session=True,
                                env=child_environment(registry.state_layout))
    except OSError as exc:
        mark(log_dir, task_id, generation, state='not_started', error=str(exc))
        exc.harflow_not_started = True
        os.close(write_fd)
        raise
    finally:
        os.close(read_fd)
    # The transfer and acknowledgement share one deadline. Shielding only separates caller
    # cancellation from the owner; the transfer itself always closes its descriptor by deadline.
    deadline = asyncio.get_running_loop().time() + LAUNCH_HANDSHAKE_SECONDS
    transfer_job = asyncio.create_task(_transfer_authorization(write_fd, raw, deadline, task_id))
    registry._control_jobs.add(transfer_job)
    def transfer_finished(done):
        registry._control_jobs.discard(done)
        if not done.cancelled():
            done.exception()  # Retrieve an uncertain-transfer failure after caller cancellation.
    transfer_job.add_done_callback(transfer_finished)
    await asyncio.shield(transfer_job)
    while True:
        try:
            record = read_task_record(log_dir, task_id)
        except store.TaskDocumentError as exc:
            if not getattr(exc, '_source_changed_during_read', False):
                raise
            if asyncio.get_running_loop().time() >= deadline:
                raise
            await asyncio.sleep(0.025)
            continue
        if record is not None:
            task = AttachedTask(log_dir, task_id, registry=registry)
            registry._attached[task_id] = task
            registry.prune()
            return task
        try:
            value = read_launch(log_dir, task_id)
        except store.TaskDocumentError as exc:
            if not getattr(exc, '_source_changed_during_read', False) or asyncio.get_running_loop().time() >= deadline:
                raise
            await asyncio.sleep(0.025)
            continue
        if value is None:
            raise store.TaskDocumentError(task_id, 'Reserved launch source disappeared', code='task_unavailable')
        if value['state'] == 'not_started':
            # A launch that asked for resources and was refused (nothing free right now, or a borrow
            # that was no longer covered) is a request error, not an owner fault.
            error = (ValueError if value.get('resources') is not None or value.get('worktree') is not None or value.get('not_started_reason') == 'request' else RuntimeError)(value.get('error', 'Task owner refused launch before execution'))
            error.harflow_not_started = True
            raise error
        if value['state'] in {'waiting_for_resources', 'granted'}:
            # `waiting_for_resources` is written before the ledger entry exists (so a crash between
            # the two reads as never started). Acknowledge only once the entry is visible, so a
            # launch that declared resources holds or queues for them by the time `start` returns.
            from .task_resources import PendingLaunch, ledger_entry_visible
            # A wait of 0 is "grant now or fail": the caller hears the owner's first answer instead.
            immediate = value['state'] == 'waiting_for_resources' and (value.get('resources') or {}).get('wait_seconds') == 0
            if not immediate and (value['state'] == 'granted' or await asyncio.to_thread(ledger_entry_visible, log_dir, value)):
                # The owner holds the wait; the caller gets a handle, never a task that does not exist.
                return PendingLaunch(log_dir, value)
        if proc.poll() is not None or asyncio.get_running_loop().time() >= deadline:
            raise RuntimeError(f'Task launch acknowledgement uncertain: {task_id}; reservation retained, do not retry execution')
        await asyncio.sleep(0.025)


@task_operation
def owner_holds_unsettled(log_dir: Path, record):
    """A qualified detached owner may still be draining after its agent process exits."""
    if not store.outcome_unobserved(record):
        return False
    try:
        launch = read_launch(log_dir, record.task_id)
    except store.TaskDocumentError:
        return True  # Refused launch evidence cannot prove settlement.
    return bool(launch and launch['state'] in {'claimed', 'spawned'}
                and launch.get('owner') == record.owner
                and identity.identity_check(record.owner) == 'alive')


@task_operation
def attachable(log_dir: Path, task_id: str):
    record = read_task_record(log_dir, task_id)
    if record is None:
        return False
    if record.exit_code is not None and record.output_drained and record.finished_at:
        return True
    launch = read_launch(log_dir, task_id)
    return bool(launch and launch['state'] in {'claimed', 'spawned'} and launch.get('owner') == record.owner
                and identity.identity_check(record.owner) == 'alive')


@task_operation
def launch_refusal(log_dir: Path, task_id: str):
    """Optional per-task diagnostic; observing a refusal grants no mutation authority."""
    try:
        read_launch(log_dir, task_id)
    except LaunchDocumentError as exc:
        return exc.as_dict()
    return None
