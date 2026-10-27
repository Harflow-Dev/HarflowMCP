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
    store.validate_task_id(task_id)
    path = log_dir / f'{task_id}.launch.json'
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise store.TaskDocumentError(task_id, 'Task launch source is not a regular file')
        with os.fdopen(descriptor, 'rb', closefd=False) as handle:
            raw = handle.read(65537)
        try:
            after = path.lstat()
        except FileNotFoundError as exc:
            raise store.TaskDocumentError(task_id, 'Task launch source disappeared while reading', code='task_unavailable') from exc
        if (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size) != (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_size):
            error = store.TaskDocumentError(task_id, 'Task launch source changed while reading', code='task_unavailable')
            error._source_changed_during_read = True
            raise error
    finally:
        os.close(descriptor)
    try:
        value = json.loads(raw)
        if (len(raw) > 65536 or not isinstance(value, dict) or value.get('format') != FORMAT
                or value.get('product') != PRODUCT or type(value.get('v')) is not int or value.get('v') != 1
                or not persisted_compatible(value.get('product_version'), FORMAT)
                or value.get('task_id') != task_id or value.get('root') != str(log_dir.parent)
                or value.get('directory_identity') != _root_identity(log_dir)
                or not isinstance(value.get('generation'), str) or re.fullmatch('[a-f0-9]{32}', value['generation']) is None
                or not isinstance(value.get('request_sha256'), str) or re.fullmatch('[a-f0-9]{64}', value['request_sha256']) is None
                or not isinstance(value.get('backend'), str)
                or any(value.get(key) is not None and not isinstance(value[key], str) for key in ('session_id', 'parent_task_id', 'chain_root_task_id', 'scratch_chain_id'))
                or value.get('owner') is not None and (not isinstance(value['owner'], dict) or type(value['owner'].get('pid')) is not int or value['owner']['pid'] <= 0 or value['owner'].get('start_time') is not None and not isinstance(value['owner']['start_time'], str) or not isinstance(value['owner'].get('markers'), list) or any(not isinstance(marker, str) for marker in value['owner']['markers']))
                or value.get('state') not in STATES
                or any(value.get(key) is not None and not isinstance(value[key], str) for key in ('spawned_by', 'root_task_id', 'repo_path', 'freedom', 'title', 'group', 'created_at', 'not_started_reason'))
                or value.get('resources') is not None and (not isinstance(value['resources'], dict) or not isinstance(value['resources'].get('names'), list))
                or value.get('worktree') is not None and (not isinstance(value['worktree'], dict) or not isinstance(value['worktree'].get('path'), str))
                or value.get('followups') is not None and (not isinstance(value['followups'], dict) or not isinstance(value['followups'].get('task_id'), str) or not isinstance(value['followups'].get('ids'), list))):
            raise ValueError('Incompatible task launch reservation')
        return value
    except (ValueError, TypeError) as exc:
        raise store.TaskDocumentError(task_id, str(exc)) from exc


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
def reservations(log_dir: Path):
    """Qualified launch evidence, including unresolved execution attempts."""
    return [value for path in log_dir.glob('*.launch.json')
            if (value := read_launch(log_dir, path.name.removesuffix('.launch.json'))) is not None]


@task_operation
def unresolved(log_dir: Path):
    return [value for value in reservations(log_dir) if value['state'] not in {'settled', 'not_started'}]


@task_operation
def session_reservations(log_dir: Path):
    """Unsettled launches that can still write a conversation; ambiguous launches stay reserved.

    A crash may leave a spawned reservation forever. Only matching durable process identity,
    with both owner and agent confirmed dead, releases its busy-session claim. This does not
    authorize replaying the original launch or fabricate an exit receipt.
    """
    from . import task_resources
    busy = []
    for launch in unresolved(log_dir):
        record = read_task_record(log_dir, launch['task_id'], include_prompt=False)
        # A dead owner is confirmed first and the receipt and record are read again afterwards: the
        # probe can be slow, and an owner that granted, spawned and died meanwhile left an agent.
        if (launch['state'] == 'waiting_for_resources' and record is None and launch.get('owner') is not None
                and task_resources.owner_confirmed_gone(log_dir, launch) is not None):
            # Spawn requires `granted` first, so a dead owner that was still waiting never started.
            continue
        if (launch['state'] == 'granted' and record is None and launch.get('owner') is not None
                and (launch.get('worktree') or {}).get('state') in {'planned', 'creating'}
                and (fresh := task_resources.owner_confirmed_gone(log_dir, launch, ('granted',))) is not None
                and (fresh.get('worktree') or {}).get('state') in {'planned', 'creating'}):
            # Spawn follows `created`, so an owner that died while the worktree was still being
            # planned or made never started the agent.
            continue
        if (launch['state'] in {'claimed', 'granted', 'spawned'} and record is not None
                and launch.get('owner') is not None and record.owner == launch['owner']
                and record.session_id == launch.get('session_id') and record.pid is not None
                and identity.identity_check(record.owner) == 'dead'
                and identity.identity_check(identity.task_identity(record.pid, record.start_time, record.markers)) == 'dead'):
            # `granted` belongs here too: the record is written before the receipt moves to `spawned`.
            continue
        busy.append(launch)
    return busy


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
                 'directory_identity': _root_identity(log_dir), 'state': 'reserved',
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
            error = (ValueError if value.get('resources') is not None or value.get('worktree') is not None else RuntimeError)(value.get('error', 'Task owner refused launch before execution'))
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
    launch = read_launch(log_dir, record.task_id)
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
