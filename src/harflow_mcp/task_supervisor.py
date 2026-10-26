"""Exec-only task owner entrypoint; authorization is accepted solely on an inherited pipe."""
from __future__ import annotations

import asyncio
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid

from . import identity, store
from .backends import Invocation, get
from .extra_paths import restore_paths
from .state_ownership import StateOwnershipError, state_session
from . import followups
from .tasks import TaskRegistry, _before_spawn
from .task_supervision import MAX_REQUEST, mark, read_launch
from . import resources as resource_ledger
from . import task_resources
from datetime import datetime, timedelta, timezone


REAP_INTERVAL_SECONDS = 15.0


class _NotStarted(Exception):
    """The launch settled `not_started` before spawning (its receipt already says so)."""


def _iso(moment: datetime) -> str:
    return moment.isoformat()


async def _create_worktree(registry, task_id: str, generation: str, plan: dict, grant: dict | None) -> dict:
    """Create the planned worktree in the owner, recording `creating` and `created` in the receipt.

    Runs after any resource grant, so the base ref is resolved after the wait. A refusal or failure
    settles the launch `not_started`, releases the resources and reports a partial directory; nothing
    is removed automatically.
    """
    from . import worktrees
    log_dir = registry.log_dir

    def settle(exc: Exception, partial: str | None) -> None:
        if grant is not None:
            task_resources.release_quietly(log_dir, grant['reservation_id'])
        info = plan | {'state': 'failed', 'error': str(exc)[:500], **({'partial_path': partial} if partial else {})}
        mark(log_dir, task_id, generation, state='not_started', not_started_reason='worktree_failed', error=str(exc)[:1000],
             worktree=info)
        task_resources.clear_cancel(log_dir, task_id)

    if task_resources.cancel_requested(log_dir, task_id):
        settle(RuntimeError('cancelled before the worktree was created'), None)
        mark(log_dir, task_id, generation, not_started_reason='cancelled')
        raise _NotStarted()

    def creating(info: dict) -> None:
        mark(log_dir, task_id, generation, worktree=info)

    try:
        created = await asyncio.to_thread(worktrees.create, plan, on_creating=creating)
    except worktrees.WorktreeError as exc:
        settle(exc, exc.partial_path)
        raise _NotStarted() from None
    except OSError as exc:
        settle(exc, plan['path'] if os.path.lexists(plan['path']) else None)
        raise _NotStarted() from None
    mark(log_dir, task_id, generation, worktree=created)
    return created


async def _acquire_resources(registry, task_id: str, generation: str, request: dict) -> dict:
    """Wait for the requested names in the owner, recording each step in the launch receipt.

    The reservation id is written before the entry can exist, so a crash between the two leaves a
    receipt that names an entry that was never created, which the resolver reads as never started.
    Returns the grant to attach to the record. Raises `_NotStarted` after settling the receipt.
    """
    log_dir = registry.log_dir
    ledger = task_resources.ledger_for(log_dir)
    names, wait = request['names'], request['wait_seconds']
    scope = request.get('scope')
    reservation_id = uuid.uuid4().hex
    started = datetime.now(timezone.utc)
    held = {'names': names, 'wait_seconds': wait, 'scope': scope, 'state': 'waiting', 'reservation_id': reservation_id,
            'wait_started_at': _iso(started), 'deadline_at': _iso(started + timedelta(seconds=wait))}

    def settle(reason: str, error: str) -> None:
        task_resources.release_quietly(log_dir, reservation_id)
        mark(log_dir, task_id, generation, state='not_started', not_started_reason=reason, error=error,
             resources=held | {'state': 'not_started'})
        task_resources.clear_cancel(log_dir, task_id)

    mark(log_dir, task_id, generation, state='waiting_for_resources', resources=held)
    last_reap = [0.0]

    async def keep_waiting() -> bool:
        # Every waiter also drives the reaper, so a holder that died is noticed while anyone waits.
        if time.monotonic() - last_reap[0] >= REAP_INTERVAL_SECONDS:
            last_reap[0] = time.monotonic()
            try:
                await asyncio.to_thread(task_resources.reap, log_dir)
            except StateOwnershipError:
                raise
            except Exception:
                pass
        return not task_resources.cancel_requested(log_dir, task_id)

    try:
        entry = await ledger.acquire(
            names, {'kind': 'task', 'task_id': task_id},
            scope=resource_ledger.Scope(scope['reservation_id']) if scope is not None else None,
            timeout=wait, reservation_id=reservation_id,
            should_continue=keep_waiting)
    except resource_ledger.ResourceWaitExpired as exc:
        settle('resource_wait_expired', str(exc))
        raise _NotStarted() from None
    except resource_ledger.ResourceWaitCancelled as exc:
        settle('cancelled', str(exc))
        raise _NotStarted() from None
    except resource_ledger.ResourceError as exc:
        settle('owner_error', f'{exc.code}: {exc}')
        raise _NotStarted() from None
    granted = held | {'state': 'granted', 'granted_at': entry.granted_at}
    if entry.borrows_from is not None:
        granted['borrowed_from'] = entry.borrows_from
    if task_resources.cancel_requested(log_dir, task_id):
        held = granted
        settle('cancelled', 'cancelled before the task started')
        raise _NotStarted()
    mark(log_dir, task_id, generation, state='granted', resources=granted)
    return {'names': names, 'reservation_id': reservation_id, 'state': 'held', 'wait_seconds': wait,
            'wait_started_at': held['wait_started_at'], 'granted_at': entry.granted_at,
            **({'borrowed_from': entry.borrows_from} if entry.borrows_from is not None else {})}


def _release_followups(log_dir, task_id: str, request: dict, outcome: str) -> None:
    """Settle the queued follow-ups a waiting launch was handed. Guarded: bookkeeping never changes an outcome."""
    handed = request.get('followups')
    if not handed:
        return
    try:
        followups.finish(log_dir, handed['task_id'], task_id, handed['ids'], outcome)
    except StateOwnershipError:
        raise
    except Exception:
        pass


async def run(descriptor: int, task_id: str, generation: str):
    # No durable file can authorize execution. EOF before a complete bounded request refuses it.
    with os.fdopen(descriptor, 'rb') as handle:
        raw = handle.read(MAX_REQUEST + 1)
    if len(raw) > MAX_REQUEST:
        raise ValueError('Private authorization exceeds its bound')
    request = json.loads(raw)
    registry = TaskRegistry(execution_mode='supervised')
    with state_session(registry.state_layout):
        lock = os.open(registry.log_dir / f'{store.validate_task_id(task_id)}.launch.lock', os.O_RDWR | os.O_NOFOLLOW)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            saved = read_launch(registry.log_dir, task_id)
            if (saved is None or saved['generation'] != generation or saved['state'] != 'reserved'
                    or request.get('task_id') != task_id or request.get('generation') != generation
                    or request.get('root') != str(registry.state_layout.root)
                    or hashlib.sha256(raw).hexdigest() != saved['request_sha256']):
                raise ValueError('Private authorization does not match task reservation')
            mark(registry.log_dir, task_id, generation, state='claimed', owner=registry._owner)
            started = False
            grant = None
            try:
                worktree_request = request['kwargs'].pop('worktree_request', None)
                rebuild = request['kwargs'].pop('rebuild', None)
                if request['kwargs'].get('resource_request') is not None:
                    try:
                        grant = await _acquire_resources(registry, task_id, generation, request['kwargs'].pop('resource_request'))
                    except _NotStarted:
                        _release_followups(registry.log_dir, task_id, request, 'released')
                        return
                else:
                    request['kwargs'].pop('resource_request', None)
                    if worktree_request is not None:
                        # Acknowledge now: creating a checkout can take longer than the caller's handshake.
                        mark(registry.log_dir, task_id, generation, state='granted')
                created = None
                if worktree_request is not None:
                    try:
                        created = await _create_worktree(registry, task_id, generation, worktree_request, grant)
                    except _NotStarted:
                        _release_followups(registry.log_dir, task_id, request, 'released')
                        return
                # Everything from here to the spawn is before the spawn: a conclusive failure in
                # rebuilding the invocation settles the launch `not_started`, never leaves it uncertain.
                with _before_spawn():
                    prepared = request['invocation']
                    prepared['initial_input'] = base64.b64decode(prepared['initial_input'], validate=True) if prepared['initial_input'] is not None else None
                    prepared['extra_writable_paths'] = tuple(prepared['extra_writable_paths'])
                    prepared['allowed_commands'] = tuple(prepared['allowed_commands'])
                    if 'identity_markers' in prepared:
                        prepared['identity_markers'] = tuple(prepared['identity_markers'])
                    invocation = Invocation(**prepared)
                    if created is not None:
                        # The checkout exists now: everything that reads the repository is built against it.
                        invocation = await registry._rebuild_invocation(rebuild, Path(created['path']))
                        request['kwargs']['worktree_info'] = {key: created.get(key) for key in (
                            'path', 'source_repo_path', 'base_ref', 'base_commit', 'branch', 'detached')}
                    kwargs = request['kwargs']
                    kwargs['repo_path'] = Path(kwargs['repo_path'])
                    kwargs['backend'] = get(request['backend'])
                    kwargs['extra_path_grants'] = restore_paths(request['grants'])
                    registry._launch_generations = {task_id: generation}
                task = await registry._execute_prepared(invocation, **kwargs, **({'resource_grant': grant} if grant is not None else {}))
                started = True
                # The spawn is the consumption point for follow-ups that were handed to a launch that
                # may have waited; `finish` is idempotent with the caller-side consumption.
                _release_followups(registry.log_dir, task_id, request, 'consumed')
                mark(registry.log_dir, task_id, generation, state='spawned')
                # A cancel that arrived after the last pre-spawn look: the agent exists now, so it
                # takes the ordinary cancel path. The request file is cleared either way.
                late_cancel = task_resources.cancel_requested(registry.log_dir, task_id)
                task_resources.clear_cancel(registry.log_dir, task_id)
                if late_cancel:
                    try:
                        await registry.cancel_cascade(task.task_id, workflow_control=True)
                    except StateOwnershipError:
                        raise
                    except Exception:
                        pass  # the owner keeps supervising the agent; the caller can cancel it again
                await task.done.wait()
                if task.monitor is not None:
                    await task.monitor
                # Continuations are owned by this supervisor through reservation and acknowledgement.
                while getattr(registry, '_continuations', None):
                    await asyncio.gather(*tuple(registry._continuations), return_exceptions=True)
                # Bounded drain failure remains an observed terminal run. Release its launch
                # reservation without authorizing continuation or native-child settlement.
                if (task.exit_code is not None and task.finished_at is not None
                        and task.status in store.TERMINAL_RECORD_STATUSES and task.latest_record_landed):
                    mark(registry.log_dir, task_id, generation, state='settled', continuation_scheduling='settled')
            except BaseException as exc:
                # A record or ambiguous process creation means execution can never safely retry.
                if not started and store.read(registry.log_dir, task_id) is None and getattr(exc, 'harflow_not_started', False):
                    cancelled = isinstance(exc, task_resources.LaunchCancelled)
                    if grant is not None:
                        task_resources.release_quietly(registry.log_dir, grant['reservation_id'])
                    _release_followups(registry.log_dir, task_id, request, 'released')
                    mark(registry.log_dir, task_id, generation, state='not_started', error=str(exc),
                         **({'not_started_reason': 'cancelled' if cancelled else 'owner_error'}
                            if grant is not None or worktree_request is not None or cancelled else {}))
                    task_resources.clear_cancel(registry.log_dir, task_id)
                    if cancelled:
                        return  # an honoured cancel is an outcome, not an owner failure
                raise
        finally:
            task_resources.clear_cancel(registry.log_dir, task_id)
            os.close(lock)


if __name__ == '__main__':
    import traceback
    from .task_supervision import _write
    from .state_layout import resolve_layout
    from .wire_contracts import PRODUCT, PRODUCT_VERSION
    layout = resolve_layout()
    # Keep the store lease through asyncio's cancellation and default-executor shutdown.
    with state_session(layout):
        try:
            asyncio.run(run(int(sys.argv[1]), sys.argv[2], sys.argv[3]))
        except BaseException as exc:
            _write(layout.tasks_directory / f'{store.validate_task_id(sys.argv[2])}.supervisor-error.json',
                   {'product': PRODUCT, 'product_version': PRODUCT_VERSION,
                    'format': 'harflow.task-supervisor-error', 'v': 1,
                    'task_id': sys.argv[2], 'generation': sys.argv[3],
                    'root': str(layout.root), 'error': str(exc)[:4096],
                    'traceback': ''.join(traceback.format_exception(exc))[-16384:]})
            raise
