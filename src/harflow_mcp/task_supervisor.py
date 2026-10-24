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

from . import identity, store
from .backends import Invocation, get
from .extra_paths import restore_paths
from .state_ownership import state_session
from .tasks import TaskRegistry
from .task_supervision import MAX_REQUEST, mark, read_launch


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
            try:
                prepared = request['invocation']
                prepared['initial_input'] = base64.b64decode(prepared['initial_input'], validate=True) if prepared['initial_input'] is not None else None
                prepared['extra_writable_paths'] = tuple(prepared['extra_writable_paths'])
                prepared['allowed_commands'] = tuple(prepared['allowed_commands'])
                invocation = Invocation(**prepared)
                kwargs = request['kwargs']
                kwargs['repo_path'] = Path(kwargs['repo_path'])
                kwargs['backend'] = get(request['backend'])
                kwargs['extra_path_grants'] = restore_paths(request['grants'])
                registry._launch_generations = {task_id: generation}
                task = await registry._execute_prepared(invocation, **kwargs)
                started = True
                mark(registry.log_dir, task_id, generation, state='spawned')
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
                    mark(registry.log_dir, task_id, generation, state='not_started', error=str(exc))
                raise
        finally:
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
