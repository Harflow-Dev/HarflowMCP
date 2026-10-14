"""Fresh sessions with fail-closed, cross-process continuation reservations."""
from __future__ import annotations

import asyncio
import json
import hashlib
import os
import stat
import uuid
from pathlib import Path
from typing import Any

from . import control, store
from .backends import get as get_backend

CONTEXT_BYTES = 64 * 1024
NOTES_BYTES = 64 * 1024


def validate_budget(value: int) -> None:
    if type(value) is not int or not 0 <= value <= 10:
        raise ValueError("auto_continue must be an integer from 0 to 10")


def _write(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('x', encoding='utf-8') as file:
            os.fchmod(file.fileno(), 0o600)
            json.dump(value, file, ensure_ascii=False)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _section(label: str, text: str, reference: str) -> str:
    if len(text.encode('utf-8')) > CONTEXT_BYTES:
        return f'{label}: oversized evidence; read verbatim from {reference}\n'
    return f'{label}:\n{text}\n'


def context(registry: Any, record: store.TaskRecord, extra_prompt: str | None, notes_path: str | None) -> str:
    root = store.read(registry.log_dir, record.chain_root_task_id) if record.chain_root_task_id else record
    if root is None or root.prompt_error or root.prompt_truncated:
        raise ValueError('Original chain assignment unavailable')
    original = root.chain_prompt if root.chain_prompt is not None else root.prompt
    assignment_reference = str(registry.log_dir / root.prompt_source) if root.prompt_source else str(store.record_path(registry.log_dir, root.task_id))
    parts = ['Continue the original assignment in a fresh session. Inspect the existing work before changing it; observations do not establish task attribution.\n']
    parts.append(_section('Original assignment', original, assignment_reference))
    task = registry.get(record.task_id)
    snapshot = task.snapshot() if task else store.snapshot(registry.log_dir, record)
    parts.append(_section('Latest summary', snapshot.get('summary') or 'Unavailable.', str(store.log_path(registry.log_dir, record.task_id))))
    parts.append(_section('Observed outcome', json.dumps(record.outcome, ensure_ascii=False), str(store.record_path(registry.log_dir, record.task_id))))
    if notes_path is not None:
        path = Path(notes_path).expanduser().resolve()
        # Caller-selected local evidence only; special files and oversized reads are refused.
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise ValueError('notes_path must identify a regular file')
            with os.fdopen(descriptor, 'rb', closefd=False) as file:
                raw = file.read(NOTES_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(raw) > NOTES_BYTES:
            parts.append(f'Notes: oversized evidence; read verbatim from {path}\n')
        else:
            parts.append(_section('Caller notes', raw.decode('utf-8'), str(path)))
    if extra_prompt is not None:
        parts.append('Caller follow-up:\n' + extra_prompt + '\n')
    return '\n'.join(parts)


async def stop_chain(registry: Any, task_id: str) -> None:
    """Serialize cancellation with reservation/spawn so an acknowledged stop cannot restart."""
    source = store.read(registry.log_dir, task_id)
    if source is None or not (source.auto_continue or source.continuation_of_task_id or (registry.log_dir / f'{task_id}.continuation.json').exists()):
        return
    root = source.chain_root_task_id or task_id
    async with control.session_lock(registry.log_dir, 'continuation-' + root):
        _write(registry.log_dir / f'{root}.continuation-stopped.json', {'cancelled_task_id': task_id})


async def continue_task(registry: Any, task_id: str, extra_prompt: str | None = None, notes_path: str | None = None, *, automatic: bool = False) -> Any:
    store.validate_task_id(task_id)
    if extra_prompt is not None and not isinstance(extra_prompt, str):
        raise ValueError('extra_prompt must be text')
    if notes_path is not None and not isinstance(notes_path, str):
        raise ValueError('notes_path must be text')
    caller = await registry._mutation_caller()
    from .workflow_hooks import refuse_managed
    if caller is not None:
        refuse_managed(registry.log_dir, caller.record.task_id)
    refuse_managed(registry.log_dir, task_id)
    source = store.read(registry.log_dir, task_id)
    if source is None:
        raise ValueError(f'Unknown task: {task_id}')
    root = source.chain_root_task_id or task_id
    async with control.session_lock(registry.log_dir, 'continuation-' + root):
        if (registry.log_dir / f'{root}.continuation-stopped.json').exists():
            raise ValueError('Continuation chain was cancelled; start a new task explicitly')
        # Reload inside the lock: durable exit/drain evidence is mandatory, including recovery.
        source = store.read(registry.log_dir, task_id)
        if source is None or source.status not in {'completed', 'failed', 'timed_out', 'cancelled'} or source.exit_code is None or not source.finished_at or not source.output_drained:
            raise ValueError('Continuation requires observed exit, fully drained output and a durable terminal record')
        live = registry.get(task_id)
        if live is not None and (not live.done.is_set() or live.proc is not None and live.proc.returncode is None):
            raise ValueError('Source task has not settled')
        if source.prompt_error or source.prompt_truncated:
            raise ValueError('Original assignment unavailable; continuation refused')
        path = registry.log_dir / f'{task_id}.continuation.json'
        prompt = context(registry, source, extra_prompt, notes_path)
        fingerprint = hashlib.sha256(json.dumps({'prompt': prompt, 'automatic': automatic}, ensure_ascii=False).encode('utf-8')).hexdigest()
        reservation = None
        if path.exists():
            with path.open('rb') as file:
                reservation_bytes = file.read(4097)
            if len(reservation_bytes) > 4096:
                raise ValueError('Continuation reservation exceeds its bounded metadata size')
            reservation = json.loads(reservation_bytes)
        if reservation:
            if reservation.get('request_fingerprint') != fingerprint:
                # Public manual recovery may take over a proven unlaunched automatic
                # attempt. Match the complete reconstructed request, including legacy
                # failure records which did not retain the automatic metadata flag.
                automatic_fingerprint = hashlib.sha256(json.dumps({'prompt': prompt, 'automatic': True}, ensure_ascii=False).encode('utf-8')).hexdigest()
                if (automatic or reservation.get('state') != 'not_started'
                        or reservation.get('request_fingerprint') != automatic_fingerprint
                        or registry.get(reservation['task_id']) is not None
                        or store.read(registry.log_dir, reservation['task_id']) is not None):
                    raise ValueError('Continuation already reserved for a different caller request; use its successor for further work')
            child = registry.get(reservation['task_id'])
            if child is not None:
                return child
            if store.read(registry.log_dir, reservation['task_id']) is not None:
                raise ValueError(f"Continuation already exists: {reservation['task_id']}")
            if reservation.get('state') != 'not_started':
                raise ValueError(f"Continuation reservation unsettled: {reservation['task_id']}; refusing duplicate execution")
        if automatic:
            if source.auto_continue <= 0 or live is None or source.status == 'cancelled' or live.cancel_requested or live.acc.denials or live.acc.failure_diagnostic:
                raise ValueError('Automatic continuation is not eligible')
            reason = get_backend(source.backend).continuation_reason(live.acc, source.exit_code)
            if reason is None:
                raise ValueError('No certified recoverable cutoff observed')
        identifier = reservation['task_id'] if reservation else str(uuid.uuid4())
        _write(path, {'task_id': identifier, 'state': 'reserved', 'chain_root_task_id': root, 'automatic': automatic, 'request_fingerprint': fingerprint})
        try:
            child = await registry.start(prompt, Path(source.repo_path), backend=get_backend(source.backend), freedom=source.freedom, model=source.model, reasoning_effort=source.reasoning_effort, network=source.network, max_turns=source.max_turns, group=source.group, title=source.title, task_id=identifier, auto_continue=max(source.auto_continue - 1, 0) if automatic else 0, continuation_of_task_id=task_id, chain_root_task_id=root, chain_prompt=None)
            # Preserve delegation ownership, independently of fresh-session ancestry.
            child.spawned_by, child.root_task_id = source.spawned_by, source.root_task_id
            child.depth, child.max_depth = source.depth, source.max_depth
            registry.persist(child)
        except BaseException as exc:
            if getattr(exc, 'polybridge_not_started', False):
                _write(path, {'task_id': identifier, 'state': 'not_started', 'chain_root_task_id': root, 'request_fingerprint': fingerprint})
            raise
        _write(path, {'task_id': identifier, 'state': 'started', 'chain_root_task_id': root, 'request_fingerprint': fingerprint})
        return child


def schedule_automatic(registry: Any, task: Any) -> None:
    """Run after durable settlement; retain the source until the reservation resolves."""
    if task.status == 'cancelled' or task.cancel_requested or task.acc.denials or task.acc.failure_diagnostic or get_backend(task.backend).continuation_reason(task.acc, task.exit_code) is None:
        registry.prune()
        return
    sources = getattr(registry, "_continuation_sources", None)
    if sources is None:
        sources = registry._continuation_sources = set()
    sources.add(task.task_id)
    async def launch() -> None:
        try:
            await continue_task(registry, task.task_id, automatic=True)
        except Exception as exc:
            task.bridge_notices.append(f'Automatic continuation was not started: {exc}')
            registry.persist(task)
        finally:
            sources.discard(task.task_id)
            registry.prune()
    pending = getattr(registry, '_continuations', None)
    if pending is None:
        pending = registry._continuations = set()
    successor = asyncio.create_task(launch())
    pending.add(successor)
    successor.add_done_callback(pending.discard)
