"""Advisory ctl observations; admission continues to use its locked existence check."""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
from datetime import datetime, timezone
from typing import Any

from . import store
from .catalog import current_metadata_budget
from .state_access import open_descriptor, task_layout, task_operation
from .state_ownership import StateOwnershipError, assert_owned

ATTRIBUTION_BYTES = 4096
REFUSAL_MESSAGE = 'Continuation chain was cancelled; start a new task explicitly'


def marker_path(directory: Path, root: str) -> Path:
    store.validate_task_id(root)
    return directory / f'{root}.continuation-stopped.json'


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _valid_id(value: Any) -> bool:
    return isinstance(value, str) and store.TASK_ID_PATTERN.fullmatch(value) is not None


def refusal_data(value: Any) -> dict[str, str] | None:
    if (isinstance(value, dict) and set(value) == {'reason', 'chain_root_task_id'}
            and value['reason'] == 'chain_stopped' and _valid_id(value['chain_root_task_id'])):
        return dict(value)
    return None


def project(value: Any) -> dict[str, Any] | None:
    """Validate a complete observation without truncating identities or reading state."""
    keys = {'state', 'chain_root_task_id', 'reason', 'cancelled_task_id', 'observed_at'}
    if not isinstance(value, dict) or set(value) != keys:
        return None
    state, root, reason, cancelled, stamp = (value[k] for k in ('state', 'chain_root_task_id', 'reason', 'cancelled_task_id', 'observed_at'))
    if state not in ('stopped', 'not_stopped', 'unknown') or not isinstance(stamp, str) or len(stamp) > 64:
        return None
    try:
        if datetime.fromisoformat(stamp.replace('Z', '+00:00')).utcoffset() is None:
            return None
    except (ValueError, OverflowError):
        return None
    if root is not None and not _valid_id(root):
        return None
    if state == 'stopped':
        if root is None or reason != 'chain_cancelled' or cancelled is not None and not _valid_id(cancelled):
            return None
    elif reason is not None or cancelled is not None or state == 'not_stopped' and root is None:
        return None
    return dict(value)


def _identity(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _inspect(directory: Path, path: Path) -> tuple[str, str | None]:
    layout = task_layout(directory)
    parent = descriptor = None
    try:
        before = directory.lstat()
        if not stat.S_ISDIR(before.st_mode):
            return 'unknown', None
        parent = open_descriptor(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK, layout=layout)
        if _identity(before) != _identity(os.fstat(parent)):
            return 'unknown', None

        def stable_parent():
            return _identity(before) == _identity(os.fstat(parent)) == _identity(directory.lstat())

        try:
            source = path.lstat()
        except FileNotFoundError:
            # A failed open alone cannot establish absence, especially across rename.
            try:
                path.lstat()
            except FileNotFoundError:
                return ('not_stopped', None) if stable_parent() else ('unknown', None)
            return 'unknown', None
        if not stat.S_ISREG(source.st_mode):
            return 'unknown', None
        try:
            descriptor = open_descriptor(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW,
                                         layout=layout, dir_fd=parent)
        except StateOwnershipError:
            # A moved task directory is observer uncertainty, but a failed root
            # lease must retain its enclosing refusal rather than being hidden.
            assert_owned(layout)
            if not stable_parent():
                return 'unknown', None
            raise
        if _identity(source) != _identity(os.fstat(descriptor)):
            return 'unknown', None
        cancelled = None
        budget = current_metadata_budget()
        available = ATTRIBUTION_BYTES if budget is None else max(0, budget.metadata_limit - budget.metadata_bytes)
        if source.st_size <= min(ATTRIBUTION_BYTES, available):
            raw = bytearray()
            while len(raw) < source.st_size:
                chunk = os.read(descriptor, source.st_size - len(raw))
                if budget is not None:
                    budget.metadata_bytes += len(chunk)
                if not chunk:
                    return 'unknown', None
                raw.extend(chunk)
            if budget is None or budget.metadata_decodes < 100:
                if budget is not None:
                    budget.metadata_decodes += 1
                try:
                    value = json.loads(raw)
                    if isinstance(value, dict) and _valid_id(value.get('cancelled_task_id')):
                        cancelled = value['cancelled_task_id']
                except (ValueError, UnicodeError, RecursionError):
                    pass
        if (_identity(source) != _identity(os.fstat(descriptor))
                or _identity(source) != _identity(path.lstat()) or not stable_parent()):
            return 'unknown', None
        return 'stopped', cancelled
    finally:
        if descriptor is not None:
            descriptor.close()
        if parent is not None:
            parent.close()


@task_operation
def observe(directory: Path, record: store.TaskRecord) -> dict[str, Any]:
    """Read an already-qualified record's chain marker under selected-root ownership."""
    return observe_identity(directory, task_id=record.task_id, chain_root_task_id=record.chain_root_task_id)


@task_operation
def observe_identity(directory: Path, *, task_id: Any, chain_root_task_id: Any) -> dict[str, Any]:
    """Observe the identity from one qualified record or its coherent status projection.

    Callers supply both fields from the same source; no task or ancestry is reread.
    """
    root = chain_root_task_id if chain_root_task_id is not None else task_id
    state, cancelled = 'unknown', None
    if not _valid_id(task_id) or not _valid_id(root):
        root = None
    else:
        try:
            state, cancelled = _inspect(directory, marker_path(directory, root))
        except StateOwnershipError:
            raise
        except Exception:
            # Optional diagnostics cannot turn a successful authorized status into failure.
            pass
    return dict(state=state, chain_root_task_id=root,
                reason='chain_cancelled' if state == 'stopped' else None,
                cancelled_task_id=cancelled, observed_at=_now())
