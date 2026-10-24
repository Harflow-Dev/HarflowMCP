"""Read-only preflight for registered, schema-preserving storage transitions."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sqlite3
import stat

from .state_compatibility import persisted_compatible
from .wire_contracts import PRODUCT

_TASK_FORMATS = {
    '.meta.json': 'harflow.task', '.followups.json': 'harflow.task.followups',
    '.artifacts.json': 'harflow.task.artifacts', '.approvals.json': 'harflow.task.approvals',
    '.launch.json': 'harflow.task-launch', '.observations-final.json': 'harflow.task-observations',
    '.observations.jsonl': 'harflow.task-observations', '.inbox.jsonl': 'harflow.task.inbox',
}
_CONTROL = re.compile(r'.+\.(?:cancel|takeover)\.\d+\.[A-Za-z0-9_-]+$')


def authoritative_format(relative: Path) -> str | None:
    """Classify only authoritative paths; raw events/vendor logs are never decoded."""
    if len(relative.parts) != 2:
        return None
    directory, name = relative.parts
    if directory in {'workflows', 'workflow-runs', 'workflow-owners'} and name.endswith('.json') and not name.startswith('.'):
        return {'workflows': 'harflow.workflow.definition', 'workflow-runs': 'harflow.workflow.run',
                'workflow-owners': 'harflow.workflow.receipt'}[directory]
    if directory == 'tasks':
        for suffix, format_name in _TASK_FORMATS.items():
            if name.endswith(suffix):
                return format_name
        if _CONTROL.fullmatch(name):
            return 'harflow.task.control'
    return None


def read_regular(path: Path, bound: int = 64 * 1024 * 1024) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > bound:
            raise ValueError('Authoritative state document is not a bounded regular file')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read(bound + 1)
        after = path.lstat()
        if len(raw) > bound or (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size) != (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_size):
            raise ValueError('Authoritative state document changed during preflight')
        return raw
    finally:
        os.close(fd)


def validate_promotion(root: Path, *, target_version: str) -> None:
    """Validate known private envelopes/schemas before changing the marker.

    Payload semantics remain the responsibility of existing format readers. No
    payload is rewritten. Unknown raw logs and derived indexes are not schemas.
    """
    from .state_ownership import StateIncompatible
    try:
        for directory_name in ('tasks', 'workflows', 'workflow-runs', 'workflow-owners'):
            directory = root / directory_name
            if not directory.exists() and not directory.is_symlink():
                continue
            if not stat.S_ISDIR(directory.lstat().st_mode):
                raise ValueError('Authoritative state directory must not be a symlink')
            for path in directory.iterdir():
                format_name = authoritative_format(path.relative_to(root))
                if format_name is None:
                    continue
                raw = read_regular(path)
                rows = raw.splitlines() if path.suffix == '.jsonl' else [raw]
                for row in rows:
                    value = json.loads(row)
                    if (not isinstance(value, dict) or value.get('product') != PRODUCT
                            or value.get('format') != format_name
                            or not persisted_compatible(value.get('product_version'), format_name,
                                                        value.get('version') if format_name == 'harflow.task.approvals' else value.get('v'), target_version=target_version)):
                        raise ValueError('Unqualified private state envelope: ' + str(path.relative_to(root)))
        path = root / 'notifications.sqlite3'
        if path.exists() or path.is_symlink():
            if not stat.S_ISREG(path.lstat().st_mode):
                raise ValueError('Notification database must be regular')
            for suffix in ('-wal', '-shm', '-journal'):
                sidecar = Path(str(path) + suffix)
                if sidecar.exists() or sidecar.is_symlink():
                    # Immutable SQLite does not read WAL. A clean checkpoint is
                    # required before a storage-line transition.
                    if not stat.S_ISREG(sidecar.lstat().st_mode) or sidecar.stat().st_size:
                        raise ValueError('Checkpoint notification database before upgrading the storage line')
            with sqlite3.connect(path.absolute().as_uri() + '?mode=ro&immutable=1', uri=True) as db:
                from .notification_database import qualify
                qualify(db)
    except (OSError, ValueError, TypeError, sqlite3.Error, RecursionError) as exc:
        raise StateIncompatible('Storage migration preflight failed: ' + str(exc),
                                reason='migration_preflight_failed') from exc
