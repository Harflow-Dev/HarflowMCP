"""Best-effort capacity preflight; not a reservation against concurrent disk use."""
from __future__ import annotations

from pathlib import Path
import shutil
import tempfile

MIN_FREE_BYTES = 256 * 1024 * 1024


class DiskSpaceError(ValueError):
    """A launch refused before agent creation; safe to retry after freeing space."""
    harflow_not_started = True


def check_launch_space(state: Path, repository: Path) -> None:
    """Check state/output, checkout and temporary storage before creating a launch reservation."""
    try:
        temporary = Path(tempfile.gettempdir())
    except OSError as exc:
        raise DiskSpaceError(f'Cannot locate writable temporary storage: {exc}; free disk space and restore temporary-directory access before retrying. No agent was started.') from exc
    checked = set()
    for requested in (state, repository, temporary):
        path = requested
        while not path.exists() and path.parent != path:
            path = path.parent
        try:
            device = path.stat().st_dev
            if device in checked:
                continue
            checked.add(device)
            free = shutil.disk_usage(path).free
        except OSError as exc:
            raise DiskSpaceError(f'Cannot check available disk space for {requested}: {exc}; restore filesystem access before retrying') from exc
        if free < MIN_FREE_BYTES:
            raise DiskSpaceError(
                f'Disk nearly full at {path}: {free} bytes available; at least {MIN_FREE_BYTES} bytes '
                'are required to launch a task. Free space on this filesystem and retry; no agent was started.'
            )
