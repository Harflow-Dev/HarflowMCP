"""The static scratch-folder note Harflow delivers through a backend's measured system-prompt seam.

The note names the scratch environment variables and says to prefer them over `/tmp`. It carries no
path: the variables carry the per-run paths, so a seam that reads the note once per session (Claude
and Codex keep the text a session started with) is harmless. The caller's prompt is never touched: it
is stored and delivered byte for byte, which is why guidance travels through a system-prompt seam and
never as a prefix. A backend declares its seam as `Capabilities.system_guidance`:

* `first_turn_sticky`: the text is read when the session starts and kept for the session. Delivered on a
  fresh start; a resume reuses the session's own copy (`reused_from_session`).
* `per_run`: read on every launch (start and resume).
* `None`: no seam that grants nothing else was measured; the variables and docs are the only guidance.

Delivery is disclosed per run in the task's `scratch.system_guidance` (`delivered`,
`reused_from_session` or `unavailable`). Nothing depends on it: a failure to attach it never blocks or
changes a launch.
"""
from __future__ import annotations

import os
from pathlib import Path

from .state_layout import resolve_layout
from .state_ownership import assert_owned

VERSION = "scratch-guidance-3"
FIRST_TURN_STICKY = "first_turn_sticky"
PER_RUN = "per_run"
MODES = frozenset({FIRST_TURN_STICKY, PER_RUN})

DELIVERED = "delivered"
REUSED_FROM_SESSION = "reused_from_session"
UNAVAILABLE = "unavailable"

TEXT = (
    "Harflow: folders may be read-only; respect scratch.access; do not fall back to /tmp. Prefer writable $HARFLOW_SCRATCH for temporary files and plan-<task_id>.md; otherwise use the backend built-in plan location. "
    "Never change settings to enable writes. Writable $HARFLOW_GROUP_SCRATCH permits report-$HARFLOW_TASK_ID.md; peers share it, so use task-specific filenames and atomic replacement. "
    "$HARFLOW_CHAIN_SCRATCH and group scratch persist across related tasks. Return final plans as result.technical_plan in workflow JSON. Harflow publishes final task-named outputs without agent write grants."
)

MAX_BYTES = 600

# opencode reads instruction files by path. The file is bridge-owned, outside every writable scope.
FILE_NAME = f"opencode-{VERSION}.md"
FILE_DIRECTORY = "guidance"


def instructions_file(state_root: Path) -> Path:
    """The read-only instructions file for per-run seams, created (or repaired) idempotently.

    Written only into a state root this process owns, like every other state writer."""
    assert_owned(resolve_layout(root=Path(state_root)))
    directory = Path(state_root) / FILE_DIRECTORY
    if directory.is_symlink():
        raise ValueError("the guidance directory must not be a symlink")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / FILE_NAME
    wanted = TEXT + "\n"
    if path.is_symlink():
        raise ValueError("the guidance file must not be a symlink")
    try:
        if path.read_text(encoding="utf-8") == wanted:
            return path
    except FileNotFoundError:
        pass
    temporary = path.with_name(f".{FILE_NAME}.{os.getpid()}.tmp")
    try:
        temporary.write_text(wanted, encoding="utf-8")
        os.chmod(temporary, 0o444)
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return path


def is_canonical_file(path: str, state_root: str | os.PathLike[str] | None) -> bool:
    """Whether `path` is the bridge-owned instructions file of `state_root`: absolute, named
    `FILE_DIRECTORY/FILE_NAME` directly under that root, neither it nor its directory a symlink, and
    holding exactly the static note. An invocation that points a CLI at any other file, a well-formed
    copy under another root included, is refused, however the pointer looks."""
    if state_root is None:
        return False
    candidate = Path(path)
    if (not candidate.is_absolute() or candidate.name != FILE_NAME or candidate.parent.name != FILE_DIRECTORY
            or candidate.parent.is_symlink() or candidate.is_symlink()):
        return False
    try:
        if os.path.realpath(candidate.parent.parent) != os.path.realpath(state_root):
            return False
        return candidate.is_file() and candidate.read_text(encoding="utf-8") == TEXT + "\n"
    except (OSError, ValueError):
        return False


def state_of(mode: str | None, *, delivered: bool, resume: bool) -> str:
    """What a run got: delivered, reused from its session, or unavailable."""
    if delivered:
        return DELIVERED
    if mode == FIRST_TURN_STICKY and resume:
        return REUSED_FROM_SESSION
    return UNAVAILABLE
