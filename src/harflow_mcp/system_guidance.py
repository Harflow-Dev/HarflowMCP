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

VERSION = "scratch-guidance-1"
FIRST_TURN_STICKY = "first_turn_sticky"
PER_RUN = "per_run"
MODES = frozenset({FIRST_TURN_STICKY, PER_RUN})

DELIVERED = "delivered"
REUSED_FROM_SESSION = "reused_from_session"
UNAVAILABLE = "unavailable"

TEXT = (
    "Harflow scratch folders. When set, $HARFLOW_SCRATCH is private to this task, $HARFLOW_CHAIN_SCRATCH is "
    "shared with every resume and continuation of this conversation, and $HARFLOW_GROUP_SCRATCH is shared with "
    "every task in the same group. Prefer them over /tmp for screenshots, logs and notes; the chain and group "
    "folders persist, so put there anything a later run or a sibling task should find. A folder may be read-only "
    "for this run: say so instead of writing to /tmp."
)
MAX_BYTES = 600

# opencode reads instruction files by path. The file is bridge-owned, outside every writable scope.
FILE_NAME = f"opencode-{VERSION}.md"
FILE_DIRECTORY = "guidance"


def instructions_file(state_root: Path) -> Path:
    """The read-only instructions file for per-run seams, created (or repaired) idempotently."""
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


def state_of(mode: str | None, *, delivered: bool, resume: bool) -> str:
    """What a run got: delivered, reused from its session, or unavailable."""
    if delivered:
        return DELIVERED
    if mode == FIRST_TURN_STICKY and resume:
        return REUSED_FROM_SESSION
    return UNAVAILABLE
