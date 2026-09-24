"""Process identity: telling a live task's process apart from a reused pid.

A pid alone proves nothing once the OS recycles it. `capture` snapshots a pid's start time (from
`ps -o lstart=`) alongside markers that must appear on its command line; `identity_check` later
re-reads the same pid and decides whether it is still that process, a different one that landed on
the same number, or something `ps` could not settle either way.

This is deliberately not built on `store.process_alive`, which assumes a pid is still the task
whenever `ps` cannot say otherwise — the right default for "is this run still going" (the answer
that must never manufacture a false "gone" for a live run), but wrong for a control decision. For a
control decision — cancellation, takeover (A2+) — `undecidable` must never count as a match: only
`alive` may authorize acting on the process, and only `dead` may authorize treating it as gone.
"""

from __future__ import annotations

import functools
import logging
import os
import re
import subprocess
from collections.abc import Mapping, Sequence
from typing import Literal

log = logging.getLogger(__name__)

# `ps -o lstart=,command=` on a C locale: "Wed Sep 24 10:00:00 2026 <command...>". `command=` can be
# empty (e.g. a zombie), hence the trailing group being optional.
_PS_LINE = re.compile(r"^\s*(\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s?(.*)$")

IdentityCheck = Literal["alive", "dead", "undecidable"]


def _run_ps(pid: int) -> subprocess.CompletedProcess[str] | None:
    """One `ps` lookup for `pid`'s start time and command line. None if it could not be run."""
    try:
        return subprocess.run(
            ["ps", "-o", "lstart=,command=", "-p", str(pid)],
            env={**os.environ, "LC_ALL": "C"},
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None


def capture(pid: int, markers: Sequence[str]) -> dict | None:
    """Snapshot `pid`'s start time and given markers, for a later `identity_check` to compare.

    Returns None when `ps` could not tell (missing binary, timeout, the pid is already gone, or
    unparsable output) — never raises, per CLAUDE.md: bookkeeping must never change an outcome.
    """
    result = _run_ps(pid)
    if result is None or result.returncode != 0 or not result.stdout.strip():
        return None
    match = _PS_LINE.match(result.stdout.splitlines()[0])
    if match is None:
        return None
    start_time = " ".join(match.group(1).split())
    return {"pid": pid, "start_time": start_time, "markers": list(markers)}


def identity_check(identity: Mapping | None) -> IdentityCheck:
    """Whether the process an earlier `capture` recorded is still that same process.

    See the module docstring for why `undecidable` must never be treated as a match by a caller
    making a control decision.
    """
    if not isinstance(identity, Mapping):
        return "undecidable"
    pid = identity.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return "undecidable"
    markers = [m for m in (identity.get("markers") or [])]
    recorded_start = identity.get("start_time")

    result = _run_ps(pid)
    if result is None:
        return "undecidable"

    stdout = result.stdout.strip()
    if result.returncode != 0 and not stdout:
        return "dead" if not result.stderr.strip() else "undecidable"
    if not stdout:
        return "undecidable"

    match = _PS_LINE.match(result.stdout.splitlines()[0])
    if match is None:
        return "undecidable"
    current_start = " ".join(match.group(1).split())
    command = match.group(2)

    if recorded_start is not None:
        if current_start != recorded_start:
            return "dead"
        if not markers:
            return "alive"
        return "alive" if all(marker in command for marker in markers) else "undecidable"

    # Legacy record, or a fresh one whose `capture` call has not landed yet: no start_time to
    # compare, so fall back to today's pid+markers test — reported conservatively, since this is a
    # weaker signal than a matching start time.
    if not markers:
        return "undecidable"
    return "undecidable" if all(marker in command for marker in markers) else "dead"


@functools.cache
def own_identity() -> dict:
    """This server process's own identity, computed once and cached.

    Warmed in `server.main()` before `mcp.run()` so the first task this process dispatches already
    has a real owner identity rather than paying the `ps` call lazily.
    """
    identity = capture(os.getpid(), [])
    if identity is not None:
        return identity
    return {"pid": os.getpid(), "start_time": None, "markers": []}
