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


def check_detail(identity: Mapping | None) -> tuple[IdentityCheck, str]:
    """Same verdict as `identity_check`, plus the reason it was reached.

    See the module docstring for why `undecidable` must never be treated as a match by a caller
    making a control decision. Reasons: `invalid` (not a mapping, or an unusable pid), `ps_failed`
    (the `ps` lookup itself did not work or returned nothing usable), `pid_absent` (`ps` confirmed
    the pid is gone), `unparsable` (`ps` ran but its output line could not be read); for a record
    carrying a `start_time`: `start_time_differs` (dead — a different process landed on the same
    pid), `start_time_match` (alive, whether or not markers were also required), `markers_missing`
    (start time matched but a required marker did not); and for a legacy record with no
    `start_time`: `legacy_no_markers`, `legacy_markers_seen`, `legacy_markers_not_seen`.
    """
    if not isinstance(identity, Mapping):
        return "undecidable", "invalid"
    pid = identity.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return "undecidable", "invalid"
    markers = [m for m in (identity.get("markers") or [])]
    recorded_start = identity.get("start_time")

    result = _run_ps(pid)
    if result is None:
        return "undecidable", "ps_failed"

    stdout = result.stdout.strip()
    if result.returncode != 0 and not stdout:
        if not result.stderr.strip():
            return "dead", "pid_absent"
        return "undecidable", "ps_failed"
    if not stdout:
        return "undecidable", "ps_failed"

    match = _PS_LINE.match(result.stdout.splitlines()[0])
    if match is None:
        return "undecidable", "unparsable"
    current_start = " ".join(match.group(1).split())
    command = match.group(2)

    if recorded_start is not None:
        if current_start != recorded_start:
            return "dead", "start_time_differs"
        if not markers:
            return "alive", "start_time_match"
        if all(marker in command for marker in markers):
            return "alive", "start_time_match"
        return "undecidable", "markers_missing"

    # Legacy record, or a fresh one whose `capture` call has not landed yet: no start_time to
    # compare, so fall back to today's pid+markers test — reported conservatively, since this is a
    # weaker signal than a matching start time.
    if not markers:
        return "undecidable", "legacy_no_markers"
    if all(marker in command for marker in markers):
        return "undecidable", "legacy_markers_seen"
    return "dead", "legacy_markers_not_seen"


def identity_check(identity: Mapping | None) -> IdentityCheck:
    """Whether the process an earlier `capture` recorded is still that same process.

    See the module docstring for why `undecidable` must never be treated as a match by a caller
    making a control decision.
    """
    return check_detail(identity)[0]


def may_signal(identity: Mapping | None) -> bool:
    """Whether it is safe to send this process a signal.

    True only for a confirmed `alive` verdict, or for the legacy pid+markers fallback that actually
    saw its markers on the command line (`legacy_markers_seen`) — both `undecidable` outcomes, but
    one of them once matched real evidence and the other (e.g. `ps_failed`) confirmed nothing at
    all. A caller with a real process handle for the target (e.g. its own child) does not need this
    at all; it is for the cross-process case, where `ps` is the only evidence available.
    """
    return signalable(*check_detail(identity))


def signalable(verdict: IdentityCheck, reason: str) -> bool:
    """`may_signal`'s rule applied to an observation already made — so a caller that also records
    the verdict (e.g. as `leader_alive`) decides and records from the same `ps` call, not two."""
    return verdict == "alive" or reason == "legacy_markers_seen"


def task_identity(pid: int, start_time: str | None, markers: Sequence[str]) -> dict:
    """Build the `{pid, start_time, markers}` shape `identity_check`/`check_detail` expect."""
    return {"pid": pid, "start_time": start_time, "markers": list(markers)}


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
