"""The one wait contract shared by wait_for_task, wait_for_tasks and wait_for_workflow.

Waits are read-only observers: they never write a record, signal a process or resume a session.
Everything here is about *how long* an observer may hold a request, how it says why it stopped, and
how it reports liveness to a client — not about what any one wait is watching.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("polybridge")

# The largest explicit `timeout_seconds` any wait accepts. A request is honoured up to this value
# and never silently clamped; anything larger is refused and pointed at `until="terminal"`.
MAX_BOUNDED_WAIT_SECONDS = 3600

# `until="terminal"` holds until the stop condition, but not forever: a client that never sends
# cancellation must not be able to pin an observer for the life of the server.
UNTIL_TERMINAL_CEILING_SECONDS = 6 * 3600

# Emitted while waiting so the client can see the wait is alive; per the MCP spec a client may
# also reset its request timeout on progress.
PROGRESS_INTERVAL_SECONDS = 5.0

# A progress callback gets this long before it is abandoned for the round.
PROGRESS_CALL_SECONDS = 0.25

# A recovered run whose process is gone but whose owning polybridge server is still alive is not
# yet "settled without an observed exit": that server is draining output and about to persist the
# outcome. Waits keep observing for this long (tasks.DRAIN_GRACE_SECONDS plus a margin) before
# treating the run as settled-but-unobserved.
OWNER_DRAIN_GRACE_SECONDS = 15.0

# Consecutive failed observation rounds before an unobservable target stops a wait.
UNAVAILABLE_ROUNDS = 3

UNTIL_MODES = ("timeout", "terminal")

STOP_TERMINAL = "terminal"
STOP_ATTENTION = "attention"
STOP_UNAVAILABLE = "observation_unavailable"
STOP_TIMEOUT = "timeout"
STOP_CEILING = "ceiling"


def monotonic() -> float:
    """The clock waits measure deadlines and elapsed time with (a seam for tests)."""
    return time.monotonic()


@dataclass(frozen=True)
class WaitPlan:
    """A validated wait request: the mode, what was asked for, and the hold it resolves to."""

    until: str
    requested: int | None
    effective: int

    @classmethod
    def validate(cls, timeout_seconds: Any, until: Any, default: int) -> "WaitPlan":
        """Resolve a request or raise ValueError naming what is wrong. Never clamps."""
        if until not in UNTIL_MODES:
            raise ValueError('until must be "timeout" or "terminal"')
        if until == "terminal":
            if timeout_seconds is not None:
                raise ValueError(
                    'timeout_seconds cannot be combined with until="terminal"; omit it '
                    "(the wait is bounded by a server ceiling instead)"
                )
            return cls("terminal", None, UNTIL_TERMINAL_CEILING_SECONDS)
        if timeout_seconds is None:
            timeout_seconds = default
        if type(timeout_seconds) is not int or not 0 <= timeout_seconds <= MAX_BOUNDED_WAIT_SECONDS:
            raise ValueError(
                f"timeout_seconds must be between 0 and {MAX_BOUNDED_WAIT_SECONDS}, got "
                f'{timeout_seconds!r}; for a longer wait use until="terminal"'
            )
        return cls("timeout", timeout_seconds, timeout_seconds)

    @property
    def terminal_mode(self) -> bool:
        return self.until == "terminal"

    @property
    def expiry_reason(self) -> str:
        return STOP_CEILING if self.terminal_mode else STOP_TIMEOUT

    @property
    def progress_total(self) -> int | None:
        if self.terminal_mode:
            return None
        return max(1, math.ceil(self.effective / PROGRESS_INTERVAL_SECONDS))

    def begin(self) -> "WaitClock":
        return WaitClock(self, monotonic())


@dataclass(frozen=True)
class WaitClock:
    plan: WaitPlan
    started: float

    def elapsed(self) -> float:
        return max(0.0, monotonic() - self.started)

    def remaining(self) -> float:
        return self.plan.effective - self.elapsed()


async def report_progress(
    callback: Callable[..., Awaitable[Any]] | None,
    progress: float,
    total: float | None,
    message: str,
) -> None:
    """Best-effort progress: a missing, failing or slow callback never affects the wait.

    Only `Exception` is absorbed — cancellation of the wait itself still propagates.
    """
    if callback is None:
        return
    try:
        await asyncio.wait_for(callback(progress, total=total, message=message), PROGRESS_CALL_SECONDS)
    except Exception:
        log.debug("client did not accept progress", exc_info=True)


def polling_guidance(stop_reason: str, tool: str, inspect_with: str, elapsed: float) -> str:
    seconds = f"{elapsed:.0f}s"
    if stop_reason == STOP_TERMINAL:
        return "The wait condition is met; nothing further to wait for."
    if stop_reason == STOP_ATTENTION:
        return (
            f"Needs attention; inspect it with {inspect_with}, act, then call {tool} again if you "
            "still need to wait. Waits never cancel or signal anything."
        )
    if stop_reason == STOP_UNAVAILABLE:
        return (
            f"Could not keep observing after {seconds}; the run itself is unaffected. Inspect it "
            f"with {inspect_with}, then call {tool} again."
        )
    if stop_reason == STOP_CEILING:
        return (
            f"Reached the server's {UNTIL_TERMINAL_CEILING_SECONDS}s ceiling for until=\"terminal\" "
            f"after {seconds}; the run is unaffected. Call {tool} again to keep waiting."
        )
    return (
        f"Still active after {seconds} and unaffected by this wait. Call {tool} again to keep "
        "waiting; waits never cancel or signal anything."
    )


def result_fields(
    plan: WaitPlan, stop_reason: str, elapsed: float, tool: str, inspect_with: str,
    guidance: str | None = None,
) -> dict[str, Any]:
    """The additive response fields every wait carries, under the same names.

    `guidance` replaces the generic wording when the caller knows something more precise, such as
    a `terminal` stop that left other tasks active.
    """
    return {
        "requested_timeout_seconds": plan.requested,
        "effective_timeout_seconds": plan.effective,
        "until": plan.until,
        "timed_out": stop_reason in (STOP_TIMEOUT, STOP_CEILING),
        "stop_reason": stop_reason,
        "elapsed_seconds": round(elapsed, 3),
        "polling_guidance": guidance or polling_guidance(stop_reason, tool, inspect_with, elapsed),
    }


WAIT_FIELD_NAMES = (
    "requested_timeout_seconds", "effective_timeout_seconds", "until", "timed_out",
    "stop_reason", "elapsed_seconds", "polling_guidance",
)
