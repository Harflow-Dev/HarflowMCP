"""Background lifecycle for headless agent runs.

One `Task` owns one subprocess plus the asyncio tasks that drain its pipes and watch for its
exit. Nothing here blocks a caller for the duration of a run: `start` returns as soon as the
process is spawned, and completion is observed through `Task.done`.

Three invariants keep the state machine honest:

* Only `_monitor` publishes a terminal status and sets `Task.done`, and only after the process has
  exited and its pipes have been fully read. Callers therefore never observe a task as finished
  while its process is still alive.
* Conversely, `_monitor` publishes no status when it is itself cancelled: that means this server is
  being torn down, while the run — its own session leader — carries on. The task stays `running` for
  whichever process looks next. The exception is a cancellation already in flight, whose intent
  nothing on disk could reconstruct.
* Termination is owned by a `Task`-scoped asyncio task, not by the caller that asked for it, so
  SIGKILL escalation still happens if that caller goes away mid-cancellation.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
import subprocess
import sys
import uuid
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Awaitable, Callable
from typing import Any, Literal, NamedTuple

from . import control, identity, inbox, lineage, retention, store
from .backends import (
    Accumulator,
    Backend,
    Enforcement,
    Invocation,
    check_nested_depth,
    check_nested_enforcement,
)
from .backends import get as get_backend
from .backends import resume_command
from .events import EVENTS_SUFFIX, EventLog, events_path
from .stream import parse_line

log = logging.getLogger(__name__)

Status = Literal["running", "completed", "failed", "timed_out", "cancelled"]

TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "failed", "timed_out", "cancelled"})

# A single `system`/`init` event already runs to several KB and grows with the host's tool count,
# so StreamReader's 64 KiB default line limit is not enough headroom.
STREAM_LINE_LIMIT = 8 * 1024 * 1024

# Retained per task for `last_output_tail`. Individual lines are truncated too, so a long run
# cannot grow this without bound.
TAIL_LINES = 200
TAIL_LINE_CHARS = 2000
STDERR_TAIL_LINES = 50

TAIL_LINES_RETURNED = 20

SIGKILL_GRACE_SECONDS = 5.0

# How often the monitor re-checks `control.cancel_verdict` while an attempt is pending, and how
# often a cascade polls a non-local target's record for a terminal status. No deadline of its own
# on the monitor side — see `_await_cancel_verdict` — but this is also the tick size for every
# bounded cascade wait, so it stays short relative to `SIGKILL_GRACE_SECONDS`/`DRAIN_GRACE_SECONDS`.
CANCEL_VERDICT_POLL_SECONDS = 0.25

# `TaskRegistry.cancel_cascade` re-scans for new targets after every round (a target's own
# signalling can itself spawn a child, or advance a settled intermediate); this bounds how many
# times it will do that before reporting what it found rather than looping forever on a lineage
# that keeps growing.
CASCADE_MAX_ROUNDS = 5

# Retries for a `.sig` whose write failed after the SIGTERM it records was delivered: a few quick
# in-line attempts, then a registry-held background job that keeps going with capped backoff.
_SIG_WRITE_ATTEMPTS = 3
_SIG_WRITE_RETRY_SECONDS = 0.1
SIG_RETRY_INITIAL_SECONDS = 0.5
SIG_RETRY_MAX_SECONDS = 30.0

# Why a signalled case-3 target's `cancelled` did not land, by `control.close_record_if_open`
# outcome. "written" and "terminal" are absent: the first landed, the second was already settled.
_NOT_RECORDED_REASONS = {
    "missing": "signalled, but its record no longer exists, so no cancellation was recorded",
    "owner_not_dead": (
        "signalled, but its owning server is no longer confirmed dead, so only that server may "
        "record the outcome"
    ),
    "write_failed": "signalled, but writing its cancelled status to disk failed",
}

# How many times a joiner restarts after finding its attempt settled under it (see
# `_deliver_recorded_cancel`) before reporting the target as not signalled.
_JOIN_ATTEMPTS = 3

# How long `resume`/`resume_record` wait to acquire a session's lock before giving up. Held across
# the check-and-spawn it protects (see `control.session_lock`'s docstring and the plan's Notes on
# why that is fine), so this bounds how long a second resume on a busy session waits before it is
# told to try again rather than starting a competing run.
SESSION_LOCK_TIMEOUT_SECONDS = 30.0

# How long to keep reading after the process exits. Normally EOF is immediate, but a backgrounded
# grandchild can inherit the write end of stdout and hold it open indefinitely, which would
# otherwise leave the run unfinishable.
DRAIN_GRACE_SECONDS = 10.0

MAX_TASKS = 200

# Live input: how long a run may wait on background tasks alone — a result in, nothing else
# running, no output at all — before the input pump closes its stdin anyway. Closing kills those
# tasks (measured), so the run is then reported `failed`, never a clean completion.
LIVE_IDLE_ENV = "PB_LIVE_IDLE_SECONDS"
DEFAULT_LIVE_IDLE_SECONDS = 600.0

# What a close is for, so `_close_input` can re-check that it still holds once it has the lock.
CLOSE_STOP = "stop"
CLOSE_IDLE = "idle"
CLOSE_IDLE_BOUND = "idle_bound"


def live_idle_seconds(environ: Any = None) -> float:
    """`PB_LIVE_IDLE_SECONDS`, or the default for anything missing, unparsable, or not > 0."""
    raw = (os.environ if environ is None else environ).get(LIVE_IDLE_ENV)
    if raw is None:
        return DEFAULT_LIVE_IDLE_SECONDS
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return DEFAULT_LIVE_IDLE_SECONDS
    if not (value > 0) or value == float("inf"):
        return DEFAULT_LIVE_IDLE_SECONDS
    return value


def background_idle_bound_reached(acc: Accumulator, silent_for: float, bound: float) -> bool:
    """Whether a live run has waited on background tasks alone for the idle bound: a result has
    arrived, no turn is running, it is not idle only because background tasks are open, and there
    has been no output for `bound` seconds."""
    return (
        acc.result_count > 0
        and bool(acc.background_open)
        and not acc.turn_open
        and not acc.awaiting_input
        and silent_for >= bound
    )

# Short and strict on purpose: these are local, read-only ref lookups (never a fetch or ls-remote),
# so a slow or hanging git must not be allowed to delay a dispatch. The budget is for the *whole*
# check, not per call — several probes each allowed the full timeout would stack up into a delay
# before the agent even starts.
GIT_PROBE_BUDGET_SECONDS = 3.0

# Separate budget from GIT_PROBE_BUDGET_SECONDS: this is its own `to_thread` hop, run unconditionally
# (unlike the branch-disclosure probe, which is skipped outright below `publish`), so it needs its
# own accounting rather than sharing one that was sized for a check some dispatches never pay.
GIT_BASELINE_BUDGET_SECONDS = 1.5

_UNDETERMINED_PUBLISH_NOTICE = (
    "this task was dispatched at freedom {freedom!r}, which authorizes it to commit, push, and "
    "open a PR — but which branch it would land on could not be determined at spawn (no remote "
    "HEAD recorded locally, a detached HEAD, several remotes with none named 'origin', git being "
    "unavailable, or the check timing out). For a publish-authorized run, that is itself worth "
    "knowing: cancel this task now if you need to confirm the target branch before it proceeds. "
    "This was sampled once, immediately before the agent was launched — not during the run — so "
    "the branch may already have changed by the time the agent started, and will not be "
    "re-checked if it changes later."
)

_ON_DEFAULT_BRANCH_PUBLISH_NOTICE = (
    "this task was dispatched at freedom {freedom!r} while checked out on the repository's default "
    "branch ({branch!r}), and is authorized to commit, push, and open a PR directly against it — "
    "nothing here blocked that. Cancel this task now if publishing to {branch!r} was not intended. "
    "This was sampled once, immediately before the agent was launched — not during the run — so "
    "the branch may already have changed by the time the agent started, and will not be "
    "re-checked if it changes later."
)

# The disclosure state comes from the Enforcement block, never from the freedom or backend name:
# authorization is `publish_attempts_allowed_by_polybridge`, network reachability is
# `network_access`. Two more states needed their own wordings once `network` became requestable —
# authorized with polybridge's own network barrier raised (publish + network=False on codex: a
# network-backed push is blocked, but a push to a local path was measured to still succeed, so
# the barrier must never be worded as stopping publishing outright), and not authorized while
# the mechanism nevertheless reaches a remote (codex write_in_repo + network=True:
# byte-identical to publish's default, so the old freedom-name gate silently skipped it).
_ON_DEFAULT_BRANCH_NETWORK_BLOCKED_NOTICE = (
    "this task was dispatched at freedom {freedom!r} while checked out on the repository's default "
    "branch ({branch!r}), and is authorized to commit, push, and open a PR directly against it — "
    "polybridge's own network barrier blocks a network-backed push and any network-backed PR "
    "creation here, but a push to a local path (measured: a bare repo under a writable root) "
    "still succeeds, so the barrier stops network-backed operations only. Cancel this task now if "
    "publishing to {branch!r} was not intended. This was sampled once, immediately before the "
    "agent was launched — not during the run — so the branch may already have changed by the time "
    "the agent started, and will not be re-checked if it changes later."
)
_UNDETERMINED_NETWORK_BLOCKED_NOTICE = (
    "this task was dispatched at freedom {freedom!r}, which authorizes it to commit, push, and "
    "open a PR — but which branch it would land on could not be determined at spawn (no remote "
    "HEAD recorded locally, a detached HEAD, several remotes with none named 'origin', git being "
    "unavailable, or the check timing out). Polybridge's own network barrier blocks a "
    "network-backed push and any network-backed PR creation here, but a push to a local path "
    "(measured: a bare repo under a writable root) still succeeds, so the barrier stops "
    "network-backed operations only. For a publish-authorized run, that is itself worth "
    "knowing: cancel this task now if you need to confirm the target branch before it proceeds. "
    "This was sampled once, immediately before the agent was launched — not during the run — so "
    "the branch may already have changed by the time the agent started, and will not be "
    "re-checked if it changes later."
)
_ON_DEFAULT_BRANCH_UNAUTHORIZED_REACHABLE_NOTICE = (
    "this task was dispatched at freedom {freedom!r}, which does not authorize committing, "
    "pushing, or opening a PR — but the mechanism it runs under nevertheless permits remote "
    "publication (enforcement.network_access is {network_access!r}), so a push can genuinely "
    "reach a remote and nothing here blocked that. It is checked out on the repository's default "
    "branch ({branch!r}). Cancel this task now if that was not intended. This was sampled once, "
    "immediately before the agent was launched — not during the run — so the branch may already "
    "have changed by the time the agent started, and will not be re-checked if it changes later."
)
_UNDETERMINED_UNAUTHORIZED_REACHABLE_NOTICE = (
    "this task was dispatched at freedom {freedom!r}, which does not authorize committing, "
    "pushing, or opening a PR — but the mechanism it runs under nevertheless permits remote "
    "publication (enforcement.network_access is {network_access!r}), so a push can genuinely "
    "reach a remote, and which branch it would land on could not be determined at spawn (no "
    "remote HEAD recorded locally, a detached HEAD, several remotes with none named 'origin', "
    "git being unavailable, or the check timing out). That is itself worth knowing: cancel this "
    "task now if you need to confirm the target branch before it proceeds. This was sampled "
    "once, immediately before the agent was launched — not during the run — so the branch may "
    "already have changed by the time the agent started, and will not be re-checked if it "
    "changes later."
)

# Where network=True was accepted by a backend that has no barrier of its own to impose
# (enforcement.network_access stays "not_controlled"), acceptance must not silently read as "the
# network now works". Worded about what polybridge imposes, never about reachability — the
# environment decides there, and a claim either way would overclaim.
_UNCONTROLLED_NETWORK_NOTICE = (
    "network=True was accepted, but polybridge imposes no network barrier of its own on this "
    "backend — there is no mechanism it could raise or lower, so whether this run can reach the "
    "network is decided entirely by the surrounding environment. This is not a claim of "
    "reachability either way."
)


# These probes run in the polybridge server itself, before any agent sandbox exists, so a target
# repository must not get to run code through them (Codex PR review). Its config can name programs
# git runs on its behalf: `core.fsmonitor` (disabled here; `-c` also reaches submodules' child
# gits), and a promisor remote's lazy fetch, which can run `core.sshCommand` or a remote helper
# (`GIT_NO_LAZY_FETCH`). Only ref lookups are ever made — nothing that reads file contents, which
# is where clean/process filters would run; that is why there is no `git status` here.
GIT_SAFE_CONFIG = ("-c", "core.fsmonitor=false")
GIT_SAFE_ENV = {"GIT_NO_LAZY_FETCH": "1"}


def _git_read(repo_path: Path, deadline: float, *args: str) -> str | None:
    """Run one read-only git ref lookup. `None` for a non-zero exit — including git's own graceful
    "not applicable" cases, like `--quiet` on a detached HEAD — never an exception for those.

    A missing `git` binary or a timed-out call raise instead of returning `None`: those are exactly
    the failures `_publish_branch_notice`'s single guard exists to catch, so they must reach it
    rather than being silently absorbed a layer early. Never a fetch or `ls-remote` — only locally
    recorded refs.
    """
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise subprocess.TimeoutExpired(cmd="git", timeout=GIT_PROBE_BUDGET_SECONDS)
    result = subprocess.run(
        ["git", "-C", str(repo_path), *GIT_SAFE_CONFIG, *args],
        env={**os.environ, **GIT_SAFE_ENV},
        capture_output=True,
        text=True,
        timeout=remaining,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _git_baseline(repo_path: Path) -> tuple[str | None, bool | None]:
    """`(base_commit, start_dirty)` at spawn time, best-effort — nulls on any failure.

    `start_dirty` is always None now: telling a dirty worktree needs `git status`, which reads file
    contents and so runs the repository's own clean/process filters in this process (see
    `GIT_SAFE_CONFIG`). Nothing in polybridge or the Monitor acts on it, so it is not probed rather
    than probed unsafely; the field stays for payload compatibility.

    Same rule as `_publish_branch_notice`: a slow, missing, or broken git must never change a
    dispatch's outcome, so every failure mode here is absorbed rather than raised.
    """
    try:
        deadline = time.monotonic() + GIT_BASELINE_BUDGET_SECONDS
        return _git_read(repo_path, deadline, "rev-parse", "--verify", "--quiet", "HEAD"), None
    except Exception:
        log.warning("could not determine git baseline for %s", repo_path, exc_info=True)
        return None, None


def _default_branch_remote(repo_path: Path, deadline: float) -> str | None:
    """Which remote's locally recorded HEAD names the default branch — never guessed.

    `origin` is preferred when present. With no `origin` but exactly one remote, that one is
    unambiguous. Several remotes with none named `origin` is left unavailable rather than picking
    arbitrarily.
    """
    remotes_raw = _git_read(repo_path, deadline, "remote")
    if not remotes_raw:
        return None
    remotes = [line.strip() for line in remotes_raw.splitlines() if line.strip()]
    if not remotes:
        return None
    if "origin" in remotes:
        return "origin"
    if len(remotes) == 1:
        return remotes[0]
    return None


def _branch_disclosure_warranted(enforcement: Enforcement) -> bool:
    """Whether this run's restrictions make default-branch exposure worth disclosing at all.

    Keys off the Enforcement block rather than the freedom name, so the gate covers the crossed
    cells the old `freedom in PUBLISH_FREEDOMS` check missed in both directions: a
    network-enabled run at a freedom that never authorized publishing (codex write_in_repo with
    network=True — mechanically identical to publish, so just as exposed), and a
    publish-authorized run whose network-backed push is blocked (still disclosed, with its own
    wording). `not_controlled` deliberately does not count as reaching a remote: there polybridge
    cannot say either way, and a notice asserting exposure it cannot establish would overclaim.
    """
    return enforcement.publish_attempts_allowed_by_polybridge or enforcement.network_access in (
        "enabled",
        "unrestricted",
    )


def _branch_notice_templates(enforcement: Enforcement) -> tuple[str, str]:
    """The (on-default-branch, could-not-determine) wordings for this run's disclosure state."""
    if enforcement.network_access == "blocked":
        return _ON_DEFAULT_BRANCH_NETWORK_BLOCKED_NOTICE, _UNDETERMINED_NETWORK_BLOCKED_NOTICE
    if not enforcement.publish_attempts_allowed_by_polybridge:
        return (
            _ON_DEFAULT_BRANCH_UNAUTHORIZED_REACHABLE_NOTICE,
            _UNDETERMINED_UNAUTHORIZED_REACHABLE_NOTICE,
        )
    return _ON_DEFAULT_BRANCH_PUBLISH_NOTICE, _UNDETERMINED_PUBLISH_NOTICE


def _format_branch_notice(
    template: str, freedom: str, enforcement: Enforcement, branch: str | None = None
) -> str:
    # str.format ignores keywords a template does not use, so all three states share one call.
    return template.format(freedom=freedom, branch=branch, network_access=enforcement.network_access)


def _detect_publish_branch_notice(
    freedom: str, repo_path: Path, enforcement: Enforcement
) -> str | None:
    """The three outcomes: on the default branch, not on it (nothing to report), or undeterminable.

    Never falls back to "the branch is named main or master" — a feature branch can be named
    `main`, and a repo's real default can be something else entirely; the default is only ever what
    a remote's own recorded `HEAD` says. Which runs get a disclosure at all, and which wording,
    is decided by the Enforcement block (see `_branch_notice_templates`), not by the freedom name.
    """
    deadline = time.monotonic() + GIT_PROBE_BUDGET_SECONDS
    current_branch = _git_read(repo_path, deadline, "symbolic-ref", "--quiet", "--short", "HEAD")
    remote = _default_branch_remote(repo_path, deadline)
    default_ref = (
        _git_read(
            repo_path, deadline, "symbolic-ref", "--quiet", "--short", f"refs/remotes/{remote}/HEAD"
        )
        if remote is not None
        else None
    )
    default_branch = (
        default_ref[len(remote) + 1 :]
        if remote is not None and default_ref is not None and default_ref.startswith(f"{remote}/")
        else None
    )

    if default_branch is not None and (
        _git_read(
            repo_path, deadline, "rev-parse", "--verify", "--quiet", f"refs/remotes/{default_ref}"
        )
        is None
    ):
        # A recorded remote HEAD can outlive the branch it names — the ref file is local and is not
        # cleaned up when the branch is deleted upstream. Warning that the checkout is "on the
        # default branch" on the strength of a dangling pointer would be a claim about nothing.
        default_branch = None

    on_branch, undetermined = _branch_notice_templates(enforcement)
    if not current_branch or not default_branch:
        return _format_branch_notice(undetermined, freedom, enforcement)
    if current_branch == default_branch:
        return _format_branch_notice(on_branch, freedom, enforcement, default_branch)
    return None


def _publish_branch_notice(
    freedom: str, repo_path: Path, enforcement: Enforcement
) -> str | None:
    """A bridge notice disclosing default-branch exposure for a run that can publish — or reach
    a remote.

    This is disclosure, not a guard: `start_task` returns after the process has already spawned, so
    nothing here can stop a run once it is going. It does run *before* the launch, though — bounded
    by `GIT_PROBE_BUDGET_SECONDS` and skipped entirely where the run is neither publish-authorized
    nor network-reachable — so on a run it applies to it delays the launch slightly rather than not
    at all. That is the price of keeping the window between spawning and registering the process
    free of awaits. Detection is entirely best-effort — per CLAUDE.md,
    bookkeeping must never change an outcome — so every failure mode (a missing `git`, a timed-out
    probe, a detached HEAD, no recorded remote HEAD, an unexpected bug in the detection itself) is
    caught here and turned into the "could not determine" notice rather than an exception. Which
    runs it applies to is decided by `_branch_disclosure_warranted`, from the Enforcement block —
    never from the freedom or backend name, which would miss the crossed cells.
    """
    if not _branch_disclosure_warranted(enforcement):
        return None
    try:
        return _detect_publish_branch_notice(freedom, repo_path, enforcement)
    except Exception:
        log.warning(
            "could not determine publish-branch exposure for %s; reporting that rather than the "
            "branch itself",
            repo_path,
            exc_info=True,
        )
        return _format_branch_notice(_branch_notice_templates(enforcement)[1], freedom, enforcement)


# A4.3: a root task can open the Monitor app in the background — opt-in, with this set to "1".
# Opening it by default brought the app to the front on every dispatch an agent made, which the
# owner asked to stop (2026-09-28): the Monitor is opened by hand unless this asks otherwise.
OPEN_MONITOR_ENV = "PB_OPEN_MONITOR"
MONITOR_URL = "polybridge-monitor://task/{task_id}"
MONITOR_OPEN_TIMEOUT_SECONDS = 30.0


async def _launch_monitor(url: str) -> int:
    """`open -g <url>`: hand the URL to the Monitor app without bringing it to the front. Returns
    the exit status; the process is always reaped."""
    proc = await asyncio.create_subprocess_exec(
        "open",
        "-g",
        url,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        return await asyncio.wait_for(proc.wait(), timeout=MONITOR_OPEN_TIMEOUT_SECONDS)
    except BaseException:
        if proc.returncode is None:
            proc.kill()
            await asyncio.shield(proc.wait())
        raise


class SessionBusyError(RuntimeError):
    """A session already has a live run, so a second one would fight it for session state."""


class SessionUnknownError(RuntimeError):
    """The backend never disclosed a session id, so the conversation cannot be continued."""


class RepoUnavailableError(RuntimeError):
    """A recovered task's repository is no longer there to resume into."""


class _Case2Outcome(NamedTuple):
    """What became of one non-local cascade target whose owning server looked alive or
    undecidable (`TaskRegistry._cascade_case2`). `kind` is one of: `"done"` (it settled, whatever
    it settled to — the caller re-resolves its status rather than trusting this), `"handoff"`
    (still unsettled at the bound, but its owner is now confirmed dead — case 3 should take it),
    `"still_settling"` (still unsettled, owner still alive/undecidable — reported, not written),
    or `"not_signalled"` (never got a signal at all — `reason` is set)."""

    task_id: str
    kind: str
    reason: str | None = None


def _identity_markers(
    backend: Backend, session_id: str | None, repo_path: Path
) -> tuple[str, ...]:
    """Strings that must all appear in the process's command line for it to still be this task.

    A live pid alone proves nothing — pids get reused. Claude carries its session id on the command
    line, so that is a precise marker. Codex mints its own id and never receives it as an argument on
    a *fresh* run, so the best available markers there are its binary and its `-C <repo>`; a reused
    pid landing on another codex run in the same repository is the residual risk, and a far smaller
    one than reporting a running task as dead. A codex *resume* does carry the id, as a positional
    (see `backends/codex.py`) — `session_id` is not None there, so the branch below already prefers
    it over `repo_path`, without this function needing to know which case it is.
    """
    markers: list[str] = [backend.binary]
    if session_id:
        markers.append(session_id)
    else:
        markers.append(str(repo_path))
    return tuple(markers)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def default_log_dir() -> Path:
    return Path.home() / ".polybridge" / "tasks"


@dataclass
class Task:
    task_id: str
    backend: str
    session_id: str | None
    repo_path: Path
    prompt: str
    max_turns: int
    log_path: Path
    started_at: datetime
    model: str | None = None
    reasoning_effort: str | None = None
    parent_task_id: str | None = None
    freedom: str = "write_in_repo"
    network: bool | None = None
    """The network request this task was dispatched under: True/False explicit, None the
    freedom's historical default. Kept raw rather than resolved so a resume inherits exactly
    what was asked; the resolved outcome is `enforcement.network_access`."""
    enforcement: dict[str, Any] = field(default_factory=dict)
    # Strings guaranteed to appear in the process's command line, so a later server process can tell
    # this task's pid apart from a reused one. What identifies a run differs per backend: Claude
    # carries its session id on the command line, Codex mints its own and does not.
    markers: tuple[str, ...] = ()

    spawned_by: str | None = None
    """The task_id of the task whose own agent dispatched this one via polybridge — best-effort,
    from `lineage.detect_caller`. None for a root task (no caller detected)."""
    root_task_id: str | None = None
    """The top of this dispatch chain: this task's own id for a root task, else inherited from the
    caller's `root_task_id` (or the caller's own id, for a caller one level up from the root)."""
    depth: int = 0
    """How many nested dispatches deep this task is; 0 for a root task."""
    max_depth: int | None = None
    """The nesting budget this task (and anything it spawns) is checked against — `PB_MAX_DEPTH`
    for a root task, inherited from the caller otherwise. See `lineage.max_depth_default`."""
    group: str | None = None
    """An optional caller-chosen label, inherited by nested dispatches unless overridden — see
    `TaskRegistry.start`'s `group` parameter."""
    title: str | None = None
    """An optional short human-readable label — see `start_task`'s `title`. Never inherited from a
    caller; a resume carries the resumed task's own."""
    lineage_detected: str | None = None
    """Which `lineage.detect_caller` method found this task's caller (`"pb_task_id"` | `"session"`
    | `"ancestry"`), or None if no caller was detected — i.e. this is a root task."""
    live_input: bool = False
    """Whether this run was spawned with a stdin pipe that takes further messages — taken from the
    run's own `Invocation`, never inferred from the backend. `send_message`, `polybridge-ctl send`
    and the snapshot all read this field."""

    status: Status = "running"
    exit_code: int | None = None
    finished_at: datetime | None = None

    proc: asyncio.subprocess.Process | None = None
    # `start_new_session=True` makes the child its own group leader, so its pid is the group id.
    # Stored rather than looked up with getpgid() later: after the child is reaped its pid may
    # already name an unrelated process, and signalling that process's group would be a disaster.
    pgid: int | None = None

    events: EventLog | None = None
    """The task's normalized event log — see `events.py`. None only in tests that build a `Task`
    directly without going through `_spawn`."""
    start_time: str | None = None
    """This task's own process start time, from `identity.capture` shortly after spawn. Null
    ("pending") until that capture lands — see `_spawn` — so `identity.identity_check` on a record
    with a null `start_time` falls back to its legacy pid+markers comparison."""
    owner: dict[str, Any] | None = None
    """The bridge server process that dispatched this task — `identity.own_identity()` at spawn."""
    base_commit: str | None = None
    """`git rev-parse HEAD` in the repo at spawn, before the agent ran. None if the repo had no
    commit yet, or the probe failed or timed out."""
    start_dirty: bool | None = None
    """Whether `git status --porcelain` was non-empty at spawn. None if the probe failed or
    timed out."""

    acc: Accumulator = field(default_factory=Accumulator)
    bridge_notices: list[str] = field(default_factory=list)
    """Notices the bridge itself generates about the dispatch, kept on a channel separate from
    `acc.notices`: vibe's `ingest` resets `notices` to `[]` at the start of every new turn (see
    backends/vibe.py), which would silently discard anything the bridge added there across a
    resumed run. Currently populated in `_spawn` by the branch disclosure (gated on the
    Enforcement block — a publish-authorized run, or one whose mechanism nevertheless reaches
    a remote) and by the line saying polybridge imposed nothing, where network=True was
    accepted on a backend with no barrier of its own."""
    tail: deque[str] = field(default_factory=lambda: deque(maxlen=TAIL_LINES))
    stderr_tail: deque[str] = field(default_factory=lambda: deque(maxlen=STDERR_TAIL_LINES))

    input_closed: bool = False
    """Whether polybridge has closed (or lost) this live-input run's stdin. Always False for a
    DEVNULL run, which never had one to close."""
    inbox_closed: bool = False
    """Input is closed: nothing more is accepted from this server's `send_message`."""
    inbox_sealed: bool = False
    """The inbox's seal (marker or seal line) actually landed, so every other process sees input as
    closed. Until it has, another process can still append and be told "queued"."""
    inbox_reconciled: bool = False
    """Every message queued before the close — in memory and on disk — was written or reported
    undelivered, *and* the seal landed so nothing can be appended after that read. Distinct from
    `inbox_closed`: a forced close can end input before the on-disk inbox could be read or sealed,
    and `_finish_pump` must still do a final read before the run is published."""
    inbox_queue: deque[dict[str, Any]] = field(default_factory=deque)
    """Messages `send_message` accepted on this (owning) server, waiting for the pump."""
    inbox_offset: int = 0
    """How far into `<id>.inbox.jsonl` (other processes' appends) the pump has read."""
    pump: asyncio.Task[Any] | None = None
    """The input pump — retained here, and deliberately not in `watchers`: `_finish_draining`
    must neither wait on nor cancel it. `_monitor` settles it itself (`_finish_pump`)."""
    pump_wake: asyncio.Event = field(default_factory=asyncio.Event)
    close_failures: int = 0
    """Consecutive close attempts that could not read the inbox or seal it — see `_close_input`."""
    input_after_result: int | None = None
    """`acc.result_count` when polybridge last wrote a message to the run. A turn is owed for it, so
    until a later result arrives an earlier success is not the run's outcome — persisted, so a
    recovered run's replay knows it too (`store._resolve`)."""
    last_output_at: float = field(default_factory=time.monotonic)
    """`time.monotonic()` of the last stdout line — the idle bound's clock."""

    done: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_requested: bool = False
    # Whether any `persist` of this task landed on disk. A record that did and is now gone was
    # deleted (retention); one that never did is merely unwritten, and resuming it stays allowed.
    record_landed: bool = False
    drain_failed: bool = False

    # Held so the event loop keeps strong references: a bare create_task() result is only weakly
    # referenced and may be garbage-collected mid-run.
    watchers: list[asyncio.Task[Any]] = field(default_factory=list)
    monitor: asyncio.Task[Any] | None = None
    termination: asyncio.Task[Any] | None = None

    @property
    def finished(self) -> bool:
        """True once the process is gone, its output fully read, and its status published."""
        return self.done.is_set()

    @property
    def duration_seconds(self) -> float:
        end = self.finished_at or _now()
        return round((end - self.started_at).total_seconds(), 3)

    def _notices(self) -> list[str]:
        """Bridge notices merged with the backend's own, bridge first since they describe the
        dispatch rather than the run. Neither source list is mutated."""
        return [*self.bridge_notices, *self.acc.notices]

    def brief(self) -> dict[str, Any]:
        """The listing shape: identity and state, without stream detail.

        `notices` here is the bridge's own only. The backend's `Accumulator.notices` are parsed out
        of the run's stream, so they *are* stream detail and belong in `snapshot`; bridge notices
        describe the dispatch instead, and a recovered task carries them on its record with no
        replay cost — so live and recovered listings can hold the same shape.
        """
        return {
            "task_id": self.task_id,
            "backend": self.backend,
            "session_id": self.session_id,
            "repo_path": str(self.repo_path),
            "status": self.status,
            "freedom": self.freedom,
            "started_at": self.started_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "parent_task_id": self.parent_task_id,
            "spawned_by": self.spawned_by,
            "root_task_id": self.root_task_id,
            "depth": self.depth,
            "max_depth": self.max_depth,
            "group": self.group,
            "title": self.title,
            "lineage_detected": self.lineage_detected,
            "live_input": self.live_input,
            "notices": list(self.bridge_notices),
            "owner": self.owner,
            # Meaningful only while the task is still running — a settled task has no live server
            # to speak of, so this is null rather than a claim about the process that finished it.
            "owned_by_live_server": (True if not self.finished else None),
        } | control.taken_over_fields(self.log_path.parent, self.task_id)

    def snapshot(self) -> dict[str, Any]:
        """Full current state of the run, safe to call at any point."""
        snap = self.brief() | {
            # Overrides brief's bridge-only list with the full merge; see `brief`.
            "notices": self._notices(),
            "summary": self.acc.summary,
            "is_error": self.acc.is_error,
            "total_cost_usd": self.acc.total_cost_usd,
            "num_turns": self.acc.num_turns,
            "exit_code": self.exit_code,
            "permission_denials": self.acc.denials,
            "last_output_tail": list(self.tail)[-TAIL_LINES_RETURNED:],
            "raw_stream_log": str(self.log_path),
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "max_turns": self.max_turns,
            "mcp_servers": self.acc.mcp_servers,
            "available_tool_count": self.acc.available_tool_count,
            "usage": self.acc.usage,
            "enforcement": self.enforcement,
            "base_commit": self.base_commit,
            "start_dirty": self.start_dirty,
            "events_log": str(self.log_path.with_name(f"{self.task_id}{EVENTS_SUFFIX}")),
            "resume_command": resume_command(self.backend, self.session_id, self.repo_path),
        }
        # Only meaningful when something went wrong, and usually empty otherwise.
        if self.status == "failed" and self.stderr_tail:
            snap["stderr_tail"] = list(self.stderr_tail)
        return snap


class TaskRegistry:
    """In-memory registry of dispatched runs, bounded to `max_tasks` finished entries."""

    def __init__(
        self,
        log_dir: Path | None = None,
        max_tasks: int = MAX_TASKS,
        owner: dict[str, Any] | None = None,
        *,
        open_monitor: bool = True,
        monitor_launcher: Callable[[str], Awaitable[int]] | None = None,
    ) -> None:
        self._tasks: dict[str, Task] = {}
        # Whether a root task this registry starts opens the Monitor app (A4.3). False for
        # `polybridge-ctl run`/`resume`, which the app itself drives.
        self._open_monitor = open_monitor
        self._monitor_launcher = monitor_launcher
        # Strong references to the background `open` jobs (see `_maybe_open_monitor`).
        self._monitor_jobs: set[asyncio.Task[Any]] = set()
        self._log_dir = log_dir or default_log_dir()
        self._max_tasks = max_tasks
        # Computed once per registry rather than per task: every task this registry spawns shares
        # the same owning server process. `owner` is accepted as a parameter so tests can inject a
        # deterministic identity instead of depending on this process's own `ps` call.
        self._owner = owner if owner is not None else identity.own_identity()
        self._maintenance: asyncio.Task[Any] | None = None
        # Strong references to shielded cancel jobs (see `_shielded`) and `.sig` retries (see
        # `_retry_phase`), which outlive their caller.
        self._control_jobs: set[asyncio.Task[Any]] = set()
        # The loop cancels run on, captured when one starts, so a `.sig` retry requested from a
        # worker thread can be scheduled back onto it.
        self._loop: asyncio.AbstractEventLoop | None = None
        # The live phase-write retry per (task_id, attempt n, phase) — see `_start_phase_retry`.
        self._phase_retries: dict[tuple[str, int, str], asyncio.Task[Any]] = {}

    async def _detect_caller(self) -> lineage.Caller | None:
        """Best-effort: which task (if any) dispatched the process calling us.

        Looked up through the `lineage` module attribute at call time — never imported as a bare
        name — so a test can neutralise it with `monkeypatch.setattr(lineage, "detect_caller",
        ...)` (see `tests/conftest.py`). `detect_caller` already turns its own failures into None;
        this wrapper exists so a failure in the `to_thread` dispatch itself (or a misbehaving
        monkeypatch) cannot become an exception either — per CLAUDE.md, bookkeeping must never
        change an outcome.
        """
        try:
            return await asyncio.to_thread(lineage.detect_caller, self._log_dir)
        except Exception:
            log.debug("caller detection failed", exc_info=True)
            return None

    def _resolve_lineage(
        self,
        caller: lineage.Caller | None,
        *,
        child_enforcement: Enforcement,
        child_backend: str,
        child_repo: Path,
    ) -> tuple[str | None, str | None, int, int, str | None]:
        """`(spawned_by, root_task_id, depth, max_depth, lineage_detected)` for a new dispatch.

        With no detected caller, this is a root task: no depth or enforcement cap applies, and
        `max_depth` is its own budget for whatever it spawns. With one, the nested-dispatch caps
        are checked here — `check_nested_depth` and `check_nested_enforcement` both raise
        `NestedDispatchRefused` (propagated, never caught here) when the child would be weaker
        than the caller, on depth or on any `Enforcement` field; see `backends.base`.
        """
        if caller is None:
            return None, None, 0, lineage.max_depth_default(), None

        parent_max_depth = (
            caller.record.max_depth
            if caller.record.max_depth is not None
            else lineage.max_depth_default()
        )
        check_nested_depth(caller.record.depth, parent_max_depth)
        check_nested_enforcement(
            caller.record.enforcement or {},
            child_enforcement,
            parent_backend=caller.record.backend,
            child_backend=child_backend,
            parent_repo=caller.record.repo_path,
            child_repo=str(child_repo),
        )
        root_task_id = caller.record.root_task_id or caller.record.task_id
        return caller.record.task_id, root_task_id, caller.record.depth + 1, parent_max_depth, caller.method

    async def start(
        self,
        prompt: str,
        repo_path: Path,
        *,
        backend: Backend,
        freedom: str = "write_in_repo",
        max_turns: int | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        network: bool | None = None,
        group: str | None = None,
        title: str | None = None,
    ) -> Task:
        """Dispatch a fresh session. Returns once the subprocess exists, not once it finishes."""
        # Only some backends let us name the session up front. Where we can, knowing it immediately
        # means a resume works even if the run dies before saying anything.
        session_id = str(uuid.uuid4()) if backend.capabilities.chooses_session_id else None
        invocation = backend.build_start_argv(
            prompt,
            repo=repo_path,
            freedom=freedom,  # type: ignore[arg-type]
            session_id=session_id,
            model=model,
            max_turns=max_turns,
            reasoning_effort=reasoning_effort,
            network=network,
        )

        caller = await self._detect_caller()
        spawned_by, root_task_id, depth, max_depth, lineage_detected = self._resolve_lineage(
            caller,
            child_enforcement=backend.enforcement(freedom, network),  # type: ignore[arg-type]
            child_backend=backend.name,
            child_repo=repo_path,
        )
        # An explicit group wins; otherwise it is inherited from the caller (never invented for a
        # root task with none given).
        resolved_group = group if group is not None else (caller.record.group if caller else None)

        return await self._spawn(
            invocation,
            backend=backend,
            prompt=prompt,
            repo_path=repo_path,
            session_id=session_id,
            freedom=freedom,
            max_turns=max_turns,
            model=model,
            reasoning_effort=reasoning_effort,
            network=network,
            spawned_by=spawned_by,
            root_task_id=root_task_id,
            depth=depth,
            max_depth=max_depth,
            group=resolved_group,
            title=title,
            lineage_detected=lineage_detected,
        )

    async def resume(
        self,
        parent: Task,
        followup_prompt: str,
        *,
        max_turns: int | None = None,
        network: bool | None = None,
    ) -> Task:
        """Continue `parent`'s session as a new task sharing its session id."""
        if parent.session_id is None:
            raise SessionUnknownError(
                f"task {parent.task_id} never disclosed a session id, so its conversation cannot "
                "be resumed; start a new task instead"
            )

        backend = get_backend(parent.backend)
        # None inherits the parent's recorded request; an explicit boolean overrides it for this
        # subprocess only. Network is a per-run sandbox setting — changing it cannot
        # misrepresent the earlier reply — and max_turns is already per-run overridable on
        # resume, so this matches the existing shape rather than inventing one. An override the
        # parent's freedom cannot honour raises inside the builder below, exactly as on start.
        effective_network = network if network is not None else parent.network

        # Caller detection and the nested-dispatch caps run before the session lock — they are
        # read-only and must never block on, or be blocked by, another resume of this session.
        caller = await self._detect_caller()
        spawned_by, root_task_id, depth, max_depth, lineage_detected = self._resolve_lineage(
            caller,
            child_enforcement=backend.enforcement(parent.freedom, effective_network),  # type: ignore[arg-type]
            child_backend=backend.name,
            child_repo=parent.repo_path,
        )
        resolved_group = caller.record.group if caller is not None else parent.group

        try:
            async with control.session_lock(
                self._log_dir, parent.session_id, timeout=SESSION_LOCK_TIMEOUT_SECONDS
            ):
                if self.session_has_live_run(parent.session_id):
                    raise SessionBusyError(
                        f"session {parent.session_id} already has a running task, or is held by a takeover in "
                        "the Monitor; two concurrent runs would corrupt its shared conversation state"
                    )
                # The retention sweep takes this session lock before it deletes, so a parent still
                # on disk here cannot vanish under the spawn below — and one that landed and is
                # gone means the sweep deleted first, leaving nothing for the child to resume from.
                if (
                    parent.record_landed
                    and await asyncio.to_thread(store.read, self._log_dir, parent.task_id) is None
                ):
                    raise SessionUnknownError(
                        f"task {parent.task_id}'s record is no longer readable on disk (retention "
                        "removes aged tasks), so its conversation cannot be resumed; start a new "
                        "task instead"
                    )
                invocation = backend.build_resume_argv(
                    followup_prompt,
                    repo=parent.repo_path,
                    freedom=parent.freedom,  # type: ignore[arg-type]
                    session_id=parent.session_id,
                    # Carried over so the continuation runs on the model and effort the run
                    # started with, and so what we report for it stays true.
                    model=parent.model,
                    max_turns=max_turns,
                    reasoning_effort=parent.reasoning_effort,
                    network=effective_network,
                )
                return await self._spawn(
                    invocation,
                    backend=backend,
                    prompt=followup_prompt,
                    repo_path=parent.repo_path,
                    session_id=parent.session_id,
                    freedom=parent.freedom,
                    max_turns=max_turns,
                    model=parent.model,
                    reasoning_effort=parent.reasoning_effort,
                    network=effective_network,
                    parent_task_id=parent.task_id,
                    spawned_by=spawned_by,
                    root_task_id=root_task_id,
                    depth=depth,
                    max_depth=max_depth,
                    group=resolved_group,
                    # The resumed task's own title, whoever the caller is — unlike `group`.
                    title=parent.title,
                    lineage_detected=lineage_detected,
                )
        except control.LockTimeout:
            raise SessionBusyError(
                f"another resume of session {parent.session_id} is already in progress; "
                "try again shortly"
            ) from None

    async def _spawn(
        self,
        invocation: Invocation,
        *,
        backend: Backend,
        prompt: str,
        repo_path: Path,
        session_id: str | None,
        freedom: str,
        max_turns: int | None,
        model: str | None,
        reasoning_effort: str | None,
        network: bool | None = None,
        parent_task_id: str | None = None,
        spawned_by: str | None = None,
        root_task_id: str | None = None,
        depth: int = 0,
        max_depth: int | None = None,
        group: str | None = None,
        title: str | None = None,
        lineage_detected: str | None = None,
    ) -> Task:
        # Re-checked at the point of execution, not only where the argv was built, so no future
        # caller of this method can launch an agent without its backend's guarantees — and, now
        # that assert_safe takes the (freedom, network) pair, that the argv matches the
        # *resolved mechanism* that pair maps to. Where two authorizations resolve to the same
        # argv (codex write_in_repo+network=True is byte-identical to publish's default),
        # assert_safe genuinely cannot tell which produced it — a loss of provenance, not a
        # sandbox escape: the collapsed argv already had identical powers.
        #
        # The whole Invocation is checked, not just its argv, and the stdin wiring below comes from
        # it — never from the backend's name or its static capability — so a live-input argv can
        # only ever run with the pipe it was built for, and every other run keeps stdin DEVNULL
        # (codex and vibe block forever reading an open stdin).
        backend.assert_safe(invocation, freedom, network)  # type: ignore[arg-type]

        task_id = str(uuid.uuid4())
        # A root task (no detected caller) is the root of its own dispatch chain.
        root_task_id = root_task_id if root_task_id is not None else task_id
        if max_depth is None:
            max_depth = lineage.max_depth_default()
        self._log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self._log_dir / f"{task_id}.jsonl"

        # Computed before the notice block, not at Task construction below: the branch
        # disclosure keys on what this run's restrictions actually amount to, so it needs the
        # block — and `backend.enforcement` is a pure synchronous call, so hoisting it
        # introduces no await into the window between spawn and registration that the comment
        # below guards. One computation serves the notice, the Task, and the record, so all
        # three describe the same (freedom, network) pair.
        enforcement = backend.enforcement(freedom, network)  # type: ignore[arg-type]

        # Before the spawn, deliberately. Between `create_subprocess_exec` and the registration
        # below there must be no await at all: one there could be cancelled — a client
        # disconnecting mid-call — leaving a live agent no tool can reach, and it would hold the
        # process's pipes unread meanwhile, which blocks the agent (drainers are load-bearing).
        #
        # The disclosure gate is checked *here*, not only inside the helper, so a run that is
        # neither publish-authorized nor network-reachable does not pay a thread hop it has no
        # use for — and cannot queue behind a saturated executor. The deadline inside the helper
        # only starts once a worker picks the work up, so the wait for a worker is bounded out
        # here instead, on the event loop, where it is observable.
        publish_notice: str | None = None
        if _branch_disclosure_warranted(enforcement):
            try:
                publish_notice = await asyncio.wait_for(
                    asyncio.to_thread(
                        _publish_branch_notice, freedom, repo_path, enforcement
                    ),
                    timeout=GIT_PROBE_BUDGET_SECONDS * 2,
                )
            except (TimeoutError, asyncio.TimeoutError):
                # Same rule as every other failure in this check: the dispatch proceeds and the
                # caller is told the branch could not be determined.
                publish_notice = _format_branch_notice(
                    _branch_notice_templates(enforcement)[1], freedom, enforcement
                )

        # Where network=True was accepted by a backend with no barrier of its own to impose
        # (enforcement.network_access stays "not_controlled"), acceptance must not silently
        # read as "the network now works": the bridge says so itself, worded about what
        # polybridge imposes rather than about reachability. Sync, so the no-await window below
        # is untouched.
        bridge_notices: list[str] = [publish_notice] if publish_notice else []
        if network is True and enforcement.network_access == "not_controlled":
            bridge_notices.append(_UNCONTROLLED_NETWORK_NOTICE)

        # A second, independent `to_thread` hop from the branch-disclosure one above: this one is
        # unconditional (every dispatch gets a baseline, regardless of freedom), so it needs its
        # own bounded wait rather than piggybacking on a budget sized for a check some dispatches
        # skip outright. Still strictly before the spawn, so it cannot introduce an await into the
        # no-await window between `create_subprocess_exec` and registration below.
        try:
            base_commit, start_dirty = await asyncio.wait_for(
                asyncio.to_thread(_git_baseline, repo_path), timeout=GIT_BASELINE_BUDGET_SECONDS
            )
        except Exception:
            base_commit, start_dirty = None, None

        # So a nested polybridge server started by this agent (a codex/vibe MCP tool call, a
        # claude/opencode subprocess) can find its way back to this task via
        # `lineage.detect_caller`'s PB_TASK_ID method, when the environment survives that far.
        # Built here, synchronously, so it introduces no await into the no-await window below.
        spawn_env = {
            **os.environ,
            lineage.ENV_TASK_ID: task_id,
            lineage.ENV_ROOT_TASK_ID: root_task_id,
            lineage.ENV_DEPTH: str(depth),
        }

        proc = await asyncio.create_subprocess_exec(
            *invocation.argv,
            cwd=str(repo_path),
            stdin=asyncio.subprocess.PIPE if invocation.live_input else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=STREAM_LINE_LIMIT,
            env=spawn_env,
            # Own process group, so cancellation reaches the shell children an agent spawns instead
            # of orphaning them.
            start_new_session=True,
        )

        task = Task(
            task_id=task_id,
            backend=backend.name,
            session_id=session_id,
            repo_path=repo_path,
            prompt=prompt,
            max_turns=max_turns,
            log_path=log_path,
            started_at=_now(),
            model=model,
            reasoning_effort=reasoning_effort,
            parent_task_id=parent_task_id,
            freedom=freedom,
            network=network,
            enforcement=enforcement.as_dict(),
            markers=_identity_markers(backend, session_id, repo_path),
            proc=proc,
            pgid=proc.pid,
            bridge_notices=bridge_notices,
            owner=self._owner,
            base_commit=base_commit,
            start_dirty=start_dirty,
            spawned_by=spawned_by,
            root_task_id=root_task_id,
            depth=depth,
            max_depth=max_depth,
            group=group,
            title=title,
            lineage_detected=lineage_detected,
            live_input=invocation.live_input,
        )

        # Written before anything can go wrong, so even a task whose server dies immediately is
        # discoverable by whichever process comes next.
        self.persist(task)

        task.watchers = [
            asyncio.create_task(_drain_stdout(task, self), name=f"pb-stdout-{task_id}"),
            asyncio.create_task(_drain_stderr(task), name=f"pb-stderr-{task_id}"),
        ]
        task.monitor = asyncio.create_task(_monitor(task, self), name=f"pb-monitor-{task_id}")

        # Registered without awaiting anything in between: an await here could be cancelled (a
        # client disconnecting mid-call) and leave a live agent process no tool can reach.
        self._tasks[task_id] = task
        self.prune()

        # The first event a task's log ever gets, written synchronously right after registration —
        # no await has run yet and the drainer tasks just created cannot have executed, so this is
        # always seq 0. Guarded on its own: a broken event log must not cost the dispatch itself.
        try:
            task.events = EventLog(events_path(self._log_dir, task_id), task_id)
            task.events.write(
                "task_started",
                {
                    "backend": task.backend,
                    "freedom": task.freedom,
                    "network": task.network,
                    "repo_path": str(task.repo_path),
                    "prompt": task.prompt,
                    "model": task.model,
                    "reasoning_effort": task.reasoning_effort,
                    "parent_task_id": task.parent_task_id,
                    "spawned_by": task.spawned_by,
                    "root_task_id": task.root_task_id,
                    "depth": task.depth,
                    "max_depth": task.max_depth,
                    "group": task.group,
                    "title": task.title,
                    "lineage_detected": task.lineage_detected,
                    "live_input": task.live_input,
                },
            )
        except Exception:
            log.exception("task %s: could not write task_started event", task_id)

        # Outside the no-await window, and still before any await: the prompt is the first stdin
        # line of a live-input run (a positional would be ignored), written synchronously so it is
        # queued ahead of anything else, then flushed below.
        if invocation.live_input:
            self._write_initial_input(task, invocation.initial_input or b"")
            task.pump = asyncio.create_task(self._pump(task), name=f"pb-pump-{task_id}")
            await self._flush_stdin(task)

        # Best-effort start-time capture, so a later server can tell this pid apart from a reused
        # one (see `identity.identity_check`). `except Exception`, not `BaseException`: a
        # `CancelledError` here must propagate — the task is already registered, so nothing is lost
        # by not finishing this step.
        try:
            captured = await asyncio.wait_for(
                asyncio.to_thread(identity.capture, proc.pid, task.markers), timeout=2.0
            )
        except Exception:
            captured = None
        if captured is not None:
            task.start_time = captured["start_time"]

        # A second, explicit persist so the on-disk record carries the start time once captured.
        # The monitor may already have finished and persisted a terminal status by the time this
        # runs — `persist` always rebuilds from the in-memory `Task`, so this stays consistent with
        # whatever the monitor last wrote rather than clobbering it with stale spawn-time data.
        try:
            self.persist(task)
        except Exception:
            log.exception("task %s: could not persist after identity capture", task_id)

        # Last, and with no await between here and the caller building its response from this
        # task: the background job cannot run first, so however the launcher fares, the
        # `start_task` result is the same.
        self._maybe_open_monitor(task)

        log.info(
            "task %s started (%s pid=%s session=%s repo=%s%s)",
            task_id,
            backend.name,
            proc.pid,
            session_id,
            repo_path,
            f" resumed-from={parent_task_id}" if parent_task_id else "",
        )
        return task

    def _maybe_open_monitor(self, task: Task) -> None:
        """Open the Monitor app on this task, in the background, for a root task only (A4.3).

        Synchronous — it only schedules — and called as `_spawn`'s last step, after registration. A
        root task is
        one with no detected caller and no `PB_TASK_ID` in this server's environment: a nested
        dispatch belongs to a tree the user is already watching. Off unless `PB_OPEN_MONITOR=1`;
        skipped off darwin too, and for a registry built with `open_monitor=False` (`polybridge-ctl
        run`/`resume`, which the app drives). Nothing here can change the dispatch's outcome, and
        the response never claims the app opened.
        """
        try:
            if not self._open_monitor or task.lineage_detected is not None:
                return
            if os.environ.get(lineage.ENV_TASK_ID):
                return
            if sys.platform != "darwin" or os.environ.get(OPEN_MONITOR_ENV) != "1":
                return
            job = asyncio.create_task(
                self._open_monitor_job(task), name=f"pb-open-monitor-{task.task_id}"
            )
            self._monitor_jobs.add(job)
            job.add_done_callback(self._monitor_jobs.discard)
        except Exception:
            log.debug("task %s: could not schedule opening the Monitor", task.task_id, exc_info=True)

    async def _open_monitor_job(self, task: Task) -> None:
        launcher = self._monitor_launcher or _launch_monitor
        try:
            code = await launcher(MONITOR_URL.format(task_id=task.task_id))
        except Exception as exc:
            notice = (
                f"The Monitor app could not be opened ({type(exc).__name__}: {exc}); the task "
                "runs regardless."
            )
        else:
            if code == 0:
                return
            notice = (
                f"The Monitor app could not be opened (`open -g` exited {code}); the task runs "
                "regardless."
            )
        task.bridge_notices.append(notice)
        try:
            self.persist(task)
        except Exception:
            log.exception("task %s: could not persist the Monitor notice", task.task_id)

    def _write_initial_input(self, task: Task, data: bytes) -> None:
        """Queue a live-input run's prompt on its stdin — synchronous, so nothing can be written
        ahead of it — and log it as the run's first `user_message`."""
        if _write_stdin(task, data):
            _write_event(task, "user_message", {"text": task.prompt, "source": "initial"})
        else:
            log.warning("task %s: stdin was already gone before the prompt could be written", task.task_id)

    async def _flush_stdin(self, task: Task) -> None:
        """Await the prompt's delivery to the pipe. The input pump, not this, decides when stdin
        closes."""
        await _drain_stdin(task)

    # --- live input: send, pump, close protocol ------------------------------------------------

    async def send_message(self, task: Task, text: str) -> dict[str, Any]:
        """Queue `text` for a live-input task this server owns. Returns "queued", never
        "delivered": the pump writes it, and the event log records whether it did.

        Takes the same inbox lock as the close protocol, so a message is either accepted before
        the close (and then forwarded or reported undelivered) or refused after it.
        """
        if not task.live_input:
            raise inbox.SendRefused(
                f"task {task.task_id} was not started with live input, so it cannot take a message "
                "while it runs; continue it with resume_task once it has finished",
                code="not_live_input",
            )
        try:
            fd = await inbox.lock_async(self._log_dir, task.task_id)
        except control.LockTimeout:
            raise inbox.SendRefused(
                f"task {task.task_id}'s inbox is locked by another process; try again shortly",
                code="lock_timeout",
            ) from None
        try:
            if task.inbox_closed or inbox.is_closed(self._log_dir, task.task_id):
                raise inbox.SendRefused(
                    f"task {task.task_id} has {inbox.CLOSED_MESSAGE}", code="closed"
                )
            if task.finished:
                raise inbox.SendRefused(
                    f"task {task.task_id} has settled ({task.status}); continue with resume_task",
                    code="settled",
                )
            message = inbox.make_message(text, self._owner)
            task.inbox_queue.append(message)
        finally:
            inbox.unlock(fd)
        task.pump_wake.set()
        return inbox.queued_response(task.task_id, message)

    async def send_to_record(self, task_id: str, text: str) -> dict[str, Any]:
        """Queue `text` for a live-input task another server owns, via its on-disk inbox."""
        return await asyncio.to_thread(
            inbox.send_to_record, self._log_dir, task_id, text, by=self._owner
        )

    def _take_pending(self, task: Task) -> list[dict[str, Any]]:
        """Everything queued, from this server and from other processes, in queue order. Call with
        the inbox lock held. The on-disk inbox is read first: if that raises `OSError`, nothing has
        been taken, so no acknowledged message is lost by a failed read."""
        appended, task.inbox_offset = inbox.read_new(
            self._log_dir, task.task_id, task.inbox_offset
        )
        pending = [*task.inbox_queue, *appended]
        task.inbox_queue.clear()
        pending.sort(key=lambda message: str(message.get("queued_at") or ""))
        return pending

    def _write_messages(self, task: Task, messages: list[dict[str, Any]]) -> int:
        """Write each message to stdin, synchronously. A message that cannot be written — and every
        one after it — is reported undelivered rather than dropped silently."""
        backend = get_backend(task.backend)
        written = 0
        for index, message in enumerate(messages):
            try:
                data = backend.encode_live_message(message["text"])
            except Exception:
                log.warning("task %s: could not encode a message", task.task_id, exc_info=True)
                self._report_undelivered(task, [message], "it could not be encoded for this run")
                continue
            if not _write_stdin(task, data):
                self._report_undelivered(
                    task, messages[index:], "the run's stdin was already closed"
                )
                break
            written += 1
            _write_event(
                task,
                "user_message",
                {"text": message["text"], "source": "injected", "message_id": message.get("id")},
            )
        return written

    def _report_undelivered(
        self, task: Task, messages: list[dict[str, Any]], reason: str
    ) -> None:
        for message in messages:
            _write_event(
                task,
                "undelivered",
                {"message_id": message.get("id"), "text": message.get("text"), "reason": reason},
            )
            task.bridge_notices.append(
                f"a message queued with send_message (id {message.get('id')}) was not delivered: "
                f"{reason}"
            )

    @staticmethod
    def _stop_reason(task: Task) -> str | None:
        """Why the pump must stop forwarding and close, dropping what is queued — or None."""
        if task.proc is None or task.proc.returncode is not None:
            return "the run exited before it could be delivered"
        if task.acc.error_result_seen:
            return (
                "the run reported an error, so polybridge stopped forwarding messages and closed "
                "its input"
            )
        if task.input_closed:
            return "the run's stdin had closed"
        return None

    def _note_input_written(self, task: Task) -> None:
        """A message reached the run: a turn is owed for it. Not idle until its result arrives,
        and an earlier success is not the outcome until then — recorded durably for recovery."""
        task.acc.awaiting_input = False
        task.acc.turn_open = True
        task.input_after_result = task.acc.result_count
        try:
            self.persist(task)
        except Exception:
            log.warning("task %s: could not record the written input", task.task_id, exc_info=True)

    async def _forward_pending(self, task: Task) -> bool:
        """Write whatever is queued. True if anything was taken off the queue.

        The stop conditions are re-checked once the lock is held: the lock can take a while, and
        an error result or an exit that arrived meanwhile must stop forwarding, not be raced past.
        """
        if not task.inbox_queue and not inbox.has_unread(
            self._log_dir, task.task_id, task.inbox_offset
        ):
            return False
        try:
            fd = await inbox.lock_async(self._log_dir, task.task_id)
        except control.LockTimeout:
            log.warning("task %s: inbox lock busy; will retry", task.task_id)
            return False
        try:
            if self._stop_reason(task) is not None:
                return False
            try:
                pending = self._take_pending(task)
            except OSError:
                log.warning("task %s: could not read its inbox; will retry", task.task_id, exc_info=True)
                return False
            written = self._write_messages(task, pending)
        finally:
            inbox.unlock(fd)
        if written:
            self._note_input_written(task)
            await _drain_stdin(task)
        return bool(pending)

    def _close_still_warranted(self, task: Task, kind: str, bound: float) -> bool:
        """Re-made under the inbox lock: the idle state the pump saw before waiting for the lock
        may be gone by now (a follow-up turn, a new background task)."""
        if kind == CLOSE_IDLE:
            return task.acc.awaiting_input
        if kind == CLOSE_IDLE_BOUND:
            return background_idle_bound_reached(
                task.acc, time.monotonic() - task.last_output_at, bound
            )
        return True

    async def _close_input(
        self,
        task: Task,
        *,
        drop_reason: str | None = None,
        kind: str = CLOSE_STOP,
        bound: float = DEFAULT_LIVE_IDLE_SECONDS,
    ) -> bool:
        """The close protocol: 1) lock the inbox; 2) forward — or, with a drop reason, report
        undelivered — every message still queued; 3) seal the inbox (marker, or a seal line); 4)
        unlock; 5) close stdin. The same sequence on every path (idle, error, idle bound, exit), so
        no send can slip in between the last forward and EOF. Returns True once input is closed.

        The decision is re-made under the lock: an error or exit that arrived while waiting for it
        turns a forwarding close into a dropping one, and an idle close whose idleness has passed is
        abandoned (False, nothing changed). A close whose inbox cannot be read or sealed also
        returns False while the run lives, and the pump retries — up to `CLOSE_RETRY_LIMIT`
        attempts, after which `_force_close` ends input anyway, because a live run that can never
        reach EOF would never settle. After exit there is nothing to hold stdin open for, and a lock
        that stays busy for `EXIT_CLOSE_GIVE_UP_SECONDS` hands over to `_close_unlocked`.
        """
        exited_since: float | None = None
        fd: int | None = None
        while fd is None:
            try:
                fd = await inbox.lock_async(self._log_dir, task.task_id)
            except control.LockTimeout:
                if task.proc is None or task.proc.returncode is not None:
                    exited_since = exited_since or time.monotonic()
                    if time.monotonic() - exited_since >= EXIT_CLOSE_GIVE_UP_SECONDS:
                        self._close_unlocked(task, "the run exited and its inbox stayed locked")
                        return True
                log.warning("task %s: inbox lock busy; still waiting to close input", task.task_id)
        try:
            reason = drop_reason or self._stop_reason(task)
            exited = task.proc is None or task.proc.returncode is not None
            if reason is None and not self._close_still_warranted(task, kind, bound):
                return False
            try:
                pending = self._take_pending(task)
            except OSError:
                log.warning("task %s: could not read its inbox to close it", task.task_id, exc_info=True)
                if exited:
                    self._close_unlocked(task, "its inbox could not be read")
                    return True
                return self._close_failed(task, "its inbox could not be read", bound)
            if reason is None:
                wrote = self._write_messages(task, pending)
                if wrote:
                    self._note_input_written(task)  # a turn is owed for those, EOF or not
            else:
                self._report_undelivered(task, pending, reason)
            try:
                inbox.seal(self._log_dir, task.task_id)
                task.inbox_sealed = True
            except OSError:
                log.error("task %s: could not seal its inbox", task.task_id, exc_info=True)
                if not exited:
                    return self._close_failed(task, "its inbox could not be sealed", bound)
                # Not sealed: `_finish_pump`'s final read still owes whatever lands after this.
            if kind == CLOSE_IDLE_BOUND and reason is None:
                # Committed only now: a close that did not happen abandons nothing.
                self._mark_abandoned(task, bound)
            task.inbox_closed = True
            task.inbox_reconciled = task.inbox_sealed
            task.close_failures = 0
        finally:
            inbox.unlock(fd)
        _close_stdin(task)
        return True

    def _close_failed(self, task: Task, why: str, bound: float) -> bool:
        """Count a close that could not complete; after `CLOSE_RETRY_LIMIT` of them, end input
        regardless (`_force_close`). True if input is now closed. Called with the inbox lock
        held."""
        task.close_failures += 1
        if task.close_failures < CLOSE_RETRY_LIMIT:
            return False
        self._force_close(task, why, bound)
        return True

    def _force_close(self, task: Task, why: str, bound: float) -> None:
        """End input for a live run whose inbox cannot be closed properly. Called with the inbox
        lock held. Still tries to seal (so later sends are refused) and to read the on-disk inbox;
        whatever it could not account for stays owed — `inbox_reconciled` stays False, and
        `_finish_pump` retries it before the run is published. EOF kills any background task still
        open, which is recorded as abandoned, exactly as the idle bound would."""
        try:
            inbox.seal(self._log_dir, task.task_id)
            task.inbox_sealed = True
        except OSError:
            log.error("task %s: could not seal its inbox", task.task_id, exc_info=True)
        pending = list(task.inbox_queue)
        task.inbox_queue.clear()
        try:
            appended, task.inbox_offset = inbox.read_new(
                self._log_dir, task.task_id, task.inbox_offset
            )
            pending.extend(appended)
            # Reconciled only if nothing can be appended after this read: with no seal, another
            # process still sees input open once the lock is released.
            task.inbox_reconciled = task.inbox_sealed
        except OSError:
            log.warning("task %s: could not read its inbox to force it closed", task.task_id)
        self._report_undelivered(
            task, pending, f"input was closed without the inbox protocol ({why})"
        )
        notice = (
            f"input was closed without the inbox protocol ({why}): until this task settles, a "
            "message sent to it from another process may be accepted and never delivered"
        )
        task.bridge_notices.append(notice)
        _write_event(task, "notice", {"text": notice})
        if task.acc.background_open and not task.acc.background_abandoned:
            self._mark_abandoned(task, bound)
        task.inbox_closed = True
        _close_stdin(task)

    def _mark_abandoned(self, task: Task, bound: float) -> None:
        task.acc.background_abandoned = True
        notice = _BACKGROUND_ABANDONED_NOTICE.format(bound=bound)
        task.bridge_notices.append(notice)
        _write_event(task, "notice", {"text": notice})

    def _close_unlocked(self, task: Task, why: str) -> None:
        """Last resort after exit, when the protocol cannot run: marker FIRST, then everything
        queued — memory and disk — reported undelivered. Senders re-check the marker after
        appending (`inbox.send_to_record`), so one that appended before the marker existed is read
        here, and one that finds it afterwards is refused: nothing acknowledged goes unreported."""
        try:
            inbox.seal(self._log_dir, task.task_id)
            task.inbox_sealed = True
        except OSError:
            log.error("task %s: could not seal its inbox", task.task_id, exc_info=True)
        task.inbox_closed = True
        try:
            appended, task.inbox_offset = inbox.read_new(
                self._log_dir, task.task_id, task.inbox_offset
            )
            task.inbox_reconciled = task.inbox_sealed
        except OSError:
            appended = []
            task.bridge_notices.append(
                f"input was closed without its inbox being readable ({why}); a message sent to "
                "this task from another process at that moment may not have been reported"
            )
        pending = [*task.inbox_queue, *appended]
        task.inbox_queue.clear()
        self._report_undelivered(task, pending, "the run exited before it could be delivered")
        _close_stdin(task)

    async def _pump(self, task: Task) -> None:
        """Feed a live-input run's stdin until it is idle, then close it so the run settles.

        Woken by the stdout drainer after every event and by `send_message`, and otherwise every
        `PUMP_POLL_SECONDS` (which is also how often other processes' appends are noticed). Every
        exit from the loop goes through `_close_input`; a close that did not happen is retried.
        """
        bound = live_idle_seconds()
        try:
            while not task.inbox_closed:
                task.pump_wake.clear()
                acc = task.acc
                reason = self._stop_reason(task)
                kind: str | None = CLOSE_STOP if reason is not None else None
                if kind is None and await self._forward_pending(task):
                    continue
                if kind is None and acc.awaiting_input:
                    kind = CLOSE_IDLE
                if kind is None and background_idle_bound_reached(
                    acc, time.monotonic() - task.last_output_at, bound
                ):
                    kind = CLOSE_IDLE_BOUND
                if kind is not None and await self._close_input(
                    task, drop_reason=reason, kind=kind, bound=bound
                ):
                    return
                try:
                    await asyncio.wait_for(task.pump_wake.wait(), timeout=PUMP_POLL_SECONDS)
                except (asyncio.TimeoutError, TimeoutError):
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("task %s: input pump failed", task.task_id)
            # Bookkeeping must never change an outcome — but an open stdin would keep the run
            # from ever settling, so the input is closed regardless.
            _close_stdin(task)

    async def _finish_pump(self, task: Task) -> None:
        """Let the pump run its exit-path close, so undelivered messages are reported before
        `task_finished`. That close is itself bounded (`EXIT_CLOSE_GIVE_UP_SECONDS`), so this waits
        for it; `PUMP_STOP_GRACE_SECONDS` only backstops a pump that is stuck somewhere else.
        Called by `_monitor` after the process exits."""
        pump = task.pump
        if pump is not None and not pump.done():
            task.pump_wake.set()
            try:
                await asyncio.wait_for(asyncio.shield(pump), timeout=PUMP_STOP_GRACE_SECONDS)
            except (asyncio.TimeoutError, TimeoutError):
                log.warning("task %s: input pump did not stop; cancelling it", task.task_id)
                pump.cancel()
                await asyncio.gather(pump, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.debug("task %s: input pump ended with an error", task.task_id, exc_info=True)
        if task.live_input and not task.inbox_closed:
            # The pump never finished its close (it failed, or was cancelled above).
            self._close_unlocked(task, "the input pump did not finish")
        if task.live_input and not task.inbox_reconciled:
            # A close that could not read the inbox, or could not seal it: another process may have
            # appended — and been told "queued" — after the last read. One final read, under the
            # lock where it can be had, reports every line still unread as undelivered.
            await self._final_inbox_read(task)

    async def _final_inbox_read(self, task: Task) -> None:
        """Report every message still unread in the on-disk inbox as undelivered, once the run has
        exited. Senders are refused from here on regardless (its process is confirmed dead), so this
        read is the last word. Taken under the inbox lock when it can be had within its timeout;
        otherwise read anyway — a torn line from a stuck writer is left unread, never guessed at."""
        fd: int | None = None
        try:
            fd = await inbox.lock_async(self._log_dir, task.task_id)
        except control.LockTimeout:
            log.warning("task %s: inbox lock busy at the final read; reading anyway", task.task_id)
        try:
            appended, task.inbox_offset = inbox.read_new(
                self._log_dir, task.task_id, task.inbox_offset
            )
        except OSError:
            log.warning("task %s: could not read its inbox at the final read", task.task_id)
            task.bridge_notices.append(
                "the run's inbox could not be read after it exited; a message sent to it from "
                "another process may have been accepted and never reported"
            )
            return
        finally:
            if fd is not None:
                inbox.unlock(fd)
        task.inbox_reconciled = True
        if appended:
            self._report_undelivered(
                task,
                appended,
                "it was queued after input closed without a seal, and the run has exited",
            )

    @property
    def log_dir(self) -> Path:
        return self._log_dir

    def persist(self, task: Task) -> None:
        landed = store.write_landed(
            self._log_dir,
            store.TaskRecord(
                task_id=task.task_id,
                backend=task.backend,
                session_id=task.session_id,
                repo_path=str(task.repo_path),
                freedom=task.freedom,
                markers=list(task.markers),
                started_at=task.started_at.isoformat(),
                pid=task.proc.pid if task.proc is not None else None,
                pgid=task.pgid,
                model=task.model,
                reasoning_effort=task.reasoning_effort,
                max_turns=task.max_turns,
                network=task.network,
                parent_task_id=task.parent_task_id,
                prompt=task.prompt[: store.PROMPT_PREVIEW_CHARS],
                status=task.status,
                exit_code=task.exit_code,
                finished_at=task.finished_at.isoformat() if task.finished_at else None,
                enforcement=dict(task.enforcement),
                bridge_notices=list(task.bridge_notices),
                start_time=task.start_time,
                owner=dict(task.owner) if task.owner is not None else None,
                base_commit=task.base_commit,
                start_dirty=task.start_dirty,
                spawned_by=task.spawned_by,
                root_task_id=task.root_task_id,
                depth=task.depth,
                max_depth=task.max_depth,
                group=task.group,
                title=task.title,
                lineage_detected=task.lineage_detected,
                live_input=task.live_input,
                input_after_result=task.input_after_result,
            ),
        )
        if landed:
            task.record_landed = True

    def get(self, task_id: str) -> Task | None:
        return self._tasks.get(task_id)

    def recover(self, task_id: str) -> store.TaskRecord | None:
        """A task this process did not spawn, read back from disk."""
        return store.read(self._log_dir, task_id)

    def recovered_briefs(self, exclude: set[str]) -> list[dict[str, Any]]:
        """Listing entries for on-disk tasks not held in memory."""
        return [
            store.brief(self._log_dir, record)
            for record in store.read_all(self._log_dir)
            if record.task_id not in exclude
        ]

    def list(self, status: str | None = None) -> list[Task]:
        tasks = sorted(self._tasks.values(), key=lambda t: t.started_at)
        if status is None:
            return tasks
        return [t for t in tasks if t.status == status]

    def session_has_live_run(self, session_id: str | None) -> bool:
        """Whether any run on this session is live, including ones another server process started.

        Checking only our own registry would let two bridge processes resume the same session at
        once, which is exactly the concurrent mutation this guard exists to prevent.
        """
        if session_id is None:
            return False
        if any(
            task.session_id == session_id and not task.finished for task in self._tasks.values()
        ):
            return True
        return session_id in store.live_session_ids(self._log_dir)

    def _deliver_local_cancel(self, task: Task) -> None:
        """The synchronous half of a local cancel: `.req` -> flag -> SIGTERM -> `.sig`/`.failed`.

        No `await` anywhere in this method, so ordering versus the monitor's own
        `cancel_verdict` poll is unobservable — either the monitor sees the flag first (owner's
        own rule decides) or it sees the phase files first (cross-process rule decides), and both
        land on the same answer. `begin_attempt` raises `PhaseWriteError` before the flag is set
        or anything is signalled, so a caller that cannot even open an attempt has changed
        nothing. A joined attempt (another controller's `.req` already pending) still gets the
        flag set and a harmless repeat SIGTERM, but writes no phase file of its own — the owning
        controller's attempt is the one of record.
        """
        attempt = control.begin_attempt(self._log_dir, task.task_id, control.CANCEL, self._owner)
        task.cancel_requested = True
        delivered = _signal_group(task, signal.SIGTERM)
        if attempt.owned and delivered:
            # One in-line attempt only: this runs on the event loop, so any retry is the background
            # job's business (see `_record_delivery`).
            self._record_delivery(
                task.task_id,
                attempt.n,
                leader_alive=task.proc.returncode is None,
                inline_attempts=1,
            )
        elif attempt.owned:
            try:
                control.mark_failed(
                    self._log_dir,
                    task.task_id,
                    control.CANCEL,
                    attempt.n,
                    reason="process group already gone",
                )
            except control.PhaseWriteError:
                # The group was already gone; a failed bookkeeping write must not change that
                # outcome, and the owner's own `cancel_requested` rule decides this task's status
                # regardless of what the phase files say.
                log.warning(
                    "task %s: could not record cancel attempt %d's outcome",
                    task.task_id,
                    attempt.n,
                    exc_info=True,
                )

    async def cancel(self, task: Task) -> Task:
        """Stop a run, waiting for it to actually die before returning.

        Safe to call concurrently: the termination sequence is owned by the task, so repeat callers
        join the one already in flight rather than starting a second.
        """
        if task.finished:
            return task

        self._loop = asyncio.get_running_loop()
        if task.proc is None:
            # No process was ever attached (only reachable in tests); nothing to signal.
            task.cancel_requested = True
            task.status = "cancelled"
            task.finished_at = _now()
            task.done.set()
            return task

        self._deliver_local_cancel(task)

        if task.termination is None or task.termination.done():
            task.termination = asyncio.create_task(
                _escalate(task), name=f"pb-terminate-{task.task_id}"
            )
        # Shielded so a disconnecting client cannot abandon the escalation to SIGKILL.
        await asyncio.shield(task.termination)

        try:
            await asyncio.wait_for(task.done.wait(), timeout=SIGKILL_GRACE_SECONDS)
        except asyncio.TimeoutError:
            # Reported honestly: the returned snapshot still says "running", because it is.
            log.warning(
                "task %s did not settle after cancellation; still reporting %s",
                task.task_id,
                task.status,
            )
        else:
            log.info("task %s cancelled", task.task_id)
        return task

    def _cascade_targets(
        self, task_id: str, local_lineage: list[tuple[str, str | None, str | None]]
    ) -> set[str]:
        """The full closure of `cancel_cascade`'s targets for `task_id`, recomputed fresh on every
        round so a target that only appears mid-cascade (a child spawned during the wait, or a
        settled intermediate that unblocks a grandchild) is still picked up on the next pass.

        Includes `task_id` itself, every descendant reachable by following `spawned_by` — built
        from *every* record on disk, live or not, so a completed hop in the middle does not stop
        the walk reaching a live descendant beyond it — and every record whose `root_task_id`
        names `task_id`, even one not `spawned_by`-reachable at all: lineage detection is
        best-effort, and a dispatch that only ever recorded the root is still worth cascading to.

        Runs in a worker thread (it reads every record), so the in-memory tasks arrive as a
        snapshot `(task_id, spawned_by, root_task_id)` taken on the event loop.
        """
        records = store.read_all(self._log_dir)
        lineage_rows = [(r.task_id, r.spawned_by, r.root_task_id) for r in records]
        lineage_rows.extend(local_lineage)
        return lineage.lineage_closure(lineage_rows, task_id)

    def _triage_target(self, task_id: str) -> tuple[str, Any]:
        """Decide what a non-local cascade target needs. Blocking (`ps`, disk), so run in a thread.

        Returns `("skip", None)` for a target that is already settled or gone,
        `("not_signalled", reason)`, or `("case2" | "case3", record)` by whether its owning server
        is confirmed dead.
        """
        record = store.read(self._log_dir, task_id)
        if record is None:
            return "skip", None
        if record.status in store.TERMINAL_RECORD_STATUSES and record.exit_code is not None:
            return "skip", None
        leader = identity.task_identity(record.pid, record.start_time, record.markers)
        verdict, reason = identity.check_detail(leader)
        if verdict == "dead":
            return "skip", None
        if not identity.signalable(verdict, reason):
            return "not_signalled", reason
        if record.pgid is None:
            return "not_signalled", "no process group recorded"
        if identity.identity_check(record.owner) == "dead":
            return "case3", record
        return "case2", record

    def _deliver_recorded_cancel(
        self, record: store.TaskRecord, *, already_signalled: bool = False
    ) -> tuple[str, str | None]:
        """`.req` -> SIGTERM -> `.sig`/`.failed` for a task this server does not own, all at once.

        Synchronous and run as ONE `asyncio.to_thread` call on purpose: cancelling the coroutine
        awaiting it (a client disconnecting mid-`cancel_task`) cannot stop a worker thread, so a
        `.req` can never be left behind without its outcome — which would keep the owner's monitor
        waiting for as long as this server lives, since a live controller's lease is never
        recovered.

        The leader's identity is re-checked here, immediately before delivery, rather than trusted
        from discovery: a cascade may have spent seconds on other targets since, and a leader that
        exited in the meantime may have handed its pid (and so its group id) to something else.

        Returns `("signalled", None)`, `("gone", None)` (the leader is no longer ours to signal and
        nothing was sent), or `("not_signalled", reason)`. `already_signalled` marks a target handed
        off from case 2, whose SIGTERM already landed there (its `.sig` is on disk): if it is gone
        now, that signal worked, so it still reports `signalled`.
        """
        task_id = record.task_id
        leader = identity.task_identity(record.pid, record.start_time, record.markers)
        verdict, reason = identity.check_detail(leader)
        if not identity.signalable(verdict, reason):
            if already_signalled:
                return "signalled", None
            if verdict == "dead":
                return "gone", None
            return "not_signalled", reason
        join_token: str | None = None
        for _ in range(_JOIN_ATTEMPTS):
            try:
                attempt = control.begin_attempt(
                    self._log_dir, task_id, control.CANCEL, self._owner
                )
                if attempt.owned:
                    break
                # A joiner records its intent before signalling: while it is outstanding, the
                # owner's `.failed` does not settle the attempt (`control.attempt_state`), so a
                # delivery whose `.sig` is still being written cannot be outrun by it.
                join_token = control.begin_join(
                    self._log_dir, task_id, control.CANCEL, attempt.n, self._owner
                )
            except control.PhaseWriteError as exc:
                return "not_signalled", f"phase write failed: {exc}"
            if control.attempt_outcome(self._log_dir, task_id, control.CANCEL, attempt.n) != "failed":
                break
            # The attempt already had a `.failed` that may have been read before this intent
            # existed, so delivering under it could go unnoticed. Withdraw and start afresh.
            self._record_phase(
                task_id, attempt.n, control.nosig_phase(join_token), {"reason": "attempt settled"}
            )
            join_token = None
        else:
            return "not_signalled", "the cancel attempt kept settling concurrently; retry"

        # Re-checked immediately before the signal, with nothing in between: the phase writes and
        # withdraw rounds above take time, and a leader that exited meanwhile — its group kept
        # alive by a child, or its pgid reused — must neither be signalled nor recorded as having
        # been alive. `leader_alive` comes from this final observation only.
        verdict, reason = identity.check_detail(leader)
        if identity.signalable(verdict, reason):
            delivered = _signal_recorded_group(record, signal.SIGTERM)
            undelivered_reason = "process group already gone"
        else:
            delivered = False
            undelivered_reason = f"leader no longer ours to signal ({reason})"

        if delivered:
            # Written by a joining controller too, not only the attempt's owner — that is also what
            # resolves a joiner's intent. `.sig` outranks `.failed` in `control.attempt_outcome`.
            self._record_delivery(task_id, attempt.n, leader_alive=verdict == "alive")
        elif join_token is not None:
            self._record_phase(
                task_id, attempt.n, control.nosig_phase(join_token), {"reason": undelivered_reason}
            )
        elif attempt.owned:
            try:
                control.mark_failed(
                    self._log_dir, task_id, control.CANCEL, attempt.n, reason=undelivered_reason
                )
            except control.PhaseWriteError:
                log.warning(
                    "cascade: could not record cancel attempt %d's failure for %s",
                    attempt.n,
                    task_id,
                    exc_info=True,
                )

        if delivered or already_signalled:
            return "signalled", None
        if verdict == "dead":
            return "gone", None
        return "not_signalled", undelivered_reason

    def _record_delivery(
        self,
        task_id: str,
        n: int,
        *,
        leader_alive: bool,
        inline_attempts: int = _SIG_WRITE_ATTEMPTS,
    ) -> None:
        """Write `.sig` for a delivered SIGTERM, retried until it lands (see `_record_phase`).

        Never falls back to `.failed`: that means delivery failed, and publishing it for a signal
        that landed would turn a cancelled run into whatever `classify` makes of its exit.
        """
        self._record_phase(
            task_id, n, "sig", {"leader_alive": leader_alive}, inline_attempts=inline_attempts
        )

    def _record_phase(
        self,
        task_id: str,
        n: int,
        phase: str,
        payload: dict[str, Any],
        *,
        inline_attempts: int = _SIG_WRITE_ATTEMPTS,
    ) -> None:
        """Write a cancel attempt's outcome phase; if that keeps failing, hand it to `_retry_phase`.

        The attempt may not simply be left unresolved: its `.req` (or a joiner's intent) names this
        server as controller, and lease recovery only closes what a dead controller left behind —
        so while this server lives, an unwritten outcome would keep the owner's monitor waiting and
        retention keeping the record, forever. The retry job makes the unresolved state last
        exactly as long as this controller's work does; if the server dies, lease recovery takes
        over.
        """
        for attempt_index in range(inline_attempts):
            try:
                control.write_phase(
                    self._log_dir,
                    task_id,
                    control.CANCEL,
                    n,
                    phase,
                    {"at": _now().isoformat(), **payload},
                )
                return
            except control.PhaseWriteError:
                if attempt_index + 1 < inline_attempts:
                    time.sleep(_SIG_WRITE_RETRY_SECONDS)
        log.warning(
            "cancel of %s: could not write attempt %d's %s yet; retrying in the background",
            task_id,
            n,
            phase,
        )
        self._request_phase_retry(task_id, n, phase, payload)

    def _request_phase_retry(
        self, task_id: str, n: int, phase: str, payload: dict[str, Any]
    ) -> None:
        """Schedule `_retry_phase` on the registry's loop, from the loop or from a worker thread."""
        loop = self._loop
        if loop is None:  # pragma: no cover - every cancel path captures the loop first
            log.error("cancel of %s: no event loop to retry its %s on", task_id, phase)
            return
        try:
            on_loop = asyncio.get_running_loop() is loop
        except RuntimeError:
            on_loop = False
        if on_loop:
            self._start_phase_retry(task_id, n, phase, payload)
        else:
            loop.call_soon_threadsafe(self._start_phase_retry, task_id, n, phase, payload)

    def _start_phase_retry(
        self, task_id: str, n: int, phase: str, payload: dict[str, Any]
    ) -> None:
        """At most one live retry per (task, attempt, phase): a repeat request reuses the running
        job, and the key is released when that job ends."""
        key = (task_id, n, phase)
        running = self._phase_retries.get(key)
        if running is not None and not running.done():
            return
        job = asyncio.create_task(
            self._retry_phase(task_id, n, phase, payload), name=f"pb-phase-retry-{task_id}-{n}-{phase}"
        )
        self._phase_retries[key] = job
        self._control_jobs.add(job)

        def _release(done: asyncio.Task[Any]) -> None:
            self._control_jobs.discard(done)
            if self._phase_retries.get(key) is done:
                del self._phase_retries[key]

        job.add_done_callback(_release)

    async def settle_phase_writes(self, timeout: float) -> list[str]:
        """Wait up to `timeout` for outstanding cancel phase-write retries (`_retry_phase`), and
        return a description of each still pending.

        For a short-lived controller (`polybridge-ctl cancel`/`takeover`): its loop ending would
        cancel those retries, and lease recovery would then publish `.failed` for an attempt whose
        SIGTERM was in fact delivered — turning a deliberate cancel into whatever `classify` makes
        of the exit. A long-lived server has no such deadline and never needs this.
        """
        jobs = {key: job for key, job in self._phase_retries.items() if not job.done()}
        if jobs:
            await asyncio.wait(list(jobs.values()), timeout=timeout)
        return [
            f"{task_id} attempt {n} {phase}"
            for (task_id, n, phase), job in jobs.items()
            if not job.done()
        ]

    async def _retry_phase(
        self, task_id: str, n: int, phase: str, payload: dict[str, Any]
    ) -> None:
        """Keep trying to write attempt `n`'s `phase`, with capped backoff, until it exists.

        No give-up: stopping while this server lives would strand the attempt (see
        `_record_phase`). It ends early once `.sig` exists, whoever wrote it — that resolves a
        joiner's intent as well — but not on a `.failed`, which `.sig` outranks.
        """
        delay = SIG_RETRY_INITIAL_SECONDS
        while True:
            await asyncio.sleep(delay)
            try:
                if any(
                    control.phase_path(self._log_dir, task_id, control.CANCEL, n, done).exists()
                    for done in {phase, "sig"}
                ):
                    return
                await asyncio.to_thread(
                    control.write_phase,
                    self._log_dir,
                    task_id,
                    control.CANCEL,
                    n,
                    phase,
                    {"at": _now().isoformat(), **payload},
                )
                log.info("cancel of %s: wrote attempt %d's %s after retrying", task_id, n, phase)
                return
            except control.PhaseWriteError:
                delay = min(delay * 2, SIG_RETRY_MAX_SECONDS)
            except Exception:
                log.warning("cancel of %s: %s retry failed unexpectedly", task_id, phase, exc_info=True)
                delay = min(delay * 2, SIG_RETRY_MAX_SECONDS)

    @staticmethod
    def _kill_if_ours(record: store.TaskRecord) -> bool:
        """SIGKILL the recorded group only if the leader is still provably ours — checked in the
        same worker call as the signal, so no await separates the evidence from the act."""
        leader = identity.task_identity(record.pid, record.start_time, record.markers)
        if not identity.signalable(*identity.check_detail(leader)):
            return False
        return _signal_recorded_group(record, signal.SIGKILL)

    def _settled_record(self, task_id: str) -> store.TaskRecord | None:
        """The record if its owner has settled it — terminal and observed, or terminal with the
        leader confirmed dead. An unobserved terminal status (a torn-down server's backstop) can
        sit on a live process, and trusting it would skip the SIGKILL that process still needs."""
        record = store.read(self._log_dir, task_id)
        if record is None or record.status not in store.TERMINAL_RECORD_STATUSES:
            return None
        if not store.outcome_unobserved(record):
            return record
        leader = identity.task_identity(record.pid, record.start_time, record.markers)
        return record if identity.identity_check(leader) == "dead" else None

    async def _await_record_terminal(
        self, task_id: str, budget: float
    ) -> store.TaskRecord | None:
        """Poll a non-local cascade target's own record until its owner settles it, up to `budget`
        seconds — the record is expected to be settled by *its own* owning server (this is what
        lets case 2 lean on the same `cancel_verdict` rule a local cancel uses, rather than
        re-implementing termination for a process this server never spawned)."""
        deadline = asyncio.get_running_loop().time() + budget
        while True:
            record = await asyncio.to_thread(self._settled_record, task_id)
            if record is not None:
                return record
            if asyncio.get_running_loop().time() >= deadline:
                return None
            await asyncio.sleep(CANCEL_VERDICT_POLL_SECONDS)

    async def _cascade_case2(self, record: store.TaskRecord) -> _Case2Outcome:
        """One non-local cascade target whose owning server looks alive or undecidable: signal it
        and let that owner's own monitor settle it via `cancel_verdict`, exactly as a local cancel
        would — escalating to SIGKILL only if it does not, and handing off to case 3 only once the
        owner itself is confirmed dead rather than merely slow.
        """
        task_id = record.task_id
        kind, reason = await asyncio.to_thread(self._deliver_recorded_cancel, record)
        if kind == "gone":
            return _Case2Outcome(task_id, "done")
        if kind == "not_signalled":
            return _Case2Outcome(task_id, "not_signalled", reason)

        if await self._await_record_terminal(task_id, SIGKILL_GRACE_SECONDS) is not None:
            return _Case2Outcome(task_id, "done")

        await asyncio.to_thread(self._kill_if_ours, record)

        if await self._await_record_terminal(task_id, DRAIN_GRACE_SECONDS) is not None:
            return _Case2Outcome(task_id, "done")

        owner_verdict = await asyncio.to_thread(identity.identity_check, record.owner)
        if owner_verdict == "dead":
            return _Case2Outcome(task_id, "handoff")
        return _Case2Outcome(task_id, "still_settling")

    async def _cascade_case3_batch(
        self, records: list[store.TaskRecord], already_signalled: frozenset[str] = frozenset()
    ) -> dict[str, tuple[str, str | None]]:
        """A batch of non-local cascade targets whose owning server is confirmed dead: nobody else
        will ever settle their records, so the cascade signals and closes them itself.

        Each target gets its own `.req` -> SIGTERM -> `.sig` delivery (`_deliver_recorded_cancel`;
        one handed off from case 2 joins its existing attempt), but every leader actually signalled
        shares one wait/SIGKILL/wait sequence: there is no owner left to race against, so nothing is
        gained by doing this per target.

        `already_signalled` names targets handed off from case 2, whose SIGTERM already landed there
        (their `.sig` is on disk): a repeat delivery finding the group gone just means that signal
        worked, so they are still closed rather than reported as never signalled.

        Returns `{task_id: (outcome, reason)}`, outcome one of `"cancelled"`, `"sigkill_survivor"`,
        `"not_signalled"` or `"not_recorded"` (signalled, but writing `cancelled` failed). `reason`
        is set for the last two, and on a `"sigkill_survivor"` whose record also could not be
        written.
        """
        outcomes: dict[str, tuple[str, str | None]] = {}
        signalled: list[tuple[store.TaskRecord, dict]] = []
        for record in records:
            kind, reason = await asyncio.to_thread(
                self._deliver_recorded_cancel,
                record,
                already_signalled=record.task_id in already_signalled,
            )
            if kind == "signalled":
                leader = identity.task_identity(record.pid, record.start_time, record.markers)
                signalled.append((record, leader))
            elif kind == "not_signalled":
                outcomes[record.task_id] = ("not_signalled", reason)

        async def _not_yet_dead(
            candidates: list[tuple[store.TaskRecord, dict]], budget: float
        ) -> list[tuple[store.TaskRecord, dict]]:
            # Only a confirmed `dead` leaves the list: an `undecidable` leader (a transient `ps`
            # failure, say) may well still be running, and must stay visible as a survivor.
            deadline = asyncio.get_running_loop().time() + budget
            remaining = list(candidates)
            while remaining and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(CANCEL_VERDICT_POLL_SECONDS)
                remaining = [
                    pair
                    for pair in remaining
                    if await asyncio.to_thread(identity.identity_check, pair[1]) != "dead"
                ]
            return remaining

        survivors = await _not_yet_dead(signalled, SIGKILL_GRACE_SECONDS)
        if survivors:
            for record, _leader in survivors:
                await asyncio.to_thread(self._kill_if_ours, record)
            survivors = await _not_yet_dead(survivors, SIGKILL_GRACE_SECONDS)

        survivor_ids = {record.task_id for record, _leader in survivors}
        for record, _leader in signalled:
            survivor = record.task_id in survivor_ids
            reason: str | None = None
            try:
                written, _ = await asyncio.to_thread(
                    control.close_record_if_open,
                    self._log_dir,
                    record.task_id,
                    lambda r: replace(
                        r, status="cancelled", finished_at=r.finished_at or _now().isoformat()
                    ),
                )
            except (control.LockTimeout, OSError) as exc:
                # The signals have already gone out; raising here would lose the whole cascade
                # result to an unstructured error after an irreversible cancel. The record is left
                # exactly as it was — nothing half-written — and the caller is told it was not
                # published.
                log.warning("cascade: could not record %s as cancelled", record.task_id, exc_info=True)
                reason = f"signalled, but the cancellation could not be recorded: {exc}"
            else:
                # "terminal" is benign — whoever settled it (its owner, or another canceller) got
                # there first, and the status that stands is theirs — so it is not reported.
                reason = _NOT_RECORDED_REASONS.get(written)
            if reason is not None:
                outcomes[record.task_id] = (
                    ("sigkill_survivor", reason) if survivor else ("not_recorded", reason)
                )
                continue
            outcomes[record.task_id] = ("sigkill_survivor" if survivor else "cancelled", None)

        return outcomes

    async def cancel_cascade(self, task_id: str) -> dict[str, Any]:
        """Run `_cancel_cascade` as a registry-held task, shielded from the caller.

        Same reason `cancel` shields `_escalate`: the SIGKILL escalation for tasks this server does
        not own lives inside the cascade, and a client disconnecting mid-call must not abandon it.
        """
        return await self._shielded(self._cancel_cascade(task_id), f"pb-cascade-{task_id}")

    async def _shielded(self, coro: Any, name: str) -> Any:
        self._loop = asyncio.get_running_loop()
        job = asyncio.create_task(coro, name=name)
        self._control_jobs.add(job)
        job.add_done_callback(self._control_jobs.discard)
        return await asyncio.shield(job)

    async def _cancel_cascade(self, task_id: str) -> dict[str, Any]:
        """Cancel `task_id` and, best-effort, every live descendant this bridge can find via
        lineage — nested dispatches that task's own agent made through polybridge (`spawned_by`),
        or any record reporting the same `root_task_id`. Not a sandboxed guarantee: an agent that
        dispatched outside polybridge entirely is invisible to this.

        Targets are recomputed fresh every round (see `_cascade_targets`), and only the ones not
        already processed are acted on — so this converges to a fixed point rather than looping
        forever on a lineage that keeps growing, bounded at `CASCADE_MAX_ROUNDS` either way.
        """
        processed: set[str] = set()
        sigkill_survivors: list[str] = []
        owner_still_settling: list[str] = []
        not_signalled: list[dict[str, str]] = []
        not_recorded: list[dict[str, str]] = []
        rounds = 0
        converged = False

        for round_index in range(1, CASCADE_MAX_ROUNDS + 1):
            local_lineage = [(t.task_id, t.spawned_by, t.root_task_id) for t in self._tasks.values()]
            targets = await asyncio.to_thread(self._cascade_targets, task_id, local_lineage)
            new_targets = sorted(targets - processed)
            if not new_targets:
                converged = True
                break
            rounds = round_index
            processed.update(new_targets)

            local_ids: list[str] = []
            local_coros: list[Any] = []
            case2_ids: list[str] = []
            case2_coros: list[Any] = []
            case3_records: list[store.TaskRecord] = []
            handed_off: set[str] = set()

            for tid in new_targets:
                local_task = self._tasks.get(tid)
                if local_task is not None and not local_task.finished:
                    local_ids.append(tid)
                    local_coros.append(self.cancel(local_task))
                    continue

                kind, payload = await asyncio.to_thread(self._triage_target, tid)
                if kind == "not_signalled":
                    not_signalled.append({"task_id": tid, "reason": payload})
                elif kind == "case3":
                    case3_records.append(payload)
                elif kind == "case2":
                    case2_ids.append(tid)
                    case2_coros.append(self._cascade_case2(payload))

            if local_coros or case2_coros:
                # return_exceptions: one target failing (a phase file that cannot be written, say)
                # must neither abort the cascade nor leave its siblings running unobserved.
                results = await asyncio.gather(*local_coros, *case2_coros, return_exceptions=True)
                for tid, outcome in zip([*local_ids, *case2_ids], results):
                    if isinstance(outcome, BaseException):
                        if isinstance(outcome, asyncio.CancelledError):
                            raise outcome
                        log.warning("cascade: cancelling %s failed", tid, exc_info=outcome)
                        reason = (
                            f"phase write failed: {outcome}"
                            if isinstance(outcome, control.PhaseWriteError)
                            else f"error: {outcome}"
                        )
                        not_signalled.append({"task_id": tid, "reason": reason})
                        continue
                    if not isinstance(outcome, _Case2Outcome):
                        continue
                    if outcome.kind == "handoff":
                        fresh = await asyncio.to_thread(store.read, self._log_dir, outcome.task_id)
                        if fresh is not None:
                            case3_records.append(fresh)
                            handed_off.add(outcome.task_id)
                    elif outcome.kind == "still_settling":
                        owner_still_settling.append(outcome.task_id)
                    elif outcome.kind == "not_signalled":
                        not_signalled.append(
                            {"task_id": outcome.task_id, "reason": outcome.reason or ""}
                        )

            if case3_records:
                case3_outcomes = await self._cascade_case3_batch(
                    case3_records, frozenset(handed_off)
                )
                for tid, (kind, reason) in case3_outcomes.items():
                    if kind == "sigkill_survivor":
                        sigkill_survivors.append(tid)
                        if reason:
                            not_recorded.append({"task_id": tid, "reason": reason})
                    elif kind == "not_recorded":
                        not_recorded.append({"task_id": tid, "reason": reason or ""})
                    elif kind == "not_signalled":
                        not_signalled.append({"task_id": tid, "reason": reason or ""})

        # Fail closed at the round cap: the last round may itself have spawned or advanced
        # descendants, so scan once more. Anything new there was never signalled and appears in no
        # other list — reported, so a caller (takeover above all) cannot read the cascade as the
        # whole tree stopped.
        unconverged: list[str] = []
        if not converged:
            local_lineage = [(t.task_id, t.spawned_by, t.root_task_id) for t in self._tasks.values()]
            final_targets = await asyncio.to_thread(self._cascade_targets, task_id, local_lineage)
            unconverged = sorted(final_targets - processed)

        cancelled_descendants: list[str] = []
        for tid in processed:
            if tid == task_id:
                continue
            local_task = self._tasks.get(tid)
            if local_task is not None:
                status = local_task.status
            else:
                fresh = await asyncio.to_thread(store.read, self._log_dir, tid)
                if fresh is None:
                    continue
                status = (
                    await asyncio.to_thread(store.resolve_status, self._log_dir, fresh, detail=False)
                )[0]
            if status == "cancelled":
                cancelled_descendants.append(tid)

        return {
            "cancelled_descendants": sorted(cancelled_descendants),
            "sigkill_survivors": sorted(set(sigkill_survivors)),
            "owner_still_settling": sorted(set(owner_still_settling)),
            "not_signalled": not_signalled,
            "not_recorded": not_recorded,
            "rounds": rounds,
            "cascade_incomplete": bool(unconverged),
            "unconverged": unconverged,
        }

    async def cancel_recovered(self, record: store.TaskRecord) -> store.TaskRecord:
        """Shielded like `cancel_cascade`, for the same reason — see `_cancel_recovered`."""
        return await self._shielded(
            self._cancel_recovered(record), f"pb-cancel-recovered-{record.task_id}"
        )

    async def _cancel_recovered(self, record: store.TaskRecord) -> store.TaskRecord:
        """Stop a task this process never spawned, running the same non-local case-2/case-3 logic
        `cancel_cascade` uses for a single target — no cascade to descendants, just this one
        record. Kept for callers that mean to stop exactly this task. Its old unconditional
        `cancelled` write is gone: the record is only ever closed by the same rules a cascade
        target is — case 3's shared write once the owner is confirmed dead, or the record's own
        owner settling it after case 2's signal.
        """
        kind, payload = await asyncio.to_thread(self._triage_target, record.task_id)
        if kind == "case3":
            await self._cascade_case3_batch([payload])
        elif kind == "case2":
            outcome = await self._cascade_case2(payload)
            if outcome.kind == "handoff":
                fresh = await asyncio.to_thread(store.read, self._log_dir, record.task_id)
                if fresh is not None:
                    await self._cascade_case3_batch([fresh], frozenset({record.task_id}))
        else:
            log.info(
                "recovered task %s not signalled (%s)", record.task_id, payload or "already settled"
            )

        return store.read(self._log_dir, record.task_id) or record

    async def resume_record(
        self,
        record: store.TaskRecord,
        followup_prompt: str,
        *,
        max_turns: int | None = None,
        network: bool | None = None,
    ) -> Task:
        """Continue the session of a task recovered from disk."""
        if not record.session_id:
            raise SessionUnknownError(
                f"task {record.task_id} never disclosed a session id, so its conversation cannot "
                "be resumed; start a new task instead"
            )

        # Revalidated rather than trusted: the recorded path may have been deleted, or replaced by
        # a different repository, since the original run.
        repo_path = Path(record.repo_path)
        if not repo_path.is_dir():
            raise RepoUnavailableError(
                f"the repository this task ran in no longer exists: {record.repo_path}"
            )

        backend = get_backend(record.backend)
        # Same semantics as the live-parent path: None inherits the recorded request — which is
        # also None for a record written before the field existed, resuming at the freedom's
        # historical default — and an explicit boolean overrides it for this subprocess only.
        effective_network = network if network is not None else record.network

        # Same ordering as `resume`: caller detection and the caps run before the session lock.
        caller = await self._detect_caller()
        spawned_by, root_task_id, depth, max_depth, lineage_detected = self._resolve_lineage(
            caller,
            child_enforcement=backend.enforcement(record.freedom, effective_network),  # type: ignore[arg-type]
            child_backend=backend.name,
            child_repo=repo_path,
        )
        resolved_group = caller.record.group if caller is not None else record.group

        try:
            async with control.session_lock(
                self._log_dir, record.session_id, timeout=SESSION_LOCK_TIMEOUT_SECONDS
            ):
                if self.session_has_live_run(record.session_id):
                    raise SessionBusyError(
                        f"session {record.session_id} already has a running task, or is held by a takeover in "
                        "the Monitor; two concurrent runs would corrupt its shared conversation state"
                    )
                # Same revalidation as `resume`: the sweep holds this lock while deleting, so a
                # parent still readable here survives the spawn below — and one already gone was
                # deleted first, leaving nothing for the child to resume from.
                if await asyncio.to_thread(store.read, self._log_dir, record.task_id) is None:
                    raise SessionUnknownError(
                        f"task {record.task_id}'s record is no longer readable on disk (retention "
                        "removes aged tasks), so its conversation cannot be resumed; start a new "
                        "task instead"
                    )
                invocation = backend.build_resume_argv(
                    followup_prompt,
                    repo=repo_path,
                    freedom=record.freedom,  # type: ignore[arg-type]
                    session_id=record.session_id,
                    model=record.model,
                    max_turns=max_turns,
                    reasoning_effort=record.reasoning_effort,
                    network=effective_network,
                )
                return await self._spawn(
                    invocation,
                    backend=backend,
                    prompt=followup_prompt,
                    repo_path=repo_path,
                    session_id=record.session_id,
                    freedom=record.freedom,
                    max_turns=max_turns,
                    model=record.model,
                    reasoning_effort=record.reasoning_effort,
                    network=effective_network,
                    parent_task_id=record.task_id,
                    spawned_by=spawned_by,
                    root_task_id=root_task_id,
                    depth=depth,
                    max_depth=max_depth,
                    group=resolved_group,
                    title=record.title,
                    lineage_detected=lineage_detected,
                )
        except control.LockTimeout:
            raise SessionBusyError(
                f"another resume of session {record.session_id} is already in progress; "
                "try again shortly"
            ) from None

    def prune(self) -> None:
        """Drop the oldest finished tasks once over capacity.

        Called when tasks finish as well as when they start, so capacity is reclaimed either way.
        Live tasks are never evicted, so the registry can exceed `max_tasks` while more than that
        many runs are in flight; it settles back as they finish.

        Synchronous on purpose. Every mutation of `_tasks` happens without an intervening await, so
        the event loop cannot interleave two of them and no lock is needed.
        """
        overflow = len(self._tasks) - self._max_tasks
        if overflow <= 0:
            return
        for task in [t for t in self.list() if t.finished][:overflow]:
            del self._tasks[task.task_id]
            log.debug("evicted finished task %s", task.task_id)

    def start_maintenance(self) -> None:
        """Kick off a background retention sweep, at most once per registry instance.

        Lazily started here rather than from `__init__`, so a registry constructed directly by a
        unit test never triggers a sweep against real disk state — only `server._reg()` calls this,
        on a real event loop. `retention.maybe_sweep` runs in a thread since it does blocking file
        and `flock` I/O; the resulting `asyncio.Task` is held on `self._maintenance` so the event
        loop keeps a strong reference to it (a bare `create_task` result is only weakly referenced).
        """
        if self._maintenance is not None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        task = loop.create_task(
            asyncio.to_thread(retention.maybe_sweep, self._log_dir), name="pb-maintenance"
        )
        self._maintenance = task

        def _log_failure(done: asyncio.Task[Any]) -> None:
            if done.cancelled():
                return
            exc = done.exception()
            if exc is not None:
                log.debug("background maintenance sweep failed", exc_info=exc)

        task.add_done_callback(_log_failure)



# How long one flush of a live-input run's stdin may wait for the pipe to drain. The agent reads its
# stdin promptly, so this only bounds a wedged reader; the message stays queued in the transport.
STDIN_DRAIN_SECONDS = 10.0

# How often the input pump re-checks a live-input run without being woken (another process's
# inbox append is only noticed this way), and how long `_monitor` lets it finish after exit.
PUMP_POLL_SECONDS = 1.0
# After the run exits, how long the close protocol keeps waiting for a busy inbox lock before
# closing without it (`_close_unlocked`). Every holder keeps the lock for a few file operations, so
# only a stuck or suspended sender gets anywhere near this.
EXIT_CLOSE_GIVE_UP_SECONDS = 30.0
# How many consecutive close attempts may fail to read or seal the inbox, while the run lives, before
# input is ended anyway (`_force_close`) — a run that can never reach EOF would never settle.
CLOSE_RETRY_LIMIT = 10

# `_monitor`'s backstop for a pump stuck somewhere other than that bounded close.
PUMP_STOP_GRACE_SECONDS = EXIT_CLOSE_GIVE_UP_SECONDS + 3 * inbox.LOCK_TIMEOUT_SECONDS

_BACKGROUND_ABANDONED_NOTICE = (
    "background task abandoned at the idle bound: the run waited on background tasks alone, with "
    "no output, for {bound:.0f}s (PB_LIVE_IDLE_SECONDS), so polybridge closed its input anyway — "
    "which kills them, and nobody sees their result. Reported as failed."
)

# What a write to a live-input run's stdin raises once the reader is gone.
_PIPE_ERRORS = (BrokenPipeError, ConnectionResetError)


def _write_stdin(task: Task, data: bytes) -> bool:
    """Queue `data` on the run's stdin, synchronously. False if the pipe is closed or going away —
    asyncio's pipe transport silently drops a write once it is closing, so that is checked first."""
    stdin = task.proc.stdin if task.proc is not None else None
    if stdin is None or task.input_closed:
        return False
    try:
        if stdin.is_closing():
            task.input_closed = True
            return False
        stdin.write(data)
    except (*_PIPE_ERRORS, RuntimeError):
        task.input_closed = True
        return False
    return True


async def _drain_stdin(task: Task) -> bool:
    """Wait for queued stdin bytes to reach the pipe. False (and stdin marked closed) if the reader
    went away; a timeout is not a failure — the bytes stay queued."""
    stdin = task.proc.stdin if task.proc is not None else None
    if stdin is None or task.input_closed:
        return False
    try:
        await asyncio.wait_for(stdin.drain(), timeout=STDIN_DRAIN_SECONDS)
    except (asyncio.TimeoutError, TimeoutError):
        log.warning("task %s: stdin did not drain within %.0fs", task.task_id, STDIN_DRAIN_SECONDS)
    except (*_PIPE_ERRORS, RuntimeError):
        task.input_closed = True
        return False
    return True


def _close_stdin(task: Task) -> None:
    """Close a live-input run's stdin (EOF). Idempotent, and never raises."""
    task.input_closed = True
    stdin = task.proc.stdin if task.proc is not None else None
    if stdin is None:
        return
    try:
        stdin.close()
    except (*_PIPE_ERRORS, RuntimeError, OSError):
        pass


def _write_event(task: Task, kind: str, fields: dict[str, Any]) -> None:
    """One bridge-written event, guarded: a broken event log never changes an outcome."""
    if task.events is None:
        return
    try:
        task.events.write(kind, fields)
    except Exception:
        log.debug("task %s: could not write a %s event", task.task_id, kind, exc_info=True)


def _signal_recorded_group(record: store.TaskRecord, sig: int) -> bool:
    """Signal a recovered task's process group. False if it could not be signalled."""
    if record.pgid is None:
        return False
    try:
        os.killpg(record.pgid, sig)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _signal_group(task: Task, sig: int) -> bool:
    """Signal the run's whole process group. False if the group is already gone.

    Deliberately attempted even after the leader has exited: `claude`'s Bash children can outlive
    it and still hold the pipes open.
    """
    if task.pgid is None:
        return False
    try:
        os.killpg(task.pgid, sig)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover - group ownership changed underneath us
        log.warning("task %s: not permitted to signal process group %s", task.task_id, task.pgid)
        if task.proc is None:
            return False
        try:
            task.proc.send_signal(sig)
            return True
        except (ProcessLookupError, PermissionError, ValueError):
            return False


async def _escalate(task: Task) -> None:
    """Wait for the run to exit after its SIGTERM (already sent synchronously by
    `TaskRegistry._deliver_local_cancel`), escalating to SIGKILL if it does not go quietly.

    The old `_terminate` minus its own initial SIGTERM: that send now happens in the no-await
    section before this task is even created, so cancel-phase bookkeeping can complete before any
    await gives the monitor a chance to run.
    """
    proc = task.proc
    assert proc is not None

    try:
        await asyncio.wait_for(proc.wait(), timeout=SIGKILL_GRACE_SECONDS)
    except asyncio.TimeoutError:
        log.warning("task %s ignored SIGTERM, sending SIGKILL", task.task_id)
    # Unconditional: the leader may be gone while a child still holds the pipes.
    _signal_group(task, signal.SIGKILL)


async def _drain_stdout(task: Task, registry: TaskRegistry) -> None:
    """Continuously consume the event stream.

    This is load-bearing rather than an optimisation: stream-json is verbose, and an unread pipe
    fills its buffer and blocks `claude` indefinitely. That is also why the raw log is best-effort
    — a failing disk must not cost us the drain.
    """
    assert task.proc is not None and task.proc.stdout is not None
    reader = task.proc.stdout
    handle = None
    try:
        # Binary, not text mode: `raw_offset` below is `handle.tell()`, which counts bytes, and a
        # text-mode handle's tell() is not a reliable byte offset. `text` is still encoded with the
        # same `errors="replace"` decoding used for everything else here, so the file on disk stays
        # valid UTF-8 and `replay_log` reads it exactly as before.
        handle = task.log_path.open("ab")
    except OSError:
        log.warning("task %s: cannot open %s; continuing without a raw log", task.task_id, task.log_path)

    try:
        while True:
            try:
                raw = await reader.readline()
            except (ValueError, asyncio.LimitOverrunError):
                # readline() consumes the offending data before raising, so retrying makes
                # progress rather than spinning on the same bytes.
                log.warning(
                    "task %s: dropped a stream line over %d bytes", task.task_id, STREAM_LINE_LIMIT
                )
                task.acc.unparsable_lines += 1
                continue
            if not raw:
                break

            text = raw.decode("utf-8", errors="replace")
            raw_offset: int | None = None
            if handle is not None:
                try:
                    handle.write(text.encode("utf-8"))
                    handle.flush()
                    raw_offset = handle.tell()
                except OSError:
                    log.warning("task %s: raw log write failed; dropping it", task.task_id)
                    handle.close()
                    handle = None

            task.last_output_at = time.monotonic()
            line = text.rstrip("\n")
            if not line.strip():
                continue
            task.tail.append(line[:TAIL_LINE_CHARS])

            event = parse_line(line)
            if event is None:
                task.acc.unparsable_lines += 1
                continue
            backend = get_backend(task.backend)
            backend.ingest(event, task.acc)
            _record_events(task, backend, event, raw_offset)
            if task.pump is not None:
                task.pump_wake.set()

            # Recorded the moment it is disclosed, not at the end of the run. A backend that mints
            # its own session id only reveals it mid-stream, so waiting until exit would mean a
            # server that dies first loses any chance of resuming the conversation. Markers are
            # deliberately left alone: the id is not on that process's command line.
            if task.session_id is None and task.acc.session_id:
                task.session_id = task.acc.session_id
                log.info("task %s: session id is %s", task.task_id, task.session_id)
                registry.persist(task)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("task %s: stdout drainer failed", task.task_id)
        task.drain_failed = True
        # Nobody is reading the pipe any more, so the run would hang forever. End it instead.
        _signal_group(task, signal.SIGKILL)
    finally:
        if handle is not None:
            handle.close()


def _record_events(task: Task, backend: Backend, event: dict[str, Any], raw_offset: int | None) -> None:
    """Normalize one raw stream event and append the results to the task's event log.

    Guarded on its own, separately from `ingest` above: a broken normalizer must never touch
    `task.status` or `task.drain_failed` — those describe the run itself, which the raw stream and
    `ingest` have already faithfully advanced regardless of what this does. Written after the raw
    line it derives from, never before, so `raw_offset` always points at bytes already on disk.
    """
    if task.events is None:
        return
    try:
        normalized = backend.normalize(event, task.acc)  # type: ignore[attr-defined]
    except Exception:
        task.acc.normalize_errors += 1
        log.debug("task %s: normalize() failed for one event", task.task_id, exc_info=True)
        return
    for entry in normalized:
        fields = dict(entry)
        kind = fields.pop("kind", None)
        if kind is None:
            task.acc.normalize_errors += 1
            continue
        source_ts = fields.pop("source_ts", None)
        try:
            task.events.write(kind, fields, raw_offset=raw_offset, source_ts=source_ts)
        except Exception:
            task.acc.normalize_errors += 1
            log.debug("task %s: could not write a normalized event", task.task_id, exc_info=True)


async def _drain_stderr(task: Task) -> None:
    assert task.proc is not None and task.proc.stderr is not None
    reader = task.proc.stderr
    try:
        while True:
            try:
                raw = await reader.readline()
            except (ValueError, asyncio.LimitOverrunError):
                continue
            if not raw:
                break
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            if line.strip():
                task.stderr_tail.append(line[:TAIL_LINE_CHARS])
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("task %s: stderr drainer failed", task.task_id)
        task.drain_failed = True
        _signal_group(task, signal.SIGKILL)


async def _finish_draining(task: Task) -> None:
    """Read whatever is left of the pipes, but never wait on them forever.

    `proc.wait()` returns when `claude` exits, which does not mean its pipes are closed: a
    grandchild that inherited stdout keeps the write end open, so the drainers would never see EOF
    and the run could never be marked finished. Give them a grace period, then cut them loose.
    """
    if not task.watchers:
        return
    _, pending = await asyncio.wait(task.watchers, timeout=DRAIN_GRACE_SECONDS)
    if not pending:
        return

    log.warning(
        "task %s: output still open %.0fs after exit (a background child likely inherited it); "
        "abandoning the rest of the stream",
        task.task_id,
        DRAIN_GRACE_SECONDS,
    )
    for watcher in pending:
        watcher.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


async def _await_cancel_verdict(task: Task, registry: TaskRegistry) -> str:
    """Whether a cross-process cancel authorizes reading this exit as `cancelled`.

    Polls `control.cancel_verdict` every `CANCEL_VERDICT_POLL_SECONDS` while it reports
    `"pending"` — another controller's attempt is underway and its own leader-alive verdict has
    not landed yet. No deadline: an attempt only stays `pending` while its controller looks live
    or undecidable (see `cancel_verdict`'s own docstring), so this loop is bounded by that
    controller's lease recovery, not by anything here. Breaks out the moment this task's *own*
    owner requests a cancel (`task.cancel_requested`) — the caller re-checks that flag itself and
    it takes priority regardless of what this returns. Any failure reading the verdict is treated
    as `"none"`, per CLAUDE.md: bookkeeping must never change an outcome.
    """
    while True:
        if task.cancel_requested:
            return "none"
        try:
            verdict = await asyncio.to_thread(
                control.cancel_verdict, registry.log_dir, task.task_id
            )
        except Exception:
            log.warning(
                "task %s: could not read the cross-process cancel verdict; treating as none",
                task.task_id,
                exc_info=True,
            )
            return "none"
        if verdict != "pending":
            return verdict
        await asyncio.sleep(CANCEL_VERDICT_POLL_SECONDS)


async def _monitor(task: Task, registry: TaskRegistry) -> None:
    """Await exit, finish reading the stream, then publish the final status.

    The sole writer of terminal status and of `Task.done`.
    """
    assert task.proc is not None
    abandoned = False
    try:
        exit_code = await task.proc.wait()
        await _finish_draining(task)
        await registry._finish_pump(task)

        task.exit_code = exit_code
        task.finished_at = _now()

        if task.cancel_requested:
            task.status = "cancelled"
        else:
            classified = _classify(task, exit_code)
            verdict = await _await_cancel_verdict(task, registry)
            task.status = (
                "cancelled" if (task.cancel_requested or verdict == "authorized") else classified
            )

        observed = task.acc.session_id
        if observed and observed != task.session_id:
            # The drainer records a first sighting; reaching here means a CLI stopped honouring the
            # id we asked for. Trust the stream.
            log.warning(
                "task %s: session id from stream (%s) differs from requested (%s)",
                task.task_id,
                observed,
                task.session_id,
            )
            task.session_id = observed

        try:
            log.info(
                "task %s %s (exit=%s turns=%s cost=%s denials=%d)",
                task.task_id,
                task.status,
                exit_code,
                task.acc.num_turns,
                task.acc.total_cost_usd,
                len(task.acc.denials),
            )
        except Exception:  # pragma: no cover - logging must never change an outcome
            log.exception("task %s: could not log its completion", task.task_id)
    except asyncio.CancelledError:
        # Our event loop is going away, not the run: the agent is its own session leader and keeps
        # going without us. Publishing a terminal status here would record an outcome that never
        # happened — and because `store.write` refuses to move a task backwards, that lie would be
        # permanent, hiding a live process from every later server. So the record is left saying
        # `running` and the next process resolves it from the process itself.
        #
        # That resolution is correct whether or not the process is actually still alive, which is
        # why this path does not need to determine which: a dead one is reconstructed from its
        # stream log instead. Nothing in this package cancels a monitor, so getting here at all
        # means the loop is being torn down.
        #
        # A cancellation already in flight is the exception. That intent cannot be reconstructed
        # from anything on disk — a signalled run dies without a result event, so recovery would
        # infer `failed` and lose the fact that the user asked for this. Recording it is safe now
        # that a `cancelled` record carries no exit code and so is rechecked against the process:
        # if the SIGKILL never landed, it still resolves to `running`.
        abandoned = not task.cancel_requested
        log.warning(
            "task %s: monitor cancelled (process returncode=%s); recording it as %s",
            task.task_id,
            task.proc.returncode,
            "running, for a later server process to resolve" if abandoned else "cancelled as asked",
        )
        raise
    except Exception:
        log.exception("task %s: monitor failed", task.task_id)
        task.finished_at = _now()
        task.status = "cancelled" if task.cancel_requested else "failed"
    finally:
        # Skipped when abandoned, which is the one case where a task is left non-terminal: `done`
        # alongside a non-terminal status is only unobservable because the loop that could observe
        # it is the one being torn down.
        if not abandoned:
            if task.status not in TERMINAL_STATUSES:
                task.status = "cancelled" if task.cancel_requested else "failed"
                task.finished_at = task.finished_at or _now()
            task.done.set()
            try:
                registry.persist(task)
                registry.prune()
            except Exception:  # pragma: no cover - bookkeeping must never mask a finished run
                log.exception("task %s: recording the finished run failed", task.task_id)
            # Guarded separately from the persist/prune above: a broken event log must not affect
            # whether the run's terminal status and record landed, which is already done by here.
            try:
                if task.events is not None:
                    task.events.write(
                        "task_finished",
                        {
                            "status": task.status,
                            "exit_code": task.exit_code,
                            "summary": task.acc.summary,
                            "observed": task.exit_code is not None,
                        },
                    )
                    task.events.close()
            except Exception:
                log.exception("task %s: could not write the task_finished event", task.task_id)


def _classify(task: Task, exit_code: int) -> Status:
    """Delegate to the backend, except where the bridge itself broke the run."""
    if task.drain_failed:
        # We stopped reading its output, so whatever the agent reported cannot be trusted.
        return "failed"
    return get_backend(task.backend).classify(task.acc, exit_code)
