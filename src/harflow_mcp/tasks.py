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

from .task_outcomes import project_outcome

from . import supervision

import json
import asyncio
import contextvars
import functools
import threading
from concurrent.futures import ThreadPoolExecutor
import logging
import os
import copy
import signal
import time
import subprocess
import sys
import uuid
import weakref
from contextlib import contextmanager, nullcontext
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Awaitable, Callable, Iterator, Mapping
from typing import Any, Literal, NamedTuple

from . import control, followups, identity, inbox, lineage, retention, shared_scratch, store, system_guidance
from .backends import (
    Accumulator,
    Backend,
    classified_denials,
    Enforcement,
    Invocation,
    NestedDispatchRefused,
    check_nested_depth,
    check_nested_enforcement,
)
from .backends import get as get_backend
from .backends import resume_command
from .extra_paths import has_extra_path_authority
from .events import EVENTS_SUFFIX, EventLog, events_path
from .runtime_paths import MONITOR_BUNDLE
from .state_access import layout_operation, task_layout, assert_task_owned, open_descriptor
from .state_ownership import StateOwnershipError, state_session, assert_owned, owned_job, run_owned_worker
from .stream import parse_line

log = logging.getLogger(__name__)

EXTRA_PATH_CHECK_BUDGET_SECONDS = 20.0
EXTRA_PATH_CHECK_WORKERS = 4

# Symbolic stand-in for the per-task scratch root a dispatch appends to its recorded
# `Enforcement.writable_roots` (see `_spawn`). `preview_permissions` discloses it so the
# previewed writable_roots match what a launched task would carry, without allocating anything.
FUTURE_TASK_SCRATCH_ROOT = "the task scratch directory (allocated per task at launch)"
FUTURE_CHAIN_SCRATCH_ROOT = "the shared chain scratch directory (allocated at launch; the same for every resume and continuation)"
FUTURE_GROUP_SCRATCH_ROOT = "the shared group scratch directory (allocated at launch when the task has a group)"

# One shared wall-clock budget for every preflight authority read in a single request. Each
# individual check can legally wait up to EXTRA_PATH_CHECK_BUDGET_SECONDS, so several of them
# in one dispatch could together outlast a caller's own handshake window (detached's is 30 s).
# 15 s deliberately reserves half that window as headroom: the bounded publish-branch wait
# (GIT_PROBE_BUDGET_SECONDS * 2) and git baseline read (GIT_BASELINE_BUDGET_SECONDS + 0.2) plus fork and
# response overhead all have to fit after the authority budget for the handshake to answer
# before it expires. This budget governs only the pure preflight waits in
# `_bounded_extra_path_check` — never the subprocess await, and never the registry invocation as
# a whole: expiry refuses the next check before it is submitted. The deadline is an absolute
# wall-clock moment, not an accumulated CPU allowance: it ages during any other startup work
# between checks and is only consulted by them — nothing meters or debits that other work.
# Existing pre-spawn
# boundaries, rather than this timer, determine whether a refusal is safely retryable.
AUTHORITY_CHECK_REQUEST_BUDGET_SECONDS = 15.0
_AUTHORITY_CHECK_DEADLINE: contextvars.ContextVar[tuple[weakref.ReferenceType[Any] | None, float] | None] = contextvars.ContextVar(
    "harflow_mcp_authority_check_deadline", default=None
)


def authority_check_deadline() -> float | None:
    """The shared preflight authority-check deadline in force for this request, if any."""
    value = _AUTHORITY_CHECK_DEADLINE.get()
    return value[1] if value is not None else None


def _current_authority_check_deadline() -> float | None:
    value = _AUTHORITY_CHECK_DEADLINE.get()
    if value is None:
        return None
    owner, deadline = value
    if owner is None:
        return deadline
    try:
        current = asyncio.current_task()
    except RuntimeError:
        return None
    return deadline if owner() is current else None


@contextmanager
def authority_check_budget(seconds: float | None = None) -> Iterator[None]:
    """Start a fresh shared deadline for the current request; reset it on exit."""
    try:
        owner = asyncio.current_task()
    except RuntimeError:
        owner = None
    duration = AUTHORITY_CHECK_REQUEST_BUDGET_SECONDS if seconds is None else seconds
    token = _AUTHORITY_CHECK_DEADLINE.set((weakref.ref(owner) if owner is not None else None,
                                         time.monotonic() + duration))
    try:
        yield
    finally:
        _AUTHORITY_CHECK_DEADLINE.reset(token)


def _with_authority_check_budget(function):
    """Decorate a registry entry point so one request shares one preflight deadline.

    A nested entry point reached from inside another (`continue_task` reaching `start`, a resume
    reaching `_spawn`) sees a deadline already in force and reuses it; a fresh root request
    starts a new one. The budget is not a cancellation of the invocation itself — only the
    bounded preflight reads consult it.
    """

    @functools.wraps(function)
    async def wrapper(*args, **kwargs):
        if _current_authority_check_deadline() is not None:
            return await function(*args, **kwargs)
        with authority_check_budget():
            return await function(*args, **kwargs)

    return wrapper

_EXTRA_PATH_EXECUTOR = ThreadPoolExecutor(
    max_workers=EXTRA_PATH_CHECK_WORKERS, thread_name_prefix="harflow-extra-path"
)
_EXTRA_PATH_SLOTS = threading.BoundedSemaphore(EXTRA_PATH_CHECK_WORKERS)


async def _bounded_extra_path_check(function, *args):
    """Bound pure authority reads independently from the server's shared executor.

    Timeout cannot stop a filesystem syscall. Keep its slot occupied until the actual
    concurrent future finishes, so abandoned workers cannot create an unbounded queue.
    Callbacks must not allocate or mutate task state. A request deadline in force caps each
    wait at what remains of it, and an expired budget refuses the check before submitting it —
    so several individually in-budget checks cannot together outlast the request.
    """
    timeout = EXTRA_PATH_CHECK_BUDGET_SECONDS
    deadline = _current_authority_check_deadline()
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError(
                "Extra writable path validation request budget expired; "
                "check filesystem availability and retry"
            )
        timeout = min(timeout, remaining)
    slots = _EXTRA_PATH_SLOTS
    if not slots.acquire(blocking=False):
        raise ValueError(
            "Extra writable path validation capacity is exhausted; "
            "check filesystem availability and retry after outstanding checks finish"
        )
    try:
        context = contextvars.copy_context()
        worker = _EXTRA_PATH_EXECUTOR.submit(context.run, function, *args)
    except BaseException:
        slots.release()
        raise
    worker.add_done_callback(lambda _: slots.release())
    try:
        return await asyncio.wait_for(
            asyncio.wrap_future(worker), timeout=timeout
        )
    except TimeoutError as exc:
        raise ValueError(
            "Extra writable path validation timed out; check filesystem availability and retry"
        ) from exc


@contextmanager
def _before_spawn() -> Iterator[None]:
    """Mark failures only inside a boundary that cannot have created a process.

    Cancellation during preflight is likewise safe to retry; cancellation in the subprocess
    await is deliberately outside this boundary because process ownership is then uncertain.
    """
    try:
        yield
    except BaseException as exc:
        exc.harflow_not_started = True
        raise


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
CONTROL_EXIT_GRACE_SECONDS = 5.0
"""How long a control-pipe server may take to exit after its stdin closed at terminal evidence."""

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
_COVERED_KEY = "_covered_targets"  # internal: the targets a `_cancel_cascade` pass processed; never returned to callers

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
LIVE_IDLE_ENV = "HARFLOW_LIVE_IDLE_SECONDS"
DEFAULT_LIVE_IDLE_SECONDS = 600.0

# What a close is for, so `_close_input` can re-check that it still holds once it has the lock.
CLOSE_STOP = "stop"
CLOSE_IDLE = "idle"
CLOSE_IDLE_BOUND = "idle_bound"


def live_idle_seconds(environ: Any = None) -> float:
    """`HARFLOW_LIVE_IDLE_SECONDS`, or the default for anything missing, unparsable, or not > 0."""
    from .background_idle import resolve
    return resolve(environ=environ)


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
    "open a PR — but the checkout branch could not be determined at dispatch (no remote "
    "HEAD recorded locally, a detached HEAD, several remotes with none named 'origin', git being "
    "unavailable, or the check timing out). For a publish-authorized run, that is itself worth "
    "knowing: cancel this task now if you need to confirm the checkout before it proceeds. "
    "This was sampled once, immediately before the agent was launched — not during the run — so "
    "the branch may already have changed by the time the agent started, and will not be "
    "re-checked if it changes later."
)

_ON_DEFAULT_BRANCH_PUBLISH_NOTICE = (
    "this task was dispatched at freedom {freedom!r} while checked out on the repository's default "
    "branch ({branch!r}). The task is authorized to attempt commits, pushes, and PR creation; "
    "the eventual commit or publication branch is not established by this observation. Cancel this task "
    "now if the sampled checkout was not intended. "
    "This was sampled once, immediately before the agent was launched — not during the run — so "
    "the branch may already have changed by the time the agent started, and will not be "
    "re-checked if it changes later."
)

# The disclosure state comes from the Enforcement block, never from the freedom or backend name:
# authorization is `publish_attempts_allowed_by_polybridge`, network reachability is
# `network_access`. Two more states needed their own wordings once `network` became requestable —
# authorized with harflow_mcp's own network barrier raised (publish + network=False on codex: a
# network-backed push is blocked, but a push to a local path was measured to still succeed, so
# the barrier must never be worded as stopping publishing outright), and not authorized while
# the mechanism nevertheless reaches a remote (codex write_in_repo + network=True:
# byte-identical to publish's default, so the old freedom-name gate silently skipped it).
_ON_DEFAULT_BRANCH_NETWORK_BLOCKED_NOTICE = (
    "this task was dispatched at freedom {freedom!r} while checked out on the repository's default "
    "branch ({branch!r}). The task is authorized to attempt commits, pushes, and PR creation; "
    "harflow_mcp's own network barrier blocks a network-backed push and any network-backed PR "
    "creation here, but a push to a local path (measured: a bare repo under a writable root) "
    "still succeeds, so the barrier stops network-backed operations only. The eventual commit or "
    "publication branch is not established by this observation. This was sampled once, immediately before the "
    "agent was launched — not during the run — so the branch may already have changed by the time "
    "the agent started, and will not be re-checked if it changes later."
)
_UNDETERMINED_NETWORK_BLOCKED_NOTICE = (
    "this task was dispatched at freedom {freedom!r}, which authorizes it to commit, push, and "
    "open a PR — but the checkout branch could not be determined at dispatch (no remote "
    "HEAD recorded locally, a detached HEAD, several remotes with none named 'origin', git being "
    "unavailable, or the check timing out). HarflowMCP's own network barrier blocks a "
    "network-backed push and any network-backed PR creation here, but a push to a local path "
    "(measured: a bare repo under a writable root) still succeeds, so the barrier stops "
    "network-backed operations only. For a publish-authorized run, that is itself worth "
    "knowing: cancel this task now if you need to confirm the checkout before it proceeds. "
    "This was sampled once, immediately before the agent was launched — not during the run — so "
    "the branch may already have changed by the time the agent started, and will not be "
    "re-checked if it changes later."
)
_ON_DEFAULT_BRANCH_UNAUTHORIZED_REACHABLE_NOTICE = (
    "this task was dispatched at freedom {freedom!r}, which does not authorize committing, "
    "pushing, or opening a PR — but the mechanism it runs under nevertheless permits remote "
    "publication (enforcement.network_access is {network_access!r}), so a push can genuinely "
    "reach a remote. The checkout sampled at dispatch was on the repository's default "
    "branch ({branch!r}); this does not establish the eventual commit or publication branch. "
    "Cancel this task now if the sampled checkout was not intended. This was sampled once, "
    "immediately before the agent was launched — not during the run — so the branch may already "
    "have changed by the time the agent started, and will not be re-checked if it changes later."
)
_UNDETERMINED_UNAUTHORIZED_REACHABLE_NOTICE = (
    "this task was dispatched at freedom {freedom!r}, which does not authorize committing, "
    "pushing, or opening a PR — but the mechanism it runs under nevertheless permits remote "
    "publication (enforcement.network_access is {network_access!r}), so a push can genuinely "
    "reach a remote; the checkout branch could not be determined at dispatch (no "
    "remote HEAD recorded locally, a detached HEAD, several remotes with none named 'origin', "
    "git being unavailable, or the check timing out). That is itself worth knowing: cancel this "
    "task now if you need to confirm the checkout before it proceeds. This was sampled "
    "once, immediately before the agent was launched — not during the run — so the branch may "
    "already have changed by the time the agent started, and will not be re-checked if it "
    "changes later."
)

# Where network=True was accepted by a backend that has no barrier of its own to impose
# (enforcement.network_access stays "not_controlled"), acceptance must not silently read as "the
# network now works". Worded about what harflow_mcp imposes, never about reachability — the
# environment decides there, and a claim either way would overclaim.
_UNCONTROLLED_NETWORK_NOTICE = (
    "network=True was accepted, but harflow_mcp imposes no network barrier of its own on this "
    "backend — there is no mechanism it could raise or lower, so whether this run can reach the "
    "network is decided entirely by the surrounding environment. This is not a claim of "
    "reachability either way."
)


# These probes run in the harflow_mcp server itself, before any agent sandbox exists, so a target
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
    """Best-effort compatibility fields, with repository executable filters disabled."""
    from .task_outcomes import baseline
    observed = baseline(repo_path)
    status = observed["working_tree_status"]
    return observed["head"], bool(status) if status is not None else None


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
    wording). `not_controlled` deliberately does not count as reaching a remote: there harflow_mcp
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
    except StateOwnershipError:
        raise
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
OPEN_MONITOR_ENV = "HARFLOW_OPEN_MONITOR"
MONITOR_URL = "harflow://task/{task_id}"
MONITOR_OPEN_TIMEOUT_SECONDS = 30.0


async def _launch_monitor(url: str) -> int:
    """Launch the fixed Monitor bundle explicitly — `open -g -a <bundle> <url>` —
    rather than letting LaunchServices resolve the URL scheme through its mutable handler
    mapping (which writable authority elsewhere could have repointed). Returns the exit
    status; nonzero, and without spawning anything, when the bundle is absent. The
    existence probe runs off the event loop without consuming authority-validation
    capacity. The process is always reaped."""
    try:
        installed = await asyncio.wait_for(asyncio.to_thread(MONITOR_BUNDLE.is_dir),
                                           timeout=MONITOR_OPEN_TIMEOUT_SECONDS)
    except StateOwnershipError:
        raise
    except Exception:
        return 1
    if not installed:
        return 1
    proc = await asyncio.create_subprocess_exec(
        "open",
        "-g",
        "-a",
        str(MONITOR_BUNDLE),
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
    from .state_layout import resolve_layout
    return resolve_layout().tasks_directory


class _CascadeTargets(set):
    """Per-discovery targets with explicit refused history, isolated between calls."""
    def __init__(self, targets, refusals):
        super().__init__(targets)
        self.refusals = refusals


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
    display_prompt: str | None = None
    workflow_builder: bool = False
    model: str | None = None
    reasoning_effort: str | None = None
    parent_task_id: str | None = None
    continuation_of_task_id: str | None = None
    chain_root_task_id: str | None = None
    auto_continue: int = 0
    output_drained: bool = False
    chain_prompt: str | None = None
    freedom: str = "write_in_repo"
    caller_approvals: bool = False
    caller_write_approvals: bool = False
    caller_approval_version: str | None = None
    allowed_commands: list[str] = field(default_factory=list)
    allowed_commands_version: str | None = None
    resources: dict[str, Any] | None = None
    """The named resources this run holds; see `store.TaskRecord.resources`. Released (state
    `released`) by `_monitor` once the exit is observed."""
    worktree: dict[str, Any] | None = None
    """The linked worktree this run executes in; see `store.TaskRecord.worktree`."""
    scratch_chain_id: str | None = None
    """The shared chain scratch identity; see `store.TaskRecord.scratch_chain_id`."""
    scratch: dict[str, Any] | None = None
    """The scratch folders this run was given; see `store.TaskRecord.scratch`."""
    interaction_owner: str = "caller"
    approval_generation: str = field(default_factory=lambda: str(uuid.uuid4()))
    approval_pending: bool = False
    approval_receiving: bool = False
    approval_view: dict[str, Any] = field(default_factory=lambda: {"pending_count": 0, "awaiting_decision_count": 0, "interaction_state": None, "requests": []})
    network: bool | None = None
    """The network request this task was dispatched under: True/False explicit, None the
    freedom's historical default. Kept raw rather than resolved so a resume inherits exactly
    what was asked; the resolved outcome is `enforcement.network_access`."""
    extra_writable_paths: list[dict[str, Any]] = field(default_factory=list)
    extra_path_certification: str | None = None
    enforcement: dict[str, Any] = field(default_factory=dict)
    # Strings guaranteed to appear in the process's command line, so a later server process can tell
    # this task's pid apart from a reused one. What identifies a run differs per backend: Claude
    # carries its session id on the command line, Codex mints its own and does not.
    markers: tuple[str, ...] = ()

    spawned_by: str | None = None
    """The task_id of the task whose own agent dispatched this one via harflow_mcp — best-effort,
    from `lineage.detect_caller`. None for a root task (no caller detected)."""
    root_task_id: str | None = None
    """The top of this dispatch chain: this task's own id for a root task, else inherited from the
    caller's `root_task_id` (or the caller's own id, for a caller one level up from the root)."""
    depth: int = 0
    """How many nested dispatches deep this task is; 0 for a root task."""
    max_depth: int | None = None
    """The nesting budget this task (and anything it spawns) is checked against — `HARFLOW_MAX_DEPTH`
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
    background_idle_seconds: float = DEFAULT_LIVE_IDLE_SECONDS
    live_input: bool = False
    """Whether this run was spawned with a stdin pipe that takes further messages — taken from the
    run's own `Invocation`, never inferred from the backend. `send_message`, `harflow-ctl send`
    and the snapshot all read this field."""
    tool_procs: dict[int, tuple[int, str]] = field(default_factory=dict)
    """Tool commands of a control-pipe run as pid -> (process group, start time), recorded when a stop begins so the
    SIGKILL sweep reaches their groups after the parents are gone; the start time guards against pid reuse."""
    control_pipe: bool = False
    """Whether this run was spawned with a stdin pipe owned by the bridge for protocol frames only
    (approval answers, Harflow-originated frames) — taken from the run's own `Invocation`. It is
    independent of `live_input`: no inbox forwarding and no user-message delivery."""

    status: Status = "running"
    exit_code: int | None = None
    finished_at: datetime | None = None

    proc: asyncio.subprocess.Process | None = None
    _process_owner_pid: int = field(default_factory=os.getpid, init=False, repr=False, compare=False)
    # `start_new_session=True` makes the child its own group leader, so its pid is the group id.
    # Stored rather than looked up with getpgid() later: after the child is reaped its pid may
    # already name an unrelated process, and signalling that process's group would be a disaster.
    pgid: int | None = None

    events: EventLog | None = None
    """The task's normalized event log — see `events.py`. None only in tests that build a `Task`
    directly without going through `_spawn`."""
    _events_cleanup_armed: bool = False
    start_time: str | None = None
    """This task's own process start time, from `identity.capture` shortly after spawn. Null
    ("pending") until that capture lands — see `_spawn` — so `identity.identity_check` on a record
    with a null `start_time` falls back to its legacy pid+markers comparison."""
    dispatch_origin: dict[str, Any] | None = None
    plan_observations: Any = None
    owner: dict[str, Any] | None = None
    """The process owning execution/output capture, separate from the dispatching server."""
    base_commit: str | None = None
    """`git rev-parse HEAD` in the repo at spawn, before the agent ran. None if the repo had no
    commit yet, or the probe failed or timed out."""
    git_baseline: dict[str, Any] | None = None
    outcome: dict[str, Any] | None = None
    outcome_evidence: dict[str, Any] = field(default_factory=dict)
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
    a remote) and by the line saying harflow_mcp imposed nothing, where network=True was
    accepted on a backend with no barrier of its own."""
    tail: deque[str] = field(default_factory=lambda: deque(maxlen=TAIL_LINES))
    stderr_tail: deque[str] = field(default_factory=lambda: deque(maxlen=STDERR_TAIL_LINES))

    input_closed: bool = False
    """Whether harflow_mcp has closed (or lost) this live-input run's stdin. Always False for a
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
    """`acc.result_count` when harflow_mcp last wrote a message to the run. A turn is owed for it, so
    until a later result arrives an earlier success is not the run's outcome — persisted, so a
    recovered run's replay knows it too (`store._resolve`)."""
    results_owed: int = 0
    """How many turn results this live run owes before it may be considered idle: 1 for the initial
    prompt, plus one per message written — but only for a backend where each written line starts a
    turn of its own (`live_input_message_is_turn`, agy, measured). claude folds a mid-turn message
    into the running turn, so however many messages were written it is only ever owed
    `result_count + 1`. Not persisted: recovery derives the same fact from `input_after_result`,
    which both shapes set to "results still owed" (see `store._resolve` and
    `_note_input_written`). 0 for a run that never took live input, and for a directly-constructed
    Task whose idle behaviour is driven by `acc.awaiting_input` alone."""
    last_output_at: float = field(default_factory=time.monotonic)
    """`time.monotonic()` of the last stdout line — the idle bound's clock."""

    done: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_requested: bool = False
    # Whether any `persist` of this task landed on disk. A record that did and is now gone was
    # deleted (retention); one that never did is merely unwritten, and resuming it stays allowed.
    record_landed: bool = False
    latest_record_landed: bool = False
    persistence_error: store.TaskDocumentError | None = None
    drain_failed: bool = False

    # Held so the event loop keeps strong references: a bare create_task() result is only weakly
    # referenced and may be garbage-collected mid-run.
    watchers: list[asyncio.Task[Any]] = field(default_factory=list)
    monitor: asyncio.Task[Any] | None = None
    usage_settlement: asyncio.Task[Any] | None = None
    termination: asyncio.Task[Any] | None = None

    @property
    def state_layout(self):
        return task_layout(self.log_path.parent)

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

    @layout_operation
    def brief(self) -> dict[str, Any]:
        """The listing shape: identity and state, without stream detail.

        `notices` here is the bridge's own only. The backend's `Accumulator.notices` are parsed out
        of the run's stream, so they *are* stream detail and belong in `snapshot`; bridge notices
        describe the dispatch instead, and a recovered task carries them on its record with no
        replay cost — so live and recovered listings can hold the same shape.
        """
        from . import approvals
        return {
            "metadata_persisted": self.latest_record_landed,
            **({"persistence_error": self.persistence_error.as_dict()} if self.persistence_error is not None else {}),
            "caller_approvals": self.caller_approvals,
            "caller_write_approvals": self.caller_write_approvals,
            "interaction_owner": self.interaction_owner,
            **({"resources": dict(self.resources)} if self.resources else {}),
            **({"worktree": dict(self.worktree)} if self.worktree else {}),
            **({"scratch": dict(self.scratch)} if self.scratch else {}),
            "approval_summary": self.approval_view,
            "task_id": self.task_id,
            **supervision.fields(self, self.log_path, observed_terminal=self.finished, live=True, input_closed=self.input_closed),
            "continuation_of_task_id": self.continuation_of_task_id,
            "chain_root_task_id": self.chain_root_task_id,
            "auto_continue": self.auto_continue,
            "output_drained": self.output_drained,
            "workflow_builder": self.workflow_builder,
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
            "background_idle_seconds": self.background_idle_seconds,
            "notices": list(self.bridge_notices),
            "queued_followup_count": followups.outstanding_count(self.log_path.parent, self.task_id),
            "failure_diagnostic": self.acc.failure_diagnostic,
            "owner": self.owner,
            # Meaningful only while the task is still running — a settled task has no live server
            # to speak of, so this is null rather than a claim about the process that finished it.
            "owned_by_live_server": (True if not self.finished else None),
        } | control.taken_over_fields(self.log_path.parent, self.task_id)

    @layout_operation
    def snapshot(self) -> dict[str, Any]:
        """Full current state of the run, safe to call at any point."""
        if self.persistence_error is None:
            store.read(self.log_path.parent, self.task_id, include_prompt=False)
        from . import artifacts
        from . import approvals
        denials, denial_categories = classified_denials(get_backend(self.backend), self.acc, self.freedom)
        snap = self.brief() | {
            "caller_approvals": self.caller_approvals,
            "caller_write_approvals": self.caller_write_approvals,
            "interaction_owner": self.interaction_owner,
            "approval_summary": self.approval_view,
            "artifact_summary": artifacts.summary(self.log_path.parent, self.task_id),
            # Overrides brief's bridge-only list with the full merge; see `brief`.
            "notices": self._notices(),
            "summary": self.acc.summary,
            "failure_diagnostic": self.acc.failure_diagnostic,
            "is_error": self.acc.is_error,
            "total_cost_usd": self.acc.total_cost_usd,
            "num_turns": self.acc.num_turns,
            "exit_code": self.exit_code,
            "permission_denials": denials,
            "denial_categories": denial_categories,
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
            "outcome": project_outcome(self.outcome),
            "continuation_of_task_id": self.continuation_of_task_id,
            "chain_root_task_id": self.chain_root_task_id,
            "auto_continue": self.auto_continue,
            "output_drained": self.output_drained,
            "events_log": str(self.log_path.with_name(f"{self.task_id}{EVENTS_SUFFIX}")),
            "resume_command": None if has_extra_path_authority(self) else resume_command(self.backend, self.session_id, self.repo_path),
            "extra_writable_paths": self.extra_writable_paths,
            "extra_path_certification": self.extra_path_certification,
            "allowed_commands": list(self.allowed_commands),
        }
        # Only meaningful when something went wrong, and usually empty otherwise.
        if self.status == "failed" and self.stderr_tail:
            snap["stderr_tail"] = list(self.stderr_tail)
        if self.caller_write_approvals:
            snap["caller_approved_writes"], write_notice = approvals.backend_approved_writes(self.log_path.parent, self.task_id, get_backend(self.backend))
            if write_notice:
                snap["notices"] = [*snap["notices"], write_notice]
        snap["pending_messages"] = inbox.pending_messages(self.log_path.parent, self.task_id)
        if self.workflow_builder:
            from .workflows import builder_pending_messages
            try:
                snap["pending_messages"] = builder_pending_messages(self.log_path.parent, self.task_id, snap["pending_messages"])
            except (OSError, ValueError, KeyError, TypeError):
                log.warning("task %s: workflow pending-message bookkeeping unavailable", self.task_id, exc_info=True)
        return snap


class TaskRegistry:
    """In-memory registry of dispatched runs, bounded to `max_tasks` finished entries."""

    def __init__(
        self,
        log_dir: Path | None = None,
        max_tasks: int = MAX_TASKS,
        owner: dict[str, Any] | None = None,
        *,
        open_monitor: bool = False,
        caller_override: lineage.Caller | None = None,
        monitor_launcher: Callable[[str], Awaitable[int]] | None = None,
        execution_mode: str = "in_process",
    ) -> None:
        if execution_mode not in {"in_process", "supervised"}:
            raise ValueError("Unknown task execution mode")
        self.execution_mode = execution_mode
        self._attached = {}
        self._tasks: dict[str, Task] = {}
        self._caller_override = caller_override
        # Whether a root task this registry starts opens the Monitor app (A4.3). False for
        # `harflow-ctl run`/`resume`, which the app itself drives.
        self._open_monitor = open_monitor
        self._monitor_launcher = monitor_launcher
        # Strong references to the background `open` jobs (see `_maybe_open_monitor`).
        self._monitor_jobs: set[asyncio.Task[Any]] = set()
        from .state_layout import resolve_layout
        self.state_layout = resolve_layout(log_dir=log_dir)
        self._log_dir = self.state_layout.tasks_directory
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

    async def _detect_caller(self, candidates: Any = None) -> lineage.Caller | None:
        """Best-effort: which task (if any) dispatched the process calling us.

        Looked up through the `lineage` module attribute at call time — never imported as a bare
        name — so a test can neutralise it with `monkeypatch.setattr(lineage, "detect_caller",
        ...)` (see `tests/conftest.py`). `detect_caller` already turns its own failures into None;
        this wrapper exists so a failure in the `to_thread` dispatch itself (or a misbehaving
        monkeypatch) cannot become an exception either — per CLAUDE.md, bookkeeping must never
        change an outcome.

        `candidates` reuses a caller-supplied metadata scan (from `lineage.read_candidates`) so a
        path that also needs `detect_caller_detail` reads the records once, not three times.
        """
        if self._caller_override is not None:
            return self._caller_override
        try:
            return await asyncio.to_thread(lineage.detect_caller, self._log_dir, candidates=candidates)
        except StateOwnershipError:
            raise
        except Exception:
            log.debug("caller detection failed", exc_info=True)
            return None

    async def _detect_caller_detail(self) -> lineage.Detection:
        if self._caller_override is not None:
            return lineage.Detection(self._caller_override)
        caller = await self._detect_caller()
        if caller is not None:
            return lineage.Detection(caller)
        try:
            result = await asyncio.to_thread(lineage.detect_caller_detail, self._log_dir)
        except StateOwnershipError:
            raise
        except Exception as exc:
            return lineage.Detection(None, f"caller detection failed: {exc}")
        if result.caller is None and os.environ.get(lineage.ENV_TASK_ID):
            return lineage.Detection(None, result.undecidable or "HARFLOW_TASK_ID is set but its authority cannot be verified")
        return result

    async def _mutation_caller(self) -> lineage.Caller | None:
        from .caller_policy import require_mutation, signals
        environ = dict(os.environ)
        signals(environ)
        detection = await self._detect_caller_detail()
        await asyncio.to_thread(require_mutation, self._log_dir, caller=detection.caller, environ=environ)
        if detection.undecidable is not None:
            raise NestedDispatchRefused("Caller authority cannot be established: " + detection.undecidable, rule="caller_undecidable")
        return detection.caller

    def _resolve_lineage(
        self,
        caller: lineage.Caller | None,
        *,
        child_enforcement: Enforcement,
        child_backend: str,
        child_repo: Path,
        caller_write_approvals: bool = False,
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

        from .workflow_hooks import refuse_managed
        refuse_managed(self._log_dir, caller.record.task_id)

        parent_max_depth = (
            caller.record.max_depth
            if caller.record.max_depth is not None
            else lineage.max_depth_default()
        )
        from .caller_policy import signals
        parent_depth = max(caller.record.depth, signals()[1])
        check_nested_depth(parent_depth, parent_max_depth)
        check_nested_enforcement(
            caller.record.enforcement or {},
            child_enforcement,
            parent_backend=caller.record.backend,
            child_backend=child_backend,
            parent_repo=caller.record.repo_path,
            child_repo=str(child_repo),
        )
        if caller_write_approvals and not getattr(caller.record, "caller_write_approvals", False):
            raise NestedDispatchRefused("nested dispatch refused: the caller does not hold caller_write_approvals; this cap is best-effort", rule="caller_write_approvals")
        root_task_id = caller.record.root_task_id or caller.record.task_id
        return caller.record.task_id, root_task_id, parent_depth + 1, parent_max_depth, caller.method

    @staticmethod
    def _command_grants(backend: Backend, requested: Any, inherited: Any, freedom: str, repo_path: Path, *, native: bool = False, builder: bool = False) -> tuple[str, ...]:
        """An explicit list replaces; None inherits the saved list. Bounds and the backend grammar are always re-checked
        against the current repository, so a stale or tampered saved list is never trusted."""
        from . import command_grants
        patterns = command_grants.check_bounds(list(inherited or ())) if requested is None else command_grants.check_bounds(requested)
        if patterns:
            if native or builder:
                raise command_grants.CommandGrantError("allowed_commands is unavailable to native workflows and workflow builders")
            backend.validate_allowed_commands(patterns, freedom, repo_path)
        return patterns

    @staticmethod
    def _with_commands(backend: Backend, enforcement: Enforcement, patterns: tuple[str, ...]) -> Enforcement:
        return backend.allowed_commands_enforcement(enforcement, patterns) if patterns else enforcement

    @staticmethod
    async def _configure_commands(backend: Backend, invocation: Invocation, patterns: tuple[str, ...], freedom: str, repo_path: Path, network: bool | None) -> Invocation:
        """Qualify the installed version, then add the grants to the argv; asserted before it is returned."""
        if not patterns:
            return invocation
        version = await asyncio.to_thread(backend.qualify_allowed_commands)
        result = backend.configure_allowed_commands(invocation, patterns, freedom, repo_path, version)
        backend.assert_safe(result, freedom, network)
        return result

    @staticmethod
    def _extra_enforcement(backend, enforcement, grants, repo_path):
        if not grants:
            return enforcement
        if enforcement.freedom == "unrestricted":
            from .extra_paths import serialize_paths
            return replace(enforcement, extra_writable_paths=tuple(serialize_paths(grants)),
                           extra_path_certification=None,
                           caveats=(*enforcement.caveats, "extra_writable_paths was validated but is a no-op at unrestricted freedom; it grants no additional access, confinement, or certification."))
        return backend.extra_path_enforcement(enforcement, grants, repo_path)

    def _saved_extra_paths(self, record: Any, backend: Backend):
        from .extra_paths import restore_paths, revalidate_paths
        grants = restore_paths(record.extra_writable_paths)
        certificate = record.extra_path_certification
        if not grants and certificate is None and has_extra_path_authority(record):
            raise ValueError("Saved extra writable path authority is missing")
        if grants or certificate is not None:
            expected = backend.extra_path_support().get("certification")
            noop = record.freedom == "unrestricted" and certificate is None
            if not grants or (not noop and (not certificate or certificate != expected)):
                raise ValueError("Saved extra writable path certification is unavailable or incompatible")
            if not isinstance(record.enforcement, Mapping) or record.enforcement.get("extra_writable_paths") != record.extra_writable_paths or record.enforcement.get("extra_path_certification") != certificate:
                raise ValueError("Saved extra writable path authority disagrees with enforcement")
            revalidate_paths(grants)
        return grants

    def _resolve_extra_paths(self, caller: Any, request: list[str] | None, backend: Backend, source: Any = None):
        from .extra_paths import normalize_paths, check_child_paths
        owner = self._saved_extra_paths(caller.record, get_backend(caller.record.backend)) if caller else ()
        if source is not None:
            grants = self._saved_extra_paths(source, backend)
            if source.chain_root_task_id and source.chain_root_task_id != source.task_id:
                root = store.read(self._log_dir, source.chain_root_task_id)
                if root is None:
                    if grants:
                        raise ValueError("Continuation root authority unavailable")
                else:
                    roots = self._saved_extra_paths(root, get_backend(root.backend))
                    if roots and not grants:
                        raise ValueError("Continuation cannot restore legacy ambient authority")
                    check_child_paths(roots, grants, root.repo_path)
            if request is not None:
                raise ValueError("Continuations inherit saved path identities; path substitution is refused")
        else:
            grants = owner if request is None and caller else normalize_paths(request)
        if caller:
            if owner and not grants:
                raise ValueError("An extra-path owner cannot narrow to legacy ambient write authority")
            check_child_paths(owner, grants, Path(caller.record.repo_path))
        return grants

    async def _resolve_extra_paths_async(self, caller, request, backend, source=None):
        # Fresh ordinary calls carry no filesystem authority to inspect. Keep them
        # available even if an unrelated mount has exhausted the bounded-check pool.
        if caller is None and source is None and (request is None or (type(request) is list and not request)):
            return ()
        return await _bounded_extra_path_check(self._resolve_extra_paths, caller, request, backend, source)

    def _assert_extra_state(self, repo_path, grants, invocation=None):
        if not grants:
            return
        from .bridge_state import assert_state_outside_writes
        assert_state_outside_writes(self._log_dir, repo_path, grants,
                                   (*((invocation.scratch_directory,) if invocation.scratch_directory else ()), *invocation.shared_scratch_directories)
                                   if invocation is not None else None)

    @_with_authority_check_budget
    @layout_operation
    async def preview_permissions(self, repo_path: Path, *, backend: Backend, freedom: str = "write_in_repo", network: bool | None = None, caller_approvals: bool = False, extra_writable_paths: list[str] | None = None, caller_write_approvals: bool = False, allowed_commands: list[str] | None = None) -> dict[str, Any]:
        from .extra_paths import serialize_paths
        from . import approval_runtime
        approval_runtime.validate(backend, caller_approvals, freedom, None, write=caller_write_approvals)
        commands = self._command_grants(backend, allowed_commands, (), freedom, repo_path)
        approval_version = None
        if caller_approvals:
            approval_version = await asyncio.to_thread(backend.qualify_caller_approvals)
            approval_runtime._write_validator(backend)(caller_write_approvals, caller_approvals, approval_version)
        caller = await self._mutation_caller()
        grants = await self._resolve_extra_paths_async(caller, extra_writable_paths, backend)
        if grants:
            await _bounded_extra_path_check(self._assert_extra_state, repo_path, grants)
        invocation = backend.build_start_argv("Permission preview", repo=repo_path, freedom=freedom, session_id=str(uuid.uuid4()) if backend.capabilities.chooses_session_id else None, model=None, max_turns=None, reasoning_effort=None, network=network)
        invocation = await self._configure_commands(backend, invocation, commands, freedom, repo_path, network)
        invocation = await _bounded_extra_path_check(backend.qualify_extra_writable_paths, invocation, grants, freedom, network) if grants and freedom != "unrestricted" else invocation
        if grants and freedom != "unrestricted":
            # The qualifier asserted the invocation it returned; only the bridge-state check
            # remains, against the qualified invocation's scratch directory.
            await _bounded_extra_path_check(self._assert_extra_state, repo_path, grants, invocation)
        else:
            backend.assert_safe(invocation, freedom, network)
        enforcement = self._extra_enforcement(backend, backend.enforcement(freedom, network), grants, repo_path)
        enforcement = self._with_commands(backend, approval_runtime.with_approval_enforcement(backend, enforcement, approval_version, caller_approvals, caller_write_approvals), commands)
        self._resolve_lineage(caller, child_enforcement=enforcement, child_backend=backend.name, child_repo=repo_path, caller_write_approvals=caller_write_approvals)
        # A dispatch grants every non-read-only task of a scratch-aware backend a task-owned
        # scratch directory and appends its path to the recorded writable_roots — after the
        # lineage check above, exactly like here. The preview allocates nothing, so it discloses
        # the future root symbolically and never feeds it to the nested-authority comparison.
        if freedom != "read_only" and getattr(backend, "with_writable_directory", None) is not None and not (caller_approvals and not getattr(backend, "approval_scratch", lambda: True)()):
            enforcement = replace(enforcement, writable_roots=(*enforcement.writable_roots, FUTURE_TASK_SCRATCH_ROOT))
            if getattr(backend, "with_shared_directories", None) is not None:
                enforcement = replace(enforcement, writable_roots=(*enforcement.writable_roots, FUTURE_CHAIN_SCRATCH_ROOT, FUTURE_GROUP_SCRATCH_ROOT))
        return {"backend": backend.name, "caller_approvals": caller_approvals, "caller_write_approvals": caller_write_approvals, "caller_approval_support": backend.caller_approval_support() if getattr(backend.capabilities, "supports_caller_approvals", False) else {"supported": False}, "approval_scope": "exact_action_once" if caller_approvals else None, "approval_transport": approval_runtime.transport(backend) if caller_approvals else None, "approval_limitations": approval_runtime.limitations(backend, caller_write_approvals), "freedom": freedom, "network": network, "repo_path": str(repo_path), "extra_writable_paths": serialize_paths(grants), "extra_path_certification": enforcement.extra_path_certification, "allowed_commands": list(commands), "enforcement": enforcement.as_dict(), "limitations": "Existing typed path identities are checked before launch; grants are unavailable to native workflows and interactive takeover."}

    @_with_authority_check_budget
    @layout_operation
    async def start(
        self,
        prompt: str,
        repo_path: Path,
        *,
        backend: Backend,
        freedom: str = "write_in_repo",
        max_turns: int | None = None,
        background_idle_seconds: float | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        network: bool | None = None,
        caller_approvals: bool = False,
        interaction_owner: str = "caller",
        task_id: str | None = None,
        display_prompt: str | None = None,
        workflow_builder: bool = False,
        group: str | None = None,
        title: str | None = None,
        auto_continue: int = 0,
        caller_write_approvals: bool = False,
        continuation_of_task_id: str | None = None,
        chain_root_task_id: str | None = None,
        chain_prompt: str | None = None,
        extra_writable_paths: list[str] | None = None,
        message_ids: list[str] | None = None,
        allowed_commands: list[str] | None = None,
        resources: list[str] | None = None,
        resource_wait_seconds: int | None = None,
        resource_inherit: dict[str, Any] | None = None,
        followup_source_task_id: str | None = None,
        worktree: dict[str, Any] | None = None,
        worktree_inherit: dict[str, Any] | None = None,
        scratch_chain_id: str | None = None,
    ) -> Task:
        """Dispatch a fresh session. Returns once the subprocess exists, not once it finishes.

        `message_ids` are the queued follow-ups a continuation consumed; its `task_started` event
        carries them."""
        from .state_layout import require_dispatch_layout
        require_dispatch_layout(self._log_dir)
        from .background_idle import resolve
        background_idle_seconds = resolve(background_idle_seconds)
        with _before_spawn():
            from .task_continuation import validate_budget
            validate_budget(auto_continue)
            if auto_continue and not backend.capabilities.supports_auto_continue:
                raise ValueError(f"{backend.name} has no certified automatic continuation cutoff; use explicit continue_task")
            from . import approval_runtime
            approval_runtime.validate(backend, caller_approvals, freedom, max_turns, write=caller_write_approvals)
            if interaction_owner not in {"caller", "monitor"}:
                raise ValueError("interaction_owner must be caller or monitor")
            if interaction_owner == "monitor":
                await approval_runtime.authorize(self, interaction_owner="monitor")
            caller = await self._mutation_caller()
            resource_request = await asyncio.to_thread(self._resource_request, resources, resource_wait_seconds, resource_inherit, caller)
            worktree_plan = None
            if worktree is not None:
                # Planned only: the owner creates it after any resource grant. The invocation below is
                # built against the deterministic planned path and rebuilt by the owner once it exists.
                task_id = task_id or str(uuid.uuid4())
                worktree_plan = self._plan_worktree(worktree, repo_path, freedom, caller, task_id)
                source_repo_path, repo_path = repo_path, Path(worktree_plan["path"])
            from .task_supervision import read_task_record
            source = await _bounded_extra_path_check(read_task_record, self._log_dir, continuation_of_task_id) if continuation_of_task_id is not None else None
            if continuation_of_task_id is not None and source is None:
                raise ValueError("Continuation source unavailable")
            if source is not None and (source.backend != backend.name or source.freedom != freedom or source.network != network or Path(source.repo_path) != repo_path or source.model != model or source.reasoning_effort != reasoning_effort or source.caller_approvals != caller_approvals or source.caller_write_approvals != caller_write_approvals or source.interaction_owner != interaction_owner):
                raise ValueError("Continuation settings must match saved source authority")
            grants = await self._resolve_extra_paths_async(caller, extra_writable_paths, backend, source)
            if grants and workflow_builder:
                raise ValueError("Extra writable paths are unavailable to workflow builders")
            commands = self._command_grants(backend, allowed_commands, source.allowed_commands if source is not None else (), freedom, repo_path, builder=workflow_builder)
            # Only some backends let us name the session up front. Where we can, knowing it immediately
            # means a resume works even if the run dies before saying anything.
            session_id = str(uuid.uuid4()) if backend.capabilities.chooses_session_id else None
            if task_id and self.execution_mode == "supervised":
                from .task_supervision import read_launch
                previous_launch = read_launch(self._log_dir, task_id)
                if previous_launch and previous_launch['state'] == 'not_started':
                    # The launcher still requires the entire prepared configuration fingerprint.
                    # Keep the originally authorized session identity for a proven pre-exec retry.
                    session_id = previous_launch.get('session_id')
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

            invocation = await self._configure_commands(backend, invocation, commands, freedom, repo_path, network)
            invocation = await approval_runtime.configure(backend, invocation, caller_approvals, freedom, max_turns, caller_write_approvals)
            invocation = self._with_system_guidance(backend, invocation, freedom=freedom, network=network, resume=False, enabled=not workflow_builder)

            planned_enforcement = self._with_commands(backend, approval_runtime.with_approval_enforcement(backend, self._extra_enforcement(backend, backend.enforcement(freedom, network), grants, repo_path), invocation.caller_approval_version, caller_approvals, caller_write_approvals), commands)  # type: ignore[arg-type]
            spawned_by, root_task_id, depth, max_depth, lineage_detected = self._resolve_lineage(
                caller,
                child_enforcement=planned_enforcement,
                child_backend=backend.name,
                child_repo=repo_path,
                caller_write_approvals=caller_write_approvals,
            )
            if continuation_of_task_id is not None:
                source = await _bounded_extra_path_check(read_task_record, self._log_dir, continuation_of_task_id)
                if source is None:
                    raise ValueError("Continuation source unavailable")
                spawned_by, root_task_id, depth, max_depth, lineage_detected = (
                    source.spawned_by, source.root_task_id, source.depth, source.max_depth, source.lineage_detected
                )
            # An explicit group wins; otherwise it is inherited from the caller (never invented for a
            # root task with none given).
            resolved_group = group if group is not None else (caller.record.group if caller else None)

        return await self._spawn(
            invocation,
            extra_path_grants=grants,
            backend=backend,
            prompt=prompt,
            display_prompt=display_prompt,
            workflow_builder=workflow_builder,
            repo_path=repo_path,
            session_id=session_id,
            freedom=freedom,
            max_turns=max_turns,
            background_idle_seconds=background_idle_seconds,
            model=model,
            reasoning_effort=reasoning_effort,
            network=network,
            caller_approvals=caller_approvals,
            caller_write_approvals=caller_write_approvals,
            allowed_commands=commands,
            interaction_owner=interaction_owner,
            spawned_by=spawned_by,
            root_task_id=root_task_id,
            depth=depth,
            max_depth=max_depth,
            group=resolved_group,
            title=title,
            lineage_detected=lineage_detected,
            task_id=task_id,
            auto_continue=auto_continue,
            continuation_of_task_id=continuation_of_task_id,
            chain_root_task_id=chain_root_task_id,
            chain_prompt=chain_prompt,
            message_ids=message_ids,
            planned_enforcement=planned_enforcement.as_dict(),
            **({"scratch_chain_id": store.validate_task_id(scratch_chain_id)} if scratch_chain_id is not None else {}),
            # A continuation takes the source's lineage, so the shared-folder grant is bounded by the
            # actual caller's authority as well as the source's.
            **({"shared_caller": caller.record.task_id} if continuation_of_task_id is not None and caller is not None else {}),
            **({"resource_request": resource_request} if resource_request is not None else {}),
            **({"followup_source_task_id": followup_source_task_id} if followup_source_task_id is not None else {}),
            **({"worktree_request": worktree_plan, "rebuild": {
                "backend": backend.name, "prompt": prompt, "freedom": freedom, "session_id": session_id, "model": model,
                "max_turns": max_turns, "reasoning_effort": reasoning_effort, "network": network, "commands": list(commands),
                "caller_approvals": caller_approvals, "caller_write_approvals": caller_write_approvals,
                "guidance": invocation.system_guidance is not None}} if worktree_plan is not None else {}),
            **({"worktree_info": worktree_inherit} if worktree_inherit else {}),
        )

    def _plan_worktree(self, spec: dict[str, Any], repo_path: Path, freedom: str, caller: Any, task_id: str) -> dict[str, Any]:
        """The worktree plan for a fresh task, or a refusal raised before anything is launched.

        Supervised execution only (the owner creates it), never at `read_only` (creating a worktree
        writes the source repository's Git directory, which a read-only task must not cause), and
        never for a nested dispatch: a confined parent must not be able to make Harflow write its
        `.git` through a child. `caller` is None only when no caller was positively established.
        """
        if self.execution_mode != "supervised":
            raise ValueError("worktrees require supervised execution (a server or harflow-ctl); this in-process registry refuses them")
        if freedom == "read_only":
            raise ValueError("worktree is unavailable at read_only: creating one writes the source repository's Git directory")
        if caller is not None:
            raise ValueError("worktree is unavailable to nested dispatches; start the task from outside another task")
        from . import worktrees
        return worktrees.plan(task_id, repo_path, spec, self._log_dir)

    async def _rebuild_invocation(self, rebuild: dict[str, Any], repo_path: Path) -> Invocation:
        """Build a worktree task's invocation again, now that its working directory exists.

        Everything that reads the repository (command grants, approval configuration) is evaluated
        against the real checkout, not the planned path.
        """
        from . import approval_runtime
        backend = get_backend(rebuild["backend"])
        invocation = backend.build_start_argv(
            rebuild["prompt"], repo=repo_path, freedom=rebuild["freedom"], session_id=rebuild["session_id"],
            model=rebuild["model"], max_turns=rebuild["max_turns"], reasoning_effort=rebuild["reasoning_effort"],
            network=rebuild["network"])
        invocation = await self._configure_commands(backend, invocation, tuple(rebuild["commands"]), rebuild["freedom"], repo_path, rebuild["network"])
        invocation = await approval_runtime.configure(backend, invocation, rebuild["caller_approvals"], rebuild["freedom"],
                                                      rebuild["max_turns"], rebuild["caller_write_approvals"])
        if rebuild.get("guidance"):
            invocation = self._with_system_guidance(backend, invocation, freedom=rebuild["freedom"], network=rebuild["network"], resume=False)
        return invocation

    def _with_system_guidance(self, backend: Backend, invocation: Invocation, *, freedom: str, network: bool | None,
                              resume: bool, enabled: bool = True) -> Invocation:
        """Attach the static scratch note through the backend's measured seam, or leave the invocation alone.

        Never blocks a launch: any failure (a file that cannot be written, a shape the backend refuses) means no
        note, disclosed as `unavailable`. Workflow builders and native children, whose prompts Harflow composes
        or whose transport is its own, never carry it."""
        method = getattr(backend, "with_system_guidance", None)
        if not enabled or method is None or getattr(backend.capabilities, "system_guidance", None) is None or invocation.native_subagent:
            return invocation
        try:
            guided = method(invocation, system_guidance.TEXT, resume=resume,
                            file=lambda: system_guidance.instructions_file(self.state_layout.root))
            if guided is not invocation and guided.system_guidance_root is not None:
                guided = replace(guided, system_guidance_root=str(self.state_layout.root))
            if guided is not invocation:
                backend.assert_safe(guided, freedom, network)
            return guided
        except StateOwnershipError:
            raise
        except Exception:
            log.debug("system guidance is unavailable for this launch", exc_info=True)
            return invocation

    def _parent_carried_guidance(self, parent_task_id: str | None) -> bool:
        """Whether the session being resumed started with the note (delivered, or itself a reuse of it)."""
        if parent_task_id is None:
            return False
        try:
            parent = store.read(self._log_dir, parent_task_id, include_prompt=False)
        except StateOwnershipError:
            raise
        except Exception:
            return False
        state = ((parent.scratch or {}).get("system_guidance") or {}).get("state") if parent is not None else None
        return state in (system_guidance.DELIVERED, system_guidance.REUSED_FROM_SESSION)

    def _resource_request(self, names: list[str] | None, wait_seconds: int | None, inherit: dict[str, Any] | None, caller: Any) -> dict[str, Any] | None:
        """The resource request for a dispatch, or None when it asks for nothing.

        Resources need the supervised registry: its detached owner is what waits, so no lock and no
        caller is blocked on the ledger. An in-process registry refuses rather than block in-process.
        """
        if names is None and not (inherit and inherit.get("names")):
            return None
        if self.execution_mode != "supervised":
            raise ValueError("resources require supervised execution (a server or harflow-ctl); this in-process registry refuses them")
        from . import task_resources
        if names is not None:
            return task_resources.build_request(self._log_dir, names, wait_seconds, caller)
        return task_resources.inherited_request(self._log_dir, inherit, caller)

    @_with_authority_check_budget
    @layout_operation
    async def continue_task(self, task_id: str, extra_prompt: str | None = None, notes_path: str | None = None, *, automatic: bool = False, consume_followups: bool = False, background_idle_seconds: float | None = None, allowed_commands: list[str] | None = None) -> Task:
        """Start a fresh session from a durably settled task without broadening access.

        `consume_followups` is set by the caller-facing paths only; an automatic continuation never
        consumes."""
        from .state_layout import require_dispatch_layout
        require_dispatch_layout(self._log_dir)
        from .task_continuation import continue_task
        return await continue_task(self, task_id, extra_prompt, notes_path, automatic=automatic, consume_followups=consume_followups, background_idle_seconds=background_idle_seconds, allowed_commands=allowed_commands)

    @_with_authority_check_budget
    @layout_operation
    async def resume(
        self,
        parent: Task,
        followup_prompt: str | None,
        *,
        max_turns: int | None = None,
        background_idle_seconds: float | None = None,
        network: bool | None = None,
        task_id: str | None = None,
        display_prompt: str | None = None,
        workflow_builder: bool = False,
        native_subagent: bool = False,
        native_settings: dict[str, Any] | None = None,
        require_unchanged_session: bool = False,
        consume_followups: bool = False,
        allowed_commands: list[str] | None = None,
    ) -> Task:
        """Continue the session; strict inheritance may require no later writers.

        `consume_followups` is set only by the caller-facing paths (`resume_task`, `harflow-ctl
        resume`): the task's queued follow-ups then go to this continuation, ahead of
        `followup_prompt`, which may be omitted only when something is queued. It is a flag of its
        own, never inferred from `task_id`, which keeps marking supervisor-owned resumes.
        """
        from .task_attachment import AttachedTask
        if isinstance(parent, AttachedTask):
            return await self.resume_record(parent.record(), followup_prompt,
                max_turns=max_turns, background_idle_seconds=background_idle_seconds,
                network=network, task_id=task_id, display_prompt=display_prompt,
                workflow_builder=workflow_builder, native_subagent=native_subagent,
                native_settings=native_settings, require_unchanged_session=require_unchanged_session,
                consume_followups=consume_followups, allowed_commands=allowed_commands)
        from .task_supervision import read_task_record
        read_task_record(self._log_dir, parent.task_id, include_prompt=False)
        from .state_layout import require_dispatch_layout
        require_dispatch_layout(self._log_dir)
        if followup_prompt is None and not consume_followups:
            raise ValueError("followup_prompt is required")
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
        from .background_idle import resolve
        inherited_idle = resolve(inherited=parent.background_idle_seconds)
        background_idle_seconds = resolve(background_idle_seconds, inherited_idle)
        if native_subagent and background_idle_seconds != inherited_idle:
            raise ValueError("Native child transports cannot set an independent background idle bound")

        # Caller detection and the nested-dispatch caps run before the session lock — they are
        # read-only and must never block on, or be blocked by, another resume of this session.
        caller = await self._mutation_caller()
        with _before_spawn():
            resource_request = await asyncio.to_thread(self._resource_request, None, None, getattr(parent, "resources", None), caller)
            grants = await self._resolve_extra_paths_async(caller, None, backend, parent)
        if grants and (native_subagent or workflow_builder):
            raise ValueError("Extra writable paths are unavailable to native workflows or workflow builders")
        commands = self._command_grants(backend, allowed_commands, parent.allowed_commands, parent.freedom, Path(parent.repo_path), native=native_subagent, builder=workflow_builder)
        spawned_by, root_task_id, depth, max_depth, lineage_detected = self._resolve_lineage(
            caller,
            child_enforcement=self._with_commands(backend, self._extra_enforcement(backend, backend.enforcement(parent.freedom, effective_network), grants, parent.repo_path), commands),  # type: ignore[arg-type]
            child_backend=backend.name,
            child_repo=parent.repo_path,
            caller_write_approvals=parent.caller_write_approvals,
        )
        resolved_group = caller.record.group if caller is not None else parent.group

        if task_id is None:
            from .workflow_hooks import pause_for_task
            pause_for_task(self._log_dir, parent.task_id, "Task resumed outside the workflow supervisor")

        try:
            async with control.session_lock(
                self._log_dir, parent.session_id, timeout=SESSION_LOCK_TIMEOUT_SECONDS
            ):
                if self.session_has_live_run(parent.session_id):
                    raise SessionBusyError(
                        f"session {parent.session_id} already has a running task, or is held by a takeover in "
                        "the Monitor, or has unverified launch/process liveness; two concurrent runs would corrupt its shared conversation state. Inspect get_task_status supervision and unresolved launch evidence; wait for a live run, restore process-inspection access for an indeterminate run, or close the active takeover before retrying. Do not delete reservations to force resume."
                    )
                if require_unchanged_session:
                    try:
                        successors = await asyncio.to_thread(store.session_successor_task_ids, self._log_dir, parent.task_id, parent.session_id)
                    except ValueError as exc:
                        raise SessionBusyError(f"Selected conversation checkpoint cannot be confirmed unchanged: {exc}") from exc
                    if successors:
                        raise SessionBusyError("Selected conversation checkpoint has successor tasks; strict Resume requires an unchanged session")
                # The retention sweep takes this session lock before it deletes, so a parent still
                # on disk here cannot vanish under the spawn below — and one that landed and is
                # gone means the sweep deleted first, leaving nothing for the child to resume from.
                if parent.persistence_error is not None:
                    raise parent.persistence_error
                if (
                    parent.record_landed
                    and await asyncio.to_thread(store.read, self._log_dir, parent.task_id) is None
                ):
                    raise SessionUnknownError(
                        f"task {parent.task_id}'s record is no longer readable on disk (retention "
                        "removes aged tasks), so its conversation cannot be resumed; start a new "
                        "task instead"
                    )
                return await self._launch_resume(
                    backend=backend,
                    parent_task_id=parent.task_id,
                    session_id=parent.session_id,
                    repo_path=parent.repo_path,
                    freedom=parent.freedom,
                    model=parent.model,
                    reasoning_effort=parent.reasoning_effort,
                    # The resumed task's own title, whoever the caller is — unlike `group`.
                    title=parent.title,
                    followup_prompt=followup_prompt,
                    max_turns=max_turns,
                    background_idle_seconds=background_idle_seconds,
                    effective_network=effective_network,
                    caller_approvals=parent.caller_approvals,
                    caller_write_approvals=parent.caller_write_approvals,
                    allowed_commands=commands,
                    interaction_owner=parent.interaction_owner,
                    grants=grants,
                    native_subagent=native_subagent,
                    native_settings=native_settings,
                    workflow_builder=workflow_builder,
                    display_prompt=display_prompt,
                    spawned_by=spawned_by,
                    root_task_id=root_task_id,
                    depth=depth,
                    max_depth=max_depth,
                    group=resolved_group,
                    lineage_detected=lineage_detected,
                    task_id=task_id,
                    consume_followups=consume_followups,
                    resource_request=resource_request,
                    worktree_info=getattr(parent, "worktree", None),
                    scratch_chain_id=shared_scratch.effective_chain(parent),
                )
        except control.LockTimeout:
            raise SessionBusyError(
                f"another resume of session {parent.session_id} is already in progress; "
                "try again shortly"
            ) from None

    async def _launch_resume(
        self,
        *,
        backend: Backend,
        parent_task_id: str,
        session_id: str,
        repo_path: Path,
        freedom: str,
        model: str | None,
        reasoning_effort: str | None,
        title: str | None,
        followup_prompt: str | None,
        max_turns: int | None,
        effective_network: bool | None,
        caller_approvals: bool,
        caller_write_approvals: bool,
        interaction_owner: str,
        grants: tuple,
        native_subagent: bool,
        native_settings: dict[str, Any] | None,
        workflow_builder: bool,
        display_prompt: str | None,
        spawned_by: str | None,
        root_task_id: str | None,
        depth: int,
        max_depth: int | None,
        group: str | None,
        lineage_detected: str | None,
        task_id: str | None,
        consume_followups: bool,
        background_idle_seconds: float | None = None,
        allowed_commands: tuple[str, ...] = (),
        resource_request: dict[str, Any] | None = None,
        worktree_info: dict[str, Any] | None = None,
        scratch_chain_id: str | None = None,
    ) -> Task:
        """Build and spawn a resume, consuming the parent's queued follow-ups when asked to.

        Called with the session lock held, after every refusal check. The reservation is the last
        step before the build; see `followups` for how a failure after it is resolved.
        """
        attempt: followups.Attempt | None = None
        prompt = followup_prompt
        spawn_id = task_id
        if consume_followups:
            spawn_id = task_id or str(uuid.uuid4())
            attempt = await followups.begin(self._log_dir, parent_task_id, spawn_id, self._owner)
            if attempt.items:
                prompt = attempt.compose(followup_prompt)
            else:
                attempt = None
                if followup_prompt is None:
                    refusal = ValueError(
                        f"task {parent_task_id} has no queued follow-ups, so followup_prompt is "
                        "required"
                    )
                    refusal.harflow_not_started = True  # type: ignore[attr-defined]
                    raise refusal
        assert prompt is not None
        spawn_called = False
        try:
            # Failures before the spawn are flagged as provably not started, so a consumption
            # releases its reservation; ordinary resumes keep their exceptions unmarked.
            with _before_spawn() if attempt is not None else nullcontext():
                invocation = backend.build_resume_argv(
                    prompt,
                    repo=repo_path,
                    freedom=freedom,  # type: ignore[arg-type]
                    session_id=session_id,
                    # Carried over so the continuation runs on the model and effort the run
                    # started with, and so what we report for it stays true.
                    model=model,
                    max_turns=max_turns,
                    reasoning_effort=reasoning_effort,
                    network=effective_network,
                )
                from . import approval_runtime
                approval_runtime.validate(backend, caller_approvals, freedom, max_turns, native_subagent, caller_write_approvals)
                invocation = await self._configure_commands(backend, invocation, allowed_commands, freedom, repo_path, effective_network)
                invocation = await approval_runtime.configure(backend, invocation, caller_approvals, freedom, max_turns, caller_write_approvals)
                if native_subagent:
                    from .backends.native import adapter
                    native = adapter(backend)
                    if native is None:
                        raise ValueError("No certified native adapter")
                    try:
                        invocation = native.configure(invocation, native_settings) if native_settings is not None else native.configure(invocation)
                    except StateOwnershipError:
                        raise
                    except Exception as exc:
                        exc.harflow_not_started = True
                        raise
                invocation = self._with_system_guidance(backend, invocation, freedom=freedom, network=effective_network, resume=True,
                                                        enabled=not (workflow_builder or native_subagent))
            spawn_called = True
            task = await self._spawn(
                invocation,
                extra_path_grants=grants,
                backend=backend,
                prompt=prompt,
                display_prompt=display_prompt,
                workflow_builder=workflow_builder,
                repo_path=repo_path,
                session_id=session_id,
                freedom=freedom,
                max_turns=max_turns,
                background_idle_seconds=background_idle_seconds,
                model=model,
                reasoning_effort=reasoning_effort,
                network=effective_network,
                caller_approvals=caller_approvals,
                caller_write_approvals=caller_write_approvals,
                allowed_commands=allowed_commands,
                interaction_owner=interaction_owner,
                parent_task_id=parent_task_id,
                spawned_by=spawned_by,
                root_task_id=root_task_id,
                depth=depth,
                max_depth=max_depth,
                group=group,
                title=title,
                lineage_detected=lineage_detected,
                task_id=spawn_id,
                message_ids=attempt.ids if attempt is not None else None,
                **({"resource_request": resource_request} if resource_request is not None else {}),
                **({"followup_source_task_id": parent_task_id} if attempt is not None else {}),
                **({"worktree_info": worktree_info} if worktree_info else {}),
                **({"scratch_chain_id": scratch_chain_id} if scratch_chain_id is not None else {}),
            )
        except BaseException as exc:
            if attempt is not None:
                await attempt.fail(self, exc, spawn_called)
            raise
        if attempt is not None:
            from .task_resources import PendingLaunch
            if isinstance(task, PendingLaunch):
                # Queued for resources: the owner finalizes consumption at spawn, and the
                # reservation now belongs to the launch owner.
                await attempt.handed_off(task)
            else:
                await attempt.consumed(self, task)
        return task

    @layout_operation
    async def _spawn(self, invocation: Invocation, **kwargs) -> Task:
        from .disk_space import check_launch_space
        if "worktree_request" not in kwargs:  # a planned path does not exist yet; the owner checks after creating it
            with _before_spawn():
                await asyncio.to_thread(check_launch_space, self._log_dir, kwargs['repo_path'])
        if self.execution_mode == "supervised":
            from .task_supervision import launch
            return await launch(self, invocation, kwargs)
        kwargs.pop("followup_source_task_id", None)
        kwargs.pop("planned_enforcement", None)
        return await self._execute_prepared(invocation, **kwargs)

    async def _execute_prepared(self, invocation: Invocation, **kwargs) -> Task:
        """Run one prepared launch; the shared scratch locks it takes are always released on the way out."""
        hold = shared_scratch.Hold()
        try:
            return await self._execute_prepared_locked(invocation, shared_hold=hold, **kwargs)
        finally:
            hold.release()

    async def _execute_prepared_locked(
        self,
        invocation: Invocation,
        *,
        shared_hold: shared_scratch.Hold,
        scratch_chain_id: str | None = None,
        shared_caller: str | None = None,
        extra_path_grants: tuple = (),
        dispatch_origin: dict[str, Any] | None = None,
        backend: Backend,
        prompt: str,
        repo_path: Path,
        session_id: str | None,
        freedom: str,
        max_turns: int | None,
        background_idle_seconds: float | None = None,
        model: str | None,
        reasoning_effort: str | None,
        network: bool | None = None,
        caller_approvals: bool = False,
        caller_write_approvals: bool = False,
        interaction_owner: str = "caller",
        parent_task_id: str | None = None,
        spawned_by: str | None = None,
        root_task_id: str | None = None,
        depth: int = 0,
        max_depth: int | None = None,
        group: str | None = None,
        title: str | None = None,
        lineage_detected: str | None = None,
        task_id: str | None = None,
        display_prompt: str | None = None,
        workflow_builder: bool = False,
        auto_continue: int = 0,
        continuation_of_task_id: str | None = None,
        chain_root_task_id: str | None = None,
        chain_prompt: str | None = None,
        message_ids: list[str] | None = None,
        allowed_commands: tuple[str, ...] | list[str] = (),
        resource_grant: dict[str, Any] | None = None,
        worktree_info: dict[str, Any] | None = None,
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
        # only ever run with the pipe it was built for (claude without a turn cap, and every
        # antigravity run, is live). Classic runs use DEVNULL or a backend-validated one-shot
        # prompt pipe which closes immediately at EOF, without a live-input pump.
        from .background_idle import resolve
        background_idle_seconds = resolve(background_idle_seconds)
        from .state_layout import require_dispatch_layout
        require_dispatch_layout(self._log_dir)
        with _before_spawn():
            from .disk_space import check_launch_space
            await asyncio.to_thread(check_launch_space, self._log_dir, repo_path)
            from . import approval_runtime
            approval_runtime.validate(backend, caller_approvals, freedom, max_turns, invocation.native_subagent, caller_write_approvals)
            if invocation.caller_approvals != caller_approvals or invocation.caller_write_approvals != caller_write_approvals:
                raise ValueError("Caller approval launch policy does not match invocation")
            approval_version = None
            if caller_approvals:
                approval_version = await asyncio.to_thread(backend.qualify_caller_approvals)
                if approval_version != invocation.caller_approval_version:
                    raise ValueError("Caller approval qualification changed between configuration and launch; retry")
            allowed_commands = tuple(allowed_commands)
            if tuple(invocation.allowed_commands) != allowed_commands:
                raise ValueError("allowed_commands launch policy does not match invocation")
            command_version = None
            if allowed_commands:
                # Re-qualified and re-validated at every launch, resume and continuation; a saved version is never trusted.
                if workflow_builder or invocation.native_subagent:
                    raise ValueError("allowed_commands is unavailable to native workflows and workflow builders")
                backend.validate_allowed_commands(allowed_commands, freedom, repo_path)
                command_version = await asyncio.to_thread(backend.qualify_allowed_commands)
                if command_version != invocation.allowed_commands_version:
                    raise ValueError("allowed_commands qualification changed between configuration and launch; retry")
            if extra_path_grants or invocation.extra_writable_paths:
                # Version probes can block for seconds. Qualify once per execution off loop,
                # even for externally supplied invocations, before allocating scratch; the
                # qualifier asserts the invocation it returns, so no immediate re-assert follows.
                # Final backend validation below also reads filesystem identities off loop.
                await _bounded_extra_path_check(self._assert_extra_state, repo_path, extra_path_grants, invocation)
                if freedom == "unrestricted":
                    if invocation.extra_writable_paths:
                        raise ValueError("Unrestricted extra paths must not configure bounded grants")
                    backend.assert_safe(invocation, freedom, network)
                else:
                    invocation = await _bounded_extra_path_check(backend.qualify_extra_writable_paths, invocation, extra_path_grants, freedom, network)
            else:
                backend.assert_safe(invocation, freedom, network)
            if getattr(backend.capabilities, "tracks_background_tasks", False) and hasattr(backend, "configure_dispatch_context"):
                invocation = backend.configure_dispatch_context(invocation, background_idle_seconds)
                backend.assert_safe(invocation, freedom, network)
            if extra_path_grants and workflow_builder:
                raise ValueError("Extra writable paths are unavailable to workflow builders")
            if invocation.control_pipe:
                from .backends.base import control_pipe_problem
                problem = control_pipe_problem(backend)
                if problem is not None:
                    raise ValueError(problem)

            task_id = store.validate_task_id(task_id) if task_id else str(uuid.uuid4())
            if caller_approvals:
                approval_context = await asyncio.to_thread(approval_runtime.context, self._log_dir, task_id, interaction_owner)
                interaction_owner = approval_context["interaction_owner"]
            if self.get(task_id) is not None or store.read(self._log_dir, task_id) is not None:
                raise ValueError(f"task_id already exists: {task_id}")
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
            if extra_path_grants:
                enforcement = self._extra_enforcement(backend, enforcement, extra_path_grants, repo_path)
            enforcement = approval_runtime.with_approval_enforcement(backend, enforcement, approval_version, caller_approvals, caller_write_approvals)
            enforcement = self._with_commands(backend, enforcement, allowed_commands)

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
            # harflow_mcp imposes rather than about reachability. Sync, so the no-await window below
            # is untouched.
            bridge_notices: list[str] = [publish_notice] if publish_notice else []
            if caller_approvals:
                bridge_notices.append(approval_runtime.notice(backend, caller_write_approvals))
            if network is True and enforcement.network_access == "not_controlled":
                bridge_notices.append(_UNCONTROLLED_NETWORK_NOTICE)

            # A second, independent `to_thread` hop from the branch-disclosure one above: this one is
            # unconditional (every dispatch gets a baseline, regardless of freedom), so it needs its
            # own bounded wait rather than piggybacking on a budget sized for a check some dispatches
            # skip outright. Still strictly before the spawn, so it cannot introduce an await into the
            # no-await window between `create_subprocess_exec` and registration below.
            from .task_outcomes import baseline, empty_baseline, interrupted_baseline
            partial_baseline = empty_baseline()
            try:
                git_baseline = await asyncio.wait_for(
                    asyncio.to_thread(baseline, repo_path, partial_baseline), timeout=GIT_BASELINE_BUDGET_SECONDS + 0.2
                )
                base_commit = git_baseline["head"]
                observed_status = git_baseline["working_tree_status"]
                start_dirty = bool(observed_status) if observed_status is not None else None
            except StateOwnershipError:
                raise
            except Exception as exc:
                git_baseline = interrupted_baseline(partial_baseline,
                    "outer_timeout" if isinstance(exc, TimeoutError) else "worker_error",
                    GIT_BASELINE_BUDGET_SECONDS + 0.2)
                base_commit = git_baseline["head"]
                observed_status = git_baseline["working_tree_status"]
                start_dirty = bool(observed_status) if observed_status is not None else None

            # Allocate after initial probes. Final authority reads may await below; the enclosing
            # cleanup boundary removes scratch on timeout, refusal or cancellation.
            task_scratch = None
            allocation = None
            try:
                scratch_allowed = not (caller_approvals and not getattr(backend, "approval_scratch", lambda: True)())
                task_granted = False
                if freedom != "read_only" and scratch_allowed:
                    from . import scratch
                    from dataclasses import replace
                    task_scratch = scratch.create(self._log_dir, task_id)
                    writable = getattr(backend, "with_writable_directory", None)
                    if writable is not None:
                        if extra_path_grants:
                            invocation = await _bounded_extra_path_check(writable, invocation, task_scratch, freedom)
                        else:
                            invocation = writable(invocation, task_scratch, freedom)
                        backend.assert_safe(invocation, freedom, network)
                        enforcement = replace(enforcement, writable_roots=(*enforcement.writable_roots, str(task_scratch)))
                        task_granted = True
                if not invocation.native_subagent:
                    invocation, enforcement, allocation = await self._allocate_shared_scratch(
                        invocation, backend=backend, freedom=freedom, network=network, enforcement=enforcement,
                        task_id=task_id, chain_id=scratch_chain_id or task_id, group=group, spawned_by=spawned_by,
                        shared_caller=shared_caller, max_depth=max_depth, hold=shared_hold, task_scratch=task_scratch, task_granted=task_granted,
                        scratch_allowed=scratch_allowed, extra_grants=bool(extra_path_grants))
                    for kind in allocation.denied:
                        bridge_notices.append(
                            f"No write access was granted to the shared {kind} scratch folder "
                            f"(${shared_scratch.ENV_CHAIN if kind == 'chain' else shared_scratch.ENV_GROUP}): the task that dispatched this one "
                            "cannot write there itself, so a nested dispatch is not given more. `scratch.access` reports what it has.")
                if extra_path_grants or invocation.extra_writable_paths:
                    await _bounded_extra_path_check(self._assert_extra_state, repo_path, extra_path_grants, invocation)
                    if freedom != "unrestricted":
                        await _bounded_extra_path_check(backend.assert_extra_writable_paths, invocation, extra_path_grants, freedom, network)
            except BaseException:
                shared_hold.release()
                if task_scratch is not None:
                    try:
                        scratch.remove(self._log_dir, task_id)
                    except OSError:
                        log.exception("task %s: could not clean scratch before spawn", task_id)
                raise

            mode = getattr(backend.capabilities, "system_guidance", None)
            guidance_state = system_guidance.state_of(
                mode, delivered=invocation.system_guidance is not None,
                resume=parent_task_id is not None and self._parent_carried_guidance(parent_task_id))

            # So a nested harflow_mcp server started by this agent (a codex/vibe MCP tool call, a
            # claude/opencode subprocess) can find its way back to this task via
            # `lineage.detect_caller`'s HARFLOW_TASK_ID method, when the environment survives that far.
            # Built here, synchronously, so it introduces no await into the no-await window below.
            from .state_layout import child_environment
            spawn_env = {
                **child_environment(self.state_layout),
                **invocation.env,
                lineage.ENV_TASK_ID: task_id,
                lineage.ENV_ROOT_TASK_ID: root_task_id,
                lineage.ENV_DEPTH: str(depth),
                "PB_TASK_ID": task_id,
                "PB_DEPTH": str(depth),
                "HARFLOW_BACKGROUND_IDLE_SECONDS": str(background_idle_seconds),
            }

            for name in shared_scratch.ENV_NAMES:
                spawn_env.pop(name, None)
            if task_scratch is not None:
                spawn_env[shared_scratch.ENV_TASK] = str(task_scratch)
                spawn_env[shared_scratch.ENV_TASK_ALIAS] = str(task_scratch)
            if allocation is not None:
                for name, value in ((shared_scratch.ENV_CHAIN, allocation.chain), (shared_scratch.ENV_GROUP, allocation.group)):
                    if value:
                        spawn_env[name] = value

            # The last look at a cancel request for a launch that may have waited or built a checkout: a
            # synchronous file test, so the no-await window before the spawn is untouched. A request that
            # lands after this is seen by the owner right after the spawn and takes the normal cancel path.
            if self.execution_mode == "supervised":
                from . import task_resources
                if task_resources.cancel_requested(self._log_dir, task_id):
                    shared_hold.release()
                    if task_scratch is not None:
                        try:
                            scratch.remove(self._log_dir, task_id)
                        except OSError:
                            log.exception("task %s: could not clean scratch after a cancel", task_id)
                    raise task_resources.LaunchCancelled("cancelled before the task started")

        try:
            proc = await asyncio.create_subprocess_exec(
                *invocation.argv,
                cwd=str(repo_path),
                stdin=asyncio.subprocess.PIPE if invocation.stdin_mode in ("pipe", "pipe_once") or invocation.control_pipe else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=STREAM_LINE_LIMIT,
                env=spawn_env,
                # Own process group, so cancellation reaches the shell children an agent spawns instead
                # of orphaning them.
                start_new_session=True,
            )
        except OSError as exc:
            # exec failed before returning a process; later failures remain ambiguous.
            exc.harflow_not_started = True  # type: ignore[attr-defined]
            shared_hold.release()
            if task_scratch is not None:
                try:
                    scratch.remove(self._log_dir, task_id)
                except OSError:
                    log.exception("task %s: could not clean scratch after failed exec", task_id)
            raise

        from .extra_paths import serialize_paths
        task = Task(
            task_id=task_id,
            backend=backend.name,
            session_id=session_id,
            repo_path=repo_path,
            prompt=prompt,
            display_prompt=display_prompt,
            workflow_builder=workflow_builder,
            max_turns=max_turns,
            log_path=log_path,
            started_at=_now(),
            model=model,
            reasoning_effort=reasoning_effort,
            parent_task_id=parent_task_id,
            continuation_of_task_id=continuation_of_task_id,
            chain_root_task_id=chain_root_task_id or task_id,
            auto_continue=auto_continue,
            chain_prompt=chain_prompt,
            freedom=freedom,
            network=network,
            caller_approvals=caller_approvals,
            caller_write_approvals=caller_write_approvals,
            caller_approval_version=approval_version,
            allowed_commands=list(allowed_commands),
            allowed_commands_version=command_version,
            resources=dict(resource_grant) if resource_grant else None,
            worktree=dict(worktree_info) if worktree_info else None,
            scratch_chain_id=scratch_chain_id or task_id,
            scratch=(allocation.as_dict() | {"notice": shared_scratch.notice(allocation), "system_guidance": {"state": guidance_state, "version": system_guidance.VERSION}}
                     if allocation is not None and (allocation.task or allocation.chain) else None),
            interaction_owner=interaction_owner,
            extra_writable_paths=serialize_paths(extra_path_grants),
            extra_path_certification=enforcement.extra_path_certification,
            enforcement=enforcement.as_dict(),
            markers=invocation.identity_markers if invocation.identity_markers is not None else _identity_markers(backend, session_id, repo_path),
            proc=proc,
            pgid=proc.pid,
            bridge_notices=bridge_notices,
            owner=self._owner,
            dispatch_origin=dispatch_origin if dispatch_origin is not None else self._owner,
            base_commit=base_commit,
            start_dirty=start_dirty,
            git_baseline=git_baseline,
            spawned_by=spawned_by,
            root_task_id=root_task_id,
            depth=depth,
            max_depth=max_depth,
            group=group,
            title=title,
            lineage_detected=lineage_detected,
            live_input=invocation.live_input,
            control_pipe=invocation.control_pipe,
            background_idle_seconds=background_idle_seconds,
            # A live run owes one result for its initial prompt — plus, on a backend where each
            # written line is its own turn, one more per message the pump writes (see
            # `_note_input_written`). Non-live runs owe nothing: no pump ever runs for them.
            results_owed=1 if invocation.live_input else 0,
        )

        if invocation.control_context is not None:
            # Backend-private settings for the frames its `outgoing_frames` originates.
            task.acc.stream_state["control_context"] = copy.deepcopy(invocation.control_context)
        task.native_observation_generation = getattr(self, '_launch_generations', {}).get(task_id) if invocation.native_subagent else None
        task.native_observation_sequence = 0
        task.native_observation_failed = False

        # Written before anything can go wrong, so even a task whose server dies immediately is
        # discoverable by whichever process comes next.
        try:
            self.persist(task)
            # Durable now: from here a cleaner sees this task's record, so the per-folder locks end.
            shared_hold.release()
            for drain, name in [
                (_drain_stdout, f"harflow-stdout-{task_id}"),
                (_drain_stderr, f"harflow-stderr-{task_id}"),
            ]:
                watcher = owned_job(self.state_layout, drain(task, self), name=name)
                task.watchers.append(watcher)
            task.monitor = owned_job(self.state_layout, _monitor(task, self), name=f"harflow-monitor-{task_id}")
            task.monitor.add_done_callback(lambda done: _close_events_after_writers(task))
        except StateOwnershipError:
            _stop_failed_drainer(task)
            _close_events_after_writers(task)
            raise

        # Registered without awaiting anything in between: an await here could be cancelled (a
        # client disconnecting mid-call) and leave a live agent process no tool can reach.
        self._tasks[task_id] = task
        self.prune()

        # The first event a task's log ever gets, written synchronously right after registration —
        # no await has run yet and the drainer tasks just created cannot have executed, so this is
        # always seq 0. Guarded on its own: a broken event log must not cost the dispatch itself.
        try:
            task.events = EventLog(events_path(self._log_dir, task_id), task_id, layout=self.state_layout)
            _write_event(
                task,
                "task_started",
                {
                    "backend": task.backend,
                    "freedom": task.freedom,
                    "network": task.network,
                    "repo_path": str(task.repo_path),
                    "prompt": task.display_prompt if task.display_prompt is not None else task.prompt,
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
                    # The queued follow-ups this continuation consumed, in queue order.
                    **({"message_ids": list(message_ids)} if message_ids else {}),
                },
            )
        except StateOwnershipError:
            _stop_failed_drainer(task)
            raise
        except Exception:
            log.exception("task %s: could not write task_started event", task_id)

        # Outside the no-await window, and still before any await: the prompt is the first stdin
        # line of a live-input run (a positional would be ignored), written synchronously so it is
        # queued ahead of anything else, then flushed below.
        try:
            if invocation.stdin_mode == "pipe_once":
                self._write_initial_input(task, invocation.initial_input or b"")
                try:
                    await self._flush_stdin(task)
                finally:
                    _close_stdin(task)
            elif invocation.live_input:
                self._write_initial_input(task, invocation.initial_input or b"")
                task.pump = owned_job(self.state_layout, self._pump(task), name=f"harflow-pump-{task_id}")
                await self._flush_stdin(task)
            elif invocation.control_pipe:
                task.pump = owned_job(self.state_layout, self._control_pump(task), name=f"harflow-control-pump-{task_id}")
                await self._flush_stdin(task)
        except StateOwnershipError:
            _stop_failed_drainer(task)
            raise

        # Best-effort start-time capture, so a later server can tell this pid apart from a reused
        # one (see `identity.identity_check`). `except Exception`, not `BaseException`: a
        # `CancelledError` here must propagate — the task is already registered, so nothing is lost
        # by not finishing this step.
        try:
            captured = await asyncio.wait_for(
                asyncio.to_thread(identity.capture, proc.pid, task.markers), timeout=2.0
            )
        except StateOwnershipError:
            _stop_failed_drainer(task)
            raise
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
        except StateOwnershipError:
            _stop_failed_drainer(task)
            raise
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

    async def _allocate_shared_scratch(
        self,
        invocation: Invocation,
        *,
        backend: Backend,
        freedom: str,
        network: bool | None,
        enforcement: Enforcement,
        task_id: str,
        chain_id: str,
        group: str | None,
        spawned_by: str | None,
        shared_caller: str | None = None,
        max_depth: int | None,
        hold: shared_scratch.Hold,
        task_scratch: Path | None,
        task_granted: bool,
        scratch_allowed: bool,
        extra_grants: bool,
    ) -> tuple[Invocation, Enforcement, shared_scratch.Allocation]:
        """Allocate this run's shared chain and group folders and grant the ones it may write.

        Each folder's lock is taken (chain first, then group) before it is created and held in `hold`
        until the task record is durable. The grant goes into the invocation and `writable_roots`
        only where the backend can take one and the dispatcher is allowed to give it; every other
        folder is still allocated and exposed (the environment variables), with the access it really
        has recorded.
        """
        wanted = [("chain", chain_id)] + ([("group", group)] if group else [])
        paths: dict[str, Path] = {}
        created: dict[str, bool] = {}
        for kind, identity in wanted:
            await shared_scratch.lock_async(self._log_dir, kind, identity, hold)
            paths[kind], created[kind] = shared_scratch.allocate_folder(self._log_dir, kind, identity)
        denied: list[str] = []
        with_shared = getattr(backend, "with_shared_directories", None)
        granted: list[Path] = []
        if freedom != "read_only" and scratch_allowed and with_shared is not None:
            for kind, identity in wanted:
                allowed = True
                for who in dict.fromkeys((spawned_by, shared_caller)):
                    if who is not None and allowed:
                        allowed = await asyncio.to_thread(
                            self._caller_allows_shared, kind, paths[kind], identity, task_id, who, max_depth, created[kind])
                if allowed:
                    granted.append(paths[kind])
                else:
                    denied.append(kind)
        if granted:
            if extra_grants:
                invocation = await _bounded_extra_path_check(with_shared, invocation, tuple(granted), freedom)
            else:
                invocation = with_shared(invocation, tuple(granted), freedom)
            backend.assert_safe(invocation, freedom, network)
            enforcement = replace(enforcement, writable_roots=(*enforcement.writable_roots, *(str(path) for path in granted)))
        # Caller approvals let an approved command run outside the sandbox, so the sandbox alone no longer
        # says a folder without a grant is read-only.
        sandboxed = enforcement.os_enforced and enforcement.writes_confined and not enforcement.caller_approval_tools

        def access(is_granted: bool) -> str:
            return shared_scratch.WRITABLE if is_granted else (shared_scratch.SANDBOX_READ_ONLY if sandboxed else shared_scratch.NO_ADDITIONAL_GRANT)

        allocation = shared_scratch.Allocation(
            task=str(task_scratch) if task_scratch is not None else None,
            chain=str(paths["chain"]), group=str(paths["group"]) if "group" in paths else None,
            chain_id=chain_id, group_name=group, denied=denied)
        if task_scratch is not None:
            allocation.access["task"] = access(task_granted)
        allocation.access["chain"] = access(paths["chain"] in granted)
        if "group" in paths:
            allocation.access["group"] = access(paths["group"] in granted)
        return invocation, enforcement, allocation

    def _caller_allows_shared(self, kind: str, path: Path, identity: str, task_id: str, spawned_by: str, max_depth: int | None,
                              created: bool = False) -> bool:
        """Whether a nested dispatch may be granted write access to a shared folder.

        A dispatcher whose writes are not confined has no restriction to preserve. A confined one
        (codex's sandbox) cannot widen into another family's folder: the folder must already be one
        of its own recorded writable roots, or (chain only) be a chain the dispatcher itself began
        (a brand-new chain, or one whose root record descends from it by `spawned_by`, never merely
        by sharing a root task). Anything unreadable denies.
        """
        try:
            caller = store.read(self._log_dir, spawned_by, include_prompt=False)
            if caller is None:
                return False
            recorded = caller.enforcement or {}
            if recorded.get("writes_confined") is not True:
                return True
            if str(path) in (recorded.get("writable_roots") or ()):
                return True
            if kind != "chain":
                return False
            if identity == task_id and created:
                return True  # the child's own brand-new chain; a folder that already existed is someone's
            node: str | None = identity
            for _ in range((max_depth if max_depth is not None else lineage.max_depth_default()) + 1):
                record = store.read(self._log_dir, node, include_prompt=False) if node else None
                if record is None:
                    return False
                if record.spawned_by == caller.task_id:
                    return True
                node = record.spawned_by
            return False
        except StateOwnershipError:
            raise
        except Exception:
            log.debug("shared scratch authority could not be read; denying the grant", exc_info=True)
            return False

    def _maybe_open_monitor(self, task: Task) -> None:
        return  # Native Harflow integration awaits the companion runtime.
        """Open the Monitor app on this task, in the background, for a root task only (A4.3).

        Synchronous — it only schedules — and called as `_spawn`'s last step, after registration. A
        root task is
        one with no detected caller and no `HARFLOW_TASK_ID` in this server's environment: a nested
        dispatch belongs to a tree the user is already watching. Off unless `HARFLOW_OPEN_MONITOR=1`;
        skipped off darwin too, and for a registry built with `open_monitor=False` (`harflow-ctl
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
            job = owned_job(self.state_layout,
                self._open_monitor_job(task), name=f"harflow-open-monitor-{task.task_id}"
            )
            self._monitor_jobs.add(job)
            job.add_done_callback(self._monitor_jobs.discard)
        except StateOwnershipError:
            _stop_failed_drainer(task)
            raise
        except Exception:
            log.debug("task %s: could not schedule opening the Monitor", task.task_id, exc_info=True)

    async def _open_monitor_job(self, task: Task) -> None:
        launcher = self._monitor_launcher or _launch_monitor
        try:
            code = await launcher(MONITOR_URL.format(task_id=task.task_id))
        except StateOwnershipError:
            raise
        except Exception as exc:
            notice = (
                f"The Monitor app could not be opened ({type(exc).__name__}: {exc}); the task "
                "runs regardless."
            )
        else:
            if code == 0:
                return
            notice = (
                f"The Monitor app could not be opened (`open -g -a` failed with status {code}); "
                "the task runs regardless."
            )
        task.bridge_notices.append(notice)
        try:
            self.persist(task)
        except StateOwnershipError:
            raise
        except Exception:
            log.exception("task %s: could not persist the Monitor notice", task.task_id)

    def _write_initial_input(self, task: Task, data: bytes) -> None:
        """Queue a live-input run's prompt on its stdin — synchronous, so nothing can be written
        ahead of it — and log it as the run's first `user_message`."""
        if _write_stdin(task, data):
            _write_event(task, "user_message", {"text": task.display_prompt if task.display_prompt is not None else task.prompt, "source": "initial"})
        else:
            log.warning("task %s: stdin was already gone before the prompt could be written", task.task_id)

    async def _flush_stdin(self, task: Task) -> None:
        """Await the prompt's delivery to the pipe. The input pump, not this, decides when stdin
        closes."""
        await _drain_stdin(task)

    # --- live input: send, pump, close protocol ------------------------------------------------

    @layout_operation
    def workflow_notice(self, task_id: str, text: str) -> None:
        """Supervisor-owned presentation event; never changes the observed task outcome."""
        task = self.get(task_id)
        if task is not None:
            try:
                _write_event(task, "notice", {"text": text})
            except (OSError, ValueError):
                log.warning("Workflow notice unavailable for %s", task_id, exc_info=True)

    @layout_operation
    async def send_message(self, task: Task, text: str) -> dict[str, Any]:
        """Queue `text` for a live-input task this server owns. Returns "queued", never
        "delivered": the pump writes it, and the event log records whether it did.

        Takes the same inbox lock as the close protocol, so a message is either accepted before
        the close (and then forwarded or reported undelivered) or refused after it.
        """
        from .task_attachment import AttachedTask
        if isinstance(task, AttachedTask):
            return await self.send_to_record(task.task_id, text)
        from .task_supervision import read_task_record
        read_task_record(self._log_dir, task.task_id, include_prompt=False)
        from .workflow_hooks import refuse_direct_message
        await asyncio.to_thread(refuse_direct_message, self._log_dir, task.task_id)
        if not task.live_input:
            # No live pipe to write to: the message waits in the follow-up ledger for the next
            # continuation. The inbox, its lock and its close protocol are not involved.
            return await self.queue_followup(task.task_id, task, text)
        if has_extra_path_authority(task):
            caller = await self._mutation_caller()
            await self._resolve_extra_paths_async(caller, None, get_backend(task.backend), task)
        try:
            fd = await inbox.lock_async(self._log_dir, task.task_id)
        except control.LockTimeout:
            raise inbox.SendRefused(
                f"task {task.task_id}'s inbox is locked by another process; try again shortly",
                code="lock_timeout",
            ) from None
        try:
            if task.finished:
                raise inbox.SendRefused(
                    f"task {task.task_id} has settled ({task.status}); continue with resume_task",
                    code="settled",
                )
            if task.inbox_closed or inbox.is_closed(self._log_dir, task.task_id):
                raise inbox.SendRefused(
                    f"task {task.task_id} has {inbox.closed_message(self._log_dir, task.task_id)}", code="closed"
                )
            message = inbox.make_message(text, self._owner)
            inbox.append_locked(self._log_dir, task.task_id, message)
        finally:
            inbox.unlock(fd)
        task.pump_wake.set()
        return inbox.queued_response(task.task_id, message)

    @layout_operation
    async def send_to_record(self, task_id: str, text: str) -> dict[str, Any]:
        """Queue `text` for a live-input task another server owns, via its on-disk inbox."""
        from .task_supervision import read_task_record
        from .workflow_hooks import refuse_direct_message
        await asyncio.to_thread(refuse_direct_message, self._log_dir, task_id)
        record = await asyncio.to_thread(read_task_record, self._log_dir, task_id)
        if record is not None and not record.live_input:
            return await self.queue_followup(task_id, record, text)
        if record is not None and has_extra_path_authority(record):
            caller = await self._mutation_caller()
            await self._resolve_extra_paths_async(caller, None, get_backend(record.backend), record)
        return await asyncio.to_thread(
            inbox.send_to_record, self._log_dir, task_id, text, by=self._owner
        )

    @layout_operation
    async def queue_followup(self, task_id: str, source: Any, text: str) -> dict[str, Any]:
        """Queue `text` for the next continuation of a task without live input. `source` is the
        live `Task` or recovered record, for the extra-path authority check. Returns "queued"
        with `delivery: "next_continuation"` — nothing was started or delivered."""
        if has_extra_path_authority(source):
            caller = await self._mutation_caller()
            await self._resolve_extra_paths_async(caller, None, get_backend(source.backend), source)
        return await asyncio.to_thread(followups.send, self._log_dir, task_id, text, by=self._owner)

    @layout_operation
    async def cancel_queued_followup(self, task_id: str, message_id: str) -> dict[str, Any]:
        """Cancel a still-queued follow-up, under the checks a send takes."""
        from .task_supervision import read_task_record
        from .workflow_hooks import refuse_direct_message
        await asyncio.to_thread(refuse_direct_message, self._log_dir, task_id)
        record = await asyncio.to_thread(read_task_record, self._log_dir, task_id)
        if record is None:
            raise inbox.SendRefused(f"unknown task_id: {task_id}", code="unknown_task")
        if has_extra_path_authority(record):
            caller = await self._mutation_caller()
            await self._resolve_extra_paths_async(caller, None, get_backend(record.backend), record)
        return await asyncio.to_thread(followups.cancel, self._log_dir, task_id, message_id)

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
        try:
            store.read(self._log_dir, task.task_id, include_prompt=False)
        except store.TaskDocumentError:
            self._report_undelivered(task, messages, "task metadata qualification was refused")
            raise
        backend = get_backend(task.backend)
        written = 0
        for index, message in enumerate(messages):
            try:
                data = backend.encode_live_message(message["text"])
            except StateOwnershipError:
                raise
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
                "the run reported an error, so harflow_mcp stopped forwarding messages and closed "
                "its input"
            )
        if task.input_closed:
            return "the run's stdin had closed"
        return None

    def _note_input_written(self, task: Task, written: int) -> None:
        """Messages reached the run: turns are owed for them. Not idle until their results arrive,
        and an earlier success is not the outcome until then — recorded durably for recovery.

        How many turns the writes opened is the backend's own semantics, read from its
        `live_input_message_is_turn` capability rather than its name. claude folds every message
        written mid-turn into the running turn (measured, 2.1.281), so exactly one turn is owed
        however many landed — owed is `result_count + 1`, and a result that already arrived
        covers it. agy runs each written line as its own turn, even mid-turn (measured: two
        queued lines, EOF at the first result, three results), so every message adds one turn to
        wait for, and `input_after_result` records that count of still-owed turns — which is what
        makes `store._resolve`'s `result_count <= input_after_result` mean "a turn is still owed"
        under both shapes."""
        task.acc.awaiting_input = False
        task.acc.turn_open = True
        if get_backend(task.backend).capabilities.live_input_message_is_turn:
            task.results_owed += written
            task.input_after_result = task.results_owed - 1
        else:
            task.input_after_result = task.acc.result_count
            task.results_owed = task.acc.result_count + 1
        try:
            self.persist(task)
        except StateOwnershipError:
            raise
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
            store.read(self._log_dir, task.task_id, include_prompt=False)
            try:
                pending = self._take_pending(task)
            except OSError:
                log.warning("task %s: could not read its inbox; will retry", task.task_id, exc_info=True)
                return False
            written = self._write_messages(task, pending)
        finally:
            inbox.unlock(fd)
        if written:
            self._note_input_written(task, written)
            await _drain_stdin(task)
        return bool(pending)

    def _close_still_warranted(self, task: Task, kind: str, bound: float) -> bool:
        """Re-made under the inbox lock: the idle state the pump saw before waiting for the lock
        may be gone by now (a follow-up turn, a new background task)."""
        if task.caller_approvals and task.approval_pending and kind in {CLOSE_IDLE, CLOSE_IDLE_BOUND}:
            return False
        if kind == CLOSE_IDLE:
            # Every owed result must have arrived, not merely the latest one: on a backend where
            # each written message is its own turn (agy), awaiting_input after the first of
            # several owed results is not idle — closing stdin there would kill the turns the
            # queued messages were owed. On claude owed is result_count + 1 at the last write, so
            # this condition is exactly the old `awaiting_input` one for it.
            return task.acc.awaiting_input and task.acc.result_count >= task.results_owed
        if kind == CLOSE_IDLE_BOUND:
            # No owed-result check here, deliberately: the bound only fires when no turn is
            # running and nothing but background work holds the run open, and a turn owed for a
            # written message has already set turn_open (via `_note_input_written`) and turned
            # this branch off.
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
            if reason is None:
                store.read(self._log_dir, task.task_id, include_prompt=False)
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
                    self._note_input_written(task, wrote)  # turns are owed for those, EOF or not
            else:
                self._report_undelivered(task, pending, reason)
            close_reason = reason or {
                CLOSE_IDLE: "all owed results arrived; stdin closed to allow process settlement",
                CLOSE_IDLE_BOUND: "background work reached the configured idle bound",
            }.get(kind, "input pump stopped")
            try:
                inbox.seal(self._log_dir, task.task_id, close_reason)
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
            inbox.seal(self._log_dir, task.task_id, why)
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
        if task.acc.background_open:
            task.acc.background_abandoned = True
        task.inbox_closed = True
        _close_stdin(task)

    def _mark_abandoned(self, task: Task, bound: float) -> None:
        task.acc.background_abandoned = True
        latest = task.acc.latest_result
        if task.acc.failure_diagnostic is None:
            now = time.monotonic()
            task.acc.failure_diagnostic = {
                "category": "background_abandoned_at_idle_bound",
                "reason": ("Agent result succeeded, but background work remained open at the idle bound"
                           if latest and not latest.get("is_error") and latest.get("subtype") == "success"
                           else "Agent result reported an error; background work remained open at the idle bound"),
                "source": "live_input_pump", "background_idle_seconds": bound,
                "elapsed_background_wait_seconds": round(now - task.last_output_at, 3),
                "background_tasks": [{"id": key, "description": task.acc.background_details.get(key, {}).get("description"),
                    "elapsed_seconds": round(now - task.acc.background_details.get(key, {}).get("started", now), 3)}
                    for key in sorted(task.acc.background_open)[:50]],
                "latest_result": latest,
                "successful_agent_result": bool(latest and not latest.get("is_error") and latest.get("subtype") == "success"),
            }
        notice = _BACKGROUND_ABANDONED_NOTICE.format(bound=bound)
        task.bridge_notices.append(notice)
        _write_event(task, "notice", {"text": notice})

    def _close_unlocked(self, task: Task, why: str) -> None:
        """Last resort after exit, when the protocol cannot run: marker FIRST, then everything
        queued — memory and disk — reported undelivered. Senders re-check the marker after
        appending (`inbox.send_to_record`), so one that appended before the marker existed is read
        here, and one that finds it afterwards is refused: nothing acknowledged goes unreported."""
        try:
            inbox.seal(self._log_dir, task.task_id, why)
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
        bound = task.background_idle_seconds
        try:
            while not task.inbox_closed:
                task.pump_wake.clear()
                acc = task.acc
                reason = self._stop_reason(task)
                kind: str | None = CLOSE_STOP if reason is not None else None
                from . import approval_runtime
                approval_pending = await approval_runtime.pump(self, task)
                if kind is None and approval_pending:
                    try:
                        await asyncio.wait_for(task.pump_wake.wait(), timeout=PUMP_POLL_SECONDS)
                    except (asyncio.TimeoutError, TimeoutError):
                        pass
                    continue
                if kind is None and await self._forward_pending(task):
                    continue
                if kind is None and acc.awaiting_input and acc.result_count >= task.results_owed:
                    # Idle means every owed result has arrived — on a backend where each written
                    # message is its own turn, that is several results, not one (see
                    # `_close_still_warranted` for the same rule made under the lock).
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
        except StateOwnershipError:
            raise
        except Exception:
            log.exception("task %s: input pump failed", task.task_id)
            if task.caller_approvals:
                self._stop_native_transport(task)
            # Bookkeeping must never change an outcome — but an open stdin would keep the run
            # from ever settling, so the input is closed regardless.
            _close_stdin(task)

    def _write_outgoing_frames(self, task: Task, phase: str) -> None:
        """Write the backend's Harflow-originated frames for `phase` through the single ordered stdin writer.

        A frame must be newline-terminated bytes; anything else, or a closed pipe, raises so the control
        pump's failure path stops the run rather than leaving a child waiting on a frame that never came."""
        for frame in get_backend(task.backend).outgoing_frames(phase, task.acc):
            if type(frame) is not bytes or not frame.endswith(b"\n"):
                raise ValueError(f"malformed {phase} frame from backend {task.backend}")
            if not _write_stdin(task, frame):
                raise BrokenPipeError(f"control pipe closed before the {phase} frame was written")

    async def _control_pump(self, task: Task) -> None:
        """Own a control-pipe-only run's stdin: answer approvals and write backend frames, never user messages.

        There is no inbox, no forwarding and no idle close. The initial frames are written first, inside this
        protected lifecycle. The pipe closes when the backend says its control exchange is finished and no
        reserved answer still needs it, when the run must stop (exit, error result, closed stdin,
        cancellation), or on any failure; the approval pump always gets one last pass first so pending rows
        are invalidated promptly. Woken by the stdout drainer after every event, otherwise every
        `PUMP_POLL_SECONDS`."""
        from . import approval_runtime
        backend = get_backend(task.backend)
        finished = False
        try:
            self._write_outgoing_frames(task, "initial")
            await self._flush_stdin(task)
            while True:
                task.pump_wake.clear()
                stopping = task.cancel_requested or self._stop_reason(task) is not None
                pending = await approval_runtime.pump(self, task)
                if stopping:
                    break
                self._write_outgoing_frames(task, "turn")
                if not pending and backend.control_pipe_finished(task.acc):
                    # Tool identities are captured while the server is alive, so a survivor can still be swept
                    # after it exits by itself.
                    task.tool_procs.update(await asyncio.to_thread(_tool_procs, task))
                    finished = True
                    break
                try:
                    await asyncio.wait_for(task.pump_wake.wait(), timeout=PUMP_POLL_SECONDS)
                except (asyncio.TimeoutError, TimeoutError):
                    pass
        except asyncio.CancelledError:
            raise
        except StateOwnershipError:
            raise
        except Exception:
            log.exception("task %s: control pump failed", task.task_id)
            # Non-deliverable first, synchronously: the approval pump refuses to write once stdin is marked closed,
            # so the pass below can only invalidate (sent stays delivery_unknown, reserved/pending are invalidated).
            _close_stdin(task)
            try:
                await approval_runtime.pump(self, task)
            except StateOwnershipError:
                raise
            except Exception:
                log.debug("task %s: final approval pass failed", task.task_id, exc_info=True)
            self._stop_native_transport(task)
        finally:
            _close_stdin(task)
        if finished and not task.cancel_requested:
            await self._control_shutdown(task)

    async def _control_shutdown(self, task: Task) -> None:
        """After terminal evidence stdin is closed: give the server a bounded time to exit by itself, then SIGTERM its
        process group, then sweep the tree. The observed turn outcome stands (`shutdown_after_terminal`), because the
        run was ended by Harflow only after it had finished. Tool commands that survive a server which did exit are
        swept from the identities captured just before stdin closed (verified by pid, group and start time).

        Deviation from R6-8's stop order, recorded here until the docs: a *cancel* signals the group with SIGTERM at
        once (stdin is closed in the same step by the pump), it does not wait on EOF first. Remaining risk: a tool
        group of a server that crashes or is killed before terminal evidence is swept only from the identities
        recorded when a stop began, so commands started after that record, or by a server that died on its own
        first, can outlive it."""
        proc = task.proc
        if proc is None:
            return
        if not await _process_exited(proc, CONTROL_EXIT_GRACE_SECONDS):
            if task.cancel_requested:
                return
            task.acc.stream_state["shutdown_after_terminal"] = True
            task.tool_procs.update(await asyncio.to_thread(_tool_procs, task))
            log.warning("task %s: the app-server outlived its completed turn; terminating it", task.task_id)
            _signal_group(task, signal.SIGTERM)
            await _process_exited(proc, SIGKILL_GRACE_SECONDS)
        if task.tool_procs or proc.returncode is None:
            await asyncio.to_thread(_kill_control_tree, task)

    async def _finish_pump(self, task: Task) -> None:
        """Let the pump run its exit-path close, so undelivered messages are reported before
        `task_finished`. That close is itself bounded (`EXIT_CLOSE_GIVE_UP_SECONDS`), so this waits
        for it; `PUMP_STOP_GRACE_SECONDS` only backstops a pump that is stuck somewhere else.
        Called by `_monitor` after the process exits."""
        if task.caller_approvals:
            from . import approvals
            try:
                await asyncio.to_thread(approvals.invalidate, self._log_dir, task.task_id, "cancelled" if task.cancel_requested else "invalidated", task.approval_generation)
                task.approval_view = await asyncio.to_thread(approvals.summary, self._log_dir, task.task_id)
            except (OSError, ValueError):
                task.bridge_notices.append("Approval ledger invalidation failed; prior answers cannot be reused")
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
            except StateOwnershipError:
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

    @layout_operation
    def persist(self, task: Task) -> None:
        from .task_attachment import AttachedTask
        if isinstance(task, AttachedTask):
            raise TypeError("Cannot persist an attached task; only its supervisor owns metadata")
        task.latest_record_landed = False
        task.persistence_error = None
        from .accounting import snapshot, unavailable
        accounting = None
        try:
            accounting_task_id = store.validate_task_id(task.task_id)
        except Exception:
            # Preserve the persistence path's existing identity validation.
            # Accounting cannot create an identity or change that outcome.
            pass
        else:
            try:
                accounting = snapshot(get_backend(task.backend), task.acc, {
                    "task_id": accounting_task_id, "resumed": bool(task.parent_task_id),
                    "observed": not (task.finished_at and task.exit_code is None),
                    "terminal": task.finished_at is not None,
                })
            except Exception:
                accounting = unavailable(accounting_task_id, "accounting_evidence_error")
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
                background_idle_seconds=task.background_idle_seconds,
                network=task.network,
                caller_approvals=task.caller_approvals,
                caller_write_approvals=task.caller_write_approvals,
                caller_approval_version=task.caller_approval_version,
                allowed_commands=list(task.allowed_commands),
                resources=dict(task.resources) if task.resources else None,
                worktree=dict(task.worktree) if task.worktree else None,
                scratch_chain_id=task.scratch_chain_id,
                scratch=dict(task.scratch) if task.scratch else None,
                interaction_owner=task.interaction_owner,
                approval_generation=task.approval_generation,
                parent_task_id=task.parent_task_id,
                continuation_of_task_id=task.continuation_of_task_id,
                chain_root_task_id=task.chain_root_task_id,
                auto_continue=task.auto_continue,
                output_drained=task.output_drained,
                chain_prompt=task.chain_prompt,
                stderr_tail=list(task.stderr_tail),
                failure_diagnostic=task.acc.failure_diagnostic,
                accounting=accounting,
                workflow_builder=task.workflow_builder,
                prompt=(task.display_prompt if task.display_prompt is not None else task.prompt),
                status=task.status,
                exit_code=task.exit_code,
                finished_at=task.finished_at.isoformat() if task.finished_at else None,
                extra_writable_paths=task.extra_writable_paths,
                extra_path_certification=task.extra_path_certification,
                enforcement=dict(task.enforcement),
                bridge_notices=list(task.bridge_notices),
                start_time=task.start_time,
                owner=dict(task.owner) if task.owner is not None else None,
                dispatch_origin=dict(task.dispatch_origin) if task.dispatch_origin is not None else None,
                base_commit=task.base_commit,
                start_dirty=task.start_dirty,
                git_baseline=task.git_baseline,
                outcome=task.outcome,
                terminal_summary=(task.acc.summary.encode('utf-8')[:4096].decode('utf-8', errors='ignore')
                                  if task.finished_at and isinstance(task.acc.summary, str) else None),
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
        task.latest_record_landed = landed
        task.persistence_error = store.last_write_error()
        if landed:
            task.record_landed = True
        elif task.persistence_error is not None:
            notice = f"Task metadata persistence refused ({task.persistence_error.code}): {task.persistence_error.reason}"
            if notice not in task.bridge_notices:
                task.bridge_notices.append(notice)

    def get(self, task_id: str) -> Task | None:
        return self._tasks.get(task_id) or self._attached.get(task_id)

    @layout_operation
    def attach(self, task_id: str):
        from .task_attachment import AttachedTask
        from .task_supervision import read_task_record
        store.validate_task_id(task_id)
        if task_id in self._tasks:
            return self._tasks[task_id]
        if read_task_record(self._log_dir, task_id) is None:
            return None
        task = self._attached.setdefault(task_id, AttachedTask(self._log_dir, task_id, registry=self))
        self.prune()
        return task

    @layout_operation
    def recover(self, task_id: str) -> store.TaskRecord | None:
        """A task this process did not spawn, read back from disk."""
        return store.read(self._log_dir, task_id)

    @layout_operation
    def recovered_briefs(self, exclude: set[str]) -> list[dict[str, Any]]:
        """Listing entries for on-disk tasks not held in memory."""
        scanned = store.scan(self._log_dir)
        entries = []
        for record in scanned.records:
            if record.task_id in exclude:
                continue
            try:
                entries.append(store.brief(self._log_dir, record))
            except store.TaskDocumentError as exc:
                scanned.refusals[record.task_id] = exc
        entries.extend({"task_id": identifier, "status": "unknown", "history_incomplete": True, "needs_direct_lookup": True, "task_error": exc.as_dict()} for identifier, exc in scanned.refusals.items() if identifier not in exclude)
        return entries

    def list(self, status: str | None = None) -> list[Task]:
        tasks = sorted(self._tasks.values(), key=lambda t: t.started_at)
        if status is None:
            return tasks
        return [t for t in tasks if t.status == status]

    @layout_operation
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
        store.read(self._log_dir, task.task_id, include_prompt=False)
        attempt = control.begin_attempt(self._log_dir, task.task_id, control.CANCEL, self._owner)
        task.cancel_requested = True
        if task.control_pipe:
            # Tool commands run in process groups of their own, and a clean shutdown orphans them: record the
            # groups while their parents are still alive, so the SIGKILL sweep can reach them afterwards.
            task.tool_procs.update(_tool_procs(task, timeout=1.0))
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

    def _record_usage_limit(self, task: Task) -> None:
        """Latch evidence before asynchronous settlement; never cancel away the cause."""
        diagnostic = task.acc.failure_diagnostic
        notice = diagnostic["reason"]
        if "reset_at" in diagnostic:
            notice += " Provider-supplied reset: " + str(diagnostic["reset_at"])
        task.bridge_notices.append(notice)
        self.persist(task)
        task.pump_wake.set()
        if task.usage_settlement is None or task.usage_settlement.done():
            task.usage_settlement = owned_job(self.state_layout, self._settle_usage_limit(task))

    async def _settle_usage_limit(self, task: Task) -> None:
        deadline = time.monotonic() + 5.0
        # The live-input pump owns the inbox close protocol under its flock. It was
        # woken when the evidence was latched; do not race another close against it
        # or cancel its lock acquisition to seal an inbox without ownership. Process
        # settlement has its own deadline even if a sender keeps the inbox locked.
        if task.proc is None:
            return
        try:
            await asyncio.wait_for(asyncio.shield(task.proc.wait()), timeout=max(0.001, deadline - time.monotonic()))
            return
        except asyncio.TimeoutError:
            pass
        # This registry spawned and retains the subprocess handle, just as for
        # local cancellation. Cross-process argv markers can disappear when a CLI
        # retitles itself; they are not the ownership proof for our own child.
        # Recovered tasks use the separate identity-checked control paths.
        _signal_group(task, signal.SIGTERM)
        await _escalate(task)

    def _stop_native_transport(self, task: Task) -> None:
        """Stop an invalid native control turn without waiting on its stdout drainer."""
        try:
            self._deliver_local_cancel(task)
        except StateOwnershipError:
            raise
        except Exception:
            # Failed evidence/control storage must not leave an invalid owner
            # running. Retain the failure and stop the owned process directly.
            task.cancel_requested = True
            _signal_group(task, signal.SIGTERM)
            log.warning("task %s: native transport cancel bookkeeping failed", task.task_id, exc_info=True)
        if task.proc is not None and (task.termination is None or task.termination.done()):
            task.termination = owned_job(self.state_layout, _escalate(task), name=f"harflow-native-terminate-{task.task_id}")

    @layout_operation
    async def cancel(self, task: Task) -> Task:
        """Stop a run, waiting for it to actually die before returning.

        Safe to call concurrently: the termination sequence is owned by the task, so repeat callers
        join the one already in flight rather than starting a second.
        """
        from .task_attachment import AttachedTask
        if isinstance(task, AttachedTask):
            from .task_continuation import stop_chain
            await stop_chain(self, task.task_id)
            await self.cancel_recovered(task.record())
            return task
        store.read(self._log_dir, task.task_id, include_prompt=False)
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
            task.termination = owned_job(self.state_layout,
                _escalate(task), name=f"harflow-terminate-{task.task_id}"
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

    @layout_operation
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
        scanned = store.scan(self._log_dir)
        records = scanned.records
        lineage_rows = [(r.task_id, r.spawned_by, r.root_task_id) for r in records]
        lineage_rows.extend((r.task_id, r.continuation_of_task_id, r.root_task_id) for r in records if r.continuation_of_task_id)
        lineage_rows.extend(local_lineage)
        return _CascadeTargets(lineage.lineage_closure(lineage_rows, task_id), scanned.refusals)

    @layout_operation
    def _triage_target(self, task_id: str) -> tuple[str, Any]:
        """Decide what a non-local cascade target needs. Blocking (`ps`, disk), so run in a thread.

        Returns `("skip", None)` for a target that is already settled or gone,
        `("not_signalled", reason)`, or `("case2" | "case3", record)` by whether its owning server
        is confirmed dead.
        """
        from .task_supervision import read_task_record
        record = read_task_record(self._log_dir, task_id, include_prompt=False)
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

    @layout_operation
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
        from .task_supervision import read_task_record
        fresh = read_task_record(self._log_dir, task_id, include_prompt=False)
        if fresh is None:
            return "gone", None
        record = fresh
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

    @layout_operation
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

    @layout_operation
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
            transfer = assert_owned(self.state_layout).retain()
            gate = threading.Lock()
            acknowledged = threading.Event()
            active = True
            failure: list[BaseException] = []
            def schedule() -> None:
                nonlocal active
                with gate:
                    if not active:
                        return
                    active = False
                    try:
                        with transfer:
                            self._start_phase_retry(task_id, n, phase, payload)
                    except BaseException as exc:
                        failure.append(exc)
                    finally:
                        transfer.close()
                        acknowledged.set()
            try:
                callback = loop.call_soon_threadsafe(schedule)
            except BaseException:
                transfer.close()
                raise
            if not acknowledged.wait(1.0):
                def watch_handoff() -> None:
                    nonlocal active
                    while not acknowledged.wait(0.05):
                        if loop.is_closed():
                            with gate:
                                if active:
                                    active = False
                                    callback.cancel()
                                    transfer.close()
                                    acknowledged.set()
                                    log.error("cancel of %s: event loop closed before phase retry", task_id)
                            return
                    if failure:
                        exc = failure[0]
                        detail = "ownership validation failed" if isinstance(exc, StateOwnershipError) else "scheduling failed"
                        log.error("cancel of %s: phase retry %s", task_id, detail,
                                  exc_info=(type(exc), exc, exc.__traceback__))
                try:
                    watcher = threading.Thread(target=watch_handoff,
                                               name="harflow-phase-handoff", daemon=True)
                    watcher.start()
                except BaseException as exc:
                    with gate:
                        if active:
                            active = False
                            callback.cancel()
                            transfer.close()
                            raise control.PhaseWriteError("cannot monitor pending cancel phase retry") from exc
            if failure:
                raise failure[0]

    @layout_operation
    def _start_phase_retry(
        self, task_id: str, n: int, phase: str, payload: dict[str, Any]
    ) -> None:
        """At most one live retry per (task, attempt, phase): a repeat request reuses the running
        job, and the key is released when that job ends."""
        key = (task_id, n, phase)
        running = self._phase_retries.get(key)
        if running is not None and not running.done():
            return
        job = owned_job(self.state_layout,
            self._retry_phase(task_id, n, phase, payload), name=f"harflow-phase-retry-{task_id}-{n}-{phase}"
        )
        self._phase_retries[key] = job
        self._control_jobs.add(job)

        def _release(done: asyncio.Task[Any]) -> None:
            self._control_jobs.discard(done)
            if self._phase_retries.get(key) is done:
                del self._phase_retries[key]

        job.add_done_callback(_release)

    @layout_operation
    async def settle_phase_writes(self, timeout: float) -> list[str]:
        """Wait up to `timeout` for outstanding cancel phase-write retries (`_retry_phase`), and
        return a description of each still pending.

        For a short-lived controller (`harflow-ctl cancel`/`takeover`): its loop ending would
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
            except StateOwnershipError:
                raise
            except Exception:
                log.warning("cancel of %s: %s retry failed unexpectedly", task_id, phase, exc_info=True)
                delay = min(delay * 2, SIG_RETRY_MAX_SECONDS)

    def _kill_if_ours(self, record: store.TaskRecord) -> bool:
        """SIGKILL the recorded group only if the leader is still provably ours — checked in the
        same worker call as the signal, so no await separates the evidence from the act."""
        from .task_supervision import read_task_record
        fresh = read_task_record(self._log_dir, record.task_id, include_prompt=False)
        if fresh is None:
            return False
        record = fresh
        leader = identity.task_identity(record.pid, record.start_time, record.markers)
        if not identity.signalable(*identity.check_detail(leader)):
            return False
        return _signal_recorded_group(record, signal.SIGKILL)

    @layout_operation
    def _settled_record(self, task_id: str) -> store.TaskRecord | None:
        """The record if its owner has settled it — terminal and observed, or terminal with the
        leader confirmed dead. An unobserved terminal status (a torn-down server's backstop) can
        sit on a live process, and trusting it would skip the SIGKILL that process still needs."""
        from .task_supervision import read_task_record
        record = read_task_record(self._log_dir, task_id, include_prompt=False)
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
            try:
                kind, reason = await asyncio.to_thread(
                    self._deliver_recorded_cancel, record,
                    already_signalled=record.task_id in already_signalled,
                )
            except store.TaskDocumentError as exc:
                outcomes[record.task_id] = ("not_recorded" if record.task_id in already_signalled else "not_signalled", f"{exc.code}: {exc}")
                continue
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
                try:
                    await asyncio.to_thread(self._kill_if_ours, record)
                except store.TaskDocumentError as exc:
                    outcomes[record.task_id] = ("not_recorded", f"Already signalled; further signal refused: {exc.code}: {exc}")
            survivors = await _not_yet_dead(survivors, SIGKILL_GRACE_SECONDS)

        survivor_ids = {record.task_id for record, _leader in survivors}
        for record, _leader in signalled:
            if record.task_id in outcomes:
                continue
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
            except (control.LockTimeout, OSError, store.TaskDocumentError) as exc:
                # The signals have already gone out; raising here would lose the whole cascade
                # result to an unstructured error after an irreversible cancel. The record is left
                # exactly as it was — nothing half-written — and the caller is told it was not
                # published.
                log.warning("cascade: could not record %s as cancelled", record.task_id, exc_info=True)
                reason = f"signalled, but the cancellation could not be recorded: {getattr(exc, 'code', 'write_failed')}: {exc}"
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

    @layout_operation
    async def cancel_cascade(self, task_id: str, *, workflow_control: bool = False) -> dict[str, Any]:
        """Run `_cancel_cascade` as a registry-held task, shielded from the caller.

        Same reason `cancel` shields `_escalate`: the SIGKILL escalation for tasks this server does
        not own lives inside the cascade, and a client disconnecting mid-call must not abandon it.
        """
        from .task_supervision import read_task_record
        read_task_record(self._log_dir, task_id, include_prompt=False)
        if not workflow_control:
            from .workflow_hooks import pause_for_task, refuse_managed, refuse_native_control
            refuse_native_control(self._log_dir, task_id)
            caller = await self._mutation_caller()
            if caller is not None:
                refuse_managed(self._log_dir, caller.record.task_id)
            pause_for_task(self._log_dir, task_id, "Task cancelled outside the workflow supervisor")
        result = await self._shielded(self._cancel_cascade(task_id), f"harflow-cascade-{task_id}")
        from . import task_resources
        # `covered` is what the cascade passes have already processed (None when a pass reported none).
        covered = self._covered_by(result)
        requested: list[str] = []
        handled: set[str] = set()
        for _ in range(CASCADE_MAX_ROUNDS):
            # A pending launch can sit under a recorded intermediate (a task that already has a record),
            # so the walk is seeded with every recorded descendant the cascade covers.
            local_lineage = [(t.task_id, t.spawned_by, t.root_task_id) for t in self._tasks.values()]
            seeds = await asyncio.to_thread(self._cascade_targets, task_id, local_lineage)
            pending = [item for item in await asyncio.to_thread(task_resources.pending_descendants, self._log_dir, task_id, tuple(seeds))
                       if item not in handled]
            # A pending launch that spawns between the recorded snapshot above and the pending scan is in
            # neither: it is no longer pending and was not recorded when the seeds were taken. Convergence is
            # therefore decided from a view taken after the scan, and a launch that became a task at any
            # point before it shows up there as a recorded task the cascade has not covered.
            local_lineage = [(t.task_id, t.spawned_by, t.root_task_id) for t in self._tasks.values()]
            recorded = await asyncio.to_thread(self._cascade_targets, task_id, local_lineage)
            unsignalled = set(recorded) - covered if covered is not None else set()
            if not pending and not unsignalled:
                break
            if pending:
                # Launches still waiting for resources have no process to signal; ask their owners to stop.
                handled.update(pending)
                stopped = await asyncio.to_thread(self._cancel_pending_launches, pending)
                requested.extend(item for item in stopped if item not in requested)
            # A launch can spawn while its request is written, after the owner's last look at it. It is
            # a task by now, so the ordinary cascade (which also reaches what it dispatched) stops it.
            latest = await self._shielded(self._cancel_cascade(task_id), f"harflow-cascade-{task_id}")
            if covered is not None:
                covered |= self._covered_by(latest) or set()
            result = self._merge_cascade_results(result, latest)
        if handled:
            result = {**result, "pending_cancel_requested": requested}
        return {key: value for key, value in result.items() if key != _COVERED_KEY}

    @staticmethod
    def _covered_by(result: dict[str, Any]) -> set[str] | None:
        """The targets a cascade pass processed, or None when the pass did not say."""
        covered = result.get(_COVERED_KEY)
        return set(covered) if covered is not None else None

    @staticmethod
    def _merge_cascade_results(first: dict[str, Any], latest: dict[str, Any]) -> dict[str, Any]:
        """Combine two cascade passes: lists are united (so a `sigkill_survivor` or `not_recorded` entry
        from an earlier pass is kept), the round count is the largest, the incompleteness flags are re-derived from the
        merged evidence, and `unconverged` is the last pass's."""
        merged = dict(first)
        for key, value in latest.items():
            prior = merged.get(key)
            if key == "unconverged" or not isinstance(prior, list) or not isinstance(value, list):
                merged[key] = max(prior, value) if key == "rounds" and isinstance(prior, int) and isinstance(value, int) else value
                continue
            united = prior + [item for item in value if item not in prior]
            merged[key] = sorted(united) if all(isinstance(item, str) for item in united) else united
        if "refused_tasks" in merged:
            merged["history_incomplete"] = bool(merged["refused_tasks"])
            merged["cascade_incomplete"] = bool(merged.get("unconverged")) or bool(merged["refused_tasks"])
        return merged

    def _cancel_pending_launches(self, task_ids: list[str]) -> list[str]:
        from . import task_resources
        return [target for task_id in task_ids for target in task_resources.cancel_launch(self._log_dir, task_id, authorized=True)["cascade"]["pending_cancel_requested"]]

    async def cancel_pending(self, task_id: str) -> dict[str, Any] | None:
        """Cancel a launch that has no record yet, under the authorization `cancel_cascade` applies.

        None when `task_id` is not a pending launch. The checks are the cascade's: native transports
        are controlled through their workflow, the caller must be established (an undecidable or
        unverifiable one is refused, never read as "no caller"), a managed workflow worker is
        refused, and a workflow-associated target pauses its run. Pending descendants are covered by
        the same authorization.
        """
        from . import task_resources
        pending = await asyncio.to_thread(task_resources.pending_receipt, self._log_dir, task_id)
        if pending is None or not task_resources.is_pending_kind(pending):
            return None
        from .workflow_hooks import pause_for_task, refuse_managed, refuse_native_control
        refuse_native_control(self._log_dir, task_id)
        caller = await self._mutation_caller()
        if caller is not None:
            refuse_managed(self._log_dir, caller.record.task_id)
        pause_for_task(self._log_dir, task_id, "Task cancelled outside the workflow supervisor")
        return await asyncio.to_thread(task_resources.cancel_launch, self._log_dir, task_id, authorized=True)

    async def _shielded(self, coro: Any, name: str) -> Any:
        self._loop = asyncio.get_running_loop()
        job = owned_job(self.state_layout, coro, name=name)
        self._control_jobs.add(job)
        job.add_done_callback(self._control_jobs.discard)
        return await asyncio.shield(job)

    async def _cancel_cascade(self, task_id: str) -> dict[str, Any]:
        """Cancel `task_id` and, best-effort, every live descendant this bridge can find via
        lineage — nested dispatches that task's own agent made through harflow_mcp (`spawned_by`),
        or any record reporting the same `root_task_id`. Not a sandboxed guarantee: an agent that
        dispatched outside harflow_mcp entirely is invisible to this.

        Targets are recomputed fresh every round (see `_cascade_targets`), and only the ones not
        already processed are acted on — so this converges to a fixed point rather than looping
        forever on a lineage that keeps growing, bounded at `CASCADE_MAX_ROUNDS` either way.
        """
        refusals: dict[str, store.TaskDocumentError] = {}
        processed: set[str] = set()
        sigkill_survivors: list[str] = []
        owner_still_settling: list[str] = []
        not_signalled: list[dict[str, str]] = []
        not_recorded: list[dict[str, str]] = []
        rounds = 0
        converged = False

        for round_index in range(1, CASCADE_MAX_ROUNDS + 1):
            local_lineage = [(t.task_id, t.spawned_by, t.root_task_id) for t in self._tasks.values()]
            local_lineage.extend((t.task_id, t.continuation_of_task_id, t.root_task_id) for t in self._tasks.values() if t.continuation_of_task_id)
            targets = await asyncio.to_thread(self._cascade_targets, task_id, local_lineage)
            refusals.update(getattr(targets, "refusals", {}))
            new_targets = sorted(targets - processed)
            if not new_targets:
                converged = True
                break
            rounds = round_index
            from .task_continuation import stop_chain
            for target_id in new_targets:
                try:
                    store.read(self._log_dir, target_id, include_prompt=False)
                except store.TaskDocumentError as exc:
                    refusals[target_id] = exc
                    not_signalled.append({"task_id": target_id, "reason": str(exc), "code": exc.code})
                    continue
                await stop_chain(self, target_id)
            processed.update(new_targets)

            local_ids: list[str] = []
            local_coros: list[Any] = []
            case2_ids: list[str] = []
            case2_coros: list[Any] = []
            case3_records: list[store.TaskRecord] = []
            handed_off: set[str] = set()

            for tid in new_targets:
                if tid in refusals:
                    continue
                local_task = self._tasks.get(tid)
                if local_task is not None and not local_task.finished:
                    local_ids.append(tid)
                    local_coros.append(self.cancel(local_task))
                    continue

                try:
                    kind, payload = await asyncio.to_thread(self._triage_target, tid)
                except store.TaskDocumentError as exc:
                    refusals[tid] = exc
                    not_signalled.append({"task_id": tid, "reason": str(exc), "code": exc.code})
                    continue
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
                        if isinstance(outcome, store.TaskDocumentError):
                            refusals[tid] = outcome
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
                        try:
                            fresh = await asyncio.to_thread(store.read, self._log_dir, outcome.task_id)
                        except store.TaskDocumentError as exc:
                            refusals[outcome.task_id] = exc
                            not_recorded.append({"task_id": outcome.task_id, "reason": "Already signalled; metadata refused: " + str(exc), "code": exc.code})
                            continue
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
            local_lineage.extend((t.task_id, t.continuation_of_task_id, t.root_task_id) for t in self._tasks.values() if t.continuation_of_task_id)
            final_targets = await asyncio.to_thread(self._cascade_targets, task_id, local_lineage)
            refusals.update(getattr(final_targets, "refusals", {}))
            unconverged = sorted(final_targets - processed)

        cancelled_descendants: list[str] = []
        for tid in processed:
            if tid == task_id:
                continue
            try:
                fresh = await asyncio.to_thread(store.read, self._log_dir, tid, include_prompt=False)
            except store.TaskDocumentError as exc:
                refusals[tid] = exc
                continue
            local_task = self._tasks.get(tid)
            if local_task is not None:
                status = local_task.status
            else:
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
            "cascade_incomplete": bool(unconverged) or bool(refusals),
            "history_incomplete": bool(refusals),
            "refused_tasks": [exc.as_dict() for exc in refusals.values()],
            "unconverged": unconverged,
            _COVERED_KEY: sorted(processed),
        }

    @layout_operation
    async def cancel_recovered(self, record: store.TaskRecord) -> store.TaskRecord:
        """Shielded like `cancel_cascade`, for the same reason — see `_cancel_recovered`."""
        return await self._shielded(
            self._cancel_recovered(record), f"harflow-cancel-recovered-{record.task_id}"
        )

    async def _cancel_recovered(self, record: store.TaskRecord) -> store.TaskRecord:
        """Stop a task this process never spawned, running the same non-local case-2/case-3 logic
        `cancel_cascade` uses for a single target — no cascade to descendants, just this one
        record. Kept for callers that mean to stop exactly this task. Its old unconditional
        `cancelled` write is gone: the record is only ever closed by the same rules a cascade
        target is — case 3's shared write once the owner is confirmed dead, or the record's own
        owner settling it after case 2's signal.
        """
        from .task_supervision import read_task_record

        kind, payload = await asyncio.to_thread(self._triage_target, record.task_id)
        if kind == "case3":
            await self._cascade_case3_batch([payload])
        elif kind == "case2":
            outcome = await self._cascade_case2(payload)
            if outcome.kind == "handoff":
                fresh = await asyncio.to_thread(read_task_record, self._log_dir, record.task_id, include_prompt=False)
                if fresh is not None:
                    await self._cascade_case3_batch([fresh], frozenset({record.task_id}))
        else:
            log.info(
                "recovered task %s not signalled (%s)", record.task_id, payload or "already settled"
            )

        return read_task_record(self._log_dir, record.task_id) or record

    @_with_authority_check_budget
    @layout_operation
    async def resume_record(
        self,
        record: store.TaskRecord,
        followup_prompt: str | None,
        *,
        max_turns: int | None = None,
        background_idle_seconds: float | None = None,
        network: bool | None = None,
        task_id: str | None = None,
        display_prompt: str | None = None,
        workflow_builder: bool = False,
        native_subagent: bool = False,
        native_settings: dict[str, Any] | None = None,
        require_unchanged_session: bool = False,
        consume_followups: bool = False,
        allowed_commands: list[str] | None = None,
    ) -> Task:
        """Continue the session of a task recovered from disk (see `resume` for the flags)."""
        from .state_layout import require_dispatch_layout
        require_dispatch_layout(self._log_dir)
        if followup_prompt is None and not consume_followups:
            raise ValueError("followup_prompt is required")
        if not record.session_id:
            raise SessionUnknownError(
                f"task {record.task_id} never disclosed a session id, so its conversation cannot "
                "be resumed; start a new task instead"
            )

        # Revalidated rather than trusted: the recorded path may have been deleted, or replaced by
        # a different repository, since the original run.
        repo_path = Path(record.repo_path)
        if not repo_path.is_dir():
            if record.worktree:
                raise RepoUnavailableError(
                    f"the worktree this task ran in no longer exists: {record.repo_path} (it was removed; "
                    f"its branch, if any, is kept in {record.worktree.get('source_repo_path')}). Start a new task instead"
                )
            raise RepoUnavailableError(
                f"the repository this task ran in no longer exists: {record.repo_path}"
            )

        backend = get_backend(record.backend)
        # Same semantics as the live-parent path: None inherits the recorded request — which is
        # also None for a record written before the field existed, resuming at the freedom's
        # historical default — and an explicit boolean overrides it for this subprocess only.
        effective_network = network if network is not None else record.network
        from .background_idle import resolve
        background_idle_seconds = resolve(background_idle_seconds, record.background_idle_seconds)
        if native_subagent and background_idle_seconds != resolve(inherited=record.background_idle_seconds):
            raise ValueError("Native child transports cannot set an independent background idle bound")

        # Same ordering as `resume`: caller detection and the caps run before the session lock.
        caller = await self._mutation_caller()
        with _before_spawn():
            resource_request = await asyncio.to_thread(self._resource_request, None, None, record.resources, caller)
            grants = await self._resolve_extra_paths_async(caller, None, backend, record)
        if grants and (native_subagent or workflow_builder):
            raise ValueError("Extra writable paths are unavailable to native workflows or workflow builders")
        commands = self._command_grants(backend, allowed_commands, record.allowed_commands, record.freedom, repo_path, native=native_subagent, builder=workflow_builder)
        spawned_by, root_task_id, depth, max_depth, lineage_detected = self._resolve_lineage(
            caller,
            child_enforcement=self._with_commands(backend, self._extra_enforcement(backend, backend.enforcement(record.freedom, effective_network), grants, repo_path), commands),  # type: ignore[arg-type]
            child_backend=backend.name,
            child_repo=repo_path,
            caller_write_approvals=record.caller_write_approvals,
        )
        resolved_group = caller.record.group if caller is not None else record.group

        if task_id is None:
            from .workflow_hooks import pause_for_task
            pause_for_task(self._log_dir, record.task_id, "Task resumed outside the workflow supervisor")

        try:
            async with control.session_lock(
                self._log_dir, record.session_id, timeout=SESSION_LOCK_TIMEOUT_SECONDS
            ):
                if self.session_has_live_run(record.session_id):
                    raise SessionBusyError(
                        f"session {record.session_id} already has a running task, or is held by a takeover in "
                        "the Monitor, or has unverified launch/process liveness; two concurrent runs would corrupt its shared conversation state. Inspect get_task_status supervision and unresolved launch evidence; wait for a live run, restore process-inspection access for an indeterminate run, or close the active takeover before retrying. Do not delete reservations to force resume."
                    )
                if require_unchanged_session:
                    try:
                        successors = await asyncio.to_thread(store.session_successor_task_ids, self._log_dir, record.task_id, record.session_id)
                    except ValueError as exc:
                        raise SessionBusyError(f"Selected conversation checkpoint cannot be confirmed unchanged: {exc}") from exc
                    if successors:
                        raise SessionBusyError("Selected conversation checkpoint has successor tasks; strict Resume requires an unchanged session")
                # Same revalidation as `resume`: the sweep holds this lock while deleting, so a
                # parent still readable here survives the spawn below — and one already gone was
                # deleted first, leaving nothing for the child to resume from.
                if await asyncio.to_thread(store.read, self._log_dir, record.task_id) is None:
                    raise SessionUnknownError(
                        f"task {record.task_id}'s record is no longer readable on disk (retention "
                        "removes aged tasks), so its conversation cannot be resumed; start a new "
                        "task instead"
                    )
                return await self._launch_resume(
                    backend=backend,
                    parent_task_id=record.task_id,
                    session_id=record.session_id,
                    repo_path=repo_path,
                    freedom=record.freedom,
                    model=record.model,
                    reasoning_effort=record.reasoning_effort,
                    # The resumed task's own title, whoever the caller is — unlike `group`.
                    title=record.title,
                    followup_prompt=followup_prompt,
                    max_turns=max_turns,
                    background_idle_seconds=background_idle_seconds,
                    effective_network=effective_network,
                    caller_approvals=record.caller_approvals,
                    caller_write_approvals=record.caller_write_approvals,
                    allowed_commands=commands,
                    interaction_owner=record.interaction_owner,
                    grants=grants,
                    native_subagent=native_subagent,
                    native_settings=native_settings,
                    workflow_builder=workflow_builder,
                    display_prompt=display_prompt,
                    spawned_by=spawned_by,
                    root_task_id=root_task_id,
                    depth=depth,
                    max_depth=max_depth,
                    group=resolved_group,
                    lineage_detected=lineage_detected,
                    task_id=task_id,
                    consume_followups=consume_followups,
                    resource_request=resource_request,
                    worktree_info=record.worktree,
                    scratch_chain_id=shared_scratch.effective_chain(record),
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
        overflow = len(self._tasks) + len(self._attached) - self._max_tasks
        if overflow <= 0:
            return
        candidates = [(task.started_at, task.task_id, self._tasks) for task in self._tasks.values()
                      if task.finished and task.task_id not in getattr(self, '_continuation_sources', set())]
        from .task_supervision import read_task_record
        for identifier in tuple(self._attached):
            try:
                record = read_task_record(self._log_dir, identifier)
            except (store.TaskDocumentError, OSError):
                continue  # Unknown settlement must never evict a live or uncertain handle.
            if (record is not None and record.status in store.TERMINAL_RECORD_STATUSES
                    and record.exit_code is not None and record.finished_at):
                candidates.append((datetime.fromisoformat(record.started_at), identifier, self._attached))
        for _started, identifier, collection in sorted(candidates, key=lambda item: (item[0], item[1]))[:overflow]:
            collection.pop(identifier, None)
            log.debug('evicted finished task %s', identifier)

    @layout_operation
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

        task = owned_job(self.state_layout,
            run_owned_worker(self.state_layout, _maintain, self._log_dir), name="harflow-maintenance"
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
    "no output, for {bound:.0f}s (HARFLOW_LIVE_IDLE_SECONDS), so harflow_mcp closed its input anyway — "
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
    assert_task_owned(task.log_path.parent)
    if task.events is None:
        return
    try:
        if task.workflow_builder and ((kind == "user_message" and fields.get("source") == "initial") or (kind == "task_started" and not task.live_input)):
            from .workflows import builder_feedback_ids
            try:
                fields = {**fields, "message_ids": builder_feedback_ids(task.log_path.parent, task.task_id)}
            except StateOwnershipError:
                raise
            except Exception:
                log.debug("task %s: builder feedback association unavailable", task.task_id, exc_info=True)
        task.events.write(kind, fields)
    except StateOwnershipError:
        raise
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


def _process_table(timeout: float = 5.0) -> dict[int, tuple[int, int, str]]:
    """pid -> (ppid, pgid, start time) for every process; empty when `ps` cannot say within `timeout`."""
    try:
        out = subprocess.run(["ps", "-axo", "pid=,ppid=,pgid=,lstart="], capture_output=True, text=True, timeout=timeout, check=False, env={**os.environ, "LC_ALL": "C"}).stdout
    except (OSError, subprocess.SubprocessError):
        return {}
    table = {}
    for line in out.splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4 and all(part.isdigit() for part in parts[:3]):
            table[int(parts[0])] = (int(parts[1]), int(parts[2]), " ".join(parts[3].split()))
    return table


def _descendants(root: int, table: dict[int, tuple[int, int, str]]) -> set[int]:
    found: set[int] = set()
    frontier = {root}
    while frontier:
        frontier = {pid for pid, (ppid, _pgid, _start) in table.items() if ppid in frontier and pid not in found and pid != root}
        found |= frontier
    return found


def _tool_procs(task: Task, timeout: float = 5.0) -> dict[int, tuple[int, str]]:
    """Identities (pid -> (group, start time)) of every descendant of a live control-pipe run outside the run's own group."""
    if task.proc is None or task.proc.returncode is not None:
        return {}
    table = _process_table(timeout)
    return {pid: (table[pid][1], table[pid][2]) for pid in _descendants(task.proc.pid, table) if pid in table and table[pid][1] != task.pgid}


def _kill_pid(pid: int, sig: int) -> None:
    try:
        os.kill(pid, sig)
    except (ProcessLookupError, PermissionError):
        pass


def _kill_group(pgid: int, sig: int = signal.SIGKILL) -> None:
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError):
        pass


async def _process_exited(proc: asyncio.subprocess.Process, timeout: float) -> bool:
    """Whether the process itself exited within `timeout`. Polls `returncode`: `Process.wait()` also waits for the
    pipes to close, which a surviving tool process that inherited stdout would hold open indefinitely."""
    deadline = time.monotonic() + timeout
    while proc.returncode is None:
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(0.05)
    return True


def _verified_groups(recorded: dict[int, tuple[int, str]], table: dict[int, tuple[int, int, str]], exclude: int | None) -> set[int]:
    """Recorded groups that still have a member whose pid, group and start time match what was recorded.

    A pid or group id the system has since reused belongs to an unrelated process and never matches, so a group is
    only ever signalled while at least one member proves it is still the group that was recorded."""
    return {group for pid, (group, start) in recorded.items()
            if group != exclude and pid in table and table[pid][1] == group and table[pid][2] == start}


def _kill_control_tree(task: Task) -> None:
    """SIGKILL a control-pipe run and every tool process group beneath it.

    Tool commands run in process groups of their own, so killing only the run's group would orphan them
    (measured). While the leader is alive: SIGSTOP it and its direct children (the native process is the run's
    root or the wrapper's child), then enumerate descendants and kill their groups, re-enumerating so a group
    created meanwhile is caught, then kill the run's group and sweep once more. Recorded groups are signalled only
    while a recorded member still matches its pid, group and start time (in both the live and the reaped case, and
    in the final sweep), so a reused pid or process group is never signalled. Once the leader has been reaped its
    descendants cannot be walked and only that verified, recorded set is swept."""
    recorded = dict(task.tool_procs)
    alive = task.proc is not None and task.proc.returncode is None
    if alive:
        root = task.proc.pid
        table = _process_table()
        for pid in [root, *sorted(p for p in _descendants(root, table) if table[p][0] == root)]:
            _kill_pid(pid, signal.SIGSTOP)
        groups = _verified_groups(recorded, table, task.pgid)
        killed: set[int] = set()
        for _round in range(3):
            table = _process_table() or table
            tree = _descendants(root, table)
            for pid in tree:
                if pid in table and table[pid][1] != task.pgid:
                    recorded[pid] = (table[pid][1], table[pid][2])   # a live descendant: identity established by ancestry
            groups |= {table[pid][1] for pid in tree if pid in table and table[pid][1] != task.pgid}
            fresh = groups - killed
            if not fresh and _round:
                break
            for group in sorted(fresh):
                _kill_group(group)
            killed |= fresh
            for pid in sorted(tree):
                _kill_pid(pid, signal.SIGKILL)
        _signal_group(task, signal.SIGKILL)
        for group in sorted(_verified_groups(recorded, _process_table(), task.pgid) & groups):
            _kill_group(group)
        return
    for group in sorted(_verified_groups(recorded, _process_table(), task.pgid)):
        _kill_group(group)
    _signal_group(task, signal.SIGKILL)


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
    if task.control_pipe:
        await asyncio.to_thread(_kill_control_tree, task)
    else:
        _signal_group(task, signal.SIGKILL)


def _stop_failed_drainer(task: Task) -> None:
    """Stop a still-live process owned here after its reader loses state authority."""
    task.drain_failed = True
    process = task.proc
    if (task._process_owner_pid == os.getpid() and process is not None
            and process.returncode is None and task.pgid == process.pid):
        if task.control_pipe:
            _kill_control_tree(task)
        else:
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
    descriptor = None
    try:
        assert_owned(registry.state_layout)
        try:
            # Binary, not text mode: `raw_offset` below is `handle.tell()`, which counts bytes, and a
            # text-mode handle's tell() is not a reliable byte offset. `text` is still encoded with the
            # same `errors="replace"` decoding used for everything else here, so the file on disk stays
            # valid UTF-8 and `replay_log` reads it exactly as before.
            descriptor = open_descriptor(task.log_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600, layout=registry.state_layout)
            handle = os.fdopen(descriptor, "ab", closefd=False)
        except OSError:
            log.warning("task %s: cannot open %s; continuing without a raw log", task.task_id, task.log_path)
            task.bridge_notices.append("Raw output capture unavailable: disk or filesystem write failed; the agent output is still being drained.")

        # Set once this drain sees the stream report a session other than the one a resume asked
        # for, so the bridge notice below is appended exactly once per run however many events
        # carry the different id.
        session_swap_noticed = False
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
            control_event = parse_line(text.rstrip("\n"))
            if task.caller_approvals and isinstance(control_event, dict) and get_backend(task.backend).approval_frame_kind(control_event) == "request":
                from . import approval_runtime
                await approval_runtime.observe(registry, task, get_backend(task.backend), control_event)
                task.last_output_at = time.monotonic()
                continue
            raw_offset: int | None = None
            if handle is not None:
                try:
                    descriptor.lease.validate()
                    handle.write(text.encode("utf-8"))
                    handle.flush()
                    raw_offset = handle.tell()
                except OSError:
                    log.warning("task %s: raw log write failed; dropping it", task.task_id)
                    task.bridge_notices.append("Raw output capture stopped after a disk or filesystem write failure; later output is still being drained but may not be recoverable.")
                    try:
                        handle.close()
                    except OSError:
                        pass  # Buffered close may repeat ENOSPC; draining must continue.
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
            # Native lifecycle persistence is authoritative and intentionally
            # precedes best-effort normalization. Failure invalidates native
            # settlement and stops the owning transport. Draining continues so
            # termination cannot deadlock against a full stdout pipe.
            if getattr(task, 'native_observation_generation', None) and not task.native_observation_failed:
                try:
                    from .task_observations import append
                    append(registry.log_dir, task.task_id, task.native_observation_generation,
                           task.native_observation_sequence + 1, event)
                    task.native_observation_sequence += 1
                except StateOwnershipError:
                    raise
                except Exception:
                    task.native_observation_failed = True
                    registry._stop_native_transport(task)
                    log.warning('Native durable observation failed for %s', task.task_id, exc_info=True)
            observer = getattr(registry, "_workflow_native_observers", {}).get(task.task_id)
            if observer is not None:
                try:
                    observer(event)
                except StateOwnershipError:
                    raise
                except Exception as exc:
                    failures = getattr(registry, "_workflow_native_failures", None)
                    if failures is None:
                        failures = registry._workflow_native_failures = {}
                    failures[task.task_id] = str(exc)
                    registry._workflow_native_observers.pop(task.task_id, None)
                    registry._stop_native_transport(task)
                    log.warning("Native lifecycle observation failed for %s; transport stop requested and draining continues", task.task_id, exc_info=True)
            backend = get_backend(task.backend)
            if task.caller_approvals or task.allowed_commands:
                from . import approval_runtime
                await approval_runtime.observe(registry, task, backend, event)
            previous_diagnostic = task.acc.failure_diagnostic
            backend.ingest(event, task.acc)
            if task.freedom == 'read_only':
                try:
                    if task.plan_observations is None:
                        from .plan_observations import PlanObservations
                        task.plan_observations = PlanObservations(backend, task.repo_path, registry.log_dir,
                                                                task.task_id, task.bridge_notices.append)
                    task.plan_observations.enqueue(event)
                except StateOwnershipError:
                    raise
                except Exception:
                    # Plan observation is bookkeeping; never change the agent's outcome.
                    log.warning('task %s: built-in plan artifact observation unavailable', task.task_id, exc_info=True)
                    if 'Built-in plan artifact tracking is unavailable; inspect the raw stream for successful plan writes.' not in task.bridge_notices:
                        task.bridge_notices.append('Built-in plan artifact tracking is unavailable; inspect the raw stream for successful plan writes.')
            if task.acc.failure_diagnostic is not None and previous_diagnostic is None:
                registry._record_usage_limit(task)
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
            elif (
                not session_swap_noticed
                and task.session_id is not None
                and task.acc.session_id
                and task.acc.session_id != task.session_id
            ):
                # A resume that lands on a different session: some CLIs do not refuse an unknown
                # id, they silently start a new conversation (agy does — measured, exit 0, a
                # different conversation_id). The monitor below still trusts the stream and
                # re-points the task at the id the agent actually used, but the caller must be
                # told the run is not a continuation of the conversation they asked for.
                session_swap_noticed = True
                task.bridge_notices.append(
                    f"the agent reported session {task.acc.session_id} instead of the requested "
                    f"{task.session_id}: the CLI did not continue that conversation, so this run "
                    "is not a continuation of it"
                )
                _write_event(
                    task,
                    "notice",
                    {"text": task.bridge_notices[-1]},
                )
                # Re-pointed now, not only at exit: a server restart before then would otherwise
                # leave the record naming a conversation that was never continued, and the next
                # resume would ask for it again. The identity markers were fixed at spawn and do
                # not move with it.
                task.session_id = task.acc.session_id
                registry.persist(task)
    except asyncio.CancelledError:
        raise
    except StateOwnershipError:
        _stop_failed_drainer(task)
        raise
    except Exception:
        log.exception("task %s: stdout drainer failed", task.task_id)
        task.drain_failed = True
        # Nobody is reading the pipe any more, so the run would hang forever. End it instead.
        _signal_group(task, signal.SIGKILL)
    finally:
        try:
            if handle is not None:
                try:
                    handle.close()
                except OSError:
                    log.warning("task %s: raw log close failed", task.task_id)
        finally:
            if descriptor is not None:
                descriptor.close()


def _record_events(task: Task, backend: Backend, event: dict[str, Any], raw_offset: int | None) -> None:
    """Normalize one raw stream event and append the results to the task's event log.

    Guarded on its own, separately from `ingest` above: a broken normalizer must never touch
    `task.status` or `task.drain_failed` — those describe the run itself, which the raw stream and
    `ingest` have already faithfully advanced regardless of what this does. Written after the raw
    line it derives from, never before, so `raw_offset` always points at bytes already on disk.
    """
    assert_task_owned(task.log_path.parent)
    if task.events is None:
        return
    try:
        normalized = backend.normalize(event, task.acc)  # type: ignore[attr-defined]
    except StateOwnershipError:
        raise
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
        if kind == "user_message" and task.display_prompt is not None and fields.get("text") == task.prompt:
            fields["text"] = task.display_prompt
        source_ts = fields.pop("source_ts", None)
        try:
            from .task_outcomes import observe
            root = (task.git_baseline or {}).get("repository_root")
            observe(task.outcome_evidence, kind, fields, task.repo_path, Path(root) if root else None)
        except StateOwnershipError:
            raise
        except Exception:
            log.debug("task %s: outcome evidence unavailable", task.task_id, exc_info=True)
        try:
            task.events.write(kind, fields, raw_offset=raw_offset, source_ts=source_ts)
        except StateOwnershipError:
            raise
        except Exception:
            task.acc.normalize_errors += 1
            log.debug("task %s: could not write a normalized event", task.task_id, exc_info=True)


async def _drain_stderr(task: Task, registry: TaskRegistry | None = None) -> None:
    assert task.proc is not None and task.proc.stderr is not None
    reader = task.proc.stderr
    try:
        assert_owned(registry.state_layout if registry is not None else task.state_layout)
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
                try:
                    getattr(get_backend(task.backend), "observe_stderr", lambda _acc, _line: None)(task.acc, line)
                except Exception:
                    # Stderr evidence is bookkeeping: it must never end the drain, which an unread pipe would turn into a hang.
                    log.debug("task %s: stderr observer failed", task.task_id, exc_info=True)
                parser = getattr(get_backend(task.backend), "stderr_usage_limit_diagnostic", None)
                diagnostic = parser(line) if parser is not None else None
                if diagnostic is not None and task.acc.failure_diagnostic is None:
                    task.acc.failure_diagnostic = diagnostic
                    task.acc.error_result_seen = True
                    if registry is not None:
                        registry._record_usage_limit(task)
    except asyncio.CancelledError:
        raise
    except StateOwnershipError:
        _stop_failed_drainer(task)
        raise
    except Exception:
        log.exception("task %s: stderr drainer failed", task.task_id)
        task.drain_failed = True
        _signal_group(task, signal.SIGKILL)


async def _finish_draining(task: Task) -> bool:
    """Read whatever is left of the pipes, but never wait on them forever.

    `proc.wait()` returns when `claude` exits, which does not mean its pipes are closed: a
    grandchild that inherited stdout keeps the write end open, so the drainers would never see EOF
    and the run could never be marked finished. Give them a grace period, then cut them loose.
    """
    if not task.watchers:
        return True
    _, pending = await asyncio.wait(task.watchers, timeout=DRAIN_GRACE_SECONDS)
    if not pending:
        return not task.drain_failed and all(not w.cancelled() and w.exception() is None for w in task.watchers)

    log.warning(
        "task %s: output still open %.0fs after exit (a background child likely inherited it); "
        "abandoning the rest of the stream",
        task.task_id,
        DRAIN_GRACE_SECONDS,
    )
    for watcher in pending:
        watcher.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    return False


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
        except StateOwnershipError:
            raise
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
    assert_owned(registry.state_layout)
    assert task.proc is not None
    abandoned = False
    try:
        exit_code = await task.proc.wait()
        drained = await _finish_draining(task)
        await registry._finish_pump(task)
        if task.plan_observations is not None:
            await task.plan_observations.finish()
        pump_finished = task.pump is None or (
            task.pump.done() and not task.pump.cancelled() and task.pump.exception() is None
        )
        task.output_drained = drained and not task.drain_failed and pump_finished and (
            not task.live_input or task.inbox_closed and task.inbox_reconciled
        )

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
        except StateOwnershipError:
            raise
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
    except StateOwnershipError:
        raise
    except Exception:
        log.exception("task %s: monitor failed", task.task_id)
        task.finished_at = _now()
        task.status = "cancelled" if task.cancel_requested else "failed"
    finally:
        try:
            # Skipped when abandoned, which is the one case where a task is left non-terminal: `done`
            # alongside a non-terminal status is only unobservable because the loop that could observe
            # it is the one being torn down.
            if not abandoned:
                if task.status not in TERMINAL_STATUSES:
                    task.status = "cancelled" if task.cancel_requested else "failed"
                    task.finished_at = task.finished_at or _now()
                if task.exit_code is not None:
                    try:
                        from .task_outcomes import capture
                        task.outcome = await asyncio.to_thread(capture, task.repo_path, task.git_baseline, task.outcome_evidence)
                        task.outcome["structured_evidence_complete"] = task.output_drained and not task.acc.normalize_errors
                    except StateOwnershipError:
                        task.done.set()
                        raise
                    except Exception:
                        log.warning("task %s: outcome inspection unavailable", task.task_id, exc_info=True)
                if task.caller_approvals and task.exit_code is not None and task.output_drained:
                    task.approval_view = {"pending_count": 0, "awaiting_decision_count": 0, "delivery_uncertain_count": 0, "interaction_state": None, "requests": [], "uncertainty_requests": []}
                task.done.set()
                try:
                    if getattr(task, 'native_observation_generation', None):
                        if task.native_observation_failed:
                            task.output_drained = False
                        else:
                            try:
                                from .task_observations import finish
                                finish(registry.log_dir, task.task_id, task.native_observation_generation,
                                       task.native_observation_sequence)
                            except Exception:
                                task.output_drained = False
                                log.warning('Native final observation barrier failed for %s', task.task_id, exc_info=True)
                    _release_task_resources(registry, task)
                    _touch_shared_scratch(registry, task)
                    launch_generation = getattr(registry, '_launch_generations', {}).get(task.task_id)
                    if launch_generation and task.auto_continue and task.output_drained:
                        from .task_supervision import mark
                        mark(registry.log_dir, task.task_id, launch_generation,
                             continuation_scheduling='pending')
                    registry.persist(task)
                    if task.auto_continue and task.output_drained and task.latest_record_landed:
                        from .task_continuation import schedule_automatic
                        schedule_automatic(registry, task)
                    else:
                        registry.prune()
                except StateOwnershipError:
                    raise
                except Exception:  # pragma: no cover - bookkeeping must never mask a finished run
                    log.exception("task %s: recording the finished run failed", task.task_id)
                # Guarded separately from the persist/prune above: a broken event log must not affect
                # whether the run's terminal status and record landed, which is already done by here.
                try:
                    if task.events is not None:
                        task.events.write(
                            "task_finished",
                            (lambda base: {**base, **_finished_denials(task, base)})({
                                "status": task.status,
                                "exit_code": task.exit_code,
                                "summary": task.acc.summary,
                                "observed": task.exit_code is not None,
                            }),
                        )
                except StateOwnershipError:
                    raise
                except Exception:
                    log.exception("task %s: could not write the task_finished event", task.task_id)
        finally:
            _close_events_after_writers(task)


def _maintain(log_dir: Path) -> Any:
    """The server's background pass: the retention sweep, then the resource reaper.

    The reaper only ever removes entries whose owner is provably settled or never started, and
    waiting launches run it themselves, so a failure here is logged and never changes the sweep.
    """
    swept = retention.maybe_sweep(log_dir)
    try:
        from . import task_resources
        task_resources.reap(log_dir)
    except StateOwnershipError:
        raise
    except Exception:
        log.debug("resource reaper pass failed", exc_info=True)
    return swept


def _touch_shared_scratch(registry: "TaskRegistry", task: Task) -> None:
    """Record the settlement as the shared folders' last use, so the age policy counts from it.

    Guarded: bookkeeping never changes an outcome (`shared_scratch.touch` swallows its own failures)."""
    if task.exit_code is None or not task.scratch:
        return
    try:
        # One non-blocking try: this runs on the event loop, and `finished_at` already covers an observed exit.
        shared_scratch.touch(registry.log_dir, "chain", shared_scratch.effective_chain(task), timeout=0.0)
        if task.group:
            shared_scratch.touch(registry.log_dir, "group", task.group, timeout=0.0)
    except StateOwnershipError:
        raise
    except Exception:  # pragma: no cover - bookkeeping must never mask a finished run
        log.debug("task %s: could not record shared scratch use", task.task_id, exc_info=True)


def _release_task_resources(registry: "TaskRegistry", task: Task) -> None:
    """Free the task's named resources on the terminal-publish path, once its exit was observed.

    Never reached from the `abandoned`/`CancelledError` path (invariant 1): a server being torn
    down does not know the agent is gone, and the reaper decides that later. A run whose exit was
    never observed keeps its hold for the same reason. Guarded: bookkeeping never changes an outcome.
    """
    held = task.resources
    if not held or held.get("state") != "held" or task.exit_code is None:
        return
    try:
        from .task_resources import release_quietly
        if release_quietly(registry.log_dir, held.get("reservation_id")):
            task.resources = {**held, "state": "released", "released_at": _now().isoformat()}
    except StateOwnershipError:
        raise
    except Exception:  # pragma: no cover - bookkeeping must never mask a finished run
        log.exception("task %s: releasing its resources failed", task.task_id)


# `task_finished` carries a bounded projection (never tool input), so the event stays within its size budget.
FINISHED_DENIAL_FIELDS = ("tool_use_id", "tool_name", "denial_category", "denial_remedy", "part", "policy_reason", "harflow_access_deny")
FINISHED_DENIAL_ENTRIES = 32


FINISHED_EVENT_ENVELOPE_BYTES = 1024


def _finished_denials(task: Task, base: dict[str, Any]) -> dict[str, Any]:
    """Authoritative classified denials, only once the final result is in; absent when there are none.

    Entries are dropped (and counted in `denials_omitted`) until the complete event, measured in encoded
    bytes after the reader's own string truncation and with envelope headroom, fits `EVENT_MAX_BYTES`."""
    from .events import EVENT_MAX_BYTES, _truncate_value
    denials, categories = classified_denials(get_backend(task.backend), task.acc, task.freedom)
    if not denials:
        return {}
    items = [{k: (v[:200] if isinstance(v, str) else v) for k, v in entry.items() if k in FINISHED_DENIAL_FIELDS} for entry in denials if isinstance(entry, dict)][:FINISHED_DENIAL_ENTRIES]

    def size(count: int) -> int:
        event = {**base, "denial_categories": categories, "permission_denials": items[:count], "denials_omitted": len(denials) - count}
        return len(json.dumps(_truncate_value(event)[0], ensure_ascii=False, default=str).encode("utf-8")) + FINISHED_EVENT_ENVELOPE_BYTES

    count = len(items)
    while count and size(count) > EVENT_MAX_BYTES:
        count -= 1
    return {"denial_categories": categories, "permission_denials": items[:count], "denials_omitted": len(denials) - count}


def _close_events_after_writers(task: Task) -> None:
    """Close the event resource once the monitor and remaining pipe writers settle."""
    if task._events_cleanup_armed:
        return
    task._events_cleanup_armed = True
    pending = {writer for writer in [*task.watchers, task.pump]
               if writer is not None and not writer.done()}

    def settled(done=None) -> None:
        pending.discard(done)
        if not pending and task.events is not None:
            task.events.close()

    for writer in pending:
        writer.add_done_callback(settled)
    if not pending:
        settled()


def _classify(task: Task, exit_code: int) -> Status:
    """Delegate to the backend, except where the bridge itself broke the run."""
    if task.drain_failed:
        # We stopped reading its output, so whatever the agent reported cannot be trusted.
        return "failed"
    if task.live_input and task.acc.result_count < task.results_owed:
        # Messages were written that the stream never answered: on a backend where each one is
        # its own turn, the run exiting after the first of several owed results leaves turns
        # unanswered. The same rule `store._resolve` applies on recovery, so a live verdict and a
        # recovered one cannot disagree about the same stream.
        task.acc.turn_open = True
    return get_backend(task.backend).classify(task.acc, exit_code)
