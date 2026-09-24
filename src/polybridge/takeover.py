"""Taking a task over into the user's own interactive terminal (A4.1).

Humans only: this is reached through `polybridge-ctl takeover` / `takeover-attach`, never through
MCP, and both refuse when the caller looks like an agent — `PB_TASK_ID` in the environment, or a
caller `lineage.detect_caller` can confirm. The interactive session runs under the user's own
default permissions, not the task's freedom, so an agent reaching this would escape its own
enforcement.

The steps, each a phase file of the attempt (see `control.py`'s takeover section):

1. `.req` — written under the session lock; from here `store.live_session_ids` reports the session
   busy, so a `resume_task` is refused with the existing `SessionBusyError`.
2. Refusals that write `.failed`: no session id, no safe interactive command, the binary is not on
   PATH, the repository is gone, or another run holds the session.
3. A live task is cascade-cancelled (A2.2) and its death confirmed; a survivor, or a task whose
   liveness cannot be decided (the legacy fallback included), writes `.failed`. A task that had
   already finished is left exactly as it was — no signal, and its status stands.
4. `.ready`, then `{argv, cwd, session_id, note}` for the app to run in a terminal.
5. `takeover-attach` records the terminal's process as `.attach`; the session stays busy until that
   process is confirmed gone.

Measured: claude has no session lock of its own, so an interactive resume beside a live headless run
silently forks the transcript, and codex refuses input in that state — which is why step 3 always
stops the headless run first and confirms it died.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import backends, control, identity, lineage, store

log = logging.getLogger(__name__)

SESSION_LOCK_TIMEOUT_SECONDS = 30.0
DEATH_CONFIRM_SECONDS = 5.0
_DEATH_POLL_SECONDS = 0.2

_NOTE = (
    "{state} This interactive session runs under your own default {backend} permissions — the "
    "task's enforcement (freedom: {freedom}) does not apply to it. Record the terminal's process "
    "with `polybridge-ctl takeover-attach {task_id} --pid <pid>` within {window:.0f} s."
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def agent_caller_reason(log_dir: Path, environ: Mapping[str, str] | None = None) -> str | None:
    """Why the current process looks like an agent, or None for a person.

    Stricter than caller detection alone: `PB_TASK_ID` in the environment refuses even when the
    task it names cannot be confirmed, since only a polybridge-spawned agent (or something it
    started) carries it. `lineage.detect_caller` is looked up at call time so tests can stub it.
    """
    env = os.environ if environ is None else environ
    if env.get(lineage.ENV_TASK_ID):
        return f"{lineage.ENV_TASK_ID} is set, so this is running inside a polybridge task"
    caller = lineage.detect_caller(log_dir)
    if caller is not None:
        return f"it was called from task {caller.record.task_id} (detected by {caller.method})"
    return None


def _refuse_agents(log_dir: Path, environ: Mapping[str, str] | None) -> None:
    reason = agent_caller_reason(log_dir, environ)
    if reason is not None:
        raise control.TakeoverRefused(
            "agent_caller",
            f"takeover is for a person at the Monitor, not an agent: {reason}. An interactive "
            "session would not carry the task's enforcement.",
        )


def _lineage_tree(log_dir: Path, task_id: str) -> set[str]:
    rows = [(r.task_id, r.spawned_by, r.root_task_id) for r in store.read_all(log_dir)]
    return lineage.lineage_closure(rows, task_id)


def other_session_holders(log_dir: Path, task_id: str, session_id: str) -> list[str]:
    """Runs other than `task_id` and its lineage descendants that may hold `session_id`: live
    records on it, and other tasks' takeover reservations of it."""
    tree = _lineage_tree(log_dir, task_id)
    holders = {tid for tid in store.live_session_task_ids(log_dir, session_id) if tid not in tree}
    holders.update(
        tid
        for tid, reserved in control.takeover_reservations(log_dir).items()
        if reserved == session_id and tid != task_id
    )
    return sorted(holders)


class _Refusal(Exception):
    def __init__(self, code: str, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason


def _interactive_command(log_dir: Path, record: store.TaskRecord) -> list[str]:
    """Step 2: the command to hand out, or `_Refusal`. Blocking (`ps`, disk), run in a thread."""
    if not record.session_id:
        raise _Refusal("no_session", "the task never disclosed a session id, so there is nothing to resume")
    try:
        backend = backends.get(record.backend)
    except backends.UnknownBackend:
        raise _Refusal("unknown_backend", f"unknown backend {record.backend!r}") from None
    repo = Path(record.repo_path)
    argv = backend.interactive_resume_argv(record.session_id, repo)
    if not argv:
        raise _Refusal(
            "no_interactive_command",
            f"the {record.backend} backend has no safe interactive resume for session "
            f"{record.session_id!r}",
        )
    # Not symlink-resolved: `~/.local/bin/claude` points into a versioned directory, and pinning
    # that would break at the CLI's next update (CLAUDE.md).
    binary = shutil.which(argv[0])
    if binary is None:
        raise _Refusal("binary_not_found", f"`{argv[0]}` is not on PATH")
    if not repo.is_dir():
        raise _Refusal("repo_unavailable", f"the repository no longer exists: {record.repo_path}")
    holders = other_session_holders(log_dir, record.task_id, record.session_id)
    if holders:
        raise _Refusal(
            "session_busy",
            f"another run holds session {record.session_id}: {', '.join(holders)}; take over that "
            "one instead",
        )
    return [binary, *argv[1:]]


def _liveness(record: store.TaskRecord) -> tuple[str, str]:
    """`("finished", "")`, `("live", "")` or `("undecidable", reason)` for the task's own run."""
    if record.status in store.TERMINAL_RECORD_STATUSES and record.exit_code is not None:
        return "finished", ""
    verdict, reason = identity.check_detail(
        identity.task_identity(record.pid, record.start_time, record.markers)
    )
    if verdict == "dead":
        return "finished", ""
    if verdict == "alive":
        return "live", ""
    return "undecidable", reason


async def _confirm_dead(record: store.TaskRecord) -> bool:
    leader = identity.task_identity(record.pid, record.start_time, record.markers)
    deadline = asyncio.get_running_loop().time() + DEATH_CONFIRM_SECONDS
    while True:
        if await asyncio.to_thread(identity.identity_check, leader) == "dead":
            return True
        if asyncio.get_running_loop().time() >= deadline:
            return False
        await asyncio.sleep(_DEATH_POLL_SECONDS)


async def _stop_run(record: store.TaskRecord, registry_factory: Callable[[], Any]) -> None:
    """Step 3 for a live task: cascade-cancel, then confirm it is gone, or `_Refusal`."""
    registry = registry_factory()
    try:
        cascade = await registry.cancel_cascade(record.task_id)
    except control.PhaseWriteError as exc:
        raise _Refusal("cancel_failed", f"the cancel could not be attempted: {exc}") from None

    survivors = set(cascade.get("sigkill_survivors", []))
    survivors.update(entry["task_id"] for entry in cascade.get("not_signalled", []))
    if survivors:
        raise _Refusal(
            "not_stopped",
            f"the headless run could not be confirmed stopped; still running or not signalled: "
            f"{', '.join(sorted(survivors))}",
        )
    if record.task_id in cascade.get("owner_still_settling", []):
        if await asyncio.to_thread(identity.identity_check, record.owner) == "undecidable":
            raise _Refusal(
                "not_stopped",
                "the task's owning server could not be confirmed alive or dead, so its run cannot "
                "be confirmed settled",
            )
    if not await _confirm_dead(record):
        raise _Refusal("not_stopped", "the headless run's process was not confirmed gone")


def _fail(log_dir: Path, task_id: str, n: int, reason: str) -> None:
    try:
        control.mark_takeover_failed(log_dir, task_id, n, reason)
    except control.PhaseWriteError:
        # The refusal still stands; an attempt left pending is recovered once this controller
        # exits and its lease expires.
        log.warning("takeover of %s: could not record attempt %d as failed", task_id, n, exc_info=True)


def _session_lock(log_dir: Path, session_id: str | None):
    if not session_id:
        return contextlib.nullcontext()
    return control.session_lock(log_dir, session_id, timeout=SESSION_LOCK_TIMEOUT_SECONDS)


async def take_over(
    log_dir: Path,
    task_id: str,
    *,
    registry_factory: Callable[[], Any],
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run takeover steps 1–4 for `task_id`. Returns `{argv, cwd, session_id, note}`; raises
    `control.TakeoverRefused` (with a `code`) on every refusal, after writing `.failed` for any
    attempt it opened."""
    try:
        store.validate_task_id(task_id)
    except store.InvalidTaskId as exc:
        raise control.TakeoverRefused("invalid_task_id", str(exc)) from None
    await asyncio.to_thread(_refuse_agents, log_dir, environ)

    record = await asyncio.to_thread(store.read, log_dir, task_id)
    if record is None:
        raise control.TakeoverRefused("unknown_task", f"unknown task_id: {task_id}")
    controller = await asyncio.to_thread(identity.own_identity)

    n: int | None = None
    try:
        try:
            async with _session_lock(log_dir, record.session_id):
                try:
                    n = control.begin_takeover(
                        log_dir, task_id, controller=controller, session_id=record.session_id
                    )
                except control.PhaseWriteError as exc:
                    raise control.TakeoverRefused(
                        "phase_write_failed", f"the takeover could not be recorded: {exc}"
                    ) from None
                argv = await asyncio.to_thread(_interactive_command, log_dir, record)
        except control.LockTimeout:
            raise control.TakeoverRefused(
                "session_busy",
                f"session {record.session_id} is being resumed or taken over right now; try again",
            ) from None

        state, reason = await asyncio.to_thread(_liveness, record)
        if state == "undecidable":
            raise _Refusal(
                "not_stopped", f"whether the headless run is still alive cannot be decided ({reason})"
            )
        if state == "live":
            await _stop_run(record, registry_factory)

        try:
            async with _session_lock(log_dir, record.session_id):
                holders = await asyncio.to_thread(
                    other_session_holders, log_dir, task_id, record.session_id
                )
                if holders:
                    raise _Refusal(
                        "session_busy",
                        f"another run started on session {record.session_id} meanwhile: "
                        f"{', '.join(holders)}",
                    )
                control.mark_takeover_ready(log_dir, task_id, n)
        except control.LockTimeout:
            raise _Refusal("session_busy", "the session lock could not be taken to finish") from None
    except _Refusal as refusal:
        if n is not None:
            await asyncio.to_thread(_fail, log_dir, task_id, n, refusal.reason)
        raise control.TakeoverRefused(refusal.code, refusal.reason) from None
    except control.TakeoverRefused:
        raise
    except BaseException as exc:
        # Synchronous on purpose: this path includes a cancellation, where awaiting is unreliable.
        if n is not None:
            _fail(log_dir, task_id, n, f"error: {exc!r}")
        if isinstance(exc, Exception):
            raise control.TakeoverRefused("internal_error", f"takeover failed: {exc}") from exc
        raise

    fresh = await asyncio.to_thread(store.read, log_dir, task_id) or record
    status = (await asyncio.to_thread(store.resolve_status, log_dir, fresh, detail=False))[0]
    state_line = (
        "The headless run is stopped."
        if state == "live"
        else f"The task had already finished ({status}); nothing was stopped."
    )
    return {
        "argv": argv,
        "cwd": record.repo_path,
        "session_id": record.session_id,
        "note": _NOTE.format(
            state=state_line,
            backend=record.backend,
            freedom=record.freedom,
            task_id=task_id,
            window=control.TAKEOVER_ATTACH_WINDOW_SECONDS,
        ),
    }


def attach(
    log_dir: Path,
    task_id: str,
    pid: int,
    *,
    environ: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Step 5: record the interactive terminal's process on the latest attempt as `.attach`.

    Synchronous. Takes the session lock, then `<id>.lock` — the order used everywhere. Refused when
    no attempt is ready, it already failed or attached, its window has expired, or another run now
    holds the session; the last two also write `.failed`, ending the attempt. A pid whose identity
    cannot be captured is refused without writing — the window then runs out on its own. Markers
    are recorded, not required: a process not showing them reads `undecidable`, which stays busy.
    """
    try:
        store.validate_task_id(task_id)
    except store.InvalidTaskId as exc:
        raise control.TakeoverRefused("invalid_task_id", str(exc)) from None
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 1:
        raise control.TakeoverRefused("invalid_pid", f"not a usable pid: {pid!r}")
    _refuse_agents(log_dir, environ)

    record = store.read(log_dir, task_id)
    if record is None:
        raise control.TakeoverRefused("unknown_task", f"unknown task_id: {task_id}")
    if not record.session_id:
        raise control.TakeoverRefused("not_ready", f"task {task_id} has no takeover ready to attach")
    try:
        binary = backends.get(record.backend).binary
    except backends.UnknownBackend:
        raise control.TakeoverRefused("unknown_backend", f"unknown backend {record.backend!r}") from None

    try:
        with control.session_lock_sync(
            log_dir, record.session_id, SESSION_LOCK_TIMEOUT_SECONDS
        ), control.record_lock_sync(log_dir, task_id):
            attempt = control.takeover_attempt(log_dir, task_id)
            if attempt is None or attempt.ready is None:
                raise control.TakeoverRefused(
                    "not_ready", f"task {task_id} has no takeover ready to attach"
                )
            if attempt.failed:
                raise control.TakeoverRefused(
                    "takeover_failed", f"takeover attempt {attempt.n} of {task_id} already failed"
                )
            if attempt.attach is not None:
                raise control.TakeoverRefused(
                    "already_attached", f"takeover attempt {attempt.n} of {task_id} is already attached"
                )
            resolved_now = now or _now()
            if not control.takeover_window_open(attempt, resolved_now):
                _fail(log_dir, task_id, attempt.n, "attach window expired")
                raise control.TakeoverRefused(
                    "window_expired",
                    f"the {control.TAKEOVER_ATTACH_WINDOW_SECONDS:.0f} s attach window for takeover "
                    f"attempt {attempt.n} of {task_id} has expired; take it over again",
                )
            holders = other_session_holders(log_dir, task_id, record.session_id)
            if holders:
                _fail(log_dir, task_id, attempt.n, "another run holds the session")
                raise control.TakeoverRefused(
                    "session_busy",
                    f"another run now holds session {record.session_id}: {', '.join(holders)}",
                )
            captured = identity.capture(pid, [binary, record.session_id])
            if captured is None:
                raise control.TakeoverRefused("pid_not_found", f"process {pid} could not be identified")
            payload = {"at": resolved_now.isoformat(), **captured}
            try:
                created = control.write_phase(
                    log_dir, task_id, control.TAKEOVER, attempt.n, "attach", payload
                )
            except control.PhaseWriteError as exc:
                raise control.TakeoverRefused(
                    "phase_write_failed", f"the attach could not be recorded: {exc}"
                ) from None
            if not created:
                raise control.TakeoverRefused(
                    "already_attached", f"takeover attempt {attempt.n} of {task_id} is already attached"
                )
    except control.LockTimeout as exc:
        raise control.TakeoverRefused("lock_timeout", str(exc)) from None

    return {
        "task_id": task_id,
        "attempt": attempt.n,
        "pid": captured["pid"],
        "start_time": captured["start_time"],
        "status": "attached",
    }
