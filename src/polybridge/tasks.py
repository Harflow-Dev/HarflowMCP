"""Background lifecycle for headless agent runs.

One `Task` owns one subprocess plus the asyncio tasks that drain its pipes and watch for its
exit. Nothing here blocks a caller for the duration of a run: `start` returns as soon as the
process is spawned, and completion is observed through `Task.done`.

Three invariants keep the state machine honest:

* Only `_monitor` publishes a terminal status and sets `Task.done`, and only after the process has
  exited and its pipes have been fully read. Callers therefore never observe a task as finished
  while its process is still alive.
* Conversely, `_monitor` publishes no status at all when it is itself cancelled: that means this
  server is being torn down, while the run — its own session leader — carries on. The task stays
  `running` for whichever process looks next.
* Termination is owned by a `Task`-scoped asyncio task, not by the caller that asked for it, so
  SIGKILL escalation still happens if that caller goes away mid-cancellation.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import uuid
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from . import store
from .backends import Accumulator, Backend
from .backends import get as get_backend
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

# How long to keep reading after the process exits. Normally EOF is immediate, but a backgrounded
# grandchild can inherit the write end of stdout and hold it open indefinitely, which would
# otherwise leave the run unfinishable.
DRAIN_GRACE_SECONDS = 10.0

MAX_TASKS = 200


class SessionBusyError(RuntimeError):
    """A session already has a live run, so a second one would fight it for session state."""


class SessionUnknownError(RuntimeError):
    """The backend never disclosed a session id, so the conversation cannot be continued."""


class RepoUnavailableError(RuntimeError):
    """A recovered task's repository is no longer there to resume into."""


def _identity_markers(
    backend: Backend, session_id: str | None, repo_path: Path
) -> tuple[str, ...]:
    """Strings that must all appear in the process's command line for it to still be this task.

    A live pid alone proves nothing — pids get reused. Claude carries its session id on the command
    line, so that is a precise marker. Codex mints its own id and never receives it as an argument,
    so the best available markers are its binary and its `-C <repo>`; a reused pid landing on another
    codex run in the same repository is the residual risk, and a far smaller one than reporting a
    running task as dead.
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
    parent_task_id: str | None = None
    freedom: str = "write_in_repo"
    enforcement: dict[str, Any] = field(default_factory=dict)
    # Strings guaranteed to appear in the process's command line, so a later server process can tell
    # this task's pid apart from a reused one. What identifies a run differs per backend: Claude
    # carries its session id on the command line, Codex mints its own and does not.
    markers: tuple[str, ...] = ()

    status: Status = "running"
    exit_code: int | None = None
    finished_at: datetime | None = None

    proc: asyncio.subprocess.Process | None = None
    # `start_new_session=True` makes the child its own group leader, so its pid is the group id.
    # Stored rather than looked up with getpgid() later: after the child is reaped its pid may
    # already name an unrelated process, and signalling that process's group would be a disaster.
    pgid: int | None = None

    acc: Accumulator = field(default_factory=Accumulator)
    tail: deque[str] = field(default_factory=lambda: deque(maxlen=TAIL_LINES))
    stderr_tail: deque[str] = field(default_factory=lambda: deque(maxlen=STDERR_TAIL_LINES))

    done: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_requested: bool = False
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

    def brief(self) -> dict[str, Any]:
        """The listing shape: identity and state, without stream detail."""
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
        }

    def snapshot(self) -> dict[str, Any]:
        """Full current state of the run, safe to call at any point."""
        snap = self.brief() | {
            "summary": self.acc.summary,
            "is_error": self.acc.is_error,
            "total_cost_usd": self.acc.total_cost_usd,
            "num_turns": self.acc.num_turns,
            "exit_code": self.exit_code,
            "permission_denials": self.acc.denials,
            "last_output_tail": list(self.tail)[-TAIL_LINES_RETURNED:],
            "raw_stream_log": str(self.log_path),
            "model": self.model,
            "max_turns": self.max_turns,
            "mcp_servers": self.acc.mcp_servers,
            "available_tool_count": self.acc.available_tool_count,
            "usage": self.acc.usage,
            "enforcement": self.enforcement,
            "notices": list(self.acc.notices),
        }
        # Only meaningful when something went wrong, and usually empty otherwise.
        if self.status == "failed" and self.stderr_tail:
            snap["stderr_tail"] = list(self.stderr_tail)
        return snap


class TaskRegistry:
    """In-memory registry of dispatched runs, bounded to `max_tasks` finished entries."""

    def __init__(self, log_dir: Path | None = None, max_tasks: int = MAX_TASKS) -> None:
        self._tasks: dict[str, Task] = {}
        self._log_dir = log_dir or default_log_dir()
        self._max_tasks = max_tasks

    async def start(
        self,
        prompt: str,
        repo_path: Path,
        *,
        backend: Backend,
        freedom: str = "write_in_repo",
        max_turns: int | None = None,
        model: str | None = None,
    ) -> Task:
        """Dispatch a fresh session. Returns once the subprocess exists, not once it finishes."""
        # Only some backends let us name the session up front. Where we can, knowing it immediately
        # means a resume works even if the run dies before saying anything.
        session_id = str(uuid.uuid4()) if backend.capabilities.chooses_session_id else None
        argv = backend.build_start_argv(
            prompt,
            repo=repo_path,
            freedom=freedom,  # type: ignore[arg-type]
            session_id=session_id,
            model=model,
            max_turns=max_turns,
        )
        return await self._spawn(
            argv,
            backend=backend,
            prompt=prompt,
            repo_path=repo_path,
            session_id=session_id,
            freedom=freedom,
            max_turns=max_turns,
            model=model,
        )

    async def resume(
        self,
        parent: Task,
        followup_prompt: str,
        *,
        max_turns: int | None = None,
    ) -> Task:
        """Continue `parent`'s session as a new task sharing its session id."""
        if parent.session_id is None:
            raise SessionUnknownError(
                f"task {parent.task_id} never disclosed a session id, so its conversation cannot "
                "be resumed; start a new task instead"
            )
        if self.session_has_live_run(parent.session_id):
            raise SessionBusyError(
                f"session {parent.session_id} already has a running task; "
                "two concurrent runs would corrupt its shared conversation state"
            )

        backend = get_backend(parent.backend)
        argv = backend.build_resume_argv(
            followup_prompt,
            repo=parent.repo_path,
            freedom=parent.freedom,  # type: ignore[arg-type]
            session_id=parent.session_id,
            # Carried over so the continuation runs on the model the run started with, and so the
            # model we report for it stays true.
            model=parent.model,
            max_turns=max_turns,
        )
        return await self._spawn(
            argv,
            backend=backend,
            prompt=followup_prompt,
            repo_path=parent.repo_path,
            session_id=parent.session_id,
            freedom=parent.freedom,
            max_turns=max_turns,
            model=parent.model,
            parent_task_id=parent.task_id,
        )

    async def _spawn(
        self,
        argv: list[str],
        *,
        backend: Backend,
        prompt: str,
        repo_path: Path,
        session_id: str | None,
        freedom: str,
        max_turns: int | None,
        model: str | None,
        parent_task_id: str | None = None,
    ) -> Task:
        # Re-checked at the point of execution, not only where the argv was built, so no future
        # caller of this method can launch an agent without its backend's guarantees.
        backend.assert_safe(argv)

        task_id = str(uuid.uuid4())
        self._log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self._log_dir / f"{task_id}.jsonl"

        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=str(repo_path),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=STREAM_LINE_LIMIT,
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
            parent_task_id=parent_task_id,
            freedom=freedom,
            enforcement=backend.enforcement(freedom).as_dict(),  # type: ignore[arg-type]
            markers=_identity_markers(backend, session_id, repo_path),
            proc=proc,
            pgid=proc.pid,
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

    @property
    def log_dir(self) -> Path:
        return self._log_dir

    def persist(self, task: Task) -> None:
        store.write(
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
                max_turns=task.max_turns,
                parent_task_id=task.parent_task_id,
                prompt=task.prompt[: store.PROMPT_PREVIEW_CHARS],
                status=task.status,
                exit_code=task.exit_code,
                finished_at=task.finished_at.isoformat() if task.finished_at else None,
            ),
        )

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

    async def cancel(self, task: Task) -> Task:
        """Stop a run, waiting for it to actually die before returning.

        Safe to call concurrently: the termination sequence is owned by the task, so repeat callers
        join the one already in flight rather than starting a second.
        """
        if task.finished:
            return task

        task.cancel_requested = True
        if task.proc is None:
            # No process was ever attached (only reachable in tests); nothing to signal.
            task.status = "cancelled"
            task.finished_at = _now()
            task.done.set()
            return task

        if task.termination is None or task.termination.done():
            task.termination = asyncio.create_task(
                _terminate(task), name=f"pb-terminate-{task.task_id}"
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

    async def cancel_recovered(self, record: store.TaskRecord) -> store.TaskRecord:
        """Stop a task this process never spawned, using its recorded process group.

        There is no subprocess handle to wait on, so liveness is polled instead. The record is only
        marked cancelled if the process was actually ours and actually signalled — otherwise the
        response would claim an outcome that never happened.
        """
        alive = store.process_alive(record.pid, record.markers)
        if not alive:
            log.info("recovered task %s was already gone; nothing to cancel", record.task_id)
            return record

        # Attempted even though the leader looked alive a moment ago: the group may hold children
        # that outlive it and keep the run's pipes open.
        signalled = _signal_recorded_group(record, signal.SIGTERM)
        if signalled and not await _await_recorded_death(record):
            log.warning("recovered task %s ignored SIGTERM, sending SIGKILL", record.task_id)
            _signal_recorded_group(record, signal.SIGKILL)
            # Waited for too: the response is resolved through `store.resolve_status`, which
            # rechecks liveness, so returning while the group is still dying would answer this
            # cancellation with "running" and contradict itself.
            if not await _await_recorded_death(record):
                log.warning(
                    "recovered task %s survived SIGKILL; it will be reported as still running",
                    record.task_id,
                )

        if not signalled:
            log.warning(
                "recovered task %s looked alive but could not be signalled; leaving its record "
                "untouched rather than claiming it was cancelled",
                record.task_id,
            )
            return record

        cancelled = replace(
            record, status="cancelled", finished_at=record.finished_at or _now().isoformat()
        )
        store.write(self._log_dir, cancelled)
        log.info("recovered task %s cancelled", record.task_id)
        return cancelled

    async def resume_record(
        self,
        record: store.TaskRecord,
        followup_prompt: str,
        *,
        max_turns: int | None = None,
    ) -> Task:
        """Continue the session of a task recovered from disk."""
        if not record.session_id:
            raise SessionUnknownError(
                f"task {record.task_id} never disclosed a session id, so its conversation cannot "
                "be resumed; start a new task instead"
            )
        if self.session_has_live_run(record.session_id):
            raise SessionBusyError(
                f"session {record.session_id} already has a running task; "
                "two concurrent runs would corrupt its shared conversation state"
            )

        # Revalidated rather than trusted: the recorded path may have been deleted, or replaced by
        # a different repository, since the original run.
        repo_path = Path(record.repo_path)
        if not repo_path.is_dir():
            raise RepoUnavailableError(
                f"the repository this task ran in no longer exists: {record.repo_path}"
            )

        backend = get_backend(record.backend)
        argv = backend.build_resume_argv(
            followup_prompt,
            repo=repo_path,
            freedom=record.freedom,  # type: ignore[arg-type]
            session_id=record.session_id,
            model=record.model,
            max_turns=max_turns,
        )
        return await self._spawn(
            argv,
            backend=backend,
            prompt=followup_prompt,
            repo_path=repo_path,
            session_id=record.session_id,
            freedom=record.freedom,
            max_turns=max_turns,
            model=record.model,
            parent_task_id=record.task_id,
        )

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


async def _await_recorded_death(record: store.TaskRecord, timeout: float | None = None) -> bool:
    """Poll a recovered task's process until it is gone. False if it outlasted `timeout`."""
    # Read at call time rather than bound as a default, so the grace period stays patchable.
    timeout = SIGKILL_GRACE_SECONDS if timeout is None else timeout
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.25)
        if not store.process_alive(record.pid, record.markers):
            return True
    return not store.process_alive(record.pid, record.markers)


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


async def _terminate(task: Task) -> None:
    """SIGTERM the run's process group, escalating to SIGKILL if it does not go quietly."""
    proc = task.proc
    assert proc is not None

    _signal_group(task, signal.SIGTERM)
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
        handle = task.log_path.open("a", encoding="utf-8")
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
            if handle is not None:
                try:
                    handle.write(text)
                    handle.flush()
                except OSError:
                    log.warning("task %s: raw log write failed; dropping it", task.task_id)
                    handle.close()
                    handle = None

            line = text.rstrip("\n")
            if not line.strip():
                continue
            task.tail.append(line[:TAIL_LINE_CHARS])

            event = parse_line(line)
            if event is None:
                task.acc.unparsable_lines += 1
                continue
            get_backend(task.backend).ingest(event, task.acc)

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


async def _monitor(task: Task, registry: TaskRegistry) -> None:
    """Await exit, finish reading the stream, then publish the final status.

    The sole writer of terminal status and of `Task.done`.
    """
    assert task.proc is not None
    abandoned = False
    try:
        exit_code = await task.proc.wait()
        await _finish_draining(task)

        task.exit_code = exit_code
        task.finished_at = _now()
        task.status = "cancelled" if task.cancel_requested else _classify(task, exit_code)

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
        abandoned = True
        log.warning(
            "task %s: monitor cancelled (process returncode=%s); leaving it recorded as running so "
            "a later server process can observe the real outcome",
            task.task_id,
            task.proc.returncode,
        )
        raise
    except Exception:
        log.exception("task %s: monitor failed", task.task_id)
        task.finished_at = _now()
        task.status = "cancelled" if task.cancel_requested else "failed"
    finally:
        # Skipped when abandoned: `done` alongside a non-terminal status is only unobservable
        # because the loop that could observe it is the one being torn down.
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


def _classify(task: Task, exit_code: int) -> Status:
    """Delegate to the backend, except where the bridge itself broke the run."""
    if task.drain_failed:
        # We stopped reading its output, so whatever the agent reported cannot be trusted.
        return "failed"
    return get_backend(task.backend).classify(task.acc, exit_code)
