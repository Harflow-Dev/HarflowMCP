"""On-disk record of dispatched tasks.

The registry is in-process, but the process is not stable: an MCP client may run several bridge
servers or restart one, and a server that did not spawn a task would otherwise have no idea it
exists — leaving a live `claude` running that no tool can report on or stop.

So each task also gets a small sidecar JSON beside its raw stream log. Any server can read those
back, replay the log for the outcome, and check whether the process is still alive.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .backends import Accumulator
from .backends import get as get_backend
from .stream import parse_line

log = logging.getLogger(__name__)

RECORD_SUFFIX = ".meta.json"

# Ids are uuid4, but they arrive from a caller and become file paths, so the charset is enforced.
TASK_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")

TERMINAL_RECORD_STATUSES = frozenset({"completed", "failed", "timed_out", "cancelled"})

# Prompts can be huge; enough is kept to recognise a task without bloating the record.
PROMPT_PREVIEW_CHARS = 2000

TAIL_LINES_RETURNED = 20
TAIL_LINE_CHARS = 2000


@dataclass(frozen=True)
class TaskRecord:
    """What is known about a task without holding its subprocess."""

    task_id: str
    backend: str
    session_id: str | None
    repo_path: str
    started_at: str
    freedom: str = "write_in_repo"
    markers: list[str] = field(default_factory=list)
    """Strings that must appear in the process command line for a pid to still be this task."""
    pid: int | None = None
    pgid: int | None = None
    model: str | None = None
    max_turns: int | None = None
    parent_task_id: str | None = None
    prompt: str = ""
    status: str = "running"
    exit_code: int | None = None
    finished_at: str | None = None


class InvalidTaskId(ValueError):
    """A task id that must not be turned into a filesystem path."""


def validate_task_id(task_id: str) -> str:
    """Reject anything that could escape the task directory.

    Ids we mint are uuid4, but this value reaches us from a caller, and it is used to build file
    paths — `../` in it would otherwise read records outside the directory.
    """
    if not TASK_ID_PATTERN.fullmatch(task_id or ""):
        raise InvalidTaskId(f"not a valid task id: {task_id!r}")
    return task_id


def record_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{validate_task_id(task_id)}{RECORD_SUFFIX}"


def log_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{validate_task_id(task_id)}.jsonl"


def write(log_dir: Path, record: TaskRecord) -> None:
    """Persist a record, atomically. Never raises — losing a record must not fail a dispatch.

    Refuses to move a task backwards: with several server processes writing, a stale "running"
    record must not overwrite an already-recorded outcome.
    """
    try:
        target = record_path(log_dir, record.task_id)
    except InvalidTaskId:
        log.warning("refusing to persist a record for an invalid task id")
        return

    existing = read(log_dir, record.task_id)
    if (
        existing is not None
        and existing.status in TERMINAL_RECORD_STATUSES
        and record.status not in TERMINAL_RECORD_STATUSES
    ):
        log.debug(
            "keeping recorded %s for task %s over stale %s",
            existing.status,
            record.task_id,
            record.status,
        )
        return

    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=log_dir, prefix=f".{record.task_id}.", delete=False
        )
        try:
            with handle:
                json.dump(asdict(record), handle, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(handle.name, target)
        except BaseException:
            Path(handle.name).unlink(missing_ok=True)
            raise
    except OSError:
        log.warning("could not persist record for task %s", record.task_id, exc_info=True)


def read(log_dir: Path, task_id: str) -> TaskRecord | None:
    path = record_path(log_dir, task_id)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        log.warning("ignoring unreadable task record %s", path, exc_info=True)
        return None
    if not isinstance(raw, dict):
        return None

    fields = {f for f in TaskRecord.__dataclass_fields__}
    try:
        # Unknown keys are dropped so a record written by a newer version still loads.
        return TaskRecord(**{k: v for k, v in raw.items() if k in fields})
    except TypeError:
        log.warning("ignoring malformed task record %s", path)
        return None


def read_all(log_dir: Path) -> list[TaskRecord]:
    try:
        paths = sorted(log_dir.glob(f"*{RECORD_SUFFIX}"))
    except OSError:
        return []
    records = [read(log_dir, path.name[: -len(RECORD_SUFFIX)]) for path in paths]
    return sorted((r for r in records if r is not None), key=lambda r: r.started_at)


def process_alive(pid: int | None, markers: Sequence[str]) -> bool:
    """Whether the recorded process is still running *and* is still the task we think it is.

    The pid alone is not enough — pids get reused. Each backend supplies markers that must all
    appear in the command line for it to be the same run (see `tasks._identity_markers`).
    """
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Alive but owned by someone else, so it is not ours.
        return False

    try:
        cmdline = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        # The pid is alive; we just cannot confirm identity. Reporting "gone" would be a worse
        # error than reporting "running", since it invents an outcome for a task still going.
        log.warning("could not run ps to identify pid %s; assuming it is still the task", pid)
        return True
    return all(marker in cmdline for marker in markers) if markers else True


def replay_log(log_dir: Path, task_id: str, backend_name: str) -> tuple[Accumulator, list[str]]:
    """Rebuild what the stream said, so a recovered task reports the same fields as a live one."""
    backend = get_backend(backend_name)
    state = Accumulator()
    tail: list[str] = []
    try:
        with log_path(log_dir, task_id).open(encoding="utf-8") as handle:
            for line in handle:
                line = line.rstrip("\n")
                if not line.strip():
                    continue
                tail.append(line[:TAIL_LINE_CHARS])
                event = parse_line(line)
                if event is not None:
                    backend.ingest(event, state)
    except FileNotFoundError:
        pass
    except OSError:
        log.warning("could not replay stream log for task %s", task_id, exc_info=True)
    return state, tail[-TAIL_LINES_RETURNED:]


def _duration_seconds(record: TaskRecord, finished_at: str | None) -> float | None:
    try:
        start = datetime.fromisoformat(record.started_at)
    except ValueError:
        return None
    end = datetime.now(timezone.utc)
    if finished_at:
        try:
            end = datetime.fromisoformat(finished_at)
        except ValueError:
            pass
    return round((end - start).total_seconds(), 3)


def outcome_unobserved(record: TaskRecord) -> bool:
    """Whether the recorded status might be a lie about a process that is still running.

    A terminal status with no exit code was never observed to exit: it is what a bridge server
    writes about a live run while its own event loop is being torn down, and what it writes when
    its monitor crashes. The agent process survives both, so these records have to be rechecked
    against the process rather than believed.
    """
    return record.status == "running" or record.exit_code is None


def resolve_status(log_dir: Path, record: TaskRecord) -> tuple[str, str, Accumulator, list[str]]:
    """The single place a recovered task's status is decided.

    Shared by `snapshot` and `brief` so the same task can never be listed as one status and
    reported as another.
    """
    state, tail = replay_log(log_dir, record.task_id, record.backend)
    unobserved = outcome_unobserved(record)
    alive = unobserved and process_alive(record.pid, record.markers)

    if alive and record.status in TERMINAL_RECORD_STATUSES:
        return (
            "running",
            f"Recovered from disk: this task is recorded as '{record.status}', but its process is "
            "still alive and working — that record was written by a bridge server being shut down "
            "mid-run, and it was wrong. Cancellation works. Its output, however, is going nowhere: "
            "the pipes died with that server, so the raw log stops at the teardown and no summary "
            "will ever arrive for the rest of the run. Judge it by what it changed on disk, or "
            "cancel it and dispatch again.",
            state,
            tail,
        )
    if alive:
        return (
            "running",
            "Recovered from disk: this task was started by an earlier bridge server process and "
            "is still running. Status and cancellation work; its live output is not being read by "
            "this process, so the summary appears only once it finishes.",
            state,
            tail,
        )
    if record.status in TERMINAL_RECORD_STATUSES and not unobserved:
        return (
            record.status,
            "Recovered from disk: recorded by the bridge server that ran it.",
            state,
            tail,
        )
    # Only an unobserved `failed` is an inference the stream may overrule: it is what the monitor's
    # backstop writes when it never saw the process exit at all. Every other status records
    # something the bridge did — `cancelled` above all — so it stands even without an exit code.
    if record.status in TERMINAL_RECORD_STATUSES and record.status != "failed":
        return (
            record.status,
            "Recovered from disk: recorded by the bridge server that ran it, which never saw the "
            "process exit. The status stands because it describes what the bridge did, not what it "
            "guessed the run was doing.",
            state,
            tail,
        )
    if state.terminal is not None or state.saw_final_message:
        status = get_backend(record.backend).classify(
            state, record.exit_code if record.exit_code is not None else 0
        )
        return (
            status,
            "Recovered from disk: the bridge server that started this task went away before "
            "recording the outcome, so this was reconstructed from the run's own output. The run "
            "reported this result itself, though its exit code was never observed.",
            state,
            tail,
        )
    return (
        "failed",
        "Recovered from disk: the process is gone and its output has no result event, so it was "
        "interrupted — most likely killed when the bridge server that started it exited. This is "
        "inferred rather than observed. Nothing it had already written to the repo was undone.",
        state,
        tail,
    )


def _enforcement(record: TaskRecord) -> dict[str, Any]:
    try:
        return get_backend(record.backend).enforcement(record.freedom).as_dict()  # type: ignore[arg-type]
    except Exception:
        # An unrecognised backend or freedom in an older record must not break recovery.
        log.debug("could not rebuild enforcement for task %s", record.task_id)
        return {}


def snapshot(log_dir: Path, record: TaskRecord) -> dict[str, Any]:
    """A recovered task's state, shaped like a live snapshot so callers need no special casing."""
    status, note, state, tail = resolve_status(log_dir, record)

    return {
        "task_id": record.task_id,
        "backend": record.backend,
        "session_id": record.session_id,
        "repo_path": record.repo_path,
        "status": status,
        "freedom": record.freedom,
        "started_at": record.started_at,
        "duration_seconds": _duration_seconds(record, record.finished_at),
        "parent_task_id": record.parent_task_id,
        "summary": state.summary,
        "is_error": state.is_error,
        "total_cost_usd": state.total_cost_usd,
        "num_turns": state.num_turns,
        "exit_code": record.exit_code,
        "permission_denials": state.denials,
        "last_output_tail": tail,
        "raw_stream_log": str(log_path(log_dir, record.task_id)),
        "model": record.model,
        "max_turns": record.max_turns,
        "mcp_servers": state.mcp_servers,
        "available_tool_count": state.available_tool_count,
        "usage": state.usage,
        "notices": list(state.notices),
        # Reconstructed from the recorded backend and freedom, so a recovered task reports the same
        # honest picture of what was enforced as a live one.
        "enforcement": _enforcement(record),
        "recovered": True,
        "note": note,
    }


def brief(log_dir: Path, record: TaskRecord) -> dict[str, Any]:
    """Listing shape for a recovered task, using the same status resolution as `snapshot`."""
    status, _, _, _ = resolve_status(log_dir, record)
    return {
        "task_id": record.task_id,
        "backend": record.backend,
        "session_id": record.session_id,
        "repo_path": record.repo_path,
        "status": status,
        "freedom": record.freedom,
        "started_at": record.started_at,
        "duration_seconds": _duration_seconds(record, record.finished_at),
        "parent_task_id": record.parent_task_id,
        "recovered": True,
    }


def live_session_ids(log_dir: Path) -> set[str]:
    """Sessions with a still-running task according to disk, whichever server started it.

    Uses the same "was this outcome actually observed?" test as `resolve_status`, so a run whose
    server was torn down mid-flight still counts as live here. Trusting its recorded status instead
    would let a second resume start against a session that is still being written to.
    """
    return {
        record.session_id
        for record in read_all(log_dir)
        # A backend that mints its own id may have died before disclosing one; nothing to exclude.
        if record.session_id
        and outcome_unobserved(record)
        and process_alive(record.pid, record.markers)
    }
