"""Queued input for a live-input run: `<task_id>.inbox.jsonl` and `<task_id>.inbox.closed`.

A live-input claude run keeps its stdin open so a caller can add messages while it works
(`send_message`, `polybridge-ctl send`). The owning server's input pump writes them to the process;
anyone else appends them here, and the pump picks them up.

Three rules keep an acknowledged message from being lost silently:

* **One lock, `flock` on `<id>.inbox.jsonl` itself.** Every send and the pump's close protocol take it,
  so "is input still open?" and "append" happen as one step. It is a lock file, so it is never deleted —
  retention leaves it in place.
* **The close is a marker, created under that lock.** The pump forwards (or drops) whatever is queued,
  creates `<id>.inbox.closed`, releases the lock, and only then closes stdin. A send that finds the
  marker is refused with "finished; continue with resume_task"; one that got the lock first was
  already forwarded or reported undelivered.
* **"queued", never "delivered".** A send only ever says it queued the message. The pump's own event
  log records what actually happened: a `user_message` event when written, an `undelivered` one (plus
  a notice) when dropped.

Nothing here awaits while the lock is held. The async acquirer retries a non-blocking `flock` on the
loop (via `control.acquire`); the sync one sleeps between tries and is meant for worker threads and
`polybridge-ctl`.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import control, identity, store

INBOX_SUFFIX = ".inbox.jsonl"
CLOSED_SUFFIX = ".inbox.closed"

# Bounds one wait for the inbox lock. Every holder keeps it for a handful of file operations (a ps
# call at most), so this is only reached when something is badly wrong.
LOCK_TIMEOUT_SECONDS = 10.0
_LOCK_POLL_SECONDS = 0.05

CLOSED_MESSAGE = "finished; continue with resume_task"


class SendRefused(RuntimeError):
    """A message that was not queued, and why. `code` is stable for callers and `polybridge-ctl`:
    `closed`, `not_live_input`, `settled`, `exited`, `owner_not_alive`, `unknown_task`,
    `lock_timeout`, `write_failed`."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def inbox_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{store.validate_task_id(task_id)}{INBOX_SUFFIX}"


def closed_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{store.validate_task_id(task_id)}{CLOSED_SUFFIX}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_message(text: str, by: dict[str, Any] | None) -> dict[str, Any]:
    return {"id": uuid.uuid4().hex, "text": text, "queued_at": _now_iso(), "by": by}


def queued_response(task_id: str, message: dict[str, Any]) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "message_id": message["id"],
        "status": "queued",
        "note": (
            "queued, not yet delivered: the run's input pump writes it to the agent — on claude "
            "it is folded into the running turn if one is in progress, on antigravity it is its "
            "own turn. Its event log records a user_message event once written, or an "
            "undelivered event if it never is."
        ),
    }


def _is_seal(entry: Any) -> bool:
    return isinstance(entry, dict) and entry.get("closed") is True and "id" not in entry


def is_closed(log_dir: Path, task_id: str) -> bool:
    """Input is closed: the marker exists, or — where the marker could not be created — the inbox
    itself carries a seal line (see `seal`)."""
    if closed_path(log_dir, task_id).exists():
        return True
    try:
        data = inbox_path(log_dir, task_id).read_bytes()
    except OSError:
        return False
    if b'"closed"' not in data:
        return False
    for line in data.splitlines():
        try:
            if _is_seal(json.loads(line.decode("utf-8"))):
                return True
        except (UnicodeDecodeError, ValueError):
            continue
    return False


def mark_closed(log_dir: Path, task_id: str) -> None:
    """Create the closed marker (idempotent). Raises `OSError` if it cannot be written."""
    path = closed_path(log_dir, task_id)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return
    try:
        os.write(fd, json.dumps({"at": _now_iso()}).encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    control._fsync_dir(log_dir)


def seal(log_dir: Path, task_id: str) -> None:
    """Close input durably, so every sender sees it: the marker, or — if the directory refuses a new
    file — a seal line appended to the inbox itself, which `is_closed` also honours. Call with the
    lock held. Raises `OSError` only if neither could be written."""
    try:
        mark_closed(log_dir, task_id)
        return
    except OSError:
        pass
    append_locked(log_dir, task_id, {"closed": True, "at": _now_iso()})


async def lock_async(log_dir: Path, task_id: str, timeout: float = LOCK_TIMEOUT_SECONDS) -> int:
    """The inbox lock, without ever blocking the event loop. Raises `control.LockTimeout`."""
    return await control.acquire(inbox_path(log_dir, task_id), timeout)


def lock_sync(log_dir: Path, task_id: str, timeout: float = LOCK_TIMEOUT_SECONDS) -> int:
    """The inbox lock, for a worker thread or a CLI. Raises `control.LockTimeout`."""
    path = inbox_path(log_dir, task_id)
    log_dir.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise control.LockTimeout(f"timed out acquiring inbox lock {path}") from None
                time.sleep(_LOCK_POLL_SECONDS)
    except BaseException:
        os.close(fd)
        raise


def unlock(fd: int) -> None:
    control.release(fd)


def has_unread(log_dir: Path, task_id: str, offset: int) -> bool:
    """A cheap, lock-free hint that another process appended past `offset`."""
    try:
        return inbox_path(log_dir, task_id).stat().st_size > offset
    except OSError:
        return False


def read_new(log_dir: Path, task_id: str, offset: int) -> tuple[list[dict[str, Any]], int]:
    """Messages appended after byte `offset`, and the offset just past the last complete line.

    Call with the lock held: every writer appends a whole line under it, so a partial line can only
    be a torn write from a crashed writer — it is left unread rather than parsed, and so is a line
    that is not a message object.
    """
    try:
        with inbox_path(log_dir, task_id).open("rb") as handle:
            handle.seek(offset)
            data = handle.read()
    except FileNotFoundError:
        return [], offset
    end = data.rfind(b"\n")
    if end < 0:
        return [], offset
    messages: list[dict[str, Any]] = []
    for line in data[: end + 1].splitlines():
        try:
            message = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            continue
        if (
            isinstance(message, dict)
            and isinstance(message.get("id"), str)
            and isinstance(message.get("text"), str)
        ):
            messages.append(message)
    return messages, offset + end + 1


def append_locked(log_dir: Path, task_id: str, message: dict[str, Any]) -> None:
    """Append one message line, durably. Call with the lock held.

    A writer that crashed mid-line leaves a fragment with no newline. Appending straight after it
    would merge the two into one unparsable line, and `read_new` would skip an acknowledged
    message along with the fragment — so the fragment is terminated first, becoming a line of its
    own that `read_new` skips.
    """
    line = (json.dumps(message, ensure_ascii=False, default=str) + "\n").encode("utf-8")
    path = inbox_path(log_dir, task_id)
    with path.open("ab") as handle:
        if handle.tell() > 0:
            with path.open("rb") as reader:
                reader.seek(-1, os.SEEK_END)
                if reader.read(1) != b"\n":
                    line = b"\n" + line
        handle.write(line)
        handle.flush()
        os.fsync(handle.fileno())


def send_to_record(
    log_dir: Path,
    task_id: str,
    text: str,
    *,
    by: dict[str, Any] | None,
    timeout: float = LOCK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Queue `text` for a live-input task this process does not own. Blocking: run it in a worker
    thread, or from a CLI. Refusals raise `SendRefused`, checked in this order under the lock: not a
    live-input run, input already closed, task settled, its process confirmed exited, owner not
    confirmed alive.

    The owner check is strict: only `alive` passes. An owner that is gone cannot pump the message,
    and an undecidable one — including a legacy record with no start time — cannot be trusted to.
    """
    store.validate_task_id(task_id)
    try:
        fd = lock_sync(log_dir, task_id, timeout)
    except control.LockTimeout:
        raise SendRefused(
            f"task {task_id}'s inbox is locked by another process; try again shortly",
            code="lock_timeout",
        ) from None
    try:
        record = store.read(log_dir, task_id)
        if record is None:
            raise SendRefused(f"unknown task_id: {task_id}", code="unknown_task")
        _check_open(log_dir, record)
        message = make_message(text, by)
        try:
            append_locked(log_dir, task_id, message)
        except OSError as exc:
            raise SendRefused(
                f"could not queue the message for task {task_id}: {exc}", code="write_failed"
            ) from exc
        # Re-checked after the append: the owner's last-resort close (after its run exited with this
        # lock held too long) creates the marker without the lock and only then reads the inbox. A
        # marker seen now means that read may already have happened, so the message is refused
        # rather than acknowledged; one appended before the marker existed is read and reported.
        if is_closed(log_dir, task_id):
            raise SendRefused(f"task {task_id} has {CLOSED_MESSAGE}", code="closed")
        return queued_response(task_id, message)
    finally:
        unlock(fd)


def _check_open(log_dir: Path, record: store.TaskRecord) -> None:
    task_id = record.task_id
    if not record.live_input:
        raise SendRefused(
            f"task {task_id} was not started with live input, so it cannot take a message while it "
            "runs; continue it with resume_task once it has finished",
            code="not_live_input",
        )
    if is_closed(log_dir, task_id):
        raise SendRefused(f"task {task_id} has {CLOSED_MESSAGE}", code="closed")
    if record.status in store.TERMINAL_RECORD_STATUSES:
        raise SendRefused(
            f"task {task_id} has settled ({record.status}); continue with resume_task",
            code="settled",
        )
    if record.pid is not None:
        leader = identity.task_identity(record.pid, record.start_time, record.markers)
        if identity.identity_check(leader) == "dead":
            # Its record may still say running (the owner has not persisted the outcome yet, or
            # never will), but nothing is left to deliver to.
            raise SendRefused(
                f"task {task_id}'s process has exited; continue with resume_task once it has "
                "settled",
                code="exited",
            )
    verdict = identity.identity_check(record.owner)
    if verdict != "alive":
        raise SendRefused(
            f"task {task_id}'s owning server is not confirmed alive ({verdict}), so nothing would "
            "deliver the message; check get_task_status, and continue with resume_task once it "
            "has settled",
            code="owner_not_alive",
        )
