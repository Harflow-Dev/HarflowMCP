"""Retention sweep: delete settled task records and their files once they age out.

Runs at most once per 24h, kicked off lazily by a live polybridge server
(`tasks.TaskRegistry.start_maintenance`) in a background thread — never from `polybridge-ctl`, which
is read-only and never touches disk beyond reading it. Coordinated across concurrently running
servers with a directory-level lock file plus a stamp file, and per-task with a lock file of its own
so a sweep never deletes a record another process (a cancellation, a takeover) is actively working
with.
"""

from __future__ import annotations

import contextlib
import fcntl
import logging
import os
import re
from collections.abc import Collection
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import control, identity, store

log = logging.getLogger(__name__)

DEFAULT_RETENTION_DAYS = 30
SWEEP_INTERVAL_SECONDS = 24 * 3600
TEMP_FILE_MIN_AGE_SECONDS = 3600

# store.write's NamedTemporaryFile prefix is f".{task_id}." — task ids never contain a dot
# (TASK_ID_PATTERN), so splitting on the first dot after the leading one is unambiguous.
_TEMP_FILE_RE = re.compile(r"^\.(?P<id>[^.]+)\.")
_ATTEMPT_RE = re.compile(r"^(?P<id>[^.]+)\.(?P<family>cancel|takeover)\.(?P<n>[^.]+)\.(?P<phase>[^.]+)$")


def retention_days() -> int | None:
    """How many days a settled task's record is kept, or None to disable the sweep entirely."""
    raw = os.environ.get("PB_RETENTION_DAYS")
    if raw is None:
        return DEFAULT_RETENTION_DAYS
    try:
        value = int(raw)
    except ValueError:
        log.warning("PB_RETENTION_DAYS=%r is not an integer; retention disabled", raw)
        return None
    if value <= 0:
        if value < 0:
            log.warning("PB_RETENTION_DAYS=%r is negative; retention disabled", raw)
        return None
    return value


def maybe_sweep(log_dir: Path, *, now: datetime | None = None) -> dict[str, int] | None:
    """Entry point run in a background thread by `TaskRegistry.start_maintenance`.

    Never raises: a failed sweep must not take down the server that kicked it off, nor say
    anything about whether a task actually finished. Returns None when no sweep ran (disabled,
    another process already holds the lock, or the last sweep was too recent) — the caller cannot
    tell those apart from the return value alone, which is fine, since none of them are errors.
    """
    try:
        return _maybe_sweep(log_dir, now=now)
    except Exception:
        log.exception("retention sweep failed")
        return None


def _maybe_sweep(log_dir: Path, *, now: datetime | None) -> dict[str, int] | None:
    days = retention_days()
    if days is None:
        return None

    resolved_now = now or datetime.now(timezone.utc)
    root = log_dir.parent
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / "maintenance.lock"
    stamp_path = root / "maintenance.stamp"

    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Another server process is sweeping right now.
            return None

        if stamp_path.exists():
            try:
                stamp_age = resolved_now - datetime.fromtimestamp(
                    stamp_path.stat().st_mtime, tz=timezone.utc
                )
            except OSError:
                stamp_age = None
            if stamp_age is not None and stamp_age < timedelta(seconds=SWEEP_INTERVAL_SECONDS):
                return None

        stats = sweep(log_dir, days, resolved_now)
        try:
            stamp_path.write_text(resolved_now.isoformat(), encoding="utf-8")
        except OSError:
            log.warning("could not write %s after a sweep", stamp_path, exc_info=True)
        return stats
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(lock_fd)


def sweep(log_dir: Path, days: int, now: datetime) -> dict[str, int]:
    """One retention pass. Never raises — the caller (`maybe_sweep`) is the one that guarantees it."""
    stats = {
        "deleted_tasks": 0,
        "kept_live_descendant": 0,
        "kept_active_attempt": 0,
        "kept_locked": 0,
        "kept_delete_failed": 0,
        "deleted_temp_files": 0,
    }

    from .workflows import WorkflowStore
    pinned = WorkflowStore(root=log_dir.parent).pinned_tasks()
    records = store.read_all(log_dir)
    by_id = {r.task_id: r for r in records}
    statuses = {r.task_id: store.resolve_status(log_dir, r, detail=False)[0] for r in records}

    # Both edges count as descent: `parent_task_id` (resumed from) and `spawned_by` (dispatched by,
    # from a nested call). A child that names its ancestor only through `spawned_by` must still
    # keep that ancestor's record alive while it runs.
    children: dict[str, list[str]] = {}
    for record in records:
        for ancestor in {record.parent_task_id, record.spawned_by} - {None}:
            children.setdefault(ancestor, []).append(record.task_id)

    def has_live_descendant(task_id: str, seen: set[str]) -> bool:
        for child_id in children.get(task_id, ()):
            if child_id in seen:
                continue
            seen.add(child_id)
            # Not merely `== "running"`: a child whose process cannot be proven gone keeps its
            # ancestors too, by the same rule that keeps the child itself.
            child = by_id.get(child_id)
            if child is None or not _conclusively_settled(child, statuses.get(child_id)):
                return True
            if has_live_descendant(child_id, seen):
                return True
        return False

    _sweep_temp_files(log_dir, by_id, statuses, now, stats)

    for record in records:
        if record.task_id in pinned:
            continue
        if statuses.get(record.task_id) not in store.TERMINAL_RECORD_STATUSES:
            continue
        if not _older_than(record, days, now):
            continue
        if not _conclusively_settled(record, statuses.get(record.task_id)):
            continue
        if has_live_descendant(record.task_id, set()):
            stats["kept_live_descendant"] += 1
            continue
        _delete_task(log_dir, record, stats, now, _family(record.task_id, children), by_id)

    return stats


def _conclusively_settled(record: store.TaskRecord, status: str | None) -> bool:
    """Settled per `resolve_status`, and — where nothing observed the exit — its process proven gone.

    `resolve_status` leans on `record_process_alive`, which answers "yes" for a process whose
    recorded start time matches — even when its command line no longer carries its markers
    (a process that renamed itself after spawn), and falls back to `process_alive`'s pid+markers
    test for a record with no start time. That is the right lean for reporting a status — never
    manufacture a false "gone" — but deletion is a control decision, and there `undecidable` must
    never count: an unobserved record is only deletable once `identity_check` says `dead`.
    """
    if status not in store.TERMINAL_RECORD_STATUSES:
        return False
    if not store.outcome_unobserved(record):
        return True
    task_identity = {"pid": record.pid, "start_time": record.start_time, "markers": record.markers}
    return identity.identity_check(task_identity) == "dead"


def _older_than(record: store.TaskRecord, days: int, now: datetime) -> bool:
    source = record.finished_at or record.started_at
    if not source:
        return False
    try:
        age_dt = datetime.fromisoformat(source)
    except ValueError:
        return False
    if age_dt.tzinfo is None:
        age_dt = age_dt.replace(tzinfo=timezone.utc)
    try:
        age = now - age_dt
    except TypeError:
        return False
    return age > timedelta(days=days)


def _sweep_temp_files(
    log_dir: Path,
    by_id: dict[str, store.TaskRecord],
    statuses: dict[str, str],
    now: datetime,
    stats: dict[str, int],
) -> None:
    """Deleted first, while records still exist, so a settled task's own deletion below cannot
    race a leftover temp file check that needs the record to still be readable."""
    try:
        entries = list(log_dir.iterdir())
    except OSError:
        return
    for path in entries:
        match = _TEMP_FILE_RE.match(path.name)
        if match is None:
            continue
        task_id = match.group("id")
        try:
            store.validate_task_id(task_id)
        except store.InvalidTaskId:
            continue
        if task_id not in by_id or not _conclusively_settled(by_id[task_id], statuses.get(task_id)):
            continue
        try:
            age_seconds = now.timestamp() - path.stat().st_mtime
        except OSError:
            continue
        if age_seconds <= TEMP_FILE_MIN_AGE_SECONDS:
            continue
        try:
            path.unlink()
            stats["deleted_temp_files"] += 1
        except OSError:
            pass


def _has_active_attempt(log_dir: Path, task_id: str, now: datetime | None = None) -> bool:
    """Whether the latest cancel or takeover attempt for `task_id` has not yet finished.

    Each family's own rule decides: `control.cancel_attempt_active` (a pending attempt whose
    controller is not provably abandoned, A2) and `control.takeover_attempt_active` (the attempt
    still holds its session, A4 — deleting its phase files then would drop the reservation while
    the user's terminal is open). For either family, an attempt number that fails to parse as an int
    still forces this task active regardless, the same conservative fallback both have always had.
    """
    if control.cancel_attempt_active(log_dir, task_id, now):
        return True
    if control.takeover_attempt_active(log_dir, task_id, now):
        return True

    try:
        entries = list(log_dir.iterdir())
    except OSError:
        return False
    for path in entries:
        match = _ATTEMPT_RE.match(path.name)
        if match is None or match.group("id") != task_id:
            continue
        try:
            int(match.group("n"))
        except ValueError:
            return True  # unparsable n: treat this task as active regardless
    return False


def _family(task_id: str, children: dict[str, list[str]]) -> frozenset[str]:
    """`task_id` and every descendant the snapshot knows of, settled or not."""
    family = {task_id}
    stack = [task_id]
    while stack:
        for child_id in children.get(stack.pop(), ()):
            if child_id not in family:
                family.add(child_id)
                stack.append(child_id)
    return frozenset(family)


def _has_fresh_descendant(
    log_dir: Path, family: frozenset[str], snapshot_ids: Collection[str]
) -> bool:
    """Whether a record written after the sweep's snapshot descends from anything in `family`.

    The snapshot's own descendants were already judged in `sweep` (`has_live_descendant`), so only
    a record that appeared since can still save the task — a resume's child, written under a
    session lock the caller now holds. It may name any family member, not just the task itself:
    resuming a settled grandchild produces a record naming that grandchild. A fresh record whose
    parent is another fresh record needs no second pass, since that parent is fresh too and names
    the family itself. Only new ids are read, never the whole store again; one that cannot be read
    counts, since unreadable is not provably unrelated.
    """
    try:
        entries = list(log_dir.iterdir())
    except OSError:
        return True
    for path in entries:
        name = path.name
        if not name.endswith(store.RECORD_SUFFIX):
            continue
        child_id = name[: -len(store.RECORD_SUFFIX)]
        try:
            store.validate_task_id(child_id)
        except store.InvalidTaskId:
            continue  # not a task record at all, whatever it is
        if child_id in snapshot_ids:
            continue
        child = store.read(log_dir, child_id)
        if child is None or {child.parent_task_id, child.spawned_by} & family:
            return True
    return False


def _delete_task(
    log_dir: Path,
    record: store.TaskRecord,
    stats: dict[str, int],
    now: datetime,
    family: frozenset[str],
    by_id: dict[str, store.TaskRecord],
) -> None:
    task_id = record.task_id
    # Session locks first, then `<id>.lock` — the invariant order. A resume holds its session lock
    # across check-and-spawn, and resuming *any* family member creates a descendant of this task, so
    # every family session is taken — they differ once a `spawned_by` child is involved. Each is
    # non-blocking: a busy one means such a child may be moments from existing, and the task stays
    # for a later sweep that will see it. Non-blocking also means no ordering can deadlock.
    sessions = sorted(
        {by_id[m].session_id for m in family if m in by_id and by_id[m].session_id}
    )
    with contextlib.ExitStack() as locks:
        for session_id in sessions:
            try:
                locks.enter_context(control.session_lock_sync(log_dir, session_id, timeout=0))
            except (control.LockTimeout, OSError):
                stats["kept_locked"] += 1
                return

        try:
            lock_fd = os.open(log_dir / f"{task_id}.lock", os.O_CREAT | os.O_RDWR, 0o644)
        except OSError:
            # Without the lock nothing may be deleted, but one unopenable lock must not abort the
            # whole sweep — and with it the stamp, which would retry the same failure every start.
            stats["kept_locked"] += 1
            return
        try:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                stats["kept_locked"] += 1
                return

            fresh = store.read(log_dir, task_id)
            if fresh is None:
                return  # already gone — another sweep, or a takeover, got there first
            if not _conclusively_settled(fresh, store.resolve_status(log_dir, fresh, detail=False)[0]):
                return

            if _has_fresh_descendant(log_dir, family, by_id.keys()):
                stats["kept_live_descendant"] += 1
                return

            if _has_active_attempt(log_dir, task_id, now):
                stats["kept_active_attempt"] += 1
                return

            # Emptied first: once the record is gone no later sweep can find this task again, so an
            # inbox that cannot be emptied now (its lock is busy) keeps the whole task for a retry.
            if not _empty_inbox(log_dir, task_id):
                stats["kept_locked"] += 1
                return
            if not _delete_task_files(log_dir, task_id):
                stats["kept_delete_failed"] += 1
                return
            stats["deleted_tasks"] += 1
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(lock_fd)


def _empty_inbox(log_dir: Path, task_id: str) -> bool:
    """Drop the message payloads a settled task's inbox still holds, keeping the file itself — it
    is a lock file, and lock files are never deleted. Done under the inbox's own lock. False when
    that could not be done (the lock is busy, or truncation failed), so the caller keeps the task
    for a later sweep; True when there was no inbox at all."""
    path = log_dir / f"{task_id}.inbox.jsonl"
    try:
        fd = os.open(path, os.O_RDWR)
    except FileNotFoundError:
        return True
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        try:
            os.ftruncate(fd, 0)
            return True
        except OSError:
            log.debug("could not empty the inbox of %s", task_id, exc_info=True)
            return False
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
    finally:
        os.close(fd)


def _delete_task_files(log_dir: Path, task_id: str) -> bool:
    """Delete every `{task_id}.*` file except the lock files — `.lock`, and `.inbox.jsonl`, which is
    the inbox's `flock` target (see `inbox.py`) — with the record (`.meta.json`) last, and only once
    everything else is gone: the record is what lets a later sweep find this task again, so removing
    it past a failed unlink would orphan that file (prompts, raw output) for good. False when
    anything, the record included, was left behind."""
    try:
        entries = list(log_dir.iterdir())
    except OSError:
        return False
    lock_names = {f"{task_id}.lock", f"{task_id}.inbox.jsonl"}
    record_name = f"{task_id}{store.RECORD_SUFFIX}"
    prefix = f"{task_id}."
    record_path: Path | None = None
    complete = True
    for path in entries:
        name = path.name
        if name in lock_names or not name.startswith(prefix):
            continue
        if name == record_name:
            record_path = path
            continue
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            complete = False
    if not complete:
        return False
    try:
        from . import scratch
        scratch.remove(log_dir, task_id)
    except OSError:
        return False  # Retain the record so cleanup can be retried safely.
    if record_path is not None:
        try:
            record_path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            return False
    return True
