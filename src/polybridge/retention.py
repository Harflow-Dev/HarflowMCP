"""Retention sweep: delete settled task records and their files once they age out.

Runs at most once per 24h, kicked off lazily by a live polybridge server
(`tasks.TaskRegistry.start_maintenance`) in a background thread — never from `polybridge-ctl`, which
is read-only and never touches disk beyond reading it. Coordinated across concurrently running
servers with a directory-level lock file plus a stamp file, and per-task with a lock file of its own
so a sweep never deletes a record another process (a cancellation, a takeover) is actively working
with.
"""

from __future__ import annotations

import fcntl
import logging
import os
import re
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
        "deleted_temp_files": 0,
    }

    records = store.read_all(log_dir)
    by_id = {r.task_id: r for r in records}
    statuses = {r.task_id: store.resolve_status(log_dir, r, detail=False)[0] for r in records}

    children: dict[str, list[str]] = {}
    for record in records:
        if record.parent_task_id:
            children.setdefault(record.parent_task_id, []).append(record.task_id)

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
        if statuses.get(record.task_id) not in store.TERMINAL_RECORD_STATUSES:
            continue
        if not _older_than(record, days, now):
            continue
        if not _conclusively_settled(record, statuses.get(record.task_id)):
            continue
        if has_live_descendant(record.task_id, set()):
            stats["kept_live_descendant"] += 1
            continue
        _delete_task(log_dir, record, stats, now)

    return stats


def _conclusively_settled(record: store.TaskRecord, status: str | None) -> bool:
    """Settled per `resolve_status`, and — where nothing observed the exit — its process proven gone.

    `resolve_status` leans on `process_alive`, which answers "no" for a live process whose command
    line no longer carries its markers. That is the right lean for reporting a status, but deletion
    is a control decision, and there `undecidable` must never count: an unobserved record is only
    deletable once `identity_check` says `dead`.
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

    Cancel-family activity is delegated to `control.cancel_attempt_active`, which understands the
    `.req`/`.sig`/`.failed` phase vocabulary and lease-based abandoned-controller recovery (A2).
    Takeover keeps the original conservative rule — active unless the latest attempt has a
    `.failed` marker — since A4 owns its own phase semantics. For either family, an attempt number
    that fails to parse as an int still forces this task active regardless, the same conservative
    fallback both families have always had.
    """
    if control.cancel_attempt_active(log_dir, task_id, now):
        return True

    latest_n: dict[str, int] = {}
    forced_active: set[str] = set()
    try:
        entries = list(log_dir.iterdir())
    except OSError:
        return False
    for path in entries:
        match = _ATTEMPT_RE.match(path.name)
        if match is None or match.group("id") != task_id:
            continue
        family = match.group("family")
        try:
            n = int(match.group("n"))
        except ValueError:
            forced_active.add(family)  # unparsable n: treat this family as active regardless
            continue
        if family == control.CANCEL:
            continue  # cancel-family activity was already decided above
        if n > latest_n.get(family, -1):
            latest_n[family] = n

    if forced_active:
        return True
    for family, n in latest_n.items():
        if not (log_dir / f"{task_id}.{family}.{n}.failed").exists():
            return True
    return False


def _delete_task(log_dir: Path, record: store.TaskRecord, stats: dict[str, int], now: datetime) -> None:
    task_id = record.task_id
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

        if _has_active_attempt(log_dir, task_id, now):
            stats["kept_active_attempt"] += 1
            return

        _delete_task_files(log_dir, task_id)
        stats["deleted_tasks"] += 1
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(lock_fd)


def _delete_task_files(log_dir: Path, task_id: str) -> None:
    """Delete every `{task_id}.*` file except the lock, the record (`.meta.json`) last — a crash
    mid-sweep then leaves a record the next sweep retries, rather than orphaned stream files with
    nothing on disk to say they ever belonged to a task."""
    try:
        entries = list(log_dir.iterdir())
    except OSError:
        return
    lock_name = f"{task_id}.lock"
    record_name = f"{task_id}{store.RECORD_SUFFIX}"
    prefix = f"{task_id}."
    record_path: Path | None = None
    for path in entries:
        name = path.name
        if name == lock_name or not name.startswith(prefix):
            continue
        if name == record_name:
            record_path = path
            continue
        try:
            path.unlink()
        except OSError:
            pass
    if record_path is not None:
        try:
            record_path.unlink()
        except OSError:
            pass
