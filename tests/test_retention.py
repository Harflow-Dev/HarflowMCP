"""Retention sweep: deleting settled task records and their files once they age out."""

from __future__ import annotations

import fcntl
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from polybridge import retention, store


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def make_record(**overrides) -> store.TaskRecord:
    now = datetime.now(timezone.utc)
    base = {
        "task_id": "task-1",
        "backend": "claude",
        "session_id": "s1",
        "markers": ["s1"],
        "repo_path": "/tmp/repo",
        "started_at": _iso(now - timedelta(days=40)),
        "status": "completed",
        "exit_code": 0,
        "finished_at": _iso(now - timedelta(days=40)),
    }
    return store.TaskRecord(**(base | overrides))


# --- retention_days --------------------------------------------------------------------------


def test_default_retention_is_30_days(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PB_RETENTION_DAYS", raising=False)
    assert retention.retention_days() == 30


def test_retention_disabled_by_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PB_RETENTION_DAYS", "0")
    assert retention.retention_days() is None


def test_retention_disabled_by_an_invalid_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PB_RETENTION_DAYS", "not-a-number")
    assert retention.retention_days() is None


def test_retention_disabled_by_a_negative_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PB_RETENTION_DAYS", "-5")
    assert retention.retention_days() is None


def test_a_positive_value_is_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PB_RETENTION_DAYS", "7")
    assert retention.retention_days() == 7


# --- maybe_sweep coordination -----------------------------------------------------------------


def test_maybe_sweep_writes_the_stamp_after_the_sweep_not_before(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    stamp_path = tmp_path / "maintenance.stamp"

    def fake_sweep(ld, days, now):
        assert not stamp_path.exists(), "the stamp must not exist while the sweep is still running"
        return {"deleted_tasks": 0}

    monkeypatch.setattr(retention, "sweep", fake_sweep)

    stats = retention.maybe_sweep(log_dir)

    assert stats == {"deleted_tasks": 0}
    assert stamp_path.exists()


def test_maybe_sweep_does_nothing_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PB_RETENTION_DAYS", "0")
    log_dir = tmp_path / "tasks"

    assert retention.maybe_sweep(log_dir) is None
    assert not (tmp_path / "maintenance.lock").exists()


def test_maybe_sweep_returns_none_when_another_process_holds_the_lock(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir(parents=True)
    lock_fd = os.open(tmp_path / "maintenance.lock", os.O_CREAT | os.O_RDWR, 0o644)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert retention.maybe_sweep(log_dir) is None
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def test_maybe_sweep_is_skipped_when_the_stamp_is_recent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir(parents=True)
    (tmp_path / "maintenance.stamp").write_text(datetime.now(timezone.utc).isoformat())

    called = False

    def fake_sweep(*args, **kwargs):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(retention, "sweep", fake_sweep)

    assert retention.maybe_sweep(log_dir) is None
    assert not called


def test_maybe_sweep_never_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir(parents=True)

    def boom(*args, **kwargs):
        raise RuntimeError("sweep exploded")

    monkeypatch.setattr(retention, "sweep", boom)

    assert retention.maybe_sweep(log_dir) is None


# --- sweep -------------------------------------------------------------------------------------


def test_sweep_deletes_an_old_settled_task_but_never_its_lock_file(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record()
    store.write(log_dir, record)
    (log_dir / f"{record.task_id}.jsonl").write_text("raw\n")
    (log_dir / f"{record.task_id}.events.jsonl").write_text('{"kind":"task_started"}\n')

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["deleted_tasks"] == 1
    assert store.read(log_dir, record.task_id) is None
    assert not (log_dir / f"{record.task_id}.jsonl").exists()
    assert not (log_dir / f"{record.task_id}.events.jsonl").exists()
    assert (log_dir / f"{record.task_id}.lock").exists()


def test_sweep_keeps_the_inbox_lock_file_but_deletes_the_closed_marker(tmp_path: Path) -> None:
    """`<id>.inbox.jsonl` is the live-input inbox's flock target, and lock files are never deleted."""
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record()
    store.write(log_dir, record)
    inbox_file = log_dir / f"{record.task_id}.inbox.jsonl"
    inbox_file.write_text('{"id": "m1", "text": "a private message"}\n')
    inode = inbox_file.stat().st_ino
    (log_dir / f"{record.task_id}.inbox.closed").write_text("{}")

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["deleted_tasks"] == 1
    assert inbox_file.exists()
    assert inbox_file.stat().st_ino == inode  # the same lock file, never replaced
    assert inbox_file.read_bytes() == b""  # but its message payloads are gone
    assert not (log_dir / f"{record.task_id}.inbox.closed").exists()


def test_sweep_keeps_a_young_task(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    now = datetime.now(timezone.utc)
    record = make_record(
        started_at=_iso(now - timedelta(days=1)), finished_at=_iso(now - timedelta(days=1))
    )
    store.write(log_dir, record)

    stats = retention.sweep(log_dir, 30, now)

    assert stats["deleted_tasks"] == 0
    assert store.read(log_dir, record.task_id) is not None


def test_sweep_keeps_a_running_task_regardless_of_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    now = datetime.now(timezone.utc)
    record = make_record(
        status="running",
        exit_code=None,
        finished_at=None,
        started_at=_iso(now - timedelta(days=40)),
        pid=4242,
    )
    store.write(log_dir, record)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: True)

    stats = retention.sweep(log_dir, 30, now)

    assert stats["deleted_tasks"] == 0
    assert store.read(log_dir, record.task_id) is not None


def test_sweep_keeps_a_settled_old_task_with_a_running_descendant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    now = datetime.now(timezone.utc)
    parent = make_record(
        task_id="parent",
        pid=1,
        started_at=_iso(now - timedelta(days=40)),
        finished_at=_iso(now - timedelta(days=40)),
    )
    child = make_record(
        task_id="child",
        pid=2,
        parent_task_id="parent",
        status="running",
        exit_code=None,
        finished_at=None,
        started_at=_iso(now),
    )
    store.write(log_dir, parent)
    store.write(log_dir, child)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: pid == 2)

    stats = retention.sweep(log_dir, 30, now)

    assert stats["kept_live_descendant"] == 1
    assert stats["deleted_tasks"] == 0
    assert store.read(log_dir, "parent") is not None


def test_sweep_keeps_a_task_with_an_active_cancel_attempt(tmp_path: Path) -> None:
    """An unparsable `.req` (empty, not JSON) can never be proven abandoned — `controller_abandoned`
    refuses to guess — so `cancel_attempt_active` stays True and the task is kept."""
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record()
    store.write(log_dir, record)
    (log_dir / f"{record.task_id}.cancel.1.req").write_text("")

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["kept_active_attempt"] == 1
    assert stats["deleted_tasks"] == 0
    assert store.read(log_dir, record.task_id) is not None


def test_sweep_deletes_a_task_whose_latest_attempt_has_failed(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record()
    store.write(log_dir, record)
    (log_dir / f"{record.task_id}.cancel.1.req").write_text("")
    (log_dir / f"{record.task_id}.cancel.1.failed").write_text("")

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["deleted_tasks"] == 1
    assert store.read(log_dir, record.task_id) is None


def test_sweep_deletes_a_task_whose_latest_cancel_attempt_was_signalled(tmp_path: Path) -> None:
    """A `.sig` settles the attempt regardless of what the `.req` says — `attempt_outcome` checks
    existence, not payload content, for `.sig`/`.failed`."""
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record()
    store.write(log_dir, record)
    (log_dir / f"{record.task_id}.cancel.1.req").write_text("{}")
    (log_dir / f"{record.task_id}.cancel.1.sig").write_text('{"leader_alive": false}')

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["deleted_tasks"] == 1
    assert store.read(log_dir, record.task_id) is None


def test_sweep_keeps_a_task_whose_cancel_controller_is_still_live(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Even with an expired lease, a controller that is confirmed `alive` (not `dead`) is never
    declared abandoned — the lease alone is not enough."""
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    now = datetime.now(timezone.utc)
    record = make_record()
    store.write(log_dir, record)
    req = {"at": _iso(now - timedelta(seconds=120)), "by": {"pid": 999}, "lease_seconds": 60}
    (log_dir / f"{record.task_id}.cancel.1.req").write_text(json.dumps(req))
    monkeypatch.setattr(retention.control.identity, "identity_check", lambda identity: "alive")

    stats = retention.sweep(log_dir, 30, now)

    assert stats["kept_active_attempt"] == 1
    assert stats["deleted_tasks"] == 0
    assert store.read(log_dir, record.task_id) is not None


def test_sweep_deletes_a_task_whose_cancel_controller_died_with_an_expired_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    now = datetime.now(timezone.utc)
    record = make_record()
    store.write(log_dir, record)
    req = {"at": _iso(now - timedelta(seconds=120)), "by": {"pid": 999}, "lease_seconds": 60}
    (log_dir / f"{record.task_id}.cancel.1.req").write_text(json.dumps(req))
    monkeypatch.setattr(retention.control.identity, "identity_check", lambda identity: "dead")

    stats = retention.sweep(log_dir, 30, now)

    assert stats["deleted_tasks"] == 1
    assert store.read(log_dir, record.task_id) is None


def test_takeover_family_semantics_are_unchanged_by_the_cancel_rewrite(tmp_path: Path) -> None:
    """Cancel now routes through `control.cancel_attempt_active`; takeover still uses the original
    "active unless the latest attempt has `.failed`" rule this task does not own."""
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record()
    store.write(log_dir, record)
    (log_dir / f"{record.task_id}.takeover.1.requested").write_text("")

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["kept_active_attempt"] == 1
    assert stats["deleted_tasks"] == 0
    assert store.read(log_dir, record.task_id) is not None


def test_an_unparsable_cancel_attempt_number_still_forces_the_task_active(tmp_path: Path) -> None:
    """"x" fails retention's own `int()` parse (unlike control.py's stricter numbering, this rule
    predates A2 and is only about Python's own int() conversion) and so forces the family active,
    even though `control.cancel_attempt_active` cannot see this attempt at all — its own
    `latest_attempt` silently ignores any n outside `[1-9][0-9]*`."""
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record()
    store.write(log_dir, record)
    (log_dir / f"{record.task_id}.cancel.x.req").write_text("{}")

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["kept_active_attempt"] == 1
    assert stats["deleted_tasks"] == 0
    assert store.read(log_dir, record.task_id) is not None


def test_sweep_keeps_a_task_whose_lock_is_held_by_another_process(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record()
    store.write(log_dir, record)
    lock_fd = os.open(log_dir / f"{record.task_id}.lock", os.O_CREAT | os.O_RDWR, 0o644)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))
        assert stats["kept_locked"] == 1
        assert stats["deleted_tasks"] == 0
        assert store.read(log_dir, record.task_id) is not None
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def test_sweep_deletes_an_old_temp_file_belonging_to_a_settled_task(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    now = datetime.now(timezone.utc)
    record = make_record(
        started_at=_iso(now - timedelta(days=1)), finished_at=_iso(now - timedelta(days=1))
    )
    store.write(log_dir, record)
    temp = log_dir / f".{record.task_id}.abcdef"
    temp.write_text("partial")
    old_time = (now - timedelta(hours=2)).timestamp()
    os.utime(temp, (old_time, old_time))

    stats = retention.sweep(log_dir, 30, now)

    assert stats["deleted_temp_files"] == 1
    assert not temp.exists()


def test_sweep_keeps_a_young_temp_file(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record()
    store.write(log_dir, record)
    temp = log_dir / f".{record.task_id}.abcdef"
    temp.write_text("partial")

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["deleted_temp_files"] == 0
    assert temp.exists()


def test_sweep_keeps_a_recordless_temp_file_even_if_old(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    temp = log_dir / ".unknown-task.abcdef"
    temp.write_text("partial")
    old_time = (datetime.now(timezone.utc) - timedelta(hours=2)).timestamp()
    os.utime(temp, (old_time, old_time))

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["deleted_temp_files"] == 0
    assert temp.exists()


def test_an_unopenable_task_lock_keeps_that_task_and_the_sweep_carries_on(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    blocked = make_record(task_id="aaaaaaaa-0000-4000-8000-000000000001")
    other = make_record(task_id="aaaaaaaa-0000-4000-8000-000000000002")
    store.write(log_dir, blocked)
    store.write(log_dir, other)
    # A directory where the lock file should be makes os.open fail with an OSError.
    (log_dir / f"{blocked.task_id}.lock").mkdir()

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["kept_locked"] == 1
    assert stats["deleted_tasks"] == 1
    assert store.read(log_dir, blocked.task_id) is not None
    assert store.read(log_dir, other.task_id) is None


@pytest.mark.parametrize(("verdict", "deleted"), [("undecidable", 0), ("alive", 0), ("dead", 1)])
def test_an_unobserved_record_is_deleted_only_once_its_process_is_proven_dead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, verdict: str, deleted: int
) -> None:
    """`resolve_status` reads a live process whose command line lost its markers as gone, and so
    settles the record as `failed` — but deletion is a control decision, where undecidable never
    counts."""
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record(status="running", exit_code=None, finished_at=None, pid=4242)
    store.write(log_dir, record)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: False)
    monkeypatch.setattr(retention.identity, "identity_check", lambda identity: verdict)

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["deleted_tasks"] == deleted
    assert (store.read(log_dir, record.task_id) is None) == bool(deleted)


def test_an_old_temp_file_of_an_unobserved_record_is_kept_while_its_identity_is_undecidable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    now = datetime.now(timezone.utc)
    record = make_record(
        status="failed", exit_code=None, started_at=_iso(now), finished_at=_iso(now), pid=4242
    )
    store.write(log_dir, record)
    temp = log_dir / f".{record.task_id}.abc123"
    temp.write_text("{}")
    old = now.timestamp() - 7200
    os.utime(temp, (old, old))
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: False)
    monkeypatch.setattr(retention.identity, "identity_check", lambda identity: "undecidable")

    stats = retention.sweep(log_dir, 30, now)

    assert stats["deleted_temp_files"] == 0
    assert temp.exists()


def test_a_parent_is_kept_while_its_unobserved_childs_identity_is_undecidable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The child resolves as `failed` (its markers vanished) but cannot be proven dead, so it may
    still be running — and a task with a possibly-live descendant is kept."""
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    parent = make_record(task_id="parent-1")
    child = make_record(
        task_id="child-1", parent_task_id="parent-1", status="running", exit_code=None,
        finished_at=None, pid=4242,
    )
    store.write(log_dir, parent)
    store.write(log_dir, child)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: False)
    monkeypatch.setattr(retention.identity, "identity_check", lambda identity: "undecidable")

    stats = retention.sweep(log_dir, 30, datetime.now(timezone.utc))

    assert stats["deleted_tasks"] == 0
    assert stats["kept_live_descendant"] == 1
    assert store.read(log_dir, "parent-1") is not None


def test_sweep_keeps_an_aged_root_whose_live_child_names_it_only_via_spawned_by(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A nested dispatch records its caller as `spawned_by`, not `parent_task_id`; the root must
    not be deleted while that child — or its own child — still runs."""
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    now = datetime.now(timezone.utc)
    store.write(log_dir, make_record(task_id="root", pid=1))
    store.write(log_dir, make_record(task_id="middle", pid=2, spawned_by="root"))
    store.write(
        log_dir,
        make_record(
            task_id="leaf",
            pid=3,
            spawned_by="middle",
            status="running",
            exit_code=None,
            finished_at=None,
            started_at=_iso(now),
        ),
    )
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: pid == 3)

    stats = retention.sweep(log_dir, 30, now)

    assert store.read(log_dir, "root") is not None
    assert store.read(log_dir, "middle") is not None
    assert stats["kept_live_descendant"] == 2
