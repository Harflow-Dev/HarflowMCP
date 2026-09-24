"""Cross-process control: phase files, attempts, lease recovery, and the two locks."""

from __future__ import annotations

import asyncio
import fcntl
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from polybridge import control, store


def make_record(**overrides) -> store.TaskRecord:
    now = datetime.now(timezone.utc)
    base = {
        "task_id": "task-1",
        "backend": "claude",
        "session_id": "s1",
        "repo_path": "/tmp/repo",
        "started_at": now.isoformat(),
        "status": "running",
    }
    return store.TaskRecord(**(base | overrides))


# --- write_phase / read_phase -------------------------------------------------------------------


def test_write_phase_creates_the_file_and_returns_true(tmp_path: Path) -> None:
    created = control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", {"a": 1})

    assert created is True
    payload = control.read_phase(tmp_path, "t1", control.CANCEL, 1, "req")
    assert payload == {"a": 1}
    assert list(tmp_path.glob(".t1.*")) == []


def test_write_phase_returns_false_and_keeps_the_original_on_eexist(tmp_path: Path) -> None:
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", {"a": 1})

    created_again = control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", {"a": 2})

    assert created_again is False
    assert control.read_phase(tmp_path, "t1", control.CANCEL, 1, "req") == {"a": 1}
    assert list(tmp_path.glob(".t1.*")) == []


def test_write_phase_raises_phase_write_error_on_a_real_failure_and_cleans_up_the_temp_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(src, dst):
        raise PermissionError("nope")

    monkeypatch.setattr(control.os, "link", boom)

    with pytest.raises(control.PhaseWriteError):
        control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", {"a": 1})

    assert list(tmp_path.glob(".t1.*")) == []
    assert not control.phase_path(tmp_path, "t1", control.CANCEL, 1, "req").exists()


def test_read_phase_is_none_for_a_missing_file(tmp_path: Path) -> None:
    assert control.read_phase(tmp_path, "t1", control.CANCEL, 1, "req") is None


def test_read_phase_is_unparsable_for_invalid_json(tmp_path: Path) -> None:
    control.phase_path(tmp_path, "t1", control.CANCEL, 1, "req").write_text("not json")

    assert control.read_phase(tmp_path, "t1", control.CANCEL, 1, "req") is control.UNPARSABLE


def test_read_phase_is_unparsable_for_a_non_object_json_value(tmp_path: Path) -> None:
    control.phase_path(tmp_path, "t1", control.CANCEL, 1, "req").write_text("[1, 2]")

    assert control.read_phase(tmp_path, "t1", control.CANCEL, 1, "req") is control.UNPARSABLE


# --- latest_attempt / attempt_outcome -------------------------------------------------------


def test_latest_attempt_is_none_with_nothing_on_disk(tmp_path: Path) -> None:
    assert control.latest_attempt(tmp_path, "t1", control.CANCEL) is None


def test_latest_attempt_compares_numerically_not_lexically(tmp_path: Path) -> None:
    control.write_phase(tmp_path, "t1", control.CANCEL, 9, "req", {})
    control.write_phase(tmp_path, "t1", control.CANCEL, 10, "req", {})

    assert control.latest_attempt(tmp_path, "t1", control.CANCEL) == 10


@pytest.mark.parametrize("bad_n", ["01", "x", "1a", "-1", "0"])
def test_latest_attempt_ignores_an_invalid_n(tmp_path: Path, bad_n: str) -> None:
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", {})
    (tmp_path / f"t1.cancel.{bad_n}.req").write_text("{}")

    assert control.latest_attempt(tmp_path, "t1", control.CANCEL) == 1


def test_attempt_outcome_is_pending_with_only_a_req(tmp_path: Path) -> None:
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", {})

    assert control.attempt_outcome(tmp_path, "t1", control.CANCEL, 1) == "pending"


def test_attempt_outcome_is_failed_once_failed_exists(tmp_path: Path) -> None:
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "failed", {})

    assert control.attempt_outcome(tmp_path, "t1", control.CANCEL, 1) == "failed"


def test_attempt_outcome_sig_wins_over_failed(tmp_path: Path) -> None:
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "sig", {})
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "failed", {})

    assert control.attempt_outcome(tmp_path, "t1", control.CANCEL, 1) == "sig"


# --- controller_abandoned / recover_abandoned ------------------------------------------------


def test_controller_abandoned_false_for_an_unparsable_payload() -> None:
    assert control.controller_abandoned(control.UNPARSABLE, datetime.now(timezone.utc)) is False
    assert control.controller_abandoned(None, datetime.now(timezone.utc)) is False


def test_controller_abandoned_false_before_the_lease_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "dead")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    req = {"at": (now - timedelta(seconds=10)).isoformat(), "by": {"pid": 1}, "lease_seconds": 60}

    assert control.controller_abandoned(req, now) is False


def test_controller_abandoned_false_when_identity_is_undecidable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "undecidable")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    req = {"at": (now - timedelta(seconds=120)).isoformat(), "by": {"pid": 1}, "lease_seconds": 60}

    assert control.controller_abandoned(req, now) is False


def test_controller_abandoned_true_once_lease_expired_and_identity_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "dead")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    req = {"at": (now - timedelta(seconds=120)).isoformat(), "by": {"pid": 1}, "lease_seconds": 60}

    assert control.controller_abandoned(req, now) is True


def test_recover_abandoned_does_nothing_for_a_non_pending_attempt(tmp_path: Path) -> None:
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "failed", {})

    assert control.recover_abandoned(tmp_path, "t1", control.CANCEL, 1) is False


def test_recover_abandoned_never_recovers_an_unparsable_req(tmp_path: Path) -> None:
    control.phase_path(tmp_path, "t1", control.CANCEL, 1, "req").write_text("not json")

    assert control.recover_abandoned(tmp_path, "t1", control.CANCEL, 1) is False
    assert control.attempt_outcome(tmp_path, "t1", control.CANCEL, 1) == "pending"


def test_recover_abandoned_writes_failed_once_abandoned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "dead")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    req = {"at": (now - timedelta(seconds=120)).isoformat(), "by": {"pid": 1}, "lease_seconds": 60}
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", req)

    assert control.recover_abandoned(tmp_path, "t1", control.CANCEL, 1, now) is True
    failed = control.read_phase(tmp_path, "t1", control.CANCEL, 1, "failed")
    assert failed["reason"] == "controller died"


# --- begin_attempt -----------------------------------------------------------------------------


def test_begin_attempt_opens_attempt_one_when_none_exists(tmp_path: Path) -> None:
    attempt = control.begin_attempt(tmp_path, "t1", control.CANCEL, {"pid": 1})

    assert attempt == control.Attempt(n=1, owned=True)
    assert control.attempt_outcome(tmp_path, "t1", control.CANCEL, 1) == "pending"


def test_begin_attempt_joins_a_pending_attempt_with_a_live_controller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "alive")
    first = control.begin_attempt(tmp_path, "t1", control.CANCEL, {"pid": 1})
    assert first == control.Attempt(n=1, owned=True)

    second = control.begin_attempt(tmp_path, "t1", control.CANCEL, {"pid": 2})

    assert second == control.Attempt(n=1, owned=False)


def test_begin_attempt_joins_an_already_signalled_attempt(tmp_path: Path) -> None:
    control.write_phase(
        tmp_path, "t1", control.CANCEL, 1, "req", {"at": "x", "by": {}, "lease_seconds": 60}
    )
    control.mark_signalled(tmp_path, "t1", control.CANCEL, 1, leader_alive=True)

    attempt = control.begin_attempt(tmp_path, "t1", control.CANCEL, {"pid": 1})

    assert attempt == control.Attempt(n=1, owned=False)


def test_begin_attempt_opens_a_new_attempt_once_the_latest_has_failed(tmp_path: Path) -> None:
    control.write_phase(
        tmp_path, "t1", control.CANCEL, 1, "req", {"at": "x", "by": {}, "lease_seconds": 60}
    )
    control.mark_failed(tmp_path, "t1", control.CANCEL, 1, reason="gave up")

    attempt = control.begin_attempt(tmp_path, "t1", control.CANCEL, {"pid": 1})

    assert attempt == control.Attempt(n=2, owned=True)


def test_begin_attempt_recovers_an_abandoned_controller_and_opens_attempt_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "dead")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    old_req = {"at": (now - timedelta(seconds=120)).isoformat(), "by": {"pid": 1}, "lease_seconds": 60}
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", old_req)

    attempt = control.begin_attempt(tmp_path, "t1", control.CANCEL, {"pid": 2}, now=now)

    assert attempt == control.Attempt(n=2, owned=True)
    failed = control.read_phase(tmp_path, "t1", control.CANCEL, 1, "failed")
    assert failed["reason"] == "controller died"


def test_begin_attempt_does_not_recover_before_the_lease_expires(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "dead")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    fresh_req = {"at": (now - timedelta(seconds=5)).isoformat(), "by": {"pid": 1}, "lease_seconds": 60}
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", fresh_req)

    attempt = control.begin_attempt(tmp_path, "t1", control.CANCEL, {"pid": 2}, now=now)

    assert attempt == control.Attempt(n=1, owned=False)
    assert control.attempt_outcome(tmp_path, "t1", control.CANCEL, 1) == "pending"


def test_begin_attempt_does_not_recover_an_undecidable_controller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "undecidable")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    old_req = {"at": (now - timedelta(seconds=120)).isoformat(), "by": {"pid": 1}, "lease_seconds": 60}
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", old_req)

    attempt = control.begin_attempt(tmp_path, "t1", control.CANCEL, {"pid": 2}, now=now)

    assert attempt == control.Attempt(n=1, owned=False)


def test_begin_attempt_joins_when_it_loses_the_race_to_create(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`os.link` raising `FileExistsError` here stands in for a concurrent controller's
    `write_phase` call winning the race between our `latest_attempt` read and our own create."""

    def fake_link(src, dst):
        raise FileExistsError()

    monkeypatch.setattr(control.os, "link", fake_link)

    attempt = control.begin_attempt(tmp_path, "t1", control.CANCEL, {"pid": 1})

    assert attempt == control.Attempt(n=1, owned=False)


# --- cancel_verdict ------------------------------------------------------------------------------


def test_cancel_verdict_is_none_with_no_attempts(tmp_path: Path) -> None:
    assert control.cancel_verdict(tmp_path, "t1") == "none"


def test_cancel_verdict_is_authorized_when_the_leader_was_alive_at_signal_time(tmp_path: Path) -> None:
    control.mark_signalled(tmp_path, "t1", control.CANCEL, 1, leader_alive=True)

    assert control.cancel_verdict(tmp_path, "t1") == "authorized"


def test_cancel_verdict_is_not_authorized_when_the_leader_was_already_gone(tmp_path: Path) -> None:
    control.mark_signalled(tmp_path, "t1", control.CANCEL, 1, leader_alive=False)

    assert control.cancel_verdict(tmp_path, "t1") == "not_authorized"


def test_cancel_verdict_is_not_authorized_for_an_unparsable_sig(tmp_path: Path) -> None:
    control.phase_path(tmp_path, "t1", control.CANCEL, 1, "sig").write_text("not json")

    assert control.cancel_verdict(tmp_path, "t1") == "not_authorized"


def test_cancel_verdict_is_not_authorized_once_failed(tmp_path: Path) -> None:
    control.mark_failed(tmp_path, "t1", control.CANCEL, 1, reason="gave up")

    assert control.cancel_verdict(tmp_path, "t1") == "not_authorized"


def test_cancel_verdict_is_pending_for_a_live_controller(tmp_path: Path) -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    req = {"at": now.isoformat(), "by": {"pid": 1}, "lease_seconds": 60}
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", req)

    assert control.cancel_verdict(tmp_path, "t1", now=now) == "pending"


def test_cancel_verdict_recovers_an_abandoned_pending_attempt_as_not_authorized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "dead")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    old_req = {"at": (now - timedelta(seconds=120)).isoformat(), "by": {"pid": 1}, "lease_seconds": 60}
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", old_req)

    assert control.cancel_verdict(tmp_path, "t1", now=now) == "not_authorized"
    failed = control.read_phase(tmp_path, "t1", control.CANCEL, 1, "failed")
    assert failed["reason"] == "controller died"


# --- cancel_attempt_active ------------------------------------------------------------------


def test_cancel_attempt_active_false_with_no_attempts(tmp_path: Path) -> None:
    assert control.cancel_attempt_active(tmp_path, "t1") is False


def test_cancel_attempt_active_true_for_a_pending_live_controller(tmp_path: Path) -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    req = {"at": now.isoformat(), "by": {"pid": 1}, "lease_seconds": 60}
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", req)

    assert control.cancel_attempt_active(tmp_path, "t1", now=now) is True


def test_cancel_attempt_active_false_once_signalled(tmp_path: Path) -> None:
    control.mark_signalled(tmp_path, "t1", control.CANCEL, 1, leader_alive=True)

    assert control.cancel_attempt_active(tmp_path, "t1") is False


def test_cancel_attempt_active_false_once_failed(tmp_path: Path) -> None:
    control.mark_failed(tmp_path, "t1", control.CANCEL, 1, reason="gave up")

    assert control.cancel_attempt_active(tmp_path, "t1") is False


def test_cancel_attempt_active_false_and_read_only_for_an_abandoned_controller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "dead")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    old_req = {"at": (now - timedelta(seconds=120)).isoformat(), "by": {"pid": 1}, "lease_seconds": 60}
    control.write_phase(tmp_path, "t1", control.CANCEL, 1, "req", old_req)

    assert control.cancel_attempt_active(tmp_path, "t1", now=now) is False
    # Unlike cancel_verdict, this must not itself recover the attempt.
    assert control.attempt_outcome(tmp_path, "t1", control.CANCEL, 1) == "pending"


# --- record lock: write_record_if_open --------------------------------------------------------


def test_write_record_if_open_returns_none_when_the_record_is_missing(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()

    assert control.write_record_if_open(log_dir, "does-not-exist", lambda r: r) is None


def test_write_record_if_open_refuses_a_terminal_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record(
        status="completed", exit_code=0, owner={"pid": 1, "start_time": "x", "markers": []}
    )
    store.write(log_dir, record)
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "dead")

    result = control.write_record_if_open(log_dir, "task-1", lambda r: replace(r, status="cancelled"))

    assert result is None
    assert store.read(log_dir, "task-1").status == "completed"


def test_write_record_if_open_refuses_when_the_owner_is_alive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record(owner={"pid": 1, "start_time": "x", "markers": []})
    store.write(log_dir, record)
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "alive")

    result = control.write_record_if_open(log_dir, "task-1", lambda r: replace(r, status="cancelled"))

    assert result is None
    assert store.read(log_dir, "task-1").status == "running"


def test_write_record_if_open_refuses_when_the_owner_is_undecidable(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record(owner=None)  # identity_check(None) is "undecidable", no monkeypatch needed
    store.write(log_dir, record)

    result = control.write_record_if_open(log_dir, "task-1", lambda r: replace(r, status="cancelled"))

    assert result is None
    assert store.read(log_dir, "task-1").status == "running"


def test_write_record_if_open_writes_when_the_owner_is_dead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    record = make_record(owner={"pid": 1, "start_time": "x", "markers": []})
    store.write(log_dir, record)
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "dead")

    result = control.write_record_if_open(
        log_dir, "task-1", lambda r: replace(r, status="cancelled", finished_at="now")
    )

    assert result is not None
    assert result.status == "cancelled"
    assert store.read(log_dir, "task-1").status == "cancelled"


def test_write_record_if_open_times_out_when_locked_by_another_holder(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    lock_path = control.record_lock_path(log_dir, "task-1")
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(control.LockTimeout):
            control.write_record_if_open(log_dir, "task-1", lambda r: r, timeout=0.2)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


# --- session lock --------------------------------------------------------------------------------


def test_session_lock_path_uses_the_session_id_directly_when_it_is_filesystem_safe(
    tmp_path: Path,
) -> None:
    path = control.session_lock_path(tmp_path / "tasks", "abc-123_XYZ")

    assert path.name == "abc-123_XYZ.lock"
    assert path.parent.name == "sessions"


def test_session_lock_path_hashes_an_unsafe_session_id(tmp_path: Path) -> None:
    unsafe = "../../etc/passwd"
    path = control.session_lock_path(tmp_path / "tasks", unsafe)

    assert path.name != f"{unsafe}.lock"
    assert path.name.startswith("h-")
    assert ".." not in path.name
    assert "/" not in path.name


def test_session_lock_path_is_stable_for_the_same_unsafe_id(tmp_path: Path) -> None:
    unsafe = "weird/id with spaces"

    first = control.session_lock_path(tmp_path / "tasks", unsafe)
    second = control.session_lock_path(tmp_path / "tasks", unsafe)

    assert first == second


async def test_acquire_and_release_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "x.lock"

    fd = await control.acquire(path, timeout=1)
    control.release(fd)

    assert path.exists()


async def test_acquire_times_out_when_already_held(tmp_path: Path) -> None:
    path = tmp_path / "x.lock"
    holder_fd = await control.acquire(path, timeout=1)
    try:
        with pytest.raises(control.LockTimeout):
            await control.acquire(path, timeout=0.2)
    finally:
        control.release(holder_fd)


async def test_session_lock_serialises_two_coroutines(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    events: list[str] = []

    async def worker(name: str, hold_seconds: float) -> None:
        async with control.session_lock(log_dir, "session-1", timeout=2):
            events.append(f"{name}-enter")
            await asyncio.sleep(hold_seconds)
            events.append(f"{name}-exit")

    await asyncio.gather(worker("a", 0.2), worker("b", 0))

    # The two coroutines share one session lock, so the second cannot enter until the first
    # has fully released it — never interleaved.
    assert events.index("a-exit") < events.index("b-enter")


async def test_session_lock_times_out_when_already_held(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    holder_fd = await control.acquire(control.session_lock_path(log_dir, "session-1"), timeout=1)
    try:
        with pytest.raises(control.LockTimeout):
            async with control.session_lock(log_dir, "session-1", timeout=0.2):
                pass
    finally:
        control.release(holder_fd)


async def test_lock_files_are_never_deleted(tmp_path: Path) -> None:
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    path = control.session_lock_path(log_dir, "session-1")

    fd = await control.acquire(path, timeout=1)
    control.release(fd)
    assert path.exists()

    # Re-acquiring works against the same file rather than a freshly recreated one.
    fd2 = await control.acquire(path, timeout=1)
    control.release(fd2)
    assert path.exists()


async def test_a_cancelled_lock_waiter_closes_its_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The context manager's `finally` never receives the fd of an acquire cancelled mid-retry, so
    acquire itself must close it — or repeated cancelled resumes exhaust descriptors."""
    path = tmp_path / "x.lock"
    holder_fd = await control.acquire(path, timeout=1)
    closed: list[int] = []
    real_close = os.close
    monkeypatch.setattr(control.os, "close", lambda fd: (closed.append(fd), real_close(fd)))
    try:
        waiter = asyncio.create_task(control.acquire(path, timeout=5))
        await asyncio.sleep(0.15)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
    finally:
        monkeypatch.undo()
        control.release(holder_fd)

    assert len(closed) == 1


def test_write_phase_wraps_a_temp_file_creation_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ENOSPC at mkstemp must surface as PhaseWriteError, which cancel paths handle — a raw OSError
    once escaped a local cancel after its SIGTERM and skipped the SIGKILL escalation."""

    def no_space(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(control.tempfile, "mkstemp", no_space)

    with pytest.raises(control.PhaseWriteError):
        control.write_phase(tmp_path, "t1", control.CANCEL, 1, "sig", {})


def test_close_record_if_open_reports_a_write_that_did_not_land(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store.write(tmp_path, make_record(owner={"pid": 1, "start_time": "x", "markers": []}))
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "dead")
    monkeypatch.setattr(store, "write_landed", lambda log_dir, record: False)

    outcome, record = control.close_record_if_open(
        tmp_path, "task-1", lambda r: replace(r, status="cancelled")
    )

    assert (outcome, record) == ("write_failed", None)
    assert store.read(tmp_path, "task-1").status == "running"


def test_close_record_if_open_distinguishes_missing_terminal_and_live_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close = lambda: control.close_record_if_open(  # noqa: E731
        tmp_path, "task-1", lambda r: replace(r, status="cancelled")
    )
    assert close() == ("missing", None)

    store.write(tmp_path, make_record(owner={"pid": 1, "start_time": "x", "markers": []}))
    monkeypatch.setattr(control.identity, "identity_check", lambda ident: "alive")
    assert close() == ("owner_not_dead", None)

    store.write(tmp_path, make_record(status="completed", exit_code=0))
    assert close()[0] == "terminal"


def test_store_write_landed_is_false_on_an_io_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_space(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(store.tempfile, "NamedTemporaryFile", no_space)

    assert store.write_landed(tmp_path, make_record()) is False
    store.write(tmp_path, make_record())  # the plain write still never raises


def test_a_failed_attempt_with_an_outstanding_joiner_is_not_settled(tmp_path: Path) -> None:
    controller = {"pid": 1, "start_time": "x", "markers": []}
    control.begin_attempt(tmp_path, "t1", control.CANCEL, controller)
    token = control.begin_join(tmp_path, "t1", control.CANCEL, 1, controller)
    control.mark_failed(tmp_path, "t1", control.CANCEL, 1, reason="gone")

    assert control.attempt_state(tmp_path, "t1", control.CANCEL, 1) == "pending"
    assert control.cancel_verdict(tmp_path, "t1") == "pending"
    assert control.cancel_attempt_active(tmp_path, "t1") is True
    # a pending joiner also stops a new attempt starting, which would hide its `.sig`
    assert control.begin_attempt(tmp_path, "t1", control.CANCEL, controller) == control.Attempt(1, False)

    control.write_phase(tmp_path, "t1", control.CANCEL, 1, control.nosig_phase(token), {})
    assert control.attempt_state(tmp_path, "t1", control.CANCEL, 1) == "failed"
    assert control.cancel_verdict(tmp_path, "t1") == "not_authorized"


def test_an_unlistable_directory_counts_as_an_outstanding_joiner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Not being able to enumerate intents is undecidable, not "no joiners": a `.failed` must not
    settle the attempt on the strength of an empty answer."""
    controller = {"pid": 1, "start_time": "x", "markers": []}
    control.begin_attempt(tmp_path, "t1", control.CANCEL, controller)
    control.mark_failed(tmp_path, "t1", control.CANCEL, 1, reason="gone")
    assert control.attempt_state(tmp_path, "t1", control.CANCEL, 1) == "failed"

    real_iterdir = Path.iterdir

    def unlistable(self):
        if self == tmp_path:
            raise PermissionError("cannot list")
        return real_iterdir(self)

    monkeypatch.setattr(Path, "iterdir", unlistable)
    monkeypatch.setattr(control, "latest_attempt", lambda *a: 1)

    with pytest.raises(control.JoinsUnreadable):
        control.join_tokens(tmp_path, "t1", control.CANCEL, 1)
    assert control.joins_outstanding(tmp_path, "t1", control.CANCEL, 1) is True
    assert control.attempt_state(tmp_path, "t1", control.CANCEL, 1) == "pending"
    assert control.cancel_verdict(tmp_path, "t1") == "pending"
