"""Tests for bounded polling around strict task-document reads."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from harflow_mcp import state_ownership, store, task_documents
from harflow_mcp.state_layout import resolve_layout
from task_polling import read_task_until


def _record(task_id: str = "poll", **changes: object) -> store.TaskRecord:
    values: dict[str, object] = {
        "task_id": task_id,
        "backend": "codex",
        "session_id": "session-1",
        "repo_path": "/tmp/repo",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "running",
        "exit_code": None,
        "markers": ["session-1"],
    }
    return store.TaskRecord(**(values | changes))


@pytest.fixture
def task_dir(tmp_path: Path) -> Path:
    root = tmp_path / "state"
    directory = root / "tasks"
    with state_ownership.state_session(resolve_layout(root=root)):
        directory.mkdir(parents=True)
        yield directory


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay


def _marked(task_id: str = "poll", value: object = True, *, code: str = "task_unavailable"):
    error = task_documents.TaskDocumentError(task_id, "Task metadata changed while reading", code=code)
    error._source_changed_during_read = value
    return error


def test_atomic_replacement_is_refused_then_fresh_terminal_sample_is_returned(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = _record(status="running")
    after = replace(before, status="completed", exit_code=0, finished_at="2026-10-21T00:00:00+00:00")
    assert store.write_landed(task_dir, before)
    path = store.record_path(task_dir, before.task_id)
    original_read_text = Path.read_text
    original_raw = original_read_text(path, encoding="utf-8")
    injected = False
    replacement = after

    def race(candidate: Path, *args: object, **kwargs: object) -> str:
        nonlocal injected
        raw = original_read_text(candidate, *args, **kwargs)
        if candidate == path and not injected:
            injected = True
            assert store.write_landed(task_dir, replacement)
        return raw

    monkeypatch.setattr(Path, "read_text", race)
    with pytest.raises(store.TaskDocumentError) as direct_error:
        store.read(task_dir, before.task_id, include_prompt=False)
    assert original_read_text(path, encoding="utf-8") != original_raw
    assert getattr(direct_error.value, "_source_changed_during_read", None) is True
    assert injected

    # Restore the pre-race source, then make the helper encounter the same
    # production replacement on its first acquisition.  The first raw sample
    # must be discarded; only the later terminal record may satisfy the poll.
    helper_before = replace(before, task_id="poll-helper")
    helper_after = replace(after, task_id="poll-helper")
    replacement = helper_after
    path = store.record_path(task_dir, helper_before.task_id)
    injected = True
    assert store.write_landed(task_dir, helper_before)
    injected = False
    original_raw = original_read_text(path, encoding="utf-8")
    clock = FakeClock()
    result = read_task_until(
        task_dir,
        helper_before.task_id,
        lambda record: record is not None and record.status == "completed",
        deadline=1.0,
        interval=0.05,
        clock=clock.clock,
        sleep=clock.sleep,
    )
    assert result == helper_after
    assert injected
    assert clock.sleeps == [0.05]


def test_successful_missing_sample_returns_immediately(task_dir: Path) -> None:
    clock = FakeClock()
    assert read_task_until(task_dir, "missing", lambda record: record is None, deadline=10.0,
                           clock=clock.clock, sleep=clock.sleep) is None
    assert clock.sleeps == []


def test_marked_race_then_successful_sample_is_retried(task_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    expected = _record(status="completed", exit_code=0)
    assert store.write_landed(task_dir, expected)
    real_read = store.read
    attempts = 0

    def read(*args: object, **kwargs: object):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _marked(expected.task_id)
        return real_read(*args, **kwargs)

    monkeypatch.setattr(store, "read", read)
    clock = FakeClock()
    assert read_task_until(task_dir, expected.task_id, lambda record: record is not None,
                           deadline=1.0, interval=0.05, clock=clock.clock, sleep=clock.sleep) == expected
    assert attempts == 2
    assert clock.sleeps == [0.05]


def test_marked_race_then_missing_sample_returns_none_after_one_retry(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempts = 0

    def read(*args: object, **kwargs: object):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _marked("missing")
        return None

    monkeypatch.setattr(store, "read", read)
    clock = FakeClock()
    assert read_task_until(task_dir, "missing", lambda record: record is None,
                           deadline=10.0, clock=clock.clock, sleep=clock.sleep) is None
    assert attempts == 2
    assert clock.sleeps == [0.05]


def test_nonterminal_samples_are_polled_until_terminal(task_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    running = _record(status="running")
    terminal = replace(running, status="completed", exit_code=0)
    samples = iter([None, running, running, terminal])
    monkeypatch.setattr(store, "read", lambda *args, **kwargs: next(samples))
    clock = FakeClock()
    result = read_task_until(task_dir, running.task_id,
                             lambda record: record is not None and record.status in store.TERMINAL_RECORD_STATUSES,
                             deadline=1.0, interval=0.05, clock=clock.clock, sleep=clock.sleep)
    assert result == terminal
    assert clock.sleeps == [0.05, 0.05, 0.05]


def test_marked_races_until_deadline_return_none(task_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(store, "read", lambda *args, **kwargs: (_ for _ in ()).throw(_marked("poll")))
    clock = FakeClock()
    assert read_task_until(task_dir, "poll", lambda record: record is not None,
                           deadline=0.11, interval=0.05, clock=clock.clock, sleep=clock.sleep) is None
    assert clock.sleeps == [0.05, 0.05, 0.05]


@pytest.mark.parametrize(
    ("error", "marker"),
    [
        (store.TaskDocumentError("poll", "unavailable", code="task_unavailable"), None),
        (store.TaskDocumentError("poll", "incompatible", code="task_incompatible"), None),
        (_marked("poll", 1), 1),
    ],
)
def test_unmarked_or_nonexact_marker_errors_are_reraised(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch, error: Exception, marker: object
) -> None:
    monkeypatch.setattr(store, "read", lambda *args, **kwargs: (_ for _ in ()).throw(error))
    clock = FakeClock()
    with pytest.raises(type(error)) as caught:
        read_task_until(task_dir, "poll", lambda record: record is not None,
                        deadline=1.0, clock=clock.clock, sleep=clock.sleep)
    if marker is not None:
        assert getattr(caught.value, "_source_changed_during_read") == marker


def test_non_task_document_error_is_not_swallowed(task_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    failure = OSError("permission denied")
    monkeypatch.setattr(store, "read", lambda *args, **kwargs: (_ for _ in ()).throw(failure))
    clock = FakeClock()
    with pytest.raises(OSError, match="permission denied"):
        read_task_until(task_dir, "poll", lambda record: record is not None,
                        deadline=1.0, clock=clock.clock, sleep=clock.sleep)


def test_predicate_false_samples_expire_without_returning_stale_record(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sample = _record(status="running")
    monkeypatch.setattr(store, "read", lambda *args, **kwargs: sample)
    clock = FakeClock()
    assert read_task_until(task_dir, sample.task_id, lambda record: False,
                           deadline=0.11, interval=0.05, clock=clock.clock, sleep=clock.sleep) is None
    assert clock.sleeps == [0.05, 0.05, 0.05]
