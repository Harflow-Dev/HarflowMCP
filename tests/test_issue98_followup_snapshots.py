"""Qualified follow-up history uses one strict metadata snapshot per decision."""

import pytest

from harflow_mcp import followups, identity, store, task_supervision
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session


def _record(task_id, *, session_id=None, started_at="2026-01-01T00:00:00+00:00", **fields):
    status = fields.pop("status", "completed")
    exit_code = fields.pop("exit_code", None if status == "running" else 0)
    finished_at = fields.pop("finished_at", None if status == "running" else started_at)
    return store.TaskRecord(
        task_id=task_id,
        backend="claude",
        session_id=session_id,
        repo_path="/repo",
        started_at=started_at,
        status=status,
        exit_code=exit_code,
        finished_at=finished_at,
        **fields,
    )


@pytest.fixture
def task_directory(tmp_path):
    directory = tmp_path / "state" / "tasks"
    layout = resolve_layout(log_dir=directory)
    with state_session(layout):
        directory.mkdir(parents=True)
        yield directory


def _seed_history(directory):
    source = _record("1" * 32, session_id="session-a", status="running", pid=101,
                     start_time="agent-generation", markers=["agent"])
    successor = _record("2" * 32, session_id="session-a", started_at="2026-01-02T00:00:00+00:00")
    assert store.write_landed(directory, source)
    assert store.write_landed(directory, successor)
    return source, successor


def _run_consumer(consumer, directory, source):
    if consumer == "successors":
        return followups.successor_candidates(directory, source)
    if consumer == "resolve":
        return followups._resolve(directory, source, {"items": []}, successors=None)
    if consumer == "live_sessions":
        return store.live_session_ids(directory)
    if consumer == "live_task_ids":
        return store.live_session_task_ids(directory, "session-a")
    if consumer == "session_successors":
        return store.session_successor_task_ids(directory, source.task_id, "session-a")
    raise AssertionError(consumer)


@pytest.mark.parametrize("consumer", ["successors", "resolve", "live_sessions", "live_task_ids", "session_successors"])
def test_converted_history_consumers_retry_only_the_qualified_replacement(task_directory, monkeypatch, consumer):
    source, successor = _seed_history(task_directory)
    real_scan = store.scan
    calls = []
    monkeypatch.setattr(task_supervision, "session_reservations", lambda _directory: [])
    monkeypatch.setattr(identity, "identity_check", lambda _value: "alive")

    def scan(directory):
        calls.append(directory)
        if len(calls) == 1:
            error = store.TaskDocumentError(source.task_id, "metadata replaced during read", code="task_unavailable")
            error._source_changed_during_read = True
            return store.TaskScan([], {source.task_id: error})
        return real_scan(directory)

    monkeypatch.setattr(store, "scan", scan)
    result = _run_consumer(consumer, task_directory, source)

    assert len(calls) == 2
    if consumer == "successors":
        assert result == [successor.task_id]
    elif consumer == "resolve":
        assert result is False
    elif consumer == "live_sessions":
        assert result == {"session-a"}
    elif consumer == "live_task_ids":
        assert result == {source.task_id}
    else:
        assert result == {successor.task_id}


@pytest.mark.parametrize("consumer", ["successors", "live_task_ids"])
@pytest.mark.parametrize("refusal", ["replacement_exhausted", "unmarked", "malformed"])
def test_history_refusals_stop_candidate_or_admission_work_before_side_effects(
    task_directory, monkeypatch, consumer, refusal
):
    source, _successor = _seed_history(task_directory)
    real_scan = store.scan
    calls = []
    reservations = []
    downstream = []
    monkeypatch.setattr(task_supervision, "session_reservations", lambda _directory: reservations.append(True) or [])
    monkeypatch.setattr(store, "write", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("refused history wrote state")))
    monkeypatch.setattr(store, "session_successor_task_ids", lambda *args, **kwargs: downstream.append(True))

    if refusal == "malformed":
        store.record_path(task_directory, source.task_id).write_text("not json")

        def scan(directory):
            calls.append(directory)
            return real_scan(directory)
    else:
        def scan(directory):
            calls.append(directory)
            error = store.TaskDocumentError(source.task_id, "history unavailable", code="task_unavailable")
            if refusal == "replacement_exhausted":
                error._source_changed_during_read = True
            return store.TaskScan([], {source.task_id: error})

    monkeypatch.setattr(store, "scan", scan)
    with pytest.raises(store.TaskDocumentError):
        _run_consumer(consumer, task_directory, source)

    assert len(calls) == (3 if refusal == "replacement_exhausted" else 1)
    assert reservations == []
    assert downstream == []


def test_successor_candidates_reuses_one_qualified_snapshot_and_sorts_launch_only_ids(monkeypatch, tmp_path):
    source = _record("1" * 32, session_id="session-a")
    later = _record("2" * 32, session_id="session-a", started_at="2026-01-03T00:00:00+00:00")
    earlier = _record("3" * 32, session_id="session-a", started_at="2026-01-02T00:00:00+00:00")
    continuation = _record("4" * 32, continuation_of_task_id=source.task_id,
                           started_at="2026-01-01T12:00:00+00:00")
    snapshot = [source, later, earlier, continuation]
    reads = []
    passed_snapshots = []
    monkeypatch.setattr(task_supervision, "read_task_records", lambda _path: (reads.append(_path), snapshot)[1])

    real_successors = store.session_successor_task_ids

    def successors(path, task_id, session_id, *, records=None):
        passed_snapshots.append(records)
        return real_successors(path, task_id, session_id, records=records)

    monkeypatch.setattr(store, "session_successor_task_ids", successors)
    monkeypatch.setattr(task_supervision, "scan_launches", lambda _path: task_supervision.LaunchScan(entries=[
        {"task_id": "a" * 32, "session_id": "session-a", "state": "spawned"},
        {"task_id": "b" * 32, "session_id": "session-a", "state": "claimed"},
        # A launch receipt for an ID already present in the task snapshot must deduplicate.
        {"task_id": earlier.task_id, "session_id": "session-a", "state": "spawned"},
    ]))
    monkeypatch.setattr(store, "write", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("history observation wrote state")))

    result = followups.successor_candidates(tmp_path / "tasks", source)

    assert reads == [tmp_path / "tasks"]
    assert passed_snapshots == [snapshot]
    assert result == ["a" * 32, "b" * 32, continuation.task_id, earlier.task_id, later.task_id]
    assert len(result) == len(set(result))
