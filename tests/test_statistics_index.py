from __future__ import annotations

import sqlite3
import os

import pytest

from harflow_mcp import state_ownership, store
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.statistics_index import IndexLimit, StatisticsIndex


@pytest.fixture
def owned_layout(tmp_path):
    layout = resolve_layout(root=tmp_path / "state")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        yield layout


def record(task_id: str, *, status="completed", parent_task_id=None):
    return store.TaskRecord(
        task_id=task_id,
        backend="codex",
        session_id=None,
        repo_path="/repo",
        started_at="2027-01-01T00:00:00+00:00",
        finished_at="2027-01-01T00:00:10+00:00" if status == "completed" else None,
        status=status,
        parent_task_id=parent_task_id,
    )


def test_discovery_ignores_unrelated_task_directory_files(owned_layout):
    store.write(owned_layout.tasks_directory, record("task-1"))
    (owned_layout.tasks_directory / "task-1.jsonl").write_text("{not metadata}")
    (owned_layout.tasks_directory / "notes.txt").write_text("unrelated")

    rows = StatisticsIndex(owned_layout).discover()

    assert [(row["kind"], row["id"]) for row in rows] == [("task", "task-1")]


def test_raw_name_limit_is_enforced_before_unbounded_roster_growth(owned_layout, monkeypatch):
    (owned_layout.tasks_directory / "noise-a").touch()
    (owned_layout.tasks_directory / "noise-b").touch()
    monkeypatch.setattr("harflow_mcp.statistics_index.RAW_ENTRY_LIMIT", 1)

    with pytest.raises(IndexLimit) as caught:
        StatisticsIndex(owned_layout).discover()

    assert caught.value.code == "history_limit"


def test_candidate_source_limit_is_enforced_separately_from_raw_entry_limit(owned_layout, monkeypatch):
    store.write(owned_layout.tasks_directory, record("task-a"))
    store.write(owned_layout.tasks_directory, record("task-b"))
    monkeypatch.setattr("harflow_mcp.statistics_index.SOURCE_LIMIT", 1)

    with pytest.raises(IndexLimit) as caught:
        StatisticsIndex(owned_layout).discover()

    assert caught.value.code == "history_limit"


def test_decode_cap_returns_preparing_with_incomplete_coverage(owned_layout, monkeypatch):
    store.write(owned_layout.tasks_directory, record("task-a"))
    store.write(owned_layout.tasks_directory, record("task-b"))
    monkeypatch.setattr("harflow_mcp.statistics_index.DECODE_LIMIT", 1)

    prepared = StatisticsIndex(owned_layout).prepare()

    assert prepared["state"] == "preparing"
    assert prepared["history_complete"] is False
    assert any((row.get("error") or {}).get("code") == "classification_pending" for row in prepared["sources"])


def test_byte_budget_returns_preparing_without_claiming_complete_coverage(owned_layout, monkeypatch):
    from harflow_mcp import statistics_index

    store.write(owned_layout.tasks_directory, record("task-a"))
    budget_type = statistics_index.MetadataBudget
    monkeypatch.setattr(statistics_index, "MetadataBudget", lambda: budget_type(metadata_limit=1))

    prepared = StatisticsIndex(owned_layout).prepare()

    assert prepared["state"] == "preparing"
    assert prepared["history_complete"] is False


def test_deadline_exhaustion_is_bounded_and_explicit(owned_layout, monkeypatch):
    monkeypatch.setattr("harflow_mcp.statistics_index.DEADLINE_SECONDS", 0)

    prepared = StatisticsIndex(owned_layout).prepare()

    assert prepared["state"] == "unavailable"
    assert prepared["history_complete"] is False
    assert prepared["diagnostics"][0]["code"] == "discovery_budget"


def test_fifo_candidate_is_rejected_without_opening_a_blocking_reader(owned_layout):
    fifo = owned_layout.tasks_directory / "fifo.meta.json"
    os.mkfifo(fifo)
    index = StatisticsIndex(owned_layout)
    row = index.discover()[0]

    payload, error = index._read(row)

    assert payload is None
    assert error["code"] == "source_unavailable"


def test_metadata_reader_uses_bounded_chunks_and_checks_deadline_between_them(owned_layout, monkeypatch):
    import harflow_mcp.statistics_index as statistics_index

    path = owned_layout.root / "large-source"
    path.write_bytes(b"x" * 150_000)
    descriptor = os.open(path, os.O_RDONLY)
    index = StatisticsIndex(owned_layout, deadline=statistics_index.time.monotonic() + 10)
    original_read = os.read
    sizes = []

    def track_read(fd, size):
        sizes.append(size)
        return original_read(fd, size)

    monkeypatch.setattr(statistics_index.os, "read", track_read)
    try:
        result = index._read_descriptor(descriptor, 150_000)
    finally:
        os.close(descriptor)

    assert len(result) == 150_000
    assert sizes and max(sizes) <= 65_536
    assert len(sizes) >= 3


def test_deadline_after_a_classified_row_commits_progress_and_returns_preparing(owned_layout, monkeypatch):
    store.write(owned_layout.tasks_directory, record("task-a"))
    store.write(owned_layout.tasks_directory, record("task-b"))
    index = StatisticsIndex(owned_layout)
    original_read = index._read

    def exhaust_on_second(row):
        if row["id"] == "task-b":
            raise IndexLimit("discovery_budget", "test deadline")
        return original_read(row)

    monkeypatch.setattr(index, "_read", exhaust_on_second)
    prepared = index.prepare()
    assert prepared["state"] == "preparing"
    assert prepared["history_complete"] is False
    with index.connect() as database:
        cached = {row[0]: row[1] for row in database.execute("SELECT key,payload FROM sources")}

    assert cached["task:task-a"] is not None
    assert cached.get("task:task-b") is None


def test_unknown_derivative_schema_rebuilds_cache_without_touching_sources(owned_layout):
    store.write(owned_layout.tasks_directory, record("task-1"))
    source_path = store.record_path(owned_layout.tasks_directory, "task-1")
    source_before = source_path.read_bytes()
    index_path = owned_layout.root / ".statistics.v1.sqlite3"
    connection = sqlite3.connect(index_path)
    connection.execute("PRAGMA user_version=99")
    connection.close()

    prepared = StatisticsIndex(owned_layout).prepare()

    assert prepared["state"] == "ready"
    assert source_path.read_bytes() == source_before
    connection = sqlite3.connect(index_path)
    assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
    connection.close()
