"""`harflow-ctl task-list-watch`: bounded invalidation wait, fences and refusals."""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import pytest

from conftest import ALIVE_OWNER

from harflow_mcp import catalog, ctl, state_ownership, store, takeover, task_list_watch
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import PRODUCT_VERSION


@pytest.fixture
def state(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)
    layout = resolve_layout(root=tmp_path / ".harflow")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
        yield layout


def record(task_id="watch-task-1", **overrides) -> store.TaskRecord:
    base = {
        "task_id": task_id,
        "backend": "claude",
        "session_id": "s1",
        "markers": ["s1"],
        "repo_path": "/repo",
        "started_at": "2027-01-01T00:00:00+00:00",
        "status": "completed",
        "exit_code": 0,
        "finished_at": "2027-01-01T00:00:10+00:00",
        "output_drained": True,
        "owner": ALIVE_OWNER,
        "title": "one",
    }
    return store.TaskRecord(**(base | overrides))


def run(args, capsys, *, expect=0):
    code = ctl.main(["task-list-watch", *args, "--json"])
    captured = capsys.readouterr()
    assert code == expect, captured.err or captured.out
    document = json.loads(captured.out)
    assert len(captured.out.encode()) <= task_list_watch.MAX_DOCUMENT_BYTES
    return document["result"] if expect == 0 and "result" in document else document.get("error", document.get("result"))


def test_baseline_without_cursor_is_immediate(state, capsys):
    result = run(["--wait-seconds", "30"], capsys)
    assert result["state"] == "baseline"
    assert result["cursor"] and "reason" not in result
    assert (state.tasks_directory / task_list_watch.REVISION_NAME).is_file()


def test_timeout_under_continuity(state, capsys):
    baseline = run([], capsys)
    started = time.monotonic()
    timed = run(["--cursor", baseline["cursor"], "--wait-seconds", "0"], capsys)
    assert timed["state"] == "timeout"
    assert timed["cursor"] == baseline["cursor"] or timed["cursor"]
    assert time.monotonic() - started < 1.0


def test_write_between_baseline_and_wait_is_changed(state, capsys):
    baseline = run([], capsys)
    with state_ownership.state_session(state):
        store.write(state.tasks_directory, record())
    changed = run(["--cursor", baseline["cursor"], "--wait-seconds", "0"], capsys)
    assert changed["state"] == "changed"
    assert changed["cursor"] != baseline["cursor"]


def test_wait_sees_publication_during_wait(state, capsys, monkeypatch):
    baseline = run([], capsys)
    barrier = threading.Event()
    published = threading.Event()

    def publish():
        assert barrier.wait(timeout=2)
        with state_ownership.state_session(state):
            store.write(state.tasks_directory, record(task_id="watch-task-2", title="two"))
        published.set()

    def poll_gate(_seconds):
        barrier.set()
        assert published.wait(timeout=2)

    from types import SimpleNamespace
    monkeypatch.setattr(task_list_watch, "time", SimpleNamespace(monotonic=time.monotonic, sleep=poll_gate))
    thread = threading.Thread(target=publish)
    thread.start()
    changed = run(["--cursor", baseline["cursor"], "--wait-seconds", "2"], capsys)
    thread.join(timeout=3)
    assert changed["state"] == "changed"


def test_malformed_cursor_is_typed_refusal(state, capsys):
    error = run(["--cursor", "not-a-cursor", "--wait-seconds", "0"], capsys, expect=1)
    assert error["code"] == "task_list_watch_cursor_invalid"


def test_wrong_root_cursor_is_typed_refusal(state, tmp_path, monkeypatch, capsys):
    baseline = run([], capsys)
    other = resolve_layout(root=tmp_path / "other-root")
    with state_ownership.state_session(other):
        other.tasks_directory.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(other.root))
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)
    error = run(["--cursor", baseline["cursor"], "--wait-seconds", "0"], capsys, expect=1)
    assert error["code"] == "task_list_watch_cursor_invalid"


def test_agent_caller_refused_before_sources(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HOME", str(tmp_path))
    layout = resolve_layout(root=tmp_path / ".harflow")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
        store.write(layout.tasks_directory, record())
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: ("agent_caller", "managed"))
    before = list(layout.tasks_directory.glob(task_list_watch.REVISION_NAME))
    error = run([], capsys, expect=1)
    assert error["code"] == "agent_caller"
    assert list(layout.tasks_directory.glob(task_list_watch.REVISION_NAME)) == before


def test_missing_root_is_unavailable_without_initialization(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "absent-root"))
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)
    result = run([], capsys)
    assert result["state"] == "unavailable"
    assert result["cursor"] is None
    assert result["reason"] == task_list_watch.REASON_STATE_MISSING
    assert not (tmp_path / "absent-root").exists()


def test_fence_begin_without_end_reports_changed(state, capsys):
    baseline = run([], capsys)
    with state_ownership.state_session(state):
        task_list_watch.begin_publication(state.tasks_directory, layout=state)
    changed = run(["--cursor", baseline["cursor"], "--wait-seconds", "0"], capsys)
    assert changed["state"] == "changed"


def test_dead_fence_pid_reclaims_as_reset(state, capsys):
    baseline = run([], capsys)
    path = task_list_watch.revision_path(state.tasks_directory)
    with state_ownership.state_session(state):
        with catalog.catalog_lock(state.tasks_directory, layout=state):
            current = task_list_watch._ensure_locked(state.tasks_directory)
            task_list_watch._write_unlocked(
                path,
                task_list_watch.RevisionState(current.epoch, current.revision + 1, 1, fence_pids=(2**30 - 3,)),
            )
    reset = run(["--cursor", baseline["cursor"], "--wait-seconds", "0"], capsys)
    assert reset["state"] == "reset"
    assert reset["reason"] == task_list_watch.REASON_RECLAIMED
    assert reset["cursor"]


def test_abort_after_invalidate_closes_fence_so_later_waits_can_timeout(state, capsys):
    baseline = run([], capsys)
    with state_ownership.state_session(state):
        publication = task_list_watch.begin_publication(state.tasks_directory, layout=state)
        # Simulate store's finally after a failed replace: close the fence without a record.
        task_list_watch.end_publication(state.tasks_directory, publication, layout=state)
    changed = run(["--cursor", baseline["cursor"], "--wait-seconds", "0"], capsys)
    assert changed["state"] == "changed"
    timed = run(["--cursor", changed["cursor"], "--wait-seconds", "0"], capsys)
    assert timed["state"] == "timeout"


def test_env_agent_refusal_does_not_create_root(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "absent-root"))
    monkeypatch.setenv("HARFLOW_TASK_ID", "agent-task")
    error = run([], capsys, expect=1)
    assert error["code"] == "agent_caller"
    assert not (tmp_path / "absent-root").exists()


def test_capability_advertised():
    from harflow_mcp.wire_contracts import CAPABILITIES, version_info
    assert "task-list-watch.v1" in CAPABILITIES
    assert PRODUCT_VERSION == "0.0.13"
    info = version_info({"HOME": "/tmp", "HARFLOW_STATE_ROOT": "/tmp/harflow-fixture-home/.harflow"})
    assert "task-list-watch.v1" in info["capabilities"]


def test_wait_seconds_bounds(state, capsys):
    with pytest.raises(SystemExit) as raised:
        ctl.main(["task-list-watch", "--wait-seconds", "31", "--json"])
    assert raised.value.code == 2
    assert "0..30" in capsys.readouterr().err
