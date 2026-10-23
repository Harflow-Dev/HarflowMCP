"""Transactional and recovery coverage for supervision-loss notifications."""

import json
import os
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest

from harflow_mcp import notifications, store, supervision_notifications, workflow_notifications
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session


@pytest.fixture
def logs(tmp_path, monkeypatch):
    root = tmp_path / "state"
    logs = root / "tasks"
    monkeypatch.setenv(
        "HARFLOW_NOTIFY_DESTINATIONS",
        json.dumps({"hook": {"url": "https://example.test/hook"}}),
    )
    with state_session(resolve_layout(root=root)):
        logs.mkdir()
        yield logs


def _record(logs, task_id="lost-owner", **changes):
    fields = dict(
        task_id=task_id,
        backend="codex",
        session_id=None,
        repo_path=str(logs),
        started_at="2026-10-23T00:00:00+00:00",
        status="failed",
        exit_code=None,
        owner={"pid": 123, "start_time": "old", "markers": []},
        pid=123,
        terminal_summary="captured before supervision loss",
    )
    fields.update(changes)
    record = store.TaskRecord(**fields)
    store.write(logs, record)
    return record


def _dead(monkeypatch):
    monkeypatch.setattr("harflow_mcp.identity.identity_check", lambda _ident: "dead")
    monkeypatch.setattr("harflow_mcp.identity.check_detail", lambda _ident: ("dead", None))


def _delivery(logs):
    with notifications._db(logs) as db:
        return dict(db.execute("SELECT * FROM supervision_deliveries").fetchone())


def test_concurrent_fresh_detectors_insert_one_episode_and_leave_completion_pending(logs, monkeypatch):
    """Two independent server processes must race through the same SQLite uniqueness gate."""
    _dead(monkeypatch)
    record = _record(logs)
    subscription = notifications.subscribe(logs, record.task_id, "hook")

    ready_a, ready_b, gate = (logs.parent / name for name in ("ready-a", "ready-b", "go"))
    script = """
import sys, time
from pathlib import Path
from harflow_mcp import identity, supervision_notifications
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session
root, ready, gate = map(Path, sys.argv[1:])
identity.identity_check = lambda _ident: "dead"
identity.check_detail = lambda _ident: ("dead", None)
ready.touch()
deadline = time.time() + 10
while not gate.exists() and time.time() < deadline:
    time.sleep(0.01)
if not gate.exists():
    raise SystemExit("detector barrier timed out")
with state_session(resolve_layout(root=root)):
    supervision_notifications.detect(root / "tasks")
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(logs.parent), str(ready), str(gate)],
            cwd=Path(__file__).parents[1],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for ready in (ready_a, ready_b)
    ]
    try:
        deadline = time.time() + 10
        while time.time() < deadline and not (ready_a.exists() and ready_b.exists()):
            time.sleep(0.01)
        assert ready_a.exists() and ready_b.exists()
        gate.touch()
        results = [process.communicate(timeout=10) for process in processes]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
    assert all(process.returncode == 0 for process in processes), results

    with state_session(resolve_layout(root=logs.parent)):
        with notifications._db(logs) as db:
            rows = db.execute(
                "SELECT * FROM supervision_deliveries WHERE subscription_id=?",
                (subscription["id"],),
            ).fetchall()
        assert len(rows) == 1
        assert notifications.status(logs, record.task_id)[0]["state"] == "pending"


def test_failed_retry_keeps_supervision_payload_and_id_after_task_changes(logs, monkeypatch):
    _dead(monkeypatch)
    record = _record(logs)
    notifications.subscribe(logs, record.task_id, "hook")
    supervision_notifications.detect(logs)
    first = _delivery(logs)
    changed = replace(record, terminal_summary="changed after the loss episode")
    store.write(logs, changed)

    sent = []

    def fail(*args):
        sent.append(args[2])
        raise OSError("receiver unavailable")

    monkeypatch.setattr(supervision_notifications.transport, "_deliver", fail)
    supervision_notifications.tick(logs)
    with notifications._db(logs) as db:
        db.execute("UPDATE supervision_deliveries SET due=0")
    supervision_notifications.tick(logs)
    second = _delivery(logs)

    assert first["id"] == second["id"]
    assert first["payload"] == second["payload"] == sent[0] == sent[1]
    assert json.loads(second["payload"])["affected_nodes"][0]["last_activity"] == record.terminal_summary
    assert second["state"] == "retry"


def test_expired_supervision_lease_is_reclaimed_by_another_worker(logs, monkeypatch):
    _dead(monkeypatch)
    record = _record(logs)
    notifications.subscribe(logs, record.task_id, "hook")
    supervision_notifications.detect(logs)
    with notifications._db(logs) as db:
        db.execute(
            "UPDATE supervision_deliveries SET state='delivering',lease='dead-worker',lease_until=?,attempts=1,due=0",
            (time.time() - 1,),
        )
    delivered = []
    monkeypatch.setattr(supervision_notifications.transport, "_deliver", lambda *args: delivered.append(args))
    supervision_notifications.tick(logs)
    row = _delivery(logs)
    assert row["state"] == "delivered"
    assert row["attempts"] == 2
    assert delivered and delivered[0][1] == row["id"]
    assert row["lease"] is None


def test_dead_owner_and_process_settle_completion_without_mutating_task(logs, monkeypatch):
    _dead(monkeypatch)
    record = _record(logs, terminal_summary="inferred terminal summary")
    notifications.subscribe(logs, record.task_id, "hook")
    supervision_notifications.detect(logs)
    before = store.read(logs, record.task_id)
    sent = []
    monkeypatch.setattr(notifications, "_deliver", lambda _config, identifier, payload: sent.append((identifier, payload)))
    notifications._tick(logs)
    after = store.read(logs, record.task_id)

    assert sent
    payload = json.loads(sent[0][1])
    assert payload["recovered"] is True
    assert payload["exit_code"] is None
    assert payload["outcome_observed"] is False
    assert after == before
    assert notifications.status(logs, record.task_id)[0]["state"] == "delivered"


def test_each_workflow_destination_reconciles_all_siblings_with_independent_cursors(logs, monkeypatch):
    """A bounded workflow scan must not let one destination consume another's cursor."""
    task_ids = [f"task-{index:03d}" for index in range(65)]
    root = {
        "workflow_run_id": "root",
        "activations": [{
            "id": "execution-root",
            "node_id": "node-root",
            "tasks": [{"task_id": task_id} for task_id in task_ids],
        }],
    }

    class Storage:
        owners = logs.parent / "owners"

        def get_run(self, _run_id, **_kwargs):
            return root

    records = {task_id: _record(logs, task_id=task_id) for task_id in task_ids}
    monkeypatch.setattr(workflow_notifications, "_run", lambda _log_dir, _run_id: root)
    monkeypatch.setattr(workflow_notifications, "_storage", lambda _log_dir: Storage())
    monkeypatch.setattr(
        "harflow_mcp.bounded_io.read_receipt",
        lambda _path: {
            "workflow_run_id": "root",
            "workflow_node_id": "node-root",
            "workflow_execution_id": "execution-root",
        },
    )
    monkeypatch.setattr(supervision_notifications.store, "read", lambda _log_dir, task_id, **_kwargs: records.get(task_id))
    monkeypatch.setattr(
        supervision_notifications,
        "loss",
        lambda record, _log_dir: ("generation", {"capture_state": "lost_with_owner"}),
    )
    with notifications._db(logs) as db:
        now = time.time()
        for identifier, destination in (("one", "hook-one"), ("two", "hook-two")):
            db.execute(
                "INSERT INTO workflow_subscriptions(id,workflow_run_id,destination,events,state,from_sequence,last_point_sequence,created,updated) VALUES(?,?,?,?,?,?,?,?,?)",
                (identifier, "root", destination, '["attention"]', "active", 0, 0, now, now),
            )
    monkeypatch.setattr(
        notifications,
        "preflight",
        lambda destination: {"alias": destination, "url": "https://example.test/hook", "allow_private": False, "secret_env": None},
    )
    supervision_notifications._SCAN_CURSORS.clear()

    supervision_notifications.detect(logs)
    supervision_notifications.detect(logs)
    with notifications._db(logs) as db:
        counts = dict(db.execute("SELECT subscription_id, count(*) FROM supervision_deliveries GROUP BY subscription_id").fetchall())
    assert counts == {"one": 65, "two": 65}
