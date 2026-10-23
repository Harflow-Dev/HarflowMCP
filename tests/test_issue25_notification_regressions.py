import json

import pytest
from harflow_mcp import notifications, store, supervision_notifications
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session


@pytest.fixture
def logs(tmp_path, monkeypatch):
    root = tmp_path / "state"
    with state_session(resolve_layout(root=root)):
        logs = root / "tasks"
        logs.mkdir()
        monkeypatch.setenv("HARFLOW_NOTIFY_DESTINATIONS", json.dumps({"hook": {"url": "https://example.test/hook"}}))
        yield logs


def _record(logs, task_id="lost", **changes):
    fields = dict(task_id=task_id, backend="codex", session_id=None, repo_path=str(logs),
                  started_at="2026-10-23T00:00:00+00:00", status="failed", exit_code=None,
                  owner={"pid": 123, "start_time": "start", "markers": []}, pid=123)
    fields.update(changes)
    record = store.TaskRecord(**fields)
    if logs is not None:
        store.write(logs, record)
    return record


def test_settlement_payload_infers_terminal_projection_without_mutating_record(logs, monkeypatch):
    monkeypatch.setattr("harflow_mcp.identity.identity_check", lambda ident: "dead")
    monkeypatch.setattr("harflow_mcp.identity.check_detail", lambda ident: ("dead", None))
    monkeypatch.setattr("harflow_mcp.store.resolve_status", lambda *args: ("failed", "", type("State", (), {"summary": "inferred", "failure_diagnostic": None})(), []))
    record = _record(logs)
    candidate = {"id": "sub", "task_id": record.task_id, "destination": "hook"}
    payload = notifications._settlement_payload(logs, candidate, record)
    assert payload is not None
    decoded = json.loads(payload)
    assert decoded["recovered"] is True
    assert decoded["outcome_observed"] is False
    assert store.read(logs, record.task_id).exit_code is None


def test_supervision_loss_requires_dead_owner_and_process(monkeypatch, tmp_path):
    record = _record(None)
    monkeypatch.setattr("harflow_mcp.identity.identity_check", lambda ident: "undecidable")
    assert supervision_notifications.loss(record, tmp_path) is None


def test_loss_episode_deduplicates_concurrent_detection_and_keeps_settlement_subscription(logs, monkeypatch):
    record = _record(logs)
    monkeypatch.setattr("harflow_mcp.identity.identity_check", lambda ident: "dead")
    monkeypatch.setattr("harflow_mcp.identity.check_detail", lambda ident: ("dead", None))
    subscription = notifications.subscribe(logs, record.task_id, "hook")
    found = supervision_notifications.loss(record, logs)
    assert found is not None
    generation, evidence = found
    node = supervision_notifications._node(record, evidence)
    supervision_notifications._insert(logs, subscription, "task", generation, [node])
    supervision_notifications._insert(logs, subscription, "task", generation, [node])
    with notifications._db(logs) as db:
        rows = db.execute("SELECT * FROM supervision_deliveries").fetchall()
    assert len(rows) == 1
    assert notifications.status(logs, record.task_id)[0]["state"] == "pending"


def test_supervision_delivery_pin_cap_is_enforced_without_truncating_independent_tasks(logs, monkeypatch):
    notifications.subscribe(logs, "seed", "hook", reservation=True)
    monkeypatch.setattr(notifications, "MAX_PINNED_TASKS", 2)
    with notifications._db(logs) as db:
        for index in range(3):
            db.execute(
                "INSERT INTO supervision_deliveries(id,subscription_id,subscription_kind,task_id,destination,event_key,event,state,payload,created,updated) VALUES(?,?,?,?,?,?,?,'pending',?,?,?)",
                (f"delivery-{index}", "seed", "task", f"task-{index}", "hook", f"episode-{index}", "task.supervision_lost", "{}", 0, 0),
            )
    with pytest.raises(ValueError, match="pin budget"):
        notifications.pinned_tasks(logs)
