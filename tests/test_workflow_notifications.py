"""Workflow notification subscriptions and delivery intent contracts (#84)."""

import json
import time
from pathlib import Path

import pytest

from polybridge import workflow_notifications as wn, workflow_notification_points as points

ORIGINAL_RUN = wn._run


@pytest.fixture
def logs(tmp_path, monkeypatch):
    task_dir = tmp_path / "tasks"
    task_dir.mkdir()
    monkeypatch.setenv("PB_NOTIFY_DESTINATIONS", json.dumps({"hook": {"url": "https://example.test/hook"}}))
    def direct_run(log_dir, run_id):
        run = json.loads((Path(log_dir).parent / "workflow-runs" / f"{run_id}.json").read_text())
        if run.get("parent_link"):
            raise ValueError("Subscribe to the root workflow run")
        if run.get("kind") == "builder":
            raise ValueError("Builder runs do not support workflow notifications")
        return run
    monkeypatch.setattr(wn, "_run", direct_run)
    return task_dir


def write_run(logs, *, run_id="root", status="running", sequence=1, **fields):
    path = logs.parent / "workflow-runs"
    path.mkdir(exist_ok=True)
    run = {"workflow_run_id": run_id, "kind": "workflow", "name": "Build", "status": status,
           "sequence": sequence, "updated_at": f"2026-10-18T00:00:{sequence:02d}+00:00",
           "definition": {"workflow_id": "wf", "revision": 1}, "activations": [], **fields}
    (path / f"{run_id}.json").write_text(json.dumps(run))
    return run


def point(key, event, sequence, status, **extra):
    return {"key": key, "event": event, "sequence": sequence, "status": status,
            "observed_at": f"2026-10-18T00:00:{sequence:02d}+00:00", "snapshot": {
                "name": "Build", "kind": "workflow", "status": status, "goal_revision": 1,
                "definition": {"workflow_id": "wf", "revision": 1}, **extra}}


def subscribe(logs, run_id="root", events=None):
    return wn.subscribe(logs, run_id, "hook", events)


def db_subscription(logs, subscription_id):
    with wn.transport._db(logs) as db:
        return db.execute("SELECT * FROM workflow_subscriptions WHERE id=?", (subscription_id,)).fetchone()


def test_existing_terminal_subscription_starts_at_current_point_only(logs):
    terminal = point("terminal:completed", points.SETTLED, 5, "completed")
    write_run(logs, status="completed", sequence=5, terminal_notification_points=[terminal])
    sub = subscribe(logs)
    assert sub["from_sequence"] == 4
    wn.tick(logs)
    rows = wn.status(logs, "root")["deliveries"]
    assert len(rows) == 1 and rows[0]["event_key"] == "terminal:completed"


def test_interleaved_points_and_eviction_watermark_are_preserved(logs):
    attention = [point(f"attention:needs_attention:{i}:root:None", points.ATTENTION, i, "needs_attention") for i in range(1, 4)]
    terminal = [point(f"terminal:failed:{i}", points.SETTLED, i + 1, "failed") for i in range(1, 4)]
    write_run(logs, sequence=0)
    subscribe(logs)
    write_run(logs, sequence=6, notification_points=attention, terminal_notification_points=terminal,
              status="running", notification_evicted={"attention": 0, "terminal": 0})
    wn.tick(logs)
    rows = wn.status(logs, "root")["deliveries"]
    assert len(rows) == 6
    assert [row["event"] for row in rows].count(points.ATTENTION) == 3
    assert [row["event"] for row in rows].count(points.SETTLED) == 3


def test_attention_claim_is_superseded_when_current_key_changes(logs, monkeypatch):
    key = "attention:needs_input:d1:root"
    write_run(logs, status="needs_input", sequence=1, input_decision_id="d1",
              notification_points=[point(key, points.ATTENTION, 1, "needs_input")])
    subscribe(logs, events=["attention"])
    wn._detect(logs, db_subscription(logs, wn.status(logs, "root")["subscriptions"][0]["id"]))
    write_run(logs, status="running", sequence=2)
    monkeypatch.setattr(wn.transport, "_deliver", lambda *args: pytest.fail("superseded attention was delivered"))
    wn.tick(logs)
    assert wn.status(logs, "root")["deliveries"][0]["state"] == "superseded"


def test_terminal_delivery_is_not_superseded_after_recovery(logs, monkeypatch):
    terminal = point("terminal:failed:1", points.SETTLED, 1, "failed")
    write_run(logs, status="failed", sequence=1, terminal_notification_points=[terminal])
    subscribe(logs, events=["terminal"])
    write_run(logs, status="running", sequence=2, terminal_notification_points=[terminal])
    sent = []
    monkeypatch.setattr(wn.transport, "_deliver", lambda *a: sent.append(a))
    wn.tick(logs)
    assert sent and wn.status(logs, "root")["deliveries"][0]["state"] == "delivered"


def test_tree_settlement_pending_defers_and_unconfirmed_marks_payload(logs, monkeypatch):
    terminal = point("terminal:completed", points.SETTLED, 1, "completed")
    terminal["requires_tree_check"] = True
    write_run(logs, status="completed", sequence=1, terminal_notification_points=[terminal])
    subscribe(logs, events=["terminal"])
    monkeypatch.setattr(points, "tree_settlement", lambda *a, **k: "pending")
    wn.tick(logs)
    assert wn.status(logs, "root")["deliveries"][0]["state"] == "pending"
    monkeypatch.setattr(points, "tree_settlement", lambda *a, **k: "unconfirmed")
    monkeypatch.setattr(wn.transport, "_deliver", lambda *a: None)
    wn.tick(logs)
    assert wn.status(logs, "root")["deliveries"][0]["state"] == "delivered"


def test_mtime_and_size_skip_avoids_duplicate_detection(logs, monkeypatch):
    terminal = point("terminal:failed:1", points.SETTLED, 1, "failed")
    write_run(logs, status="failed", sequence=1, terminal_notification_points=[terminal])
    subscribe(logs, events=["terminal"])
    sent = []
    monkeypatch.setattr(wn.transport, "_deliver", lambda *a: sent.append(a))
    wn.tick(logs)
    wn.tick(logs)
    assert len(sent) == 1


@pytest.mark.parametrize("bad", [
    {"parent_link": {"root_workflow_run_id": "root"}},
    {"kind": "builder"},
])
def test_only_root_non_builder_runs_can_be_subscribed(logs, bad):
    write_run(logs, **bad)
    with pytest.raises(ValueError, match="root|Builder"):
        subscribe(logs)


def test_repeat_subscription_updates_event_selection(logs):
    write_run(logs)
    first = subscribe(logs, events=["terminal"])
    second = subscribe(logs, events=["attention"])
    assert second["id"] == first["id"] and second["updated"] is True
    assert second["events"] == ["attention"]


def test_unsubscribe_supersedes_pending_deliveries(logs):
    terminal = point("terminal:failed:1", points.SETTLED, 1, "failed")
    write_run(logs, status="failed", sequence=1, terminal_notification_points=[terminal])
    subscribe(logs, events=["terminal"])
    wn._detect(logs, db_subscription(logs, wn.status(logs, "root")["subscriptions"][0]["id"]))
    result = wn.unsubscribe(logs, "root", "hook")
    assert result["subscriptions"][0]["state"] == "unsubscribed"
    assert result["deliveries"][0]["state"] == "superseded"


def test_payload_is_frozen_at_insertion(logs):
    terminal = point("terminal:failed:1", points.SETTLED, 1, "failed", result={"summary": "first"})
    write_run(logs, status="failed", sequence=1, failure_reason="first", terminal_notification_points=[terminal])
    subscribe(logs, events=["terminal"])
    sub = wn.status(logs, "root")["subscriptions"][0]
    wn._detect(logs, db_subscription(logs, sub["id"]))
    write_run(logs, status="failed", sequence=2, failure_reason="changed", terminal_notification_points=[terminal])
    with wn.transport._db(logs) as db:
        payload = json.loads(db.execute("SELECT payload FROM workflow_deliveries").fetchone()[0])
    assert payload["result"]["summary"] == "first"


def test_reserved_subscription_activates_after_run_creation_and_discards_orphan(logs):
    reserved = wn.subscribe(logs, "future", "hook", reservation=True)
    assert reserved["state"] == "reserved"
    write_run(logs, run_id="future")
    wn.activate(logs, "future")
    with wn.transport._db(logs) as db:
        assert db.execute("SELECT state FROM workflow_subscriptions WHERE id=?", (reserved["id"],)).fetchone()[0] == "active"
    orphan = wn.subscribe(logs, "orphan", "hook", reservation=True)
    assert wn.discard_unstarted(logs, "orphan") is True
    with wn.transport._db(logs) as db:
        assert db.execute("SELECT 1 FROM workflow_subscriptions WHERE id=?", (orphan["id"],)).fetchone() is None


def test_expired_orphan_reservation_is_removed_by_tick(logs, monkeypatch):
    sub = wn.subscribe(logs, "orphan", "hook", reservation=True)
    old = time.time() - 5 * wn.transport.LEASE_SECONDS - 1
    with wn.transport._db(logs) as db:
        db.execute("UPDATE workflow_subscriptions SET created=?,updated=?", (old, old))
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        assert db.execute("SELECT 1 FROM workflow_subscriptions WHERE id=?", (sub["id"],)).fetchone() is None


def test_legacy_terminal_state_is_synthesized_without_points(logs):
    write_run(logs, status="completed", sequence=4)
    subscribe(logs, events=["terminal"])
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        row = db.execute("SELECT payload FROM workflow_deliveries").fetchone()
    assert row is not None and json.loads(row[0])["snapshot_source"] == "legacy_state"


def test_point_error_reconciles_current_terminal_state(logs):
    write_run(logs, status="failed", sequence=4, notification_point_error=4)
    subscribe(logs, events=["terminal"])
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        payload = json.loads(db.execute("SELECT payload FROM workflow_deliveries").fetchone()[0])
    assert payload["snapshot_source"] == "reconciled_state"


def test_attention_cap_does_not_suppress_terminal_delivery(logs):
    attention = [point(f"attention:needs_attention:{i}:root:None", points.ATTENTION, i, "needs_attention") for i in range(1, 53)]
    terminal = point("terminal:completed", points.SETTLED, 53, "completed")
    write_run(logs, status="running", sequence=0)
    subscribe(logs)
    write_run(logs, status="completed", sequence=53, notification_points=attention,
              terminal_notification_points=[terminal], notification_evicted={"attention": 2})
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        assert db.execute("SELECT count(*) FROM workflow_deliveries WHERE event=?", (points.ATTENTION,)).fetchone()[0] == 50
        assert db.execute("SELECT count(*) FROM workflow_deliveries WHERE event=?", (points.SETTLED,)).fetchone()[0] == 1


def test_failed_delivery_retry_keeps_notification_id_and_run_untouched(logs, monkeypatch):
    terminal = point("terminal:failed:1", points.SETTLED, 1, "failed")
    write_run(logs, status="failed", sequence=1, terminal_notification_points=[terminal])
    subscribe(logs, events=["terminal"])
    monkeypatch.setattr(wn.transport, "_deliver", lambda *a: (_ for _ in ()).throw(OSError("receiver down")))
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        first = db.execute("SELECT id,state,attempts FROM workflow_deliveries").fetchone()
        db.execute("UPDATE workflow_deliveries SET due=0")
    run_before = (logs.parent / "workflow-runs" / "root.json").read_bytes()
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        second = db.execute("SELECT id,state,attempts FROM workflow_deliveries").fetchone()
    assert first[0] == second[0] and second[2] == 2 and run_before == (logs.parent / "workflow-runs" / "root.json").read_bytes()


def test_shared_capacity_counts_task_and_workflow_subscriptions(logs):
    from polybridge import notifications
    write_run(logs)
    with notifications._db(logs) as db:
        for i in range(notifications.MAX_SUBSCRIPTIONS):
            db.execute("INSERT INTO subscriptions(id,task_id,destination,state,created,updated) VALUES(?,?,?,?,?,?)", (f"task-{i}", f"task-{i}", "hook", "pending", 0, 0))
    with pytest.raises(ValueError, match="capacity"):
        subscribe(logs)


def test_historical_null_snapshot_is_not_rebuilt_from_current_run(logs):
    write_run(logs, sequence=0)
    subscribe(logs, events=["terminal"])
    historical = point("terminal:failed:1", points.SETTLED, 1, "failed")
    historical["snapshot"] = None
    write_run(logs, status="failed", sequence=1, terminal_notification_points=[historical])
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        payload = json.loads(db.execute("SELECT payload FROM workflow_deliveries").fetchone()[0])
    assert payload["status"] == "failed" and payload["name"] is None and payload["result"] is None


def test_artifact_notes_are_not_in_notification_payload(logs, monkeypatch):
    from polybridge import artifacts
    terminal = point("terminal:failed:1", points.SETTLED, 1, "failed")
    write_run(logs, status="failed", sequence=1, terminal_notification_points=[terminal])
    subscribe(logs, events=["terminal"])
    monkeypatch.setattr(artifacts, "workflow_page", lambda *a, **k: {"state": "available", "items": [{"artifact_id": "a", "note": "private", "kind": "report", "path": "/tmp/a", "workflow": {"node_id": "n", "execution_id": "e", "task_id": "t", "availability": "available"}}], "next_cursor": None})
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        body = db.execute("SELECT payload FROM workflow_deliveries").fetchone()[0]
    assert b"private" not in body and b'"note"' not in body


def test_unreadable_root_does_not_starve_healthy_subscription(logs, monkeypatch):
    write_run(logs, run_id="bad", sequence=1)
    write_run(logs, run_id="good", status="failed", sequence=1,
              terminal_notification_points=[point("terminal:failed:1", points.SETTLED, 1, "failed")])
    wn.subscribe(logs, "bad", "hook", ["terminal"])
    wn.subscribe(logs, "good", "hook", ["terminal"])
    original = wn._run
    def flaky(log_dir, run_id):
        if run_id == "bad":
            raise ValueError("unreadable")
        return original(log_dir, run_id)
    monkeypatch.setattr(wn, "_run", flaky)
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        assert db.execute("SELECT count(*) FROM workflow_deliveries WHERE event=?", (points.SETTLED,)).fetchone()[0] == 1


def test_old_subscription_with_recent_durable_point_is_detected(logs):
    write_run(logs, status="running", sequence=0)
    sub = subscribe(logs, events=["terminal"])
    terminal = point("terminal:failed:1", points.SETTLED, 1, "failed")
    terminal["observed_at"] = time.time()
    write_run(logs, status="failed", sequence=1, terminal_notification_points=[terminal])
    old = time.time() - wn.transport.RETENTION_SECONDS - 1
    with wn.transport._db(logs) as db:
        db.execute("UPDATE workflow_subscriptions SET updated=? WHERE id=?", (old, sub["id"]))
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        assert db.execute("SELECT state FROM workflow_subscriptions WHERE id=?", (sub["id"],)).fetchone()[0] != "expired"


def test_reconciled_attention_with_started_invocation_is_immediate(logs, monkeypatch):
    write_run(logs, status="running", sequence=0)
    subscribe(logs, events=["attention"])
    write_run(logs, status="needs_attention", sequence=1, attention_reason="child question",
              attention_source={"workflow_run_id": "child", "path": "node"},
              activations=[{"invocation": {"started": True}}])
    monkeypatch.setattr(points, "tree_settlement", lambda *a, **k: "pending")
    monkeypatch.setattr(wn.transport, "_deliver", lambda *a: None)
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        row = db.execute("SELECT state,payload FROM workflow_deliveries").fetchone()
    assert row[0] == "delivered"
    assert "tree_settlement_unconfirmed" not in json.loads(row[1])


def test_large_real_run_record_is_read_with_notification_metadata_budget(logs):
    from polybridge.workflows import WorkflowStore
    write_run(logs, sequence=1, metadata="x" * (300 * 1024))
    run = ORIGINAL_RUN(logs, "root")
    assert len(run["metadata"]) == 300 * 1024


def test_old_unseen_point_does_not_reactivate_expired_subscription(logs):
    write_run(logs, sequence=0)
    sub = subscribe(logs, events=["terminal"])
    old = time.time() - wn.transport.RETENTION_SECONDS - 1
    terminal = point("terminal:failed:1", points.SETTLED, 1, "failed")
    terminal["observed_at"] = "2020-01-01T00:00:00+00:00"
    write_run(logs, status="failed", sequence=1, terminal_notification_points=[terminal])
    with wn.transport._db(logs) as db:
        db.execute("UPDATE workflow_subscriptions SET updated=? WHERE id=?", (old, sub["id"]))
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        assert db.execute("SELECT state FROM workflow_subscriptions WHERE id=?", (sub["id"],)).fetchone()[0] == "expired"


def test_blocked_batch_rotates_to_healthy_subscription(logs, monkeypatch):
    for index in range(wn.transport.BATCH):
        write_run(logs, run_id=f"bad{index}", sequence=0)
        subscribe(logs, f"bad{index}", events=["terminal"])
        bad_point = point(f"terminal:failed:{index}", points.SETTLED, 1, "failed")
        write_run(logs, run_id=f"bad{index}", status="failed", sequence=1, terminal_notification_points=[bad_point])
        sub = wn.status(logs, f"bad{index}")["subscriptions"][0]
        wn._detect(logs, db_subscription(logs, sub["id"]))
    terminal = point("terminal:failed:1", points.SETTLED, 1, "failed")
    write_run(logs, run_id="healthy", status="failed", sequence=1, terminal_notification_points=[terminal])
    subscribe(logs, "healthy", events=["terminal"])
    healthy_sub = wn.status(logs, "healthy")["subscriptions"][0]
    wn._detect(logs, db_subscription(logs, healthy_sub["id"]))
    for index in range(wn.transport.BATCH):
        (logs.parent / "workflow-runs" / f"bad{index}.json").write_text("corrupt")
    original = wn._run
    def blocked(log_dir, run_id):
        if run_id.startswith("bad"):
            raise ValueError("pending tree")
        return original(log_dir, run_id)
    monkeypatch.setattr(wn, "_run", blocked)
    wn.tick(logs)
    wn.tick(logs)
    with wn.transport._db(logs) as db:
        assert db.execute("SELECT count(*) FROM workflow_deliveries WHERE subscription_id=(SELECT id FROM workflow_subscriptions WHERE workflow_run_id='healthy')").fetchone()[0] == 1
