import json
from pathlib import Path

import pytest

from polybridge import events, inbox, workflows


def test_pending_survives_reload_and_delivery_removes_only_exact_id(tmp_path):
    first = inbox.make_message("same text", None)
    second = inbox.make_message("same text", None)
    inbox.append_locked(tmp_path, "task", first)
    inbox.append_locked(tmp_path, "task", second)
    assert [m["id"] for m in inbox.pending_messages(tmp_path, "task")] == [first["id"], second["id"]]
    log = events.EventLog(events.events_path(tmp_path, "task"), "task")
    log.write("user_message", {"text": "same text", "message_id": first["id"]})
    log.close()
    assert [m["id"] for m in inbox.pending_messages(tmp_path, "task")] == [second["id"]]


def test_terminal_ids_not_lost_when_old_event_outside_recent_tail(tmp_path):
    message = inbox.make_message("feedback", None)
    inbox.append_locked(tmp_path, "task", message)
    path = events.events_path(tmp_path, "task")
    path.write_text(json.dumps({"kind": "undelivered", "message_id": message["id"]}) + "\n" + (json.dumps({"kind": "assistant_text", "text": "x" * 10000}) + "\n") * 120)
    assert inbox.pending_messages(tmp_path, "task") == []


def test_torn_rows_and_oversized_rows_do_not_hide_following_queue(tmp_path):
    message = inbox.make_message("feedback", None)
    path = inbox.inbox_path(tmp_path, "task")
    path.write_bytes(b"x" * 1_000_002 + b"\n" + (json.dumps(message) + "\n").encode() + b'{"id":"torn"')
    assert [m["id"] for m in inbox.pending_messages(tmp_path, "task")] == [message["id"]]


def test_builder_canonical_ids_mapping_and_claimed_delivery(tmp_path):
    storage = workflows.WorkflowStore(tmp_path)
    logs = tmp_path / "tasks"
    logs.mkdir()
    run = storage.create_run({"name": "builder", "orchestrator": {"backend": "codex"}}, "request", tmp_path, kind="builder")
    messages = [{"id": "queued", "prompt": "same", "status": "queued_to_agent", "inbox_message_id": "inboxid"}, {"id": "claimed", "prompt": "same", "status": "claimed"}, {"id": "later", "prompt": "same", "status": "pending"}]
    storage.update_run(run["workflow_run_id"], lambda r: r.update(builder_messages=messages, activations=[{"id": "activation", "role": "builder", "node_id": "builder", "feedback_ids": ["claimed"], "tasks": [{"task_id": "task", "status": "running"}]}]), "fixture")
    assert workflows.builder_feedback_ids(logs, "task") == ["claimed"]
    rows = workflows.builder_pending_messages(logs, "task", [{"id": "inboxid", "text": "same"}])
    assert [m["id"] for m in rows] == ["queued", "claimed", "later"]
    log = events.EventLog(events.events_path(logs, "task"), "task")
    log.write("user_message", {"message_id": "inboxid", "text": "same"})
    log.write("user_message", {"message_ids": ["claimed"], "text": "same"})
    log.close()
    assert [m["id"] for m in workflows.builder_pending_messages(logs, "task", [])] == ["later"]


async def test_owned_sender_is_durable_and_pump_takes_once(tmp_path):
    from polybridge.tasks import Task, TaskRegistry
    from datetime import datetime, timezone
    task = Task(task_id="task", backend="claude", session_id="s", repo_path=tmp_path, prompt="initial", max_turns=5, log_path=tmp_path / "task.jsonl", started_at=datetime.now(timezone.utc), live_input=True)
    registry = TaskRegistry(log_dir=tmp_path, open_monitor=False)
    response = await registry.send_message(task, "feedback")
    assert task.inbox_queue == __import__("collections").deque()
    assert inbox.pending_messages(tmp_path, "task")[0]["id"] == response["message_id"]
    assert [m["id"] for m in registry._take_pending(task)] == [response["message_id"]]
    assert registry._take_pending(task) == []


@pytest.mark.parametrize("receipt", [b"x" * 1_000_002 + b"\n", b'{"kind":"user_message","message_id":"torn"'])
def test_incomplete_delivery_evidence_omits_pending_instead_of_resurrecting(tmp_path, receipt):
    message = inbox.make_message("feedback", None)
    inbox.append_locked(tmp_path, "task", message)
    events.events_path(tmp_path, "task").write_bytes(receipt)
    assert inbox.pending_messages(tmp_path, "task") == []


def test_builder_association_failure_does_not_suppress_user_event(tmp_path, monkeypatch):
    from polybridge.tasks import Task, _write_event
    from datetime import datetime, timezone
    task = Task(task_id="task", backend="claude", session_id="s", repo_path=tmp_path, prompt="initial", max_turns=5, log_path=tmp_path / "task.jsonl", started_at=datetime.now(timezone.utc), workflow_builder=True)
    task.events = events.EventLog(events.events_path(tmp_path, "task"), "task")
    def fail(*args):
        raise OSError("unavailable")
    monkeypatch.setattr(workflows, "builder_feedback_ids", fail)
    _write_event(task, "user_message", {"source": "initial", "text": "actual request"})
    task.events.close()
    assert json.loads(events.events_path(tmp_path, "task").read_text())["text"] == "actual request"


@pytest.mark.parametrize("live_input,kind", [(False, "task_started"), (True, "user_message")])
def test_builder_initial_delivery_ids_cover_positional_and_live_input(tmp_path, monkeypatch, live_input, kind):
    from polybridge.tasks import Task, _write_event
    from datetime import datetime, timezone
    task = Task(task_id="task", backend="codex", session_id="s", repo_path=tmp_path, prompt="injected", max_turns=None, log_path=tmp_path / "task.jsonl", started_at=datetime.now(timezone.utc), workflow_builder=True, live_input=live_input)
    task.events = events.EventLog(events.events_path(tmp_path, "task"), "task")
    monkeypatch.setattr(workflows, "builder_feedback_ids", lambda *args: ["feedback-id"])
    _write_event(task, kind, {"source": "initial", "text": "actual feedback"})
    task.events.close()
    assert inbox.delivered_ids(tmp_path, "task") == {"feedback-id"}
