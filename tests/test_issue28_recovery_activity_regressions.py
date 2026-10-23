import json
from pathlib import Path

from harflow_mcp import store
from harflow_mcp.backends.claude_recovery import read_activity


def _record(tmp_path, session="session-1"):
    return store.TaskRecord(
        task_id="recovery-task", backend="claude", session_id=session,
        repo_path=str(tmp_path), started_at="2026-10-23T00:00:00+00:00",
        status="failed", exit_code=None,
    )


def _line(session, timestamp, uuid, content, kind="assistant"):
    return json.dumps({"sessionId": session, "timestamp": timestamp, "uuid": uuid,
                       "type": kind, "message": {"content": content}}) + "\n"


def test_recovery_activity_normalizes_text_tool_call_and_result(tmp_path):
    project = tmp_path / "projects" / "repo"
    project.mkdir(parents=True)
    session = "session-1"
    transcript = project / f"{session}.jsonl"
    transcript.write_text(
        _line(session, "2026-10-23T00:00:01+00:00", "a", [{"type": "text", "text": "hello"}])
        + _line(session, "2026-10-23T00:00:02+00:00", "b", [{"type": "tool_use", "id": "call", "name": "Read", "input": {"path": "x"}}])
        + _line(session, "2026-10-23T00:00:03+00:00", "c", [{"type": "tool_result", "tool_use_id": "call", "content": "ok"}], "user")
    )
    page = read_activity(_record(tmp_path), "2026-10-23T00:00:00+00:00", limit=10, projects=tmp_path / "projects")
    assert page["display_only"] is True
    assert [event["kind"] for event in page["events"]] == ["assistant_text", "tool_call", "tool_result"]
    assert page["events"][1]["call_id"] == "recovery:call"
    assert page["events"][2]["call_id"] == "recovery:call"


def test_recovery_activity_omits_partial_trailing_line_and_supports_cursor_append(tmp_path):
    project = tmp_path / "projects" / "repo"
    project.mkdir(parents=True)
    transcript = project / "session-1.jsonl"
    transcript.write_text(_line("session-1", "2026-10-23T00:00:01+00:00", "a", [{"type": "text", "text": "one"}]) + "{partial\n")
    record = _record(tmp_path)
    first = read_activity(record, "2026-10-23T00:00:00+00:00", limit=10, projects=tmp_path / "projects")
    assert [e["text"] for e in first["events"]] == ["one"]
    with transcript.open("a") as handle:
        handle.write(_line("session-1", "2026-10-23T00:00:02+00:00", "b", [{"type": "text", "text": "two"}]))
    second = read_activity(record, "2026-10-23T00:00:00+00:00", cursor=first["next_cursor"], limit=10, projects=tmp_path / "projects")
    assert [e["text"] for e in second["events"]] == ["two"]


def test_recovery_activity_missing_or_ambiguous_session_is_bounded(tmp_path):
    record = _record(tmp_path, session="bad session")
    page = read_activity(record, None, projects=tmp_path / "projects")
    assert page["events"] == [] and page["notices"]


def test_recovery_activity_filters_prior_session_events_and_bounds_page(tmp_path):
    project = tmp_path / "projects" / "repo"
    project.mkdir(parents=True)
    transcript = project / "session-1.jsonl"
    lines = [
        _line("session-1", "2026-10-22T23:59:59+00:00", "old", [{"type": "text", "text": "old"}]),
        *(_line("session-1", f"2026-10-23T00:00:{i:02d}+00:00", str(i), [{"type": "text", "text": str(i)}]) for i in range(1, 6)),
    ]
    transcript.write_text("".join(lines))
    page = read_activity(_record(tmp_path), "2026-10-23T00:00:00+00:00", limit=2, projects=tmp_path / "projects")
    assert [e["text"] for e in page["events"]] == ["4", "5"]
    assert "old" not in {e["text"] for e in page["events"]}
    assert any("bounded" in notice.lower() for notice in page["notices"])


def test_recovery_activity_tolerates_malformed_and_oversized_lines(tmp_path):
    project = tmp_path / "projects" / "repo"
    project.mkdir(parents=True)
    transcript = project / "session-1.jsonl"
    transcript.write_text("not-json\n" + _line("session-1", "2026-10-23T00:00:01+00:00", "a", [{"type": "text", "text": "ok"}]) + "x" * 400000 + "\n")
    page = read_activity(_record(tmp_path), "2026-10-23T00:00:00+00:00", projects=tmp_path / "projects")
    assert page["events"] == []
    assert page["notices"]
    assert page["display_only"] is True
