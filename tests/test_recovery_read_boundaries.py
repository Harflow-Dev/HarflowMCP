import asyncio
import json
import os
import threading
from datetime import datetime, timezone

import pytest

from harflow_mcp import ctl, server, store, task_reads, tasks
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session


def _record(task_id="boundary", **changes):
    fields = dict(task_id=task_id, backend="claude", session_id="s", repo_path="/repo",
                  started_at="2026-10-23T00:00:00+00:00", status="running",
                  background_idle_seconds=42.0, live_input=True)
    fields.update(changes)
    return store.TaskRecord(**fields)


def test_ctl_recovery_activity_reads_bounded_metadata_without_prompt_or_replay(tmp_path, monkeypatch, capsys):
    root = tmp_path / "state"
    with state_session(resolve_layout(root=root)):
        logs = root / "tasks"
        logs.mkdir()
        store.write(logs, _record())
        monkeypatch.setattr(ctl, "default_log_dir", lambda: logs)
        monkeypatch.setattr(ctl.store, "read", lambda _dir, _id, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected full read")) if kwargs.get("include_prompt", True) else _record())
        monkeypatch.setattr("harflow_mcp.workflow_inspection.guard_task_read", lambda *args: None)
        monkeypatch.setattr("harflow_mcp.workflow_inspection.managed_reader", lambda _dir: None)
        monkeypatch.setattr("harflow_mcp.recovery_activity.page", lambda *args: {"events": [], "notices": [], "next_cursor": None, "reset": False, "has_more": False, "source": "backend_transcript", "display_only": True})
        assert ctl._main(["recovery-activity", "boundary", "--json", "--limit", "10"]) == 0
        assert json.loads(capsys.readouterr().out)["recovery_activity"]["display_only"] is True


@pytest.mark.asyncio
async def test_get_task_events_runs_recovery_reader_off_event_loop(tmp_path, monkeypatch):
    root = tmp_path / "state"
    log_dir = root / "tasks"
    registry = tasks.TaskRegistry(log_dir=log_dir, open_monitor=False)
    record = _record("recovery", owner={"pid": 999_999_999, "start_time": "never", "markers": []}, status="running")
    with state_session(resolve_layout(root=root)):
        store.write(log_dir, record)

        monkeypatch.setattr(server, "_registry", registry)
        monkeypatch.setattr(server, "_guard_task_read", lambda *_: asyncio.sleep(0))
        monkeypatch.setattr(server, "read_page", lambda *args, **kwargs: type("Page", (), {
            "events": [], "has_more": False, "next_before_seq": None,
            "next_after_seq": None, "skipped_oversized": 0,
        })())
        entered = threading.Event()
        release = threading.Event()

        def blocked(*args, **kwargs):
            entered.set()
            assert release.wait(2), "recovery reader was not released"
            return {"events": [], "next_cursor": None, "has_more": False}

        monkeypatch.setattr("harflow_mcp.recovery_activity.page", blocked)
        pending = asyncio.create_task(server.get_task_events("recovery", recovery_cursor="x"))
        assert await asyncio.to_thread(entered.wait, 1)

        heartbeat = asyncio.Event()
        asyncio.get_running_loop().call_soon(heartbeat.set)
        await asyncio.wait_for(heartbeat.wait(), timeout=0.5)

        release.set()
        result = await asyncio.wait_for(pending, timeout=2)
        assert result["task_id"] == "recovery"
        assert result["recovery_activity"]["events"] == []


def test_list_tasks_exposes_new_bound_and_supervision_fields_for_indexed_records(tmp_path, monkeypatch):
    root = tmp_path / "state"
    with state_session(resolve_layout(root=root)):
        logs = root / "tasks"
        logs.mkdir()
        record = _record("boundary", owner={"pid": 999_999_999, "start_time": "never", "markers": []}, status="running")
        store.write(logs, record)
        # Process visibility is unavailable in the sandbox; keep the catalog/record/raw-log
        # path real while making the owner-loss classification deterministic.
        monkeypatch.setattr("harflow_mcp.identity.identity_check", lambda _owner: "dead")
        raw_log = store.log_path(logs, record.task_id)
        raw_log.write_text("raw output\n", encoding="utf-8")
        expected_mtime = 1_792_713_600.0
        os.utime(raw_log, (expected_mtime, expected_mtime))
        result = task_reads.list_tasks(logs, fields=["background_idle_seconds", "output_capture", "input_channel", "last_captured_output_at"])
        item = result["items"][0]
        assert item["output_capture"] == "lost_with_owner"
        assert item["input_channel"] == "closed_with_owner"
        assert item["last_captured_output_at"] == datetime.fromtimestamp(expected_mtime, timezone.utc).isoformat()
