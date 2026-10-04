"""Monitor paging is a frozen transport snapshot, not a changing run read."""
import json
import os
import time

import pytest

from polybridge import workflow_responses


def test_snapshot_pages_remain_lossless_when_live_run_changes(tmp_path):
    run = {"workflow_run_id": "run", "status": "running", "summary": "x" * 400000, "activations": [{"id": "a", "status": "running"}]}
    first = workflow_responses.monitor_snapshot(run, "run", tmp_path)
    run["activations"][0]["status"] = "completed"
    text, cursor = first["chunk"], first["next_cursor"]
    while cursor:
        page = workflow_responses.monitor_snapshot(None, "run", tmp_path, cursor)
        assert len(json.dumps(page).encode()) < 280 * 1024
        assert page["content_sha256"] == first["content_sha256"]
        text += page["chunk"]
        cursor = page["next_cursor"]
    restored = json.loads(text)
    assert restored["summary"] == "x" * 400000
    assert restored["activations"][0]["status"] == "running"


def test_snapshot_cursor_cannot_read_other_runs_or_arbitrary_paths(tmp_path):
    first = workflow_responses.monitor_snapshot({"workflow_run_id": "run", "summary": "x" * 400000}, "run", tmp_path)
    with pytest.raises(ValueError):
        workflow_responses.monitor_snapshot(None, "other", tmp_path, first["next_cursor"])
    with pytest.raises(ValueError):
        workflow_responses.monitor_snapshot(None, "run", tmp_path, "../../etc/passwd")
    assert (next(tmp_path.iterdir()).stat().st_mode & 0o777) == 0o600


def test_snapshot_expiry_removes_cache_and_requires_refresh(tmp_path):
    first = workflow_responses.monitor_snapshot({"workflow_run_id": "run", "summary": "x" * 400000}, "run", tmp_path)
    path = next(tmp_path.iterdir())
    os.utime(path, (time.time() - 1000, time.time() - 1000))
    with pytest.raises(ValueError, match="expired"):
        workflow_responses.monitor_snapshot(None, "run", tmp_path, first["next_cursor"])
    assert not path.exists()


def test_forged_cursor_run_identity_cannot_relabel_snapshot(tmp_path):
    import base64
    page = workflow_responses.monitor_snapshot({"workflow_run_id": "run", "summary": "x" * 400000}, "run", tmp_path)
    cursor = json.loads(base64.urlsafe_b64decode(page["next_cursor"]))
    cursor["run"] = "other"
    forged = base64.urlsafe_b64encode(json.dumps(cursor).encode()).decode()
    with pytest.raises(ValueError):
        workflow_responses.monitor_snapshot(None, "other", tmp_path, forged)


def test_finished_paging_removes_snapshot_file(tmp_path):
    page = workflow_responses.monitor_snapshot({"workflow_run_id": "run", "summary": "x" * 400000}, "run", tmp_path)
    while page["next_cursor"]:
        page = workflow_responses.monitor_snapshot(None, "run", tmp_path, page["next_cursor"])
    assert not list(tmp_path.iterdir())


def test_cli_snapshot_pages_do_not_reload_changing_run(monkeypatch, tmp_path, capsys):
    from polybridge import ctl, server
    from unittest.mock import AsyncMock
    monkeypatch.setattr(ctl, "default_log_dir", lambda: tmp_path / "tasks")
    monkeypatch.setattr(server, "_verified_workflow_caller", AsyncMock(return_value=None))
    read = AsyncMock(return_value={"workflow_run_id": "run", "summary": "x" * 400000})
    monkeypatch.setattr(server, "_workflow_call", read)
    assert ctl.main(["workflow-status", "run", "--monitor-view", "--snapshot", "--json"]) == 0
    first = json.loads(capsys.readouterr().out)["result"]
    assert ctl.main(["workflow-status", "run", "--monitor-view", "--snapshot", "--cursor", first["next_cursor"], "--json"]) == 0
    assert read.await_count == 1


def test_managed_cli_snapshot_is_refused_before_any_content(monkeypatch, tmp_path, capsys):
    from polybridge import ctl, server
    from unittest.mock import AsyncMock
    monkeypatch.setattr(ctl, "default_log_dir", lambda: tmp_path / "tasks")
    monkeypatch.setattr(server, "_verified_workflow_caller", AsyncMock(return_value=object()))
    read = AsyncMock()
    monkeypatch.setattr(server, "_workflow_call", read)
    assert ctl.main(["workflow-status", "run", "--monitor-view", "--snapshot", "--json"]) != 0
    assert "only available to the local Monitor" in capsys.readouterr().out
    assert read.await_count == 0
