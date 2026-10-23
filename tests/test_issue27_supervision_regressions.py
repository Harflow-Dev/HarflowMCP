from dataclasses import replace

from harflow_mcp import store, supervision


def _record(tmp_path, **changes):
    base = store.TaskRecord(
        task_id="supervision-task", backend="codex", session_id=None,
        repo_path=str(tmp_path), started_at="2026-10-23T00:00:00+00:00",
        live_input=True, pid=None, status="running",
    )
    return replace(base, **changes)


def test_observed_terminal_history_is_not_retroactively_orphaned(tmp_path):
    record = _record(tmp_path, status="completed", finished_at="2026-10-23T00:01:00+00:00")
    log = store.log_path(tmp_path, record.task_id)
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text("{}\n")
    evidence = supervision.evidence(record, log, observed_terminal=True)
    assert evidence["capture_state"] == "observed_terminal"
    assert evidence["owner_state"] == "undecidable"
    assert evidence["process_state"] == "undecidable"


def test_missing_log_reports_unknown_cutoff_without_claiming_liveness(tmp_path, monkeypatch):
    monkeypatch.setattr("harflow_mcp.identity.identity_check", lambda *_: "undecidable")
    record = _record(tmp_path, owner={"pid": 123, "start_time": "start", "markers": []})
    evidence = supervision.evidence(record, tmp_path / "missing.jsonl")
    assert evidence["owner_state"] == "undecidable"
    assert evidence["process_state"] == "undecidable"
    assert evidence["capture_state"] == "unknown"
    assert evidence["last_captured_output_at"] is None
    assert evidence["last_captured_output_source"] is None


def test_supervision_fields_include_compatibility_aliases(tmp_path):
    record = _record(tmp_path)
    log = store.log_path(tmp_path, record.task_id)
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text("{}\n")
    fields = supervision.fields(record, log)
    assert fields["output_capture"] == fields["supervision"]["capture_state"]
    assert fields["input_channel"] == fields["supervision"]["live_input_state"]
    assert fields["last_captured_output_at"] == fields["supervision"]["last_captured_output_at"]
