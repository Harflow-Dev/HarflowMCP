"""Caller loss never follows scheduler loss or unverifiable process evidence."""
import copy
import os
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from harflow_mcp import workflow_interactions as i, workflows as w, identity
from harflow_mcp import workflow_goals as goals
from test_workflow_delegation import graph


@pytest.fixture
def pending(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    storage = w.WorkflowStore(tmp_path)
    run = storage.create_run(w.validate_definition(graph()), "request", tmp_path, goal="first")
    evidence = {"kind": "external_parent", "identity": {"pid": 1234, "start_time": "qualified", "markers": []}}
    run = storage.update_run(run["workflow_run_id"], lambda r: r.update(status="paused", interaction_owner="caller", interaction_caller=evidence), "prepare")
    return storage, run


@pytest.mark.parametrize("verdict,reason", [("alive", "start_time_match"), ("undecidable", "ps_failed"), ("undecidable", "markers_missing"), ("dead", "legacy_markers_not_seen")])
def test_monitor_refuses_alive_or_unknown(pending, monkeypatch, verdict, reason):
    storage, run = pending
    monkeypatch.setattr(identity, "check_detail", lambda evidence: (verdict, reason))
    with pytest.raises(w.WorkflowError, match="original caller"):
        goals.revise(storage, run["workflow_run_id"], "next", 1, interaction_owner="monitor")
    assert "interaction_handover" not in storage.get_run(run["workflow_run_id"])


@pytest.mark.parametrize("reason", ["pid_absent", "start_time_differs"])
def test_dead_handover_and_returning_caller(pending, monkeypatch, reason):
    storage, run = pending
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", reason))
    revised = goals.revise(storage, run["workflow_run_id"], "next", 1, interaction_owner="monitor")
    assert revised["interaction_owner"] == "monitor"
    assert revised["interaction_handover"]["responder"] == "monitor"
    with pytest.raises(w.WorkflowError, match="original caller"):
        goals.revise(storage, run["workflow_run_id"], "return", 2)


def test_bad_revision_does_not_claim_authority(pending, monkeypatch):
    storage, run = pending
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    with pytest.raises(w.WorkflowError, match="stale"):
        goals.revise(storage, run["workflow_run_id"], "next", 999, interaction_owner="monitor")
    assert storage.get_run(run["workflow_run_id"])["interaction_owner"] == "caller"


def test_concurrent_goal_revision_accepted_once(pending, monkeypatch):
    storage, run = pending
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    def change():
        try:
            goals.revise(storage, run["workflow_run_id"], "next", 1, interaction_owner="monitor")
            return True
        except w.WorkflowError:
            return False
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(lambda _: change(), range(2))) == 1


def test_live_client_survives_dead_server(pending):
    storage, run = pending
    live = identity.capture(os.getpid(), [])
    run.update(interaction_caller={"kind": "external_parent", "identity": live}, supervisor_identity={"pid": 99999999, "start_time": "gone", "markers": []})
    assert i.owner_state(run)[0] == "alive"
    with pytest.raises(w.WorkflowError, match="alive"):
        i.authorize(storage, run, "monitor", "resume")


def test_missing_evidence_and_agent_responder_refused(pending, monkeypatch):
    storage, run = pending
    run.pop("interaction_caller")
    assert i.owner_state(run)[0] == "unknown"
    with pytest.raises(w.WorkflowError):
        i.authorize(storage, run, "monitor", "resume")
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    with pytest.raises(w.WorkflowError):
        i.authorize(storage, run, "agent", "resume")


def test_strict_evidence():
    for evidence in ({}, {"kind": "external_parent", "identity": {"pid": 42, "start_time": None, "markers": []}}, {"kind": "unknown", "identity": None, "extra": True}):
        with pytest.raises(ValueError):
            i.validate_caller(evidence)


def test_ctl_capture_follows_uv_before_fork(monkeypatch):
    from harflow_mcp import lineage
    monkeypatch.setattr(i.os, "getppid", lambda: 100)
    table = {100: 200, 200: 1}
    monkeypatch.setattr(lineage, "process_table", lambda: table)
    monkeypatch.setattr(lineage, "_load_process_table", lambda: table)
    monkeypatch.setattr(i.subprocess, "run", lambda argv, **kw: SimpleNamespace(returncode=0, stdout="uv" if argv[-1] == "100" else "ClientApp"))
    monkeypatch.setattr(identity, "capture", lambda pid, markers: {"pid": pid, "start_time": "qualified", "markers": []})
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("alive", "start_time_match"))
    assert i.capture_transport_parent()["identity"]["pid"] == 200
    monkeypatch.setattr(i.os, "getppid", lambda: 999)
    assert i.admission()["identity"]["pid"] == 200
    monkeypatch.setattr(i, "_transport_caller", None)


def test_child_inherits_root_after_handover(pending, monkeypatch):
    storage, run = pending
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    goals.revise(storage, run["workflow_run_id"], "next", 1, interaction_owner="monitor")
    child = {"workflow_run_id": "child", "status": "needs_input", "interaction_owner": "caller", "parent_link": {"root_workflow_run_id": run["workflow_run_id"]}}
    i.authorize(storage, child, "monitor", "resume")
    with pytest.raises(w.WorkflowError):
        i.authorize(storage, child, "caller", "resume")
    assert i.projection(storage, child)["interaction_state"]["owner"] == "monitor"


@pytest.mark.parametrize("scheduler", ["alive", "undecidable"])
def test_recovery_keeps_scheduler_gate(pending, monkeypatch, scheduler):
    from harflow_mcp import workflow_recovery
    storage, run = pending
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    monkeypatch.setattr(identity, "identity_check", lambda evidence: scheduler)
    run = storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", supervisor_identity={"pid": 7777, "start_time": "scheduler", "markers": []}), "prepare")
    with pytest.raises(w.WorkflowError, match="scheduler is live or uncertain"):
        workflow_recovery.request(storage, run, "restore", "monitor")
    assert storage.get_run(run["workflow_run_id"])["interaction_owner"] == "caller"
    assert "resumed_at" not in storage.get_run(run["workflow_run_id"])


def test_recovery_handover_only_after_validation(pending, monkeypatch):
    from harflow_mcp import workflow_recovery
    storage, run = pending
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    monkeypatch.setattr(identity, "identity_check", lambda evidence: "dead")
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    run = storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", supervisor_identity={"pid": 7777, "start_time": "scheduler", "markers": []}), "prepare")
    accepted = workflow_recovery.request(storage, run, "restore", "monitor")
    assert accepted["interaction_handover"]["action"] == "recover"
    assert accepted["scheduler_recovery"]["reason"] == "restore"
    assert accepted["resumed_at"] == accepted["updated_at"]


def test_repeated_scheduler_recovery_request_stamps_once(pending, monkeypatch):
    from harflow_mcp import workflow_recovery
    storage, run = pending
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    monkeypatch.setattr(identity, "identity_check", lambda evidence: "dead")
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    run = storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", supervisor_identity={"pid": 7777, "start_time": "scheduler", "markers": []}), "prepare")
    first = workflow_recovery.request(storage, run, "restore", "monitor")
    stamp = first["resumed_at"]
    again = storage.update_run(run["workflow_run_id"], lambda r: None, "poll")
    assert again["scheduler_recovery"]
    second = workflow_recovery.request(storage, again, "restore again", "monitor")
    assert second["scheduler_recovery"]["reason"] == "restore again"
    assert second["resumed_at"] == stamp
    # A replacement scheduler that later dies makes the next request a new continuation.
    storage.update_run(run["workflow_run_id"], lambda r: r.update(supervisor_identity={"pid": 8888, "start_time": "replacement", "markers": []}), "replacement_started")
    third = workflow_recovery.request(storage, storage.get_run(run["workflow_run_id"]), "restore after second death", "monitor")
    assert third["resumed_at"] > stamp
    assert third["resumed_at"] == third["updated_at"]
    assert third["scheduler_recovery"]["replaced_owner"]["pid"] == 8888


def test_managed_changed_record_remains_unknown(pending, monkeypatch):
    from harflow_mcp import store
    storage, run = pending
    run["interaction_caller"] = {"kind": "managed_task", "identity": {"pid": 1234, "start_time": "qualified", "markers": []}, "task_id": "caller", "session_id": "conversation"}
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    monkeypatch.setattr(store, "read", lambda *args: None)
    assert i.owner_state(run, storage)[0] == "unknown"
    monkeypatch.setattr(store, "read", lambda *args: SimpleNamespace(pid=1235, start_time="new", markers=[]))
    assert i.owner_state(run, storage)[0] == "unknown"


def test_omitted_responder_cannot_bypass_handover(pending, monkeypatch):
    storage, run = pending
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    revised = goals.revise(storage, run["workflow_run_id"], "next", 1, interaction_owner="monitor")
    with pytest.raises(w.WorkflowError, match="original caller"):
        i.authorize(storage, revised, None, "resume")


def _managed_record(storage, task_id, *, parent=None, prompt="", session="conversation"):
    from harflow_mcp import store
    record = store.TaskRecord(task_id=task_id, backend="claude", session_id=session,
        repo_path=str(storage.root), started_at="2026-10-28T00:00:00+00:00",
        status="completed", exit_code=0, pid=1234 if task_id == "caller" else 1235,
        start_time="qualified", markers=[], parent_task_id=parent, prompt=prompt)
    store.write(storage.root / "tasks", record)
    return record


def _managed_run(run, record):
    return {**run, "interaction_caller": {"kind": "managed_task", "task_id": record.task_id,
        "session_id": record.session_id, "identity": identity.task_identity(record.pid, record.start_time, record.markers)}}


def test_managed_liveness_never_reads_oversized_prompt_sidecar(pending, monkeypatch):
    from pathlib import Path
    from harflow_mcp import store
    storage, run = pending
    record = _managed_record(storage, "caller", prompt="p" * (5 * 1024 * 1024), session=None)
    sidecars = list((storage.root / "tasks").glob("*.prompt.*.txt"))
    assert len(sidecars) == 1 and sidecars[0].stat().st_size > 4 * 1024 * 1024
    original = Path.read_bytes
    def no_prompt_read(path):
        assert path not in sidecars, "Caller authority must never hydrate the assignment"
        return original(path)
    monkeypatch.setattr(Path, "read_bytes", no_prompt_read)
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    observed = []
    original_read = store.read
    def read(*args, **kwargs):
        observed.append(kwargs)
        return original_read(*args, **kwargs)
    monkeypatch.setattr(store, "read", read)
    assert i.owner_state(_managed_run(run, record), storage) == ("dead", "pid_absent")
    assert observed and all(row["include_prompt"] is False and row["metadata_byte_limit"] == 4 * 1024 * 1024 and row["metadata_budget"] is not None for row in observed)


def test_completed_session_ancestor_does_not_block_handover(pending, monkeypatch):
    storage, run = pending
    _managed_record(storage, "ancestor")
    record = _managed_record(storage, "caller", parent="ancestor")
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    assert i.owner_state(_managed_run(run, record), storage) == ("dead", "pid_absent")
    _managed_record(storage, "later", parent="caller")
    assert i.owner_state(_managed_run(run, record), storage)[0] == "unknown"


def test_missing_session_ancestor_refuses_handover(pending, monkeypatch):
    storage, run = pending
    record = _managed_record(storage, "caller", parent="missing")
    monkeypatch.setattr(identity, "check_detail", lambda evidence: ("dead", "pid_absent"))
    assert i.owner_state(_managed_run(run, record), storage)[0] == "unknown"


def test_authority_root_uses_shared_bounded_metadata(pending, monkeypatch):
    storage, run = pending
    child = {"workflow_run_id": "child", "parent_link": {"root_workflow_run_id": run["workflow_run_id"]}}
    observed = []
    original = storage.get_run
    def get_run(*args, **kwargs):
        observed.append(kwargs)
        return original(*args, **kwargs)
    monkeypatch.setattr(storage, "get_run", get_run)
    assert i.authority_root(storage, child)["workflow_run_id"] == run["workflow_run_id"]
    assert observed[0]["metadata_byte_limit"] == 4 * 1024 * 1024
    assert observed[0]["metadata_budget"] is not None
