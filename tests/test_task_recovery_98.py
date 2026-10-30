"""Regression tests for isolated launch receipts and conservative abandoned-task settlement."""
from __future__ import annotations

from dataclasses import replace
import pytest
import subprocess

from harflow_mcp import identity, store, task_recovery, task_supervision
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session


@pytest.fixture
def owned_log_dir(tmp_path):
    layout = resolve_layout(root=tmp_path / "state")
    layout.root.mkdir()
    with state_session(layout):
        layout.tasks_directory.mkdir()
        yield layout.tasks_directory


def receipt(log_dir, task_id, *, state="spawned", **extra):
    info = log_dir.stat()
    value = {
        "format": task_supervision.FORMAT,
        "product": "harflow",
        "product_version": task_supervision.PRODUCT_VERSION,
        "v": 1,
        "task_id": task_id,
        "generation": "a" * 32,
        "root": str(log_dir.parent),
        "directory_identity": [info.st_dev + 1, info.st_ino],
        "state": state,
        "request_sha256": "b" * 64,
        "backend": "claude",
        "session_id": f"session-{task_id[:4]}",
        "owner": {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []},
    }
    value.update(extra)
    task_supervision._write(log_dir / f"{task_id}.launch.json", value)
    return value


def task_record(task_id, *, session_id=None, owner=None, status="running", **extra):
    fields = dict(
        task_id=task_id,
        backend="claude",
        session_id=session_id,
        repo_path="/tmp/repo",
        started_at="2026-01-01T00:00:00+00:00",
        status=status,
        owner=owner or {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []},
        pid=515151,
        start_time="Tue Jan  2 00:00:00 2001",
        markers=["claude-agent-marker"],
    )
    fields.update(extra)
    return store.TaskRecord(**fields)


def test_mixed_scan_returns_healthy_receipts_and_structured_refusal(owned_log_dir):
    good_id, bad_id = "1" * 32, "2" * 32
    receipt(owned_log_dir, good_id)
    bad = receipt(owned_log_dir, bad_id, session_id="refused-session")
    bad["generation"] = "invalid"
    task_supervision._write(owned_log_dir / f"{bad_id}.launch.json", bad)

    scan = task_supervision.scan_launches(owned_log_dir)

    assert [row["task_id"] for row in scan.entries] == [good_id]
    assert scan.refusals[bad_id].code == "generation"
    assert scan.refusals[bad_id].as_dict()["path"].endswith(f"{bad_id}.launch.json")
    assert scan.claims[0]["session_id"] == "refused-session"
    assert scan.claims[0]["deny_only"] is True
    with pytest.raises(task_supervision.LaunchDocumentError):
        scan.require_complete()


def test_opaque_receipt_blocks_session_reuse_without_poisoning_per_id_reads(owned_log_dir):
    opaque_id = "3" * 32
    (owned_log_dir / f"{opaque_id}.launch.json").write_text("not json")

    scan = task_supervision.scan_launches(owned_log_dir)
    assert opaque_id in scan.opaque
    with pytest.raises(task_supervision.LaunchDocumentError) as refused:
        task_supervision.session_reservations(owned_log_dir)
    assert refused.value.code == "associations_incomplete"
    assert task_supervision.read_launch(owned_log_dir, "4" * 32) is None


@pytest.mark.parametrize("raw", ["null", "[]", "17", '"receipt"'])
def test_non_object_receipt_is_reported_as_refusal(owned_log_dir, raw):
    task_id = "4" * 32
    (owned_log_dir / f"{task_id}.launch.json").write_text(raw)

    scan = task_supervision.scan_launches(owned_log_dir)

    assert task_id in scan.refusals
    assert task_id in scan.opaque
    assert scan.refusals[task_id].code == "format"


def test_refused_receipt_and_qualified_record_keep_both_session_claims_busy(owned_log_dir):
    task_id = "e" * 32
    value = receipt(owned_log_dir, task_id, session_id="receipt-session")
    value["generation"] = "malformed"
    task_supervision._write(owned_log_dir / f"{task_id}.launch.json", value)
    record = task_record(task_id, session_id="record-session", owner=value["owner"])
    store.write(owned_log_dir, record)

    claims = task_supervision.session_reservations(owned_log_dir)

    sessions = {claim.get("session_id") for claim in claims}
    assert {"receipt-session", "record-session"} <= sessions


def test_refused_receipt_with_malformed_session_uses_matching_record_claim(owned_log_dir):
    task_id = "f" * 32
    value = receipt(owned_log_dir, task_id, session_id=42)
    value["generation"] = "malformed"
    task_supervision._write(owned_log_dir / f"{task_id}.launch.json", value)
    record = task_record(task_id, session_id="record-session", owner=value["owner"])
    store.write(owned_log_dir, record)

    claims = task_supervision.session_reservations(owned_log_dir)

    assert any(claim.get("session_id") == "record-session" for claim in claims)
    assert any(claim.get("deny_only") is True and claim.get("session_id") is None for claim in claims)


def test_directory_binding_is_authoritative_and_legacy_inode_tolerates_device_drift(owned_log_dir):
    task_id = "5" * 32
    original = receipt(owned_log_dir, task_id, state="granted")
    original_bytes = (owned_log_dir / f"{task_id}.launch.json").read_bytes()
    assert task_supervision.read_launch(owned_log_dir, task_id)["state"] == "granted"
    assert (owned_log_dir / f"{task_id}.launch.json").read_bytes() == original_bytes

    original["directory_binding"] = {"path": str(owned_log_dir.resolve()), "inode": owned_log_dir.stat().st_ino}
    task_supervision._write(owned_log_dir / f"{task_id}.launch.json", original)
    assert task_supervision.read_launch(owned_log_dir, task_id)["directory_binding"] == original["directory_binding"]

    original["directory_binding"] = {"path": str(owned_log_dir.parent / "other"), "inode": owned_log_dir.stat().st_ino}
    task_supervision._write(owned_log_dir / f"{task_id}.launch.json", original)
    with pytest.raises(task_supervision.LaunchDocumentError) as refused:
        task_supervision.read_launch(owned_log_dir, task_id)
    assert refused.value.code == "identity_mismatch"


def test_dead_spawned_launch_is_marked_interrupted_without_inventing_exit(owned_log_dir, monkeypatch):
    task_id = "6" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    agent = {"pid": 515151, "start_time": "Tue Jan  2 00:00:00 2001", "markers": ["claude-agent-marker"]}
    receipt(owned_log_dir, task_id, session_id=None, owner=owner)
    store.write(owned_log_dir, task_record(task_id, session_id="learned-session", owner=owner))

    monkeypatch.setattr(identity, "identity_check", lambda saved: "dead" if saved["pid"] in {424242, 515151} else "undecidable")
    result = task_recovery.recover(owned_log_dir, task_id, held_session="learned-session")

    updated = store.read(owned_log_dir, task_id)
    settled = task_supervision.read_launch(owned_log_dir, task_id)
    assert result["outcome"] == "interrupted"
    assert updated.status == "failed" and updated.interruption["reason"] == "owner_and_agent_confirmed_dead"
    assert updated.exit_code is None and updated.output_drained is False
    assert settled["state"] == "settled" and settled["settled_reason"] == "abandoned"
    assert settled["reconciliation_pending"] is False


@pytest.mark.parametrize("owner_state,agent_state,code", [
    ("alive", "dead", "owner_not_dead"),
    ("dead", "undecidable", "agent_not_dead"),
])
def test_recovery_refuses_live_or_uncertain_process_generation(owned_log_dir, monkeypatch, owner_state, agent_state, code):
    task_id = "7" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    record = task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner)
    receipt(owned_log_dir, task_id, owner=owner)
    store.write(owned_log_dir, record)
    monkeypatch.setattr(identity, "identity_check", lambda saved: owner_state if saved["pid"] == 424242 else agent_state)

    with pytest.raises(task_recovery.RecoveryRefused) as refused:
        task_recovery.recover(owned_log_dir, task_id)
    assert refused.value.code == code
    assert store.read(owned_log_dir, task_id).status == "running"
    assert task_supervision.read_launch(owned_log_dir, task_id)["state"] == "spawned"


def test_potentially_spawned_granted_receipt_without_agent_identity_is_not_settled(owned_log_dir, monkeypatch):
    task_id = "8" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, state="granted", owner=owner)
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")

    with pytest.raises(task_recovery.RecoveryRefused) as refused:
        task_recovery.recover(owned_log_dir, task_id, human=True)
    assert refused.value.code == "agent_identity_unavailable"
    assert task_supervision.read_launch(owned_log_dir, task_id)["state"] == "granted"


def test_receipt_and_record_session_mismatch_refuses_recovery(owned_log_dir, monkeypatch):
    task_id = "b" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, session_id="receipt-session", owner=owner)
    store.write(owned_log_dir, task_record(task_id, session_id="record-session", owner=owner))
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")

    with pytest.raises(task_recovery.RecoveryRefused) as refused:
        task_recovery.recover(owned_log_dir, task_id)
    assert refused.value.code == "identity_mismatch"


def test_held_launch_lock_skips_process_probes(owned_log_dir, monkeypatch):
    from harflow_mcp import control

    task_id = "c" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, owner=owner)
    store.write(owned_log_dir, task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner))

    def should_not_probe(_saved):
        raise AssertionError("identity probes must wait until the owner releases its launch lock")

    monkeypatch.setattr(identity, "identity_check", should_not_probe)
    with control._flock_sync(owned_log_dir / f"{task_id}.launch.lock", timeout=0, what="test launch owner", layout=resolve_layout(log_dir=owned_log_dir)):
        result = task_recovery.recover_abandoned(owned_log_dir)

    assert result["recovered"] == []
    assert result["refusals"][task_id]["code"] == "recovery_locked"


def test_missing_agent_start_time_refuses_recovery(owned_log_dir, monkeypatch):
    task_id = "9" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, owner=owner)
    store.write(owned_log_dir, task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner, start_time=None))
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")

    with pytest.raises(task_recovery.RecoveryRefused) as refused:
        task_recovery.recover(owned_log_dir, task_id)
    assert refused.value.code == "durable_identity_missing"


def test_reused_pids_are_dead_for_saved_generations_and_recovery_never_signals(owned_log_dir, monkeypatch):
    task_id = "0" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    agent = {"pid": 515151, "start_time": "Tue Jan  2 00:00:00 2001", "markers": ["old-agent-marker"]}
    receipt(owned_log_dir, task_id, owner=owner)
    store.write(owned_log_dir, task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner,
                                           pid=agent["pid"], start_time=agent["start_time"], markers=agent["markers"]))

    # Both numeric PIDs now belong to different process generations.
    current = {
        424242: "Wed Jan  3 00:00:00 2001 replacement-owner",
        515151: "Thu Jan  4 00:00:00 2001 replacement-agent",
    }
    monkeypatch.setattr(identity, "_run_ps", lambda pid: subprocess.CompletedProcess(
        ["ps"], 0, current[int(pid)] + "\n", ""))
    signals = []
    monkeypatch.setattr(identity.os, "kill", lambda *args: signals.append(args))
    assert identity.identity_check(owner) == "dead"
    assert identity.identity_check(agent) == "dead"
    result = task_recovery.recover(owned_log_dir, task_id)

    assert result["outcome"] == "interrupted"
    assert task_supervision.read_launch(owned_log_dir, task_id)["state"] == "settled"
    assert signals == []


def test_human_recovery_can_rebind_only_after_both_saved_generations_are_dead(owned_log_dir, monkeypatch):
    task_id = "1" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    value = receipt(owned_log_dir, task_id, owner=owner,
                    directory_binding={"path": str(owned_log_dir.parent / "wrong"), "inode": owned_log_dir.stat().st_ino})
    store.write(owned_log_dir, task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner))
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")

    with pytest.raises(task_supervision.LaunchDocumentError) as refused:
        task_supervision.read_launch(owned_log_dir, task_id)
    assert refused.value.code == "identity_mismatch"
    task_recovery.recover(owned_log_dir, task_id, human=True)
    rebound = task_supervision.read_launch(owned_log_dir, task_id)
    assert rebound["directory_binding"] == {"path": str(owned_log_dir.resolve()), "inode": owned_log_dir.stat().st_ino}


def test_human_retry_rebinds_a_pending_recovery_intent(owned_log_dir, monkeypatch):
    """A previously recorded recovery intent remains repairable after directory drift."""
    from harflow_mcp import followups

    task_id = "6" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, owner=owner,
            followups={"task_id": "7" * 32, "ids": ["message-1"]})
    store.write(owned_log_dir, task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner))
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")
    monkeypatch.setattr(followups, "finish", lambda *args, **kwargs: False)

    with pytest.raises(task_recovery.RecoveryRefused) as first:
        task_recovery.recover(owned_log_dir, task_id)
    assert first.value.code == "followup_write_failed"
    pending = task_supervision.read_launch(owned_log_dir, task_id)
    assert pending["reconciliation_pending"] is True

    pending["directory_binding"] = {"path": str(owned_log_dir.parent / "drifted"), "inode": owned_log_dir.stat().st_ino}
    task_supervision._write(owned_log_dir / f"{task_id}.launch.json", pending)
    monkeypatch.setattr(followups, "finish", lambda *args, **kwargs: True)
    task_recovery.recover(owned_log_dir, task_id, human=True)
    repaired = task_supervision.read_launch(owned_log_dir, task_id)
    assert repaired["reconciliation_pending"] is False
    assert repaired["directory_binding"] == {"path": str(owned_log_dir.resolve()), "inode": owned_log_dir.stat().st_ino}


def test_waiting_for_resources_without_record_recovers_as_never_started(owned_log_dir, monkeypatch):
    task_id = "8" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, state="waiting_for_resources", owner=owner,
            resources={"names": ["resource:waiting"], "state": "waiting"})
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")

    result = task_recovery.recover(owned_log_dir, task_id)

    assert result["outcome"] == "never_started"
    settled = task_supervision.read_launch(owned_log_dir, task_id)
    assert settled["state"] == "not_started"
    assert settled["not_started_reason"] == "owner_died_before_spawn"
    assert settled["reconciliation_pending"] is False


def test_owner_death_during_worktree_creation_is_proven_pre_spawn(owned_log_dir, monkeypatch):
    task_id = "9" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, state="granted", owner=owner,
            worktree={"path": "/tmp/checkout", "state": "creating"})
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")

    outcome = task_recovery.recover(owned_log_dir, task_id)

    settled = task_supervision.read_launch(owned_log_dir, task_id)
    assert outcome["outcome"] == "never_started"
    assert settled["state"] == "not_started"
    assert settled["worktree"]["state"] == "unknown"
    assert settled["reconciliation_pending"] is False


def test_human_recovery_repairs_terminal_binding_without_rewriting_outcome(owned_log_dir, monkeypatch):
    task_id = "a" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, state="settled", owner=owner,
            directory_binding={"path": str(owned_log_dir.parent / "old-path"), "inode": owned_log_dir.stat().st_ino})
    record = task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner,
                         status="failed", finished_at="2026-01-02T00:00:00+00:00", exit_code=7)
    store.write(owned_log_dir, record)
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")

    outcome = task_recovery.recover(owned_log_dir, task_id, human=True)

    repaired = task_supervision.read_launch(owned_log_dir, task_id)
    assert outcome["outcome"] == "binding_repaired"
    assert repaired["state"] == "settled"
    assert repaired["directory_binding"] == {"path": str(owned_log_dir.resolve()), "inode": owned_log_dir.stat().st_ino}
    assert store.read(owned_log_dir, task_id) == record


def test_generation_change_during_process_probe_prevents_recovery(owned_log_dir, monkeypatch):
    task_id = "2" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, owner=owner)
    store.write(owned_log_dir, task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner))
    changed = False

    def replace_generation(saved):
        nonlocal changed
        if not changed:
            changed = True
            value = task_supervision.read_launch(owned_log_dir, task_id)
            value["generation"] = "c" * 32
            task_supervision._write(owned_log_dir / f"{task_id}.launch.json", value)
        return "dead"

    monkeypatch.setattr(identity, "identity_check", replace_generation)
    with pytest.raises(task_recovery.RecoveryRefused) as refused:
        task_recovery.recover(owned_log_dir, task_id)
    assert refused.value.code == "source_changed"
    assert store.read(owned_log_dir, task_id).status == "running"


def test_record_change_during_process_probe_prevents_compare_and_swap(owned_log_dir, monkeypatch):
    task_id = "3" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, owner=owner)
    original = task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner)
    store.write(owned_log_dir, original)
    changed = False

    def change_record(_saved):
        nonlocal changed
        if not changed:
            changed = True
            store.write(owned_log_dir, replace(original, status="cancelled", finished_at="2026-01-03T00:00:00+00:00"))
        return "dead"

    monkeypatch.setattr(identity, "identity_check", change_record)
    with pytest.raises(task_recovery.RecoveryRefused) as refused:
        task_recovery.recover(owned_log_dir, task_id)
    assert refused.value.code == "source_changed"
    assert store.read(owned_log_dir, task_id).status == "cancelled"


def test_failed_record_persistence_keeps_launch_unsettled(owned_log_dir, monkeypatch):
    task_id = "a" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, owner=owner)
    record = task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner)
    store.write(owned_log_dir, record)
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")
    monkeypatch.setattr(store, "write_landed", lambda *args, **kwargs: False)

    with pytest.raises(task_recovery.RecoveryRefused) as refused:
        task_recovery.recover(owned_log_dir, task_id)
    assert refused.value.code == "record_write_failed"
    assert task_supervision.read_launch(owned_log_dir, task_id)["state"] == "spawned"
    assert "reconciliation_pending" not in task_supervision.read_launch(owned_log_dir, task_id)


def test_recovery_retries_a_durable_intent_after_record_write_recovers(owned_log_dir, monkeypatch):
    task_id = "d" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    receipt(owned_log_dir, task_id, owner=owner)
    store.write(owned_log_dir, task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner))
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")
    real_write_landed = store.write_landed
    monkeypatch.setattr(store, "write_landed", lambda *args, **kwargs: False)

    with pytest.raises(task_recovery.RecoveryRefused) as refused:
        task_recovery.recover(owned_log_dir, task_id)
    assert refused.value.code == "record_write_failed"
    assert "recovery_intent" in task_supervision.read_launch(owned_log_dir, task_id)

    monkeypatch.setattr(store, "write_landed", real_write_landed)
    result = task_recovery.recover(owned_log_dir, task_id)
    assert result["outcome"] == "interrupted"
    assert task_supervision.read_launch(owned_log_dir, task_id)["reconciliation_pending"] is False


def test_pre_spawn_reconciliation_resumes_after_settled_receipt_and_unknown_worktree(owned_log_dir, monkeypatch):
    from harflow_mcp import resources as resource_ledger
    from harflow_mcp import task_resources

    task_id = "e" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    reservation_id = "f" * 32
    receipt(owned_log_dir, task_id, state="granted", owner=owner,
            worktree={"path": "/tmp/partial-worktree", "state": "creating"},
            resources={"names": ["checkout:repo"], "reservation_id": reservation_id, "state": "granted"})
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")

    class BrokenLedger:
        def entries(self):
            raise resource_ledger.ResourceError("ledger temporarily unavailable")

    real_ledger_for = task_resources.ledger_for
    monkeypatch.setattr(task_resources, "ledger_for", lambda _log_dir: BrokenLedger())
    with pytest.raises(resource_ledger.ResourceError):
        task_recovery.recover(owned_log_dir, task_id)
    intermediate = task_supervision.read_launch(owned_log_dir, task_id)
    assert intermediate["state"] == "not_started"
    assert intermediate["worktree"]["state"] == "unknown"
    assert intermediate["reconciliation_pending"] is True
    assert intermediate["recovery_intent"]["reason"] == "owner_died_before_spawn"

    monkeypatch.setattr(task_resources, "ledger_for", real_ledger_for)
    result = task_recovery.recover_abandoned(owned_log_dir)
    final = task_supervision.read_launch(owned_log_dir, task_id)
    assert result["recovered"] and final["reconciliation_pending"] is False


def test_partial_resource_release_is_completed_idempotently_on_retry(owned_log_dir, monkeypatch):
    from harflow_mcp import resources as resource_ledger
    from harflow_mcp import task_resources

    task_id = "f" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    agent = {"pid": 515151, "start_time": "Tue Jan  2 00:00:00 2001", "markers": ["claude-agent-marker"]}
    real_ledger = task_resources.ledger_for(owned_log_dir)
    primary = real_ledger.request(["resource:first"], {"kind": "task", "task_id": task_id})
    secondary = real_ledger.request(["resource:second"], {"kind": "task", "task_id": task_id})
    receipt(owned_log_dir, task_id, owner=owner,
            resources={"names": ["resource:first"], "reservation_id": primary.reservation_id, "state": "granted"})
    store.write(owned_log_dir, task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner,
                                           pid=agent["pid"], start_time=agent["start_time"], markers=agent["markers"]))
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")

    class ReleaseOneThenFail:
        def __init__(self):
            self.calls = 0

        def entries(self):
            return real_ledger.entries()

        def release_owned_task(self, reservation_id, owner_task_id):
            self.calls += 1
            if self.calls == 2:
                raise resource_ledger.ResourceError("interrupted during resource release")
            return real_ledger.release_owned_task(reservation_id, owner_task_id)

    flaky = ReleaseOneThenFail()
    monkeypatch.setattr(task_resources, "ledger_for", lambda _log_dir: flaky)
    with pytest.raises(resource_ledger.ResourceError, match="interrupted"):
        task_recovery.recover(owned_log_dir, task_id)
    pending = task_supervision.read_launch(owned_log_dir, task_id)
    remaining = {entry.reservation_id for entry in real_ledger.entries()}
    assert pending["state"] == "settled" and pending["reconciliation_pending"] is True
    assert len(remaining) == 1

    monkeypatch.setattr(task_resources, "ledger_for", lambda _log_dir: real_ledger)
    result = task_recovery.recover_abandoned(owned_log_dir)
    assert result["recovered"]
    assert real_ledger.entries() == []
    assert task_supervision.read_launch(owned_log_dir, task_id)["reconciliation_pending"] is False


def test_recovery_refuses_receipt_resource_id_owned_by_another_task(owned_log_dir, monkeypatch):
    from harflow_mcp import resources as resource_ledger
    from harflow_mcp import task_resources

    task_id = "4" * 32
    other_id = "5" * 32
    owner = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}
    ledger = task_resources.ledger_for(owned_log_dir)
    other = ledger.request(["resource:other"], {"kind": "task", "task_id": other_id})
    receipt(owned_log_dir, task_id, owner=owner,
            resources={"names": ["resource:other"], "reservation_id": other.reservation_id, "state": "granted"})
    store.write(owned_log_dir, task_record(task_id, session_id=f"session-{task_id[:4]}", owner=owner))
    monkeypatch.setattr(identity, "identity_check", lambda _saved: "dead")

    with pytest.raises(resource_ledger.ResourceError):
        task_recovery.recover(owned_log_dir, task_id)

    assert {entry.reservation_id for entry in ledger.entries()} == {other.reservation_id}
    assert task_supervision.read_launch(owned_log_dir, task_id)["reconciliation_pending"] is True


def test_reconciliation_pending_receipt_protects_retention_worktree_and_scratch(owned_log_dir):
    from datetime import datetime, timezone
    from harflow_mcp import retention, shared_scratch, worktrees

    task_id = "a" * 32
    chain_id = "b" * 32
    repo_path = owned_log_dir.parent / "repo"
    repo_path.mkdir()
    scratch_path = shared_scratch.root(owned_log_dir) / "chain" / shared_scratch.key_for("chain", chain_id)
    scratch_path.mkdir(parents=True)
    old = "2020-01-01T00:00:00+00:00"
    store.write(owned_log_dir, store.TaskRecord(
        task_id=task_id, backend="claude", session_id=f"session-{task_id[:4]}", repo_path=str(repo_path),
        started_at=old, status="failed", exit_code=1, finished_at=old, output_drained=False,
        interruption={"reason": "owner_and_agent_confirmed_dead", "source": "maintenance", "recorded_at": old},
    ))
    receipt(owned_log_dir, task_id, state="settled", session_id=f"session-{task_id[:4]}",
            repo_path=str(repo_path), scratch_chain_id=chain_id, reconciliation_pending=True,
            settled_reason="abandoned", recovery_intent={"recorded_at": old, "reason": "owner_and_agent_confirmed_dead",
                                                           "source": "maintenance", "evidence": {"generation": "a" * 32}})

    users = worktrees.users(owned_log_dir, repo_path)
    assert any(row["kind"] == "recovery_pending" and row["task_id"] == task_id for row in users)
    state = shared_scratch.State(owned_log_dir)
    assert any(row["task_id"] == task_id for row in state.blockers("chain", chain_id, scratch_path))
    stats = retention.sweep(owned_log_dir, 1, datetime.now(timezone.utc))
    assert stats["deleted_tasks"] == 0
    assert store.read(owned_log_dir, task_id) is not None
    assert (owned_log_dir / f"{task_id}.launch.json").exists()
