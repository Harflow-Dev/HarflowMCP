"""Focused phase-slot protocol tests; every state root and process is test-owned."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
from pathlib import Path

import pytest

from harflow_mcp import identity, phase_slots, resources, state_ownership, store, task_resources
from harflow_mcp import ctl, server, task_reads
from harflow_mcp.backends import get as get_backend
from harflow_mcp.state_layout import resolve_layout


def enabled_root(path: Path) -> Path:
    layout = resolve_layout(root=path)
    layout.root.mkdir(parents=True)
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
    phase_slots.enable(layout.root)
    return layout.root


def active_record(root: Path, task_id: str, **changes) -> store.TaskRecord:
    values = {"task_id": task_id, "backend": "claude", "session_id": "fixture",
        "repo_path": str(root), "started_at": "2026-10-31T00:00:00+00:00", "pid": 123456,
        "pgid": 123456, "start_time": "fixture-agent-generation", "markers": ["fixture-agent"],
        "owner": {"pid": os.getpid(), "start_time": "fixture-owner-generation", "markers": []},
        "freedom": "read_only"}
    values.update(changes)
    record = store.TaskRecord(**values)
    assert store.write_landed(root / "tasks", record), store.last_write_error()
    return record


def authorize_task(monkeypatch, record):
    monkeypatch.setattr(phase_slots, "_authority", lambda _log_dir: record)


def _wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.01)
    assert predicate(), "condition did not become true before timeout"


def test_enable_promotes_quiescent_layout_and_old_layout_cannot_read_v2(tmp_path):
    root = enabled_root(tmp_path / "state")
    marker = state_ownership._read_marker(root)
    assert marker["layout_version"] == 2
    assert resources.ResourceLedger(root)._read_only() == []

    marker["layout_version"] = 1
    resources._atomic_write(root / state_ownership.MARKER_NAME, marker)
    with pytest.raises(resources.ResourceLedgerCorrupt, match="layout 2"):
        resources.ResourceLedger(root)._read_only()


def test_enable_refuses_legacy_reservation_without_promoting(tmp_path):
    layout = resolve_layout(root=tmp_path / "state")
    layout.root.mkdir(parents=True)
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
    ledger = resources.ResourceLedger(layout.root)
    ledger.request(["sim"], resources.Owner("task", task_id="legacy"))
    with pytest.raises(phase_slots.SlotRefused, match="no legacy reservations"):
        phase_slots.enable(layout.root)
    assert state_ownership._read_marker(layout.root)["layout_version"] == 1


def test_enable_refuses_while_any_shared_root_owner_is_live(tmp_path):
    layout = resolve_layout(root=tmp_path / "state")
    layout.root.mkdir(parents=True)
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
        with pytest.raises(state_ownership.StateBusy):
            phase_slots.enable(layout.root)
    assert state_ownership._read_marker(layout.root)["layout_version"] == 1


def test_capacity_decrease_keeps_existing_holds_and_fifo_admission(tmp_path):
    root = enabled_root(tmp_path / "state")
    directory = root / "tasks"
    phase_slots.configure(directory, "sim", capacity=2)
    ledger = resources.ResourceLedger(root)
    first_record = active_record(root, "task-a")
    second_record = active_record(root, "task-b")
    first = ledger.request(["sim"], resources.Owner("task", task_id=first_record.task_id))
    second = ledger.request(["sim"], resources.Owner("task", task_id=second_record.task_id))
    assert first.held and second.held

    phase_slots.configure(directory, "sim", capacity=1)
    third_record = active_record(root, "task-c")
    queued = ledger.request(["sim"], resources.Owner("task", task_id=third_record.task_id))
    entries = {row["owner"]["task_id"]: row for row in ledger._read_only()}
    assert entries["task-b"]["state"] == "held"
    assert entries["task-c"]["state"] == "queued"
    ledger.release(first.reservation_id)
    entries = {row["owner"]["task_id"]: row for row in ledger._read_only()}
    assert entries["task-b"]["state"] == "held"
    assert entries["task-c"]["state"] == "queued"
    ledger.release(second.reservation_id)
    assert next(row for row in ledger._read_only() if row["owner"]["task_id"] == "task-c")["state"] == "held"


def test_slot_configuration_rejects_hook_capacity_above_one_and_invalid_argv(tmp_path):
    root = enabled_root(tmp_path / "state")
    with pytest.raises(ValueError, match="capacity one"):
        phase_slots.configure(root / "tasks", "device", capacity=2,
                              cleanup_argv=[sys.executable, "-c", "pass"])
    with pytest.raises(phase_slots.SlotRefused, match="absolute executable"):
        phase_slots.configure(root / "tasks", "device", cleanup_argv=["python", "-c", "pass"])


def test_hook_configuration_requires_phase_authority_and_cannot_be_added_over_legacy_holds(tmp_path):
    root = enabled_root(tmp_path / "state")
    cleanup = tmp_path / "cleanup.py"
    cleanup.write_text("pass\n")
    log_dir = root / "tasks"
    record = active_record(root, "legacy-holder")
    legacy = resources.ResourceLedger(root).request(["device"], resources.Owner("task", task_id=record.task_id))
    with pytest.raises(phase_slots.SlotRefused, match="cannot change while any reservation"):
        phase_slots.configure(log_dir, "device", cleanup_argv=[sys.executable, str(cleanup)])
    resources.ResourceLedger(root).release(legacy.reservation_id)

    phase_slots.configure(log_dir, "device", cleanup_argv=[sys.executable, str(cleanup)])
    with pytest.raises(resources.InvalidResourceRequest, match="qualified phase lease"):
        resources.ResourceLedger(root).request(["device"], resources.Owner("task", task_id=record.task_id))


def test_hooked_acquire_refuses_without_a_live_supervisor_even_with_compatible_task_authority(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    log_dir = root / "tasks"
    cleanup = tmp_path / "cleanup.py"
    cleanup.write_text("pass\n")
    phase_slots.configure(log_dir, "device", cleanup_argv=[sys.executable, str(cleanup)])
    record = SimpleNamespace(task_id="1234567890abcdef", pid=123, start_time="agent", markers=[],
        started_at="2026-10-31T00:00:00+00:00", owner={"pid": 456, "start_time": "owner", "markers": []},
        spawned_by=None, freedom="unrestricted", network=None, enforcement={"network_access": "not_controlled"})
    monkeypatch.setattr(phase_slots, "_authority", lambda _log_dir: record)
    monkeypatch.setattr(phase_slots, "_task_lineage", lambda _log_dir, _record: ["task:" + record.task_id])
    monkeypatch.setattr(phase_slots, "_live", lambda _log_dir, previous: (previous, None))
    with pytest.raises(phase_slots.SlotRefused, match="live host supervisor"):
        phase_slots.acquire(log_dir, "device", 0)
    assert phase_slots.status(log_dir, "device")["leases"] == []


def test_ledger_v2_rejects_unqualified_phase_entry_and_caps_events(tmp_path):
    root = enabled_root(tmp_path / "state")
    ledger = resources.ResourceLedger(root)
    ledger._transact(lambda tx: setattr(tx, "dirty", True))
    document = json.loads(ledger.path.read_text())
    from harflow_mcp.resources import _Transaction
    tx = _Transaction([], lambda _name: 1, lambda: "2026-10-31T00:00:00+00:00")
    tx.document = document
    event = phase_entry("a" * 32, "event-bound")
    for _ in range(300):
        tx.phase_event(event, "queued")
    assert len(tx.document["slot_events"]) == 256
    document["slot_events"] = tx.document["slot_events"]
    malformed = phase_entry("b" * 32, "bad-phase-entry")
    malformed.pop("scope_chain")
    document["entries"] = [malformed]
    resources._atomic_write(ledger.path, document)
    with pytest.raises(resources.ResourceLedgerCorrupt, match="unqualified"):
        ledger._read_only()


def test_corrupt_v2_configuration_stays_closed_when_repair_cannot_qualify_it(tmp_path):
    root = enabled_root(tmp_path / "state")
    ledger = resources.ResourceLedger(root)
    ledger._transact(lambda tx: setattr(tx, "dirty", True))
    document = json.loads(ledger.path.read_text())
    document["slot_configuration"] = {"epoch": 4, "names": {"device": {"capacity": 0}}}
    resources._atomic_write(ledger.path, document)

    with pytest.raises(resources.ResourceLedgerRepairing, match="configuration is corrupt"):
        ledger.begin_repair()
    # A failed configuration reconstruction must never materialize layout-1 defaults or
    # let a subsequent writer admit work while the evidence is still unresolved.
    with pytest.raises(resources.ResourceLedgerCorrupt):
        ledger._transact(lambda tx: setattr(tx, "dirty", True))
    with pytest.raises(resources.ResourceLedgerCorrupt):
        phase_slots.configure(root / "tasks", "device", capacity=1)
    assert json.loads(ledger.path.read_text())["slot_configuration"]["names"]["device"]["capacity"] == 0


def phase_entry(task_id: str, lease_id: str, *, deadline: str | None = None) -> dict:
    now = "2026-10-31T00:00:00+00:00"
    raw = {"reservation_id": lease_id, "names": ["sim"], "state": "queued",
        "owner": {"kind": "task", "task_id": task_id}, "generation": 1,
        "enqueued_at": now, "purpose": "phase", "phase_state": "queued",
        "execution_generation": "a" * 64, "scope_chain": ["task:" + task_id],
        "agent_identity": {"pid": 10, "start_time": "agent", "markers": []},
        "owner_identity": {"pid": 11, "start_time": "owner", "markers": []},
        "task_authority": {"freedom": "unrestricted", "network": None, "network_access": "not_controlled"},
        "cleanup_policy": None, "policy_epoch": 0}
    if deadline is not None:
        raw["deadline_at"] = deadline
    return raw


def qualified_phase_entry(root: Path, task_id: str, lease_id: str, hook: dict) -> dict:
    enforcement = get_backend("claude").enforcement("unrestricted", None).as_dict()
    record = active_record(root, task_id, freedom="unrestricted", enforcement=enforcement)
    raw = phase_entry(task_id, lease_id)
    configuration = json.loads((root / "resources" / "ledger.json").read_text())["slot_configuration"]
    raw.update(execution_generation=phase_slots._generation(record, None),
        scope_chain=phase_slots._task_lineage(root / "tasks", record),
        agent_identity=identity.task_identity(record.pid, record.start_time, record.markers),
        owner_identity=record.owner,
        task_authority={"freedom": record.freedom, "network": record.network,
                        "network_access": record.enforcement["network_access"]},
        cleanup_policy=hook, policy_epoch=configuration["epoch"])
    return raw


def seed_phase_pair(root: Path, hook: dict, first_id: str, second_id: str) -> resources.ResourceLedger:
    first = qualified_phase_entry(root, "cleanup-owner", first_id, hook)
    first.update(state="held", phase_state="held", granted_at=first["enqueued_at"], cleanup_policy=hook)
    second = qualified_phase_entry(root, "queued-owner", second_id, hook)
    second["cleanup_policy"] = hook
    ledger = resources.ResourceLedger(root)
    def enqueue(tx):
        tx.entries.extend((first, second))
        tx.phase_event(first, "granted")
        tx.phase_event(second, "queued")
        tx.dirty = True
    ledger._transact(enqueue)
    ledger._transact(lambda tx: (tx.release(first_id), tx.grant_pass()))
    return ledger


def test_phase_queue_is_fifo_and_expiry_never_becomes_a_hold(tmp_path):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    ledger = resources.ResourceLedger(root, clock=lambda: "2026-10-31T00:00:03+00:00")
    expired = phase_entry("task-expired", "lease-expired", deadline="2026-10-31T00:00:01+00:00")
    first = phase_entry("task-first", "lease-first")
    second = phase_entry("task-second", "lease-second")
    def enqueue(tx):
        tx.entries.extend([expired, first, second])
        tx.document["entries"] = tx.entries
        tx.dirty = True
        for raw in (expired, first, second):
            tx.phase_event(raw, "queued")
        tx.grant_pass()
    ledger._transact(enqueue)
    rows = {row["reservation_id"]: row for row in ledger._read_only()}
    assert "lease-expired" not in rows
    assert rows["lease-first"]["phase_state"] == "held"
    assert rows["lease-second"]["phase_state"] == "queued"
    view = phase_slots.status(root / "tasks", "sim")
    assert next(row for row in view["leases"] if row["lease_id"] == "lease-second")["position"] == 1


def test_receipt_without_ledger_entry_restores_as_uncertain_capacity_blocker(tmp_path):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    phase_slots.write_receipt(root, phase_entry("task-a", "lease-a"))
    ledger = resources.ResourceLedger(root)
    ledger._transact(lambda tx: tx.grant_pass())
    row = next(item for item in ledger._read_only() if item["reservation_id"] == "lease-a")
    assert row["state"] == "held"
    assert row["phase_state"] == "cleanup_uncertain"
    assert "human inspection" in row["diagnostic"]


def test_ledger_entry_without_its_receipt_becomes_uncertain_before_any_mutation(tmp_path):
    root = enabled_root(tmp_path / "state")
    ledger = resources.ResourceLedger(root)
    raw = phase_entry("task-a", "ledger-only")
    raw.update(state="held", phase_state="held", granted_at=raw["enqueued_at"])
    ledger._transact(lambda tx: (tx.entries.append(raw), setattr(tx, "dirty", True)))
    ledger._transact(lambda tx: tx.grant_pass())
    entry = next(row for row in ledger._read_only() if row["reservation_id"] == "ledger-only")
    assert entry["phase_state"] == "cleanup_uncertain"
    assert (root / "phase-leases" / "ledger-only.json").exists()


def test_receipt_only_cleanup_runner_survives_supervisor_restart_and_blocks_resolution(tmp_path):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim")
    ledger = resources.ResourceLedger(root)
    runner_process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    runner = identity.capture(runner_process.pid, [])
    assert runner is not None
    raw = phase_entry("runner-task", "runner-receipt-only")
    raw.update(state="held", phase_state="cleanup_running", granted_at=raw["enqueued_at"],
        cleanup_claim="c" * 32, cleanup_supervisor="d" * 32, cleanup_started_at=raw["enqueued_at"],
        cleanup_runner_tracking="spawn_pending")
    ledger._transact(lambda tx: (tx.entries.append(raw), tx.phase_event(raw, "cleanup_claimed"), setattr(tx, "dirty", True)))
    receipt = dict(raw, cleanup_runner=runner, cleanup_runner_tracking="recorded")
    phase_slots.write_receipt(root, receipt)
    supervisor = phase_slots.service_start(root / "tasks")
    assert supervisor["state"] == "alive"
    try:
        stored = next(row for row in resources.ResourceLedger(root)._read_only() if row["reservation_id"] == "runner-receipt-only")
        assert stored["cleanup_runner"] == runner, "recovery must union runner progress from both qualified sources"
        assert stored["phase_state"] == "cleanup_uncertain"
        assert identity.identity_check(runner) == "alive"
        with pytest.raises(phase_slots.SlotRefused, match="runner remains live or uncertain"):
            phase_slots.resolve(root / "tasks", "runner-receipt-only")

        assert identity.check_detail(runner)[0] == "alive"
        runner_process.terminate()
        runner_process.wait(timeout=5)
        assert phase_slots.resolve(root / "tasks", "runner-receipt-only")["state"] == "resolved"
    finally:
        if runner_process.poll() is None:
            runner_process.kill()
            runner_process.wait(timeout=5)
        if any(row["reservation_id"] == "runner-receipt-only" for row in resources.ResourceLedger(root)._read_only()):
            def clear_runner_claim(tx):
                raw = tx.find("runner-receipt-only")
                raw["phase_state"] = "resolved"
                tx.phase_event(raw, "human_resolved")
                tx.entries.remove(raw)
                tx.dirty = True
            ledger._transact(clear_runner_claim)
        phase_slots.service_stop(root / "tasks")
        deadline = time.monotonic() + 5
        while phase_slots.service_status(root)["state"] != "missing" and time.monotonic() < deadline:
            time.sleep(.02)
        assert phase_slots.service_status(root)["state"] == "missing"


def test_missing_task_record_marks_phase_holder_uncertain_instead_of_leaving_it_held(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim")
    record = active_record(root, "missing-record-holder")
    authorize_task(monkeypatch, record)
    view = phase_slots.acquire(root / "tasks", "sim", 0)
    monkeypatch.setattr(store, "read", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(identity, "check_detail", lambda _identity: ("undecidable", "fixture"))

    phase_slots.reconcile(root / "tasks")

    row = next(item for item in resources.ResourceLedger(root)._read_only() if item["reservation_id"] == view["lease_id"])
    assert row["state"] == "held"
    assert row["phase_state"] == "cleanup_uncertain"
    assert "task" in row["diagnostic"].lower() or "generation" in row["diagnostic"].lower()


def test_shape_valid_but_wrong_agent_generation_never_proves_holder_death(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim")
    record = active_record(root, "identity-mismatch-holder")
    authorize_task(monkeypatch, record)
    view = phase_slots.acquire(root / "tasks", "sim", 0)
    wrong = {"pid": record.pid + 1, "start_time": "different-generation", "markers": ["unrelated-process"]}
    ledger = resources.ResourceLedger(root)
    def replace_identity(tx):
        raw = tx.find(view["lease_id"])
        raw["agent_identity"] = wrong
        tx.touch(raw)
        phase_slots.write_receipt(root, raw)
    ledger._transact(replace_identity)
    monkeypatch.setattr(identity, "check_detail", lambda _value: pytest.fail("must not probe a substituted PID generation"))

    phase_slots.reconcile(root / "tasks")

    stored = next(row for row in resources.ResourceLedger(root)._read_only() if row["reservation_id"] == view["lease_id"])
    assert stored["state"] == "held" and stored["phase_state"] == "cleanup_uncertain"
    assert "identity does not match" in stored["diagnostic"]


def test_release_missing_task_metadata_is_a_typed_refusal(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim")
    record = active_record(root, "missing-release-record")
    authorize_task(monkeypatch, record)
    monkeypatch.setattr(store, "read", lambda *_args, **_kwargs: None)
    with pytest.raises(phase_slots.SlotRefused, match="task metadata is missing"):
        phase_slots.release(root / "tasks", "f" * 32)


def test_expired_queue_attempt_is_stable_until_explicit_release_acknowledgment(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    holder = active_record(root, "expiry-holder")
    authorize_task(monkeypatch, holder)
    held = phase_slots.acquire(root / "tasks", "sim", 0)
    queued_record = active_record(root, "expiry-queued")
    authorize_task(monkeypatch, queued_record)
    queued = phase_slots.acquire(root / "tasks", "sim", 60)
    assert queued["state"] == "queued"

    ledger = resources.ResourceLedger(root)
    def expire(tx):
        entry = tx.find(queued["lease_id"])
        entry["deadline_at"] = "2020-01-01T00:00:00+00:00"
        tx.touch(entry)
        phase_slots.write_receipt(root, entry)
    ledger._transact(expire)
    expired = phase_slots.acquire(root / "tasks", "sim", 60)
    repeated = phase_slots.acquire(root / "tasks", "sim", 60)
    assert expired["lease_id"] == repeated["lease_id"] == queued["lease_id"]
    assert expired["state"] == repeated["state"] == "expired"
    assert expired["deadline_at"] == repeated["deadline_at"] == "2020-01-01T00:00:00+00:00"

    acknowledgment = phase_slots.release(root / "tasks", queued["lease_id"])
    assert acknowledgment["state"] == "expired" and acknowledgment["acknowledged"] is True
    fresh = phase_slots.acquire(root / "tasks", "sim", 60)
    assert fresh["lease_id"] != queued["lease_id"]
    assert fresh["state"] == "queued"
    authorize_task(monkeypatch, holder)
    phase_slots.release(root / "tasks", held["lease_id"])


def test_receipt_admission_limit_rejects_new_attempt_but_existing_holder_can_settle(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    record = active_record(root, "bounded-receipt-holder")
    authorize_task(monkeypatch, record)
    lease = phase_slots.acquire(root / "tasks", "sim", 0)
    for index in range(phase_slots.MAX_ENTRIES - 1):
        raw = phase_entry("unacknowledged-expiry", f"expired-{index:04d}", deadline="2020-01-01T00:00:00+00:00")
        raw["phase_state"] = "expired"
        phase_slots.write_receipt(root, raw)
    receipt_dir = root / "phase-leases"
    assert len(list(receipt_dir.glob("*.json"))) == phase_slots.MAX_ENTRIES

    next_record = active_record(root, "receipt-limit-next")
    authorize_task(monkeypatch, next_record)
    with pytest.raises(phase_slots.SlotRefused, match="receipt capacity|resource ledger is full"):
        phase_slots.acquire(root / "tasks", "sim", 0)
    assert len(list(receipt_dir.glob("*.json"))) == phase_slots.MAX_ENTRIES

    authorize_task(monkeypatch, record)
    assert phase_slots.release(root / "tasks", lease["lease_id"])["state"] == "released"
    assert len(list(receipt_dir.glob("*.json"))) == phase_slots.MAX_ENTRIES


def test_settled_final_receipts_are_pruned_across_lifetime_cycles(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    monkeypatch.setattr(phase_slots, "MAX_ENTRIES", 3)

    for index in range(8):
        record = active_record(root, f"lifetime-{index}")
        authorize_task(monkeypatch, record)
        lease = phase_slots.acquire(root / "tasks", "sim", 0)
        assert lease["state"] == "held"
        assert phase_slots.release(root / "tasks", lease["lease_id"])["state"] == "released"
        assert len(list((root / "phase-leases").glob("*.json"))) <= 3

    assert len(list((root / "phase-leases").glob("*.json"))) < 3


@pytest.mark.parametrize("failure", ["capture_none", "claim_cas"])
def test_cleanup_spawn_publication_failure_stops_only_owned_child_and_retains_uncertainty(tmp_path, monkeypatch, failure):
    root = enabled_root(tmp_path / "state")
    cleanup = tmp_path / "cleanup.py"
    started = tmp_path / "cleanup-started"
    cleanup.write_text("import pathlib, sys, time\npathlib.Path(sys.argv[1]).touch()\ntime.sleep(30)\n")
    log_dir = root / "tasks"
    phase_slots.configure(log_dir, "sim", cleanup_argv=[sys.executable, str(cleanup), str(started)], cleanup_timeout=60)
    hook = json.loads((root / "resources" / "ledger.json").read_text())["slot_configuration"]["names"]["sim"]["cleanup"]
    ledger = seed_phase_pair(root, hook, "spawn-failure", "spawn-successor")
    processes = []
    original_popen = phase_slots.subprocess.Popen
    def record_popen(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        argv = args[0] if args else kwargs.get("args", [])
        if isinstance(argv, (list, tuple)) and str(cleanup) in argv:
            processes.append(process)
        return process
    monkeypatch.setattr(phase_slots.subprocess, "Popen", record_popen)
    original_capture = identity.capture
    def fail_runner_capture(pid, markers):
        if pid == os.getpid():
            return original_capture(pid, markers)
        deadline = time.monotonic() + 5
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        assert started.exists(), "cleanup child did not enter its fixture before identity failure"
        if failure == "capture_none":
            return None
        def invalidate_claim(tx):
            entry = tx.find("spawn-failure")
            entry["generation"] += 1
            tx.touch(entry)
        resources.ResourceLedger(root)._transact(invalidate_claim)
        return original_capture(pid, markers)
    monkeypatch.setattr(identity, "capture", fail_runner_capture)

    phase_slots._cleanup(root, "spawn-failure", "e" * 32)

    assert started.exists() and len(processes) == 1
    assert processes[0].poll() is not None, "failed publication must stop and reap only its own Popen child"
    first = next(row for row in ledger._read_only() if row["reservation_id"] == "spawn-failure")
    successor = next(row for row in ledger._read_only() if row["reservation_id"] == "spawn-successor")
    assert first["phase_state"] == "cleanup_uncertain"
    assert first["cleanup_runner_tracking"] == "stopped_untracked"
    assert successor["phase_state"] == "queued", "spawn uncertainty must keep capacity blocked"
    if failure == "capture_none":
        with pytest.raises(phase_slots.SlotRefused, match="untracked runner"):
            phase_slots.resolve(log_dir, "spawn-failure")
        resolved = phase_slots.resolve(log_dir, "spawn-failure", confirm_untracked_cleanup_stopped=True)
    else:
        # The runner identity was captured, then the exact owned Popen child was
        # stopped and reaped after the generation CAS failed.
        resolved = phase_slots.resolve(log_dir, "spawn-failure")
    assert resolved["state"] == "resolved"


def test_host_supervisor_survives_transient_receipt_failure_and_task_lifecycle_contention(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    log_dir = root / "tasks"
    record = active_record(root, "contention-task")
    authorize_task(monkeypatch, record)
    supervisor = phase_slots.service_start(log_dir)
    assert supervisor["state"] == "alive"
    malformed = root / "phase-leases" / ("f" * 32 + ".json")
    malformed.parent.mkdir(parents=True, exist_ok=True)
    malformed.write_text("{", encoding="utf-8")
    try:
        time.sleep(.7)
        assert phase_slots.service_status(root)["state"] == "alive", "transient receipt replacement must defer maintenance, not kill the supervisor"
    finally:
        malformed.unlink(missing_ok=True)

    locked, unlock = threading.Event(), threading.Event()
    def hold_task_lifecycle():
        with phase_slots.lifecycle_lock(log_dir, record.task_id):
            locked.set()
            assert unlock.wait(5)
    thread = threading.Thread(target=hold_task_lifecycle, daemon=True)
    thread.start()
    try:
        assert locked.wait(5)
        started = time.monotonic()
        with pytest.raises(phase_slots.SlotLifecycleBusy):
            phase_slots.acquire(log_dir, "sim", 0)
        assert time.monotonic() - started < 4
        assert phase_slots.status(log_dir)["service"]["state"] == "alive"
    finally:
        unlock.set()
        thread.join(timeout=5)
        phase_slots.service_stop(log_dir)
        deadline = time.monotonic() + 5
        while phase_slots.service_status(root)["state"] != "missing" and time.monotonic() < deadline:
            time.sleep(.02)
        assert phase_slots.service_status(root)["state"] == "missing"


def test_missing_configured_ledger_with_live_receipt_refuses_default_reinitialization(tmp_path):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=4)
    task = active_record(root, "legacy-configured-holder",
        resources={"reservation_id": "legacy-held", "names": ["sim"], "state": "held"})
    ledger = resources.ResourceLedger(root)
    assert ledger.request(["sim"], resources.Owner("task", task_id=task.task_id),
                          reservation_id="legacy-held").held
    phase_slots.write_receipt(root, phase_entry("task-a", "lease-a"))
    assert ledger.snapshot()[0]["capacity"] == 4
    ledger.path.unlink()

    # A configured live lease is evidence that the lost ledger cannot be replaced with
    # the enable-time empty defaults. Human repair must restore policy and holders first.
    with pytest.raises((resources.ResourceLedgerCorrupt, resources.ResourceLedgerRepairing)):
        ledger._transact(lambda tx: tx.grant_pass())
    assert not ledger.path.exists()
    assert (root / "phase-leases" / "lease-a.json").exists()


def test_acquire_refuses_published_terminal_task_and_pending_cancellation(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    terminal = active_record(root, "terminal-task", status="completed", exit_code=0,
        output_drained=True, finished_at="2026-10-31T00:00:01+00:00")
    authorize_task(monkeypatch, terminal)
    with pytest.raises(phase_slots.SlotRefused, match="settled|no longer owns"):
        phase_slots.acquire(root / "tasks", "sim", 0)
    assert resources.ResourceLedger(root)._read_only() == []


def test_acquisition_lifecycle_lock_orders_grant_before_terminal_publication(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    record = active_record(root, "race-task")
    authorize_task(monkeypatch, record)
    entered_live, allow_live = threading.Event(), threading.Event()
    real_live = phase_slots._live
    def paused_live(log_dir, previous):
        result = real_live(log_dir, previous)
        entered_live.set()
        assert allow_live.wait(5), "test did not release the admission barrier"
        return result
    monkeypatch.setattr(phase_slots, "_live", paused_live)
    terminal = replace(record, status="completed", exit_code=0, output_drained=True,
                       finished_at="2026-10-31T00:00:01+00:00")
    with ThreadPoolExecutor(max_workers=2) as pool:
        acquired = pool.submit(phase_slots.acquire, root / "tasks", "sim", 0)
        assert entered_live.wait(5)
        published = pool.submit(store.write_landed, root / "tasks", terminal)
        time.sleep(.05)
        assert not published.done(), "terminal metadata must wait for the phase lifecycle lock"
        allow_live.set()
        lease = acquired.result(timeout=5)
        assert published.result(timeout=5)
    assert lease["state"] == "held"
    assert store.read(root / "tasks", record.task_id, include_prompt=False).status == "completed"
    monkeypatch.setattr(phase_slots, "_live", real_live)
    phase_slots.reconcile(root / "tasks")
    assert phase_slots.status(root / "tasks", "sim")["leases"] == []


def test_admission_refuses_a_task_execution_generation_replaced_before_grant(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    original = active_record(root, "reused-task")
    authorize_task(monkeypatch, original)
    replacement = replace(original, freedom="write_in_repo")
    assert store.write_landed(root / "tasks", replacement)
    with pytest.raises(phase_slots.SlotRefused, match="generation changed"):
        phase_slots.acquire(root / "tasks", "sim", 0)
    assert resources.ResourceLedger(root)._read_only() == []


def test_parent_phase_and_child_legacy_admission_race_never_commit_both(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    parent = active_record(root, "parent-task")
    child = active_record(root, "child-task", spawned_by=parent.task_id)
    start = threading.Barrier(2)
    monkeypatch.setattr(phase_slots, "_authority", lambda _log_dir: parent
        if threading.current_thread().name == "phase-parent" else parent)
    def acquire_phase():
        start.wait(timeout=5)
        return phase_slots.acquire(root / "tasks", "sim", 0)
    def acquire_legacy():
        start.wait(timeout=5)
        return resources.ResourceLedger(root).request(["sim"], resources.Owner("task", task_id=child.task_id))
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="phase-parent") as pool:
        phase_future = pool.submit(acquire_phase)
        legacy_future = pool.submit(acquire_legacy)
        outcomes = []
        for future in (phase_future, legacy_future):
            try:
                outcomes.append(future.result(timeout=10))
            except (phase_slots.SlotRefused, resources.InvalidResourceRequest) as exc:
                outcomes.append(exc)
    winners = [value for value in outcomes if not isinstance(value, Exception)]
    refusals = [value for value in outcomes if isinstance(value, Exception)]
    assert len(winners) == 1 and len(refusals) == 1
    entries = resources.ResourceLedger(root)._read_only()
    assert len(entries) == 1 and entries[0]["state"] == "held"
    if entries[0].get("purpose") == "phase":
        authorize_task(monkeypatch, parent)
        phase_slots.release(root / "tasks", entries[0]["reservation_id"])
    else:
        resources.ResourceLedger(root).release(entries[0]["reservation_id"])

    cancelled = active_record(root, "cancelled-task")
    authorize_task(monkeypatch, cancelled)
    task_resources.request_cancel(root / "tasks", cancelled.task_id)
    with pytest.raises(phase_slots.SlotRefused, match="cancelling"):
        phase_slots.acquire(root / "tasks", "sim", 0)
    assert resources.ResourceLedger(root)._read_only() == []


def test_queued_phase_release_cancels_but_held_phase_deadline_never_revokes(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    first = active_record(root, "first-task")
    authorize_task(monkeypatch, first)
    first_view = phase_slots.acquire(root / "tasks", "sim", 0)
    assert first_view["state"] == "held"

    second = active_record(root, "second-task")
    authorize_task(monkeypatch, second)
    queued = phase_slots.acquire(root / "tasks", "sim", 30)
    assert queued["state"] == "queued"
    assert phase_slots.release(root / "tasks", queued["lease_id"])["state"] == "released"
    assert all(row["lease_id"] != queued["lease_id"] for row in phase_slots.status(root / "tasks")["leases"])

    ledger = resources.ResourceLedger(root, clock=lambda: "2030-01-01T00:00:00+00:00")
    def age_held(tx):
        raw = tx.find(first_view["lease_id"])
        raw["deadline_at"] = "2020-01-01T00:00:00+00:00"
        phase_slots.write_receipt(root, raw)
        tx.grant_pass()
        tx.dirty = True
    ledger._transact(age_held)
    retained = next(row for row in ledger._read_only() if row["reservation_id"] == first_view["lease_id"])
    assert retained["state"] == "held" and retained["phase_state"] == "held"


@pytest.mark.parametrize("agent_verdict", [("undecidable", "ps_failed"), ("dead", "identity_mismatch")])
def test_uncertain_or_reused_holder_pid_keeps_capacity(tmp_path, monkeypatch, agent_verdict):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    record = active_record(root, "uncertain-task")
    authorize_task(monkeypatch, record)
    view = phase_slots.acquire(root / "tasks", "sim", 0)
    monkeypatch.setattr(identity, "check_detail", lambda ident: agent_verdict if ident["pid"] == record.pid else ("dead", "pid_absent"))
    phase_slots.reconcile(root / "tasks")
    retained = next(row for row in resources.ResourceLedger(root)._read_only() if row["reservation_id"] == view["lease_id"])
    assert retained["phase_state"] == "held"


def test_confirmed_holder_and_owner_death_releases_only_after_both_are_dead(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    record = active_record(root, "dead-task")
    authorize_task(monkeypatch, record)
    view = phase_slots.acquire(root / "tasks", "sim", 0)
    monkeypatch.setattr(identity, "check_detail", lambda _ident: ("dead", "pid_absent"))
    phase_slots.reconcile(root / "tasks")
    assert all(row["lease_id"] != view["lease_id"] for row in phase_slots.status(root / "tasks")["leases"])


def test_cancellation_request_alone_never_releases_a_held_phase_lease(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    record = active_record(root, "cancel-after-grant")
    authorize_task(monkeypatch, record)
    view = phase_slots.acquire(root / "tasks", "sim", 0)
    task_resources.request_cancel(root / "tasks", record.task_id)
    monkeypatch.setattr(identity, "check_detail", lambda _identity: ("alive", "matched"))
    phase_slots.reconcile(root / "tasks")
    row = next(row for row in phase_slots.status(root / "tasks", "sim")["leases"] if row["lease_id"] == view["lease_id"])
    assert row["state"] == "held"


def test_task_and_ancestor_descendant_phase_conflicts_are_checked_both_directions():
    tx = type("Tx", (), {"entries": [
        {"purpose": "phase", "scope_chain": ["task:child", "task:parent"], "names": ["sim"], "state": "queued"},
    ]})()
    with pytest.raises(phase_slots.SlotRefused, match="cannot stack"):
        phase_slots.check_conflicts(tx, {"kind": "task", "task_id": "parent"}, ["task:parent"], phase=True)
    tx.entries = [{"purpose": "phase", "scope_chain": ["task:parent"], "names": ["sim"], "state": "held"}]
    with pytest.raises(phase_slots.SlotRefused, match="cannot stack"):
        phase_slots.check_conflicts(tx, {"kind": "task", "task_id": "child"}, ["task:child", "task:parent"], phase=True)
    tx.entries = [{"purpose": "legacy", "scope_chain": ["task:parent"], "names": ["sim"], "state": "held"}]
    with pytest.raises(phase_slots.SlotRefused, match="cannot stack"):
        phase_slots.check_conflicts(tx, {"kind": "task", "task_id": "child"}, ["task:child", "task:parent"], phase=True)


def test_phase_authority_refuses_absent_and_undecidable_task_callers(tmp_path, monkeypatch):
    from harflow_mcp import lineage
    root = enabled_root(tmp_path / "state")
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *_args, **_kwargs: lineage.Detection(None))
    with pytest.raises(phase_slots.SlotRefused, match="no task caller"):
        phase_slots._authority(root / "tasks")
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *_args, **_kwargs: lineage.Detection(None, "ps_failed"))
    with pytest.raises(phase_slots.SlotRefused, match="ps_failed"):
        phase_slots._authority(root / "tasks")


def test_task_ancestry_cycle_and_missing_parent_evidence_fail_closed(tmp_path):
    root = enabled_root(tmp_path / "state")
    first = active_record(root, "1" * 32, spawned_by="2" * 32)
    active_record(root, "2" * 32, spawned_by=first.task_id)
    with pytest.raises(phase_slots.SlotRefused, match="cyclic"):
        phase_slots.owner_lineage(root, resources.Owner("task", task_id=first.task_id))
    with pytest.raises(phase_slots.SlotRefused, match="evidence is missing"):
        phase_slots.owner_lineage(root, resources.Owner("task", task_id="3" * 32))


def test_empty_workflow_scope_is_qualified_but_missing_workflow_scope_refuses(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    class WorkflowStoreStub:
        def __init__(self, _root): pass
        def get_run(self, run_id):
            return {"workflow_run_id": run_id, "parent_link": None, "caller_record": None}
    monkeypatch.setattr("harflow_mcp.workflows.WorkflowStore", WorkflowStoreStub)
    scope = phase_slots.owner_lineage(root, resources.Owner("workflow_run", workflow_run_id="run-empty"))
    assert scope == ["workflow:run-empty"]
    assert phase_slots.check_conflicts(type("Tx", (), {"entries": []})(),
        {"kind": "workflow_run", "workflow_run_id": "run-empty"}, scope, phase=True) is None

    class MissingWorkflowStore(WorkflowStoreStub):
        def get_run(self, _run_id): return None
    monkeypatch.setattr("harflow_mcp.workflows.WorkflowStore", MissingWorkflowStore)
    with pytest.raises(phase_slots.SlotRefused, match="workflow"):
        phase_slots.owner_lineage(root, resources.Owner("workflow_run", workflow_run_id="missing-run"))


def test_workflow_owner_lineage_includes_caller_task_and_attempt_identity(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    caller = active_record(root, "d" * 32)
    class WorkflowStoreStub:
        def __init__(self, _root): pass
        def get_run(self, run_id):
            return {"workflow_run_id": run_id, "parent_link": None,
                    "caller_record": {"task_id": caller.task_id, "started_at": caller.started_at}}
    monkeypatch.setattr("harflow_mcp.workflows.WorkflowStore", WorkflowStoreStub)
    chain = phase_slots.owner_lineage(root, resources.Owner("workflow_run", workflow_run_id="workflow-1"))
    assert "workflow:workflow-1" in chain and "task:" + caller.task_id in chain
    attempt = phase_slots.owner_lineage(root, resources.Owner("workflow_attempt", workflow_run_id="workflow-1", activation_id="attempt-2"))
    assert "attempt:workflow-1:attempt-2" in attempt
    tx = type("Tx", (), {"entries": [{"purpose": "phase", "scope_chain": ["task:" + caller.task_id], "state": "queued"}]})()
    with pytest.raises(phase_slots.SlotRefused, match="cannot stack"):
        phase_slots.check_conflicts(tx, {"kind": "workflow_run", "workflow_run_id": "workflow-1"}, chain, phase=True)


def test_slot_status_cli_and_mcp_return_the_same_read_only_projection(tmp_path, monkeypatch, capsys):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=2)
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))
    assert ctl.main(["slot", "status", "sim", "--json"]) == 0
    cli = json.loads(capsys.readouterr().out)["result"]
    monkeypatch.setattr(server, "_reg", lambda: SimpleNamespace(log_dir=root / "tasks"))
    mcp = asyncio.run(server.slot_status("sim"))
    assert cli["configuration"] == mcp["configuration"]
    assert cli["leases"] == mcp["leases"] == []
    assert cli["events"] == mcp["events"]


def test_disabled_layout_refusal_has_distinct_code_at_ctl_and_mcp_boundaries(tmp_path, monkeypatch, capsys):
    root = enabled_root(tmp_path / "state")
    marker = state_ownership._read_marker(root)
    marker["layout_version"] = 1
    resources._atomic_write(root / state_ownership.MARKER_NAME, marker)
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))

    assert ctl.main(["slot", "status", "sim", "--json"]) == 1
    status_error = json.loads(capsys.readouterr().out)["error"]
    assert status_error["code"] == "phase_slots_not_enabled"
    assert "phase slots are disabled" in status_error["message"]

    assert ctl.main(["slot", "acquire", "sim", "--wait-seconds", "0", "--json"]) == 1
    acquire_error = json.loads(capsys.readouterr().out)["error"]
    assert acquire_error["code"] == "phase_slots_not_enabled"

    monkeypatch.setattr(server, "_reg", lambda: SimpleNamespace(log_dir=root / "tasks"))
    with pytest.raises(server.MCPError) as mcp_status:
        asyncio.run(server.slot_status("sim"))
    assert mcp_status.value.data == {"code": "phase_slots_not_enabled"}

    with pytest.raises(server.MCPError) as mcp_acquire:
        asyncio.run(server.acquire_slot("sim", wait_seconds=0))
    assert mcp_acquire.value.data == {"code": "phase_slots_not_enabled"}


def test_unrelated_phase_slot_refusals_keep_generic_code_at_public_boundaries(tmp_path, monkeypatch, capsys):
    root = enabled_root(tmp_path / "state")
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))

    def refuse(*_args, **_kwargs):
        raise phase_slots.SlotRefused("fixture non-layout refusal")
    monkeypatch.setattr(phase_slots, "status", refuse)
    assert ctl.main(["slot", "status", "sim", "--json"]) == 1
    ctl_error = json.loads(capsys.readouterr().out)["error"]
    assert ctl_error["code"] == "slot_refused"
    monkeypatch.undo()

    from harflow_mcp import lineage
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *_args, **_kwargs: lineage.Detection(None))
    monkeypatch.setattr(server, "_reg", lambda: SimpleNamespace(log_dir=root / "tasks"))
    with pytest.raises(server.MCPError) as mcp_error:
        asyncio.run(server.acquire_slot("sim", wait_seconds=0))
    assert mcp_error.value.data == {"code": "slot_refused"}


def test_slot_cli_keeps_resource_and_state_ownership_errors_structured(tmp_path, monkeypatch, capsys):
    root = enabled_root(tmp_path / "state")
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))
    def refuse(*_args, **_kwargs):
        raise phase_slots.SlotRefused("fixture resource refusal")
    monkeypatch.setattr(phase_slots, "status", refuse)
    assert ctl.main(["slot", "status", "sim", "--json"]) == 1
    resource_error = json.loads(capsys.readouterr().out)["error"]
    assert resource_error["code"] == "slot_refused"
    assert resource_error["message"] == "fixture resource refusal"

    from harflow_mcp.state_ownership import StateBusy
    def busy(*_args, **_kwargs):
        raise StateBusy("fixture state lock", reason="lock_busy", data={"root": "fixture"})
    monkeypatch.setattr(phase_slots, "status", busy)
    assert ctl.main(["slot", "status", "sim", "--json"]) == 1
    state_error = json.loads(capsys.readouterr().out)["error"]
    assert state_error == {"code": "state_busy", "message": "fixture state lock",
                           "data": {"root": "fixture", "reason": "lock_busy"}}


def test_task_projection_retention_and_generic_reaper_do_not_drop_a_phase_holder(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    record = active_record(root, "retained-task")
    authorize_task(monkeypatch, record)
    view = phase_slots.acquire(root / "tasks", "sim", 0)
    assert phase_slots.retained(root / "tasks", record.task_id)
    assert phase_slots.task_view(root / "tasks", record.task_id)[0]["lease_id"] == view["lease_id"]
    resources.ResourceLedger(root).reap({"task": lambda _entry: "settled"})
    assert any(row["lease_id"] == view["lease_id"] for row in phase_slots.status(root / "tasks")["leases"])
    assert phase_slots.release(root / "tasks", view["lease_id"])["state"] == "released"
    assert not phase_slots.retained(root / "tasks", record.task_id)


def test_task_list_phase_projection_and_waiting_filter_include_queued_slots(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    queued = [{"lease_id": "queued-id", "name": "sim", "state": "queued"}]
    monkeypatch.setattr(store, "list_page", lambda *_args, **_kwargs: {"items": [{"task_id": "queued-task", "status": "running"}], "has_more": False})
    monkeypatch.setattr(phase_slots, "task_view", lambda _dir, _task_id: queued)
    page = task_reads.list_tasks(root / "tasks", fields=["task_id", "status", "phase_slots"], limit=5)
    assert page["items"] == [{"task_id": "queued-task", "status": "running", "phase_slots": queued}]

    entry = SimpleNamespace(task_id="queued-task", brief=lambda: {"task_id": "queued-task", "status": "running", "started_at": "2026-10-31", "phase_slots": queued})
    held_entry = SimpleNamespace(task_id="held-task", brief=lambda: {"task_id": "held-task", "status": "running", "started_at": "2026-10-30", "phase_slots": [{"lease_id": "held-id", "name": "sim", "state": "held"}]})
    registry = SimpleNamespace(log_dir=root / "tasks", list=lambda: [entry, held_entry], recovered_briefs=lambda exclude: [])
    monkeypatch.setattr(server, "_reg", lambda: registry)
    monkeypatch.setattr(task_resources, "pending_views", lambda _dir: [])
    async def passthrough(entries): return entries
    monkeypatch.setattr(server, "_filter_task_reads", passthrough)
    result = asyncio.run(server.list_tasks(waiting_for_resource="sim"))
    assert [item["task_id"] for item in result] == ["queued-task"]


def test_wait_for_task_preserves_phase_slot_status_projection(tmp_path, monkeypatch):
    task_id = "9" * 32
    slots = [{"lease_id": "lease", "name": "sim", "state": "held"}]
    task = SimpleNamespace(task_id=task_id, caller_approvals=False, done=asyncio.Event(), finished=True,
        snapshot=lambda: {"task_id": task_id, "status": "completed", "phase_slots": slots})
    task.done.set()
    registry = SimpleNamespace(log_dir=tmp_path, get=lambda _task_id: task)
    monkeypatch.setattr(server, "_reg", lambda: registry)
    async def guarded(_task_id):
        from harflow_mcp.id_resolution import Resolution
        return Resolution(task_id, None)
    monkeypatch.setattr(server, "_guard_task_read", guarded)
    monkeypatch.setattr(server, "_status_payload", lambda snapshot, **_kwargs: snapshot)
    result = asyncio.run(server.wait_for_task(task_id, timeout_seconds=0, detail="full"))
    assert result["phase_slots"] == slots


def test_queue_diagnostics_show_only_actual_capacity_and_fifo_blockers(tmp_path, monkeypatch):
    """A queued task sees holders and earlier FIFO requests, never later peers."""
    root = enabled_root(tmp_path / "state")
    directory = root / "tasks"
    phase_slots.configure(directory, "sim", capacity=1)
    records = {name: active_record(root, f"task-{name}") for name in ("holder", "first", "second")}
    local = threading.local()
    local.name = "holder"
    monkeypatch.setattr(phase_slots, "_authority", lambda _directory: records[local.name])
    holder = phase_slots.acquire(directory, "sim", 0)
    assert holder["state"] == "held"

    futures = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        for name in ("first", "second"):
            started = threading.Event()
            def request(name=name, started=started):
                local.name = name
                started.set()
                return phase_slots.acquire(directory, "sim", 10)
            futures.append((name, started, pool.submit(request)))
            assert started.wait(2)
            _wait_until(lambda: any(row["task_id"] == f"task-{name}" and row["state"] == "queued"
                                    for row in phase_slots.status(directory, "sim")["leases"]))

        rows = {row["task_id"]: row for row in phase_slots.status(directory, "sim")["leases"]}
        first = rows["task-first"]
        second = rows["task-second"]
        assert first["blocked_by"] == [holder["lease_id"]]
        assert first["blockers"] == [{"reservation_id": holder["lease_id"], "purpose": "phase",
            "owner": {"kind": "task", "task_id": "task-holder"}, "state": "held",
            "since": holder["granted_at"], "enqueued_at": holder["enqueued_at"],
            "granted_at": holder["granted_at"]}]
        assert second["blocked_by"] == [holder["lease_id"], first["lease_id"]]
        assert [item["reservation_id"] for item in second["blockers"]] == second["blocked_by"]
        assert all(item["owner"]["task_id"] in {"task-holder", "task-first"}
                   and item["since"] for item in second["blockers"])
        projected = phase_slots.task_view(directory, "task-second")[0]
        assert projected["blockers"] == second["blockers"]
        assert projected["blocked_by"] == second["blocked_by"]

        local.name = "holder"
        phase_slots.release(directory, holder["lease_id"])
        _wait_until(lambda: phase_slots.status(directory, "sim")["leases"] and
                    next(row for row in phase_slots.status(directory, "sim")["leases"]
                         if row["task_id"] == "task-first")["state"] == "held")
        assert phase_slots.status(directory, "sim")["leases"]
        local.name = "first"
        phase_slots.release(directory, first["lease_id"])
        for _, _, future in futures:
            try:
                future.result(timeout=2)
            except phase_slots.SlotRefused:
                pass


def test_multiname_fifo_queue_is_blocked_only_by_prior_request_with_spare_capacity(tmp_path, monkeypatch):
    """A multi-name predecessor can block FIFO while one name has spare capacity."""
    root = enabled_root(tmp_path / "state")
    directory = root / "tasks"
    phase_slots.configure(directory, "sim", capacity=2)
    phase_slots.configure(directory, "other", capacity=1)
    records = {name: active_record(root, f"task-{name}") for name in ("holder", "first", "later")}
    local = threading.local()
    monkeypatch.setattr(phase_slots, "_authority", lambda _directory: records[local.name])
    legacy = resources.ResourceLedger(root)
    holder = legacy.request(["other"], resources.Owner("task", task_id="task-holder"))
    assert holder.held
    prior = legacy.request(["other", "sim"], resources.Owner("task", task_id="task-first"))
    assert not prior.held

    local.name = "later"
    later = phase_slots.acquire(directory, "sim", 10)
    assert later["state"] == "queued"
    assert later["blocked_by"] == [prior.reservation_id]
    assert later["blockers"] == [{"reservation_id": prior.reservation_id, "purpose": "whole_task",
        "owner": {"kind": "task", "task_id": "task-first"}, "state": "queued",
        "since": prior.enqueued_at, "enqueued_at": prior.enqueued_at, "granted_at": None}]
    assert holder.reservation_id not in later["blocked_by"]  # its name has spare capacity


def test_v2_repair_preserves_overcommitted_legacy_holders_and_phase_receipts(tmp_path):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "sim", capacity=1)
    task_ids = ["e" * 32, "f" * 32]
    for task_id in task_ids:
        active_record(root, task_id)
    ledger = resources.ResourceLedger(root)
    ledger._transact(lambda tx: setattr(tx, "dirty", True))
    document = json.loads(ledger.path.read_text())
    document["entries"] = [{"reservation_id": "bad", "names": ["sim"]}]
    resources._atomic_write(ledger.path, document)
    receipt = phase_entry("a" * 32, "phase-receipt")
    phase_slots.write_receipt(root, receipt)

    token = ledger.begin_repair()
    outcome = ledger.complete_repair(token, [
        {"reservation_id": "legacy-a", "names": ["sim"], "owner": {"kind": "task", "task_id": task_ids[0]}},
        {"reservation_id": "legacy-b", "names": ["sim"], "owner": {"kind": "task", "task_id": task_ids[1]}},
    ])
    entries = ledger._read_only()
    assert {row["reservation_id"] for row in entries} >= {"legacy-a", "legacy-b", "phase-receipt"}
    assert len([row for row in entries if "sim" in row["names"] and row["state"] == "held"]) == 3
    assert any(row["reservation_id"] == "phase-receipt" and row["phase_state"] == "cleanup_uncertain" for row in entries)
    assert set(outcome.restored) >= {"legacy-a", "legacy-b", "phase-receipt"}
    overcommit = next(row for row in phase_slots.status(root / "tasks", "sim")["leases"] if row["lease_id"] == "phase-receipt")
    assert overcommit["capacity"] == 1 and overcommit["overcommitted"] is True


def test_genuine_process_lineage_cli_acquire_uses_task_record(tmp_path):
    """The child is the persisted task process; detection uses its actual process group and ps identity."""
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "fixture", capacity=1)
    tasks = root / "tasks"
    tasks.mkdir(exist_ok=True)
    task_id = "a" * 32
    gate = tmp_path / "ready"
    driver = Path(__file__).with_name("phase_slot_driver.py")
    env = {**os.environ, "HARFLOW_STATE_ROOT": str(root), "PYTHONPATH": str(Path(__file__).parents[1])}
    process = subprocess.Popen([sys.executable, str(driver), str(gate)], cwd=Path(__file__).parents[1],
                               env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                               start_new_session=True)
    try:
        deadline = time.monotonic() + 5
        while True:
            captured = identity.capture(process.pid, [driver.name])
            if captured is not None:
                break
            if time.monotonic() > deadline:
                raise AssertionError("could not capture fixture task identity")
            time.sleep(.01)
        parent_identity = identity.capture(os.getpid(), ["pytest"])
        assert parent_identity is not None
        record = store.TaskRecord(task_id=task_id, backend="claude", session_id="fixture",
            repo_path=str(tmp_path), started_at="2026-10-31T00:00:00+00:00", pid=process.pid,
            pgid=process.pid, start_time=captured["start_time"], markers=[driver.name],
            owner=parent_identity, freedom="read_only", network=None)
        assert store.write_landed(tasks, record), store.last_write_error()
        gate.touch()
        stdout, stderr = process.communicate(timeout=15)
        assert process.returncode == 0, stderr
        result = json.loads(stdout.strip().splitlines()[-1])
        assert result["result"]["state"] == "held"
        assert result["result"]["task_id"] == task_id
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=3)


def test_three_real_lineage_tasks_overlap_code_but_serialize_heavy_intervals(tmp_path):
    root = enabled_root(tmp_path / "state")
    phase_slots.configure(root / "tasks", "fixture", capacity=1)
    tasks = root / "tasks"
    driver = Path(__file__).with_name("phase_slot_driver.py")
    gate = tmp_path / "start-all"
    env = {**os.environ, "HARFLOW_STATE_ROOT": str(root), "PYTHONPATH": str(Path(__file__).parents[1])}
    parent_identity = identity.capture(os.getpid(), ["pytest"])
    assert parent_identity is not None
    processes = []
    try:
        for index in range(3):
            task_id = f"{index + 1:032x}"
            process = subprocess.Popen([sys.executable, str(driver), str(gate), task_id],
                cwd=Path(__file__).parents[1], env=env, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, start_new_session=True)
            processes.append((task_id, process))
            deadline = time.monotonic() + 5
            captured = None
            while captured is None and time.monotonic() < deadline:
                captured = identity.capture(process.pid, [driver.name])
                if captured is None:
                    time.sleep(.01)
            assert captured is not None
            record = store.TaskRecord(task_id=task_id, backend="claude", session_id=task_id,
                repo_path=str(tmp_path), started_at="2026-10-31T00:00:00+00:00", pid=process.pid,
                pgid=process.pid, start_time=captured["start_time"], markers=[driver.name],
                owner=parent_identity, freedom="read_only")
            assert store.write_landed(tasks, record), store.last_write_error()
        gate.touch()
        events = []
        for task_id, process in processes:
            stdout, stderr = process.communicate(timeout=30)
            assert process.returncode == 0, stderr
            events.extend(json.loads(line) for line in stdout.splitlines())
        by_task = {task_id: {event["kind"]: event["at"] for event in events if event["task"] == task_id}
                   for task_id, _process in processes}
        assert len(by_task) == 3
        # All fixture tasks write code concurrently before entering the phase queue.
        assert max(row["code_start"] for row in by_task.values()) < min(row["code_end"] for row in by_task.values())
        heavy = sorted((row["heavy_start"], row["heavy_end"], task_id) for task_id, row in by_task.items())
        assert all(left[1] <= right[0] for left, right in zip(heavy, heavy[1:]))
        evidence = {"capacity": 1, "events": sorted(events, key=lambda event: event["at"]),
                    "code_overlap": [max(row["code_start"] for row in by_task.values()), min(row["code_end"] for row in by_task.values())],
                    "heavy_intervals": [{"task_id": task_id, "start": start_at, "end": end_at}
                                        for start_at, end_at, task_id in heavy]}
        (tmp_path / "phase-slot-overlap-evidence.json").write_text(json.dumps(evidence, indent=2))
        view = phase_slots.status(tasks, "fixture")
        assert {event["kind"] for event in view["events"]} >= {"queued", "granted", "released"}
    finally:
        for _task_id, process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)


def test_real_holder_and_owner_death_release_queue_only_after_both_generations_are_dead(tmp_path):
    root = enabled_root(tmp_path / "state")
    starts, finishes = tmp_path / "cleanup-starts", tmp_path / "cleanup-finishes"
    cleanup = tmp_path / "cleanup.py"
    cleanup.write_text(
        "import pathlib, sys, time\n"
        "with pathlib.Path(sys.argv[1]).open('a') as stream: stream.write(str(time.time_ns()) + '\\n')\n"
        "time.sleep(.25)\n"
        "with pathlib.Path(sys.argv[2]).open('a') as stream: stream.write(str(time.time_ns()) + '\\n')\n"
    )
    phase_slots.configure(root / "tasks", "fixture", capacity=1,
        cleanup_argv=[sys.executable, str(cleanup), str(starts), str(finishes)], cleanup_timeout=5)
    tasks = root / "tasks"
    agent_driver = Path(__file__).with_name("phase_slot_driver.py")
    owner_driver = Path(__file__).with_name("phase_owner_driver.py")
    start, owner_finish = tmp_path / "start", tmp_path / "finish-owner"
    release_first, release_second = tmp_path / "release-first", tmp_path / "release-second"
    env = {**os.environ, "HARFLOW_STATE_ROOT": str(root), "PYTHONPATH": str(Path(__file__).parents[1])}
    owner = subprocess.Popen([sys.executable, str(owner_driver), str(start), str(owner_finish)],
        cwd=Path(__file__).parents[1], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        text=True, start_new_session=True)
    owner_identity = None
    agents = {}
    identities = {}
    supervisor_started = False
    death_timeline = []
    try:
        deadline = time.monotonic() + 5
        while owner_identity is None and time.monotonic() < deadline:
            owner_identity = identity.capture(owner.pid, [owner_driver.name])
            if owner_identity is None:
                time.sleep(.01)
        assert owner_identity is not None
        phase_slots.service_start(tasks)
        supervisor_started = True
        for task_id, stop in (("1" * 32, release_first), ("2" * 32, release_second)):
            process = subprocess.Popen([sys.executable, str(agent_driver), str(start), "hold", task_id, str(stop)],
                cwd=Path(__file__).parents[1], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, start_new_session=True)
            agents[task_id] = process
            agent_identity = None
            deadline = time.monotonic() + 5
            while agent_identity is None and time.monotonic() < deadline:
                agent_identity = identity.capture(process.pid, [agent_driver.name])
                if agent_identity is None:
                    time.sleep(.01)
            assert agent_identity is not None
            identities[task_id] = agent_identity
            record = store.TaskRecord(task_id=task_id, backend="claude", session_id=task_id,
                repo_path=str(tmp_path), started_at="2026-10-31T00:00:00+00:00", pid=process.pid,
                pgid=process.pid, start_time=agent_identity["start_time"], markers=[agent_driver.name],
                owner=owner_identity, freedom="unrestricted", network=None,
                enforcement=get_backend("claude").enforcement("unrestricted", None).as_dict())
            assert store.write_landed(tasks, record), store.last_write_error()
        start.touch()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            leases = phase_slots.status(tasks, "fixture")["leases"]
            states = {row["task_id"]: row["state"] for row in leases}
            if states.get("1" * 32) == "held" and states.get("2" * 32) == "queued":
                break
            time.sleep(.03)
        assert states.get("1" * 32) == "held" and states.get("2" * 32) == "queued"

        assert identity.check_detail(identities["1" * 32])[0] == "alive"
        os.kill(agents["1" * 32].pid, 9)
        agents["1" * 32].wait(timeout=5)
        death_timeline.append({"kind": "holder_generation_dead", "at": time.time_ns()})
        phase_slots.reconcile(tasks)
        after_agent_death = {row["task_id"]: row["state"] for row in phase_slots.status(tasks, "fixture")["leases"]}
        assert after_agent_death.get("1" * 32) == "held", "owner death is also required to release the holder"
        assert after_agent_death.get("2" * 32) == "queued"

        assert identity.check_detail(owner_identity)[0] == "alive"
        os.kill(owner.pid, 9)
        owner.wait(timeout=5)
        death_timeline.append({"kind": "owner_generation_dead", "at": time.time_ns()})
        phase_slots.reconcile(tasks)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            leases = phase_slots.status(tasks, "fixture")["leases"]
            successor = next((row for row in leases if row["task_id"] == "2" * 32), None)
            if successor and successor["state"] == "held":
                break
            time.sleep(.03)
            phase_slots.reconcile(tasks)
        assert successor is not None and successor["state"] == "held"
        assert all(row["task_id"] != "1" * 32 for row in leases)
        assert len(starts.read_text().splitlines()) == 1
        assert len(finishes.read_text().splitlines()) == 1
        assert int(starts.read_text().splitlines()[0]) < int(finishes.read_text().splitlines()[0])
        events = phase_slots.status(tasks, "fixture")["events"]
        cleanup_success = next(i for i, event in enumerate(events) if event["kind"] == "cleanup_succeeded")
        successor_grant = next(i for i, event in enumerate(events) if event["lease_id"] == successor["lease_id"] and event["kind"] == "granted")
        assert cleanup_success < successor_grant
        (tmp_path / "phase-slot-death-evidence.json").write_text(json.dumps({
            "timeline": death_timeline,
            "cleanup_starts": starts.read_text().splitlines(),
            "cleanup_finishes": finishes.read_text().splitlines(),
            "slot_events": events,
            "successor": successor,
        }, indent=2))
    finally:
        owner_finish.touch()
        for task_id, process in agents.items():
            if process.poll() is None:
                expected = identities.get(task_id)
                if expected is not None and identity.check_detail(expected)[0] == "alive":
                    os.kill(process.pid, 9)
                elif expected is None or identity.check_detail(expected)[0] != "dead":
                    process.kill()
                process.wait(timeout=5)
        if owner.poll() is None:
            if owner_identity is not None and identity.check_detail(owner_identity)[0] == "alive":
                os.kill(owner.pid, 9)
            elif owner_identity is None or identity.check_detail(owner_identity)[0] != "dead":
                owner.kill()
            owner.wait(timeout=5)
        phase_slots.reconcile(tasks)
        if supervisor_started:
            deadline = time.monotonic() + 10
            while phase_slots.status(tasks, "fixture")["leases"] and time.monotonic() < deadline:
                phase_slots.reconcile(tasks)
                time.sleep(.03)
            assert phase_slots.status(tasks, "fixture")["leases"] == []
            assert len(starts.read_text().splitlines()) == 2
            assert len(finishes.read_text().splitlines()) == 2
            phase_slots.service_stop(tasks)
            deadline = time.monotonic() + 5
            while phase_slots.service_status(root)["state"] != "missing" and time.monotonic() < deadline:
                time.sleep(.02)
            assert phase_slots.service_status(root)["state"] == "missing"


def test_detached_supervisor_records_cleanup_before_next_grant(tmp_path):
    root = enabled_root(tmp_path / "state")
    cleanup = tmp_path / "cleanup.py"
    started, finished = tmp_path / "cleanup-started", tmp_path / "cleanup-finished"
    cleanup.write_text(
        "import pathlib, sys, time\n"
        "pathlib.Path(sys.argv[1]).write_text(str(time.time_ns()))\n"
        "time.sleep(0.4)\n"
        "pathlib.Path(sys.argv[2]).write_text(str(time.time_ns()))\n"
    )
    log_dir = root / "tasks"
    phase_slots.configure(log_dir, "sim", cleanup_argv=[sys.executable, str(cleanup), str(started), str(finished)], cleanup_timeout=5)
    policy = resources.ResourceLedger(root)._read_only()
    assert policy == []
    configuration = json.loads((root / "resources" / "ledger.json").read_text())["slot_configuration"]
    hook = configuration["names"]["sim"]["cleanup"]
    first = qualified_phase_entry(root, "cleanup-owner", "cleanup-one", hook)
    first.update(state="held", phase_state="held", granted_at=first["enqueued_at"], cleanup_policy=hook)
    second = qualified_phase_entry(root, "queued-owner", "cleanup-two", hook)
    second["cleanup_policy"] = hook
    ledger = resources.ResourceLedger(root)
    def enqueue(tx):
        tx.entries.extend((first, second))
        tx.phase_event(first, "granted")
        tx.phase_event(second, "queued")
        tx.dirty = True
    ledger._transact(enqueue)
    ledger._transact(lambda tx: (tx.release("cleanup-one"), tx.grant_pass()))
    supervisor = phase_slots.service_start(log_dir)
    assert supervisor["state"] == "alive"
    try:
        deadline = time.monotonic() + 10
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        assert started.exists(), "detached supervisor did not launch the configured cleanup argv"
        before_end = phase_slots.status(log_dir, "sim")
        assert next(row for row in before_end["leases"] if row["lease_id"] == "cleanup-two")["state"] == "queued"
        assert not finished.exists(), "fixture cleanup should remain active while the second lease is blocked"
        with pytest.raises(phase_slots.SlotRefused, match="resolve every phase lease"):
            phase_slots.service_stop(log_dir)

        while time.monotonic() < deadline:
            current = phase_slots.status(log_dir, "sim")
            target = next((row for row in current["leases"] if row["lease_id"] == "cleanup-two"), None)
            if target and target["state"] == "held":
                break
            time.sleep(.03)
        assert finished.exists()
        events = phase_slots.status(log_dir, "sim")["events"]
        cleanup_success = next(i for i, event in enumerate(events) if event["lease_id"] == "cleanup-one" and event["kind"] == "cleanup_succeeded")
        successor_grant = next(i for i, event in enumerate(events) if event["lease_id"] == "cleanup-two" and event["kind"] == "granted")
        assert cleanup_success < successor_grant
        assert int(started.read_text()) < int(finished.read_text())
        (tmp_path / "phase-slot-supervisor-evidence.json").write_text(json.dumps({
            "cleanup_start_ns": int(started.read_text()),
            "cleanup_finish_ns": int(finished.read_text()),
            "slot_events": events,
        }, indent=2))
    finally:
        # End the test-owned supervisor cleanly. Test entries are settled in this fixture root only.
        def clear_test_entries(tx):
            for raw in list(tx.entries):
                if raw.get("purpose") == "phase" and raw["reservation_id"] in {"cleanup-one", "cleanup-two"}:
                    raw["phase_state"] = "released"
                    phase_slots.write_receipt(root, raw)
                    tx.entries.remove(raw)
                    tx.dirty = True
        ledger._transact(clear_test_entries)
        phase_slots.service_stop(log_dir)
        stop_deadline = time.monotonic() + 5
        while phase_slots.service_status(root)["state"] != "missing" and time.monotonic() < stop_deadline:
            time.sleep(.02)
        assert phase_slots.service_status(root)["state"] == "missing"


@pytest.mark.parametrize(("script", "timeout", "blocked_state"), [
    ("raise SystemExit(7)", 4, "cleanup_failed"),
    ("time.sleep(4)", 1, "cleanup_uncertain"),
])
def test_failed_or_timed_out_cleanup_keeps_capacity_until_human_resolution(tmp_path, script, timeout, blocked_state):
    root = enabled_root(tmp_path / "state")
    cleanup = tmp_path / "cleanup.py"
    cleanup.write_text("import time\n" + script + "\n")
    log_dir = root / "tasks"
    phase_slots.configure(log_dir, "sim", cleanup_argv=[sys.executable, str(cleanup)], cleanup_timeout=timeout)
    hook = json.loads((root / "resources" / "ledger.json").read_text())["slot_configuration"]["names"]["sim"]["cleanup"]
    ledger = seed_phase_pair(root, hook, "failure-one", "failure-two")
    supervisor = phase_slots.service_start(log_dir)
    assert supervisor["state"] == "alive"
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            rows = phase_slots.status(log_dir, "sim")["leases"]
            first = next(row for row in rows if row["lease_id"] == "failure-one")
            if first["state"] == blocked_state:
                break
            time.sleep(.03)
        rows = phase_slots.status(log_dir, "sim")["leases"]
        first = next(row for row in rows if row["lease_id"] == "failure-one")
        second = next(row for row in rows if row["lease_id"] == "failure-two")
        assert first["state"] == blocked_state
        assert second["state"] == "queued"
        assert first["diagnostic"]
        assert phase_slots.resolve(log_dir, "failure-one")["state"] == "resolved"
        rows = phase_slots.status(log_dir, "sim")["leases"]
        assert next(row for row in rows if row["lease_id"] == "failure-two")["state"] == "held"
    finally:
        def clear_entries(tx):
            for raw in list(tx.entries):
                if raw.get("purpose") == "phase" and raw["reservation_id"] in {"failure-one", "failure-two"}:
                    raw["phase_state"] = "released"
                    phase_slots.write_receipt(root, raw)
                    tx.entries.remove(raw)
                    tx.dirty = True
        ledger._transact(clear_entries)
        phase_slots.service_stop(log_dir)
        deadline = time.monotonic() + 5
        while phase_slots.service_status(root)["state"] != "missing" and time.monotonic() < deadline:
            time.sleep(.02)
        assert phase_slots.service_status(root)["state"] == "missing"


def test_supervisor_restart_converts_claim_to_uncertain_without_replaying_hook(tmp_path):
    root = enabled_root(tmp_path / "state")
    ran = tmp_path / "hook-ran"
    cleanup = tmp_path / "cleanup.py"
    cleanup.write_text("import pathlib, sys\npathlib.Path(sys.argv[1]).touch()\n")
    log_dir = root / "tasks"
    phase_slots.configure(log_dir, "sim", cleanup_argv=[sys.executable, str(cleanup), str(ran)], cleanup_timeout=5)
    hook = json.loads((root / "resources" / "ledger.json").read_text())["slot_configuration"]["names"]["sim"]["cleanup"]
    old = phase_slots.service_start(log_dir)
    assert old["state"] == "alive"
    old_receipt = phase_slots._read_private(root / "resources" / "slot-service.json")
    old_identity = old_receipt["identity"]
    ledger = resources.ResourceLedger(root)

    # Terminate only the exact test-owned supervisor generation after matching its process identity.
    assert identity.check_detail(old_identity)[0] == "alive"
    os.kill(old_identity["pid"], 9)
    deadline = time.monotonic() + 5
    while phase_slots.service_status(root)["state"] == "alive" and time.monotonic() < deadline:
        time.sleep(.02)
    assert phase_slots.service_status(root)["state"] != "alive"

    first = qualified_phase_entry(root, "cleanup-owner", "restart-lease", hook)
    first.update(state="held", phase_state="cleanup_running", granted_at=first["enqueued_at"],
        cleanup_started_at=first["enqueued_at"], cleanup_claim="a" * 32,
        cleanup_supervisor=old["generation"], cleanup_runner_tracking="spawn_pending", cleanup_policy=hook)
    def claim(tx):
        tx.entries.append(first)
        tx.phase_event(first, "cleanup_claimed")
        tx.dirty = True
    ledger._transact(claim)

    restarted = phase_slots.service_start(log_dir)
    assert restarted["state"] == "alive" and restarted["generation"] != old["generation"]
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            current = phase_slots.status(log_dir, "sim")["leases"]
            if current and current[0]["state"] == "cleanup_uncertain":
                break
            time.sleep(.03)
        row = next(row for row in phase_slots.status(log_dir, "sim")["leases"] if row["lease_id"] == "restart-lease")
        assert row["state"] == "cleanup_uncertain"
        assert not ran.exists(), "restart must never replay a cleanup with uncertain external effects"
        assert phase_slots.resolve(log_dir, "restart-lease", confirm_untracked_cleanup_stopped=True)["state"] == "resolved"
        lost = qualified_phase_entry(root, "lost-result-owner", "lost-result-lease", hook)
        lost.update(state="held", phase_state="cleanup_running", granted_at=lost["enqueued_at"],
            cleanup_started_at=lost["enqueued_at"], cleanup_claim="b" * 32,
            cleanup_supervisor=restarted["generation"], cleanup_runner_tracking="spawn_pending", cleanup_policy=hook)
        def seed_lost_result(tx):
            tx.entries.append(lost)
            tx.phase_event(lost, "cleanup_claimed")
            tx.dirty = True
        ledger._transact(seed_lost_result)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            current = phase_slots.status(log_dir, "sim")["leases"]
            lost_view = next((item for item in current if item["lease_id"] == "lost-result-lease"), None)
            if lost_view and lost_view["state"] == "cleanup_uncertain":
                break
            time.sleep(.03)
        lost_view = next(item for item in phase_slots.status(log_dir, "sim")["leases"] if item["lease_id"] == "lost-result-lease")
        assert lost_view["state"] == "cleanup_uncertain"
        assert "lost its result" in lost_view["diagnostic"]
        assert phase_slots.resolve(log_dir, "lost-result-lease", confirm_untracked_cleanup_stopped=True)["state"] == "resolved"
    finally:
        def clear_restart_entry(tx):
            for lease_id in ("restart-lease", "lost-result-lease"):
                raw = tx.find(lease_id)
                if raw is not None:
                    raw["phase_state"] = "resolved"
                    phase_slots.write_receipt(root, raw)
                    tx.entries.remove(raw)
                    tx.dirty = True
        ledger._transact(clear_restart_entry)
        if phase_slots.service_status(root)["state"] == "alive":
            phase_slots.service_stop(log_dir)
        deadline = time.monotonic() + 5
        while phase_slots.service_status(root)["state"] != "missing" and time.monotonic() < deadline:
            time.sleep(.02)
        assert phase_slots.service_status(root)["state"] == "missing"


def test_late_cleanup_success_cannot_clear_repair_uncertainty_or_admit_successor(tmp_path):
    root = enabled_root(tmp_path / "state")
    started, finish = tmp_path / "runner-started", tmp_path / "finish-runner"
    cleanup = tmp_path / "cleanup.py"
    cleanup.write_text(
        "import pathlib, sys, time\n"
        "pathlib.Path(sys.argv[1]).touch()\n"
        "gate = pathlib.Path(sys.argv[2])\n"
        "deadline = time.monotonic() + 10\n"
        "while not gate.exists() and time.monotonic() < deadline: time.sleep(.01)\n"
    )
    log_dir = root / "tasks"
    phase_slots.configure(log_dir, "sim", cleanup_argv=[sys.executable, str(cleanup), str(started), str(finish)], cleanup_timeout=8)
    hook = json.loads((root / "resources" / "ledger.json").read_text())["slot_configuration"]["names"]["sim"]["cleanup"]
    ledger = seed_phase_pair(root, hook, "cas-one", "cas-two")
    phase_slots.service_start(log_dir)
    try:
        deadline = time.monotonic() + 10
        while not started.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        assert started.exists()
        running = next(row for row in phase_slots.status(log_dir, "sim")["leases"] if row["lease_id"] == "cas-one")
        assert running["state"] == "cleanup_running"
        assert running["cleanup_started_at"]

        def mark_uncertain(tx):
            raw = tx.find("cas-one")
            raw["phase_state"] = "cleanup_uncertain"
            raw["diagnostic"] = "repair made runner result uncertain"
            tx.phase_event(raw, "cleanup_uncertain")
            tx.touch(raw)
        ledger._transact(mark_uncertain)
        finish.touch()
        # Wait for the exact recorded fixture runner to settle before inspecting the worker CAS.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            current = next(row for row in ledger._read_only() if row["reservation_id"] == "cas-one")
            if current.get("cleanup_runner") and identity.identity_check(current["cleanup_runner"]) == "dead":
                break
            time.sleep(.03)
        assert identity.identity_check(current["cleanup_runner"]) == "dead"
        rows = phase_slots.status(log_dir, "sim")["leases"]
        assert next(row for row in rows if row["lease_id"] == "cas-one")["state"] == "cleanup_uncertain"
        assert next(row for row in rows if row["lease_id"] == "cas-two")["state"] == "queued"
        assert phase_slots.resolve(log_dir, "cas-one")["state"] == "resolved"
        assert next(row for row in phase_slots.status(log_dir, "sim")["leases"] if row["lease_id"] == "cas-two")["state"] == "held"
    finally:
        def clear_entries(tx):
            for raw in list(tx.entries):
                if raw.get("purpose") == "phase" and raw["reservation_id"] in {"cas-one", "cas-two"}:
                    raw["phase_state"] = "resolved"
                    phase_slots.write_receipt(root, raw)
                    tx.entries.remove(raw)
                    tx.dirty = True
        ledger._transact(clear_entries)
        if phase_slots.service_status(root)["state"] == "alive":
            phase_slots.service_stop(log_dir)
        deadline = time.monotonic() + 5
        while phase_slots.service_status(root)["state"] != "missing" and time.monotonic() < deadline:
            time.sleep(.02)
        assert phase_slots.service_status(root)["state"] == "missing"


def test_human_resolution_refuses_while_recorded_cleanup_runner_is_live(tmp_path, monkeypatch):
    root = enabled_root(tmp_path / "state")
    log_dir = root / "tasks"
    runner = identity.capture(os.getpid(), ["pytest-fixture-runner"])
    assert runner is not None
    row = phase_entry("cleanup-owner", "live-runner-lease")
    row.update(state="held", phase_state="cleanup_failed", granted_at=row["enqueued_at"],
        cleanup_started_at=row["enqueued_at"], cleanup_finished_at=row["enqueued_at"],
        cleanup_claim="c" * 32, cleanup_supervisor="d" * 32, cleanup_runner_tracking="recorded",
        diagnostic="inspect cleanup", cleanup_runner=runner)
    ledger = resources.ResourceLedger(root)
    def seed(tx):
        tx.entries.append(row)
        tx.phase_event(row, "cleanup_failed")
        tx.dirty = True
    ledger._transact(seed)
    with pytest.raises(phase_slots.SlotRefused, match="runner remains live or uncertain"):
        phase_slots.resolve(log_dir, "live-runner-lease")
    monkeypatch.setattr(identity, "identity_check", lambda _identity: "dead")
    assert phase_slots.resolve(log_dir, "live-runner-lease")["state"] == "resolved"
    assert resources.ResourceLedger(root)._read_only() == []
