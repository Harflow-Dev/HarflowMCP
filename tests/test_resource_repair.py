"""Ledger repair is atomic and fail-closed, keeps live borrowers, and the holding scope is found through
non-holding intermediates."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from harflow_mcp import resources, store, task_resources
from harflow_mcp.resources import ResourceLedger

from test_task_resources import FakeCaller, OWNER, ledger, owned, tasks_dir, write_receipt  # noqa: F401
from test_worktrees import record_for  # noqa: F401


def task(name: str) -> dict:
    return {"kind": "task", "task_id": name}


def corrupt(root: Path) -> None:
    ledger(root).directory.mkdir(parents=True, exist_ok=True)
    ledger(root).path.write_text("garbage")


def running(task_id: str, rid: str, names: list[str], *, pid: int, borrowed_from: str | None = None, **extra) -> store.TaskRecord:
    held = {"names": names, "reservation_id": rid, "state": "held", **({"borrowed_from": borrowed_from} if borrowed_from else {})}
    return store.TaskRecord(task_id=task_id, backend="claude", session_id=None, repo_path="/x", started_at="2026-01-01T00:00:00+00:00",
                            status="running", pid=pid, start_time="Mon Jan  1 00:00:00 2001", markers=["x"], owner=OWNER,
                            resources=held, **extra)


# ---- item 6: repair is fail-closed until the whole reconstruction is published -------------------


def test_every_operation_refuses_between_the_start_and_the_publication_of_a_repair(tmp_path) -> None:
    led = ResourceLedger(tmp_path / "state")
    held = led.request(["sim"], task("a"))
    led.path.write_text("garbage")
    token = led.begin_repair()
    other = ResourceLedger(tmp_path / "state")  # another process
    for operation in (lambda: other.request(["sim"], task("b")), other.entries, other.snapshot, lambda: other.get("x"),
                      lambda: other.poll(held.reservation_id), lambda: other.release(held.reservation_id),
                      lambda: other.reap({}), lambda: other.restore_held(["sim"], task("a"), "r1")):
        with pytest.raises(resources.ResourceLedgerRepairing) as refused:
            operation()
        assert refused.value.code == "resource_ledger_repairing"
    result = led.complete_repair(token, [{"reservation_id": held.reservation_id, "names": ["sim"], "owner": task("a")}])
    assert result.restored == [held.reservation_id] and result.conflicts == []
    assert not led.request(["sim"], task("b")).held  # the restored holder was never invisible
    assert [e.owner.task_id for e in led.entries() if e.held] == ["a"]


def test_an_interrupted_repair_stays_closed_and_can_be_resumed(tmp_path) -> None:
    led = ResourceLedger(tmp_path / "state")
    led.request(["sim"], task("a"))
    led.path.write_text("garbage")
    first = led.begin_repair()
    again = ResourceLedger(tmp_path / "state").begin_repair()  # the person runs it again after a crash
    assert again.id == first.id
    with pytest.raises(resources.ResourceLedgerRepairing):
        led.entries()
    with pytest.raises(resources.ResourceError):  # a stale token cannot publish over a newer repair
        led.complete_repair(resources.RepairToken("stale", None), [])
    led.complete_repair(again, [])
    assert led.entries() == []
    with pytest.raises(resources.LedgerNotCorrupt):  # and it is healthy again
        led.begin_repair()
    assert any(p.name.startswith("ledger.corrupt-") for p in led.directory.iterdir())


def test_competing_acquisition_during_evidence_collection_is_refused(tmp_path, monkeypatch, identities, owned) -> None:
    log_dir = owned.tasks_directory
    identities.alive(OWNER["pid"])
    identities.alive(777)
    store.write(log_dir, running("a" * 32, "r" * 32, ["sim"], pid=777))
    corrupt(tmp_path)
    seen = []
    real = task_resources.collect_evidence

    def collecting(directory):
        try:
            ledger(tmp_path).request(["sim"], task("competitor"))
        except resources.ResourceLedgerRepairing:
            seen.append("refused")
        return real(directory)

    monkeypatch.setattr(task_resources, "collect_evidence", collecting)
    result = task_resources.repair(log_dir)
    assert seen == ["refused"] and result["restored"] == ["r" * 32]
    assert not ledger(tmp_path).request(["sim"], task("competitor")).held


# ---- item 7: live borrowers survive a repair -----------------------------------------------------


def settled_root(task_id: str, rid: str, names: list[str]) -> store.TaskRecord:
    return record_for(task_id, "/x", resources={"names": names, "reservation_id": rid, "state": "released"})


def test_repair_reconstructs_lending_and_keeps_the_root_while_a_borrower_lives(tmp_path, identities, owned) -> None:
    log_dir = owned.tasks_directory
    identities.alive(OWNER["pid"])
    identities.alive(777)
    store.write(log_dir, running("a" * 32, "r1" + "0" * 30, ["sim", "lab"], pid=776))
    store.write(log_dir, running("b" * 32, "r2" + "0" * 30, ["sim"], pid=777, borrowed_from="r1" + "0" * 30))
    identities.alive(776)
    corrupt(tmp_path)
    result = task_resources.repair(log_dir)
    assert sorted(result["restored"]) == ["r1" + "0" * 30, "r2" + "0" * 30] and result["conflicts"] == []
    entries = {e.reservation_id: e for e in ledger(tmp_path).entries()}
    assert entries["r2" + "0" * 30].borrows_from == "r1" + "0" * 30 and entries["r1" + "0" * 30].names == ("lab", "sim")
    # The root cannot go while its borrower is unresolved.
    assert ledger(tmp_path).release("r1" + "0" * 30) == "release_requested"
    assert not ledger(tmp_path).request(["lab"], task("c")).held
    assert ledger(tmp_path).release("r2" + "0" * 30) == "released"
    assert [e.owner.task_id for e in ledger(tmp_path).entries() if e.held] == ["c"]


def test_a_settled_root_with_a_live_borrower_is_restored_as_release_requested(tmp_path, identities, owned) -> None:
    log_dir = owned.tasks_directory
    identities.alive(OWNER["pid"])
    identities.alive(777)
    rid = "r1" + "0" * 30
    store.write(log_dir, settled_root("a" * 32, rid, ["sim", "lab"]))
    store.write(log_dir, running("b" * 32, "r2" + "0" * 30, ["sim"], pid=777, borrowed_from=rid))
    corrupt(tmp_path)
    task_resources.repair(log_dir)
    root = next(e for e in ledger(tmp_path).entries() if e.reservation_id == rid)
    assert root.held and root.release_requested and root.owner.task_id == "a" * 32
    assert not ledger(tmp_path).request(["lab"], task("c")).held


def test_an_unprovable_root_is_replaced_by_a_conservative_blocking_entry(tmp_path, identities, owned) -> None:
    log_dir = owned.tasks_directory
    identities.alive(OWNER["pid"])
    identities.alive(777)
    rid = "r1" + "0" * 30
    store.write(log_dir, running("b" * 32, "r2" + "0" * 30, ["sim"], pid=777, borrowed_from=rid))
    corrupt(tmp_path)
    result = task_resources.repair(log_dir)
    assert any(item.get("reason") == "lender_unknown" for item in result["conflicts"])
    root = next(e for e in ledger(tmp_path).entries() if e.reservation_id == rid)
    assert root.held and root.release_requested and "sim" in root.names
    assert not ledger(tmp_path).request(["sim"], task("c")).held
    # Only a person (or the borrower settling) frees it.
    assert task_resources.reap(log_dir).removed == []
    assert ledger(tmp_path).release("r2" + "0" * 30) == "released"
    assert all(e.reservation_id != rid or e.owner.task_id == "c" for e in ledger(tmp_path).entries())


# ---- item 8: the holding scope is the nearest ancestor that holds ---------------------------------


def test_scope_is_found_through_a_non_holding_intermediate(tmp_path, owned) -> None:
    log_dir = owned.tasks_directory
    entry = ledger(tmp_path).request(["sim", "lab"], task("g" * 32))
    store.write(log_dir, running("g" * 32, entry.reservation_id, ["sim", "lab"], pid=1))
    store.write(log_dir, record_for("m" * 32, "/x", status="running", spawned_by="g" * 32))
    caller = FakeCaller(record_for("c" * 32, "/x", status="running", spawned_by="m" * 32))
    assert task_resources.holding_scope(log_dir, caller) == resources.Scope(entry.reservation_id)
    request = task_resources.build_request(log_dir, ["sim"], None, caller)  # covered: borrowed
    assert request["scope"] == {"reservation_id": entry.reservation_id}
    with pytest.raises(resources.ResourceNotDeclaredByScope):  # uncovered: refused, not queued
        task_resources.build_request(log_dir, ["elsewhere"], None, caller)


def test_a_workflow_attempt_ancestor_is_a_scope_that_refuses_everything(tmp_path, owned) -> None:
    log_dir = owned.tasks_directory
    (log_dir.parent / "workflow-owners").mkdir()
    (log_dir.parent / "workflow-owners" / f"{'w' * 32}.json").write_text("{}")
    store.write(log_dir, record_for("w" * 32, "/x", status="running"))
    store.write(log_dir, record_for("m" * 32, "/x", status="running", spawned_by="w" * 32))
    caller = FakeCaller(record_for("c" * 32, "/x", status="running", spawned_by="m" * 32))
    assert task_resources.holding_scope(log_dir, caller) == resources.Scope(None)
    with pytest.raises(resources.ResourceNotDeclaredByScope):
        task_resources.build_request(log_dir, ["sim"], None, caller)


def test_a_pending_receipt_ancestor_that_holds_is_a_scope(tmp_path, owned) -> None:
    log_dir = owned.tasks_directory
    entry = ledger(tmp_path).request(["sim"], task("p" * 32))
    write_receipt(tmp_path, "p" * 32, "granted", owner=OWNER,
                  resources={"names": ["sim"], "state": "granted", "reservation_id": entry.reservation_id})
    store.write(log_dir, record_for("m" * 32, "/x", status="running", spawned_by="p" * 32))
    caller = FakeCaller(record_for("c" * 32, "/x", status="running", spawned_by="m" * 32))
    assert task_resources.holding_scope(log_dir, caller) == resources.Scope(entry.reservation_id)


def test_an_unreadable_ancestor_fails_closed(tmp_path, owned) -> None:
    log_dir = owned.tasks_directory
    store.write(log_dir, record_for("m" * 32, "/x", status="running", spawned_by="z" * 32))  # no record, no receipt
    caller = FakeCaller(record_for("c" * 32, "/x", status="running", spawned_by="m" * 32))
    with pytest.raises(resources.ResourceNotDeclaredByScope):
        task_resources.holding_scope(log_dir, caller)
    # A chain with no holder anywhere is simply no scope.
    store.write(log_dir, record_for("z" * 32, "/x", status="running"))
    assert task_resources.holding_scope(log_dir, caller) is None


# ---- round 2: grants and spawns racing a repair ---------------------------------------------------


@pytest.mark.asyncio
async def test_a_grant_dropped_by_a_repair_that_ran_before_the_receipt_said_granted_is_requested_again(tmp_path, monkeypatch, identities, owned) -> None:
    from types import SimpleNamespace
    from harflow_mcp import task_supervisor
    log_dir = owned.tasks_directory
    identities.alive(OWNER["pid"])
    write_receipt(tmp_path, "a" * 32, "claimed", owner=OWNER)
    real_mark = task_supervisor.mark
    fired = []

    def mark(directory, task_id, generation, **changes):
        if changes.get("state") == "granted" and not fired:
            fired.append(1)
            # The ledger was corrupted and repaired after the owner acquired but while the receipt still
            # said waiting: the repair has no evidence of this grant and rebuilds without it.
            corrupt(tmp_path)
            task_resources.repair(directory)
            assert ledger(tmp_path).entries() == []
        return real_mark(directory, task_id, generation, **changes)

    monkeypatch.setattr(task_supervisor, "mark", mark)
    grant = await task_supervisor._acquire_resources(
        SimpleNamespace(log_dir=log_dir), "a" * 32, "c" * 32, {"names": ["sim"], "wait_seconds": 5, "scope": None})
    assert fired and grant["state"] == "held"
    held = [e for e in ledger(tmp_path).entries() if e.held]
    assert [(e.owner.task_id, e.reservation_id) for e in held] == [("a" * 32, grant["reservation_id"])]
    assert not ledger(tmp_path).request(["sim"], task("other")).held
    assert task_supervisor.read_launch(log_dir, "a" * 32)["state"] == "granted"


def test_a_launch_that_spawns_between_the_receipt_and_record_scans_is_still_restored(tmp_path, monkeypatch, identities, owned) -> None:
    from harflow_mcp import task_supervision
    log_dir = owned.tasks_directory
    identities.alive(OWNER["pid"])
    identities.alive(777)
    rid = "r" * 32
    write_receipt(tmp_path, "a" * 32, "granted", owner=OWNER, resources={"names": ["sim"], "state": "granted", "reservation_id": rid})
    spawned = running("a" * 32, rid, ["sim"], pid=777)
    fired = []

    def after_first_scan(real):
        def scan(*args, **kwargs):
            found = real(*args, **kwargs)
            if not fired:
                fired.append(1)
                store.write(log_dir, spawned)  # the owner spawns right after whichever scan came first
            return found
        return scan

    monkeypatch.setattr(store, "read_all", after_first_scan(store.read_all))
    monkeypatch.setattr(task_supervision, "reservations", after_first_scan(task_supervision.reservations))
    corrupt(tmp_path)
    result = task_resources.repair(log_dir)
    assert result["restored"] == [rid]
    assert not ledger(tmp_path).request(["sim"], task("other")).held


def test_a_borrower_whose_lender_lost_a_conflict_is_kept_not_dropped(tmp_path) -> None:
    led = ResourceLedger(tmp_path / "state")
    led.request(["x"], task("seed"))
    led.path.write_text("garbage")
    token = led.begin_repair()
    result = led.complete_repair(token, [
        {"reservation_id": "ra", "names": ["lab", "sim"], "owner": task("a")},
        {"reservation_id": "rb", "names": ["gpu", "lab"], "owner": task("b")},  # overlaps ra: not restored
        {"reservation_id": "rc", "names": ["gpu"], "owner": task("c"), "borrows_from": "rb"},
        {"reservation_id": "rd", "names": ["lab"], "owner": task("d"), "borrows_from": "rb"},
    ])
    assert {c["reservation_id"] for c in result.conflicts} >= {"rb", "rc", "rd"}
    entries = {e.reservation_id: e for e in led.entries()}
    assert entries["rd"].borrows_from == "ra"  # attached to the restored root that covers it
    assert entries["rc"].names == ("gpu",) and entries["rc"].borrows_from == "rb"  # under a stand-in for its lender
    assert entries["rb"].names == ("gpu",) and entries["rb"].release_requested and entries["rb"].borrows_from is None
    assert not led.request(["gpu"], task("late")).held
    assert led.release("ra") == "release_requested"  # ra still has its borrower


def repaired(tmp_path, specs):
    led = ResourceLedger(tmp_path / "state")
    led.request(["x"], task("seed"))
    led.path.write_text("garbage")
    return led, led.complete_repair(led.begin_repair(), specs)


def test_a_borrower_spanning_overlapping_and_uncovered_names_keeps_every_name_blocked(tmp_path) -> None:
    led, result = repaired(tmp_path, [
        {"reservation_id": "ra", "names": ["lab", "sim"], "owner": task("a")},
        {"reservation_id": "rb", "names": ["gpu", "lab"], "owner": task("b")},  # overlaps ra: not restored
        {"reservation_id": "rc", "names": ["gpu", "lab"], "owner": task("c"), "borrows_from": "rb"},
    ])
    entries = {e.reservation_id: e for e in led.entries()}
    assert entries["rc"].names == ("gpu", "lab")  # not narrowed to the names ra did not win
    assert led.release("ra") == "released"  # the winning root settles
    late = [led.request(["lab"], task("late-lab")), led.request(["gpu"], task("late-gpu"))]
    assert not any(e.held for e in late)  # the overlap name stays blocked while rc lives
    for e in late:
        led.release(e.reservation_id)
    assert led.release("rc") == "released"
    assert led.request(["lab", "gpu"], task("after")).held  # and is freed once the borrower settles


def test_a_borrower_covered_only_by_several_roots_together_is_not_dropped(tmp_path) -> None:
    led, result = repaired(tmp_path, [
        {"reservation_id": "ra", "names": ["lab"], "owner": task("a")},
        {"reservation_id": "rb", "names": ["sim"], "owner": task("b")},
        {"reservation_id": "rl", "names": ["lab", "sim"], "owner": task("l")},  # overlaps both: not restored
        {"reservation_id": "rc", "names": ["lab", "sim"], "owner": task("c"), "borrows_from": "rl"},
    ])
    assert "rc" in result.restored
    assert led.release("ra") == "released" and led.release("rb") == "released"
    late = [led.request(["lab"], task("late-lab")), led.request(["sim"], task("late-sim"))]
    assert not any(e.held for e in late)
    for e in late:
        led.release(e.reservation_id)
    assert led.release("rc") == "released"
    assert led.request(["lab", "sim"], task("after")).held
