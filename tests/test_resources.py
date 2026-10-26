"""The persisted resource reservation ledger (`resources.py`) and its ctl commands."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from harflow_mcp import ctl, lineage, resources
from harflow_mcp.resources import Entry, Owner, ResourceLedger, Scope

DRIVER = Path(__file__).with_name("resources_driver.py")


def task(name: str) -> dict:
    return {"kind": "task", "task_id": name}


@pytest.fixture
def ledger(tmp_path: Path) -> ResourceLedger:
    return ResourceLedger(tmp_path / "state")


def held_ids(ledger: ResourceLedger, name: str) -> list[str]:
    resource = next(item for item in ledger.snapshot() if item["name"] == name)
    return [row["reservation_id"] for row in resource["holders"]]


def queued_ids(ledger: ResourceLedger, name: str) -> list[str]:
    resource = next(item for item in ledger.snapshot() if item["name"] == name)
    return [row["reservation_id"] for row in resource["queue"]]


# ---- grant, queue, release ---------------------------------------------------------------------


def test_grant_queue_release(ledger: ResourceLedger) -> None:
    first = ledger.request(["repo:main"], task("a"))
    second = ledger.request(["repo:main"], task("b"))
    assert first.held and first.granted_at is not None
    assert not second.held
    assert ledger.poll(second.reservation_id).state == "queued"

    assert ledger.release(first.reservation_id) == "released"
    assert ledger.get(second.reservation_id).held
    assert ledger.release(second.reservation_id) == "released"
    assert ledger.entries() == []
    assert ledger.release(second.reservation_id) == "absent"


def test_names_are_validated_sorted_and_bounded(ledger: ResourceLedger) -> None:
    entry = ledger.request(["b", "a"], task("a"))
    assert entry.names == ("a", "b")
    for bad in (["A"], ["-x"], ["has space"], ["x" * 65], [], ["a", "a"], [f"n{i}" for i in range(17)], "abc", [1]):
        with pytest.raises(resources.InvalidResourceRequest):
            ledger.request(bad, task("x"))
    ledger.request(["a" * 64, "ok.name:1_"[:-1]], task("y"))
    with pytest.raises(resources.InvalidResourceRequest):
        ledger.request(["ok"], {"kind": "robot"})


def test_fifo_with_overlapping_requests_and_no_starvation(ledger: ResourceLedger) -> None:
    holder = ledger.request(["a"], task("holder"))
    big = ledger.request(["a", "b"], task("big"))
    later_b = ledger.request(["b"], task("later-b"))  # b is free, but big queued earlier on b
    later_c = ledger.request(["c"], task("later-c"))  # disjoint: not blocked
    assert not big.held and not ledger.get(later_b.reservation_id).held
    assert ledger.get(later_c.reservation_id).held

    ledger.release(holder.reservation_id)
    assert ledger.get(big.reservation_id).held
    assert not ledger.get(later_b.reservation_id).held
    ledger.release(big.reservation_id)
    assert ledger.get(later_b.reservation_id).held


def test_never_partial(ledger: ResourceLedger) -> None:
    ledger.request(["a"], task("holder"))
    wanting_both = ledger.request(["a", "b"], task("both"))
    assert not wanting_both.held
    assert held_ids(ledger, "b") == []  # b is not held while the request waits for a
    only_b = ledger.request(["b"], task("only-b"))
    assert not only_b.held  # strict FIFO behind the waiting two-name request


def test_poll_writes_nothing_when_nothing_changed(ledger: ResourceLedger) -> None:
    ledger.request(["a"], task("holder"))
    waiting = ledger.request(["a"], task("waiting"))
    before = (ledger.path.stat().st_mtime_ns, ledger.path.read_bytes())
    for _ in range(3):
        ledger.poll(waiting.reservation_id)
    assert (ledger.path.stat().st_mtime_ns, ledger.path.read_bytes()) == before


def test_generation_bumps_on_each_change(ledger: ResourceLedger) -> None:
    holder = ledger.request(["a"], task("holder"))
    waiting = ledger.request(["a"], task("waiting"))
    assert waiting.generation == 1
    ledger.release(holder.reservation_id)
    assert ledger.get(waiting.reservation_id).generation == 2


def test_capacity_hook(tmp_path: Path) -> None:
    two = ResourceLedger(tmp_path / "state", capacity_of=lambda name: 2 if name == "pool" else 1)
    assert two.request(["pool"], task("a")).held
    assert two.request(["pool"], task("b")).held
    assert not two.request(["pool"], task("c")).held
    assert next(item for item in two.snapshot() if item["name"] == "pool")["capacity"] == 2


# ---- borrowing ---------------------------------------------------------------------------------


def test_borrow_is_immediate_and_consumes_no_capacity(ledger: ResourceLedger) -> None:
    parent = ledger.request(["a", "b"], task("parent"))
    child = ledger.request(["a"], task("child"), scope=Scope(parent.reservation_id))
    assert child.held and child.borrows_from == parent.reservation_id
    other = ledger.request(["a"], task("other"))
    assert not other.held  # the borrow took no slot, and the parent still holds it
    assert held_ids(ledger, "a") == [parent.reservation_id, child.reservation_id]


def test_nested_borrow_attaches_to_root_and_must_be_covered(ledger: ResourceLedger) -> None:
    parent = ledger.request(["a", "b"], task("parent"))
    child = ledger.request(["a"], task("child"), scope=Scope(parent.reservation_id))
    grandchild = ledger.request(["a"], task("grand"), scope=Scope(child.reservation_id))
    assert grandchild.borrows_from == parent.reservation_id
    with pytest.raises(resources.ResourceNotDeclaredByScope):
        ledger.request(["b"], task("grand2"), scope=Scope(child.reservation_id))  # child only had a


def test_parent_release_deferred_until_last_borrower(ledger: ResourceLedger) -> None:
    parent = ledger.request(["a"], task("parent"))
    waiting = ledger.request(["a"], task("waiting"))
    one = ledger.request(["a"], task("one"), scope=Scope(parent.reservation_id))
    two = ledger.request(["a"], task("two"), scope=Scope(parent.reservation_id))

    assert ledger.release(parent.reservation_id) == "release_requested"
    assert ledger.get(parent.reservation_id).release_requested
    assert not ledger.get(waiting.reservation_id).held
    with pytest.raises(resources.ResourceNotDeclaredByScope):  # a releasing parent lends no more
        ledger.request(["a"], task("late"), scope=Scope(parent.reservation_id))

    assert ledger.release(one.reservation_id) == "released"
    assert ledger.get(parent.reservation_id) is not None
    assert ledger.release(two.reservation_id) == "released"
    assert ledger.get(parent.reservation_id) is None
    assert ledger.get(waiting.reservation_id).held


def test_uncovered_name_and_empty_scope_are_refused(ledger: ResourceLedger) -> None:
    parent = ledger.request(["a"], task("parent"))
    with pytest.raises(resources.ResourceNotDeclaredByScope) as uncovered:
        ledger.request(["a", "z"], task("child"), scope=Scope(parent.reservation_id))
    assert uncovered.value.code == "resource_not_declared_by_scope"
    assert "outermost scope" in str(uncovered.value) and "z" in str(uncovered.value)

    with pytest.raises(resources.ResourceNotDeclaredByScope) as empty:
        ledger.request(["a"], task("child"), scope=Scope(None))
    assert "outermost scope" in str(empty.value)

    with pytest.raises(resources.ResourceNotDeclaredByScope):  # unknown or queued scope fails closed
        ledger.request(["a"], task("child"), scope=Scope("missing"))
    queued = ledger.request(["a"], task("queued"))
    with pytest.raises(resources.ResourceNotDeclaredByScope):
        ledger.request(["a"], task("child"), scope=Scope(queued.reservation_id))
    assert len(ledger.entries()) == 2  # nothing was left behind by the refusals


# ---- acquire -----------------------------------------------------------------------------------


def test_acquire_waits_and_is_granted_in_order(ledger: ResourceLedger) -> None:
    holder = ledger.request(["a"], task("holder"))

    async def scenario() -> Entry:
        waiter = asyncio.ensure_future(ledger.acquire(["a"], task("waiter"), poll_initial=0.01, poll_max=0.02))
        await asyncio.sleep(0.1)
        assert not waiter.done()
        ledger.release(holder.reservation_id)
        return await asyncio.wait_for(waiter, 5)

    entry = asyncio.run(scenario())
    assert entry.held and held_ids(ledger, "a") == [entry.reservation_id]


def test_acquire_deadline_dequeues(ledger: ResourceLedger) -> None:
    ledger.request(["a"], task("holder"))
    with pytest.raises(resources.ResourceWaitExpired):
        asyncio.run(ledger.acquire(["a"], task("waiter"), timeout=0.05, poll_initial=0.01, poll_max=0.01))
    assert len(ledger.entries()) == 1

    with pytest.raises(resources.ResourceWaitExpired):  # 0 means grant now or fail
        asyncio.run(ledger.acquire(["a"], task("waiter"), timeout=0))
    assert len(ledger.entries()) == 1
    assert asyncio.run(ledger.acquire(["free"], task("waiter"), timeout=0)).held


def test_acquire_should_continue_cancels_and_dequeues(ledger: ResourceLedger) -> None:
    ledger.request(["a"], task("holder"))
    calls = []

    def should_continue() -> bool:
        calls.append(1)
        return len(calls) < 3

    with pytest.raises(resources.ResourceWaitCancelled):
        asyncio.run(ledger.acquire(["a"], task("waiter"), should_continue=should_continue, poll_initial=0.01, poll_max=0.01))
    assert len(ledger.entries()) == 1 and len(calls) == 3

    async def async_check() -> bool:
        return False

    with pytest.raises(resources.ResourceWaitCancelled):
        asyncio.run(ledger.acquire(["a"], task("waiter"), should_continue=async_check))
    assert len(ledger.entries()) == 1


def test_acquire_task_cancellation_releases_the_entry(ledger: ResourceLedger) -> None:
    ledger.request(["a"], task("holder"))

    async def scenario() -> None:
        waiter = asyncio.ensure_future(ledger.acquire(["a"], task("waiter"), poll_initial=0.01, poll_max=0.01))
        await asyncio.sleep(0.1)
        assert len(ledger.entries()) == 2
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

    asyncio.run(scenario())
    assert len(ledger.entries()) == 1


def test_acquire_borrow_and_refusal(ledger: ResourceLedger) -> None:
    parent = ledger.request(["a"], task("parent"))
    entry = asyncio.run(ledger.acquire(["a"], task("child"), scope=Scope(parent.reservation_id), timeout=0))
    assert entry.borrows_from == parent.reservation_id
    with pytest.raises(resources.ResourceNotDeclaredByScope):
        asyncio.run(ledger.acquire(["b"], task("child"), scope=Scope(parent.reservation_id)))
    assert len(ledger.entries()) == 2


# ---- reaper ------------------------------------------------------------------------------------


def test_reaper_keeps_without_resolver_and_on_keep_verdicts(ledger: ResourceLedger) -> None:
    ledger.request(["a"], task("a"))
    ledger.request(["b"], {"kind": "workflow_run", "workflow_run_id": "run"})
    report = ledger.reap({"task": lambda entry: resources.KEEP})
    assert report.removed == [] and len(report.kept) == 2
    report = ledger.reap({})
    assert report.removed == [] and len(ledger.entries()) == 2
    report = ledger.reap({"task": lambda entry: "undecidable", "workflow_run": lambda entry: 1 / 0})
    assert report.removed == [] and len(report.errors) == 1 and len(ledger.entries()) == 2


def test_reaper_removes_settled_and_never_started_and_grants_next(ledger: ResourceLedger) -> None:
    gone = ledger.request(["a"], task("gone"))
    never = ledger.request(["b"], task("never"))
    alive = ledger.request(["c"], task("alive"))
    waiting = ledger.request(["a"], task("waiting"))
    verdicts = {"gone": resources.SETTLED, "never": resources.NEVER_STARTED, "alive": resources.KEEP, "waiting": resources.KEEP}
    report = ledger.reap({"task": lambda entry: verdicts[entry.owner.task_id]})
    assert sorted(report.removed) == sorted([gone.reservation_id, never.reservation_id])
    assert ledger.get(gone.reservation_id) is None and ledger.get(alive.reservation_id) is not None
    assert ledger.get(waiting.reservation_id).held


def test_reaper_defers_a_settled_parent_with_borrowers(ledger: ResourceLedger) -> None:
    parent = ledger.request(["a"], task("parent"))
    child = ledger.request(["a"], task("child"), scope=Scope(parent.reservation_id))
    settled_parent = lambda entry: resources.SETTLED if entry.owner.task_id == "parent" else resources.KEEP
    report = ledger.reap({"task": settled_parent})
    assert report.deferred == [parent.reservation_id] and report.removed == []
    stamp = ledger.path.read_bytes()
    ledger.reap({"task": settled_parent})
    assert ledger.path.read_bytes() == stamp  # nothing changed, nothing written
    ledger.reap({"task": lambda entry: resources.SETTLED if entry.owner.task_id == "child" else resources.KEEP})
    assert ledger.entries() == [] and child


def test_reaper_generation_race_between_resolve_and_apply(ledger: ResourceLedger) -> None:
    holder = ledger.request(["a"], task("holder"))
    ledger.request(["a"], task("waiting"))
    other = ledger.request(["b"], task("other"))

    def resolver(entry: Entry) -> str:
        if entry.reservation_id == holder.reservation_id:
            # The owner re-flags the entry between our read and our apply: its generation moves.
            ledger.request(["a"], task("borrower"), scope=Scope(holder.reservation_id))
            ledger.release(holder.reservation_id)
        return resources.SETTLED if entry.reservation_id in (holder.reservation_id, other.reservation_id) else resources.KEEP

    report = ledger.reap({"task": resolver})
    assert holder.reservation_id in report.stale
    assert other.reservation_id in report.removed
    assert ledger.get(holder.reservation_id) is not None  # kept: it changed after we decided


def test_flock_is_not_held_while_a_resolver_runs(ledger: ResourceLedger) -> None:
    ledger.request(["a"], task("a"))
    entered, proceed = threading.Event(), threading.Event()
    seen: list[str] = []

    def slow_resolver(entry: Entry) -> str:
        entered.set()
        assert proceed.wait(10)
        return resources.KEEP

    reaper = threading.Thread(target=lambda: ledger.reap({"task": slow_resolver}))
    reaper.start()
    try:
        assert entered.wait(10)
        started = time.monotonic()
        other = ResourceLedger(ledger.directory.parent, lock_timeout=2)
        seen.append(other.request(["b"], task("b")).state)  # would time out if the flock were held
        assert time.monotonic() - started < 1.5
    finally:
        proceed.set()
        reaper.join(10)
    assert seen == ["held"]


# ---- processes and failures --------------------------------------------------------------------


def drive(root: Path, *args: str) -> subprocess.Popen:
    env = dict(os.environ, HARFLOW_STATE_ROOT=str(root))
    return subprocess.Popen([sys.executable, str(DRIVER), *args], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def test_two_processes_lose_no_update(tmp_path: Path) -> None:
    root = tmp_path / "state"
    workers = [drive(root, "unique", "30", tag) for tag in ("p", "q", "r")]
    for worker in workers:
        _, err = worker.communicate(timeout=120)
        assert worker.returncode == 0, err
    names = {name for entry in ResourceLedger(root).entries() for name in entry.names}
    assert len(names) == 90


def test_processes_share_one_exclusive_slot(tmp_path: Path) -> None:
    root, marker = tmp_path / "state", tmp_path / "inside"
    workers = [drive(root, "exclusive", "8", str(marker)) for _ in range(3)]
    for worker in workers:
        out, err = worker.communicate(timeout=120)
        assert worker.returncode == 0, err
        assert json.loads(out) == {"violations": 0}
    assert ResourceLedger(root).entries() == []


def test_corrupt_ledger_is_refused_everywhere_never_reset(ledger: ResourceLedger) -> None:
    ledger.request(["a"], task("a"))
    ledger.path.write_text("{not json")
    garbage = ledger.path.read_bytes()
    operations = [
        lambda: ledger.request(["b"], task("b")),
        lambda: ledger.release("anything"),
        lambda: ledger.poll("anything"),
        lambda: ledger.snapshot(),
        lambda: ledger.entries(),
        lambda: ledger.reap({}),
        lambda: asyncio.run(ledger.acquire(["b"], task("b"))),
    ]
    for operation in operations:
        with pytest.raises(resources.ResourceLedgerCorrupt):
            operation()
    assert ledger.path.read_bytes() == garbage

    for broken in ('{"product": "other"}', json.dumps({**json.loads(_valid(ledger)), "v": 2}), "[]"):
        ledger.path.write_text(broken)
        with pytest.raises(resources.ResourceLedgerCorrupt):
            ledger.entries()


def _valid(ledger: ResourceLedger) -> str:
    return json.dumps(resources._empty_document())


def test_repair_moves_aside_and_starts_empty(ledger: ResourceLedger) -> None:
    with pytest.raises(resources.LedgerNotCorrupt):
        ledger.repair()
    ledger.request(["a"], task("a"))
    with pytest.raises(resources.LedgerNotCorrupt):  # a healthy ledger is never wiped
        ledger.repair()
    assert len(ledger.entries()) == 1

    ledger.path.write_text("garbage")
    moved = ledger.repair()
    assert moved.name.startswith("ledger.corrupt-") and moved.read_text() == "garbage"
    assert ledger.entries() == []
    assert ledger.request(["a"], task("a")).held


def test_reads_do_not_create_the_state_directory(tmp_path: Path) -> None:
    ledger = ResourceLedger(tmp_path / "fresh")
    assert ledger.snapshot() == [] and ledger.entries() == [] and ledger.get("x") is None
    assert not (tmp_path / "fresh").exists()


# ---- ctl ---------------------------------------------------------------------------------------


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    # The first ctl call marks the state root as a store; a ledger created before that would make
    # an unmarked root look foreign, which is not the order a real installation goes through.
    assert ctl.main(["resources", "--json"]) == 0
    capsys.readouterr()
    return tmp_path


def run_ctl(capsys: pytest.CaptureFixture[str], *argv: str) -> tuple[int, dict]:
    code = ctl.main(list(argv))
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return code, json.loads(lines[-1]) if lines else {}


def test_ctl_resources_json_shape(home: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code, document = run_ctl(capsys, "resources", "--json")
    assert code == 0 and document["resources"] == []
    assert not (home / ".harflow" / "resources").exists()  # a read creates nothing
    assert document["format"] == "harflow.ctl" and document["v"] == 1

    ledger = ResourceLedger(home / ".harflow")
    first = ledger.request(["repo:main", "sim"], task("a"))
    second = ledger.request(["sim"], task("b"))
    code, document = run_ctl(capsys, "resources", "--json")
    assert code == 0
    by_name = {item["name"]: item for item in document["resources"]}
    assert set(by_name) == {"repo:main", "sim"}
    sim = by_name["sim"]
    assert sim["capacity"] == 1
    assert [row["reservation_id"] for row in sim["holders"]] == [first.reservation_id]
    assert [row["reservation_id"] for row in sim["queue"]] == [second.reservation_id]
    assert sim["holders"][0]["owner"] == {"kind": "task", "task_id": "a"}
    assert set(sim) == {"name", "capacity", "holders", "queue"}


def test_ctl_resources_release_and_repair_for_a_human(home: Path, capsys: pytest.CaptureFixture[str]) -> None:
    ledger = ResourceLedger(home / ".harflow")
    held = ledger.request(["a"], task("a"))
    waiting = ledger.request(["a"], task("b"))
    code, document = run_ctl(capsys, "resources-release", "--reservation-id", held.reservation_id, "--confirm-uncertain", "--also-release-session", "--json")
    assert code == 0
    assert document["result"] == {"reservation_id": held.reservation_id, "outcome": "released", "also_release_session": True, "session_released": False, "session": "unknown"}
    assert ledger.get(waiting.reservation_id).held

    code, document = run_ctl(capsys, "resources-release", "--reservation-id", "nope", "--confirm-uncertain", "--json")
    assert code == 1 and document["error"]["code"] == "unknown_reservation"

    code, document = run_ctl(capsys, "resources-repair", "--json")
    assert code == 1 and document["error"]["code"] == "ledger_not_corrupt"

    ledger.path.write_text("garbage")
    code, document = run_ctl(capsys, "resources", "--json")
    assert code == 1 and document["error"]["code"] == "resource_ledger_corrupt"
    code, document = run_ctl(capsys, "resources-repair", "--json")
    assert code == 0 and document["result"]["rebuilt"] is True and document["result"]["restored"] == []
    assert Path(document["result"]["moved_to"]).read_text() == "garbage"
    code, document = run_ctl(capsys, "resources", "--json")
    assert code == 0 and document["resources"] == []


def test_ctl_release_requires_confirmation(home: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exit_info:
        ctl.main(["resources-release", "--reservation-id", "x", "--json"])
    assert exit_info.value.code == 2
    document = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert document["error"]["code"] == "usage"


def test_ctl_recovery_commands_are_humans_only(home: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    ledger = ResourceLedger(home / ".harflow")
    held = ledger.request(["a"], task("a"))
    commands = [
        ("resources-release", "--reservation-id", held.reservation_id, "--confirm-uncertain", "--json"),
        ("resources-repair", "--json"),
    ]

    class FakeCaller:
        method = "test"

        class record:
            task_id = "agent-task"

    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(FakeCaller()))
    for command in commands:
        code, document = run_ctl(capsys, *command)
        assert code == 1 and document["error"]["code"] == "agent_caller"

    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None, "ps is not readable"))
    for command in commands:
        code, document = run_ctl(capsys, *command)
        assert code == 1 and document["error"]["code"] == "caller_undecidable"

    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None))
    monkeypatch.setenv("HARFLOW_TASK_ID", "stale")
    for command in commands:
        code, document = run_ctl(capsys, *command)
        assert code == 1 and document["error"]["code"] == "agent_caller"

    assert ledger.get(held.reservation_id) is not None  # every refusal left the ledger alone
    assert json.loads(ledger.path.read_text())["entries"]
