"""Task resources: waiting in the launch receipt, grants, release, scopes and recovery (supervised, fake claude)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from harflow_mcp import resources, store, task_resources, task_supervision
from harflow_mcp.backends import get
from harflow_mcp.tasks import TaskRegistry

from restart_test_support import install_fake_claude


def tasks_dir(root: Path) -> Path:
    return root / "state" / "tasks"


async def wait_for(predicate, timeout: float = 20.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            value = predicate()
            if value:
                return value
        except store.TaskDocumentError:
            pass
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("timed out waiting for supervised task state")
        await asyncio.sleep(0.05)


def registry(root: Path) -> TaskRegistry:
    return TaskRegistry(log_dir=tasks_dir(root), execution_mode="supervised")


def launch_state(root: Path, task_id: str) -> str | None:
    receipt = task_supervision.read_launch(tasks_dir(root), task_id)
    return receipt["state"] if receipt else None


@pytest.mark.asyncio
async def test_second_task_waits_then_spawns_after_first_releases(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    first = await reg.start("slow 2 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    await wait_for(lambda: store.read(log_dir, first.task_id) is not None)
    second = await reg.start("reply two", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    assert isinstance(second, task_resources.PendingLaunch)
    brief = second.brief()
    assert brief["status"] == "running" and brief["launch_state"] == "waiting_for_resources"
    assert store.read(log_dir, second.task_id) is None
    await wait_for(lambda: store.read(log_dir, second.task_id) is not None and launch_state(tmp_path, second.task_id) in {"spawned", "settled"})
    record = store.read(log_dir, second.task_id)
    assert record.resources["names"] == ["sim"]


# ---- helpers -----------------------------------------------------------------------------------


def write_receipt(root: Path, task_id: str, state: str, *, owner: dict | None = None, resources: dict | None = None, **extra) -> dict:
    """A hand-written launch receipt, for states a live owner passes through too quickly to observe."""
    log_dir = tasks_dir(root)
    log_dir.mkdir(parents=True, exist_ok=True)
    info = log_dir.stat()
    document = {
        "format": task_supervision.FORMAT, "product": "harflow", "product_version": task_supervision.PRODUCT_VERSION, "v": 1,
        "task_id": task_id, "generation": "c" * 32, "root": str(log_dir.parent), "directory_identity": [info.st_dev, info.st_ino],
        "state": state, "request_sha256": "d" * 64, "configuration_sha256": "e" * 64, "backend": "claude",
        "session_id": f"session-{task_id[:4]}", "parent_task_id": None, "chain_root_task_id": None, "owner": owner,
        "repo_path": str(root), "freedom": "read_only",
        **({"resources": resources} if resources is not None else {}), **extra,
    }
    task_supervision._write(log_dir / f"{task_id}.launch.json", document)
    return document


OWNER = {"pid": 424242, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}


@pytest.fixture
def owned(tmp_path):
    """An active state session over an empty marked root, for tests that call engine modules directly."""
    from harflow_mcp.state_layout import resolve_layout
    from harflow_mcp.state_ownership import state_session
    layout = resolve_layout(root=tmp_path / "state")
    layout.root.mkdir()
    with state_session(layout):
        layout.tasks_directory.mkdir()
        yield layout


def ledger(root: Path) -> resources.ResourceLedger:
    return resources.ResourceLedger(root / "state")


def held_names(root: Path) -> list[str]:
    return sorted(name for entry in ledger(root).entries() if entry.held for name in entry.names)


def use_server(monkeypatch, reg):
    from harflow_mcp import server
    monkeypatch.setattr(server, "_registry", reg)
    return server


# ---- waiting, grant order, release --------------------------------------------------------------


@pytest.mark.asyncio
async def test_grants_are_first_come_first_served_and_release_on_exit(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    first = await reg.start("slow 2 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    second = await reg.start("reply two", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    third = await reg.start("reply three", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="c" * 32)
    assert isinstance(second, task_resources.PendingLaunch) and isinstance(third, task_resources.PendingLaunch)
    view = task_resources.pending_view(log_dir, third.task_id)
    assert view["resources"]["position"] == 2
    assert view["resources"]["blocked_by"][0]["holder_task_ids"] == [first.task_id]
    await wait_for(lambda: store.read(log_dir, third.task_id) is not None and store.read(log_dir, third.task_id).finished_at)
    assert store.read(log_dir, second.task_id).started_at < store.read(log_dir, third.task_id).started_at
    records = [store.read(log_dir, task_id) for task_id in (first.task_id, second.task_id, third.task_id)]
    assert all(record.resources["state"] == "released" and record.resources["released_at"] for record in records)
    assert ledger(tmp_path).entries() == []


@pytest.mark.asyncio
async def test_independent_names_do_not_wait(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    await reg.start("slow 3 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["a-device"], task_id="a" * 32)
    other = await reg.start("slow 3 two", tmp_path, backend=get("claude"), freedom="read_only", resources=["b-device"], task_id="b" * 32)
    await wait_for(lambda: store.read(log_dir, other.task_id) is not None)
    assert sorted(held_names(tmp_path)) == ["a-device", "b-device"]


@pytest.mark.asyncio
async def test_wait_expiry_never_starts_and_frees_the_queue(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    holder = await reg.start("slow 4 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    waiter = await reg.start("reply two", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], resource_wait_seconds=1, task_id="b" * 32)
    await wait_for(lambda: launch_state(tmp_path, waiter.task_id) == "not_started")
    receipt = task_supervision.read_launch(log_dir, waiter.task_id)
    assert receipt["not_started_reason"] == "resource_wait_expired"
    assert store.read(log_dir, waiter.task_id) is None
    view = task_resources.pending_view(log_dir, waiter.task_id)
    assert view["status"] == "failed" and view["not_started"] is True
    assert [entry.owner.task_id for entry in ledger(tmp_path).entries()] == [holder.task_id]


@pytest.mark.asyncio
async def test_zero_wait_is_grant_now_or_fail(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    reg = registry(tmp_path)
    await reg.start("slow 3 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    with pytest.raises(ValueError, match="grants now or fails") as refused:
        await reg.start("reply two", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], resource_wait_seconds=0, task_id="b" * 32)
    assert refused.value.harflow_not_started is True
    assert task_supervision.read_launch(tasks_dir(tmp_path), "b" * 32)["not_started_reason"] == "resource_wait_expired"


@pytest.mark.asyncio
async def test_queued_cancel_through_mcp_stops_the_launch(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    server = use_server(monkeypatch, reg)
    await reg.start("slow 6 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    waiter = await reg.start("reply two", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    response = await server.cancel_task(waiter.task_id)
    assert response["cancel_requested"] is True
    await wait_for(lambda: launch_state(tmp_path, waiter.task_id) == "not_started")
    status = await server.get_task_status(waiter.task_id)
    assert status["status"] == "cancelled" and status["not_started_reason"] == "cancelled"
    assert not task_resources.cancel_path(log_dir, waiter.task_id).exists()
    assert len(ledger(tmp_path).entries()) == 1


@pytest.mark.asyncio
async def test_failure_after_grant_releases_and_marks_not_started(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    repo = tmp_path / "gone"
    repo.mkdir()
    reg = registry(tmp_path)
    await reg.start("slow 3 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    waiter = await reg.start("reply two", repo, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    repo.rmdir()  # what the pre-spawn qualification needs is gone by the time the grant lands
    await wait_for(lambda: launch_state(tmp_path, waiter.task_id) in {"not_started", "settled", "spawned"}, 30)
    assert launch_state(tmp_path, waiter.task_id) == "not_started"
    assert store.read(tasks_dir(tmp_path), waiter.task_id) is None
    assert all(entry.owner.task_id != waiter.task_id for entry in ledger(tmp_path).entries())


@pytest.mark.asyncio
async def test_pending_task_is_visible_from_a_second_server(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    first, second = registry(tmp_path), registry(tmp_path)
    await first.start("slow 5 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    waiter = await first.start("reply two", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    server = use_server(monkeypatch, second)
    identities.alive(task_supervision.read_launch(tasks_dir(tmp_path), waiter.task_id)["owner"]["pid"])
    status = await server.get_task_status(waiter.task_id)
    assert status["launch_state"] == "waiting_for_resources" and status["status"] == "running"
    assert status["resources"]["names"] == ["sim"] and status["resources"]["state"] == "waiting"
    waited = await server.wait_for_task(waiter.task_id, timeout_seconds=1)
    assert waited["status"] == "running" and waited["timed_out"] is True and "next_step" in waited
    listing = await server.list_tasks()
    assert waiter.task_id in {entry["task_id"] for entry in listing if entry.get("pending_launch")}
    assert [entry["task_id"] for entry in await server.list_tasks(waiting_for_resource="sim")] == [waiter.task_id]
    assert await server.list_tasks(waiting_for_resource="other") == []
    many = await server.wait_for_tasks([waiter.task_id], timeout_seconds=0)
    assert many["tasks"][0]["status"] == "running" and many["tasks"][0]["status_source"] == "pending_launch"


@pytest.mark.asyncio
async def test_in_process_registry_refuses_resources(tmp_path, monkeypatch):
    install_fake_claude(tmp_path, monkeypatch)
    reg = TaskRegistry(log_dir=tasks_dir(tmp_path))
    with pytest.raises(ValueError, match="supervised") as refused:
        await reg.start("reply x", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"])
    assert refused.value.harflow_not_started is True
    assert not ledger(tmp_path).directory.exists()


@pytest.mark.asyncio
async def test_invalid_resource_requests_are_refused_before_launch(tmp_path, monkeypatch):
    install_fake_claude(tmp_path, monkeypatch)
    reg = registry(tmp_path)
    server = use_server(monkeypatch, reg)
    from mcp import MCPError
    for kwargs in ({"resources": ["Bad Name"]}, {"resources": []}, {"resources": ["a", "a"]}, {"resources": ["a"], "resource_wait_seconds": 21601},
                   {"resource_wait_seconds": 5}):
        with pytest.raises(MCPError):
            await server.start_task("reply x", str(tmp_path), backend="claude", freedom="read_only", **kwargs)
    with pytest.raises(MCPError):
        await server.start_task("reply x", str(tmp_path), workflow="anything", resources=["a"])
    assert list(tasks_dir(tmp_path).glob("*.launch.json")) == []


# ---- nested dispatch: borrow or refuse ----------------------------------------------------------


class FakeCaller:
    method = "test"

    def __init__(self, record):
        self.record = record


@pytest.mark.asyncio
async def test_nested_child_borrows_and_uncovered_names_are_refused(tmp_path, monkeypatch, identities):
    from harflow_mcp import lineage
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    parent = await reg.start("slow 6 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim", "lab"], task_id="a" * 32)
    await wait_for(lambda: store.read(log_dir, parent.task_id) is not None and store.read(log_dir, parent.task_id).resources)
    record = store.read(log_dir, parent.task_id)
    caller = lineage.Caller(record, "session")
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(caller))
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: caller)
    child = await reg.start("reply child", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    await wait_for(lambda: store.read(log_dir, child.task_id) is not None and store.read(log_dir, child.task_id).resources)
    assert store.read(log_dir, child.task_id).resources["borrowed_from"] == record.resources["reservation_id"]
    with pytest.raises(resources.ResourceNotDeclaredByScope) as refused:
        await reg.start("reply other", tmp_path, backend=get("claude"), freedom="read_only", resources=["elsewhere"], task_id="c" * 32)
    assert "outermost" in str(refused.value)
    assert task_supervision.read_launch(log_dir, "c" * 32) is None
    # The parent's release waits for its borrower, then everything goes.
    await wait_for(lambda: ledger(tmp_path).entries() == [], 40)


def test_workflow_attempt_callers_are_a_scope_that_refuses_everything(tmp_path):
    log_dir = tasks_dir(tmp_path)
    (log_dir.parent / "workflow-owners").mkdir(parents=True)
    (log_dir.parent / "workflow-owners" / f"{'a' * 32}.json").write_text("{}")
    caller = FakeCaller(store.TaskRecord(task_id="a" * 32, backend="claude", session_id=None, repo_path="/x", started_at="2026-01-01T00:00:00+00:00"))
    scope = task_resources.holding_scope(log_dir, caller)
    assert scope == resources.Scope(None)
    with pytest.raises(resources.ResourceNotDeclaredByScope):
        task_resources.build_request(log_dir, ["sim"], None, caller)
    free = FakeCaller(store.TaskRecord(task_id="b" * 32, backend="claude", session_id=None, repo_path="/x", started_at="2026-01-01T00:00:00+00:00"))
    assert task_resources.holding_scope(log_dir, free) is None
    assert task_resources.holding_scope(log_dir, None) is None


# ---- the resolver and recovery ------------------------------------------------------------------


def seed_entry(root: Path, task_id: str, names=("sim",)) -> str:
    entry = ledger(root).request(list(names), {"kind": "task", "task_id": task_id})
    assert entry.held
    return entry.reservation_id


def test_resolver_keeps_every_uncertain_state_and_frees_provable_ones(tmp_path, identities, owned):
    log_dir = tasks_dir(tmp_path)
    cases = {
        "1" * 32: ("waiting_for_resources", OWNER, "dead", True),
        "2" * 32: ("waiting_for_resources", OWNER, "alive", False),
        "3" * 32: ("waiting_for_resources", OWNER, "undecidable", False),
        "4" * 32: ("granted", OWNER, "dead", False),
        "5" * 32: ("granted", OWNER, "alive", False),
        "6" * 32: ("not_started", OWNER, "alive", True),
        "7" * 32: ("claimed", OWNER, "dead", False),
    }
    for index, (task_id, (state, owner, verdict, frees)) in enumerate(cases.items()):
        owner = {**owner, "pid": 1000 + index}
        getattr(identities, {"dead": "dead", "alive": "alive"}.get(verdict, "alive"))(owner["pid"]) if verdict != "undecidable" else None
        write_receipt(tmp_path, task_id, state, owner=owner, resources={"names": ["sim"], "state": "waiting"})
        seed_entry(tmp_path, task_id, (f"name{index}",))
    resolve = task_resources.task_resolver(log_dir)
    for index, (task_id, (state, owner, verdict, frees)) in enumerate(cases.items()):
        entry = next(e for e in ledger(tmp_path).entries() if e.owner.task_id == task_id)
        assert (resolve(entry) != resources.KEEP) is frees, (state, verdict)
    # No evidence at all is never a reason to free.
    seed_entry(tmp_path, "8" * 32, ("orphan",))
    assert resolve(next(e for e in ledger(tmp_path).entries() if e.owner.task_id == "8" * 32)) == resources.KEEP


@pytest.mark.asyncio
async def test_owner_killed_while_waiting_never_started_and_session_is_freed(tmp_path, monkeypatch, identities):
    import os
    import signal
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    await reg.start("slow 8 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    waiter = await reg.start("reply two", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    receipt = task_supervision.read_launch(log_dir, waiter.task_id)
    assert waiter.task_id in {r["task_id"] for r in task_supervision.session_reservations(log_dir)}
    os.kill(receipt["owner"]["pid"], signal.SIGKILL)
    identities.dead(receipt["owner"]["pid"])
    await wait_for(lambda: waiter.task_id not in {r["task_id"] for r in task_supervision.session_reservations(log_dir)})
    view = task_resources.pending_view(log_dir, waiter.task_id)
    assert view["status"] == "failed" and view["not_started_reason"] == "owner_died"
    report = task_resources.reap(log_dir)
    assert all(entry.owner.task_id != waiter.task_id for entry in ledger(tmp_path).entries())
    assert report.errors == {}


@pytest.mark.asyncio
async def test_owner_killed_after_spawn_keeps_the_hold_until_the_agent_is_dead(tmp_path, monkeypatch, identities):
    import os
    import signal
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    task = await reg.start("slow 30 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    await wait_for(lambda: store.read(log_dir, task.task_id) is not None and store.read(log_dir, task.task_id).pid)
    record = store.read(log_dir, task.task_id)
    os.kill(record.owner["pid"], signal.SIGKILL)
    identities.dead(record.owner["pid"])
    identities.alive(record.pid)
    resolve = task_resources.task_resolver(log_dir)
    entry = ledger(tmp_path).entries()[0]
    assert resolve(entry) == resources.KEEP
    assert task_resources.reap(log_dir).removed == []
    os.kill(record.pid, signal.SIGKILL)
    identities.dead(record.pid)
    await wait_for(lambda: task_resources.reap(log_dir).removed != [] or ledger(tmp_path).entries() == [])
    assert ledger(tmp_path).entries() == []


def test_granted_launch_without_a_record_is_kept_until_a_person_releases_it(tmp_path, monkeypatch, identities, capsys):
    from harflow_mcp import ctl
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "state"))
    assert ctl.main(["resources", "--json"]) == 0
    capsys.readouterr()
    identities.dead(OWNER["pid"])
    task_id = "9" * 32
    write_receipt(tmp_path, task_id, "granted", owner=OWNER, resources={"names": ["sim"], "state": "granted"})
    rid = seed_entry(tmp_path, task_id)
    assert task_resources.reap(tasks_dir(tmp_path)).removed == []
    assert task_id in {r["task_id"] for r in task_supervision.session_reservations(tasks_dir(tmp_path))}
    assert ctl.main(["resources-release", "--reservation-id", rid, "--confirm-uncertain", "--json"]) == 0
    capsys.readouterr()
    assert ledger(tmp_path).entries() == []
    assert launch_state(tmp_path, task_id) == "granted"  # the entry alone was released
    assert task_id in {r["task_id"] for r in task_supervision.session_reservations(tasks_dir(tmp_path))}
    rid = seed_entry(tmp_path, task_id)
    assert ctl.main(["resources-release", "--reservation-id", rid, "--confirm-uncertain", "--also-release-session", "--json"]) == 0
    import json
    result = json.loads(capsys.readouterr().out.splitlines()[-1])["result"]
    assert result["session_released"] is True
    assert launch_state(tmp_path, task_id) == "not_started"
    assert task_id not in {r["task_id"] for r in task_supervision.session_reservations(tasks_dir(tmp_path))}


@pytest.mark.asyncio
async def test_repair_restores_provable_holders(tmp_path, monkeypatch, identities, capsys):
    import json
    from harflow_mcp import ctl
    install_fake_claude(tmp_path, monkeypatch)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "state"))
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    task = await reg.start("slow 6 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    await wait_for(lambda: store.read(log_dir, task.task_id) is not None and store.read(log_dir, task.task_id).resources)
    record = store.read(log_dir, task.task_id)
    identities.alive(record.pid)
    identities.alive(record.owner["pid"])
    write_receipt(tmp_path, "8" * 32, "granted", owner=OWNER, resources={"names": ["device"], "state": "granted", "reservation_id": "f" * 32})
    ledger(tmp_path).path.write_text("garbage")
    assert await asyncio.to_thread(ctl.main, ["resources-repair", "--json"]) == 0
    result = json.loads(capsys.readouterr().out.splitlines()[-1])["result"]
    assert result["rebuilt"] is True
    assert sorted(result["restored"]) == sorted([record.resources["reservation_id"], "f" * 32])
    assert held_names(tmp_path) == ["device", "sim"]


# ---- resume, follow-ups and continuation --------------------------------------------------------


async def finished_task(reg, root: Path, task_id: str, **kwargs):
    log_dir = tasks_dir(root)
    task = await reg.start("reply done", root, backend=get("claude"), freedom="read_only", max_turns=3, task_id=task_id, **kwargs)
    await wait_for(lambda: (record := store.read(log_dir, task_id)) is not None and record.finished_at and record.output_drained)
    await wait_for(lambda: launch_state(root, task_id) in {"settled", "spawned"})
    return store.read(log_dir, task_id)


@pytest.mark.asyncio
async def test_resume_requeues_and_queued_followups_return_to_the_queue_when_the_wait_expires(tmp_path, monkeypatch, identities):
    from harflow_mcp import followups
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    source = await finished_task(reg, tmp_path, "a" * 32, resources=["sim"], resource_wait_seconds=1)
    followups.send(log_dir, source.task_id, "remember this", by=None)
    await reg.start("slow 8 hold", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)

    pending = await reg.resume_record(store.read(log_dir, source.task_id), None, consume_followups=True)
    assert isinstance(pending, task_resources.PendingLaunch)
    assert pending.brief()["resources"]["names"] == ["sim"]
    items = followups.view(log_dir, source.task_id)["items"]
    assert [item["status"] for item in items] == ["reserved"] and items[0]["successor_task_id"] == pending.task_id
    await wait_for(lambda: launch_state(tmp_path, pending.task_id) == "not_started")
    await wait_for(lambda: [i["status"] for i in followups.view(log_dir, source.task_id)["items"]] == ["queued"])
    assert store.read(log_dir, pending.task_id) is None


@pytest.mark.asyncio
async def test_followups_are_consumed_by_the_owner_at_spawn(tmp_path, monkeypatch, identities):
    from harflow_mcp import followups
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    source = await finished_task(reg, tmp_path, "a" * 32, resources=["sim"])
    followups.send(log_dir, source.task_id, "remember this", by=None)
    await reg.start("slow 2 hold", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    pending = await reg.resume_record(store.read(log_dir, source.task_id), None, consume_followups=True)
    assert isinstance(pending, task_resources.PendingLaunch)
    await wait_for(lambda: store.read(log_dir, pending.task_id) is not None)
    await wait_for(lambda: [i["status"] for i in followups.view(log_dir, source.task_id)["items"]] == ["consumed"])


def test_granted_launch_with_a_dead_owner_and_no_record_is_delivery_unknown(tmp_path, identities, owned):
    from harflow_mcp import followups
    log_dir = tasks_dir(tmp_path)
    identities.dead(OWNER["pid"])
    write_receipt(tmp_path, "b" * 32, "granted", owner=OWNER, resources={"names": ["sim"], "state": "granted"})
    item = {"status": "reserved", "successor_task_id": "b" * 32, "owner": OWNER}
    assert followups._reserved_verdict(log_dir, item) == followups.DELIVERY_UNKNOWN
    write_receipt(tmp_path, "b" * 32, "waiting_for_resources", owner=OWNER, resources={"names": ["sim"], "state": "waiting"})
    assert followups._reserved_verdict(log_dir, item) == followups.RELEASED
    identities.alive(OWNER["pid"])
    assert followups._reserved_verdict(log_dir, item) is None
    write_receipt(tmp_path, "b" * 32, "not_started", owner=OWNER, resources={"names": ["sim"], "state": "waiting"})
    assert followups._reserved_verdict(log_dir, item) == followups.RELEASED


@pytest.mark.asyncio
async def test_explicit_continuation_while_blocked_is_accepted_not_started(tmp_path, monkeypatch, identities):
    import json
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    source = await finished_task(reg, tmp_path, "a" * 32, resources=["sim"])
    await reg.start("slow 4 hold", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    pending = await reg.continue_task(source.task_id, "carry on")
    assert isinstance(pending, task_resources.PendingLaunch)
    reservation = json.loads((log_dir / f"{source.task_id}.continuation.json").read_text())
    assert reservation["state"] == "accepted" and reservation["task_id"] == pending.task_id
    again = await reg.continue_task(source.task_id, "carry on")
    assert isinstance(again, task_resources.PendingLaunch) and again.task_id == pending.task_id
    await wait_for(lambda: store.read(log_dir, pending.task_id) is not None)
    child = store.read(log_dir, pending.task_id)
    assert child.continuation_of_task_id == source.task_id and child.resources["names"] == ["sim"]


def test_continuation_stays_under_its_lender_while_it_holds(tmp_path):
    log_dir = tasks_dir(tmp_path)
    held = ledger(tmp_path).request(["sim", "lab"], {"kind": "task", "task_id": "p" * 32})
    inherited = {"names": ["sim"], "wait_seconds": 5, "borrowed_from": held.reservation_id, "state": "released"}
    request = task_resources.inherited_request(log_dir, inherited, None)
    assert request["scope"] == {"reservation_id": held.reservation_id} and request["wait_seconds"] == 5
    ledger(tmp_path).release(held.reservation_id)
    assert task_resources.inherited_request(log_dir, inherited, None)["scope"] is None
    assert task_resources.inherited_request(log_dir, None, None) is None


# ---- ctl ---------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ctl_reports_a_pending_id_with_its_own_code_and_cancels_it(tmp_path, monkeypatch, identities, capsys):
    import json
    from harflow_mcp import ctl
    install_fake_claude(tmp_path, monkeypatch)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "state"))
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    await reg.start("slow 8 hold", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    waiter = await reg.start("reply two", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)

    def run(*argv):
        code = ctl.main(list(argv))
        lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
        return code, json.loads(lines[-1]) if lines else {}

    code, document = await asyncio.to_thread(run, "status", waiter.task_id, "--json")
    assert code == 1 and document["error"]["code"] == "task_pending_launch"
    assert document["error"]["data"]["launch_state"] == "waiting_for_resources"
    code, document = await asyncio.to_thread(run, "status-batch", "--task-ids", f"{waiter.task_id},{'e' * 32}", "--json")
    assert code == 0 and document["tasks"] == []
    assert document["errors"][waiter.task_id]["code"] == "task_pending_launch"
    assert document["errors"][waiter.task_id]["data"]["launch_state"] == "waiting_for_resources"
    assert document["errors"]["e" * 32]["code"] == "unknown_task"
    code, document = await asyncio.to_thread(run, "cancel", waiter.task_id, "--json")
    assert code == 0 and document["result"]["cascade"]["pending_cancel_requested"] == [waiter.task_id]
    await wait_for(lambda: launch_state(tmp_path, waiter.task_id) == "not_started")
    code, document = await asyncio.to_thread(run, "resources", "--json")
    assert [row["name"] for row in document["resources"]] == ["sim"]
    assert [row["owner"]["task_id"] for row in document["resources"][0]["holders"]] == ["a" * 32]
    assert document["resources"][0]["queue"] == []


def test_ctl_run_accepts_resource_flags(tmp_path):
    from harflow_mcp import ctl
    parser = ctl._build_parser()[0]
    args = parser.parse_args(["run", "--backend", "claude", "--repo", ".", "--prompt", "x", "--resource", "a", "--resource", "b", "--resource-wait-seconds", "9"])
    assert args.resource == ["a", "b"] and args.resource_wait_seconds == 9


@pytest.mark.asyncio
async def test_automatic_continuation_requeues_and_is_acknowledged_while_blocked(tmp_path, monkeypatch, identities):
    import json
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    first = await reg.start("cap", tmp_path, backend=get("claude"), freedom="read_only", auto_continue=1, resources=["sim"], task_id="a" * 32)
    # Queued behind the first run, so it takes the names the instant the first releases them and the
    # automatic successor has to wait its turn.
    competitor = await reg.start("slow 3 mine", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    reservation_path = log_dir / f"{first.task_id}.continuation.json"
    await wait_for(lambda: reservation_path.exists() and json.loads(reservation_path.read_text()).get("state") in {"accepted", "started"}, 60)
    reservation = json.loads(reservation_path.read_text())
    successor = reservation["task_id"]
    if reservation["state"] == "accepted":
        assert launch_state(tmp_path, successor) in {"waiting_for_resources", "granted", "spawned", "settled"}
    await wait_for(lambda: store.read(log_dir, successor) is not None, 60)
    # read_task_record retries a metadata replacement observed mid-read, which the successor's first writes cause.
    assert task_supervision.read_task_record(log_dir, successor).continuation_of_task_id == first.task_id
    assert task_supervision.read_task_record(log_dir, successor).resources["names"] == ["sim"]
    assert competitor.task_id != successor


# ---- authorization of pending cancels ------------------------------------------------------------


async def pending_pair(tmp_path, monkeypatch):
    install_fake_claude(tmp_path, monkeypatch)
    reg = registry(tmp_path)
    await reg.start("slow 8 hold", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    waiter = await reg.start("reply two", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    return reg, waiter


def caller_for(record_id: str):
    from harflow_mcp import lineage
    record = store.TaskRecord(task_id=record_id, backend="claude", session_id=None, repo_path="/x", started_at="2026-01-01T00:00:00+00:00")
    return lineage.Caller(record, "session")


@pytest.mark.asyncio
async def test_managed_workflow_worker_cannot_cancel_a_pending_launch(tmp_path, monkeypatch, identities):
    from harflow_mcp import lineage, workflow_hooks
    from harflow_mcp.backends.base import NestedDispatchRefused
    from mcp import MCPError
    reg, waiter = await pending_pair(tmp_path, monkeypatch)
    worker = caller_for("c" * 32)
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(worker))
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: worker)
    monkeypatch.setattr(workflow_hooks, "owner", lambda log_dir, task_id, strict=False: {"role": "node"} if task_id == worker.record.task_id else None)
    server = use_server(monkeypatch, reg)
    with pytest.raises((NestedDispatchRefused, MCPError)):
        await server.cancel_task(waiter.task_id)
    assert not task_resources.cancel_path(tasks_dir(tmp_path), waiter.task_id).exists()
    with pytest.raises(NestedDispatchRefused):
        await reg.cancel_pending(waiter.task_id)
    # And through ctl.
    from harflow_mcp import ctl
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "state"))
    code = await asyncio.to_thread(ctl.main, ["cancel", waiter.task_id, "--json"])
    assert code != 0
    assert not task_resources.cancel_path(tasks_dir(tmp_path), waiter.task_id).exists()


@pytest.mark.asyncio
async def test_undecidable_and_unverifiable_callers_cannot_cancel_a_pending_launch(tmp_path, monkeypatch, identities):
    from harflow_mcp import ctl, lineage
    from harflow_mcp.backends.base import NestedDispatchRefused
    reg, waiter = await pending_pair(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None, "ps is not readable"))
    with pytest.raises(NestedDispatchRefused):
        await reg.cancel_pending(waiter.task_id)
    assert not task_resources.cancel_path(log_dir, waiter.task_id).exists()
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "state"))
    assert await asyncio.to_thread(ctl.main, ["cancel", waiter.task_id, "--json"]) != 0
    assert not task_resources.cancel_path(log_dir, waiter.task_id).exists()
    # A worker variable that no live caller confirms is refused as well.
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None))
    monkeypatch.setenv("HARFLOW_TASK_ID", "d" * 32)
    with pytest.raises(ValueError):
        await reg.cancel_pending(waiter.task_id)
    assert await asyncio.to_thread(ctl.main, ["cancel", waiter.task_id, "--json"]) != 0
    assert not task_resources.cancel_path(log_dir, waiter.task_id).exists()
    with pytest.raises(PermissionError):
        task_resources.cancel_launch(log_dir, waiter.task_id)


@pytest.mark.asyncio
async def test_root_caller_cancels_a_pending_launch_and_its_pending_descendants(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    await reg.start("slow 8 hold", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    parent = await reg.start("reply parent", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    child = await reg.start("reply child", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="c" * 32)
    # Make the second launch a descendant of the first by lineage, as a nested dispatch would record.
    receipt = task_supervision.read_launch(log_dir, child.task_id)
    task_supervision.mark(log_dir, child.task_id, receipt["generation"], spawned_by=parent.task_id, root_task_id=parent.task_id)
    result = await reg.cancel_pending(parent.task_id)
    assert sorted(result["cascade"]["pending_cancel_requested"]) == [parent.task_id, child.task_id]
    await wait_for(lambda: launch_state(tmp_path, parent.task_id) == "not_started" and launch_state(tmp_path, child.task_id) == "not_started")
    assert await reg.cancel_pending("e" * 32) is None
