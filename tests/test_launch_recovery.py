"""Launch races and recovery: cancellation at the spawn boundary, pre-spawn settlement, stale-evidence
reconciliation and the humans-only settlement of an uncertain granted launch."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import followups, identity, resources, store, task_resources, task_supervision, worktrees
from harflow_mcp.backends import get
from harflow_mcp.tasks import TaskRegistry

from restart_test_support import install_fake_claude
from test_task_resources import OWNER, finished_task, ledger, launch_state, owned, registry, tasks_dir, wait_for, write_receipt  # noqa: F401
from test_worktrees import commit, git, record_for, source  # noqa: F401

HOOKS = Path(__file__).parent / "owner_hooks"


def hook(monkeypatch, tmp_path: Path, point: str) -> Path:
    """Make every owner this test starts pause at `point`; returns the directory that signals it."""
    signal_dir = tmp_path / f"hook-{point}"
    signal_dir.mkdir()
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(HOOKS), os.environ.get("PYTHONPATH", "")])))
    monkeypatch.setenv("HARFLOW_TEST_HOOK", f"{point}:{signal_dir}")
    return signal_dir


def cancel_pending(tmp_path: Path, task_id: str) -> dict:
    return task_resources.cancel_launch(tasks_dir(tmp_path), task_id, authorized=True)


def spawn_log(tmp_path: Path) -> list[str]:
    path = tmp_path / "claude-spawns.log"
    return path.read_text().split() if path.exists() else []


def assert_cancelled_before_start(tmp_path: Path, task_id: str) -> dict:
    log_dir = tasks_dir(tmp_path)
    receipt = task_supervision.read_launch(log_dir, task_id)
    assert receipt["state"] == "not_started" and receipt["not_started_reason"] == "cancelled"
    assert store.read(log_dir, task_id) is None
    assert not task_resources.cancel_path(log_dir, task_id).exists()
    assert all(entry.owner.task_id != task_id for entry in ledger(tmp_path).entries())
    assert spawn_log(tmp_path) == []
    return receipt


# ---- item 18: acknowledgement means the names are held or queued ---------------------------------


@pytest.mark.asyncio
async def test_start_returns_only_once_the_ledger_has_the_launch(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    reg = registry(tmp_path)
    for index in range(4):
        task_id = f"{index:x}" * 32
        started = await reg.start("slow 1 one", tmp_path, backend=get("claude"), freedom="read_only", resources=[f"dev{index}"], task_id=task_id)
        assert any(entry.owner.task_id == started.task_id and entry.held for entry in ledger(tmp_path).entries())


# ---- item 1: a cancel is honoured at the last point before the agent starts ----------------------


@pytest.mark.asyncio
async def test_cancel_during_worktree_creation_settles_not_started(tmp_path, source, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    signal_dir = hook(monkeypatch, tmp_path, "worktree_create")
    reg = registry(tmp_path)
    started = await reg.start("reply x", source, backend=get("claude"), freedom="write_in_repo", worktree={}, resources=["sim"], task_id="a" * 32)
    await wait_for(lambda: (signal_dir / "reached").exists())
    cancel_pending(tmp_path, started.task_id)
    (signal_dir / "release").write_text("1")
    await wait_for(lambda: launch_state(tmp_path, started.task_id) == "not_started")
    receipt = assert_cancelled_before_start(tmp_path, started.task_id)
    assert receipt["worktree"]["state"] == "created"  # the checkout is kept and reported, never removed


@pytest.mark.asyncio
async def test_cancel_in_granted_during_pre_spawn_qualification_settles_not_started(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    signal_dir = hook(monkeypatch, tmp_path, "baseline")
    reg = registry(tmp_path)
    started = await reg.start("reply x", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    await wait_for(lambda: (signal_dir / "reached").exists())
    assert launch_state(tmp_path, started.task_id) == "granted"
    cancel_pending(tmp_path, started.task_id)
    (signal_dir / "release").write_text("1")
    await wait_for(lambda: launch_state(tmp_path, started.task_id) == "not_started")
    assert_cancelled_before_start(tmp_path, started.task_id)


@pytest.mark.asyncio
async def test_cancel_after_the_last_pre_spawn_look_takes_the_normal_cancel_path(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(filter(None, [str(HOOKS), os.environ.get("PYTHONPATH", "")])))
    monkeypatch.setenv("HARFLOW_TEST_FAIL", "cancel_at_persist")
    started = await reg.start("slow 30 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    await wait_for(lambda: (r := store.read(log_dir, started.task_id)) is not None and r.status in {"cancelled", "failed"} and r.finished_at, 40)
    assert store.read(log_dir, started.task_id).status == "cancelled"
    assert not task_resources.cancel_path(log_dir, started.task_id).exists()


def test_cancel_requests_are_only_written_for_launches_an_owner_can_still_read(tmp_path, identities, owned):
    log_dir = owned.tasks_directory
    for index, state in enumerate(("not_started", "settled")):
        task_id = f"{index + 1}" * 32
        write_receipt(tmp_path, task_id, state, owner=OWNER, resources={"names": ["sim"], "state": "waiting"})
        assert task_resources.cancel_launch(log_dir, task_id, authorized=True)["cascade"]["pending_cancel_requested"] == []
        assert not task_resources.cancel_path(log_dir, task_id).exists()


# ---- item 2: the whole owner preparation is before the spawn -------------------------------------


@pytest.mark.asyncio
async def test_rebuild_failure_after_creation_settles_not_started_and_keeps_the_worktree(tmp_path, source, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    # Held at creation so `start` acknowledges the grant before the owner can settle the launch.
    signal_dir = hook(monkeypatch, tmp_path, "worktree_create")
    monkeypatch.setenv("HARFLOW_TEST_FAIL", "rebuild")
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    started = await reg.start("reply x", source, backend=get("claude"), freedom="write_in_repo", worktree={}, resources=["sim"], task_id="a" * 32)
    await wait_for(lambda: (signal_dir / "reached").exists())
    (signal_dir / "release").write_text("1")
    await wait_for(lambda: launch_state(tmp_path, started.task_id) == "not_started")
    receipt = task_supervision.read_launch(log_dir, started.task_id)
    assert receipt["not_started_reason"] == "owner_error" and "rebuild failed" in receipt["error"]
    assert receipt["worktree"]["state"] == "created" and Path(receipt["worktree"]["path"]).is_dir()
    assert store.read(log_dir, started.task_id) is None
    assert all(entry.owner.task_id != started.task_id for entry in ledger(tmp_path).entries())
    assert spawn_log(tmp_path) == []


# ---- item 4: a verdict is not taken from evidence read before a slow probe -----------------------


class Race:
    """Makes the first liveness probe of OWNER slow in the way that matters: while it runs, the owner
    granted, spawned and died, leaving a record and an agent."""

    def __init__(self, monkeypatch, identities, owned_layout, task_id: str):
        self.calls = 0

        def check(ident):
            if isinstance(ident, dict) and ident.get("pid") == OWNER["pid"]:
                self.calls += 1
                if self.calls == 1:
                    store.write(owned_layout.tasks_directory, record_for(
                        task_id, "/x", status="running", exit_code=None, finished_at=None, output_drained=False,
                        pid=777, start_time="Mon Jan  1 00:00:00 2001", markers=["x"], owner=OWNER))
                    task_supervision.mark(owned_layout.tasks_directory, task_id, "c" * 32, state="spawned")
                return "dead"
            return identities(ident)

        monkeypatch.setattr(identity, "identity_check", check)


def waiting_launch(tmp_path, task_id="a" * 32, state="waiting_for_resources", **extra):
    return write_receipt(tmp_path, task_id, state, owner=OWNER, resources={"names": ["sim"], "state": "waiting", "reservation_id": "r" * 32}, **extra)


def test_resolver_reads_again_after_the_owner_probe(tmp_path, monkeypatch, identities, owned):
    log_dir = owned.tasks_directory
    waiting_launch(tmp_path)
    entry = ledger(tmp_path).request(["sim"], {"kind": "task", "task_id": "a" * 32})
    Race(monkeypatch, identities, owned, "a" * 32)
    assert task_resources.task_resolver(log_dir)(entry) == resources.KEEP


def test_session_reconciliation_reads_again_after_the_owner_probe(tmp_path, monkeypatch, identities, owned):
    waiting_launch(tmp_path)
    Race(monkeypatch, identities, owned, "a" * 32)
    assert "a" * 32 in {r["task_id"] for r in task_supervision.session_reservations(owned.tasks_directory)}


def test_followup_reconciliation_reads_again_after_the_owner_probe(tmp_path, monkeypatch, identities, owned):
    waiting_launch(tmp_path)
    Race(monkeypatch, identities, owned, "a" * 32)
    item = {"status": "reserved", "successor_task_id": "a" * 32, "owner": OWNER}
    assert followups._reserved_verdict(owned.tasks_directory, item) == followups.CONSUMED


def test_pending_cancel_does_not_settle_a_launch_that_spawned_during_the_probe(tmp_path, monkeypatch, identities, owned):
    waiting_launch(tmp_path)
    Race(monkeypatch, identities, owned, "a" * 32)
    result = task_resources.cancel_launch(owned.tasks_directory, "a" * 32, authorized=True)
    assert result["cascade"]["pending_settled"] == [] and result["spawned"] is True
    assert launch_state(tmp_path, "a" * 32) == "spawned"


# ---- item 5: the crash between the record and `spawned` ------------------------------------------


def test_granted_launch_with_a_matching_dead_record_frees_the_session(tmp_path, identities, owned):
    log_dir = owned.tasks_directory
    identities.dead(OWNER["pid"])
    identities.dead(777)
    write_receipt(tmp_path, "a" * 32, "granted", owner=OWNER, session_id="s-1", resources={"names": ["sim"], "state": "granted"})
    store.write(log_dir, store.TaskRecord(task_id="a" * 32, backend="claude", session_id="s-1", repo_path="/x", started_at="2026-01-01T00:00:00+00:00",
                                          status="running", pid=777, start_time="Mon Jan  1 00:00:00 2001", markers=["x"], owner=OWNER))
    # Observations stay read-only; explicit maintenance recovery performs the settlement.
    assert "a" * 32 in {r["task_id"] for r in task_supervision.session_reservations(log_dir)}
    from harflow_mcp.task_recovery import recover_abandoned
    assert recover_abandoned(log_dir)["recovered"][0]["outcome"] == "interrupted"
    assert "a" * 32 not in {r["task_id"] for r in task_supervision.session_reservations(log_dir)}
    identities.alive(777)  # the pid now names another process; the recovered receipt stays settled.
    assert "a" * 32 not in {r["task_id"] for r in task_supervision.session_reservations(log_dir)}
    identities.dead(777)
    write_receipt(tmp_path, "a" * 32, "granted", owner=OWNER, session_id="other", resources={"names": ["sim"], "state": "granted"})
    assert "a" * 32 in {r["task_id"] for r in task_supervision.session_reservations(log_dir)}  # identity must match


# ---- item 13: waiting on a launch whose owner is gone --------------------------------------------


@pytest.mark.asyncio
async def test_waiting_on_a_granted_launch_with_a_dead_owner_and_no_record_resolves(tmp_path, identities, owned):
    identities.dead(OWNER["pid"])
    receipt = write_receipt(tmp_path, "a" * 32, "granted", owner=OWNER, resources={"names": ["sim"], "state": "granted"})
    handle = task_resources.PendingLaunch(owned.tasks_directory, receipt)
    assert handle.resolved() is True
    assert await asyncio.wait_for(handle.done.wait(), 3) is True


@pytest.mark.asyncio
async def test_detached_child_leaves_a_pending_launch_to_its_owner(tmp_path, monkeypatch, identities, owned):
    from harflow_mcp import detached, state_layout
    identities.alive(OWNER["pid"])
    receipt = write_receipt(tmp_path, "a" * 32, "waiting_for_resources", owner=OWNER, resources={"names": ["sim"], "state": "waiting"})
    sent: list = []

    class Channel(detached._Handshake):
        def __init__(self):
            pass

        def send(self, document):
            sent.append(document)

        def close(self):
            pass

    monkeypatch.setattr(detached, "current_owned", lambda: None)
    monkeypatch.setattr(detached, "assert_owned", lambda *a, **k: None)
    monkeypatch.setattr(state_layout, "require_dispatch_layout", lambda *a, **k: None)
    registry_stub = SimpleNamespace(log_dir=owned.tasks_directory, list=lambda: [], _continuations=set())

    async def action(_registry):
        return task_resources.PendingLaunch(owned.tasks_directory, receipt)

    assert await asyncio.wait_for(detached._child(action, Channel(), lambda: registry_stub), 3) == 0
    assert sent == [{"task_id": "a" * 32}]


# ---- item 16: the owner finalizes follow-ups for any launch that was handed some -----------------


@pytest.mark.asyncio
async def test_owner_consumes_followups_of_a_worktree_only_launch_at_spawn(tmp_path, source, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    await reg.start("reply done", source, backend=get("claude"), freedom="read_only", max_turns=3, task_id="a" * 32)
    await wait_for(lambda: (r := store.read(log_dir, "a" * 32)) is not None and r.finished_at and r.output_drained)
    await wait_for(lambda: launch_state(tmp_path, "a" * 32) in {"settled", "spawned"})
    previous = store.read(log_dir, "a" * 32)
    followups.send(log_dir, previous.task_id, "remember this", by=None)
    attempt = await followups.begin(log_dir, previous.task_id, "b" * 32, reg._owner)
    assert attempt.ids
    started = await reg.start("reply two", source, backend=get("claude"), freedom="write_in_repo", worktree={}, task_id="b" * 32,
                              message_ids=attempt.ids, followup_source_task_id=previous.task_id)
    await wait_for(lambda: store.read(log_dir, started.task_id) is not None)
    path = log_dir / f"{previous.task_id}{followups.STATE_SUFFIX}"
    # The document itself, not `followups.view`, which would repair a missing consumption on read.
    await wait_for(lambda: [item["status"] for item in json.loads(path.read_text())["ledger"]["items"]] == ["consumed"])


# ---- item 3: humans-only settlement of an uncertain granted launch -------------------------------


def settle(tmp_path, task_id: str):
    return task_resources.settle_uncertain(tasks_dir(tmp_path), task_id)


def dead_agent_record(task_id: str, session_id: str, repo_path: str, *, owner=OWNER, pid=777):
    return store.TaskRecord(task_id=task_id, backend="claude", session_id=session_id, repo_path=repo_path,
                            started_at="2026-01-01T00:00:00+00:00", status="running", pid=pid,
                            start_time="Tue Jan  2 00:00:00 2001", markers=["claude-agent-marker"], owner=owner)


def test_settle_refuses_anything_but_a_granted_recordless_launch_with_a_dead_owner(tmp_path, identities, owned):
    log_dir = owned.tasks_directory
    with pytest.raises(task_resources.LaunchSettleRefused) as refused:
        settle(tmp_path, "9" * 32)
    assert refused.value.code == "unknown_launch"
    write_receipt(tmp_path, "a" * 32, "waiting_for_resources", owner=OWNER, resources={"names": ["sim"], "state": "waiting"})
    with pytest.raises(task_resources.LaunchSettleRefused) as refused:
        settle(tmp_path, "a" * 32)
    assert refused.value.code == "owner_not_dead"
    write_receipt(tmp_path, "b" * 32, "granted", owner=OWNER, resources={"names": ["sim"], "state": "granted"})
    identities.alive(OWNER["pid"])
    with pytest.raises(task_resources.LaunchSettleRefused) as refused:
        settle(tmp_path, "b" * 32)
    assert refused.value.code == "agent_identity_unavailable" and launch_state(tmp_path, "b" * 32) == "granted"
    identities.dead(OWNER["pid"])
    store.write(log_dir, record_for("b" * 32, "/x"))
    with pytest.raises(task_resources.LaunchSettleRefused) as refused:
        settle(tmp_path, "b" * 32)
    assert refused.value.code == "identity_mismatch"


def test_settle_refuses_granted_launch_with_created_worktree_and_no_agent_identity(tmp_path, source, identities, owned):
    log_dir = owned.tasks_directory
    identities.dead(OWNER["pid"])
    made = worktrees.create(worktrees.plan("a" * 32, source, {}, log_dir))
    entry = ledger(tmp_path).request(["sim"], {"kind": "task", "task_id": "a" * 32})
    write_receipt(tmp_path, "a" * 32, "granted", owner=OWNER, repo_path=made["path"], worktree=made,
                  resources={"names": ["sim"], "state": "granted", "reservation_id": entry.reservation_id})
    assert "a" * 32 in {r["task_id"] for r in task_supervision.session_reservations(log_dir)}
    assert [u["task_id"] for u in worktrees.users(log_dir, Path(made["path"]))] == ["a" * 32]
    # A human confirmation cannot prove whether a spawned agent remains live.
    assert ledger(tmp_path).release(entry.reservation_id) == "released"
    with pytest.raises(task_resources.LaunchSettleRefused) as refused:
        settle(tmp_path, "a" * 32)
    assert refused.value.code == "agent_identity_unavailable"
    assert launch_state(tmp_path, "a" * 32) == "granted"
    assert "a" * 32 in {r["task_id"] for r in task_supervision.session_reservations(log_dir)}
    assert [u["task_id"] for u in worktrees.users(log_dir, Path(made["path"]))] == ["a" * 32]
    assert Path(made["path"]).is_dir()  # nothing is removed


@pytest.mark.asyncio
async def test_settle_finalizes_followups_by_what_is_provable(tmp_path, source, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    previous = await finished_task(reg, tmp_path, "a" * 32)
    identities.dead(OWNER["pid"])
    outcomes = {}
    for successor, worktree_state in (("b" * 32, "creating"), ("c" * 32, "created")):
        followups.send(log_dir, previous.task_id, f"note {worktree_state}", by=None)
        attempt = await followups.begin(log_dir, previous.task_id, successor, OWNER)
        write_receipt(tmp_path, successor, "granted", owner=OWNER, repo_path=str(tmp_path / worktree_state),
                      worktree={"path": str(tmp_path / worktree_state), "state": worktree_state, "source_repo_path": str(source)},
                      followups={"task_id": previous.task_id, "ids": attempt.ids})
        if worktree_state == "created":
            identities.dead(777)
            store.write(log_dir, dead_agent_record(successor, f"session-{successor[:4]}", str(tmp_path / worktree_state)))
        outcomes[successor] = settle(tmp_path, successor)
        statuses = [item["status"] for item in json.loads((log_dir / f"{previous.task_id}{followups.STATE_SUFFIX}").read_text())["ledger"]["items"]]
        assert statuses[-1] == ("queued" if worktree_state == "creating" else "delivery_unknown")
    assert outcomes["b" * 32]["outcome"] == "never_started" and outcomes["c" * 32]["outcome"] == "interrupted"
    settled_pre_spawn = task_supervision.read_launch(log_dir, "b" * 32)
    assert settled_pre_spawn["state"] == "not_started"
    assert settled_pre_spawn["reconciliation_pending"] is False


def test_ctl_launch_settle_is_humans_only(tmp_path, monkeypatch, identities, capsys):
    from harflow_mcp import ctl, lineage
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "state"))
    assert ctl.main(["resources", "--json"]) == 0
    capsys.readouterr()
    identities.dead(OWNER["pid"])
    write_receipt(tmp_path, "a" * 32, "granted", owner=OWNER, resources={"names": ["sim"], "state": "granted"})
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None, "ps unreadable"))
    assert ctl.main(["launch-settle", "--task-id", "a" * 32, "--confirm-uncertain", "--json"]) == 1
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["error"]["code"] == "caller_undecidable"
    assert launch_state(tmp_path, "a" * 32) == "granted"
    with pytest.raises(SystemExit):
        ctl.main(["launch-settle", "--task-id", "a" * 32, "--json"])  # --confirm-uncertain is required
    capsys.readouterr()
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None))
    assert ctl.main(["launch-settle", "--task-id", "a" * 32, "--confirm-uncertain", "--json"]) == 1
    error = json.loads(capsys.readouterr().out.splitlines()[-1])["error"]
    assert error["code"] == "agent_identity_unavailable" and launch_state(tmp_path, "a" * 32) == "granted"
    assert ctl.main(["launch-settle", "--task-id", "a" * 32, "--confirm-uncertain", "--json"]) == 1
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["error"]["code"] == "agent_identity_unavailable"


# ---- item 17: a caller that really goes away -----------------------------------------------------


async def abandoned_resume(tmp_path: Path, source_id: str, identities) -> dict:
    """Resume `source_id` with its follow-ups from a separate process and kill that process mid-wait."""
    import signal
    import subprocess
    import sys
    ready = tmp_path / "caller-ready.json"
    proc = subprocess.Popen([sys.executable, str(Path(__file__).with_name("followup_driver.py")), str(tmp_path), source_id, str(ready)])
    try:
        await wait_for(lambda: ready.exists() and ready.read_text().strip().endswith("}"), 40)
        info = json.loads(ready.read_text())
    finally:
        proc.send_signal(signal.SIGKILL)
        proc.wait()
    assert info["caller_pid"] == proc.pid
    receipt = task_supervision.read_launch(tasks_dir(tmp_path), info["task_id"])
    assert receipt["owner"]["pid"] != proc.pid  # the wait belongs to the launch owner, which outlives its caller
    identities.alive(receipt["owner"]["pid"])
    return info


def item_statuses(log_dir: Path, source_id: str) -> list[str]:
    return [item["status"] for item in followups.view(log_dir, source_id)["items"]]


@pytest.mark.asyncio
async def test_followups_return_to_the_queue_when_the_wait_expires_after_the_caller_died(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    source = await finished_task(reg, tmp_path, "a" * 32, resources=["sim"], resource_wait_seconds=2)
    followups.send(log_dir, source.task_id, "remember this", by=None)
    await reg.start("slow 10 hold", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    info = await abandoned_resume(tmp_path, source.task_id, identities)
    assert item_statuses(log_dir, source.task_id) == ["reserved"]
    await wait_for(lambda: launch_state(tmp_path, info["task_id"]) == "not_started", 40)
    await wait_for(lambda: item_statuses(log_dir, source.task_id) == ["queued"])
    assert store.read(log_dir, info["task_id"]) is None


@pytest.mark.asyncio
async def test_followups_are_consumed_when_the_launch_spawns_after_the_caller_died(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    source = await finished_task(reg, tmp_path, "a" * 32, resources=["sim"])
    followups.send(log_dir, source.task_id, "remember this", by=None)
    await reg.start("slow 4 hold", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    info = await abandoned_resume(tmp_path, source.task_id, identities)
    assert item_statuses(log_dir, source.task_id) == ["reserved"]
    await wait_for(lambda: task_supervision.read_task_record(log_dir, info["task_id"]) is not None, 60)
    path = log_dir / f"{source.task_id}{followups.STATE_SUFFIX}"
    await wait_for(lambda: [i["status"] for i in json.loads(path.read_text())["ledger"]["items"]] == ["consumed"])


# ---- round 2 ------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["returns_false", "raises"])
async def test_an_unfinalized_followup_settlement_is_never_requeued(tmp_path, source, monkeypatch, identities, failure):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    previous = await finished_task(reg, tmp_path, "a" * 32)
    identities.dead(OWNER["pid"])
    followups.send(log_dir, previous.task_id, "note", by=None)
    attempt = await followups.begin(log_dir, previous.task_id, "c" * 32, OWNER)
    write_receipt(tmp_path, "c" * 32, "granted", owner=OWNER, repo_path=str(tmp_path / "wt"),
                  worktree={"path": str(tmp_path / "wt"), "state": "created", "source_repo_path": str(source)},
                  followups={"task_id": previous.task_id, "ids": attempt.ids})
    identities.dead(777)
    store.write(log_dir, dead_agent_record("c" * 32, f"session-{'c' * 4}", str(tmp_path / "wt")))

    def broken(*args, **kwargs):
        if failure == "raises":
            raise RuntimeError("stopped between the receipt and the ledger")
        return False

    with monkeypatch.context() as patched:
        patched.setattr(followups, "finish", broken)
        if failure == "raises":
            with pytest.raises(RuntimeError):
                settle(tmp_path, "c" * 32)
        else:
            with pytest.raises(task_resources.LaunchSettleRefused) as refused:
                settle(tmp_path, "c" * 32)
            assert refused.value.code == "followup_write_failed"
    receipt = task_supervision.read_launch(log_dir, "c" * 32)
    assert receipt["state"] == "settled" and receipt["reconciliation_pending"] is True
    followups._INFLIGHT.clear()  # a fresh process knows nothing of this attempt: only the receipt speaks
    assert item_statuses(log_dir, previous.task_id) == ["delivery_unknown"]
    with monkeypatch.context() as patched:
        patched.setattr(followups, "finish", lambda *args, **kwargs: True)
        from harflow_mcp.task_recovery import recover_abandoned
        result = recover_abandoned(log_dir)
    assert result["recovered"] and task_supervision.read_launch(log_dir, "c" * 32)["reconciliation_pending"] is False
    assert item_statuses(log_dir, previous.task_id) == ["delivery_unknown"]


def test_ctl_launch_settle_reports_a_corrupt_ledger_instead_of_failing(tmp_path, monkeypatch, identities, capsys):
    from harflow_mcp import ctl, lineage
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "state"))
    assert ctl.main(["resources", "--json"]) == 0
    capsys.readouterr()
    identities.dead(OWNER["pid"])
    write_receipt(tmp_path, "a" * 32, "granted", owner=OWNER, resources={"names": ["sim"], "state": "granted"})
    identities.dead(777)
    store.write(tasks_dir(tmp_path), dead_agent_record("a" * 32, f"session-{'a' * 4}", str(tmp_path)))
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None))
    ledger(tmp_path).directory.mkdir(parents=True, exist_ok=True)
    ledger(tmp_path).path.write_text("garbage")
    assert ctl.main(["launch-settle", "--task-id", "a" * 32, "--confirm-uncertain", "--json"]) == 1
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["error"]["code"] == "resource_ledger_corrupt"
    assert launch_state(tmp_path, "a" * 32) == "settled"
    assert task_supervision.read_launch(tasks_dir(tmp_path), "a" * 32)["reconciliation_pending"] is True


@pytest.mark.asyncio
async def test_the_worktree_scan_for_a_launch_runs_off_the_event_loop(tmp_path, monkeypatch, identities):
    import threading
    install_fake_claude(tmp_path, monkeypatch)
    seen = []
    real = worktrees.containing_worktree
    monkeypatch.setattr(worktrees, "containing_worktree", lambda *a, **k: (seen.append(threading.current_thread()), real(*a, **k))[1])
    await registry(tmp_path).start("reply x", tmp_path, backend=get("claude"), freedom="read_only", task_id="a" * 32)
    assert seen and all(thread is not threading.main_thread() for thread in seen)


@pytest.mark.asyncio
async def test_owner_killed_while_the_worktree_is_being_created_is_never_started_and_settles(tmp_path, source, monkeypatch, identities):
    import signal
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    signal_dir = hook(monkeypatch, tmp_path, "worktree_add")
    reg = registry(tmp_path)
    started = await reg.start("reply x", source, backend=get("claude"), freedom="write_in_repo", worktree={}, resources=["sim"], task_id="a" * 32)
    await wait_for(lambda: (signal_dir / "reached").exists())
    receipt = task_supervision.read_launch(log_dir, started.task_id)
    assert receipt["state"] == "granted" and receipt["worktree"]["state"] == "creating"
    # The child owner uses the real PID, while its start-time probe may be unavailable in a
    # sandbox. Persist a nonempty generation token before simulating its death.
    receipt["owner"] = receipt["owner"] | {"start_time": "owner-generation-before-worktree"}
    task_supervision._write(log_dir / f"{started.task_id}.launch.json", receipt)
    os.kill(receipt["owner"]["pid"], signal.SIGKILL)
    identities.dead(receipt["owner"]["pid"])
    from harflow_mcp.task_recovery import recover_abandoned
    recovery = recover_abandoned(log_dir)
    assert any(item["task_id"] == started.task_id and item["outcome"] == "never_started"
               for item in recovery["recovered"])
    assert started.task_id not in {r["task_id"] for r in task_supervision.session_reservations(log_dir)}
    task_resources.reap(log_dir)
    assert all(entry.owner.task_id != started.task_id for entry in ledger(tmp_path).entries())
    settled = task_supervision.read_launch(log_dir, started.task_id)
    assert settled["state"] == "not_started" and settled["worktree"]["state"] == "unknown"
    assert not Path(receipt["worktree"]["path"]).exists() and spawn_log(tmp_path) == []
