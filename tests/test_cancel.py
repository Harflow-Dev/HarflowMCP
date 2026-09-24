"""Cross-process cancel: local delivery ordering, the monitor's cross-process wait, and cascade."""

from __future__ import annotations

import asyncio
import os
import signal
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from polybridge import control, identity, store
from polybridge import tasks as tasks_module
from polybridge.tasks import Task, TaskRegistry

OWNER = {"pid": 1, "start_time": None, "markers": []}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_task(tmp_path: Path, task_id: str, **overrides) -> Task:
    defaults = dict(
        task_id=task_id,
        backend="claude",
        session_id=f"session-{task_id}",
        repo_path=tmp_path,
        prompt="x",
        max_turns=5,
        log_path=tmp_path / f"{task_id}.jsonl",
        started_at=datetime.now(timezone.utc),
    )
    defaults.update(overrides)
    return Task(**defaults)


async def spawn_sleep_task(
    registry: TaskRegistry, task_id: str, tmp_path: Path, *, with_monitor: bool = True, **overrides
) -> Task:
    """A task backed by a real `sleep 30` subprocess, so cancel delivery has something real to
    signal. `sleep` terminates immediately on SIGTERM (no trap), so it settles fast once cancelled."""
    proc = await asyncio.create_subprocess_exec(
        "sleep",
        "30",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    task = make_task(tmp_path, task_id, **overrides)
    task.proc = proc
    task.pgid = proc.pid
    registry._tasks[task_id] = task
    registry.persist(task)
    if with_monitor:
        task.watchers = [
            asyncio.create_task(tasks_module._drain_stdout(task, registry)),
            asyncio.create_task(tasks_module._drain_stderr(task)),
        ]
        task.monitor = asyncio.create_task(tasks_module._monitor(task, registry))
    return task


async def cleanup(*tasks: Task) -> None:
    for task in tasks:
        if task.proc is not None and task.proc.returncode is None:
            try:
                tasks_module._signal_group(task, signal.SIGKILL)
            except Exception:
                pass
        if task.monitor is not None:
            await asyncio.gather(task.monitor, return_exceptions=True)
        for watcher in task.watchers:
            watcher.cancel()
        if task.watchers:
            await asyncio.gather(*task.watchers, return_exceptions=True)
        if task.proc is not None:
            try:
                await asyncio.wait_for(task.proc.wait(), timeout=2)
            except (asyncio.TimeoutError, ProcessLookupError):
                pass


# --- A: local cancel delivery ordering ----------------------------------------------------------


async def test_local_cancel_delivers_synchronously_before_task_done_is_touched(
    tmp_path: Path,
) -> None:
    """`_deliver_local_cancel` has no `await` in it at all, so by the time it returns, the phase
    file already reflects the outcome — regardless of whether anything has since observed
    `task.done`. This is the property that makes ordering versus the monitor unobservable."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    task = await spawn_sleep_task(registry, "t0", tmp_path, with_monitor=False)

    registry._deliver_local_cancel(task)

    assert task.cancel_requested is True
    assert not task.done.is_set()
    payload = control.read_phase(tmp_path, "t0", control.CANCEL, 1, "sig")
    assert payload is not None
    assert payload["leader_alive"] is True

    await cleanup(task)


async def test_local_cancel_req_failure_signals_nothing_and_does_not_set_the_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    task = await spawn_sleep_task(registry, "t0", tmp_path, with_monitor=False)

    def boom(*args, **kwargs):
        raise control.PhaseWriteError("disk is gone")

    monkeypatch.setattr(control, "begin_attempt", boom)
    signalled: list[int] = []
    monkeypatch.setattr(
        tasks_module, "_signal_group", lambda t, sig: signalled.append(sig) or True
    )

    with pytest.raises(control.PhaseWriteError):
        registry._deliver_local_cancel(task)

    assert task.cancel_requested is False
    assert signalled == []

    await cleanup(task)


async def test_a_joined_local_cancel_writes_no_phase_file_of_its_own(tmp_path: Path) -> None:
    """A repeat/concurrent local cancel still sets the flag and re-signals, but does not create a
    second attempt — it joins the one already pending."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    task = await spawn_sleep_task(registry, "t0", tmp_path, with_monitor=False)

    registry._deliver_local_cancel(task)
    assert control.latest_attempt(tmp_path, "t0", control.CANCEL) == 1

    registry._deliver_local_cancel(task)

    assert control.latest_attempt(tmp_path, "t0", control.CANCEL) == 1
    assert control.attempt_outcome(tmp_path, "t0", control.CANCEL, 1) == "sig"

    await cleanup(task)


async def test_full_cancel_settles_with_the_monitor_reading_its_own_flag(tmp_path: Path) -> None:
    """End-to-end: `cancel()` delivers, the real monitor observes `cancel_requested` and settles
    without needing to consult the cross-process phase files at all (the owner's own rule wins)."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    task = await spawn_sleep_task(registry, "t0", tmp_path)

    result = await asyncio.wait_for(registry.cancel(task), timeout=5)

    assert result.status == "cancelled"
    assert result.finished
    assert store.read(tmp_path, "t0").status == "cancelled"

    await cleanup(task)


# --- B: the monitor's cross-process wait ----------------------------------------------------------


async def test_monitor_reads_an_authorized_sig_as_cancelled_even_on_a_graceful_exit(
    tmp_path: Path,
) -> None:
    """A `.sig leader_alive: true` written by another controller means cancelled, regardless of
    what `_classify` would otherwise make of a clean exit."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    proc = await asyncio.create_subprocess_exec(
        "true", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True
    )
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    registry._tasks["t0"] = task
    registry.persist(task)
    task.watchers = [
        asyncio.create_task(tasks_module._drain_stdout(task, registry)),
        asyncio.create_task(tasks_module._drain_stderr(task)),
    ]
    control.write_phase(tmp_path, "t0", control.CANCEL, 1, "sig", {"at": _now_iso(), "leader_alive": True})

    await asyncio.wait_for(tasks_module._monitor(task, registry), timeout=5)

    assert task.exit_code == 0
    assert task.status == "cancelled"


async def test_monitor_reads_a_sig_with_leader_alive_false_as_a_plain_classify(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    proc = await asyncio.create_subprocess_exec(
        "true", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True
    )
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    registry._tasks["t0"] = task
    registry.persist(task)
    task.watchers = [
        asyncio.create_task(tasks_module._drain_stdout(task, registry)),
        asyncio.create_task(tasks_module._drain_stderr(task)),
    ]
    control.write_phase(
        tmp_path, "t0", control.CANCEL, 1, "sig", {"at": _now_iso(), "leader_alive": False}
    )

    await asyncio.wait_for(tasks_module._monitor(task, registry), timeout=5)

    assert task.status != "cancelled"
    assert task.status == tasks_module._classify(task, 0)


async def test_monitor_reads_a_failed_attempt_as_a_plain_classify(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    proc = await asyncio.create_subprocess_exec(
        "true", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True
    )
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    registry._tasks["t0"] = task
    registry.persist(task)
    task.watchers = [
        asyncio.create_task(tasks_module._drain_stdout(task, registry)),
        asyncio.create_task(tasks_module._drain_stderr(task)),
    ]
    control.write_phase(
        tmp_path, "t0", control.CANCEL, 1, "failed", {"at": _now_iso(), "reason": "process group already gone"}
    )

    await asyncio.wait_for(tasks_module._monitor(task, registry), timeout=5)

    assert task.status != "cancelled"
    assert task.status == tasks_module._classify(task, 0)


async def test_monitor_recovers_an_abandoned_pending_attempt_and_classifies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pending `.req` whose controller is dead and whose lease has expired must not hang the
    monitor forever: `cancel_verdict` recovers it (publishing `.failed`) and classify stands."""
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    monkeypatch.setattr(identity, "identity_check", lambda ident: "dead")
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    proc = await asyncio.create_subprocess_exec(
        "true", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True
    )
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    registry._tasks["t0"] = task
    registry.persist(task)
    task.watchers = [
        asyncio.create_task(tasks_module._drain_stdout(task, registry)),
        asyncio.create_task(tasks_module._drain_stderr(task)),
    ]
    stale_at = (datetime.now(timezone.utc) - timedelta(seconds=1000)).isoformat()
    control.write_phase(
        tmp_path,
        "t0",
        control.CANCEL,
        1,
        "req",
        {"at": stale_at, "by": {"pid": 2, "start_time": None, "markers": []}, "lease_seconds": 60},
    )

    await asyncio.wait_for(tasks_module._monitor(task, registry), timeout=5)

    assert task.status != "cancelled"
    assert task.status == tasks_module._classify(task, 0)
    failed_payload = control.read_phase(tmp_path, "t0", control.CANCEL, 1, "failed")
    assert failed_payload == {"at": failed_payload["at"], "reason": "controller died"}


async def test_monitor_waits_out_a_pending_attempt_with_a_live_controller_then_settles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    monkeypatch.setattr(identity, "identity_check", lambda ident: "alive")
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    proc = await asyncio.create_subprocess_exec(
        "true", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True
    )
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    registry._tasks["t0"] = task
    registry.persist(task)
    task.watchers = [
        asyncio.create_task(tasks_module._drain_stdout(task, registry)),
        asyncio.create_task(tasks_module._drain_stderr(task)),
    ]
    control.write_phase(
        tmp_path,
        "t0",
        control.CANCEL,
        1,
        "req",
        {"at": _now_iso(), "by": {"pid": 2, "start_time": None, "markers": []}, "lease_seconds": 60},
    )

    monitor = asyncio.create_task(tasks_module._monitor(task, registry))
    await asyncio.sleep(0.1)
    assert not task.done.is_set()

    control.write_phase(tmp_path, "t0", control.CANCEL, 1, "sig", {"at": _now_iso(), "leader_alive": True})
    await asyncio.wait_for(monitor, timeout=5)

    assert task.status == "cancelled"


async def test_monitor_owners_own_cancel_wins_regardless_of_files(
    tmp_path: Path,
) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    proc = await asyncio.create_subprocess_exec(
        "true", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True
    )
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    task.cancel_requested = True
    registry._tasks["t0"] = task
    registry.persist(task)
    task.watchers = [
        asyncio.create_task(tasks_module._drain_stdout(task, registry)),
        asyncio.create_task(tasks_module._drain_stderr(task)),
    ]
    control.write_phase(
        tmp_path, "t0", control.CANCEL, 1, "sig", {"at": _now_iso(), "leader_alive": False}
    )

    await asyncio.wait_for(tasks_module._monitor(task, registry), timeout=5)

    assert task.status == "cancelled"


async def test_monitor_with_no_attempt_files_classifies_normally(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    proc = await asyncio.create_subprocess_exec(
        "true", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True
    )
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    registry._tasks["t0"] = task
    registry.persist(task)
    task.watchers = [
        asyncio.create_task(tasks_module._drain_stdout(task, registry)),
        asyncio.create_task(tasks_module._drain_stderr(task)),
    ]

    await asyncio.wait_for(tasks_module._monitor(task, registry), timeout=5)

    assert task.status != "cancelled"
    assert task.status == tasks_module._classify(task, 0)


# --- D: cascade -----------------------------------------------------------------------------------


async def test_cascade_cancels_a_local_target_and_its_local_child(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    root = await spawn_sleep_task(registry, "root", tmp_path)
    child = await spawn_sleep_task(registry, "child", tmp_path, spawned_by="root")

    result = await asyncio.wait_for(registry.cancel_cascade("root"), timeout=5)

    assert root.status == "cancelled"
    assert child.status == "cancelled"
    assert result["cancelled_descendants"] == ["child"]

    await cleanup(root, child)


async def test_cascade_reports_a_failed_descendant_and_still_cancels_the_rest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One target's phase file failing must neither abort the cascade nor orphan its siblings."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    root = await spawn_sleep_task(registry, "root", tmp_path)
    broken = await spawn_sleep_task(registry, "broken", tmp_path, spawned_by="root")
    sibling = await spawn_sleep_task(registry, "sibling", tmp_path, spawned_by="root")

    real_begin = control.begin_attempt

    def begin(log_dir, task_id, *args, **kwargs):
        if task_id == "broken":
            raise control.PhaseWriteError("disk full")
        return real_begin(log_dir, task_id, *args, **kwargs)

    monkeypatch.setattr(control, "begin_attempt", begin)

    result = await asyncio.wait_for(registry.cancel_cascade("root"), timeout=5)

    assert root.status == "cancelled"
    assert sibling.status == "cancelled"
    assert not broken.cancel_requested
    assert result["cancelled_descendants"] == ["sibling"]
    assert [entry["task_id"] for entry in result["not_signalled"]] == ["broken"]
    assert "phase write failed" in result["not_signalled"][0]["reason"]

    await cleanup(root, broken, sibling)


def _child_record(tmp_path: Path, *, pid: int = 424242) -> store.TaskRecord:
    """A non-local descendant of a local no-op `root` task (see `spawn_noop_local_task`), owned by
    some other (simulated) bridge server."""
    return store.TaskRecord(
        task_id="child",
        backend="claude",
        session_id="s-child",
        markers=["m"],
        repo_path=str(tmp_path),
        started_at=_now_iso(),
        pid=pid,
        pgid=pid,
        owner={"pid": 55555, "start_time": None, "markers": []},
        spawned_by="root",
        status="running",
    )


def spawn_noop_local_task(registry: TaskRegistry, tmp_path: Path, task_id: str = "root") -> Task:
    """A trivial local task with no real process attached — `TaskRegistry.cancel` settles it
    synchronously (see the `task.proc is None` branch), so it is a cheap stand-in for "some local
    task with a non-local descendant" in cascade tests that are really about the descendant."""
    task = make_task(tmp_path, task_id)
    registry._tasks[task_id] = task
    registry.persist(task)
    return task


async def test_cascade_case2_settles_when_the_other_owner_finishes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 1.0)
    monkeypatch.setattr(tasks_module, "DRAIN_GRACE_SECONDS", 1.0)
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.02)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    spawn_noop_local_task(registry, tmp_path)
    store.write(tmp_path, _child_record(tmp_path))

    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    monkeypatch.setattr(identity, "may_signal", lambda ident: True)
    monkeypatch.setattr(identity, "identity_check", lambda ident: "alive")
    monkeypatch.setattr(tasks_module, "_signal_recorded_group", lambda record, sig: True)

    async def owner_settles() -> None:
        await asyncio.sleep(0.1)
        current = store.read(tmp_path, "child")
        store.write(
            tmp_path, replace(current, status="cancelled", finished_at=_now_iso(), exit_code=0)
        )

    settler = asyncio.create_task(owner_settles())
    result = await asyncio.wait_for(registry.cancel_cascade("root"), timeout=5)
    await settler

    assert "child" in result["cancelled_descendants"]
    assert result["owner_still_settling"] == []


async def test_cascade_case2_reports_still_settling_and_leaves_the_record_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(tasks_module, "DRAIN_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    spawn_noop_local_task(registry, tmp_path)
    # A genuinely alive pid (this test process itself) with no markers to check, so
    # `store.resolve_status`'s own real `process_alive` — separate from the mocked identity
    # checks cascade uses — agrees the leader is still running: this is what "still settling"
    # means (the leader really has not exited yet), as opposed to a leader that already died
    # while its owner just has not persisted that yet, which is a different, legitimate case
    # `resolve_status` resolves as `cancelled` on its own (see `test_store.py`).
    store.write(tmp_path, replace(_child_record(tmp_path, pid=os.getpid()), markers=[]))

    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    monkeypatch.setattr(identity, "may_signal", lambda ident: True)
    monkeypatch.setattr(identity, "identity_check", lambda ident: "alive")  # owner never dies
    monkeypatch.setattr(tasks_module, "_signal_recorded_group", lambda record, sig: True)

    result = await asyncio.wait_for(registry.cancel_cascade("root"), timeout=5)

    assert result["owner_still_settling"] == ["child"]
    assert result["cancelled_descendants"] == []
    assert store.read(tmp_path, "child").status == "running"


async def test_cascade_case2_hands_off_to_case3_once_the_owner_is_confirmed_dead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(tasks_module, "DRAIN_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    spawn_noop_local_task(registry, tmp_path)
    store.write(tmp_path, _child_record(tmp_path))

    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))

    killed = {"v": False}

    def signal_group(record, sig):
        if sig == signal.SIGKILL:
            killed["v"] = True
        return True

    monkeypatch.setattr(tasks_module, "_signal_recorded_group", signal_group)
    monkeypatch.setattr(identity, "may_signal", lambda ident: not killed["v"])

    calls = {"n": 0}

    def identity_check(ident):
        calls["n"] += 1
        # First call is the discovery-loop owner check (routes to case 2); every call after that
        # is case 2's own recheck once its bound elapses, and case 3's leader-alive probe.
        return "alive" if calls["n"] == 1 else "dead"

    monkeypatch.setattr(identity, "identity_check", identity_check)

    result = await asyncio.wait_for(registry.cancel_cascade("root"), timeout=5)

    assert "child" in result["cancelled_descendants"]
    assert result["sigkill_survivors"] == []
    assert result["owner_still_settling"] == []


async def test_a_handed_off_target_whose_group_is_already_gone_is_still_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Measured in acceptance: case 2's SIGTERM kills the child, its owner dies, case 3's repeat
    SIGTERM finds no group. That is the first signal having worked — the record must be written
    `cancelled`, not reported as never signalled."""
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(tasks_module, "DRAIN_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    spawn_noop_local_task(registry, tmp_path)
    store.write(tmp_path, _child_record(tmp_path))

    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    sigterms = {"n": 0}

    def signal_group(record, sig):
        if sig == signal.SIGTERM:
            sigterms["n"] += 1
            return sigterms["n"] == 1  # the group is gone by the time case 3 re-signals
        return False

    monkeypatch.setattr(tasks_module, "_signal_recorded_group", signal_group)
    monkeypatch.setattr(identity, "may_signal", lambda ident: sigterms["n"] == 0)
    owner_calls = {"n": 0}

    def identity_check(ident):
        if (ident or {}).get("markers"):  # the leader: killed by case 2's SIGTERM
            return "alive" if sigterms["n"] == 0 else "dead"
        owner_calls["n"] += 1  # the owner: alive at discovery, dead by case 2's recheck
        return "alive" if owner_calls["n"] == 1 else "dead"

    monkeypatch.setattr(identity, "identity_check", identity_check)

    result = await asyncio.wait_for(registry.cancel_cascade("root"), timeout=5)

    assert result["not_signalled"] == []
    assert store.read(tmp_path, "child").status == "cancelled"
    assert "child" in result["cancelled_descendants"]


async def test_cascade_dead_owner_batch_of_two_descendants_both_written_cancelled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)

    for task_id in ("a", "b"):
        store.write(
            tmp_path,
            store.TaskRecord(
                task_id=task_id,
                backend="claude",
                session_id=f"s-{task_id}",
                markers=["m"],
                repo_path=str(tmp_path),
                started_at=_now_iso(),
                pid=100 if task_id == "a" else 200,
                pgid=100 if task_id == "a" else 200,
                owner={"pid": 55555, "start_time": None, "markers": []},
                spawned_by="root",
                status="running",
            ),
        )

    killed_pids: set[int] = set()

    def signal_group(record, sig):
        if sig == signal.SIGKILL:
            killed_pids.add(record.pid)
        return True

    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    # Alive (signallable) until this specific pid has actually been SIGKILLed — not dead from the
    # start, or the discovery loop's own `may_signal` gate would skip both before ever reaching
    # case 3 at all.
    monkeypatch.setattr(identity, "may_signal", lambda ident: ident["pid"] not in killed_pids)
    monkeypatch.setattr(identity, "identity_check", lambda ident: "dead")
    monkeypatch.setattr(tasks_module, "_signal_recorded_group", signal_group)

    result = await asyncio.wait_for(registry.cancel_cascade("root"), timeout=5)

    assert set(result["cancelled_descendants"]) == {"a", "b"}
    assert store.read(tmp_path, "a").status == "cancelled"
    assert store.read(tmp_path, "b").status == "cancelled"


async def test_cascade_reports_a_sigkill_survivor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 0.02)
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)

    record = store.TaskRecord(
        task_id="stubborn",
        backend="claude",
        session_id="s-stubborn",
        markers=["m"],
        repo_path=str(tmp_path),
        started_at=_now_iso(),
        pid=4242,
        pgid=4242,
        owner={"pid": 55555, "start_time": None, "markers": []},
        status="running",
    )
    store.write(tmp_path, record)

    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    monkeypatch.setattr(identity, "may_signal", lambda ident: True)  # never dies, even after SIGKILL
    # Owner (no markers) dead, so case 3; the leader (markers ["m"]) never dies.
    monkeypatch.setattr(
        identity, "identity_check", lambda ident: "alive" if (ident or {}).get("markers") else "dead"
    )
    monkeypatch.setattr(tasks_module, "_signal_recorded_group", lambda record, sig: True)

    result = await asyncio.wait_for(registry.cancel_cascade("stubborn"), timeout=5)

    assert result["sigkill_survivors"] == ["stubborn"]


async def test_cascade_reaches_a_fixed_point_across_rounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A grandchild that only appears on disk *after* round 1 (written as a side effect of
    signalling the root) is still picked up, because targets are recomputed every round."""
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    root = await spawn_sleep_task(registry, "root", tmp_path, with_monitor=False)

    wrote_grandchild = {"v": False}
    real_signal_group = tasks_module._signal_group

    def fake_signal_group(task, sig):
        if task.task_id == "root" and not wrote_grandchild["v"]:
            wrote_grandchild["v"] = True
            store.write(
                tmp_path,
                store.TaskRecord(
                    task_id="grandchild",
                    backend="claude",
                    session_id="s-gc",
                    markers=["m"],
                    repo_path=str(tmp_path),
                    started_at=_now_iso(),
                    pid=42,
                    pgid=None,
                    owner={"pid": 55555, "start_time": None, "markers": []},
                    spawned_by="root",
                    status="running",
                ),
            )
        return real_signal_group(task, sig)

    monkeypatch.setattr(tasks_module, "_signal_group", fake_signal_group)
    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    monkeypatch.setattr(identity, "may_signal", lambda ident: True)

    result = await asyncio.wait_for(registry.cancel_cascade("root"), timeout=5)

    assert result["rounds"] >= 2
    assert any(entry["task_id"] == "grandchild" for entry in result["not_signalled"])

    await cleanup(root)


async def test_cascade_reaches_through_a_settled_intermediate_and_via_root_task_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)

    intermediate = store.TaskRecord(
        task_id="mid",
        backend="claude",
        session_id="s-mid",
        markers=["m"],
        repo_path=str(tmp_path),
        started_at=_now_iso(),
        pid=1,
        pgid=1,
        owner={"pid": 1, "start_time": None, "markers": []},
        spawned_by="root",
        status="completed",
        exit_code=0,
    )
    grandchild = store.TaskRecord(
        task_id="grandchild",
        backend="claude",
        session_id="s-gc",
        markers=["m"],
        repo_path=str(tmp_path),
        started_at=_now_iso(),
        pid=2,
        pgid=None,
        owner={"pid": 1, "start_time": None, "markers": []},
        spawned_by="mid",
        status="running",
    )
    cousin = store.TaskRecord(
        task_id="cousin",
        backend="claude",
        session_id="s-cousin",
        markers=["m"],
        repo_path=str(tmp_path),
        started_at=_now_iso(),
        pid=3,
        pgid=None,
        owner={"pid": 1, "start_time": None, "markers": []},
        root_task_id="root",
        status="running",
    )
    store.write(tmp_path, intermediate)
    store.write(tmp_path, grandchild)
    store.write(tmp_path, cousin)

    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    monkeypatch.setattr(identity, "may_signal", lambda ident: True)

    result = await asyncio.wait_for(registry.cancel_cascade("root"), timeout=5)

    signalled_ids = {e["task_id"] for e in result["not_signalled"]}
    assert "grandchild" in signalled_ids
    assert "cousin" in signalled_ids


async def test_cascade_refuses_to_signal_a_legacy_record_when_ps_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    record = store.TaskRecord(
        task_id="legacy",
        backend="claude",
        session_id="s-legacy",
        markers=["m"],
        repo_path=str(tmp_path),
        started_at=_now_iso(),
        pid=4321,
        pgid=4321,
        owner=None,
        start_time=None,
        status="running",
    )
    store.write(tmp_path, record)

    monkeypatch.setattr(identity, "_run_ps", lambda pid: None)

    signalled: list[int] = []
    monkeypatch.setattr(
        tasks_module, "_signal_recorded_group", lambda record, sig: signalled.append(sig) or True
    )

    result = await asyncio.wait_for(registry.cancel_cascade("legacy"), timeout=5)

    assert signalled == []
    assert any(
        entry["task_id"] == "legacy" and entry["reason"] == "ps_failed"
        for entry in result["not_signalled"]
    )
    assert store.read(tmp_path, "legacy").status == "running"


# --- round-1 review regressions -------------------------------------------------------------------


async def test_a_cancelled_cascade_still_finishes_the_delivery_it_started(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`.req` -> SIGTERM -> `.sig` runs as one worker-thread call, so a client disconnecting
    mid-cancel cannot leave a pending `.req` that the owner's monitor would wait on forever."""
    import threading

    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    store.write(tmp_path, _child_record(tmp_path))
    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    monkeypatch.setattr(identity, "may_signal", lambda ident: True)
    monkeypatch.setattr(identity, "identity_check", lambda ident: "alive")
    in_signal = threading.Event()

    def slow_signal(record, sig):
        in_signal.set()
        time.sleep(0.3)  # the awaiting coroutine is cancelled while this runs
        return True

    monkeypatch.setattr(tasks_module, "_signal_recorded_group", slow_signal)

    cascade = asyncio.create_task(registry.cancel_cascade("child"))
    await asyncio.to_thread(in_signal.wait, 2)
    cascade.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cascade
    await asyncio.sleep(0.5)

    assert control.attempt_outcome(tmp_path, "child", control.CANCEL, 1) == "sig"


async def test_delivery_revalidates_the_leader_and_never_signals_a_reused_pid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Discovery saw the leader alive; by delivery its pid belongs to something else."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    record = _child_record(tmp_path)
    store.write(tmp_path, record)
    # Discovery (elsewhere) saw it alive; at delivery, a different process holds the pid.
    monkeypatch.setattr(identity, "check_detail", lambda ident: ("dead", "start_time_differs"))
    monkeypatch.setattr(identity, "may_signal", lambda ident: False)
    signals: list[int] = []
    monkeypatch.setattr(
        tasks_module, "_signal_recorded_group", lambda record, sig: signals.append(sig) or True
    )

    outcomes = await registry._cascade_case3_batch([record])

    assert signals == []
    assert outcomes == {}
    assert control.latest_attempt(tmp_path, "child", control.CANCEL) is None
    assert store.read(tmp_path, "child").status == "running"


async def test_an_unobserved_terminal_record_does_not_suppress_the_sigkill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A torn-down server's backstop `failed` (no exit code) can sit on a live process."""
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(tasks_module, "DRAIN_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    record = replace(_child_record(tmp_path), status="failed", exit_code=None)
    store.write(tmp_path, record)
    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    monkeypatch.setattr(identity, "may_signal", lambda ident: True)
    monkeypatch.setattr(
        identity, "identity_check", lambda ident: "alive" if (ident or {}).get("markers") else "undecidable"
    )
    signals: list[int] = []
    monkeypatch.setattr(
        tasks_module, "_signal_recorded_group", lambda record, sig: signals.append(sig) or True
    )

    outcome = await registry._cascade_case2(record)

    assert signal.SIGKILL in signals
    assert outcome.kind == "still_settling"


async def test_an_undecidable_leader_stays_a_survivor_rather_than_counting_as_dead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 0.03)
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    record = _child_record(tmp_path)
    store.write(tmp_path, record)
    signalled = {"v": False}

    def signal_group(record, sig):
        signalled["v"] = True
        return True

    monkeypatch.setattr(tasks_module, "_signal_recorded_group", signal_group)
    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    # after the SIGTERM, `ps` starts failing: can neither signal nor call it dead
    monkeypatch.setattr(identity, "may_signal", lambda ident: not signalled["v"])
    monkeypatch.setattr(
        identity,
        "identity_check",
        lambda ident: "undecidable" if (ident or {}).get("markers") else "dead",
    )

    outcomes = await registry._cascade_case3_batch([record])

    assert outcomes["child"] == ("sigkill_survivor", None)


# --- round-2 review regressions -------------------------------------------------------------------


async def test_a_joining_controller_records_its_own_successful_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The attempt's owner may find the group gone and write `.failed`; the joiner's real delivery
    must still reach the owner's monitor as `authorized`."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    record = _child_record(tmp_path)
    store.write(tmp_path, record)
    control.begin_attempt(tmp_path, "child", control.CANCEL, {"pid": 2, "start_time": "x", "markers": []})
    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    monkeypatch.setattr(identity, "identity_check", lambda ident: "alive")  # the owner of `.req`
    monkeypatch.setattr(tasks_module, "_signal_recorded_group", lambda record, sig: True)

    assert registry._deliver_recorded_cancel(record) == ("signalled", None)
    control.mark_failed(tmp_path, "child", control.CANCEL, 1, reason="process group already gone")

    assert control.cancel_verdict(tmp_path, "child") == "authorized"


async def test_leader_alive_and_the_signal_decision_come_from_one_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    record = _child_record(tmp_path)
    store.write(tmp_path, record)
    observations = iter([("alive", "start_time_match"), ("undecidable", "ps_failed")])
    monkeypatch.setattr(identity, "check_detail", lambda ident: next(observations))
    monkeypatch.setattr(tasks_module, "_signal_recorded_group", lambda record, sig: True)

    assert registry._deliver_recorded_cancel(record) == ("signalled", None)
    payload = control.read_phase(tmp_path, "child", control.CANCEL, 1, "sig")
    assert payload["leader_alive"] is True


async def test_a_failed_sig_write_is_retried_and_never_replaced_by_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tasks_module, "_SIG_WRITE_RETRY_SECONDS", 0)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    record = _child_record(tmp_path)
    store.write(tmp_path, record)
    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    monkeypatch.setattr(tasks_module, "_signal_recorded_group", lambda record, sig: True)

    def never(*args, **kwargs):
        raise control.PhaseWriteError("disk full")

    monkeypatch.setattr(control, "mark_signalled", never)

    registry._deliver_recorded_cancel(record)

    assert control.attempt_outcome(tmp_path, "child", control.CANCEL, 1) == "pending"


async def test_sigkill_is_never_sent_once_the_leader_is_no_longer_ours(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = _child_record(tmp_path)
    monkeypatch.setattr(identity, "check_detail", lambda ident: ("dead", "start_time_differs"))
    signals: list[int] = []
    monkeypatch.setattr(
        tasks_module, "_signal_recorded_group", lambda record, sig: signals.append(sig) or True
    )

    assert TaskRegistry._kill_if_ours(record) is False
    assert signals == []


async def test_escalation_survives_the_caller_going_away(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A client disconnecting mid-`cancel_task` must not abandon a SIGTERM-resistant task."""
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 0.1)
    monkeypatch.setattr(tasks_module, "CANCEL_VERDICT_POLL_SECONDS", 0.01)
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    store.write(tmp_path, _child_record(tmp_path))
    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    monkeypatch.setattr(
        identity, "identity_check", lambda ident: "alive" if (ident or {}).get("markers") else "dead"
    )
    signals: list[int] = []
    monkeypatch.setattr(
        tasks_module, "_signal_recorded_group", lambda record, sig: signals.append(sig) or True
    )

    caller = asyncio.create_task(registry.cancel_cascade("child"))
    await asyncio.sleep(0.05)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    await asyncio.wait_for(asyncio.gather(*registry._control_jobs), timeout=5)

    assert signals.count(signal.SIGKILL) == 1
