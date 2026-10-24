"""Production supervised-task restart and durable-observation regressions."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from harflow_mcp import events, store, task_observations, task_supervision
from harflow_mcp.backends import get
from harflow_mcp.tasks import TaskRegistry

from restart_test_support import install_fake_claude


def _tasks(root: Path) -> Path:
    return root / "state" / "tasks"


async def _wait_for(predicate, timeout: float = 5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            if predicate():
                return
        except store.TaskDocumentError:
            pass
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("timed out waiting for supervised task state")
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_supervised_observer_restart_sends_live_input_without_duplicate_execution(tmp_path, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch, session="observer-session")
    log_dir = _tasks(tmp_path)
    first = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    second = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    task_id = "1" * 32
    task = await first.start("slow 2 first", tmp_path, backend=get("claude"), freedom="read_only", task_id=task_id)
    await _wait_for(lambda: store.read(log_dir, task.task_id) is not None)
    identities.alive(store.read(log_dir, task.task_id).owner["pid"])
    attached = second.attach(task.task_id)
    assert attached is not None
    await _wait_for(lambda: attached.acc.turn_open)

    queued = await second.send_message(attached, "reply second")
    assert queued["status"] == "queued"
    await _wait_for(lambda: attached.acc.result_count == 1)

    launch = task_supervision.read_launch(log_dir, task.task_id)
    assert launch is not None
    assert len(list(log_dir.glob(f"{task.task_id}.launch.json"))) == 1
    user_events = [
        json.loads(line)
        for line in events.events_path(log_dir, task.task_id).read_text().splitlines()
        if json.loads(line).get("kind") == "user_message"
    ]
    assert [event["text"] for event in user_events] == ["slow 2 first", "reply second"]

    await second.cancel(attached)


@pytest.mark.asyncio
async def test_two_supervised_servers_race_reserved_task_id_without_second_spawn(tmp_path, monkeypatch):
    install_fake_claude(tmp_path, monkeypatch, session="reservation-race")
    log_dir = _tasks(tmp_path)
    first = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    second = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    task_id = "4" * 32
    results = await asyncio.gather(
        first.start("reply race", tmp_path, backend=get("claude"), freedom="read_only", task_id=task_id),
        second.start("reply race", tmp_path, backend=get("claude"), freedom="read_only", task_id=task_id),
        return_exceptions=True,
    )
    assert sum(isinstance(item, BaseException) for item in results) == 1
    assert sum(not isinstance(item, BaseException) for item in results) == 1
    await _wait_for(lambda: store.read(log_dir, task_id) is not None)
    await _wait_for(lambda: (tmp_path / "claude-spawns.log").exists())
    assert len((tmp_path / "claude-spawns.log").read_text().splitlines()) == 1
    winner = next(item for item in results if not isinstance(item, BaseException))
    await first.cancel(winner)


@pytest.mark.asyncio
async def test_proven_preexec_failure_retries_same_session_but_changed_request_refuses(tmp_path, monkeypatch):
    install_fake_claude(tmp_path, monkeypatch, session="preexec-session")
    log_dir = _tasks(tmp_path)
    registry = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    task_id = "5" * 32
    real_popen = task_supervision.subprocess.Popen
    failure = OSError("test executable missing")
    failure.harflow_not_started = True
    monkeypatch.setattr(task_supervision.subprocess, "Popen", lambda *args, **kwargs: (_ for _ in ()).throw(failure))
    with pytest.raises(OSError):
        await registry.start("reply retry", tmp_path, backend=get("claude"), freedom="read_only", task_id=task_id)
    reservation = task_supervision.read_launch(log_dir, task_id)
    assert reservation is not None and reservation["state"] == "not_started"
    session = reservation["session_id"]
    monkeypatch.setattr(task_supervision.subprocess, "Popen", real_popen)
    task = await registry.start("reply retry", tmp_path, backend=get("claude"), freedom="read_only", task_id=task_id)
    assert task.session_id == session
    with pytest.raises((ValueError, BlockingIOError)):
        await registry.start("reply changed", tmp_path, backend=get("claude"), freedom="read_only", task_id=task_id)


@pytest.mark.asyncio
async def test_authorization_pipe_transfer_times_out_without_blocking_event_loop():
    read_descriptor, write_descriptor = os.pipe()
    heartbeat = 0

    async def pulse():
        nonlocal heartbeat
        while True:
            heartbeat += 1
            await asyncio.sleep(0.001)

    pulse_task = asyncio.create_task(pulse())
    try:
        with pytest.raises(RuntimeError, match="authorization transfer uncertain"):
            await task_supervision._transfer_authorization(
                write_descriptor,
                b"x" * (1024 * 1024),
                asyncio.get_running_loop().time() + 0.05,
                "a" * 32,
            )
        assert heartbeat > 0
        with pytest.raises(OSError):
            os.fstat(write_descriptor)
    finally:
        pulse_task.cancel()
        await asyncio.gather(pulse_task, return_exceptions=True)
        os.close(read_descriptor)


def test_reservations_skip_a_file_that_disappears_between_glob_and_read(tmp_path, monkeypatch):
    from harflow_mcp.state_layout import resolve_layout
    from harflow_mcp.state_ownership import state_session
    log_dir = _tasks(tmp_path)
    with state_session(resolve_layout(root=tmp_path / "state")):
        log_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(task_supervision.Path, "glob", lambda _self, _pattern: [log_dir / ("6" * 32 + ".launch.json")])
        monkeypatch.setattr(task_supervision, "read_launch", lambda *_args, **_kwargs: None)
        assert task_supervision.reservations(log_dir) == []


@pytest.mark.asyncio
async def test_supervised_attach_requires_durable_record(tmp_path):
    registry = TaskRegistry(log_dir=_tasks(tmp_path), execution_mode="supervised")
    assert registry.attach("a" * 32) is None
    assert task_supervision.attachable(registry.log_dir, "a" * 32) is False


@pytest.mark.asyncio
async def test_native_observations_replay_in_order_and_require_final_watermark(tmp_path, monkeypatch):
    install_fake_claude(tmp_path, monkeypatch, session="observation-session")
    log_dir = _tasks(tmp_path)
    registry = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    task = await registry.start("reply observation", tmp_path, backend=get("claude"), freedom="read_only")
    await _wait_for(lambda: store.read(log_dir, task.task_id) is not None)
    launch = task_supervision.read_launch(log_dir, task.task_id)
    assert launch is not None
    generation = launch["generation"]

    task_observations.append(log_dir, task.task_id, generation, 1, {"kind": "child_started", "child_id": "c1"})
    task_observations.append(log_dir, task.task_id, generation, 2, {"kind": "child_finished", "child_id": "c1"})
    events, watermark = task_observations.read(log_dir, task.task_id)
    assert [entry["sequence"] for entry in events] == [1, 2]
    assert watermark is None

    task_observations.finish(log_dir, task.task_id, generation, 2)
    replay, final = task_observations.read(log_dir, task.task_id, after=2)
    assert replay == []
    assert final == 2

    with pytest.raises(ValueError, match="generation"):
        task_observations.append(log_dir, task.task_id, "0" * 32, 3, {"kind": "foreign"})


@pytest.mark.asyncio
async def test_native_observations_page_at_1000_and_reject_incomplete_final_barrier(tmp_path, monkeypatch):
    install_fake_claude(tmp_path, monkeypatch, session="observation-page-session")
    log_dir = _tasks(tmp_path)
    registry = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    task = await registry.start("slow 30 observation page", tmp_path, backend=get("claude"), freedom="read_only")
    await _wait_for(lambda: store.read(log_dir, task.task_id) is not None)
    launch = task_supervision.read_launch(log_dir, task.task_id)
    assert launch is not None
    generation = launch["generation"]
    for sequence in range(1, 1002):
        for _ in range(3):
            try:
                task_observations.append(log_dir, task.task_id, generation, sequence, {"sequence": sequence})
                break
            except store.TaskDocumentError:
                await asyncio.sleep(0.02)
        else:
            raise AssertionError("observation append never found a stable launch reservation")

    first, watermark = task_observations.read(log_dir, task.task_id)
    assert len(first) == 1000 and first[-1]["sequence"] == 1000 and watermark is None
    second, watermark = task_observations.read(log_dir, task.task_id, after=1000)
    assert [entry["sequence"] for entry in second] == [1001] and watermark is None

    task_observations.finish(log_dir, task.task_id, generation, 1000)
    with pytest.raises(ValueError, match="final barrier incomplete"):
        task_observations.read(log_dir, task.task_id, after=1001)
    await registry.cancel(task)


@pytest.mark.asyncio
async def test_supervisor_death_keeps_running_attempt_unattachable_and_does_not_relaunch(
    tmp_path, monkeypatch, identities
):
    install_fake_claude(tmp_path, monkeypatch, session="uncertain-session")
    log_dir = _tasks(tmp_path)
    registry = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    task_id = "2" * 32
    task = await registry.start("slow 30 uncertain", tmp_path, backend=get("claude"), freedom="read_only", task_id=task_id)
    await _wait_for(lambda: store.read(log_dir, task.task_id) is not None)
    record = None
    for _ in range(20):
        try:
            record = store.read(log_dir, task.task_id)
            if record is not None:
                break
        except store.TaskDocumentError:
            await asyncio.sleep(0.05)
    assert record is not None and record.owner is not None
    identities.dead(record.owner["pid"])
    os.kill(record.owner["pid"], signal.SIGKILL)
    await asyncio.sleep(0.2)

    recovered = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    attached = recovered.attach(task.task_id)
    assert attached is not None
    assert task_supervision.attachable(log_dir, task.task_id) is False
    assert not list(log_dir.glob(f"{task.task_id}.launch.*.json"))

    # The detached agent has no surviving monitor after supervisor death.  Cleanup uses only the
    # identity recorded in the task metadata; the test never signals an unknown process.
    if record.pgid:
        try:
            os.killpg(record.pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass


@pytest.mark.asyncio
async def test_fresh_caller_sigkill_keeps_active_command_for_new_observer(tmp_path, monkeypatch):
    """The caller process may disappear while the detached owner keeps draining the agent."""
    install_fake_claude(tmp_path, monkeypatch, session="fresh-process-session")
    ready = tmp_path / "ready.json"
    task_id = "3" * 32
    driver = Path(__file__).with_name("supervised_process_driver.py")
    caller = subprocess.Popen(
        [sys.executable, str(driver), "start", str(tmp_path), task_id, str(ready)],
        cwd=Path(__file__).parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        await _wait_for(lambda: ready.exists(), timeout=10)
        os.kill(caller.pid, signal.SIGKILL)
        assert caller.wait(timeout=5) < 0

        observer = subprocess.run(
            [sys.executable, str(driver), "observe", str(tmp_path), task_id],
            cwd=Path(__file__).parents[1],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert observer.returncode == 0, observer.stderr
        lines = [json.loads(line) for line in observer.stdout.splitlines() if line.strip()]
        assert lines[0]["turn_open"] is True
        assert lines[-1]["queued"] == "queued"
        assert lines[-1]["status"] == "completed"
        assert lines[-1]["output_drained"] is True
        assert lines[-1]["result_count"] >= 1
        spawn_log = tmp_path / "claude-spawns.log"
        assert len(spawn_log.read_text().splitlines()) == 1
    finally:
        if caller.poll() is None:
            caller.kill()
            caller.wait()
        record = store.read(_tasks(tmp_path), task_id)
        if record is not None and record.pgid:
            try:
                os.killpg(record.pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass

@pytest.mark.asyncio
async def test_fresh_caller_sigkill_preserves_live_automatic_continuation(tmp_path, monkeypatch):
    install_fake_claude(tmp_path, monkeypatch, session="continuation-process-session")
    monkeypatch.setenv("FAKE_CLAUDE_CAP_COUNTER", str(tmp_path / "cap-counter"))
    monkeypatch.setenv("FAKE_CLAUDE_CAP_DELAY", "1")
    cap_ready = tmp_path / "cap-ready"
    monkeypatch.setenv("FAKE_CLAUDE_CAP_READY", str(cap_ready))
    ready = tmp_path / "ready-cont.json"
    task_id = "7" * 32
    driver = Path(__file__).with_name("supervised_process_driver.py")
    caller = subprocess.Popen([sys.executable, str(driver), "start_continuation", str(tmp_path), task_id, str(ready)], cwd=Path(__file__).parents[1])
    try:
        await _wait_for(lambda: cap_ready.exists(), timeout=10)
        os.kill(caller.pid, signal.SIGKILL)
        caller.wait(timeout=5)
        await _wait_for(
            lambda: any(
                record.continuation_of_task_id == task_id
                and record.status == "completed"
                and record.output_drained
                for record in store.scan(_tasks(tmp_path)).records
            ),
            timeout=10,
        )
        records = store.scan(_tasks(tmp_path)).records
        successors = [record for record in records if record.continuation_of_task_id == task_id]
        assert len(successors) == 1
        assert successors[0].status == "completed"
        assert successors[0].output_drained
        assert len((tmp_path / "claude-spawns.log").read_text().splitlines()) == 2
    finally:
        if caller.poll() is None:
            caller.kill(); caller.wait()
