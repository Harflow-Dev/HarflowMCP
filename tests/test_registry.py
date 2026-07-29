"""Registry bookkeeping, using tasks with no real subprocess behind them."""

from __future__ import annotations

import asyncio
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from polybridge import store
from polybridge import tasks as tasks_module
from polybridge.tasks import (
    TERMINAL_STATUSES,
    RepoUnavailableError,
    SessionBusyError,
    Task,
    TaskRegistry,
)


def make_task(
    tmp_path: Path,
    task_id: str,
    *,
    session_id: str = "session",
    finished: bool = False,
    age_seconds: int = 0,
) -> Task:
    task = Task(
        task_id=task_id,
        backend="claude",
        session_id=session_id,
        repo_path=tmp_path,
        prompt="x",
        max_turns=5,
        log_path=tmp_path / f"{task_id}.jsonl",
        started_at=datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
    )
    if finished:
        task.status = "completed"
        task.done.set()
    return task


async def test_prune_trims_finished_tasks_oldest_first(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, max_tasks=2)
    for index in range(4):
        task = make_task(tmp_path, f"t{index}", finished=True, age_seconds=100 - index)
        registry._tasks[task.task_id] = task

    registry.prune()

    assert [t.task_id for t in registry.list()] == ["t2", "t3"]


async def test_prune_never_evicts_a_live_task(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, max_tasks=1)
    live = make_task(tmp_path, "live", age_seconds=100)
    finished = make_task(tmp_path, "finished", finished=True, age_seconds=50)
    registry._tasks[live.task_id] = live
    registry._tasks[finished.task_id] = finished

    registry.prune()

    # The live task is older, but capacity is reclaimed from finished tasks only.
    assert [t.task_id for t in registry.list()] == ["live"]


async def test_registry_may_exceed_capacity_while_everything_is_live(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, max_tasks=1)
    for index in range(3):
        task = make_task(tmp_path, f"t{index}")
        registry._tasks[task.task_id] = task

    registry.prune()

    assert len(registry.list()) == 3


async def test_a_finishing_task_makes_room(tmp_path: Path) -> None:
    """Eviction must not depend on a new task being started."""
    registry = TaskRegistry(log_dir=tmp_path, max_tasks=1)
    for index in range(3):
        task = make_task(tmp_path, f"t{index}", age_seconds=100 - index)
        registry._tasks[task.task_id] = task

    for task in registry.list():
        task.status = "completed"
        task.done.set()
    registry.prune()

    assert [t.task_id for t in registry.list()] == ["t2"]


def test_session_is_busy_only_while_a_run_is_live(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path)
    task = make_task(tmp_path, "t0", session_id="s1")
    registry._tasks[task.task_id] = task

    assert registry.session_has_live_run("s1")
    assert not registry.session_has_live_run("s2")

    task.done.set()
    assert not registry.session_has_live_run("s1")


async def test_resume_refuses_while_the_session_has_a_live_run(tmp_path: Path) -> None:
    """Two processes on one --resume session would fight over the same conversation state."""
    registry = TaskRegistry(log_dir=tmp_path)
    parent = make_task(tmp_path, "parent", session_id="s1", finished=True)
    sibling = make_task(tmp_path, "sibling", session_id="s1")
    registry._tasks[parent.task_id] = parent
    registry._tasks[sibling.task_id] = sibling

    with pytest.raises(SessionBusyError):
        await registry.resume(parent, "carry on")


async def test_finish_draining_abandons_pipes_held_open_after_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A grandchild holding stdout open must not make a task unfinishable."""
    monkeypatch.setattr(tasks_module, "DRAIN_GRACE_SECONDS", 0.05)
    task = make_task(tmp_path, "t0")
    never_eof = asyncio.create_task(asyncio.sleep(3600))
    task.watchers = [never_eof]

    await tasks_module._finish_draining(task)

    assert never_eof.cancelled()


async def test_finish_draining_returns_as_soon_as_the_drainers_do(tmp_path: Path) -> None:
    task = make_task(tmp_path, "t0")
    task.watchers = [asyncio.create_task(asyncio.sleep(0))]

    await asyncio.wait_for(tasks_module._finish_draining(task), timeout=1)

    assert all(watcher.done() and not watcher.cancelled() for watcher in task.watchers)


async def test_a_cancelled_monitor_still_publishes_a_terminal_status(tmp_path: Path) -> None:
    """`done` must never be observable next to a non-terminal status.

    CancelledError bypasses the monitor's `except Exception`, so the invariant relies on its
    `finally` backstop. Uses a plain `sleep` process rather than a real `claude` run.
    """
    registry = TaskRegistry(log_dir=tmp_path)
    proc = await asyncio.create_subprocess_exec(
        "sleep",
        "30",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    registry._tasks[task.task_id] = task
    task.watchers = [
        asyncio.create_task(tasks_module._drain_stdout(task, registry)),
        asyncio.create_task(tasks_module._drain_stderr(task)),
    ]

    monitor = asyncio.create_task(tasks_module._monitor(task, registry))
    await asyncio.sleep(0.05)
    monitor.cancel()
    await asyncio.gather(monitor, return_exceptions=True)

    assert task.finished
    assert task.status in TERMINAL_STATUSES

    tasks_module._signal_group(task, signal.SIGKILL)
    for watcher in task.watchers:
        watcher.cancel()
    await asyncio.gather(*task.watchers, return_exceptions=True)
    await proc.wait()


async def test_cancel_recovered_does_not_claim_success_it_cannot_deliver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Marking it cancelled when the signal failed would have later calls contradict this one."""
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="orphan",
        backend="claude",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        pid=1234,
        pgid=None,  # nothing to signal
    )
    store.write(tmp_path, record)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: True)

    result = await registry.cancel_recovered(record)

    assert result.status == "running"
    assert store.read(tmp_path, "orphan").status == "running"


async def test_cancel_recovered_of_an_already_dead_task_changes_nothing(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="orphan",
        backend="claude",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        pid=999_999_999,
        pgid=999_999_999,
    )

    assert (await registry.cancel_recovered(record)).status == "running"


async def test_session_exclusivity_spans_server_processes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Another process's live run on this session must also block a resume."""
    registry = TaskRegistry(log_dir=tmp_path)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: True)
    store.write(
        tmp_path,
        store.TaskRecord(
            task_id="elsewhere",
            backend="claude",
            session_id="shared",
            markers=["shared"],
            repo_path=str(tmp_path),
            started_at=datetime.now(timezone.utc).isoformat(),
            pid=4321,
        ),
    )

    assert registry.session_has_live_run("shared")
    assert not registry.session_has_live_run("unrelated")


async def test_resuming_a_task_whose_repo_is_gone_fails_clearly(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="old",
        backend="claude",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path / "deleted-repo"),
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
    )

    with pytest.raises(RepoUnavailableError, match="no longer exists"):
        await registry.resume_record(record, "carry on")


async def test_cancelling_a_finished_task_is_a_no_op(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path)
    task = make_task(tmp_path, "t0", finished=True)
    registry._tasks[task.task_id] = task

    await registry.cancel(task)

    assert task.status == "completed"
    assert not task.cancel_requested
