"""Deterministic caller-cancellation boundaries for multi-task waits."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from harflow_mcp import server, store, task_waiting, wait_policy
from harflow_mcp.backends.codex import CodexBackend
from harflow_mcp.tasks import Task

TEST_GUARD_SECONDS = 5


def _live_task(task_id, tmp_path):
    task = Task(
        task_id=task_id,
        backend="codex",
        session_id=None,
        repo_path=tmp_path,
        prompt="test",
        max_turns=None,
        log_path=tmp_path / f"{task_id}.jsonl",
        started_at=datetime.now(timezone.utc),
        status="running",
        enforcement=CodexBackend().enforcement("write_in_repo").as_dict(),
    )
    server._reg()._tasks[task_id] = task
    server._reg().persist(task)
    assert task.latest_record_landed is True
    return task


def _record(task_id, tmp_path, status="running"):
    record = store.TaskRecord(
        task_id=task_id,
        backend="codex",
        session_id=None,
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        status=status,
        exit_code=0 if status == "completed" else None,
        enforcement=CodexBackend().enforcement("write_in_repo").as_dict(),
    )
    assert store.write_landed(server._reg().log_dir, record)
    return record


async def _assert_cancelled(caller, evidence):
    try:
        await caller
    except asyncio.CancelledError:
        evidence["propagated"] = True
    else:
        evidence["propagated"] = False
    assert evidence.get("cancel_accepted") is True, evidence
    assert evidence.get("caller_pending") is True, evidence
    assert evidence.get("propagated") is True, evidence


def _cancel_on_to_thread(monkeypatch, predicate, evidence):
    original = asyncio.to_thread

    def to_thread(func, *args, **kwargs):
        if evidence.get("injected") or not predicate(func):
            return original(func, *args, **kwargs)
        owner = asyncio.current_task()
        loop = asyncio.get_running_loop()
        pending = loop.create_future()
        evidence["injected"] = True

        def complete_then_cancel():
            result = func(*args, **kwargs)
            assert result is not None
            pending.set_result(result)
            evidence["caller_pending"] = not owner.done()
            evidence["cancel_accepted"] = owner.cancel()

        loop.call_soon(complete_then_cancel)
        return pending

    monkeypatch.setattr(task_waiting.asyncio, "to_thread", to_thread)


@pytest.mark.asyncio
async def test_live_metadata_completion_preserves_caller_cancellation(tmp_path, monkeypatch):
    _live_task("live", tmp_path)
    evidence = {}
    _cancel_on_to_thread(monkeypatch, lambda func: func is store.read, evidence)
    caller = asyncio.create_task(server.wait_for_tasks(["live"], timeout_seconds=0))
    await _assert_cancelled(caller, evidence)
    task = server._reg().get("live")
    assert task.status == "running"
    assert not task.cancel_requested


@pytest.mark.asyncio
async def test_recovered_metadata_completion_preserves_caller_cancellation(tmp_path, monkeypatch):
    _record("recovered", tmp_path)
    registry = SimpleNamespace(get=lambda _task_id: None, log_dir=server._reg().log_dir)
    evidence = {}
    _cancel_on_to_thread(monkeypatch, lambda func: func is task_waiting._recovered, evidence)
    caller = asyncio.create_task(task_waiting.wait_for_tasks(registry, ["recovered"], timeout_seconds=0))
    await _assert_cancelled(caller, evidence)


@pytest.mark.asyncio
async def test_completed_existence_gather_preserves_caller_cancellation(tmp_path, monkeypatch):
    _record("recovered", tmp_path)
    registry = SimpleNamespace(get=lambda _task_id: None, log_dir=server._reg().log_dir)
    evidence = {}
    original_gather = asyncio.gather

    def gather(*aws, **kwargs):
        aggregate = original_gather(*aws, **kwargs)
        if not evidence.get("injected"):
            owner = asyncio.current_task()

            def cancel_after_completion(future):
                if future.cancelled():
                    return
                evidence["injected"] = True
                evidence["caller_pending"] = not owner.done()
                evidence["cancel_accepted"] = owner.cancel()

            aggregate.add_done_callback(cancel_after_completion)
        return aggregate

    monkeypatch.setattr(task_waiting.asyncio, "gather", gather)
    caller = asyncio.create_task(task_waiting.wait_for_tasks(registry, ["recovered"], timeout_seconds=0))
    await _assert_cancelled(caller, evidence)


@pytest.mark.asyncio
async def test_progress_callback_completion_preserves_caller_cancellation(tmp_path, monkeypatch):
    _live_task("live", tmp_path)
    monkeypatch.setattr(wait_policy, "PROGRESS_INTERVAL_SECONDS", 0)
    evidence = {}
    callback_started = asyncio.Event()

    def progress(*_args, **_kwargs):
        owner = asyncio.current_task()
        loop = asyncio.get_running_loop()
        pending = loop.create_future()
        callback_started.set()

        def complete_then_cancel():
            pending.set_result(None)
            evidence["caller_pending"] = not owner.done()
            evidence["cancel_accepted"] = owner.cancel()

        loop.call_soon(complete_then_cancel)
        return pending

    monkeypatch.setattr(task_waiting, "POLL_SECONDS", 0.01)
    caller = asyncio.create_task(
        task_waiting.wait_for_tasks(server._reg(), ["live"], timeout_seconds=1, progress=progress)
    )
    await asyncio.wait_for(asyncio.shield(callback_started.wait()), 1)
    await _assert_cancelled(caller, evidence)
    task = server._reg().get("live")
    assert task.status == "running"
    assert not task.cancel_requested


@pytest.mark.asyncio
async def test_observation_round_cancellation_cleans_only_its_observers(tmp_path, monkeypatch):
    task = _live_task("live", tmp_path)
    original_read = store.read
    first_ready = asyncio.Event()
    sibling_ready = asyncio.Event()
    first_release = None
    first_owner = None
    release_done = asyncio.Event()
    blocked = set()
    evidence = {}
    observer_owners = set()
    original_create_task = task_waiting.asyncio.create_task

    def create_task(coro, *args, **kwargs):
        owner = asyncio.current_task()
        is_event_wait = getattr(coro, "cr_code", None) is not None and coro.cr_code.co_name == "wait"
        result = original_create_task(coro, *args, **kwargs)
        if is_event_wait:
            observer_owners.add(owner)
        return result

    monkeypatch.setattr(task_waiting.asyncio, "create_task", create_task)

    def to_thread(func, *args, **kwargs):
        nonlocal first_release, first_owner
        owner = asyncio.current_task()
        if func is original_read and owner in observer_owners and owner not in blocked:
            blocked.add(owner)
            if first_release is None:
                first_owner = owner
                first_release = asyncio.get_running_loop().create_future()
                first_ready.set()
                return first_release
            sibling_ready.set()
        return original_to_thread(func, *args, **kwargs)

    original_to_thread = asyncio.to_thread
    monkeypatch.setattr(task_waiting.asyncio, "to_thread", to_thread)
    # The first caller's round read is deliberately released before the
    # owner cancellation, while its sibling observer remains installed.
    first = asyncio.create_task(server.wait_for_tasks(["live"], timeout_seconds=1))
    sibling = None
    try:
        await asyncio.wait_for(asyncio.shield(first_ready.wait()), TEST_GUARD_SECONDS)
        sibling = asyncio.create_task(server.wait_for_tasks(["live"], timeout_seconds=TEST_GUARD_SECONDS))
        await asyncio.wait_for(asyncio.shield(sibling_ready.wait()), TEST_GUARD_SECONDS)

        def release_and_cancel():
            first_release.set_result(original_read(server._reg().log_dir, "live", include_prompt=False,
                                                    metadata_byte_limit=task_waiting.METADATA_BYTES))
            evidence["caller_pending"] = not first_owner.done()
            evidence["cancel_accepted"] = first_owner.cancel()
            release_done.set()

        asyncio.get_running_loop().call_soon(release_and_cancel)
        await release_done.wait()
        finished, _ = await asyncio.wait({first}, timeout=TEST_GUARD_SECONDS)
        assert finished, f"accepted cancellation did not complete caller: {evidence}"
        active_observers = [waiter for waiter in task.done._waiters if not waiter.cancelled()]
        assert len(active_observers) == 1, "cancelled wait did not clean only its own observer"
        try:
            first.result()
        except asyncio.CancelledError:
            evidence["propagated"] = True
        else:
            evidence["propagated"] = False
        assert evidence["cancel_accepted"] is True
        assert evidence["caller_pending"] is True
        assert evidence["propagated"] is True
        assert task.status == "running"
        assert not task.cancel_requested
        assert task.done._waiters, "sibling observer was cleaned up by the cancelled wait"

        task.status = "completed"
        task.done.set()
        result = await sibling
        assert result["terminal_task_ids"] == ["live"]
        assert not task.done._waiters
    finally:
        for caller in (first, sibling):
            if caller is not None and not caller.done():
                caller.cancel()
        await asyncio.gather(first, sibling, return_exceptions=True)
