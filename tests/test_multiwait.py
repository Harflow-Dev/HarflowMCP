"""No-model multi-task wait contracts."""
import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from mcp import MCPError

from polybridge import server, store, task_waiting
from polybridge.tasks import Task


def add_task(task_id, tmp_path, status="running"):
    task = Task(task_id=task_id, backend="codex", session_id=None, repo_path=tmp_path,
                prompt="x", max_turns=None, log_path=tmp_path / f"{task_id}.jsonl",
                started_at=datetime.now(timezone.utc), status=status)
    if status != "running":
        task.done.set()
    server._reg()._tasks[task_id] = task
    return task


@pytest.mark.parametrize("ids,mode,timeout", [([], "any", 0), (["a", "a"], "any", 0), (["a"]*33, "all", 0), (["a"], "bad", 0), (["a"], "any", -1), (["a"], "any", 301), (["a"], "any", True), ([""], "any", 0)])
async def test_invalid(ids, mode, timeout):
    with pytest.raises(MCPError):
        await server.wait_for_tasks(ids, mode, timeout)


async def test_order_terminal_and_timeout_cap(tmp_path):
    add_task("b", tmp_path)
    add_task("a", tmp_path, "completed")
    result = await server.wait_for_tasks(["b", "a"], timeout_seconds=300)
    assert [row["task_id"] for row in result["tasks"]] == ["b", "a"]
    assert result["terminal_task_ids"] == ["a"]
    assert result["effective_timeout_seconds"] == 45
    assert not result["timed_out"]
    assert (await server.wait_for_tasks(["b", "a"], "all", 0))["timed_out"]


@pytest.mark.parametrize("requested,effective", [(0, 0), (1, 1), (300, 45)])
async def test_initial_io_does_not_consume_observation_interval(monkeypatch, requested, effective):
    clock = [100.0]
    monkeypatch.setattr(task_waiting.asyncio, "get_running_loop",
                        lambda: SimpleNamespace(time=lambda: clock[0]))
    source = SimpleNamespace(backend="codex", session_id=None, title=None)
    reads = []
    sleeps = []

    def exists(*args):
        clock[0] += .75
        return True

    def read(*args):
        reads.append(clock[0])
        # Initial materialization and the first observation both take time.
        clock[0] += .75 if len(reads) == 1 else .25
        return source, "running", None, "recovered_record"

    async def to_thread(func, *args):
        return func(*args)

    async def sleep(interval):
        sleeps.append(interval)
        clock[0] += interval

    monkeypatch.setattr(task_waiting, "_record_exists", exists)
    monkeypatch.setattr(task_waiting, "_recovered", read)
    monkeypatch.setattr(task_waiting.asyncio, "to_thread", to_thread)
    monkeypatch.setattr(task_waiting.asyncio, "sleep", sleep)
    registry = SimpleNamespace(get=lambda _: None, log_dir=None)
    result = await task_waiting.wait_for_tasks(registry, ["recovered"], timeout_seconds=requested)
    assert result["timed_out"]
    assert result["effective_timeout_seconds"] == effective
    assert reads[1] == 101.5  # Existence and initial materialization precede the interval.
    assert clock[0] >= 101.5 + effective
    assert bool(sleeps) is (effective > 0)


async def test_missing_validates_before_observers(tmp_path):
    live = add_task("live", tmp_path)
    with pytest.raises(MCPError, match="unknown task_id"):
        await server.wait_for_tasks(["live", "missing"])
    assert not live.done._waiters
    assert live.status == "running"


async def test_all_guards_precede_reads(monkeypatch):
    guarded = []
    async def guard(task_id):
        guarded.append(task_id)
        if task_id == "forbidden":
            raise ValueError("forbidden")
    monkeypatch.setattr(server, "_guard_task_read", guard)
    monkeypatch.setattr(server._reg(), "get", lambda _: pytest.fail("read before authorization"))
    with pytest.raises(MCPError, match="forbidden"):
        await server.wait_for_tasks(["ok", "forbidden"])
    assert guarded == ["ok", "forbidden"]


async def test_concurrent_waits_and_caller_cancellation(tmp_path):
    a = add_task("a", tmp_path)
    b = add_task("b", tmp_path)
    first = asyncio.create_task(server.wait_for_tasks(["a", "b"], "any"))
    second = asyncio.create_task(server.wait_for_tasks(["a", "b"], "all"))
    await asyncio.sleep(.01)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert a.status == b.status == "running"
    assert not a.cancel_requested and not b.cancel_requested
    a.status = "completed"
    a.done.set()
    await asyncio.sleep(.01)
    assert not second.done()
    b.status = "failed"
    b.done.set()
    assert (await second)["terminal_task_ids"] == ["a", "b"]
    assert not a.done._waiters and not b.done._waiters


async def test_recovered_uses_bounded_headers(tmp_path, monkeypatch):
    record = store.TaskRecord(task_id="r", backend="codex", session_id=None,
                              repo_path=str(tmp_path), started_at=datetime.now(timezone.utc).isoformat(),
                              status="failed", exit_code=None, prompt_truncated=True,
                              prompt_source="r.prompt.unavailable.txt")
    store.write(server._reg().log_dir, record)
    monkeypatch.setattr(store, "resolve_status", lambda *a, **kw: pytest.fail("stream replay forbidden"))
    monkeypatch.setattr(server._reg(), "recover", lambda *a: pytest.fail("unbounded recovery forbidden"))
    calls = []
    original = store.read
    def read(*args, **kwargs):
        calls.append(kwargs)
        return original(*args, **kwargs)
    monkeypatch.setattr(store, "read", read)
    result = await server.wait_for_tasks(["r"], timeout_seconds=0)
    assert result["tasks"][0]["status"] == "unknown"
    assert result["terminal_task_ids"] == []
    assert all(call["include_prompt"] is False and call["metadata_byte_limit"] for call in calls)
    assert result["tasks"][0]["status_source"] == "recovered_liveness"


async def test_progress_and_size(tmp_path, monkeypatch):
    monkeypatch.setattr(task_waiting, "PROGRESS_SECONDS", 0)
    monkeypatch.setattr(task_waiting, "POLL_SECONDS", .001)
    tasks = [add_task(f"task-{i}", tmp_path) for i in range(32)]
    for task in tasks:
        task.title = "\x00" * 10000
        task.acc.summary = "😀" * 10000
    progress = []
    async def report(*args, **kwargs):
        progress.append(args)
        for task in tasks:
            task.status = "completed"
            task.done.set()
    result = await task_waiting.wait_for_tasks(server._reg(), [task.task_id for task in tasks], "all", 1, report)
    assert progress
    assert len(json.dumps(result).encode()) <= 65536
    assert all("last_output_tail" not in row for row in result["tasks"])


async def test_invalid_path_id():
    with pytest.raises(MCPError):
        await server.wait_for_tasks(["../escape"], timeout_seconds=0)


async def test_disappearing_recovered_record(monkeypatch):
    monkeypatch.setattr(task_waiting, "_record_exists", lambda *a: True)
    source = SimpleNamespace(backend="codex", session_id=None, title=None)
    records = iter([(source, "running", None, "recovered_liveness"), None])
    monkeypatch.setattr(task_waiting, "_recovered", lambda *a: next(records))
    result = await server.wait_for_tasks(["r"], timeout_seconds=0)
    assert result["tasks"][0]["status"] == "unavailable"
    assert not result["terminal_task_ids"]


async def test_recovered_refresh_with_live_completion(tmp_path, monkeypatch):
    live = add_task("live", tmp_path)
    monkeypatch.setattr(task_waiting, "_record_exists", lambda *a: True)
    source = SimpleNamespace(backend="codex", session_id=None, title=None)
    monkeypatch.setattr(task_waiting, "POLL_SECONDS", .001)
    polls = []
    def read(*a):
        polls.append(True)
        status = "completed" if len(polls) > 2 else "running"
        if len(polls) > 2:
            live.status = "completed"
            live.done.set()
        return source, status, None, "recovered_record"
    monkeypatch.setattr(task_waiting, "_recovered", read)
    result = await server.wait_for_tasks(["live", "recovered"], "all", 1)
    assert result["terminal_task_ids"] == ["live", "recovered"]


async def test_terminal_live_status_requires_settlement(tmp_path):
    task = add_task("a", tmp_path)
    task.status = "completed"
    result = await server.wait_for_tasks(["a"], timeout_seconds=0)
    assert result["tasks"][0]["status"] == "running"
    assert result["tasks"][0]["status_source"] == "live_settling"
    assert not result["terminal_task_ids"]
    task.done.set()
    assert (await server.wait_for_tasks(["a"], timeout_seconds=0))["terminal_task_ids"] == ["a"]


async def test_size_adversarial_maximum_fields(tmp_path):
    ids = ["a" * 61 + str(i).zfill(3) for i in range(32)]
    for task_id in ids:
        task = add_task(task_id, tmp_path, "completed")
        task.title = "\x00" * 10000
        task.session_id = "\x00" * 10000
        task.acc.summary = "\x00" * 10000
    result = await server.wait_for_tasks(ids, "all", 0)
    assert len(json.dumps(result).encode()) < 65536


async def test_observed_recovered_terminal_avoids_detail(tmp_path, monkeypatch):
    record = store.TaskRecord(task_id="r", backend="codex", session_id=None,
                              repo_path=str(tmp_path), started_at=datetime.now(timezone.utc).isoformat(),
                              status="completed", exit_code=0)
    store.write(server._reg().log_dir, record)
    monkeypatch.setattr(store, "resolve_status", lambda *a, **kw: pytest.fail("detail replay forbidden"))
    result = await server.wait_for_tasks(["r"], timeout_seconds=0)
    assert result["terminal_task_ids"] == ["r"]
    assert result["tasks"][0]["status_source"] == "recovered_record"


async def test_oversized_metadata_unavailable(tmp_path):
    record = store.TaskRecord(task_id="r", backend="codex", session_id=None,
                              repo_path=str(tmp_path), started_at=datetime.now(timezone.utc).isoformat(),
                              prompt="x" * (task_waiting.METADATA_BYTES + 1))
    store.write(server._reg().log_dir, record)
    # Write a deliberately oversized persisted metadata record, bypassing prompt compaction.
    path = store.record_path(server._reg().log_dir, "r")
    raw = json.loads(path.read_text())
    raw["prompt"] = "x" * (task_waiting.METADATA_BYTES + 1)
    path.write_text(json.dumps(raw))
    result = await server.wait_for_tasks(["r"], timeout_seconds=0)
    assert result["tasks"][0]["status"] == "unavailable"
    assert not result["terminal_task_ids"]

async def test_corrupt_existing_metadata_is_unavailable():
    registry = server._reg()
    registry.log_dir.mkdir(parents=True, exist_ok=True)
    store.record_path(registry.log_dir, 'corrupt').write_text('{not json')
    result = await server.wait_for_tasks(['corrupt'], timeout_seconds=0)
    assert result['tasks'][0]['status'] == 'unavailable'
    assert result['terminal_task_ids'] == []


async def test_missing_after_slow_metadata_is_rejected_before_reads(tmp_path, monkeypatch):
    live = add_task("live", tmp_path)
    registry = server._reg()
    registry.log_dir.mkdir(parents=True, exist_ok=True)
    store.record_path(registry.log_dir, "early").write_text("{}")
    monkeypatch.setattr(task_waiting, "OBSERVATION_IO_SECONDS", .02)
    import threading
    release = threading.Event()
    reads = []
    # On the old path this read exhausted the budget, hiding the later missing ID.
    def read(*args):
        reads.append(True)
        release.wait(1)
        return None
    monkeypatch.setattr(task_waiting, "_recovered", read)
    try:
        with pytest.raises(MCPError, match="unknown task_id: missing"):
            await server.wait_for_tasks(["live", "early", "missing"], timeout_seconds=0)
    finally:
        release.set()
    assert not reads
    assert not live.done._waiters
    assert live.status == "running" and not live.cancel_requested


async def test_existing_metadata_after_budget_is_unavailable(monkeypatch):
    import threading
    release = threading.Event()
    calls = []
    monkeypatch.setattr(task_waiting, "OBSERVATION_IO_SECONDS", .01)
    monkeypatch.setattr(task_waiting, "_record_exists", lambda *a: True)
    def read(directory, task_id):
        calls.append(task_id)
        release.wait(1)
        raise PermissionError("unreadable")
    monkeypatch.setattr(task_waiting, "_recovered", read)
    try:
        result = await server.wait_for_tasks(["early", "later"], timeout_seconds=0)
    finally:
        release.set()
    assert calls and set(calls) == {"early"}
    assert [row["status"] for row in result["tasks"]] == ["unavailable", "unavailable"]
    assert not result["terminal_task_ids"]


@pytest.mark.parametrize("failure", ["timeout", "permission"])
async def test_undecidable_existence_fails_before_observation(tmp_path, monkeypatch, failure):
    import threading
    live = add_task("live", tmp_path)
    release = threading.Event()
    monkeypatch.setattr(task_waiting, "OBSERVATION_IO_SECONDS", .01)
    def exists(*args):
        if failure == "timeout":
            release.wait(1)
            return True
        raise PermissionError("stat denied")
    monkeypatch.setattr(task_waiting, "_record_exists", exists)
    monkeypatch.setattr(task_waiting, "_recovered", lambda *a: pytest.fail("metadata before existence validation"))
    try:
        with pytest.raises(MCPError, match="Unable to validate task existence"):
            await server.wait_for_tasks(["live", "unvalidated"], timeout_seconds=0)
    finally:
        release.set()
    assert not live.done._waiters
    assert live.status == "running" and not live.cancel_requested
