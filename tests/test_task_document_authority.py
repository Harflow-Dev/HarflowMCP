"""Authority boundaries for incompatible persisted task documents."""

from __future__ import annotations

import json
import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import pytest
from mcp import MCPError

from harflow_mcp import backends, ctl, inbox, lineage, retention, server, state_ownership, store, task_documents
from harflow_mcp import tasks as tasks_module
from harflow_mcp.events import EventLog, events_path
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import Task, TaskRegistry
from harflow_mcp.lineage import detect_caller_detail as real_detect_caller_detail


def _record(task_id: str = "task-1", **overrides: object) -> store.TaskRecord:
    values: dict[str, object] = {
        "task_id": task_id,
        "backend": "claude",
        "session_id": "session-1",
        "markers": ["session-1"],
        "repo_path": "/tmp/repo",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed",
        "exit_code": 0,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "prompt": "do the work",
    }
    values.update(overrides)
    return store.TaskRecord(**values)


def _foreign(log_dir: Path, task_id: str = "task-1") -> Path:
    path = store.record_path(log_dir, task_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"product": "polybridge", "format": "polybridge.task", "v": 1}), encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def isolated_server_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Keep direct MCP calls on a fresh owned registry even when run alone."""
    root = tmp_path / "state"
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))
    registry = TaskRegistry(log_dir=root / "tasks", open_monitor=False)
    with state_ownership.state_session(resolve_layout(root=root)):
        monkeypatch.setattr(server, "_registry", registry)
        yield


def test_ctl_status_and_batch_keep_incompatible_documents_distinct_from_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    root = tmp_path / "home" / ".harflow"
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))
    with state_ownership.state_session(resolve_layout(root=root)):
        log_dir = root / "tasks"
        store.write(log_dir, _record("good"))
        _foreign(log_dir, "bad")

        assert ctl.main(["status", "bad", "--json"]) == 1
        single = json.loads(capsys.readouterr().out)
        assert single["error"]["code"] == "task_incompatible"
        assert "unknown task" not in single["error"]["message"].lower()

        assert ctl.main(["status-batch", "--task-ids", "good,bad,missing", "--json"]) == 0
        batch = json.loads(capsys.readouterr().out)
        assert batch["errors"]["bad"]["code"] == "task_incompatible"
        assert batch["errors"]["missing"]["code"] == "unknown_task"


async def test_mcp_status_reports_typed_task_refusal_before_recovered_authority() -> None:
    log_dir = server._reg().log_dir
    _foreign(log_dir, "bad")

    with pytest.raises(MCPError) as caught:
        await server.get_task_status("bad")

    assert getattr(caught.value, "data", {}).get("code") == "task_incompatible"
    assert "unknown task_id" not in str(caught.value)


async def test_resume_record_refuses_before_launch_when_metadata_is_replaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = tmp_path / "state" / "tasks"
    log_dir.mkdir(parents=True)
    registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
    parent = Task(
        task_id="parent",
        backend="claude",
        session_id="session-parent",
        repo_path=tmp_path,
        prompt="original",
        max_turns=None,
        log_path=log_dir / "parent.jsonl",
        started_at=datetime.now(timezone.utc),
    )
    parent.enforcement = backends.get(parent.backend).enforcement(parent.freedom, parent.network).as_dict()
    registry.persist(parent)
    _foreign(log_dir, "parent")
    monkeypatch.setattr(registry, "_spawn", lambda *args, **kwargs: pytest.fail("launch attempted"))

    with pytest.raises(store.TaskDocumentError) as caught:
        await registry.resume_record(store.TaskRecord(
            task_id="parent", backend="claude", session_id="session-parent",
            repo_path=str(tmp_path), markers=["session-parent"],
            started_at=datetime.now(timezone.utc).isoformat(), status="completed",
        ), "continue")
    assert caught.value.code == "task_incompatible"


@pytest.mark.parametrize("tool", ["cancel_task", "send_message"])
async def test_mcp_mutations_refuse_replaced_metadata_before_side_effects(
    tool: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_dir = server._reg().log_dir
    _foreign(log_dir, "foreign")
    called: list[str] = []

    async def unexpected(*args, **kwargs):
        called.append(tool)
        pytest.fail("mutation side effect attempted")

    monkeypatch.setattr(server._reg(), "cancel_cascade", unexpected)
    monkeypatch.setattr(server._reg(), "send_to_record", unexpected)
    arguments = {"task_id": "foreign"}
    if tool == "send_message":
        arguments["text"] = "hello"

    with pytest.raises(MCPError) as caught:
        await server.mcp.call_tool(tool, arguments)

    assert caught.value.data["code"] == "task_incompatible"
    assert called == []


def test_lineage_does_not_establish_a_negative_from_foreign_history(tmp_path: Path) -> None:
    """A foreign task document makes caller absence undecidable for strict gates."""
    log_dir = tmp_path / "state" / "tasks"
    log_dir.mkdir(parents=True)
    _foreign(log_dir, "foreign")

    detection = real_detect_caller_detail(
        log_dir,
        environ={},
        getsid=lambda _: 4242,
        getpid=lambda: 999,
        process_table=lambda: {999: 1},
        check=lambda _: "dead",
    )

    assert detection.caller is None
    assert detection.undecidable is not None


async def test_monitor_finalization_refusal_still_reaps_closes_and_blocks_continuation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A replaced latest document cannot suppress monitor cleanup or trigger a restart."""
    root = tmp_path / "state"
    log_dir = root / "tasks"
    log_dir.mkdir(parents=True)
    registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
    task = Task(
        task_id="monitor-task",
        backend="claude",
        session_id="session-monitor",
        repo_path=tmp_path,
        prompt="finish",
        max_turns=None,
        log_path=log_dir / "monitor-task.jsonl",
        started_at=datetime.now(timezone.utc),
    )
    task.enforcement = backends.get(task.backend).enforcement(task.freedom, task.network).as_dict()
    task.auto_continue = 1
    registry._tasks[task.task_id] = task
    registry.persist(task)
    assert task.record_landed is True
    task.events = EventLog(events_path(log_dir, task.task_id), task.task_id)
    task.events.write("task_started", {"backend": task.backend})
    _foreign(log_dir, task.task_id)
    foreign_bytes = store.record_path(log_dir, task.task_id).read_bytes()
    scheduled: list[str] = []

    def unexpected_schedule(*args, **kwargs):
        scheduled.append(task.task_id)

    monkeypatch.setattr("harflow_mcp.task_continuation.schedule_automatic", unexpected_schedule)
    proc = await asyncio.create_subprocess_exec("true", start_new_session=True)
    task.proc = proc
    task.pgid = proc.pid
    try:
        await tasks_module._monitor(task, registry)
    finally:
        if proc.returncode is None:
            await proc.wait()

    assert proc.returncode == 0
    assert task.done.is_set()
    assert task.finished
    assert task.latest_record_landed is False
    assert task.record_landed is True
    assert task.persistence_error is not None
    assert task.persistence_error.code == "task_incompatible"
    assert scheduled == []
    assert store.record_path(log_dir, task.task_id).read_bytes() == foreign_bytes
    assert task.events._handle is None


def test_retention_refuses_incomplete_history_before_deleting_aged_records(tmp_path: Path) -> None:
    log_dir = tmp_path / "state" / "tasks"
    log_dir.mkdir()
    old = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
    record = _record("old", started_at=old, finished_at=old)
    store.write(log_dir, record)
    payload = store.record_path(log_dir, "old").read_bytes()
    _foreign(log_dir, "foreign")

    with pytest.raises(store.TaskDocumentError) as caught:
        retention.sweep(log_dir, 30, datetime.now(timezone.utc))
    assert caught.value.code == "task_incompatible"
    assert store.record_path(log_dir, "old").read_bytes() == payload


async def test_cancel_cascade_reports_a_replaced_descendant_without_signalling(
    tmp_path: Path,
) -> None:
    log_dir = tmp_path / "state" / "tasks"
    log_dir.mkdir(parents=True)
    registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
    root = Task(
        task_id="root", backend="claude", session_id="session-root", repo_path=tmp_path,
        prompt="root", max_turns=None, log_path=log_dir / "root.jsonl",
        started_at=datetime.now(timezone.utc),
    )
    root.enforcement = backends.get(root.backend).enforcement(root.freedom, root.network).as_dict()
    child = Task(
        task_id="child", backend="claude", session_id="session-child", repo_path=tmp_path,
        prompt="child", max_turns=None, log_path=log_dir / "child.jsonl",
        started_at=datetime.now(timezone.utc), spawned_by="root",
    )
    child.enforcement = backends.get(child.backend).enforcement(child.freedom, child.network).as_dict()
    registry._tasks.update({"root": root, "child": child})
    registry.persist(root)
    registry.persist(child)
    _foreign(log_dir, "child")

    result = await registry.cancel_cascade("root")

    assert result["history_incomplete"] is True
    assert any(item["task_id"] == "child" and item["code"] == "task_incompatible"
               for item in result["refused_tasks"])
    assert result["not_signalled"]


def test_registry_persist_does_not_replace_foreign_metadata_or_claim_latest_landed(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        log_dir = root / "tasks"
        log_dir.mkdir(parents=True)
        registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
        task = Task(
            task_id="task-1",
            backend="claude",
            session_id="session-1",
            repo_path=tmp_path,
            prompt="do the work",
            max_turns=None,
            log_path=log_dir / "task-1.jsonl",
            started_at=datetime.now(timezone.utc),
        )
        task.enforcement = backends.get(task.backend).enforcement(task.freedom, task.network).as_dict()
        registry.persist(task)
        assert task.record_landed is True
        _foreign(log_dir)
        foreign_bytes = store.record_path(log_dir, "task-1").read_bytes()
        task.status = "completed"
        task.done.set()

        registry.persist(task)

        assert task.latest_record_landed is False
        assert task.record_landed is True
        assert task.persistence_error is not None
        assert task.persistence_error.code == "task_incompatible"
        assert store.record_path(log_dir, "task-1").read_bytes() == foreign_bytes
        assert task.bridge_notices
        assert task.done.is_set()


def test_direct_read_race_does_not_leave_a_sticky_catalog_blocker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    log_dir.mkdir(parents=True)
    with state_ownership.state_session(resolve_layout(root=root)):
        original = _record("raced", title="before")
        replacement = _record("raced", title="after")
        store.write(log_dir, original)
        path = store.record_path(log_dir, original.task_id)
        real_stat = Path.stat
        raced = False

        def replace_after_initial_stat(candidate: Path, *args, **kwargs):
            nonlocal raced
            result = real_stat(candidate, *args, **kwargs)
            if candidate == path and not raced:
                raced = True
                path.write_text(json.dumps(task_documents.encode(asdict(replacement))), encoding="utf-8")
            return result

        monkeypatch.setattr(Path, "stat", replace_after_initial_stat)
        with pytest.raises(store.TaskDocumentError) as caught:
            store.read(log_dir, original.task_id, include_prompt=False)
        assert caught.value.code == "task_unavailable"
        monkeypatch.setattr(Path, "stat", real_stat)
        loaded = store.read(log_dir, original.task_id, include_prompt=False)
        assert loaded is not None and loaded.title == "after"
        page = store.list_page(log_dir, task_ids=[original.task_id])
        assert page["items"][0]["title"] == "after"
        assert not page["items"][0].get("needs_direct_lookup", False)
        assert not page.get("history_incomplete", False)


@pytest.mark.parametrize("reader", [store.brief, store.resolve_status])
def test_status_readers_refuse_a_vanished_source(tmp_path: Path, reader) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    log_dir.mkdir(parents=True)
    with state_ownership.state_session(resolve_layout(root=root)):
        record = _record("vanished")
        store.write(log_dir, record)
        store.record_path(log_dir, record.task_id).unlink()
        with pytest.raises(store.TaskDocumentError) as caught:
            reader(log_dir, record)
        assert caught.value.code == "task_unavailable"


def test_recovered_briefs_keep_unrelated_records_when_one_source_vanishes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    log_dir.mkdir(parents=True)
    registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
    with state_ownership.state_session(resolve_layout(root=root)):
        store.write(log_dir, _record("gone"))
        store.write(log_dir, _record("kept"))
        original_brief = store.brief
        vanished = False

        def race(directory, record):
            nonlocal vanished
            if record.task_id == "gone" and not vanished:
                vanished = True
                store.record_path(directory, "gone").unlink()
            return original_brief(directory, record)

        monkeypatch.setattr(store, "brief", race)
        rows = registry.recovered_briefs(set())
        by_id = {row["task_id"]: row for row in rows}
        assert by_id["kept"]["status"] == "completed"
        assert by_id["gone"]["status"] == "unknown"
        assert by_id["gone"]["history_incomplete"] is True
        assert by_id["gone"]["task_error"]["code"] == "task_unavailable"


async def test_legacy_task_list_keeps_valid_rows_when_recovered_source_vanishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    log_dir.mkdir(parents=True)
    registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
    monkeypatch.setattr(server, "_registry", registry)
    with state_ownership.state_session(resolve_layout(root=root)):
        store.write(log_dir, _record("gone"))
        store.write(log_dir, _record("kept"))
        original_brief = store.brief
        vanished = False

        def race(directory, record):
            nonlocal vanished
            if record.task_id == "gone" and not vanished:
                vanished = True
                store.record_path(directory, "gone").unlink()
            return original_brief(directory, record)

        monkeypatch.setattr(store, "brief", race)
        result = await server.list_tasks()
        rows = {row["task_id"]: row for row in result}
        assert rows["kept"]["status"] == "completed"
        assert rows["gone"]["status"] == "unknown"
        assert rows["gone"]["history_incomplete"] is True


def test_stale_terminal_write_reports_no_error_while_durability_is_false(tmp_path: Path) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    log_dir.mkdir(parents=True)
    with state_ownership.state_session(resolve_layout(root=root)):
        settled = _record("settled", status="completed", exit_code=0)
        stale = _record("settled", status="running", exit_code=None, finished_at=None)
        assert store.write_landed(log_dir, settled)
        assert not store.write_landed(log_dir, stale)
        assert store.last_write_error() is None
        assert store.read(log_dir, "settled", include_prompt=False).status == "completed"


def test_pending_messages_taken_before_metadata_guard_are_reported_undelivered(tmp_path: Path) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    log_dir.mkdir(parents=True)
    with state_ownership.state_session(resolve_layout(root=root)):
        record = _record("send-race", status="running", exit_code=None)
        store.write(log_dir, record)
        task = Task(task_id=record.task_id, backend="claude", session_id="session-1", repo_path=tmp_path,
                    prompt="do the work", max_turns=None, log_path=log_dir / "send-race.jsonl",
                    started_at=datetime.now(timezone.utc), live_input=True)
        task.events = EventLog(events_path(log_dir, task.task_id), task.task_id)
        task.inbox_queue.append(inbox.make_message("queued", None))
        registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
        pending = registry._take_pending(task)
        assert len(pending) == 1 and not task.inbox_queue and task.inbox_offset == 0
        _foreign(log_dir, task.task_id)

        class Stdin:
            def __init__(self):
                self.written: list[bytes] = []
            def write(self, data: bytes) -> None:
                self.written.append(data)

        stdin = Stdin()
        task.proc = type("Proc", (), {"stdin": stdin})()
        with pytest.raises(store.TaskDocumentError) as caught:
            registry._write_messages(task, pending)
        assert caught.value.code == "task_incompatible"
        assert stdin.written == []
        task.events.close()
        rows = [json.loads(line) for line in events_path(log_dir, task.task_id).read_text().splitlines()]
        assert rows[-1]["kind"] == "undelivered"
        assert rows[-1]["message_id"] == pending[0]["id"]
        assert task.bridge_notices and "was not delivered" in task.bridge_notices[-1]
