"""Deterministic regressions for metadata replacement during observations."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from harflow_mcp import (
    bounded_io,
    catalog,
    ctl,
    identity,
    inbox,
    state_ownership,
    store,
    task_documents,
    task_reads,
    task_supervision,
    workflow_hooks,
    workflows,
)
from harflow_mcp.state_layout import resolve_layout


def _record(task_id: str = "race", **changes: object) -> store.TaskRecord:
    values: dict[str, object] = {
        "task_id": task_id,
        "backend": "codex",
        "session_id": "session-1",
        "repo_path": "/tmp/repo",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "running",
        "exit_code": None,
        "markers": ["session-1"],
    }
    return store.TaskRecord(**(values | changes))


@pytest.fixture
def task_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "state" / "tasks"
    with state_ownership.state_session(resolve_layout(root=tmp_path / "state")):
        directory.mkdir(parents=True)
        yield directory


def _marked(value: object = True) -> task_documents.TaskDocumentError:
    error = task_documents.TaskDocumentError("race", "Task metadata changed while reading", code="task_unavailable")
    error._source_changed_during_read = value
    return error


def _unmarked() -> task_documents.TaskDocumentError:
    return task_documents.TaskDocumentError("race", "unmarked metadata failure", code="task_unavailable")


@pytest.mark.parametrize("marker", [False, 1, "yes", object()])
def test_only_boolean_true_source_change_marker_is_retryable(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch, marker: object
) -> None:
    calls = 0

    def read(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        raise _marked(marker)

    monkeypatch.setattr(store, "read", read)
    with pytest.raises(store.TaskDocumentError):
        task_supervision.read_task_record(task_dir, "race")
    assert calls == 1


def test_marked_replacement_is_attempted_at_most_three_times_and_preserves_final_error(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    errors = [_marked(True) for _ in range(3)]
    calls = 0

    def read(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        raise errors[calls - 1]

    monkeypatch.setattr(store, "read", read)
    with pytest.raises(store.TaskDocumentError) as raised:
        task_supervision.read_task_record(task_dir, "race")
    assert calls == 3
    assert raised.value is errors[-1]


@pytest.mark.parametrize("failure", [_unmarked(), ValueError("malformed"), OSError("unavailable")])
def test_unmarked_or_unrelated_failures_are_not_retried(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch, failure: Exception
) -> None:
    calls = 0

    def read(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        raise failure

    monkeypatch.setattr(store, "read", read)
    with pytest.raises(type(failure)):
        task_supervision.read_task_record(task_dir, "race")
    assert calls == 1


def test_verified_absence_is_returned_without_retry(task_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def read(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(store, "read", read)
    assert task_supervision.read_task_record(task_dir, "race") is None
    assert calls == 1


def test_read_limit_is_not_retried_or_budget_reset(task_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    before = _record()
    after = replace(before, status="completed", exit_code=0)
    assert store.write_landed(task_dir, before)
    path = store.record_path(task_dir, before.task_id)
    size = path.stat().st_size
    budget = catalog.MetadataBudget(metadata_limit=size)
    real_read_json = bounded_io.read_json
    reads = 0
    injected = False

    def race(candidate: Path, limit: int, *, budget=None):
        nonlocal reads, injected
        if candidate == path:
            reads += 1
        value = real_read_json(candidate, limit, budget=budget)
        if candidate == path and not injected:
            injected = True
            assert store.write_landed(task_dir, after)
        return value

    monkeypatch.setattr(bounded_io, "read_json", race)
    with pytest.raises(bounded_io.ReadLimit):
        task_supervision.read_task_record(
            task_dir, before.task_id, include_prompt=False,
            metadata_byte_limit=1024 * 1024, metadata_budget=budget,
        )
    assert reads == 2
    assert budget.metadata_bytes == size
    assert budget.metadata_decodes == 1
    assert injected


def test_metadata_read_options_are_forwarded_without_resetting_shared_budget(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    budget = object()
    options = {"include_prompt": False, "metadata_byte_limit": 128, "metadata_budget": budget}
    seen: list[dict[str, object]] = []
    expected = _record()

    def read(*args: object, **kwargs: object):
        seen.append(kwargs)
        return expected

    monkeypatch.setattr(store, "read", read)
    assert task_supervision.read_task_record(task_dir, "race", **options) == expected
    assert seen == [options]


def test_bounded_replacement_is_retried_at_bounded_io_injection_point(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = _record()
    after = replace(before, status="completed", exit_code=0, finished_at="2026-10-26T00:00:00+00:00")
    assert store.write_landed(task_dir, before)
    path = store.record_path(task_dir, before.task_id)
    original_raw = path.read_text(encoding="utf-8")
    real_read_json = bounded_io.read_json
    injected = False

    def race(candidate: Path, limit: int, *, budget=None):
        nonlocal injected
        raw = real_read_json(candidate, limit, budget=budget)
        if candidate == path and not injected:
            injected = True
            assert store.write_landed(task_dir, after)
        return raw

    monkeypatch.setattr(bounded_io, "read_json", race)
    budget = catalog.MetadataBudget(metadata_limit=1024 * 1024)
    with pytest.raises(store.TaskDocumentError) as direct:
        store.read(task_dir, before.task_id, include_prompt=False, metadata_byte_limit=1024 * 1024, metadata_budget=budget)
    assert getattr(direct.value, "_source_changed_during_read", None) is True
    assert injected
    path.write_text(original_raw, encoding="utf-8")
    injected = False
    budget = catalog.MetadataBudget(metadata_limit=1024 * 1024)
    result = task_supervision.read_task_record(
        task_dir,
        before.task_id,
        include_prompt=False, metadata_byte_limit=1024 * 1024, metadata_budget=budget,
    )
    assert result == after
    assert injected


def test_attachable_discards_replaced_running_sample_before_observing_terminal_record(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = _record()
    after = replace(before, status="completed", exit_code=0, output_drained=True, finished_at="2026-10-26T00:00:00+00:00")
    assert store.write_landed(task_dir, before)
    path = store.record_path(task_dir, before.task_id)
    real_read_text = Path.read_text
    injected = False

    def race(candidate: Path, *args: object, **kwargs: object) -> str:
        nonlocal injected
        raw = real_read_text(candidate, *args, **kwargs)
        if candidate == path and not injected:
            injected = True
            assert store.write_landed(task_dir, after)
        return raw

    monkeypatch.setattr(Path, "read_text", race)
    assert task_supervision.attachable(task_dir, before.task_id) is True
    assert injected


def test_registry_attach_returns_fresh_handle_without_launching_execution(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from harflow_mcp.tasks import TaskRegistry

    before = _record()
    after = replace(before, status="running", session_id="fresh-session")
    assert store.write_landed(task_dir, before)
    path = store.record_path(task_dir, before.task_id)
    real_read_text = Path.read_text
    injected = False

    def race(candidate: Path, *args: object, **kwargs: object) -> str:
        nonlocal injected
        raw = real_read_text(candidate, *args, **kwargs)
        if candidate == path and not injected:
            injected = True
            assert store.write_landed(task_dir, after)
        return raw

    monkeypatch.setattr(Path, "read_text", race)
    registry = TaskRegistry(log_dir=task_dir, owner={"pid": 1, "start_time": "test", "markers": []})
    attached = registry.attach(before.task_id)
    assert attached is not None
    assert attached.task_id == before.task_id
    assert attached.record().session_id == "fresh-session"
    assert registry._tasks == {}
    assert not list(task_dir.glob("*.launch.json"))
    assert injected


def test_task_association_uses_fresh_parent_after_replacement_and_keeps_active_workflow_gate(
    task_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = workflows.WorkflowStore(tmp_path / "state")
    definition = workflows.validate_definition({
        "name": "race-workflow",
        "nodes": [{"id": "start", "type": "start"}, {"id": "work", "type": "agent", "role": "task", "agent": {"backend": "codex"}}, {"id": "end", "type": "end"}],
        "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}],
    })
    run = storage.create_run(definition, "assignment", tmp_path)
    parent_id = "parent"
    child_id = "child"
    storage.update_run(run["workflow_run_id"], lambda value: value.update(
        status="running",
        activations=[{"id": "activation", "node_id": "work", "role": "node", "status": "running",
                      "created_at": 0.0, "tasks": [{"task_id": parent_id, "status": "running"}]}],
    ), "fixture")
    before = _record(child_id)
    after = replace(before, parent_task_id=parent_id)
    assert store.write_landed(task_dir, before)
    path = store.record_path(task_dir, child_id)
    real_read_text = Path.read_text
    injected = False

    def race(candidate: Path, *args: object, **kwargs: object) -> str:
        nonlocal injected
        raw = real_read_text(candidate, *args, **kwargs)
        if candidate == path and not injected:
            injected = True
            assert store.write_landed(task_dir, after)
        return raw

    monkeypatch.setattr(Path, "read_text", race)
    association = storage.task_association(child_id)
    assert association and association["workflow_run_id"] == run["workflow_run_id"]
    assert injected
    with pytest.raises(inbox.SendRefused, match="active workflow"):
        workflow_hooks.refuse_direct_message(task_dir, child_id)


def test_locked_send_retries_replacement_and_appends_once(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = {"pid": 1, "start_time": "test", "markers": ["owner"]}
    before = _record(owner=owner, live_input=True)
    after = replace(before, status="running")
    assert store.write_landed(task_dir, before)
    path = store.record_path(task_dir, before.task_id)
    real_read_text = Path.read_text
    injected = False
    monkeypatch.setattr(identity, "identity_check", lambda value: "alive")
    monkeypatch.setattr(workflow_hooks, "refuse_direct_message", lambda *args, **kwargs: None)

    def race(candidate: Path, *args: object, **kwargs: object) -> str:
        nonlocal injected
        raw = real_read_text(candidate, *args, **kwargs)
        if candidate == path and not injected:
            injected = True
            assert store.write_landed(task_dir, after)
        return raw

    monkeypatch.setattr(Path, "read_text", race)
    result = inbox.send_to_record(task_dir, before.task_id, "hello", by=None)
    assert result["status"] == "queued"
    messages, _ = inbox.read_new(task_dir, before.task_id, 0)
    assert len(messages) == 1 and messages[0]["text"] == "hello"
    assert injected


def test_locked_send_persistent_replacement_does_not_append_or_acknowledge(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = {"pid": 1, "start_time": "test", "markers": ["owner"]}
    record = _record(owner=owner, live_input=True)
    assert store.write_landed(task_dir, record)
    path = store.record_path(task_dir, record.task_id)
    real_read_text = Path.read_text
    replacements = 0
    writing = False
    monkeypatch.setattr(identity, "identity_check", lambda value: "alive")
    monkeypatch.setattr(workflow_hooks, "refuse_direct_message", lambda *args, **kwargs: None)

    def persistent_race(candidate: Path, *args: object, **kwargs: object) -> str:
        nonlocal replacements, writing
        raw = real_read_text(candidate, *args, **kwargs)
        if candidate == path and not writing:
            replacements += 1
            writing = True
            try:
                assert store.write_landed(task_dir, replace(record, status="running"))
            finally:
                writing = False
        return raw

    monkeypatch.setattr(Path, "read_text", persistent_race)
    with pytest.raises(store.TaskDocumentError) as raised:
        inbox.send_to_record(task_dir, record.task_id, "never", by=None)
    assert getattr(raised.value, "_source_changed_during_read", None) is True
    assert replacements == 3
    inbox_file = inbox.inbox_path(task_dir, record.task_id)
    assert not inbox_file.exists() or inbox_file.read_bytes() == b""


def test_list_tasks_refreshes_fresh_header_after_bounded_replacement(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = _record()
    after = replace(before, status="completed", exit_code=0, finished_at="2026-10-26T00:00:00+00:00")
    assert store.write_landed(task_dir, before)
    path = store.record_path(task_dir, before.task_id)
    real_read_text = Path.read_text
    injected = False

    def race(candidate: Path, *args: object, **kwargs: object) -> str:
        nonlocal injected
        value = real_read_text(candidate, *args, **kwargs)
        if candidate == path and not injected:
            injected = True
            assert store.write_landed(task_dir, after)
        return value

    monkeypatch.setattr(Path, "read_text", race)
    recovered = task_supervision.read_task_record(task_dir, before.task_id)
    assert recovered == after and injected
    monkeypatch.setattr(Path, "read_text", real_read_text)
    page = task_reads.list_tasks(task_dir)
    page["items"] = [item for item in page["items"] if item["task_id"] == before.task_id]
    assert page["items"][0]["status"] == "completed"
    assert not page["items"][0].get("needs_direct_lookup")
    assert injected


def test_status_snapshot_uses_fresh_record_after_bounded_replacement(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = _record()
    after = replace(before, status="completed", exit_code=0, finished_at="2026-10-26T00:00:00+00:00")
    assert store.write_landed(task_dir, before)
    path = store.record_path(task_dir, before.task_id)
    real_read_text = Path.read_text
    injected = False

    def race(candidate: Path, *args: object, **kwargs: object) -> str:
        nonlocal injected
        raw = real_read_text(candidate, *args, **kwargs)
        if candidate == path and not injected:
            injected = True
            assert store.write_landed(task_dir, after)
        return raw

    monkeypatch.setattr(Path, "read_text", race)
    recovered = task_supervision.read_task_record(task_dir, before.task_id)
    assert recovered == after and injected
    monkeypatch.setattr(Path, "read_text", real_read_text)
    snapshot = ctl._status_snapshot(task_dir, before.task_id, None)
    assert snapshot["status"] == "completed"
    assert injected
