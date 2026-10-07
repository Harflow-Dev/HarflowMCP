"""Strict conversation checkpoints reject settled writers without changing ordinary Resume."""
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from polybridge import control, retention, store
from polybridge.tasks import SessionBusyError, TaskRegistry
from polybridge.workflows import WorkflowStore
from test_registry import make_task


def record(tmp_path, task_id="source", **changes):
    return store.TaskRecord(**({"task_id": task_id, "backend": "claude", "session_id": "conversation", "repo_path": str(tmp_path), "started_at": "2026-10-01T00:00:00Z", "finished_at": "2026-10-01T01:00:00Z", "status": "completed", "exit_code": 0} | changes))


@pytest.mark.parametrize("kind", ["completed", "undisclosed", "transitive", "ancestor_sibling", "ancestor_sibling_undisclosed", "clock_rollback"])
def test_successors_are_conversation_writes_not_live_processes(tmp_path, kind):
    source = record(tmp_path)
    prior = record(tmp_path, "prior", started_at="2026-09-01T00:00:00Z")
    source = replace(source, parent_task_id=prior.task_id)
    store.write(tmp_path, prior)
    store.write(tmp_path, source)
    successor = record(tmp_path, "successor", parent_task_id=source.task_id, started_at="2026-10-02T00:00:00Z")
    if kind == "undisclosed":
        successor = replace(successor, session_id=None)
    elif kind == "transitive":
        intermediary = replace(successor, task_id="intermediary", session_id=None)
        store.write(tmp_path, intermediary)
        successor = replace(successor, session_id=None, parent_task_id=intermediary.task_id)
    elif kind in {"ancestor_sibling", "ancestor_sibling_undisclosed", "clock_rollback"}:
        successor = replace(successor, parent_task_id=prior.task_id)
        if kind == "clock_rollback":
            successor = replace(successor, started_at="2026-08-01T00:00:00Z")
        if kind == "ancestor_sibling_undisclosed":
            successor = replace(successor, session_id=None, started_at="2026-08-01T00:00:00Z")
    store.write(tmp_path, successor)
    assert successor.task_id in store.session_successor_task_ids(tmp_path, source.task_id, source.session_id)
    assert source.session_id not in store.live_session_ids(tmp_path)


def test_proven_resume_ancestor_chain_is_not_a_successor(tmp_path):
    prior = record(tmp_path, "prior")
    source = record(tmp_path, parent_task_id=prior.task_id)
    store.write(tmp_path, prior)
    store.write(tmp_path, source)
    assert store.session_successor_task_ids(tmp_path, source.task_id, source.session_id) == set()
    store.record_path(tmp_path, prior.task_id).unlink()
    with pytest.raises(ValueError, match="ancestry is incomplete"):
        store.session_successor_task_ids(tmp_path, source.task_id, source.session_id)


@pytest.mark.parametrize("path", ["live", "record"])
@pytest.mark.parametrize("strict", [True, False])
async def test_successor_arriving_between_validation_and_session_lock(tmp_path, monkeypatch, path, strict):
    registry = TaskRegistry(log_dir=tmp_path)
    parent = make_task(tmp_path, "source", session_id="conversation", finished=True)
    registry._tasks[parent.task_id] = parent
    registry.persist(parent)
    source = store.read(tmp_path, parent.task_id)
    assert store.session_successor_task_ids(tmp_path, source.task_id, source.session_id) == set()
    original_lock = control.session_lock
    @asynccontextmanager
    async def inject(log_dir, session_id, **kwargs):
        async with original_lock(log_dir, session_id, **kwargs):
            store.write(tmp_path, record(tmp_path, "successor", parent_task_id=source.task_id, session_id=None))
            yield
    monkeypatch.setattr(control, "session_lock", inject)
    spawns = []
    async def fake_spawn(invocation, **kwargs):
        spawns.append(kwargs)
        return make_task(tmp_path, "next", session_id="conversation", finished=True)
    monkeypatch.setattr(registry, "_spawn", fake_spawn)
    async def resume():
        target = parent if path == "live" else source
        method = registry.resume if path == "live" else registry.resume_record
        return await method(target, "Continue", task_id="next", **({"require_unchanged_session": True} if strict else {}))
    if strict:
        with pytest.raises(SessionBusyError, match="successor"):
            await resume()
        assert spawns == []
    else:
        await resume()
        assert len(spawns) == 1


def test_retention_keeps_successor_evidence_while_checkpoint_pinned(tmp_path, monkeypatch):
    log_dir = tmp_path / "tasks"
    source = record(tmp_path)
    successor = record(tmp_path, "successor", parent_task_id=source.task_id, session_id=None)
    sibling = record(tmp_path, "sibling", session_id=source.session_id)
    unrelated = record(tmp_path, "unrelated", session_id="other")
    for value in (source, successor, sibling, unrelated):
        store.write(log_dir, value)
    monkeypatch.setattr(WorkflowStore, "pinned_tasks", lambda self: {source.task_id})
    result = retention.sweep(log_dir, 1, datetime(2026, 12, 1, tzinfo=timezone.utc))
    assert result["deleted_tasks"] == 1
    assert store.read(log_dir, unrelated.task_id) is None
    assert store.session_successor_task_ids(log_dir, source.task_id, source.session_id) == {successor.task_id, sibling.task_id}
    monkeypatch.setattr(WorkflowStore, "pinned_tasks", lambda self: set())
    retention.sweep(log_dir, 1, datetime(2026, 12, 1, tzinfo=timezone.utc))
    assert store.read(log_dir, successor.task_id) is None
