"""Explicit consumer retries must preserve strict metadata qualification."""
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from harflow_mcp import state_ownership, store, task_resources, task_supervision, worktrees
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry


@pytest.fixture
def task_dir(tmp_path):
    directory = tmp_path / "state" / "tasks"
    with state_ownership.state_session(resolve_layout(root=directory.parent)):
        directory.mkdir(parents=True)
        yield directory


def record(**changes):
    return store.TaskRecord(**dict(task_id="race", backend="codex", session_id="session",
        repo_path="/tmp/repo", started_at=datetime.now(timezone.utc).isoformat(),
        status="running", exit_code=None, markers=["session"], **changes))


def replacing(task_dir, monkeypatch, *, persistent=False):
    before = record(worktree={"path": "/tmp/old-tree"})
    assert store.write_landed(task_dir, before)
    path = store.record_path(task_dir, "race")
    real_read = Path.read_text
    calls = []
    writing = False

    def race(candidate, *args, **kwargs):
        nonlocal writing
        raw = real_read(candidate, *args, **kwargs)
        if candidate == path and not writing:
            calls.append(raw)
            if persistent or len(calls) == 1:
                writing = True
                try:
                    assert store.write_landed(task_dir, replace(before, session_id=f"latest-{len(calls)}",
                        worktree={"path": f"/tmp/latest-tree-{len(calls)}"}))
                finally:
                    writing = False
        return raw

    monkeypatch.setattr(Path, "read_text", race)
    return calls


def test_worktree_scan_uses_latest_complete_metadata(task_dir, monkeypatch):
    calls = replacing(task_dir, monkeypatch)
    assert worktrees.recorded_paths(task_dir) == [Path("/tmp/latest-tree-1").resolve()]
    assert len(calls) == 2


def test_worktree_scan_persistent_churn_refuses_after_three_reads(task_dir, monkeypatch):
    calls = replacing(task_dir, monkeypatch, persistent=True)
    with pytest.raises(store.TaskDocumentError):
        worktrees.recorded_paths(task_dir)
    assert len(calls) == 3


def test_mixed_scan_refusal_never_retries(task_dir, monkeypatch):
    calls = replacing(task_dir, monkeypatch)
    store.record_path(task_dir, "zbroken").write_text("{malformed")
    with pytest.raises(store.TaskDocumentError):
        task_supervision.read_task_records(task_dir)
    assert len(calls) == 1


@pytest.mark.parametrize("operation", [store.read, store.scan, store.read_all])
def test_core_observations_stay_strict(task_dir, monkeypatch, operation):
    calls = replacing(task_dir, monkeypatch)
    if operation is store.scan:
        result = operation(task_dir)
        assert result.history_incomplete
    else:
        with pytest.raises(store.TaskDocumentError):
            operation(task_dir, "race") if operation is store.read else operation(task_dir)
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("persistent", [False, True])
async def test_cascade_guard_qualifies_before_any_side_effect(task_dir, monkeypatch, persistent):
    calls = replacing(task_dir, monkeypatch, persistent=persistent)
    registry = TaskRegistry(log_dir=task_dir, owner={"pid": 1, "start_time": "test", "markers": []})
    cascades = []

    async def cascade(task_id):
        cascades.append(store.read(task_dir, task_id).session_id)
        return {"task_id": task_id}

    monkeypatch.setattr(registry, "_cancel_cascade", cascade)
    monkeypatch.setattr(registry, "_cascade_targets", lambda *args: [])
    monkeypatch.setattr(task_resources, "pending_descendants", lambda *args: [])
    if persistent:
        with pytest.raises(store.TaskDocumentError):
            await registry.cancel_cascade("race", workflow_control=True)
        assert len(calls) == 3
        assert cascades == []
    else:
        assert await registry.cancel_cascade("race", workflow_control=True) == {"task_id": "race"}
        assert cascades == ["latest-1"]
        assert len(calls) == 3  # two guard acquisitions and one inspection inside the stub


@pytest.mark.parametrize("marker,code", [(False, "task_unavailable"), (1, "task_unavailable"),
    ("yes", "task_unavailable"), (True, "task_incompatible")])
def test_scan_requires_exact_marker_and_unavailable_code(task_dir, monkeypatch, marker, code):
    error = store.TaskDocumentError("race", "unqualified refusal", code=code)
    error._source_changed_during_read = marker
    scans = []

    def scan(directory):
        scans.append(directory)
        return store.TaskScan([], {"race": error})

    monkeypatch.setattr(store, "scan", scan)
    with pytest.raises(store.TaskDocumentError) as raised:
        task_supervision.read_task_records(task_dir)
    assert raised.value is error
    assert scans == [task_dir]


@pytest.mark.asyncio
async def test_cascade_replacement_with_incompatible_document_refuses_before_side_effects(task_dir, monkeypatch):
    import json
    import os
    from harflow_mcp import workflow_hooks

    assert store.write_landed(task_dir, record())
    path = store.record_path(task_dir, "race")
    real_read = Path.read_text
    reads = []

    def race(candidate, *args, **kwargs):
        raw = real_read(candidate, *args, **kwargs)
        if candidate == path:
            reads.append(raw)
            if len(reads) == 1:
                document = json.loads(raw)
                document["task_id"] = "different"
                temporary = path.with_suffix(".replacement")
                temporary.write_text(json.dumps(document))
                os.replace(temporary, path)
        return raw

    monkeypatch.setattr(Path, "read_text", race)
    registry = TaskRegistry(log_dir=task_dir, owner={"pid": 1, "start_time": "test", "markers": []})
    effects = []
    monkeypatch.setattr(workflow_hooks, "refuse_native_control", lambda *args: effects.append("workflow"))

    async def cascade(task_id):
        effects.append("cascade")
        return {}

    monkeypatch.setattr(registry, "_cancel_cascade", cascade)
    with pytest.raises(store.TaskDocumentError) as raised:
        await registry.cancel_cascade("race")
    assert raised.value.code == "task_incompatible"
    assert len(reads) == 2
    assert effects == []
