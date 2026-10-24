"""Storage-line promotion preflights every authoritative private document."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from harflow_mcp.state_migration import validate_promotion
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import StateIncompatible, state_session
from harflow_mcp import state_compatibility, state_ownership
from harflow_mcp import approvals, inbox, store, task_documents


def _document(format_name: str, *, version: str = "0.0.1", schema: int = 1) -> bytes:
    value = {
        "product": "harflow",
        "format": format_name,
        "v": schema,
        "product_version": version,
    }
    if format_name == "harflow.task.approvals":
        value["version"] = value.pop("v")
    return (json.dumps(value) + "\n").encode()


@pytest.mark.parametrize(
    ("directory", "name", "format_name"),
    [
        ("tasks", "task.meta.json", "harflow.task"),
        ("tasks", "task.followups.json", "harflow.task.followups"),
        ("tasks", "task.artifacts.json", "harflow.task.artifacts"),
        ("tasks", "task.approvals.json", "harflow.task.approvals"),
        ("tasks", "task.launch.json", "harflow.task-launch"),
        ("tasks", "task.observations-final.json", "harflow.task-observations"),
        ("workflows", "workflow.json", "harflow.workflow.definition"),
        ("workflow-runs", "run.json", "harflow.workflow.run"),
        ("workflow-owners", "owner.json", "harflow.workflow.receipt"),
    ],
)
def test_promotion_accepts_qualified_prior_line_documents(tmp_path: Path, directory: str, name: str, format_name: str) -> None:
    path = tmp_path / directory
    path.mkdir()
    document = path / name
    raw = _document(format_name)
    document.write_bytes(raw)

    validate_promotion(tmp_path, target_version="0.0.2")
    assert document.read_bytes() == raw


def test_promotion_rejects_foreign_known_document_before_marker_change(tmp_path: Path) -> None:
    path = tmp_path / "tasks"
    path.mkdir()
    document = path / "task.meta.json"
    raw = _document("harflow.task").replace(b'"harflow"', b'"foreign"', 1)
    document.write_bytes(raw)

    with pytest.raises(StateIncompatible, match="preflight"):
        validate_promotion(tmp_path, target_version="0.0.2")
    assert document.read_bytes() == raw


def test_promotion_rejects_unknown_newer_document_before_marker_change(tmp_path: Path) -> None:
    path = tmp_path / "tasks"
    path.mkdir()
    document = path / "task.meta.json"
    raw = _document("harflow.task", version="0.1.0")
    document.write_bytes(raw)

    with pytest.raises(StateIncompatible, match="preflight"):
        validate_promotion(tmp_path, target_version="0.0.2")
    assert document.read_bytes() == raw


def test_only_exact_four_empty_legacy_directories_are_adopted(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    for name in ("workflows", "workflow-runs", "workflow-leases", "workflow-owners"):
        (root / name).mkdir()
    with state_session(resolve_layout(root=root)):
        pass
    assert (root / ".harflow-store.json").exists()


@pytest.mark.parametrize("mutation", ["missing", "extra", "nonempty", "symlink"])
def test_legacy_empty_adoption_refuses_near_matches(tmp_path: Path, mutation: str) -> None:
    root = tmp_path / "state"
    root.mkdir()
    names = ["workflows", "workflow-runs", "workflow-leases", "workflow-owners"]
    for name in names:
        (root / name).mkdir()
    if mutation == "missing":
        (root / "workflow-owners").rmdir()
    elif mutation == "extra":
        (root / "unexpected").mkdir()
    elif mutation == "nonempty":
        (root / "workflows" / "record").write_bytes(b"data")
    else:
        (root / "workflow-owners").rmdir()
        (root / "workflow-owners-target").mkdir()
        (root / "workflow-owners").symlink_to(root / "workflow-owners-target", target_is_directory=True)

    with pytest.raises(StateIncompatible):
        with state_session(resolve_layout(root=root), timeout=0.2):
            pass
    assert not (root / ".harflow-store.json").exists()


def test_state_session_promotes_qualified_prior_line_and_preserves_provenance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    root.mkdir()
    marker = {
        "product": "harflow", "format": "harflow.store", "v": 1,
        "product_version": "0.0.1", "layout_version": 1,
        "unknown_provenance": {"bytes": "retained"},
    }
    (root / ".harflow-store.json").write_text(json.dumps(marker) + "\n")
    (root / ".harflow-store.lock").touch()
    monkeypatch.setattr(state_ownership, "PRODUCT_VERSION", "0.1.0")
    monkeypatch.setattr(state_compatibility, "PRODUCT_VERSION", "0.1.0")

    with state_session(resolve_layout(root=root)):
        pass

    promoted = json.loads((root / ".harflow-store.json").read_text())
    assert promoted["product_version"] == "0.1.0"
    assert promoted["origin_product_version"] == "0.0.1"
    assert promoted["unknown_provenance"] == marker["unknown_provenance"]
    assert not state_compatibility.persisted_compatible("0.1.0", "harflow.store", target_version="0.0.1")


def test_state_session_aborts_promotion_on_foreign_nested_document(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    root.mkdir()
    marker = _document("harflow.store")
    (root / ".harflow-store.json").write_bytes(marker)
    (root / ".harflow-store.lock").touch()
    task_dir = root / "tasks"
    task_dir.mkdir()
    foreign = _document("harflow.task").replace(b'"harflow"', b'"foreign"', 1)
    (task_dir / "task.meta.json").write_bytes(foreign)
    monkeypatch.setattr(state_ownership, "PRODUCT_VERSION", "0.1.0")
    monkeypatch.setattr(state_compatibility, "PRODUCT_VERSION", "0.1.0")

    with pytest.raises(StateIncompatible):
        with state_session(resolve_layout(root=root)):
            pass
    assert (root / ".harflow-store.json").read_bytes() == marker


def test_populated_prior_line_task_inbox_and_approvals_promote_and_remain_readable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    root.mkdir()
    lock = root / ".harflow-store.lock"
    lock.touch()
    old_modules = (state_ownership, state_compatibility, task_documents, approvals, inbox.inbox_documents)
    for module in old_modules:
        monkeypatch.setattr(module, "PRODUCT_VERSION", "0.0.1")
    task_id = "task-cross-line"
    record = store.TaskRecord(task_id, "codex", "session", str(tmp_path), "2026-10-01T00:00:00+00:00", status="completed", exit_code=0, output_drained=True)
    layout = resolve_layout(root=root)
    with state_session(layout):
        store.write(layout.tasks_directory, record)
        inbox.append_locked(layout.tasks_directory, task_id, {"id": "message-1", "text": "retained"})
        approvals._write(layout.tasks_directory, approvals._path(layout.tasks_directory, task_id), task_id, approvals._empty(task_id))
    raw_task = store.record_path(layout.tasks_directory, task_id).read_bytes()
    raw_inbox = inbox.inbox_path(layout.tasks_directory, task_id).read_bytes()
    raw_approvals = approvals._path(layout.tasks_directory, task_id).read_bytes()

    for module in old_modules:
        monkeypatch.setattr(module, "PRODUCT_VERSION", "0.1.0")
    with state_session(layout):
        loaded = store.read(layout.tasks_directory, task_id)
        messages, _ = inbox.read_new(layout.tasks_directory, task_id, 0)
        rows = approvals.rows(layout.tasks_directory, task_id)
    promoted = json.loads((root / ".harflow-store.json").read_text())
    assert promoted["product_version"] == "0.1.0"
    assert loaded is not None and loaded.task_id == task_id and loaded.status == "completed"
    assert messages[0]["text"] == "retained"
    assert rows == []
    assert store.record_path(layout.tasks_directory, task_id).read_bytes() == raw_task
    assert inbox.inbox_path(layout.tasks_directory, task_id).read_bytes() == raw_inbox
    assert approvals._path(layout.tasks_directory, task_id).read_bytes() == raw_approvals
