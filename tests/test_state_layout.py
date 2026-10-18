"""Pure state-root selection and child-environment contract tests."""

from pathlib import Path
from types import SimpleNamespace
import pwd
import json
from unittest.mock import AsyncMock

import pytest

from harflow_mcp.state_layout import (
    InvalidStateRoot,
    StateLayout,
    UnsupportedStateLayout,
    child_environment,
    protected_roots,
    require_canonical_layout,
    resolve_layout,
)
from harflow_mcp.tasks import TaskRegistry
from harflow_mcp.workflows import WorkflowStore


def test_default_and_patched_home_use_canonical_tasks_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "patched-home")
    layout = resolve_layout()
    assert layout.root == tmp_path / "patched-home" / ".harflow"
    assert layout.tasks_directory == layout.root / "tasks"
    assert not layout.root.exists()


@pytest.mark.parametrize("value", ["", "relative/root", "root\0with-nul"])
def test_invalid_state_root_override_is_rejected_without_filesystem_access(value):
    with pytest.raises(InvalidStateRoot, match="Invalid state root"):
        resolve_layout(environ={"HARFLOW_STATE_ROOT": value})


def test_explicit_root_wins_over_invalid_ambient_override(tmp_path):
    selected = tmp_path / "selected"
    layout = resolve_layout(root=selected, environ={"HARFLOW_STATE_ROOT": "relative"})
    assert layout == StateLayout(selected, selected / "tasks")
    assert not selected.exists()


def test_valid_nonexistent_override_is_selected_without_creation(tmp_path):
    selected = tmp_path / "custom" / "StateRoot"
    layout = resolve_layout(environ={"HARFLOW_STATE_ROOT": str(selected)})
    assert layout.root == selected
    assert layout.tasks_directory == selected / "tasks"
    assert not selected.exists()


def test_relative_explicit_paths_and_home_values_are_normalized_lexically(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = resolve_layout(root="./state/Case/../Selected/")
    log_dir = resolve_layout(log_dir="./state/Case/../Selected/tasks/../tasks")
    assert root.root == tmp_path / "state" / "Selected"
    assert log_dir == StateLayout(tmp_path / "state" / "Selected", tmp_path / "state" / "Selected" / "tasks")
    monkeypatch.setenv("HOME", str(tmp_path / "ambient-home"))
    account_home = tmp_path / "account-home"
    monkeypatch.setattr(pwd, "getpwuid", lambda _: SimpleNamespace(pw_dir=str(account_home)))
    assert resolve_layout(environ={"HOME": ""}).root == Path("/") / ".harflow"
    assert resolve_layout(environ={}).root == account_home / ".harflow"
    assert resolve_layout(environ={"HOME": "relative-home"}).root == tmp_path / "relative-home" / ".harflow"


def test_explicit_log_directory_normalizes_parent_without_creation(tmp_path):
    tasks = tmp_path / "state" / "tasks" / ".." / "tasks"
    layout = resolve_layout(log_dir=tasks)
    assert layout.root == tmp_path / "state"
    assert layout.tasks_directory == tmp_path / "state" / "tasks"
    assert not (tmp_path / "state").exists()


def test_explicit_root_and_log_dir_are_mutually_exclusive(tmp_path):
    with pytest.raises(InvalidStateRoot):
        resolve_layout(root=tmp_path / "root", log_dir=tmp_path / "root" / "tasks")


def test_noncanonical_injected_task_directory_is_refused():
    layout = StateLayout(Path("/tmp/state"), Path("/tmp/state/streams"))
    with pytest.raises(UnsupportedStateLayout, match="task directory named exactly 'tasks'"):
        require_canonical_layout(layout)


def test_inconsistent_canonical_descriptor_is_refused_by_dispatch_and_child_environment():
    layout = StateLayout(Path("/tmp/state"), Path("/tmp/other/tasks"))
    with pytest.raises(UnsupportedStateLayout):
        require_canonical_layout(layout)
    with pytest.raises(UnsupportedStateLayout):
        child_environment(layout, {"HOME": "/vendor-home"})


def test_child_environment_overwrites_only_selected_state_root():
    layout = StateLayout(Path("/tmp/selected"), Path("/tmp/selected/tasks"))
    source = {"HOME": "/vendor-home", "CODEX_HOME": "/vendor-codex", "PATH": "/bin", "HARFLOW_STATE_ROOT": "/ambient"}
    child = child_environment(layout, source)
    assert child["HARFLOW_STATE_ROOT"] == "/tmp/selected"
    assert {key: child[key] for key in source if key != "HARFLOW_STATE_ROOT"} == {
        key: value for key, value in source.items() if key != "HARFLOW_STATE_ROOT"
    }
    assert set(child) == set(source)


def test_registry_and_workflow_store_keep_root_a_when_ambient_root_is_b(tmp_path, monkeypatch):
    selected = tmp_path / "root-a"
    ambient = tmp_path / "root-b"
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(ambient))
    registry = TaskRegistry(log_dir=selected / "tasks", open_monitor=False)
    workflows = WorkflowStore(root=selected)
    assert registry.state_layout.root == selected
    assert registry.log_dir == selected / "tasks"
    assert workflows.root == selected
    assert not ambient.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["start", "resume", "resume_record", "continue_task"])
async def test_noncanonical_dispatch_entrypoints_refuse_before_spawn(tmp_path, operation):
    registry = TaskRegistry(log_dir=tmp_path / "streams", open_monitor=False)
    registry._spawn = AsyncMock(side_effect=AssertionError("spawned before layout refusal"))
    if operation == "start":
        call = registry.start("assignment", tmp_path, backend=object())
    elif operation == "resume":
        call = registry.resume(SimpleNamespace(session_id="session", task_id="task", backend="codex"), "continue")
    elif operation == "resume_record":
        call = registry.resume_record(SimpleNamespace(session_id="session", task_id="task", backend="codex"), "continue")
    else:
        call = registry.continue_task("task")
    with pytest.raises(UnsupportedStateLayout):
        await call
    registry._spawn.assert_not_awaited()


@pytest.mark.asyncio
async def test_server_workflow_reads_selected_registry_root_not_ambient_store(tmp_path, monkeypatch):
    from harflow_mcp import server

    selected = tmp_path / "root-a"
    ambient = tmp_path / "root-b"
    selected_store = WorkflowStore(root=selected)
    ambient_store = WorkflowStore(root=ambient)
    selected_store.definitions.joinpath("sample.json").write_text(json.dumps({"name": "sample", "nodes": [], "connections": []}))
    ambient_store.definitions.joinpath("sample.json").write_text(json.dumps({"name": "ambient", "nodes": [], "connections": []}))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(ambient))
    registry = TaskRegistry(log_dir=selected / "tasks", owner={})
    registry._detect_caller = AsyncMock(return_value=None)
    monkeypatch.setattr(server, "_registry", registry)
    result = await server.get_workflow("sample")
    assert result["name"] == "sample"


def test_protected_roots_include_selected_legacy_and_valid_ambient(tmp_path):
    home = tmp_path / "home"
    selected = tmp_path / "selected"
    roots = protected_roots(
        log_dir=selected / "tasks",
        environ={"HOME": str(home), "HARFLOW_STATE_ROOT": str(tmp_path / "ambient")},
    )
    assert roots == frozenset({selected, home / ".harflow", home / ".polybridge", tmp_path / "ambient"})


def test_invalid_ambient_override_is_ignored_for_protection(tmp_path):
    selected = tmp_path / "selected"
    roots = protected_roots(
        log_dir=selected / "tasks",
        environ={"HOME": str(tmp_path / "home"), "HARFLOW_STATE_ROOT": "relative"},
    )
    assert selected in roots
    assert tmp_path / "home" / ".harflow" in roots
    assert all(root != Path("relative") for root in roots)


def test_invalid_ambient_override_is_rejected_without_explicit_selection():
    with pytest.raises(InvalidStateRoot):
        resolve_layout(environ={"HARFLOW_STATE_ROOT": "relative"})


@pytest.mark.parametrize("layout", [
    StateLayout(Path("relative"), Path("relative/tasks")),
    StateLayout(Path("/tmp/state"), Path("relative/tasks")),
])
def test_manually_constructed_relative_or_mismatched_layout_is_not_dispatchable(layout):
    with pytest.raises(UnsupportedStateLayout):
        require_canonical_layout(layout)
