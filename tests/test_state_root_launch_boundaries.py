"""Dispatch boundaries reject noncanonical state before reserving or spawning."""

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from harflow_mcp import detached, followups, server, workflows
from harflow_mcp.state_layout import UnsupportedStateLayout, resolve_layout
from harflow_mcp.state_ownership import LOCK_NAME, MARKER_NAME, state_session
from harflow_mcp.tasks import TaskRegistry

UNSUPPORTED_LAYOUT_MESSAGE = (
    "Unsupported state layout: fresh agent and supervisor launches require a task "
    "directory named exactly 'tasks' under the selected state root"
)


@pytest.mark.asyncio
async def test_followup_begin_refuses_before_reservation(tmp_path, monkeypatch):
    log_dir = tmp_path / "streams"
    monkeypatch.setattr(followups, "_reserve", lambda *args: pytest.fail("reserved"))
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    with pytest.raises(UnsupportedStateLayout):
        await followups.begin(log_dir, "task", "successor", None)
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert after == [log_dir.parent.relative_to(tmp_path) / MARKER_NAME,
                     log_dir.parent.relative_to(tmp_path) / LOCK_NAME]
    assert after != before


def test_detached_parent_uses_selected_log_dir_without_constructing_registry(tmp_path, monkeypatch):
    selected = tmp_path / "state"
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "ambient"))
    observed = {}

    async def child(*_args):
        observed["child"] = True
        return 1

    class ChildExit(BaseException):
        pass

    monkeypatch.setattr(detached, "_child", child)
    monkeypatch.setattr(detached, "_detach_stdio", lambda _path, **kwargs: None)
    monkeypatch.setattr(detached.os, "pipe", lambda: (10, 11))
    monkeypatch.setattr(detached.os, "fork", lambda: 0)
    monkeypatch.setattr(detached.os, "close", lambda _fd: None)
    monkeypatch.setattr(detached.os, "setsid", lambda: None)
    monkeypatch.setattr(detached.os, "_exit", lambda _code: (_ for _ in ()).throw(ChildExit()))
    with pytest.raises(ChildExit):
        detached.run_detached(
            lambda _: None,
            log_path=tmp_path / "ctl.log",
            log_dir=selected / "tasks",
            registry_factory=lambda: pytest.fail("parent constructed registry"),
        )
    assert observed == {"child": True}


def test_detached_refuses_before_pipe_or_fork(tmp_path, monkeypatch):
    registry = SimpleNamespace(log_dir=tmp_path / "streams")
    monkeypatch.setattr(detached.os, "pipe", lambda: pytest.fail("pipe created"))
    monkeypatch.setattr(detached.os, "fork", lambda: pytest.fail("forked"))
    with pytest.raises(UnsupportedStateLayout):
        detached.run_detached(lambda _: None, log_path=tmp_path / "ctl.log", registry_factory=lambda: registry)


@pytest.mark.asyncio
async def test_server_workflow_start_refuses_before_create_or_reservation(tmp_path, monkeypatch):
    registry = TaskRegistry(log_dir=tmp_path / "streams", owner={})
    monkeypatch.setattr(server, "_registry", registry)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    with pytest.raises(UnsupportedStateLayout) as exc_info:
        await server.start_workflow("missing", "run", str(tmp_path))
    assert str(exc_info.value) == UNSUPPORTED_LAYOUT_MESSAGE
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert after == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation",
    [
        "start_task",
        "start_workflow",
        "builder_followup",
        "resume_task",
        "continue_task",
        "workflow_builder",
        "resume_workflow",
    ],
)
async def test_public_launch_entrypoints_refuse_before_state_mutation(tmp_path, monkeypatch, operation):
    registry = TaskRegistry(log_dir=tmp_path / "streams", owner={})
    monkeypatch.setattr(server, "_registry", registry)
    async def caller_probe(*_args, **_kwargs):
        pytest.fail("caller or harness probe reached before layout refusal")

    monkeypatch.setattr(server, "_guard_mutation", caller_probe)
    monkeypatch.setattr(server, "_verified_workflow_caller", caller_probe)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    if operation == "start_task":
        call = server.start_task("run", str(tmp_path), backend="codex")
    elif operation == "start_workflow":
        call = server.start_workflow("missing", "run", str(tmp_path))
    elif operation == "builder_followup":
        call = server.followup_workflow_builder("task", "run")
    elif operation == "resume_task":
        call = server.resume_task("task", "continue")
    elif operation == "continue_task":
        call = server.continue_task("task", "continue")
    elif operation == "workflow_builder":
        call = server.workflow_builder("workflow", "run")
    elif operation == "resume_workflow":
        call = server.resume_workflow("run", "continue")
    with pytest.raises(UnsupportedStateLayout) as exc_info:
        await call
    assert str(exc_info.value) == UNSUPPORTED_LAYOUT_MESSAGE
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")) == before


@pytest.mark.asyncio
async def test_public_recovery_refuses_before_state_mutation(tmp_path, monkeypatch):
    registry = TaskRegistry(log_dir=tmp_path / "streams", owner={})
    monkeypatch.setattr(server, "_registry", registry)
    async def caller_probe(*_args, **_kwargs):
        pytest.fail("caller or harness probe reached before layout refusal")

    monkeypatch.setattr(server, "_guard_mutation", caller_probe)
    monkeypatch.setattr(server, "_verified_workflow_caller", caller_probe)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    with pytest.raises(UnsupportedStateLayout) as exc_info:
        await server.recover_workflow("run", "retry")
    assert str(exc_info.value) == UNSUPPORTED_LAYOUT_MESSAGE
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert after == before


@pytest.mark.asyncio
async def test_public_cancel_refuses_before_state_mutation(tmp_path, monkeypatch):
    registry = TaskRegistry(log_dir=tmp_path / "streams", owner={})
    monkeypatch.setattr(server, "_registry", registry)
    async def caller_probe(*_args, **_kwargs):
        pytest.fail("caller or harness probe reached before layout refusal")

    monkeypatch.setattr(server, "_guard_mutation", caller_probe)
    monkeypatch.setattr(server, "_verified_workflow_caller", caller_probe)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    with pytest.raises(UnsupportedStateLayout) as exc_info:
        await server.cancel_workflow("run")
    assert str(exc_info.value) == UNSUPPORTED_LAYOUT_MESSAGE
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert after == before


def test_workflow_supervisor_launch_refuses_before_run_read_or_process(tmp_path, monkeypatch):
    storage = workflows.WorkflowStore(root=tmp_path / "state")
    storage.state_layout = storage.state_layout.__class__(storage.root, storage.root / "streams")
    monkeypatch.setattr(storage, "get_run", lambda *_: pytest.fail("run read"))
    monkeypatch.setattr(workflows.subprocess, "Popen", lambda *args, **kwargs: pytest.fail("process started"))
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    with pytest.raises(UnsupportedStateLayout):
        workflows._launch(storage, "run")
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")) == before


def test_workflow_supervisor_constructor_refuses_before_dispatch_methods(tmp_path):
    registry = TaskRegistry(log_dir=tmp_path / "streams", owner={})
    storage = workflows.WorkflowStore(root=tmp_path / "state")
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    with pytest.raises(UnsupportedStateLayout):
        workflows.WorkflowSupervisor(registry, storage)
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")) == before


def test_detached_child_receives_selected_root_and_vendor_home(tmp_path, monkeypatch):
    selected = tmp_path / "root-a"
    ambient = tmp_path / "root-b"
    registry = SimpleNamespace(log_dir=selected / "tasks")
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(ambient))
    vendor_home = tmp_path / "vendor-home"
    monkeypatch.setenv("HOME", str(vendor_home))
    observed = {}

    async def fake_child(_action, _write_fd, _factory):
        observed.update(state=os.environ.get("HARFLOW_STATE_ROOT"), home=os.environ.get("HOME"))
        return 0

    class ChildExit(BaseException):
        pass

    monkeypatch.setattr(detached, "_child", fake_child)
    monkeypatch.setattr(detached, "_detach_stdio", lambda _path, **kwargs: None)
    monkeypatch.setattr(detached.os, "pipe", lambda: (10, 11))
    monkeypatch.setattr(detached.os, "fork", lambda: 0)
    monkeypatch.setattr(detached.os, "close", lambda _fd: None)
    monkeypatch.setattr(detached.os, "setsid", lambda: None)
    monkeypatch.setattr(detached.os, "_exit", lambda _code: (_ for _ in ()).throw(ChildExit()))
    with pytest.raises(ChildExit):
        detached.run_detached(lambda _: None, log_path=tmp_path / "ctl.log", registry_factory=lambda: registry)
    assert observed == {"state": str(selected), "home": str(vendor_home)}


def test_workflow_supervisor_child_receives_selected_root_and_vendor_home(tmp_path, monkeypatch):
    selected = tmp_path / "root-a"
    ambient = tmp_path / "root-b"
    with state_session(resolve_layout(root=selected)):
        storage = workflows.WorkflowStore(root=selected)
        definition = storage.save(
            "launch",
            {
                "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
                "connections": [{"id": "finish", "source": "start", "target": "end"}],
            },
        )
        run = storage.create_run(definition, "launch", tmp_path)
        monkeypatch.setenv("HARFLOW_STATE_ROOT", str(ambient))
        vendor_home = tmp_path / "vendor-home"
        monkeypatch.setenv("HOME", str(vendor_home))
        captured = {}

        class Process:
            pass

        def popen(argv, **kwargs):
            captured["argv"] = argv
            captured.update(kwargs["env"])
            return Process()

        monkeypatch.setattr(workflows.subprocess, "Popen", popen)
        workflows._launch(storage, run["workflow_run_id"])
        assert captured["HARFLOW_STATE_ROOT"] == str(selected)
        assert captured["HOME"] == str(vendor_home)
        assert captured["argv"][-2:] == ["--root", str(selected)]
        assert os.environ["HARFLOW_STATE_ROOT"] == str(ambient)
        assert os.environ["HOME"] == str(vendor_home)


@pytest.mark.asyncio
async def test_public_resume_relaunch_uses_selected_root_and_vendor_home(tmp_path, monkeypatch):
    selected = tmp_path / "root-a"
    ambient = tmp_path / "root-b"
    storage = workflows.WorkflowStore(root=selected)
    definition = storage.save(
        "resume",
        {
            "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
            "connections": [{"id": "finish", "source": "start", "target": "end"}],
        },
    )
    run = storage.create_run(definition, "resume", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda value: value.update(status="paused"), "pause")
    registry = TaskRegistry(log_dir=selected / "tasks", owner={})
    monkeypatch.setattr(server, "_registry", registry)
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(ambient))
    vendor_home = tmp_path / "vendor-home"
    monkeypatch.setenv("HOME", str(vendor_home))
    captured = {}

    class Process:
        pid = 4242
        returncode = None

        def poll(self):
            return self.returncode

    def popen(argv, **kwargs):
        captured["argv"] = argv
        captured["env"] = kwargs["env"]
        return Process()

    monkeypatch.setattr(workflows.subprocess, "Popen", popen)
    monkeypatch.setattr(server, "_guard_mutation", AsyncMock())
    result = await server.resume_workflow(run["workflow_run_id"], "continue")
    assert result["workflow_run_id"] == run["workflow_run_id"]
    assert captured["argv"][-2:] == ["--root", str(selected)]
    assert captured["env"]["HARFLOW_STATE_ROOT"] == str(selected)
    assert captured["env"]["HOME"] == str(vendor_home)
    assert os.environ["HARFLOW_STATE_ROOT"] == str(ambient)


@pytest.mark.asyncio
async def test_public_recover_relaunch_uses_selected_root_and_vendor_home(tmp_path, monkeypatch):
    selected = tmp_path / "root-a"
    ambient = tmp_path / "root-b"
    storage = workflows.WorkflowStore(root=selected)
    definition = storage.save(
        "recover",
        {
            "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
            "connections": [{"id": "finish", "source": "start", "target": "end"}],
        },
    )
    run = storage.create_run(definition, "recover", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda value: value.update(status="failed"), "fail")
    registry = TaskRegistry(log_dir=selected / "tasks", owner={})
    monkeypatch.setattr(server, "_registry", registry)
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(ambient))
    vendor_home = tmp_path / "vendor-home"
    monkeypatch.setenv("HOME", str(vendor_home))
    captured = {}

    class Process:
        pid = 4343
        returncode = None

        def poll(self):
            return self.returncode

    def popen(argv, **kwargs):
        captured["argv"] = argv
        captured["env"] = kwargs["env"]
        return Process()

    monkeypatch.setattr(workflows.subprocess, "Popen", popen)
    monkeypatch.setattr(server, "_guard_mutation", AsyncMock())
    result = await server.recover_workflow(run["workflow_run_id"], "retry")
    assert result["workflow_run_id"] == run["workflow_run_id"]
    assert captured["argv"][-2:] == ["--root", str(selected)]
    assert captured["env"]["HARFLOW_STATE_ROOT"] == str(selected)
    assert captured["env"]["HOME"] == str(vendor_home)
    assert os.environ["HARFLOW_STATE_ROOT"] == str(ambient)
    assert os.environ["HOME"] == str(vendor_home)


@pytest.mark.asyncio
async def test_workflow_main_selects_explicit_root_before_registry_construction(tmp_path, monkeypatch):
    selected = tmp_path / "root-a"
    ambient = tmp_path / "root-b"
    with state_session(resolve_layout(root=selected)):
        storage = workflows.WorkflowStore(root=selected)
        definition = storage.save(
            "main",
            {
                "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
                "connections": [{"id": "finish", "source": "start", "target": "end"}],
            },
        )
        run = storage.create_run(definition, "main", tmp_path)
        monkeypatch.setenv("HARFLOW_STATE_ROOT", str(ambient))
        vendor_home = tmp_path / "vendor-home"
        monkeypatch.setenv("HOME", str(vendor_home))
        observed = {}

        class FakeRegistry:
            def __init__(self, *, log_dir, **_kwargs):
                observed["registry"] = log_dir

        class FakeSupervisor:
            def __init__(self, registry, _storage):
                observed["supervisor"] = registry

            async def execute(self, _run_id):
                observed["env"] = os.environ.copy()

            def attention(self, _reason):
                raise AssertionError("unexpected supervisor failure")

        monkeypatch.setattr("harflow_mcp.tasks.TaskRegistry", FakeRegistry)
        monkeypatch.setattr(workflows, "WorkflowSupervisor", FakeSupervisor)
        await workflows._main(SimpleNamespace(root=str(selected), run_id=run["workflow_run_id"]))
        assert observed["registry"] == selected / "tasks"
        assert observed["env"]["HARFLOW_STATE_ROOT"] == str(selected)
        assert observed["env"]["HOME"] == str(vendor_home)
        assert os.environ["HARFLOW_STATE_ROOT"] == str(selected)
