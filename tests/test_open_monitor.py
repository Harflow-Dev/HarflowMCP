"""A4.3: a root task opens the Monitor app in the background — and nothing about that can change
the dispatch or be claimed in its response."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

from harflow_mcp import backends, lineage, server, store
from harflow_mcp import state_ownership
from harflow_mcp.backends import Enforcement, Invocation
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry


class _Backend:
    name = "fake-open"
    binary = "/bin/sh"
    capabilities = backends.get("claude").capabilities._replace(
        chooses_session_id=False, supports_live_input=False
    )

    def build_start_argv(self, prompt, **kwargs):
        return Invocation([self.binary, "-c", "true"])

    def build_resume_argv(self, prompt, **kwargs):
        return Invocation([self.binary, "-c", "true"])

    def assert_safe(self, invocation, freedom, network=None):
        assert isinstance(invocation, Invocation)

    def encode_live_message(self, text):
        raise backends.UnsupportedCapability("no live input")

    def interactive_resume_argv(self, session_id, repo_path):
        return [self.binary, "--resume", session_id]

    def enforcement(self, freedom, network=None):
        return Enforcement(freedom=freedom, mechanism="none", os_enforced=False, writes_confined=False)

    def ingest(self, event, acc):
        return None

    def normalize(self, event, acc):
        return []

    def classify(self, acc, exit_code):
        return "completed" if exit_code == 0 else "failed"


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> _Backend:
    fake = _Backend()
    monkeypatch.setitem(backends.BACKENDS, fake.name, fake)
    return fake


@pytest.fixture
def opening_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HARFLOW_OPEN_MONITOR", "1")
    monkeypatch.delenv("HARFLOW_TASK_ID", raising=False)
    monkeypatch.setattr(sys, "platform", "darwin")


class Launcher:
    def __init__(self, result=0) -> None:
        self.result = result
        self.urls: list[str] = []

    async def __call__(self, url: str) -> int:
        self.urls.append(url)
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


@pytest.fixture
def owned_state(tmp_path: Path):
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        pass
    return root / "tasks"


async def _start(registry: TaskRegistry, repo: Path, backend: _Backend):
    task = await registry.start("hi", repo, backend=backend)
    await asyncio.wait_for(task.done.wait(), timeout=10)
    await asyncio.gather(*registry._monitor_jobs, return_exceptions=True)
    return task


async def test_a_root_task_does_not_open_monitor_automatically(
    tmp_path: Path, git_repo: Path, backend: _Backend, opening_enabled, owned_state: Path
) -> None:
    launcher = Launcher()
    registry = TaskRegistry(log_dir=owned_state, monitor_launcher=launcher)

    task = await _start(registry, git_repo, backend)

    assert launcher.urls == []
    assert task.bridge_notices == []


async def test_a_nested_task_does_not_open_the_monitor(
    tmp_path: Path, git_repo: Path, backend: _Backend, opening_enabled, monkeypatch, owned_state: Path
) -> None:
    parent = store.TaskRecord(
        task_id="parent",
        backend="fake-open",
        session_id=None,
        repo_path=str(git_repo),
        started_at="2026-09-25T00:00:00+00:00",
        enforcement=backend.enforcement("write_in_repo").as_dict(),
        root_task_id="parent",
        max_depth=2,
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: lineage.Caller(parent, "session"))
    launcher = Launcher()
    registry = TaskRegistry(log_dir=owned_state, monitor_launcher=launcher)

    task = await _start(registry, git_repo, backend)

    assert task.spawned_by == "parent"
    assert launcher.urls == []


@pytest.mark.parametrize("skip", ["harflow_task_id", "env_off", "env_unset", "not_darwin", "registry_off"])
async def test_opening_is_skipped(
    tmp_path: Path, git_repo: Path, backend: _Backend, opening_enabled, monkeypatch, skip: str, owned_state: Path
) -> None:
    if skip == "harflow_task_id":
        monkeypatch.setenv("HARFLOW_TASK_ID", "someone")
    elif skip == "env_off":
        monkeypatch.setenv("HARFLOW_OPEN_MONITOR", "0")
    elif skip == "env_unset":
        monkeypatch.delenv("HARFLOW_OPEN_MONITOR", raising=False)
    elif skip == "not_darwin":
        monkeypatch.setattr(sys, "platform", "linux")
    launcher = Launcher()
    registry = TaskRegistry(
        log_dir=owned_state, monitor_launcher=launcher, open_monitor=skip != "registry_off"
    )

    if skip == "harflow_task_id":
        with pytest.raises(ValueError, match="parent is missing or unconfirmed"):
            await _start(registry, git_repo, backend)
    else:
        await _start(registry, git_repo, backend)

    assert launcher.urls == []


@pytest.mark.parametrize("result", [1, OSError("no such app"), RuntimeError("boom")])
async def test_monitor_launcher_is_unused_and_adds_no_notice(
    tmp_path: Path, git_repo: Path, backend: _Backend, opening_enabled, result, owned_state: Path
) -> None:
    launcher = Launcher(result)
    registry = TaskRegistry(log_dir=owned_state, monitor_launcher=launcher)

    task = await _start(registry, git_repo, backend)

    assert task.status == "completed"
    assert task.bridge_notices == []
    assert store.read(owned_state, task.task_id).bridge_notices == []


@pytest.mark.parametrize("result", [0, 1, OSError("no app")])
async def test_start_task_response_is_the_same_whatever_the_launcher_does(
    tmp_path: Path, git_repo: Path, backend: _Backend, opening_enabled, monkeypatch, result, owned_state: Path
) -> None:
    """The launcher is scheduled as `_spawn`'s last step with no await before the response is
    built, so even one that fails instantly cannot reach the response — which never mentions the
    app either way. (Found in acceptance: scheduled earlier, a fast failure's notice landed in
    the response of `start_task`.)"""
    monkeypatch.setattr(server.backends, "is_installed", lambda b: True)
    launcher = Launcher(result)
    registry = TaskRegistry(log_dir=owned_state, monitor_launcher=launcher)
    monkeypatch.setattr(server, "_registry", registry)

    response = await server.start_task("hi", str(git_repo), backend="fake-open")

    assert response["notices"] == []
    assert "monitor" not in str(response).lower()
    task = registry.get(response["task_id"])
    await asyncio.wait_for(task.done.wait(), timeout=10)
    await asyncio.gather(*registry._monitor_jobs, return_exceptions=True)
    assert launcher.urls == []
    assert task.status == "completed"
    assert task.bridge_notices == []


@pytest.mark.parametrize('saturated', [False, True])
async def test_the_default_launcher_opens_the_fixed_bundle_and_reaps_it(monkeypatch, tmp_path, saturated) -> None:
    from harflow_mcp import tasks as tasks_module
    import threading

    if saturated:
        monkeypatch.setattr(tasks_module, '_EXTRA_PATH_SLOTS', threading.BoundedSemaphore(0))

    calls: list[tuple] = []

    class _Proc:
        returncode = None

        async def wait(self):
            self.returncode = 0
            return 0

    async def fake_exec(*argv, **kwargs):
        calls.append(argv)
        return _Proc()

    monkeypatch.setattr(tasks_module, "MONITOR_BUNDLE", tmp_path)
    monkeypatch.setattr(tasks_module.asyncio, "create_subprocess_exec", fake_exec)

    assert await tasks_module._launch_monitor("harflow://task/x") == 0
    assert calls == [("open", "-g", "-a", str(tmp_path), "harflow://task/x")]


async def test_the_default_launcher_refuses_without_spawning_when_the_bundle_is_absent(
    monkeypatch, tmp_path
) -> None:
    from harflow_mcp import tasks as tasks_module

    calls: list[tuple] = []

    async def fake_exec(*argv, **kwargs):
        calls.append(argv)
        raise AssertionError("nothing may be spawned when the bundle is absent")

    monkeypatch.setattr(tasks_module, "MONITOR_BUNDLE", tmp_path / "Harflow.app")
    monkeypatch.setattr(tasks_module.asyncio, "create_subprocess_exec", fake_exec)

    assert await tasks_module._launch_monitor("harflow://task/x") != 0
    assert calls == []
