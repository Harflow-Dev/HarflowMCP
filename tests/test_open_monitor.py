"""A4.3: a root task opens the Monitor app in the background — and nothing about that can change
the dispatch or be claimed in its response."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

from polybridge import backends, lineage, server, store
from polybridge.backends import Enforcement, Invocation
from polybridge.tasks import TaskRegistry


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
    monkeypatch.setenv("PB_OPEN_MONITOR", "1")
    monkeypatch.delenv("PB_TASK_ID", raising=False)
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


async def _start(registry: TaskRegistry, repo: Path, backend: _Backend):
    task = await registry.start("hi", repo, backend=backend)
    await asyncio.wait_for(task.done.wait(), timeout=10)
    await asyncio.gather(*registry._monitor_jobs, return_exceptions=True)
    return task


async def test_a_root_task_opens_the_monitor_on_its_own_url(
    tmp_path: Path, git_repo: Path, backend: _Backend, opening_enabled
) -> None:
    launcher = Launcher()
    registry = TaskRegistry(log_dir=tmp_path / "tasks", monitor_launcher=launcher)

    task = await _start(registry, git_repo, backend)

    assert launcher.urls == [f"polybridge-monitor://task/{task.task_id}"]
    assert task.bridge_notices == []


async def test_a_nested_task_does_not_open_the_monitor(
    tmp_path: Path, git_repo: Path, backend: _Backend, opening_enabled, monkeypatch
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
    registry = TaskRegistry(log_dir=tmp_path / "tasks", monitor_launcher=launcher)

    task = await _start(registry, git_repo, backend)

    assert task.spawned_by == "parent"
    assert launcher.urls == []


@pytest.mark.parametrize("skip", ["pb_task_id", "env_off", "env_unset", "not_darwin", "registry_off"])
async def test_opening_is_skipped(
    tmp_path: Path, git_repo: Path, backend: _Backend, opening_enabled, monkeypatch, skip: str
) -> None:
    if skip == "pb_task_id":
        monkeypatch.setenv("PB_TASK_ID", "someone")
    elif skip == "env_off":
        monkeypatch.setenv("PB_OPEN_MONITOR", "0")
    elif skip == "env_unset":
        monkeypatch.delenv("PB_OPEN_MONITOR", raising=False)
    elif skip == "not_darwin":
        monkeypatch.setattr(sys, "platform", "linux")
    launcher = Launcher()
    registry = TaskRegistry(
        log_dir=tmp_path / "tasks", monitor_launcher=launcher, open_monitor=skip != "registry_off"
    )

    if skip == "pb_task_id":
        from polybridge.backends import NestedDispatchRefused
        with pytest.raises(NestedDispatchRefused, match="PB_TASK_ID"):
            await _start(registry, git_repo, backend)
    else:
        await _start(registry, git_repo, backend)

    assert launcher.urls == []


@pytest.mark.parametrize("result", [1, OSError("no such app"), RuntimeError("boom")])
async def test_a_failing_launcher_only_adds_a_notice(
    tmp_path: Path, git_repo: Path, backend: _Backend, opening_enabled, result
) -> None:
    launcher = Launcher(result)
    registry = TaskRegistry(log_dir=tmp_path / "tasks", monitor_launcher=launcher)

    task = await _start(registry, git_repo, backend)

    assert task.status == "completed"
    assert len(task.bridge_notices) == 1
    assert task.bridge_notices[0].startswith("The Monitor app could not be opened")
    assert store.read(tmp_path / "tasks", task.task_id).bridge_notices == task.bridge_notices


@pytest.mark.parametrize("result", [0, 1, OSError("no app")])
async def test_start_task_response_is_the_same_whatever_the_launcher_does(
    tmp_path: Path, git_repo: Path, backend: _Backend, opening_enabled, monkeypatch, result
) -> None:
    """The launcher is scheduled as `_spawn`'s last step with no await before the response is
    built, so even one that fails instantly cannot reach the response — which never mentions the
    app either way. (Found in acceptance: scheduled earlier, a fast failure's notice landed in
    the response of `start_task`.)"""
    monkeypatch.setattr(server.backends, "is_installed", lambda b: True)
    launcher = Launcher(result)
    registry = TaskRegistry(log_dir=tmp_path / "tasks", monitor_launcher=launcher)
    monkeypatch.setattr(server, "_registry", registry)

    response = await server.start_task("hi", str(git_repo), backend="fake-open")

    assert response["notices"] == []
    assert "monitor" not in str(response).lower()
    task = registry.get(response["task_id"])
    await asyncio.wait_for(task.done.wait(), timeout=10)
    await asyncio.gather(*registry._monitor_jobs, return_exceptions=True)
    assert launcher.urls == [f"polybridge-monitor://task/{task.task_id}"]
    assert task.status == "completed"
    assert len(task.bridge_notices) == (0 if result == 0 else 1)


async def test_the_default_launcher_runs_open_g_and_reaps_it(monkeypatch) -> None:
    from polybridge import tasks as tasks_module

    calls: list[tuple] = []

    class _Proc:
        returncode = None

        async def wait(self):
            self.returncode = 0
            return 0

    async def fake_exec(*argv, **kwargs):
        calls.append(argv)
        return _Proc()

    monkeypatch.setattr(tasks_module.asyncio, "create_subprocess_exec", fake_exec)

    assert await tasks_module._launch_monitor("polybridge-monitor://task/x") == 0
    assert calls == [("open", "-g", "polybridge-monitor://task/x")]
