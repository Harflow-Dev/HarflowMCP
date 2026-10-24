"""Child-process environment contributions and safety validation."""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from harflow_mcp.backends import Invocation
from harflow_mcp.backends.antigravity import AntigravityBackend
from harflow_mcp.backends.claude import ClaudeBackend
from harflow_mcp.backends.codex import CodexBackend
from harflow_mcp.backends.opencode import OpencodeBackend
from harflow_mcp.backends.vibe import VibeBackend
from harflow_mcp.backends.native import adapter
from harflow_mcp.backends.claude_native import ClaudeNativeAdapter
from harflow_mcp.backends.codex_native import CodexNativeAdapter
from harflow_mcp import takeover
from harflow_mcp.tasks import TaskRegistry


BACKENDS = [ClaudeBackend, CodexBackend, OpencodeBackend, VibeBackend, AntigravityBackend]


def _build(backend, method: str, prompt: str, repo: Path, freedom: str = "read_only") -> Invocation:
    session_id = "session" if method == "build_resume_argv" or backend.capabilities.chooses_session_id else None
    return getattr(backend, method)(
        prompt,
        repo=repo,
        freedom=freedom,
        session_id=session_id,
        model=None,
        max_turns=None,
        reasoning_effort=None,
    )


@pytest.mark.parametrize("backend_type", BACKENDS, ids=lambda cls: cls.__name__)
@pytest.mark.parametrize("method", ["build_start_argv", "build_resume_argv"])
def test_backend_invocation_environment_is_canonical(backend_type, method, tmp_path):
    invocation = _build(backend_type(), method, "assignment", tmp_path)
    expected = {"VIBE_CLI": "python"} if backend_type is VibeBackend else {}
    assert dict(invocation.env) == expected
    backend_type().assert_safe(invocation, "read_only")


@pytest.mark.parametrize("backend_type", BACKENDS, ids=lambda cls: cls.__name__)
@pytest.mark.parametrize("bad_env", [{"UNEXPECTED": "value"}, None, {"UNEXPECTED": 1}, {1: "value"}])
def test_backend_safety_rejects_noncanonical_invocation_environment(backend_type, bad_env, tmp_path):
    backend = backend_type()
    invocation = _build(backend, "build_start_argv", "assignment", tmp_path)
    with pytest.raises((RuntimeError, ValueError)):
        backend.assert_safe(replace(invocation, env=bad_env), "read_only")


@pytest.mark.parametrize("bad_env", [{}, {"VIBE_CLI": "rust"}, {"VIBE_CLI": "python", "EXTRA": "x"}])
def test_vibe_safety_rejects_missing_or_altered_environment(bad_env, tmp_path):
    backend = VibeBackend()
    invocation = _build(backend, "build_start_argv", "assignment", tmp_path)
    with pytest.raises(RuntimeError):
        backend.assert_safe(replace(invocation, env=bad_env), "read_only")


def test_invocation_environment_defaults_to_empty_mapping():
    assert dict(Invocation(["agent"]).env) == {}


@pytest.mark.parametrize("adapter_type, backend_type, settings", [
    (ClaudeNativeAdapter, ClaudeBackend, {"freedom": "read_only"}),
    (CodexNativeAdapter, CodexBackend, {"freedom": "read_only", "network": False}),
])
def test_native_wrapper_preserves_invocation_environment(adapter_type, backend_type, settings, tmp_path):
    backend = backend_type()
    invocation = _build(backend, "build_resume_argv", "assignment", tmp_path)
    custom = replace(invocation, env={"OWNER_SENTINEL": "kept"})
    wrapped = adapter_type().configure(custom, settings)
    assert dict(wrapped.env) == {"OWNER_SENTINEL": "kept"}
    assert wrapped.native_subagent


def test_vibe_has_no_native_subagent_adapter():
    assert adapter(VibeBackend()) is None


def test_interactive_takeover_command_does_not_add_environment_pins(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from harflow_mcp import store

    repo = tmp_path / "repo"
    repo.mkdir()
    record = store.TaskRecord(
        task_id="takeover-task", backend="vibe", session_id="session", repo_path=str(repo),
        started_at=datetime.now(timezone.utc).isoformat(), status="completed", exit_code=0,
    )
    monkeypatch.setattr(takeover.shutil, "which", lambda name: "/usr/local/bin/vibe")
    monkeypatch.setattr(takeover, "other_session_holders", lambda *args, **kwargs: [])
    command = takeover._interactive_command(tmp_path / "tasks", record, will_cascade=False)
    assert command == ["/usr/local/bin/vibe", "--trust", "--workdir", str(repo), "--resume", "session"]


@pytest.mark.parametrize("resume", [False, True])
@pytest.mark.parametrize("large", [False, True])
async def test_vibe_spawn_merges_environment_without_mutating_parent(
    tmp_path, monkeypatch, resume, large
):
    """The registry passes inherited variables plus VIBE_CLI to the real child launch boundary."""
    backend = VibeBackend()
    prompt = "assignment " + ("x" * 40_000 if large else "small")
    invocation = _build(
        backend,
        "build_resume_argv" if resume else "build_start_argv",
        prompt,
        tmp_path,
    )
    # The fake executable records its own environment. Replacing only the executable keeps this
    # probe independent of an installed Vibe binary while preserving final invocation validation.
    real_exec = asyncio.create_subprocess_exec
    env_file = tmp_path / "child-env.json"

    async def fake_exec(*argv, **kwargs):
        code = "import json,os,sys; json.dump({k:os.environ.get(k) for k in sys.argv[2].split(',')}, open(sys.argv[1], 'w')); print('{\"role\":\"assistant\",\"content\":\"done\"}')"
        keys = "PATH,HOME,VIBE_CLI,HARFLOW_STATE_ROOT,HARFLOW_TASK_ID,HARFLOW_ROOT_TASK_ID,HARFLOW_DEPTH,PB_TASK_ID,PB_DEPTH,HARFLOW_TASK_SCRATCH,VENDOR_SENTINEL"
        return await real_exec(sys.executable, "-c", code, str(env_file), keys, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    inherited = {
        "PATH": "/sentinel/path",
        "HOME": "/sentinel/home",
        "VIBE_CLI": "rust",
        "HARFLOW_STATE_ROOT": str(tmp_path / "state"),
        "HARFLOW_TASK_ID": "parent-task",
        "PB_DEPTH": "4",
        "VENDOR_SENTINEL": "kept",
    }
    for key, value in inherited.items():
        monkeypatch.setenv(key, value)
    before = dict(os.environ)

    registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)
    task = await registry._spawn(
        invocation,
        backend=backend,
        prompt=prompt,
        repo_path=tmp_path,
        session_id="session" if resume else None,
        freedom="read_only",
        max_turns=None,
        model=None,
        reasoning_effort=None,
        task_id="child-task",
    )
    await asyncio.wait_for(task.proc.wait(), timeout=5)
    await task.monitor
    import json
    child_env = json.loads(env_file.read_text())
    assert child_env["VIBE_CLI"] == "python"
    for key in ("PATH", "HOME", "VENDOR_SENTINEL"):
        assert child_env[key] == inherited[key]
    assert child_env["HARFLOW_STATE_ROOT"] == inherited["HARFLOW_STATE_ROOT"]
    assert child_env["HARFLOW_TASK_ID"] == "child-task"
    assert child_env["HARFLOW_ROOT_TASK_ID"] == "child-task"
    assert child_env["HARFLOW_DEPTH"] == "0"
    assert child_env["PB_TASK_ID"] == "child-task"
    assert child_env["PB_DEPTH"] == "0"
    assert child_env["HARFLOW_TASK_SCRATCH"] is None
    assert dict(os.environ) == before


async def test_spawn_scrubs_stale_scratch_and_allocates_writable_child_scratch(tmp_path, monkeypatch):
    backend = VibeBackend()
    invocation = _build(backend, "build_start_argv", "assignment", tmp_path, "write_in_repo")
    real_exec = asyncio.create_subprocess_exec
    env_file = tmp_path / "child-env.json"

    async def fake_exec(*argv, **kwargs):
        code = "import json,os,sys; json.dump({'VIBE_CLI':os.environ.get('VIBE_CLI'),'HARFLOW_TASK_SCRATCH':os.environ.get('HARFLOW_TASK_SCRATCH')}, open(sys.argv[1], 'w')); print('{\"role\":\"assistant\",\"content\":\"done\"}')"
        return await real_exec(sys.executable, "-c", code, str(env_file), **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setenv("HARFLOW_TASK_SCRATCH", "/stale/parent/scratch")
    registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)
    task = await registry._spawn(
        invocation,
        backend=backend,
        prompt="assignment",
        repo_path=tmp_path,
        session_id=None,
        freedom="write_in_repo",
        max_turns=None,
        model=None,
        reasoning_effort=None,
        task_id="scratch-child",
    )
    await asyncio.wait_for(task.proc.wait(), timeout=5)
    await task.monitor
    import json
    child_env = json.loads(env_file.read_text())
    assert child_env["VIBE_CLI"] == "python"
    assert child_env["HARFLOW_TASK_SCRATCH"] != "/stale/parent/scratch"
    assert child_env["HARFLOW_TASK_SCRATCH"].endswith("scratch-child")


async def test_non_vibe_child_preserves_inherited_vibe_cli_value(tmp_path, monkeypatch):
    """Empty backend contributions must not erase a caller's existing VIBE_CLI setting."""
    backend = CodexBackend()
    invocation = _build(backend, "build_start_argv", "assignment", tmp_path)
    env_file = tmp_path / "codex-child-env.json"
    real_exec = asyncio.create_subprocess_exec

    async def fake_exec(*argv, **kwargs):
        code = "import json,os,sys; json.dump({'VIBE_CLI':os.environ.get('VIBE_CLI'),'PATH':os.environ.get('PATH')}, open(sys.argv[1], 'w')); print('{\"type\":\"turn.completed\"}')"
        return await real_exec(sys.executable, "-c", code, str(env_file), **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setenv("VIBE_CLI", "rust")
    monkeypatch.setenv("PATH", "/sentinel/path")
    registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)
    task = await registry._spawn(
        invocation,
        backend=backend,
        prompt="assignment",
        repo_path=tmp_path,
        session_id=None,
        freedom="read_only",
        max_turns=None,
        model=None,
        reasoning_effort=None,
        task_id="codex-child",
    )
    await asyncio.wait_for(task.proc.wait(), timeout=5)
    await task.monitor
    import json
    child_env = json.loads(env_file.read_text())
    assert child_env == {"VIBE_CLI": "rust", "PATH": "/sentinel/path"}
