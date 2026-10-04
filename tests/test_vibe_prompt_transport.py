"""Vibe's stdin transport closes at EOF and never rewrites an assignment."""
import asyncio
import sys
from dataclasses import replace

import pytest

from polybridge.backends.vibe import VibeBackend, UnsafeInvocationError
from polybridge.backends.base import UnsupportedCapability
from polybridge.tasks import TaskRegistry


@pytest.mark.parametrize("resume", [False, True])
def test_large_vibe_prompt_uses_one_shot_stdin(tmp_path, resume):
    backend = VibeBackend()
    prompt = "assignment 🦋\n" + "x" * 1_050_838
    method = backend.build_resume_argv if resume else backend.build_start_argv
    invocation = method(prompt, repo=tmp_path, freedom="read_only", session_id="native-session" if resume else None, model=None, max_turns=None, reasoning_effort=None)
    assert max(len(arg.encode()) for arg in invocation.argv) < 10000
    assert invocation.argv[-1] == "--prompt="
    assert invocation.stdin_mode == "pipe_once"
    assert invocation.initial_input == prompt.encode()
    assert not invocation.live_input
    if resume:
        assert invocation.argv[invocation.argv.index("--resume") + 1] == "native-session"
    else:
        assert "--resume" not in invocation.argv
    backend.assert_safe(invocation, "read_only")


@pytest.mark.parametrize("padding", [" ", "\n", "\r\n"])
def test_large_vibe_prompt_rejects_outer_whitespace_instead_of_silent_trim(tmp_path, padding):
    with pytest.raises(UnsupportedCapability, match="outer whitespace"):
        VibeBackend().build_start_argv("x" * 40000 + padding, repo=tmp_path, freedom="read_only", session_id=None, model=None, max_turns=None, reasoning_effort=None)


@pytest.mark.parametrize("resume", [False, True])
async def test_large_vibe_stdin_delivers_exact_bytes_then_eof(tmp_path, monkeypatch, resume):
    backend = VibeBackend()
    prompt = "assignment 🦋\n" + "x" * 1_050_838
    method = backend.build_resume_argv if resume else backend.build_start_argv
    invocation = method(prompt, repo=tmp_path, freedom="read_only", session_id="native-session" if resume else None, model=None, max_turns=None, reasoning_effort=None)
    create = asyncio.create_subprocess_exec
    received = tmp_path / "received.txt"
    async def fake_exec(*argv, **kwargs):
        assert max(len(arg.encode()) for arg in argv) < 10000
        code = 'import sys,pathlib;pathlib.Path(sys.argv[1]).write_bytes(sys.stdin.buffer.read());print(\'{"role":"assistant","content":"done"}\')'
        return await create(sys.executable, "-c", code, str(received), **kwargs)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    registry = TaskRegistry(log_dir=tmp_path / "logs")
    task = await registry._spawn(invocation, backend=backend, prompt=prompt, repo_path=tmp_path, session_id="native-session" if resume else None, freedom="read_only", max_turns=None, model=None, reasoning_effort=None)
    await asyncio.wait_for(task.proc.wait(), timeout=5)
    assert received.read_bytes() == prompt.encode()
    assert task.input_closed
    assert task.pump is None
    if task.monitor:
        await task.monitor


@pytest.mark.parametrize("changes", [
    {"stdin_mode": "pipe"},
    {"stdin_mode": "devnull"},
    {"initial_input": None},
    {"initial_input": b"\xff"},
    {"initial_input": b" padded "},
    {"initial_input": b""},
])
def test_vibe_one_shot_transport_rejects_mismatched_wiring(tmp_path, changes):
    backend = VibeBackend()
    invocation = backend.build_start_argv("x" * 40000, repo=tmp_path, freedom="read_only", session_id=None, model=None, max_turns=None, reasoning_effort=None)
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(replace(invocation, **changes), "read_only")


def test_vibe_one_shot_transport_rejects_nonempty_argv_prompt(tmp_path):
    backend = VibeBackend()
    invocation = backend.build_start_argv("x" * 40000, repo=tmp_path, freedom="read_only", session_id=None, model=None, max_turns=None, reasoning_effort=None)
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(replace(invocation, argv=[*invocation.argv[:-1], "--prompt=other"]), "read_only")


def test_small_vibe_prompt_preserves_outer_whitespace_in_argv(tmp_path):
    prompt = " \nsmall assignment\r\n "
    invocation = VibeBackend().build_start_argv(prompt, repo=tmp_path, freedom="read_only", session_id=None, model=None, max_turns=None, reasoning_effort=None)
    assert invocation.argv[-1] == "--prompt=" + prompt
    assert invocation.stdin_mode == "devnull"
