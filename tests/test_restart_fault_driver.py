"""Baseline real-process probes used by restart/supervision regression tests."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from restart_test_support import encode_user_message, install_fake_claude, read_events, terminate_process


async def _start_fake(tmp_path: Path, monkeypatch, *, session: str = "probe-session"):
    install_fake_claude(tmp_path, monkeypatch, session=session)
    return await asyncio.create_subprocess_exec(
        "claude",
        "--print",
        "--output-format",
        "stream-json",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=tmp_path,
        start_new_session=True,
    )


@pytest.mark.asyncio
async def test_fake_claude_real_executable_preserves_session_and_output(tmp_path, monkeypatch):
    process = await _start_fake(tmp_path, monkeypatch)
    assert process.stdin is not None and process.stdout is not None
    try:
        process.stdin.write(encode_user_message("reply preserve  spaces"))
        await process.stdin.drain()
        events = []
        async for event in read_events(process.stdout):
            events.append(event)
            if event.get("type") == "result":
                break
        assert events[0]["subtype"] == "init"
        result = events[-1]
        assert result["type"] == "result"
        assert result["session_id"] == "probe-session"
        assert result["result"] == "preserve spaces"
    finally:
        await terminate_process(process)


@pytest.mark.asyncio
async def test_fake_claude_accepts_live_input_after_first_result_and_drains_on_eof(tmp_path, monkeypatch):
    process = await _start_fake(tmp_path, monkeypatch)
    assert process.stdin is not None and process.stdout is not None
    try:
        process.stdin.write(encode_user_message("reply first"))
        await process.stdin.drain()
        first = []
        async for event in read_events(process.stdout):
            first.append(event)
            if event.get("type") == "result":
                break
        assert first[-1]["result"] == "first"

        process.stdin.write(encode_user_message("reply after observer restart"))
        await process.stdin.drain()
        second = []
        async for event in read_events(process.stdout):
            second.append(event)
            if event.get("type") == "result":
                break
        assert second[-1]["result"] == "after observer restart"

        process.stdin.close()
        assert await asyncio.wait_for(process.wait(), timeout=3) == 0
        assert process.stdout.at_eof()
    finally:
        await terminate_process(process)


@pytest.mark.asyncio
async def test_sigkill_probe_leaves_no_live_fake_agent_and_keeps_observed_prefix(tmp_path, monkeypatch):
    process = await _start_fake(tmp_path, monkeypatch, session="killed-session")
    assert process.stdin is not None and process.stdout is not None
    try:
        process.stdin.write(encode_user_message("slow 30 never-settles"))
        await process.stdin.drain()
        init = json.loads((await process.stdout.readline()).decode())
        assistant = json.loads((await process.stdout.readline()).decode())
        assert init["subtype"] == "init"
        assert assistant["type"] == "assistant"
        await terminate_process(process)
        assert process.returncode is not None
        assert process.returncode < 0
    finally:
        await terminate_process(process)
