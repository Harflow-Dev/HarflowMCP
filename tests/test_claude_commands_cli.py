"""Opt-in: dispatch-time `allowed_commands` against the installed Claude and a localhost fake API (no model calls)."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import threading
import uuid
from pathlib import Path

import pytest

from claude_cli_support import isolate, require_claude
from fake_anthropic_api import fake_anthropic_api
from harflow_mcp.backends import claude_denials, get
from harflow_mcp.tasks import TaskRegistry

pytestmark = [
    pytest.mark.cli_integration,
    pytest.mark.skipif(not os.environ.get("HARFLOW_CLI_INTEGRATION"), reason="opt in with HARFLOW_CLI_INTEGRATION=1"),
]


def bash(command: str, identifier: str = "grant_1") -> list[dict]:
    return [{"name": "Bash", "input": {"command": command, "description": "grant fixture"}, "id": identifier}]


async def run_task(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str, grants: list[str], freedom: str = "write_in_repo"):
    version = require_claude("allowed_commands", monkeypatch)
    repo = tmp_path / "repo"
    repo.mkdir()
    with fake_anthropic_api(steps=bash(command)) as (base_url, requests):
        isolate(monkeypatch, tmp_path, base_url)
        registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)
        task = await registry.start("run the scripted action", repo, backend=get("claude"), freedom=freedom, allowed_commands=grants)
        try:
            await asyncio.wait_for(task.done.wait(), timeout=90)
        finally:
            if not task.done.is_set():
                await registry.cancel(task.task_id)
        snapshot = task.snapshot()
    assert requests
    assert task.acc.stream_state["claude_code_version"] == version
    assert not any("qualified" in notice for notice in task.bridge_notices), task.bridge_notices
    return snapshot


def denial_for(snapshot: dict, identifier: str = "grant_1") -> dict | None:
    return next((item for item in snapshot["permission_denials"] if item.get("tool_use_id") == identifier), None)


@pytest.mark.asyncio
async def test_an_allowed_command_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = await run_task(tmp_path, monkeypatch, "python3 -m this", ["python3 -m this"])
    assert snapshot["permission_denials"] == []
    assert snapshot["allowed_commands"] == ["python3 -m this"]
    assert snapshot["enforcement"]["extra_allowed_commands"] == ["python3 -m this"]


@pytest.mark.asyncio
async def test_an_unlisted_command_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = await run_task(tmp_path, monkeypatch, "python3 -m calendar", ["python3 -m this"])
    denial = denial_for(snapshot)
    assert denial is not None and denial["denial_category"] == "approval_required", snapshot["permission_denials"]


@pytest.mark.asyncio
async def test_a_chained_command_with_one_unlisted_part_is_refused_and_names_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = await run_task(tmp_path, monkeypatch, "python3 -m this && python3 -m calendar", ["python3 -m this"])
    denial = denial_for(snapshot)
    assert denial is not None and denial["denial_category"] == "chained_command", snapshot["permission_denials"]
    assert denial["part"] == "python3 -m calendar"


@pytest.mark.asyncio
async def test_a_chained_command_with_every_part_allowed_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = await run_task(tmp_path, monkeypatch, "python3 -m this && python3 -m calendar", ["python3 -m this", "python3 -m calendar"])
    assert snapshot["permission_denials"] == []


def run_direct(argv: list[str], initial_input: bytes, repo: Path, timeout: float = 90) -> tuple[list[dict], dict[str, str]]:
    """Run one live-input claude directly (validation deliberately bypassed) and return its events and tool_result texts."""
    process = subprocess.Popen(argv, cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    timer = threading.Timer(timeout, process.kill)
    timer.start()
    events: list[dict] = []
    texts: dict[str, str] = {}
    try:
        process.stdin.write(initial_input)
        process.stdin.flush()
        for raw in process.stdout:
            try:
                event = json.loads(raw)
            except ValueError:
                continue
            events.append(event)
            message = event.get("message")
            for block in (message.get("content") if isinstance(message, dict) and isinstance(message.get("content"), list) else []):
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    content = block.get("content")
                    texts[block["tool_use_id"]] = content if isinstance(content, str) else "".join(part.get("text", "") for part in content or [] if isinstance(part, dict))
            if event.get("type") == "result":
                break
    finally:
        timer.cancel()
        process.stdin.close()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
    return events, texts


def forged_argv(repo: Path, allow: str, freedom: str = "write_in_repo") -> tuple[list[str], bytes]:
    """The product's own argv with a hand-written `--allowedTools` value, which no product path can produce for these patterns."""
    invocation = get("claude").build_start_argv("run the scripted action", repo=repo, freedom=freedom, session_id=str(uuid.uuid4()), model=None, max_turns=None, reasoning_effort=None)
    argv = list(invocation.argv)
    if "--allowedTools" in argv:
        argv[argv.index("--allowedTools") + 1] += f",{allow}"
    else:
        at = argv.index("--disallowedTools") + 2
        argv[at:at] = ["--allowedTools", allow]
    return argv, invocation.initial_input


def direct(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str, allow: str, freedom: str = "write_in_repo"):
    require_claude("allowed_commands", monkeypatch)
    repo = tmp_path / "repo"
    repo.mkdir()
    argv, initial = forged_argv(repo, allow, freedom)
    with fake_anthropic_api(steps=bash(command)) as (base_url, _requests):
        isolate(monkeypatch, tmp_path, base_url)
        events, texts = run_direct(argv, initial, repo)
    result = next(event for event in events if event.get("type") == "result")
    return result, texts


def test_git_commit_is_denied_at_write_in_repo_even_when_a_pattern_covers_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result, texts = direct(tmp_path, monkeypatch, "git commit -m x", "Bash(git commit:*)")
    assert [item["tool_use_id"] for item in result["permission_denials"]] == ["grant_1"]
    assert claude_denials.match(texts["grant_1"]) == ("deny_rule", "git commit -m x")


@pytest.mark.parametrize("pattern,command,refused", [
    ("python3 -m this:*", "python3 -m this", False),
    ("python3 -m this:*", "python3 -m this -h", False),
    ("python3 -m this *", "python3 -m this", False),
    ("python3 -m this *", "python3 -m this -h", False),
    ("python3 -m this:*", "python3 -m thisx", True),
    ("python3 -m this *", "python3 -m thisx", True),
])
def test_wildcard_forms_match_with_and_without_arguments_and_respect_the_word_boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pattern: str, command: str, refused: bool) -> None:
    result, _texts = direct(tmp_path, monkeypatch, command, f"Bash({pattern})")
    assert bool(result["permission_denials"]) is refused, result["permission_denials"]
