"""Opt-in Claude caller-approval certification against a localhost fake Anthropic API."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from harflow_mcp import approval_runtime, approvals
from harflow_mcp.backends import get
from harflow_mcp.tasks import TaskRegistry

from fake_anthropic_api import fake_anthropic_api


pytestmark = [
    pytest.mark.cli_integration,
    pytest.mark.skipif(not os.environ.get("HARFLOW_CLI_INTEGRATION"), reason="opt in with HARFLOW_CLI_INTEGRATION=1"),
]


async def _wait_for_request(registry: TaskRegistry, task_id: str) -> dict:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        rows = approvals.rows(registry.log_dir, task_id)
        if rows:
            return rows[0]
        await asyncio.sleep(0.05)
    raise AssertionError("Claude did not emit a caller approval request")


@pytest.mark.parametrize("decision,marker_expected,outcome", [("approve", True, "succeeded"), ("reject", False, "failed")])
@pytest.mark.asyncio
async def test_real_claude_taskregistry_caller_approval_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, decision: str, marker_expected: bool, outcome: str) -> None:
    executable = shutil.which("claude")
    if not executable:
        pytest.skip("Claude CLI not installed")
    version = subprocess.run([executable, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    if not version.startswith("2.1.302"):
        pytest.skip(f"Caller approval certification is pinned to Claude 2.1.302; found {version}")
    repo = tmp_path / "repo"
    repo.mkdir()
    marker = repo / "approval-marker.txt"
    command = "python3 -c \"open('approval-marker.txt','a').write('APPROVED')\""
    with fake_anthropic_api(command) as (base_url, requests):
        for key in tuple(os.environ):
            if key.startswith(("ANTHROPIC_", "CLAUDE_", "AWS_", "GOOGLE_")) or key == "CLAUDECODE":
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "config"))
        monkeypatch.setenv("ANTHROPIC_API_KEY", "local-fake-key")
        monkeypatch.setenv("ANTHROPIC_BASE_URL", base_url)
        monkeypatch.setenv("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
        registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)
        task = await registry.start("run one exact Bash action", repo, backend=get("claude"), freedom="write_in_repo", caller_approvals=True)
        try:
            row = await _wait_for_request(registry, task.task_id)
            assert task.proc is not None and task.proc.returncode is None
            assert not marker.exists()
            full = await approval_runtime.get_request(registry, task.task_id, row["request_id"])
            assert full["action"]["input"]["command"] == command
            answered = await approval_runtime.respond_request(registry, task.task_id, row["request_id"], full["action_digest"], full["owner_revision"], decision)
            assert answered["decision"] == decision
            await asyncio.wait_for(task.done.wait(), timeout=20)
            final = next(item for item in approvals.rows(registry.log_dir, task.task_id) if item["request_id"] == row["request_id"])
            assert marker.exists() is marker_expected
            assert final["execution_outcome"] == outcome
            assert final["action"]["input"]["command"] == command
            assert requests
        finally:
            if not task.done.is_set():
                await registry.cancel(task.task_id)

