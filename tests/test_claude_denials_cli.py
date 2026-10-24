"""Opt-in: re-capture each Claude refusal text on the installed CLI against a localhost fake API."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from claude_cli_support import isolate, require_claude
from fake_anthropic_api import fake_anthropic_api
from harflow_mcp.backends import claude_denials, get
from harflow_mcp.backends.claude_approvals import encode
from harflow_mcp.tasks import TaskRegistry

pytestmark = [
    pytest.mark.cli_integration,
    pytest.mark.skipif(not os.environ.get("HARFLOW_CLI_INTEGRATION"), reason="opt in with HARFLOW_CLI_INTEGRATION=1"),
]

# category -> (tool, input builder taking (repo, outside), expected captured detail)
CASES = {
    "deny_rule": ("Bash", lambda repo, out: {"command": "git commit -m x", "description": "fixture"}, None),
    "approval_required": ("Bash", lambda repo, out: {"command": "make build", "description": "fixture"}, None),
    "chained_command": ("Bash", lambda repo, out: {"command": "uname -a; python3 -c 'print(1)'", "description": "fixture"}, "python3 -c 'print(1)'"),
    "unverifiable_command": ("Bash", lambda repo, out: {"command": "uname $(pwd)", "description": "fixture"}, None),
    "path_safety": ("Read", lambda repo, out: {"file_path": str(repo / "trailing " / "file.txt")}, None),
    "outside_working_directory": ("Write", lambda repo, out: {"file_path": str(out / "x.txt"), "content": "x"}, None),
}


@pytest.mark.parametrize("category", list(CASES))
@pytest.mark.asyncio
async def test_real_claude_refusal_text_is_classified(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, category: str) -> None:
    version = require_claude("denial_texts", monkeypatch)
    tool, build, detail = CASES[category]
    repo = tmp_path / "repo"
    (repo / "trailing ").mkdir(parents=True)
    (repo / "trailing " / "file.txt").write_text("x")
    outside = tmp_path / "outside"
    outside.mkdir()
    with fake_anthropic_api(steps=[{"name": tool, "input": build(repo, outside), "id": "denial_1"}]) as (base_url, requests):
        isolate(monkeypatch, tmp_path, base_url)
        registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)
        task = await registry.start("run the scripted action", repo, backend=get("claude"), freedom="write_in_repo")
        try:
            await asyncio.wait_for(task.done.wait(), timeout=60)
        finally:
            if not task.done.is_set():
                await registry.cancel(task.task_id)
        snapshot = task.snapshot()
    assert requests
    assert task.acc.stream_state["claude_code_version"] == version
    (denial,) = [item for item in snapshot["permission_denials"] if item.get("tool_use_id") == "denial_1"]
    assert denial["denial_category"] == category, denial
    assert denial["denial_remedy"] == claude_denials.REMEDIES[category]
    assert denial["classification_basis"]["claude_code_version"] == version
    assert snapshot["denial_categories"] == [category]
    if detail is not None:
        assert denial["part"] == detail
    assert denial.get("harflow_access_deny", False) is (category == "deny_rule")


def test_caller_rejection_text_is_the_measured_one() -> None:
    request = {"native_request_id": "r1", "action": {"input": {"command": "x"}}}
    message = json.loads(encode(request, "reject"))["response"]["response"]["message"]
    assert claude_denials.match(message) == ("caller_rejected", None)
