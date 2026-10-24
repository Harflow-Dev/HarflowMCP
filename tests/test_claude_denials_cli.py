"""Opt-in: re-capture each Claude refusal text on the installed CLI against a localhost fake API."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from fake_anthropic_api import fake_anthropic_api
from harflow_mcp.backends import claude_denials, claude_qualification, get
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


def _installed_version() -> str:
    executable = shutil.which("claude")
    if not executable:
        pytest.skip("Claude CLI not installed")
    observed = subprocess.run([executable, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    version = claude_qualification.parse_version(observed)
    candidate = os.environ.get("HARFLOW_QUALIFY_CLAUDE_VERSION")
    if version is None or not (claude_qualification.supports(version, "denial_texts") or candidate == version):
        pytest.skip(f"Installed Claude {observed!r} is not in the measured set; set HARFLOW_QUALIFY_CLAUDE_VERSION to measure it")
    return version


@pytest.mark.parametrize("category", list(CASES))
@pytest.mark.asyncio
async def test_real_claude_refusal_text_is_classified(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, category: str) -> None:
    version = _installed_version()
    tool, build, detail = CASES[category]
    repo = tmp_path / "repo"
    (repo / "trailing ").mkdir(parents=True)
    (repo / "trailing " / "file.txt").write_text("x")
    outside = tmp_path / "outside"
    outside.mkdir()
    with fake_anthropic_api(steps=[{"name": tool, "input": build(repo, outside), "id": "denial_1"}]) as (base_url, requests):
        for key in tuple(os.environ):
            if key.startswith(("ANTHROPIC_", "CLAUDE_", "AWS_", "GOOGLE_")) or key == "CLAUDECODE":
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "config"))
        monkeypatch.setenv("ANTHROPIC_API_KEY", "local-fake-key")
        monkeypatch.setenv("ANTHROPIC_BASE_URL", base_url)
        monkeypatch.setenv("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
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
