"""Opt-in installed Claude 2.1.310 regression with isolated settings and a local fake API."""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from claude_cli_support import isolate
from fake_anthropic_api import fake_anthropic_api
from harflow_mcp.backends import get
from harflow_mcp.backends import claude_qualification
from harflow_mcp.tasks import TaskRegistry

pytestmark = [
    pytest.mark.cli_integration,
    pytest.mark.claude_settings_observation,
    pytest.mark.skipif(not os.environ.get("HARFLOW_CLI_INTEGRATION"), reason="opt in with HARFLOW_CLI_INTEGRATION=1"),
]


@pytest.mark.parametrize("effect,freedom,command,expected_category", [
    ("ask", freedom, "python3 -c \"open('should-not-exist','w').write('x')\"", "approval_required")
    for effect, freedom in (("ask", "publish"), ("ask", "unrestricted"))
] + [
    ("deny", freedom, "echo fixture > marker", "deny_rule")
    for freedom in ("publish", "unrestricted")
])
@pytest.mark.parametrize("scope", ["user", "project"])
@pytest.mark.asyncio
async def test_measured_claude_inherited_restriction_is_observed_without_causal_attribution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, effect: str, freedom: str, command: str,
    expected_category: str, scope: str,
):
    if sys.platform != "darwin":
        pytest.skip("Inherited settings discovery is qualified only on macOS")
    executable = shutil.which("claude")
    if not executable:
        pytest.skip("Claude CLI not installed")
    isolated_home = tmp_path / "home"
    isolated_config = tmp_path / "config"
    isolated_home.mkdir()
    isolated_config.mkdir()
    version_env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("ANTHROPIC_", "CLAUDE_", "AWS_", "GOOGLE_")) and key != "CLAUDECODE"}
    version_env.update({"HOME": str(isolated_home), "CLAUDE_CONFIG_DIR": str(isolated_config),
                        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"})
    observed = subprocess.run([executable, "--version"], env=version_env, capture_output=True, text=True, check=True).stdout.strip()
    version = claude_qualification.parse_version(observed)
    if version != "2.1.310":
        pytest.skip(f"Inherited settings discovery is qualified only for Claude 2.1.310; installed {observed!r}")

    repo = tmp_path / "repo"
    repo.mkdir()
    settings = isolated_config / "settings.json" if scope == "user" else repo / ".claude" / "settings.json"
    settings.parent.mkdir(parents=True, exist_ok=True)
    settings.write_text(json.dumps({"permissions": {
        effect: ["Bash"] if effect == "ask" else ["Bash(echo fixture > marker)"]}}))
    # A parent project candidate must not be treated as part of cwd discovery.
    ancestor = tmp_path / ".claude" / "settings.json"
    ancestor.parent.mkdir(parents=True, exist_ok=True)
    ancestor.write_text('{"permissions":{"deny":["Bash(ancestor-only)"]}}')
    with fake_anthropic_api(command) as (base_url, requests):
        isolate(monkeypatch, tmp_path, base_url)
        registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)
        task = await registry.start("run the scripted command", repo, backend=get("claude"), freedom=freedom)
        try:
            await asyncio.wait_for(task.done.wait(), timeout=60)
        finally:
            if not task.done.is_set():
                await registry.cancel(task.task_id)
        snapshot = task.snapshot()

    assert requests, "Claude must contact only the localhost fake API"
    assert not (repo / "should-not-exist").exists()
    assert not (repo / "marker").exists()
    assert snapshot["enforcement"]["settings_observation"]["version"] == "2.1.310"
    assert snapshot["enforcement"]["settings_observation"]["applicability"] == "cwd_discovery_measured"
    rules = snapshot["enforcement"]["inherited_rules"]
    assert any(item["effect"] == effect and item["source"] == str(settings) for item in rules)
    assert all(item["source"] != str(ancestor) for item in rules)
    assert all(Path(item["source"]).is_relative_to(tmp_path) for item in rules)
    (denial,) = [item for item in snapshot["permission_denials"] if item.get("tool_use_id") == "caller_bash_1"]
    assert denial["denial_category"] == expected_category
    if expected_category == "approval_required":
        assert "inherited ask or deny rules can remain" in denial["denial_remedy"]
    assert "denial_source" not in denial and "denial_rule" not in denial
    assert denial["classification_basis"] == {
        "claude_code_version": "2.1.310", "evidence": "result_event_and_tool_result_text"
    }
    assert any("may restrict" in notice for notice in snapshot["notices"])


@pytest.mark.asyncio
async def test_installed_cli_applies_project_rule_from_cwd_and_omits_ancestor_rule(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    if sys.platform != "darwin":
        pytest.skip("Inherited settings discovery is qualified only on macOS")
    executable = shutil.which("claude")
    if not executable:
        pytest.skip("Claude CLI not installed")
    isolated_home = tmp_path / "home"
    isolated_config = tmp_path / "config"
    isolated_home.mkdir()
    isolated_config.mkdir()
    version_env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("ANTHROPIC_", "CLAUDE_", "AWS_", "GOOGLE_")) and key != "CLAUDECODE"}
    version_env.update({"HOME": str(isolated_home), "CLAUDE_CONFIG_DIR": str(isolated_config),
                        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"})
    observed = subprocess.run([executable, "--version"], env=version_env, capture_output=True, text=True, check=True).stdout.strip()
    if claude_qualification.parse_version(observed) != "2.1.310":
        pytest.skip(f"Inherited settings discovery is qualified only for Claude 2.1.310; installed {observed!r}")

    root = tmp_path / "repo"
    nested = root / "nested"
    nested.mkdir(parents=True)
    settings = root / ".claude" / "settings.json"
    settings.parent.mkdir()
    settings.write_text('{"permissions":{"deny":["Bash(echo fixture > marker)"]}}')
    command = "echo fixture > marker"
    registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)

    with fake_anthropic_api(command) as (base_url, root_requests):
        isolate(monkeypatch, tmp_path, base_url)
        root_task = await registry.start("run the scripted command", root, backend=get("claude"), freedom="unrestricted")
        try:
            await asyncio.wait_for(root_task.done.wait(), timeout=60)
        finally:
            if not root_task.done.is_set():
                await registry.cancel(root_task.task_id)
    root_snapshot = root_task.snapshot()
    assert root_requests
    assert not (root / "marker").exists()
    assert any(item["source"] == str(settings) and item["effect"] == "deny"
               for item in root_snapshot["enforcement"]["inherited_rules"])
    (root_denial,) = [item for item in root_snapshot["permission_denials"] if item.get("tool_use_id") == "caller_bash_1"]
    assert root_denial["denial_category"] == "deny_rule"

    with fake_anthropic_api(command) as (base_url, nested_requests):
        isolate(monkeypatch, tmp_path, base_url)
        nested_task = await registry.start("run the scripted command", nested, backend=get("claude"), freedom="unrestricted")
        try:
            await asyncio.wait_for(nested_task.done.wait(), timeout=60)
        finally:
            if not nested_task.done.is_set():
                await registry.cancel(nested_task.task_id)
    nested_snapshot = nested_task.snapshot()
    assert nested_requests
    assert (nested / "marker").read_text().strip() == "fixture"
    assert all(item["source"] != str(settings) for item in nested_snapshot["enforcement"]["inherited_rules"])
    assert all(item["path"] != str(settings) for item in nested_snapshot["enforcement"]["settings_observation"]["sources"])
