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


@pytest.mark.asyncio
@pytest.mark.parametrize("command,denied,grants_enabled", [
    ("git -C . fetch origin develop", False, True),
    ("git -C . pull --ff-only origin develop", False, True),
    ("git -C . switch -c feature/prep", False, True),
    ("git -C . reset --hard", True, True),
    ("git -C . pull --ff-only origin develop --force", True, True),
    pytest.param("git -C . switch -c feature/prep develop", True, False, id="no-grants-create"),
    pytest.param("git -C . switch develop", True, False, id="no-grants-switch"),
    pytest.param("git -C . fetch origin develop", True, False, id="no-grants-fetch"),
    pytest.param("git -C . pull --ff-only origin develop", True, False, id="no-grants-pull"),
])
@pytest.mark.parametrize("freedom", ["write_in_repo", "publish"])
async def test_git_preparation_grants_real_cli(tmp_path, monkeypatch, command, denied, grants_enabled, freedom):
    require_claude("allowed_commands", monkeypatch)
    repo = tmp_path / "repo"
    repo.mkdir()
    # A local remote proves fetch/pull execution without any paid API or external Git writes.
    def git(*args):
        return subprocess.run(["git", "-c", "core.hooksPath=/dev/null", *args], cwd=repo,
                              check=True, capture_output=True, text=True)
    git("init", "-b", "develop")
    git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "seed")
    git("clone", "--bare", str(repo), str(tmp_path / "remote.git"))
    git("remote", "add", "origin", str(tmp_path / "remote.git"))
    grants = ["git -C . fetch origin develop", "git -C . pull --ff-only origin develop", "git -C . switch -c feature/prep"] if grants_enabled else []
    with fake_anthropic_api(steps=bash(command)) as (url, requests):
        isolate(monkeypatch, tmp_path, url)
        registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)
        preview = await registry.preview_permissions(repo, backend=get("claude"), freedom=freedom, allowed_commands=grants)
        assert preview["enforcement"].get("extra_allowed_commands", []) == grants
        task = await registry.start("run scripted Git preparation", repo, backend=get("claude"), freedom=freedom, allowed_commands=grants)
        try:
            await asyncio.wait_for(task.done.wait(), timeout=90)
        finally:
            if not task.done.is_set():
                await registry.cancel(task.task_id)
        snapshot = task.snapshot()
    assert requests
    assert (denial_for(snapshot) is not None) == denied, snapshot
    if not denied:
        if "switch" in command:
            assert git("branch", "--show-current").stdout.strip() == "feature/prep"
        else:
            assert (repo / ".git" / "FETCH_HEAD").is_file()


@pytest.mark.parametrize("command,denied", [
    *[(f"gh pr {verb}{suffix}", False)
      for verb in ("create", "edit", "view", "comment", "review", "checks", "diff", "list")
      for suffix in ("", " --help")],
    ("gh repo view", False),
    ("gh repo view --help", False),
    ("gh pr merge", True),
    ("gh repo delete", True),
    ("gh release create v0", True),
    ("gh api repos/example/repo/pulls", True),
    ("gh api --method POST repos/example/repo/pulls", True),
])
def test_publish_github_command_families_real_cli(tmp_path, monkeypatch, command, denied):
    """Real CLI permissions with a harmless gh executable: no GitHub request or paid model."""
    require_claude("allowed_commands", monkeypatch)
    repo = tmp_path / "repo"
    repo.mkdir()
    binaries = tmp_path / "bin"
    binaries.mkdir()
    marker = tmp_path / "executed"
    executable = binaries / "gh"
    executable.write_text(f"#!/bin/sh\nprintf '%s\\n' \\\"$*\\\" > '{marker}'\n")
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", f"{binaries}{os.pathsep}{os.environ['PATH']}")
    invocation = get("claude").build_start_argv(
        "run the scripted action", repo=repo, freedom="publish", session_id=str(uuid.uuid4()),
        model=None, max_turns=None, reasoning_effort=None,
    )
    with fake_anthropic_api(steps=bash(command)) as (url, requests):
        isolate(monkeypatch, tmp_path, url)
        events, texts = run_direct(invocation.argv, invocation.initial_input, repo)
    assert requests
    result = next(event for event in events if event.get("type") == "result")
    assert bool(result["permission_denials"]) is denied
    assert marker.exists() is not denied
    if denied:
        assert claude_denials.match(texts["grant_1"])[0] == "approval_required"


@pytest.mark.asyncio
async def test_saved_prepare_branches_workflow_clean_multi_repo(tmp_path, monkeypatch):
    """Actual saved-flow scheduler and Claude worker; deterministic in-process orchestrator decisions."""
    from fake_anthropic_api import serve
    from harflow_mcp import workflows as w
    from test_workflow_delegation import Registry, graph, run_flow

    require_claude('allowed_commands', monkeypatch)
    workspace = tmp_path / 'workspace'
    workspace.mkdir()
    grants = []
    repos = []
    for name in ('app', 'server'):
        repo = workspace / name
        repo.mkdir()
        def git(*args):
            return subprocess.run(['git', '-c', 'core.hooksPath=/dev/null', *args], cwd=repo, check=True, capture_output=True)
        git('init', '-b', 'develop')
        git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '--allow-empty', '-m', 'seed')
        remote = tmp_path / (name + '.git')
        git('clone', '--bare', str(repo), str(remote))
        git('remote', 'add', 'origin', str(remote))
        repos.append(repo)
        grants.extend([f'git -C {name} fetch origin develop', f'git -C {name} pull --ff-only origin develop', f'git -C {name} switch -c feature/prep develop'])
    def decide(body):
        answered = sum(1 for message in body.get('messages', []) for block in (message.get('content') if isinstance(message.get('content'), list) else []) if block.get('type') == 'tool_result')
        if answered < len(grants):
            return [{'type': 'tool_use', 'id': f'prep_{answered}', 'name': 'Bash', 'input': {'command': grants[answered]}}], 'tool_use'
        result = {'status': 'succeeded', 'result': {'summary': 'Prepared both branches'}, 'evidence': ['local Git checkout branches observed']}
        return [{'type': 'text', 'text': json.dumps(result)}], 'end_turn'

    with serve(decide) as (url, requests):
        isolate(monkeypatch, tmp_path, url)
        monkeypatch.setattr(w, '_launch', lambda *_args: None)
        storage = w.WorkflowStore(tmp_path / 'state')
        actual = TaskRegistry(log_dir=storage.root / 'tasks', open_monitor=False)
        class Hybrid(Registry):
            async def start(self, prompt, repo, **kwargs):
                if self.decode_context(prompt) is not None:
                    return await super().start(prompt, repo, **kwargs)
                task = await actual.start(prompt, repo, **kwargs)
                self.tasks[task.task_id] = task
                return task
        registry = Hybrid(storage.root)
        definition = graph()
        definition['name'] = 'Multi-platform Feature Implementation fixture'
        definition['nodes'][1].update(title='Prepare Branches', freedom='write_in_repo', agent={'backend': 'claude', 'allowed_commands': grants})
        storage.save('prepare-fixture', definition)
        saved = storage.get('prepare-fixture')
        run, registry = await run_flow(storage, workspace, saved, registry, timeout_seconds=120)
    assert run['status'] == 'completed', run
    workers = [task for task in registry.tasks.values() if hasattr(task, 'acc')]
    assert len(workers) == 1
    assert workers[0].snapshot()['permission_denials'] == []
    assert not workers[0].caller_approvals
    assert requests
    for repo in repos:
        assert subprocess.check_output(['git', 'branch', '--show-current'], cwd=repo).strip() == b'feature/prep'
