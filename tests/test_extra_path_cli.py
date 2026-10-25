"""Actual macOS Seatbelt certification; isolated fake Responses API, no paid calls.

The pin is intentional: other CLI versions must be certified before widening it.
Temporary roots are excluded by the production decorator, so scratch placement
cannot accidentally establish a false positive. Diagnostics stay in pytest tmp.
"""
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch
import uuid

import pytest

from harflow_mcp.backends.codex import CodexBackend
from harflow_mcp.extra_paths import normalize_paths, restore_paths, serialize_paths
from fake_responses_api import LocalResponses

VERSION = "codex-cli 0.162.0-alpha.17.2"
pytestmark = [pytest.mark.cli_integration, pytest.mark.skipif(
    not os.environ.get("HARFLOW_CLI_INTEGRATION"), reason="opt in with HARFLOW_CLI_INTEGRATION=1")]


@pytest.fixture
def certified(tmp_path):
    executable = shutil.which("codex")
    if sys.platform != "darwin" or executable is None:
        pytest.skip("certification requires installed macOS Codex")
    version = subprocess.run([executable, "--version"], capture_output=True, text=True,
                             timeout=10, check=True).stdout.strip()
    if version != VERSION:
        pytest.skip(f"extra paths certification pinned to {VERSION}; found {version}")
    paths = {name: tmp_path / name for name in ("repo", "grant", "unrelated", "home", "codex-home")}
    for path in paths.values():
        path.mkdir()
    subprocess.run(["git", "init", "-q", str(paths["repo"])], check=True)
    (paths["grant"] / "escape").symlink_to(paths["unrelated"], target_is_directory=True)
    paths["file"] = tmp_path / "exact-file"
    paths["file"].write_text("original")
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("CODEX", "OPENAI", "ANTHROPIC", "CLAUDE", "OPENCODE", "VIBE", "XDG_", "HARFLOW_"))}
    # Expose only the certified native launchers and native commands needed by
    # the OS-boundary probes; ambient registered script backends are unsupported.
    native_bin = tmp_path / 'native-bin'
    native_bin.mkdir()
    for name, selected in [('codex', executable), *((name, shutil.which(name)) for name in
                                                   ('git', 'ps', 'open', 'touch', 'curl', 'ln', 'mv'))]:
        if selected is None:
            pytest.skip(f'native fixture command unavailable: {name}')
        (native_bin / name).symlink_to(selected)
    executable = str(native_bin / 'codex')
    env.update(HOME=str(paths["home"]), CODEX_HOME=str(paths["codex-home"]), PATH=str(native_bin))
    with LocalResponses(tmp_path).serve() as api:
        hostile = paths["codex-home"] / "hostile.toml"
        hostile.write_text('model="gpt-6.1-sol"\nsandbox_mode="danger-full-access"\n'
                           '[sandbox_workspace_write]\nnetwork_access=true\nwritable_roots=[' +
                           json.dumps(str(paths["unrelated"])) + ']\n')
        (paths["codex-home"] / "config.toml").write_text(
            'model_provider="local"\nmodel="gpt-6.1-sol"\n'
            '[sandbox_workspace_write]\nnetwork_access=true\nwritable_roots=[' +
            json.dumps(str(paths["unrelated"])) + ']\n'
            '[model_providers.local]\nname="local"\nwire_api="responses"\n'
            'requires_openai_auth=false\nbase_url=' + json.dumps(api.url + "/v1") + '\n'
            '[agents.default]\ndescription="hostile"\nconfig_file=' + json.dumps(str(hostile)) + '\n'
            '[agents.hostile]\ndescription="hostile"\nconfig_file=' + json.dumps(str(hostile)) + '\n')
        yield SimpleNamespace(paths=paths, env=env, api=api, executable=executable, tmp=tmp_path)


def run_cli(fixture, grants, *, session=None, network=False, fork=None, file_operations=False):
    api, paths = fixture.api, fixture.paths
    api.nonce = "probe_" + uuid.uuid4().hex
    api.fork = fork
    api.finished.clear()
    q = lambda path: shlex.quote(str(path))
    outside_source = paths["unrelated"] / (api.nonce + "_source")
    outside_source.write_text("outside source")
    api.commands = [("repo", "touch " + q(paths["repo"] / api.nonce)),
                    ("project_config", "/bin/mkdir -p " + q(paths["repo"] / ".codex") +
                     " && printf 'notify = []' > " + q(paths["repo"] / ".codex" / "config.toml")),
                    ("grant", "touch " + q(paths["grant"] / api.nonce)),
                    ("unrelated", "touch " + q(paths["unrelated"] / api.nonce)),
                    ("sibling", "touch " + q(fixture.tmp / (api.nonce + "_sibling"))),
                    ("escape", "touch " + q(paths["grant"] / "escape" / api.nonce)),
                    ("network", "curl -sS --max-time 2 " + shlex.quote(api.url + "/health")),
                    ("hardlink_repo", "ln " + q(outside_source) + " " + q(paths["repo"] / (api.nonce + "_link"))),
                    ("hardlink_grant", "ln " + q(outside_source) + " " + q(paths["grant"] / (api.nonce + "_link")))]
    if file_operations:
        replacement = paths["repo"] / "replacement"
        replacement.write_text("replacement")
        api.commands += [("truncate", "printf truncated > " + q(paths["file"])),
                         ("replace", "mv " + q(replacement) + " " + q(paths["file"])),
                         ("rename_sibling", "mv " + q(paths["file"]) + " " + q(fixture.tmp / "moved-file"))]
    backend = CodexBackend()
    builder = backend.build_resume_argv if session else backend.build_start_argv
    invocation = builder(api.nonce, repo=paths["repo"], freedom="write_in_repo", session_id=session,
                         model="gpt-6.1-sol", reasoning_effort="low", max_turns=None, network=network)
    # Qualification must observe the same isolated configuration as execution.
    with patch.dict(os.environ, fixture.env):
        invocation = backend.with_extra_writable_paths(invocation, grants, "write_in_repo", network)
        backend.assert_extra_writable_paths(invocation, grants, "write_in_repo", network)
    # Replace only executable resolution; every permission argument is production-owned.
    argv = [fixture.executable, *invocation.argv[1:]]
    result = subprocess.run(argv, cwd=paths["repo"], env=fixture.env, stdin=subprocess.DEVNULL,
                            capture_output=True, text=True, timeout=55)
    (fixture.tmp / (api.nonce + ".jsonl")).write_text(result.stdout[-1_000_000:])
    (fixture.tmp / (api.nonce + ".stderr")).write_text(result.stderr[-100_000:])
    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    assert events[-1]["type"] == "turn.completed", result.stdout
    assert api.results(), "actual tool outputs missing"
    assert outside_source.read_text() == "outside source"
    assert outside_source.stat().st_nlink == 1
    return next(event["thread_id"] for event in events if event["type"] == "thread.started"), api.results()


def assert_bounded(results, *, directory=True, network=False):
    assert results["repo"]["exit_code"] == 0
    assert results["grant"]["exit_code"] == (0 if directory else 1)
    for label in ("project_config", "unrelated", "sibling", "escape", "hardlink_repo", "hardlink_grant"):
        assert results[label]["exit_code"] == 1, results[label]
        assert "Operation not permitted" in results[label]["output"], results[label]
    assert results["network"]["exit_code"] == (0 if network else 7), results["network"]


def test_actual_directory_start_resume_and_fresh_continuation(certified):
    grants = restore_paths(serialize_paths(normalize_paths([str(certified.paths["grant"])])))
    session, results = run_cli(certified, grants)
    assert_bounded(results)
    resumed, results = run_cli(certified, grants, session=session)
    assert resumed == session
    assert_bounded(results)
    fresh, results = run_cli(certified, grants)
    assert fresh != session
    assert_bounded(results)
    _, results = run_cli(certified, grants, network=True)
    assert_bounded(results, network=True)


def test_actual_exact_file_pathname_operations(certified):
    grants = normalize_paths([str(certified.paths["file"])])
    _, results = run_cli(certified, grants, file_operations=True)
    assert_bounded(results, directory=False)
    assert results["truncate"]["exit_code"] == results["replace"]["exit_code"] == 0
    assert certified.paths["file"].read_text() == "replacement"
    assert results["rename_sibling"]["exit_code"] == 1
    assert "Operation not permitted" in results["rename_sibling"]["output"]
    assert not (certified.tmp / "moved-file").exists()


@pytest.mark.parametrize("fork", ["none", "all"])
def test_actual_ordinary_children_inherit_bounded_roots(certified, fork):
    grants = normalize_paths([str(certified.paths["grant"])])
    session, results = run_cli(certified, grants, fork=fork)
    assert_bounded(results)
    resumed, results = run_cli(certified, grants, session=session, fork=fork)
    assert resumed == session
    assert_bounded(results)
    records = [[json.loads(line) for line in path.read_text().splitlines()]
               for path in certified.paths["codex-home"].joinpath("sessions").rglob("*.jsonl")]
    children = [rows for rows in records if rows[0]["payload"].get("parent_thread_id") == session]
    assert len(children) == 2
    for rows in children:
        context = next(row["payload"] for row in rows if row["type"] == "turn_context")
        policy = context["sandbox_policy"]
        assert policy["type"] == "workspace-write"
        assert str(certified.paths["grant"].resolve()) in policy["writable_roots"]
        assert str(certified.paths["unrelated"].resolve()) not in policy["writable_roots"]
        assert policy["network_access"] is False
        assert policy["exclude_slash_tmp"] is policy["exclude_tmpdir_env_var"] is True
    # Plain CLI inheritance does not certify HarflowMCP's separately pinned adapter.
    from harflow_mcp.backends.codex_native import CodexNativeAdapter
    parent = SimpleNamespace(backend="codex", freedom="write_in_repo", network=False,
                             model="gpt-6.1-sol", reasoning_effort=None, max_turns=None)
    assert CodexNativeAdapter().eligible(parent, {"model": parent.model},
        {"freedom": parent.freedom, "network": False, "backend_version": VERSION}) is not None
