"""Regression coverage for ordinary Codex exec reporting and nested admission."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile

import pytest

from harflow_mcp import backends
from harflow_mcp.backends.base import (
    NestedDispatchRefused,
    check_nested_enforcement,
    requested_cap_for_comparison,
)
from harflow_mcp.backends.codex import CodexBackend
from harflow_mcp.extra_paths import normalize_paths, serialize_paths


@pytest.mark.parametrize("freedom,network", [
    ("read_only", None), ("read_only", False),
    ("write_in_repo", None), ("write_in_repo", False), ("write_in_repo", True),
    ("publish", None), ("publish", False), ("publish", True),
    ("unrestricted", None), ("unrestricted", True),
])
def test_ordinary_exec_does_not_claim_effective_sandbox(freedom, network, monkeypatch, tmp_path):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    backend = CodexBackend()
    result = backend.enforcement(freedom, network)

    assert result.os_enforced is False
    assert result.writes_confined is False
    expected_network = (
        "unrestricted" if freedom == "unrestricted"
        else "enabled" if backend._resolve_network(freedom, network)
        else "not_controlled"
    )
    assert result.network_access == expected_network
    assert any("allow rules" in caveat for caveat in result.caveats)


@pytest.mark.parametrize("freedom,network,expected", [
    ("read_only", None, "not_controlled"),
    ("write_in_repo", False, "not_controlled"),
    ("write_in_repo", True, "enabled"),
    ("publish", False, "not_controlled"),
    ("publish", True, "enabled"),
    ("unrestricted", None, "unrestricted"),
])
def test_network_report_distinguishes_requested_setting_from_effective_barrier(
    freedom, network, expected, monkeypatch, tmp_path
):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    enforcement = CodexBackend().enforcement(freedom, network)
    assert enforcement.network_access == expected
    assert enforcement.os_enforced is False
    assert enforcement.writes_confined is False


@pytest.mark.parametrize("contents", [
    "[sandbox_workspace_write]\nwritable_roots = [\"/one\"]\n",
    "[sandbox_workspace_write]\nwritable_roots = \"/one\"\n",
    "[sandbox_workspace_write]\nwritable_roots = [1]\n",
    "not = [valid toml",
])
def test_config_roots_are_partial_observations_and_bad_values_are_not_authority(
    contents, monkeypatch, tmp_path
):
    codex_home = tmp_path / "codex"
    codex_home.mkdir()
    (codex_home / "config.toml").write_text(contents)
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    enforcement = CodexBackend().enforcement("write_in_repo")

    assert enforcement.os_enforced is False
    assert enforcement.writes_confined is False
    assert any("partial observations" in caveat for caveat in enforcement.caveats)
    if 'writable_roots = ["/one"]' in contents:
        assert "/one" in enforcement.writable_roots
    else:
        assert "/one" not in enforcement.writable_roots


def test_missing_or_unreadable_config_does_not_claim_no_additional_roots(monkeypatch, tmp_path):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "missing"))
    enforcement = CodexBackend().enforcement("write_in_repo")
    assert any("partial observations" in caveat for caveat in enforcement.caveats)

    codex_home = tmp_path / "unreadable"
    codex_home.mkdir()
    config = codex_home / "config.toml"
    config.mkdir()  # read_text raises OSError, independently of process permissions
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    enforcement = CodexBackend().enforcement("write_in_repo")
    assert any("partial observations" in caveat for caveat in enforcement.caveats)


def test_requested_comparison_caps_restore_old_codex_claim_without_mutating_live_report(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    backend = CodexBackend()
    old = replace(backend.enforcement("write_in_repo"), os_enforced=True, writes_confined=True,
                  network_access="blocked", writable_roots=("historical-root",))
    live = backend.enforcement("write_in_repo", False).as_dict()
    normalized = requested_cap_for_comparison(backend, old.as_dict(), "write_in_repo", None)

    assert normalized["os_enforced"] is True
    assert normalized["writes_confined"] is True
    assert normalized["network_access"] == "blocked"
    assert normalized["writable_roots"] == ["historical-root"]
    assert live["os_enforced"] is False
    assert live["writes_confined"] is False


def test_requested_comparison_caps_cover_new_records_and_keep_network_intent(monkeypatch, tmp_path):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    backend = CodexBackend()
    new = backend.enforcement("write_in_repo", False).as_dict()
    normalized = requested_cap_for_comparison(backend, new, "write_in_repo", False)
    assert normalized["os_enforced"] is True
    assert normalized["writes_confined"] is True
    assert normalized["network_access"] == "blocked"
    assert requested_cap_for_comparison(backend, new, "write_in_repo", True)["network_access"] == "enabled"


def test_profile_root_observation_stays_compatible_with_saved_parent_scope(monkeypatch, tmp_path):
    codex_home = tmp_path / "codex"
    codex_home.mkdir()
    (codex_home / "config.toml").write_text(
        'profile="work"\n[sandbox_workspace_write]\nwritable_roots=["/global"]\n'
        '[profiles.work.sandbox_workspace_write]\nwritable_roots=["/profile"]\n'
    )
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    backend = CodexBackend()
    parent = replace(backend.enforcement("write_in_repo"), os_enforced=True,
                     writes_confined=True, network_access="blocked")
    child = requested_cap_for_comparison(backend, backend.enforcement("write_in_repo"),
                                         "write_in_repo", None)
    assert "/profile" in parent.writable_roots
    assert child["writable_roots"] == list(parent.writable_roots)


@pytest.mark.parametrize("bad_record", [
    {},
    {"freedom": "publish"},
    {"freedom": "write_in_repo", "writable_roots": "not-a-list"},
])
def test_requested_comparison_fails_closed_for_incomplete_records(bad_record):
    with pytest.raises(NestedDispatchRefused) as exc:
        requested_cap_for_comparison(CodexBackend(), bad_record, "write_in_repo", None)
    assert exc.value.rule == "parent_enforcement_unrecorded"


@pytest.mark.parametrize("field,value", [
    ("os_enforced", "true"),
    ("writable_roots", "/tmp"),
    ("network_access", "maybe"),
    ("freedom", "publish"),
    ("publish_attempts_allowed", True),
])
def test_requested_comparison_rejects_malformed_complete_policy(field, value, monkeypatch, tmp_path):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    policy = CodexBackend().enforcement("write_in_repo").as_dict()
    policy[field] = value
    with pytest.raises(NestedDispatchRefused) as exc:
        requested_cap_for_comparison(CodexBackend(), policy, "write_in_repo", None)
    assert exc.value.rule == "parent_enforcement_unrecorded"


def test_requested_comparison_preserves_recorded_grants_and_never_reads_current_config(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    backend = CodexBackend()
    policy = backend.enforcement("write_in_repo").as_dict()
    (tmp_path / "grant").mkdir()
    grants = serialize_paths(normalize_paths([str(tmp_path / "grant")]))
    policy.update(caller_approval_tools=["commandExecution"],
                  extra_writable_paths=grants,
                  extra_allowed_commands=["make test"])
    monkeypatch.setattr(backend, "_configured_writable_roots",
                        lambda: pytest.fail("comparison must not read current config"))
    compared = requested_cap_for_comparison(backend, policy, "write_in_repo", None)
    assert compared["caller_approval_tools"] == ["commandExecution"]
    assert compared["extra_writable_paths"] == grants
    assert compared["extra_allowed_commands"] == ["make test"]
    assert policy["writes_confined"] is False


def test_unavailable_backend_fails_closed():
    with pytest.raises(NestedDispatchRefused) as exc:
        requested_cap_for_comparison(None, {}, "write_in_repo", None)
    assert exc.value.rule == "parent_enforcement_unrecorded"


def test_other_backend_comparison_caps_remain_identity():
    backend = backends.get("claude")
    enforcement = backend.enforcement("write_in_repo")
    compared = requested_cap_for_comparison(backend, enforcement, "write_in_repo", None)
    assert compared["writes_confined"] is enforcement.writes_confined


def test_group_output_and_app_server_reports_do_not_inherit_ordinary_exec_caveats(tmp_path):
    backend = CodexBackend()
    ordinary = backend.enforcement("read_only")
    qualified_reports = (
        backend.group_report_output_enforcement(ordinary, tmp_path / "report.json"),
        backend.approval_enforcement(ordinary, "0.0.0-test", True, False),
    )
    ordinary_exec_caveat = next(c for c in ordinary.caveats if "allow rules" in c)
    for report in qualified_reports:
        assert ordinary_exec_caveat not in report.caveats
        assert report.os_enforced is False
        assert report.writes_confined is False
        assert report.network_access == "not_controlled"


def test_nested_admission_uses_historical_requested_caps_and_rejects_scope_weakening(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    codex = CodexBackend()
    parent = replace(codex.enforcement("write_in_repo"), os_enforced=True,
                     writes_confined=True, network_access="blocked",
                     writable_roots=("the working directory", "/tmp", "$TMPDIR"))
    parent_cap = requested_cap_for_comparison(codex, parent.as_dict(), "write_in_repo", None)
    child = requested_cap_for_comparison(
        codex, codex.enforcement("write_in_repo", False), "write_in_repo", False
    )
    # Same backend/repository/root and blocked network remains admissible.
    check_nested_enforcement(parent_cap, child, parent_backend="codex", child_backend="codex",
                             parent_repo=str(tmp_path), child_repo=str(tmp_path))

    for kwargs, expected_rule in [
        ({"child_backend": "claude"}, "backend"),
        ({"child_repo": str(tmp_path.parent / "outside")}, "repo"),
    ]:
        params = dict(parent_backend="codex", child_backend="codex",
                      parent_repo=str(tmp_path), child_repo=str(tmp_path))
        params.update(kwargs)
        with pytest.raises(NestedDispatchRefused) as exc:
            check_nested_enforcement(parent_cap, child, **params)
        assert exc.value.rule == expected_rule

    network_enabled = requested_cap_for_comparison(
        codex, codex.enforcement("write_in_repo", True), "write_in_repo", True
    )
    with pytest.raises(NestedDispatchRefused) as exc:
        check_nested_enforcement(parent_cap, network_enabled, parent_backend="codex",
                                 child_backend="codex", parent_repo=str(tmp_path),
                                 child_repo=str(tmp_path))
    assert exc.value.rule == "network_access"


def test_unrestricted_parent_does_not_turn_requested_roots_into_effective_confinement(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    backend = CodexBackend()
    parent = backend.enforcement("unrestricted")
    normalized = requested_cap_for_comparison(backend, parent, "unrestricted", None)
    assert normalized["writes_confined"] is False


def test_resolve_lineage_admits_same_codex_scope_and_refuses_backend_or_network_weakening(
    monkeypatch, tmp_path
):
    from types import SimpleNamespace

    from harflow_mcp.tasks import TaskRegistry

    monkeypatch.setattr("harflow_mcp.workflow_hooks.refuse_managed", lambda *_: None)
    monkeypatch.setattr("harflow_mcp.caller_policy.signals", lambda: (None, 0))
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    registry = TaskRegistry(log_dir=tmp_path / "tasks")
    parent_backend = CodexBackend()
    current = parent_backend.enforcement("write_in_repo")
    legacy = replace(current, os_enforced=True, writes_confined=True, network_access="blocked",
                     writable_roots=("the working directory", "/tmp", "$TMPDIR"))
    child = parent_backend.enforcement("write_in_repo", False)

    for report in (current, legacy):
        caller = SimpleNamespace(record=SimpleNamespace(
            task_id="parent", max_depth=4, depth=0, root_task_id=None,
            enforcement=report.as_dict(), backend="codex", freedom="write_in_repo",
            network=None, repo_path=str(tmp_path), caller_write_approvals=False,
        ), method="test")
        result = registry._resolve_lineage(caller, child_enforcement=child,
                                           child_backend="codex", child_repo=tmp_path,
                                           child_network=False)
        assert result[0] == "parent"
        with pytest.raises(NestedDispatchRefused) as exc:
            registry._resolve_lineage(caller, child_enforcement=child,
                                      child_backend="claude", child_repo=tmp_path,
                                      child_network=False)
        assert exc.value.rule == "os_enforced"
        with pytest.raises(NestedDispatchRefused) as exc:
            registry._resolve_lineage(caller, child_enforcement=parent_backend.enforcement("write_in_repo", True),
                                      child_backend="codex", child_repo=tmp_path,
                                      child_network=True)
        assert exc.value.rule == "network_access"


@pytest.mark.cli_integration
@pytest.mark.skipif(not os.environ.get("HARFLOW_CLI_INTEGRATION"),
                    reason="opt in with HARFLOW_CLI_INTEGRATION=1")
def test_codex_exec_inherited_project_rules_bypass_requested_sandbox_without_paid_api(
    monkeypatch,
):
    """Real Codex 0.164.0, isolated HOME and localhost-only Responses API."""
    from fake_responses_api import LocalResponses

    executable = shutil.which("codex")
    if sys.platform != "darwin" or executable is None:
        pytest.skip("measured on codex-cli 0.164.0 on macOS only")
    version = subprocess.run([executable, "--version"], capture_output=True, text=True,
                             timeout=20, check=True).stdout.strip()
    if version != "codex-cli 0.164.0":
        pytest.skip("requires codex-cli 0.164.0")

    workspace = Path(__file__).resolve().parents[2]
    fixture = Path(tempfile.mkdtemp(prefix=".hf40-cli-fixture-", dir=workspace))
    real_config = Path.home() / ".codex" / "config.toml"
    original_real_config = real_config.read_bytes() if real_config.exists() else None
    try:
        repo, outside = fixture / "repo", fixture / "outside"
        home, codex_home, tmpdir = fixture / "home", fixture / "codex-home", fixture / "tmp"
        for directory in (repo, outside, home, codex_home, tmpdir):
            directory.mkdir()
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        subprocess.run(["git", "-C", str(repo), "-c", "user.name=test", "-c",
                        "user.email=test@example.invalid", "commit", "-q", "--allow-empty",
                        "-m", "fixture"], check=True)
        project_codex = repo / ".codex"
        rules = project_codex / "rules"
        rules.mkdir(parents=True)
        (project_codex / "config.toml").write_text(
            "[sandbox_workspace_write]\nwritable_roots=[]\nnetwork_access=false\n"
            "[shell_environment_policy.set]\nHF40_PROJECT=\"loaded\"\n"
        )
        (rules / "probe.rules").write_text(
            'prefix_rule(pattern=["touch"], decision="allow")\n'
            'prefix_rule(pattern=["curl"], decision="allow")\n'
        )
        user_config = codex_home / "config.toml"
        config_body = (
            'model_provider="local"\nmodel="gpt-6.1-sol"\n'
            '[model_providers.local]\nname="local"\nwire_api="responses"\n'
            'requires_openai_auth=false\nbase_url={base}\n'
            '[projects.{project}]\ntrust_level="trusted"\n'
        )
        marker = outside / "written.txt"
        scrubbed = {key: value for key, value in os.environ.items()
                    if not key.startswith(("CODEX", "OPENAI", "ANTHROPIC", "CLAUDE",
                                          "OPENCODE", "VIBE", "XDG_", "HARFLOW_"))}
        real_config_digest = hashlib.sha256(original_real_config).hexdigest() if original_real_config is not None else None

        with LocalResponses(fixture).serve() as api:
            api.nonce = "hf40_rules_probe"
            api.commands = [
                ("outside_write", "touch " + shlex.quote(str(marker))),
                ("project_environment", "printenv HF40_PROJECT"),
                ("localhost", "curl -fsS --max-time 3 " + shlex.quote(api.url + "/health")),
            ]
            rendered = config_body.format(base=json.dumps(api.url + "/v1"),
                                          project=json.dumps(str(repo)))
            user_config.write_text(rendered)
            env = dict(scrubbed, HOME=str(home), CODEX_HOME=str(codex_home),
                       TMPDIR=str(tmpdir),
                       PATH=str(Path(executable).parent) + os.pathsep + os.environ.get("PATH", ""))
            backend = CodexBackend()
            invocation = backend.build_start_argv(
                api.nonce, repo=repo, freedom="read_only", session_id=None,
                model="gpt-6.1-sol", max_turns=None, reasoning_effort=None, network=False,
            )
            run = subprocess.run(
                invocation.argv, cwd=repo, env=env, stdin=subprocess.DEVNULL,
                capture_output=True, text=True, timeout=45,
            )
            assert run.returncode == 0, "isolated Codex fixture did not complete"
            assert marker.is_file(), "trusted project allow rule did not permit the outside write"
            results = api.results()
            assert "loaded" in str(results.get("project_environment"))
            assert "local fixture" in str(results.get("localhost"))
            assert user_config.read_text() == rendered

        current_real_config = real_config.read_bytes() if real_config.exists() else None
        current_digest = hashlib.sha256(current_real_config).hexdigest() if current_real_config is not None else None
        assert current_digest == real_config_digest
    finally:
        shutil.rmtree(fixture, ignore_errors=True)
