"""Fixture-only tests for Claude's bounded, observation-only settings inventory."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
from dataclasses import replace
from types import SimpleNamespace

import pytest

from harflow_mcp.backends.base import Enforcement
from harflow_mcp.backends import claude_settings
from harflow_mcp.backends import claude_denials
from harflow_mcp.task_documents import qualify_enforcement


class _VersionProcess:
    def __init__(self, probe, stdout):
        self.probe = probe
        self.stdout = stdout
        self.returncode = None

    def wait(self, timeout=None):
        self.probe.wait_timeouts.append(timeout)
        if timeout is not None and self.probe.wait_timeout is not None:
            raise self.probe.wait_timeout
        if self.probe.late_output is not None and self.probe.write_fd is not None:
            os.write(self.probe.write_fd, self.probe.late_output)
            self.probe.close_writer()
        if self.returncode is None:
            self.returncode = self.probe.exit_code
        return self.returncode

    def poll(self):
        return self.returncode

    def kill(self):
        self.probe.kill_calls += 1
        self.returncode = -9
        self.probe.close_writer()


class _VersionProbe:
    def __init__(self):
        self.output = b"2.1.310\n"
        self.exit_code = 0
        self.wait_timeout = None
        self.late_output = None
        self.launch_error = None
        self.calls = []
        self.wait_timeouts = []
        self.kill_calls = 0
        self.write_fd = None
        self.process = None
        self.stdout = None

    def close_writer(self):
        if self.write_fd is not None:
            os.close(self.write_fd)
            self.write_fd = None

    def popen(self, args, **kwargs):
        self.calls.append((args, kwargs))
        if self.launch_error is not None:
            raise self.launch_error
        read_fd, self.write_fd = os.pipe()
        stdout = os.fdopen(read_fd, "rb", buffering=0)
        self.stdout = stdout
        if self.late_output is None:
            os.write(self.write_fd, self.output)
            self.close_writer()
        self.process = _VersionProcess(self, stdout)
        return self.process


@pytest.fixture(autouse=True)
def fake_claude_version_probe(monkeypatch):
    """Keep settings inventory tests deterministic while exercising real select/read pipes."""
    probe = _VersionProbe()
    monkeypatch.setattr(claude_settings, "subprocess", SimpleNamespace(
        Popen=probe.popen,
        DEVNULL=subprocess.DEVNULL,
        PIPE=subprocess.PIPE,
        SubprocessError=subprocess.SubprocessError,
        TimeoutExpired=subprocess.TimeoutExpired,
    ))
    try:
        yield probe
    finally:
        probe.close_writer()
        if probe.stdout is not None and not probe.stdout.closed:
            probe.stdout.close()


def executable(tmp_path: Path, text: str = "2.1.310\n") -> Path:
    path = tmp_path / "claude-version"
    path.write_text("#!/bin/sh\nprintf '%s' " + repr(text) + "\n")
    path.chmod(0o755)
    return path


def observe(tmp_path: Path, monkeypatch, *, cwd=None, env=None):
    monkeypatch.setattr(claude_settings.sys, "platform", "darwin")
    monkeypatch.setattr(claude_settings, "_managed_directory", lambda _platform: None)
    cwd = Path(cwd or tmp_path / "work")
    cwd.mkdir(parents=True, exist_ok=True)
    child_env = {"HOME": str(tmp_path / "home"), "CLAUDE_CONFIG_DIR": str(tmp_path / "config")}
    child_env.update(env or {})
    return claude_settings.observe(
        Enforcement(freedom="unrestricted", mechanism="test", os_enforced=False, writes_confined=False),
        cwd, child_env, str(executable(tmp_path)),
    )


def write_settings(path: Path, permissions: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"permissions": permissions}))


def statuses(result):
    return {Path(item["path"]): item["status"] for item in result.settings_observation["sources"]}


def test_inventory_combines_scopes_and_keeps_rules_as_candidates(tmp_path, monkeypatch):
    work = tmp_path / "work"
    write_settings(tmp_path / "config/settings.json", {"allow": ["Bash(*)"], "ask": ["Bash(git push)"], "deny": ["Bash(git commit)", "Bash"]})
    write_settings(work / ".claude/settings.json", {"deny": ["Read(./secret)"]})
    write_settings(work / ".claude/settings.local.json", {"ask": ["Write(./release)"]})
    # A parent project file is not part of the measured cwd discovery set.
    write_settings(tmp_path / ".claude/settings.json", {"deny": ["Bash(ancestor)"]})

    enforcement = observe(tmp_path, monkeypatch, cwd=work)
    rules = enforcement.inherited_rules
    assert [(item["effect"], item["rule"]) for item in rules] == [
        ("deny", "Bash(git commit)"), ("deny", "Bash"),
        ("ask", "Bash(git push)"), ("deny", "Read(./secret)"),
        ("ask", "Write(./release)"),
    ]
    assert {Path(item["source"]) for item in rules} == {
        tmp_path / "config/settings.json", work / ".claude/settings.json", work / ".claude/settings.local.json"
    }
    assert all(item["applicability"] == "candidate" for item in rules)
    assert enforcement.settings_observation["version"] == "2.1.310"
    assert enforcement.settings_observation["applicability"] == "cwd_discovery_measured"
    assert enforcement.settings_observation["completeness"] == "partial"
    assert any("unknown" in notice.lower() or "partially observed" in notice.lower() for notice in enforcement.observation_notices)
    assert any("may overlap publishing commands" in notice for notice in enforcement.observation_notices)
    assert "inherited_rules" not in Enforcement(freedom="unrestricted", mechanism="test", os_enforced=False, writes_confined=False).as_dict()


def test_version_probe_uses_expected_process_arguments(tmp_path, monkeypatch, fake_claude_version_probe):
    work = tmp_path / "work"
    env = {"HOME": str(tmp_path / "home"), "CLAUDE_CONFIG_DIR": str(tmp_path / "config")}
    result = observe(tmp_path, monkeypatch, cwd=work, env=env)
    args, kwargs = fake_claude_version_probe.calls[0]
    assert args == [str(tmp_path / "claude-version"), "--version"]
    assert kwargs == {"env": env, "stdin": subprocess.DEVNULL, "stdout": subprocess.PIPE,
                      "stderr": subprocess.DEVNULL, "cwd": work}
    assert result.settings_observation["version"] == "2.1.310"
    assert fake_claude_version_probe.wait_timeouts == [0.3]
    assert fake_claude_version_probe.process.stdout.closed


@pytest.mark.parametrize(("output", "exit_code"), [
    (b"Claude Code 2.1.310\n", 7),
    (b"Claude Code version unavailable\n", 0),
])
def test_nonzero_or_malformed_version_probe_is_unqualified(
    tmp_path, monkeypatch, fake_claude_version_probe, output, exit_code
):
    fake_claude_version_probe.output = output
    fake_claude_version_probe.exit_code = exit_code
    result = observe(tmp_path, monkeypatch)
    assert result.settings_observation["version"] is None
    assert result.settings_observation["applicability"] == "unqualified"
    assert fake_claude_version_probe.process.stdout.closed


def test_late_version_output_is_read_after_process_exit(tmp_path, monkeypatch, fake_claude_version_probe):
    fake_claude_version_probe.output = b""
    fake_claude_version_probe.late_output = b"Claude Code 2.1.310\n"
    result = observe(tmp_path, monkeypatch)
    assert result.settings_observation["version"] == "2.1.310"
    assert result.settings_observation["applicability"] == "cwd_discovery_measured"
    assert fake_claude_version_probe.wait_timeouts == [0.3]
    assert fake_claude_version_probe.process.stdout.closed


def test_timed_out_version_probe_is_killed_and_pipe_closed(tmp_path, monkeypatch, fake_claude_version_probe):
    fake_claude_version_probe.wait_timeout = subprocess.TimeoutExpired("claude --version", 0.3)
    result = observe(tmp_path, monkeypatch)
    assert result.settings_observation["version"] is None
    assert result.settings_observation["applicability"] == "unqualified"
    assert fake_claude_version_probe.process.poll() == -9
    assert fake_claude_version_probe.kill_calls == 1
    assert fake_claude_version_probe.wait_timeouts == [0.3, None]
    assert fake_claude_version_probe.write_fd is None
    assert fake_claude_version_probe.process.stdout.closed


def test_version_probe_spawn_error_keeps_real_file_inventory(tmp_path, monkeypatch, fake_claude_version_probe):
    fake_claude_version_probe.launch_error = OSError("fixture spawn failure")
    work = tmp_path / "work"
    path = tmp_path / "config/settings.json"
    write_settings(path, {"deny": ["Bash(candidate)"]})
    result = observe(tmp_path, monkeypatch, cwd=work)
    assert result.settings_observation["version"] is None
    assert result.settings_observation["applicability"] == "unqualified"
    assert statuses(result)[path] == "readable"
    assert [rule["rule"] for rule in result.inherited_rules] == ["Bash(candidate)"]
    assert len(fake_claude_version_probe.calls) == 1
    assert fake_claude_version_probe.process is None


@pytest.mark.parametrize("payload", [b"{", b"[]", b'{"permissions":{"ask":"Bash"}}', b'{"permissions":{"deny":[2]}}'])
def test_malformed_or_wrong_permission_shapes_are_invalid_and_not_retained(tmp_path, monkeypatch, payload):
    work = tmp_path / "work"
    path = tmp_path / "config/settings.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    result = claude_settings.observe(
        Enforcement(freedom="unrestricted", mechanism="test", os_enforced=False, writes_confined=False),
        work, {"HOME": str(tmp_path / "home"), "CLAUDE_CONFIG_DIR": str(tmp_path / "config")},
        str(executable(tmp_path)),
    )
    assert statuses(result)[path] == "invalid"
    assert result.inherited_rules == ()


def test_missing_unreadable_symlink_special_and_oversize_sources_are_explicit(tmp_path, monkeypatch):
    work = tmp_path / "work"
    config = tmp_path / "config"
    config.mkdir()
    (config / "settings.json").symlink_to(tmp_path / "outside.json")
    (tmp_path / "outside.json").write_text('{"permissions":{"deny":["Bash(secret)"]}}')
    (work / ".claude").mkdir(parents=True)
    (work / ".claude/settings.json").write_text("x" * (claude_settings.MAX_BYTES + 1))
    os.mkfifo(work / ".claude/settings.local.json")
    result = observe(tmp_path, monkeypatch, cwd=work)
    source_status = statuses(result)
    assert source_status[config / "settings.json"] == "symlink"
    assert source_status[work / ".claude/settings.json"] == "oversize"
    assert source_status[work / ".claude/settings.local.json"] == "special"
    assert (tmp_path / "config/settings.json") in source_status  # candidate path is explicit even when unavailable
    assert result.inherited_rules == ()


def test_symlinked_parent_is_not_followed_and_missing_files_are_reported(tmp_path, monkeypatch):
    real = tmp_path / "real"
    write_settings(real / "settings.json", {"deny": ["Bash(should-not-read)"]})
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    work = tmp_path / "work"
    result = observe(tmp_path, monkeypatch, cwd=work, env={"CLAUDE_CONFIG_DIR": str(alias)})
    # An explicitly configured root alias is canonicalized and treated as the same root.
    assert [rule["rule"] for rule in result.inherited_rules] == ["Bash(should-not-read)"]
    assert result.inherited_rules[0]["source"] == str(real / "settings.json")


def test_symlinked_claude_subdirectory_is_not_followed(tmp_path, monkeypatch):
    real = tmp_path / "real"
    write_settings(real / "settings.json", {"deny": ["Bash(should-not-read)"]})
    work = tmp_path / "work"
    work.mkdir()
    (work / ".claude").symlink_to(real, target_is_directory=True)
    result = observe(tmp_path, monkeypatch, cwd=work)
    assert statuses(result)[work / ".claude/settings.json"] == "symlink"
    assert not result.inherited_rules


def test_candidate_version_is_unqualified_outside_measured_version(tmp_path, monkeypatch, fake_claude_version_probe):
    monkeypatch.setattr(claude_settings.sys, "platform", "darwin")
    monkeypatch.setattr(claude_settings, "_managed_directory", lambda _platform: None)
    work = tmp_path / "work"
    work.mkdir()
    exe = executable(tmp_path, "Claude Code 2.1.312\n")
    fake_claude_version_probe.output = b"Claude Code 2.1.312\n"
    result = claude_settings.observe(Enforcement(freedom="unrestricted", mechanism="test", os_enforced=False, writes_confined=False),
                                     work, {"HOME": str(tmp_path / "home"), "CLAUDE_CONFIG_DIR": str(tmp_path / "config")}, str(exe))
    assert result.settings_observation["version"] == "2.1.312"
    assert result.settings_observation["applicability"] == "unqualified"


def test_relocated_config_is_the_user_candidate_and_home_claude_is_not_scanned(tmp_path, monkeypatch):
    work = tmp_path / "work"
    relocated = tmp_path / "relocated"
    write_settings(relocated / "settings.json", {"deny": ["Bash(relocated)"]})
    write_settings(tmp_path / "home/.claude/settings.json", {"deny": ["Bash(home-fallback)"]})
    result = observe(tmp_path, monkeypatch, cwd=work,
                     env={"HOME": str(tmp_path / "home"), "CLAUDE_CONFIG_DIR": str(relocated)})
    assert [rule["rule"] for rule in result.inherited_rules] == ["Bash(relocated)"]
    assert result.inherited_rules[0]["source"] == str(relocated / "settings.json")


def test_managed_candidates_are_reported_without_claiming_they_are_selected(tmp_path, monkeypatch):
    managed = tmp_path / "managed"
    write_settings(managed / "managed-settings.json", {"deny": ["Bash(managed)"]})
    write_settings(managed / "managed-settings.d/10-fixture.json", {"ask": ["Write(*)"]})
    monkeypatch.setattr(claude_settings.sys, "platform", "linux")
    monkeypatch.setattr(claude_settings, "_managed_directory", lambda _platform: managed)
    work = tmp_path / "work"
    work.mkdir()
    result = claude_settings.observe(Enforcement(freedom="unrestricted", mechanism="test", os_enforced=False, writes_confined=False),
        work, {"HOME": str(tmp_path / "home"), "CLAUDE_CONFIG_DIR": str(tmp_path / "config")}, str(executable(tmp_path)))
    managed_rules = [item for item in result.inherited_rules if item["source"].startswith(str(managed))]
    assert [(item["effect"], item["rule"]) for item in managed_rules] == [("deny", "Bash(managed)"), ("ask", "Write(*)")]
    assert all(item["applicability"] == "candidate" for item in managed_rules)
    assert any("Managed tier selection" in item and "unknown" in item for item in result.settings_observation["limitations"])


def test_measured_version_on_linux_is_still_unqualified(tmp_path, monkeypatch):
    monkeypatch.setattr(claude_settings.sys, "platform", "linux")
    monkeypatch.setattr(claude_settings, "_managed_directory", lambda _platform: None)
    work = tmp_path / "work"
    work.mkdir()
    result = claude_settings.observe(Enforcement(freedom="unrestricted", mechanism="test", os_enforced=False, writes_confined=False),
        work, {"HOME": str(tmp_path / "home"), "CLAUDE_CONFIG_DIR": str(tmp_path / "config")}, str(executable(tmp_path)))
    assert result.settings_observation["version"] == "2.1.310"
    assert result.settings_observation["applicability"] == "unqualified"


@pytest.mark.asyncio
async def test_observation_exception_returns_unknown_without_failing_caller():
    from harflow_mcp.tasks import _observe_enforcement

    class Broken:
        def observe_enforcement(self, *_args):
            raise OSError("fixture")
        def observation_unavailable(self, enforcement):
            return replace(enforcement, settings_observation={"applicability": "unknown"},
                           observation_notices=("settings unknown",))

    enforcement = Enforcement(freedom="publish", mechanism="test", os_enforced=False, writes_confined=False)
    result = await _observe_enforcement(Broken(), enforcement, Path("."), {}, "claude")
    assert result.settings_observation == {"applicability": "unknown"}
    assert result.observation_notices == ("settings unknown",)


@pytest.mark.asyncio
async def test_wrong_typed_observer_result_uses_unknown_fallback():
    from harflow_mcp.tasks import _observe_enforcement

    class WrongType:
        def observe_enforcement(self, *_args):
            return (None, None)
        def observation_unavailable(self, enforcement):
            return claude_settings.unavailable(enforcement)

    enforcement = Enforcement(freedom="publish", mechanism="test", os_enforced=False, writes_confined=False)
    result = await _observe_enforcement(WrongType(), enforcement, Path("."), {}, "claude")
    assert isinstance(result, Enforcement)
    assert result.inherited_rules is None
    assert result.settings_observation["applicability"] == "unknown"
    assert result.observation_notices


def test_limits_truncate_inventory_and_rule_text_without_reading_unbounded_content(tmp_path, monkeypatch):
    work = tmp_path / "work"
    cfg = tmp_path / "config/settings.json"
    write_settings(cfg, {"deny": ["Bash(" + "x" * (claude_settings.MAX_RULE_LENGTH + 1) + ")"] + [f"Bash(rule-{i})" for i in range(claude_settings.MAX_RULES + 2)]})
    result = observe(tmp_path, monkeypatch, cwd=work)
    assert len(result.inherited_rules) == claude_settings.MAX_RULES
    assert all(len(rule["rule"]) <= claude_settings.MAX_RULE_LENGTH for rule in result.inherited_rules)
    assert any("limit reached" in item for item in result.settings_observation["limitations"])


def test_concurrent_growth_is_charged_to_aggregate_budget_before_later_candidates(tmp_path, monkeypatch):
    work = tmp_path / "work"
    config_file = tmp_path / "config/settings.json"
    small = b'{"permissions":{}}'
    config_file.parent.mkdir(parents=True)
    config_file.write_bytes(small)
    write_settings(work / ".claude/settings.json", {})
    write_settings(work / ".claude/settings.local.json", {})
    monkeypatch.setattr(claude_settings.sys, "platform", "linux")
    managed = tmp_path / "managed"
    write_settings(managed / "managed-settings.json", {})
    monkeypatch.setattr(claude_settings, "_managed_directory", lambda _platform: managed)
    monkeypatch.setattr(claude_settings, "MAX_BYTES", 32)
    monkeypatch.setattr(claude_settings, "MAX_TOTAL_BYTES", 40)

    original_open = claude_settings.os.open
    opened = []
    grew = False

    def grow_after_lstat(candidate, flags, *args, **kwargs):
        nonlocal grew
        if os.fspath(candidate) == os.fspath(config_file):
            opened.append(os.fspath(candidate))
            if not grew:
                grew = True
                config_file.write_bytes(b"x" * 100)
        return original_open(candidate, flags, *args, **kwargs)

    monkeypatch.setattr(claude_settings.os, "open", grow_after_lstat)
    result = claude_settings.observe(
        Enforcement(freedom="unrestricted", mechanism="test", os_enforced=False, writes_confined=False),
        work, {"HOME": str(tmp_path / "home"), "CLAUDE_CONFIG_DIR": str(tmp_path / "config")},
        str(executable(tmp_path)),
    )
    source_status = statuses(result)
    assert source_status[config_file] == "truncated"
    assert source_status[work / ".claude/settings.json"] == "truncated"
    assert opened == [os.fspath(config_file)]
    assert any("Settings total byte limit reached." in item for item in result.settings_observation["limitations"])


def test_unreadable_settings_are_reported_without_exposing_os_error(tmp_path, monkeypatch):
    work = tmp_path / "work"
    path = tmp_path / "config/settings.json"
    write_settings(path, {"deny": ["Bash(secret)"]})
    real_open = claude_settings.os.open

    def denied_open(candidate, flags, *args, **kwargs):
        if os.fspath(candidate) == os.fspath(path):
            raise PermissionError("private fixture path should not leak")
        return real_open(candidate, flags, *args, **kwargs)

    monkeypatch.setattr(claude_settings.os, "open", denied_open)
    result = observe(tmp_path, monkeypatch, cwd=work)
    assert statuses(result)[path] == "unreadable"
    assert not result.inherited_rules
    assert "private fixture path" not in json.dumps(result.settings_observation)


def test_unavailable_observation_omits_unknown_inventory_instead_of_claiming_empty():
    enforcement = claude_settings.unavailable(
        Enforcement(freedom="publish", mechanism="test", os_enforced=False, writes_confined=False))
    assert enforcement.inherited_rules is None
    assert "inherited_rules" not in enforcement.as_dict()
    assert enforcement.settings_observation["applicability"] == "unknown"


@pytest.mark.asyncio
async def test_unrelated_claude_dispatch_does_not_probe_host_settings(tmp_path):
    from harflow_mcp.backends import get
    from harflow_mcp.tasks import _observe_enforcement

    backend = get("claude")
    enforcement = Enforcement(freedom="publish", mechanism="test", os_enforced=False, writes_confined=False)
    result = await _observe_enforcement(backend, enforcement, tmp_path, {}, "/bin/echo")
    assert result is enforcement


@pytest.mark.parametrize("observation", [
    {"inherited_rules": [{"source": "x", "effect": "allow", "rule": "Bash", "applicability": "candidate"}]},
    {"settings_observation": {"version": None, "applicability": [], "sources": [], "completeness": "partial", "limitations": []}},
    {"settings_observation": {"version": None, "applicability": "unknown", "sources": [{"path": "x", "scope": {}, "status": "readable", "applicability": "candidate"}], "completeness": "partial", "limitations": []}},
    {"settings_observation": {"version": None, "applicability": "unknown", "sources": [{"path": "x", "scope": "user", "status": "invented", "applicability": "candidate"}], "completeness": "partial", "limitations": []}},
    {"settings_observation": {"version": None, "applicability": "unknown", "sources": [], "completeness": "partial", "limitations": []}, "observation_notices": None},
])
def test_observation_normalization_is_tolerant_but_core_authority_stays_strict(observation):
    core = {"freedom": "unrestricted", "mechanism": "test", "os_enforced": False, "writes_confined": False}
    malformed_observation = {**core, **observation}
    qualify_enforcement(malformed_observation, require_core=True)
    assert "inherited_rules" not in malformed_observation
    assert malformed_observation["settings_observation"]["applicability"] == "unknown"
    assert malformed_observation["observation_notices"]
    with pytest.raises(ValueError, match="Incomplete enforcement"):
        qualify_enforcement({"inherited_rules": "malformed"}, require_core=True)


def test_unknown_candidate_rule_fields_are_dropped_during_normalization():
    value = {"freedom": "unrestricted", "mechanism": "test", "os_enforced": False, "writes_confined": False,
        "inherited_rules": [{"source": "fixture", "effect": "ask", "rule": "Bash", "applicability": "candidate", "future_vendor_key": "opaque"}]}
    qualify_enforcement(value, require_core=True)
    assert value["inherited_rules"] == [{"source": "fixture", "effect": "ask", "rule": "Bash", "applicability": "candidate"}]


def test_generic_ask_refusal_is_caveated_and_spoof_without_result_is_not_attributed():
    ask = "Claude requested permissions to use Bash, but you haven't granted it yet."
    denial = {"tool_name": "Bash", "tool_use_id": "tool-1", "tool_input": {"command": "git push"}}
    event_confirmed = claude_denials.classify_one(denial, {
        "claude_code_version": "2.1.310", "denial_result_ids": ["tool-1"], "tool_error_text": {"tool-1": ask}
    }, ())
    assert event_confirmed["denial_category"] == "approval_required"
    assert "inherited ask or deny rules can remain" in event_confirmed["denial_remedy"]
    assert "source" not in event_confirmed and "rule" not in event_confirmed
    spoofed = claude_denials.classify_one(denial, {
        "claude_code_version": "2.1.310", "denial_result_ids": [], "tool_error_text": {"tool-1": ask}
    }, ())
    assert spoofed["denial_category"] == "unclassified"
    assert spoofed["classification_basis"]["reason"] == "stream_only"
    unmeasured = claude_denials.classify_one(denial, {
        "claude_code_version": "2.1.312", "denial_result_ids": ["tool-1"], "tool_error_text": {"tool-1": ask}
    }, ())
    assert unmeasured["denial_category"] == "unclassified"
    assert unmeasured["classification_basis"]["reason"] in {"unmeasured_version", "unmeasured_text_version"}


def test_observation_fields_do_not_change_nested_authority_comparison():
    from harflow_mcp.backends.base import nested_enforcement_violation

    parent = Enforcement(freedom="write_in_repo", mechanism="bounded", os_enforced=True,
                         writes_confined=True, writable_roots=("repo",)).as_dict()
    child = dict(parent)
    child["inherited_rules"] = [{"source": "/fixture", "effect": "deny", "rule": "Bash",
                                 "applicability": "candidate"}]
    child["settings_observation"] = {"version": "2.1.310", "applicability": "unqualified",
        "sources": [], "completeness": "partial", "limitations": ["unknown"]}
    child["observation_notices"] = ["may restrict"]
    assert nested_enforcement_violation(parent, child, parent_backend="claude", child_backend="claude",
                                        parent_repo="/repo", child_repo="/repo") is None


async def test_preview_and_dispatch_keep_observation_and_dispatch_notices(tmp_path, monkeypatch):
    from harflow_mcp import state_ownership, store, tasks
    from harflow_mcp.backends.base import Invocation
    from harflow_mcp.state_layout import resolve_layout
    from test_registry import _TrivialBackend

    class ObservedBackend(_TrivialBackend):
        name = "observed-test"
        def __init__(self):
            self.observations = []
        def observe_enforcement(self, enforcement, cwd, env, executable):
            self.observations.append((Path(cwd), dict(env), executable))
            rule = {"source": "/fixture/settings.json", "effect": "deny", "rule": "Bash(git push)", "applicability": "candidate"}
            return replace(enforcement, inherited_rules=(rule,), settings_observation={
                "version": "2.1.310", "applicability": "cwd_discovery_measured", "sources": [
                    {"path": rule["source"], "scope": "user", "status": "readable", "applicability": "candidate"}],
                "completeness": "partial", "limitations": ["fixture observation"]},
                observation_notices=("Candidate Bash rule may restrict this run.",))

    root = tmp_path / "state"
    layout = resolve_layout(root=root)
    monkeypatch.setenv("HOME", str(tmp_path / "child-home"))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "child-config"))
    with state_ownership.state_session(layout):
        (root / "tasks").mkdir(exist_ok=True)
        registry = tasks.TaskRegistry(log_dir=root / "tasks")
        backend = ObservedBackend()
        preview = await registry.preview_permissions(tmp_path, backend=backend, freedom="publish")
        assert preview["enforcement"]["inherited_rules"][0]["rule"] == "Bash(git push)"
        assert any("may restrict" in notice for notice in preview["notices"])
        assert backend.observations[-1][0] == tmp_path
        assert backend.observations[-1][2] == "/bin/echo"
        assert backend.observations[-1][1]["HOME"] == str(tmp_path / "child-home")
        assert backend.observations[-1][1]["CLAUDE_CONFIG_DIR"] == str(tmp_path / "child-config")

        actual_config = tmp_path / "dispatch-config"
        task = await registry._spawn(Invocation(["/bin/echo", "{}"], env={"CLAUDE_CONFIG_DIR": str(actual_config)}), backend=backend,
            prompt="fixture", repo_path=tmp_path, session_id=None, freedom="publish",
            max_turns=None, model=None, reasoning_effort=None)
        await task.done.wait()
        assert task.enforcement["inherited_rules"][0]["rule"] == "Bash(git push)"
        assert any("may restrict" in notice for notice in task.bridge_notices)
        assert backend.observations[-1][1]["HOME"] == str(tmp_path / "child-home")
        assert backend.observations[-1][1]["CLAUDE_CONFIG_DIR"] == str(actual_config)
        saved = store.read(root / "tasks", task.task_id)
        assert saved.enforcement["settings_observation"]["version"] == "2.1.310"
        # Historical reads return the recorded launch snapshot; they do not invoke observation.
        calls = len(backend.observations)
        assert store.read(root / "tasks", task.task_id).enforcement == saved.enforcement
        assert len(backend.observations) == calls
        # A resumed execution gets a fresh launch snapshot while the source keeps its old one.
        monkeypatch.setattr(tasks, "get_backend", lambda _name: backend)
        parent = replace(saved, session_id="fixture-session")
        store.write(root / "tasks", parent)
        resumed = await registry.resume_record(parent, "resume fixture", task_id="fixture-resumed")
        await resumed.done.wait()
        assert resumed.enforcement["inherited_rules"] == task.enforcement["inherited_rules"]
        assert len(backend.observations) == calls + 1
        assert store.read(root / "tasks", task.task_id).enforcement == saved.enforcement
        assert store.read(root / "tasks", resumed.task_id).enforcement == resumed.enforcement
