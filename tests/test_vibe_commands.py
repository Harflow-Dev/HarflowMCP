"""Dispatch grants match original Vibe Bash requests without changing native policy."""
from copy import deepcopy
from dataclasses import replace

import pytest

from harflow_mcp.backends.base import Invocation, UnsupportedCapability
from harflow_mcp.backends import vibe_commands as grants
from harflow_mcp.backends import vibe_acp
from harflow_mcp.backends.vibe import VibeBackend
from harflow_mcp.command_grants import CommandGrantError


def invocation(tmp_path, patterns=("make test",)):
    return grants.configure(Invocation(["vibe"]), patterns, "write_in_repo", tmp_path, "3.0.3")


def request(command="make test"):
    return {"native_method": "session/request_permission",
            "action": {"tool_name": "bash", "input": {"command": command}},
            "required_permissions": [{"scope": "command_pattern", "invocation_pattern": command}]}


def test_exact_and_prefix_are_distinct(tmp_path):
    exact = invocation(tmp_path)
    assert grants.automatic_decision(request(), exact)
    assert not grants.automatic_decision(request("make test extra"), exact)
    prefix = invocation(tmp_path, ("make test:*",))
    assert grants.automatic_decision(request("make test extra"), prefix)
    assert not grants.automatic_decision(request("make testing"), prefix)


@pytest.mark.parametrize("command", [
    "make test; touch marker", "make test > marker", "make test && touch marker",
    "make test $(touch marker)", "make test `touch marker`", "make test\nwhoami",
    "make test /outside/file", "make test ../outside", "make test *",
])
def test_prefix_cannot_grant_shell_syntax_or_outside_operands(tmp_path, command):
    assert not grants.automatic_decision(request(command), invocation(tmp_path, ("make test:*",)))


@pytest.mark.parametrize("scope", ["outside_directory", "file_pattern", "url_pattern", "future_scope"])
def test_only_command_scope_is_eligible(tmp_path, scope):
    action = request()
    action["required_permissions"].append({"scope": scope, "invocation_pattern": "make test"})
    assert not grants.automatic_decision(action, invocation(tmp_path))


@pytest.mark.parametrize("mutation", ["tool", "method", "input", "permissions", "invocation"])
def test_ambiguous_requests_are_not_automatically_granted(tmp_path, mutation):
    action = request()
    if mutation == "tool":
        action["action"]["tool_name"] = "write_file"
    elif mutation == "method":
        action["native_method"] = "other/request"
    elif mutation == "input":
        action["action"]["input"]["cwd"] = "/outside"
    elif mutation == "permissions":
        action["required_permissions"] = []
    else:
        action["required_permissions"][0]["invocation_pattern"] = "different command"
    assert not grants.automatic_decision(action, invocation(tmp_path))


def test_metadata_is_frozen_and_retains_unrelated_native_settings(tmp_path):
    original = Invocation(["vibe"], native_settings={"other": {"keep": True}})
    configured = grants.configure(original, ("make test",), "publish", tmp_path, "3.0.3")
    assert configured.native_settings["other"] == original.native_settings["other"]
    assert grants.KEY not in original.native_settings
    grants.assert_safe(configured, "publish")
    tampered = deepcopy(configured)
    tampered.native_settings[grants.KEY]["patterns"].append("make other")
    with pytest.raises(CommandGrantError, match="changed"):
        grants.assert_safe(tampered, "publish")
    assert not grants.automatic_decision(request(), tampered)
    with pytest.raises(CommandGrantError, match="differs"):
        grants.assert_safe(replace(configured, allowed_commands=("make other",)), "publish")


@pytest.mark.parametrize("version", [None, "3.0.1", "3.0.4"])
def test_unknown_versions_refuse(tmp_path, version):
    with pytest.raises(UnsupportedCapability):
        grants.configure(Invocation(["vibe"]), ("make test",), "publish", tmp_path, version)


def test_vibe302_command_grant_configuration_refuses(tmp_path):
    with pytest.raises(UnsupportedCapability, match="qualified Vibe 3.0.3"):
        grants.configure(Invocation(["vibe"]), ("make test",), "write_in_repo", tmp_path, "3.0.2")


@pytest.mark.parametrize("approval", ["caller", "caller_write"])
def test_vibe302_acp_approval_preparation_refuses_before_prompt(tmp_path, approval):
    write = approval == "caller_write"
    invocation = Invocation(["vibe"], caller_approvals=True)
    with pytest.raises(UnsupportedCapability, match="qualified only on 3.0.3"):
        vibe_acp.prepare(
            invocation,
            {"repo": tmp_path, "prompt": "isolated approval test"},
            "write_in_repo",
            version="3.0.2",
            write=write,
            caller_approvals=True,
        )


def test_vibe302_caller_write_approval_validation_refuses():
    with pytest.raises(UnsupportedCapability, match="qualified Vibe 3.0.3"):
        VibeBackend().validate_caller_write_approvals(True, True, "3.0.2")


def test_read_only_and_shell_program_grants_refuse(tmp_path):
    with pytest.raises(UnsupportedCapability):
        grants.configure(Invocation(["vibe"]), ("make test",), "read_only", tmp_path, "3.0.3")
    with pytest.raises(UnsupportedCapability):
        invocation(tmp_path, ("bash -c echo",))


def test_control_context_uses_same_frozen_validation(tmp_path):
    configured = invocation(tmp_path)
    payload = deepcopy(configured.native_settings[grants.KEY])
    assert grants.automatic_decision(request(), payload)
    assert grants.automatic_decision(request(), {grants.KEY: payload})
    payload["patterns"].append("make other")
    assert not grants.automatic_decision(request(), payload)
