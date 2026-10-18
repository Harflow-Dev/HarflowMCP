"""Harflow/PB caller identity and authority boundaries."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from harflow_mcp import artifacts, backends, caller_policy, server, takeover, state_ownership
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.backends.base import UnsupportedCapability


VALID_ID = "0199a3f2-7c1e-7b8a-9d0e-123456789abc"


@pytest.mark.parametrize(
    ("environ", "present", "depth"),
    [
        ({}, False, 0),
        ({"HARFLOW_TASK_ID": VALID_ID}, True, 0),
        ({"HARFLOW_TASK_ID": VALID_ID, "PB_TASK_ID": VALID_ID}, True, 0),
        ({"HARFLOW_TASK_ID": VALID_ID, "HARFLOW_DEPTH": "2", "PB_DEPTH": "5"}, True, 5),
    ],
)
def test_signals_accepts_human_or_matching_worker_environment(environ, present, depth):
    assert caller_policy.signals(environ) == (present, depth)


@pytest.mark.parametrize(
    "environ",
    [
        {"HARFLOW_TASK_ID": VALID_ID, "PB_TASK_ID": "other"},
        {"PB_TASK_ID": VALID_ID},
        {"HARFLOW_TASK_ID": "", "HARFLOW_DEPTH": "1"},
        {"HARFLOW_DEPTH": "-1"},
        {"PB_DEPTH": "abc"},
        {"HARFLOW_TASK_ID": "../outside"},
    ],
)
def test_signals_rejects_conflicting_empty_malformed_and_foreign_ids(environ):
    with pytest.raises(ValueError):
        caller_policy.signals(environ)


@pytest.mark.parametrize("environ", [{"HARFLOW_TASK_ID": VALID_ID}, {"PB_TASK_ID": VALID_ID}])
def test_worker_environment_requires_confirmed_related_parent(tmp_path, environ, monkeypatch):
    monkeypatch.setattr(
        "harflow_mcp.lineage.detect_caller_detail",
        lambda *args, **kwargs: SimpleNamespace(caller=None, undecidable=None),
    )
    with pytest.raises(ValueError, match="Foreign or conflicting|missing or unconfirmed"):
        caller_policy.require_mutation(tmp_path, environ=environ)


def test_unrelated_confirmed_parent_is_refused(tmp_path):
    record = SimpleNamespace(task_id="different")
    caller = SimpleNamespace(record=record)
    with pytest.raises(ValueError, match="does not match"):
        caller_policy.require_mutation(tmp_path, caller=caller, environ={"HARFLOW_TASK_ID": VALID_ID})


@pytest.mark.parametrize("key", ["HARFLOW_TASK_ID", "PB_TASK_ID"])
def test_takeover_refuses_either_worker_identity_before_state_access(tmp_path, key):
    code, reason = takeover.caller_refusal(tmp_path, environ={key: VALID_ID})
    assert code == "agent_caller"
    assert "HARFLOW_TASK_ID" in reason


async def test_foreign_mutation_guard_does_not_activate_registry_services(tmp_path, monkeypatch):
    monkeypatch.setenv("HARFLOW_TASK_ID", "foreign")
    monkeypatch.setattr(server, "_registry", None)
    with pytest.raises(ValueError):
        await server._guard_mutation()
    assert server._registry is None


def test_mcp_allowlist_read_is_allowed_but_edits_are_human_only(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HARFLOW_TASK_ID", "foreign")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(server, "_registry", None)
    assert server._registry is None
    from harflow_mcp import ctl, mcp_allowlist

    assert ctl.main(["mcp-allowlist", "--backend", "claude", "--json"]) == 0
    capsys.readouterr()
    monkeypatch.setattr(mcp_allowlist, "edit", lambda *args, **kwargs: pytest.fail("edit must be guarded"))
    assert ctl.main(["mcp-allowlist", "--backend", "claude", "--allow", "server/tool"]) == 1
    assert ctl.main(["mcp-allowlist", "--backend", "claude", "--remove", "server/tool"]) == 1


def test_workflow_read_action_map_keeps_mcp_and_cli_read_surfaces_aligned():
    from harflow_mcp.workflow_reads import READ_ACTIONS, READ_ONLY_CLI_ACTIONS, READ_ONLY_CLI_COMMANDS

    assert set(READ_ONLY_CLI_COMMANDS) == {"workflow-" + key for key in READ_ONLY_CLI_ACTIONS}
    assert set(READ_ONLY_CLI_ACTIONS.values()).issubset(READ_ACTIONS)
    assert {"list", "list_runs", "status", "detail"}.issubset(READ_ACTIONS)
    assert READ_ACTIONS.isdisjoint({"save", "delete", "start", "resume", "cancel"})


def test_lineage_depth_budget_uses_the_stricter_product_limit():
    from harflow_mcp.lineage import max_depth_default

    assert max_depth_default({"HARFLOW_MAX_DEPTH": "7", "PB_MAX_DEPTH": "2"}) == 2
    assert max_depth_default({"HARFLOW_MAX_DEPTH": "2", "PB_MAX_DEPTH": "7"}) == 2


@pytest.mark.parametrize("entry", ["start", "resume", "cancel"])
async def test_mutating_server_entrypoints_refuse_worker_without_authority(entry, tmp_path, monkeypatch):
    monkeypatch.setenv("HARFLOW_TASK_ID", VALID_ID)
    monkeypatch.setattr(server, "_registry", None)
    monkeypatch.setattr(server, "_reg", lambda: SimpleNamespace(log_dir=tmp_path))
    kwargs = {
        "start": {"prompt": "do work", "repo_path": str(tmp_path)},
        "resume": {"task_id": VALID_ID, "followup_prompt": "continue"},
        "cancel": {"task_id": VALID_ID},
    }[entry]
    operation = {"start": server.start_task, "resume": server.resume_task, "cancel": server.cancel_task}[entry]
    with pytest.raises(ValueError, match="missing or unconfirmed"):
        await operation(**kwargs)


def test_artifact_targets_protect_both_harflow_and_polybridge_namespaces(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    root = tmp_path / "state"
    tasks = root / "tasks"
    with state_ownership.state_session(resolve_layout(root=root)):
        pass
    tasks.mkdir()
    record = SimpleNamespace(
        repo_path=str(repo), task_id=VALID_ID, enforcement={}, extra_writable_paths=[]
    )
    with state_ownership.state_session(resolve_layout(root=root)):
        for name in (".harflow", ".polybridge"):
            target = repo / name / "state.json"
            target.parent.mkdir()
            target.write_text("state")
            with pytest.raises(ValueError, match="protected"):
                artifacts._target(tasks, record, str(target))


@pytest.mark.parametrize("name", ["claude", "codex"])
def test_native_adapters_are_unavailable_until_requalified_by_default(name):
    backend = backends.get(name)
    assert backend.native_subagent_adapter is None


@pytest.mark.parametrize("name", ["claude", "codex"])
def test_extra_path_operations_refuse_before_cli_spawn(name, tmp_path, monkeypatch):
    backend = backends.get(name)
    isolated_home = tmp_path / "home"
    isolated_home.mkdir()
    monkeypatch.setenv("HOME", str(isolated_home))
    monkeypatch.setenv("CODEX_HOME", str(isolated_home / ".codex"))
    grant = tmp_path / "grant"
    grant.mkdir()
    from harflow_mcp.extra_paths import normalize_paths
    invocation = backend.build_start_argv(
        "x", repo=tmp_path, freedom="write_in_repo", network=None,
        session_id=("0199a3f2-7c1e-7b8a-9d0e-123456789abc" if name == "claude" else None),
        model=None, max_turns=None, reasoning_effort=None,
    )

    with pytest.raises(UnsupportedCapability):
        backend.with_extra_writable_paths(invocation, normalize_paths([str(grant)]), "write_in_repo", False)
