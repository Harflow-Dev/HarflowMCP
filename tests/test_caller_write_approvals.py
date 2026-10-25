"""Caller Read/Write/Edit approvals: qualification, strict schemas, system rejections, protected paths and disclosure."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import approval_paths, approval_runtime, approvals, state_ownership, store
from harflow_mcp.backends import Accumulator, get
from harflow_mcp.backends import claude_qualification as qualification
from harflow_mcp.backends.base import ApprovalRefused, Enforcement, UnsupportedCapability, nested_enforcement_violation
from harflow_mcp.backends.claude_approvals import SYSTEM_REASONS, decode, decode_oversize, encode, support, validate_write
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.workflows import _candidate, _candidate_key

Q310 = {"version": "2.1.310", "observed": "2.1.310", "write": True}
Q310_NO_WRITE = {**Q310, "write": False}
Q302 = {"version": "2.1.302", "observed": "2.1.302", "write": False}
CONTEXT = {"interaction_owner": "caller", "workflow_run_id": None, "root_workflow_run_id": None, "workflow_node_id": None, "workflow_execution_id": None}


@pytest.fixture
def tmp_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("write-approvals") / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir()
        yield root / "tasks"


def event(tool: str, tool_input: dict, index: int = 1, **request) -> dict:
    return {"type": "control_request", "request_id": f"native-{index}", "request": {"subtype": "can_use_tool", "tool_name": tool, "tool_use_id": f"tool-{index}", "input": tool_input, **request}}


def task_stub(directory: Path) -> SimpleNamespace:
    return SimpleNamespace(task_id="task-1", proc=SimpleNamespace(pid=os.getpid()), start_time="self-start", markers=("session-1",),
                           owner={"pid": os.getpid(), "start_time": "bridge-start", "markers": []}, approval_generation="generation-1", session_id="session-1",
                           repo_path=directory, freedom="write_in_repo", caller_approvals=True, caller_write_approvals=True, caller_approval_version="2.1.310", protected_paths=None)


def admit(directory: Path, decoded: dict, **overrides) -> dict:
    return approvals.admit(directory, task_stub(directory), decoded, CONTEXT)


# --- qualification table and validation ---------------------------------------------------------

def test_table_scopes_tools_by_version_and_opt_in():
    assert qualification.approval_tools("2.1.302", write=True) == ("Bash",)
    assert qualification.approval_tools("2.1.310") == ("Bash", "Read")
    assert qualification.approval_tools("2.1.310", write=True) == ("Bash", "Read", "Write", "Edit")
    assert qualification.approval_tools("2.1.303", write=True) == ()


def test_support_reports_versions_as_a_list_with_per_version_tools():
    value = support()
    assert value["versions"] == ["2.1.302", "2.1.310"]
    assert value["tools_by_version"] == {"2.1.302": ["Bash"], "2.1.310": ["Bash", "Read", "Write", "Edit"]}
    assert value["path_safety_prompt_versions"] == ["2.1.310"]
    assert value["tools"] == ["Bash", "Read", "Write", "Edit"]


def test_write_opt_in_validation():
    validate_write(False, False)
    validate_write(True, True, "2.1.310")
    with pytest.raises(ValueError, match="requires caller_approvals"):
        validate_write(True, False)
    with pytest.raises(UnsupportedCapability):
        validate_write(True, True, "2.1.302")
    with pytest.raises(ValueError):
        validate_write("yes", True)  # type: ignore[arg-type]
    with pytest.raises(UnsupportedCapability):
        get("opencode").validate_caller_write_approvals(True, True)
    get("codex").validate_caller_write_approvals(False, False)


def test_unsupported_tools_per_version():
    assert decode(event("Read", {"file_path": "/x"}), Q310_NO_WRITE)["action"]["tool_name"] == "Read"
    for tool, tool_input in (("Read", {"file_path": "/x"}), ("Write", {"file_path": "/x", "content": "c"})):
        with pytest.raises(ValueError):
            decode(event(tool, tool_input), Q302)


# --- version cross-check ------------------------------------------------------------------------

@pytest.mark.parametrize("qualified", [None, {"version": None, "observed": "2.1.310", "write": False}])
def test_missing_qualification_is_refused(qualified):
    with pytest.raises(ApprovalRefused, match="qualification is missing"):
        decode(event("Bash", {"command": "x"}), qualified)


@pytest.mark.parametrize("observed", [None, "2.1.311", "2.1.302"])
def test_init_version_must_equal_the_qualified_version(observed):
    with pytest.raises(ApprovalRefused) as raised:
        decode(event("Bash", {"command": "x"}), {**Q310, "observed": observed})
    assert "2.1.310" in str(raised.value)


# --- strict schemas -----------------------------------------------------------------------------

@pytest.mark.parametrize("tool,tool_input", [
    ("Write", {"file_path": "relative.txt", "content": "c"}),
    ("Write", {"file_path": "/x", "content": "c", "extra": 1}),
    ("Write", {"file_path": "/x"}),
    ("Write", {"file_path": "/x", "content": 3}),
    ("Edit", {"file_path": "/x", "old_string": "a", "new_string": "b", "replace_all": "yes"}),
    ("Edit", {"file_path": "rel", "old_string": "a", "new_string": "b"}),
    ("Edit", {"file_path": "/x", "old_string": "a"}),
    ("Read", {"file_path": "/x", "offset": True}),
    ("Read", {"file_path": "/x", "extra": 1}),
    ("Read", {"offset": 1}),
    ("Read", {"file_path": ""}),
    ("Read", {"file_path": "~/.harflow/tasks/x.json"}),
    ("Read", {"file_path": "../../etc/passwd"}),
    ("Read", {"file_path": "~/.ssh/id_rsa"}),
    ("Read", {"file_path": "/ok\x00/bad"}),
    ("Bash", {"command": "x", "dangerouslyDisableSandbox": True}),
])
def test_invalid_inputs_are_refused(tool, tool_input):
    with pytest.raises(ValueError):
        decode(event(tool, tool_input), Q310)


def test_valid_inputs_echo_exactly_and_never_store_suggestions():
    read = {"file_path": "/x", "offset": 3, "limit": 4, "pages": "1-2"}
    edit = {"file_path": "/x", "old_string": "a", "new_string": "b", "replace_all": True}
    decoded = decode(event("Edit", edit, permission_suggestions=[{"type": "addDirectories"}], decision_reason="r" * 5000, decision_reason_type="workingDir"), Q310)
    assert decoded["action"]["input"] == edit
    assert "permission_suggestions" not in json.dumps(decoded)
    assert len(decoded["decision_reason"].encode()) <= 1024 and decoded["decision_reason_type"] == "workingDir"
    assert json.loads(encode(decoded, "approve"))["response"]["response"] == {"behavior": "allow", "updatedInput": edit}
    assert decode(event("Read", read), Q310_NO_WRITE)["action"]["input"] == read


# --- system rejections --------------------------------------------------------------------------

def test_write_without_opt_in_decodes_as_a_system_rejection():
    decoded = decode(event("Write", {"file_path": "/x", "content": "c"}), Q310_NO_WRITE)
    assert decoded["system_rejection"] == "write_approvals_not_enabled"
    assert "system_rejection" not in decode(event("Write", {"file_path": "/x", "content": "c"}), Q310)


@pytest.mark.parametrize("reason", SYSTEM_REASONS)
def test_encode_delivers_only_the_harflow_refusal_for_system_rows(reason):
    row = {"native_request_id": "n", "action": {"input": {"file_path": "/x"}}, "system_rejection": reason}
    message = json.loads(encode(row, "reject"))["response"]["response"]
    assert message == {"behavior": "deny", "message": f"Harflow refused this action: {reason}"}
    with pytest.raises(ValueError):
        encode(row, "approve")
    with pytest.raises(ValueError):
        encode({**row, "system_rejection": "invented"}, "reject")


def test_system_rejection_row_is_answered_at_admission_and_unanswerable(tmp_path):
    row = admit(tmp_path, {**decode(event("Write", {"file_path": "/x", "content": "c"}), Q310_NO_WRITE)})
    assert (row["decided_by"], row["decision"], row["effective_decision"], row["delivery"], row["disposition"]) == ("harflow", None, "reject", "reserved", "pending")
    with pytest.raises(approvals.ApprovalError) as raised:
        approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    assert raised.value.reason == "settled"
    assert approvals.summary(tmp_path, "task-1")["requests"] == []
    sending = approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    assert sending["effective_decision"] == "reject"
    approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], True, "reject")
    final = approvals.get(tmp_path, "task-1", row["request_id"])
    assert (final["disposition"], final["delivery"], final["delivered_decision"]) == ("system_rejected", "sent", "reject")
    approvals.observe_outcome(tmp_path, "task-1", row["request_id"], row["generation"], "failed")
    assert approvals.get(tmp_path, "task-1", row["request_id"])["disposition"] == "system_rejected"


def test_system_rejection_never_counts_as_a_caller_decision(tmp_path):
    row = admit(tmp_path, decode(event("Write", {"file_path": "/x", "content": "c"}), Q310_NO_WRITE))
    assert row["decision"] is None and approvals.approved_writes(tmp_path, "task-1") == []


def test_duplicate_native_id_is_refused_for_system_rows(tmp_path):
    decoded = decode(event("Write", {"file_path": "/x", "content": "c"}), Q310_NO_WRITE)
    admit(tmp_path, decoded)
    with pytest.raises(ValueError, match="Repeated native"):
        admit(tmp_path, decoded)


def test_failed_stdin_write_keeps_a_system_rejection_uncertain(tmp_path):
    row = admit(tmp_path, decode(event("Write", {"file_path": "/x", "content": "c"}), Q310_NO_WRITE))
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], False, "reject")
    final = approvals.get(tmp_path, "task-1", row["request_id"])
    assert (final["delivery"], final["delivered_decision"], final["disposition"]) == ("delivery_unknown", None, "system_rejected")


def test_cancellation_before_send_invalidates_without_a_send(tmp_path):
    row = admit(tmp_path, decode(event("Write", {"file_path": "/x", "content": "c"}), Q310_NO_WRITE))
    approvals.invalidate(tmp_path, "task-1", "cancelled", row["generation"])
    final = approvals.get(tmp_path, "task-1", row["request_id"])
    assert (final["delivery"], final["disposition"], final["delivered_decision"]) == ("failed", "cancelled", None)


def test_stale_owner_revision_is_refused_after_expiry(tmp_path, monkeypatch):
    row = admit(tmp_path, decode(event("Write", {"file_path": "/x", "content": "c"}), Q310))
    monkeypatch.setattr(approvals.time, "time", lambda: row["deadline"] + 1)
    with pytest.raises(ValueError, match="expired"):
        approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])


# --- oversize ------------------------------------------------------------------------------------

def test_oversize_request_becomes_a_bounded_audit_row_without_content(tmp_path):
    big = event("Write", {"file_path": "/out/x.txt", "content": "z" * (approvals.MAX_REQUEST_BYTES + 10)})
    assert len(approvals.canonical(big)) > approvals.MAX_REQUEST_BYTES
    decoded = decode_oversize(big, Q310)
    assert decoded["system_rejection"] == "request_too_large" and decoded["action"] == {"tool_name": "Write", "input": {}}
    assert decoded["file_path"] == "/out/x.txt" and decoded["oversize_bytes"] > approvals.MAX_REQUEST_BYTES and len(decoded["oversize_action_digest"]) == 64
    row = admit(tmp_path, decoded)
    assert row["effective_decision"] == "reject" and row["delivery"] == "reserved" and row["decided_by"] == "harflow"
    assert "zzzz" not in json.dumps(approvals.get(tmp_path, "task-1", row["request_id"]))
    assert row["action_digest"] == approvals.digest(row["action"]) != decoded["oversize_action_digest"]


def test_oversize_is_not_a_rejection_for_other_tools_or_bad_identity():
    big = "z" * approvals.MAX_REQUEST_BYTES
    assert decode_oversize(event("Bash", {"command": big}), Q310) is None
    uncorrelated = event("Write", {"file_path": "/x", "content": big})
    uncorrelated["request"]["tool_use_id"] = ""
    assert decode_oversize(uncorrelated, Q310) is None
    assert decode_oversize(event("Write", {"file_path": "/x", "content": big}), Q302) is None
    with pytest.raises(ApprovalRefused):
        decode_oversize(event("Write", {"file_path": "/x", "content": big}), {**Q310, "observed": None})


def test_full_ledger_keeps_the_stop_behaviour(tmp_path):
    for index in range(approvals.MAX_PENDING):
        admit(tmp_path, decode(event("Write", {"file_path": f"/x{index}", "content": "c"}, index), Q310_NO_WRITE))
    with pytest.raises(ValueError, match="count bound"):
        admit(tmp_path, decode_oversize(event("Write", {"file_path": "/x", "content": "z" * approvals.MAX_REQUEST_BYTES}, 99), Q310))


# --- disclosure ---------------------------------------------------------------------------------

def _approve(tmp_path, index=1, path="/out/x.txt", inside=False):
    row = admit(tmp_path, {**decode(event("Write", {"file_path": path, "content": "SECRET"}, index), Q310), "inside_repository": inside})
    return approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])


def test_approved_writes_disclose_metadata_only(tmp_path):
    row = _approve(tmp_path)
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], True, "approve")
    (entry,) = approvals.approved_writes(tmp_path, "task-1")
    assert entry == {"request_id": row["request_id"], "tool_name": "Write", "file_path": "/out/x.txt", "inside_repository": False, "decision": "approve",
                     "effective_decision": "approve", "delivered_decision": "approve", "expiry_rejection": False, "delivery": "sent", "execution_outcome": "unknown", "write_possible": True}
    assert "SECRET" not in json.dumps(entry)


def test_approve_then_expiry_before_delivery_is_not_a_possible_write(tmp_path, monkeypatch):
    row = _approve(tmp_path)
    monkeypatch.setattr(approvals.time, "time", lambda: row["deadline"] + 1)
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], True, "reject", True)
    (entry,) = approvals.approved_writes(tmp_path, "task-1")
    assert (entry["decision"], entry["effective_decision"], entry["delivered_decision"], entry["expiry_rejection"], entry["write_possible"]) == ("approve", "reject", "reject", True, False)


def test_failed_stdin_write_of_an_approval_stays_possible_and_unsent_rows_do_not(tmp_path):
    row = _approve(tmp_path)
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], False, "approve")
    assert approvals.approved_writes(tmp_path, "task-1")[0]["write_possible"] is True
    other = _approve(tmp_path, 2, "/out/y.txt")
    approvals.invalidate(tmp_path, "task-1", "cancelled", other["generation"])
    flags = {e["file_path"]: e["write_possible"] for e in approvals.approved_writes(tmp_path, "task-1")}
    assert flags["/out/y.txt"] is False


def test_legacy_ledger_rows_without_the_new_fields_still_work(tmp_path):
    legacy = {"native_request_id": "n1", "tool_use_id": "t1", "action": {"tool_name": "Bash", "input": {"command": "x"}}}
    row = admit(tmp_path, legacy)
    assert "system_rejection" not in row and row["disposition"] == "pending" and row["delivery"] == "pending"
    assert approvals.approved_writes(tmp_path, "task-1") == []
    reserved = approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "reject", row["generation"])
    assert reserved["decided_by"] == "caller"


def test_old_task_records_load_without_write_fields(tmp_path):
    record = store.TaskRecord(task_id="old", backend="claude", session_id="s", repo_path="/r", started_at="2026-01-01T00:00:00+00:00", pid=None, prompt="p", status="completed")
    store.write(tmp_path, record)
    path = store.record_path(tmp_path, "old")
    document = json.loads(path.read_text())
    for key in ("caller_write_approvals", "caller_approval_version"):
        document.pop(key, None)
        document.get("record", {}).pop(key, None)
    path.write_text(json.dumps(document))
    loaded = store.read(tmp_path, "old")
    assert loaded.caller_write_approvals is False and loaded.caller_approval_version is None


# --- enforcement disclosure and nesting ----------------------------------------------------------

def test_enforcement_discloses_tools_and_caveats_by_version_and_opt_in():
    backend = get("claude")
    base = backend.enforcement("write_in_repo")
    assert "caller_approval_tools" not in base.as_dict()
    assert backend.approval_enforcement(base, "2.1.310", False, False) is base
    read_only = backend.approval_enforcement(base, "2.1.310", True, False)
    assert read_only.caller_approval_tools == ("Bash", "Read") and any("Read" in c for c in read_only.caveats) and not any("writes outside" in c for c in read_only.caveats)
    write = backend.approval_enforcement(base, "2.1.310", True, True)
    assert write.as_dict()["caller_approval_tools"] == ["Bash", "Read", "Write", "Edit"] and any("writes outside the repository" in c for c in write.caveats)
    assert backend.approval_enforcement(base, "2.1.302", True, False).caller_approval_tools == ("Bash",)
    assert write.writes_confined is False and write.commit_push_blocked is False


def test_nested_child_may_not_widen_write_approvals():
    parent = get("claude").enforcement("write_in_repo").as_dict()
    child = get("claude").approval_enforcement(get("claude").enforcement("write_in_repo"), "2.1.310", True, True)
    args = dict(parent_backend="claude", child_backend="claude", parent_repo="/r", child_repo="/r")
    assert nested_enforcement_violation(parent, child, **args)[0] == "caller_approval_tools"
    wide = {**parent, "caller_approval_tools": ["Bash", "Read", "Write", "Edit"]}
    assert nested_enforcement_violation(wide, child, **args) is None
    assert nested_enforcement_violation(parent, get("claude").enforcement("write_in_repo"), **args) is None


# --- workflow candidates --------------------------------------------------------------------------

def test_legacy_candidate_signature_is_unchanged_and_write_changes_it():
    legacy = {"backend": "claude", "model": None, "reasoning_effort": None, "max_turns": None}
    assert _candidate_key(legacy) == _candidate_key({**legacy, "caller_approvals": False, "caller_write_approvals": False})
    assert _candidate_key({**legacy, "caller_approvals": True}) != _candidate_key({**legacy, "caller_approvals": True, "caller_write_approvals": True})
    assert "caller_write_approvals" not in _candidate_key({**legacy, "caller_approvals": True})


def test_candidate_validation_of_write_opt_in():
    assert _candidate({"backend": "claude", "caller_approvals": True, "caller_write_approvals": True})["caller_write_approvals"] is True
    assert "caller_write_approvals" not in _candidate({"backend": "claude", "caller_write_approvals": False})
    for bad in ({"caller_write_approvals": True}, {"caller_approvals": True, "caller_write_approvals": "yes"}):
        with pytest.raises(Exception):
            _candidate({"backend": "claude", **bad})


# --- protected paths ------------------------------------------------------------------------------

@pytest.fixture
def repo(tmp_path_factory, monkeypatch):
    base = tmp_path_factory.mktemp("protected")
    repo = base / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    monkeypatch.setenv("HOME", str(base / "home"))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(base / "config"))
    (base / "home").mkdir()
    (base / "config").mkdir()
    root = base / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir()
        yield SimpleNamespace(base=base, repo=repo, log_dir=root / "tasks", root=root)


def check(r, tool: str, path: str | Path) -> str | None:
    targets = approval_paths.protected_targets(r.repo, r.log_dir)
    return approval_paths.protection(targets, tool, str(path), r.repo)


@pytest.mark.parametrize("relative", [".claude/settings.json", ".claude/settings.local.json", ".claude/hooks/x.sh", ".claude/agents/a.md", ".claude/commands/c.md", ".mcp.json", ".git/hooks/pre-commit", ".git/config"])
def test_repo_protected_targets_refuse_write_and_edit(repo, relative):
    for tool in ("Write", "Edit"):
        assert check(repo, tool, repo.repo / relative) == "protected_path"


@pytest.mark.parametrize("name", ["settings.json", "settings.local.json"])
def test_symlinked_repo_settings_protect_their_outside_target(repo, name):
    outside = repo.base / "outside" / "real-settings.json"
    outside.parent.mkdir()
    outside.write_text("{}")
    (repo.repo / ".claude").mkdir()
    (repo.repo / ".claude" / name).symlink_to(outside)
    for tool in ("Write", "Edit"):
        assert check(repo, tool, outside) == "protected_path"
        assert check(repo, tool, repo.repo / ".claude" / name) == "protected_path"
    assert check(repo, "Write", outside.parent / "other.json") is None


def test_symlinked_mcp_json_and_config_settings_protect_their_outside_target(repo):
    outside = repo.base / "outside"
    outside.mkdir()
    (repo.repo / ".mcp.json").symlink_to(outside / "mcp.json")
    (repo.base / "config" / "settings.json").symlink_to(outside / "user-settings.json")
    for target in (outside / "mcp.json", outside / "user-settings.json"):
        assert check(repo, "Write", target) == "protected_path"


def test_config_dir_and_home_files_are_protected(repo):
    for path in (repo.base / "config" / "settings.json", repo.base / "config" / "settings.local.json", repo.base / "home" / ".claude.json"):
        assert check(repo, "Write", path) == "protected_path"


def test_state_root_is_protected_for_write_and_read(repo):
    target = repo.root / "tasks" / "anything.json"
    assert check(repo, "Write", target) == "protected_path" and check(repo, "Read", target) == "protected_path"


def test_ordinary_paths_and_non_state_reads_pass(repo):
    assert check(repo, "Write", repo.repo / "src" / "a.py") is None
    assert check(repo, "Edit", repo.base / "elsewhere.txt") is None
    assert check(repo, "Read", repo.repo / ".claude" / "settings.json") is None


def test_symlink_parent_and_case_variants_are_resolved(repo):
    link = repo.base / "link"
    link.symlink_to(repo.repo / ".git")
    assert check(repo, "Write", link / "hooks" / "post-commit") == "protected_path"
    assert check(repo, "Write", repo.repo / ".CLAUDE" / "SETTINGS.json") == "protected_path"
    state_link = repo.base / "state-link"
    state_link.symlink_to(repo.root)
    assert check(repo, "Read", state_link / "tasks" / "x") == "protected_path"


def test_effective_hooks_path_and_worktree_locations_are_protected(repo):
    custom = repo.base / "custom-hooks"
    subprocess.run(["git", "-C", str(repo.repo), "config", "core.hooksPath", str(custom)], check=True)
    assert check(repo, "Write", custom / "pre-push") == "protected_path"
    subprocess.run(["git", "-C", str(repo.repo), "commit", "-q", "--allow-empty", "-m", "x", "-c", "user.name=a", "-c", "user.email=a@b"], check=False, capture_output=True)
    worktree = repo.base / "wt"
    subprocess.run(["git", "-C", str(repo.repo), "-c", "user.name=a", "-c", "user.email=a@b", "commit", "-q", "--allow-empty", "-m", "x"], check=True)
    subprocess.run(["git", "-C", str(repo.repo), "worktree", "add", "-q", str(worktree)], check=True)
    gitdir = Path(subprocess.run(["git", "-C", str(worktree), "rev-parse", "--absolute-git-dir"], capture_output=True, text=True, check=True).stdout.strip())
    targets = approval_paths.protected_targets(worktree, repo.log_dir)
    assert approval_paths.protection(targets, "Write", str(gitdir / "config.worktree"), worktree) == "protected_path"
    assert approval_paths.protection(targets, "Write", str(repo.repo / ".git" / "config"), worktree) == "protected_path"


def test_non_git_directory_has_no_git_protection_but_unresolvable_git_refuses_writes(repo, monkeypatch):
    plain = repo.base / "plain"
    plain.mkdir()
    targets = approval_paths.protected_targets(plain, repo.log_dir)
    assert targets["git"] == [] and approval_paths.protection(targets, "Write", str(plain / "a.txt"), plain) is None
    assert approval_paths.protection(targets, "Write", str(plain / ".git" / "hooks" / "x"), plain) == "protected_path"
    monkeypatch.setattr(approval_paths, "_git", lambda *a, **k: (_ for _ in ()).throw(subprocess.TimeoutExpired("git", 2)))
    broken = approval_paths.protected_targets(repo.repo, repo.log_dir)
    assert broken["git"] is None
    assert approval_paths.protection(broken, "Edit", str(repo.repo / "a.txt"), repo.repo) == "protected_path"
    assert approval_paths.protection(broken, "Read", str(repo.repo / "a.txt"), repo.repo) is None


def test_inside_repository_uses_realpath(repo):
    outside = repo.base / "outside"
    outside.mkdir()
    (repo.repo / "escape").symlink_to(outside)
    assert approval_paths.inside_repository(repo.repo, str(repo.repo / "src" / "a.py")) is True
    assert approval_paths.inside_repository(repo.repo, "src/a.py") is False
    assert approval_paths.inside_repository(repo.repo, str(repo.repo / "escape" / "x")) is False
    assert approval_paths.inside_repository(repo.repo, str(outside / "x")) is False


# --- runtime admission ----------------------------------------------------------------------------

def runtime_task(directory: Path, *, write: bool = True, version: str = "2.1.310", observed: str | None = "2.1.310") -> SimpleNamespace:
    task = task_stub(directory)
    task.caller_write_approvals, task.caller_approval_version = write, version
    task.interaction_owner, task.cancel_requested, task.bridge_notices = "caller", False, []
    task.approval_pending = task.approval_receiving = False
    task.approval_view = {"pending_count": 0, "interaction_state": None, "requests": []}
    task.pump_wake = asyncio.Event()
    task.acc = Accumulator()
    if observed:
        task.acc.stream_state["claude_code_version"] = observed
    return task


class Registry:
    def __init__(self, directory: Path):
        self.log_dir = directory
        self.state_layout = resolve_layout(log_dir=directory)
        self.stopped = 0

    def persist(self, task):
        return None

    def _stop_native_transport(self, task):
        self.stopped += 1


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setattr(approval_runtime, "context", lambda *_a: dict(CONTEXT))
    return tmp_path, Registry(tmp_path)


@pytest.mark.asyncio
async def test_runtime_stops_and_invalidates_on_a_version_mismatch_or_missing_init(runtime):
    directory, registry = runtime
    first = runtime_task(directory)
    assert await approval_runtime.observe(registry, first, get("claude"), event("Bash", {"command": "ok"}, 1)) is True
    assert approvals.rows(directory, "task-1")[0]["disposition"] == "pending"
    for number, observed in ((2, "2.1.311"), (3, None)):
        task = runtime_task(directory, observed=observed)
        task.approval_generation = f"generation-{number}"
        await approval_runtime.observe(registry, task, get("claude"), event("Bash", {"command": "x"}, number))
        assert registry.stopped == number - 1
        assert "qualified on 2.1.310" in task.bridge_notices[0]
    stopped = [r for r in approvals.rows(directory, "task-1") if r["generation"] == "generation-2"]
    assert stopped and all(r["disposition"] != "pending" for r in stopped)


@pytest.mark.asyncio
async def test_runtime_write_without_opt_in_is_admitted_as_a_system_rejection_and_the_run_is_not_stopped(runtime):
    directory, registry = runtime
    task = runtime_task(directory, write=False)
    await approval_runtime.observe(registry, task, get("claude"), event("Write", {"file_path": str(directory / "x"), "content": "c"}))
    (row,) = approvals.rows(directory, "task-1")
    assert row["system_rejection"] == "write_approvals_not_enabled" and row["delivery"] == "reserved" and registry.stopped == 0
    assert task.approval_pending is True


@pytest.mark.asyncio
async def test_runtime_protected_path_and_inside_repository_facts(runtime, monkeypatch):
    directory, registry = runtime
    task = runtime_task(directory)
    task.repo_path = directory
    monkeypatch.setattr(approval_paths, "protected_targets", lambda *_a, **_k: {"state": [], "settings": [Path("/nope/settings.json")], "git": []})
    await approval_runtime.observe(registry, task, get("claude"), event("Write", {"file_path": "/nope/settings.json", "content": "c"}, 1))
    await approval_runtime.observe(registry, task, get("claude"), event("Edit", {"file_path": str(directory / "a.txt"), "old_string": "a", "new_string": "b"}, 2))
    first, second = approvals.rows(directory, "task-1")
    assert first["system_rejection"] == "protected_path" and first["inside_repository"] is False
    assert second.get("system_rejection") is None and second["inside_repository"] is True and second["decision_reason"] is None


@pytest.mark.asyncio
async def test_runtime_re_resolves_protected_targets_on_every_admission(runtime, tmp_path_factory):
    directory, registry = runtime
    repo = tmp_path_factory.mktemp("rehook") / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    hooks = repo.parent / "late-hooks"
    task = runtime_task(directory)
    task.repo_path = repo
    target = {"file_path": str(hooks / "pre-push"), "content": "c"}
    await approval_runtime.observe(registry, task, get("claude"), event("Write", target, 1))
    subprocess.run(["git", "-C", str(repo), "config", "core.hooksPath", str(hooks)], check=True)
    await approval_runtime.observe(registry, task, get("claude"), event("Write", target, 2))
    first, second = approvals.rows(directory, "task-1")
    assert first.get("system_rejection") is None and second["system_rejection"] == "protected_path"


@pytest.mark.asyncio
async def test_runtime_resolution_failure_refuses_with_protected_path(runtime, monkeypatch):
    directory, registry = runtime
    task = runtime_task(directory)
    monkeypatch.setattr(approval_paths, "protected_targets", lambda *_a, **_k: (_ for _ in ()).throw(OSError("boom")))
    await approval_runtime.observe(registry, task, get("claude"), event("Write", {"file_path": str(directory / "a"), "content": "c"}))
    (row,) = approvals.rows(directory, "task-1")
    assert row["system_rejection"] == "protected_path"


@pytest.mark.asyncio
async def test_runtime_oversize_write_is_rejected_not_stopped(runtime):
    directory, registry = runtime
    task = runtime_task(directory)
    await approval_runtime.observe(registry, task, get("claude"), event("Write", {"file_path": "/out/x", "content": "z" * approvals.MAX_REQUEST_BYTES}))
    (row,) = approvals.rows(directory, "task-1")
    assert row["system_rejection"] == "request_too_large" and registry.stopped == 0
    registry2 = Registry(directory)
    other = runtime_task(directory)
    other.approval_generation = "generation-2"
    await approval_runtime.observe(registry2, other, get("claude"), event("Bash", {"command": "z" * approvals.MAX_REQUEST_BYTES}, 2))
    assert registry2.stopped == 1


@pytest.mark.asyncio
async def test_pump_delivers_a_system_rejection_through_the_existing_path(runtime, monkeypatch):
    directory, registry = runtime
    task = runtime_task(directory, write=False)
    task.proc = SimpleNamespace(pid=os.getpid(), returncode=None)
    task.input_closed, task.backend = False, "claude"
    await approval_runtime.observe(registry, task, get("claude"), event("Write", {"file_path": "/out/x", "content": "c"}))
    writes: list[bytes] = []
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _i: "alive")
    monkeypatch.setattr("harflow_mcp.tasks._write_stdin", lambda _t, wire: writes.append(wire) or True)
    registry.get = lambda _id: task
    assert await approval_runtime.pump(registry, task) is False
    assert json.loads(writes[0])["response"]["response"]["message"] == "Harflow refused this action: write_approvals_not_enabled"
    (row,) = approvals.rows(directory, "task-1")
    assert (row["disposition"], row["delivered_decision"], row["decided_by"]) == ("system_rejected", "reject", "harflow")


@pytest.mark.asyncio
async def test_concurrent_runs_with_different_qualifications_do_not_share_state(runtime):
    directory, _registry = runtime
    a, b = Q302, Q310
    read = event("Read", {"file_path": "/x"})

    async def attempt(qualified):
        await asyncio.sleep(0)
        try:
            return decode(read, qualified)["action"]["tool_name"]
        except ValueError:
            return "refused"

    assert await asyncio.gather(attempt(a), attempt(b), attempt(a), attempt(b)) == ["refused", "Read", "refused", "Read"]
