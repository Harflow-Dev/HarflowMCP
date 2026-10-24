"""Review follow-ups: Git directories, task_finished projection, expiry identity, init-version stop and case handling."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import approval_paths, approval_runtime, approvals, backends, state_ownership, workflow_child_sessions, workflow_inspection
from harflow_mcp import store as store_module
from harflow_mcp.backends import Accumulator, get
from harflow_mcp.backends.claude_approvals import check_event, decode, encode
from harflow_mcp.events import EVENT_MAX_BYTES, events_path
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry
from test_caller_write_approvals import CONTEXT, Q310, Registry, event, repo, runtime, runtime_task, task_stub, tmp_path  # noqa: F401
from test_claude_denials import _ClaudeStream, call, init, refusal, result
from test_events import registered, task_root  # noqa: F401
from test_events import tmp_path as events_tmp_path  # noqa: F401


# --- Git directories beyond the task repository ----------------------------------------------------

def protected(r, tool, path) -> str | None:
    return approval_paths.protection(approval_paths.protected_targets(r.repo, r.log_dir), tool, str(path), r.repo)


def test_submodule_bare_and_linked_worktree_git_dirs_are_protected(repo):
    modules = repo.repo / ".git" / "modules" / "sub"
    for name in ("objects", "refs", "hooks"):
        (modules / name).mkdir(parents=True)
    (modules / "HEAD").write_text("ref: refs/heads/main\n")
    bare = repo.base / "other.git"
    for name in ("objects", "refs", "hooks"):
        (bare / name).mkdir(parents=True)
    (bare / "HEAD").write_text("ref: refs/heads/main\n")
    linked = repo.repo / ".git" / "worktrees" / "w"
    linked.mkdir(parents=True)
    (linked / "HEAD").write_text("x\n")
    (linked / "commondir").write_text("../..\n")
    for tool in ("Write", "Edit"):
        for target in (modules / "hooks" / "pre-commit", modules / "config", bare / "hooks" / "post-update", bare / "config", linked / "config.worktree", linked / "hooks" / "x"):
            assert protected(repo, tool, target) == "protected_path", target
    assert protected(repo, "Write", bare / "description") is None
    assert protected(repo, "Write", repo.base / "plain" / "hooks" / "x") is None


# --- Read paths --------------------------------------------------------------------------------------

@pytest.mark.parametrize("path", ["~/.harflow/tasks/x.json", "../..", "~/.ssh/id_rsa", "relative/file"])
def test_non_absolute_paths_are_never_inside_the_repository(repo, path):
    assert approval_paths.inside_repository(repo.repo, path) is False
    with pytest.raises(ValueError):
        decode(event("Read", {"file_path": path}), Q310)


# --- case handling -----------------------------------------------------------------------------------

def test_inside_repository_folds_case_only_on_case_insensitive_volumes(tmp_path, monkeypatch):
    work = Path("/work-case-test")
    monkeypatch.setattr(approval_paths, "case_insensitive", lambda _p: False)
    assert approval_paths.inside_repository(work / "Repo", str(work / "repo" / "a.txt")) is False
    assert approval_paths.inside_repository(work / "Repo", str(work / "Repo" / "a.txt")) is True
    monkeypatch.setattr(approval_paths, "case_insensitive", lambda _p: True)
    assert approval_paths.inside_repository(work / "Repo", str(work / "repo" / "a.txt")) is True


def test_protected_paths_stay_case_folded_everywhere(repo, monkeypatch):
    monkeypatch.setattr(approval_paths, "case_insensitive", lambda _p: False)
    assert protected(repo, "Write", repo.repo / ".CLAUDE" / "Settings.JSON") == "protected_path"


# --- workflows -----------------------------------------------------------------------------------------

def _record(write: bool) -> SimpleNamespace:
    enforcement = get("claude").enforcement("write_in_repo").as_dict()
    return SimpleNamespace(freedom="write_in_repo", network=None, enforcement=enforcement, backend="claude", repo_path="/r", caller_write_approvals=write)


def test_agent_caller_without_write_approvals_cannot_save_a_candidate_that_has_them():
    configs = [({"backend": "claude", "caller_approvals": True, "caller_write_approvals": True}, "write_in_repo", None)]
    with pytest.raises(backends.NestedDispatchRefused) as raised:
        workflow_inspection._check_configs(_record(False), configs)
    assert raised.value.rule == "caller_write_approvals"
    configs[0][0]["fallbacks"] = []
    configs = [({"backend": "claude", "fallbacks": [{"backend": "claude", "caller_approvals": True, "caller_write_approvals": True}]}, "write_in_repo", None)]
    with pytest.raises(backends.NestedDispatchRefused):
        workflow_inspection._check_configs(_record(False), configs)


def test_lineage_refuses_write_approvals_the_caller_lacks(tmp_path, monkeypatch):
    from harflow_mcp.tasks import TaskRegistry as Registry_
    caller = SimpleNamespace(record=SimpleNamespace(task_id="parent", max_depth=3, depth=0, enforcement=get("claude").enforcement("write_in_repo").as_dict(), backend="claude",
                                                    repo_path="/r", root_task_id=None, caller_write_approvals=False), method="test")
    monkeypatch.setattr("harflow_mcp.workflow_hooks.refuse_managed", lambda *_a: None)
    monkeypatch.setattr("harflow_mcp.caller_policy.signals", lambda: (None, 0))
    shell = SimpleNamespace(_log_dir=Path("/nowhere"))
    child = get("claude").enforcement("write_in_repo")
    args = dict(child_enforcement=child, child_backend="claude", child_repo=Path("/r"))
    with pytest.raises(backends.NestedDispatchRefused):
        Registry_._resolve_lineage(shell, caller, caller_write_approvals=True, **args)
    Registry_._resolve_lineage(shell, caller, caller_write_approvals=False, **args)
    caller.record.caller_write_approvals = True
    Registry_._resolve_lineage(shell, caller, caller_write_approvals=True, **args)


def _first_turn_run(tmp_path, receipt_write: bool):
    candidate = {"backend": "claude", "model": None, "reasoning_effort": None, "max_turns": None}
    child = {"inherited_orchestrator_binding": {"source_task_id": "src", "source_session_id": "sid", "candidate": candidate}, "sessions": {},
             "pending": [{"decision_id": "d1", "id": "tok"}], "repo_path": str(tmp_path),
             "activations": [{"role": "orchestrator", "decision_id": "d1", "token": {"id": "tok", "decision_id": "d1"},
                              "tasks": [{"task_id": "t1", "status": "completed", "session_mode": "resume", "resume_task_id": "src", "candidate": candidate}]}]}
    receipt = SimpleNamespace(task_id="t1", parent_task_id="src", status="completed", session_id="sid", backend="claude", model=None, reasoning_effort=None, max_turns=None,
                              caller_approvals=False, caller_write_approvals=receipt_write, freedom="read_only", network=None, repo_path=str(tmp_path))
    return child, receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt_write,reaches_session_lock", [(False, True), (True, False)])
async def test_first_turn_recovery_compares_write_approvals(tmp_path, monkeypatch, receipt_write, reaches_session_lock):
    child, receipt = _first_turn_run(tmp_path, receipt_write)
    monkeypatch.setattr(store_module, "read", lambda *_a, **_k: receipt)
    monkeypatch.setattr(store_module, "outcome_unobserved", lambda _r: False)
    monkeypatch.setattr("harflow_mcp.workflow_native_policy.orchestrator_contract", lambda *_a: {"freedom": "read_only", "network": None})

    class Reached(Exception):
        pass

    def lock(*_a, **_k):
        raise Reached

    monkeypatch.setattr("harflow_mcp.control.session_lock", lock)
    supervisor = SimpleNamespace(run=lambda **_k: child, store=SimpleNamespace(root=tmp_path), update=lambda *_a: None)
    if reaches_session_lock:
        with pytest.raises(Reached):
            await workflow_child_sessions._recover_first_turn(supervisor)
    else:
        await workflow_child_sessions._recover_first_turn(supervisor)


# --- expiry ----------------------------------------------------------------------------------------------

def test_expiry_rejection_is_a_harflow_refusal_never_a_caller_rejection():
    from harflow_mcp.backends import claude_denials
    row = {"native_request_id": "n", "action": {"input": {"command": "x"}}, "expiry_rejection": True}
    message = json.loads(encode(row, "reject"))["response"]["response"]["message"]
    assert message == "Harflow refused this action: approval_expired"
    assert claude_denials.match(message) == ("harflow_policy_rejected", "approval_expired")
    assert json.loads(encode({**row, "expiry_rejection": False}, "reject"))["response"]["response"]["message"] == "Caller rejected this exact action"


@pytest.mark.asyncio
async def test_expired_caller_approve_is_delivered_as_approval_expired_and_keeps_the_decision(runtime, monkeypatch):
    directory, registry = runtime
    task = runtime_task(directory)
    task.proc, task.input_closed, task.backend = SimpleNamespace(pid=os.getpid(), returncode=None), False, "claude"
    await approval_runtime.observe(registry, task, get("claude"), event("Bash", {"command": "ok"}))
    (row,) = approvals.rows(directory, "task-1")
    approvals.reserve(directory, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    writes: list[bytes] = []
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _i: "alive")
    monkeypatch.setattr("harflow_mcp.tasks._write_stdin", lambda _t, wire: writes.append(wire) or True)
    monkeypatch.setattr(approval_runtime.time, "time", lambda: row["deadline"] + 1)
    registry.get = lambda _id: task
    await approval_runtime.pump(registry, task)
    assert json.loads(writes[0])["response"]["response"] == {"behavior": "deny", "message": "Harflow refused this action: approval_expired"}
    (final,) = approvals.rows(directory, "task-1")
    assert (final["decision"], final["effective_decision"], final["expiry_rejection"], final["delivered_decision"], final["disposition"]) == ("approve", "reject", True, "reject", "expired")


# --- init-version stop -------------------------------------------------------------------------------------

INIT = {"type": "system", "subtype": "init", "claude_code_version": "2.1.310"}
WRITE_USE = {"type": "assistant", "message": {"content": [{"type": "tool_use", "id": "w", "name": "Write", "input": {"file_path": "/x", "content": "c"}}]}}


def test_check_event_rules():
    check_event(INIT, Q310)
    check_event({"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}}, {**Q310, "observed": None})
    for bad in ({**INIT, "claude_code_version": "2.1.311"}, {"type": "system", "subtype": "init"}):
        with pytest.raises(backends.base.ApprovalRefused):
            check_event(bad, Q310)
    with pytest.raises(backends.base.ApprovalRefused):
        check_event(INIT, {**Q310, "version": None})
    for early in (WRITE_USE, event("Bash", {"command": "x"})):
        with pytest.raises(backends.base.ApprovalRefused):
            check_event(early, {**Q310, "observed": None})
        check_event(early, Q310)


@pytest.mark.asyncio
async def test_mismatched_init_stops_the_run_immediately_and_a_later_write_does_nothing_more(runtime):
    directory, registry = runtime
    task = runtime_task(directory, observed=None)
    pending = event("Bash", {"command": "ok"})
    good = runtime_task(directory)
    await approval_runtime.observe(registry, good, get("claude"), pending)
    assert registry.stopped == 0
    backend = get("claude")
    assert await approval_runtime.observe(registry, task, backend, {**INIT, "claude_code_version": "2.1.311"}) is False
    assert registry.stopped == 1 and "qualified on 2.1.310" in task.bridge_notices[0]
    backend.ingest({**INIT, "claude_code_version": "2.1.311"}, task.acc)
    await approval_runtime.observe(registry, task, backend, WRITE_USE)
    assert task.bridge_notices.count(task.bridge_notices[0]) == 1 and registry.stopped == 2
    assert all(r["disposition"] != "pending" for r in approvals.rows(directory, "task-1") if r["generation"] == good.approval_generation)


@pytest.mark.asyncio
async def test_missing_init_version_and_tool_use_before_init_stop_the_run(runtime):
    directory, registry = runtime
    first = runtime_task(directory, observed=None)
    await approval_runtime.observe(registry, first, get("claude"), {"type": "system", "subtype": "init"})
    assert registry.stopped == 1
    second = runtime_task(directory, observed=None)
    await approval_runtime.observe(registry, second, get("claude"), WRITE_USE)
    assert registry.stopped == 2


@pytest.mark.asyncio
async def test_two_tasks_with_different_qualifications_are_decided_independently(runtime):
    directory, registry = runtime
    old = runtime_task(directory, write=False, version="2.1.302", observed="2.1.302")
    new = runtime_task(directory)
    new.task_id = old.task_id = "task-1"
    old.approval_generation, new.approval_generation = "g-old", "g-new"
    await approval_runtime.observe(registry, old, get("claude"), event("Read", {"file_path": "/x"}, 1))
    await approval_runtime.observe(registry, new, get("claude"), event("Read", {"file_path": "/x"}, 2))
    assert registry.stopped == 1 and old.bridge_notices and not new.bridge_notices
    rows = approvals.rows(directory, "task-1")
    assert [r["generation"] for r in rows if r["tool_name"] == "Read" and r["disposition"] == "pending"] == ["g-new"]


# --- task_finished projection -----------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_task_finished_stays_bounded_with_many_large_denied_writes(events_tmp_path, registered):
    big = "SECRETCONTENT" * 400
    lines = [init()]
    denied = []
    for index in range(60):
        tool_input = {"file_path": f"/out/{index}.txt", "content": big}
        lines += [call(f"t{index}", "Write", tool_input), refusal(f"t{index}", "Claude requested permissions to write to /out/x.txt, but you haven't granted it yet.")]
        denied.append({"tool_name": "Write", "tool_use_id": f"t{index}", "tool_input": tool_input})
    lines.append(result(*denied))
    script = events_tmp_path / "stream.jsonl"
    script.write_text("".join(json.dumps(line) + "\n" for line in lines), encoding="utf-8")
    backend = registered(_ClaudeStream(f"cat {script}"))
    registry = TaskRegistry(log_dir=task_root(events_tmp_path))
    task = await registry.start("hi", events_tmp_path, backend=backend)
    await task.done.wait()
    path = events_path(task_root(events_tmp_path), task.task_id)
    raw = path.read_text()
    # Ordinary tool_call previews are existing event behavior; the finished event must carry no tool input.
    assert "SECRETCONTENT" not in raw.splitlines()[-1]
    finished = json.loads(raw.splitlines()[-1])
    assert finished["kind"] == "task_finished" and finished["status"] == task.status and finished["exit_code"] == task.exit_code and "summary" in finished
    assert len(raw.splitlines()[-1].encode()) <= EVENT_MAX_BYTES
    assert 0 < len(finished["permission_denials"]) <= 32
    assert finished["denials_omitted"] == 60 - len(finished["permission_denials"]) > 0
    assert all("tool_input" not in d and d["denial_category"] == "outside_working_directory" for d in finished["permission_denials"])
    assert finished["denial_categories"] == ["outside_working_directory"]
    assert len(task.snapshot()["permission_denials"]) == 60


# --- round 2 ------------------------------------------------------------------------------------------------

def test_symlinked_git_locations_in_another_repository_are_protected(repo):
    other = repo.base / "other"
    (other / ".git").mkdir(parents=True)
    shared = repo.base / "shared-hooks"
    shared.mkdir()
    (other / ".git" / "hooks").symlink_to(shared)
    elsewhere = repo.base / "shared-config"
    elsewhere.write_text("[core]\n")
    (other / ".git" / "config").symlink_to(elsewhere)
    (other / ".git" / "config.worktree").symlink_to(elsewhere)
    for tool in ("Write", "Edit"):
        for target in (other / ".git" / "hooks" / "pre-commit", other / ".git" / "config", other / ".git" / "config.worktree"):
            assert protected(repo, tool, target) == "protected_path", target
    assert protected(repo, "Write", other / "src" / "a.py") is None


@pytest.mark.parametrize("order", ["delivery_then_outcome", "outcome_then_delivery"])
def test_expired_system_rejection_stays_system_rejected_in_both_observation_orders(tmp_path, monkeypatch, order):
    row = approvals.admit(tmp_path, task_stub(tmp_path), decode(event("Write", {"file_path": "/x", "content": "c"}), {**Q310, "write": False}), CONTEXT)
    monkeypatch.setattr(approvals.time, "time", lambda: row["deadline"] + 1)
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    steps = [lambda: approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], True, "reject", True),
             lambda: approvals.observe_outcome(tmp_path, "task-1", row["request_id"], row["generation"], "failed")]
    for step in (steps if order == "delivery_then_outcome" else steps[::-1]):
        step()
    final = approvals.get(tmp_path, "task-1", row["request_id"])
    assert (final["disposition"], final["expiry_rejection"], final["execution_outcome"]) == ("system_rejected", True, "failed")


@pytest.mark.asyncio
async def test_task_finished_with_multibyte_denials_and_summary_is_not_stubbed(events_tmp_path, registered):
    from harflow_mcp.events import read_page
    lines = [init()]
    denied = []
    for index in range(32):
        part = f"python3 -c '{chr(0x6F22) * 190}{index}'"
        lines += [call(f"t{index}", "Bash", {"command": f"ls; {part}"}),
                  refusal(f"t{index}", f"This Bash command contains multiple operations. The following part requires approval: {part}")]
        denied.append({"tool_name": "Bash", "tool_use_id": f"t{index}", "tool_input": {"command": f"ls; {part}"}})
    final = result(*denied)
    final["result"] = chr(0x6F22) * 2000
    script = events_tmp_path / "stream.jsonl"
    script.write_text("".join(json.dumps(line) + "\n" for line in [*lines, final]), encoding="utf-8")
    backend = registered(_ClaudeStream(f"cat {script}"))
    registry = TaskRegistry(log_dir=task_root(events_tmp_path))
    task = await registry.start("hi", events_tmp_path, backend=backend)
    await task.done.wait()
    path = events_path(task_root(events_tmp_path), task.task_id)
    raw_line = path.read_text(encoding="utf-8").splitlines()[-1]
    page = read_page(path, limit=200, layout=resolve_layout(root=events_tmp_path))
    finished = page.events[-1]
    assert page.skipped_oversized == 0 and finished["kind"] == "task_finished" and "oversized" not in finished
    assert finished["status"] == task.status and finished["exit_code"] == task.exit_code and finished["summary"].startswith(chr(0x6F22) * 100)
    assert finished["denials_omitted"] + len(finished["permission_denials"]) == 32 and finished["permission_denials"]
    assert len(json.dumps(finished, ensure_ascii=False).encode("utf-8")) <= EVENT_MAX_BYTES and raw_line
