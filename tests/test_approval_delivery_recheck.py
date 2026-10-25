"""Delivery-time path re-check: an approve is written only for paths that still resolve as admitted and are not protected now."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import approval_paths, approval_runtime, approvals
from harflow_mcp.backends import claude_denials, get
from harflow_mcp.backends import codex_app_server as app
from harflow_mcp.backends.claude_approvals import SYSTEM_REASONS as CLAUDE_REASONS, decode, encode as claude_encode
from test_caller_write_approvals import CONTEXT, Q310, Registry, event, repo, runtime_task, task_stub, tmp_path  # noqa: F401
from test_codex_app_server import THREAD, VERSION, change, file_request, file_started, make_ctx, prepared, started  # noqa: F401

CHANGED = "path_changed_since_admission"


# --- harness ---------------------------------------------------------------------------------------

def claude_world(repo, monkeypatch):
    registry = Registry(repo.log_dir)
    monkeypatch.setattr(approval_runtime, "context", lambda *_a: dict(CONTEXT))
    task = runtime_task(repo.log_dir)
    task.repo_path, task.backend = repo.repo, "claude"
    task.proc, task.input_closed = SimpleNamespace(pid=os.getpid(), returncode=None), False
    writes: list[bytes] = []
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _i: "alive")
    monkeypatch.setattr("harflow_mcp.tasks._write_stdin", lambda _t, wire: writes.append(wire) or True)
    registry.get = lambda _id: task
    return SimpleNamespace(registry=registry, task=task, writes=writes, directory=repo.log_dir, repo=repo.repo)


async def admit_claude(world, tool, path, index=1, **extra):
    action = {"file_path": str(path), **({"content": "c"} if tool == "Write" else {"old_string": "a", "new_string": "b"} if tool == "Edit" else {}), **extra}
    await approval_runtime.observe(world.registry, world.task, get("claude"), event(tool, action, index))
    return [r for r in approvals.rows(world.directory, "task-1") if r["tool_use_id"] == f"tool-{index}"][0]


def decide(directory, row, decision="approve"):
    approvals.reserve(directory, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], decision, row["generation"])


def swap_to_symlink(directory: Path, target: Path):
    """Replace a real directory by a symlink to `target`, as a process with workspace write access can."""
    if directory.is_symlink():
        directory.unlink()
    elif directory.exists():
        directory.rmdir()
    directory.symlink_to(target)


def wire_response(wire: bytes) -> dict:
    return json.loads(wire)["response"]["response"]


def final_row(directory):
    (row,) = approvals.rows(directory, "task-1")
    return row


# --- Claude: Read, Write and Edit ---------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["Read", "Write", "Edit"])
async def test_a_parent_swapped_for_a_symlink_into_git_hooks_is_never_approved(repo, monkeypatch, tool):
    world = claude_world(repo, monkeypatch)
    (repo.repo / "src").mkdir()
    row = await admit_claude(world, tool, repo.repo / "src" / "pre-commit")
    assert not row.get("system_rejection")
    assert [e["literal"] for e in row["path_snapshot"]] == [str(repo.repo / "src" / "pre-commit")]
    decide(world.directory, row)
    swap_to_symlink(repo.repo / "src", repo.repo / ".git" / "hooks")
    await approval_runtime.pump(world.registry, world.task)
    assert len(world.writes) == 1
    assert wire_response(world.writes[0]) == {"behavior": "deny", "message": f"Harflow refused this action: {CHANGED}"}
    final = final_row(world.directory)
    assert (final["decision"], final["decided_by"], final["system_rejection"], final["effective_decision"]) == ("approve", "caller", CHANGED, "reject")
    assert (final["delivered_decision"], final["delivery"], final["disposition"]) == ("reject", "sent", "system_rejected")
    assert claude_denials.match(wire_response(world.writes[0])["message"]) == ("harflow_policy_rejected", CHANGED)


@pytest.mark.asyncio
async def test_an_unchanged_path_is_approved_and_the_snapshot_is_not_exposed(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    (repo.repo / "src").mkdir()
    row = await admit_claude(world, "Write", repo.repo / "src" / "a.txt")
    decide(world.directory, row)
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0])["behavior"] == "allow"
    final = final_row(world.directory)
    assert (final["effective_decision"], final["delivered_decision"], final["disposition"]) == ("approve", "approve", "decided")
    assert not final.get("system_rejection")
    assert "path_snapshot" not in approvals.metadata(final) and "path_snapshot" not in approvals.get(world.directory, "task-1", final["request_id"])
    assert all("path_snapshot" not in r for r in approvals.summary(world.directory, "task-1")["requests"])


@pytest.mark.asyncio
async def test_a_directory_replaced_by_a_symlink_to_git_is_refused(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    (repo.repo / "work").mkdir()
    row = await admit_claude(world, "Write", repo.repo / "work" / "config")
    decide(world.directory, row)
    swap_to_symlink(repo.repo / "work", repo.repo / ".git")
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0])["message"].endswith(CHANGED)


@pytest.mark.asyncio
async def test_a_target_that_becomes_protected_without_moving_is_refused_as_protected(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    (repo.repo / "custom-hooks").mkdir()
    row = await admit_claude(world, "Write", repo.repo / "custom-hooks" / "pre-commit")
    assert not row.get("system_rejection")
    decide(world.directory, row)
    subprocess.run(["git", "-C", str(repo.repo), "config", "core.hooksPath", "custom-hooks"], check=True)
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0])["message"] == "Harflow refused this action: protected_path"
    assert final_row(world.directory)["disposition"] == "system_rejected"


@pytest.mark.asyncio
async def test_a_reject_decision_is_delivered_unchanged_even_if_the_path_changed(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    (repo.repo / "src").mkdir()
    row = await admit_claude(world, "Write", repo.repo / "src" / "pre-commit")
    decide(world.directory, row, "reject")
    swap_to_symlink(repo.repo / "src", repo.repo / ".git" / "hooks")
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0]) == {"behavior": "deny", "message": "Caller rejected this exact action"}
    final = final_row(world.directory)
    assert final.get("system_rejection") is None and final["disposition"] == "decided" and final["effective_decision"] == "reject"


@pytest.mark.asyncio
async def test_a_row_without_paths_is_unaffected(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    await approval_runtime.observe(world.registry, world.task, get("claude"), event("Bash", {"command": "ls"}, 1))
    (row,) = approvals.rows(world.directory, "task-1")
    assert "path_snapshot" not in row
    decide(world.directory, row)
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0])["behavior"] == "allow"


@pytest.mark.asyncio
@pytest.mark.parametrize("snapshot", [None, "x", [{"literal": 1, "resolved": "/a"}], [{"literal": "/a"}], []])
async def test_a_missing_or_malformed_snapshot_is_refused(repo, monkeypatch, snapshot):
    world = claude_world(repo, monkeypatch)
    decoded = decode(event("Write", {"file_path": str(repo.repo / "a.txt"), "content": "c"}), Q310)
    if snapshot is not None:
        decoded["path_snapshot"] = snapshot
    row = approvals.admit(world.directory, world.task, decoded, CONTEXT)
    decide(world.directory, row)
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0])["message"].endswith(CHANGED)


@pytest.mark.asyncio
async def test_a_failure_to_resolve_the_targets_at_delivery_is_refused(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    row = await admit_claude(world, "Write", repo.repo / "a.txt")
    decide(world.directory, row)
    monkeypatch.setattr(approval_paths, "protected_targets", lambda *_a, **_k: (_ for _ in ()).throw(OSError("boom")))
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0])["message"] == "Harflow refused this action: protected_path"


@pytest.mark.asyncio
async def test_a_failure_to_rebuild_the_snapshot_at_delivery_is_refused_as_protected(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    row = await admit_claude(world, "Write", repo.repo / "a.txt")
    decide(world.directory, row)
    monkeypatch.setattr(approval_runtime, "_path_snapshot", lambda *_a: (_ for _ in ()).throw(OSError("boom")))
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0])["message"] == "Harflow refused this action: protected_path"


@pytest.mark.asyncio
async def test_a_failure_to_build_the_snapshot_at_admission_is_refused_as_protected(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    monkeypatch.setattr(approval_runtime, "_path_snapshot", lambda *_a: (_ for _ in ()).throw(OSError("boom")))
    row = await admit_claude(world, "Write", repo.repo / "a.txt")
    assert row["system_rejection"] == "protected_path" and "path_snapshot" not in row


@pytest.mark.asyncio
async def test_the_delivery_check_runs_off_the_event_loop(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    row = await admit_claude(world, "Write", repo.repo / "a.txt")
    decide(world.directory, row)
    calls = []
    real = approval_runtime.asyncio.to_thread

    async def spy(func, *args, **kwargs):
        calls.append(getattr(func, "__name__", ""))
        return await real(func, *args, **kwargs)

    monkeypatch.setattr(approval_runtime.asyncio, "to_thread", spy)
    await approval_runtime.pump(world.registry, world.task)
    assert "_delivery_path_reason" in calls


@pytest.mark.asyncio
async def test_unresolvable_git_locations_at_delivery_refuse_writes_as_protected(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    row = await admit_claude(world, "Write", repo.repo / "a.txt")
    decide(world.directory, row)
    monkeypatch.setattr(approval_paths, "git_protected", lambda _repo: None)
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0])["message"] == "Harflow refused this action: protected_path"


@pytest.mark.asyncio
async def test_a_request_that_fits_only_without_its_snapshot_is_a_bounded_rejection_and_the_run_continues(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    # Find a path length whose Write event is within the request limit and whose action alone fits, but action plus snapshot does not.
    for count in range(8000, 12000, 50):
        path = "/a" * count
        raw = event("Write", {"file_path": path, "content": "c"})
        decoded = decode(raw, Q310)
        if len(approvals.canonical(decoded)) <= approvals.MAX_REQUEST_BYTES and len(approvals.canonical({**decoded, "path_snapshot": [{"literal": path, "resolved": path}]})) > approvals.MAX_REQUEST_BYTES and len(approvals.canonical(raw)) <= approvals.MAX_REQUEST_BYTES:
            break
    else:
        pytest.fail("no boundary path length found")
    assert len(approvals.canonical(decoded)) <= approvals.MAX_REQUEST_BYTES
    await approval_runtime.observe(world.registry, world.task, get("claude"), raw)
    assert world.registry.stopped == 0
    (row,) = approvals.rows(world.directory, "task-1")
    assert row["system_rejection"] == "request_too_large" and "path_snapshot" not in row and row["action"]["input"] == {} and row["disposition"] == "pending"
    assert row["tool_use_id"] == "tool-1" and row["oversize_bytes"] > 0
    await approval_runtime.pump(world.registry, world.task)
    assert len(world.writes) == 1 and wire_response(world.writes[0]) == {"behavior": "deny", "message": "Harflow refused this action: request_too_large"}
    assert final_row(world.directory)["disposition"] == "system_rejected"


@pytest.mark.asyncio
async def test_expiry_and_a_path_change_together_deliver_the_path_reason_as_a_system_rejection(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    (repo.repo / "src").mkdir()
    row = await admit_claude(world, "Write", repo.repo / "src" / "pre-commit")
    decide(world.directory, row)
    swap_to_symlink(repo.repo / "src", repo.repo / ".git" / "hooks")
    monkeypatch.setattr(approval_runtime.time, "time", lambda: row["deadline"] + 1)
    monkeypatch.setattr(approvals.time, "time", lambda: row["deadline"] + 1)
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0])["message"] == f"Harflow refused this action: {CHANGED}"
    final = final_row(world.directory)
    assert (final["disposition"], final["effective_decision"], final["expiry_rejection"], final["system_rejection"]) == ("system_rejected", "reject", True, CHANGED)


@pytest.mark.asyncio
async def test_a_concurrent_outcome_observation_wins_and_nothing_is_written(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    (repo.repo / "src").mkdir()
    row = await admit_claude(world, "Write", repo.repo / "src" / "pre-commit")
    decide(world.directory, row)
    swap_to_symlink(repo.repo / "src", repo.repo / ".git" / "hooks")
    real = approvals.update

    def racing(directory, task_id, request_id, generation, expected_delivery=None, **changes):
        if "system_rejection" in changes:
            approvals.observe_outcome(directory, task_id, request_id, generation, "succeeded")
        return real(directory, task_id, request_id, generation, expected_delivery, **changes)

    monkeypatch.setattr(approvals, "update", racing)
    await approval_runtime.pump(world.registry, world.task)
    assert world.writes == []
    final = final_row(world.directory)
    assert final["execution_outcome"] == "succeeded" and final["delivery"] == "failed"


@pytest.mark.asyncio
async def test_a_failed_write_of_the_conversion_is_unknown_and_never_resent(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    (repo.repo / "src").mkdir()
    row = await admit_claude(world, "Write", repo.repo / "src" / "pre-commit")
    decide(world.directory, row)
    swap_to_symlink(repo.repo / "src", repo.repo / ".git" / "hooks")
    attempts: list[bytes] = []
    monkeypatch.setattr("harflow_mcp.tasks._write_stdin", lambda _t, wire: attempts.append(wire) or False)
    await approval_runtime.pump(world.registry, world.task)
    final = final_row(world.directory)
    assert (final["delivery"], final["delivered_decision"], final["effective_decision"]) == ("delivery_unknown", None, "reject")
    await approval_runtime.pump(world.registry, world.task)
    assert len(attempts) == 1 and all(wire_response(w)["behavior"] == "deny" for w in attempts)


@pytest.mark.asyncio
async def test_a_crash_between_the_conversion_and_the_send_reservation_never_leaves_an_approve(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    (repo.repo / "src").mkdir()
    row = await admit_claude(world, "Write", repo.repo / "src" / "pre-commit")
    decide(world.directory, row)
    swap_to_symlink(repo.repo / "src", repo.repo / ".git" / "hooks")
    real = approvals.update

    def crashing(directory, task_id, request_id, generation, expected_delivery=None, **changes):
        if changes.get("delivery") == "sending":
            raise RuntimeError("crash")
        return real(directory, task_id, request_id, generation, expected_delivery, **changes)

    monkeypatch.setattr(approvals, "update", crashing)
    with pytest.raises(RuntimeError):
        await approval_runtime.pump(world.registry, world.task)
    assert world.writes == []
    mid = final_row(world.directory)
    assert (mid["delivery"], mid["system_rejection"], mid["effective_decision"]) == ("reserved", CHANGED, "reject")
    # Even if the path were restored, the converted row is never approved; the next pump sends only the rejection, once.
    (repo.repo / "src").unlink()
    (repo.repo / "src").mkdir()
    monkeypatch.setattr(approvals, "update", real)
    await approval_runtime.pump(world.registry, world.task)
    assert len(world.writes) == 1 and wire_response(world.writes[0])["behavior"] == "deny"
    await approval_runtime.pump(world.registry, world.task)
    assert len(world.writes) == 1


def test_the_send_reservation_never_restores_an_approve_over_a_system_rejection(tmp_path):
    row = approvals.admit(tmp_path, task_stub(tmp_path), decode(event("Write", {"file_path": "/out/a.txt", "content": "c"}), Q310), CONTEXT)
    approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", system_rejection=CHANGED)
    sending = approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    assert sending["effective_decision"] == "reject" and sending["decision"] == "approve"


def test_a_converted_row_discloses_no_possible_write_and_no_pending_request(tmp_path):
    row = approvals.admit(tmp_path, task_stub(tmp_path), decode(event("Write", {"file_path": "/out/a.txt", "content": "c"}), Q310), CONTEXT)
    approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", system_rejection=CHANGED, effective_decision="reject")
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], True, "reject")
    (entry,) = approvals.approved_writes(tmp_path, "task-1")
    assert (entry["decision"], entry["effective_decision"], entry["delivered_decision"], entry["write_possible"]) == ("approve", "reject", "reject", False)
    assert approvals.summary(tmp_path, "task-1")["pending_count"] == 0
    assert approvals.rows(tmp_path, "task-1")[0]["disposition"] == "system_rejected"


def test_the_new_reason_is_a_deliverable_system_rejection_on_both_backends():
    assert CHANGED in CLAUDE_REASONS and CHANGED in app.SYSTEM_REASONS
    row = {"native_request_id": "n", "system_rejection": CHANGED, "action": {"tool_name": "Write", "input": {"file_path": "/x", "content": "c"}}}
    message = json.loads(claude_encode(row, "reject"))["response"]["response"]["message"]
    assert message == f"Harflow refused this action: {CHANGED}" and "aller" not in message
    with pytest.raises(ValueError):
        claude_encode(row, "approve")
    codex_row = {"native_request_id": "i:5", "native_method": app.FILE_CHANGE_REQUEST, "system_rejection": CHANGED, "action": {"tool_name": "fileChange", "input": {}}}
    assert json.loads(app.encode(codex_row, "reject")) == {"id": 5, "result": {"decision": "decline"}}
    with pytest.raises(ValueError):
        app.encode(codex_row, "approve")


# --- Codex file changes ---------------------------------------------------------------------------------

def codex_world(prepared, tmp_path, monkeypatch):
    registry = Registry(tmp_path)
    monkeypatch.setattr(approval_runtime, "context", lambda *_a: dict(CONTEXT))
    ctx = make_ctx(prepared.repo, write=True)
    state = started(ctx)
    task = runtime_task(tmp_path, write=True, version=VERSION)
    task.caller_approvals, task.repo_path, task.backend = True, prepared.repo, "codex"
    task.acc.stream_state.update(control_context=ctx, app_server=state)
    task.proc, task.input_closed = SimpleNamespace(pid=os.getpid(), returncode=None), False
    writes: list[bytes] = []
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _i: "alive")
    monkeypatch.setattr("harflow_mcp.tasks._write_stdin", lambda _t, wire: writes.append(wire) or True)
    registry.get = lambda _id: task
    return SimpleNamespace(registry=registry, task=task, state=state, writes=writes, directory=tmp_path, repo=prepared.repo)


async def admit_patch(world, changes):
    app.step(world.state, file_started(changes))
    assert await approval_runtime.observe(world.registry, world.task, get("codex"), file_request()) is True
    (row,) = approvals.rows(world.directory, "task-1")
    assert not row.get("system_rejection")
    decide(world.directory, row)
    return row


@pytest.fixture
def codex_dirs(prepared):
    for name in ("a", "b"):
        (prepared.repo / name).mkdir()
    return prepared


@pytest.mark.asyncio
@pytest.mark.parametrize("swap", ["source", "destination"])
async def test_a_moved_file_is_refused_when_either_end_is_swapped_for_a_symlink(codex_dirs, tmp_path, monkeypatch, swap):
    world = codex_world(codex_dirs, tmp_path, monkeypatch)
    repo_dir = codex_dirs.repo
    await admit_patch(world, [change(str(repo_dir / "a" / "f.txt"), "update", move_path=str(repo_dir / "b" / "f.txt"))])
    swap_to_symlink(repo_dir / ("a" if swap == "source" else "b"), repo_dir / ".git" / "hooks")
    await approval_runtime.pump(world.registry, world.task)
    assert [json.loads(w) for w in world.writes] == [{"id": 5, "result": {"decision": "decline"}}]
    final = final_row(world.directory)
    assert (final["system_rejection"], final["effective_decision"], final["decision"], final["decided_by"], final["disposition"]) == (CHANGED, "reject", "approve", "caller", "system_rejected")
    assert world.state.answers["fileChange:p1"] == {"answer": "harflow", "reason": CHANGED}


@pytest.mark.asyncio
async def test_a_swap_to_a_harmless_location_is_also_refused(codex_dirs, tmp_path, monkeypatch):
    world = codex_world(codex_dirs, tmp_path, monkeypatch)
    repo_dir = codex_dirs.repo
    (repo_dir / "elsewhere").mkdir()
    await admit_patch(world, [change(str(repo_dir / "a" / "f.txt"))])
    swap_to_symlink(repo_dir / "a", repo_dir / "elsewhere")
    await approval_runtime.pump(world.registry, world.task)
    assert [json.loads(w) for w in world.writes] == [{"id": 5, "result": {"decision": "decline"}}]
    assert final_row(world.directory)["system_rejection"] == CHANGED


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["delete", "multiple"])
async def test_deletes_and_multi_path_patches_are_rechecked(codex_dirs, tmp_path, monkeypatch, kind):
    world = codex_world(codex_dirs, tmp_path, monkeypatch)
    repo_dir = codex_dirs.repo
    changes = [change(str(repo_dir / "a" / "f.txt"), "delete")] + ([change(str(repo_dir / "b" / "g.txt"))] if kind == "multiple" else [])
    row = await admit_patch(world, changes)
    assert len(row["path_snapshot"]) == len(changes)
    swap_to_symlink(repo_dir / ("a" if kind == "delete" else "b"), repo_dir / ".git" / "hooks")
    await approval_runtime.pump(world.registry, world.task)
    assert [json.loads(w) for w in world.writes] == [{"id": 5, "result": {"decision": "decline"}}]


@pytest.mark.asyncio
async def test_an_unchanged_patch_is_accepted(codex_dirs, tmp_path, monkeypatch):
    world = codex_world(codex_dirs, tmp_path, monkeypatch)
    await admit_patch(world, [change(str(codex_dirs.repo / "a" / "f.txt"))])
    await approval_runtime.pump(world.registry, world.task)
    assert [json.loads(w) for w in world.writes] == [{"id": 5, "result": {"decision": "accept"}}]
    assert final_row(world.directory)["disposition"] == "decided"
    assert world.state.answers["fileChange:p1"] == {"answer": "approved"}


@pytest.mark.asyncio
async def test_a_codex_reject_decision_is_delivered_unchanged_even_if_the_path_changed(codex_dirs, tmp_path, monkeypatch):
    world = codex_world(codex_dirs, tmp_path, monkeypatch)
    app.step(world.state, file_started([change(str(codex_dirs.repo / "a" / "f.txt"))]))
    await approval_runtime.observe(world.registry, world.task, get("codex"), file_request())
    (row,) = approvals.rows(world.directory, "task-1")
    decide(world.directory, row, "reject")
    swap_to_symlink(codex_dirs.repo / "a", codex_dirs.repo / ".git" / "hooks")
    await approval_runtime.pump(world.registry, world.task)
    assert [json.loads(w) for w in world.writes] == [{"id": 5, "result": {"decision": "decline"}}]
    assert final_row(world.directory).get("system_rejection") is None
    assert world.state.answers["fileChange:p1"] == {"answer": "caller_rejected"}


@pytest.mark.asyncio
async def test_a_command_approval_on_codex_has_no_paths_to_recheck(prepared, tmp_path, monkeypatch):
    from test_codex_app_server import command
    world = codex_world(prepared, tmp_path, monkeypatch)
    await approval_runtime.observe(world.registry, world.task, get("codex"), command(item="call-1"))
    (row,) = approvals.rows(world.directory, "task-1")
    assert "path_snapshot" not in row
    decide(world.directory, row)
    await approval_runtime.pump(world.registry, world.task)
    assert [json.loads(w) for w in world.writes] == [{"id": 0, "result": {"decision": "accept"}}]


# --- Codex .agents ---------------------------------------------------------------------------------------

def test_agents_directories_are_protected_at_the_root_the_working_directory_and_linked_roots(prepared):
    from test_codex_app_server import linked_worktree
    backend = get("codex")
    nested = prepared.repo / "pkg" / "inner"
    nested.mkdir(parents=True)
    worktree = linked_worktree(prepared)
    log_dir = prepared.base / "state" / "tasks"
    log_dir.mkdir(parents=True)
    root = Path(os.path.realpath(prepared.repo))
    cases = {"root": (nested, root / ".agents" / "hooks.json"), "middle": (nested, root / "pkg" / ".agents" / "x"), "cwd": (nested, root / "pkg" / "inner" / ".agents" / "x"),
             "linked root": (worktree, Path(os.path.realpath(prepared.repo)) / ".agents" / "x"), "worktree": (worktree, Path(os.path.realpath(worktree)) / ".agents" / "x")}
    for name, (cwd, path) in cases.items():
        targets = approval_paths.protected_targets(cwd, log_dir, extra=backend.protected_config_paths(cwd))
        assert approval_paths.protection(targets, "fileChange", str(path), cwd, backend.approval_write_tools()) == "protected_path", name
    outside = approval_paths.protected_targets(nested, log_dir, extra=backend.protected_config_paths(nested))
    assert approval_paths.protection(outside, "fileChange", str(root / "src" / "x"), nested, backend.approval_write_tools()) is None


def test_agents_is_protected_without_a_resolvable_git_root_too(tmp_path_factory, monkeypatch):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path_factory.mktemp("home")))
    plain = tmp_path_factory.mktemp("plain")
    found = get("codex").protected_config_paths(plain)
    assert Path(os.path.realpath(plain)) / ".agents" in found and Path(os.path.realpath(plain)) / ".codex" in found
