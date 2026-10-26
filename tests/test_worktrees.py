"""Task worktrees: planning, creation safety, guarded removal and the supervised launch path."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path

import pytest

from harflow_mcp import resources, store, task_resources, task_supervision, worktrees
from harflow_mcp.backends import get
from harflow_mcp.tasks import TaskRegistry

from restart_test_support import FAKE_AGENT, install_fake_claude
from test_task_resources import OWNER, ledger, launch_state, tasks_dir, wait_for, write_receipt  # noqa: F401


def git(repo: Path, *args: str, check: bool = True) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=False,
                            env={**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null"})
    if check and result.returncode != 0:
        raise AssertionError(result.stderr)
    return result.stdout.strip()


def commit(repo: Path, name: str, text: str = "x") -> str:
    (repo / name).write_text(text)
    git(repo, "add", name)
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=T", "commit", "-q", "-m", name)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def source(tmp_path, git_repo, monkeypatch):
    commit(git_repo, "a.txt")
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(tmp_path / "worktrees"))
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", "/dev/null")
    return git_repo


def make_plan(source: Path, tmp_path: Path, task_id: str = "a" * 32, spec=None):
    return worktrees.plan(task_id, source, spec or {}, tmp_path / "state" / "tasks")


# ---- planning ----------------------------------------------------------------------------------


def test_plan_defaults_and_spec_validation(source, tmp_path):
    plan = make_plan(source, tmp_path)
    assert plan["path"] == str(Path(os.path.realpath(tmp_path)) / "worktrees" / ("a" * 32))
    assert plan["branch"] == "harflow/" + "a" * 32 and plan["detached"] is False and plan["state"] == "planned"
    assert make_plan(source, tmp_path, spec={"detach": True})["branch"] is None
    assert make_plan(source, tmp_path, spec={"branch": "feature/x", "base_ref": "main"})["branch"] == "feature/x"
    for bad in ({"detach": True, "branch": "x"}, {"base_ref": "-x"}, {"branch": ""}, {"nope": 1}, {"detach": "yes"}, "string"):
        with pytest.raises(worktrees.WorktreeError):
            make_plan(source, tmp_path, spec=bad)


def test_root_outside_protected_state_and_the_repository(source, tmp_path, monkeypatch):
    log_dir = tmp_path / "state" / "tasks"
    for bad in (tmp_path / "state" / "wt", tmp_path / "state", source / "wt", source.parent, tmp_path / "state" / "tasks"):
        monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(bad))
        with pytest.raises(worktrees.WorktreeError) as refused:
            worktrees.plan("a" * 32, source, {}, log_dir)
        assert refused.value.code in {"worktree_root_protected", "worktree_root_in_repository"}
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", "relative/path")
    with pytest.raises(worktrees.WorktreeError):
        worktrees.worktree_root()
    monkeypatch.delenv("HARFLOW_WORKTREE_ROOT")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    assert worktrees.worktree_root() == tmp_path / "home" / ".harflow-worktrees"


# ---- creation ----------------------------------------------------------------------------------


def test_create_branch_detach_and_explicit_base(source, tmp_path):
    first = git(source, "rev-parse", "HEAD")
    second = commit(source, "b.txt")
    made = worktrees.create(make_plan(source, tmp_path, "a" * 32))
    assert made["state"] == "created" and made["base_commit"] == second
    assert git(Path(made["path"]), "rev-parse", "--abbrev-ref", "HEAD") == "harflow/" + "a" * 32
    detached = worktrees.create(make_plan(source, tmp_path, "b" * 32, {"detach": True, "base_ref": first}))
    assert detached["base_commit"] == first
    assert git(Path(detached["path"]), "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"
    named = worktrees.create(make_plan(source, tmp_path, "c" * 32, {"branch": "topic"}))
    assert git(Path(named["path"]), "rev-parse", "--abbrev-ref", "HEAD") == "topic"


def test_create_refuses_unsafe_requests_without_creating_anything(source, tmp_path):
    worktrees.create(make_plan(source, tmp_path, "a" * 32, {"branch": "taken"}))
    cases = [
        ({"branch": "taken"}, "worktree_branch_exists"),
        ({"branch": "bad..name"}, "worktree_branch_invalid"),
        ({"base_ref": "no-such-ref"}, "worktree_base_ref_unknown"),
    ]
    for index, (spec, code) in enumerate(cases):
        plan = make_plan(source, tmp_path, f"{index}" * 32, spec)
        with pytest.raises(worktrees.WorktreeError) as refused:
            worktrees.create(plan)
        assert refused.value.code == code and not os.path.lexists(plan["path"])
    existing = make_plan(source, tmp_path, "a" * 32, {"branch": "other"})
    with pytest.raises(worktrees.WorktreeError) as refused:
        worktrees.create(existing)
    assert refused.value.code == "worktree_path_exists"


def test_a_branch_checked_out_elsewhere_stands(source, tmp_path):
    current = git(source, "rev-parse", "--abbrev-ref", "HEAD")
    plan = make_plan(source, tmp_path, "a" * 32, {"branch": current})
    with pytest.raises(worktrees.WorktreeError) as refused:
        worktrees.create(plan)
    assert refused.value.code == "worktree_branch_exists"


@pytest.mark.parametrize("driver", ["clean", "smudge", "process"])
def test_filter_drivers_are_refused(source, tmp_path, driver):
    git(source, "config", f"filter.lfs.{driver}", "cat")
    with pytest.raises(worktrees.WorktreeError) as refused:
        worktrees.create(make_plan(source, tmp_path))
    assert refused.value.code == "worktree_filters_unsupported"
    assert not (tmp_path / "worktrees").exists() or not any((tmp_path / "worktrees").iterdir())


def test_repository_hooks_do_not_run_and_the_callers_checkout_is_untouched(source, tmp_path):
    marker = tmp_path / "hook-ran"
    for name in ("post-checkout", "reference-transaction", "post-merge"):
        hook = source / ".git" / "hooks" / name
        hook.write_text(f"#!/bin/sh\necho {name} >> {marker}\n")
        hook.chmod(0o755)
    (source / "a.txt").write_text("modified")
    (source / "untracked.txt").write_text("u")
    before = (git(source, "status", "--porcelain"), git(source, "diff"), git(source, "rev-parse", "HEAD"), (source / ".git" / "index").read_bytes())
    worktrees.create(make_plan(source, tmp_path))
    after = (git(source, "status", "--porcelain"), git(source, "diff"), git(source, "rev-parse", "HEAD"), (source / ".git" / "index").read_bytes())
    assert after == before
    assert not marker.exists()
    assert (source / "a.txt").read_text() == "modified"


def test_on_creating_runs_after_checks_and_before_the_write(source, tmp_path):
    seen = []
    plan = make_plan(source, tmp_path)
    worktrees.create(plan, on_creating=lambda info: seen.append((info["state"], info["base_commit"], os.path.lexists(plan["path"]))))
    assert seen == [("creating", git(source, "rev-parse", "HEAD"), False)]
    bad = make_plan(source, tmp_path, "b" * 32, {"base_ref": "nope"})
    with pytest.raises(worktrees.WorktreeError):
        worktrees.create(bad, on_creating=lambda info: seen.append("called"))
    assert seen[-1] != "called"


# ---- removal -----------------------------------------------------------------------------------


@pytest.fixture
def owned(tmp_path):
    from harflow_mcp.state_layout import resolve_layout
    from harflow_mcp.state_ownership import state_session
    layout = resolve_layout(root=tmp_path / "state")
    layout.root.mkdir()
    with state_session(layout):
        layout.tasks_directory.mkdir()
        yield layout


def record_for(task_id: str, path: str, **extra):
    fields = dict(status="completed", exit_code=0, finished_at="2026-01-01T00:01:00+00:00", output_drained=True) | extra
    return store.TaskRecord(task_id=task_id, backend="claude", session_id=None, repo_path=path, started_at="2026-01-01T00:00:00+00:00", **fields)


def seed_created(source, tmp_path, owned, task_id="a" * 32):
    made = worktrees.create(make_plan(source, tmp_path, task_id))
    info = {key: made.get(key) for key in ("path", "source_repo_path", "base_ref", "base_commit", "branch", "detached")}
    store.write(owned.tasks_directory, record_for(task_id, made["path"], worktree=info))
    return made


def test_remove_refuses_dirty_trees_and_live_users_then_removes_clean_ones(source, tmp_path, owned, identities):
    made = seed_created(source, tmp_path, owned)
    task_id, path = "a" * 32, Path(made["path"])
    (path / "new.txt").write_text("untracked")
    with pytest.raises(worktrees.WorktreeError) as refused:
        worktrees.remove(owned.tasks_directory, task_id)
    assert refused.value.code == "worktree_dirty"
    (path / "new.txt").unlink()
    (path / "a.txt").write_text("edited")
    with pytest.raises(worktrees.WorktreeError) as refused:
        worktrees.remove(owned.tasks_directory, task_id)
    assert refused.value.code == "worktree_dirty"
    git(path, "checkout", "--", "a.txt")

    # A successor that is still unsettled keeps it, and so does one nobody can inspect.
    live = record_for("b" * 32, str(path), status="running", exit_code=None, finished_at=None, output_drained=False,
                      pid=4242, start_time="Mon Jan  1 00:00:00 2001", markers=["x"], owner=OWNER)
    store.write(owned.tasks_directory, live)
    with pytest.raises(worktrees.WorktreeError) as refused:
        worktrees.remove(owned.tasks_directory, task_id)
    assert refused.value.code == "worktree_in_use" and "bbbb" in str(refused.value)
    write_receipt(tmp_path, "c" * 32, "granted", owner=OWNER, repo_path=str(path))
    store.write(owned.tasks_directory, record_for("b" * 32, str(path)))
    with pytest.raises(worktrees.WorktreeError) as refused:
        worktrees.remove(owned.tasks_directory, task_id)
    assert "cccc" in str(refused.value)
    write_receipt(tmp_path, "c" * 32, "not_started", owner=OWNER, repo_path=str(path))

    result = worktrees.remove(owned.tasks_directory, task_id)
    assert result["removed"] is True and not path.exists()
    assert git(source, "branch", "--list", "harflow/" + task_id)  # the branch is never deleted
    with pytest.raises(worktrees.WorktreeError):
        worktrees.remove(owned.tasks_directory, "d" * 32)


def test_launch_and_removal_serialize_on_the_usage_lock(source, tmp_path, owned, identities):
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    with worktrees.usage_lock(path):
        with pytest.raises(worktrees.WorktreeError) as busy:
            with worktrees.usage_lock(path, timeout=0.2):
                pass
        assert busy.value.code == "worktree_busy"
        with pytest.raises(worktrees.WorktreeError) as removal:
            # remove() takes the same lock; with it held elsewhere it must wait out its bound, not proceed.
            original = worktrees.usage_lock
            worktrees.usage_lock = lambda p, timeout=10.0: original(p, timeout=0.2)
            try:
                worktrees.remove(owned.tasks_directory, "a" * 32)
            finally:
                worktrees.usage_lock = original
        assert removal.value.code == "worktree_busy" and path.exists()


def test_inventory_lists_receipts_and_records(source, tmp_path, owned, identities):
    made = seed_created(source, tmp_path, owned)
    entries = worktrees.inventory(owned.tasks_directory)
    assert [(e["task_id"], e["path"], e["exists"], e["users"]) for e in entries] == [("a" * 32, made["path"], True, [])]


def test_retention_keeps_a_task_whose_worktree_exists(source, tmp_path, owned, identities):
    from datetime import datetime, timezone
    from harflow_mcp import retention
    made = seed_created(source, tmp_path, owned)
    record = store.read(owned.tasks_directory, "a" * 32)
    stats = retention.sweep(owned.tasks_directory, 1, datetime(2030, 1, 1, tzinfo=timezone.utc))
    assert store.read(owned.tasks_directory, "a" * 32) is not None and stats.get("kept_worktree") == 1
    git(source, "worktree", "remove", made["path"])
    retention.sweep(owned.tasks_directory, 1, datetime(2030, 1, 1, tzinfo=timezone.utc))
    assert store.read(owned.tasks_directory, "a" * 32) is None and record is not None


# ---- supervised launches -----------------------------------------------------------------------


def install_recording_claude(tmp_path, monkeypatch):
    """Like `install_fake_claude`, but the stand-in also records its working directory and argv."""
    install_fake_claude(tmp_path, monkeypatch)
    executable = tmp_path / "fake-bin" / "claude"
    body = executable.read_text().replace(
        "session=${HARFLOW_FAKE_CLAUDE_SESSION", f"pwd -P >> {tmp_path}/cwd.log; printf '%s\\n' \"$*\" >> {tmp_path}/argv.log\nsession=${{HARFLOW_FAKE_CLAUDE_SESSION", 1)
    executable.write_text(body)


def registry(root: Path) -> TaskRegistry:
    return TaskRegistry(log_dir=tasks_dir(root), execution_mode="supervised")


@pytest.mark.asyncio
async def test_worktree_task_runs_in_the_worktree_and_records_it(tmp_path, source, monkeypatch, identities):
    install_recording_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    marker = tmp_path / "hook-ran"
    hook = source / ".git" / "hooks" / "post-checkout"
    hook.write_text(f"#!/bin/sh\ntouch {marker}\n")
    hook.chmod(0o755)
    (source / "dirty.txt").write_text("caller work in progress")
    before = git(source, "status", "--porcelain")
    reg = registry(tmp_path)
    task_id = "a" * 32
    started = await reg.start("slow 1 hi", source, backend=get("claude"), freedom="write_in_repo", worktree={}, task_id=task_id)
    assert isinstance(started, task_resources.PendingLaunch) or started.task_id == task_id
    await wait_for(lambda: (r := store.read(log_dir, task_id)) is not None and r.finished_at and r.output_drained)
    record = store.read(log_dir, task_id)
    expected = str(Path(os.path.realpath(tmp_path)) / "worktrees" / task_id)
    assert record.repo_path == expected
    assert record.worktree["path"] == expected and record.worktree["source_repo_path"] == str(source)
    assert record.worktree["branch"] == "harflow/" + task_id and record.worktree["base_commit"] == git(source, "rev-parse", "HEAD")
    assert (tmp_path / "cwd.log").read_text().strip() == expected  # the process cwd
    assert str(source) not in (tmp_path / "argv.log").read_text()
    assert git(source, "status", "--porcelain") == before and not marker.exists()
    receipt = task_supervision.read_launch(log_dir, task_id)
    assert receipt["worktree"]["state"] == "created" and receipt["repo_path"] == expected
    snapshot = store.snapshot(log_dir, record)
    assert snapshot["worktree"]["path"] == expected and snapshot["repo_path"] == expected
    # Resuming works in the same worktree and carries it.
    again = await reg.resume_record(record, "reply again")
    await wait_for(lambda: store.read(log_dir, again.task_id) is not None)
    assert store.read(log_dir, again.task_id).repo_path == expected and store.read(log_dir, again.task_id).worktree["path"] == expected


@pytest.mark.asyncio
async def test_worktree_is_refused_at_read_only_nested_and_in_process(tmp_path, source, monkeypatch, identities):
    from harflow_mcp import lineage
    install_fake_claude(tmp_path, monkeypatch)
    reg = registry(tmp_path)
    with pytest.raises(ValueError, match="read_only") as refused:
        await reg.start("reply x", source, backend=get("claude"), freedom="read_only", worktree={})
    assert refused.value.harflow_not_started is True
    with pytest.raises(ValueError, match="supervised"):
        await TaskRegistry(log_dir=tasks_dir(tmp_path)).start("reply x", source, backend=get("claude"), freedom="write_in_repo", worktree={})
    parent = store.TaskRecord(task_id="p" * 32, backend="claude", session_id=None, repo_path=str(source), started_at="2026-01-01T00:00:00+00:00")
    caller = lineage.Caller(parent, "session")
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(caller))
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: caller)
    with pytest.raises(ValueError, match="nested"):
        await reg.start("reply x", source, backend=get("claude"), freedom="write_in_repo", worktree={})
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None, "ps unreadable"))
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: None)
    with pytest.raises(Exception, match="cannot be established"):
        await reg.start("reply x", source, backend=get("claude"), freedom="write_in_repo", worktree={})
    assert list(tasks_dir(tmp_path).glob("*.launch.json")) == []


@pytest.mark.asyncio
async def test_base_ref_is_resolved_after_the_resource_grant(tmp_path, source, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    await reg.start("slow 3 hold", source, backend=get("claude"), freedom="write_in_repo", resources=["sim"], task_id="a" * 32)
    waiter = await reg.start("reply two", source, backend=get("claude"), freedom="write_in_repo", resources=["sim"], worktree={}, task_id="b" * 32)
    assert store.read(log_dir, waiter.task_id) is None
    moved = commit(source, "while-waiting.txt")
    await wait_for(lambda: store.read(log_dir, waiter.task_id) is not None)
    assert store.read(log_dir, waiter.task_id).worktree["base_commit"] == moved
    assert (Path(store.read(log_dir, waiter.task_id).repo_path) / "while-waiting.txt").exists()


@pytest.mark.asyncio
async def test_worktree_failure_after_grant_releases_resources_and_reports_it(tmp_path, source, monkeypatch, identities):
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    await reg.start("slow 3 hold", source, backend=get("claude"), freedom="write_in_repo", resources=["sim"], task_id="a" * 32)
    waiter = await reg.start("reply two", source, backend=get("claude"), freedom="write_in_repo", resources=["sim"],
                             worktree={"branch": "will-exist"}, task_id="b" * 32)
    git(source, "branch", "will-exist")  # appears while the launch waits
    await wait_for(lambda: launch_state(tmp_path, waiter.task_id) == "not_started", 30)
    receipt = task_supervision.read_launch(log_dir, waiter.task_id)
    assert receipt["not_started_reason"] == "worktree_failed" and receipt["worktree"]["state"] == "failed"
    assert "already exists" in receipt["error"]
    assert store.read(log_dir, waiter.task_id) is None
    await wait_for(lambda: all(e.owner.task_id != waiter.task_id for e in ledger(tmp_path).entries()))
    assert not os.path.lexists(receipt["worktree"]["path"])


def test_pending_worktree_launch_reports_worktree_and_unknown_after_owner_death(tmp_path, source, identities, owned):
    log_dir = owned.tasks_directory
    write_receipt(tmp_path, "e" * 32, "granted", owner=OWNER, repo_path=str(tmp_path / "w"),
                  worktree={"path": str(tmp_path / "w"), "state": "creating", "source_repo_path": str(source)})
    identities.dead(OWNER["pid"])
    view = task_resources.pending_view(log_dir, "e" * 32)
    assert view["worktree"]["state"] == "unknown" and view["status"] == "failed" and view["not_started_reason"] == "owner_died"
    assert "e" * 32 not in {r["task_id"] for r in task_supervision.session_reservations(log_dir)}
    write_receipt(tmp_path, "f" * 32, "granted", owner=OWNER, repo_path=str(tmp_path / "w2"),
                  worktree={"path": str(tmp_path / "w2"), "state": "created", "source_repo_path": str(source)})
    assert "f" * 32 in {r["task_id"] for r in task_supervision.session_reservations(log_dir)}


def test_ctl_worktree_commands(tmp_path, source, monkeypatch, identities, capsys):
    from harflow_mcp import ctl, lineage
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(tmp_path / "state"))
    assert ctl.main(["worktree-list", "--json"]) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["worktrees"] == []
    from harflow_mcp.state_layout import resolve_layout
    from harflow_mcp.state_ownership import state_session
    log_dir = tasks_dir(tmp_path)
    with state_session(resolve_layout(root=tmp_path / "state")):
        made = worktrees.create(make_plan(source, tmp_path, "a" * 32))
        info = {key: made.get(key) for key in ("path", "source_repo_path", "base_ref", "base_commit", "branch", "detached")}
        store.write(log_dir, record_for("a" * 32, made["path"], worktree=info))
    assert ctl.main(["worktree-list", "--json"]) == 0
    out = capsys.readouterr()
    assert out.out.strip().startswith('{'), (out.out, out.err)
    listed = json.loads(out.out.splitlines()[-1])["worktrees"]
    assert listed[0]["task_id"] == "a" * 32 and listed[0]["exists"] is True
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None, "ps unreadable"))
    assert ctl.main(["worktree-remove", "--task-id", "a" * 32, "--json"]) == 1
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["error"]["code"] == "caller_undecidable"
    assert Path(made["path"]).exists()
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None))
    assert ctl.main(["worktree-remove", "--task-id", "a" * 32, "--json"]) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["result"]["removed"] is True
    assert not Path(made["path"]).exists()
    assert ctl.main(["worktree-remove", "--task-id", "a" * 32, "--json"]) == 1
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["error"]["code"] == "worktree_missing"


def test_ctl_run_worktree_flags(tmp_path):
    from harflow_mcp import ctl
    parser = ctl._build_parser()[0]
    args = parser.parse_args(["run", "--backend", "claude", "--repo", ".", "--prompt", "x", "--worktree", "--worktree-base", "main", "--worktree-branch", "t"])
    assert args.worktree and args.worktree_base == "main" and args.worktree_branch == "t" and args.worktree_detach is False
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--backend", "claude", "--repo", ".", "--prompt", "x", "--worktree", "--worktree-branch", "t", "--worktree-detach"])
