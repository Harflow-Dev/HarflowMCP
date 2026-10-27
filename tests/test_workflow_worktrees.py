"""Workflow-level worktree planning and refusal behavior."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import subprocess
from types import SimpleNamespace

import pytest

from harflow_mcp import resources, workflow_resources, workflows, worktrees


def definition(*, freedom: str = "write_in_repo") -> dict:
    return {
        "name": "worktree fixture",
        "routing_mode": "explicit",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "work", "type": "agent", "role": "task", "freedom": freedom,
             "agent": {"backend": "codex"}, "instructions": "Work"},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "work"},
            {"id": "finish", "source": "work", "target": "end"},
        ],
    }


def test_worktree_plan_changes_preview_hash(tmp_path: Path):
    state = tmp_path / "state"
    repo = tmp_path / "repo"
    repo.mkdir()
    storage = workflows.WorkflowStore(state)
    value = definition()
    storage.save(value["name"], value)

    without = workflows.preview_workflow_run(value["name"], repo, root=state)
    with_worktree = workflows.preview_workflow_run(
        value["name"], repo, root=state, worktree={"branch": "feature/workflow"},
    )

    assert without["preview_hash"] != with_worktree["preview_hash"]
    assert with_worktree["worktree"]["branch"] == "feature/workflow"


def test_worktree_plan_refused_when_all_agent_nodes_are_read_only(tmp_path: Path):
    state = tmp_path / "state"
    repo = tmp_path / "repo"
    repo.mkdir()
    storage = workflows.WorkflowStore(state)
    value = definition(freedom="read_only")
    storage.save(value["name"], value)

    with pytest.raises(workflows.WorkflowError, match="worktree"):
        workflows.preview_workflow_run(value["name"], repo, root=state, worktree={})


@pytest.fixture
def workflow_storage(tmp_path):
    from harflow_mcp import state_ownership
    from harflow_mcp.state_layout import resolve_layout

    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir(exist_ok=True)
        yield workflows.WorkflowStore(root)


async def test_workflow_worktree_creation_leaves_dirty_source_checkout_untouched(
    git_repo: Path, workflow_storage, tmp_path: Path, monkeypatch,
):
    source = git_repo
    (source / "tracked.txt").write_text("base")
    subprocess.run(["git", "-C", str(source), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "base"], check=True)
    (source / "tracked.txt").write_text("modified source")
    (source / "untracked.txt").write_text("keep me")
    before_status = subprocess.run(["git", "-C", str(source), "status", "--porcelain"],
                                   capture_output=True, text=True, check=True).stdout
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(tmp_path / "worktrees"))
    run_id = "workflow-worktree-run"
    plan = worktrees.plan(run_id, source, {}, workflow_storage.root / "tasks")
    run = workflow_storage.create_run(workflows.validate_definition(definition()), "prompt", source,
                                      permission_policy="saved_node", run_id=run_id, worktree=plan)

    class RunningTree:
        @staticmethod
        def tree_running(*args, **kwargs):
            return True

    class Supervisor:
        def __init__(self):
            self.run_id = run_id
            self.registry = SimpleNamespace(log_dir=workflow_storage.root / "tasks")
            self.store = workflow_storage
            self.tree = RunningTree()

        def run(self, *, refresh_atomic=False):
            return workflow_storage.get_run(run_id, refresh_atomic=True)

        def update(self, change, event, payload=None):
            return workflow_storage.update_run(run_id, change, event, payload)

        def attention(self, reason):
            raise AssertionError(reason)

    assert await workflow_resources.prepare_run(Supervisor())
    created = workflow_storage.get_run(run_id)
    after_status = subprocess.run(["git", "-C", str(source), "status", "--porcelain"],
                                  capture_output=True, text=True, check=True).stdout

    assert created["worktree"]["state"] == "created"
    assert Path(created["repo_path"]).is_dir()
    assert created["source_repo_path"] == str(source.resolve())
    assert not (Path(created["repo_path"]) / "untracked.txt").exists()
    assert after_status == before_status
    inventory_row = next(row for row in worktrees.inventory(workflow_storage.root / "tasks")
                         if row.get("workflow_run_id") == run_id)
    assert "task_id" not in inventory_row
    with pytest.raises(worktrees.WorktreeError) as refused:
        worktrees.remove(workflow_storage.root / "tasks", run_id)
    assert refused.value.code == "worktree_in_use"


@pytest.mark.asyncio
async def test_worktree_creation_error_releases_independent_run_resource_hold(
    git_repo: Path, workflow_storage, tmp_path: Path, monkeypatch,
):
    source = git_repo
    (source / "tracked.txt").write_text("base")
    subprocess.run(["git", "-C", str(source), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "base"], check=True)
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(tmp_path / "worktrees"))
    run_id = "worktree-create-failure"
    value = definition()
    value["resources"] = ["checkout:repo"]
    plan = worktrees.plan(run_id, source, {}, workflow_storage.root / "tasks")
    run = workflow_storage.create_run(
        workflows.validate_definition(value), "prompt", source, permission_policy="saved_node",
        run_id=run_id, worktree=plan,
    )
    workflow_storage.update_run(run_id, lambda r: r.update(status="running"), "fixture_running")

    class RunningTree:
        @staticmethod
        def tree_running(*args, **kwargs):
            return True

    attention = []
    supervisor = SimpleNamespace(
        run_id=run_id,
        registry=SimpleNamespace(log_dir=workflow_storage.root / "tasks"),
        store=workflow_storage,
        tree=RunningTree(),
        run=lambda *, refresh_atomic=False: workflow_storage.get_run(run_id, refresh_atomic=True),
        update=lambda change, event, payload=None: workflow_storage.update_run(run_id, change, event, payload),
        attention=attention.append,
    )

    def refuse_creation(*args, **kwargs):
        raise worktrees.WorktreeError("simulated git worktree creation failure", code="worktree_git_failed")

    monkeypatch.setattr(worktrees, "create", refuse_creation)
    assert await workflow_resources.prepare_run(supervisor) is False

    failed = workflow_storage.get_run(run_id)
    assert failed["status"] == "failed"
    assert failed["worktree"]["state"] == "failed"
    assert failed["resources"]["released"] is True
    assert resources.ResourceLedger(workflow_storage.root).get(failed["resources"]["reservation_id"]) is None
    assert not attention


def test_ordinary_worktree_lookup_uses_receipts_while_inventory_and_removal_fail_closed(
    git_repo: Path, workflow_storage, tmp_path: Path, monkeypatch,
):
    from harflow_mcp import workflow_worktree_receipts
    from harflow_mcp.workflow_run_documents import WorkflowRunDocumentError

    source = git_repo
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(tmp_path / "worktrees"))
    (source / "tracked.txt").write_text("base")
    subprocess.run(["git", "-C", str(source), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "base"], check=True)
    run_id = "lookup-receipt-run"
    plan = worktrees.plan(run_id, source, {}, workflow_storage.root / "tasks")
    workflow_storage.create_run(
        workflows.validate_definition(definition()), "prompt", source,
        permission_policy="saved_node", run_id=run_id, worktree=plan,
    )
    unrelated = workflow_storage.create_run(
        workflows.validate_definition(definition()), "prompt", source,
        permission_policy="saved_node", run_id="unreadable-unrelated-run",
    )
    receipt_path = workflow_storage.root / "workflow-worktrees" / f"{run_id}.json"
    assert workflow_worktree_receipts.paths(workflow_storage.root / "tasks") == [Path(plan["path"])]
    assert receipt_path.is_file()

    (workflow_storage.runs / f"{unrelated['workflow_run_id']}.json").write_text("{ unreadable", encoding="utf-8")

    assert worktrees.recorded_paths(workflow_storage.root / "tasks") == [Path(plan["path"])]
    assert worktrees.containing_worktree(workflow_storage.root / "tasks", Path(plan["path"]) / "nested") == Path(plan["path"])
    with pytest.raises(WorkflowRunDocumentError):
        worktrees.users(workflow_storage.root / "tasks", Path(plan["path"]))
    with pytest.raises(WorkflowRunDocumentError):
        worktrees.remove(workflow_storage.root / "tasks", run_id)


def test_worktree_lookup_receipt_survives_checkout_symlink_replacement_but_removal_refuses(
    git_repo: Path, workflow_storage, tmp_path: Path, monkeypatch,
):
    from harflow_mcp import workflow_worktree_receipts
    from harflow_mcp.workflow_run_documents import WorkflowRunDocumentError

    source = git_repo
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(tmp_path / "worktrees"))
    (source / "tracked.txt").write_text("base")
    subprocess.run(["git", "-C", str(source), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "base"], check=True)
    run_id = "symlinked-checkout-receipt"
    plan = worktrees.plan(run_id, source, {}, workflow_storage.root / "tasks")
    workflow_storage.create_run(
        workflows.validate_definition(definition()), "prompt", source,
        permission_policy="saved_node", run_id=run_id, worktree=plan,
    )

    original = Path(plan["path"])
    original.parent.mkdir(parents=True, exist_ok=True)
    replacement = tmp_path / "agent-controlled-directory"
    replacement.mkdir()
    original.symlink_to(replacement, target_is_directory=True)

    # A lookup receipt locates the former checkout path without resolving the
    # agent-controlled replacement into engine authority.
    assert workflow_worktree_receipts.paths(workflow_storage.root / "tasks") == [original]
    assert replacement in worktrees.recorded_paths(workflow_storage.root / "tasks")
    with pytest.raises(worktrees.WorktreeError) as refused:
        worktrees.remove(workflow_storage.root / "tasks", run_id)
    assert refused.value.code == "worktree_in_use"


@pytest.mark.parametrize("tamper", ["run_id", "state_root", "non_normalized_path"])
def test_worktree_lookup_receipt_rejects_tampered_qualification(
    git_repo: Path, workflow_storage, tmp_path: Path, monkeypatch, tamper,
):
    from harflow_mcp import workflow_worktree_receipts
    from harflow_mcp.worktrees import WorktreeError

    source = git_repo
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(tmp_path / "worktrees"))
    (source / "tracked.txt").write_text("base")
    subprocess.run(["git", "-C", str(source), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "base"], check=True)
    run_id = "tampered-lookup-receipt"
    plan = worktrees.plan(run_id, source, {}, workflow_storage.root / "tasks")
    workflow_storage.create_run(
        workflows.validate_definition(definition()), "prompt", source,
        permission_policy="saved_node", run_id=run_id, worktree=plan,
    )
    receipt_path = workflow_storage.root / "workflow-worktrees" / f"{run_id}.json"
    value = json.loads(receipt_path.read_text(encoding="utf-8"))
    if tamper == "run_id":
        value["workflow_run_id"] = "another-run"
    elif tamper == "state_root":
        value["state_root"] = str(tmp_path / "other-state")
    else:
        value["path"] = str(Path(plan["path"]).parent / ".." / run_id)
    receipt_path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(WorktreeError, match="lookup metadata"):
        workflow_worktree_receipts.paths(workflow_storage.root / "tasks")


def test_worktree_lookup_receipt_namespace_symlink_fails_closed(
    workflow_storage, tmp_path: Path,
):
    from harflow_mcp import workflow_worktree_receipts
    from harflow_mcp.worktrees import WorktreeError

    namespace = workflow_storage.root / "workflow-worktrees"
    namespace.symlink_to(tmp_path, target_is_directory=True)

    with pytest.raises(WorktreeError, match="lookup metadata"):
        workflow_worktree_receipts.paths(workflow_storage.root / "tasks")


def test_worktree_lookup_receipt_symlink_and_conflicting_registration_fail_closed(
    git_repo: Path, workflow_storage, tmp_path: Path, monkeypatch,
):
    from harflow_mcp import workflow_worktree_receipts
    from harflow_mcp.worktrees import WorktreeError

    source = git_repo
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(tmp_path / "worktrees"))
    (source / "tracked.txt").write_text("base")
    subprocess.run(["git", "-C", str(source), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "base"], check=True)
    run_id = "conflicting-lookup-registration"
    plan = worktrees.plan(run_id, source, {}, workflow_storage.root / "tasks")
    workflow_storage.create_run(
        workflows.validate_definition(definition()), "prompt", source,
        permission_policy="saved_node", run_id=run_id, worktree=plan,
    )
    receipt_path = workflow_storage.root / "workflow-worktrees" / f"{run_id}.json"
    before = receipt_path.read_bytes()

    conflicting = {**plan, "source_repo_path": str(tmp_path / "other-source")}
    with worktrees.usage_lock(Path(plan["path"])):
        with pytest.raises(ValueError, match="Conflicting"):
            workflow_worktree_receipts.register(workflow_storage.root / "tasks", run_id, conflicting)
    assert receipt_path.read_bytes() == before

    receipt_path.unlink()
    receipt_path.symlink_to(tmp_path / "missing-target.json")
    with pytest.raises(WorktreeError, match="lookup metadata"):
        workflow_worktree_receipts.paths(workflow_storage.root / "tasks")


def test_publication_lookup_uses_workflow_receipt_without_scanning_task_history(
    git_repo: Path, workflow_storage, tmp_path: Path, monkeypatch,
):
    from harflow_mcp import store

    source = git_repo
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(tmp_path / "worktrees"))
    (source / "tracked.txt").write_text("base")
    subprocess.run(["git", "-C", str(source), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "base"], check=True)
    run_id = "receipt-only-publication"
    plan = worktrees.plan(run_id, source, {}, workflow_storage.root / "tasks")
    workflow_storage.create_run(
        workflows.validate_definition(definition()), "prompt", source,
        permission_policy="saved_node", run_id=run_id, worktree=plan,
    )
    monkeypatch.setattr(store, "read_all", lambda *_args, **_kwargs: pytest.fail("publication lookup scanned all task history"))

    assert worktrees.containing_worktree_for_publication(
        workflow_storage.root / "tasks", Path(plan["path"]) / "nested",
    ) == Path(plan["path"])


def test_publication_lookup_finds_record_only_worktree_by_qualified_ancestor(
    workflow_storage, tmp_path: Path, monkeypatch,
):
    from harflow_mcp import store

    task_id = "9" * 32
    worktree_path = tmp_path / task_id
    target = worktree_path / "nested" / "repo"
    store.write(workflow_storage.root / "tasks", store.TaskRecord(
        task_id=task_id, backend="codex", session_id=None, repo_path=str(target),
        started_at="2026-01-01T00:00:00+00:00",
        worktree={"path": str(worktree_path), "source_repo_path": str(tmp_path / "source")},
    ))
    monkeypatch.setattr(store, "read_all", lambda *_args, **_kwargs: pytest.fail("publication lookup scanned all task history"))

    assert worktrees.containing_worktree_for_publication(
        workflow_storage.root / "tasks", target,
    ) == worktree_path


@pytest.mark.asyncio
async def test_start_workflow_plans_worktree_under_root_with_run_specific_branch(
    git_repo: Path, workflow_storage, tmp_path: Path, monkeypatch,
):
    source = git_repo
    (source / "tracked.txt").write_text("base")
    subprocess.run(["git", "-C", str(source), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "base"], check=True)
    worktree_root = tmp_path / "workflow-worktrees"
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(worktree_root))
    value = definition()
    workflow_storage.save(value["name"], value)
    monkeypatch.setattr(workflows, "_launch", lambda storage, run_id: None)
    run_id = "workflow-run-default-branch"

    run = await workflows.start_workflow(
        value["name"], "prompt", source, root=workflow_storage.root, run_id=run_id,
        worktree={}, _verified_caller=None,
    )

    assert run["worktree"]["path"] == str(worktree_root / run_id)
    assert run["worktree"]["branch"] == f"harflow/workflow-{run_id}"
    assert run["worktree"]["state"] == "planned"
    assert run["source_repo_path"] == str(source.resolve())


@pytest.mark.parametrize("caller_kind", ["nested", "undecidable"])
@pytest.mark.asyncio
async def test_start_workflow_worktree_refuses_nested_or_undecidable_callers(
    git_repo: Path, workflow_storage, tmp_path: Path, monkeypatch, caller_kind,
):
    source = git_repo
    (source / "tracked.txt").write_text("base")
    subprocess.run(["git", "-C", str(source), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "base"], check=True)
    value = definition()
    workflow_storage.save(value["name"], value)
    monkeypatch.setattr(workflows, "_launch", lambda storage, run_id: None)
    if caller_kind == "nested":
        caller = SimpleNamespace(record=SimpleNamespace(task_id="c" * 32, spawned_by=None))
        verified = caller
    else:
        from harflow_mcp import lineage
        monkeypatch.setattr(lineage, "detect_caller", lambda log_dir: None)
        monkeypatch.setattr(lineage, "detect_caller_detail", lambda log_dir: lineage.Detection(None, "process ancestry unreadable"))
        verified = workflows._CALLER_UNSET

    with pytest.raises(workflows.WorkflowError, match="(nested|undecidable|authority)"):
        await workflows.start_workflow(
            value["name"], "prompt", source, root=workflow_storage.root,
            worktree={}, _verified_caller=verified,
        )
