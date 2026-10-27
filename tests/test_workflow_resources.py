"""Workflow resource declarations and launch preview behavior."""

from __future__ import annotations

import asyncio
import copy
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import resources, workflow_resources, workflows, worktrees


def definition() -> dict:
    return {
        "name": "resource fixture",
        "routing_mode": "explicit",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "work", "type": "agent", "role": "task", "agent": {"backend": "codex"}, "instructions": "Work"},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "work"},
            {"id": "finish", "source": "work", "target": "end"},
        ],
    }


def invoke_workflow(name: str, workflow_id: str, *, run_resources: list[str] | None = None) -> dict:
    definition = {
        "name": name,
        "routing_mode": "explicit",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "invoke", "type": "workflow", "workflow_ref": {"workflow_id": workflow_id},
             "orchestrator_mode": "child", "instructions": "Delegate"},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "invoke"},
            {"id": "finish", "source": "invoke", "target": "end"},
        ],
    }
    if run_resources is not None:
        definition["resources"] = run_resources
    return definition


def invoke_two_workflows(name: str, first_id: str, second_id: str) -> dict:
    nodes = [{"id": "start", "type": "start"}]
    connections = []
    previous = "start"
    for index, workflow_id in enumerate((first_id, second_id)):
        node_id = f"invoke-{index}"
        nodes.append({"id": node_id, "type": "workflow", "workflow_ref": {"workflow_id": workflow_id},
                      "orchestrator_mode": "child", "instructions": "Delegate"})
        connections.append({"id": f"edge-{index}", "source": previous, "target": node_id})
        previous = node_id
    nodes.append({"id": "end", "type": "end"})
    connections.append({"id": "finish", "source": previous, "target": "end"})
    return {"name": name, "routing_mode": "explicit", "orchestrator": {"backend": "codex"},
            "nodes": nodes, "connections": connections}


@pytest.mark.parametrize("names", [["checkout:repo"], ["checkout:repo", "review-slot"]])
def test_definition_preserves_valid_run_and_attempt_resource_declarations(names):
    raw = definition()
    raw["resources"] = names
    raw["nodes"][1]["resources"] = names[:1]

    normalized = workflows.validate_definition(raw)

    assert normalized["resources"] == names
    assert normalized["nodes"][1]["resources"] == names[:1]


def test_attempt_resources_without_run_scope_remain_attempt_scoped():
    raw = definition()
    raw["nodes"][1]["resources"] = ["review-slot"]

    normalized = workflows.validate_definition(raw)

    assert "resources" not in normalized
    assert normalized["nodes"][1]["resources"] == ["review-slot"]


def test_on_demand_attempt_resources_must_be_covered_by_run_scope():
    raw = definition()
    raw["resources"] = ["checkout:repo"]
    raw["on_demand_nodes"] = [{
        "id": "specialist", "title": "Specialist", "instructions": "Work",
        "agent": {"backend": "codex"}, "resources": ["review-slot"],
    }]

    with pytest.raises(workflows.WorkflowError, match="resource"):
        workflows.validate_definition(raw)


def test_node_resources_must_be_covered_by_run_declaration():
    raw = definition()
    raw["resources"] = ["checkout:repo"]
    raw["nodes"][1]["resources"] = ["review-slot"]

    with pytest.raises(workflows.WorkflowError, match="resource"):
        workflows.validate_definition(raw)


@pytest.mark.asyncio
async def test_pinned_reused_child_scope_is_checked_for_each_preview_and_start_path(
    workflow_storage, tmp_path, monkeypatch,
):
    leaf = definition()
    leaf["name"] = "shared leaf"
    leaf["resources"] = ["checkout:repo"]
    leaf["nodes"][1]["execution_mode"] = "headless"
    leaf["nodes"][1]["resources"] = ["checkout:repo"]
    leaf = workflow_storage.save(leaf["name"], leaf)
    leaf_id = leaf["workflow_id"]

    left = workflow_storage.save(
        "left checkout scope", invoke_workflow("left checkout scope", leaf_id,
                                               run_resources=["checkout:repo"]),
    )
    right = workflow_storage.save(
        "right review scope", invoke_workflow("right review scope", leaf_id,
                                              run_resources=["review-slot"]),
    )
    root = invoke_two_workflows("two enclosing scopes", left["workflow_id"], right["workflow_id"])
    workflow_storage.save(root["name"], root)
    monkeypatch.setattr(workflows, "_launch", lambda *_: pytest.fail("scope conflict reached supervisor launch"))

    with pytest.raises(workflows.WorkflowError, match="(resource|scope|checkout:repo)"):
        workflows.preview_workflow_run(root["name"], tmp_path, root=workflow_storage.root)
    with pytest.raises(workflows.WorkflowError, match="(resource|scope|checkout:repo)"):
        await workflows.start_workflow(
            root["name"], "prompt", tmp_path, root=workflow_storage.root,
            run_id="scope-conflict-start", _verified_caller=None,
        )
    assert workflow_storage.list_runs(strict=True) == []


def test_native_subagent_node_cannot_declare_attempt_resources():
    raw = definition()
    raw["nodes"][1]["execution_mode"] = "prefer_subagent"
    raw["nodes"][1]["resources"] = ["checkout:repo"]

    with pytest.raises(workflows.WorkflowError, match="resource"):
        workflows.validate_definition(raw)


def test_workflow_invocation_node_cannot_declare_resources():
    raw = definition()
    raw["nodes"][1] = {
        "id": "work", "type": "workflow", "workflow_ref": {"workflow_id": "child"},
        "resources": ["checkout:repo"],
    }

    with pytest.raises(workflows.WorkflowError, match="resource"):
        workflows.validate_definition(raw)


def test_resource_declaration_validation_rejects_malformed_names():
    for invalid in ("", " ", "a\nb", 1, None, [], ["same", "same"], ["UPPER"]):
        raw = definition()
        raw["resources"] = invalid if isinstance(invalid, list) else [invalid]
        with pytest.raises(workflows.WorkflowError):
            workflows.validate_definition(raw)


def test_preview_hash_includes_declared_resources(tmp_path: Path, monkeypatch):
    storage = workflows.WorkflowStore(tmp_path / "state")
    raw = definition()
    storage.save(raw["name"], raw)
    before = workflows.preview_workflow_run(raw["name"], tmp_path, root=tmp_path / "state")

    changed = copy.deepcopy(raw)
    changed["resources"] = ["checkout:repo"]
    storage.save(raw["name"], changed, expected_revision=1)
    after = workflows.preview_workflow_run(raw["name"], tmp_path, root=tmp_path / "state")

    assert before["preview_hash"] != after["preview_hash"]


@pytest.fixture
def workflow_storage(tmp_path):
    from harflow_mcp import state_ownership
    from harflow_mcp.state_layout import resolve_layout

    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir(exist_ok=True)
        yield workflows.WorkflowStore(root)


def stored_run(storage, tmp_path, *, run_id="run-resource-test"):
    return storage.create_run(workflows.validate_definition(definition()), "prompt", tmp_path,
                              permission_policy="saved_node", run_id=run_id)


def _root_resource_run(storage, tmp_path, run_id, names=("checkout:repo",), *, worktree=None):
    value = definition()
    value["name"] = f"root resource run {run_id}"
    value["resources"] = list(names)
    return storage.create_run(
        workflows.validate_definition(value), "prompt", tmp_path,
        permission_policy="saved_node", run_id=run_id, worktree=worktree,
    )


def _resource_supervisor(storage, run_id):
    class Tree:
        def tree_running(self, run=None, *, refresh_atomic=False):
            current = run or storage.get_run(run_id, refresh_atomic=True)
            return current["status"] in {"starting", "running"}

    attention = []
    return SimpleNamespace(
        run_id=run_id,
        registry=SimpleNamespace(log_dir=storage.root / "tasks"),
        store=storage,
        tree=Tree(),
        run=lambda *, refresh_atomic=False: storage.get_run(run_id, refresh_atomic=True),
        update=lambda change, event, payload=None: storage.update_run(run_id, change, event, payload),
        attention=attention.append,
    ), attention


def attempt_record(*, state="granted", task_status=None, candidate_index=0, attempt_id="attempt-id"):
    ra = {"id": attempt_id, "candidate_index": candidate_index, "state": state,
          "reservation_id": "reservation-" + attempt_id, "names": ["checkout:repo"]}
    activation = {"id": "activation-id", "node_id": "work", "role": "node", "status": "running",
                  "created_at": 1.0, "tasks": [], "resource_attempt": ra}
    if task_status is not None:
        activation["tasks"].append({"task_id": "task-id-" + attempt_id, "status": task_status,
                                     "resource_attempt": dict(ra, state="attached", task_id="task-id-" + attempt_id)})
    return activation, ra


def test_attempt_without_named_resources_is_an_empty_holding_scope(workflow_storage, tmp_path):
    run = stored_run(workflow_storage, tmp_path)
    activation, _ = attempt_record(state="attached")
    activation.pop("resource_attempt")
    run = workflow_storage.update_run(run["workflow_run_id"], lambda r: r["activations"].append(activation), "fixture")
    scope = workflow_resources.scope_for_task(
        workflow_storage.root / "tasks", {"workflow_run_id": run["workflow_run_id"], "activation_id": activation["id"]},
    )

    assert scope == resources.Scope(None)
    with pytest.raises(resources.ResourceNotDeclaredByScope):
        resources.ResourceLedger(workflow_storage.root).check_borrow(["checkout:repo"], scope)


@pytest.mark.asyncio
async def test_competing_no_caller_roots_queue_for_run_scope_and_advance_fifo(workflow_storage, tmp_path):
    first = _root_resource_run(workflow_storage, tmp_path, "root-resource-first")
    second = _root_resource_run(workflow_storage, tmp_path, "root-resource-second")
    for run in (first, second):
        workflow_storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running"), "fixture")
    first_supervisor, _ = _resource_supervisor(workflow_storage, first["workflow_run_id"])
    second_supervisor, _ = _resource_supervisor(workflow_storage, second["workflow_run_id"])

    assert await workflow_resources.prepare_run(first_supervisor)
    first_held = workflow_storage.get_run(first["workflow_run_id"])["resources"]
    assert first_held["state"] == "held"
    assert "resource_scope" not in first

    waiting = asyncio.create_task(workflow_resources.prepare_run(second_supervisor))
    for _ in range(100):
        queued = workflow_storage.get_run(second["workflow_run_id"])
        if queued.get("resources", {}).get("state") == "queued":
            break
        await asyncio.sleep(0.01)
    else:
        pytest.fail("second independent workflow did not publish its queued run grant")
    assert queued["resources"]["names"] == ["checkout:repo"]

    workflow_storage.update_run(first["workflow_run_id"], lambda r: r.update(status="completed"), "fixture_complete")
    workflow_resources.release_run(workflow_storage, workflow_storage.get_run(first["workflow_run_id"]))
    assert await asyncio.wait_for(waiting, timeout=2)
    second_held = workflow_storage.get_run(second["workflow_run_id"])["resources"]
    assert second_held["state"] == "held"
    ledger = resources.ResourceLedger(workflow_storage.root)
    assert ledger.get(first_held["reservation_id"]) is None
    assert ledger.get(second_held["reservation_id"]).held


@pytest.mark.parametrize("stopped_status", ["paused", "cancelling"])
@pytest.mark.asyncio
async def test_root_resource_wait_pause_or_cancel_marks_grant_not_started(
    workflow_storage, tmp_path, stopped_status,
):
    holder = _root_resource_run(workflow_storage, tmp_path, "root-wait-holder")
    waiting_run = _root_resource_run(workflow_storage, tmp_path, "root-wait-stopped-" + stopped_status)
    for run in (holder, waiting_run):
        workflow_storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running"), "fixture")
    holder_supervisor, _ = _resource_supervisor(workflow_storage, holder["workflow_run_id"])
    waiting_supervisor, attention = _resource_supervisor(workflow_storage, waiting_run["workflow_run_id"])
    assert await workflow_resources.prepare_run(holder_supervisor)

    waiting = asyncio.create_task(workflow_resources.prepare_run(waiting_supervisor))
    for _ in range(100):
        queued = workflow_storage.get_run(waiting_run["workflow_run_id"])
        if queued.get("resources", {}).get("state") == "queued":
            break
        await asyncio.sleep(0.01)
    else:
        pytest.fail("root-level resource wait did not enter the ledger")
    workflow_storage.update_run(waiting_run["workflow_run_id"], lambda r: r.update(status=stopped_status), "fixture_stop")

    assert await asyncio.wait_for(waiting, timeout=2) is False
    stopped = workflow_storage.get_run(waiting_run["workflow_run_id"])
    assert stopped["resources"]["state"] == "not_started"
    assert "resource_wait" not in stopped
    assert not attention
    assert resources.ResourceLedger(workflow_storage.root).get(stopped["resources"]["reservation_id"]) is None


@pytest.mark.asyncio
async def test_root_resource_wait_expiry_is_attention_and_not_independent_fallback(
    workflow_storage, tmp_path, monkeypatch,
):
    holder = _root_resource_run(workflow_storage, tmp_path, "root-expiry-holder")
    waiting_run = _root_resource_run(workflow_storage, tmp_path, "root-expiry-waiter")
    for run in (holder, waiting_run):
        workflow_storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running"), "fixture")
    holder_supervisor, _ = _resource_supervisor(workflow_storage, holder["workflow_run_id"])
    waiter_supervisor, attention = _resource_supervisor(workflow_storage, waiting_run["workflow_run_id"])
    assert await workflow_resources.prepare_run(holder_supervisor)
    monkeypatch.setattr(workflow_resources, "WAIT_SECONDS", 0)

    assert await workflow_resources.prepare_run(waiter_supervisor) is False

    waiter = workflow_storage.get_run(waiting_run["workflow_run_id"])
    assert waiter["resources"]["state"] == "not_started"
    assert waiter["status"] == "running"
    assert attention and "resource acquisition failed" in attention[-1].lower()
    assert resources.ResourceLedger(workflow_storage.root).get(waiter["resources"]["reservation_id"]) is None


@pytest.mark.parametrize("state", ["held", "granted"])
@pytest.mark.asyncio
async def test_root_resume_refuses_missing_persisted_grant_without_queueing(
    workflow_storage, tmp_path, state,
):
    run = _root_resource_run(workflow_storage, tmp_path, "missing-root-grant-" + state)
    workflow_storage.update_run(run["workflow_run_id"], lambda r: (
        r.update(status="running"),
        r.update(resources={"names": ["checkout:repo"], "reservation_id": "missing-" + state, "state": state}),
    ), "fixture_missing_grant")
    supervisor, attention = _resource_supervisor(workflow_storage, run["workflow_run_id"])

    assert await workflow_resources.prepare_run(supervisor) is False
    current = workflow_storage.get_run(run["workflow_run_id"])
    assert current["resources"]["reservation_id"] == "missing-" + state
    assert current["resources"]["state"] == state
    assert attention and "grant" in attention[-1].lower()
    assert resources.ResourceLedger(workflow_storage.root).entries() == []


@pytest.mark.asyncio
async def test_recovery_promotes_durable_granted_run_entry_to_held(workflow_storage, tmp_path):
    run = _root_resource_run(workflow_storage, tmp_path, "promote-granted-root")
    workflow_storage.update_run(run["workflow_run_id"], lambda r: (
        r.update(status="running"),
        r.update(resources={"names": ["checkout:repo"], "reservation_id": "durable-grant", "state": "granted"}),
    ), "fixture_granted_run")
    resources.ResourceLedger(workflow_storage.root).request(
        ["checkout:repo"], {"kind": "workflow_run", "workflow_run_id": run["workflow_run_id"]},
        reservation_id="durable-grant",
    )
    supervisor, attention = _resource_supervisor(workflow_storage, run["workflow_run_id"])

    assert await workflow_resources.prepare_run(supervisor)
    assert workflow_storage.get_run(run["workflow_run_id"])["resources"]["state"] == "held"
    assert not attention


@pytest.mark.parametrize("repair_phase", ["before_run_grant_publication", "after_run_grant_publication"])
@pytest.mark.asyncio
async def test_run_grant_reconciles_repair_before_and_after_durable_publication(
    workflow_storage, tmp_path, monkeypatch, repair_phase,
):
    run = _root_resource_run(workflow_storage, tmp_path, "run-repair-race-" + repair_phase)
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running"), "fixture_running")
    supervisor, attention = _resource_supervisor(workflow_storage, run["workflow_run_id"])
    ledger = resources.ResourceLedger(workflow_storage.root)
    from harflow_mcp import task_resources, task_supervisor

    repairs = []
    if repair_phase == "before_run_grant_publication":
        original_acquire = resources.ResourceLedger.acquire

        async def repair_after_ledger_grant(current_ledger, *args, **kwargs):
            entry = await original_acquire(current_ledger, *args, **kwargs)
            if not repairs:
                current_ledger.path.write_text("corrupt ledger before grant publication", encoding="utf-8")
                repairs.append(task_resources.repair(workflow_storage.root / "tasks"))
            return entry

        monkeypatch.setattr(resources.ResourceLedger, "acquire", repair_after_ledger_grant)
    else:
        original_survives = task_supervisor._grant_survives

        def repair_after_durable_publication(current_ledger, reservation_id):
            if not repairs:
                persisted = workflow_storage.get_run(run["workflow_run_id"], refresh_atomic=True)
                assert persisted["resources"]["state"] == "granted"
                assert persisted["resources"]["reservation_id"] == reservation_id
                current_ledger.path.write_text("corrupt ledger after grant publication", encoding="utf-8")
                repairs.append(task_resources.repair(workflow_storage.root / "tasks"))
            return original_survives(current_ledger, reservation_id)

        monkeypatch.setattr(task_supervisor, "_grant_survives", repair_after_durable_publication)

    assert await workflow_resources.prepare_run(supervisor)

    final = workflow_storage.get_run(run["workflow_run_id"])
    assert repairs
    assert final["resources"]["state"] == "held"
    assert ledger.get(final["resources"]["reservation_id"]).held
    assert not attention
    if repair_phase == "before_run_grant_publication":
        assert repairs[0]["restored"] == []
    else:
        assert final["resources"]["reservation_id"] in repairs[0]["restored"]


def test_nested_child_run_inherits_nearest_run_resource_scope(workflow_storage, tmp_path):
    parent = stored_run(workflow_storage, tmp_path, run_id="scope-parent")
    child = stored_run(workflow_storage, tmp_path, run_id="scope-child")
    workflow_storage.update_run(parent["workflow_run_id"], lambda r: r.update(resources={
        "names": ["checkout:repo", "review-slot"], "reservation_id": "parent-reservation", "state": "held",
    }), "fixture")
    workflow_storage.update_run(child["workflow_run_id"], lambda r: r.update(parent_link={
        "workflow_run_id": parent["workflow_run_id"], "root_workflow_run_id": parent["workflow_run_id"],
    }), "fixture")

    scope = workflow_resources.scope_for_run(workflow_storage, workflow_storage.get_run(child["workflow_run_id"]))

    assert scope == resources.Scope("parent-reservation")


def test_actual_child_run_record_inherits_parent_worktree_and_resource_scope(workflow_storage, tmp_path):
    parent = stored_run(workflow_storage, tmp_path, run_id="tree-parent")
    parent_worktree = {"path": str(tmp_path / "worktree"), "source_repo_path": str(tmp_path / "source"),
                       "branch": "harflow/tree-parent", "state": "created"}
    workflow_storage.update_run(parent["workflow_run_id"], lambda r: r.update(
        resources={"names": ["checkout:repo"], "reservation_id": "tree-parent-hold", "state": "held"},
        worktree=parent_worktree, repo_path=parent_worktree["path"],
        source_repo_path=parent_worktree["source_repo_path"],
    ), "fixture")
    child_definition = workflows.validate_definition(definition())
    child_definition["name"] = "linked child"
    child_id = "linked-child-definition"
    parent = workflow_storage.update_run(parent["workflow_run_id"], lambda r: r.update(dependency_tree={
        "root_workflow_id": "root-definition", "workflows": {child_id: {"definition": child_definition, "revision": 1}},
        "edges": [], "access": {},
    }, owner_contracts=None), "fixture")
    node = {"id": "invoke", "workflow_ref": {"workflow_id": child_id}, "orchestrator_mode": "child"}
    activation = {"id": "invoke-activation"}
    invocation = {"child_workflow_run_id": "linked-child-run", "assignment": "child assignment"}

    from harflow_mcp.workflow_invocation import child_run_record
    child = child_run_record(workflow_storage, parent, activation, node, invocation)

    assert child["repo_path"] == parent_worktree["path"]
    assert child["source_repo_path"] == parent_worktree["source_repo_path"]
    assert child["worktree"] == parent_worktree
    assert child["resource_scope"] == {"reservation_id": "tree-parent-hold"}


@pytest.mark.parametrize("execution_mode", ["headless", "prefer_subagent"])
def test_run_scope_covers_native_and_headless_nodes_equally(workflow_storage, tmp_path, execution_mode):
    value = definition()
    value["resources"] = ["checkout:repo"]
    value["nodes"][1]["execution_mode"] = execution_mode
    run = workflow_storage.create_run(workflows.validate_definition(value), "prompt", tmp_path,
                                      permission_policy="saved_node")
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r.update(resources={
        "names": ["checkout:repo"], "reservation_id": f"scope-{execution_mode}", "state": "held",
    }), "fixture")
    resources.ResourceLedger(workflow_storage.root).request(
        ["checkout:repo"], {"kind": "workflow_run", "workflow_run_id": run["workflow_run_id"]},
        reservation_id=f"scope-{execution_mode}",
    )
    scope = workflow_resources.scope_for_run(workflow_storage, workflow_storage.get_run(run["workflow_run_id"]))

    assert scope == resources.Scope(f"scope-{execution_mode}")
    resources.ResourceLedger(workflow_storage.root).check_borrow(["checkout:repo"], scope)


def test_starting_child_workflow_borrows_declared_workflow_attempt_resources(workflow_storage, tmp_path):
    run = stored_run(workflow_storage, tmp_path)
    held = {"names": ["checkout:repo"], "reservation_id": "run-hold", "state": "held"}
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r.update(resources=held), "fixture")
    activation, _ = attempt_record(state="attached")
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r["activations"].append(activation), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    ledger.request(held["names"], {"kind": "workflow_run", "workflow_run_id": run["workflow_run_id"]},
                   reservation_id=held["reservation_id"])
    log_dir = workflow_storage.root / "tasks"
    owners = log_dir.parent / "workflow-owners"
    owners.mkdir(exist_ok=True)
    from harflow_mcp.workflow_receipt_documents import encode as encode_receipt
    caller_id = "c" * 32
    receipt = encode_receipt({"workflow_run_id": run["workflow_run_id"],
                              "workflow_execution_id": activation["id"]}, caller_id)
    (owners / f"{caller_id}.json").write_text(json.dumps(receipt), encoding="utf-8")
    caller = SimpleNamespace(record=SimpleNamespace(task_id=caller_id, spawned_by=None))
    pinned_tree = {"workflows": {"child": {"definition": {
        "resources": ["checkout:repo"], "nodes": [{"type": "agent", "resources": ["checkout:repo"]}],
        "on_demand_nodes": [],
    }}}}

    scope = workflow_resources.check_launch_scope(log_dir, caller, pinned_tree)

    assert scope == {"reservation_id": "run-hold"}


def test_nested_dispatch_through_nonholding_task_cannot_join_checkout_resource_cycle(
    workflow_storage, tmp_path,
):
    """A grandchild cannot wait on R while its workflow-attempt ancestor holds X."""
    from harflow_mcp import lineage, store, task_resources, task_supervision
    from harflow_mcp.workflow_receipt_documents import encode as encode_receipt

    run = stored_run(workflow_storage, tmp_path, run_id="checkout-cycle-run")
    workflow_task_id = "a" * 32
    activation_id = "checkout-cycle-activation"
    attempt = {"id": "checkout-cycle-attempt", "candidate_index": 0, "state": "attached",
               "reservation_id": "checkout-cycle-x", "names": ["checkout:repo"],
               "task_id": workflow_task_id}
    activation = {"id": activation_id, "node_id": "work", "role": "node", "status": "running",
                  "created_at": 1.0, "tasks": [{"task_id": workflow_task_id, "status": "running",
                                                  "resource_attempt": dict(attempt)}],
                  "resource_attempt": dict(attempt)}
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r["activations"].append(activation), "fixture")

    ledger = resources.ResourceLedger(workflow_storage.root)
    checkout_hold = ledger.request(
        ["checkout:repo"], {"kind": "workflow_attempt", "workflow_run_id": run["workflow_run_id"],
                            "activation_id": activation_id}, reservation_id=attempt["reservation_id"],
    )
    peer_id = "b" * 32
    peer_resource = ledger.request(["review-slot"], {"kind": "task", "task_id": peer_id})
    peer_checkout = ledger.request(["checkout:repo"], {"kind": "task", "task_id": peer_id})
    assert checkout_hold.held and peer_resource.held and peer_checkout.state == "queued"

    owners = workflow_storage.root / "workflow-owners"
    owners.mkdir(exist_ok=True)
    (owners / f"{workflow_task_id}.json").write_text(json.dumps(encode_receipt({
        "workflow_run_id": run["workflow_run_id"], "workflow_execution_id": activation_id,
    }, workflow_task_id)), encoding="utf-8")
    store.write(workflow_storage.root / "tasks", store.TaskRecord(
        task_id=workflow_task_id, backend="codex", session_id=None, repo_path=str(tmp_path),
        started_at="2026-01-01T00:00:00+00:00",
    ))
    intermediate_id = "c" * 32
    store.write(workflow_storage.root / "tasks", store.TaskRecord(
        task_id=intermediate_id, backend="codex", session_id=None, repo_path=str(tmp_path),
        started_at="2026-01-01T00:00:00+00:00", spawned_by=workflow_task_id,
    ))
    nested = lineage.Caller(store.TaskRecord(
        task_id="d" * 32, backend="codex", session_id=None, repo_path=str(tmp_path),
        started_at="2026-01-01T00:00:00+00:00", spawned_by=intermediate_id,
    ), "session")

    assert task_resources.holding_scope(workflow_storage.root / "tasks", nested) == resources.Scope(checkout_hold.reservation_id)
    before = ledger.entries()
    with pytest.raises(resources.ResourceNotDeclaredByScope):
        task_resources.build_request(workflow_storage.root / "tasks", ["review-slot"], 3600, nested)
    assert ledger.entries() == before
    assert task_supervision.read_launch(workflow_storage.root / "tasks", "d" * 32) is None

    ledger.release(checkout_hold.reservation_id)
    assert ledger.get(peer_checkout.reservation_id).held


@pytest.mark.asyncio
async def test_empty_workflow_attempt_scope_blocks_named_borrow_while_peer_waits_on_real_checkout_lease(
    workflow_storage, tmp_path,
):
    """An empty workflow scope still closes the cycle around a real checkout lease."""
    from harflow_mcp import lineage, store, task_resources, task_supervision

    repo = tmp_path / "shared-repo"
    repo.mkdir()
    run_a = stored_run(workflow_storage, repo, run_id="empty-checkout-holder")
    run_b = stored_run(workflow_storage, repo, run_id="peer-checkout-waiter")
    lease_a = workflows.CheckoutLease(
        workflow_storage, str(repo), True, wait_seconds=2, owned_run_ids={run_a["workflow_run_id"]},
    )
    await lease_a.__aenter__()

    workflow_task_id = "a" * 32
    intermediate_id = "c" * 32
    nested_id = "d" * 32
    activation_id = "empty-scope-activation"
    workflow_storage.update_run(run_a["workflow_run_id"], lambda r: (
        r.update(status="running", supervisor_pid=os.getpid()),
        r["activations"].append({
            "id": activation_id, "node_id": "work", "role": "node", "status": "running",
            "created_at": 1.0, "tasks": [{"task_id": workflow_task_id, "status": "running"}],
        }),
    ), "fixture_active_workflow_attempt")
    store.write(workflow_storage.root / "tasks", store.TaskRecord(
        task_id=workflow_task_id, backend="codex", session_id=None, repo_path=str(repo),
        started_at="2026-01-01T00:00:00+00:00", status="running",
    ))
    store.write(workflow_storage.root / "tasks", store.TaskRecord(
        task_id=intermediate_id, backend="codex", session_id=None, repo_path=str(repo),
        started_at="2026-01-01T00:00:00+00:00", spawned_by=workflow_task_id,
    ))
    nested = lineage.Caller(store.TaskRecord(
        task_id=nested_id, backend="codex", session_id=None, repo_path=str(repo),
        started_at="2026-01-01T00:00:00+00:00", spawned_by=intermediate_id,
    ), "session")

    assert "resources" not in workflow_storage.get_run(run_a["workflow_run_id"])
    assert task_resources.holding_scope(workflow_storage.root / "tasks", nested) == resources.Scope(None)

    ledger = resources.ResourceLedger(workflow_storage.root)
    peer_resource = ledger.request(
        ["review-slot"], {"kind": "workflow_run", "workflow_run_id": run_b["workflow_run_id"]},
    )
    assert peer_resource.held
    peer_waiting = asyncio.Event()
    lease_b = workflows.CheckoutLease(
        workflow_storage, str(repo), True, wait_seconds=4,
        on_wait=lambda _reason: peer_waiting.set(), owned_run_ids={run_b["workflow_run_id"]},
    )
    peer_acquire = asyncio.create_task(lease_b.__aenter__())
    try:
        await asyncio.wait_for(peer_waiting.wait(), timeout=2)
        before = ledger.entries()
        with pytest.raises(resources.ResourceNotDeclaredByScope):
            task_resources.build_request(workflow_storage.root / "tasks", ["review-slot"], 3600, nested)
        assert ledger.entries() == before
        assert task_supervision.read_launch(workflow_storage.root / "tasks", nested_id) is None

        def settle_run(run):
            run["status"] = "completed"
            run["activations"][0]["status"] = "completed"
            run["activations"][0]["tasks"][0]["status"] = "completed"

        workflow_storage.update_run(run_a["workflow_run_id"], settle_run, "fixture_task_settled")
        store.write(workflow_storage.root / "tasks", store.TaskRecord(
            task_id=workflow_task_id, backend="codex", session_id=None, repo_path=str(repo),
            started_at="2026-01-01T00:00:00+00:00", status="completed", exit_code=0,
            finished_at="2026-01-01T00:00:01+00:00", output_drained=True,
        ))
    finally:
        await lease_a.__aexit__(None, None, None)

    try:
        await asyncio.wait_for(peer_acquire, timeout=3)
        assert lease_b.handle is not None
    finally:
        if peer_acquire.done() and not peer_acquire.cancelled() and peer_acquire.exception() is None:
            await lease_b.__aexit__(None, None, None)
        else:
            peer_acquire.cancel()
            await asyncio.gather(peer_acquire, return_exceptions=True)

    assert peer_resource.held


@pytest.mark.parametrize("location", ["run", "node", "on_demand", "pinned_child"])
@pytest.mark.parametrize(("requested", "held"), [
    ("checkout:repo", "review-slot"), ("review-slot", "checkout:repo"),
])
@pytest.mark.asyncio
async def test_start_workflow_refuses_uncovered_external_holder_scope_before_publication(
    workflow_storage, tmp_path, monkeypatch, location, requested, held,
):
    from harflow_mcp import lineage, store

    holder_id = "e" * 32
    reservation = resources.ResourceLedger(workflow_storage.root).request(
        [held], {"kind": "task", "task_id": holder_id}, reservation_id="1" * 32,
    )
    caller = lineage.Caller(store.TaskRecord(
        task_id=holder_id, backend="codex", session_id=None, repo_path=str(tmp_path),
        started_at="2026-01-01T00:00:00+00:00",
        resources={"state": "held", "reservation_id": reservation.reservation_id},
    ), "session")
    child_id = None
    if location == "pinned_child":
        child = definition()
        child["name"] = "resource child"
        child["resources"] = [requested]
        child["nodes"][1]["execution_mode"] = "headless"
        child["nodes"][1]["resources"] = [requested]
        saved_child = workflow_storage.save(child["name"], child)
        child_id = saved_child["workflow_id"]
        value = invoke_workflow("resource parent", child_id)
    else:
        value = definition()
        value["name"] = "uncovered " + location
        if location == "node":
            value["nodes"][1]["execution_mode"] = "headless"
            value["nodes"][1]["resources"] = [requested]
        elif location == "on_demand":
            value["on_demand_nodes"] = [{
                "id": "specialist", "title": "Specialist", "instructions": "Work",
                "agent": {"backend": "codex"}, "resources": [requested],
            }]
        else:
            value["resources"] = [requested]
    workflow_storage.save(value["name"], value)
    monkeypatch.setattr(workflows, "_launch", lambda *_: pytest.fail("uncovered external scope reached launch"))
    existing = workflow_storage.list_runs(strict=True)

    with pytest.raises((workflows.WorkflowError, resources.ResourceNotDeclaredByScope), match="(resource|scope|declared)"):
        await workflows.start_workflow(
            value["name"], "prompt", tmp_path, root=workflow_storage.root,
            _verified_caller=caller,
        )

    assert workflow_storage.list_runs(strict=True) == existing


@pytest.mark.asyncio
async def test_external_holder_does_not_lend_through_an_empty_workflow_scope(
    workflow_storage, tmp_path, monkeypatch,
):
    from harflow_mcp import lineage, store

    holder_id = "f" * 32
    lender = resources.ResourceLedger(workflow_storage.root).request(
        ["review-slot"], {"kind": "task", "task_id": holder_id}, reservation_id="empty-boundary-lender",
    )
    caller = lineage.Caller(store.TaskRecord(
        task_id=holder_id, backend="codex", session_id=None, repo_path=str(tmp_path),
        started_at="2026-01-01T00:00:00+00:00",
        resources={"state": "held", "reservation_id": lender.reservation_id},
    ), "session")
    value = definition()
    value["name"] = "empty resource boundary"
    workflow_storage.save(value["name"], value)
    monkeypatch.setattr(workflows, "_launch", lambda *_: None)

    run = await workflows.start_workflow(
        value["name"], "prompt", tmp_path, root=workflow_storage.root,
        _verified_caller=caller,
    )

    assert "resources" not in run
    assert not run.get("resource_anchor_names")
    assert workflow_resources.scope_for_run(workflow_storage, run) == resources.Scope(None)
    with pytest.raises(resources.ResourceNotDeclaredByScope):
        resources.ResourceLedger(workflow_storage.root).check_borrow(["review-slot"], resources.Scope(None))


@pytest.mark.parametrize("attempt_state", ["queued", "granted"])
@pytest.mark.parametrize("run_status", ["running", "paused", "needs_attention"])
@pytest.mark.parametrize("ledger_entry_exists", [False, True])
def test_reconcile_dead_supervisor_marks_unattached_attempt_not_started(
    workflow_storage, tmp_path, attempt_state, run_status, ledger_entry_exists,
):
    run = stored_run(workflow_storage, tmp_path)
    activation, ra = attempt_record(state=attempt_state)
    workflow_storage.update_run(run["workflow_run_id"],
                                lambda r: (r.update(status=run_status), r["activations"].append(activation)), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    if ledger_entry_exists:
        ledger.request(ra["names"], {"kind": "workflow_attempt", "workflow_run_id": run["workflow_run_id"],
                                     "activation_id": ra["id"]}, reservation_id=ra["reservation_id"])

    recovered = workflow_resources.reconcile(workflow_storage, workflow_storage.get_run(run["workflow_run_id"]), owner_dead=True)

    persisted = recovered["activations"][0]["resource_attempt"]
    assert persisted["state"] == "not_started"
    assert persisted["released"] is True
    assert ledger.get(ra["reservation_id"]) is None


def test_reconcile_keeps_attached_uncertain_attempt_and_releases_only_terminal_task(workflow_storage, tmp_path):
    run = stored_run(workflow_storage, tmp_path)
    uncertain, uncertain_ra = attempt_record(state="attached", task_status="uncertain", attempt_id="uncertain")
    terminal, terminal_ra = attempt_record(state="attached", task_status="failed", attempt_id="failed")
    run = workflow_storage.update_run(run["workflow_run_id"],
                                      lambda r: r["activations"].extend([uncertain, terminal]), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    for ra in (uncertain_ra, terminal_ra):
        ledger.request(ra["names"], {"kind": "workflow_attempt", "workflow_run_id": run["workflow_run_id"],
                                     "activation_id": ra["id"]}, reservation_id=ra["reservation_id"])

    recovered = workflow_resources.reconcile(workflow_storage, workflow_storage.get_run(run["workflow_run_id"]), owner_dead=True)

    assert ledger.get(uncertain_ra["reservation_id"]) is not None
    assert ledger.get(terminal_ra["reservation_id"]) is None
    assert recovered["activations"][0]["resource_attempt"].get("released") is not True


def test_dead_owner_reconciliation_clears_stale_slot_flag_once_and_skips_released_attempt(
    workflow_storage, tmp_path,
):
    run = stored_run(workflow_storage, tmp_path)
    activation, ra = attempt_record(state="attached", task_status="failed", attempt_id="already-released")
    ra.update(released=True)
    activation["tasks"][0]["resource_attempt"].update(released=True)
    activation["resource_held_waiting_for_slot"] = True
    activation["resource_wait"] = {"blocked_by": []}
    run = workflow_storage.update_run(run["workflow_run_id"], lambda r: r[
        "activations"].append(activation), "fixture_stale_slot_flag")
    before = workflow_storage.get_run(run["workflow_run_id"])["sequence"]

    reconciled = workflow_resources.reconcile(
        workflow_storage, workflow_storage.get_run(run["workflow_run_id"]), owner_dead=True,
    )
    activation_after = reconciled["activations"][0]
    assert "resource_held_waiting_for_slot" not in activation_after
    assert activation_after["resource_attempt"]["state"] == "attached"
    assert activation_after["resource_attempt"]["released"] is True
    first_sequence = reconciled["sequence"]
    assert first_sequence > before

    again = workflow_resources.reconcile(
        workflow_storage, workflow_storage.get_run(run["workflow_run_id"]), owner_dead=True,
    )
    assert again["sequence"] == first_sequence


def test_repair_evidence_distinguishes_queued_from_granted_attempts(workflow_storage, tmp_path):
    run = stored_run(workflow_storage, tmp_path)
    queued, _ = attempt_record(state="queued", attempt_id="queued")
    granted, _ = attempt_record(state="granted", attempt_id="granted")
    workflow_storage.update_run(run["workflow_run_id"],
                                lambda r: r["activations"].extend([queued, granted]), "fixture")

    evidence = workflow_resources.collect_evidence(workflow_storage.root / "tasks")

    assert [item["reservation_id"] for item in evidence] == ["reservation-granted"]
    assert evidence[0]["owner"] == {"kind": "workflow_attempt", "workflow_run_id": run["workflow_run_id"],
                                     "activation_id": "granted"}


@pytest.mark.parametrize("attempt_state", ["queued", "granted"])
def test_repair_reconstructs_only_attempts_after_durable_grant_publication(
    workflow_storage, tmp_path, attempt_state,
):
    run = stored_run(workflow_storage, tmp_path)
    activation, ra = attempt_record(state=attempt_state, attempt_id=f"repair-{attempt_state}")
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r["activations"].append(activation), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    ledger.request(["seed"], {"kind": "task", "task_id": "seed-task"})
    ledger.path.write_text("corrupt ledger", encoding="utf-8")

    from harflow_mcp import task_resources
    report = task_resources.repair(workflow_storage.root / "tasks")

    expected = [ra["reservation_id"]] if attempt_state == "granted" else []
    assert (ra["reservation_id"] in report["restored"]) is bool(expected)
    assert (ledger.get(ra["reservation_id"]) is not None) is bool(expected)


def test_reaper_keeps_dead_supervisor_queued_attempt_until_explicit_reconciliation(workflow_storage, tmp_path):
    run = stored_run(workflow_storage, tmp_path)
    activation, ra = attempt_record(state="queued", attempt_id="dead-queued")
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r["activations"].append(activation), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    holder = ledger.request(["checkout:repo"], {"kind": "task", "task_id": "holder"})
    queued = ledger.request(ra["names"], {"kind": "workflow_attempt", "workflow_run_id": run["workflow_run_id"],
                                            "activation_id": ra["id"]}, reservation_id=ra["reservation_id"])
    later = ledger.request(ra["names"], {"kind": "task", "task_id": "later"})
    resolvers = workflow_resources.owner_resolvers(workflow_storage.root / "tasks")

    resolver_entry = ledger.get(queued.reservation_id)
    assert resolvers["workflow_attempt"](resolver_entry) == resources.KEEP
    ledger.reap(resolvers)
    assert ledger.get(queued.reservation_id).state == "queued"
    assert ledger.get(later.reservation_id).state == "queued"

    workflow_resources.reconcile(workflow_storage, workflow_storage.get_run(run["workflow_run_id"]), owner_dead=True)
    assert ledger.get(queued.reservation_id) is None
    assert ledger.get(later.reservation_id).state == "queued"
    ledger.release(holder.reservation_id)
    assert ledger.get(later.reservation_id).held


def test_ledger_repair_reconstructs_run_lender_and_uncertain_attempt_borrower(workflow_storage, tmp_path):
    run = stored_run(workflow_storage, tmp_path)
    run_hold = {"names": ["checkout:repo", "review-slot"], "reservation_id": "repair-run-hold", "state": "held"}
    ra = {"id": "repair-attempt", "candidate_index": 0, "state": "attached",
          "reservation_id": "repair-attempt-hold", "names": ["checkout:repo"],
          "borrowed_from": "repair-run-hold", "task_id": "uncertain-task"}
    activation = {"id": "repair-activation", "node_id": "work", "role": "node", "status": "uncertain",
                  "created_at": 1.0, "tasks": [{"task_id": "uncertain-task", "status": "uncertain",
                                                "resource_attempt": ra}], "resource_attempt": ra}
    workflow_storage.update_run(run["workflow_run_id"],
                                lambda r: (r.update(resources=run_hold), r["activations"].append(activation)), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    ledger.request(run_hold["names"], {"kind": "workflow_run", "workflow_run_id": run["workflow_run_id"]},
                   reservation_id=run_hold["reservation_id"])
    ledger.request(ra["names"], {"kind": "workflow_attempt", "workflow_run_id": run["workflow_run_id"],
                                 "activation_id": ra["id"]}, scope=resources.Scope(run_hold["reservation_id"]),
                   reservation_id=ra["reservation_id"])
    ledger.path.write_text("corrupt ledger", encoding="utf-8")

    from harflow_mcp import task_resources
    report = task_resources.repair(workflow_storage.root / "tasks")

    assert set(report["restored"]) == {run_hold["reservation_id"], ra["reservation_id"]}
    rebuilt = {entry.reservation_id: entry for entry in ledger.entries()}
    assert rebuilt[run_hold["reservation_id"]].names == tuple(run_hold["names"])
    assert rebuilt[ra["reservation_id"]].names == tuple(ra["names"])
    assert rebuilt[ra["reservation_id"]].borrows_from == run_hold["reservation_id"]


def test_resource_wait_is_visible_in_compact_detail_and_monitor_snapshot(workflow_storage, tmp_path):
    from harflow_mcp import workflow_responses

    run = {
        "workflow_run_id": "resource-wait-run", "name": "resource wait", "status": "running",
        "resource_wait": {"names": ["workflow-resource"], "reservation_id": "run-wait"},
        "activations": [{
            "id": "waiting-execution", "node_id": "work", "role": "node", "status": "running",
            "tasks": [], "resource_attempt": {"id": "attempt-wait", "candidate_index": 0,
                                                   "state": "queued", "reservation_id": "wait-id",
                                                   "names": ["checkout:repo"]},
            "resource_wait": {"blocked_by": [{"name": "checkout:repo", "holder_task_ids": ["holder"]}]},
        }, {
            "id": "sibling-wait", "node_id": "review", "role": "node", "status": "running", "tasks": [],
            "resource_attempt": {"id": "sibling-attempt", "candidate_index": 1, "state": "queued",
                                 "reservation_id": "sibling-wait-id", "names": ["review-slot"]},
            "resource_wait": {"blocked_by": [{"name": "review-slot", "holder_workflow_run_ids": ["other-run"]}]},
        }],
    }

    compact = workflow_responses.compact(run)
    details = workflow_responses.detail(run, "resource_wait")
    execution_detail = workflow_responses.detail(run, "execution:waiting-execution")
    monitor = workflow_responses.monitor(run)
    snapshot = workflow_responses.monitor_snapshot(
        run, run["workflow_run_id"], workflow_storage.root / "monitor-cache", layout=workflow_storage.state_layout,
    )

    assert compact["resource_wait"]["names"] == ["workflow-resource"]
    assert compact["resource_waits"][0]["names"] == ["checkout:repo"]
    assert compact["resource_waits"][0]["blocked_by"][0]["holder_task_ids"] == ["holder"]
    assert compact["resource_waits"][1]["execution_id"] == "sibling-wait"
    assert compact["resource_waits"][1]["blocked_by"][0]["holder_workflow_run_ids"] == ["other-run"]
    assert json.loads(details["chunk"])["names"] == ["workflow-resource"]
    assert json.loads(execution_detail["chunk"])["resource_attempt"]["state"] == "queued"
    assert len(json.loads(workflow_responses.detail(run, "resource_waits")["chunk"])) == 2
    assert monitor["monitor_digests"]["resource_wait"]
    assert monitor["monitor_digests"]["resource_waits"]
    assert snapshot["workflow_run_id"] == run["workflow_run_id"]
    assert snapshot["monitor_snapshot"] is True
    held = workflow_responses.compact({**run, "activations": [{
        **run["activations"][0], "resource_attempt": {**run["activations"][0]["resource_attempt"], "state": "granted"},
        "resource_held_waiting_for_slot": True,
    }]})
    assert held["resource_held_waiting_for_slot"] is True


@pytest.mark.parametrize("explicit_scope", [False, True])
@pytest.mark.asyncio
async def test_start_workflow_publishes_borrow_anchor_before_return_for_holding_task(
    workflow_storage, tmp_path, monkeypatch, explicit_scope,
):
    launched_entries = []
    monkeypatch.setattr(workflows, "_launch", lambda storage, run_id: launched_entries.extend(resources.ResourceLedger(storage.root).entries()))
    name = "holding caller fixture"
    run_id = "held-caller-run-" + str(explicit_scope).lower()
    value = definition()
    value["name"] = name
    value["nodes"][1]["execution_mode"] = "headless"
    node_names = ["checkout:repo"]
    value["nodes"][1]["resources"] = node_names
    if explicit_scope:
        value["resources"] = ["checkout:repo", "review-slot"]
        node_names = ["checkout:repo"]
    workflow_storage.save(name, value)
    ledger = resources.ResourceLedger(workflow_storage.root)
    lender = ledger.request(
        ["checkout:repo", "review-slot"], {"kind": "task", "task_id": "l" * 32},
    )
    from harflow_mcp import lineage, store
    caller_record = store.TaskRecord(
        task_id="l" * 32, backend="codex", session_id=None, repo_path=str(tmp_path),
        started_at="2026-01-01T00:00:00+00:00",
        resources={"state": "held", "reservation_id": lender.reservation_id},
    )
    caller = lineage.Caller(caller_record, "session")

    run = await workflows.start_workflow(name, "prompt", tmp_path, root=workflow_storage.root,
                                         run_id=run_id, _verified_caller=caller)

    anchored = ledger.entries()
    anchor = next(entry for entry in anchored if entry.owner.kind == "workflow_run" and entry.owner.workflow_run_id == run_id)
    assert any(entry.reservation_id == anchor.reservation_id for entry in launched_entries)
    assert anchor.state == "held"
    assert anchor.borrows_from == lender.reservation_id
    assert anchor.names == tuple(value.get("resources", node_names))
    if explicit_scope:
        assert "resource_anchor_names" not in run
    else:
        assert run["resource_anchor_names"] == node_names
    assert run["resources"]["state"] == "held"
    assert run["resource_scope"]["reservation_id"] == lender.reservation_id


@pytest.mark.asyncio
async def test_repair_preserves_anchor_and_full_external_lender_coverage(
    workflow_storage, tmp_path, monkeypatch,
):
    name = "anchor repair fixture"
    run_id = "anchor-repair-run"
    value = definition()
    value["name"] = name
    value["nodes"][1]["execution_mode"] = "headless"
    value["nodes"][1]["resources"] = ["checkout:repo"]
    workflow_storage.save(name, value)
    from harflow_mcp import lineage, store
    lender_record = store.TaskRecord(
        task_id="l" * 32, backend="codex", session_id=None, repo_path=str(tmp_path),
        started_at="2026-01-01T00:00:00+00:00",
        resources={"names": ["checkout:repo", "review-slot"], "reservation_id": "external-lender",
                   "state": "held"},
    )
    store.write(workflow_storage.root / "tasks", lender_record)
    caller = lineage.Caller(lender_record, "session")
    ledger = resources.ResourceLedger(workflow_storage.root)
    ledger.request(lender_record.resources["names"], {"kind": "task", "task_id": lender_record.task_id},
                   reservation_id=lender_record.resources["reservation_id"])
    monkeypatch.setattr(workflows, "_launch", lambda *_: None)

    run = await workflows.start_workflow(name, "prompt", tmp_path, root=workflow_storage.root,
                                         run_id=run_id, _verified_caller=caller)
    anchor_id = run["resources"]["reservation_id"]
    ledger.path.write_text("corrupt resource ledger", encoding="utf-8")
    from harflow_mcp import task_resources
    repaired = task_resources.repair(workflow_storage.root / "tasks")

    assert set(repaired["restored"]) == {lender_record.resources["reservation_id"], anchor_id}
    entries = {entry.reservation_id: entry for entry in ledger.entries()}
    assert entries[lender_record.resources["reservation_id"]].names == tuple(lender_record.resources["names"])
    assert entries[anchor_id].names == ("checkout:repo",)
    assert entries[anchor_id].borrows_from == lender_record.resources["reservation_id"]
    assert run["resource_anchor_names"] == ["checkout:repo"]


@pytest.mark.asyncio
async def test_supervisor_launch_failure_releases_anchor_but_retains_external_scope(
    workflow_storage, tmp_path, monkeypatch,
):
    name = "failed launch anchor"
    run_id = "failed-launch-anchor"
    value = definition()
    value["name"] = name
    value["resources"] = ["checkout:repo"]
    value["nodes"][1]["execution_mode"] = "headless"
    value["nodes"][1]["resources"] = ["checkout:repo"]
    workflow_storage.save(name, value)
    ledger = resources.ResourceLedger(workflow_storage.root)
    lender = ledger.request(["checkout:repo"], {"kind": "task", "task_id": "l" * 32})
    caller_record = __import__("harflow_mcp.store", fromlist=["TaskRecord"]).TaskRecord(
        task_id="l" * 32, backend="codex", session_id=None, repo_path=str(tmp_path),
        started_at="2026-01-01T00:00:00+00:00",
        resources={"state": "held", "reservation_id": lender.reservation_id},
    )
    from harflow_mcp import lineage
    caller = lineage.Caller(caller_record, "session")

    def no_child_started(*args, **kwargs):
        raise OSError("simulated supervisor exec failure")

    monkeypatch.setattr(workflows.subprocess, "Popen", no_child_started)
    with pytest.raises(workflows.WorkflowError, match="could not start"):
        await workflows.start_workflow(name, "prompt", tmp_path, root=workflow_storage.root,
                                       run_id=run_id, _verified_caller=caller)

    failed = workflow_storage.get_run(run_id)
    assert failed["status"] == "needs_attention"
    assert failed["resource_scope"]["reservation_id"] == lender.reservation_id
    assert failed["resources"]["state"] == "not_started"
    assert ledger.get(failed["resources"]["reservation_id"]) is None
    assert ledger.get(lender.reservation_id).held


@pytest.mark.asyncio
@pytest.mark.parametrize("execution_kind", ["headless", "native_subagent"])
async def test_replacement_launch_failure_preserves_run_hold_for_active_descendant(
    workflow_storage, tmp_path, monkeypatch, execution_kind,
):
    name = "replacement launch preserves run hold"
    run_id = "replacement-launch-hold-" + execution_kind
    value = definition()
    value["name"] = name
    value["resources"] = ["checkout:repo"]
    workflow_storage.save(name, value)
    real_launch = workflows._launch
    ledger = resources.ResourceLedger(workflow_storage.root)
    lender = ledger.request(["checkout:repo"], {"kind": "task", "task_id": "l" * 32})
    from harflow_mcp import lineage, store
    caller = lineage.Caller(store.TaskRecord(
        task_id="l" * 32, backend="codex", session_id=None, repo_path=str(tmp_path),
        started_at="2026-01-01T00:00:00+00:00",
        resources={"state": "held", "reservation_id": lender.reservation_id},
    ), "session")
    monkeypatch.setattr(workflows, "_launch", lambda *_: None)

    run = await workflows.start_workflow(
        name, "prompt", tmp_path, root=workflow_storage.root, run_id=run_id,
        _verified_caller=caller,
    )
    anchor_id = run["resources"]["reservation_id"]
    assert ledger.get(anchor_id).held

    # Model a supervisor recovery/replacement while an already launched descendant is still
    # active. The workflow-level anchor has no node-attempt borrower in this run.
    workflow_storage.update_run(
        run_id,
        lambda current: current.update(
            status="running",
            activations=[{
                "id": "active-descendant",
                "node_id": "work",
                "role": "node",
                "status": "running",
                "created_at": 1.0,
                "tasks": [{
                    "task_id": "active-task",
                    "status": "running",
                    "execution_kind": execution_kind,
                }],
            }],
        ),
        "fixture_active_descendant",
    )
    active = workflow_storage.get_run(run_id)
    assert active["resources"]["state"] == "held"
    assert "resource_attempt" not in active["activations"][0]
    assert "resource_attempt" not in active["activations"][0]["tasks"][0]

    def replacement_cannot_start(*args, **kwargs):
        raise OSError("simulated replacement supervisor exec failure")

    monkeypatch.setattr(workflows, "_launch", real_launch)
    monkeypatch.setattr(workflows.subprocess, "Popen", replacement_cannot_start)
    with pytest.raises(workflows.WorkflowError, match="could not start"):
        workflows._launch(workflow_storage, run_id)

    failed_replacement = workflow_storage.get_run(run_id)
    assert failed_replacement["status"] == "needs_attention"
    assert failed_replacement["resources"]["state"] == "held"
    assert not failed_replacement["resources"].get("released", False)
    assert ledger.get(anchor_id).held


@pytest.mark.asyncio
async def test_resume_refuses_to_queue_independently_when_anchor_lender_was_released(workflow_storage, tmp_path, monkeypatch):
    name = "lost lender resume"
    run_id = "lost-lender-resume"
    value = definition()
    value["name"] = name
    value["nodes"][1]["execution_mode"] = "headless"
    value["nodes"][1]["resources"] = ["checkout:repo"]
    workflow_storage.save(name, value)
    ledger = resources.ResourceLedger(workflow_storage.root)
    lender = ledger.request(["checkout:repo"], {"kind": "task", "task_id": "l" * 32})
    from harflow_mcp import lineage, store
    caller = lineage.Caller(store.TaskRecord(
        task_id="l" * 32, backend="codex", session_id=None, repo_path=str(tmp_path),
        started_at="2026-01-01T00:00:00+00:00",
        resources={"state": "held", "reservation_id": lender.reservation_id},
    ), "session")
    monkeypatch.setattr(workflows, "_launch", lambda *_: None)
    run = await workflows.start_workflow(name, "prompt", tmp_path, root=workflow_storage.root,
                                         run_id=run_id, _verified_caller=caller)
    anchor_id = run["resources"]["reservation_id"]
    ledger.release(anchor_id)
    ledger.release(lender.reservation_id)
    workflow_storage.update_run(run_id, lambda r: (r.update(status="running"),
                                                    r["resources"].update(state="not_started", released=True)), "fixture")

    class Tree:
        @staticmethod
        def tree_running(*args, **kwargs):
            return True

    attention = []
    supervisor = SimpleNamespace(
        run_id=run_id, store=workflow_storage,
        registry=SimpleNamespace(log_dir=workflow_storage.root / "tasks"), tree=Tree(),
        run=lambda *, refresh_atomic=False: workflow_storage.get_run(run_id, refresh_atomic=True),
        update=lambda change, event, payload=None: workflow_storage.update_run(run_id, change, event, payload),
        attention=attention.append,
    )

    resumed = await workflow_resources.prepare_run(supervisor)

    assert resumed is False
    assert attention
    assert ledger.get(anchor_id) is None
    assert len([e for e in ledger.entries() if e.owner.workflow_run_id == run_id]) == 0


def test_fallback_candidates_have_independent_resource_attempt_owners(workflow_storage, tmp_path):
    run = stored_run(workflow_storage, tmp_path)
    first, first_ra = attempt_record(state="not_started", attempt_id="candidate-zero")
    second, second_ra = attempt_record(state="attached", task_status="running", candidate_index=1,
                                        attempt_id="candidate-one")
    second["id"] = "activation-second"
    first["resource_attempt"] = first_ra
    second["resource_attempt"] = second_ra
    run = workflow_storage.update_run(run["workflow_run_id"],
                                      lambda r: r["activations"].extend([first, second]), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    ledger.request(second_ra["names"], {"kind": "workflow_attempt", "workflow_run_id": run["workflow_run_id"],
                                        "activation_id": second_ra["id"]}, reservation_id=second_ra["reservation_id"])

    evidence = workflow_resources.collect_evidence(workflow_storage.root / "tasks")

    assert len(evidence) == 1
    assert evidence[0]["reservation_id"] == second_ra["reservation_id"]
    assert evidence[0]["owner"]["activation_id"] == second_ra["id"]
    assert second_ra["candidate_index"] == 1


@pytest.mark.parametrize("execution_kind", ["headless", "native"])
def test_run_scope_resolver_keeps_hold_until_terminal_and_attempts_settled(workflow_storage, tmp_path, execution_kind):
    run = stored_run(workflow_storage, tmp_path)
    held = {"names": ["checkout:repo"], "reservation_id": "run-hold", "state": "held"}
    activation, ra = attempt_record(state="attached", task_status="uncertain")
    activation["tasks"][0]["execution_kind"] = execution_kind
    run = workflow_storage.update_run(run["workflow_run_id"],
                                      lambda r: (r.update(resources=held), r["activations"].append(activation)), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    ledger.request(held["names"], {"kind": "workflow_run", "workflow_run_id": run["workflow_run_id"]},
                   reservation_id=held["reservation_id"])
    resolver = workflow_resources.owner_resolvers(workflow_storage.root / "tasks")["workflow_run"]
    run_entry = next(e for e in ledger.entries() if e.reservation_id == held["reservation_id"])

    assert resolver(run_entry) == resources.KEEP

    workflow_storage.update_run(run["workflow_run_id"], lambda r: r.update(status="completed"), "fixture")
    # The uncertain attached worker keeps the run reservation alive after supervisor death.
    assert resolver(run_entry) == resources.KEEP
    workflow_storage.update_run(run["workflow_run_id"],
                                lambda r: r["activations"][0]["tasks"][0].update(status="failed"), "fixture")
    assert resolver(run_entry) == resources.SETTLED


def test_run_scope_resolver_keeps_terminal_parent_while_child_run_is_active(workflow_storage, tmp_path):
    parent = stored_run(workflow_storage, tmp_path, run_id="parent-run")
    child = stored_run(workflow_storage, tmp_path, run_id="child-run")
    run_hold = {"names": ["checkout:repo"], "reservation_id": "parent-hold", "state": "held"}
    child = workflow_storage.update_run(child["workflow_run_id"],
                                        lambda r: r.update(parent_link={"workflow_run_id": parent["workflow_run_id"]}, status="running"),
                                        "fixture")
    activation = {"id": "parent-activation", "node_id": "work", "role": "node", "status": "completed",
                  "created_at": 1.0, "tasks": [], "invocation": {"child_workflow_run_id": child["workflow_run_id"]}}
    parent = workflow_storage.update_run(parent["workflow_run_id"],
                                         lambda r: (r.update(resources=run_hold, status="completed"),
                                                    r["activations"].append(activation)), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    ledger.request(run_hold["names"], {"kind": "workflow_run", "workflow_run_id": parent["workflow_run_id"]},
                   reservation_id=run_hold["reservation_id"])
    resolver = workflow_resources.owner_resolvers(workflow_storage.root / "tasks")["workflow_run"]
    entry = next(item for item in ledger.entries() if item.reservation_id == run_hold["reservation_id"])

    assert resolver(entry) == resources.KEEP

    workflow_storage.update_run(child["workflow_run_id"], lambda r: r.update(status="completed"), "fixture")
    assert resolver(entry) == resources.SETTLED


def test_supervisor_recovery_marks_interrupted_worktree_creation_unknown_without_retry(workflow_storage, tmp_path, monkeypatch):
    run = stored_run(workflow_storage, tmp_path)
    worktree = {"path": str(tmp_path / "worktree-being-made"), "source_repo_path": str(tmp_path),
                "branch": "harflow/interrupted", "state": "creating"}
    workflow_storage.update_run(run["workflow_run_id"],
                                lambda r: r.update(status="running", worktree=worktree,
                                                   source_repo_path=str(tmp_path)), "fixture")

    recovered = workflow_resources.reconcile(
        workflow_storage, workflow_storage.get_run(run["workflow_run_id"]), owner_dead=True,
    )
    assert recovered["worktree"]["state"] == "unknown"
    assert recovered["status"] == "needs_attention"

    class Tree:
        @staticmethod
        def tree_running(*args, **kwargs):
            return True

    attention = []
    supervisor = SimpleNamespace(
        run_id=run["workflow_run_id"], store=workflow_storage,
        registry=SimpleNamespace(log_dir=workflow_storage.root / "tasks"), tree=Tree(),
        run=lambda *, refresh_atomic=False: workflow_storage.get_run(run["workflow_run_id"], refresh_atomic=True),
        update=lambda change, event, payload=None: workflow_storage.update_run(run["workflow_run_id"], change, event, payload),
        attention=attention.append,
    )
    monkeypatch.setattr(worktrees, "create", lambda *args, **kwargs: pytest.fail("must not retry uncertain creation"))

    assert asyncio.run(workflow_resources.prepare_run(supervisor)) is False
    assert attention


@pytest.mark.asyncio
async def test_attempt_acquisition_borrows_run_scope_and_is_released_when_not_started(workflow_storage, tmp_path):
    run = stored_run(workflow_storage, tmp_path)
    activation, _ = attempt_record(state="queued", attempt_id="initial")
    run = workflow_storage.update_run(run["workflow_run_id"],
                                      lambda r: (r.update(resources={"names": ["checkout:repo"], "reservation_id": "run-hold", "state": "held"}),
                                                 r["activations"].append(activation)), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    ledger.request(["checkout:repo"], {"kind": "workflow_run", "workflow_run_id": run["workflow_run_id"]},
                   reservation_id="run-hold")

    class RunningTree:
        @staticmethod
        def tree_running(*args, **kwargs):
            return True

    class Supervisor:
        def __init__(self):
            self.run_id = run["workflow_run_id"]
            self.registry = SimpleNamespace(log_dir=workflow_storage.root / "tasks")
            self.store = workflow_storage
            self.tree = RunningTree()

        def run(self, *, refresh_atomic=False):
            return workflow_storage.get_run(self.run_id, refresh_atomic=True)

        def update(self, change, event, payload=None):
            return workflow_storage.update_run(self.run_id, change, event, payload)

        def attention(self, reason):
            raise AssertionError(reason)

    supervisor = Supervisor()
    ra = await workflow_resources.acquire_attempt(
        supervisor, activation, {"id": "work", "resources": ["checkout:repo"]}, 2,
    )

    assert ra is not None
    assert ra["candidate_index"] == 2
    assert ra["state"] == "granted"
    entry = ledger.get(ra["reservation_id"])
    assert entry is not None and entry.borrows_from == "run-hold"
    workflow_resources.finish_attempt(supervisor, activation["id"], ra)
    assert ledger.get(ra["reservation_id"]) is None


@pytest.mark.asyncio
async def test_pause_or_cancel_while_attempt_waits_records_not_started(workflow_storage, tmp_path):
    run = stored_run(workflow_storage, tmp_path)
    activation, _ = attempt_record(state="queued", attempt_id="initial")
    run = workflow_storage.update_run(run["workflow_run_id"], lambda r: r["activations"].append(activation), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)
    ledger.request(["checkout:repo"], {"kind": "task", "task_id": "holder"}, reservation_id="other-holder")

    class StoppedTree:
        @staticmethod
        def tree_running(*args, **kwargs):
            return False

    class Supervisor:
        def __init__(self):
            self.run_id = run["workflow_run_id"]
            self.registry = SimpleNamespace(log_dir=workflow_storage.root / "tasks")
            self.store = workflow_storage
            self.tree = StoppedTree()

        def run(self, *, refresh_atomic=False):
            return workflow_storage.get_run(self.run_id, refresh_atomic=True)

        def update(self, change, event, payload=None):
            return workflow_storage.update_run(self.run_id, change, event, payload)

        def attention(self, reason):
            raise AssertionError(reason)

    with pytest.raises(workflows.DispatchNotStarted):
        await workflow_resources.acquire_attempt(
            Supervisor(), activation, {"id": "work", "resources": ["checkout:repo"]}, 0,
        )

    recovered = workflow_storage.get_run(run["workflow_run_id"])
    attempt = recovered["activations"][0]["resource_attempt"]
    assert attempt["state"] == "not_started"
    assert ledger.get(attempt["reservation_id"]) is None


@pytest.mark.asyncio
async def test_dispatch_acquires_resources_before_tree_slot(workflow_storage, tmp_path, monkeypatch):
    from test_workflows import FakeRegistry

    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    definition_with_resources = definition()
    definition_with_resources["nodes"][1]["resources"] = ["checkout:repo"]
    run = workflow_storage.create_run(workflows.validate_definition(definition_with_resources), "prompt", tmp_path,
                                      permission_policy="saved_node")
    supervisor = workflows.WorkflowSupervisor(FakeRegistry(workflow_storage.root, ["completed"]), workflow_storage)
    supervisor.run_id = run["workflow_run_id"]
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running"), "fixture")
    node = run["definition"]["nodes"][1]
    activation = supervisor._activation("work", "node")
    events = []
    original_slot = supervisor.tree.acquire_slot
    original_resources = workflow_resources.acquire_attempt

    async def observed_slot(*args, **kwargs):
        events.append("slot")
        return await original_slot(*args, **kwargs)

    async def observed_resources(*args, **kwargs):
        events.append("resources")
        return await original_resources(*args, **kwargs)

    monkeypatch.setattr(supervisor.tree, "acquire_slot", observed_slot)
    monkeypatch.setattr(workflow_resources, "acquire_attempt", observed_resources)

    await supervisor._dispatch(node, "assignment", "node", activation)

    assert events[:2] == ["resources", "slot"]


@pytest.mark.parametrize("execution_mode", ["headless", "prefer_subagent"])
@pytest.mark.asyncio
async def test_run_scope_hold_is_disclosed_while_native_or_headless_node_waits_for_slot(
    workflow_storage, tmp_path, monkeypatch, execution_mode,
):
    from test_workflows import FakeRegistry

    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    definition_with_scope = definition()
    definition_with_scope["resources"] = ["checkout:repo"]
    definition_with_scope["max_parallel"] = 1
    definition_with_scope["nodes"][1]["execution_mode"] = execution_mode
    definition_with_scope["nodes"][1]["freedom"] = "read_only"
    run = workflow_storage.create_run(workflows.validate_definition(definition_with_scope), "prompt", tmp_path,
                                      permission_policy="saved_node")
    rid = run["workflow_run_id"]
    workflow_storage.update_run(rid, lambda r: (r.update(status="running"), r.update(resources={
        "names": ["checkout:repo"], "reservation_id": f"slot-scope-{execution_mode}", "state": "held",
    })), "fixture")
    resources.ResourceLedger(workflow_storage.root).request(
        ["checkout:repo"], {"kind": "workflow_run", "workflow_run_id": rid},
        reservation_id=f"slot-scope-{execution_mode}",
    )
    from harflow_mcp.workflow_invocation import WorkflowTree
    slot_tree = WorkflowTree(workflow_storage, rid, permits=1)
    supervisor = workflows.WorkflowSupervisor(FakeRegistry(workflow_storage.root, []), workflow_storage, tree=slot_tree)
    supervisor.run_id = rid
    node = workflow_storage.get_run(rid)["definition"]["nodes"][1]
    activation = supervisor._activation("work", "node")
    await supervisor.tree.acquire_slot(lambda: True)
    dispatch = asyncio.create_task(supervisor._dispatch(node, "assignment", "node", activation))
    for _ in range(40):
        observed = workflow_storage.get_run(rid)
        if observed["activations"][0].get("resource_held_waiting_for_slot"):
            break
        await asyncio.sleep(0.05)
    else:
        pytest.fail("workflow did not disclose a resource hold while blocked on the occupied slot")

    assert observed["activations"][0]["resource_held_waiting_for_slot"] is True
    assert __import__("harflow_mcp.workflow_responses", fromlist=["compact"]).compact(observed)["resource_held_waiting_for_slot"]
    workflow_storage.update_run(rid, lambda r: r.update(status="paused"), "fixture_pause")
    assert await asyncio.wait_for(dispatch, timeout=2) is None
    supervisor.tree.slots.release()
    assert "resource_held_waiting_for_slot" not in workflow_storage.get_run(rid)["activations"][0]


@pytest.mark.asyncio
async def test_slot_disclosure_read_failure_does_not_replace_wait_outcome_or_strand_permit(
    workflow_storage, tmp_path,
):
    from test_workflows import FakeRegistry
    from harflow_mcp.workflow_invocation import WorkflowTree

    run = stored_run(workflow_storage, tmp_path, run_id="slot-disclosure-read-failure")
    rid = run["workflow_run_id"]
    workflow_storage.update_run(rid, lambda r: r.update(status="running"), "fixture_running")
    tree = WorkflowTree(workflow_storage, rid, permits=1)
    supervisor = workflows.WorkflowSupervisor(FakeRegistry(workflow_storage.root, []), workflow_storage, tree=tree)
    supervisor.run_id = rid
    activation = supervisor._activation("work", "node")
    await tree.acquire_slot(lambda: True)

    def unreadable_run(*, refresh_atomic=False):
        raise OSError("injected disclosure-only read failure")

    supervisor.run = unreadable_run
    with pytest.raises(workflows.DispatchNotStarted, match="Scheduling stopped"):
        await workflow_resources.acquire_slot(supervisor, activation, lambda: False)

    assert tree.slots.free == 0
    assert not tree.slots.waiters
    assert "resource_held_waiting_for_slot" not in workflow_storage.get_run(rid)["activations"][0]
    tree.slots.release()
    assert tree.slots.free == 1


@pytest.mark.asyncio
async def test_slot_wait_cancellation_after_attempt_grant_releases_resource(workflow_storage, tmp_path, monkeypatch):
    from test_workflows import FakeRegistry

    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    definition_with_resources = definition()
    definition_with_resources["nodes"][1]["resources"] = ["checkout:repo"]
    definition_with_resources["nodes"][1]["freedom"] = "read_only"
    run = workflow_storage.create_run(workflows.validate_definition(definition_with_resources), "prompt", tmp_path,
                                      permission_policy="saved_node")
    rid = run["workflow_run_id"]
    workflow_storage.update_run(rid, lambda r: r.update(status="running"), "fixture")
    supervisor = workflows.WorkflowSupervisor(FakeRegistry(workflow_storage.root, []), workflow_storage)
    supervisor.run_id = rid
    activation = supervisor._activation("work", "node")
    waiting = asyncio.Event()
    release = asyncio.Event()

    async def blocked_slot(should_continue, **kwargs):
        waiting.set()
        await release.wait()
        raise workflows.DispatchNotStarted("cancelled during slot wait")

    monkeypatch.setattr(supervisor.tree, "acquire_slot", blocked_slot)
    dispatch = asyncio.create_task(supervisor._dispatch(run["definition"]["nodes"][1], "assignment", "node", activation))
    await asyncio.wait_for(waiting.wait(), timeout=2)
    persisted = workflow_storage.get_run(rid)["activations"][0]["resource_attempt"]
    assert persisted["state"] == "granted"
    ledger = resources.ResourceLedger(workflow_storage.root)
    assert ledger.get(persisted["reservation_id"]).held
    release.set()

    assert await asyncio.wait_for(dispatch, timeout=2) is None
    final = workflow_storage.get_run(rid)["activations"][0]
    assert final["resource_attempt"]["state"] == "not_started"
    assert ledger.get(persisted["reservation_id"]) is None


@pytest.mark.parametrize("fail_cleanup", [False, True])
@pytest.mark.asyncio
async def test_dispatch_reserved_publication_failure_preserves_error_and_releases_slot(
    workflow_storage, tmp_path, monkeypatch, fail_cleanup,
):
    from test_workflows import FakeRegistry

    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    definition_with_resources = definition()
    definition_with_resources["nodes"][1]["resources"] = ["checkout:repo"]
    run = workflow_storage.create_run(workflows.validate_definition(definition_with_resources), "prompt", tmp_path,
                                      permission_policy="saved_node")
    supervisor = workflows.WorkflowSupervisor(FakeRegistry(workflow_storage.root, []), workflow_storage)
    supervisor.run_id = run["workflow_run_id"]
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running"), "fixture")
    original_update = supervisor.update
    original_release_slot = supervisor.tree.release_slot
    slot_releases = []
    captured = {}

    def track_slot_release(*args, **kwargs):
        slot_releases.append(True)
        return original_release_slot(*args, **kwargs)

    monkeypatch.setattr(supervisor.tree, "release_slot", track_slot_release)

    def fail_dispatch_reserved(change, event, payload=None):
        if event == "resource_attempt_queued":
            result = original_update(change, event, payload)
            captured["reservation_id"] = result["activations"][-1]["resource_attempt"]["reservation_id"]
            return result
        if event == "dispatch_reserved":
            raise OSError("simulated failure before reservation publication")
        return original_update(change, event, payload)

    monkeypatch.setattr(supervisor, "update", fail_dispatch_reserved)
    if fail_cleanup:
        original_release = resources.ResourceLedger.release

        def fail_release(ledger, reservation_id):
            if reservation_id == captured.get("reservation_id"):
                raise resources.ResourceLedgerUnavailable("simulated cleanup ledger outage")
            return original_release(ledger, reservation_id)

        monkeypatch.setattr(resources.ResourceLedger, "release", fail_release)
    with pytest.raises(OSError, match="before reservation publication"):
        await supervisor._dispatch(run["definition"]["nodes"][1], "assignment", "node",
                                   supervisor._activation("work", "node"))

    ledger = resources.ResourceLedger(workflow_storage.root)
    assert captured["reservation_id"]
    assert slot_releases
    if not fail_cleanup:
        assert ledger.get(captured["reservation_id"]) is None


@pytest.mark.asyncio
async def test_competing_workflow_attempts_queue_on_same_resource_then_grant_in_order(workflow_storage, tmp_path):
    ledger = resources.ResourceLedger(workflow_storage.root)
    supervisors = []
    activations = []
    for index in range(2):
        run = stored_run(workflow_storage, tmp_path, run_id=f"competing-run-{index}")
        activation, _ = attempt_record(attempt_id=f"seed-{index}")
        activation["id"] = f"activation-{index}"
        workflow_storage.update_run(run["workflow_run_id"], lambda r, a=activation: r["activations"].append(a), "fixture")

        class Tree:
            @staticmethod
            def tree_running(*args, **kwargs):
                return True

        rid = run["workflow_run_id"]
        supervisor = SimpleNamespace(
            run_id=rid,
            registry=SimpleNamespace(log_dir=workflow_storage.root / "tasks"),
            store=workflow_storage,
            tree=Tree(),
            run=lambda *, refresh_atomic=False, rid=rid: workflow_storage.get_run(rid, refresh_atomic=True),
            update=lambda change, event, payload=None, rid=rid: workflow_storage.update_run(rid, change, event, payload),
            attention=lambda reason: pytest.fail(reason),
        )
        supervisors.append(supervisor)
        activations.append(activation)

    first = await workflow_resources.acquire_attempt(
        supervisors[0], activations[0], {"id": "work", "resources": ["checkout:repo"]}, 0,
    )
    second_waiter = asyncio.create_task(workflow_resources.acquire_attempt(
        supervisors[1], activations[1], {"id": "work", "resources": ["checkout:repo"]}, 0,
    ))
    await asyncio.sleep(0.05)
    queued = workflow_storage.get_run(supervisors[1].run_id)["activations"][0]["resource_attempt"]
    assert ledger.get(queued["reservation_id"]).state == "queued"

    workflow_resources.finish_attempt(supervisors[0], activations[0]["id"], first)
    second = await asyncio.wait_for(second_waiter, timeout=2)

    assert ledger.get(second["reservation_id"]).held
    workflow_resources.finish_attempt(supervisors[1], activations[1]["id"], second)
    assert ledger.entries() == []


@pytest.mark.asyncio
async def test_attempt_reacquires_when_repair_race_drops_unconfirmed_grant(workflow_storage, tmp_path, monkeypatch):
    run = stored_run(workflow_storage, tmp_path)
    activation, _ = attempt_record(attempt_id="repair-race")
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r["activations"].append(activation), "fixture")
    ledger = resources.ResourceLedger(workflow_storage.root)

    class Tree:
        @staticmethod
        def tree_running(*args, **kwargs):
            return True

    rid = run["workflow_run_id"]
    supervisor = SimpleNamespace(
        run_id=rid,
        registry=SimpleNamespace(log_dir=workflow_storage.root / "tasks"),
        store=workflow_storage,
        tree=Tree(),
        run=lambda *, refresh_atomic=False: workflow_storage.get_run(rid, refresh_atomic=True),
        update=lambda change, event, payload=None: workflow_storage.update_run(rid, change, event, payload),
        attention=lambda reason: pytest.fail(reason),
    )
    original_survives = __import__("harflow_mcp.task_supervisor", fromlist=["_grant_survives"])._grant_survives
    checks = 0

    def lose_first_grant(check_ledger, reservation_id):
        nonlocal checks
        checks += 1
        if checks == 1:
            check_ledger.release(reservation_id)
            return False
        return original_survives(check_ledger, reservation_id)

    monkeypatch.setattr("harflow_mcp.task_supervisor._grant_survives", lose_first_grant)

    ra = await workflow_resources.acquire_attempt(
        supervisor, activation, {"id": "work", "resources": ["checkout:repo"]}, 0,
    )

    assert checks >= 2
    assert ra["state"] == "granted"
    assert ledger.get(ra["reservation_id"]).held
    workflow_resources.finish_attempt(supervisor, activation["id"], ra)


@pytest.mark.parametrize("failure", [resources.ResourceLedgerCorrupt, resources.ResourceLedgerRepairing,
                                      resources.ResourceWaitExpired])
@pytest.mark.asyncio
async def test_queued_ledger_error_settles_attempt_not_started_and_attention(
    workflow_storage, tmp_path, monkeypatch, failure,
):
    run = stored_run(workflow_storage, tmp_path)
    activation, _ = attempt_record(attempt_id="ledger-error")
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r["activations"].append(activation), "fixture")
    attention = []

    class Tree:
        @staticmethod
        def tree_running(*args, **kwargs):
            return True

    rid = run["workflow_run_id"]
    supervisor = SimpleNamespace(
        run_id=rid,
        registry=SimpleNamespace(log_dir=workflow_storage.root / "tasks"),
        store=workflow_storage,
        tree=Tree(),
        run=lambda *, refresh_atomic=False: workflow_storage.get_run(rid, refresh_atomic=True),
        update=lambda change, event, payload=None: workflow_storage.update_run(rid, change, event, payload),
        attention=attention.append,
    )

    async def fail_acquire(self, *args, **kwargs):
        raise failure("simulated resource ledger failure")

    if failure is resources.ResourceWaitExpired:
        monkeypatch.setattr(workflow_resources, "WAIT_SECONDS", 0)
    monkeypatch.setattr(resources.ResourceLedger, "acquire", fail_acquire)
    with pytest.raises(workflows.DispatchNotStarted):
        await workflow_resources.acquire_attempt(
            supervisor, activation, {"id": "work", "resources": ["checkout:repo"]}, 0,
        )

    persisted = workflow_storage.get_run(rid)["activations"][0]["resource_attempt"]
    assert persisted["state"] == "not_started"
    assert attention


@pytest.mark.asyncio
async def test_resource_ledger_failure_does_not_advance_to_fallback_candidate(
    workflow_storage, tmp_path, monkeypatch,
):
    from test_workflows import FakeRegistry

    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    value = definition()
    value["nodes"][1]["resources"] = ["checkout:repo"]
    value["nodes"][1]["agent"]["fallbacks"] = [{"backend": "claude"}]
    run = workflow_storage.create_run(workflows.validate_definition(value), "prompt", tmp_path,
                                      permission_policy="saved_node")
    workflow_storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running"), "fixture")
    registry = FakeRegistry(workflow_storage.root, ["should not start"])
    supervisor = workflows.WorkflowSupervisor(registry, workflow_storage)
    supervisor.run_id = run["workflow_run_id"]

    async def fail_acquire(self, *args, **kwargs):
        raise resources.ResourceLedgerCorrupt("injected corrupt ledger")

    monkeypatch.setattr(resources.ResourceLedger, "acquire", fail_acquire)
    activation = supervisor._activation("work", "node")

    outcome = await supervisor._dispatch(run["definition"]["nodes"][1], "assignment", "node", activation)

    after = workflow_storage.get_run(run["workflow_run_id"])
    persisted = next(a for a in after["activations"] if a["id"] == activation["id"])
    assert outcome is None
    assert registry.calls == []
    assert persisted["resource_attempt"]["state"] == "not_started"
    assert persisted["status"] == "not_started"
    assert after["status"] == "needs_attention"


@pytest.mark.parametrize(
    "left_names,right_names,expected_overlap",
    [(["sibling-slot"], ["sibling-slot"], 1), (["left-slot"], ["right-slot"], 2)],
)
@pytest.mark.asyncio
async def test_parallel_siblings_share_or_separate_resource_capacity_and_reach_join(
    workflow_storage, tmp_path, monkeypatch, left_names, right_names, expected_overlap,
):
    from test_workflows import FakeRegistry, parallel_definition

    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    definition_ = parallel_definition()
    definition_["nodes"][1]["freedom"] = "read_only"
    for node in definition_["nodes"]:
        if node["id"] == "left":
            node["resources"] = left_names
        elif node["id"] == "right":
            node["resources"] = right_names
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"],
                           "reason": "Both branches are required"})
    registry = FakeRegistry(workflow_storage.root, ["split", decision, "left done", "right done"])
    active = 0
    maximum = 0
    original_start = registry.start

    async def overlapping_start(*args, **kwargs):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        try:
            await asyncio.sleep(0.05)
            return await original_start(*args, **kwargs)
        finally:
            active -= 1

    registry.start = overlapping_start
    run = workflow_storage.create_run(workflows.validate_definition(definition_), "prompt", tmp_path,
                                      permission_policy="saved_node")
    def use_historical_execution(value):
        value.pop("execution_contract", None)
        for node in value["definition"]["nodes"]:
            if node["type"] == "agent":
                node["session_mode"] = "resume"
    workflow_storage.update_run(run["workflow_run_id"], use_historical_execution, "fixture")
    monkeypatch.setattr(workflows, "_require_delegation_control", lambda _run: None)
    supervisor = workflows.WorkflowSupervisor(registry, workflow_storage)
    await asyncio.wait_for(supervisor.execute(run["workflow_run_id"]), timeout=5)
    finished = workflow_storage.get_run(run["workflow_run_id"])

    assert finished["status"] == "completed", finished.get("attention_reason")
    assert maximum == expected_overlap
    assert workflow_resources._unresolved(workflow_storage, finished) is False
    assert resources.ResourceLedger(workflow_storage.root).entries() == []
