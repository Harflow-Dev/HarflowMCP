"""Pure deterministic fence and semantic identity tests; no registry/provider IO."""
from types import SimpleNamespace

import copy
import uuid

import pytest

from harflow_mcp import orchestrator_steering as o
from harflow_mcp.workflow_steering import initialize, SteeringError
from harflow_mcp.workflows import DispatchNotStarted


def run_record(rid="root"):
    run = {"workflow_run_id": rid, "kind": "workflow", "status": "running", "definition": {"name": rid, "workflow_id": rid, "revision": 1, "nodes": [], "orchestrator": {}},
           "execution_contract": "delegation", "runner_policy": "guided", "activations": [], "pending": [{"id": "token", "node_id": "start", "decision_id": "decision"}],
           "tasks": [], "joins": {}, "decisions": [], "sessions": {}}
    initialize(run)
    return run


def link_child(root, child, aid, nid, mode="current"):
    from harflow_mcp.workflow_references import definition_sha256, subtree
    cid = child["workflow_run_id"]
    node = {"id": nid, "type": "workflow", "workflow_ref": {"workflow_id": cid}, "orchestrator_mode": mode}
    root["definition"]["nodes"].append(node)
    tree = root.setdefault("dependency_tree", {"root_workflow_id": root["workflow_run_id"], "workflows": {}, "edges": [], "access": {}})
    digest = definition_sha256(child["definition"])
    tree["workflows"][cid] = {"definition": child["definition"], "revision": 1, "definition_sha256": digest}
    tree["edges"].append({"from": root["workflow_run_id"], "node_id": nid, "to": cid, "orchestrator_mode": mode})
    invocation = {"child_workflow_run_id": cid, "workflow_id": cid, "revision": 1, "definition_sha256": digest, "orchestrator_mode": mode, "stage": "running", "timeout_elapsed_seconds": 1, "active_since": 10}
    root["activations"].append({"id": aid, "node_id": nid, "role": "node", "tasks": [], "invocation": invocation})
    child.update(revision=1, workflow_id=cid, definition_hash=digest, orchestrator_mode=mode, dependency_tree=subtree(tree, cid), parent_link={"workflow_run_id": root["workflow_run_id"], "root_workflow_run_id": root["workflow_run_id"], "execution_id": aid, "node_id": nid, "depth": 2})
    if mode == "current":
        child.update(orchestrator_session_owner_run_id=root["workflow_run_id"], orchestrator_config=root["definition"]["orchestrator"])
    return invocation


def store_for(*runs):
    records = {r["workflow_run_id"]: r for r in runs}
    return SimpleNamespace(get_run=lambda rid, **kwargs: records[rid])


def hold(root, target):
    root["steering"]["episodes"] = [{"episode_id": "episode", "hold": True, "target": target}]
    root["steering"]["turns"] = [{"episode_id": "episode", "session_owner_run_id": "root"}]


@pytest.mark.parametrize("event", sorted(o.NEW_EFFECTS))
def test_shared_tree_fence_blocks_new_root_effects(event):
    root = run_record()
    storage = store_for(root)
    hold(root, o.target_for(storage, root, "root", "token"))
    with pytest.raises(DispatchNotStarted):
        o.fence(storage, root, event)


@pytest.mark.parametrize("event", ["task_updated", "node_result", "session_recorded", "steering_reply_captured"])
def test_issued_observations_are_not_fenced(event):
    root = run_record()
    storage = store_for(root)
    hold(root, o.target_for(storage, root, "root", "token"))
    o.fence(storage, root, event)


def test_node_result_changes_tree_identity_but_control_capture_does_not():
    root = run_record()
    storage = store_for(root)
    before = o.target_for(storage, root, "root", "token")
    root["activations"].append({"id": "control", "node_id": "start", "role": "orchestrator", "tasks": [{"task_id": "provider", "status": "running"}]})
    assert o.target_for(storage, root, "root", "token") == before
    root["activations"].append({"id": "sibling", "node_id": "work", "role": "node", "tasks": [], "node_result": {"status": "succeeded", "result": {}, "evidence": []}})
    assert o.target_for(storage, root, "root", "token")["assignment_digest"] != before["assignment_digest"]


def test_reciprocal_nested_link_is_qualified_and_transport_ticks_are_not_semantic():
    root, child = run_record(), run_record("child")
    invocation = link_child(root, child, "invocation", "child-node")
    storage = store_for(root, child)
    before = o.target_for(storage, root, "child", "token")
    assert before["path"] == ["root", "child"]
    invocation.update(timeout_elapsed_seconds=2, active_since=11, stage="waiting")
    assert o.target_for(storage, root, "child", "token") == before
    invocation["definition_sha256"] = "changed"
    with pytest.raises(SteeringError, match="pinned source"):
        o.target_for(storage, root, "child", "token")
    invocation["definition_sha256"] = child["definition_hash"]
    child["parent_link"]["execution_id"] = "wrong"
    with pytest.raises(SteeringError, match="reciprocal"):
        o.targets(storage, root)


def test_accepted_unreserved_sibling_cannot_be_retracted_by_admission():
    root = run_record()
    root["pending"].append({"id": "issued-sibling", "node_id": "work", "accepted_decision_id": "already-accepted", "assignments": {"edge": {"assignment_prompt": "Issued work"}}})
    storage = store_for(root)
    witness = o.target_for(storage, root, "root", "token")
    with pytest.raises(SteeringError) as exc:
        o.admission(storage, root, {"target": witness, "allow_fresh_context": True})
    assert exc.value.code == "steering_boundary_pending"
    assert root["pending"][1]["accepted_decision_id"] == "already-accepted"


def test_current_mode_sibling_new_control_is_fenced_by_exact_shared_owner():
    root, target, sibling = run_record(), run_record("target"), run_record("sibling")
    for child in (target, sibling):
        aid = child["workflow_run_id"] + "-activation"
        nid = child["workflow_run_id"] + "-node"
        link_child(root, child, aid, nid)
    storage = store_for(root, target, sibling)
    hold(root, o.target_for(storage, root, "target", "token"))
    for event, detail in (("decision_attempt_reserved", None), ("decision_accepted", None), ("dispatch_reserved", {"steering_role": "orchestrator"})):
        with pytest.raises(DispatchNotStarted):
            o.fence(storage, sibling, event, detail)
    o.fence(storage, sibling, "task_updated")
    o.fence(storage, sibling, "dispatch_reserved", {"steering_role": "node"})


@pytest.mark.parametrize("receipt", [{}, {"turn_id": "turn"}, {"root_workflow_run_id": "root", "turn_id": "turn", "command_digest": "sha256:" + "a" * 64, "state": "invented"}, {"root_workflow_run_id": "other-root", "turn_id": "turn", "command_digest": "sha256:" + "a" * 64, "state": "applied"}])
def test_incomplete_or_forged_action_receipt_is_not_qualified(receipt):
    from harflow_mcp.workflow_run_documents import validate_steering_receipts
    with pytest.raises(ValueError, match="exact steering child receipt"):
        validate_steering_receipts({"steering_action_receipts": {"turn": receipt}}, "root")


def test_reader_bypass_cannot_turn_incomplete_child_receipt_into_committed_action(monkeypatch):
    from contextlib import nullcontext
    root = run_record()
    command = {"decision_id": "decision", "action": "continue", "reason": "Configured route"}
    turn = {"turn_id": "turn", "episode_id": "episode", "state": "captured", "target": {"workflow_run_id": "child"}, "action_commit": {"state": "intent", "command": command}}
    root["steering"]["turns"] = [turn]
    root["steering"]["episodes"] = [{"episode_id": "episode", "state": "classifying", "hold": True}]
    child = {"steering_action_receipts": {"turn": {}}}
    def update(mutator, event, detail=None):
        mutator(root)
        return root
    supervisor = SimpleNamespace(run_id="root", run=lambda **kwargs: root, update=update, store=SimpleNamespace(tree_mutation=lambda rid: nullcontext()))
    fake_child = SimpleNamespace(update=lambda mutate, *args: mutate(child))
    monkeypatch.setattr(o, "_target_supervisor", lambda *args: fake_child)
    o.apply_commit(supervisor, "turn")
    assert turn["action_commit"]["state"] == "refused"
    assert turn["outcome"] == "steering_source_unavailable"
    assert root["steering"]["episodes"][0]["hold"] is True
    assert root["steering"]["commits"] == []


@pytest.mark.parametrize("status", ["cancelling", "cancelled", "failed", "completed", "needs_input", "needs_attention"])
def test_conversation_parking_preserves_real_control_and_question_state(status):
    root = run_record()
    root.update(status=status, attention_reason="Existing real state")
    o._park(root, store_for(root))
    assert root["status"] == status
    assert root["attention_reason"] == "Existing real state"


@pytest.mark.parametrize("role,status,accepted", [("orchestrator", "completed", "new"), ("node", "completed", "new"), ("node", "running", "old")])
def test_authorizing_control_or_historical_worker_is_not_current_execution(role, status, accepted):
    root = run_record()
    token = {"id": "sibling", "node_id": "work", "accepted_decision_id": "new", "assignment_prompt": "New issued assignment"}
    root["activations"] = [{"id": "historical", "role": role, "node_id": "work", "token": {**token, "accepted_decision_id": accepted}, "tasks": [{"task_id": "task", "status": status}]}]
    assert o.current_execution_reserved(root, token) is False
    root["activations"][0].update(role="node", token=copy.deepcopy(token))
    root["activations"][0]["tasks"][0]["status"] = "reserved"
    assert o.current_execution_reserved(root, token) is True


@pytest.mark.parametrize("kind", ["workflow", "builder"])
@pytest.mark.parametrize("status", ["cancelling", "cancelled", "failed", "completed", "needs_input", "needs_attention"])
def test_settled_dismiss_preserves_real_workflow_and_builder_state(kind, status):
    from harflow_mcp.steering_admission import admit
    from harflow_mcp.workflow_steering import target
    root = run_record()
    root["kind"] = kind
    storage = store_for(root)
    witness = o.target_for(storage, root, "root", "token") if kind == "workflow" else target(root)
    root.update(status=status, attention_reason="Original real state")
    root["steering"]["episodes"] = [{"episode_id": "episode", "steering_revision": 1, "state": "awaiting_caller", "hold": True, "target": witness}]
    request = {"request_id": str(uuid.uuid4()), "action": "dismiss", "target": witness, "episode_id": "episode", "expected_steering_revision": 1, "expected_interaction_epoch": 0, "expected_control_revision": 0}
    admit(storage, root, None, True, request, "digest", "binding")
    assert root["status"] == status
    assert root["attention_reason"] == "Original real state"
    assert not root["steering"]["episodes"][0]["hold"]


def test_reply_cannot_slide_episode_to_a_fresh_changed_goal_witness():
    from harflow_mcp.steering_admission import admit
    root = run_record()
    storage = store_for(root)
    old = o.target_for(storage, root, "root", "token")
    root["steering"]["episodes"] = [{"episode_id": "episode", "steering_revision": 1, "state": "awaiting_caller", "hold": True, "target": old}]
    root["goal_revision"] = 1
    fresh = o.target_for(storage, root, "root", "token")
    request = {"request_id": str(uuid.uuid4()), "action": "reply", "target": fresh, "episode_id": "episode", "expected_steering_revision": 1, "expected_interaction_epoch": 0, "expected_control_revision": 0}
    with pytest.raises(SteeringError) as exc:
        admit(storage, root, None, True, request, "digest", "binding")
    assert exc.value.code == "steering_target_stale"
    assert not root["steering"]["turns"]


@pytest.mark.parametrize("field,value", [("definition_hash", "0" * 64), ("revision", 2), ("workflow_id", "other"), ("orchestrator_mode", "child"), ("orchestrator_session_owner_run_id", "sibling"), ("orchestrator_config", {"backend": "unrelated"})])
def test_shape_valid_child_source_or_owner_contradiction_refuses(field, value):
    root, child = run_record(), run_record("child")
    link_child(root, child, "invocation", "child-node")
    child[field] = value
    with pytest.raises(SteeringError) as exc:
        o.targets(store_for(root, child), root)
    assert exc.value.code == "steering_source_unavailable"


@pytest.mark.parametrize("task_status", ["reserved", "running", "uncertain"])
def test_dismissal_fence_survives_until_exact_issued_sibling_settlement(task_status):
    root = run_record()
    sibling = {"task_id": "real-sibling", "status": task_status}
    root["activations"] = [{"id": "sibling", "node_id": "work", "role": "node", "tasks": [sibling]}]
    episode = {"episode_id": "episode", "hold": True, "state": "discussing"}
    root["steering"]["episodes"] = [episode]
    root["steering"]["turns"] = [{"turn_id": "turn", "episode_id": "episode", "state": "settled"}]
    storage = store_for(root)
    o.finish_dismissal(storage, root, episode)
    assert episode["state"] == "dismiss_pending_settlement"
    assert episode["hold"] is True
    assert root["status"] == "running"
    assert sibling["status"] == task_status
    sibling["status"] = "completed"
    def update(mutate, event, detail=None):
        mutate(root)
        return root
    supervisor = SimpleNamespace(store=storage, run=lambda **kwargs: root, update=update)
    o.settle_dismissals(supervisor)
    assert episode["state"] == "dismissed"
    assert episode["hold"] is False
    assert root["status"] == "paused"
    assert sibling["status"] == "completed"
