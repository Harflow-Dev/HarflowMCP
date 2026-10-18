import copy

import pytest

from polybridge import workflows
from polybridge.workflow_migration import migrate_orchestrator_instructions
from polybridge.workflow_on_demand import (
    cancel_reservations,
    catalog,
    context,
    reconcile,
    reflect_reservation,
    reflect_result,
    settle,
    validate_action,
    validate_nodes,
)


def base_definition():
    return {
        "name": "specialists",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "plan", "type": "agent", "instructions": "Plan", "agent": {"backend": "codex"}},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "plan"},
            {"id": "finish", "source": "plan", "target": "end"},
        ],
    }


def specialist(node_id="research", **overrides):
    value = {
        "id": node_id,
        "title": "API researcher",
        "instructions": "Return evidence from the assigned API.",
        "call_condition": "Call when API behaviour needs verification.",
        "agent": {"backend": "codex"},
        "freedom": "read_only",
        "network": None,
        "session_mode": "agent_decides",
        "execution_mode": "headless",
    }
    value.update(overrides)
    return value


def test_migration_moves_start_prompt_once_and_preserves_explicit_instructions():
    legacy = base_definition()
    legacy["nodes"][0]["prompt"] = "Legacy purpose"
    migrated = migrate_orchestrator_instructions(legacy)
    assert migrated["orchestrator"]["instructions"] == "Legacy purpose"
    assert "prompt" not in migrated["nodes"][0]
    assert migrate_orchestrator_instructions(migrated) == migrated

    explicit = copy.deepcopy(legacy)
    explicit["orchestrator"]["instructions"] = "Explicit purpose"
    assert migrate_orchestrator_instructions(explicit)["orchestrator"]["instructions"] == "Explicit purpose"


def test_definition_round_trip_keeps_condition_separate_from_worker_instructions():
    definition = base_definition()
    definition["on_demand_nodes"] = [specialist()]
    normalized = workflows.validate_definition(definition)
    node = normalized["on_demand_nodes"][0]
    assert node["instructions"] != node["call_condition"]
    assert node["call_condition"] == "Call when API behaviour needs verification."
    assert normalized["max_on_demand_calls"] == 5


@pytest.mark.parametrize("value", [0, 51, -1, True, "5"])
def test_max_on_demand_calls_is_strictly_bounded(value):
    definition = base_definition()
    definition["max_on_demand_calls"] = value
    with pytest.raises(workflows.WorkflowError):
        workflows.validate_definition(definition)


def test_on_demand_validation_rejects_edges_collisions_and_empty_worker_guidance():
    with pytest.raises(workflows.WorkflowError, match="Duplicate"):
        validate_nodes([specialist("plan")], {"start", "plan", "end"})
    with pytest.raises(workflows.WorkflowError, match="nonempty"):
        validate_nodes([specialist(instructions=" ")], {"start", "plan", "end"})
    definition = base_definition()
    definition["on_demand_nodes"] = [specialist()]
    definition["connections"].append({"id": "bad", "source": "plan", "target": "research"})
    with pytest.raises(workflows.WorkflowError):
        workflows.validate_definition(definition)


def test_call_condition_must_be_text_but_may_be_empty():
    assert validate_nodes([specialist(call_condition="")], set())[0]["call_condition"] == ""
    with pytest.raises(workflows.WorkflowError, match="text"):
        validate_nodes([specialist(call_condition=None)], set())


class MemoryStorage:
    def __init__(self, runs):
        self.runs = runs
        self.root = __import__("pathlib").Path(".")

    def get_run(self, run_id):
        return copy.deepcopy(self.runs[run_id])

    def update_run(self, run_id, callback, *_args):
        value = copy.deepcopy(self.runs[run_id])
        callback(value)
        self.runs[run_id] = value
        return copy.deepcopy(value)


def run_fixture(run_id="owner"):
    definition = workflows.validate_definition(base_definition() | {"on_demand_nodes": [specialist()]})
    return {
        "workflow_run_id": run_id,
        "status": "running",
        "definition": definition,
        "activations": [],
        "pending": [],
        "on_demand_calls": [],
        "repo_path": ".",
        "network": None,
    }


def test_catalog_qualifies_ids_and_context_exposes_remaining_allowance():
    run = run_fixture()
    storage = MemoryStorage({"owner": run})
    entry = catalog(storage, run)[0]
    assert entry["node_id"] == "owner:research"
    assert entry["local_node_id"] == "research"
    assert entry["call_condition"] != entry["instructions"]
    checkpoint = {"decision_id": "d1", "on_demand_count": 2, "on_demand_results": []}
    prompt_context = context(storage, run, checkpoint)
    assert prompt_context["on_demand_calls_remaining"] == 3
    assert "call_condition" in prompt_context["on_demand_nodes"][0]


def test_current_children_share_catalog_with_scope_qualified_duplicate_local_ids():
    owner = run_fixture("owner")
    child = run_fixture("child")
    child["orchestrator_mode"] = "current"
    child["orchestrator_session_owner_run_id"] = "owner"
    child["parent_link"] = {"workflow_run_id": "owner", "execution_id": "invocation-1"}
    owner["activations"] = [{"id": "invocation-1", "invocation": {"child_workflow_run_id": "child"}}]
    child["activations"] = [{"id": "invocation-1", "status": "running"}]
    child["activations"] = [{"id": "invocation-1", "status": "running"}]
    storage = MemoryStorage({"owner": owner, "child": child})
    entries = catalog(storage, owner)
    assert {entry["node_id"] for entry in entries} == {"owner:research", "child:research"}


def test_child_mode_is_not_visible_to_parent_owner_scope():
    owner = run_fixture("owner")
    child = run_fixture("child")
    child["orchestrator_mode"] = "child"
    child["orchestrator_session_owner_run_id"] = "child"
    child["parent_link"] = {"workflow_run_id": "owner", "execution_id": "invocation-1"}
    owner["activations"] = [{"id": "invocation-1", "invocation": {"child_workflow_run_id": "child"}}]
    storage = MemoryStorage({"owner": owner, "child": child})
    assert [entry["node_id"] for entry in catalog(storage, owner)] == ["owner:research"]


@pytest.mark.parametrize("dispatch_state", ["preparing", "checkpoint_applied", "activation_reserved"])
def test_reconcile_after_owner_origin_write_crash_is_exactly_once(dispatch_state):
    owner = run_fixture("owner")
    child = run_fixture("child")
    child["orchestrator_mode"] = "current"
    child["orchestrator_session_owner_run_id"] = "owner"
    child["parent_link"] = {"workflow_run_id": "owner", "execution_id": "invocation-1"}
    owner["activations"] = [{"id": "invocation-1", "invocation": {"child_workflow_run_id": "child"}}]
    child["activations"] = [{"id": "invocation-1", "status": "running"}]
    child["pending"] = [{"id": "token-1", "decision_id": "d1", "on_demand_count": 0, "decision_attempts": 1}]
    owner["on_demand_calls"] = [{
        "id": "call-1", "status": "reserved", "dispatch_state": dispatch_state, "execution_id": "execution-1",
        "before_count": 0, "orchestration_activation_id": "invocation-1",
        "action": {"action": "call_on_demand", "decision_id": "d1", "node_id": "child:research", "assignment": "Verify", "reason": "Evidence"},
        "checkpoint": {"workflow_run_id": "child", "decision_id": "d1", "token_id": "token-1", "question_id": None},
        "source_workflow_run_id": "child", "node_id": "child:research", "local_node_id": "research", "title": "API researcher", "assignment": "Verify", "reason": "Evidence",
    }]
    owner["activations"].append({"id": "execution-1", "tasks": []})
    storage = MemoryStorage({"owner": owner, "child": child})
    reconcile(storage, child)
    reconcile(storage, child)
    assert storage.runs["owner"]["on_demand_calls"][0]["status"] == "uncertain"
    assert storage.runs["child"]["pending"][0]["on_demand_count"] == 1
    assert storage.runs["child"]["pending"][0]["decision_attempts"] == 0
    assert storage.runs["child"]["pending"][0]["on_demand_applied_call_ids"] == ["call-1"]


def test_validate_action_requires_issued_scope_id_and_nonempty_assignment():
    run = run_fixture()
    storage = MemoryStorage({"owner": run})
    checkpoint = {"decision_id": "d1", "on_demand_count": 0}
    valid = {"action": "call_on_demand", "decision_id": "d1", "node_id": "owner:research", "assignment": "Verify API", "reason": "Need evidence"}
    entry, mode, reference = validate_action(storage, run, checkpoint, valid)
    assert entry["node_id"] == "owner:research"
    assert mode == "fresh"
    assert reference is None
    with pytest.raises(workflows.WorkflowError):
        validate_action(storage, run, checkpoint, {**valid, "node_id": "other:research"})
    with pytest.raises(workflows.WorkflowError):
        validate_action(storage, run, checkpoint, {**valid, "assignment": " "})


def test_reservation_refunds_decision_attempt_once_and_result_is_idempotent():
    run = run_fixture()
    run["pending"] = [{"id": "token-1", "decision_id": "d1", "on_demand_count": 0, "decision_attempts": 1}]
    run["activations"] = [{"id": "orchestration-1", "status": "running"}]
    call = {
        "id": "call-1",
        "before_count": 0,
        "checkpoint": {"workflow_run_id": "owner", "decision_id": "d1", "token_id": "token-1", "question_id": None},
        "orchestration_activation_id": "orchestration-1",
        "action": {"action": "call_on_demand", "decision_id": "d1", "node_id": "owner:research", "assignment": "Verify", "reason": "Evidence"},
        "status": "succeeded",
        "result": {"status": "succeeded", "result": {"summary": "verified"}},
        "result_ref": {"workflow_run_id": "owner", "execution_id": "execution-1"},
    }
    reflect_reservation(run, call)
    reflect_reservation(run, call)
    assert run["pending"][0]["on_demand_count"] == 1
    assert run["pending"][0]["decision_attempts"] == 0
    assert run["activations"][0]["accepted_on_demand_call_id"] == "call-1"
    reflect_result(run, call)
    reflect_result(run, call)
    assert [item["id"] for item in run["pending"][0]["on_demand_results"]] == ["call-1"]


def test_settle_records_failure_and_cancel_reservations_never_relaunches():
    run = run_fixture()
    run["pending"] = [{"id": "token-1", "decision_id": "d1", "on_demand_count": 0, "decision_attempts": 0}]
    run["activations"] = [{
        "id": "execution-1", "node_id": "owner:research", "role": "node", "status": "running",
        "tasks": [], "on_demand_node": {"id": "owner:research", "type": "agent", "instructions": "worker", "agent": {"backend": "codex"}},
    }]
    run["on_demand_calls"] = [{
        "id": "call-1", "status": "running", "execution_id": "execution-1", "checkpoint": {"workflow_run_id": "owner", "decision_id": "d1", "token_id": "token-1", "question_id": None},
        "source_workflow_run_id": "owner", "node_id": "owner:research", "title": "API researcher", "assignment": "Verify", "reason": "Evidence",
    }]
    storage = MemoryStorage({"owner": run})
    settle(storage, "owner", "call-1", {"status": "failed", "summary": "permission denied"})
    failed = storage.runs["owner"]["on_demand_calls"][0]
    assert failed["status"] == "failed"
    assert "permission denied" in failed["failure"]
    cancel_reservations(storage.runs["owner"])
    assert storage.runs["owner"]["on_demand_calls"][0]["status"] == "failed"


def test_reconcile_marks_interrupted_reservation_uncertain_without_dispatch():
    run = run_fixture()
    run["pending"] = [{"id": "token-1", "decision_id": "d1", "on_demand_count": 0, "decision_attempts": 0}]
    run["on_demand_calls"] = [{
        "id": "call-1", "status": "running", "dispatch_state": "checkpoint_applied", "execution_id": "execution-1",
        "before_count": 0, "orchestration_activation_id": "execution-1",
        "action": {"action": "call_on_demand", "decision_id": "d1", "node_id": "owner:research", "assignment": "Verify", "reason": "Evidence"},
        "checkpoint": {"workflow_run_id": "owner", "decision_id": "d1", "token_id": "token-1", "question_id": None},
        "source_workflow_run_id": "owner", "node_id": "owner:research", "title": "API researcher", "assignment": "Verify", "reason": "Evidence",
    }]
    run["activations"] = [{"id": "execution-1", "tasks": []}]
    storage = MemoryStorage({"owner": run})
    reconcile(storage, run)
    assert storage.runs["owner"]["on_demand_calls"][0]["status"] == "uncertain"
    assert storage.runs["owner"]["status"] == "needs_attention"
    assert "no replacement dispatched" in storage.runs["owner"]["attention_reason"]
