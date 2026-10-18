"""Crash recovery and idempotent receipts for on-demand calls."""
import copy

from polybridge.workflow_child_questions import recover_answers
from polybridge.workflow_on_demand import reconcile
from test_workflow_on_demand_contract import MemoryStorage, run_fixture


def _current_child_fixture():
    owner = run_fixture("owner")
    child = run_fixture("child")
    child["orchestrator_mode"] = "current"
    child["orchestrator_session_owner_run_id"] = "owner"
    child["parent_link"] = {"workflow_run_id": "owner", "execution_id": "invocation-1"}
    owner["activations"] = [{"id": "invocation-1", "invocation": {"child_workflow_run_id": "child"}}]
    child["activations"] = [{"id": "invocation-1", "status": "running"}]
    child["pending"] = [{"id": "token-1", "decision_id": "d1", "on_demand_count": 0, "decision_attempts": 1}]
    return owner, child


def _call(status="running", **extra):
    return {
        "id": "call-1", "status": status, "dispatch_state": "spawn_requested", "execution_id": "execution-1",
        "before_count": 0, "orchestration_activation_id": "invocation-1",
        "action": {"action": "call_on_demand", "decision_id": "d1", "node_id": "child:research", "assignment": "Verify", "reason": "Evidence"},
        "checkpoint": {"workflow_run_id": "child", "decision_id": "d1", "token_id": "token-1", "question_id": None},
        "source_workflow_run_id": "child", "node_id": "child:research", "local_node_id": "research", "title": "API researcher", "assignment": "Verify", "reason": "Evidence",
        **extra,
    }


def test_spawn_requested_unknown_receipt_becomes_attention_without_redispatch():
    owner, child = _current_child_fixture()
    owner["on_demand_calls"] = [_call()]
    owner["activations"].append({"id": "execution-1", "status": "running", "tasks": [{"task_id": "native", "status": "uncertain", "result": {"outcome_unknown": True}}]})
    storage = MemoryStorage({"owner": owner, "child": child})
    reconcile(storage, child)
    observed = storage.runs["owner"]["on_demand_calls"][0]
    assert observed["status"] == "uncertain"
    assert storage.runs["child"]["status"] == "needs_attention"
    assert len(storage.runs["owner"]["activations"]) == 2
    reconcile(storage, child)
    assert len(storage.runs["owner"]["activations"]) == 2
    assert storage.runs["owner"]["on_demand_calls"][0]["status"] == "uncertain"


def test_settled_owner_result_repairs_origin_once():
    owner, child = _current_child_fixture()
    owner["on_demand_calls"] = [_call(status="succeeded", dispatch_state="settled", result_delivered=False, result={"status": "succeeded", "result": {"summary": "Verified"}, "evidence": []})]
    owner["activations"].append({"id": "execution-1", "status": "completed", "tasks": []})
    storage = MemoryStorage({"owner": owner, "child": child})
    reconcile(storage, child)
    reconcile(storage, child)
    results = storage.runs["child"]["pending"][0]["on_demand_results"]
    assert [result["id"] for result in results] == ["call-1"]
    assert storage.runs["owner"]["on_demand_calls"][0]["result_delivered"] is True


class _AnswerStore:
    def __init__(self, parent, child):
        self.runs = {"parent": parent, "child": child}
        self.control_calls = 0

    def get_run(self, run_id):
        return copy.deepcopy(self.runs[run_id])


class _AnswerTree:
    @staticmethod
    def tree_running(run):
        return run["status"] == "running"


class _AnswerSupervisor:
    def __init__(self, parent, child):
        self._run = parent
        self.store = _AnswerStore(parent, child)
        self.tree = _AnswerTree()

    def run(self):
        return copy.deepcopy(self._run)

    def update(self, callback, *_args):
        callback(self._run)


def test_recover_answers_acknowledges_committed_child_receipt_without_second_control():
    parent = {"workflow_run_id": "parent", "status": "running", "activations": [{
        "id": "invocation-1", "status": "suspended", "pending_question_id": "question-1", "questions": [{
            "question_id": "question-1", "question": "Which format?", "status": "answered", "answer": "JSON",
            "answer_delivery_state": "prepared", "decision_id": "q-decision", "child_question": {"workflow_run_id": "child", "decision_id": "child-decision", "question": "Which format?"},
            "child_answer_delivery": {"id": "delivery-1", "payload": {"source_id": "child", "action": "resume", "instructions": "JSON", "decision_id": "child-decision"}},
        }],
    }]}
    delivery = parent["activations"][0]["questions"][0]["child_answer_delivery"]
    child = {"workflow_run_id": "child", "status": "running", "forwarded_delivery_receipt": delivery}
    supervisor = _AnswerSupervisor(parent, child)
    recover_answers(supervisor, "invocation-1")
    question = supervisor.run()["activations"][0]["questions"][0]
    assert question["answer_delivery_state"] == "delivered"
    assert question["status"] == "answered"
    assert supervisor.store.control_calls == 0

