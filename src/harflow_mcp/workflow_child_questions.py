"""Durable parent clarification checkpoints for ordinary child questions."""
from __future__ import annotations

import copy
import time
import uuid
from typing import Any


def caller_authority_required(run: dict[str, Any]) -> bool:
    """Check the same structured recovery evidence used by caller controls."""
    from .workflow_delegation import caller_decision_block, unresolved_required
    if any(run.get(key) for key in ("orchestrator_usage_recovery", "worker_usage_recovery", "optional_review_skip_available", "attention_source", "goal_revision_request", "permission_request")):
        return True
    for token in run.get("pending", []):
        if token.get("decision_id") != run.get("input_decision_id"):
            continue
        if any(caller_decision_block(execution) for execution in unresolved_required(run, token)):
            return True
        execution = next((a for a in run.get("activations", []) if a["id"] == token.get("execution_activation_id")), {})
        result = execution.get("node_result", {}).get("result", {})
        if result.get("failure_kind") in {"permission", "authority", "uncertain", "usage_limit"} or any(t.get("result", {}).get("permission_denials") for t in execution.get("tasks", [])):
            return True
    return False


def protected_input(storage: Any, child: dict[str, Any]) -> bool:
    """Parent prose cannot grant caller-only recovery or permission authority."""
    seen: set[str] = set()
    current = child
    while True:
        rid = current["workflow_run_id"]
        if rid in seen or len(seen) >= 8:
            return True
        seen.add(rid)
        if caller_authority_required(current):
            return True
        target = (current.get("input_source") or {}).get("workflow_run_id")
        if target in {None, rid}:
            return False
        try:
            from .workflow_inspection import _authorized_linked_run
            current = _authorized_linked_run(current, storage.root, target)
        except (OSError, ValueError, KeyError, TypeError):
            return True


def prepare_question(supervisor: Any, node: dict[str, Any], token: dict[str, Any], activation_id: str, child: dict[str, Any]) -> str:
    """Bind the parent question once to the child's exact immutable input ID."""
    from .workflows import WorkflowError
    identifier = child["input_decision_id"]
    question_id = uuid.uuid4().hex
    def record(run: dict[str, Any]) -> None:
        nonlocal question_id
        activation = next(a for a in run["activations"] if a["id"] == activation_id)
        existing = next((q for q in activation.get("questions", []) if (q.get("child_question") or {}).get("workflow_run_id") == child["workflow_run_id"] and (q.get("child_question") or {}).get("decision_id") == identifier), None)
        if existing:
            if existing["question"] != child["input_question"]:
                raise WorkflowError("Child question text changed without changing its input identity")
            question_id = existing["question_id"]
            return
        question = {"question_id": question_id, "question": child["input_question"], "context": {"workflow_run_id": child["workflow_run_id"], "name": child.get("name", ""), "decision_id": identifier}, "progress": {}, "evidence": [], "origin_task_id": "child:" + child["workflow_run_id"], "origin_candidate": {}, "status": "waiting", "decision_id": uuid.uuid4().hex, "decision_attempts": 0, "inspection_count": 0, "answer_delivery_state": "waiting", "created_at": time.time(), "child_question": {"workflow_run_id": child["workflow_run_id"], "decision_id": identifier, "question": child["input_question"]}}
        activation.setdefault("questions", []).append(question)
        activation["pending_question_id"] = question_id
        activation.setdefault("assignment_prompt", (activation.get("invocation") or {}).get("assignment", node.get("instructions", "")))
    supervisor.update(record, "child_question_checkpoint", {"child_workflow_run_id": child["workflow_run_id"], "input_decision_id": identifier})
    return question_id


def deliver_answer(supervisor: Any, activation_id: str, question_id: str) -> None:
    """Persist delivery before child mutation and acknowledge only its exact receipt."""
    from .workflow_clarification import question_record
    from .workflows import WorkflowError
    question = question_record(supervisor.run(), activation_id, question_id)
    source = question["child_question"]
    if question.get("answer_delivery_state") == "delivered":
        return
    child = supervisor.store.get_run(source["workflow_run_id"])
    delivery = question.get("child_answer_delivery")
    if delivery is None:
        if child.get("status") != "needs_input" or child.get("input_decision_id") != source["decision_id"] or child.get("input_question") != source["question"] or protected_input(supervisor.store, child):
            raise WorkflowError("Child input is no longer an ordinary answerable question")
        delivery = {"id": uuid.uuid4().hex, "payload": {"source_id": child["workflow_run_id"], "action": "resume", "instructions": question["answer"], "additional_attempts": 0, "decision_id": source["decision_id"], "allow_optional_review_skip": False, "interaction_owner": child.get("interaction_owner", "caller"), "source": copy.deepcopy(source)}}
        supervisor.update(lambda r: question_record(r, activation_id, question_id).update(child_answer_delivery=copy.deepcopy(delivery)), "child_answer_delivery_prepared", question_id)
    if child.get("forwarded_delivery_receipt") != delivery:
        if child.get("status") != "needs_input" or child.get("input_decision_id") != source["decision_id"] or child.get("input_question") != source["question"] or protected_input(supervisor.store, child):
            raise WorkflowError("Child input requires caller authority or changed before answer delivery")
        if not supervisor.tree.tree_running(supervisor.run()):
            raise WorkflowError("Parent stopped before child answer delivery")
        supervisor.store.control(child["workflow_run_id"], "resume", instructions=delivery["payload"]["instructions"], decision_id=source["decision_id"], additional_attempts=0, allow_optional_review_skip=False, interaction_owner=delivery["payload"]["interaction_owner"], _internal=True, _delivery=delivery, _ordinary_answer=True)
    observed = supervisor.store.get_run(child["workflow_run_id"])
    if observed.get("forwarded_delivery_receipt") != delivery:
        raise WorkflowError("Child answer has no matching durable receipt")
    def acknowledge(run: dict[str, Any]) -> None:
        q = question_record(run, activation_id, question_id)
        q.update(answer_delivery_state="delivered", delivered_at=time.time())
        activation = next(a for a in run["activations"] if a["id"] == activation_id)
        if activation.get("pending_question_id") == question_id:
            activation.pop("pending_question_id", None)
        activation["status"] = "running"
        run.pop("suspended_via_root", None)
    supervisor.update(acknowledge, "child_answer_delivered", question_id)


def recover_answers(supervisor: Any, activation_id: str) -> None:
    activation = next(a for a in supervisor.run()["activations"] if a["id"] == activation_id)
    for q in activation.get("questions", []):
        if q.get("child_question") and q.get("status") == "answered" and q.get("answer_delivery_state") != "delivered":
            deliver_answer(supervisor, activation_id, q["question_id"])


async def handle(supervisor: Any, node: dict[str, Any], token: dict[str, Any], activation_id: str, child: dict[str, Any]) -> str:
    """Return answered, forwarded or suspended; no-call runs retain old forwarding."""
    from .workflow_on_demand import catalog
    from .workflow_clarification import answer_question, question_record
    if child.get("orchestrator_mode") != "child" or not catalog(supervisor.store, supervisor.run()) or protected_input(supervisor.store, child):
        return "forwarded"
    question_id = prepare_question(supervisor, node, token, activation_id, child)
    question = question_record(supervisor.run(), activation_id, question_id)
    if question.get("status") == "answered" and question.get("answer_delivery_state") == "delivered":
        source = question["child_question"]
        supervisor.attention(
            "Child workflow " + source["workflow_run_id"]
            + " requested input again with already answered decision " + source["decision_id"]
            + "; reconcile the reused input identity before continuing. The prior answer receipt is preserved."
        )
        return "suspended"
    if question.get("status") == "forwarded":
        return "forwarded"
    if not await answer_question(supervisor, node, token, activation_id, question_id):
        question = question_record(supervisor.run(), activation_id, question_id)
        return "forwarded" if question.get("status") == "forwarded" else "suspended"
    deliver_answer(supervisor, activation_id, question_id)
    return "answered"
