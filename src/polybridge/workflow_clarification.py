"""Durable worker questions and orchestrator answers within one node execution."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import time
import uuid
from typing import Any


def _modules():
    from . import workflows, workflow_delegation
    return workflows, workflow_delegation


def question_record(run: dict[str, Any], execution_id: str, question_id: str) -> dict[str, Any]:
    return next(q for a in run["activations"] if a["id"] == execution_id for q in a["questions"] if q["question_id"] == question_id)


def record_question(supervisor: Any, node: dict[str, Any], token: dict[str, Any], activation_id: str, value: dict[str, Any], outcome: dict[str, Any]) -> str | None:
    """Persist the request once; its completed harness turn is not a settled node."""
    w, _ = _modules()
    run = supervisor.run()
    activation = next(a for a in run["activations"] if a["id"] == activation_id)
    origin = next((t for t in reversed(activation["tasks"]) if t.get("status") == "completed"), None)
    if origin is None:
        supervisor.attention("Asking worker has no positively observed completed turn")
        return None
    existing = next((q for q in activation.get("questions", []) if q["origin_task_id"] == origin["task_id"]), None)
    if existing:
        return existing["question_id"]
    if len(activation.get("questions", [])) >= node.get("max_context_questions", 10):
        def exhausted(r: dict[str, Any]) -> None:
            r.update(status="failed", failure_reason=f"Context question limit reached for {node['id']}")
            a = next(a for a in r["activations"] if a["id"] == activation_id)
            a.update(status="failed", raw_output=outcome.get("summary", ""), node_result={"status": "failed", "result": {"failure_kind": "question_limit", "reason": r["failure_reason"]}, "evidence": value["evidence"]})
            next(t for t in r["pending"] if t["id"] == token["id"]).update(execution_complete=True, result=copy.deepcopy(a["node_result"]))
        supervisor.update(exhausted, "question_limit_reached")
        return None
    question_id = uuid.uuid4().hex
    question = {"question_id": question_id, "question": value["result"]["question"], "context": copy.deepcopy(value["result"].get("context", "")), "progress": copy.deepcopy(value["result"]), "evidence": copy.deepcopy(value["evidence"]), "origin_task_id": origin["task_id"], "origin_candidate": copy.deepcopy(origin["candidate"]), "status": "waiting", "decision_id": uuid.uuid4().hex, "decision_attempts": 0, "inspection_count": 0, "answer_delivery_state": "waiting", "created_at": time.time(), "raw_output": outcome.get("summary", "")}
    def record(r: dict[str, Any]) -> None:
        a = next(a for a in r["activations"] if a["id"] == activation_id)
        a.setdefault("questions", []).append(question)
        a.update(status="waiting_for_answer", pending_question_id=question_id)
        current = next(t for t in r["pending"] if t["id"] == token["id"])
        current["question_id"] = question_id
        current.pop("recovered_result", None)
        current.pop("recovered_failed_result", None)
    supervisor.update(record, "worker_question", {"execution_id": activation_id, "question_id": question_id})
    return question_id


def compatible_origin(run: dict[str, Any], node: dict[str, Any], activation: dict[str, Any], question: dict[str, Any], *, root: Path | None = None) -> dict[str, Any]:
    w, _ = _modules()
    source = next((t for t in activation["tasks"] if t["task_id"] == question["origin_task_id"]), None)
    network = False if run.get("network") is False else node.get("network", run.get("network"))
    if source is None or source["status"] != "completed" or not source.get("result", {}).get("session_id") or source.get("repo_path") != run["repo_path"] or source.get("freedom") != w.run_effective_freedom(run, node) or source.get("network") != network:
        raise w.WorkflowError("Question has no confirmed compatible session")
    if root is not None:
        from . import store as task_store
        record = task_store.read(root / "tasks", source["task_id"])
        if record is None or record.status != "completed" or record.session_id != source["result"]["session_id"] or record.repo_path != run["repo_path"] or record.freedom != w.run_effective_freedom(run, node) or record.network != network or record.backend != source["candidate"]["backend"]:
            raise w.WorkflowError("Question has no retained compatible session; choose Fresh")
    return source


async def answer_question(supervisor: Any, node: dict[str, Any], token: dict[str, Any], activation_id: str, question_id: str) -> bool:
    """Question decisions serialize with routing decisions; input releases the lock."""
    w, d = _modules()
    async with supervisor.decision_lock:
        while True:
            run = supervisor.run()
            activation = next(a for a in run["activations"] if a["id"] == activation_id)
            question = question_record(run, activation_id, question_id)
            if not supervisor.tree.tree_running(run):
                return False
            if question["status"] == "answered" and question["answer_delivery_state"] not in {"not_started", "requires_fresh"}:
                return True
            if question.get("decision_attempts", 0) >= run["definition"].get("max_decision_attempts", 3):
                supervisor.update(lambda r: d.exhaust_decision(r, question, clarification=True), "question_decision_exhausted")
                return False
            context_token = {**token, "decision_id": question["decision_id"], "input_result_refs": activation.get("input_result_refs", []), "inspection_count": question.get("inspection_count", 0), "inspection_results": question.get("inspection_results", [])}
            context_token.pop("execution_activation_id", None)
            context = d.decision_context(run, node, context_token, False, root=supervisor.store.root)
            try:
                source = compatible_origin(run, node, activation, question, root=supervisor.store.root)
                answer_sessions = [{"task_id": source["task_id"], "candidate": source["candidate"], "session_id": source["result"]["session_id"]}]
            except w.WorkflowError:
                answer_sessions = []
            context.update(current_stage={"node_id": node["id"], "phase": "clarification", "execution_id": activation_id}, assignment=activation["assignment_prompt"], worker_question={**{k: question[k] for k in ("question_id", "question", "origin_task_id", "answer_delivery_state")}, "context": w._bounded(question["context"], 8000), "progress": w._bounded(question["progress"], 8000), "evidence": w._bounded(question["evidence"], 8000), "execution_id": activation_id}, valid_continuations=[], valid_actions=["answer", "inspect", "failed", "needs_input"], answer_sessions=answer_sessions)
            base = {"decision_id": question["decision_id"], "reason": "Explain the decision"}
            examples = {
                "answer": {**base, "action": "answer", "question_id": question_id, "answer": "Focused answer", "session_mode": "resume" if answer_sessions else "fresh"},
                "inspect": {**base, "action": "inspect", "requests": [{"execution_id": "settled execution ID", "view": "result"}]},
                "failed": {**base, "action": "failed"},
                "needs_input": {**base, "action": "needs_input", "question": "Question for the caller"},
            }
            prompt = ("You are the workflow orchestrator. A worker needs clarification. Answer from the workflow objective, node instructions and evidence, or ask the caller. Polybridge owns state and dispatch; ordinary tools remain available under your access. Return ONLY one JSON object using the action-specific examples; omit fields belonging to other actions. Answer resumes the confirmed asking session when answer_sessions is nonempty; otherwise explicitly choose fresh. Fresh reconstructs only the assignment and observed progress. Inspect one settled execution page through final JSON requests. Examples:\n" + json.dumps(examples) + "\nContext:\n" + json.dumps(context))
            if question.get("decision_error"):
                prompt += "\nCorrection: " + question["decision_error"]
            orchestration = supervisor._activation(node["id"], "orchestrator", copy.deepcopy(token))
            supervisor.update(lambda r: (question_record(r, activation_id, question_id).update(decision_attempts=question.get("decision_attempts", 0) + 1), next(a for a in r["activations"] if a["id"] == orchestration["id"]).update(decision_id=question["decision_id"], question_id=question_id)), "question_decision_reserved")
            outcome = await supervisor._dispatch({"id": "orchestrator", "title": "Clarification"}, prompt, "orchestrator", orchestration)
            if not outcome:
                supervisor.update(lambda r: next(a for a in r["activations"] if a["id"] == orchestration["id"]).update(status="failed"), "question_decision_interrupted")
                return False
            raw = outcome.get("summary") or ""
            try:
                decision = d.parse_contract(raw, guided=run.get("runner_policy") == "guided")
                decision, protocol_warnings = d.normalize_decision(run, node, context_token, decision, clarification=True, root=supervisor.store.root)
                if decision.get("action") == "inspect":
                    d.inspect_decision(supervisor, decision, question, orchestration["id"], question_id=question_id, execution_id=activation_id, protocol_warnings=protocol_warnings)
                    continue
                if set(decision) - {"decision_id", "action", "reason", "question_id", "answer", "session_mode", "requests", "question"}:
                    raise w.WorkflowError("Unexpected clarification fields: " + ", ".join(sorted(set(decision) - {"decision_id", "action", "reason", "question_id", "answer", "session_mode", "requests", "question"})))
                action = decision.get("action")
                if decision.get("decision_id") != question["decision_id"] or action not in {"answer", "failed", "needs_input"} or not isinstance(decision.get("reason"), str) or not decision["reason"].strip() or decision.get("next") or decision.get("task_updates"):
                    raise w.WorkflowError("Invalid clarification decision")
                if action == "answer":
                    if decision.get("question_id") != question_id or not isinstance(decision.get("answer"), str) or not decision["answer"].strip():
                        raise w.WorkflowError("Answer requires this question_id and a nonempty answer")
                    mode = decision.get("session_mode", "resume")
                    if mode not in {"fresh", "resume"}:
                        raise w.WorkflowError("Answer session_mode must be fresh or resume")
                    if question.get("answer_delivery_state") == "requires_fresh" and mode != "fresh":
                        raise w.WorkflowError("Unavailable question session requires an explicit Fresh answer decision")
                    if mode == "resume":
                        compatible_origin(supervisor.run(), node, activation, question, root=supervisor.store.root)
                elif action == "needs_input" and (not isinstance(decision.get("question"), str) or not decision["question"].strip()):
                    raise w.WorkflowError("needs_input requires a nonempty question")
                def accept(r: dict[str, Any]) -> None:
                    a = next(a for a in r["activations"] if a["id"] == orchestration["id"])
                    a.update(status="completed", raw_output=raw, protocol_warnings=protocol_warnings)
                    if r["status"] != "running":
                        a["decision_ignored"] = "Human control changed run state before answer acceptance"
                        return
                    if not supervisor.tree.tree_running(r):
                        a["decision_ignored"] = "Workflow tree stopped running before answer acceptance"
                        return
                    q = question_record(r, activation_id, question_id)
                    r["decisions"].append({**decision, "node_id": node["id"], "activation_id": orchestration["id"], "execution_id": activation_id, "protocol_warnings": protocol_warnings})
                    if action == "answer":
                        q.update(status="answered", answer=decision["answer"], session_mode=decision.get("session_mode", "resume"), answer_delivery_state="pending", answered_at=time.time())
                    elif action == "failed":
                        r.update(status="failed", failure_reason=decision["reason"], failed_decision_id=q["decision_id"])
                    else:
                        r.update(status="needs_input", input_question=decision["question"], input_decision_id=q["decision_id"], attention_reason=decision["reason"])
                accepted = supervisor.update(accept, "question_decision_accepted", decision)
                return accepted["status"] == "running" and action == "answer"
            except (ValueError, TypeError, AttributeError) as exc:
                def reject(r: dict[str, Any]) -> None:
                    diagnostic = {"decision_id": question["decision_id"], "question_id": question_id, "attempt": question.get("decision_attempts", 0), "category": "clarification_validation", "error": str(exc)}
                    next(a for a in r["activations"] if a["id"] == orchestration["id"]).update(status="failed", raw_output=raw, result_error=str(exc), decision_diagnostic=diagnostic)
                    r.setdefault("decision_errors", []).append(diagnostic)
                    if r["status"] == "running" and supervisor.tree.tree_running(r):
                        question_record(r, activation_id, question_id)["decision_error"] = str(exc)
                supervisor.update(reject, "invalid_question_decision", str(exc))


async def continue_worker(supervisor: Any, node: dict[str, Any], token: dict[str, Any], activation_id: str, question_id: str) -> dict[str, Any] | None:
    w, d = _modules()
    if not await answer_question(supervisor, node, token, activation_id, question_id):
        return None
    run = supervisor.run()
    activation = next(a for a in run["activations"] if a["id"] == activation_id)
    question = question_record(run, activation_id, question_id)
    if question.get("answer_delivery_state") in {"reserved", "running", "uncertain"}:
        supervisor.attention("Answer delivery requires reconciliation; it was not replayed")
        return None
    mode = question.get("session_mode", "resume")
    source = compatible_origin(run, node, activation, question, root=supervisor.store.root) if mode == "resume" else None
    def prepare(r: dict[str, Any]) -> None:
        a = next(a for a in r["activations"] if a["id"] == activation_id)
        a.update(status="running", resume_question_id=question_id, turn_prompt=question["answer"])
        if source:
            a["resume_task_id"] = source["task_id"]
        else:
            a.pop("resume_task_id", None)
        next(t for t in r["pending"] if t["id"] == token["id"]).pop("recovered_result", None)
    supervisor.update(prepare, "answer_prepared", {"question_id": question_id})
    activation = next(a for a in supervisor.run()["activations"] if a["id"] == activation_id)
    history = [{"question_id": q["question_id"], "question": q["question"], "answer": q.get("answer"), "observed_progress": w._bounded(q.get("progress", {})), "evidence": w._bounded(q.get("evidence", []))} for q in activation.get("questions", [])]
    prompt = d.worker_prompt(run, node, token, root=supervisor.store.root) + "\nClarification history and observed progress (continue without repeating completed work):\n" + json.dumps(history) + "\nAnswer to your latest question:\n" + question["answer"]
    dispatch_node = {**node, "session_mode": mode}
    return await supervisor._dispatch(dispatch_node, prompt, "node", activation)
