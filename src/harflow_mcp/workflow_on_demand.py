"""Owner-scoped specialist calls and their durable checkpoint receipts.

The owner ledger authorizes execution. Origin records contain idempotent receipts,
not independent ledgers, so a crash between record writes cannot authorize a replay.
"""
from __future__ import annotations

import asyncio
import copy
import json
import time
import uuid
from pathlib import Path
from typing import Any

ACTIVE = {"reserved", "running", "uncertain"}
CALL_FIELDS = {"decision_id", "action", "node_id", "assignment", "reason", "session_mode", "resume_task_id", "context_ack"}


def validate_nodes(raw: Any, connected_ids: set[str], *, allow_incomplete: bool = False) -> list[dict[str, Any]]:
    from .workflows import WorkflowError, _candidate, _identifier, FREEDOMS
    if not isinstance(raw, list):
        raise WorkflowError("on_demand_nodes must be an array")
    result, ids = [], set(connected_ids)
    allowed = {"id", "title", "instructions", "call_condition", "agent", "freedom", "network", "session_mode", "execution_mode"}
    for item in raw:
        if not isinstance(item, dict) or set(item) - allowed:
            raise WorkflowError("On-demand nodes accept only id, title, instructions, call_condition, agent, freedom, network, session_mode and execution_mode")
        node = copy.deepcopy(item)
        nid = _identifier(node.get("id"))
        if nid in ids:
            raise WorkflowError(f"Duplicate node {nid}")
        ids.add(nid)
        node.setdefault("title", nid)
        node.setdefault("call_condition", "")
        if allow_incomplete:
            node.setdefault("instructions", "")
        if not isinstance(node.get("instructions"), str) or (not allow_incomplete and not node["instructions"].strip()):
            raise WorkflowError(f"On-demand instructions must be nonempty text: {nid}")
        if not isinstance(node["title"], str) or not isinstance(node["call_condition"], str):
            raise WorkflowError(f"On-demand title and call_condition must be text: {nid}")
        node["agent"] = _candidate(node.get("agent", {"backend": "codex"}))
        node.setdefault("freedom", "read_only")
        node.setdefault("session_mode", "agent_decides")
        node.setdefault("execution_mode", "headless")
        if not isinstance(node["freedom"], str) or node["freedom"] not in FREEDOMS or type(node.get("network")) not in {bool, type(None)}:
            raise WorkflowError(f"Invalid on-demand freedom or network: {nid}")
        if not isinstance(node["session_mode"], str) or node["session_mode"] not in {"agent_decides", "fresh", "resume"}:
            raise WorkflowError(f"Invalid on-demand session_mode: {nid}")
        if not isinstance(node["execution_mode"], str) or node["execution_mode"] not in {"headless", "prefer_subagent"}:
            raise WorkflowError(f"Invalid on-demand execution_mode: {nid}")
        result.append(node)
    return result


def owner_id(run: dict[str, Any]) -> str:
    return run.get("orchestrator_session_owner_run_id") or run["workflow_run_id"]


def enabled(storage: Any, run: dict[str, Any]) -> bool:
    """Historical no-call definitions need no owner-scope traversal."""
    def configured(value: dict[str, Any]) -> bool:
        return bool(value.get("definition", {}).get("on_demand_nodes") or any(entry.get("definition", {}).get("on_demand_nodes") for entry in (value.get("dependency_tree") or {}).get("workflows", {}).values()) or value.get("on_demand_calls"))
    if configured(run):
        return True
    owner = owner_id(run)
    return configured(storage.get_run(owner)) if owner != run["workflow_run_id"] else False


def scope_runs(storage: Any, run: dict[str, Any]) -> list[dict[str, Any]]:
    """Follow only positively linked Current invocations from the pinned owner."""
    from .workflows import WorkflowError
    try:
        owner = storage.get_run(owner_id(run))
    except FileNotFoundError:
        if run.get("parent_link") or run.get("orchestrator_session_owner_run_id"):
            raise
        owner = run
    found, queue, seen = [], [owner], set()
    while queue:
        current = queue.pop(0)
        rid = current["workflow_run_id"]
        if rid in seen:
            continue
        seen.add(rid)
        found.append(current)
        for activation in current.get("activations", []):
            child_id = (activation.get("invocation") or {}).get("child_workflow_run_id")
            if not child_id:
                continue
            child = storage.get_run(child_id)
            link = child.get("parent_link") or {}
            if link.get("workflow_run_id") != rid or link.get("execution_id") != activation["id"]:
                raise WorkflowError("On-demand scope has a mismatched child link")
            if child.get("orchestrator_mode") == "current" and owner_id(child) == owner["workflow_run_id"]:
                queue.append(child)
    if run["workflow_run_id"] not in seen:
        raise WorkflowError("Origin is outside its orchestrator owner scope")
    return found


def catalog(storage: Any, run: dict[str, Any]) -> list[dict[str, Any]]:
    if not enabled(storage, run):
        return []
    runs = scope_runs(storage, run)
    owner = runs[0]
    calls = owner.get("on_demand_calls", [])
    result = []
    for source in runs:
        for node in source["definition"].get("on_demand_nodes", []):
            identity = source["workflow_run_id"] + ":" + node["id"]
            history = [c for c in calls if c["node_id"] == identity]
            result.append({"node_id": identity, "source_workflow_run_id": source["workflow_run_id"], "local_node_id": node["id"], "title": node.get("title", node["id"]), "instructions": node["instructions"], "call_condition": node.get("call_condition", ""), "agent": copy.deepcopy(node["agent"]), "freedom": node.get("freedom", "read_only"), "network": node.get("network"), "effective_network": False if owner.get("network") is False or source.get("network") is False else node.get("network", source.get("network", owner.get("network"))), "session_mode": node.get("session_mode", "agent_decides"), "execution_mode": node.get("execution_mode", "headless"), "call_count": len(history), "active_call_id": next((c["id"] for c in reversed(history) if c["status"] in ACTIVE), None)})
    return result


def runtime_node(storage: Any, run: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
    from .workflows import run_effective_freedom
    source = next(r for r in scope_runs(storage, run) if r["workflow_run_id"] == entry["source_workflow_run_id"])
    node = next(n for n in source["definition"]["on_demand_nodes"] if n["id"] == entry["local_node_id"])
    owner = storage.get_run(owner_id(run))
    return {**copy.deepcopy(node), "id": entry["node_id"], "type": "agent", "role": "task", "optional": False, "max_attempts": 3, "max_context_questions": 10, "freedom": run_effective_freedom(source, node), "network": False if owner.get("network") is False or source.get("network") is False else node.get("network", source.get("network")), "_source_workflow_id": source["definition"].get("workflow_id"), "_local_node_id": node["id"]}


def sessions(storage: Any, run: dict[str, Any], entry: dict[str, Any]) -> list[dict[str, Any]]:
    from .workflow_delegation import available_sessions
    owner = storage.get_run(owner_id(run))
    return available_sessions(owner, runtime_node(storage, run, entry), root=storage.root)


def context(storage: Any, run: dict[str, Any], checkpoint: dict[str, Any]) -> dict[str, Any]:
    entries = catalog(storage, run)
    for entry in entries:
        entry["available_sessions"] = sessions(storage, run, entry)
    history = checkpoint.get("on_demand_results", [])
    return {"on_demand_nodes": entries, "on_demand_results": copy.deepcopy(history[-1:]), "on_demand_history": [{"id": c["id"], "node_id": c["node_id"], "status": c["status"], "result_ref": c.get("result_ref"), "summary": str(((c.get("result") or {}).get("result") or {}).get("summary", c.get("failure") or ""))[:2000]} for c in history[:-1]], "on_demand_calls_remaining": max(0, run["definition"].get("max_on_demand_calls", 5) - checkpoint.get("on_demand_count", 0)), "on_demand_guidance": "Return action call_on_demand with decision_id, issued node_id, nonempty assignment and reason. Optional session_mode fresh|resume and issued resume_task_id follow node session policy. Fixed Resume boots Fresh only without a retained compatible session. Agent decides must explicitly choose Fresh/Resume when a session is issued. Fresh forbids resume_task_id. Calls return to this checkpoint, do not route or update tasks, and consume only the separate on-demand allowance. call_condition is selection guidance; instructions are worker guidance."}


def projection(storage: Any, run: dict[str, Any]) -> dict[str, Any]:
    if not enabled(storage, run):
        return {"on_demand_nodes": [], "on_demand_calls": [], "on_demand_state": {"kind": "deciding", "node_id": None, "title": None}}
    entries = catalog(storage, run)
    owner = scope_runs(storage, run)[0]
    calls = [public_call(c) for c in owner.get("on_demand_calls", [])]
    for item in calls:
        activation = next((a for a in owner.get("activations", []) if a["id"] == item["execution_id"]), None)
        if activation:
            item["task_id"] = next((t["task_id"] for t in reversed(activation.get("tasks", [])) if t.get("execution_kind") != "native_subagent"), None)
    active = next((c for c in reversed(calls) if c["status"] in ACTIVE), None)
    question = next((q for a in run.get("activations", []) for q in a.get("questions", []) if q.get("status") == "waiting"), None)
    state = {"kind": "waiting" if active else "answering" if question else "deciding", "node_id": active["node_id"] if active else None, "title": active["title"] if active else None}
    if question and not active:
        activation = next(a for a in run["activations"] if question in a.get("questions", []))
        node = next((n for n in run["definition"]["nodes"] if n["id"] == activation["node_id"]), {})
        state.update(node_id=activation["node_id"], title=node.get("title", activation["node_id"]))
    return {"on_demand_nodes": entries, "on_demand_calls": calls, "on_demand_state": state}


def checkpoint_record(run: dict[str, Any], call: dict[str, Any]) -> dict[str, Any]:
    from .workflows import WorkflowError
    identity = call["checkpoint"]
    token = next((t for t in run.get("pending", []) if t["id"] == identity["token_id"]), None)
    if token is None:
        raise WorkflowError("On-demand origin token is missing")
    if identity.get("question_id"):
        target = next((q for a in run["activations"] for q in a.get("questions", []) if q["question_id"] == identity["question_id"]), None)
    else:
        target = token
    if target is None or target.get("decision_id") != identity["decision_id"]:
        raise WorkflowError("On-demand origin checkpoint changed")
    return target


def reflect_reservation(run: dict[str, Any], call: dict[str, Any]) -> None:
    from .workflows import WorkflowError
    checkpoint = checkpoint_record(run, call)
    applied = checkpoint.setdefault("on_demand_applied_call_ids", [])
    if call["id"] in applied:
        return
    if checkpoint.get("on_demand_count", 0) != call["before_count"]:
        raise WorkflowError("Contradictory on-demand checkpoint accounting")
    checkpoint.update(on_demand_count=call["before_count"] + 1, decision_attempts=max(0, checkpoint.get("decision_attempts", 0) - 1), pending_on_demand_call_id=call["id"])
    applied.append(call["id"])
    orchestration = next((a for a in run["activations"] if a["id"] == call["orchestration_activation_id"]), None)
    if orchestration is None:
        raise WorkflowError("On-demand authorizing orchestration is missing")
    orchestration.update(status="completed", accepted_on_demand_call_id=call["id"], decision=copy.deepcopy(call["action"]))


def reflect_result(run: dict[str, Any], call: dict[str, Any]) -> None:
    checkpoint = checkpoint_record(run, call)
    results = checkpoint.setdefault("on_demand_results", [])
    if not any(c["id"] == call["id"] for c in results):
        results.append(public_call(call))
    if checkpoint.get("pending_on_demand_call_id") == call["id"]:
        checkpoint.pop("pending_on_demand_call_id", None)


def public_call(call: dict[str, Any]) -> dict[str, Any]:
    fields = ("id", "call_number", "node_id", "source_workflow_run_id", "local_node_id", "title", "assignment", "reason", "checkpoint", "origin", "status", "started_at", "finished_at", "execution_id", "result_ref", "task_id", "result", "failure")
    return {k: copy.deepcopy(call.get(k)) for k in fields}


def validate_action(storage: Any, run: dict[str, Any], checkpoint: dict[str, Any], action: dict[str, Any]) -> tuple[dict[str, Any], str, str | None]:
    from .workflows import WorkflowError
    if set(action) - CALL_FIELDS or action.get("action") != "call_on_demand" or action.get("decision_id") != checkpoint.get("decision_id"):
        raise WorkflowError("Invalid call_on_demand fields or decision_id")
    for field in ("assignment", "reason"):
        if not isinstance(action.get(field), str) or not action[field].strip():
            raise WorkflowError(f"call_on_demand requires nonempty {field}")
    entries = catalog(storage, run)
    matches = [n for n in entries if n["node_id"] == action.get("node_id")]
    if not matches:
        matches = [n for n in entries if n["local_node_id"] == action.get("node_id")]
    if len(matches) != 1:
        raise WorkflowError("Unknown, hidden or ambiguous on-demand node_id")
    entry = matches[0]
    issued = sessions(storage, run, entry)
    policy = entry["session_mode"]
    mode = action.get("session_mode")
    reference = action.get("resume_task_id")
    if mode is not None and (not isinstance(mode, str) or mode not in {"fresh", "resume"}):
        raise WorkflowError("On-demand session_mode must be fresh or resume")
    if policy == "fresh":
        if mode not in {None, "fresh"}:
            raise WorkflowError("Fixed Fresh on-demand node requires Fresh")
        mode = "fresh"
    elif policy == "resume":
        expected = "resume" if issued else "fresh"
        if mode not in {None, expected}:
            raise WorkflowError("Fixed Resume on-demand node requires its issued session")
        mode = expected
    elif mode is None:
        if issued:
            raise WorkflowError("Agent decides requires explicit Fresh or Resume")
        mode = "fresh"
    if mode == "fresh" and reference is not None:
        raise WorkflowError("Fresh on-demand calls forbid resume_task_id")
    if mode == "resume":
        if reference is None and policy == "resume" and len(issued) == 1:
            reference = issued[0]["task_id"]
        if not any(s["task_id"] == reference for s in issued):
            raise WorkflowError("Resume requires an issued compatible resume_task_id")
    return entry, mode, reference


async def call(supervisor: Any, node: dict[str, Any], token: dict[str, Any], checkpoint: dict[str, Any], action: dict[str, Any], orchestration_id: str, *, question_id: str | None = None) -> bool:
    """Reserve, acknowledge, dispatch once, then return to the same checkpoint."""
    from .workflows import WorkflowError, WorkflowSupervisor
    run = supervisor.run()
    owner = owner_id(run)
    if not hasattr(supervisor.tree, "on_demand_locks"):
        supervisor.tree.on_demand_locks = {}
    lock = supervisor.tree.on_demand_locks.setdefault(owner, asyncio.Lock())
    async with lock:
        run = supervisor.run()
        if not supervisor.tree.tree_running(run):
            return False
        entry, mode, reference = validate_action(supervisor.store, run, checkpoint, action)
        current_owner = supervisor.store.get_run(owner)
        existing = next((c for c in current_owner.get("on_demand_calls", []) if c["checkpoint"]["workflow_run_id"] == run["workflow_run_id"] and c["checkpoint"]["decision_id"] == checkpoint["decision_id"] and c["orchestration_activation_id"] == orchestration_id), None)
        if existing:
            if existing["action"] != action:
                raise WorkflowError("Authorizing orchestration already reserved a different on-demand call")
            if existing["status"] not in ACTIVE:
                reflect = lambda r: reflect_result(r, existing)
                supervisor.update(reflect, "on_demand_result_redelivered", existing["id"])
                return supervisor.tree.tree_running(supervisor.run())
            supervisor.attention("Existing on-demand reservation requires reconciliation; no replacement dispatched")
            return False
        if any(c["status"] in ACTIVE for c in current_owner.get("on_demand_calls", [])):
            raise WorkflowError("Another on-demand call requires settlement before dispatch")
        if checkpoint.get("on_demand_count", 0) >= run["definition"].get("max_on_demand_calls", 5):
            supervisor.attention(f"On-demand call allowance exhausted for {entry['title']}: {action['assignment']}")
            return False
        cid, execution_id = uuid.uuid4().hex, uuid.uuid4().hex
        reservation = {"id": cid, "call_number": 1 + sum(c["node_id"] == entry["node_id"] for c in current_owner.get("on_demand_calls", [])), "node_id": entry["node_id"], "source_workflow_run_id": entry["source_workflow_run_id"], "local_node_id": entry["local_node_id"], "title": entry["title"], "assignment": action["assignment"], "reason": action["reason"], "checkpoint": {"workflow_run_id": run["workflow_run_id"], "decision_id": checkpoint["decision_id"], "token_id": token["id"], "question_id": question_id}, "origin": {"kind": "child_question" if question_id and node.get("type") == "workflow" else "node_question" if question_id else "routing", "node_id": node["id"], "title": node.get("title", node["id"]), "workflow_run_id": run["workflow_run_id"]}, "status": "reserved", "started_at": None, "finished_at": None, "execution_id": execution_id, "result_ref": {"workflow_run_id": owner, "execution_id": execution_id}, "task_id": None, "result": None, "failure": None, "before_count": checkpoint.get("on_demand_count", 0), "orchestration_activation_id": orchestration_id, "action": copy.deepcopy(action), "dispatch_state": "preparing", "execution_session_mode": mode, "resume_task_id": reference}
        def reserve(r: dict[str, Any]) -> None:
            if r.get("status") != "running":
                raise WorkflowError("On-demand owner stopped before reservation")
            if any(c["status"] in ACTIVE for c in r.get("on_demand_calls", [])):
                raise WorkflowError("Owner already has an unresolved on-demand reservation")
            r.setdefault("on_demand_calls", []).append(reservation)
            if owner == run["workflow_run_id"]:
                reflect_reservation(r, reservation)
        supervisor.store.update_run(owner, reserve, "on_demand_reserved", cid)
        if owner != run["workflow_run_id"]:
            supervisor.update(lambda r: reflect_reservation(r, reservation), "on_demand_checkpoint_applied", cid)
        supervisor.store.update_run(owner, lambda r: next(c for c in r["on_demand_calls"] if c["id"] == cid).update(dispatch_state="checkpoint_applied"), "on_demand_acknowledged", cid)
        if not supervisor.tree.tree_running(supervisor.run()):
            return False
        worker = runtime_node(supervisor.store, run, entry)
        worker["session_mode"] = mode
        class SpecialistSupervisor(WorkflowSupervisor):
            def attention(self, reason: str) -> None:
                # A specialist refusal is checkpoint evidence. Ambiguous dispatches
                # are escalated by the call coordinator after examining receipts.
                self.specialist_refusal = reason
        executor = SpecialistSupervisor(supervisor.registry, supervisor.store, tree=supervisor.tree)
        executor.run_id = owner
        activation = {"id": execution_id, "node_id": worker["id"], "role": "node", "status": "running", "created_at": time.time(), "tasks": [], "token": {}, "on_demand_call_id": cid, "assignment_prompt": action["assignment"], "resume_task_id": reference, "execution_session_mode": mode, "on_demand_node": copy.deepcopy(worker)}
        def activate(r: dict[str, Any]) -> None:
            if r.get("status") != "running":
                raise WorkflowError("On-demand owner stopped before activation")
            r["activations"].append(activation)
            next(c for c in r["on_demand_calls"] if c["id"] == cid).update(status="running", started_at=time.time(), dispatch_state="activation_reserved")
        executor.update(activate, "on_demand_activation_reserved", cid)
        prompt = "Execute the specialist assignment. Return ONLY JSON {\"status\":\"succeeded|failed|blocked\",\"result\":{\"summary\":\"findings\"},\"evidence\":[]}. Do not route the workflow or change its checklist.\nWorker instructions:\n" + worker["instructions"] + "\nAssignment:\n" + action["assignment"]
        outcome = await executor._dispatch(worker, prompt, "node", activation)
        if outcome is not None and outcome.get("outcome_unknown"):
            executor.store.update_run(owner, lambda r: next(c for c in r["on_demand_calls"] if c["id"] == cid).update(status="uncertain", failure="Specialist outcome is unknown"), "on_demand_uncertain", cid)
            supervisor.attention("On-demand specialist outcome is unknown; reconcile before another call")
            return False
        if outcome is None:
            observed = next(a for a in executor.run()["activations"] if a["id"] == execution_id)
            if all(t.get("status") == "not_started" for t in observed["tasks"]):
                if not observed["tasks"] or all(t.get("dispatch_stage") == "preparing" or t.get("status") == "not_started" for t in observed["tasks"]):
                    settle(executor.store, owner, cid, {"status": "cancelled" if not supervisor.tree.tree_running(supervisor.run()) else "failed", "summary": getattr(executor, "specialist_refusal", "Specialist dispatch did not start")})
                    return supervisor.tree.tree_running(supervisor.run())
            executor.store.update_run(owner, lambda r: next(c for c in r["on_demand_calls"] if c["id"] == cid).update(status="uncertain", failure="Specialist dispatch requires reconciliation"), "on_demand_uncertain", cid)
            supervisor.attention("On-demand call outcome requires reconciliation; no replacement dispatched")
            return False
        settle(executor.store, owner, cid, outcome)
        return supervisor.tree.tree_running(supervisor.run())


def settle(storage: Any, owner: str, call_id: str, outcome: dict[str, Any]) -> None:
    from .workflow_delegation import normalize_result
    from .workflows import WorkflowError
    def record(r: dict[str, Any]) -> None:
        item = next(c for c in r["on_demand_calls"] if c["id"] == call_id)
        activation = next(a for a in r["activations"] if a["id"] == item["execution_id"])
        if item["status"] not in ACTIVE:
            return
        try:
            value = normalize_result(activation["on_demand_node"], outcome, []) if outcome.get("status") == "completed" else {"status": "failed", "result": {"summary": outcome.get("summary") or outcome.get("execution_failure") or "Specialist failed"}, "evidence": []}
        except (WorkflowError, ValueError, TypeError) as exc:
            value = {"status": "failed", "result": {"summary": str(exc)}, "evidence": []}
        cancelled = r.get("status") in {"cancelling", "cancelled"} or outcome.get("status") == "cancelled"
        status = "cancelled" if cancelled else "succeeded" if value["status"] == "succeeded" else "failed"
        item.update(status=status, finished_at=time.time(), result=value, failure=None if status == "succeeded" else str(value["result"].get("summary", "Specialist failed")), task_id=next((t["task_id"] for t in reversed(activation["tasks"]) if t.get("execution_kind") != "native_subagent"), None), dispatch_state="settled")
        activation.update(status="completed" if status == "succeeded" else status, node_result=copy.deepcopy(value), raw_output=outcome.get("summary", ""))
    storage.update_run(owner, record, "on_demand_settled", call_id)
    item = next(c for c in storage.get_run(owner)["on_demand_calls"] if c["id"] == call_id)
    storage.update_run(item["checkpoint"]["workflow_run_id"], lambda r: reflect_result(r, item), "on_demand_result_delivered", call_id)
    storage.update_run(owner, lambda r: next(c for c in r["on_demand_calls"] if c["id"] == call_id).update(result_delivered=True), "on_demand_result_acknowledged", call_id)


def reconcile(storage: Any, run: dict[str, Any]) -> None:
    """Repair receipts and results; interrupted reservations never launch here."""
    from .workflows import WorkflowError
    if not enabled(storage, run):
        return
    owner = owner_id(run)
    ledger = storage.get_run(owner).get("on_demand_calls", [])
    for item in ledger:
        if item["checkpoint"]["workflow_run_id"] != run["workflow_run_id"] and owner != run["workflow_run_id"]:
            continue
        origin_id = item["checkpoint"]["workflow_run_id"]
        if item["status"] not in ACTIVE and item.get("result_delivered"):
            continue
        try:
            origin = storage.get_run(origin_id)
            checkpoint = checkpoint_record(origin, item)
            if item["id"] not in checkpoint.get("on_demand_applied_call_ids", []):
                storage.update_run(origin_id, lambda r: reflect_reservation(r, item), "on_demand_receipt_repaired", item["id"])
            if item["status"] not in ACTIVE:
                storage.update_run(origin_id, lambda r: reflect_result(r, item), "on_demand_result_repaired", item["id"])
                storage.update_run(owner, lambda r: next(c for c in r["on_demand_calls"] if c["id"] == item["id"]).update(result_delivered=True), "on_demand_result_acknowledged", item["id"])
                continue
            activation = next((a for a in storage.get_run(owner)["activations"] if a["id"] == item["execution_id"]), None)
            tasks = (activation or {}).get("tasks", [])
            if tasks and all(t.get("status") not in {"reserved", "running", "uncertain"} for t in tasks) and tasks[-1].get("result") and not tasks[-1]["result"].get("outcome_unknown"):
                settle(storage, owner, item["id"], tasks[-1]["result"])
                continue
            reason = "Interrupted on-demand reservation requires attention; no replacement dispatched"
            storage.update_run(owner, lambda r: next(c for c in r["on_demand_calls"] if c["id"] == item["id"]).update(status="uncertain", failure=reason, dispatch_state="checkpoint_applied" if item.get("dispatch_state") == "preparing" else item.get("dispatch_state")), "on_demand_recovery_attention", item["id"])
            storage.update_run(origin_id, lambda r: r.update(status="needs_attention", attention_reason=reason) if r.get("status") not in {"cancelled", "cancelling", "failed", "completed"} else None, "on_demand_origin_attention", item["id"])
        except (OSError, ValueError, KeyError, TypeError, WorkflowError) as exc:
            storage.update_run(owner, lambda r: r.update(status="needs_attention", attention_reason="On-demand origin reconciliation failed: " + str(exc)) if r.get("status") not in {"cancelled", "cancelling", "failed", "completed"} else None, "on_demand_recovery_blocked", item["id"])


def decision_lock(supervisor: Any):
    """Serialize control across Current descendants only when specialists exist."""
    run = supervisor.run()
    if not catalog(supervisor.store, run):
        return supervisor.decision_lock
    if not hasattr(supervisor.tree, "on_demand_decision_locks"):
        supervisor.tree.on_demand_decision_locks = {}
    return supervisor.tree.on_demand_decision_locks.setdefault(owner_id(run), asyncio.Lock())


def cancel_reservations(run: dict[str, Any]) -> None:
    """After task cancellation settles, cancel even unreflected owner reservations."""
    for item in run.get("on_demand_calls", []):
        if item["status"] not in ACTIVE:
            continue
        activation = next((a for a in run.get("activations", []) if a["id"] == item["execution_id"]), None)
        if activation and any(t.get("status") in {"reserved", "running", "uncertain"} for t in activation.get("tasks", [])):
            continue
        item.update(status="cancelled", finished_at=time.time(), failure="Workflow cancelled", dispatch_state="settled", result_delivered=True)
        if activation:
            activation["status"] = "cancelled"
