"""Fail-closed conversation inheritance for a new child workflow invocation.

Only the immediate parent's latest earlier visit is considered. The inherited
binding is not a worker session and is never copied into the new session map.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def offer(store: Any, run: dict[str, Any], node: dict[str, Any], exclude_execution_id: str | None = None, *, _recovered_successors: set[str] | None = None) -> dict[str, Any]:
    """Issue one opaque reference, or a precise refusal; never search past a visit."""
    from . import backends, store as tasks
    from .workflow_invocation import child_settled
    from .workflow_references import definition_sha256, subtree
    from .workflows import _candidate_key
    result: dict[str, Any] = {"eligible_sessions": [], "unavailable_reason": "No earlier invocation of this node in the immediate parent run"}
    if node.get("orchestrator_mode", "child") != "child":
        result["unavailable_reason"] = "Current mode uses the existing orchestrator owner"
        return result
    prior = []
    for activation in run.get("activations", []):
        if activation.get("id") == exclude_execution_id:
            break
        if activation.get("node_id") == node["id"] and activation.get("invocation"):
            prior.append(activation)
    if not prior:
        return result
    source = prior[-1]
    invocation = source["invocation"]
    def refuse(reason: str) -> dict[str, Any]:
        result["unavailable_reason"] = reason
        return result
    try:
        child = store.get_run(invocation["child_workflow_run_id"])
        link = child.get("parent_link") or {}
        if link.get("workflow_run_id") != run["workflow_run_id"] or link.get("execution_id") != source["id"] or link.get("node_id") != node["id"]:
            return refuse("Latest child ownership does not match the immediate parent invocation")
        if invocation.get("stage") != "settled" or child.get("status") != "completed" or child.get("orchestrator_mode", "child") != "child" or not child_settled(child, store=store):
            return refuse("Latest child invocation is not completed with conclusively settled descendants")
        pinned = run["dependency_tree"]["workflows"][node["workflow_ref"]["workflow_id"]]
        definition = pinned["definition"]
        digest = definition_sha256(definition)
        expected_tree = subtree(run["dependency_tree"], node["workflow_ref"]["workflow_id"])
        if child.get("definition_hash") != digest or definition_sha256(child.get("definition", {})) != digest or child.get("revision") != pinned.get("revision", definition.get("revision", 0)) or child.get("workflow_id") != node["workflow_ref"]["workflow_id"] or child.get("dependency_tree") != expected_tree:
            return refuse("Latest child pinned workflow definition or revision changed")
        if str(Path(child["repo_path"]).resolve()) != str(Path(run["repo_path"]).resolve()):
            return refuse("Latest child repository changed")
        binding = child.get("sessions", {}).get("orchestrator", {})
        record = tasks.read(store.root / "tasks", binding.get("task_id", "")) if binding.get("task_id") else None
        if record is None or record.status != "completed" or tasks.outcome_unobserved(record) or not record.session_id or binding.get("session_id") != record.session_id:
            return refuse("Latest child orchestrator session is missing or its outcome is unconfirmed")
        if tasks.session_successor_task_ids(store.root / "tasks", record.task_id, record.session_id) != (_recovered_successors or set()):
            return refuse("Latest child conversation was advanced by successor tasks after its completed invocation")
        owners = [attempt for activation in child.get("activations", []) if activation.get("role") == "orchestrator" for attempt in activation.get("tasks", []) if attempt.get("task_id") == record.task_id]
        if len(owners) != 1 or owners[0].get("status") != "completed" or "orchestrator:" + _candidate_key(owners[0].get("candidate", {})) != binding.get("candidate"):
            return refuse("Latest child session is not owned by its completed orchestrator dispatch")
        # Every launched descendant must have a durable exit receipt. Workflow
        # projections alone do not prove another server relinquished ownership.
        stack, visited = [child], set()
        busy_sessions = tasks.live_session_ids(store.root / "tasks")
        while stack:
            current = stack.pop()
            if current["workflow_run_id"] in visited:
                return refuse("Latest child contains cyclic descendant ownership")
            visited.add(current["workflow_run_id"])
            for activation in current.get("activations", []):
                if activation.get("invocation"):
                    stack.append(store.get_run(activation["invocation"]["child_workflow_run_id"]))
                for attempt in activation.get("tasks", []):
                    if attempt.get("status") == "not_started":
                        continue
                    if attempt.get("execution_kind") == "native_subagent":
                        if attempt.get("native_terminal") is not True:
                            return refuse("Latest child native descendant ownership is unconfirmed")
                        task_id = attempt.get("transport_task_id")
                    else:
                        task_id = attempt.get("task_id")
                    receipt = tasks.read(store.root / "tasks", task_id) if task_id else None
                    if receipt is None or tasks.outcome_unobserved(receipt):
                        return refuse("Latest child dispatch ownership has no conclusive exit receipt")
                    if receipt.session_id in busy_sessions:
                        return refuse("Latest child descendant session is live, uncertain, or reserved for takeover")
        if record.session_id in busy_sessions:
            return refuse("Latest child session is live, uncertain, or reserved for takeover")
        config = definition["orchestrator"]
        candidates = [config] + config.get("fallbacks", [])
        candidate = next((copy.deepcopy(c) for c in candidates if "orchestrator:" + _candidate_key(c) == binding.get("candidate")), None)
        if candidate is None or (record.backend, record.model, record.reasoning_effort, record.max_turns) != (candidate["backend"], candidate.get("model"), candidate.get("reasoning_effort"), candidate.get("max_turns")):
            return refuse("Latest child actual orchestrator candidate or launch configuration changed")
        from .workflow_native_policy import child_contracts, orchestrator_contract
        target_contracts = child_contracts(run, node["workflow_ref"]["workflow_id"])
        target = {"definition": definition, "network": run.get("network")}
        if target_contracts is not None:
            target["owner_contracts"] = target_contracts
        contract = orchestrator_contract(target, candidate)
        source_contract = orchestrator_contract(child, candidate)
        if (contract["freedom"], contract["network"]) != (source_contract["freedom"], source_contract["network"]):
            return refuse("Latest child pinned owner permissions changed")
        freedom, network = contract["freedom"], contract["network"]
        if record.freedom != freedom or record.network != network or str(Path(record.repo_path).resolve()) != str(Path(run["repo_path"]).resolve()):
            return refuse("Latest child orchestrator access, network, or repository is incompatible")
        backend = backends.get(candidate["backend"])
        if backend.capabilities.resume_may_start_fresh:
            return refuse("Latest child harness may start a Fresh conversation when Resume is unavailable; strict Child Resume is unsupported")
        if not backend.capabilities.supports_model_selection:
            return refuse("Latest child harness cannot pin its ambient model for compatible conversation reuse; select Fresh")
        if backend.capabilities.supports_model_selection and not record.model:
            return refuse("Latest child model is unknown: conversation reuse requires an explicit saved model")
        if backend.capabilities.reasoning_effort.accepts_parameter and not record.reasoning_effort:
            return refuse("Latest child reasoning effort is unknown: conversation reuse requires explicit saved effort")
        if not backends.is_installed(backend):
            return refuse("Latest child orchestrator CLI is unavailable")
        launch = backend.build_resume_argv("Validate child orchestrator resume", repo=Path(run["repo_path"]), freedom=freedom, session_id=record.session_id, model=record.model, max_turns=record.max_turns, reasoning_effort=record.reasoning_effort, network=network)
        backend.assert_safe(launch, freedom, network)
        fingerprint = _hash({"definition_hash": digest, "dependency_tree": expected_tree, "revision": child["revision"], "repo_path": str(Path(run["repo_path"]).resolve()), "candidate": candidate, "freedom": freedom, "network": network, "parent_run_id": run["workflow_run_id"], "node_id": node["id"], "source_execution_id": source["id"], "source_child_workflow_run_id": child["workflow_run_id"], "source_task_id": record.task_id, "source_session_id": record.session_id})
        selection = {"session_ref": "child-session:" + fingerprint, "compatibility_fingerprint": fingerprint, "source_execution_id": source["id"], "source_child_workflow_run_id": child["workflow_run_id"], "source_task_id": record.task_id, "source_session_id": record.session_id, "candidate": candidate, "binding": copy.deepcopy(binding)}
        return {"eligible_sessions": [selection], "unavailable_reason": ""}
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        return refuse("Latest child session cannot be validated: " + str(exc)[:500])


def select(store: Any, run: dict[str, Any], node: dict[str, Any], activation: dict[str, Any], token: dict[str, Any]) -> dict[str, Any]:
    """Choose once; a persisted choice must still match fresh eligibility."""
    from .workflows import WorkflowError
    requested = node.get("child_session_policy", "agent_decides")
    if node.get("orchestrator_mode", "child") != "child":
        return {"requested_policy": requested, "selected_mode": "current", "reason": "Current mode retains its existing orchestrator owner"}
    saved = activation.get("invocation", {}).get("child_session_selection")
    mode = saved.get("selected_mode") if saved else token.get("child_session_mode", requested)
    if mode not in {"fresh", "resume"}:
        raise WorkflowError("Agent decides requires child_session_mode fresh or resume")
    reason = saved.get("reason") if saved else token.get("child_session_reason", "Explicit " + mode)
    if not isinstance(reason, str) or not reason.strip():
        raise WorkflowError("Child session selection requires a reason")
    if requested != "agent_decides" and mode != requested:
        raise WorkflowError("Child session selection cannot override its fixed policy")
    if mode == "fresh":
        if token.get("child_session_ref"):
            raise WorkflowError("Fresh child selection cannot select a session reference")
        return saved or {"requested_policy": requested, "selected_mode": mode, "reason": reason}
    offered = offer(store, run, node, activation["id"])
    if not offered["eligible_sessions"]:
        raise WorkflowError("Child Resume unavailable: " + offered["unavailable_reason"])
    choice = offered["eligible_sessions"][0]
    if saved:
        if saved.get("compatibility_fingerprint") != choice["compatibility_fingerprint"]:
            raise WorkflowError("Persisted child Resume source changed; explicit Fresh selection required")
    elif (requested == "agent_decides" or token.get("child_session_ref")) and token.get("child_session_ref") != choice["session_ref"]:
        raise WorkflowError("Child Resume requires the issued eligible child_session_ref")
    return saved or {**choice, "requested_policy": requested, "selected_mode": mode, "reason": reason}


def validate_inherited(store: Any, child: dict[str, Any]) -> None:
    """Revalidate the parent's durable binding just before the first resume."""
    from .workflows import WorkflowError
    binding = child["inherited_orchestrator_binding"]
    link = child["parent_link"]
    parent = store.get_run(link["workflow_run_id"])
    activation = next(a for a in parent["activations"] if a["id"] == link["execution_id"])
    node = next(n for n in parent["definition"]["nodes"] if n["id"] == link["node_id"])
    selected = select(store, parent, node, activation, activation.get("token", {}))
    if selected != binding:
        raise WorkflowError("Inherited child orchestrator binding does not match the parent selection")


async def _recover_first_turn(supervisor: Any) -> None:
    """Restore only the proven first inherited turn, never exempt foreign writes."""
    from . import control, store as tasks
    from .workflows import _candidate_key
    child = supervisor.run()
    inherited = child.get("inherited_orchestrator_binding")
    if not inherited or (child.get("sessions", {}).get("orchestrator") and not child.get("recovered_inherited_turn")):
        return
    if child.get("recovered_inherited_turn"):
        supervisor.update(lambda run: (run["sessions"].pop("orchestrator", None), run.pop("recovered_inherited_turn", None)), "inherited_recovery_revalidation")
    pending = {token.get("decision_id"): token for token in child.get("pending", []) if token.get("decision_id")}
    attempts = [(activation, attempt) for activation in child.get("activations", []) if activation.get("role") == "orchestrator" for attempt in activation.get("tasks", []) if attempt.get("status") != "not_started"]
    if not attempts:
        return
    source_task = inherited["source_task_id"]
    owned = set()
    for owned_activation, owned_attempt in attempts:
        receipt = tasks.read(supervisor.store.root / "tasks", owned_attempt["task_id"])
        if owned_attempt.get("status") != "completed" or owned_attempt.get("session_mode") != "resume" or owned_attempt.get("resume_task_id") != source_task or receipt is None or receipt.parent_task_id != source_task or receipt.status != "completed" or tasks.outcome_unobserved(receipt) or receipt.session_id != inherited["source_session_id"]:
            return
        expected = inherited["candidate"]
        from .workflow_native_policy import orchestrator_contract
        contract = orchestrator_contract(child, expected)
        if owned_attempt.get("candidate") != expected or (receipt.backend, receipt.model, receipt.reasoning_effort, receipt.max_turns, receipt.freedom, receipt.network, str(Path(receipt.repo_path).resolve())) != (expected["backend"], expected.get("model"), expected.get("reasoning_effort"), expected.get("max_turns"), contract["freedom"], contract["network"], str(Path(child["repo_path"]).resolve())):
            return
        owned.add(receipt.task_id)
        source_task = receipt.task_id
    activation, attempt = attempts[-1]
    checkpoint = pending.get(activation.get("decision_id"))
    candidate = inherited["candidate"]
    from .workflow_native_policy import orchestrator_contract
    contract = orchestrator_contract(child, candidate)
    if not checkpoint or activation.get("token", {}).get("id") != checkpoint["id"] or activation.get("token", {}).get("decision_id") != checkpoint["decision_id"] or attempt.get("status") != "completed" or attempt.get("session_mode") != "resume" or attempt.get("candidate") != candidate:
        return
    sid = inherited["source_session_id"]
    async with control.session_lock(supervisor.store.root / "tasks", sid, timeout=10):
        record = tasks.read(supervisor.store.root / "tasks", attempt["task_id"])
        if record is None or record.status != "completed" or tasks.outcome_unobserved(record) or record.parent_task_id != attempt.get("resume_task_id") or record.session_id != sid:
            return
        if (record.backend, record.model, record.reasoning_effort, record.max_turns, record.freedom, record.network, str(Path(record.repo_path).resolve())) != (candidate["backend"], candidate.get("model"), candidate.get("reasoning_effort"), candidate.get("max_turns"), contract["freedom"], contract["network"], str(Path(child["repo_path"]).resolve())):
            return
        if sid in tasks.live_session_ids(supervisor.store.root / "tasks") or tasks.session_successor_task_ids(supervisor.store.root / "tasks", record.task_id, sid):
            return
        link = child["parent_link"]
        parent = supervisor.store.get_run(link["workflow_run_id"])
        owner = next(a for a in parent["activations"] if a["id"] == link["execution_id"])
        node = next(n for n in parent["definition"]["nodes"] if n["id"] == link["node_id"])
        if owner.get("invocation", {}).get("child_session_selection") != inherited:
            return
        offered = offer(supervisor.store, parent, node, owner["id"], _recovered_successors=owned)
        if not offered["eligible_sessions"] or offered["eligible_sessions"][0]["compatibility_fingerprint"] != inherited["compatibility_fingerprint"]:
            return
        binding = {"candidate": "orchestrator:" + _candidate_key(candidate), "task_id": record.task_id, "session_id": sid}
        supervisor.update(lambda run: (run["sessions"].__setitem__("orchestrator", binding), run.update(recovered_inherited_turn=True)), "inherited_turn_recovered")


async def recover_first_turn(supervisor: Any) -> None:
    """Malformed or missing recovery evidence retains ordinary strict refusal."""
    try:
        await _recover_first_turn(supervisor)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, StopIteration):
        return
