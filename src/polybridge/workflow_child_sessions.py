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


def offer(store: Any, run: dict[str, Any], node: dict[str, Any], exclude_execution_id: str | None = None) -> dict[str, Any]:
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
        network = False if run.get("network") is False else run.get("network")
        if record.freedom != "read_only" or record.network != network or str(Path(record.repo_path).resolve()) != str(Path(run["repo_path"]).resolve()):
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
        launch = backend.build_resume_argv("Validate child orchestrator resume", repo=Path(run["repo_path"]), freedom="read_only", session_id=record.session_id, model=record.model, max_turns=record.max_turns, reasoning_effort=record.reasoning_effort, network=network)
        backend.assert_safe(launch, "read_only", network)
        fingerprint = _hash({"definition_hash": digest, "dependency_tree": expected_tree, "revision": child["revision"], "repo_path": str(Path(run["repo_path"]).resolve()), "candidate": candidate, "freedom": "read_only", "network": network, "parent_run_id": run["workflow_run_id"], "node_id": node["id"], "source_execution_id": source["id"], "source_child_workflow_run_id": child["workflow_run_id"], "source_task_id": record.task_id, "source_session_id": record.session_id})
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
