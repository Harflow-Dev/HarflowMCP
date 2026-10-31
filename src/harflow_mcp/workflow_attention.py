"""Persisted workflow checkpoint identities and synchronous control preconditions."""
from __future__ import annotations

import hashlib
import json
from typing import Any

from .workflows import WorkflowError

SUSPENDED = {"needs_attention", "paused", "failed"}
SCHEDULER = {"starting", "running", "cancelling"}


class CheckpointError(WorkflowError):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def semantic_identity(run: dict[str, Any]) -> tuple:
    """Track issued work identity, excluding progress inside an existing attempt."""
    pending = [(item.get("id"), item.get("decision_id"), item.get("node_id"), item.get("execution_activation_id")) for item in run.get("pending", [])]
    attempts = [(activation.get("id"), [(task.get("task_id"), task.get("attempt")) for task in activation.get("tasks", [])]) for activation in run.get("activations", [])]
    source = run.get("attention_source") or {}
    return (run.get("status"), run.get("attention_reason"), run.get("failure_reason"), run.get("failed_decision_id"),
            source.get("workflow_run_id"), tuple(source.get("path") or []), pending, attempts)


def _digest(kind: str, value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return kind + ":" + hashlib.sha256(encoded).hexdigest()


def authoritative_chain(storage: Any, root: dict[str, Any], *, cached: dict | None = None) -> list[dict[str, Any]]:
    """Follow selected forwarding pointers only through bounded reciprocal edges."""
    from .workflow_inputs import project
    cache = {} if cached is None else cached
    verified = set()
    project(storage, root, _cached=cache, _records=verified, _tokens=False)
    root_id = (root.get("parent_link") or {}).get("root_workflow_run_id") or root["workflow_run_id"]
    current = cache[root["workflow_run_id"]]
    chain, seen = [], set()
    while True:
        identifier = current["workflow_run_id"]
        if identifier in seen:
            raise WorkflowError("Workflow attention source contains a cycle")
        seen.add(identifier)
        chain.append(current)
        source = current.get("attention_source") or {}
        target = source.get("workflow_run_id")
        if target in (None, identifier):
            return chain
        path = source.get("path")
        if not isinstance(path, list) or not path or path[0] != identifier or path[-1] != target:
            raise WorkflowError("Workflow attention source path is unavailable")
        for parent_id, child_id in zip(path, path[1:]):
            parent, child = cache.get(parent_id), cache.get(child_id)
            if parent_id not in verified or child_id not in verified or parent is None or child is None:
                raise WorkflowError("Workflow attention source is not in the verified input tree")
            link = child.get("parent_link") or {}
            if link.get("root_workflow_run_id") != root_id or link.get("workflow_run_id") != parent_id or not any(
                activation.get("id") == link.get("execution_id") and (activation.get("invocation") or {}).get("child_workflow_run_id") == child_id
                for activation in parent.get("activations", [])
            ):
                raise WorkflowError("Workflow attention source path ownership changed")
            if child_id != target:
                if child_id in seen:
                    raise WorkflowError("Workflow attention source contains a cycle")
                seen.add(child_id)
                chain.append(child)
        current = cache[target]


def _attention_token(root_id: str, chain: list[dict[str, Any]]) -> str | None:
    hops = []
    if not chain or chain[-1].get("status") not in SUSPENDED:
        return None
    for run in chain:
        marker = run.get("attention_checkpoint")
        if run.get("status") in SUSPENDED and (type(marker) is not int or marker < 0):
            return None
        source = run.get("attention_source") or {}
        hops.append([run["workflow_run_id"], run["status"], "recover" if run["status"] == "failed" else "resume", marker, source.get("workflow_run_id"), source.get("path")])
    return _digest("attention", [root_id, hops])



CONTINUING = {"starting", "running", "building", "pending"}


def received_completion(root: dict[str, Any], chain: list[dict[str, Any]]) -> bool:
    """Compose unfinished handovers only along one verified selected chain."""
    delivery = root.get("forwarded_delivery")
    payload = (delivery or {}).get("payload", {})
    marker = payload.get("attention_root_checkpoint")
    source = root.get("attention_source")
    if not (delivery and chain and source
            and type(marker) is int and marker == root.get("attention_checkpoint")
            and isinstance(payload.get("attention_checkpoint_id"), str)
            and payload["attention_checkpoint_id"].startswith("attention:")
            and payload.get("source") == source
            and payload.get("source_id") == source.get("workflow_run_id")):
        return False
    target_index = next((index for index, hop in enumerate(chain) if hop.get("workflow_run_id") == payload["source_id"]), None)
    if target_index is None or target_index == 0:
        return False
    target = chain[target_index]
    if target.get("status") in CONTINUING:
        return target.get("forwarded_delivery_receipt") == delivery
    if target.get("status") not in SUSPENDED or payload.get("action") != ("recover" if target["status"] == "failed" else "resume"):
        return False
    next_payload = (target.get("forwarded_delivery") or {}).get("payload", {})
    if any(payload.get(key, False if key == "allow_optional_review_skip" else None)
           != next_payload.get(key, False if key == "allow_optional_review_skip" else None)
           for key in ("instructions", "additional_attempts", "interaction_owner", "decision_id", "allow_optional_review_skip")):
        return False
    return received_completion(target, chain[target_index:])


def checkpoint_id(storage: Any, root: dict[str, Any]) -> str | None:
    """Read qualified persisted identity without writes or process probes."""
    if root.get("status") in SCHEDULER and not root.get("parent_link"):
        owner = root.get("supervisor_identity")
        if not isinstance(owner, dict) or type(owner.get("pid")) is not int or owner["pid"] <= 0 or not isinstance(owner.get("start_time"), str) or not owner["start_time"]:
            return None
        return _digest("scheduler", [root["workflow_run_id"], root["status"], owner])
    if root.get("status") not in SUSPENDED:
        return None
    try:
        chain = authoritative_chain(storage, root)
    except (WorkflowError, OSError, ValueError, KeyError, TypeError):
        return None
    root_id = (root.get("parent_link") or {}).get("root_workflow_run_id") or root["workflow_run_id"]
    if received_completion(chain[0], chain):
        return chain[0]["forwarded_delivery"]["payload"]["attention_checkpoint_id"]
    return _attention_token(root_id, chain)


def row_checkpoint_id(root: dict[str, Any], path: list[str], cache: dict) -> str | None:
    """Bind a verified informational row to its root and reciprocal path."""
    chain = [cache[identifier] for identifier in path]
    root_id = (root.get("parent_link") or {}).get("root_workflow_run_id") or root["workflow_run_id"]
    return _attention_token(root_id, chain)


def require_checkpoint(storage: Any, root: dict[str, Any], supplied: str, action: str) -> None:
    mode = "scheduler:" if root.get("status") in SCHEDULER else "attention:"
    if not isinstance(supplied, str) or not supplied.startswith(mode):
        raise CheckpointError("attention_checkpoint_stale", "The workflow recovery mode changed; refresh and review its current checkpoint")
    expected = checkpoint_id(storage, root)
    if expected is None:
        unavailable = root.get("status") in SCHEDULER
        if root.get("status") in SUSPENDED:
            try:
                chain = authoritative_chain(storage, root)
                unavailable = any(hop.get("status") in SUSPENDED and type(hop.get("attention_checkpoint")) is not int for hop in chain)
            except (WorkflowError, OSError, ValueError, KeyError, TypeError):
                unavailable = True
        if unavailable:
            raise CheckpointError("attention_checkpoint_unavailable", "This workflow has no qualified actionable checkpoint; refresh its status. Legacy suspended records require a legitimate engine mutation before guarded continuation is available")
        raise CheckpointError("attention_checkpoint_stale", "The workflow moved on; refresh and review its current checkpoint before submitting again")
    if supplied != expected or action != ("recover" if mode == "scheduler:" or root.get("status") == "failed" else "resume"):
        raise CheckpointError("attention_checkpoint_stale", "The workflow checkpoint changed; refresh and review the current checkpoint before submitting again. The answer and grants were not applied")
