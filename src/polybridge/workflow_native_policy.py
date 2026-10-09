"""Pinned permission planning for native workflow owners.

Planning never creates tasks or sessions. Historical runs without a plan retain
their original read-only owner contract; compatibility is rechecked at launch.
"""
from __future__ import annotations

import copy
import hashlib
import json
from types import SimpleNamespace
from typing import Any

VERSION = "native_owner_contract_v1"


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def candidate_key(candidate: dict[str, Any]) -> str:
    from .workflows import _candidate_key
    return _candidate_key(candidate)


def node_network(node: dict[str, Any], network: bool | None) -> bool | None:
    return False if network is False else node.get("network", network)


def _requirements(nodes: list[dict[str, Any]], network: bool | None) -> tuple[str, bool | None]:
    from .workflows import FREEDOMS
    freedom = max((n["freedom"] for n in nodes), key=FREEDOMS.index, default="read_only")
    requests = [network, *(n["network"] for n in nodes)]
    effective_network = True if True in requests else False if False in requests else None
    return freedom, effective_network


def plan_owner_contracts(definition: dict[str, Any], tree: dict[str, Any] | None = None, *, freedom: str = "unrestricted", network: bool | None = None, permission_policy: str = "saved_node") -> dict[str, Any]:
    """Resolve each distinct owner, including Current descendants and Child boundaries."""
    from . import backends
    from .backends.native import adapter
    from .workflow_references import workflow_identity
    from .workflows import effective_freedom
    root_id = workflow_identity(definition)
    definitions = {ident: entry["definition"] for ident, entry in (tree or {}).get("workflows", {}).items()}
    definitions[root_id] = definition
    owners: dict[str, Any] = {}
    versions: dict[str, str | None] = {}

    def compatible(native: Any, parent: Any, item: dict[str, Any]) -> str | None:
        if parent.backend not in versions:
            versions[parent.backend] = backends.version(backends.get(parent.backend))
        return native.eligible(parent, item["candidate"], {"freedom": item["freedom"], "network": item["network"], "session_mode": "fresh", "backend_version": versions[parent.backend]})

    def collect(ident: str, path: list[str], seen: set[str]) -> tuple[list[dict[str, Any]], list[str]]:
        if ident in path:
            raise ValueError("Cyclic pinned workflow owner tree")
        if ident in seen:
            return [], []
        seen.add(ident)
        current = definitions[ident]
        nodes: list[dict[str, Any]] = []
        child_owners: list[str] = []
        for node in current.get("nodes", []):
            if node.get("type") == "agent":
                for position, node_candidate in enumerate([node["agent"], *node["agent"].get("fallbacks", [])]):
                    nodes.append({"workflow_id": ident, "node_id": node["id"], "title": node.get("title") or node["id"], "freedom": effective_freedom(node, freedom, permission_policy=permission_policy), "network": node_network(node, network), "node": node, "candidate": {key: value for key, value in node_candidate.items() if key != "fallbacks"}, "candidate_position": position, "parallel": any(n.get("type") == "parallel_start" for n in current.get("nodes", []))})
            elif node.get("type") == "workflow":
                target = node["workflow_ref"]["workflow_id"]
                if target not in definitions:
                    raise ValueError("Pinned workflow dependency is missing: " + target)
                if node.get("orchestrator_mode", "child") == "current":
                    descendants, children = collect(target, path + [ident], seen)
                    nodes.extend(descendants)
                    child_owners.extend(children)
                else:
                    child_owners.append(target)
        return nodes, child_owners

    def plan(ident: str) -> None:
        if ident in owners:
            return
        current = definitions[ident]
        nodes, children = collect(ident, [], set())
        config = current["orchestrator"]
        plans: dict[str, Any] = {}
        for candidate in [config, *config.get("fallbacks", [])]:
            native = adapter(backends.get(candidate["backend"]))
            accepted: list[dict[str, Any]] = []
            outcomes: list[dict[str, Any]] = []
            for item in nodes:
                node = item["node"]
                reason = None
                if node.get("execution_mode") != "prefer_subagent":
                    reason = "Node requests Headless execution"
                elif node.get("session_mode", "fresh") not in {"fresh", "agent_decides"}:
                    reason = "Native child Resume is not certified"
                elif item["candidate"]["backend"] != candidate["backend"]:
                    reason = "The node harness differs from its owning orchestrator"
                elif native is None:
                    reason = "This harness has no certified native execution adapter"
                elif item["parallel"] and not getattr(native, "supports_parallel", False):
                    reason = "Parallel native execution is not certified"
                else:
                    owner_freedom, owner_network = _requirements([item], network)
                    parent = SimpleNamespace(**{key: candidate.get(key) for key in ("backend", "model", "reasoning_effort", "max_turns")}, freedom=owner_freedom, network=owner_network)
                    try:
                        reason = compatible(native, parent, item)
                    except (ValueError, OSError) as exc:
                        reason = "Native compatibility cannot be verified: " + str(exc)
                outcome = {key: copy.deepcopy(item[key]) for key in ("workflow_id", "node_id", "title", "freedom", "network", "candidate", "candidate_position")}
                outcome.update(execution_kind="headless" if reason else "native_subagent", fallback_reason=reason)
                outcomes.append(outcome)
                if reason is None:
                    accepted.append(item)
            # Aggregate access can change inheritance compatibility. Drop such
            # children before pinning; never retain their permissions needlessly.
            while accepted:
                owner_freedom, owner_network = _requirements(accepted, network)
                parent = SimpleNamespace(**{key: candidate.get(key) for key in ("backend", "model", "reasoning_effort", "max_turns")}, freedom=owner_freedom, network=owner_network)
                rejected = []
                for item in accepted:
                    try:
                        reason = compatible(native, parent, item)
                    except (ValueError, OSError) as exc:
                        reason = "Native compatibility cannot be verified: " + str(exc)
                    if reason:
                        rejected.append(item)
                        for outcome in outcomes:
                            if outcome["workflow_id"] == item["workflow_id"] and outcome["node_id"] == item["node_id"] and outcome["candidate_position"] == item["candidate_position"]:
                                outcome.update(execution_kind="headless", fallback_reason=reason)
                if not rejected:
                    break
                accepted = [item for item in accepted if item not in rejected]
            owner_freedom, owner_network = _requirements(accepted, network)
            plans[candidate_key(candidate)] = {"candidate": copy.deepcopy(candidate), "freedom": owner_freedom, "network": owner_network, "nodes": outcomes, "contributing_nodes": [{key: copy.deepcopy(item[key]) for key in ("workflow_id", "node_id", "candidate", "candidate_position")} for item in accepted]}
        owners[ident] = {"workflow_id": ident, "name": current["name"], "candidates": plans}
        for child in children:
            plan(child)

    plan(root_id)
    result = {"version": VERSION, "root_workflow_id": root_id, "owners": owners}
    result["contract_hash"] = digest(result)
    return result


def orchestrator_contract(run: dict[str, Any], candidate: dict[str, Any] | None = None) -> dict[str, Any]:
    """Never derive expanded permissions for an old or incomplete run record."""
    pinned = run.get("owner_contracts")
    if pinned is None:
        return {"freedom": "read_only", "network": run.get("network")}
    if pinned.get("version") != VERSION:
        raise ValueError("Unknown native owner permission contract")
    if pinned.get("contract_hash") != digest({key: value for key, value in pinned.items() if key != "contract_hash"}):
        raise ValueError("Pinned native owner permission contract does not match its hash")
    config = candidate or run["definition"]["orchestrator"]
    contract = pinned["owners"][pinned["root_workflow_id"]]["candidates"].get(candidate_key(config))
    if contract is None:
        raise ValueError("Orchestrator candidate has no pinned permission contract")
    return copy.deepcopy(contract)


def child_contracts(parent: dict[str, Any], workflow_id: str) -> dict[str, Any] | None:
    pinned = parent.get("owner_contracts")
    if pinned is None:
        return None
    if workflow_id not in pinned["owners"]:
        raise ValueError("Child owner has no pinned permission contract")
    result = copy.deepcopy(pinned)
    result["root_workflow_id"] = workflow_id
    result.pop("contract_hash", None)
    result["contract_hash"] = digest(result)
    return result
