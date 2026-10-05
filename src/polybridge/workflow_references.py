"""Stable workflow identity and whole-tree dependency resolution.

A Run workflow node references a saved workflow by its polybridge-owned
``workflow_id``.  Starting a run pins the complete dependency tree so later
edits or deletes never reinterpret an existing run.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

MAX_WORKFLOW_NESTING_DEPTH = 4


def _w():
    from . import workflows
    return workflows


def workflow_identity(definition: dict[str, Any]) -> str:
    """The id polybridge owns; legacy definitions report a name-derived id."""
    ident = definition.get("workflow_id")
    if isinstance(ident, str) and ident.strip():
        return ident
    name = definition.get("name", "")
    return "legacy-" + hashlib.sha256(str(name).encode()).hexdigest()[:32]


def definition_sha256(definition: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(definition, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def access_summary(definition: dict[str, Any], depth: int) -> dict[str, Any]:
    """A bounded view of what a pinned definition can do; never widened."""
    w = _w()
    freedoms = [w.effective_freedom(node, "unrestricted", permission_policy="saved_node") for node in definition.get("nodes", []) if node.get("type") == "agent"]
    backends: set[str] = set()
    orchestrator = definition.get("orchestrator", {})
    if isinstance(orchestrator, dict) and orchestrator.get("backend"):
        backends.add(str(orchestrator["backend"]))
    for node in definition.get("nodes", []):
        if node.get("type") != "agent":
            continue
        for candidate in [node.get("agent", {})] + node.get("agent", {}).get("fallbacks", []):
            if isinstance(candidate, dict) and candidate.get("backend"):
                backends.add(str(candidate["backend"]))
    network = any(node.get("network") is True for node in definition.get("nodes", []) if node.get("type") == "agent")
    return {
        "max_freedom": max(freedoms, key=lambda f: w.FREEDOMS.index(f)) if freedoms else "read_only",
        "network": network,
        "backends": sorted(backends),
        "depth": depth,
    }


class DependencyError(ValueError):
    """A workflow tree is unresolvable: missing, cyclic or too deep."""


def _indexed_definitions(store: Any, substitute_name: str | None = None) -> dict[str, dict[str, Any]]:
    """Index every saved definition by its stable workflow identity.

    ``substitute_name`` is the definition currently being saved: the candidate
    substitutes for its own slot, so the stale stored copy is excluded.
    """
    index: dict[str, dict[str, Any]] = {}
    for definition in store.list():
        if substitute_name is not None and definition.get("name") == substitute_name:
            continue
        ident = workflow_identity(definition)
        if ident in index and definition_sha256(index[ident]) != definition_sha256(definition):
            raise DependencyError(f"Duplicate workflow id {ident} names different definitions")
        index[ident] = definition
    return index


def resolve_dependencies_locked(store: Any, *, definition: dict[str, Any] | None = None, name: str | None = None, substitute_name: str | None = None) -> dict[str, Any]:
    """Resolve and pin the tree; the caller must hold the definitions-tree lock."""
    w = _w()
    root = definition if definition is not None else store.get(name)
    root = w.validate_definition(root)
    index = _indexed_definitions(store, substitute_name)
    root_id = workflow_identity(root)
    tree: dict[str, Any] = {"root_workflow_id": root_id, "workflows": {}, "edges": [], "access": {}}

    def pin(current: dict[str, Any], depth: int, path: list[str]) -> None:
        ident = workflow_identity(current)
        w.validate_definition(current)
        if depth > MAX_WORKFLOW_NESTING_DEPTH:
            names = [tree["workflows"].get(ident, {}).get("name") or str(ident) for ident in path]
            raise DependencyError(f"Workflow nesting exceeds {MAX_WORKFLOW_NESTING_DEPTH} levels: {' > '.join(names)} > {current.get('name', ident)}")
        if ident in index and definition_sha256(index[ident]) != definition_sha256(current):
            raise DependencyError(f"Duplicate workflow id {ident} names different definitions")
        tree["workflows"][ident] = {
            "name": current.get("name", ""),
            "revision": current.get("revision", 0),
            "definition_sha256": definition_sha256(current),
            "definition": json.loads(json.dumps(current, ensure_ascii=False)),
        }
        previous_depth = tree["access"].get(ident, {}).get("depth", 0)
        tree["access"][ident] = access_summary(current, max(depth, previous_depth))
        for node in current.get("nodes", []):
            if node.get("type") != "workflow":
                continue
            ref = node.get("workflow_ref", {})
            target_id = ref.get("workflow_id")
            if not isinstance(target_id, str) or not target_id.strip():
                raise DependencyError(f"Run workflow node {node.get('id')} has no workflow_id reference")
            if target_id in path:
                # Includes a direct self-reference: the root is on the path from level 1.
                names = {ident: (tree["workflows"].get(ident, {}).get("name") or (index.get(ident) or {}).get("name") or str(ident)) for ident in path}
                cycle = path[path.index(target_id):] + [target_id]
                raise DependencyError("Workflow reference cycle: " + " > ".join(str(names.get(item, item)) for item in cycle))
            target = index.get(target_id)
            if target is None:
                raise DependencyError(f"Unknown workflow reference {target_id} at node {node.get('id')}")
            mode = node.get("orchestrator_mode", "child")
            edge = {"from": ident, "node_id": node["id"], "to": target_id, "orchestrator_mode": mode}
            if edge not in tree["edges"]:
                tree["edges"].append(edge)
            pin(target, depth + 1, path + [target_id])

    pin(root, 1, [root_id])
    return tree


def resolve_dependencies(store: Any, *, definition: dict[str, Any] | None = None, name: str | None = None, substitute_name: str | None = None) -> dict[str, Any]:
    with store.definitions_tree_lock():
        return resolve_dependencies_locked(store, definition=definition, name=name, substitute_name=substitute_name)


def referencing_definitions(store: Any, workflow_id: str) -> list[str]:
    """Saved definitions whose Run workflow nodes reference this id."""
    referencing: list[str] = []
    for definition in store.list():
        for node in definition.get("nodes", []):
            if node.get("type") == "workflow" and node.get("workflow_ref", {}).get("workflow_id") == workflow_id:
                referencing.append(definition.get("name", ""))
                break
    return sorted(set(referencing))


def subtree(tree: dict[str, Any], workflow_id: str) -> dict[str, Any]:
    """The pinned subtree rooted at one workflow, edges restricted to it."""
    if workflow_id not in tree.get("workflows", {}):
        raise DependencyError(f"Workflow {workflow_id} is not part of this dependency tree")
    workflow_ids = {workflow_id}
    frontier = [workflow_id]
    while frontier:
        current = frontier.pop()
        for edge in tree.get("edges", []):
            if edge["from"] == current and edge["to"] not in workflow_ids:
                workflow_ids.add(edge["to"])
                frontier.append(edge["to"])
    return {
        "root_workflow_id": workflow_id,
        "workflows": {ident: tree["workflows"][ident] for ident in workflow_ids},
        "edges": [edge for edge in tree.get("edges", []) if edge["from"] in workflow_ids],
        "access": {ident: tree["access"][ident] for ident in workflow_ids if ident in tree.get("access", {})},
    }


def new_workflow_id() -> str:
    return uuid.uuid4().hex


def persist_workflow_id(saved: dict[str, Any], previous: dict[str, Any] | None) -> str:
    """The authoritative id: preserved, legacy-derived, or freshly minted."""
    if previous is not None:
        previous_id = previous.get("workflow_id") or workflow_identity(previous)
        return previous_id
    return new_workflow_id()
