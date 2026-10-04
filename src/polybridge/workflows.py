"""Durable visual workflows: agents judge, the supervisor owns execution.

All persisted definitions and execution state belong to Polybridge.  Dispatches are
reserved before spawning; an interrupted reservation is never silently replayed.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
import copy
from dataclasses import asdict
import fcntl
import hashlib
import json
import math
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid
from typing import Any

from . import backends, store as task_store

SCHEMA_VERSION = 1
TERMINAL = {"completed", "failed", "cancelled"}
ACTIVE = {"running", "paused", "needs_attention", "starting"}
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")


BUILDER_LAYOUT_GUIDANCE = (
    "Canvas layout: position.x and position.y are the node's top-left corner in logical points, "
    "not its center and not grid-cell indices. Both coordinates must be finite and nonnegative. "
    "The dot grid spacing is 10 points; place newly created or repositioned nodes on multiples of 10. "
    "Agent nodes measure 200 x 92 points (20 x 9.2 grid cells; reserve 20 x 10 cells). "
    "Parallel start and Parallel end are structural paired boundaries with parallel_group_id; reserve 12 x 8 grid cells. "
    "Start and End measure 72 x 72 points (7.2 x 7.2 grid cells; reserve 8 x 8 cells). "
    "Leave at least 40 points (4 grid cells) of clear space between node edges; do not overlap nodes. "
    "Arrange forward steps from left to right, with parallel branches on separate rows. "
    "The canvas expands automatically, so there is no fixed right or bottom boundary. "
    "Preserve every existing node position exactly unless the user explicitly asks to move or rearrange "
    "existing nodes. Adding nodes or refining instructions is not permission to move existing nodes. "
    "Apply the grid and spacing guidance to new nodes; if existing layout needs repair, report the issue."
)


_CALLER_UNSET = object()


class WorkflowError(ValueError):
    """Invalid definition, state transition, or execution decision."""


class RetryLimitReached(WorkflowError):
    def __init__(self, edge_id: str, used: int, limit: int):
        self.edge_id = edge_id
        super().__init__(f"Retry limit reached for connection {edge_id}: {used} of {limit} retries used")


class DispatchNotStarted(WorkflowError):
    """Scheduling stopped while waiting for a checkout lease; no agent was spawned."""


def role_prompt(role: str) -> str:
    """App-owned role guidance complements the user's task and custom step prompt."""
    defaults = {
        "planning": (
            "Create an actionable plan and task checklist; do not implement code. "
            "Use stable task IDs and keep every new task pending. "
            'Return ONLY JSON {"tasks":[{"id":"...","title":"...","description":"..."}],"summary":"..."}.'
        ),
        "implementation": (
            "Implement the relevant planned checklist tasks, using the supplied plan and feedback. "
            "Report completed task IDs with concrete implementation and observed validation evidence; "
            "do not change checklist statuses. "
            'Return JSON {"summary":"...","completed_task_ids":["..."]}.'
        ),
        "review": (
            "Review the supplied plan or implementation against the task and validation evidence. "
            "Report an approval or changes-needed verdict with concrete, prioritized findings, "
            "their evidence and any missing validation. Do not edit files."
        ),
        "task": (
            "Perform the requested check or bounded task. Report commands actually executed, "
            "observed results and failures; distinguish a failed check from an agent execution failure. "
            "Do not invent execution or validation results."
        ),
    }
    if role not in defaults:
        raise WorkflowError(f"Invalid agent role: {role}")
    return (
        "Do only this workflow step. Polybridge owns dispatch and routing. "
        "These role defaults supplement the custom step instructions and overall task; "
        "follow the configured permission limits. "
        + defaults[role]
        + " Only the orchestrator may change checklist statuses or mark tasks completed."
    )


FREEDOMS = ("read_only", "write_in_repo", "publish", "unrestricted")
ROLE_FREEDOM_DEFAULTS = {"planning": "read_only", "review": "read_only", "implementation": "write_in_repo", "task": "publish"}


def effective_freedom(node: dict[str, Any], ceiling: str = "unrestricted", *, permission_policy: str = "legacy_ceiling") -> str:
    """A node and all of its fallback agents share the run's access ceiling."""
    if ceiling not in FREEDOMS:
        raise WorkflowError("Workflow freedom must be read_only, write_in_repo, publish or unrestricted")
    requested = node.get("freedom", ROLE_FREEDOM_DEFAULTS[node.get("role", "task")])
    if requested not in FREEDOMS:
        raise WorkflowError("Invalid node freedom")
    if permission_policy not in {"legacy_ceiling", "saved_node"}:
        raise WorkflowError("Unknown workflow permission policy")
    effective = requested if permission_policy == "saved_node" else FREEDOMS[min(FREEDOMS.index(requested), FREEDOMS.index(ceiling))]
    if node.get("role") == "implementation" and effective == "read_only":
        raise WorkflowError("Implementation nodes cannot run read_only; choose at least write_in_repo for the run ceiling")
    return effective


def run_effective_freedom(run: dict[str, Any], node: dict[str, Any]) -> str:
    return effective_freedom(node, run.get("freedom", "write_in_repo"), permission_policy=run.get("permission_policy", "legacy_ceiling"))


def _identifier(value: Any) -> str:
    if not isinstance(value, str) or not NAME.fullmatch(value):
        raise WorkflowError("Identifiers must contain 1–100 letters, digits, dots, dashes or underscores")
    return value


def _workflow_name(value: Any) -> str:
    """Keep names exact and path-safe without weakening graph/run identifiers."""
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_. -]{0,99}", value) or value.endswith(" "):
        raise WorkflowError("Workflow names must contain 1–100 letters, digits, spaces, dots, dashes or underscores, start with a letter or digit, and have no trailing spaces")
    return value


def _positive(value: Any, field: str, maximum: int = 10000) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise WorkflowError(f"{field} must be an integer between 1 and {maximum}")
    return value


def _candidate(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise WorkflowError("Agent must be an object")
    candidate = copy.deepcopy(raw)
    unknown = set(candidate) - {"backend", "model", "reasoning_effort", "max_turns", "fallbacks"}
    if unknown:
        raise WorkflowError(f"Unknown agent settings: {sorted(unknown)}")
    if not isinstance(candidate.get("fallbacks", []), list):
        raise WorkflowError("Fallbacks must be an ordered array")
    backend = backends.get(candidate.get("backend", ""))
    if candidate.get("max_turns") is None and backend.capabilities.supports_turn_cap:
        candidate["max_turns"] = 100
    if candidate.get("model") is not None and (not isinstance(candidate["model"], str) or not candidate["model"].strip()):
        raise WorkflowError("Model must be a nonempty string")
    if candidate.get("max_turns") is not None:
        _positive(candidate["max_turns"], "max_turns")
    effort = candidate.get("reasoning_effort")
    if effort is not None and (not isinstance(effort, str) or effort not in backends.EFFORTS):
        raise WorkflowError("Invalid reasoning_effort")
    try:
        backends.reject_model(backend, candidate.get("model"))
        backends.reject_turn_cap(backend, candidate.get("max_turns"))
        backends.check_reasoning_effort(backend, effort)
    except backends.UnsupportedCapability as exc:
        if not candidate.get("fallbacks"):
            raise WorkflowError(str(exc)) from exc
    candidate["fallbacks"] = [_candidate(c) for c in candidate.get("fallbacks", [])]
    if any(c.get("fallbacks") for c in candidate["fallbacks"]):
        raise WorkflowError("Fallbacks must be an ordered flat list")
    return candidate


def validate_definition(raw: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize a structured graph, including nested fork/join regions."""
    if not isinstance(raw, dict):
        raise WorkflowError("Workflow definition must be an object")
    d = copy.deepcopy(raw)
    d["name"] = _workflow_name(d.get("name"))
    d["schema_version"] = SCHEMA_VERSION
    if d.get("routing_mode") not in (None, "explicit"):
        raise WorkflowError("Invalid routing_mode")
    d.setdefault("description", "")
    if not isinstance(d["description"], str):
        raise WorkflowError("Description must be text")
    d["orchestrator"] = _candidate(d.get("orchestrator", {"backend": "codex"}))
    d["max_parallel"] = _positive(d.get("max_parallel", 4), "max_parallel", 64)
    d["max_transitions"] = _positive(d.get("max_transitions", 100), "max_transitions")
    d["max_decision_attempts"] = _positive(d.get("max_decision_attempts", 3), "max_decision_attempts", 10)
    d["max_inspections"] = _positive(d.get("max_inspections", 20), "max_inspections", 1000)
    nodes = d.get("nodes", [])
    edges = d.get("connections", [])
    if not isinstance(nodes, list) or not isinstance(edges, list) or not nodes:
        raise WorkflowError("Workflow requires nodes and connections arrays")
    by_id: dict[str, Any] = {}
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise WorkflowError("Every node must be an object")
        node_id = _identifier(node.get("id"))
        if node_id in by_id:
            raise WorkflowError(f"Duplicate node {node_id}")
        by_id[node_id] = node
        if node.get("type") not in {"start", "agent", "join", "end", "parallel_start", "parallel_end"}:
            raise WorkflowError(f"Invalid node type: {node_id}")
        if node["type"] in {"parallel_start", "parallel_end"} and d.get("routing_mode") != "explicit":
            raise WorkflowError("Parallel boundaries require explicit routing_mode")
        if node["type"] == "start":
            node.setdefault("prompt", "")
            if not isinstance(node["prompt"], str):
                raise WorkflowError("Start prompt must be a string")
        node.setdefault("title", node_id)
        node.setdefault("position", {"x": 80 + 260 * index, "y": 80})
        if not isinstance(node["title"], str) or not isinstance(node["position"], dict) or any(not isinstance(node["position"].get(k), (int, float)) or isinstance(node["position"].get(k), bool) or not math.isfinite(node["position"][k]) or node["position"][k] < 0 for k in ("x", "y")):
            raise WorkflowError(f"Invalid title or canvas position (x and y must be finite and nonnegative): {node_id}")
        if node.get("branch_mode", "auto") not in {"auto", "choose_one", "all_matching"}:
            raise WorkflowError(f"Invalid branching mode: {node_id}")
        # Saved definitions/new launches adopt automatic routing. Existing run
        # snapshots are never revalidated, so their historical modes stay intact.
        node["branch_mode"] = "choose_one" if d.get("routing_mode") == "explicit" and node["type"] != "parallel_start" else "auto"
        if node["type"] == "agent":
            node.setdefault("optional", False)
            if not isinstance(node["optional"], bool):
                raise WorkflowError("Agent optional must be a boolean")
            node.setdefault("role", "task")
            if node["role"] not in {"planning", "review", "implementation", "task"}:
                raise WorkflowError(f"Invalid agent role: {node_id}")
            if node["role"] == "planning":
                for field in ("require_technical_plan", "require_tasks"):
                    node.setdefault(field, True)
                    if not isinstance(node[field], bool):
                        raise WorkflowError(f"Planning {field} must be a boolean")
            node["agent"] = _candidate(node.get("agent", {"backend": "codex"}))
            node.setdefault("instructions", "")
            if not isinstance(node["instructions"], str) or node.get("network") not in (None, True, False):
                raise WorkflowError(f"Invalid instructions or network: {node_id}")
            node.setdefault("freedom", ROLE_FREEDOM_DEFAULTS[node["role"]])
            if node["freedom"] not in FREEDOMS:
                raise WorkflowError("Workflow nodes support read_only, write_in_repo, publish or unrestricted")
            if node["role"] == "implementation" and node["freedom"] == "read_only":
                raise WorkflowError("Implementation nodes cannot use read_only")
            node.setdefault("session_mode", "agent_decides")
            if node["session_mode"] not in {"resume", "fresh", "agent_decides", "continue_previous"}:
                raise WorkflowError(f"Invalid session mode: {node_id}")
            node["max_attempts"] = _positive(node.get("max_attempts", 3), "max_attempts")
            timeout = node.get("timeout_seconds")
            if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 0):
                raise WorkflowError("timeout_seconds must be a nonnegative integer")
            node["max_context_questions"] = _positive(node.get("max_context_questions", 10), "max_context_questions", 100)
    starts = [n["id"] for n in nodes if n["type"] == "start"]
    if len(starts) != 1 or not any(n["type"] == "end" for n in nodes):
        raise WorkflowError("Workflow needs exactly one Start and at least one End")
    outgoing: dict[str, list[dict[str, Any]]] = {n: [] for n in by_id}
    edge_ids: set[str] = set()
    for edge in edges:
        if not isinstance(edge, dict):
            raise WorkflowError("Every connection must be an object")
        eid = _identifier(edge.get("id"))
        if eid in edge_ids:
            raise WorkflowError(f"Duplicate connection {eid}")
        edge_ids.add(eid)
        if edge.get("source") not in by_id or edge.get("target") not in by_id:
            raise WorkflowError(f"Connection {eid} references an unknown node")
        if by_id[edge["source"]]["type"] == "end" or by_id[edge["target"]]["type"] == "start":
            raise WorkflowError("End cannot have outputs and Start cannot have inputs")
        if "max_retries" in edge and (not isinstance(edge["max_retries"], int) or isinstance(edge["max_retries"], bool) or edge["max_retries"] < 0):
            raise WorkflowError(f"max_retries must be a nonnegative integer: {eid}")
        edge.setdefault("condition", "")
        edge.setdefault("default", False)
        edge.setdefault("backward", False)
        if not isinstance(edge["condition"], str) or not isinstance(edge["default"], bool) or not isinstance(edge["backward"], bool):
            raise WorkflowError(f"Invalid condition or connection flags: {eid}")
        outgoing[edge["source"]].append(edge)
    for nid, node in by_id.items():
        if node["type"] != "end" and not outgoing[nid]:
            raise WorkflowError(f"Node {nid} has no outgoing connection")
        if sum(bool(e["default"]) for e in outgoing[nid]) > 1:
            raise WorkflowError(f"Node {nid} has multiple default connections")
    # Natural loops return to a node that dominates their source. Infer these
    # from topology, not canvas positions or the old editable backward flag.
    predecessors: dict[str, set[str]] = {nid: set() for nid in by_id}
    reachable = {starts[0]}
    frontier = [starts[0]]
    for edge in edges:
        predecessors[edge["target"]].add(edge["source"])
    while frontier:
        source = frontier.pop()
        for edge in outgoing[source]:
            if edge["target"] not in reachable:
                reachable.add(edge["target"])
                frontier.append(edge["target"])
    if reachable != set(by_id):
        raise WorkflowError("All nodes must be reachable from Start")
    dominators = {nid: ({nid} if nid == starts[0] else set(by_id)) for nid in by_id}
    changed = True
    while changed:
        changed = False
        for nid in by_id:
            if nid == starts[0]:
                continue
            shared = set.intersection(*(dominators[p] for p in predecessors[nid]))
            updated = {nid} | shared
            if updated != dominators[nid]:
                dominators[nid] = updated
                changed = True
    for edge in edges:
        edge["backward"] = edge["target"] in dominators[edge["source"]]
    # Forward graph must be acyclic. All nodes must be reachable, with a path to End.
    visited: set[str] = set()
    visiting: set[str] = set()
    def visit(nid: str) -> None:
        if nid in visiting:
            raise WorkflowError("Ambiguous loop: cycles must return to a step that dominates the retrying step")
        if nid in visited:
            return
        visiting.add(nid)
        for edge in outgoing[nid]:
            if not edge["backward"]:
                visit(edge["target"])
        visiting.remove(nid)
        visited.add(nid)
    visit(starts[0])
    if visited != set(by_id):
        raise WorkflowError("All nodes must be forward-reachable from Start")
    # A reachable node with only backward outputs is a trapped loop, not a
    # terminating workflow. Every node must have a forward route to an End.
    can_end = {nid for nid, node in by_id.items() if node["type"] == "end"}
    while True:
        expanded = can_end | {nid for nid in by_id if any(not edge["backward"] and edge["target"] in can_end for edge in outgoing[nid])}
        if expanded == can_end:
            break
        can_end = expanded
    if can_end != set(by_id):
        raise WorkflowError("Every node must have a forward path to End: " + ", ".join(sorted(set(by_id) - can_end)))
    # Parallel arrows converge at their nearest common forward postdominator.
    # These are ordinary nodes; generation state supplies the barrier at runtime.
    postdominators: dict[str, set[str]] = {}
    def postdom(nid: str) -> set[str]:
        if nid not in postdominators:
            successors = [edge["target"] for edge in outgoing[nid] if not edge["backward"]]
            shared = set.intersection(*(postdom(target) for target in successors)) if successors else set()
            postdominators[nid] = {nid} | shared
        return postdominators[nid]
    if d.get("routing_mode") == "explicit":
        _validate_parallel_boundaries(d, outgoing, postdom)
    else:
        for nid, node in by_id.items():
            forward_edges = [edge for edge in outgoing[nid] if not edge["backward"]]
            if len(forward_edges) <= 1:
                node.pop("join_id", None)
                continue
            common = set.intersection(*(postdom(edge["target"]) for edge in forward_edges))
            closest = [candidate for candidate in common if common <= postdom(candidate)]
            if len(closest) != 1:
                raise WorkflowError(f"Parallel branches from {nid} must converge at one common node before ending")
            legacy = node.get("join_id")
            if legacy in by_id and by_id[legacy]["type"] == "join" and legacy != closest[0]:
                raise WorkflowError(f"Cross-branch connection at split {nid}: its legacy Join follows an earlier convergence; connect branches directly to their first shared node")
            node["join_id"] = closest[0]
        # Conditional alternatives may overlap in the *potential* graph. Safety is
        # checked against actual selections and live fork generations at dispatch.
        inferred_joins = {node.get("join_id") for node in nodes}
        for node in nodes:
            if node["type"] == "join" and node["id"] not in inferred_joins:
                raise WorkflowError(f"Join {node['id']} is outside its matching split")
    validate_optional_nodes(d)
    return d


def _validate_parallel_boundaries(definition: dict[str, Any], outgoing: dict[str, Any], postdom: Any) -> None:
    """Validate structured single-entry regions, including nested split/merge pairs."""
    nodes = {n["id"]: n for n in definition["nodes"]}
    pairs: dict[str, dict[str, str]] = {}
    for node in nodes.values():
        node.pop("join_id", None)
        if node["type"] == "join":
            raise WorkflowError("Use Parallel start and Parallel end instead of Join")
        if node["type"] in {"parallel_start", "parallel_end"}:
            group = _identifier(node.get("parallel_group_id"))
            if node["type"] in pairs.setdefault(group, {}):
                raise WorkflowError(f"Duplicate {node['type']} for parallel group {group}")
            pairs[group][node["type"]] = node["id"]
    for group, pair in pairs.items():
        if set(pair) != {"parallel_start", "parallel_end"}:
            raise WorkflowError(f"Parallel group {group} requires matching start and end")
        start, end = pair["parallel_start"], pair["parallel_end"]
        edges = [e for e in outgoing[start] if not e["backward"]]
        if len(edges) < 2 or len(edges) != len(outgoing[start]):
            raise WorkflowError(f"Parallel start {start} requires at least two forward branches and no retry outputs")
        if end not in postdom(start):
            raise WorkflowError(f"Every branch from Parallel start {start} must reach matching Parallel end {end}")
        nodes[start]["join_id"] = end
        regions = [_forward_reachable(definition, e["target"], end) for e in edges]
        if any(not region for region in regions):
            raise WorkflowError(f"Parallel start {start} cannot have an empty branch")
        for index, region in enumerate(regions):
            if any(region & previous for previous in regions[:index]):
                raise WorkflowError(f"Parallel group {group} has crossing or overlapping sibling branches")
            for nid in region:
                nested = nodes[nid]
                if nested["type"] == "end":
                    raise WorkflowError(f"Parallel group {group} reaches workflow End before its matching end")
                if nested["type"] in {"parallel_start", "parallel_end"}:
                    partner = pairs[nested["parallel_group_id"]]
                    if set(partner.values()) - region:
                        raise WorkflowError(f"Parallel group {group} contains an improperly nested boundary")
            for edge in definition["connections"]:
                source, target = edge["source"], edge["target"]
                if target in region and source not in region and not (source == start and edge in edges):
                    raise WorkflowError(f"External entry into parallel branch {group}: {edge['id']}")
                if source in region and target not in region and target != end:
                    raise WorkflowError(f"Connection escapes parallel branch {group}: {edge['id']}")
                if source == end and target in region:
                    raise WorkflowError(f"Retry cannot enter a closed parallel branch: {edge['id']}")
        for edge in definition["connections"]:
            if edge["target"] == end and not any(edge["source"] in r for r in regions):
                raise WorkflowError(f"External entry into Parallel end {end}")


def _forward_reachable(definition: dict[str, Any], start: str, stop: str | None = None) -> set[str]:
    """Reachability before a barrier; a direct edge to it has an empty region."""
    outgoing: dict[str, list[str]] = {}
    for edge in definition["connections"]:
        if not edge.get("backward"):
            outgoing.setdefault(edge["source"], []).append(edge["target"])
    reachable: set[str] = set()
    pending = [start]
    while pending:
        nid = pending.pop()
        if nid == stop or nid in reachable:
            continue
        reachable.add(nid)
        pending.extend(outgoing.get(nid, []))
    return reachable


def _selection_join(definition: dict[str, Any], targets: list[str]) -> str:
    outgoing: dict[str, list[str]] = {}
    for edge in definition["connections"]:
        if not edge.get("backward"):
            outgoing.setdefault(edge["source"], []).append(edge["target"])
    cache: dict[str, set[str]] = {}
    def postdom(nid: str) -> set[str]:
        if nid not in cache:
            successors = outgoing.get(nid, [])
            cache[nid] = {nid} | (set.intersection(*(postdom(n) for n in successors)) if successors else set())
        return cache[nid]
    common = set.intersection(*(postdom(target) for target in targets))
    closest = [candidate for candidate in common if common <= postdom(candidate)]
    if len(closest) != 1:
        raise WorkflowError("Selected parallel paths must converge at one common node")
    return closest[0]



def _required_region(definition: dict[str, Any], region: set[str]) -> bool:
    agents = [n for n in definition["nodes"] if n["id"] in region and n["type"] == "agent"]
    return bool(agents) and not any(n.get("optional", False) for n in agents)


def _optional_suffix(definition: dict[str, Any], node_id: str, join_id: str) -> bool:
    region = _forward_reachable(definition, node_id, join_id)
    return node_id in region and not any(n["type"] == "agent" and not n.get("optional", False) for n in definition["nodes"] if n["id"] in region)


def validate_optional_nodes(definition: dict[str, Any]) -> None:
    optional = {n["id"] for n in definition["nodes"] if n.get("optional", False) and n["type"] == "agent"}
    if not optional:
        return
    valid: set[str] = set()
    for split in definition["nodes"]:
        if definition.get("routing_mode") == "explicit" and split["type"] != "parallel_start":
            continue
        edges = [e for e in definition["connections"] if e["source"] == split["id"] and not e.get("backward")]
        for i, edge in enumerate(edges):
            for other in edges[:i]:
                join = _selection_join(definition, [edge["target"], other["target"]])
                left = _forward_reachable(definition, edge["target"], join)
                right = _forward_reachable(definition, other["target"], join)
                if left & right:
                    continue
                for region, sibling in ((left, right), (right, left)):
                    if _required_region(definition, sibling):
                        valid.update(nid for nid in optional & region if _optional_suffix(definition, nid, join))
    invalid = optional - valid
    if invalid:
        raise WorkflowError("Optional nodes need a safe parallel branch with a required sibling and no required successor before convergence: " + ", ".join(sorted(invalid)))


def optional_failure_join(run: dict[str, Any], node: dict[str, Any], token: dict[str, Any]) -> str | None:
    if not node.get("optional", False) or run["status"] != "running" or not token.get("stack"):
        return None
    group = run["joins"].get(token["stack"][-1])
    if not group or not group.get("selected_targets"):
        return None  # Historical generations cannot establish a safe selected required sibling.
    join = group["join_id"]
    regions = [_forward_reachable(run["definition"], target, join) for target in group["selected_targets"]]
    own = next((region for region in regions if node["id"] in region), None)
    if own is None or not _optional_suffix(run["definition"], node["id"], join):
        return None
    if any(region is not own and _required_region(run["definition"], region) for region in regions):
        return join
    return None

def retry_budget(run: dict[str, Any], edge: dict[str, Any]) -> dict[str, Any]:
    used = run.get("retry_counts", {}).get(edge["id"], 0)
    limit = edge.get("max_retries")
    effective = limit + run.get("retry_grants", {}).get(edge["id"], 0) if limit is not None else None
    return {"retry_used": used, "retry_limit": effective, "retry_remaining": max(0, effective - used) if effective is not None else None}


def validate_retry_budget(run: dict[str, Any], selected: list[dict[str, Any]]) -> None:
    for edge in selected:
        if edge.get("backward"):
            budget = retry_budget(run, edge)
            if budget["retry_limit"] is not None and budget["retry_used"] >= budget["retry_limit"]:
                raise RetryLimitReached(edge["id"], budget["retry_used"], budget["retry_limit"])


def validate_selection(definition: dict[str, Any], node: dict[str, Any], selected: list[dict[str, Any]], token: dict[str, Any], joins: dict[str, Any]) -> str | None:
    """Validate actual paths before publishing decisions and again before advancing."""
    if not selected:
        raise WorkflowError("No continuation selected")
    legal = {edge["id"] for edge in definition["connections"] if edge["source"] == node["id"]}
    if any(edge["id"] not in legal for edge in selected):
        raise WorkflowError("Continuation selected an illegal outgoing connection")
    if node.get("branch_mode") == "choose_one" and len(selected) != 1:
        raise WorkflowError("Historical run permits exactly one connection")
    if definition.get("routing_mode") == "explicit":
        if node["type"] == "parallel_start":
            forward = {e["id"] for e in definition["connections"] if e["source"] == node["id"] and not e.get("backward")}
            if {e["id"] for e in selected} != forward:
                raise WorkflowError("Parallel start must select all forward branches")
        elif len(selected) != 1:
            raise WorkflowError("Ordinary nodes must choose exactly one continuation")
    retry = [edge for edge in selected if edge.get("backward")]
    if retry:
        if len(selected) != 1:
            raise WorkflowError("A retry connection must be selected exclusively")
        reachable = _forward_reachable(definition, retry[0]["target"])
        for generation in token.get("stack", []):
            group = joins.get(generation)
            if group is None:
                raise WorkflowError("Active parallel generation is unavailable")
            split_id = group.get("split_id")
            if split_id is None:
                historical = [n["id"] for n in definition["nodes"] if n.get("join_id") == group["join_id"]]
                if len(historical) != 1:
                    raise WorkflowError("Cannot identify the historical parallel split safely")
                split_id = historical[0]
            if split_id in reachable:
                raise WorkflowError(f"Retry cannot escape active parallel split {split_id}")
        return None
    if len(selected) == 1:
        return None
    join_id = _selection_join(definition, [edge["target"] for edge in selected])
    regions = [_forward_reachable(definition, edge["target"], join_id) for edge in selected]
    for index, region in enumerate(regions):
        for other in regions[:index]:
            overlap = region & other
            if overlap:
                raise WorkflowError("Selected parallel paths overlap before convergence: " + ", ".join(sorted(overlap)))
    optional_ids = {n["id"] for n in definition["nodes"] if n["type"] == "agent" and n.get("optional", False)}
    if any(region & optional_ids for region in regions) and not any(_required_region(definition, region) for region in regions):
        raise WorkflowError("Selected optional parallel branches need an actually selected required sibling")
    return join_id


def _write(path: Path, value: Any) -> None:
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("x", encoding="utf-8") as f:
            os.chmod(temp, 0o600)
            json.dump(value, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temp.unlink(missing_ok=True)


class WorkflowStore:
    def __init__(self, root: Path | None = None):
        self.root = Path(root) if root is not None else Path.home() / ".polybridge"
        self.definitions = self.root / "workflows"
        self.runs = self.root / "workflow-runs"
        self.leases = self.root / "workflow-leases"
        self.owners = self.root / "workflow-owners"
        for directory in (self.definitions, self.runs, self.leases, self.owners):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)

    @contextmanager
    def lock(self, key: str):
        path = self.root / f".workflow-{hashlib.sha256(key.encode()).hexdigest()}.lock"
        with path.open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def get(self, name: str) -> dict[str, Any]:
        return json.loads((self.definitions / f"{_workflow_name(name)}.json").read_text())

    def list(self) -> list[dict[str, Any]]:
        return [json.loads(p.read_text()) for p in sorted(self.definitions.glob("*.json"))]

    def save(self, name: str, definition: dict[str, Any], expected_revision: int | None = None) -> dict[str, Any]:
        d = validate_definition({**definition, "name": _workflow_name(name)})
        d["execution_contract"] = "delegation"
        with self.lock(f"definition:{name}"):
            path = self.definitions / f"{name}.json"
            old = json.loads(path.read_text()) if path.exists() else None
            revision = old["revision"] if old else 0
            if expected_revision != revision and (old or expected_revision not in (None, 0)):
                raise WorkflowError(f"Stale workflow revision: expected {expected_revision}, current {revision}")
            d["revision"] = revision + 1
            d["updated_at"] = time.time()
            _write(path, d)
        return d

    def delete(self, name: str) -> dict[str, Any]:
        with self.lock(f"definition:{name}"):
            (self.definitions / f"{_workflow_name(name)}.json").unlink()
        return {"deleted": name}

    def get_run(self, run_id: str) -> dict[str, Any]:
        run = json.loads((self.runs / f"{_identifier(run_id)}.json").read_text())
        run["settling"] = any(t.get("status") in {"reserved", "running", "uncertain"} for a in run["activations"] for t in a["tasks"])
        return run

    def list_runs(self, *, strict: bool = False) -> list[dict[str, Any]]:
        runs = []
        for path in self.runs.glob("*.json"):
            try:
                runs.append(self.get_run(path.stem))
            except (OSError, ValueError, KeyError, TypeError):
                if strict:
                    raise WorkflowError(f"Workflow ownership is unavailable: {path.name}") from None
                logging.getLogger(__name__).warning("Skipping unreadable workflow record %s", path.name)
        return sorted(runs, key=lambda r: r["created_at"], reverse=True)

    def update_run(self, run_id: str, mutator: Any, event: str, detail: Any = None) -> dict[str, Any]:
        with self.lock(f"run:{run_id}"):
            run = self.get_run(run_id)
            mutator(run)
            run["updated_at"] = time.time()
            run["sequence"] = run.get("sequence", 0) + 1
            run.pop("settling", None)
            _write(self.runs / f"{run_id}.json", run)
            for activation in run.get("activations", []):
                for task in activation.get("tasks", []):
                    _write(self.owners / f"{_identifier(task['task_id'])}.json", {"workflow_run_id": run_id})
            with (self.runs / f"{run_id}.jsonl").open("a", encoding="utf-8") as f:
                f.write(json.dumps({"sequence": run["sequence"], "time": run["updated_at"], "event": event, "detail": detail}) + "\n")
                f.flush()
                os.fsync(f.fileno())
            run["settling"] = any(t.get("status") in {"reserved", "running", "uncertain"} for a in run["activations"] for t in a["tasks"])
            return run

    def create_run(self, definition: dict[str, Any], prompt: str, repo_path: Path, *, freedom: str = "write_in_repo", network: bool | None = None, kind: str = "workflow", permission_policy: str = "legacy_ceiling", caller: Any = None) -> dict[str, Any]:
        if permission_policy not in {"saved_node", "legacy_ceiling"}:
            raise WorkflowError("Unknown workflow permission policy")
        if freedom not in FREEDOMS:
            raise WorkflowError("Workflow freedom must be read_only, write_in_repo, publish or unrestricted")
        for node in definition.get("nodes", []):
            if node.get("type") == "agent":
                effective_freedom(node, freedom, permission_policy=permission_policy)
        if not Path(repo_path).is_dir():
            raise WorkflowError("Repository directory does not exist")
        rid = uuid.uuid4().hex
        run = {"workflow_run_id": rid, "kind": kind, "name": definition["name"], "definition": copy.deepcopy(definition), "revision": definition.get("revision", 0), "prompt": prompt, "repo_path": str(Path(repo_path).resolve()), "freedom": freedom, "network": network, "status": "starting", "created_at": time.time(), "updated_at": time.time(), "sequence": 0, "transitions": 0, "activations": [], "decisions": [], "sessions": {}, "suppressed_candidates": [], "pending": [], "joins": {}, "instructions": "", "attempt_grants": {}, "supervisor_pid": None}
        if caller is not None:
            run.update(caller_record=asdict(caller.record), caller_method=caller.method)
        run["permission_policy"] = permission_policy
        run["definition_hash"] = hashlib.sha256(json.dumps(definition, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        run["tasks"] = []
        if kind == "workflow":
            run["execution_contract"] = "delegation"
            run["runner_policy"] = "guided"
            run["execution_policy"] = "visit"
        run.update(retry_counts={}, retry_grants={})
        _write(self.runs / f"{rid}.json", run)
        return run

    def control(self, run_id: str, action: str, instructions: str | None = None, additional_attempts: int = 0, *, decision_id: str | None = None, interaction_owner: str | None = None) -> dict[str, Any]:
        if action not in {"pause", "resume", "cancel", "recover"}:
            raise WorkflowError("Unknown workflow action")
        if not isinstance(additional_attempts, int) or isinstance(additional_attempts, bool) or additional_attempts < 0:
            raise WorkflowError("additional_attempts must be a nonnegative integer")
        if action in {"resume", "recover", "cancel"}:
            observed = self.get_run(run_id)
            if observed.get("supervisor_identity"):
                from . import identity
                if identity.identity_check(observed["supervisor_identity"]) == "undecidable":
                    raise WorkflowError("Supervisor identity is uncertain; cannot " + ("cancel" if action == "cancel" else "resume or recover"))
            if action in {"resume", "recover"} and not _supervisor_present(observed):
                self.reconcile_run(run_id)
        control_detail: dict[str, Any] = {"instructions": instructions, "additional_attempts": additional_attempts, "retry_grants": {}}
        def change(r: dict[str, Any]) -> None:
            if r.get("execution_contract") == "delegation" and r.get("interaction_owner") and interaction_owner is not None and interaction_owner != r["interaction_owner"]:
                raise WorkflowError("Workflow interaction belongs to its original caller")
            if action in {"resume", "recover", "cancel"}:
                if r.get("supervisor_identity"):
                    from . import identity
                    if identity.identity_check(r["supervisor_identity"]) == "undecidable":
                        raise WorkflowError("Supervisor identity is uncertain; cannot " + ("cancel" if action == "cancel" else "resume or recover"))
                if action in {"resume", "recover"}:
                    _require_delegation_control(r)
            if action == "recover":
                if r.get("kind") == "builder" or r["status"] != "failed":
                    raise WorkflowError("Only failed delegation workflows can recover")
                if not isinstance(instructions, str) or not instructions.strip():
                    raise WorkflowError("Recovery requires a nonempty reason")
            elif r["status"] in TERMINAL:
                raise WorkflowError("Workflow is terminal")
            if action in {"resume", "recover"}:
                from . import control as task_control
                reservations = task_control.takeover_reservations(self.root / "tasks")
                if any(task.get("task_id") in reservations for activation in r["activations"] for task in activation["tasks"]):
                    raise WorkflowError("Workflow sessions are reserved for human takeover; cannot resume or recover")
                if action == "resume" and r["status"] not in {"paused", "needs_attention", "needs_input"}:
                    raise WorkflowError("Only paused/attention/input workflows can resume")
                if any(a["status"] in {"reserved", "uncertain", "running"} or any(t["status"] in {"reserved", "uncertain", "running"} for t in a["tasks"]) for a in r["activations"]):
                    raise WorkflowError("Unresolved dispatches must be reconciled before resume")
                if r["status"] == "needs_input":
                    if not isinstance(instructions, str) or not instructions.strip():
                        raise WorkflowError("Resuming needs_input requires an answer or reason")
                    if decision_id != r.get("input_decision_id"):
                        raise WorkflowError("Resuming needs_input requires the current input decision_id")
                r["status"] = "running"
                r["instructions"] = instructions or ""
                for token in r.get("pending", []):
                    token["decision_attempts"] = 0
                    token.pop("decision_error", None)
                for activation in r["activations"]:
                    for question in activation.get("questions", []):
                        if question["status"] == "waiting" or question.get("answer_delivery_state") == "not_started":
                            question["decision_attempts"] = 0
                            question.pop("decision_error", None)
                r.pop("input_question", None)
                r.pop("input_decision_id", None)
                r.pop("failure_reason", None)
                r["suppressed_candidates"] = []
                exhausted_edges = r.pop("exhausted_retry_edges", [])
                if additional_attempts:
                    _positive(additional_attempts, "additional_attempts")
                    if isinstance(instructions, str) and instructions.strip():
                        from .workflow_delegation import settled
                        for execution in r["activations"]:
                            if execution["role"] == "node" and settled(execution) and execution.get("node_result", {}).get("status") == "blocked":
                                r.setdefault("blocker_retry_authorizations", {})[execution["id"]] = {"node_id": execution["node_id"], "reason": instructions.strip(), "granted_at": time.time()}
                            if execution["role"] == "node" and settled(execution) and execution.get("node_result", {}).get("result", {}).get("failure_kind") == "protocol":
                                r.setdefault("protocol_retry_authorizations", {})[execution["id"]] = {"node_id": execution["node_id"], "reason": instructions.strip(), "granted_at": time.time()}
                    for n in r["definition"]["nodes"]:
                        r["attempt_grants"][n["id"]] = r["attempt_grants"].get(n["id"], 0) + additional_attempts
                    r["transition_grant"] = r.get("transition_grant", 0) + additional_attempts
                    for edge_id in exhausted_edges:
                        prior = r.get("retry_grants", {}).get(edge_id, 0)
                        r.setdefault("retry_grants", {})[edge_id] = prior + additional_attempts
                        control_detail["retry_grants"][edge_id] = {"additional": additional_attempts, "previous": prior, "total": prior + additional_attempts}
            else:
                if action == "pause" and r["status"] in {"cancelling", "needs_input"}:
                    return
                r["status"] = "paused" if action == "pause" else "cancelling"
                if instructions:
                    r["instructions"] = instructions
        result = self.update_run(run_id, change, f"control:{action}", control_detail)
        if action in {"resume", "recover", "cancel"} and not _supervisor_present(result):
            _launch(self, run_id)
        return result

    def pinned_tasks(self) -> set[str]:
        return {t["task_id"] for r in self.list_runs() if r["status"] not in TERMINAL or r.get("settling") or any(t["status"] in {"reserved", "running", "uncertain"} for a in r["activations"] for t in a["tasks"]) for a in r["activations"] for t in a["tasks"]}

    def task_association(self, task_id: str, *, strict: bool = False, _visited: set[str] | None = None) -> dict[str, Any] | None:
        visited = set() if _visited is None else _visited
        if task_id in visited:
            raise WorkflowError("Cyclic task lineage cannot establish workflow ownership")
        visited.add(task_id)
        indexed = self.owners / f"{_identifier(task_id)}.json"
        if indexed.exists():
            receipt = json.loads(indexed.read_text())
            runs = [self.get_run(receipt["workflow_run_id"])]
        else:
            runs = self.list_runs()
        for r in runs:
            for a in r["activations"]:
                for t in a["tasks"]:
                    if t["task_id"] == task_id:
                        return {"workflow_run_id": r["workflow_run_id"], "workflow_node_id": a["node_id"], "workflow_role": a["role"], "execution_contract": r.get("execution_contract"), "node_id": a["node_id"], "role": a["role"], "activation_id": a["id"], "status": r["status"], "workflow_status": r["status"], "workflow_settling": r.get("settling", False)}
        if indexed.exists():
            raise WorkflowError("Workflow ownership receipt no longer matches its run")
        record = task_store.read(self.root / "tasks", task_id)
        if record is not None:
            for ancestor in dict.fromkeys((record.parent_task_id, record.spawned_by)):
                if ancestor:
                    association = self.task_association(ancestor, strict=strict, _visited=set(visited))
                    if association is not None:
                        return {**association, "workflow_ancestor_task_id": ancestor}
        if strict:
            self.list_runs(strict=True)
        return None

    def task_owner(self, task_id: str, *, strict: bool = False) -> dict[str, Any] | None:
        return self.task_association(task_id, strict=strict)

    def abandon_dispatch(self, run_id: str, execution_id: str, task_id: str, reason: str, confirm_no_process: bool) -> dict[str, Any]:
        """Human CLI reconciliation after independently confirming a recordless dispatch is stopped."""
        if confirm_no_process is not True or not isinstance(reason, str) or not reason.strip():
            raise WorkflowError("Explicit no-process confirmation and a nonempty reason are required")
        def abandon(r: dict[str, Any]) -> None:
            if _supervisor_present(r):
                raise WorkflowError("Supervisor is live or uncertain; cannot abandon dispatch")
            activation = next((a for a in r["activations"] if a["id"] == execution_id), None)
            task = next((t for t in activation["tasks"] if t["task_id"] == task_id), None) if activation else None
            if task is None or task.get("status") not in {"reserved", "uncertain"}:
                raise WorkflowError("Only a named unresolved recordless dispatch can be abandoned")
            if task_store.read(self.root / "tasks", task_id) is not None:
                raise WorkflowError("Recorded tasks require process reconciliation, not abandonment")
            update_task_state(activation, task, {"status": "not_started", "reconciliation": {"source": "human_confirmation", "reason": reason.strip(), "confirmed_no_process": True, "time": time.time()}})
            if all(t["status"] == "not_started" for t in activation["tasks"]):
                activation["status"] = "not_started"
            if r["status"] not in TERMINAL:
                r.update(status="needs_attention", attention_reason="Dispatch abandoned after human confirmation; explicit resume is required")
        return self.update_run(run_id, abandon, "dispatch_abandoned", {"execution_id": execution_id, "task_id": task_id, "reason": reason.strip()})

    def reconcile_run(self, run_id: str) -> dict[str, Any]:
        """Recover only outcomes positively recorded by their original task owner."""
        def reconcile(r: dict[str, Any]) -> None:
            for activation in r["activations"]:
                for task in activation["tasks"]:
                    if task["status"] not in {"reserved", "running", "uncertain"}:
                        continue
                    record = task_store.read(self.root / "tasks", task["task_id"])
                    if record and task_liveness(self.root / "tasks", record)["process_alive"] is False:
                        snapshot = task_store.snapshot(self.root / "tasks", record)
                        liveness = task_liveness(self.root / "tasks", record)
                        if not liveness["outcome_known"]:
                            snapshot.update(status="failed", outcome_unknown=True, recovery_reason="Managed process is dead but its owner did not record an authoritative outcome")
                        if task.get("timeout_requested_at") and liveness["outcome_known"] and snapshot.get("status") != "completed":
                            node = next(n for n in r["definition"]["nodes"] if n["id"] == activation["node_id"])
                            snapshot.update(status="failed", timed_out=True, failure_kind="timeout", timeout_seconds=node.get("timeout_seconds"), summary="Node timed out; cancellation settled after supervisor interruption")
                        update_task_state(activation, task, {"status": snapshot["status"], "result": snapshot, "liveness": liveness})
                    elif record is None and task.get("dispatch_stage") == "preparing":
                        update_task_state(activation, task, {"status": "not_started", "recovery_reason": "Supervisor stopped before requesting spawn"})
                    else:
                        update_task_state(activation, task, {"status": "uncertain"})
                if activation["status"] in {"running", "reserved", "uncertain"} and all(t["status"] not in {"reserved", "running", "uncertain"} for t in activation["tasks"]):
                    activation["status"] = "completed" if activation["tasks"] and activation["tasks"][-1]["status"] == "completed" else "failed"
                if activation["role"] == "node":
                    owner = next((t for t in r["pending"] if t["id"] == (activation.get("token") or {}).get("id")), None)
                    if owner and (owner.get("execution_activation_id") not in {None, activation["id"]} or owner.get("retry_of_execution_id") == activation["id"] or activation.get("resolved_by_execution_id")):
                        continue  # Keep settled history without reclaiming a retry's token.
                if activation["role"] == "node" and activation["status"] == "completed" and activation["tasks"]:
                    token = next((t for t in r["pending"] if t["id"] == (activation.get("token") or {}).get("id")), None)
                    if token and not token.get("execution_complete"):
                        token.update(recovered_result=activation["tasks"][-1]["result"], execution_activation_id=activation["id"])
                        token.pop("recovered_failed_result", None)
                if activation["role"] == "node" and activation["status"] == "failed" and activation["tasks"]:
                    node = next(n for n in r["definition"]["nodes"] if n["id"] == activation["node_id"])
                    observed = activation["tasks"][-1].get("result", {})
                    token = next((t for t in r["pending"] if t["id"] == (activation.get("token") or {}).get("id")), None)
                    if node.get("optional", False) and observed.get("status") == "failed" and token and not token.get("execution_complete") and optional_failure_join({**r, "status": "running"}, node, token):
                        token.update(recovered_failed_result=observed, execution_activation_id=activation["id"])
        result = self.update_run(run_id, reconcile, "reconciled")
        if result.get("execution_contract") == "delegation":
            from .workflow_delegation import reconcile_delegation
            return self.update_run(run_id, reconcile_delegation, "delegation_reconciled")
        return result


def update_task_state(activation: dict[str, Any], task: dict[str, Any], values: dict[str, Any]) -> None:
    """Keep a reply reservation and its durable question delivery state together."""
    task.update(values)
    question = next((q for q in activation.get("questions", []) if q.get("reply_task_id") == task["task_id"]), None)
    if question and values.get("status"):
        state = values["status"]
        question["answer_delivery_state"] = "settled" if state in {"completed", "failed", "cancelled", "timed_out"} else state


def _require_delegation_control(run: dict[str, Any]) -> None:
    if run.get("kind") != "builder" and run.get("execution_contract") != "delegation":
        raise WorkflowError("Historical workflow runs cannot resume under the delegation contract; start a new run")


def migrate_workflows(root: Path | None = None) -> dict[str, Any]:
    from .workflow_delegation import migrate
    return migrate(WorkflowStore(root))


def _alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _supervisor_present(run: dict[str, Any]) -> bool:
    from . import identity
    recorded = run.get("supervisor_identity")
    if recorded:
        return identity.identity_check(recorded) != "dead"
    return _alive(run.get("supervisor_pid"))


def _launch(storage: WorkflowStore, run_id: str) -> None:
    log = storage.runs / f"{run_id}.supervisor.log"
    with log.open("ab") as output:
        subprocess.Popen([sys.executable, "-m", "polybridge.workflows", "supervise", run_id, "--root", str(storage.root)], stdin=subprocess.DEVNULL, stdout=output, stderr=output, start_new_session=True, close_fds=True)


async def start_workflow(name: str, prompt: str, repo_path: Path, *, overrides: dict[str, Any] | None = None, freedom: str | None = None, network: bool | None = None, root: Path | None = None, interaction_owner: str = "caller", definition_snapshot: dict[str, Any] | None = None, _verified_caller: Any = _CALLER_UNSET) -> dict[str, Any]:
    if interaction_owner not in {"caller", "monitor"}:
        raise WorkflowError("Invalid interaction owner")
    if not isinstance(prompt, str) or not prompt.strip():
        raise WorkflowError("Workflow prompt must be nonempty")
    storage = WorkflowStore(root)
    if any(r.get("kind") != "builder" and r.get("execution_contract") != "delegation" and r["status"] not in TERMINAL for r in storage.list_runs()):
        raise WorkflowError("Active historical runs must settle or be cancelled before delegation execution")
    if freedom is not None:
        raise WorkflowError("Workflow access is defined by saved nodes; caller freedom overrides are not supported")
    definition = copy.deepcopy(definition_snapshot) if definition_snapshot is not None else storage.get(name)
    if definition.get("name") != name:
        raise WorkflowError("Authorized workflow snapshot name mismatch")
    if overrides:
        if overrides.get("backend", definition["orchestrator"]["backend"]) != definition["orchestrator"]["backend"]:
            for setting in ("model", "reasoning_effort", "max_turns"):
                definition["orchestrator"].pop(setting, None)
        definition["orchestrator"] = {**definition["orchestrator"], **overrides}
    definition = validate_definition(definition)
    caller = await _capture_caller(storage, None, _verified_caller)
    run = storage.create_run(definition, prompt, repo_path, freedom="unrestricted", network=network, permission_policy="saved_node", caller=caller)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(interaction_owner=interaction_owner), "interaction_owner")
    _launch(storage, run["workflow_run_id"])
    return run


def _editing_context(value: Any, field: str) -> dict[str, Any]:
    """Accept an incomplete canvas as bounded JSON context, not a runnable graph."""
    if not isinstance(value, dict):
        raise WorkflowError(f"{field} must be a JSON object")
    try:
        encoded = json.dumps(value, allow_nan=False)
    except (ValueError, TypeError) as exc:
        raise WorkflowError(f"{field} must contain JSON-safe values") from exc
    if len(encoded.encode("utf-8")) > 1024 * 1024:
        raise WorkflowError(f"{field} exceeds the one-megabyte context limit")
    sanitized = json.loads(encoded)
    for key in ("nodes", "connections"):
        if key in sanitized and (not isinstance(sanitized[key], list) or any(not isinstance(item, dict) for item in sanitized[key])):
            raise WorkflowError(f"{field}.{key} must be an array of objects")
    return sanitized


def validate_builder_preview(value: Any, name: str) -> dict[str, Any]:
    """Render-safe incomplete graph; runnable topology is validated at finalization."""
    draft = _editing_context(value, "definition")
    draft["name"] = name
    draft.setdefault("routing_mode", "explicit")
    draft.pop("revision", None)
    draft.pop("updated_at", None)
    nodes, edges = draft.setdefault("nodes", []), draft.setdefault("connections", [])
    if len(nodes) > 256 or len(edges) > 1024:
        raise WorkflowError("Builder preview exceeds canvas node or connection limit")
    ids: set[str] = set()
    for index, node in enumerate(nodes):
        nid = _identifier(node.get("id"))
        if nid in ids:
            raise WorkflowError(f"Duplicate node {nid}")
        ids.add(nid)
        if not isinstance(node.get("type"), str) or node.get("type") not in {"start", "agent", "join", "end", "parallel_start", "parallel_end"}:
            raise WorkflowError(f"Invalid node type: {nid}")
        if node["type"] == "start":
            node.setdefault("prompt", "")
            if not isinstance(node["prompt"], str):
                raise WorkflowError("Start prompt must be a string")
        node.setdefault("title", nid)
        node.setdefault("position", {"x": 80 + 260 * index, "y": 80})
        position = node["position"]
        if not isinstance(node["title"], str) or not isinstance(position, dict) or any(not isinstance(position.get(k), (float, int)) or isinstance(position.get(k), bool) or not math.isfinite(position[k]) or position[k] < 0 or position[k] > 1000000 for k in ("x", "y")):
            raise WorkflowError(f"Invalid title or canvas position (x and y must be finite and nonnegative): {nid}")
        if node["type"] == "agent":
            node.setdefault("role", "task")
            if not isinstance(node["role"], str) or node["role"] not in {"planning", "implementation", "review", "task"}:
                raise WorkflowError(f"Invalid agent role: {nid}")
            if node["role"] == "planning":
                for field in ("require_technical_plan", "require_tasks"):
                    node.setdefault(field, True)
                    if not isinstance(node[field], bool):
                        raise WorkflowError(f"Planning {field} must be a boolean")
            node["agent"] = _candidate(node.get("agent", {"backend": "codex"}))
            node.setdefault("instructions", "")
            if not isinstance(node["instructions"], str):
                raise WorkflowError(f"Invalid instructions: {nid}")
            if "freedom" in node and (not isinstance(node["freedom"], str) or node["freedom"] not in FREEDOMS):
                raise WorkflowError(f"Invalid freedom: {nid}")
            if "session_mode" in node and (not isinstance(node["session_mode"], str) or node["session_mode"] not in {"resume", "fresh", "agent_decides", "continue_previous"}):
                raise WorkflowError(f"Invalid session mode: {nid}")
            if "optional" in node and not isinstance(node["optional"], bool):
                raise WorkflowError("Agent optional must be a boolean")
            timeout = node.get("timeout_seconds")
            if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 0):
                raise WorkflowError("timeout_seconds must be a nonnegative integer")
            if "max_attempts" in node:
                _positive(node["max_attempts"], "max_attempts")
            if "network" in node and node["network"] not in (None, True, False):
                raise WorkflowError(f"Invalid network: {nid}")
    if "orchestrator" in draft:
        draft["orchestrator"] = _candidate(draft["orchestrator"])
    for field in ("description",):
        if field in draft and not isinstance(draft[field], str):
            raise WorkflowError(f"Invalid {field}")
    for field in ("max_parallel", "max_transitions"):
        if field in draft:
            _positive(draft[field], field)
    edge_ids: set[str] = set()
    for edge in edges:
        eid = _identifier(edge.get("id"))
        if eid in edge_ids:
            raise WorkflowError(f"Duplicate connection {eid}")
        edge_ids.add(eid)
        if not isinstance(edge.get("source"), str) or not isinstance(edge.get("target"), str) or edge["source"] not in ids or edge["target"] not in ids:
            raise WorkflowError(f"Connection {eid} references an unknown node")
        if "max_retries" in edge and (not isinstance(edge["max_retries"], int) or isinstance(edge["max_retries"], bool) or edge["max_retries"] < 0):
            raise WorkflowError(f"max_retries must be a nonnegative integer: {eid}")
        edge.setdefault("condition", "")
        if not isinstance(edge["condition"], str) or any(key in edge and not isinstance(edge[key], bool) for key in ("default", "backward")):
            raise WorkflowError(f"Invalid condition or flags: {eid}")
    return draft


async def apply_workflow_draft(definition: dict[str, Any], expected_draft_revision: int, *, caller: Any, root: Path | None = None) -> dict[str, Any]:
    """The verified caller can publish only its own active builder's preview."""
    if caller is None or not isinstance(expected_draft_revision, int) or isinstance(expected_draft_revision, bool) or expected_draft_revision < 0:
        raise WorkflowError("A verified builder caller and nonnegative draft revision are required")
    storage = WorkflowStore(root)
    owner = storage.task_owner(caller.record.task_id)
    if not owner or owner["role"] != "builder":
        raise WorkflowError("Only the active workflow builder may apply a draft")
    run_id = owner["workflow_run_id"]
    draft = validate_builder_preview(definition, storage.get_run(run_id)["name"])
    def apply(r: dict[str, Any]) -> None:
        current = r["activations"][-1] if r["activations"] else None
        record = task_store.read(storage.root / "tasks", caller.record.task_id)
        if r["kind"] != "builder" or r["status"] != "running" or not current or current["id"] != owner["activation_id"] or current["role"] != "builder" or current["status"] != "running" or not current["tasks"] or current["tasks"][-1]["task_id"] != caller.record.task_id or current["tasks"][-1]["status"] not in {"reserved", "running"} or record is None or record.status != "running":
            raise WorkflowError("Builder dispatch is no longer active")
        if r.get("draft_revision", 0) != expected_draft_revision:
            raise WorkflowError("Builder draft revision conflict; read the current draft before applying")
        r.update(builder_draft=draft, draft_revision=expected_draft_revision + 1)
        current["draft_applied"] = True
    run = storage.update_run(run_id, apply, "builder_draft_applied", {"task_id": caller.record.task_id, "expected_draft_revision": expected_draft_revision, "definition": draft})
    return {"workflow_run_id": run_id, "draft_revision": run["draft_revision"], "builder_draft": run["builder_draft"]}


async def followup_workflow_builder(run_id: str, prompt: str, *, root: Path | None = None) -> dict[str, Any]:
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt.encode("utf-8")) > 1024 * 1024:
        raise WorkflowError("Builder feedback must be nonempty and at most one megabyte")
    storage = WorkflowStore(root)
    message = {"id": uuid.uuid4().hex, "prompt": prompt, "status": "pending", "created_at": time.time()}
    launch = False
    def queue(r: dict[str, Any]) -> None:
        nonlocal launch
        if r["kind"] != "builder" or r["status"] in {"cancelled", "cancelling", "paused"}:
            raise WorkflowError("This builder cannot accept feedback")
        if any(t["status"] in {"reserved", "running", "uncertain"} for a in r["activations"] for t in a["tasks"]) and not _supervisor_present(r):
            raise WorkflowError("Builder dispatch requires reconciliation before followup")
        if not _supervisor_present(r):
            launch = True
            r.update(status="starting", builder_followup=True)
        if "editing_definition" not in r and r.get("generated_definition"):
            baseline = copy.deepcopy(r["generated_definition"])
            r.update(editing_definition=copy.deepcopy(r.get("builder_draft", baseline)), editing_source={"name": r["name"], "revision": baseline.get("revision", 0), "saved_definition": baseline}, source_name=r["name"], source_revision=baseline.get("revision", 0), source_saved_definition=baseline)
        r.setdefault("builder_messages", []).append(message)
    run = storage.update_run(run_id, queue, "builder_feedback_queued", message)
    if launch:
        _launch(storage, run_id)
    tasks = [t for a in run["activations"] for t in a["tasks"]]
    record = task_store.read(storage.root / "tasks", tasks[-1]["task_id"]) if tasks else None
    status = "queued" if record is not None and record.status == "running" and record.live_input and _supervisor_present(run) else "queued_next_turn"
    return {"workflow_run_id": run_id, "status": status, **({"task_id": tasks[-1]["task_id"]} if tasks else {})}


def builder_workspace(storage: WorkflowStore) -> Path:
    workspace = storage.root / "builder-workspace"
    if workspace.is_symlink():
        raise WorkflowError("Builder workspace must not be a symbolic link")
    workspace.mkdir(mode=0o700, exist_ok=True)
    if not workspace.is_dir():
        raise WorkflowError("Builder workspace is not a directory")
    return workspace.resolve()


async def build_workflow(name: str, prompt: str, repo_path: Path | None = None, *, agent: dict[str, Any], fallbacks: list[dict[str, Any]] | None = None, definition: dict[str, Any] | None = None, source: dict[str, Any] | None = None, root: Path | None = None, _verified_caller: Any = _CALLER_UNSET) -> dict[str, Any]:
    if not isinstance(prompt, str) or not prompt.strip():
        raise WorkflowError("Builder prompt must be nonempty")
    storage = WorkflowStore(root)
    config = _candidate({**agent, "fallbacks": fallbacks or agent.get("fallbacks", [])})
    editing = _editing_context(definition, "definition") if definition is not None else None
    if source is not None and editing is None:
        raise WorkflowError("source requires an editing definition")
    metadata = None
    if editing is not None:
        raw_source = source if source is not None else {"name": editing.get("name", ""), "revision": editing.get("revision", 0), "saved_definition": None}
        if not isinstance(raw_source, dict) or set(raw_source) - {"name", "revision", "saved_definition"}:
            raise WorkflowError("source must contain only name, revision and saved_definition")
        source_name = raw_source.get("name", "")
        if source_name != "":
            _workflow_name(source_name)
        revision = raw_source.get("revision", 0)
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
            raise WorkflowError("source.revision must be a nonnegative integer")
        baseline = raw_source.get("saved_definition")
        metadata = {"name": source_name, "revision": revision, "saved_definition": _editing_context(baseline, "source.saved_definition") if baseline is not None else None}
    descriptor = {"name": _workflow_name(name), "orchestrator": config, "nodes": [], "connections": []}
    caller = await _capture_caller(storage, None, _verified_caller)
    supplied_repo = repo_path is not None
    repo_path = repo_path if supplied_repo else builder_workspace(storage)
    run = storage.create_run(descriptor, prompt, repo_path, freedom="read_only", kind="builder", caller=caller)
    if editing is not None:
        run = storage.update_run(run["workflow_run_id"], lambda r: r.update(editing_definition=editing, editing_source=metadata, source_name=metadata["name"], source_revision=metadata["revision"], source_saved_definition=metadata["saved_definition"]), "builder_edit_requested")
    try:
        initial_preview = validate_builder_preview(editing or {"nodes": [], "connections": []}, name)
    except WorkflowError:
        # Repair input remains available to the agent, but cannot crash the canvas.
        initial_preview = {"name": name, "nodes": [], "connections": []}
    run = storage.update_run(run["workflow_run_id"], lambda r: r.update(builder_draft=initial_preview, draft_revision=0, builder_messages=[], builder_has_repo_context=supplied_repo), "builder_draft_initialized")
    _launch(storage, run["workflow_run_id"])
    return run


async def _capture_caller(storage: WorkflowStore, run_id: str | None = None, verified_caller: Any = _CALLER_UNSET) -> Any:
    """Resolve authority before persistence; verified MCP identity must never be redetected."""
    if verified_caller is not _CALLER_UNSET:
        return verified_caller
    from . import lineage
    log_dir = storage.root / "tasks"
    caller = await asyncio.to_thread(lineage.detect_caller, log_dir)
    if caller is not None:
        return caller
    detection = await asyncio.to_thread(lineage.detect_caller_detail, log_dir)
    if detection.undecidable is not None:
        raise WorkflowError("Workflow caller authority is undecidable: " + str(detection.undecidable))
    if detection.caller is None and os.environ.get(lineage.ENV_TASK_ID):
        raise WorkflowError("Workflow caller task identity cannot be verified")
    return detection.caller


def parse_json(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```json") and text.endswith("```"):
        text = text[7:-3].strip()
    value = json.loads(text)
    if not isinstance(value, dict):
        raise WorkflowError("Agent response must be one JSON object")
    return value


def failure_diagnostic(snapshot: dict[str, Any], prompt: str) -> str:
    """A bounded startup diagnostic without exposing injected dispatch context."""
    lines = list(snapshot.get("stderr_tail", []))
    stream = snapshot.get("raw_stream_log")
    adapter = backends.BACKENDS.get(snapshot.get("backend"))
    diagnostic_hook = getattr(adapter, "workflow_failure_diagnostic", None)
    if stream and diagnostic_hook:
        from .backends.workflow_diagnostics import stream_events
        for event in stream_events(stream):
            diagnostic = diagnostic_hook(event)
            if diagnostic:
                lines.append(diagnostic)
    text = "\n".join(line for line in lines if isinstance(line, str))
    for value in (prompt, json.dumps(prompt)[1:-1], repr(prompt)[1:-1]):
        if value:
            text = text.replace(value, "[workflow context omitted]")
    ignored = ("reading additional input from stdin", "reading additional instructions from stdin", "reading prompt from stdin", "warning:")
    for line in text.splitlines():
        diagnostic = line.strip()
        if not diagnostic or diagnostic.lower().startswith(ignored):
            continue
        # Prompt fragments from multiline diagnostics are unsafe to display. Only
        # diagnostics independent of a line of the dispatched prompt are eligible.
        if any(part.strip() and (diagnostic in part or part in diagnostic) for part in prompt.splitlines() if len(part.strip()) >= 16):
            continue
        if re.search(r"(?i)(authorization|cookie|headers|bearer\s|api[_-]?key\s*[:=])", diagnostic):
            return "Runtime error details contain sensitive request information; inspect the raw diagnostics."
        return diagnostic[:500]
    return ""


def availability_failure(snapshot: dict[str, Any]) -> str | None:
    """Only process diagnostics, never summaries or tool output, authorize fallback."""
    if snapshot.get("status") != "failed" or snapshot.get("outcome_unknown") or snapshot.get("permission_denials"):
        return None
    diagnostic = "\n".join(snapshot.get("stderr_tail", []))
    adapter = backends.BACKENDS.get(snapshot.get("backend"))
    stderr_hook = getattr(adapter, "workflow_stderr_availability_failure", None)
    if stderr_hook:
        reason = stderr_hook(diagnostic)
        if reason:
            return reason
    # Inspect only authoritative top-level protocol envelopes. Assistant messages,
    # tool payloads and summaries can contain arbitrary text and are never evidence.
    stream = snapshot.get("raw_stream_log")
    classify = getattr(adapter, "workflow_availability_failure", None)
    if stream and classify:
        from .backends.workflow_diagnostics import stream_events
        for event in stream_events(stream):
            reason = classify(event)
            if reason:
                return reason
    return None


def task_liveness(log_dir: Path, record: Any) -> dict[str, Any]:
    """Process safety and outcome authority are independent recovery facts."""
    from . import identity
    observed = record.status in task_store.TERMINAL_RECORD_STATUSES and not task_store.outcome_unobserved(record)
    verdict, reason = identity.check_detail(identity.task_identity(record.pid, record.start_time, record.markers))
    # An authoritative exit receipt proves the managed process has settled.
    alive = False if observed else {"alive": True, "dead": False}.get(verdict)
    status, note, _, _ = task_store.resolve_status(log_dir, record, detail=False)
    return {"process_alive": alive, "outcome_known": observed, "resolved_status": status, "reason": reason or note}


def observed_harness_metadata(snapshot: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
    """Backend adapters select native runtime identity evidence and its provenance."""
    result = copy.deepcopy(metadata)
    adapter = backends.BACKENDS.get(snapshot.get("backend"))
    observe = getattr(adapter, "workflow_observed_metadata", None)
    if observe:
        observed = observe(snapshot)
        if observed:
            result.update(observed)
    return result


class CheckoutLease:
    """OS leases shared across supervisors; process death releases the descriptor."""
    def __init__(self, storage: WorkflowStore, repo: str, write: bool, should_continue: Any = None, *, wait_seconds: float = 30.0, on_wait: Any = None, pool: dict[str, Any] | None = None):
        self.repo = str(Path(repo).resolve())
        self.store = storage
        self.path = storage.leases / (hashlib.sha256(self.repo.encode()).hexdigest() + ".lock")
        self.write = write
        self.should_continue = should_continue
        self.handle: Any = None
        self.wait_seconds = wait_seconds
        self.on_wait = on_wait
        self.pool = pool
        self.entry: dict[str, Any] | None = None

    async def __aenter__(self):
        if self.pool is None:
            return await self._acquire()
        # One supervisor owns the pooled descriptor. Every dispatch uses the same
        # strength (exclusive if this workflow has any writer), so joining cannot
        # downgrade protection while a writer remains live.
        entry = self.pool.setdefault(self.repo, {"guard": asyncio.Lock(), "handle": None, "users": 0, "write": self.write})
        self.entry = entry
        async with entry["guard"]:
            if entry["write"] != self.write:
                raise WorkflowError("Pooled checkout lease strength must remain consistent")
            if self.should_continue is not None and not self.should_continue():
                raise DispatchNotStarted("Scheduling stopped before dispatch")
            if entry["handle"] is None:
                await self._acquire()
                entry["handle"] = self.handle
            else:
                self.handle = entry["handle"]
            entry["users"] += 1
        return self

    def _orphan_owner(self) -> dict[str, str] | None:
        """Durable live dispatches remain a barrier when their supervisor dies."""
        for run in self.store.list_runs():
            if run["repo_path"] != self.repo or _supervisor_present(run):
                continue
            for activation in run["activations"]:
                for task in activation["tasks"]:
                    if task["status"] not in {"running", "reserved", "uncertain"}:
                        continue
                    record = task_store.read(self.store.root / "tasks", task["task_id"])
                    unresolved = record is None or task_liveness(self.store.root / "tasks", record)["process_alive"] is not False
                    if unresolved and (self.write or task.get("freedom", "write_in_repo") != "read_only"):
                        return {"workflow_run_id": run["workflow_run_id"], "task_id": task["task_id"]}
        return None

    async def _acquire(self):
        self.handle = self.path.open("a")
        deadline = time.monotonic() + self.wait_seconds
        notified = False
        while True:
            if self.should_continue is not None and not self.should_continue():
                self.handle.close()
                raise DispatchNotStarted("Scheduling stopped before dispatch")
            # After a supervisor crash its agent processes may outlive the OS lease.
            # Their durable associations still block conflicting checkout activity.
            conflict_owner = self._orphan_owner()
            orphan_conflict = conflict_owner is not None
            if orphan_conflict:
                if not notified and self.on_wait:
                    self.on_wait({"reason": "Checkout held by a live or uncertain orphan task", "repo_path": self.repo, "owner": conflict_owner})
                    notified = True
                if time.monotonic() >= deadline:
                    self.handle.close()
                    raise DispatchNotStarted("Checkout wait exhausted; reconciliation is required")
                try:
                    await asyncio.sleep(0.1)
                except BaseException:
                    self.handle.close()
                    raise
                continue
            try:
                fcntl.flock(self.handle, (fcntl.LOCK_EX if self.write else fcntl.LOCK_SH) | fcntl.LOCK_NB)
                # The prior owner may die between the scan and flock acquisition.
                # Recheck durable associations while holding the descriptor.
                try:
                    conflict_owner = self._orphan_owner()
                except BaseException:
                    fcntl.flock(self.handle, fcntl.LOCK_UN)
                    self.handle.close()
                    raise
                if conflict_owner is not None:
                    fcntl.flock(self.handle, fcntl.LOCK_UN)
                    if not notified and self.on_wait:
                        self.on_wait({"reason": "Checkout held by a live or uncertain orphan task", "repo_path": self.repo, "owner": conflict_owner})
                        notified = True
                    if time.monotonic() >= deadline:
                        self.handle.close()
                        raise DispatchNotStarted("Checkout wait exhausted; reconciliation is required")
                    try:
                        await asyncio.sleep(0.1)
                    except BaseException:
                        self.handle.close()
                        raise
                    continue
                if self.should_continue is not None and not self.should_continue():
                    fcntl.flock(self.handle, fcntl.LOCK_UN)
                    self.handle.close()
                    raise DispatchNotStarted("Scheduling stopped before dispatch")
                return self
            except BlockingIOError:
                if not notified and self.on_wait:
                    self.on_wait({"reason": "Checkout held by another active workflow", "repo_path": self.repo})
                    notified = True
                if time.monotonic() >= deadline:
                    self.handle.close()
                    raise DispatchNotStarted("Checkout wait exhausted; another workflow still holds its lease")
                try:
                    await asyncio.sleep(0.1)
                except BaseException:
                    self.handle.close()
                    raise

    async def __aexit__(self, *_: Any):
        if self.entry is not None:
            async with self.entry["guard"]:
                self.entry["users"] -= 1
                if self.entry["users"]:
                    self.handle = None
                    return
                self.entry["handle"] = None
        if self.handle:
            fcntl.flock(self.handle, fcntl.LOCK_UN)
            self.handle.close()


class WorkflowSupervisor:
    def __init__(self, registry: Any, storage: WorkflowStore):
        self.registry = registry
        self.store = storage
        self.run_id = ""
        self.decision_lock = asyncio.Lock()
        self.checkout_leases: dict[str, Any] = {}

    def run(self) -> dict[str, Any]:
        return self.store.get_run(self.run_id)

    def update(self, mutate: Any, event: str, detail: Any = None) -> dict[str, Any]:
        return self.store.update_run(self.run_id, mutate, event, detail)

    def attention(self, reason: str) -> None:
        def set_status(r: dict[str, Any]) -> None:
            if r["status"] != "cancelling":
                r["status"] = "needs_attention"
                r["attention_reason"] = reason
        self.update(set_status, "needs_attention", reason)

    async def _dispatch(self, node: dict[str, Any], prompt: str, role: str, activation: dict[str, Any]) -> dict[str, Any] | None:
        from .tasks import SessionUnknownError, SessionBusyError, RepoUnavailableError
        run = self.run()
        config = node.get("agent", run["definition"]["orchestrator"])
        candidates = [config] + config.get("fallbacks", [])
        key = node["id"] if role == "node" else role
        previous = run["sessions"].get(key, {})
        if role == "node" and activation.get("resume_task_id"):
            source = next((t for a in run["activations"] if (a["node_id"] == node["id"] or activation.get("continue_previous")) and a["role"] == "node" for t in a["tasks"] if t["task_id"] == activation["resume_task_id"]), None)
            if source:
                previous = {"task_id": source["task_id"], "candidate": key + ":" + _candidate_key(source["candidate"]), "session_id": source.get("result", {}).get("session_id")}
        persistent_orchestrator = role == "orchestrator" and run.get("runner_policy") == "guided"
        if (persistent_orchestrator or role == "builder" and run.get("builder_followup")) and previous.get("task_id"):
            parent = self.registry.get(previous["task_id"])
            record = task_store.read(self.registry._log_dir, previous["task_id"])
            retained = parent.snapshot() if parent is not None else None
            compatible = (retained is not None and retained.get("status") == "completed" and retained.get("session_id")) or (record is not None and record.status == "completed" and record.session_id and record.freedom == "read_only" and record.repo_path == run["repo_path"])
            if not compatible:
                previous = {}  # Fresh bootstrap is safe before any reservation or spawn.
        preferred = previous.get("candidate")
        if preferred and (persistent_orchestrator or role == "node" and node.get("session_mode") == "resume" or role == "builder" and run.get("builder_followup")):
            candidates.sort(key=lambda c: key + ":" + _candidate_key(c) != preferred)
        if role == "node" and run.get("execution_contract") == "delegation" and node.get("session_mode") == "resume" and not activation.get("continue_previous"):
            from .workflow_delegation import recovered_resume_outage, require_fresh_checkpoint
            current_activation = next(a for a in run["activations"] if a["id"] == activation["id"])
            outage = recovered_resume_outage(run, current_activation)
            if outage:
                self.update(lambda r: require_fresh_checkpoint(r, activation["id"], *outage), "recovered_resume_requires_fresh")
                return None
        capability_refused = False
        for candidate in candidates:
            identity = key + ":" + _candidate_key(candidate)
            if identity in self.run()["suppressed_candidates"]:
                capability_refused = capability_refused or identity in self.run().get("suppressed_capability_candidates", [])
                continue
            observed = next((t.get("result") for t in reversed(activation["tasks"]) if _candidate_key(t.get("candidate", {})) == _candidate_key(candidate) and t.get("result")), None)
            if observed and observed.get("timed_out") and not observed.get("outcome_unknown"):
                previous = {}
                if candidate != candidates[-1]:
                    continue  # Settled timeout already spent this candidate before restart.
                return {**observed, "execution_failure": observed.get("summary", "Node timed out"), "optional_failure_eligible": True}
            if observed and availability_failure(observed):
                self.update(lambda r: r["suppressed_candidates"].append(identity), "recovered_fallback", {"candidate": candidate, "reason": availability_failure(observed)})
                previous = {}
                continue
            backend = backends.get(candidate["backend"])
            if not backends.is_installed(backend):
                self.update(lambda r: r["suppressed_candidates"].append(identity), "candidate_unavailable", {"candidate": candidate, "reason": "binary missing"})
                continue
            run = self.run()
            if run["status"] != "running":
                return None
            try:
                freedom = "read_only" if role != "node" else run_effective_freedom(run, node)
            except WorkflowError as exc:
                self.attention(f"Dispatch configuration refused: {exc}")
                return None
            network = False if run["network"] is False else node.get("network", run["network"])
            task_id = uuid.uuid4().hex
            display_prompt = run.get("builder_turn_prompt", run["prompt"]) if role == "builder" else activation.get("turn_prompt", activation.get("assignment_prompt", run["prompt"]))
            if role == "orchestrator" and run.get("runner_policy") == "guided" and any(t.get("status") != "not_started" for a in run["activations"] if a["role"] == "orchestrator" for t in a["tasks"]):
                stage = next((n for n in run["definition"]["nodes"] if n["id"] == activation["node_id"]), {})
                label = stage.get("title") or activation["node_id"]
                checkpoint = activation.get("token") or {}
                detail = "correcting the previous decision" if checkpoint.get("decision_error") else "choosing the next step"
                display_prompt = f"Workflow checkpoint: {label} — {detail}."
            use_resume = (persistent_orchestrator or role == "node" and node.get("session_mode") == "resume" or role == "builder" and run.get("builder_followup")) and previous.get("candidate") == identity
            reservation = {"task_id": task_id, "candidate": candidate, "status": "reserved", "dispatch_stage": "preparing", "reserved_at": time.time(), "freedom": freedom, "assignment_prompt": display_prompt, "repo_path": run["repo_path"], "network": network, "session_mode": "resume" if use_resume else "fresh", "resume_task_id": previous.get("task_id") if use_resume else None}
            reservation["harness_metadata"] = {"requested": copy.deepcopy(candidate), "effective": {"backend": candidate["backend"], "model": candidate.get("model"), "reasoning_effort": candidate.get("reasoning_effort"), "freedom": freedom, "settings_source": "explicit_launch_arguments", "default_model": "unknown" if not candidate.get("model") else None, "provider_identity": "unknown"}, "observed": None, "provenance": "validated_launch_configuration", "verification_status": "configured_not_observed"}
            def reserve(r: dict[str, Any]) -> None:
                a = next(a for a in r["activations"] if a["id"] == activation["id"])
                a["tasks"].append(reservation)
                if role == "node" and a.get("continue_previous"):
                    a["execution_session_mode"] = reservation["session_mode"]
                    a["session_reason"] = a.get("session_reason", "Resumed previous node") if use_resume else ("Started Fresh: fallback selected" if candidate != config else a.get("session_reason", "Started Fresh: compatible previous session unavailable"))
                    reservation["session_reason"] = a["session_reason"]
                q = next((q for q in a.get("questions", []) if q["question_id"] == a.get("resume_question_id")), None)
                if q:
                    q.update(reply_task_id=task_id, answer_delivery_state="reserved")
                    reservation["question_id"] = q["question_id"]
            self.update(reserve, "dispatch_reserved", reservation)
            capability_stage = "settings"
            try:
                backends.reject_model(backend, candidate.get("model"))
                backends.reject_turn_cap(backend, candidate.get("max_turns"))
                backends.check_reasoning_effort(backend, candidate.get("reasoning_effort"))
                capability_stage = "enforcement"
                backend.enforcement(freedom, network)
                async with CheckoutLease(self.store, run["repo_path"], any(n.get("freedom", ROLE_FREEDOM_DEFAULTS.get(n.get("role"), "read_only")) != "read_only" for n in run["definition"].get("nodes", []) if n["type"] == "agent") or freedom != "read_only", lambda: self.run()["status"] == "running", on_wait=lambda detail: self.update(lambda r: r.update(checkout_wait=detail), "checkout_wait", detail), pool=self.checkout_leases):
                    if self.run()["status"] != "running":
                        self._task_update(activation["id"], task_id, {"status": "not_started"})
                        return None
                    capability_stage = "spawn"
                    timeout_deadline = time.time() + node["timeout_seconds"] if role == "node" and node.get("timeout_seconds") else None
                    self._task_update(activation["id"], task_id, {"dispatch_stage": "spawn_requested", **({"timeout_deadline": timeout_deadline} if timeout_deadline is not None else {})})
                    dispatch_prompt = prompt
                    if role == "node" and freedom != "read_only":
                        from . import scratch
                        dispatch_prompt += "\nTask scratch directory (absolute): " + str(scratch.directory(self.registry._log_dir, task_id).resolve()) + "\nUse this directory for temporary artifacts outside the repository; artifacts are retained with task records."
                    if use_resume:
                        parent = self.registry.get(previous["task_id"])
                        if parent:
                            task = await self.registry.resume(parent, dispatch_prompt, max_turns=candidate.get("max_turns"), network=network, task_id=task_id, display_prompt=display_prompt, workflow_builder=role == "builder")
                        else:
                            record = task_store.read(self.registry._log_dir, previous["task_id"])
                            if record is None:
                                raise SessionUnknownError("Previous resume session is unavailable")
                            task = await self.registry.resume_record(record, dispatch_prompt, max_turns=candidate.get("max_turns"), network=network, task_id=task_id, display_prompt=display_prompt, workflow_builder=role == "builder")
                    else:
                        if role == "builder":
                            latest = self.run()
                            dispatch_prompt += "\nLatest authoritative builder preview, revision " + str(latest.get("draft_revision", 0)) + ":\n" + json.dumps(latest.get("builder_draft", {}))
                        task = await self.registry.start(dispatch_prompt, Path(run["repo_path"]), backend=backend, freedom=freedom, network=network, model=candidate.get("model"), reasoning_effort=candidate.get("reasoning_effort"), max_turns=candidate.get("max_turns"), task_id=task_id, display_prompt=display_prompt, workflow_builder=role == "builder", title=f"{run['name']} · {node.get('title', key)}")
                    self.update(lambda r: r.pop("checkout_wait", None), "checkout_acquired")
                    self._task_update(activation["id"], task_id, {"status": "running", "dispatch_stage": "spawn_confirmed", **({"timeout_deadline": timeout_deadline} if timeout_deadline is not None else {})})
                    timeout_at = time.monotonic() + max(0, timeout_deadline - time.time()) if timeout_deadline is not None else None
                    timed_out = False
                    while not task.done.is_set():
                        if timeout_at is not None and time.monotonic() >= timeout_at:
                            self._task_update(activation["id"], task_id, {"timeout_requested_at": time.time()})
                            try:
                                cancellation = await asyncio.wait_for(self.registry.cancel_cascade(task.task_id, workflow_control=True), timeout=20)
                                if not isinstance(cancellation, dict) or any(cancellation.get(key) for key in ("sigkill_survivors", "survivors", "not_signalled", "owner_still_settling", "cascade_incomplete", "unconverged", "not_recorded")):
                                    raise RuntimeError("Task cancellation cascade has unsettled or unverifiable descendants")
                                await asyncio.wait_for(task.done.wait(), timeout=10)
                            except Exception as exc:
                                self._task_update(activation["id"], task_id, {"status": "uncertain", "error": "Timeout cancellation requires reconciliation: " + str(exc)})
                                self.attention("Timed out node could not be confirmed settled; no fallback dispatched")
                                return None
                            timed_out = True
                            break
                        if role == "builder" and getattr(task, "live_input", False) and any(m.get("status") == "pending" for m in self.run().get("builder_messages", [])):
                            messages: list[dict[str, Any]] = []
                            def claim_live(r: dict[str, Any]) -> None:
                                for m in r.get("builder_messages", []):
                                    if m["status"] == "pending":
                                        m["status"] = "forwarding"
                                        messages.append(copy.deepcopy(m))
                            self.update(claim_live, "builder_feedback_forwarding")
                            for message in messages:
                                try:
                                    response = await self.registry.send_message(task, message["prompt"])
                                    delivery_id = response.get("message_id") if isinstance(response, dict) else None
                                    status = "queued_to_agent"
                                except Exception as exc:
                                    from . import inbox
                                    # SendRefused guarantees that no message entered the inbox.
                                    delivery_id = None
                                    status = "deferred" if isinstance(exc, inbox.SendRefused) else "forwarding_uncertain"
                                self.update(lambda r, mid=message["id"], state=status, did=delivery_id: next(m for m in r["builder_messages"] if m["id"] == mid).update(status=state, inbox_message_id=did, task_id=task.task_id), "builder_feedback_forwarded", {"message_id": message["id"], "status": status})
                        if self.run()["status"] == "cancelling":
                            await self.registry.cancel_cascade(task.task_id, workflow_control=True)
                        try:
                            await asyncio.wait_for(task.done.wait(), timeout=0.25)
                        except TimeoutError:
                            pass
                    snapshot = task.snapshot()
                    if timed_out and snapshot.get("status") not in {"completed", "failed", "cancelled"}:
                        self._task_update(activation["id"], task_id, {"status": "uncertain", "error": "Timeout cancellation did not produce a settled task snapshot"})
                        self.attention("Timed out task requires reconciliation before fallback")
                        return None
                    if timed_out and snapshot.get("status") != "completed":
                        notice = getattr(self.registry, "workflow_notice", None)
                        if callable(notice):
                            notice(task.task_id, "Node timed out after " + str(node["timeout_seconds"]) + " seconds; attempt stopped before fallback")
                        snapshot = {**snapshot, "status": "failed", "failure_kind": "timeout", "timeout_seconds": node["timeout_seconds"], "summary": "Node timed out after " + str(node["timeout_seconds"]) + " seconds", "timed_out": True}
                self._task_update(activation["id"], task_id, {"status": snapshot["status"], "result": snapshot, "finished_at": time.time(), "harness_metadata": observed_harness_metadata(snapshot, reservation["harness_metadata"])})
            except DispatchNotStarted as exc:
                self._task_update(activation["id"], task_id, {"status": "not_started"})
                def not_started(r: dict[str, Any]) -> None:
                    current = next(a for a in r["activations"] if a["id"] == activation["id"])
                    if all(t["status"] == "not_started" for t in current["tasks"]):
                        current["status"] = "not_started"
                self.update(not_started, "dispatch_not_started")
                if "wait exhausted" in str(exc):
                    self.attention(str(exc))
                return None
            except backends.NestedDispatchRefused as exc:
                self._task_update(activation["id"], task_id, {"status": "not_started", "error": str(exc)})
                self.attention(f"Dispatch configuration refused: {exc}")
                return None
            except backends.UnsupportedCapability as exc:
                capability_refused = capability_refused or capability_stage != "settings"
                self._task_update(activation["id"], task_id, {"status": "not_started", "error": str(exc)})
                self.update(lambda r: (r["suppressed_candidates"].append(identity), r.setdefault("suppressed_capability_candidates", []).append(identity) if capability_stage != "settings" else None), "candidate_capability_rejected", {"task_id": task_id, "candidate": candidate, "reason": str(exc)})
                if activation.get("resume_question_id") and node.get("session_mode") == "resume" and capability_stage == "spawn":
                    self.update(lambda r: next(q for a in r["activations"] if a["id"] == activation["id"] for q in a.get("questions", []) if q["question_id"] == activation["resume_question_id"]).update(answer_delivery_state="not_started"), "answer_resume_unsupported")
                    self.attention("Answer session cannot resume; orchestrator must explicitly choose Fresh")
                    return None
                previous = {}
                continue
            except (SessionUnknownError, SessionBusyError, RepoUnavailableError) as exc:
                self._task_update(activation["id"], task_id, {"status": "not_started", "error": str(exc)})
                if isinstance(exc, SessionUnknownError) and activation.get("continue_previous") and node.get("session_mode") == "resume" and not activation.get("resume_question_id"):
                    # Positive no-spawn refusal permits a Fresh bootstrap on the same candidate.
                    self.update(lambda r: next(a for a in r["activations"] if a["id"] == activation["id"]).update(session_reason="Started Fresh: retained session unavailable"), "previous_session_unavailable")
                    return await self._dispatch({**node, "session_mode": "fresh"}, prompt, role, next(a for a in self.run()["activations"] if a["id"] == activation["id"]))
                self.attention(f"Dispatch configuration refused: {exc}")
                return None
            except Exception as exc:
                # Once a dispatch was reserved, only positive evidence that no spawn happened
                # permits reconciliation. Unknown exceptions remain uncertain.
                not_started = capability_stage != "spawn" or getattr(exc, "polybridge_not_started", False) is True
                self._task_update(activation["id"], task_id, {"status": "not_started" if not_started else "uncertain", "error": str(exc)})
                if not_started:
                    self.update(lambda r: next(a for a in r["activations"] if a["id"] == activation["id"]).update(status="not_started") if all(t["status"] == "not_started" for a in r["activations"] if a["id"] == activation["id"] for t in a["tasks"]) else None, "dispatch_not_started")
                self.attention(f"Dispatch {task_id} did not start: {exc}" if not_started else f"Dispatch {task_id} requires reconciliation: {exc}")
                return None
            if snapshot.get("timed_out"):
                previous = {}
                if candidate != candidates[-1]:
                    continue  # Cancellation settled before a Fresh fallback is dispatched.
                return {**snapshot, "execution_failure": snapshot["summary"], "optional_failure_eligible": True}
            reason = availability_failure(snapshot)
            if reason:
                if role == "node" and run.get("execution_contract") == "delegation" and node.get("session_mode") == "resume" and previous.get("candidate") == identity and not activation.get("continue_previous"):
                    from .workflow_delegation import require_fresh_checkpoint
                    self.update(lambda r: require_fresh_checkpoint(r, activation["id"], reason, identity), "resume_requires_fresh", {"task_id": task_id, "reason": reason})
                    return None
                self.update(lambda r: r["suppressed_candidates"].append(identity), "fallback", {"task_id": task_id, "reason": reason})
                previous = {}
                continue
            if snapshot["status"] != "completed" or snapshot.get("is_error"):
                diagnostic = failure_diagnostic(snapshot, prompt)
                reason = f"{role} task {task_id} ended {snapshot['status']}" + (f": {diagnostic}" if diagnostic else "")
                if role == "node" and run.get("execution_contract") == "delegation":
                    unsafe = snapshot.get("permission_denials") or snapshot["status"] != "failed"
                    if unsafe:
                        self.attention(reason)
                    return {**snapshot, "execution_failure": reason, "blocked_failure": bool(unsafe), "optional_failure_eligible": not unsafe}
                if role == "node" and snapshot["status"] == "failed" and not snapshot.get("permission_denials") and optional_failure_join(self.run(), node, activation.get("token") or {}):
                    return {"optional_failure": reason, "failure_evidence": _bounded(snapshot)}
                self.attention(reason)
                return None
            self.update(lambda r: r["sessions"].__setitem__(key, {"candidate": identity, "task_id": task_id, "session_id": snapshot.get("session_id")}), "session_recorded", key)
            return snapshot
        reason = f"All agents unavailable for {key}"
        if role == "node" and run.get("execution_contract") == "delegation":
            if capability_refused:
                self.attention(reason)
            return {"status": "failed", "summary": "", "execution_failure": reason, "candidates": candidates, "blocked_failure": capability_refused, "optional_failure_eligible": not capability_refused}
        if role == "node" and not capability_refused and optional_failure_join(self.run(), node, activation.get("token") or {}):
            return {"optional_failure": reason, "failure_evidence": {"candidates": _bounded(candidates)}}
        self.attention(reason)
        return None

    def _task_update(self, aid: str, tid: str, values: dict[str, Any]) -> None:
        def update(r: dict[str, Any]) -> None:
            a = next(a for a in r["activations"] if a["id"] == aid)
            task = next(t for t in a["tasks"] if t["task_id"] == tid)
            update_task_state(a, task, values)
        self.update(update, "task_state", {"task_id": tid, **values})

    def _activation(self, node_id: str, role: str, token: dict[str, Any] | None = None) -> dict[str, Any]:
        a = {"id": uuid.uuid4().hex, "node_id": node_id, "role": role, "status": "running", "tasks": [], "created_at": time.time(), "token": token}
        self.update(lambda r: r["activations"].append(a), "activation_started", a)
        return a

    async def _decision(self, node: dict[str, Any], result: dict[str, Any], token: dict[str, Any]) -> list[dict[str, Any]] | None:
        if self.run().get("execution_contract") == "delegation":
            from .workflow_delegation import decide
            return await decide(self, node, token)
        return await self._legacy_decision(node, result, token)

    async def _node(self, token: dict[str, Any]) -> None:
        if self.run().get("execution_contract") == "delegation":
            from .workflow_delegation import process_node
            return await process_node(self, token)
        return await self._legacy_node(token)

    async def _execute_node(self, node: dict[str, Any], token: dict[str, Any]) -> None:
        if self.run().get("execution_contract") == "delegation":
            from .workflow_delegation import execute_node
            return await execute_node(self, node, token)
        return await self._legacy_execute_node(node, token)

    async def _legacy_decision(self, node: dict[str, Any], result: dict[str, Any], token: dict[str, Any]) -> list[dict[str, Any]] | None:
        async with self.decision_lock:
            r = self.run()
            edges = [e for e in r["definition"]["connections"] if e["source"] == node["id"]]
            if len(edges) == 1 and not edges[0]["condition"] and node.get("role") not in {"planning", "implementation", "review"}:
                return edges
            graph_nodes = [{"id": n["id"], "title": n["title"], "type": n["type"], "role": n.get("role"), "instructions": n.get("instructions", "")[:2000]} for n in r["definition"]["nodes"]]
            graph = {"name": r["name"], "nodes": graph_nodes, "connections": r["definition"]["connections"]}
            targets = {n["id"]: n for n in graph_nodes}
            legal = [{**edge, "target_node": targets[edge["target"]], **(retry_budget(r, edge) if edge.get("backward") else {})} for edge in edges]
            context = {"task": r["prompt"], "workflow_purpose": next((n.get("prompt", "") for n in r["definition"]["nodes"] if n["type"] == "start"), ""), "instructions": r["instructions"], "node": node, "result": _bounded(result), "tasks": r.get("tasks", []), "workflow_graph": graph, "legal_connections": legal, "recent_decisions": r["decisions"][-10:], "branch": token, "transitions_remaining": r["definition"]["max_transitions"] + r.get("transition_grant", 0) - r["transitions"]}
            routing = "Choose one or multiple legal forward connections according to the conditions, result evidence and workflow guide. Blank conditions are available unconditional paths, not a requirement to select every path. A retry/backward connection must be the only selected connection."
            routing += " Never select a retry whose retry_remaining is zero; choose a justified alternative or needs_attention. Retry limits count traversals across the entire run. Selected parallel paths must not overlap before their shared convergence. A retry must stay within every active parallel branch."
            if node.get("branch_mode") == "choose_one":
                routing += " This historical run permits exactly one connection."
            prompt = 'You are a workflow decision agent, not an executor. Do not dispatch agents or modify files. Evaluate the connection conditions from evidence. Return ONLY JSON {"action":"continue|complete|failed|needs_attention","connections":["connection-id"],"reason":"...","task_updates":[{"task_id":"id","status":"pending|completed","reason":"evidence"}]}. Only you may update checklist statuses. Complete a task only after successful implementation reporting that completed_task_id; review can reopen tasks as pending. ' + routing + ' If the evidence or continuation is unclear, return needs_attention; use a default path only when justified by its instructions. Never invent nodes or edges. Context:\n' + json.dumps(context)
            for correction in range(2):
                a = self._activation(node["id"], "orchestrator", token)
                outcome = await self._dispatch({"id": "orchestrator", "title": "Decision"}, prompt, "orchestrator", a)
                if not outcome:
                    self.update(lambda rr: next(x for x in rr["activations"] if x["id"] == a["id"]).update(status="failed"), "decision_interrupted")
                    return None
                try:
                    decision = parse_json(outcome.get("summary") or "")
                    action = decision.get("action")
                    selected = decision.get("connections", [])
                    if action not in {"continue", "complete", "failed", "needs_attention"} or not isinstance(selected, list) or not isinstance(decision.get("reason"), str):
                        raise WorkflowError("Invalid decision shape")
                    if len(set(selected)) != len(selected) or any(e not in {x["id"] for x in edges} for e in selected):
                        raise WorkflowError("Decision selected illegal connections")
                    if action in {"continue", "complete"} and (not selected or node["branch_mode"] == "choose_one" and len(selected) != 1):
                        raise WorkflowError("Invalid connection count")
                    if any(edge["backward"] for edge in edges if edge["id"] in selected) and len(selected) != 1:
                        raise WorkflowError("A retry connection must be selected exclusively")
                    if action in {"failed", "needs_attention"} and selected:
                        raise WorkflowError("Stopping decisions cannot select connections")
                    if action == "complete" and (not selected or not all(next(e for e in edges if e["id"] == i)["target"] in {n["id"] for n in r["definition"]["nodes"] if n["type"] == "end"} for i in selected)):
                        raise WorkflowError("Completion must follow End connections")
                    selected_join = validate_selection(r["definition"], node, [edge for edge in edges if edge["id"] in selected], token, r["joins"]) if action in {"continue", "complete"} else None
                    if action in {"continue", "complete"}:
                        validate_retry_budget(self.run(), [edge for edge in edges if edge["id"] in selected])
                    updates = decision.get("task_updates", [])
                    if not isinstance(updates, list):
                        raise WorkflowError("task_updates must be an array")
                    update_ids = [u.get("task_id") for u in updates if isinstance(u, dict)]
                    if len(set(update_ids)) != len(update_ids):
                        raise WorkflowError("Duplicate checklist task updates")
                    known_tasks = {t["id"]: t for t in r.get("tasks", [])}
                    for update in updates:
                        if not isinstance(update, dict) or update.get("task_id") not in known_tasks or update.get("status") not in {"pending", "completed"} or not isinstance(update.get("reason"), str):
                            raise WorkflowError("Invalid checklist task update")
                        if update["status"] == "completed":
                            if node.get("role") != "implementation" or update["task_id"] not in token.get("completed_task_ids", []):
                                raise WorkflowError("Task completion requires implementation evidence")
                    def record_decision(rr: dict[str, Any]) -> None:
                        rr["decisions"].append({**decision, "node_id": node["id"], "activation_id": a["id"], "selected_join_id": selected_join})
                        next(x for x in rr["activations"] if x["id"] == a["id"]).update(status="completed")
                        if action in {"continue", "complete"}:
                            current = next(t for t in rr["pending"] if t["id"] == token["id"])
                            current["selected_connections"] = selected
                            current["selected_join_id"] = selected_join
                        for update in updates:
                            checklist_task = next(t for t in rr["tasks"] if t["id"] == update["task_id"])
                            checklist_task.update(status=update["status"], reason=update["reason"], completed_by_activation_id=token.get("execution_activation_id") if update["status"] == "completed" else None, status_decision_activation_id=a["id"], status_changed_at=time.time())
                    self.update(record_decision, "decision", {**decision, "selected_join_id": selected_join})
                    if action in {"failed", "needs_attention"}:
                        if action == "failed":
                            self.update(lambda rr: rr.update(status="failed", failure_reason=decision["reason"]), "failed", decision["reason"])
                        else:
                            self.attention(decision["reason"])
                        return None
                    return [e for e in edges if e["id"] in selected]
                except RetryLimitReached as exc:
                    self.update(lambda rr: (rr.update(status="needs_attention", attention_reason=str(exc), exhausted_retry_edges=[exc.edge_id]), next(x for x in rr["activations"] if x["id"] == a["id"]).update(status="failed")), "retry_limit_reached", {"edge_id": exc.edge_id, "reason": str(exc)})
                    return None
                except (ValueError, TypeError) as exc:
                    self.update(lambda rr: next(x for x in rr["activations"] if x["id"] == a["id"]).update(status="failed"), "invalid_decision", str(exc))
                    prompt += f"\nYour prior response was invalid: {exc}. Return the required JSON."
            self.attention("Orchestrator returned invalid decisions twice")
            return None

    async def _legacy_node(self, token: dict[str, Any]) -> None:
        r = self.run()
        node = next(n for n in r["definition"]["nodes"] if n["id"] == token["node_id"])
        result: dict[str, Any] = {}
        if node["type"] == "agent":
            if token.get("execution_complete"):
                result = token.get("result", {})
            else:
                await self._execute_node(node, token)
                token = next((t for t in self.run()["pending"] if t["id"] == token["id"]), token)
                if token["node_id"] != node["id"]:
                    return  # An isolated recovery returned to its saved checkpoint.
                result = token.get("result", {})
            if not result:
                return
        if token.get("optional_failure_join"):
            def bypass(rr: dict[str, Any]) -> None:
                current = next(t for t in rr["pending"] if t["id"] == token["id"])
                join = optional_failure_join(rr, node, current)
                if join != current["optional_failure_join"]:
                    raise WorkflowError("Optional failure convergence changed")
                if rr["transitions"] >= rr["definition"]["max_transitions"] + rr.get("transition_grant", 0):
                    rr.update(status="needs_attention", attention_reason="Transition limit reached")
                    return
                rr["transitions"] += 1
                current.update(node_id=join, context={"optional_failure": result, "node_id": node["id"]})
                if rr.get("execution_contract") == "delegation":
                    current["input_result_refs"] = [current["execution_activation_id"]]
                    for key in ("assignment_prompt", "assigned_task_ids", "additional_result_refs", "decision_id", "decision_attempts", "decision_error", "assignments"):
                        current.pop(key, None)
                for key in ("execution_complete", "execution_activation_id", "result", "optional_failure_join", "selected_connections", "recovered_result", "recovered_failed_result"):
                    current.pop(key, None)
            self.update(bypass, "optional_branch_bypassed", {"node_id": node["id"], "join_id": token["optional_failure_join"]})
            return
        if node["type"] == "end":
            self.update(lambda rr: rr["pending"].remove(token), "end_reached", node["id"])
            return
        if token.get("selected_connections"):
            selected = [e for e in r["definition"]["connections"] if e["id"] in token["selected_connections"]]
        else:
            from .workflow_traversal import automatic_edges
            selected = automatic_edges(r, node, token) if r.get("execution_contract") == "delegation" else None
            if selected is None:
                selected = await self._decision(node, result or token.get("context", {}), token)
        if selected is None:
            return
        current = self.run()
        token = next((t for t in current["pending"] if t["id"] == token["id"]), token)
        selected_join = validate_selection(current["definition"], node, selected, token, current["joins"])
        def advance(rr: dict[str, Any]) -> None:
            if rr["status"] != "running":
                return
            if rr["transitions"] + len(selected) > rr["definition"]["max_transitions"] + rr.get("transition_grant", 0):
                rr["status"] = "needs_attention"
                rr["attention_reason"] = "Transition limit reached"
                return
            try:
                validate_retry_budget(rr, selected)
            except RetryLimitReached as exc:
                rr.update(status="needs_attention", attention_reason=str(exc), exhausted_retry_edges=[exc.edge_id])
                return
            for edge in selected:
                if edge.get("backward"):
                    counts = rr.setdefault("retry_counts", {})
                    counts[edge["id"]] = counts.get(edge["id"], 0) + 1
            rr["transitions"] += len(selected)
            rr["pending"] = [t for t in rr["pending"] if t["id"] != token["id"]]
            stack = copy.deepcopy(token.get("stack", []))
            legacy_fork = node.get("branch_mode") == "all_matching" and len([e for e in rr["definition"]["connections"] if e["source"] == node["id"]]) > 1
            automatic_fork = node.get("branch_mode") == "auto" and len(selected) > 1 and not any(e["backward"] for e in selected)
            if legacy_fork or automatic_fork:
                gid = uuid.uuid4().hex
                join_id = selected_join if automatic_fork else node["join_id"]
                rr["joins"][gid] = {"join_id": join_id, "split_id": node["id"], "expected": len(selected), "arrived": [], "stack": stack, "selected_targets": [e["target"] for e in selected], "branch_ids": [e["id"] for e in selected], "parent_branch_ids": copy.deepcopy(token.get("branch_ids", {})), "branch_states": {e["id"]: "active" for e in selected}}
                stack = stack + [gid]
            for edge in selected:
                child = {"id": uuid.uuid4().hex, "node_id": edge["target"], "stack": stack, "context": _bounded(result or token.get("context", {})), "via": edge["id"]}
                child["failed_execution_refs"] = copy.deepcopy(token.get("failed_execution_refs", []))
                child["branch_ids"] = copy.deepcopy(token.get("branch_ids", {}))
                if legacy_fork or automatic_fork:
                    child["branch_ids"][gid] = edge["id"]
                if rr.get("execution_contract") == "delegation":
                    refs = ([token["execution_activation_id"]] if token.get("execution_activation_id") else token.get("input_result_refs", []))
                    child["input_result_refs"] = refs
                    target_node = next(n for n in rr["definition"]["nodes"] if n["id"] == edge["target"])
                    if rr.get("runner_policy") == "guided" and target_node["type"] == "end" and token.get("execution_activation_id"):
                        child["execution_activation_id"] = token["execution_activation_id"]
                        child["completion_source_node_id"] = node["id"]
                    dispatch = token.get("assignments", {}).get(edge["id"], {})
                    child.update({k: copy.deepcopy(dispatch[k]) for k in ("assignment_prompt", "assigned_task_ids", "additional_result_refs", "execution_session_mode", "resume_task_id", "resume_source_execution_id", "continue_previous", "session_reason") if k in dispatch})
                    if "structural_dispatch" in dispatch:
                        child.update(copy.deepcopy(dispatch["structural_dispatch"]))
                rr["pending"].append(child)
        self.update(advance, "transition", [e["id"] for e in selected])

    async def _legacy_execute_node(self, node: dict[str, Any], token: dict[str, Any]) -> None:
        r = self.run()
        count = sum(a["role"] == "node" and a["node_id"] == node["id"] and any(t["status"] != "not_started" for t in a["tasks"]) for a in r["activations"])
        if not token.get("recovered_result") and not token.get("recovered_failed_result") and count >= node["max_attempts"] + r["attempt_grants"].get(node["id"], 0):
            self.attention(f"Attempt limit reached for {node['id']}")
            return
        a = next(a for a in r["activations"] if a["id"] == token["execution_activation_id"]) if token.get("recovered_result") or token.get("recovered_failed_result") else self._activation(node["id"], "node", token)
        prompt = f"Workflow: {r['name']}\nTask: {r['prompt']}\nStep instructions: {node['instructions']}\nRole: {node['role']}\nChecklist: {json.dumps(r.get('tasks', []))}\nHuman recovery instructions: {r['instructions']}\nPrior context: {json.dumps(_bounded(token.get('context', {})))}\nRole defaults: {role_prompt(node['role'])}"
        failed = token.get("recovered_failed_result")
        if failed and (failed.get("permission_denials") or not availability_failure(failed)):
            reason = f"Recovered {node['id']} task failed"
            if failed.get("status") == "failed" and not failed.get("permission_denials") and optional_failure_join(self.run(), node, token):
                self._optional_failure(node, token, a, {"optional_failure": reason, "failure_evidence": _bounded(failed)})
            else:
                self.attention(reason)
            return
        if failed:
            self.update(lambda rr: next(x for x in rr["activations"] if x["id"] == a["id"]).update(status="running"), "fallback_activation_resumed", a["id"])
        result = token.get("recovered_result") or await self._dispatch(node, prompt, "node", a) or {}
        if not result:
            self.update(lambda rr: next(x for x in rr["activations"] if x["id"] == a["id"]).update(status="failed"), "activation_finished", a["id"])
            return
        if result.get("optional_failure"):
            self._optional_failure(node, token, a, result)
            return
        completed_ids: list[str] = []
        if node["role"] in {"planning", "implementation"}:
            try:
                output = parse_json(result.get("summary") or "")
                if node["role"] == "planning":
                    planned = output.get("tasks")
                    if not isinstance(planned, list) or not planned:
                        raise WorkflowError("Planning must return a nonempty tasks array")
                    ids: set[str] = set()
                    for task in planned:
                        tid = _identifier(task.get("id"))
                        if tid in ids or not isinstance(task.get("title"), str) or not task["title"].strip():
                            raise WorkflowError("Planning tasks need unique IDs and nonempty titles")
                        ids.add(tid)
                    def plan(rr: dict[str, Any]) -> None:
                        for task in planned:
                            old = next((t for t in rr["tasks"] if t["id"] == task["id"]), None)
                            if old:
                                old.update(title=task["title"], description=task.get("description", ""))
                            else:
                                rr["tasks"].append({"id": task["id"], "title": task["title"], "description": task.get("description", ""), "status": "pending", "source_activation_id": a["id"]})
                    self.update(plan, "tasks_planned", planned)
                else:
                    completed_ids = output.get("completed_task_ids", [])
                    if not isinstance(completed_ids, list) or any(t not in {x["id"] for x in self.run()["tasks"]} for t in completed_ids):
                        raise WorkflowError("Implementation referenced unknown checklist tasks")
            except (ValueError, TypeError, AttributeError) as exc:
                def invalid(rr: dict[str, Any]) -> None:
                    next(x for x in rr["activations"] if x["id"] == a["id"]).update(status="failed", result=_bounded(result), result_error=str(exc))
                    current = next(t for t in rr["pending"] if t["id"] == token["id"])
                    current.pop("recovered_result", None)
                    current.pop("execution_activation_id", None)
                if optional_failure_join(self.run(), node, token):
                    self._optional_failure(node, token, a, {"optional_failure": f"Invalid {node['role']} result: {exc}", "failure_evidence": _bounded(result)})
                    return
                self.update(invalid, "invalid_node_result", str(exc))
                self.attention(f"Invalid {node['role']} result: {exc}")
                return
        def finish(rr: dict[str, Any]) -> None:
            next(x for x in rr["activations"] if x["id"] == a["id"]).update(status="completed", result=_bounded(result))
            next(t for t in rr["pending"] if t["id"] == token["id"]).update(execution_complete=True, execution_activation_id=a["id"], result=_bounded(result), completed_task_ids=completed_ids)
        self.update(finish, "node_result_ready", a["id"])

    def _optional_failure(self, node: dict[str, Any], token: dict[str, Any], activation: dict[str, Any], evidence: dict[str, Any]) -> None:
        def record(rr: dict[str, Any]) -> None:
            current = next(t for t in rr["pending"] if t["id"] == token["id"])
            join = optional_failure_join(rr, node, current)
            if join is None:
                rr.update(status="needs_attention", attention_reason="Optional failure has no active safe parallel convergence")
                return
            next(a for a in rr["activations"] if a["id"] == activation["id"]).update(status="failed", result=_bounded(evidence), optional_failure=True)
            current.update(execution_complete=True, execution_activation_id=activation["id"], result=_bounded(evidence), optional_failure_join=join, completed_task_ids=[])
        self.update(record, "optional_node_failed", {"node_id": node["id"], "activation_id": activation["id"], "evidence": _bounded(evidence)})

    def _arrive_join(self, token: dict[str, Any]) -> bool:
        r = self.run()
        node = next(n for n in r["definition"]["nodes"] if n["id"] == token["node_id"])
        stack = token.get("stack", [])
        if stack and stack[-1] in r.get("released_parallel_groups", {}):
            self.update(lambda rr: rr.update(pending=[t for t in rr["pending"] if t["id"] != token["id"]]), "duplicate_join_arrival", token["id"])
            return True
        if not stack or r["joins"].get(stack[-1], {}).get("join_id") != node["id"]:
            if node["type"] == "parallel_end" and not token.get("joined"):
                raise WorkflowError("Parallel end lacks its matching active generation")
            return False
        if r["definition"].get("routing_mode") == "explicit":
            unresolved = [a for a in r["activations"] if a["id"] in set(token.get("input_result_refs", []) + token.get("failed_execution_refs", [])) and a.get("node_result", {}).get("status") != "succeeded" and not a.get("optional_failure") and not a.get("resolved_by_execution_id")]
            if unresolved:
                self.attention("Required failed parallel branch needs explicit recovery before its Parallel end")
                return True
        def arrive(rr: dict[str, Any]) -> None:
            stack = token.get("stack", [])
            if not stack:
                raise WorkflowError("Join lacks an activation generation")
            group = rr["joins"][stack[-1]]
            if group["join_id"] != node["id"]:
                raise WorkflowError("Join generation mismatch")
            branch_id = token.get("branch_ids", {}).get(stack[-1], token["id"])
            if branch_id not in group.setdefault("arrival_ids", []):
                group.setdefault("branch_states", {})[branch_id] = "optional_skipped" if token.get("context", {}).get("optional_failure") else "resolved"
                group["arrival_ids"].append(branch_id)
                group["arrived"].append(token)
            rr["pending"] = [t for t in rr["pending"] if t["id"] != token["id"]]
            if len(group["arrived"]) == group["expected"]:
                merged = {"id": uuid.uuid4().hex, "node_id": node["id"], "stack": group["stack"], "joined": True, "branch_ids": group.get("parent_branch_ids", {}), "context": {"branches": [t["context"] for t in group["arrived"]]}}
                if rr.get("execution_contract") == "delegation":
                    merged["input_result_refs"] = list(dict.fromkeys(ref for t in group["arrived"] for ref in t.get("input_result_refs", [])))
                    merged["failed_execution_refs"] = list(dict.fromkeys(ref for t in group["arrived"] for ref in t.get("failed_execution_refs", [])))
                rr["pending"].append(merged)
                rr.setdefault("released_parallel_groups", {})[stack[-1]] = {"join_id": node["id"], "split_id": group.get("split_id"), "expected": group["expected"], "branch_states": group.get("branch_states", {}), "branch_ids": group.get("arrival_ids", []), "merged_token_id": merged["id"]}
                del rr["joins"][stack[-1]]
        self.update(arrive, "join_arrival", token["id"])
        return True

    async def reconcile(self) -> bool:
        """A new supervisor may inspect records but cannot adopt missing pipe owners."""
        self.store.reconcile_run(self.run_id)
        if any(t["status"] == "uncertain" for a in self.run()["activations"] for t in a["tasks"]):
            timed = any(t.get("status") == "uncertain" and t.get("timeout_deadline") is not None for a in self.run()["activations"] for t in a["tasks"])
            self.attention("Supervisor interrupted during a timed node: persisted timeout deadline requires process reconciliation; no worker or timer replayed" if timed else "Supervisor interrupted: reconcile observed task results before continuing; dispatches were not replayed")
            return False
        return True

    async def execute(self, run_id: str) -> None:
        self.run_id = run_id
        if not await self.reconcile() and self.run()["status"] != "cancelling":
            return
        run = self.run()
        from . import identity
        def initialize(r: dict[str, Any]) -> None:
            # Seed Start even if a caller paused before this process acquired ownership.
            if r.get("kind") != "builder" and r["status"] not in TERMINAL and not r.get("execution_initialized") and not r["pending"] and not r["activations"]:
                start = next(n["id"] for n in r["definition"]["nodes"] if n["type"] == "start")
                r["pending"] = [{"id": uuid.uuid4().hex, "node_id": start, "stack": [], "context": {}}]
            r["execution_initialized"] = True
            if r["status"] == "starting":
                r["status"] = "running"
            r.update(supervisor_pid=os.getpid(), supervisor_identity=identity.own_identity())
        self.update(initialize, "supervisor_started")
        active: dict[str, asyncio.Task[Any]] = {}
        try:
            while True:
                run = self.run()
                if run["status"] == "cancelling":
                    for a in run["activations"]:
                        for t in a["tasks"]:
                            if t["status"] in {"running", "reserved", "uncertain"}:
                                await self.registry.cancel_cascade(t["task_id"], workflow_control=True)
                    if not active:
                        settled = self.store.reconcile_run(self.run_id)
                        if any(t["status"] in {"reserved", "running", "uncertain"} for a in settled["activations"] for t in a["tasks"]):
                            self.update(lambda r: r.update(status="needs_attention", attention_reason="Cancellation could not prove all dispatches settled"), "cancel_unresolved")
                        else:
                            def cancelled(r: dict[str, Any]) -> None:
                                r["status"] = "cancelled"
                                for activation in r["activations"]:
                                    if activation["role"] == "node" and activation.get("pending_question_id") and not activation.get("node_result"):
                                        activation["status"] = "cancelled"
                            self.update(cancelled, "cancelled")
                        break
                if run["status"] == "running":
                    for token in run["pending"]:
                        if token["id"] in active or len(active) >= run["definition"]["max_parallel"]:
                            continue
                        if not self._arrive_join(token):
                            active[token["id"]] = asyncio.create_task(self._node(token))
                    if not self.run()["pending"] and not active:
                        if self.run()["joins"]:
                            self.attention("No runnable branches remain while a Join is waiting")
                        else:
                            self.update(lambda r: r.update(status="completed"), "completed")
                        break
                if run["status"] in TERMINAL and not active:
                    break
                if not active and run["status"] in {"paused", "needs_attention", "needs_input"}:
                    break
                await asyncio.sleep(0.05)
                for tid, task in list(active.items()):
                    if task.done():
                        del active[tid]
                        try:
                            await task
                        except Exception as exc:
                            self.attention(f"Workflow scheduling failed: {exc}")
        finally:
            if active:
                await asyncio.gather(*active.values(), return_exceptions=True)
            self.update(lambda r: r.update(supervisor_pid=None, supervisor_identity=None), "supervisor_stopped")

    async def build(self, run_id: str) -> None:
        self.run_id = run_id
        recovered_forwarding = False
        def recover_feedback(r: dict[str, Any]) -> None:
            nonlocal recovered_forwarding
            for message in r.get("builder_messages", []):
                if message["status"] in {"forwarding", "forwarding_uncertain"}:
                    message["status"] = "forwarding_uncertain"
                    recovered_forwarding = True
            if recovered_forwarding:
                r.update(status="needs_attention", attention_reason="Builder feedback forwarding is uncertain; inspect the conversation before submitting it again", supervisor_pid=None, supervisor_identity=None)
        self.update(recover_feedback, "builder_feedback_reconciled")
        if recovered_forwarding:
            return
        while True:
            feedback: list[dict[str, Any]] = []
            def claim(r: dict[str, Any]) -> None:
                feedback.extend(m for m in r.get("builder_messages", []) if m["status"] in {"pending", "deferred"})
                for message in feedback:
                    message["status"] = "claimed"
                if feedback:
                    if "editing_definition" not in r and r.get("generated_definition"):
                        baseline = copy.deepcopy(r["generated_definition"])
                        r.update(editing_definition=copy.deepcopy(r.get("builder_draft", baseline)), editing_source={"name": r["name"], "revision": baseline.get("revision", 0), "saved_definition": baseline}, source_name=r["name"], source_revision=baseline.get("revision", 0), source_saved_definition=baseline)
                    r.update(builder_followup=True, builder_turn_prompt="\n".join(m["prompt"] for m in feedback), builder_turn_feedback_ids=[m["id"] for m in feedback])
            self.update(claim, "builder_feedback_claimed")
            await self._build_turn(run_id)
            continue_turn = False
            def settle(r: dict[str, Any]) -> None:
                nonlocal continue_turn
                if any(m["status"] == "forwarding_uncertain" for m in r.get("builder_messages", [])):
                    r.update(status="needs_attention", attention_reason="Builder feedback forwarding is uncertain; inspect the conversation before submitting it again")
                continue_turn = not any(m["status"] == "forwarding_uncertain" for m in r.get("builder_messages", [])) and r["status"] in {"completed", "needs_attention"} and any(m["status"] in {"pending", "deferred"} for m in r.get("builder_messages", [])) and not any(t["status"] in {"reserved", "running", "uncertain"} for a in r["activations"] for t in a["tasks"])
                if continue_turn:
                    r["status"] = "running"
                else:
                    r.update(supervisor_pid=None, supervisor_identity=None)
            self.update(settle, "builder_turn_settled")
            if not continue_turn:
                return

    async def _build_turn(self, run_id: str) -> None:
        self.run_id = run_id
        if not await self.reconcile():
            return
        from . import identity
        self.update(lambda r: r.update(status="running", supervisor_pid=os.getpid(), supervisor_identity=identity.own_identity()), "builder_started")
        a = self._activation("builder", "builder")
        self.update(lambda r: next(x for x in r["activations"] if x["id"] == a["id"]).update(feedback_ids=r.get("builder_turn_feedback_ids", [])), "builder_feedback_associated")
        prompt = "Create a Polybridge workflow definition. Do not write files or dispatch agents. Return ONLY a JSON object. Schema: " + json.dumps({"name": self.run()["name"], "routing_mode": "explicit", "orchestrator": {"backend": "codex", "fallbacks": []}, "nodes": [{"id": "start", "type": "start", "position": {"x": 80, "y": 80}, "branch_mode": "auto", "prompt": "Optional workflow purpose for the orchestrator"}, {"id": "work", "type": "agent", "position": {"x": 220, "y": 80}, "instructions": "...", "agent": {"backend": "codex"}, "session_mode": "agent_decides", "branch_mode": "auto", "max_attempts": 3, "max_context_questions": 10}, {"id": "end", "type": "end", "position": {"x": 480, "y": 80}, "branch_mode": "auto"}], "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}], "max_parallel": 4, "max_transitions": 100}) + "\nStart may contain an optional prompt string describing the workflow purpose; Polybridge supplies it to orchestrator decisions alongside the runtime user request. Set routing_mode to explicit. Ordinary nodes choose exactly one outgoing path. For parallel execution use structural parallel_start and parallel_end nodes sharing parallel_group_id. Parallel start selects all forward branches; every branch must reach its matching end. Branches may contain multiple steps and properly nested parallel groups. Do not create Join nodes. Conditions for choosing a group belong on incoming alternatives; split outgoing instructions describe branch purpose and never skip branches. Retry arrows return to an earlier ancestor step; Polybridge infers loops from topology, so do not set a backward flag. Put explicit failure/retry and success/continue conditions on arrows. A retry connection may set max_retries to a nonnegative integer: this caps actual traversals of that arrow across the entire run; 0 disables retry, and an absent value adds no edge cap. Node max_attempts and max_transitions still apply and may stop earlier. A retry is selected exclusively and must stay inside its parallel region. Agent nodes may set optional:true only inside a safe parallel branch with an actually selected required sibling and no required successor before convergence. Optional steps still execute; only definitive failures or exhausted available candidates bypass to that convergence with failure evidence. Do not make sequential steps or all branches optional. Agent roles are planning, implementation, review and task; supply custom step instructions, while Polybridge adds the role guidance and result protocol. Request: " + self.run().get("builder_turn_prompt", self.run()["prompt"])
        prompt += "\n" + BUILDER_LAYOUT_GUIDANCE
        if "editing_definition" in self.run():
            prompt += "\nRefine the current unsaved canvas below according to the request; it may be incomplete. Return the complete corrected workflow. Preserve existing node and connection IDs, agent settings, permissions and instructions unless the requested edit requires changing them. Preserve existing node positions exactly unless the user explicitly asks to move or rearrange existing nodes. Do not assign a revision or overwrite any saved definition. Read applicable repository AGENTS.md and skill files, especially skills specified in the request, and use their relevant guidance when refining the graph. You may read repository guidance and skills for context; only inspect files, do not implement the task or run the workflow. Current canvas:\n" + json.dumps(self.run()["editing_definition"])
        prompt += "\nPublish canvas progress after each logical edit using polybridge.apply_workflow_draft (or polybridge-ctl workflow-builder-apply), with definition and expected_draft_revision. This updates only your builder preview, never saved workflows. Read get_workflow_status(workflow_run_id=" + self.run()["workflow_run_id"] + ") for the current draft_revision when a revision conflicts, then read get_workflow_run_detail with view=builder_draft and follow next_cursor until has_more is false. Reassemble the complete draft, preserve concurrent edits, and reapply your changes using that latest draft_revision. Never retry with a stale canvas. Incomplete but render-safe graphs are allowed while building. If you applied any preview, the latest applied preview is authoritative and you may return a final summary; otherwise return the complete JSON definition. Current draft revision: " + str(self.run().get("draft_revision", 0)) + "\nCurrent draft:\n" + json.dumps(self.run().get("builder_draft", {}))
        if self.run().get("builder_has_repo_context") is False:
            prompt += "\nNo user repository was supplied. Your working directory is an isolated Polybridge builder workspace; do not infer a project or search elsewhere for repository context. Refine the supplied canvas and request without repository-specific skills or files."
        result = await self._dispatch({"id": "builder", "title": "Workflow builder"}, prompt, "builder", a)
        if result and self.run()["status"] == "running":
            try:
                current = self.run()
                applied = next(x for x in current["activations"] if x["id"] == a["id"]).get("draft_applied", False)
                d = validate_definition({**(current["builder_draft"] if applied else parse_json(result.get("summary") or "")), "name": current["name"], "routing_mode": "explicit"})
                d["draft"] = True
                if "editing_definition" in self.run() or self.run().get("builder_followup"):
                    metadata = self.run().get("editing_source", {"name": self.run()["name"], "revision": self.run().get("generated_definition", {}).get("revision", 0)})
                    d["revision"] = metadata["revision"] if metadata["name"] == d["name"] else 0
                    d.pop("updated_at", None)
                    self.update(lambda r: (r.update(status="completed", generated_definition=d, builder_draft=d, draft_revision=r.get("draft_revision", 0) + 1), next(x for x in r["activations"] if x["id"] == a["id"]).update(status="completed")), "builder_edit_proposed")
                else:
                    if current.get("caller_record"):
                        from types import SimpleNamespace
                        from .workflow_inspection import guard_saved_workflow_authority
                        guard_saved_workflow_authority(SimpleNamespace(record=task_store.TaskRecord(**current["caller_record"])), d)
                    saved = self.store.save(d["name"], d)
                    self.update(lambda r: (r.update(status="completed", generated_definition=saved, builder_draft=saved, draft_revision=r.get("draft_revision", 0) + 1), next(x for x in r["activations"] if x["id"] == a["id"]).update(status="completed")), "builder_saved")
            except (ValueError, FileExistsError) as exc:
                self.attention(f"Workflow proposal was invalid: {exc}" if "editing_definition" in self.run() else f"Generated workflow was not saved: {exc}")
        def stop(r: dict[str, Any]) -> None:
            next(x for x in r["activations"] if x["id"] == a["id"]).update(status="completed" if r["status"] == "completed" else "failed")
            if r["status"] == "cancelling":
                if any(t["status"] in {"reserved", "running", "uncertain"} for activation in r["activations"] for t in activation["tasks"]):
                    r.update(status="needs_attention", attention_reason="Cancellation requires dispatch reconciliation")
                else:
                    r["status"] = "cancelled"
        self.update(stop, "builder_stopped")


def _candidate_key(candidate: dict[str, Any]) -> str:
    return json.dumps({k: candidate.get(k) for k in ("backend", "model", "reasoning_effort", "max_turns")}, sort_keys=True)


def _bounded(value: Any, limit: int = 24000) -> Any:
    encoded = json.dumps(value, default=str)
    if len(encoded) <= limit:
        return value
    return {"truncated": True, "excerpt": encoded[:limit], "original_characters": len(encoded)}


async def _main(args: Any) -> None:
    from .tasks import TaskRegistry
    storage = WorkflowStore(Path(args.root))
    # Exclusive nonblocking run lock prevents duplicate supervisors. Separate from short
    # state locks so control operations remain available while agents are running.
    with (storage.runs / f"{_identifier(args.run_id)}.supervisor.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        run = storage.get_run(args.run_id)
        if run["kind"] == "builder" and run["status"] in TERMINAL and not any(m["status"] in {"pending", "deferred"} for m in run.get("builder_messages", [])):
            return
        from . import lineage
        caller = lineage.Caller(task_store.TaskRecord(**run["caller_record"]), run.get("caller_method", "ancestry")) if run.get("caller_record") else None
        supervisor = WorkflowSupervisor(TaskRegistry(log_dir=storage.root / "tasks", open_monitor=False, caller_override=caller), storage)
        try:
            if run["kind"] == "builder" and run["status"] != "cancelling":
                await supervisor.build(args.run_id)
            else:
                await supervisor.execute(args.run_id)
        except Exception as exc:
            supervisor.run_id = args.run_id
            if storage.get_run(args.run_id)["status"] == "cancelling":
                # A failed cancellation must not respawn the same broken supervisor.
                # Attempt a bounded settlement, then require explicit reconciliation.
                active_ids = [t["task_id"] for a in storage.get_run(args.run_id)["activations"] for t in a["tasks"] if t["status"] in {"running", "reserved", "uncertain"}]
                try:
                    await asyncio.wait_for(asyncio.gather(*(supervisor.registry.cancel_cascade(tid, workflow_control=True) for tid in active_ids), return_exceptions=True), timeout=5)
                except TimeoutError:
                    pass
                storage.update_run(args.run_id, lambda r: r.update(status="needs_attention", attention_reason=f"Cancellation interrupted: {exc}; reconcile dispatches before resuming", supervisor_pid=None, supervisor_identity=None), "cancel_supervisor_failed")
            else:
                supervisor.attention(f"Supervisor failure: {exc}")
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
            final = storage.get_run(args.run_id)
            if (final["kind"] == "builder" and final["status"] == "starting" and any(m["status"] == "pending" for m in final.get("builder_messages", []))) or (final["kind"] != "builder" and final["status"] in {"running", "cancelling"}):
                _launch(storage, args.run_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["supervise"])
    parser.add_argument("run_id")
    parser.add_argument("--root", required=True)
    asyncio.run(_main(parser.parse_args()))


def builder_feedback_ids(log_dir: Path, task_id: str) -> list[str]:
    storage = WorkflowStore(log_dir.parent)
    owner = storage.task_owner(task_id)
    if not owner or owner.get("role") != "builder":
        return []
    run = storage.get_run(owner["workflow_run_id"])
    return next((a.get("feedback_ids", []) for a in run["activations"] if any(t["task_id"] == task_id for t in a["tasks"])), [])


def builder_pending_messages(log_dir: Path, task_id: str, regular: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from . import inbox
    storage = WorkflowStore(log_dir.parent)
    owner = storage.task_owner(task_id)
    if not owner or owner.get("role") != "builder":
        return regular
    run = storage.get_run(owner["workflow_run_id"])
    owned = [t["task_id"] for a in run["activations"] if a["role"] == "builder" for t in a["tasks"]]
    if not owned or owned[-1] != task_id:
        return regular
    terminal: set[str] = set()
    for tid in owned:
        delivered = inbox.delivered_ids(log_dir, tid)
        if delivered is None:
            return []
        terminal.update(delivered)
    pending = []
    aliases = set()
    for message in run.get("builder_messages", []):
        mid = message["id"]
        delivery = message.get("inbox_message_id")
        if delivery:
            aliases.add(delivery)
        if mid in terminal or delivery in terminal:
            continue
        if message["status"] in {"pending", "deferred", "claimed", "forwarding", "queued_to_agent"}:
            pending.append({"id": mid, "text": message["prompt"], "status": "pending", "queued_at": message.get("created_at"), "delivery_id": delivery})
    return [m for m in regular if m["id"] not in aliases] + pending
