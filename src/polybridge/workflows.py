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


def is_executable(node: dict[str, Any]) -> bool:
    """Executable nodes dispatch work: an agent, or a whole referenced workflow."""
    return node.get("type") in {"agent", "workflow"}


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
    context_delivery = d.get("context_delivery", "legacy")
    if not isinstance(context_delivery, str) or context_delivery not in {"legacy", "optimized_v1"}:
        raise WorkflowError("context_delivery must be legacy or optimized_v1")
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
        if node.get("type") not in {"start", "agent", "join", "end", "parallel_start", "parallel_end", "workflow"}:
            raise WorkflowError(f"Invalid node type: {node_id}")
        if node["type"] in {"parallel_start", "parallel_end"} and d.get("routing_mode") != "explicit":
            raise WorkflowError("Parallel boundaries require explicit routing_mode")
        if node["type"] == "start":
            node.setdefault("prompt", "")
            if not isinstance(node["prompt"], str):
                raise WorkflowError("Start prompt must be a string")
        if node["type"] == "parallel_start":
            node.setdefault("branch_selection", "all")
            if not isinstance(node["branch_selection"], str) or node["branch_selection"] not in {"all", "orchestrator"}:
                raise WorkflowError("Parallel branch_selection must be all or orchestrator")
            node.setdefault("selection_guidance", "")
            if not isinstance(node["selection_guidance"], str):
                raise WorkflowError("Parallel selection_guidance must be a string")
        node.setdefault("title", node_id)
        node.setdefault("position", {"x": 80 + 260 * index, "y": 80})
        if not isinstance(node["title"], str) or not isinstance(node["position"], dict) or any(not isinstance(node["position"].get(k), (int, float)) or isinstance(node["position"].get(k), bool) or not math.isfinite(node["position"][k]) or node["position"][k] < 0 for k in ("x", "y")):
            raise WorkflowError(f"Invalid title or canvas position (x and y must be finite and nonnegative): {node_id}")
        if node.get("branch_mode", "auto") not in {"auto", "choose_one", "all_matching"}:
            raise WorkflowError(f"Invalid branching mode: {node_id}")
        # Saved definitions/new launches adopt automatic routing. Existing run
        # snapshots are never revalidated, so their historical modes stay intact.
        node["branch_mode"] = "choose_one" if d.get("routing_mode") == "explicit" and node["type"] != "parallel_start" else "auto"
        if node["type"] == "workflow":
            if d.get("routing_mode") != "explicit":
                raise WorkflowError(f"Run workflow nodes require explicit routing_mode: {node_id}")
            for field in ("agent", "role", "freedom", "network", "session_mode", "execution_mode", "max_context_questions"):
                if field in node:
                    raise WorkflowError(f"Run workflow nodes do not accept {field}: {node_id}")
            ref = node.get("workflow_ref")
            if not isinstance(ref, dict) or set(ref) - {"workflow_id"} or not isinstance(ref.get("workflow_id"), str) or not ref["workflow_id"].strip():
                raise WorkflowError(f"Run workflow node requires workflow_ref.workflow_id: {node_id}")
            node.setdefault("workflow_name", "")
            if not isinstance(node["workflow_name"], str):
                raise WorkflowError(f"Run workflow workflow_name must be text: {node_id}")
            node.setdefault("orchestrator_mode", "child")
            if node["orchestrator_mode"] not in {"child", "current"}:
                raise WorkflowError(f"Run workflow orchestrator_mode must be child or current: {node_id}")
            node.setdefault("child_session_policy", "agent_decides")
            if not isinstance(node["child_session_policy"], str) or node["child_session_policy"] not in {"fresh", "resume", "agent_decides"}:
                raise WorkflowError(f"Invalid child_session_policy: {node_id}")
            node.setdefault("instructions", "")
            if not isinstance(node["instructions"], str):
                raise WorkflowError(f"Run workflow instructions must be text: {node_id}")
            node.setdefault("optional", False)
            if not isinstance(node["optional"], bool):
                raise WorkflowError(f"Run workflow optional must be a boolean: {node_id}")
            node["max_attempts"] = _positive(node.get("max_attempts", 3), "max_attempts")
            timeout = node.get("timeout_seconds")
            if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 0):
                raise WorkflowError(f"timeout_seconds must be a nonnegative integer: {node_id}")
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
            node.setdefault("execution_mode", "headless")
            if node["execution_mode"] not in {"headless", "prefer_subagent"}:
                raise WorkflowError(f"Invalid execution mode: {node_id}")
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
    agents = [n for n in definition["nodes"] if n["id"] in region and is_executable(n)]
    return bool(agents) and not any(n.get("optional", False) for n in agents)


def _optional_suffix(definition: dict[str, Any], node_id: str, join_id: str) -> bool:
    region = _forward_reachable(definition, node_id, join_id)
    return node_id in region and not any(is_executable(n) and not n.get("optional", False) for n in definition["nodes"] if n["id"] in region)


def validate_optional_nodes(definition: dict[str, Any]) -> None:
    optional = {n["id"] for n in definition["nodes"] if n.get("optional", False) and is_executable(n)}
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
            if node.get("branch_selection", "all") == "all" and {e["id"] for e in selected} != forward:
                raise WorkflowError("Parallel start must select all forward branches")
            if any(e.get("backward") for e in selected):
                raise WorkflowError("Parallel start selections require forward branch entries")
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
    explicit_split = definition.get("routing_mode") == "explicit" and node["type"] == "parallel_start"
    if len(selected) == 1 and not explicit_split:
        return None
    join_id = next(n["id"] for n in definition["nodes"] if n["type"] == "parallel_end" and n.get("parallel_group_id") == node.get("parallel_group_id")) if explicit_split else _selection_join(definition, [edge["target"] for edge in selected])
    regions = [_forward_reachable(definition, edge["target"], join_id) for edge in selected]
    for index, region in enumerate(regions):
        for other in regions[:index]:
            overlap = region & other
            if overlap:
                raise WorkflowError("Selected parallel paths overlap before convergence: " + ", ".join(sorted(overlap)))
    optional_ids = {n["id"] for n in definition["nodes"] if is_executable(n) and n.get("optional", False)}
    if any(region & optional_ids for region in regions) and not any(_required_region(definition, region) for region in regions):
        raise WorkflowError("Selected optional parallel branches need an actually selected required sibling")
    return join_id


def _write(path: Path, value: Any) -> os.stat_result:
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("x", encoding="utf-8") as f:
            os.chmod(temp, 0o600)
            json.dump(value, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
            written_source = os.fstat(f.fileno())
        os.replace(temp, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return written_source
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

    @contextmanager
    def definitions_tree_lock(self):
        """Store-wide lock over the definitions dependency graph.

        Taken before any ``definition:<name>`` lock so concurrent saves serialize
        their dependency resolution. It is a plain flock: never reenter it.
        """
        with self.lock("definitions-tree"):
            yield

    def get(self, name: str) -> dict[str, Any]:
        from .workflow_references import workflow_identity
        definition = json.loads((self.definitions / f"{_workflow_name(name)}.json").read_text())
        definition.setdefault("workflow_id", workflow_identity(definition))
        for node in definition.get("nodes", []):
            if node.get("type") == "agent":
                node.setdefault("execution_mode", "headless")
        return definition

    def list(self) -> list[dict[str, Any]]:
        from .workflow_references import workflow_identity
        definitions = [json.loads(p.read_text()) for p in sorted(self.definitions.glob("*.json"))]
        for definition in definitions:
            definition.setdefault("workflow_id", workflow_identity(definition))
            for node in definition.get("nodes", []):
                if node.get("type") == "agent":
                    node.setdefault("execution_mode", "headless")
        return definitions

    def save(self, name: str, definition: dict[str, Any], expected_revision: int | None = None, *, authority_guard: Any = None) -> dict[str, Any]:
        """Validate, resolve and write under the definitions-tree lock.

        The candidate definition substitutes for its own slot while its dependency
        tree is resolved; the authority guard sees that resolved tree before any
        write happens. A workflow_id supplied inside the definition is ignored.
        """
        from .workflow_references import DependencyError, persist_workflow_id, resolve_dependencies_locked, workflow_identity
        d = validate_definition({**definition, "name": _workflow_name(name)})
        d.pop("workflow_id", None)
        d["execution_contract"] = "delegation"
        with self.definitions_tree_lock():
            with self.lock(f"definition:{name}"):
                path = self.definitions / f"{_workflow_name(name)}.json"
                old = json.loads(path.read_text()) if path.exists() else None
                if "context_delivery" not in definition:
                    d["context_delivery"] = old.get("context_delivery", "legacy") if old else "optimized_v1"
                # New authoring prefers native execution. Existing nodes whose
                # historical definition omitted this field retain Headless.
                old_nodes = {n["id"]: n for n in (old or {}).get("nodes", [])}
                raw_nodes = {n["id"]: n for n in definition.get("nodes", [])}
                for node in d["nodes"]:
                    if node.get("type") == "agent" and "execution_mode" not in raw_nodes[node["id"]]:
                        node["execution_mode"] = old_nodes.get(node["id"], {}).get("execution_mode", "headless") if node["id"] in old_nodes else "prefer_subagent"
                revision = old["revision"] if old else 0
                if expected_revision != revision and (old or expected_revision not in (None, 0)):
                    raise WorkflowError(f"Stale workflow revision: expected {expected_revision}, current {revision}")
                d["revision"] = revision + 1
                d["workflow_id"] = persist_workflow_id(d, old)
                try:
                    resolved = resolve_dependencies_locked(self, definition=d, substitute_name=name)
                except DependencyError as exc:
                    raise WorkflowError(str(exc)) from exc
                for node in d["nodes"]:
                    if node.get("type") != "workflow":
                        continue
                    pinned = resolved["workflows"].get(node["workflow_ref"]["workflow_id"])
                    if pinned:
                        node["workflow_name"] = pinned["name"]
                if authority_guard is not None:
                    authority_guard(resolved)
                d["updated_at"] = time.time()
                _write(path, d)
        return d

    def delete(self, name: str) -> dict[str, Any]:
        from .workflow_references import referencing_definitions, workflow_identity
        name = _workflow_name(name)
        with self.definitions_tree_lock():
            path = self.definitions / f"{name}.json"
            ident = workflow_identity(json.loads(path.read_text())) if path.exists() else None
            with self.lock(f"definition:{name}"):
                path.unlink()
        result: dict[str, Any] = {"deleted": name}
        if ident is not None:
            # Deleting a referenced workflow is allowed; the notice names its referrers.
            referenced_by = [definition for definition in referencing_definitions(self, ident) if definition != name]
            if referenced_by:
                result["referenced_by"] = referenced_by
                result["notice"] = "This workflow is still referenced by: " + ", ".join(referenced_by)
        return result

    def get_run(self, run_id: str, *, metadata_byte_limit: int | None = None, metadata_budget: Any = None) -> dict[str, Any]:
        path = self.runs / f"{_identifier(run_id)}.json"
        from .catalog import source_identity
        captured_source = source_identity(path.stat())
        if metadata_byte_limit is None:
            run = json.loads(path.read_text())
        else:
            from .bounded_io import read_json
            run = read_json(path, metadata_byte_limit, budget=metadata_budget)
        run["settling"] = any(t.get("status") in {"reserved", "running", "uncertain"} for a in run["activations"] for t in a["tasks"])
        if metadata_byte_limit is None and (self.runs / '.listing.v8.sqlite3').exists():
            from .catalog import Catalog
            catalog = Catalog(self.runs, '.json')
            with catalog.connect() as db:
                placeholder = db.execute("SELECT json_extract(payload,'$.needs_direct_lookup') FROM entries WHERE id=?", (run_id,)).fetchone()
            if placeholder and placeholder[0] and captured_source == source_identity(path.stat()):
                header = self._catalog_run_header(run)
                header['_source_identity'] = captured_source
                catalog.record(header, float(run['created_at']), run.get('status') not in TERMINAL, run_id)
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

    def run_header(self, run: dict[str, Any]) -> dict[str, Any]:
        from .workflow_responses import compact
        result = compact(run)
        for key in ('name', 'repo_path', 'prompt'):
            result[key] = str(run.get(key, ''))[:1000]
        result['prompt_truncated'] = len(str(run.get('prompt', ''))) > 1000
        definition = run.get('definition', {})
        result['backends'] = sorted({str(c.get('backend', ''))[:100] for c in [definition.get('orchestrator', {})] + [n.get('agent', {}) for n in definition.get('nodes', [])] if c.get('backend')})[:20]
        result['decisions'] = [{'reason': str(run['decisions'][-1].get('reason', ''))[:500]}] if run.get('decisions') else []
        result['sessions'] = {'orchestrator': str(run['sessions']['orchestrator'])[:128]} if run.get('sessions', {}).get('orchestrator') else {}
        # The index contains headers, never execution history, graph or worker output.
        for key in list(result):
            if isinstance(result[key], str):
                result[key] = result[key][:1000]
        from .catalog import bound_header
        return bound_header(result)

    def _index_run(self, run: dict[str, Any], *, previous_directory_mtime: int | None = None, written_source: Any = None) -> None:
        from .catalog import Catalog
        header = self._catalog_run_header(run)
        if written_source is not None:
            from .catalog import source_identity
            header['_source_identity'] = source_identity(written_source)
        Catalog(self.runs, '.json').record(header, float(run['created_at']), run.get('status') not in TERMINAL, run['workflow_run_id'], previous_directory_mtime=previous_directory_mtime)

    def _ownership_catalog(self):
        from .catalog import Catalog
        return Catalog(self.runs, '.json')

    def _catalog_run_header(self, run: dict[str, Any]) -> dict[str, Any]:
        header = self.run_header(run)
        header['_checkout'] = [{'task_id': task['task_id'], 'workflow_run_id': run['workflow_run_id'],
                                'repo_path': str(Path(run['repo_path']).resolve()), 'freedom': task.get('freedom', 'write_in_repo'),
                                'execution_kind': task.get('execution_kind', 'headless'),
                                'supervisor_pid': run.get('supervisor_pid'), 'supervisor_identity': run.get('supervisor_identity')}
                               for activation in run.get('activations', []) for task in activation.get('tasks', [])
                               if task.get('status') in {'running', 'reserved', 'uncertain'}]
        header['_associations'] = {task['task_id']: {'workflow_run_id': run['workflow_run_id'], 'workflow_node_id': activation.get('node_id'), 'workflow_role': activation.get('role'), 'node_id': activation.get('node_id'), 'role': activation.get('role'), 'activation_id': activation.get('id'), 'execution_contract': run.get('execution_contract'), 'status': run.get('status')}
                                   for activation in run.get('activations', []) for task in activation.get('tasks', [])}
        return header

    def list_run_page(self, *, limit: int = 100, cursor: str | None = None, active_only: bool = False, run_ids: list[str] | None = None) -> dict[str, Any]:
        from .catalog import Catalog
        def load(identifier: str, *, _metadata_budget=None):
            try:
                run = self.get_run(identifier, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=_metadata_budget)
            except FileNotFoundError:
                return None
            return self._catalog_run_header(run), float(run['created_at']), run.get('status') not in TERMINAL
        load.bounded_metadata = True
        catalog = Catalog(self.runs, '.json')
        if run_ids is not None:
            identifiers = [_identifier(identifier) for identifier in run_ids]
            return catalog.header_page(identifiers, load)
        return catalog.page(load, limit=limit, cursor=cursor, active_only=active_only)

    def get_run_header(self, run_id: str, *, _catalog: Any = None) -> dict[str, Any]:
        from .catalog import Catalog
        def load(identifier: str, *, _metadata_budget=None):
            try:
                run = self.get_run(identifier, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=_metadata_budget)
            except FileNotFoundError:
                return None
            return self._catalog_run_header(run), float(run['created_at']), run.get('status') not in TERMINAL
        load.bounded_metadata = True
        catalog = _catalog or Catalog(self.runs, '.json')
        headers = catalog.headers([_identifier(run_id)], load)
        if catalog.deferred_headers or catalog.blocked_headers:
            return {'workflow_run_id': run_id, 'indexing': True, 'catalog_state': catalog.requested_preparation()}
        if not headers:
            raise FileNotFoundError(f'Workflow run unavailable: {run_id}')
        return headers[0]

    def related_run_headers(self, run_ids: list[str], *, byte_budget: int = 64 * 1024) -> list[dict[str, Any]]:
        from .catalog import Catalog, RelatedHeaders, merge_read_state
        catalog = Catalog(self.runs, '.json')
        result, seen, queue, used = RelatedHeaders(), set(), list(run_ids), 0
        while queue and len(seen) < 32:
            identifier = queue.pop(0)
            if identifier in seen:
                continue
            seen.add(identifier)
            try:
                header = self.get_run_header(identifier, _catalog=catalog)
            except (OSError, ValueError, KeyError):
                continue
            if header.get('indexing'):
                state = header['catalog_state']
                result.catalog_state = merge_read_state(result.catalog_state, state)
                continue
            size = len(json.dumps(header, ensure_ascii=True).encode())
            if used + size > byte_budget:
                break
            result.append(header)
            used += size
            queue[0:0] = [header[key] for key in ('parent_workflow_run_id', 'root_workflow_run_id', 'orchestrator_session_owner_run_id') if header.get(key) and header[key] not in seen]
        return result

    def update_run(self, run_id: str, mutator: Any, event: str, detail: Any = None) -> dict[str, Any]:
        observed = self.get_run(run_id)
        root_id = (observed.get("parent_link") or {}).get("root_workflow_run_id") or run_id
        # All tree mutations serialize with the human cancellation eligibility check.
        from .catalog import catalog_lock, Catalog
        with self.lock(f"tree-mutation:{root_id}"), self.lock(f"run:{run_id}"), catalog_lock(self.runs):
            run = self.get_run(run_id)
            if event in {"dispatch_reserved", "native_reserved", "invocation_reserved", "child_decision_reopened"}:
                root = run if root_id == run_id else self.get_run(root_id)
                runnable = {"running", "building"} if run.get("kind") == "builder" else {"running"}
                if root.get("status") not in runnable or event != "child_decision_reopened" and run.get("status") not in runnable:
                    raise DispatchNotStarted("Scheduling stopped before reservation")
            attention_before = (run.get("status"), run.get("attention_reason"), run.get("failure_reason"), run.get("failed_decision_id"))
            mutator(run)
            attention_after = (run.get("status"), run.get("attention_reason"), run.get("failure_reason"), run.get("failed_decision_id"))
            if run.get("status") in {"needs_attention", "paused", "failed"}:
                if attention_before != attention_after or "attention_checkpoint" not in run:
                    run["attention_checkpoint"] = run.get("sequence", 0) + 1
            else:
                run.pop("attention_checkpoint", None)
            run["updated_at"] = time.time()
            run["sequence"] = run.get("sequence", 0) + 1
            run.pop("settling", None)
            run.pop("can_cancel_from_monitor", None)
            run.pop("monitor_cancel_reason", None)
            previous_directory_mtime = self.runs.stat().st_mtime_ns
            Catalog(self.runs, '.json').invalidate(run_id)
            written_source = _write(self.runs / f"{run_id}.json", run)
            for activation in run.get("activations", []):
                for task in activation.get("tasks", []):
                    receipt = {"workflow_run_id": run_id, "workflow_node_id": activation.get('node_id'), "workflow_execution_id": activation.get('id'), "workflow_role": activation.get('role'), "workflow_name": run.get('name'), "workflow_status": run.get('status'), "execution_contract": run.get('execution_contract'), "interaction_owner": run.get('interaction_owner', 'caller'), "root_workflow_run_id": (run.get('parent_link') or {}).get('root_workflow_run_id', run_id)}
                    if activation.get('role') == 'orchestrator' and run.get('orchestrator_session_owner_run_id'):
                        receipt['workflow_session_owner_run_id'] = run['orchestrator_session_owner_run_id']
                    _write(self.owners / f"{_identifier(task['task_id'])}.json", receipt)
            with (self.runs / f"{run_id}.jsonl").open("a", encoding="utf-8") as f:
                f.write(json.dumps({"sequence": run["sequence"], "time": run["updated_at"], "event": event, "detail": detail}) + "\n")
                f.flush()
                os.fsync(f.fileno())
            self._index_run(run, previous_directory_mtime=previous_directory_mtime, written_source=written_source)
            run["settling"] = any(t.get("status") in {"reserved", "running", "uncertain"} for a in run["activations"] for t in a["tasks"])
            return run

    def create_run(self, definition: dict[str, Any], prompt: str, repo_path: Path, *, freedom: str = "write_in_repo", network: bool | None = None, kind: str = "workflow", permission_policy: str = "legacy_ceiling", caller: Any = None, dependency_tree: dict[str, Any] | None = None) -> dict[str, Any]:
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
        from .workflow_native import scheduling_policy
        run["scheduling_policy"] = scheduling_policy(definition, dependency_tree)
        run["tasks"] = []
        if kind == "workflow":
            run["execution_contract"] = "delegation"
            run["runner_policy"] = "guided"
            run["execution_policy"] = "visit"
        run.update(retry_counts={}, retry_grants={})
        if dependency_tree is not None:
            run["dependency_tree"] = copy.deepcopy(dependency_tree)
        from .catalog import catalog_lock, Catalog
        with catalog_lock(self.runs):
            previous_directory_mtime = self.runs.stat().st_mtime_ns
            Catalog(self.runs, '.json').invalidate(rid)
            written_source = _write(self.runs / f"{rid}.json", run)
            self._index_run(run, previous_directory_mtime=previous_directory_mtime, written_source=written_source)
        return run

    def control(self, run_id: str, action: str, instructions: str | None = None, additional_attempts: int = 0, *, decision_id: str | None = None, allow_optional_review_skip: bool = False, interaction_owner: str | None = None, _internal: bool = False, _delivery: dict[str, Any] | None = None) -> dict[str, Any]:
        if action not in {"pause", "resume", "cancel", "recover"}:
            raise WorkflowError("Unknown workflow action")
        if not isinstance(additional_attempts, int) or isinstance(additional_attempts, bool) or additional_attempts < 0:
            raise WorkflowError("additional_attempts must be a nonnegative integer")
        if type(allow_optional_review_skip) is not bool:
            raise WorkflowError("allow_optional_review_skip must be a boolean")
        if allow_optional_review_skip and action != "resume":
            raise WorkflowError("Optional review skip consent is only available when resuming needs_input")
        observed = self.get_run(run_id)
        if _delivery is not None:
            if not _internal:
                raise WorkflowError("Forwarded delivery is internal only")
            if _delivery.get("payload", {}).get("allow_optional_review_skip", False) is not allow_optional_review_skip:
                raise WorkflowError("Forwarded consent does not match its delivery")
            if observed.get("forwarded_delivery_receipt") == _delivery:
                return observed

        def qualifying_skips(r: dict[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
            if r.get("status") != "needs_input":
                raise WorkflowError("Optional review skip consent requires a qualifying needs_input checkpoint")
            if decision_id != r.get("input_decision_id"):
                raise WorkflowError("Resuming needs_input requires the current input decision_id")
            from .workflow_delegation import optional_review_refusal_join
            eligible = []
            for token in r.get("pending", []):
                if token.get("decision_id") != decision_id:
                    continue
                execution = next((a for a in r["activations"] if a["id"] == token.get("execution_activation_id")), None)
                node = next((n for n in r["definition"]["nodes"] if n["id"] == token.get("node_id")), None)
                if execution and node and optional_review_refusal_join(r, node, execution, token):
                    eligible.append((node, execution))
            if not eligible:
                raise WorkflowError("No qualifying optional review refusal is awaiting this answer")
            return eligible

        def validate_forwarded_consent(r: dict[str, Any], visited: set[str]) -> None:
            if r["workflow_run_id"] in visited:
                raise WorkflowError("Forwarded input source contains a cycle")
            visited.add(r["workflow_run_id"])
            source_id = (r.get("input_source") or {}).get("workflow_run_id")
            if source_id not in (None, r["workflow_run_id"]):
                if r.get("status") != "needs_input" or decision_id != r.get("input_decision_id"):
                    raise WorkflowError("Resuming needs_input requires the current input decision_id")
                source = self.get_run(source_id)
                previous = r.get("forwarded_delivery")
                payload = (previous or {}).get("payload", {})
                if (previous and source.get("forwarded_delivery_receipt") == previous
                        and payload.get("allow_optional_review_skip") is True
                        and payload.get("decision_id") == decision_id
                        and payload.get("instructions") == instructions
                        and payload.get("additional_attempts") == additional_attempts
                        and payload.get("interaction_owner") == interaction_owner
                        and payload.get("source") == r.get("input_source")):
                    return
                validate_forwarded_consent(source, visited)
            else:
                qualifying_skips(r)

        if allow_optional_review_skip:
            # Validate before reconciliation or preparing any durable forwarded delivery.
            validate_forwarded_consent(observed, set())

        def record_delivery(r: dict[str, Any]) -> None:
            if _delivery is not None:
                r["forwarded_delivery_receipt"] = copy.deepcopy(_delivery)

        def prepare_delivery(source_id: str, source_action: str, source: dict[str, Any]) -> dict[str, Any]:
            payload = {"source_id": source_id, "action": source_action, "instructions": instructions,
                       "additional_attempts": additional_attempts, "decision_id": decision_id,
                       "allow_optional_review_skip": allow_optional_review_skip, "interaction_owner": interaction_owner, "source": source}
            previous = observed.get("forwarded_delivery")
            replace_delivered = False
            if previous is not None and (previous.get("payload", {}).get("source") != source or previous.get("payload", {}).get("decision_id") != decision_id):
                prior_source = self.get_run(previous["payload"]["source_id"])
                replace_delivered = prior_source.get("forwarded_delivery_receipt") == previous
            def prepare(r: dict[str, Any]) -> None:
                if r.get("interaction_owner") and interaction_owner is not None and interaction_owner != r["interaction_owner"]:
                    raise WorkflowError("Workflow interaction belongs to its original caller")
                if r.get("status") != observed.get("status") or r.get("input_decision_id") != observed.get("input_decision_id") or r.get("input_source") != observed.get("input_source") or r.get("attention_source") != observed.get("attention_source"):
                    raise WorkflowError("The forwarded source moved on; re-read the current run")
                pending = r.get("forwarded_delivery")
                if pending is not None and not (replace_delivered and pending == previous):
                    pending_payload = copy.deepcopy(pending.get("payload", {}))
                    # Existing deliveries predate structured consent and mean no opt-in.
                    pending_payload.setdefault("allow_optional_review_skip", False)
                    if pending_payload != payload:
                        raise WorkflowError("A forwarded delivery is pending; retry its original answer and grants")
                else:
                    r["forwarded_delivery"] = {"id": uuid.uuid4().hex, "payload": copy.deepcopy(payload)}
            prepared = self.update_run(run_id, prepare, "forwarded_delivery_prepared", {"source": source_id})
            return prepared["forwarded_delivery"]

        link = observed.get("parent_link") or {}
        if link and not _internal:
            # Public controls belong to the root; children are driven by their parents.
            raise WorkflowError("This run is a child of a Run workflow node; control the root workflow run " + str(link.get("root_workflow_run_id") or link.get("workflow_run_id")) + " instead")
        if action == "resume" and observed.get("status") == "needs_input" and (observed.get("input_source") or {}).get("workflow_run_id") not in (None, run_id):
            source_id = observed["input_source"]["workflow_run_id"]
            if not isinstance(instructions, str) or not instructions.strip():
                raise WorkflowError("Resuming needs_input requires an answer or reason")
            if decision_id != observed.get("input_decision_id"):
                raise WorkflowError("Resuming needs_input requires the current input decision_id")
            # The answer belongs to the source run; it grants no fresh child and no
            # attempts beyond the attention source.
            delivery = prepare_delivery(source_id, "resume", observed["input_source"])
            self.control(source_id, "resume", instructions=instructions, additional_attempts=additional_attempts, decision_id=decision_id, allow_optional_review_skip=allow_optional_review_skip, interaction_owner=interaction_owner, _internal=True, _delivery=delivery)
            def clear_forwarded(r: dict[str, Any]) -> None:
                if r.get("status") != "needs_input" or r.get("input_decision_id") != decision_id or r.get("input_source") != observed.get("input_source") or r.get("forwarded_delivery") != delivery:
                    raise WorkflowError("The forwarded question moved on; re-read the current question and answer again")
                record_delivery(r)
                r.pop("forwarded_delivery", None)
                r["status"] = "running"
                r["instructions"] = instructions or ""
                r.pop("input_question", None)
                r.pop("input_decision_id", None)
                r.pop("optional_review_skip_available", None)
                r.pop("input_source", None)
                r["suppressed_candidates"] = []
                for token in r.get("pending", []):
                    token["decision_attempts"] = 0
                    token.pop("decision_error", None)
            result = self.update_run(run_id, clear_forwarded, "forwarded_input_answered", {"source": source_id})
            if not _supervisor_present(result) and not result.get("parent_link"):
                _launch(self, run_id)
            return result
        if action == "resume" and observed.get("status") in {"needs_attention", "paused"} and (observed.get("attention_source") or {}).get("workflow_run_id") not in (None, run_id):
            source = observed["attention_source"]
            source_id = source["workflow_run_id"]
            source_run = self.get_run(source_id)
            previous_payload = (observed.get("forwarded_delivery") or {}).get("payload", {})
            same_source = previous_payload.get("source_id") == source_id and previous_payload.get("source") == source
            source_action = (previous_payload.get("action") if same_source else None) or ("recover" if source_run.get("status") == "failed" else "resume")
            delivery = prepare_delivery(source_id, source_action, source)
            # The grant and recovery instructions belong to the paused child;
            # resuming the root must not allocate a fresh invocation or grant its siblings.
            self.control(source_id, source_action, instructions=instructions, additional_attempts=additional_attempts,
                         interaction_owner=interaction_owner, _internal=True, _delivery=delivery)
            def clear_attention(r: dict[str, Any]) -> None:
                if r.get("status") not in {"needs_attention", "paused"} or r.get("attention_source") != source or r.get("forwarded_delivery") != delivery:
                    raise WorkflowError("The child attention source moved on; re-read the current run before resuming")
                record_delivery(r)
                r.pop("forwarded_delivery", None)
                r["status"] = "running"
                r.pop("attention_source", None)
                r.pop("optional_review_skip_available", None)
                r.pop("attention_reason", None)
                r.pop("suspended_via_root", None)
            result = self.update_run(run_id, clear_attention, "forwarded_attention_resumed", {"source": source_id})
            if not _supervisor_present(result) and not result.get("parent_link"):
                _launch(self, run_id)
            return result
        if action in {"resume", "recover", "cancel"}:
            observed = self.get_run(run_id)
            if observed.get("supervisor_identity"):
                from . import identity
                if identity.identity_check(observed["supervisor_identity"]) == "undecidable":
                    raise WorkflowError("Supervisor identity is uncertain; cannot " + ("cancel" if action == "cancel" else "resume or recover"))
            if action in {"resume", "recover"} and not _supervisor_present(observed):
                self.reconcile_run(run_id)
        control_detail: dict[str, Any] = {"instructions": instructions, "additional_attempts": additional_attempts, "retry_grants": {}, "allow_optional_review_skip": allow_optional_review_skip}
        def change(r: dict[str, Any]) -> None:
            if _delivery is not None and r.get("forwarded_delivery_receipt") == _delivery:
                return
            if action == "cancel" and interaction_owner == "monitor":
                from .workflow_cancellation import eligibility
                allowed, reason = eligibility(self, r)
                if not allowed:
                    raise WorkflowError(reason)
            elif r.get("execution_contract") == "delegation" and r.get("interaction_owner") and interaction_owner is not None and interaction_owner != r["interaction_owner"]:
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
                if any(a["status"] in {"reserved", "uncertain", "running"} and not a.get("invocation") or any(t["status"] in {"reserved", "uncertain", "running"} for t in a["tasks"]) for a in r["activations"]):
                    raise WorkflowError("Unresolved dispatches must be reconciled before resume")
                answered_executions: set[str] = set()
                if r["status"] == "needs_input":
                    if not isinstance(instructions, str) or not instructions.strip():
                        raise WorkflowError("Resuming needs_input requires an answer or reason")
                    if decision_id != r.get("input_decision_id"):
                        raise WorkflowError("Resuming needs_input requires the current input decision_id")
                    # The latest accepted answer replaces consent at this checkpoint.
                    # Keep grants for other checkpoints; receipt replay returns above
                    # without revoking a previously delivered answer's authorization.
                    authorizations = r.get("optional_skip_authorizations", {})
                    for execution_id in list(authorizations):
                        if authorizations[execution_id].get("source_decision_id") == decision_id:
                            del authorizations[execution_id]
                    if allow_optional_review_skip:
                        for node, execution in qualifying_skips(r):
                            r.setdefault("optional_skip_authorizations", {})[execution["id"]] = {"node_id": node["id"], "reason": instructions.strip(), "source_decision_id": decision_id, "allow_optional_review_skip": True, "granted_at": time.time()}
                    from .workflow_delegation import caller_decision_block, unresolved_required
                    for token in r.get("pending", []):
                        if token.get("decision_id") != decision_id:
                            continue
                        for execution in unresolved_required(r, token):
                            if caller_decision_block(execution):
                                answered_executions.add(execution["id"])
                                r.setdefault("blocker_retry_authorizations", {})[execution["id"]] = {"node_id": execution["node_id"], "reason": instructions, "authorization_kind": "caller_answer", "source_decision_id": decision_id, "granted_at": time.time()}
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
                r.pop("optional_review_skip_available", None)
                r.pop("failure_reason", None)
                r["suppressed_candidates"] = []
                exhausted_edges = r.pop("exhausted_retry_edges", [])
                if additional_attempts:
                    _positive(additional_attempts, "additional_attempts")
                    if isinstance(instructions, str) and instructions.strip():
                        from .workflow_delegation import settled
                        for execution in r["activations"]:
                            if execution["role"] == "node" and settled(execution) and execution.get("node_result", {}).get("status") == "blocked" and execution["id"] not in answered_executions:
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
            record_delivery(r)
        result = self.update_run(run_id, change, f"control:{action}", control_detail)
        if action in {"resume", "recover", "cancel"} and not _supervisor_present(result) and not result.get("parent_link"):
            _launch(self, run_id)
        return result

    def pinned_tasks(self) -> set[str]:
        pinned: set[str] = set()
        for r in self.list_runs():
            active = r["status"] not in TERMINAL or r.get("settling") or any(t["status"] in {"reserved", "running", "uncertain"} for a in r["activations"] for t in a["tasks"])
            link = r.get("parent_link") or {}
            if not active and link:
                # Every run in an active tree keeps its tasks pinned.
                root_id = link.get("root_workflow_run_id") or link.get("workflow_run_id")
                try:
                    active = self.get_run(root_id)["status"] not in TERMINAL
                except (OSError, ValueError, KeyError):
                    active = True
            if active:
                pinned.update(t["task_id"] for a in r["activations"] for t in a["tasks"])
        return pinned

    def task_association(self, task_id: str, *, strict: bool = False, _visited: set[str] | None = None, metadata_byte_limit: int | None = None) -> dict[str, Any] | None:
        visited = set() if _visited is None else _visited
        if task_id in visited:
            raise WorkflowError("Cyclic task lineage cannot establish workflow ownership")
        visited.add(task_id)
        indexed = self.owners / f"{_identifier(task_id)}.json"
        if indexed.exists():
            from .bounded_io import read_receipt
            receipt = read_receipt(indexed)
            runs = [self.get_run(receipt["workflow_run_id"], metadata_byte_limit=metadata_byte_limit)]
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

    def task_owner(self, task_id: str, *, strict: bool = False, metadata_byte_limit: int | None = None) -> dict[str, Any] | None:
        return self.task_association(task_id, strict=strict, metadata_byte_limit=metadata_byte_limit)

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
            if task.get("execution_kind") == "native_subagent":
                raise WorkflowError("Native child outcomes require child reconciliation; absence of a parent process is insufficient")
            if task_store.read(self.root / "tasks", task_id) is not None:
                raise WorkflowError("Recorded tasks require process reconciliation, not abandonment")
            update_task_state(activation, task, {"status": "not_started", "reconciliation": {"source": "human_confirmation", "reason": reason.strip(), "confirmed_no_process": True, "time": time.time()}})
            if all(t["status"] == "not_started" for t in activation["tasks"]):
                activation["status"] = "not_started"
            if r["status"] not in TERMINAL:
                r.update(status="needs_attention", attention_reason="Dispatch abandoned after human confirmation; explicit resume is required")
        result = self.update_run(run_id, abandon, "dispatch_abandoned", {"execution_id": execution_id, "task_id": task_id, "reason": reason.strip()})
        # Ambiguous spawn artifacts stay intact until no-process reconciliation is durable.
        # Cleanup is bookkeeping and must not undo an already committed reconciliation.
        try:
            from . import scratch
            scratch.remove(self.root / "tasks", task_id)
        except Exception:
            logging.getLogger(__name__).warning("Unable to remove abandoned dispatch scratch for %s", task_id, exc_info=True)
        return result

    def reconcile_run(self, run_id: str) -> dict[str, Any]:
        """Recover only outcomes positively recorded by their original task owner."""
        def reconcile(r: dict[str, Any]) -> None:
            for activation in r["activations"]:
                for task in activation["tasks"]:
                    if task["status"] not in {"reserved", "running", "uncertain"}:
                        continue
                    if task.get("execution_kind") == "native_subagent":
                        update_task_state(activation, task, {"status": "not_started" if task.get("dispatch_stage") == "preparing" else "uncertain"})
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
                if activation.get("invocation"):
                    # Invocation activations carry no tasks; their state derives
                    # from the persisted stage plus the linked child run.
                    from .workflow_invocation import derive_invocation_activation
                    node = next((n for n in r["definition"]["nodes"] if n["id"] == activation["node_id"]), {})
                    derive_invocation_activation(self, r, activation, node)
                    continue
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


def _require_no_active_historical_runs(storage: WorkflowStore) -> None:
    # One bounded migration step per attempt; callers retry after indexing settles.
    page = storage.list_run_page(active_only=True)
    if page.get("bootstrap_pending") or page.get("history_incomplete"):
        raise WorkflowError("Workflow history indexing is incomplete; retry after indexing or inspect oversized runs directly")
    with storage._ownership_catalog().connect() as db:
        historical = db.execute("SELECT 1 FROM entries INDEXED BY active_historical WHERE active=1 AND COALESCE(json_extract(payload,'$.kind'),'workflow')!='builder' AND COALESCE(json_extract(payload,'$.execution_contract'),'')!='delegation' LIMIT 1").fetchone()
    if historical:
        raise WorkflowError("Active historical runs must settle or be cancelled before delegation execution")


def _launch(storage: WorkflowStore, run_id: str) -> None:
    reservation = storage.get_run(run_id)
    log = storage.runs / f"{run_id}.supervisor.log"
    output = None
    try:
        output = log.open("ab")
        subprocess.Popen([sys.executable, "-m", "polybridge.workflows", "supervise", run_id, "--root", str(storage.root)], stdin=subprocess.DEVNULL, stdout=output, stderr=output, start_new_session=True, close_fds=True)
    except OSError as exc:
        # These failures precede a successfully returned child process. Keep the
        # reservation and its owner recoverable instead of leaving a phantom start.
        reason = f"Supervisor could not start for workflow run {run_id}: {exc}"
        def launch_failed(r: dict[str, Any]) -> None:
            # An independently advanced run owns its newer outcome and supervisor.
            if r.get("sequence") == reservation.get("sequence") and r.get("status") == reservation.get("status"):
                r.update(status="needs_attention", attention_reason=reason)
        storage.update_run(run_id, launch_failed, "supervisor_launch_failed")
        raise WorkflowError(reason) from exc
    finally:
        if output is not None:
            output.close()


async def start_workflow(name: str, prompt: str, repo_path: Path, *, overrides: dict[str, Any] | None = None, freedom: str | None = None, network: bool | None = None, root: Path | None = None, interaction_owner: str = "caller", definition_snapshot: dict[str, Any] | None = None, dependency_tree: dict[str, Any] | None = None, _verified_caller: Any = _CALLER_UNSET) -> dict[str, Any]:
    if interaction_owner not in {"caller", "monitor"}:
        raise WorkflowError("Invalid interaction owner")
    if not isinstance(prompt, str) or not prompt.strip():
        raise WorkflowError("Workflow prompt must be nonempty")
    storage = WorkflowStore(root)
    await asyncio.to_thread(_require_no_active_historical_runs, storage)
    if freedom is not None:
        raise WorkflowError("Workflow access is defined by saved nodes; caller freedom overrides are not supported")
    definition = copy.deepcopy(definition_snapshot) if definition_snapshot is not None else storage.get(name)
    if definition.get("name") != name:
        raise WorkflowError("Authorized workflow snapshot name mismatch")
    definition = launch_definition(definition, overrides)
    from .workflow_references import definition_sha256, resolve_dependencies
    if dependency_tree is None:
        # The caller's snapshot substitutes for its own stored slot while the tree
        # is pinned; a stale stored revision never reinterprets the run.
        dependency_tree = resolve_dependencies(storage, definition=definition, substitute_name=name)
    for entry in dependency_tree.get("workflows", {}).values():
        # Revalidate every pinned snapshot: a run is interpreted only by its pin.
        validate_definition(entry["definition"])
        if entry["definition_sha256"] != definition_sha256(entry["definition"]):
            raise WorkflowError("Pinned workflow snapshot does not match its content hash")
    root_entry = dependency_tree.get("workflows", {}).get(dependency_tree.get("root_workflow_id", ""))
    if not root_entry or root_entry["name"] != name:
        raise WorkflowError("Resolved dependency tree does not match the starting definition")
    if root_entry["definition_sha256"] != definition_sha256(definition):
        raise WorkflowError("Resolved dependency tree does not match the starting definition")
    caller = await _capture_caller(storage, None, _verified_caller)
    run = storage.create_run(definition, prompt, repo_path, freedom="unrestricted", network=network, permission_policy="saved_node", caller=caller, dependency_tree=dependency_tree)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(interaction_owner=interaction_owner), "interaction_owner")
    _launch(storage, run["workflow_run_id"])
    return run


def launch_definition(definition: dict[str, Any], overrides: dict[str, Any] | None) -> dict[str, Any]:
    """Apply root orchestrator settings before pinning and authority checks."""
    definition = copy.deepcopy(definition)
    if overrides:
        if overrides.get("backend", definition["orchestrator"]["backend"]) != definition["orchestrator"]["backend"]:
            for setting in ("model", "reasoning_effort", "max_turns"):
                definition["orchestrator"].pop(setting, None)
        definition["orchestrator"] = {**definition["orchestrator"], **overrides}
    return validate_definition(definition)


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
        if not isinstance(node.get("type"), str) or node.get("type") not in {"start", "agent", "join", "end", "parallel_start", "parallel_end", "workflow"}:
            raise WorkflowError(f"Invalid node type: {nid}")
        if node["type"] == "parallel_start":
            if not isinstance(node.get("branch_selection", "all"), str) or node.get("branch_selection", "all") not in {"all", "orchestrator"}:
                raise WorkflowError("Parallel branch_selection must be all or orchestrator")
            if not isinstance(node.get("selection_guidance", ""), str):
                raise WorkflowError("Parallel selection_guidance must be a string")
        if node["type"] == "workflow":
            ref = node.get("workflow_ref")
            if not isinstance(ref, dict) or not isinstance(ref.get("workflow_id"), str) or not ref["workflow_id"].strip():
                raise WorkflowError(f"Run workflow node requires workflow_ref.workflow_id: {nid}")
            if "orchestrator_mode" in node and node["orchestrator_mode"] not in {"child", "current"}:
                raise WorkflowError(f"Invalid orchestrator_mode: {nid}")
            if not isinstance(node.get("child_session_policy", "agent_decides"), str) or node.get("child_session_policy", "agent_decides") not in {"fresh", "resume", "agent_decides"}:
                raise WorkflowError(f"Invalid child_session_policy: {nid}")
            if "optional" in node and not isinstance(node["optional"], bool):
                raise WorkflowError(f"Invalid optional: {nid}")
            timeout = node.get("timeout_seconds")
            if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 0):
                raise WorkflowError(f"timeout_seconds must be a nonnegative integer: {nid}")
            if "max_attempts" in node:
                _positive(node["max_attempts"], "max_attempts")
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
            if "execution_mode" in node and node["execution_mode"] not in {"headless", "prefer_subagent"}:
                raise WorkflowError(f"Invalid execution mode: {nid}")
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
        from .catalog import Catalog
        from . import identity
        index = self.store._ownership_catalog()
        if not index.ready():
            self.store.list_run_page()
            return {"workflow_run_id": "unknown", "task_id": "indexing"}
        with index.connect() as db:
            rows = db.execute('SELECT checkout_tasks.payload,sources.mtime,sources.size,checkout_tasks.run_id FROM checkout_tasks LEFT JOIN sources ON sources.id=checkout_tasks.run_id WHERE repo=? ORDER BY checkout_tasks.id LIMIT 101', (self.repo,)).fetchall()
        if len(rows) > 100:
            return {"workflow_run_id": "unknown", "task_id": "too_many_checkout_owners"}
        budget = Catalog(self.store.root / 'tasks', task_store.RECORD_SUFFIX)
        checked = set()
        for row in rows:
            run_id = row[3]
            if run_id in checked:
                continue
            checked.add(run_id)
            try:
                source = (self.store.runs / f'{run_id}.json').stat()
                if (source.st_mtime_ns, source.st_size) != (row[1], row[2]):
                    with self.store.lock(f'run:{run_id}'):
                        refreshed = self.store.get_run(run_id, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=budget)
                        self.store._index_run(refreshed, previous_directory_mtime=self.store.runs.stat().st_mtime_ns, written_source=source)
                    return {"workflow_run_id": run_id, "task_id": "ownership_refreshed"}
            except (OSError, ValueError, TypeError):
                return {"workflow_run_id": run_id, "task_id": "ownership_uncertain"}
        for row in rows:
            task = json.loads(row[0])
            if not self.write and task.get('freedom') == 'read_only':
                continue
            if task.get("execution_kind") == "native_subagent":
                # The receipt remains authoritative even while its supervisor
                # lives: uncertainty can release the OS descriptor before that
                # supervisor exits. Existing pooled holders do not re-probe.
                return {"workflow_run_id": task["workflow_run_id"], "task_id": task["task_id"]}
            if _supervisor_present(task):
                continue
            try:
                record = task_store.read(self.store.root / 'tasks', task['task_id'], include_prompt=False, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=budget)
                observed = record is not None and record.status in task_store.TERMINAL_RECORD_STATUSES and not task_store.outcome_unobserved(record)
                verdict = 'dead' if observed else identity.check_detail(identity.task_identity(record.pid, record.start_time, record.markers))[0] if record is not None else 'undecidable'
            except (OSError, ValueError, TypeError):
                verdict = 'undecidable'
            if verdict != 'dead':
                return {"workflow_run_id": task['workflow_run_id'], "task_id": task['task_id']}
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
            try:
                conflict_owner = await asyncio.to_thread(self._orphan_owner)
            except BaseException:
                self.handle.close()
                raise
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
                    conflict_owner = await asyncio.to_thread(self._orphan_owner)
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
    def __init__(self, registry: Any, storage: WorkflowStore, tree: Any = None):
        from .workflow_invocation import WorkflowTree
        self.registry = registry
        self.store = storage
        self.run_id = ""
        # Deliberately process-local: restart requires a positively observed bootstrap.
        self.context_baselines: dict[str, dict[str, Any]] = {}
        self.decision_lock = asyncio.Lock()
        self.checkout_leases: dict[str, Any] = {}
        # A shared tree bounds harness turns, leases and session locks across the
        # whole run tree; a supervisor without one (builder) gets a no-op budget.
        self.tree = tree if tree is not None else WorkflowTree(storage, permits=None)
        if not hasattr(self.tree, "context_baselines"):
            self.tree.context_baselines = {}
        self.context_baselines = self.tree.context_baselines
        # Pooled checkout leases are tree-wide: every supervisor in the tree joins
        # the same descriptor so a child never downgrades the root's protection.
        self.checkout_leases = self.tree.leases

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
        decision_context = activation.get("_decision_context")
        if role == "node" and node.get("execution_mode") == "prefer_subagent":
            from .workflow_native import dispatch_native
            existing_task_ids = {task["task_id"] for a in self.run()["activations"] if a["id"] == activation["id"] for task in a["tasks"]}
            handled, outcome = await dispatch_native(self, node, prompt, activation)
            if handled:
                # Native controller turns have their own contract, with no context ack.
                self.context_baselines.clear()
                from .workflow_context import account_legacy_prompt
                from .workflow_prompt_delivery import delivery_metadata
                accounting = account_legacy_prompt(prompt, role=role, checkpoint=activation.get("id"))
                accounting["compatibility_reasons"] = ["native worker retains full inputs: assigned reader identity unavailable"]
                for task in next(a for a in self.run()["activations"] if a["id"] == activation["id"])["tasks"]:
                    if task["task_id"] not in existing_task_ids:
                        self._task_update(activation["id"], task["task_id"], {"context_delivery": delivery_metadata({}, accounting)})
                return outcome
        run = self.run()
        config = node.get("agent", run["definition"]["orchestrator"])
        candidates = [config] + config.get("fallbacks", [])
        key = node["id"] if role == "node" else role
        previous = run["sessions"].get(key, {})
        orchestrator_override = getattr(self, "orchestrator_override", None)
        if role == "orchestrator" and orchestrator_override is not None:
            # Current mode: this run's nodes are directed by the session owner's
            # orchestrator candidates and its retained session.
            owner_id, owner_config = orchestrator_override
            config = owner_config
            candidates = [config] + config.get("fallbacks", [])
            try:
                previous = self.store.get_run(owner_id).get("sessions", {}).get("orchestrator", {})
            except (OSError, ValueError, KeyError):
                previous = {}
        if role == "node" and activation.get("resume_task_id"):
            source = next((t for a in run["activations"] if (a["node_id"] == node["id"] or activation.get("continue_previous")) and a["role"] == "node" for t in a["tasks"] if t["task_id"] == activation["resume_task_id"]), None)
            if source:
                previous = {"task_id": source["task_id"], "candidate": key + ":" + _candidate_key(source["candidate"]), "session_id": source.get("result", {}).get("session_id")}
        persistent_orchestrator = role == "orchestrator" and run.get("runner_policy") == "guided"
        inherited = run.get("inherited_orchestrator_binding") if role == "orchestrator" and orchestrator_override is None and not run.get("sessions", {}).get("orchestrator") else None
        strict_child_resume = inherited is not None
        def child_resume_refused(reason: str) -> None:
            self.update(lambda r: r.update(child_session_refusal=reason), "child_session_resume_refused", reason)
            link = run.get("parent_link") or {}
            if link.get("workflow_run_id") and link.get("execution_id"):
                def record_refusal(parent: dict[str, Any]) -> None:
                    source = next((a for a in parent.get("activations", []) if a["id"] == link["execution_id"]), None)
                    if source:
                        source.setdefault("invocation", {})["child_session_refusal"] = reason
                self.store.update_run(link["workflow_run_id"], record_refusal, "child_session_resume_refused", reason)
            self.attention(reason)
        if strict_child_resume:
            from .workflow_child_sessions import validate_inherited
            try:
                validate_inherited(self.store, run)
            except (WorkflowError, OSError, ValueError, KeyError, TypeError) as exc:
                child_resume_refused("Child Resume refused: " + str(exc))
                return None
            previous = copy.deepcopy(inherited["binding"])
            # Resume inherits the actual source candidate, including fallback.
            # No other candidate may start a fresh conversation for this choice.
            candidates = [copy.deepcopy(inherited["candidate"])]
            reset_scope = "\nNEW CHILD WORKFLOW INVOCATION: the retained conversation supplies context only. Reset prior completion, recovery, graph, decisions, checklist, counters and worker-session assumptions. Only this new workflow scope and its issued continuations authorize actions. Prior workers confer no reuse authority.\n"
            prompt = reset_scope + prompt
            if decision_context is not None:
                decision_context = {**decision_context, "conversation_reset": reset_scope.strip()}
        if (persistent_orchestrator or role == "builder" and run.get("builder_followup")) and previous.get("task_id"):
            parent = self.registry.get(previous["task_id"])
            record = task_store.read(self.registry._log_dir, previous["task_id"])
            retained = parent.snapshot() if parent is not None else None
            compatible = (retained is not None and retained.get("status") == "completed" and retained.get("session_id")) or (record is not None and record.status == "completed" and record.session_id and record.freedom == "read_only" and record.repo_path == run["repo_path"])
            if not compatible:
                if strict_child_resume:
                    child_resume_refused("Child Resume refused: retained orchestrator session is unavailable")
                    return None
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
        candidate_position = 0
        while candidate_position < len(candidates):
            candidate = candidates[candidate_position]
            identity = key + ":" + _candidate_key(candidate)
            if identity in self.run()["suppressed_candidates"]:
                if strict_child_resume:
                    child_resume_refused("Child Resume refused: selected source candidate is suppressed")
                    return None
                capability_refused = capability_refused or identity in self.run().get("suppressed_capability_candidates", [])
                candidate_position += 1
                continue
            observed = next((t.get("result") for t in reversed(activation["tasks"]) if _candidate_key(t.get("candidate", {})) == _candidate_key(candidate) and t.get("result")), None)
            if observed and observed.get("timed_out") and not observed.get("outcome_unknown"):
                if strict_child_resume:
                    child_resume_refused("Child Resume timed out; explicit Fresh selection required")
                    return None
                previous = {}
                if candidate != candidates[-1]:
                    candidate_position += 1
                    continue  # Settled timeout already spent this candidate before restart.
                return {**observed, "execution_failure": observed.get("summary", "Node timed out"), "optional_failure_eligible": True}
            if observed and availability_failure(observed):
                if strict_child_resume:
                    child_resume_refused("Child Resume unavailable; explicit Fresh selection required")
                    return None
                self.update(lambda r: r["suppressed_candidates"].append(identity), "recovered_fallback", {"candidate": candidate, "reason": availability_failure(observed)})
                previous = {}
                candidate_position += 1
                continue
            backend = backends.get(candidate["backend"])
            if not backends.is_installed(backend):
                if strict_child_resume:
                    child_resume_refused("Child Resume refused: selected source CLI is unavailable")
                    return None
                self.update(lambda r: r["suppressed_candidates"].append(identity), "candidate_unavailable", {"candidate": candidate, "reason": "binary missing"})
                candidate_position += 1
                continue
            run = self.run()
            if not self.tree.tree_running(run):
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
            from .workflow_context import account_legacy_prompt, render_decision_context
            from .workflow_prompt_delivery import delivery_metadata, optimized, render_worker_inputs
            dispatch_prompt = prompt
            receipt: dict[str, Any] = {}
            classification = "protocol_repair" if activation.get("protocol_repair_of") or (decision_context or {}).get("correction") else "fallback" if candidate_position else "normal"
            if classification == "normal" and (decision_context or {}).get("inspection_results"):
                classification = "inspection"
            if optimized(run) and role == "orchestrator" and decision_context is not None:
                owner_id = orchestrator_override[0] if orchestrator_override else run["workflow_run_id"]
                owner_prefix = owner_id + ":" + identity
                session_owner = owner_prefix + ":" + str(previous.get("session_id") if use_resume else task_id)
                baseline = self.context_baselines.pop(session_owner, None)
                from .workflow_prompt_delivery import orchestrator_input_manifests
                manifests = orchestrator_input_manifests(run, decision_context)
                rendered = render_decision_context(decision_context, session_owner=session_owner, scope=run["workflow_run_id"], baseline=baseline if use_resume else None, force_bootstrap=strict_child_resume or classification in {"protocol_repair", "fallback"}, classification=classification, evidence_manifests=manifests)
                dispatch_prompt, receipt, accounting = rendered.prompt, rendered.receipt, rendered.accounting
                receipt["owner_prefix"] = owner_prefix
            elif optimized(run) and role == "node":
                dispatch_prompt, accounting = render_worker_inputs(prompt, run, node, activation, root=self.store.root)
            else:
                accounting = account_legacy_prompt(prompt, role=role, checkpoint=activation.get("decision_id", activation.get("id")), classification=classification)
                if optimized(run):
                    accounting["compatibility_reasons"] = ["special turn retains complete legacy context"]
                    accounting["classification"] = "clarification" if role == "orchestrator" else classification
                if role == "orchestrator":
                    # A legacy Current-mode child can share an optimized parent's
                    # session. Its unacknowledged scope must invalidate that base.
                    owner_id = orchestrator_override[0] if orchestrator_override else run["workflow_run_id"]
                    self.context_baselines.pop(owner_id + ":" + identity + ":" + str(previous.get("session_id")), None)
            accounting["session_mode"] = "resume" if use_resume else "fresh"
            accounting["checkpoint"] = (decision_context or {}).get("decision_id") or activation.get("decision_id") or activation["id"]
            if candidate_position:
                accounting["classification"] = "fallback"
            context_delivery = delivery_metadata(receipt, accounting)
            # The slot covers exactly one candidate attempt: acquired just before
            # the reservation is written, released on every exit from the attempt.
            # An uncertain attempt keeps its slot until reconciliation.
            slot_uncertain = False
            try:
                await self.tree.acquire_slot(lambda: self.tree.tree_running(self.run()), control=role != "node")
            except DispatchNotStarted:
                return None
            reservation = {"task_id": task_id, "candidate": candidate, "status": "reserved", "dispatch_stage": "preparing", "reserved_at": time.time(), "freedom": freedom, "assignment_prompt": display_prompt, "repo_path": run["repo_path"], "network": network, "session_mode": "resume" if use_resume else "fresh", "resume_task_id": previous.get("task_id") if use_resume else None, "execution_kind": "headless", "execution_fallback_reason": activation.get("execution_fallback_reason")}
            reservation["context_delivery"] = context_delivery
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
            try:
                self.update(reserve, "dispatch_reserved", reservation)
            except BaseException:
                # No spawn was requested; failed persistence must not strand a permit.
                self.tree.release_slot(control=role != "node")
                raise
            capability_stage = "settings"
            try:
                backends.reject_model(backend, candidate.get("model"))
                backends.reject_turn_cap(backend, candidate.get("max_turns"))
                backends.check_reasoning_effort(backend, candidate.get("reasoning_effort"))
                capability_stage = "enforcement"
                backend.enforcement(freedom, network)
                from .workflow_invocation import tree_write_strength
                root_id = (run.get('parent_link') or {}).get('root_workflow_run_id', run['workflow_run_id'])
                lease_run = run if root_id == run['workflow_run_id'] else self.store.get_run(root_id)
                async with CheckoutLease(self.store, run["repo_path"], tree_write_strength(lease_run, freedom), lambda: self.tree.tree_running(self.run()), on_wait=lambda detail: self.update(lambda r: r.update(checkout_wait=detail), "checkout_wait", detail), pool=self.checkout_leases):
                    if not self.tree.tree_running(self.run()):
                        self._task_update(activation["id"], task_id, {"status": "not_started"})
                        return None
                    capability_stage = "spawn"
                    timeout_deadline = time.time() + node["timeout_seconds"] if role == "node" and node.get("timeout_seconds") else None
                    self._task_update(activation["id"], task_id, {"dispatch_stage": "spawn_requested", **({"timeout_deadline": timeout_deadline} if timeout_deadline is not None else {})})
                    if role == "node" and freedom != "read_only":
                        from . import scratch
                        dispatch_prompt += "\nTask scratch directory (absolute): " + str(scratch.directory(self.registry._log_dir, task_id).resolve()) + "\nUse this directory for temporary artifacts outside the repository; artifacts are retained with task records."
                        extra = len(dispatch_prompt.encode()) - context_delivery["total_bytes"]
                        context_delivery["total_bytes"] = context_delivery["serialized_bytes"] = len(dispatch_prompt.encode())
                        context_delivery["total_characters"] = context_delivery["serialized_characters"] = len(dispatch_prompt)
                        context_delivery["budget_overflow_bytes"] = max(0, len(dispatch_prompt.encode()) - context_delivery["budget_bytes"])
                        context_delivery["sections"]["scratch_guidance"] = {"bytes": extra, "characters": len(dispatch_prompt) - accounting["serialized_characters"]}
                        self._task_update(activation["id"], task_id, {"context_delivery": context_delivery})
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
                            extra = len(dispatch_prompt.encode()) - context_delivery["total_bytes"]
                            context_delivery["sections"]["builder_preview"] = {"bytes": extra, "characters": len(dispatch_prompt) - context_delivery["total_characters"]}
                            context_delivery["total_bytes"] = context_delivery["serialized_bytes"] = len(dispatch_prompt.encode())
                            context_delivery["total_characters"] = context_delivery["serialized_characters"] = len(dispatch_prompt)
                            context_delivery["budget_overflow_bytes"] = max(0, len(dispatch_prompt.encode()) - context_delivery["budget_bytes"])
                            self._task_update(activation["id"], task_id, {"context_delivery": context_delivery})
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
                                slot_uncertain = True
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
                        slot_uncertain = True
                        self._task_update(activation["id"], task_id, {"status": "uncertain", "error": "Timeout cancellation did not produce a settled task snapshot"})
                        self.attention("Timed out task requires reconciliation before fallback")
                        return None
                    if timed_out and snapshot.get("status") != "completed":
                        notice = getattr(self.registry, "workflow_notice", None)
                        if callable(notice):
                            notice(task.task_id, "Node timed out after " + str(node["timeout_seconds"]) + " seconds; attempt stopped before fallback")
                        snapshot = {**snapshot, "status": "failed", "failure_kind": "timeout", "timeout_seconds": node["timeout_seconds"], "summary": "Node timed out after " + str(node["timeout_seconds"]) + " seconds", "timed_out": True}
                self._task_update(activation["id"], task_id, {"status": snapshot["status"], "result": snapshot, "finished_at": time.time(), "harness_metadata": observed_harness_metadata(snapshot, reservation["harness_metadata"]), "prompt_usage": {"usage": snapshot.get("usage"), "cost_usd": snapshot.get("total_cost_usd")}})
                if strict_child_resume and snapshot.get("session_id") != inherited["source_session_id"]:
                    child_resume_refused("Child Resume did not confirm the selected conversation identity; explicit Fresh selection required")
                    return None
                if snapshot.get("timed_out"):
                    if strict_child_resume:
                        child_resume_refused("Child Resume timed out; explicit Fresh selection required")
                        return None
                    previous = {}
                    if candidate != candidates[-1]:
                        candidate_position += 1
                        continue  # Cancellation settled before a Fresh fallback is dispatched.
                    return {**snapshot, "execution_failure": snapshot["summary"], "optional_failure_eligible": True}
                reason = availability_failure(snapshot)
                if reason:
                    if strict_child_resume:
                        child_resume_refused("Child Resume unavailable: " + reason + "; explicit Fresh selection required")
                        return None
                    if role == "node" and run.get("execution_contract") == "delegation" and node.get("session_mode") == "resume" and previous.get("candidate") == identity and not activation.get("continue_previous"):
                        from .workflow_delegation import require_fresh_checkpoint
                        self.update(lambda r: require_fresh_checkpoint(r, activation["id"], reason, identity), "resume_requires_fresh", {"task_id": task_id, "reason": reason})
                        return None
                    self.update(lambda r: r["suppressed_candidates"].append(identity), "fallback", {"task_id": task_id, "reason": reason})
                    previous = {}
                    candidate_position += 1
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
                if role == "orchestrator" and orchestrator_override is not None:
                    owner_id = orchestrator_override[0]
                    def record_owner_session(r_owner: dict[str, Any], _identity=identity, _task=task_id, _session=snapshot.get("session_id")) -> None:
                        # The checkpoint's new session is recorded on the owner.
                        r_owner.setdefault("sessions", {})["orchestrator"] = {"candidate": _identity, "task_id": _task, "session_id": _session}
                    self.store.update_run(owner_id, record_owner_session, "session_recorded", {"child_workflow_run_id": run["workflow_run_id"], "task_id": task_id})
                else:
                    self.update(lambda r: r["sessions"].__setitem__(key, {"candidate": identity, "task_id": task_id, "session_id": snapshot.get("session_id")}), "session_recorded", key)
                return snapshot
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
                if strict_child_resume:
                    child_resume_refused("Child Resume refused: " + str(exc) + "; explicit Fresh selection required")
                    return None
                self.update(lambda r: (r["suppressed_candidates"].append(identity), r.setdefault("suppressed_capability_candidates", []).append(identity) if capability_stage != "settings" else None), "candidate_capability_rejected", {"task_id": task_id, "candidate": candidate, "reason": str(exc)})
                if activation.get("resume_question_id") and node.get("session_mode") == "resume" and capability_stage == "spawn":
                    self.update(lambda r: next(q for a in r["activations"] if a["id"] == activation["id"] for q in a.get("questions", []) if q["question_id"] == activation["resume_question_id"]).update(answer_delivery_state="not_started"), "answer_resume_unsupported")
                    self.attention("Answer session cannot resume; orchestrator must explicitly choose Fresh")
                    return None
                previous = {}
                candidate_position += 1
                continue
            except (SessionUnknownError, SessionBusyError, RepoUnavailableError) as exc:
                self._task_update(activation["id"], task_id, {"status": "not_started", "error": str(exc)})
                if isinstance(exc, SessionUnknownError) and activation.get("continue_previous") and node.get("session_mode") == "resume" and not activation.get("resume_question_id"):
                    # Positive no-spawn refusal permits a Fresh bootstrap on the same
                    # candidate. The retry is a loop iteration that runs after the
                    # finally below has released this attempt's slot.
                    self.update(lambda r: next(a for a in r["activations"] if a["id"] == activation["id"]).update(session_reason="Started Fresh: retained session unavailable"), "previous_session_unavailable")
                    node = {**node, "session_mode": "fresh"}
                    activation = next(a for a in self.run()["activations"] if a["id"] == activation["id"])
                    previous = {}
                    continue
                if strict_child_resume:
                    child_resume_refused(f"Child Resume configuration refused: {exc}")
                else:
                    self.attention(f"Dispatch configuration refused: {exc}")
                return None
            except Exception as exc:
                # Once a dispatch was reserved, only positive evidence that no spawn happened
                # permits reconciliation. Unknown exceptions remain uncertain.
                not_started = capability_stage != "spawn" or getattr(exc, "polybridge_not_started", False) is True
                slot_uncertain = not not_started
                self._task_update(activation["id"], task_id, {"status": "not_started" if not_started else "uncertain", "error": str(exc)})
                if not_started:
                    self.update(lambda r: next(a for a in r["activations"] if a["id"] == activation["id"]).update(status="not_started") if all(t["status"] == "not_started" for a in r["activations"] if a["id"] == activation["id"] for t in a["tasks"]) else None, "dispatch_not_started")
                reason = f"Dispatch {task_id} did not start: {exc}" if not_started else f"Dispatch {task_id} requires reconciliation: {exc}"
                if strict_child_resume:
                    child_resume_refused(reason)
                else:
                    self.attention(reason)
                return None
            finally:
                # Every exit from the attempt releases its slot; an uncertain attempt
                # keeps the slot until reconciliation frees it explicitly.
                if slot_uncertain:
                    self.tree.hold_slot(self.run_id, task_id, control=role != "node")
                else:
                    self.tree.release_slot(control=role != "node")
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
        if is_executable(node):
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
            explicit_split = rr["definition"].get("routing_mode") == "explicit" and node["type"] == "parallel_start"
            automatic_fork = explicit_split or node.get("branch_mode") == "auto" and len(selected) > 1 and not any(e["backward"] for e in selected)
            if legacy_fork or automatic_fork:
                gid = uuid.uuid4().hex
                join_id = selected_join if automatic_fork else node["join_id"]
                rr["joins"][gid] = {"join_id": join_id, "split_id": node["id"], "expected": len(selected), "arrived": [], "stack": stack, "selected_targets": [e["target"] for e in selected], "branch_ids": [e["id"] for e in selected], "parent_branch_ids": copy.deepcopy(token.get("branch_ids", {})), "branch_states": {e["id"]: "active" for e in selected}}
                if explicit_split:
                    selected_ids = [e["id"] for e in selected]
                    excluded_ids = [e["id"] for e in rr["definition"]["connections"] if e["source"] == node["id"] and not e.get("backward") and e["id"] not in selected_ids]
                    rr["joins"][gid].update(selected_connection_ids=selected_ids, excluded_connection_ids=excluded_ids, selection_reason=token.get("selection_reason", "All configured branches apply"), selection_decision_id=token.get("selection_decision_id") or token.get("accepted_decision_id"), selection_sequence=rr.get("sequence", 0) + 1, branch_assignments=copy.deepcopy(token.get("assignments", {})))
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
                    child.update({k: copy.deepcopy(dispatch[k]) for k in ("assignment_prompt", "assigned_task_ids", "additional_result_refs", "execution_session_mode", "resume_task_id", "resume_source_execution_id", "continue_previous", "session_reason", "child_session_mode", "child_session_ref", "child_session_reason") if k in dispatch})
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
            released = r["released_parallel_groups"][stack[-1]]
            if "selected_connection_ids" in released and token.get("branch_ids", {}).get(stack[-1]) not in released["selected_connection_ids"]:
                raise WorkflowError("Parallel end arrival is not part of the frozen selected branches")
            self.update(lambda rr: rr.update(pending=[t for t in rr["pending"] if t["id"] != token["id"]]), "duplicate_join_arrival", token["id"])
            return True
        if not stack or r["joins"].get(stack[-1], {}).get("join_id") != node["id"]:
            if node["type"] == "parallel_end" and not token.get("joined"):
                raise WorkflowError("Parallel end lacks its matching active generation")
            return False
        active_group = r["joins"][stack[-1]]
        if "selected_connection_ids" in active_group and token.get("branch_ids", {}).get(stack[-1]) not in active_group["selected_connection_ids"]:
            raise WorkflowError("Parallel end arrival is not part of the frozen selected branches")
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
            selected_ids = group.get("selected_connection_ids")
            if selected_ids is not None and token.get("branch_ids", {}).get(stack[-1]) not in selected_ids:
                raise WorkflowError("Parallel end arrival is not part of the frozen selected branches")
            if branch_id not in group.setdefault("arrival_ids", []):
                group.setdefault("branch_states", {})[branch_id] = "optional_skipped" if token.get("context", {}).get("optional_failure") else "resolved"
                group["arrival_ids"].append(branch_id)
                group["arrived"].append(token)
            rr["pending"] = [t for t in rr["pending"] if t["id"] != token["id"]]
            membership_complete = set(group.get("arrival_ids", [])) == set(selected_ids) if selected_ids is not None else True
            if len(group["arrived"]) == group["expected"] and membership_complete:
                merged = {"id": uuid.uuid4().hex, "node_id": node["id"], "stack": group["stack"], "joined": True, "branch_ids": group.get("parent_branch_ids", {}), "context": {"branches": [t["context"] for t in group["arrived"]]}}
                if rr.get("execution_contract") == "delegation":
                    merged["input_result_refs"] = list(dict.fromkeys(ref for t in group["arrived"] for ref in t.get("input_result_refs", [])))
                    merged["failed_execution_refs"] = list(dict.fromkeys(ref for t in group["arrived"] for ref in t.get("failed_execution_refs", [])))
                rr["pending"].append(merged)
                rr.setdefault("released_parallel_groups", {})[stack[-1]] = {"join_id": node["id"], "split_id": group.get("split_id"), "expected": group["expected"], "branch_states": group.get("branch_states", {}), "branch_ids": group.get("arrival_ids", []), "merged_token_id": merged["id"], **{key: copy.deepcopy(group[key]) for key in ("selected_connection_ids", "excluded_connection_ids", "selection_reason", "selection_decision_id", "selection_sequence", "stack", "parent_branch_ids", "branch_assignments") if key in group}}
                del rr["joins"][stack[-1]]
        self.update(arrive, "join_arrival", token["id"])
        return True

    async def reconcile(self) -> bool:
        """A new supervisor may inspect records but cannot adopt missing pipe owners."""
        self.store.reconcile_run(self.run_id)
        self.tree.release_held(self.run_id)
        if any(a.get("invocation") and a.get("status") == "uncertain" for a in self.run()["activations"]):
            # An unreadable or mismatched child puts the tree in needs_attention.
            self.attention(next(a.get("result_error", "Child invocation is uncertain") for a in self.run()["activations"] if a.get("invocation") and a.get("status") == "uncertain"))
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
        if self.tree.slots is None and run.get("kind") != "builder" and not run.get("parent_link"):
            # The root owns the tree budget; children share this object.
            from .workflow_invocation import WorkflowTree
            self.tree = WorkflowTree(self.store, run_id, permits=run["definition"].get("max_parallel", 4), scheduling_policy=run.get("scheduling_policy", "legacy"))
            self.checkout_leases = self.tree.leases
            self.tree.supervisors[run_id] = self
        def initialize(r: dict[str, Any]) -> None:
            # Seed Start even if a caller paused before this process acquired ownership.
            if r.get("kind") != "builder" and r["status"] not in TERMINAL and not r.get("execution_initialized") and not r["pending"] and not r["activations"]:
                start = next(n["id"] for n in r["definition"]["nodes"] if n["type"] == "start")
                seeded = {"id": uuid.uuid4().hex, "node_id": start, "stack": [], "context": {}}
                if r.get("invocation_inputs"):
                    seeded["input_result_refs"] = [f"invocation:{entry['index']}" for entry in r["invocation_inputs"]]
                r["pending"] = [seeded]
            r["execution_initialized"] = True
            if r["status"] == "starting":
                r["status"] = "running"
            if r.get("kind") != "builder" and any(n.get("branch_selection") == "orchestrator" for n in r["definition"]["nodes"]) and (r.get("execution_contract") != "delegation" or r.get("runner_policy") != "guided"):
                r.update(status="needs_attention", attention_reason="Selectable parallel branches require guided delegation execution")
            r.update(supervisor_pid=os.getpid(), supervisor_identity=identity.own_identity())
        self.update(initialize, "supervisor_started")
        active: dict[str, asyncio.Task[Any]] = {}
        try:
            while True:
                run = self.run()
                link = run.get("parent_link") or {}
                if link and run["status"] not in TERMINAL and run["status"] != "cancelling":
                    # An ancestor can fail while this child is between turns. Stop
                    # its live work and settle via the normal cancellation path.
                    root_id = link.get("root_workflow_run_id") or link.get("workflow_run_id")
                    try:
                        root_status = self.store.get_run(root_id)["status"]
                    except (OSError, ValueError, KeyError):
                        root_status = None
                    if root_status in TERMINAL:
                        self.update(lambda r: r.update(status="cancelling"), "ancestor_terminal", {"root": root_id, "status": root_status})
                        run = self.run()
                if run["status"] == "cancelling":
                    cancellation = await self.tree.cancel_descendants(run_id, self.registry)
                    if cancellation["unresolved_runs"] or cancellation["errors"]:
                        self.update(lambda r: r.update(cancellation_errors=cancellation), "cancel_descendant_errors", cancellation)
                    for a in run["activations"]:
                        for t in a["tasks"]:
                            if t["status"] in {"running", "reserved", "uncertain"}:
                                outcome = await self.registry.cancel_cascade(t.get("transport_task_id", t["task_id"]), workflow_control=True)
                                from .workflow_cancellation import cascade_error
                                error = cascade_error(outcome)
                                if error:
                                    cancellation["errors"].append({"workflow_run_id": run_id, "task_id": t["task_id"], "error": error})
                    if not active:
                        settled = self.store.reconcile_run(self.run_id)
                        from .workflow_invocation import invocation_children_settled
                        if not invocation_children_settled(self.store, settled) or cancellation["unresolved_runs"] or cancellation["errors"] or any(t["status"] in {"reserved", "running", "uncertain"} for a in settled["activations"] for t in a["tasks"]) or any((a.get("invocation") or {}).get("stage") in {"preparing", "created", "running"} and a.get("status") not in {"completed", "failed", "cancelled"} for a in settled["activations"]):
                            issues = [f"{e['task_id']}: {e['error']}" for e in cancellation["errors"]]
                            if cancellation["unresolved_runs"]:
                                issues.append("Unresolved descendant links: " + ", ".join(cancellation["unresolved_runs"]))
                            reason = "Cancellation could not prove all dispatches settled" + (": " + "; ".join(issues) if issues else "")
                            self.update(lambda r: r.update(status="needs_attention", attention_reason=reason), "cancel_unresolved")
                        else:
                            def cancelled(r: dict[str, Any]) -> None:
                                r["status"] = "cancelled"
                                for activation in r["activations"]:
                                    if activation["role"] == "node" and activation.get("pending_question_id") and not activation.get("node_result"):
                                        activation["status"] = "cancelled"
                            self.update(cancelled, "cancelled")
                        break
                if self.tree.tree_running(run):
                    # Tokens awaiting a child never occupy a scheduling slot budget.
                    busy = len([tid for tid in active if tid not in self.tree.executing_children])
                    for token in run["pending"]:
                        if token["id"] in active or busy >= run["definition"]["max_parallel"]:
                            continue
                        if not self._arrive_join(token):
                            active[token["id"]] = asyncio.create_task(self._node(token))
                            busy += 1
                    if not self.run()["pending"] and not active:
                        if self.run()["joins"]:
                            self.attention("No runnable branches remain while a Join is waiting")
                        else:
                            self.update(lambda r: r.update(status="completed"), "completed")
                        break
                if run["status"] in TERMINAL and not active:
                    break
                if not active and (run["status"] in {"paused", "needs_attention", "needs_input"} or self.tree.root_suspended(run)):
                    # A suspended tree parks child loops; re-entry resumes the same child.
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
            self.tree.release_held(run_id)
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
        prompt = "Create a Polybridge workflow definition. Do not write files or dispatch agents. Return ONLY a JSON object. Schema: " + json.dumps({"name": self.run()["name"], "routing_mode": "explicit", "orchestrator": {"backend": "codex", "fallbacks": []}, "nodes": [{"id": "start", "type": "start", "position": {"x": 80, "y": 80}, "branch_mode": "auto", "prompt": "Optional workflow purpose for the orchestrator"}, {"id": "work", "type": "agent", "position": {"x": 220, "y": 80}, "instructions": "...", "agent": {"backend": "codex"}, "session_mode": "agent_decides", "execution_mode": "prefer_subagent", "branch_mode": "auto", "max_attempts": 3, "max_context_questions": 10}, {"id": "end", "type": "end", "position": {"x": 480, "y": 80}, "branch_mode": "auto"}], "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}], "max_parallel": 4, "max_transitions": 100}) + "\nStart may contain an optional prompt string describing the workflow purpose; Polybridge supplies it to orchestrator decisions alongside the runtime user request. Set routing_mode to explicit. Ordinary nodes choose exactly one outgoing path. For parallel execution use structural parallel_start and parallel_end nodes sharing parallel_group_id. Parallel start defaults to branch_selection all. Set branch_selection orchestrator and optional selection_guidance when applicability should be chosen for each group invocation. Selection chooses one or more branch entries and records a reason explaining selections and exclusions; every configured branch must reach its matching end. Branches may contain multiple steps and properly nested parallel groups. Do not create Join nodes. Conditions for choosing a group belong on incoming alternatives; split outgoing instructions describe branch purpose. Applicability selection is available only on Orchestrator selects groups; an empty selection is forbidden, so skipping a whole group uses an incoming alternative. Retry arrows return to an earlier ancestor step; Polybridge infers loops from topology, so do not set a backward flag. Put explicit failure/retry and success/continue conditions on arrows. A retry connection may set max_retries to a nonnegative integer: this caps actual traversals of that arrow across the entire run; 0 disables retry, and an absent value adds no edge cap. Node max_attempts and max_transitions still apply and may stop earlier. A retry is selected exclusively and must stay inside its parallel region. Agent nodes may set optional:true only inside a safe parallel branch with an actually selected required sibling and no required successor before convergence. Optional steps still execute; only definitive failures or exhausted available candidates bypass to that convergence with failure evidence. Do not make sequential steps or all branches optional. Agent roles are planning, implementation, review and task; supply custom step instructions, while Polybridge adds the role guidance and result protocol. Request: " + self.run().get("builder_turn_prompt", self.run()["prompt"])
        prompt += "\n" + BUILDER_LAYOUT_GUIDANCE
        prompt += "\nRun workflow nodes (type workflow) reference a saved workflow by workflow_ref.workflow_id. Preserve any existing Run workflow nodes exactly as supplied: keep their workflow_ref, orchestrator_mode, child_session_policy, max_attempts, timeout_seconds, optional flag and instructions unchanged. Never author new Run workflow nodes, change their references, or convert them to agent nodes."
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
                proposal = copy.deepcopy(current["builder_draft"] if applied else parse_json(result.get("summary") or ""))
                baseline = current.get("editing_definition", current.get("generated_definition", {}))
                previous_nodes = {n["id"]: n for n in baseline.get("nodes", [])}
                for proposed_node in proposal.get("nodes", []):
                    if proposed_node.get("type") == "agent" and "execution_mode" not in proposed_node:
                        proposed_node["execution_mode"] = previous_nodes.get(proposed_node["id"], {}).get("execution_mode", "headless") if proposed_node["id"] in previous_nodes else "prefer_subagent"
                d = validate_definition({**proposal, "name": current["name"], "routing_mode": "explicit"})
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
                        record = task_store.TaskRecord(**current["caller_record"])
                        # The guard sees the resolved dependency tree before any write.
                        saved = self.store.save(d["name"], d, authority_guard=lambda tree: guard_saved_workflow_authority(SimpleNamespace(record=record), tree))
                    else:
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
