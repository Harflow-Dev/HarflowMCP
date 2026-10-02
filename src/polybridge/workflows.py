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


def effective_freedom(node: dict[str, Any], ceiling: str) -> str:
    """A node and all of its fallback agents share the run's access ceiling."""
    if ceiling not in FREEDOMS:
        raise WorkflowError("Workflow freedom must be read_only, write_in_repo, publish or unrestricted")
    requested = node.get("freedom", ROLE_FREEDOM_DEFAULTS[node.get("role", "task")])
    if requested not in FREEDOMS:
        raise WorkflowError("Invalid node freedom")
    effective = FREEDOMS[min(FREEDOMS.index(requested), FREEDOMS.index(ceiling))]
    if node.get("role") == "implementation" and effective == "read_only":
        raise WorkflowError("Implementation nodes cannot run read_only; choose at least write_in_repo for the run ceiling")
    return effective


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
    d.setdefault("description", "")
    if not isinstance(d["description"], str):
        raise WorkflowError("Description must be text")
    d["orchestrator"] = _candidate(d.get("orchestrator", {"backend": "codex"}))
    d["max_parallel"] = _positive(d.get("max_parallel", 4), "max_parallel", 64)
    d["max_transitions"] = _positive(d.get("max_transitions", 100), "max_transitions")
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
        if node.get("type") not in {"start", "agent", "join", "end"}:
            raise WorkflowError(f"Invalid node type: {node_id}")
        if node["type"] == "start":
            node.setdefault("prompt", "")
            if not isinstance(node["prompt"], str):
                raise WorkflowError("Start prompt must be a string")
        node.setdefault("title", node_id)
        node.setdefault("position", {"x": 80 + 260 * index, "y": 80})
        if not isinstance(node["title"], str) or not isinstance(node["position"], dict) or any(not isinstance(node["position"].get(k), (int, float)) or isinstance(node["position"].get(k), bool) or not math.isfinite(node["position"][k]) for k in ("x", "y")):
            raise WorkflowError(f"Invalid title or canvas position: {node_id}")
        if node.get("branch_mode", "auto") not in {"auto", "choose_one", "all_matching"}:
            raise WorkflowError(f"Invalid branching mode: {node_id}")
        # Saved definitions/new launches adopt automatic routing. Existing run
        # snapshots are never revalidated, so their historical modes stay intact.
        node["branch_mode"] = "auto"
        if node["type"] == "agent":
            node.setdefault("role", "task")
            if node["role"] not in {"planning", "review", "implementation", "task"}:
                raise WorkflowError(f"Invalid agent role: {node_id}")
            node["agent"] = _candidate(node.get("agent", {"backend": "codex"}))
            node.setdefault("instructions", "")
            if not isinstance(node["instructions"], str) or node.get("network") not in (None, True, False):
                raise WorkflowError(f"Invalid instructions or network: {node_id}")
            node.setdefault("freedom", ROLE_FREEDOM_DEFAULTS[node["role"]])
            if node["freedom"] not in FREEDOMS:
                raise WorkflowError("Workflow nodes support read_only, write_in_repo, publish or unrestricted")
            if node["role"] == "implementation" and node["freedom"] == "read_only":
                raise WorkflowError("Implementation nodes cannot use read_only")
            node.setdefault("session_mode", "resume")
            if node["session_mode"] not in {"resume", "fresh"}:
                raise WorkflowError(f"Invalid session mode: {node_id}")
            node["max_attempts"] = _positive(node.get("max_attempts", 3), "max_attempts")
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
    return d


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
        for directory in (self.definitions, self.runs, self.leases):
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
        return json.loads((self.runs / f"{_identifier(run_id)}.json").read_text())

    def list_runs(self) -> list[dict[str, Any]]:
        return sorted((json.loads(p.read_text()) for p in self.runs.glob("*.json")), key=lambda r: r["created_at"], reverse=True)

    def update_run(self, run_id: str, mutator: Any, event: str, detail: Any = None) -> dict[str, Any]:
        with self.lock(f"run:{run_id}"):
            run = self.get_run(run_id)
            mutator(run)
            run["updated_at"] = time.time()
            run["sequence"] = run.get("sequence", 0) + 1
            _write(self.runs / f"{run_id}.json", run)
            with (self.runs / f"{run_id}.jsonl").open("a", encoding="utf-8") as f:
                f.write(json.dumps({"sequence": run["sequence"], "time": run["updated_at"], "event": event, "detail": detail}) + "\n")
                f.flush()
                os.fsync(f.fileno())
            return run

    def create_run(self, definition: dict[str, Any], prompt: str, repo_path: Path, *, freedom: str = "write_in_repo", network: bool | None = None, kind: str = "workflow") -> dict[str, Any]:
        if freedom not in FREEDOMS:
            raise WorkflowError("Workflow freedom must be read_only, write_in_repo, publish or unrestricted")
        for node in definition.get("nodes", []):
            if node.get("type") == "agent":
                effective_freedom(node, freedom)
        if not Path(repo_path).is_dir():
            raise WorkflowError("Repository directory does not exist")
        rid = uuid.uuid4().hex
        run = {"workflow_run_id": rid, "kind": kind, "name": definition["name"], "definition": copy.deepcopy(definition), "revision": definition.get("revision", 0), "prompt": prompt, "repo_path": str(Path(repo_path).resolve()), "freedom": freedom, "network": network, "status": "starting", "created_at": time.time(), "updated_at": time.time(), "sequence": 0, "transitions": 0, "activations": [], "decisions": [], "sessions": {}, "suppressed_candidates": [], "pending": [], "joins": {}, "instructions": "", "attempt_grants": {}, "supervisor_pid": None}
        run["tasks"] = []
        run.update(retry_counts={}, retry_grants={})
        _write(self.runs / f"{rid}.json", run)
        return run

    def control(self, run_id: str, action: str, instructions: str | None = None, additional_attempts: int = 0) -> dict[str, Any]:
        if action not in {"pause", "resume", "cancel"}:
            raise WorkflowError("Unknown workflow action")
        if not isinstance(additional_attempts, int) or isinstance(additional_attempts, bool) or additional_attempts < 0:
            raise WorkflowError("additional_attempts must be a nonnegative integer")
        if action == "resume" and not _supervisor_present(self.get_run(run_id)):
            self.reconcile_run(run_id)
        control_detail: dict[str, Any] = {"instructions": instructions, "additional_attempts": additional_attempts, "retry_grants": {}}
        def change(r: dict[str, Any]) -> None:
            if r["status"] in TERMINAL:
                raise WorkflowError("Workflow is terminal")
            if action == "resume":
                if r["status"] not in {"paused", "needs_attention"}:
                    raise WorkflowError("Only paused/attention workflows can resume")
                if any(a["status"] in {"reserved", "uncertain", "running"} or any(t["status"] in {"reserved", "uncertain", "running"} for t in a["tasks"]) for a in r["activations"]):
                    raise WorkflowError("Unresolved dispatches must be reconciled before resume")
                r["status"] = "running"
                r["instructions"] = instructions or ""
                r["suppressed_candidates"] = []
                exhausted_edges = r.pop("exhausted_retry_edges", [])
                if additional_attempts:
                    _positive(additional_attempts, "additional_attempts")
                    for n in r["definition"]["nodes"]:
                        r["attempt_grants"][n["id"]] = r["attempt_grants"].get(n["id"], 0) + additional_attempts
                    r["transition_grant"] = r.get("transition_grant", 0) + additional_attempts
                    for edge_id in exhausted_edges:
                        prior = r.get("retry_grants", {}).get(edge_id, 0)
                        r.setdefault("retry_grants", {})[edge_id] = prior + additional_attempts
                        control_detail["retry_grants"][edge_id] = {"additional": additional_attempts, "previous": prior, "total": prior + additional_attempts}
            else:
                r["status"] = "paused" if action == "pause" else "cancelling"
                if instructions:
                    r["instructions"] = instructions
        result = self.update_run(run_id, change, f"control:{action}", control_detail)
        if action in {"resume", "cancel"} and not _supervisor_present(result):
            _launch(self, run_id)
        return result

    def pinned_tasks(self) -> set[str]:
        return {t["task_id"] for r in self.list_runs() if r["status"] not in TERMINAL for a in r["activations"] for t in a["tasks"]}

    def task_association(self, task_id: str) -> dict[str, Any] | None:
        for r in self.list_runs():
            for a in r["activations"]:
                for t in a["tasks"]:
                    if t["task_id"] == task_id:
                        return {"workflow_run_id": r["workflow_run_id"], "node_id": a["node_id"], "role": a["role"], "activation_id": a["id"], "status": r["status"]}
        return None

    def task_owner(self, task_id: str) -> dict[str, Any] | None:
        return self.task_association(task_id)

    def reconcile_run(self, run_id: str) -> dict[str, Any]:
        """Recover only outcomes positively recorded by their original task owner."""
        def reconcile(r: dict[str, Any]) -> None:
            for activation in r["activations"]:
                for task in activation["tasks"]:
                    if task["status"] not in {"reserved", "running", "uncertain"}:
                        continue
                    record = task_store.read(self.root / "tasks", task["task_id"])
                    if record and record.status in {"completed", "failed", "timed_out", "cancelled"} and not task_store.outcome_unobserved(record):
                        task.update(status=record.status, result=task_store.snapshot(self.root / "tasks", record))
                    else:
                        task["status"] = "uncertain"
                if activation["status"] == "running" and all(t["status"] not in {"reserved", "running", "uncertain"} for t in activation["tasks"]):
                    activation["status"] = "completed" if activation["tasks"] and activation["tasks"][-1]["status"] == "completed" else "failed"
                if activation["role"] == "node" and activation["status"] == "completed" and activation["tasks"]:
                    token = next((t for t in r["pending"] if t["id"] == (activation.get("token") or {}).get("id")), None)
                    if token and not token.get("execution_complete"):
                        token.update(recovered_result=activation["tasks"][-1]["result"], execution_activation_id=activation["id"])
        return self.update_run(run_id, reconcile, "reconciled")


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


async def start_workflow(name: str, prompt: str, repo_path: Path, *, overrides: dict[str, Any] | None = None, freedom: str = "write_in_repo", network: bool | None = None, root: Path | None = None) -> dict[str, Any]:
    if not isinstance(prompt, str) or not prompt.strip():
        raise WorkflowError("Workflow prompt must be nonempty")
    storage = WorkflowStore(root)
    definition = storage.get(name)
    if overrides:
        if overrides.get("backend", definition["orchestrator"]["backend"]) != definition["orchestrator"]["backend"]:
            for setting in ("model", "reasoning_effort", "max_turns"):
                definition["orchestrator"].pop(setting, None)
        definition["orchestrator"] = {**definition["orchestrator"], **overrides}
    definition = validate_definition(definition)
    run = storage.create_run(definition, prompt, repo_path, freedom=freedom, network=network)
    await _capture_caller(storage, run["workflow_run_id"])
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
        if not isinstance(node.get("type"), str) or node.get("type") not in {"start", "agent", "join", "end"}:
            raise WorkflowError(f"Invalid node type: {nid}")
        if node["type"] == "start":
            node.setdefault("prompt", "")
            if not isinstance(node["prompt"], str):
                raise WorkflowError("Start prompt must be a string")
        node.setdefault("title", nid)
        node.setdefault("position", {"x": 80 + 260 * index, "y": 80})
        position = node["position"]
        if not isinstance(node["title"], str) or not isinstance(position, dict) or any(not isinstance(position.get(k), (float, int)) or isinstance(position.get(k), bool) or not math.isfinite(position[k]) or abs(position[k]) > 1000000 for k in ("x", "y")):
            raise WorkflowError(f"Invalid title or canvas position: {nid}")
        if node["type"] == "agent":
            node.setdefault("role", "task")
            if not isinstance(node["role"], str) or node["role"] not in {"planning", "implementation", "review", "task"}:
                raise WorkflowError(f"Invalid agent role: {nid}")
            node["agent"] = _candidate(node.get("agent", {"backend": "codex"}))
            node.setdefault("instructions", "")
            if not isinstance(node["instructions"], str):
                raise WorkflowError(f"Invalid instructions: {nid}")
            if "freedom" in node and (not isinstance(node["freedom"], str) or node["freedom"] not in FREEDOMS):
                raise WorkflowError(f"Invalid freedom: {nid}")
            if "session_mode" in node and (not isinstance(node["session_mode"], str) or node["session_mode"] not in {"resume", "fresh"}):
                raise WorkflowError(f"Invalid session mode: {nid}")
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


async def build_workflow(name: str, prompt: str, repo_path: Path | None = None, *, agent: dict[str, Any], fallbacks: list[dict[str, Any]] | None = None, definition: dict[str, Any] | None = None, source: dict[str, Any] | None = None, root: Path | None = None) -> dict[str, Any]:
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
    supplied_repo = repo_path is not None
    repo_path = repo_path if supplied_repo else builder_workspace(storage)
    run = storage.create_run(descriptor, prompt, repo_path, freedom="read_only", kind="builder")
    if editing is not None:
        run = storage.update_run(run["workflow_run_id"], lambda r: r.update(editing_definition=editing, editing_source=metadata, source_name=metadata["name"], source_revision=metadata["revision"], source_saved_definition=metadata["saved_definition"]), "builder_edit_requested")
    try:
        initial_preview = validate_builder_preview(editing or {"nodes": [], "connections": []}, name)
    except WorkflowError:
        # Repair input remains available to the agent, but cannot crash the canvas.
        initial_preview = {"name": name, "nodes": [], "connections": []}
    run = storage.update_run(run["workflow_run_id"], lambda r: r.update(builder_draft=initial_preview, draft_revision=0, builder_messages=[], builder_has_repo_context=supplied_repo), "builder_draft_initialized")
    await _capture_caller(storage, run["workflow_run_id"])
    _launch(storage, run["workflow_run_id"])
    return run


async def _capture_caller(storage: WorkflowStore, run_id: str) -> None:
    from . import lineage
    from .tasks import default_log_dir
    caller = await asyncio.to_thread(lineage.detect_caller, default_log_dir())
    if caller is not None:
        storage.update_run(run_id, lambda r: r.update(caller_record=asdict(caller.record), caller_method=caller.method), "caller_recorded")


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
    if stream and snapshot.get("backend") in {"opencode", "codex"}:
        try:
            with Path(stream).open(encoding="utf-8") as f:
                for line in f:
                    if len(line) > 8 * 1024 * 1024:
                        continue
                    try:
                        event = json.loads(line)
                    except ValueError:
                        continue
                    if not isinstance(event, dict):
                        continue
                    error = event.get("error")
                    if not isinstance(error, dict):
                        continue
                    if snapshot["backend"] == "opencode" and event.get("type") == "error" and isinstance(error.get("name"), str):
                        data = error.get("data")
                        if isinstance(data, dict):
                            if data.get("statusCode") in {401, 403}:
                                lines.append(f"OpenCode authentication failed (HTTP {data['statusCode']}); check the configured provider credentials.")
                            elif isinstance(data.get("message"), str):
                                lines.append(data["message"])
                    elif snapshot["backend"] == "codex" and event.get("type") == "turn.failed" and isinstance(error.get("message"), str):
                        lines.append(error["message"])
        except OSError:
            pass
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
    if snapshot.get("status") != "failed":
        return None
    diagnostic = "\n".join(snapshot.get("stderr_tail", []))
    backend = snapshot.get("backend")
    patterns = {
        "claude": [r"(?im)^.*(?:You've hit your limit|Credit balance is too low|rate_limit_error|model_not_found).*$"],
        "codex": [r"(?im)^.*(?:usage_limit_reached|insufficient_quota|model_not_found|rate_limit_exceeded).*$"],
        "opencode": [r"(?im)^.*(?:insufficient_quota|model_not_found|rate_limit_exceeded).*$"],
        "vibe": [r"(?im)^.*(?:insufficient_quota|model_not_found|rate_limit_exceeded).*$"],
    }
    if any(re.search(p, diagnostic) for p in patterns.get(backend, [])):
        return "backend availability rejected"
    if backend in {"claude", "codex", "opencode", "vibe", "antigravity"}:
        for line in diagnostic.splitlines():
            match = re.match(r"(?i)^(?:API[ _]Error|Provider[ _]Error|HTTP[ _]Error|APIConnectionError|APITimeoutError|ConnectError|ConnectionError)\s*:?\s*(.*)$", line)
            if not match:
                continue
            body = match.group(1)
            status = re.match(r"(?i)^(?:(?:HTTP(?:\s+status)?|status(?:\s*code)?)\s*[:=]?\s*)?([1-5][0-9]{2})\b", body)
            if status:
                if int(status.group(1)) in {500, 502, 503, 504, 529}:
                    return "provider server unavailable"
                # A known client/security status cannot become an outage from later prose.
                continue
            if re.search(r"(?i)\b(?:overloaded_error|api_connection_error|service unavailable|connection (?:reset|refused)|provider timeout|timed out)\b", body):
                return "provider transport unavailable"
    # Inspect only authoritative top-level protocol envelopes. Assistant messages,
    # tool payloads and summaries can contain arbitrary text and are never evidence.
    stream = snapshot.get("raw_stream_log")
    if stream:
        try:
            with Path(stream).open(encoding="utf-8") as f:
                for line in f:
                    if len(line) > 8 * 1024 * 1024:
                        continue
                    try:
                        event = json.loads(line)
                    except ValueError:
                        continue
                    if not isinstance(event, dict):
                        continue
                    if backend in {"claude", "codex", "opencode", "vibe", "antigravity"} and event.get("type") in ({"turn.failed"} if backend == "codex" else {"error"}):
                        error = event.get("error")
                        if isinstance(error, dict):
                            data = error.get("data") if isinstance(error.get("data"), dict) else {}
                            statuses = [error.get("status"), error.get("status_code"), error.get("statusCode"), data.get("statusCode")]
                            if any(type(status) is int and status in {500, 502, 503, 504, 529} for status in statuses):
                                return "provider server unavailable"
                            codes = [error.get("type"), error.get("code"), error.get("name")]
                            recognized = {"overloaded_error", "api_connection_error", "APITimeoutError", "APIConnectionError", "service_unavailable"}
                            if backend == "claude":
                                recognized.add("api_error")
                            if any(isinstance(code, str) and code in recognized for code in codes):
                                return "provider transport unavailable"
                    if backend == "codex" and event.get("type") == "turn.failed":
                        error = event.get("error", {})
                        if isinstance(error, dict) and error.get("code") in {"usage_limit_reached", "insufficient_quota", "model_not_found", "rate_limit_exceeded"}:
                            return "codex availability rejected"
                    if backend == "claude" and event.get("type") == "rate_limit_event":
                        info = event.get("rate_limit_info", {})
                        if isinstance(info, dict) and info.get("status") == "rejected":
                            return "claude quota rejected"
        except OSError:
            pass
    return None


class CheckoutLease:
    """OS leases shared across supervisors; process death releases the descriptor."""
    def __init__(self, storage: WorkflowStore, repo: str, write: bool, should_continue: Any = None):
        self.repo = str(Path(repo).resolve())
        self.store = storage
        self.path = storage.leases / (hashlib.sha256(self.repo.encode()).hexdigest() + ".lock")
        self.write = write
        self.should_continue = should_continue
        self.handle: Any = None

    async def __aenter__(self):
        self.handle = self.path.open("a")
        while True:
            if self.should_continue is not None and not self.should_continue():
                self.handle.close()
                raise DispatchNotStarted("Scheduling stopped before dispatch")
            # After a supervisor crash its agent processes may outlive the OS lease.
            # Their durable associations still block conflicting checkout activity.
            orphan_conflict = False
            for run in self.store.list_runs():
                if run["repo_path"] != self.repo or _supervisor_present(run):
                    continue
                for activation in run["activations"]:
                    for task in activation["tasks"]:
                        if task["status"] not in {"running", "reserved", "uncertain"}:
                            continue
                        record = task_store.read(self.store.root / "tasks", task["task_id"])
                        unresolved = record is None or record.status == "running" or task_store.outcome_unobserved(record)
                        if unresolved and (self.write or task.get("freedom", "write_in_repo") != "read_only"):
                            orphan_conflict = True
            if orphan_conflict:
                await asyncio.sleep(0.1)
                continue
            try:
                fcntl.flock(self.handle, (fcntl.LOCK_EX if self.write else fcntl.LOCK_SH) | fcntl.LOCK_NB)
                return self
            except BlockingIOError:
                await asyncio.sleep(0.1)

    async def __aexit__(self, *_: Any):
        if self.handle:
            fcntl.flock(self.handle, fcntl.LOCK_UN)
            self.handle.close()


class WorkflowSupervisor:
    def __init__(self, registry: Any, storage: WorkflowStore):
        self.registry = registry
        self.store = storage
        self.run_id = ""
        self.decision_lock = asyncio.Lock()

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
        preferred = previous.get("candidate")
        if preferred:
            candidates.sort(key=lambda c: key + ":" + _candidate_key(c) != preferred)
        for candidate in candidates:
            identity = key + ":" + _candidate_key(candidate)
            if identity in self.run()["suppressed_candidates"]:
                continue
            backend = backends.get(candidate["backend"])
            if not backends.is_installed(backend):
                self.update(lambda r: r["suppressed_candidates"].append(identity), "candidate_unavailable", {"candidate": candidate, "reason": "binary missing"})
                continue
            run = self.run()
            if run["status"] != "running":
                return None
            try:
                freedom = "read_only" if role != "node" else effective_freedom(node, run["freedom"])
            except WorkflowError as exc:
                self.attention(f"Dispatch configuration refused: {exc}")
                return None
            network = False if run["network"] is False else node.get("network", run["network"])
            task_id = uuid.uuid4().hex
            display_prompt = run.get("builder_turn_prompt", run["prompt"]) if role == "builder" else run["prompt"]
            reservation = {"task_id": task_id, "candidate": candidate, "status": "reserved", "reserved_at": time.time(), "freedom": freedom}
            self.update(lambda r: next(a for a in r["activations"] if a["id"] == activation["id"])["tasks"].append(reservation), "dispatch_reserved", reservation)
            try:
                backends.reject_model(backend, candidate.get("model"))
                backends.reject_turn_cap(backend, candidate.get("max_turns"))
                backends.check_reasoning_effort(backend, candidate.get("reasoning_effort"))
                backend.enforcement(freedom, network)
                async with CheckoutLease(self.store, run["repo_path"], freedom != "read_only", lambda: self.run()["status"] == "running"):
                    if self.run()["status"] != "running":
                        self._task_update(activation["id"], task_id, {"status": "not_started"})
                        return None
                    if (role == "node" and node.get("session_mode") == "resume" or role == "builder" and run.get("builder_followup")) and previous.get("candidate") == identity:
                        parent = self.registry.get(previous["task_id"])
                        if parent:
                            task = await self.registry.resume(parent, prompt, max_turns=candidate.get("max_turns"), network=network, task_id=task_id, display_prompt=display_prompt, workflow_builder=role == "builder")
                        else:
                            record = task_store.read(self.registry._log_dir, previous["task_id"])
                            if record is None:
                                raise WorkflowError("Previous resume session is unavailable")
                            task = await self.registry.resume_record(record, prompt, max_turns=candidate.get("max_turns"), network=network, task_id=task_id, display_prompt=display_prompt, workflow_builder=role == "builder")
                    else:
                        if role == "builder":
                            latest = self.run()
                            prompt += "\nLatest authoritative builder preview, revision " + str(latest.get("draft_revision", 0)) + ":\n" + json.dumps(latest.get("builder_draft", {}))
                        task = await self.registry.start(prompt, Path(run["repo_path"]), backend=backend, freedom=freedom, network=network, model=candidate.get("model"), reasoning_effort=candidate.get("reasoning_effort"), max_turns=candidate.get("max_turns"), task_id=task_id, display_prompt=display_prompt, workflow_builder=role == "builder", title=f"{run['name']} · {node.get('title', key)}")
                    self._task_update(activation["id"], task_id, {"status": "running"})
                    while not task.done.is_set():
                        if role == "builder" and getattr(task, "live_input", False):
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
                self._task_update(activation["id"], task_id, {"status": snapshot["status"], "result": snapshot, "finished_at": time.time()})
            except DispatchNotStarted:
                self._task_update(activation["id"], task_id, {"status": "not_started"})
                return None
            except backends.NestedDispatchRefused as exc:
                self._task_update(activation["id"], task_id, {"status": "not_started", "error": str(exc)})
                self.attention(f"Dispatch configuration refused: {exc}")
                return None
            except backends.UnsupportedCapability as exc:
                self._task_update(activation["id"], task_id, {"status": "not_started", "error": str(exc)})
                self.update(lambda r: r["suppressed_candidates"].append(identity), "candidate_capability_rejected", {"task_id": task_id, "candidate": candidate, "reason": str(exc)})
                previous = {}
                continue
            except (SessionUnknownError, SessionBusyError, RepoUnavailableError) as exc:
                self._task_update(activation["id"], task_id, {"status": "not_started", "error": str(exc)})
                self.attention(f"Dispatch configuration refused: {exc}")
                return None
            except Exception as exc:
                # Once a dispatch was reserved, only positive evidence that no spawn happened
                # permits reconciliation. Unknown exceptions remain uncertain.
                self._task_update(activation["id"], task_id, {"status": "uncertain", "error": str(exc)})
                self.attention(f"Dispatch {task_id} requires reconciliation: {exc}")
                return None
            reason = availability_failure(snapshot)
            if reason:
                self.update(lambda r: r["suppressed_candidates"].append(identity), "fallback", {"task_id": task_id, "reason": reason})
                previous = {}
                continue
            if snapshot["status"] != "completed" or snapshot.get("is_error"):
                diagnostic = failure_diagnostic(snapshot, prompt)
                self.attention(f"{role} task {task_id} ended {snapshot['status']}" + (f": {diagnostic}" if diagnostic else ""))
                return None
            self.update(lambda r: r["sessions"].__setitem__(key, {"candidate": identity, "task_id": task_id, "session_id": snapshot.get("session_id")}), "session_recorded", key)
            return snapshot
        self.attention(f"All agents unavailable for {key}")
        return None

    def _task_update(self, aid: str, tid: str, values: dict[str, Any]) -> None:
        self.update(lambda r: next(t for a in r["activations"] if a["id"] == aid for t in a["tasks"] if t["task_id"] == tid).update(values), "task_state", {"task_id": tid, **values})

    def _activation(self, node_id: str, role: str, token: dict[str, Any] | None = None) -> dict[str, Any]:
        a = {"id": uuid.uuid4().hex, "node_id": node_id, "role": role, "status": "running", "tasks": [], "created_at": time.time(), "token": token}
        self.update(lambda r: r["activations"].append(a), "activation_started", a)
        return a

    async def _decision(self, node: dict[str, Any], result: dict[str, Any], token: dict[str, Any]) -> list[dict[str, Any]] | None:
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

    async def _node(self, token: dict[str, Any]) -> None:
        r = self.run()
        node = next(n for n in r["definition"]["nodes"] if n["id"] == token["node_id"])
        result: dict[str, Any] = {}
        if node["type"] == "agent":
            if token.get("execution_complete"):
                result = token.get("result", {})
            else:
                await self._execute_node(node, token)
                token = next((t for t in self.run()["pending"] if t["id"] == token["id"]), token)
                result = token.get("result", {})
            if not result:
                return
        if node["type"] == "end":
            self.update(lambda rr: rr["pending"].remove(token), "end_reached", node["id"])
            return
        if token.get("selected_connections"):
            selected = [e for e in r["definition"]["connections"] if e["id"] in token["selected_connections"]]
        else:
            selected = await self._decision(node, result or token.get("context", {}), token)
        if selected is None:
            return
        current = self.run()
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
                rr["joins"][gid] = {"join_id": join_id, "split_id": node["id"], "expected": len(selected), "arrived": [], "stack": stack}
                stack = stack + [gid]
            for edge in selected:
                rr["pending"].append({"id": uuid.uuid4().hex, "node_id": edge["target"], "stack": stack, "context": _bounded(result or token.get("context", {})), "via": edge["id"]})
        self.update(advance, "transition", [e["id"] for e in selected])

    async def _execute_node(self, node: dict[str, Any], token: dict[str, Any]) -> None:
        r = self.run()
        count = sum(a["role"] == "node" and a["node_id"] == node["id"] and any(t["status"] != "not_started" for t in a["tasks"]) for a in r["activations"])
        if not token.get("recovered_result") and count >= node["max_attempts"] + r["attempt_grants"].get(node["id"], 0):
            self.attention(f"Attempt limit reached for {node['id']}")
            return
        a = next(a for a in r["activations"] if a["id"] == token["execution_activation_id"]) if token.get("recovered_result") else self._activation(node["id"], "node", token)
        prompt = f"Workflow: {r['name']}\nTask: {r['prompt']}\nStep instructions: {node['instructions']}\nRole: {node['role']}\nChecklist: {json.dumps(r.get('tasks', []))}\nHuman recovery instructions: {r['instructions']}\nPrior context: {json.dumps(_bounded(token.get('context', {})))}\nRole defaults: {role_prompt(node['role'])}"
        result = token.get("recovered_result") or await self._dispatch(node, prompt, "node", a) or {}
        if not result:
            self.update(lambda rr: next(x for x in rr["activations"] if x["id"] == a["id"]).update(status="failed"), "activation_finished", a["id"])
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
                self.update(invalid, "invalid_node_result", str(exc))
                self.attention(f"Invalid {node['role']} result: {exc}")
                return
        def finish(rr: dict[str, Any]) -> None:
            next(x for x in rr["activations"] if x["id"] == a["id"]).update(status="completed", result=_bounded(result))
            next(t for t in rr["pending"] if t["id"] == token["id"]).update(execution_complete=True, execution_activation_id=a["id"], result=_bounded(result), completed_task_ids=completed_ids)
        self.update(finish, "node_result_ready", a["id"])

    def _arrive_join(self, token: dict[str, Any]) -> bool:
        r = self.run()
        node = next(n for n in r["definition"]["nodes"] if n["id"] == token["node_id"])
        stack = token.get("stack", [])
        if not stack or r["joins"].get(stack[-1], {}).get("join_id") != node["id"]:
            return False
        def arrive(rr: dict[str, Any]) -> None:
            stack = token.get("stack", [])
            if not stack:
                raise WorkflowError("Join lacks an activation generation")
            group = rr["joins"][stack[-1]]
            if group["join_id"] != node["id"]:
                raise WorkflowError("Join generation mismatch")
            group["arrived"].append(token)
            rr["pending"].remove(token)
            if len(group["arrived"]) == group["expected"]:
                rr["pending"].append({"id": uuid.uuid4().hex, "node_id": node["id"], "stack": group["stack"], "joined": True, "context": {"branches": [t["context"] for t in group["arrived"]]}})
                del rr["joins"][stack[-1]]
        self.update(arrive, "join_arrival", token["id"])
        return True

    async def reconcile(self) -> bool:
        """A new supervisor may inspect records but cannot adopt missing pipe owners."""
        self.store.reconcile_run(self.run_id)
        if any(t["status"] == "uncertain" for a in self.run()["activations"] for t in a["tasks"]):
            self.attention("Supervisor interrupted: reconcile observed task results before continuing; dispatches were not replayed")
            return False
        return True

    async def execute(self, run_id: str) -> None:
        self.run_id = run_id
        if not await self.reconcile() and self.run()["status"] != "cancelling":
            return
        run = self.run()
        if run["status"] == "starting":
            start = next(n["id"] for n in run["definition"]["nodes"] if n["type"] == "start")
            from . import identity
            self.update(lambda r: r.update(status="running", supervisor_pid=os.getpid(), supervisor_identity=identity.own_identity(), pending=[{"id": uuid.uuid4().hex, "node_id": start, "stack": [], "context": {}}]), "supervisor_started")
        else:
            from . import identity
            self.update(lambda r: r.update(supervisor_pid=os.getpid(), supervisor_identity=identity.own_identity()), "supervisor_resumed")
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
                            self.update(lambda r: r.update(status="cancelled"), "cancelled")
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
                if run["status"] in TERMINAL:
                    break
                if not active and run["status"] in {"paused", "needs_attention"}:
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
        prompt = "Create a Polybridge workflow definition. Do not write files or dispatch agents. Return ONLY a JSON object. Schema: " + json.dumps({"name": self.run()["name"], "orchestrator": {"backend": "codex", "fallbacks": []}, "nodes": [{"id": "start", "type": "start", "branch_mode": "auto", "prompt": "Optional workflow purpose for the orchestrator"}, {"id": "work", "type": "agent", "instructions": "...", "agent": {"backend": "codex"}, "session_mode": "resume", "branch_mode": "auto", "max_attempts": 3}, {"id": "end", "type": "end", "branch_mode": "auto"}], "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}], "max_parallel": 4, "max_transitions": 100}) + "\nStart may contain an optional prompt string describing the workflow purpose; Polybridge supplies it to orchestrator decisions alongside the runtime user request. The orchestrator chooses one or multiple outgoing paths from their condition prompts and the evidence. Parallel paths connect to a shared agent or End node; Polybridge infers convergence automatically. Do not create Join nodes or configurable branching modes. Retry arrows return to an earlier ancestor step; Polybridge infers loops from topology, so do not set a backward flag. Put explicit failure/retry and success/continue conditions on arrows. A retry connection may set max_retries to a nonnegative integer: this caps actual traversals of that arrow across the entire run; 0 disables retry, and an absent value adds no edge cap. Node max_attempts and max_transitions still apply and may stop earlier. A retry is selected exclusively and must stay inside its parallel region. Agent roles are planning, implementation, review and task; supply custom step instructions, while Polybridge adds the role guidance and result protocol. Request: " + self.run().get("builder_turn_prompt", self.run()["prompt"])
        if "editing_definition" in self.run():
            prompt += "\nRefine the current unsaved canvas below according to the request; it may be incomplete. Return the complete corrected workflow. Preserve existing node and connection IDs, agent settings, permissions, instructions and positions unless the requested edit requires changing them. Do not assign a revision or overwrite any saved definition. Read applicable repository AGENTS.md and skill files, especially skills specified in the request, and use their relevant guidance when refining the graph. You may read repository guidance and skills for context; only inspect files, do not implement the task or run the workflow. Current canvas:\n" + json.dumps(self.run()["editing_definition"])
        prompt += "\nPublish canvas progress after each logical edit using polybridge.apply_workflow_draft (or polybridge-ctl workflow-builder-apply), with definition and expected_draft_revision. This updates only your builder preview, never saved workflows. Read get_workflow_status(workflow_run_id=" + self.run()["workflow_run_id"] + ") to resolve a revision conflict. Incomplete but render-safe graphs are allowed while building. If you applied any preview, the latest applied preview is authoritative and you may return a final summary; otherwise return the complete JSON definition. Current draft revision: " + str(self.run().get("draft_revision", 0)) + "\nCurrent draft:\n" + json.dumps(self.run().get("builder_draft", {}))
        if self.run().get("builder_has_repo_context") is False:
            prompt += "\nNo user repository was supplied. Your working directory is an isolated Polybridge builder workspace; do not infer a project or search elsewhere for repository context. Refine the supplied canvas and request without repository-specific skills or files."
        result = await self._dispatch({"id": "builder", "title": "Workflow builder"}, prompt, "builder", a)
        if result and self.run()["status"] == "running":
            try:
                current = self.run()
                applied = next(x for x in current["activations"] if x["id"] == a["id"]).get("draft_applied", False)
                d = validate_definition({**(current["builder_draft"] if applied else parse_json(result.get("summary") or "")), "name": current["name"]})
                d["draft"] = True
                if "editing_definition" in self.run() or self.run().get("builder_followup"):
                    metadata = self.run().get("editing_source", {"name": self.run()["name"], "revision": self.run().get("generated_definition", {}).get("revision", 0)})
                    d["revision"] = metadata["revision"] if metadata["name"] == d["name"] else 0
                    d.pop("updated_at", None)
                    self.update(lambda r: (r.update(status="completed", generated_definition=d, builder_draft=d, draft_revision=r.get("draft_revision", 0) + 1), next(x for x in r["activations"] if x["id"] == a["id"]).update(status="completed")), "builder_edit_proposed")
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
            supervisor.attention(f"Supervisor failure: {exc}")
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
            final = storage.get_run(args.run_id)
            if final["kind"] == "builder" and final["status"] == "starting" and any(m["status"] == "pending" for m in final.get("builder_messages", [])):
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
