"""Run workflow node invocation: one supervisor per tree, linked child runs.

A Run workflow node delegates a focused assignment to another saved workflow.
Children are separate linked run records built only from the pinned dependency
tree; the root supervisor awaits the child supervisor over a shared
``WorkflowTree`` (harness slots, pooled leases, session locks).
"""
from __future__ import annotations

import asyncio
import copy
import json
import os
import time
import uuid
from collections import deque
from typing import Any

INVOCATION_PREVIEW_BUDGET = 32000
INVOCATION_PREVIEW_REF_LIMIT = 16000
CHILD_SUMMARY_LIMIT = 8000
INVOCATION_WATCH_INTERVAL = 0.1

SUSPENDED = {"paused", "needs_attention", "needs_input"}


def _w():
    from . import workflows
    return workflows


def _d():
    from . import workflow_delegation
    return workflow_delegation


class TreeSlots:
    """A polling semaphore: waiting observes tree liveness, not just permits."""

    def __init__(self, permits: int):
        self.free = max(1, int(permits))
        self.waiters: deque[asyncio.Future] = deque()

    def release(self) -> None:
        while self.waiters:
            fut = self.waiters.popleft()
            if not fut.done():
                fut.set_result(True)  # Hand the permit off directly.
                return
        self.free += 1

    async def acquire(self, should_continue: Any) -> None:
        from .workflows import DispatchNotStarted
        if not should_continue():
            raise DispatchNotStarted("Scheduling stopped before dispatch")
        if not self.waiters and self.free > 0:
            self.free -= 1
            return
        fut = asyncio.get_running_loop().create_future()
        self.waiters.append(fut)
        acquired = False
        try:
            while True:
                if not should_continue():
                    raise DispatchNotStarted("Scheduling stopped before dispatch")
                try:
                    await asyncio.wait_for(asyncio.shield(fut), 0.05)
                except TimeoutError:
                    continue
                if not should_continue():
                    raise DispatchNotStarted("Scheduling stopped before dispatch")
                acquired = True
                return
        finally:
            try:
                self.waiters.remove(fut)
            except ValueError:
                if not acquired and fut.done() and not fut.cancelled():
                    # Cancellation or suspension can race a permit handoff.
                    self.release()


class WorkflowTree:
    """State shared by every supervisor in one workflow run tree."""

    def __init__(self, store: Any, root_run_id: str | None = None, permits: int | None = None):
        self.store = store
        self.root_run_id = root_run_id or ""
        self.slots = TreeSlots(permits) if permits is not None else None
        self.leases: dict[str, Any] = {}
        self.session_locks: dict[str, Any] = {}
        self.supervisors: dict[str, Any] = {}
        # Parent tokens currently awaiting a child never occupy a scheduling slot.
        self.executing_children: set[str] = set()
        self.held: dict[tuple[str, str], bool] = {}

    def tree_running(self, run: dict[str, Any]) -> bool:
        """The run is running AND its root is running."""
        if run.get("status") != "running":
            return False
        link = run.get("parent_link") or {}
        root_id = link.get("root_workflow_run_id") or run.get("workflow_run_id")
        if not root_id or root_id == run.get("workflow_run_id"):
            return True
        try:
            return self.store.get_run(root_id).get("status") == "running"
        except (OSError, ValueError, KeyError):
            return False

    def root_suspended(self, run: dict[str, Any]) -> bool:
        """True when an ancestor root holds the tree in a human-gated state.

        A child loop exits on this signal; re-entry resumes the same child.
        """
        link = run.get("parent_link") or {}
        root_id = link.get("root_workflow_run_id")
        if not root_id or root_id == run.get("workflow_run_id"):
            return False
        try:
            return self.store.get_run(root_id).get("status") in SUSPENDED
        except (OSError, ValueError, KeyError):
            return True

    async def acquire_slot(self, should_continue: Any) -> None:
        if self.slots is None:
            # Builder runs have no tree budget; liveness still gates dispatch.
            if not should_continue():
                from .workflows import DispatchNotStarted
                raise DispatchNotStarted("Scheduling stopped before dispatch")
            return
        await self.slots.acquire(should_continue)

    def release_slot(self) -> None:
        if self.slots is not None:
            self.slots.release()

    def hold_slot(self, run_id: str, task_id: str) -> None:
        """An uncertain attempt keeps its slot until reconciliation frees it."""
        if self.slots is not None:
            self.held[(run_id, task_id)] = True

    def release_held(self, run_id: str) -> None:
        if self.slots is None:
            return
        for key in [key for key in self.held if key[0] == run_id]:
            self.held.pop(key, None)
            self.slots.release()

    async def cancel_descendants(self, run_id: str, registry: Any, *, include_self: bool = False) -> None:
        """Cancel live descendants; a child without live work settles cancelled."""
        # Only persisted invocation edges establish descendants. Retained history
        # can be arbitrarily large and contains unrelated trees.
        root = self.store.get_run(run_id)
        records, children, unresolved = {run_id: root}, {}, set()
        ordered, visited, visiting = [], set(), set()
        stack = [(run_id, False)]
        while stack:
            current_id, exiting = stack.pop()
            if exiting:
                visiting.discard(current_id)
                visited.add(current_id)
                ordered.append(current_id)
                continue
            if current_id in visited:
                continue
            if current_id in visiting:
                unresolved.add(current_id)
                continue
            visiting.add(current_id)
            stack.append((current_id, True))
            observed = records[current_id]
            linked = []
            for activation in observed.get('activations', []):
                invocation = activation.get('invocation')
                if not invocation:
                    continue
                try:
                    child_id = invocation['child_workflow_run_id']
                    child = records.get(child_id) or self.store.get_run(child_id)
                    link = child.get('parent_link') or {}
                    if child.get('workflow_run_id') != child_id or link.get('workflow_run_id') != current_id or link.get('execution_id') != activation.get('id'):
                        raise ValueError('Child invocation link does not match its parent')
                    if child_id in visiting:
                        raise ValueError('Cyclic child invocation link')
                except (OSError, ValueError, KeyError, TypeError, AttributeError):
                    unresolved.add(current_id)
                    continue
                records[child_id] = child
                linked.append(child_id)
            children[current_id] = set(linked)
            for child_id in reversed(linked):
                if child_id not in visited:
                    stack.append((child_id, False))
        settled = {}
        for current_id in ordered:
            observed = records[current_id]
            live = [t['task_id'] for a in observed.get('activations', []) for t in a.get('tasks', []) if t.get('status') in {'running', 'reserved', 'uncertain'}]
            descendants_settled = current_id not in unresolved and all(settled.get(child_id, False) for child_id in children.get(current_id, ()))
            selected = include_self or current_id != run_id
            if selected:
                # A failed parallel branch can mark its run terminal while a
                # sibling task is still live. Signal the tasks, preserve outcome.
                for task_id in live:
                    try:
                        await registry.cancel_cascade(task_id, workflow_control=True)
                    except Exception:
                        pass
            if selected and observed.get('status') not in _w().TERMINAL:
                status = 'cancelling' if live or not descendants_settled else 'cancelled'
                self.store.update_run(current_id, lambda r, status=status: r.update(status=status) if r['status'] not in _w().TERMINAL else None, 'tree_cancel_propagated' if status == 'cancelling' else 'tree_cancelled', {'root': run_id})
                records[current_id] = self.store.get_run(current_id)
            settled[current_id] = not live and descendants_settled and records[current_id].get('status') in _w().TERMINAL


def tree_write_strength(run: dict[str, Any], dispatch_freedom: str) -> bool:
    """Exclusive when any writer exists anywhere in the pinned tree."""
    if dispatch_freedom != "read_only":
        return True
    tree = run.get("dependency_tree")
    definitions = [entry.get("definition", {}) for entry in tree.get("workflows", {}).values()] if tree else [run.get("definition", {})]
    for definition in definitions:
        for node in definition.get("nodes", []):
            if node.get("type") != "agent":
                continue
            if node.get("freedom", _w().ROLE_FREEDOM_DEFAULTS.get(node.get("role"), "read_only")) != "read_only":
                return True
    return False


def tree_state(store: Any, run: dict[str, Any]) -> dict[str, Any]:
    """The root status of a run's tree, and whether anything is still settling.

    Never scans workflow history: the root record answers the gate, because a
    root settles only after every descendant has settled. Only the source run
    and the root carry suspension status; intermediate runs stay running with a
    derived suspended_via_root projection.
    """
    link = run.get("parent_link") or {}
    root_id = link.get("root_workflow_run_id") or run.get("workflow_run_id")
    try:
        root = run if root_id == run.get("workflow_run_id") else store.get_run(root_id)
    except (OSError, ValueError, KeyError):
        return {"root_workflow_run_id": root_id, "root_status": "unknown", "settling": True, "suspended_via_root": bool(run.get("suspended_via_root"))}
    settling = bool(root.get("settling")) or root.get("status") not in _w().TERMINAL or bool(run.get("settling"))
    suspended_via_root = root_id != run.get("workflow_run_id") and run.get("status") == "running" and root.get("status") in SUSPENDED
    return {"root_workflow_run_id": root_id, "root_status": root.get("status"), "settling": settling, "suspended_via_root": suspended_via_root or bool(run.get("suspended_via_root"))}


def invocation_depth(run: dict[str, Any]) -> int:
    link = run.get("parent_link") or {}
    return int(link.get("depth") or 1)


def session_owner_id(parent_run: dict[str, Any]) -> str:
    """The nearest ancestor that runs its own orchestrator."""
    return parent_run.get("orchestrator_session_owner_run_id") or parent_run["workflow_run_id"]


def write_child_run(store: Any, run: dict[str, Any]) -> None:
    """Create the child record exclusively; a second writer must fail loudly."""
    path = store.runs / f"{run['workflow_run_id']}.json"
    with path.open("x", encoding="utf-8") as handle:
        os.chmod(path, 0o600)
        json.dump(run, handle, ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())


def child_run_record(store: Any, parent_run: dict[str, Any], activation: dict[str, Any], node: dict[str, Any], invocation: dict[str, Any]) -> dict[str, Any]:
    """Build the linked child run from the pinned subtree; never from live saves."""
    from .workflow_references import definition_sha256, subtree, workflow_identity
    tree = parent_run.get("dependency_tree") or {}
    workflow_id = node["workflow_ref"]["workflow_id"]
    pinned = (tree.get("workflows") or {}).get(workflow_id)
    if pinned is None:
        raise _w().WorkflowError(f"Workflow {workflow_id} is not part of this run's pinned dependency tree")
    definition = copy.deepcopy(pinned["definition"])
    child_id = invocation["child_workflow_run_id"]
    mode = node.get("orchestrator_mode", "child")
    owner_id = session_owner_id(parent_run) if mode == "current" else None
    child: dict[str, Any] = {
        "workflow_run_id": child_id,
        "kind": "workflow",
        "name": definition["name"],
        "definition": definition,
        "revision": pinned.get("revision", definition.get("revision", 0)),
        "definition_hash": definition_sha256(definition),
        "workflow_id": workflow_identity(definition),
        "orchestrator_mode": mode,
        "prompt": invocation.get("assignment", ""),
        "repo_path": parent_run["repo_path"],
        "freedom": "unrestricted",
        "network": parent_run.get("network"),
        "status": "starting",
        "created_at": time.time(),
        "updated_at": time.time(),
        "sequence": 0,
        "transitions": 0,
        "activations": [],
        "decisions": [],
        "sessions": {},
        "suppressed_candidates": [],
        "pending": [],
        "joins": {},
        "instructions": "",
        "attempt_grants": {},
        "supervisor_pid": None,
        "permission_policy": "saved_node",
        "execution_contract": "delegation",
        "runner_policy": "guided",
        "execution_policy": "visit",
        "retry_counts": {},
        "retry_grants": {},
        "tasks": [],
        "dependency_tree": subtree(tree, workflow_id),
        "parent_link": {
            "workflow_run_id": parent_run["workflow_run_id"],
            "execution_id": activation["id"],
            "node_id": node["id"],
            "root_workflow_run_id": (parent_run.get("parent_link") or {}).get("root_workflow_run_id") or parent_run["workflow_run_id"],
            "depth": invocation_depth(parent_run) + 1,
        },
        "interaction_owner": parent_run.get("interaction_owner", "caller"),
        "invocation_inputs": invocation.get("inputs", []),
    }
    if owner_id is not None:
        child["orchestrator_session_owner_run_id"] = owner_id
        try:
            owner = store.get_run(owner_id)
            child["orchestrator_config"] = copy.deepcopy(owner.get("definition", {}).get("orchestrator", {}))
        except (OSError, ValueError, KeyError):
            child["orchestrator_config"] = copy.deepcopy(parent_run.get("definition", {}).get("orchestrator", {}))
    return child


def invocation_inputs(parent_run: dict[str, Any], token: dict[str, Any], *, root: Any = None) -> list[dict[str, Any]]:
    """Complete inputs for the child: predecessor results and assigned descriptors."""
    refs = list(dict.fromkeys(token.get("input_result_refs", []) + token.get("additional_result_refs", [])))
    inputs = _d().result_inputs(parent_run, refs, preview=False, root=root)
    descriptors = [{k: task[k] for k in ("id", "title", "description") if k in task} for task in parent_run.get("tasks", []) if task["id"] in token.get("assigned_task_ids", [])]
    return [
        {"index": 1, "kind": "input_results", "results": inputs},
        {"index": 2, "kind": "assigned_task_descriptors", "descriptors": descriptors},
    ]


def invocation_input(run: dict[str, Any], ref: str) -> dict[str, Any]:
    """Resolve an invocation:<n> reference from the child's pinned inputs."""
    try:
        index = int(ref.removeprefix("invocation:"))
    except ValueError as exc:
        raise _w().WorkflowError(f"Invalid invocation reference {ref}") from exc
    for entry in run.get("invocation_inputs", []):
        if entry.get("index") == index:
            return copy.deepcopy(entry)
    raise _w().WorkflowError(f"Invocation reference {ref} is not part of this run")


def invocation_outcome(kind: str, child: dict[str, Any] | None, invocation: dict[str, Any], node: dict[str, Any], *, failure_reason: str = "") -> dict[str, Any]:
    """The parent activation's node_result for one child invocation."""
    child_outcome: dict[str, Any] = {
        "kind": kind,
        "child_status": (child or {}).get("status"),
        "child_workflow_run_id": invocation.get("child_workflow_run_id"),
        "workflow_id": invocation.get("workflow_id"),
        "workflow_name": invocation.get("workflow_name") or node.get("workflow_name", ""),
        "revision": invocation.get("revision"),
        "definition_sha256": invocation.get("definition_sha256"),
        "orchestrator_mode": node.get("orchestrator_mode", "child"),
    }
    summary = (child or {}).get("summary") or ""
    child_outcome["summary"] = summary[:CHILD_SUMMARY_LIMIT]
    child_outcome["summary_truncated"] = len(summary) > CHILD_SUMMARY_LIMIT
    child_outcome["failure_reason"] = str(failure_reason or (child or {}).get("failure_reason") or "")[:2000]
    child_outcome["final_result_refs"] = final_result_refs(child)
    child_outcome["checklist_summary"] = [
        {k: task.get(k) for k in ("id", "title", "status")} for task in (child or {}).get("tasks", [])[:200]
    ]
    child_outcome["permission_evidence"] = collect_permission_evidence(child) if child else []
    failure_kind = None
    status = "succeeded" if kind == "completed" else "failed" if kind in {"timeout", "runtime", "child_failed"} else "blocked"
    if kind != "completed":
        failure_kind = kind
    return {"status": status, "result": {"child_outcome": child_outcome, **({"failure_kind": failure_kind} if failure_kind else {})}, "evidence": [{"child_workflow_run_id": invocation.get("child_workflow_run_id"), "kind": kind}]}


def final_result_refs(child: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Leaf refs into the child's End-feeding settled executions, with a via path."""
    if not child:
        return []
    refs: list[dict[str, Any]] = []
    end_ids = {n["id"] for n in child.get("definition", {}).get("nodes", []) if n.get("type") == "end"}
    completed = next((decision for decision in reversed(child.get("decisions", [])) if decision.get("action") == "complete" and decision.get("node_id") in end_ids), None)
    checkpoint = next((a.get("token", {}) for a in child.get("activations", []) if completed and a.get("id") == completed.get("activation_id")), {})
    selected = list(dict.fromkeys(checkpoint.get("input_result_refs", [])))
    if checkpoint.get("execution_activation_id"):
        if checkpoint["execution_activation_id"] not in selected:
            selected.append(checkpoint["execution_activation_id"])
    executions = child.get("activations", [])
    if completed:
        indexed = {a["id"]: a for a in executions}
        executions = [indexed[ref] for ref in selected if ref in indexed]
    for activation in executions:
        if activation.get("role") != "node" or not _d().settled(activation) or not activation.get("node_result"):
            continue
        nested = (activation.get("node_result", {}).get("result") or {}).get("child_outcome") or {}
        if activation.get("invocation"):
            for ref in nested.get("final_result_refs", []):
                refs.append({**ref, "via": [child["workflow_run_id"]] + ref.get("via", [])})
        else:
            refs.append({"workflow_run_id": child["workflow_run_id"], "execution_id": activation["id"], "node_id": activation.get("node_id"), "via": [child["workflow_run_id"]]})
    return refs


def collect_permission_evidence(run: dict[str, Any] | None) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    if not run:
        return evidence
    for activation in run.get("activations", []):
        nested = (activation.get("node_result", {}).get("result") or {}).get("child_outcome") or {}
        evidence.extend(nested.get("permission_evidence", []))
        for task in activation.get("tasks", []):
            for denial in (task.get("result", {}) or {}).get("permission_denials", []) or []:
                evidence.append(denial)
    return evidence


def execution_permission(execution: dict[str, Any]) -> str | None:
    """Permission or authority end, recursive through nested failure kinds."""
    value = execution.get("node_result") or {}
    result = value.get("result") or {}
    for key in ("failure_kind", "blocker_category"):
        if result.get(key) in {"permission", "authority"}:
            return result[key]
    nested = result.get("child_outcome") or {}
    if isinstance(nested, dict) and nested.get("kind") in {"permission", "authority"}:
        return nested["kind"]
    tasks = execution.get("tasks", [])
    terminal = tasks[-1] if tasks else None
    if terminal is not None and terminal.get("status") != "cancelled":
        snapshot = terminal.get("result") or {}
        if snapshot.get("status") != "completed" and snapshot.get("permission_denials"):
            return "permission"
    return None


def permission_outcome(store: Any, run: dict[str, Any]) -> str | None:
    """Permission applies only to unresolved, unsuperseded, failed or blocked ends."""
    executions = run.get("activations", [])
    superseded = {other.get("retry_of_execution_id") for other in executions}
    for execution in executions:
        if execution.get("role") != "node":
            continue
        value = execution.get("node_result") or {}
        if not _d().settled(execution) or value.get("status") not in {"failed", "blocked"}:
            continue
        if execution.get("resolved_by_execution_id") or execution.get("optional_failure"):
            continue
        if execution["id"] in superseded:
            continue
        found = execution_permission(execution)
        if found:
            return found
    return None


def outcome_for_child(store: Any, parent_run: dict[str, Any], activation: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    """Classify a settled child; precedence is first-match."""
    invocation = activation.get("invocation") or {}
    child_id = invocation.get("child_workflow_run_id")
    try:
        child = store.get_run(child_id) if child_id else None
    except (OSError, ValueError, KeyError):
        child = None
    if child is None:
        return invocation_outcome("uncertain", None, invocation, node, failure_reason="Child run record is missing")
    link = child.get("parent_link") or {}
    if link.get("execution_id") != activation["id"] or link.get("workflow_run_id") != parent_run["workflow_run_id"]:
        return invocation_outcome("uncertain", child, invocation, node, failure_reason="Child run does not match this invocation")
    status = child.get("status")
    if status in _w().TERMINAL and not child_settled(child, store=store):
        return invocation_outcome("uncertain", child, invocation, node, failure_reason="Child descendants or dispatches have not confirmed settlement")
    if invocation.get("timeout_expired") and invocation.get("timeout_confirmed") and status in _w().TERMINAL:
        return invocation_outcome("timeout", child, invocation, node, failure_reason=invocation.get("timeout_reason", "Invocation timeout"))
    if status == "completed":
        return invocation_outcome("completed", child, invocation, node)
    if status == "cancelled":
        return invocation_outcome("cancelled", child, invocation, node)
    permission = permission_outcome(store, child)
    if status == "failed":
        if permission:
            return invocation_outcome("permission", child, invocation, node, failure_reason=permission)
        if child.get("failed_decision_id"):
            return invocation_outcome("child_failed", child, invocation, node, failure_reason=child.get("failure_reason", ""))
        return invocation_outcome("runtime", child, invocation, node, failure_reason=child.get("failure_reason", ""))
    if permission:
        return invocation_outcome("permission", child, invocation, node, failure_reason=permission)
    if status == "cancelling":
        return invocation_outcome("uncertain", child, invocation, node, failure_reason="Child cancellation did not settle")
    return invocation_outcome("uncertain", child, invocation, node, failure_reason=f"Child ended {status}")


def invocation_children_settled(store: Any, run: dict[str, Any], seen: set[str] | None = None) -> bool:
    seen = set(seen or ())
    run_id = run.get("workflow_run_id")
    if run_id in seen:
        return False
    seen.add(run_id)
    for activation in run.get("activations", []):
        invocation = activation.get("invocation")
        if not invocation:
            continue
        try:
            child = store.get_run(invocation["child_workflow_run_id"])
        except (OSError, ValueError, KeyError):
            return False
        link = child.get("parent_link") or {}
        if link.get("workflow_run_id") != run_id or link.get("execution_id") != activation.get("id") or not child_settled(child, store=store, seen=seen):
            return False
    return True


def child_settled(child: dict[str, Any], *, store: Any = None, seen: set[str] | None = None) -> bool:
    if child.get("status") not in _w().TERMINAL or any(t.get("status") in {"reserved", "running", "uncertain"} for a in child.get("activations", []) for t in a.get("tasks", [])):
        return False
    if store is not None:
        return invocation_children_settled(store, child, seen)
    return not any(a.get("invocation") and a["invocation"].get("stage") != "settled" for a in child.get("activations", []))


def publish_input(store: Any, source: dict[str, Any]) -> None:
    """Forward a descendant's input question to the root, preserving identity."""
    link = source.get("parent_link") or {}
    root_id = link.get("root_workflow_run_id")
    if not root_id or root_id == source["workflow_run_id"]:
        return
    # Only persisted invocation edges establish this tree. Retained run history
    # can be arbitrarily large and must never be decoded to choose a question.
    from .bounded_io import ReadLimit
    from .catalog import Catalog
    from .workflow_references import MAX_WORKFLOW_NESTING_DEPTH
    budget = Catalog(store.runs, '.json')
    exhausted = False
    def read(identifier: str) -> dict[str, Any]:
        return store.get_run(identifier, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=budget)
    def unavailable() -> None:
        def suspend(r: dict[str, Any]) -> None:
            if r.get('status') not in _w().TERMINAL and r.get('status') != 'needs_input':
                r.update(status='needs_attention', attention_reason='Workflow input tree could not be completely verified within bounded metadata arbitration; inspect its known runs directly before retrying')
        store.update_run(root_id, suspend, 'child_input_arbitration_unavailable')
    try:
        root = read(root_id)
    except ReadLimit:
        raise _w().WorkflowError('Workflow input ancestry exceeds bounded metadata arbitration') from None
    except (OSError, ValueError, KeyError):
        return
    if root.get("status") in _w().TERMINAL or root.get("status") == "needs_input" and root.get("input_decision_id"):
        return
    records = {root_id: root}
    # Establish the publishing source's exact ownership before any root mutation.
    child_id, ancestry = source['workflow_run_id'], set()
    try:
        while child_id != root_id:
            if child_id in ancestry or len(ancestry) >= MAX_WORKFLOW_NESTING_DEPTH - 1:
                return
            ancestry.add(child_id)
            child = records.get(child_id) or read(child_id)
            records[child_id] = child
            child_link = child.get('parent_link') or {}
            if child.get('workflow_run_id') != child_id or child_link.get('root_workflow_run_id') != root_id:
                return
            parent_id = child_link.get('workflow_run_id')
            parent = records.get(parent_id) or read(parent_id)
            records[parent_id] = parent
            activation = next((a for a in parent.get('activations', []) if a.get('id') == child_link.get('execution_id') and (a.get('invocation') or {}).get('child_workflow_run_id') == child_id), None)
            if activation is None:
                return
            outcome = (activation.get('node_result', {}).get('result') or {}).get('child_outcome') or {}
            if activation.get('status') == 'completed' and activation['invocation'].get('stage') == 'settled' and outcome.get('kind') == 'completed':
                return
            child_id = parent_id
    except ReadLimit:
        raise _w().WorkflowError('Workflow input ancestry exceeds bounded metadata arbitration') from None
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return
    paths = {root_id: [root_id]}
    stack, waiting = [root_id], []
    while stack:
        parent_id = stack.pop()
        parent, path = records[parent_id], paths[parent_id]
        if len(path) >= MAX_WORKFLOW_NESTING_DEPTH:
            continue
        for activation in parent.get("activations", []):
            invocation = activation.get("invocation")
            if not invocation:
                continue
            outcome = (activation.get('node_result', {}).get('result') or {}).get('child_outcome') or {}
            if activation.get('status') == 'completed' and invocation.get('stage') == 'settled' and outcome.get('kind') == 'completed':
                continue
            try:
                child_id = invocation["child_workflow_run_id"]
                child = records.get(child_id) or read(child_id)
                child_link = child.get("parent_link") or {}
                if child.get("workflow_run_id") != child_id or child_link.get("workflow_run_id") != parent_id or child_link.get("execution_id") != activation.get("id") or child_link.get("root_workflow_run_id") != root_id:
                    exhausted = True
                    break
                records[child_id] = child
                if child_id in paths:
                    continue
                paths[child_id] = [*path, child_id]
                stack.append(child_id)
                if child.get("status") == "needs_input" and child.get("input_decision_id") and child.get("input_question"):
                    waiting.append(child)
            except ReadLimit:
                exhausted = True
                break
            except (OSError, ValueError, KeyError, TypeError, AttributeError):
                exhausted = True
                break
        if exhausted:
            unavailable()
            return
    if source['workflow_run_id'] not in paths or not waiting:
        return
    source = min(waiting, key=lambda r: (r.get("created_at", 0), r["workflow_run_id"]))
    question = source.get("input_question")
    decision_id = source.get("input_decision_id")
    if not question or not decision_id:
        return
    path = paths[source["workflow_run_id"]]
    def forward(r: dict[str, Any]) -> None:
        if r["status"] in _w().TERMINAL:
            return
        # A displayed question is immutable until its exact answer is accepted.
        # Other descendants remain suspended and will publish on re-entry.
        if r.get("status") == "needs_input" and r.get("input_decision_id"):
            return
        r.update(status="needs_input", input_question=question, input_decision_id=decision_id, attention_reason="A workflow node needs caller input", input_source={"workflow_run_id": source["workflow_run_id"], "workflow_name": source.get("name", ""), "path": path})
    store.update_run(root_id, forward, "child_input_forwarded", {"source": source["workflow_run_id"], "input_decision_id": decision_id})


def publish_attention(store: Any, source: dict[str, Any]) -> None:
    """Keep suspension authority and attempt grants on the originating child."""
    root_id = (source.get("parent_link") or {}).get("root_workflow_run_id")
    if not root_id:
        return
    attention_source = source.get("attention_source")
    if not attention_source:
        path = [source["workflow_run_id"]]
        walker = source
        while (walker.get("parent_link") or {}).get("workflow_run_id"):
            parent_id = walker["parent_link"]["workflow_run_id"]
            path.append(parent_id)
            if parent_id == root_id:
                break
            walker = store.get_run(parent_id)
        attention_source = {"workflow_run_id": source["workflow_run_id"], "workflow_name": source.get("name", ""), "path": list(reversed(path)), "attention_checkpoint": source.get("attention_checkpoint", source.get("sequence", 0))}
    def forward(r: dict[str, Any]) -> None:
        if r["status"] in _w().TERMINAL or r["status"] in {"cancelling", "needs_input"}:
            return
        r.update(status=source["status"], attention_reason=source.get("attention_reason") or "A child workflow needs attention", attention_source=copy.deepcopy(attention_source))
    store.update_run(root_id, forward, "child_attention_forwarded", {"source": attention_source["workflow_run_id"]})


def derive_invocation_activation(store: Any, run: dict[str, Any], activation: dict[str, Any], node: dict[str, Any]) -> None:
    """Reconcile one invocation activation from its stage plus its child."""
    invocation = activation.get("invocation") or {}
    child_id = invocation.get("child_workflow_run_id")
    try:
        child = store.get_run(child_id) if child_id else None
    except (OSError, ValueError, KeyError):
        child = None
    if child is None:
        if invocation.get("stage") == "preparing":
            activation["status"] = "not_started"  # No child was ever created.
        else:
            activation["status"] = "uncertain"
            activation["result_error"] = "Child run record is missing"
        return
    link = child.get("parent_link") or {}
    if link.get("execution_id") != activation["id"] or link.get("workflow_run_id") != run["workflow_run_id"]:
        activation["status"] = "uncertain"
        activation["result_error"] = "Child run does not match this invocation"
        return
    if child.get("status") in _w().TERMINAL and not child_settled(child, store=store):
        activation["status"] = "uncertain"
        activation["result_error"] = "Child descendants or dispatches have not confirmed settlement"
        if run.get("status") not in _w().TERMINAL and run.get("status") != "cancelling":
            run.update(status="needs_attention", attention_reason=activation["result_error"])
        return
    if child_settled(child, store=store):
        outcome = outcome_for_child(store, run, activation, node)
        invocation["stage"] = "settled"
        activation["invocation"] = invocation
        activation["status"] = "completed" if outcome["status"] == "succeeded" else "failed"
        activation["node_result"] = outcome
        activation["result"] = copy.deepcopy(outcome)
        activation["finished_at"] = time.time()
        token = next((t for t in run.get("pending", []) if t["id"] == (activation.get("token") or {}).get("id")), None)
        owns_token = token is not None and token.get("node_id") == activation.get("node_id") and token.get("execution_activation_id") in {None, activation["id"]} and token.get("retry_of_execution_id") != activation["id"] and not token.get("reopen_child_execution_id") and not activation.get("resolved_by_execution_id")
        if owns_token and not token.get("execution_complete"):
            token.update(execution_complete=True, execution_activation_id=activation["id"], result=copy.deepcopy(outcome), completed_task_ids=[])
            if outcome["status"] != "succeeded":
                token["failed_execution_refs"] = list(dict.fromkeys(token.get("failed_execution_refs", []) + [activation["id"]]))
        return
    if child.get("status") == "needs_input":
        activation["status"] = "waiting_for_child"
        invocation["stage"] = "waiting"
        activation["invocation"] = invocation
        return
    activation["status"] = "running"
    invocation["stage"] = "running" if child.get("status") in {"running", "starting"} else "created"
    activation["invocation"] = invocation


async def run_child(supervisor: Any, node: dict[str, Any], token: dict[str, Any]) -> None:
    """Execute one Run workflow node: create or re-enter the child, then settle."""
    w = _w()
    store = supervisor.store
    run = supervisor.run()
    activation = next((a for a in run["activations"] if a["id"] == token.get("execution_activation_id")), None)
    from .workflow_execution_policy import attempts_used
    if activation is None:
        count = attempts_used(run, node, token)
        if count >= node["max_attempts"] + run.get("attempt_grants", {}).get(node["id"], 0):
            supervisor.attention(f"Attempt limit reached for {node['id']}")
            return
    invocation = (activation or {}).get("invocation")
    reopen = token.get("reopen_child_execution_id")

    def reserve(r: dict[str, Any]) -> None:
        nonlocal invocation
        from .workflow_execution_policy import visit_id
        current = next(t for t in r["pending"] if t["id"] == token["id"])
        if activation is None:
            fresh = {"id": uuid.uuid4().hex, "node_id": node["id"], "role": "node", "status": "running", "tasks": [], "created_at": time.time(), "token": copy.deepcopy(token), "visit_id": visit_id(r, token), "invocation": {"child_workflow_run_id": uuid.uuid4().hex, "stage": "preparing", "workflow_id": node["workflow_ref"]["workflow_id"], "workflow_name": node.get("workflow_name", ""), "orchestrator_mode": node.get("orchestrator_mode", "child"), "timeout_seconds": node.get("timeout_seconds"), "timeout_elapsed_seconds": 0}}
            r["activations"].append(fresh)
            current["execution_activation_id"] = fresh["id"]
            invocation = fresh["invocation"]
        else:
            invocation = activation["invocation"]
            current["execution_activation_id"] = activation["id"]

    supervisor.update(reserve, "invocation_reserved")
    activation = next(a for a in supervisor.run()["activations"] if a["id"] == token.get("execution_activation_id") or (a.get("invocation") or {}).get("stage") == "preparing" and a["node_id"] == node["id"] and a.get("token", {}).get("id") == token["id"])
    invocation = dict(activation.get("invocation") or {})
    invocation.setdefault("workflow_id", node["workflow_ref"]["workflow_id"])
    invocation.setdefault("workflow_name", node.get("workflow_name", ""))
    invocation.setdefault("orchestrator_mode", node.get("orchestrator_mode", "child"))
    invocation.setdefault("timeout_seconds", node.get("timeout_seconds"))
    invocation.setdefault("timeout_elapsed_seconds", 0)
    from .workflow_references import definition_sha256
    pinned = ((run.get("dependency_tree") or {}).get("workflows") or {}).get(invocation["workflow_id"]) or {}
    invocation["revision"] = pinned.get("revision")
    invocation["definition_sha256"] = pinned.get("definition_sha256") or definition_sha256(pinned.get("definition", {}))

    child_id = invocation.get("child_workflow_run_id")
    existing = None
    try:
        existing = store.get_run(child_id) if child_id else None
    except (OSError, ValueError, KeyError):
        existing = None
    if existing is None and reopen:
        outcome = invocation_outcome("uncertain", None, invocation, node, failure_reason="Child run to recover is missing")
        _settle_invocation(supervisor, node, token, activation, outcome, invocation)
        return
    if existing is None:
        invocation["inputs"] = invocation_inputs(run, token, root=store.root)
        invocation["assignment"] = token.get("assignment_prompt", "")
        child = child_run_record(store, run, activation, node, invocation)
        try:
            write_child_run(store, child)
        except FileExistsError:
            existing = store.get_run(child_id)
        invocation["stage"] = "created"
        supervisor.update(lambda r: next(a for a in r["activations"] if a["id"] == activation["id"]).update(invocation=copy.deepcopy(invocation)), "child_created", {"child_workflow_run_id": child_id})
    else:
        link = existing.get("parent_link") or {}
        if link.get("execution_id") != activation["id"] or link.get("workflow_run_id") != run["workflow_run_id"]:
            outcome = invocation_outcome("uncertain", existing, invocation, node, failure_reason="Existing child run does not match this invocation")
            _settle_invocation(supervisor, node, token, activation, outcome, invocation)
            return
        supervisor.update(lambda r: next(a for a in r["activations"] if a["id"] == activation["id"]).update(invocation=copy.deepcopy(invocation)), "child_adopted", {"child_workflow_run_id": child_id})

    if reopen:
        del token["reopen_child_execution_id"]
        def reopen_child(r: dict[str, Any]) -> None:
            if r["status"] not in {"failed", "needs_attention", "needs_input", "paused"}:
                return
            r.update(status="running")
            r.pop("failure_reason", None)
            r.pop("failed_decision_id", None)
            for pending in r.get("pending", []):
                pending["decision_attempts"] = 0
                pending.pop("decision_error", None)
        store.update_run(child_id, reopen_child, "child_decision_reopened", {"parent_execution": activation["id"]})

    def stage_update(stage: str, event: str) -> None:
        invocation["stage"] = stage
        if stage == "running":
            invocation["active_since"] = time.time()
        else:
            invocation.pop("active_since", None)
        def apply(r: dict[str, Any]) -> None:
            next(a for a in r["activations"] if a["id"] == activation["id"]).update(invocation=copy.deepcopy(invocation))
            if stage == "running":
                r.pop("suspended_via_root", None)
        supervisor.update(apply, event, {"child_workflow_run_id": child_id, "stage": stage})

    mode = node.get("orchestrator_mode", "child")
    child_supervisor = w.WorkflowSupervisor(supervisor.registry, store, tree=supervisor.tree)
    if mode == "current":
        owner_id = run.get("orchestrator_session_owner_run_id") or run["workflow_run_id"]
        owner_supervisor = supervisor.tree.supervisors.get(owner_id)
        if owner_supervisor is not None:
            child_supervisor.decision_lock = owner_supervisor.decision_lock
        try:
            owner_run = store.get_run(owner_id)
            owner_config = copy.deepcopy((owner_run.get("definition") or {}).get("orchestrator", {}))
        except (OSError, ValueError, KeyError):
            owner_config = copy.deepcopy(run.get("definition", {}).get("orchestrator", {}))
        child_supervisor.orchestrator_override = (owner_id, owner_config)
    supervisor.tree.supervisors[child_id] = child_supervisor
    supervisor.tree.executing_children.add(token["id"])
    stage_update("running", "child_running")
    timed_out = False
    timeout_reason = ""
    try:
        child_task = asyncio.create_task(child_supervisor.execute(child_id))
        timeout_seconds = invocation.get("timeout_seconds")
        last_tick = time.monotonic()
        while not child_task.done():
            before_wait = store.get_run(child_id)
            charge_elapsed = supervisor.tree.tree_running(before_wait) or before_wait.get("settling", False)
            await asyncio.wait({child_task}, timeout=INVOCATION_WATCH_INTERVAL)
            now = time.monotonic()
            elapsed = now - last_tick
            last_tick = now
            if timeout_seconds:
                child_now = None
                try:
                    child_now = store.get_run(child_id)
                except (OSError, ValueError, KeyError):
                    pass
                if child_now is not None and charge_elapsed:
                    invocation["timeout_elapsed_seconds"] = round(invocation.get("timeout_elapsed_seconds", 0) + elapsed, 3)
                    def persist_elapsed(r: dict[str, Any], _value=invocation["timeout_elapsed_seconds"]) -> None:
                        current = next(a for a in r["activations"] if a["id"] == activation["id"])
                        current.setdefault("invocation", {})["timeout_elapsed_seconds"] = _value
                    supervisor.update(persist_elapsed, "child_timeout_accrued", {"child_workflow_run_id": child_id, "elapsed": invocation["timeout_elapsed_seconds"]})
                    if invocation["timeout_elapsed_seconds"] >= timeout_seconds:
                        timed_out = True
                        timeout_reason = f"Run workflow node exceeded its {timeout_seconds}s timeout"
                        try:
                            await asyncio.wait_for(supervisor.tree.cancel_descendants(child_id, supervisor.registry, include_self=True), timeout=30)
                        except Exception:
                            pass
                        if not child_task.done():
                            await asyncio.wait({child_task}, timeout=30)
                        break
        if timed_out:
            try:
                final = store.get_run(child_id)
            except (OSError, ValueError, KeyError):
                final = None
            confirmed = final is not None and child_task.done() and child_settled(final, store=store)
            invocation["timeout_expired"] = True
            invocation["timeout_confirmed"] = confirmed
            invocation["timeout_reason"] = timeout_reason
    finally:
        supervisor.tree.executing_children.discard(token["id"])
        supervisor.tree.supervisors.pop(child_id, None)

    child_final = None
    try:
        child_final = store.get_run(child_id)
    except (OSError, ValueError, KeyError):
        child_final = None
    if timed_out and not invocation.get("timeout_confirmed"):
        supervisor.attention("Invocation timeout could not be confirmed settled; reconciliation is required")
        stage_update("settled", "child_timeout_uncertain")
        return
    if child_final is not None and child_final.get("status") == "needs_input":
        try:
            await asyncio.to_thread(publish_input, store, child_final)
        except _w().WorkflowError as exc:
            supervisor.attention(str(exc))
            return
        invocation["stage"] = "waiting"
        supervisor.update(lambda r: (next(a for a in r["activations"] if a["id"] == activation["id"]).update(status="waiting_for_child", invocation=copy.deepcopy(invocation)), r.update(suspended_via_root=True)), "child_waiting_for_input", {"child_workflow_run_id": child_id})
        return
    if child_final is not None and child_final.get("status") in {"needs_attention", "paused"} and not timed_out:
        reason = child_final.get("attention_reason") or f"Child workflow needs attention: {child_final.get('status')}"
        invocation["stage"] = "waiting"
        supervisor.update(lambda r: (next(a for a in r["activations"] if a["id"] == activation["id"]).update(status="waiting_for_child", invocation=copy.deepcopy(invocation)), r.update(suspended_via_root=True)), "child_needs_attention", {"child_workflow_run_id": child_id, "reason": reason})
        publish_attention(store, child_final)
        return
    if child_final is not None and child_final.get("status") in {"running", "starting"} and not timed_out:
        # The child's loop parked on a suspended tree: re-entry resumes the same
        # child and consumes no attempt.
        invocation["stage"] = "waiting"
        supervisor.update(lambda r: (next(a for a in r["activations"] if a["id"] == activation["id"]).update(status="waiting_for_child", invocation=copy.deepcopy(invocation)), r.update(suspended_via_root=True)), "child_suspended_via_root", {"child_workflow_run_id": child_id})
        return
    outcome = outcome_for_child(store, run, {**activation, "invocation": invocation}, node)
    stage_update("settled", "child_settled")
    _settle_invocation(supervisor, node, token, activation, outcome, invocation)


def _settle_invocation(supervisor: Any, node: dict[str, Any], token: dict[str, Any], activation: dict[str, Any], outcome: dict[str, Any], invocation: dict[str, Any]) -> None:
    w = _w()
    d = _d()
    value = outcome
    def finish(r: dict[str, Any]) -> None:
        a = next(a for a in r["activations"] if a["id"] == activation["id"])
        a.update(status="completed" if value["status"] == "succeeded" else "failed", node_result=copy.deepcopy(value), result=copy.deepcopy(value), invocation=copy.deepcopy(invocation), finished_at=time.time())
        current = next((t for t in r["pending"] if t["id"] == token["id"]), None)
        if current is None:
            return
        prior_failures = list(current.get("failed_execution_refs", []))
        if current.get("retry_of_execution_id"):
            prior_failures.append(current["retry_of_execution_id"])
        if value["status"] == "succeeded":
            resolved = []
            outstanding = []
            for previous_id in dict.fromkeys(prior_failures):
                previous = next((x for x in r["activations"] if x["id"] == previous_id), None)
                if previous and previous.get("node_result", {}).get("status") in {"failed", "blocked"} and previous["node_id"] == node["id"]:
                    previous["resolved_by_execution_id"] = activation["id"]
                    resolved.append(previous_id)
                elif previous and not previous.get("resolved_by_execution_id") and not previous.get("optional_failure"):
                    outstanding.append(previous_id)
            a["resolved_execution_refs"] = resolved
            current["failed_execution_refs"] = outstanding
        else:
            current["failed_execution_refs"] = list(dict.fromkeys(prior_failures + [activation["id"]]))
        current.update(execution_complete=True, execution_activation_id=activation["id"], result=copy.deepcopy(value), completed_task_ids=[])
        for key in ("recovered_result", "recovered_failed_result", "decision_id", "decision_attempts", "decision_error", "selected_connections", "accepted_decision_id", "assignments", "reopen_child_execution_id"):
            current.pop(key, None)
        for generation, branch_id in current.get("branch_ids", {}).items():
            if generation in r["joins"]:
                r["joins"][generation].setdefault("branch_states", {})[branch_id] = "active" if value["status"] == "succeeded" else "unresolved_failure"
    supervisor.update(finish, "node_result_ready", activation["id"])
    d.mark_optional_failure(supervisor.run(), node, next(a for a in supervisor.run()["activations"] if a["id"] == activation["id"]), next(t for t in supervisor.run()["pending"] if t["id"] == token["id"]), {"summary": ""})


def invocation_settled_timeout(activation: dict[str, Any]) -> bool:
    invocation = activation.get("invocation") or {}
    value = activation.get("node_result") or {}
    return bool(invocation.get("timeout_expired") and invocation.get("timeout_confirmed") and invocation.get("stage") == "settled" and value.get("result", {}).get("failure_kind") == "timeout" and activation.get("status") == "failed" and not any(t.get("status") in {"reserved", "running", "uncertain"} for t in activation.get("tasks", [])))


def invocation_retry_eligible(activation: dict[str, Any]) -> bool:
    """A child invocation retries only for kinds the caller may reassign."""
    kind = ((activation.get("node_result") or {}).get("result", {}) or {}).get("child_outcome", {}).get("kind")
    if not kind:
        return False
    return kind not in {"cancelled", "uncertain", "permission", "authority"}
