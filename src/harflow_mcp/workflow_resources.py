"""Workflow resource ownership, pre-slot attempts and repair evidence.

Run and attempt state is durable before a ledger request. Reapers only read that state;
only the owning or restarted supervisor reconciles a pre-dispatch attempt. No workflow
lock is held while waiting, probing identity or inspecting the ledger.
"""
from __future__ import annotations

import asyncio
import copy
import logging
import time
import uuid
from pathlib import Path
from typing import Any

from . import resources

WAIT_SECONDS = 3600
ACTIVE = {"reserved", "running", "uncertain"}
TERMINAL = {"completed", "failed", "cancelled", "not_started"}


def validate_definition(definition: dict[str, Any]) -> None:
    from .workflows import WorkflowError
    try:
        declared = resources.validate_names(definition["resources"]) if "resources" in definition else None
        for node in definition.get("nodes", []) + definition.get("on_demand_nodes", []):
            if "resources" not in node:
                continue
            if node.get("type", "agent") != "agent":
                raise WorkflowError(f"Only agent nodes accept resources: {node['id']}")
            names = resources.validate_names(node["resources"])
            if node.get("execution_mode", "headless") == "prefer_subagent":
                raise WorkflowError(f"prefer_subagent nodes cannot declare resources: {node['id']}")
            if declared is not None and not set(names).issubset(declared):
                raise WorkflowError(f"Node resources must be a subset of run resources: {node['id']}")
            node["resources"] = list(names)
        if declared is not None:
            definition["resources"] = list(declared)
    except resources.ResourceError as exc:
        raise WorkflowError(str(exc)) from None


def validate_tree_scopes(tree: dict[str, Any]) -> None:
    """Validate every invocation path against its nearest declared run scope."""
    from .workflow_references import MAX_WORKFLOW_NESTING_DEPTH
    from .workflows import WorkflowError
    entries = tree.get("workflows", {})
    def visit(ident: str, inherited: set[str] | None, enclosing: str | None, path: tuple[str, ...]) -> None:
        if ident in path or len(path) >= MAX_WORKFLOW_NESTING_DEPTH:
            raise WorkflowError("Workflow resource validation found a cyclic or too-deep dependency path")
        if ident not in entries:
            raise WorkflowError("Workflow resource validation found a missing pinned dependency: " + ident)
        definition = entries[ident]["definition"]
        name = definition.get("name", ident)
        scope = inherited
        label = enclosing
        if "resources" in definition:
            declared = set(definition["resources"])
            missing = declared - inherited if inherited is not None else set()
            if missing:
                raise WorkflowError(f"Workflow {name} resources {', '.join(sorted(missing))} exceed enclosing declaration {enclosing}")
            scope, label = declared, name
        for node in definition.get("nodes", []) + definition.get("on_demand_nodes", []):
            missing = set(node.get("resources", [])) - scope if scope is not None else set()
            if missing:
                raise WorkflowError(f"Workflow {name} node {node['id']} resources {', '.join(sorted(missing))} exceed enclosing declaration {label}")
            if node.get("type") == "workflow":
                visit(node["workflow_ref"]["workflow_id"], scope, label, path + (ident,))
    visit(tree["root_workflow_id"], None, None, ())


def scope_for_run(storage: Any, run: dict[str, Any]) -> resources.Scope | None:
    seen = set()
    while True:
        rid = run["workflow_run_id"]
        if rid in seen or len(seen) >= 64:
            raise resources.ResourceNotDeclaredByScope("cyclic workflow resource scope; refused")
        seen.add(rid)
        held = run.get("resources") or {}
        if held.get("state") == "held" and not held.get("released"):
            return resources.Scope(held["reservation_id"])
        if run["definition"].get("resources") or run.get("resource_anchor_names"):
            return resources.Scope(None)
        link = run.get("parent_link") or {}
        if link.get("workflow_run_id"):
            # A child scope hint is a snapshot. Refresh the linked parent before
            # using its current held grant or its own enclosing scope.
            run = storage.get_run(link["workflow_run_id"], refresh_atomic=True)
            continue
        if "resource_scope" in run:
            # The external caller's coverage is acquisition provenance, not a
            # declaration by this workflow. No grant means an empty scope.
            return resources.Scope(None)
        return None


def scope_for_task(log_dir: Path, owner: dict[str, Any]) -> resources.Scope:
    from .workflows import WorkflowStore
    storage = WorkflowStore(log_dir.parent)
    run = storage.get_run(owner["workflow_run_id"], refresh_atomic=True)
    # Run coverage includes names a particular node did not need to borrow itself.
    scope = scope_for_run(storage, run)
    if scope is not None:
        return scope
    aid = owner.get("workflow_execution_id", owner.get("activation_id"))
    activation = next((a for a in run["activations"] if a["id"] == aid), None)
    if activation is None:
        raise resources.ResourceNotDeclaredByScope("workflow resource scope cannot be established; refused")
    attempts = _attempts(activation)
    held = next((ra for ra in reversed(attempts) if ra.get("state") in {"granted", "attached"} and not ra.get("released")), None)
    return resources.Scope(held["reservation_id"] if held else None)


def check_launch_scope(log_dir: Path, caller: Any, tree: dict[str, Any]) -> dict[str, Any] | None:
    from .task_resources import holding_scope, ledger_for
    scope = holding_scope(log_dir, caller)
    if scope is not None:
        for entry in tree.get("workflows", {}).values():
            definition = entry["definition"]
            for item in [definition, *definition.get("nodes", []), *definition.get("on_demand_nodes", [])]:
                if item.get("resources"):
                    ledger_for(log_dir).check_borrow(item["resources"], scope)
        return {"reservation_id": scope.reservation_id}
    return None


def cleanup(action: Any) -> None:
    """Best-effort bookkeeping must not replace a dispatch outcome."""
    try:
        action()
    except Exception:
        logging.getLogger(__name__).warning("Workflow resource cleanup failed", exc_info=True)


def _anchor_names(run: dict[str, Any], tree: dict[str, Any]) -> list[str]:
    if "resources" in run["definition"]:
        return list(run["definition"]["resources"])
    return sorted({name for entry in tree.get("workflows", {}).values()
                   for item in [entry["definition"], *entry["definition"].get("nodes", []), *entry["definition"].get("on_demand_nodes", [])]
                   for name in item.get("resources", [])})


async def establish_anchor(storage: Any, run: dict[str, Any], tree: dict[str, Any]) -> dict[str, Any]:
    """Publish a lender-backed hold before its caller can exit after launch."""
    from .task_resources import ledger_for
    from .task_supervisor import _grant_survives
    from .workflows import WorkflowError
    if "resource_scope" not in run:
        return run
    names = _anchor_names(run, tree)
    if not names:
        return run
    ledger = ledger_for(storage.root / "tasks")
    scope = resources.Scope(run["resource_scope"].get("reservation_id"))
    held = {"names": names, "reservation_id": uuid.uuid4().hex, "state": "queued", "wait_started_at": time.time()}
    def publish(r: dict[str, Any]) -> None:
        r["resources"] = copy.deepcopy(held)
        if "resources" not in r["definition"]:
            r["resource_anchor_names"] = names
    try:
        storage.update_run(run["workflow_run_id"], publish, "resource_anchor_queued")
        while True:
            ledger.check_borrow(names, scope)
            entry = ledger.request(names, {"kind": "workflow_run", "workflow_run_id": run["workflow_run_id"]}, scope=scope, reservation_id=held["reservation_id"])
            held.update(state="granted", granted_at=entry.granted_at, borrowed_from=entry.borrows_from)
            storage.update_run(run["workflow_run_id"], publish, "resource_anchor_granted")
            survives = await asyncio.to_thread(_grant_survives, ledger, entry.reservation_id)
            if survives is True:
                held["state"] = "held"
                return storage.update_run(run["workflow_run_id"], publish, "resource_anchor_held")
            if survives is None:
                raise resources.ResourceLedgerUnavailable("Workflow borrow anchor could not be confirmed after repair")
            held["state"] = "queued"
            storage.update_run(run["workflow_run_id"], publish, "resource_anchor_retry")
    except BaseException as exc:
        held["state"] = "not_started"
        def failed(r: dict[str, Any]) -> None:
            publish(r)
            r.update(status="failed", failure_reason="Workflow borrow anchor failed: " + str(exc))
        cleanup(lambda: storage.update_run(run["workflow_run_id"], failed, "resource_anchor_failed"))
        cleanup(lambda: ledger.release(held["reservation_id"]))
        if isinstance(exc, resources.ResourceError):
            raise WorkflowError(str(exc)) from exc
        raise


async def acquire_slot(supervisor: Any, activation: dict[str, Any], should_continue: Any, *, control: bool = False) -> None:
    """Disclose a held resource only while a slot is actually blocked."""
    tree = supervisor.tree
    held = blocked = False
    try:
        slots = tree.control_slots if control and tree.control_slots is not None else tree.slots
        scope = scope_for_run(supervisor.store, supervisor.run(refresh_atomic=True))
        current = next(a for a in supervisor.run()["activations"] if a["id"] == activation["id"])
        held = scope is not None and scope.reservation_id is not None or (current.get("resource_attempt") or {}).get("state") in {"granted", "attached"}
        blocked = slots is not None and (slots.free <= 0 or bool(slots.waiters))
    except Exception:
        logging.getLogger(__name__).warning("Workflow resource slot disclosure could not be observed", exc_info=True)
        held = blocked = False
    def mark(value: bool) -> None:
        def update(r: dict[str, Any]) -> None:
            a = next(a for a in r["activations"] if a["id"] == activation["id"])
            if value:
                a["resource_held_waiting_for_slot"] = True
            else:
                a.pop("resource_held_waiting_for_slot", None)
        supervisor.update(update, "resource_slot_wait" if value else "resource_slot_stopped")
    if held and blocked:
        cleanup(lambda: mark(True))
    try:
        await tree.acquire_slot(should_continue, control=control)
    finally:
        if held and blocked:
            cleanup(lambda: mark(False))


def _attempts(activation: dict[str, Any]) -> list[dict[str, Any]]:
    found = {t["resource_attempt"]["id"]: t["resource_attempt"] for t in activation.get("tasks", []) if t.get("resource_attempt")}
    if activation.get("resource_attempt"):
        latest = activation["resource_attempt"]
        if latest["id"] not in found or found[latest["id"]].get("state") != "attached":
            found[latest["id"]] = latest
    return list(found.values())


def _change_attempt(supervisor: Any, aid: str, ra: dict[str, Any], **values: Any) -> None:
    ra.update(values)
    def update(run: dict[str, Any]) -> None:
        activation = next(a for a in run["activations"] if a["id"] == aid)
        current = activation.get("resource_attempt")
        if current and current["id"] == ra["id"]:
            activation["resource_attempt"] = copy.deepcopy(ra)
            if ra["state"] != "queued":
                activation.pop("resource_wait", None)
            if ra["state"] == "not_started" and not any(t.get("status") in ACTIVE for t in activation["tasks"]):
                activation["status"] = "not_started"
        for task in activation["tasks"]:
            if (task.get("resource_attempt") or {}).get("id") == ra["id"]:
                task["resource_attempt"] = copy.deepcopy(ra)
    supervisor.update(update, "resource_attempt_updated")


def _wait_view(log_dir: Path, held: dict[str, Any]) -> dict[str, Any]:
    from .task_resources import _queue_details
    return _queue_details(log_dir, {"resources": held})


async def _grant(supervisor: Any, held: dict[str, Any], owner: dict[str, Any], scope: resources.Scope | None, publish: Any) -> resources.Entry:
    from .task_resources import ledger_for, reap
    from .task_supervisor import _grant_survives
    ledger = ledger_for(supervisor.registry.log_dir)
    deadline = time.monotonic() + WAIT_SECONDS
    last_reap = 0.0
    async def keep_waiting() -> bool:
        nonlocal last_reap
        if time.monotonic() - last_reap >= 5:
            last_reap = time.monotonic()
            await asyncio.to_thread(reap, supervisor.registry.log_dir)
        detail = await asyncio.to_thread(_wait_view, supervisor.registry.log_dir, held)
        def waiting(run: dict[str, Any]) -> None:
            target = run if owner["kind"] == "workflow_run" else next(a for a in run["activations"] if (a.get("resource_attempt") or {}).get("id") == owner["activation_id"])
            target["resource_wait"] = detail
        current = supervisor.run(refresh_atomic=True)
        target = current if owner["kind"] == "workflow_run" else next(a for a in current["activations"] if (a.get("resource_attempt") or {}).get("id") == owner["activation_id"])
        if target.get("resource_wait") != detail:
            supervisor.update(waiting, "resource_wait")
        return supervisor.tree.tree_running(supervisor.run(refresh_atomic=True), refresh_atomic=True)
    while True:
        entry = await ledger.acquire(held["names"], owner, scope=scope, timeout=max(0, deadline - time.monotonic()),
                                     reservation_id=held["reservation_id"], should_continue=keep_waiting)
        held.update(state="granted", granted_at=entry.granted_at)
        if entry.borrows_from:
            held["borrowed_from"] = entry.borrows_from
        publish()
        # Repair can have taken its evidence while this owner still said queued.
        # Once granted is durable, later repairs reconstruct this reservation.
        survives = await asyncio.to_thread(_grant_survives, ledger, entry.reservation_id)
        if survives is True:
            if not supervisor.tree.tree_running(supervisor.run(refresh_atomic=True), refresh_atomic=True):
                raise resources.ResourceWaitCancelled("Scheduling stopped before resource dispatch")
            if owner["kind"] == "workflow_run":
                supervisor.update(lambda r: r.pop("resource_wait", None), "resources_granted")
            return entry
        if survives is None:
            raise resources.ResourceLedgerUnavailable("resource grant could not be confirmed after repair")
        held.update(state="queued")
        publish()


async def acquire_attempt(supervisor: Any, activation: dict[str, Any], node: dict[str, Any], candidate_index: int) -> dict[str, Any] | None:
    if not node.get("resources"):
        return None
    from .task_resources import ledger_for
    from .workflows import DispatchNotStarted
    ra = {"id": uuid.uuid4().hex, "candidate_index": candidate_index, "state": "queued", "reservation_id": uuid.uuid4().hex,
          "names": list(node["resources"]), "wait_started_at": time.time()}
    supervisor.update(lambda r: next(a for a in r["activations"] if a["id"] == activation["id"]).update(resource_attempt=copy.deepcopy(ra)), "resource_attempt_queued")
    try:
        await _grant(supervisor, ra, {"kind": "workflow_attempt", "workflow_run_id": supervisor.run_id, "activation_id": ra["id"]},
                     scope_for_run(supervisor.store, supervisor.run(refresh_atomic=True)),
                     lambda: _change_attempt(supervisor, activation["id"], ra))
        return ra
    except BaseException as exc:
        cleanup(lambda: _change_attempt(supervisor, activation["id"], ra, state="not_started"))
        cleanup(lambda: ledger_for(supervisor.registry.log_dir).release(ra["reservation_id"]))
        if isinstance(exc, resources.ResourceError):
            if not isinstance(exc, resources.ResourceWaitCancelled):
                supervisor.attention("Resource acquisition stopped: " + str(exc))
            raise DispatchNotStarted(str(exc)) from None
        raise


def attach_attempt(supervisor: Any, aid: str, ra: dict[str, Any], reservation: dict[str, Any]) -> None:
    ra.update(state="attached", task_id=reservation["task_id"])
    reservation["resource_attempt"] = copy.deepcopy(ra)


def finish_attempt(supervisor: Any, aid: str, ra: dict[str, Any] | None) -> None:
    if ra is None:
        return
    from .task_resources import ledger_for
    run = supervisor.run(refresh_atomic=True)
    task = next((t for a in run["activations"] for t in a["tasks"] if t["task_id"] == ra.get("task_id")), None)
    if task is not None and task.get("status") not in TERMINAL:
        return
    _change_attempt(supervisor, aid, ra, **({"released": True} if task else {"state": "not_started", "released": True}))
    ledger_for(supervisor.registry.log_dir).release(ra["reservation_id"])


def abandon_attempt(supervisor: Any, aid: str, ra: dict[str, Any] | None) -> None:
    """Publication failed before a spawn request; no process can own this grant."""
    if ra is None:
        return
    from .task_resources import ledger_for
    try:
        def update(run: dict[str, Any]) -> None:
            activation = next(a for a in run["activations"] if a["id"] == aid)
            current = activation.get("resource_attempt")
            if current and current["id"] == ra["id"]:
                current.update(state="not_started", released=True)
            for task in activation["tasks"]:
                if (task.get("resource_attempt") or {}).get("id") == ra["id"]:
                    task.update(status="not_started")
                    task["resource_attempt"].update(state="not_started", released=True)
            if not any(t.get("status") in ACTIVE for t in activation["tasks"]):
                activation["status"] = "not_started"
        supervisor.update(update, "resource_dispatch_not_started")
    finally:
        cleanup(lambda: ledger_for(supervisor.registry.log_dir).release(ra["reservation_id"]))


async def prepare_run(supervisor: Any) -> bool:
    from .task_resources import ledger_for
    from . import worktrees
    run = supervisor.run(refresh_atomic=True)
    names = run["definition"].get("resources") or run.get("resource_anchor_names")
    held = run.get("resources") or {}
    if names and held.get("state") in {"granted", "held"} and not held.get("released"):
        from .task_supervisor import _grant_survives
        survives = await asyncio.to_thread(_grant_survives, ledger_for(supervisor.registry.log_dir), held["reservation_id"])
        if survives is not True:
            supervisor.attention("Persisted workflow resource grant is missing or unavailable; reconcile resources before resuming")
            return False
        if held["state"] == "granted":
            held["state"] = "held"
            supervisor.update(lambda r: r.update(resources=copy.deepcopy(held)), "run_resources_restored")
    if names and (held.get("state") != "held" or held.get("released")):
        link = run.get("parent_link") or {}
        acquisition_scope = (scope_for_run(supervisor.store, supervisor.store.get_run(link["workflow_run_id"], refresh_atomic=True))
                             if link.get("workflow_run_id") else resources.Scope(run["resource_scope"].get("reservation_id")) if "resource_scope" in run else None)
        if acquisition_scope is not None:
            try:
                ledger_for(supervisor.registry.log_dir).check_borrow(names, acquisition_scope)
            except resources.ResourceError as exc:
                supervisor.attention("Original workflow resource lender is unavailable: " + str(exc))
                return False
        held = {"names": names, "reservation_id": uuid.uuid4().hex, "state": "queued", "wait_started_at": time.time()}
        supervisor.update(lambda r: r.update(resources=copy.deepcopy(held)), "run_resources_queued")
        def publish() -> None:
            supervisor.update(lambda r: r.update(resources=copy.deepcopy(held)), "run_resources_granted")
        try:
            await _grant(supervisor, held, {"kind": "workflow_run", "workflow_run_id": supervisor.run_id},
                         acquisition_scope, publish)
            held["state"] = "held"
            publish()
        except BaseException as exc:
            held["state"] = "not_started"
            cleanup(publish)
            cleanup(lambda: ledger_for(supervisor.registry.log_dir).release(held["reservation_id"]))
            cleanup(lambda: supervisor.update(lambda r: r.pop("resource_wait", None), "resource_wait_stopped"))
            if isinstance(exc, resources.ResourceWaitCancelled):
                return False
            if isinstance(exc, resources.ResourceError):
                supervisor.attention("Run resource acquisition failed: " + str(exc))
                return False
            raise
    run = supervisor.run(refresh_atomic=True)
    plan = run.get("worktree")
    if plan and plan["state"] != "created":
        if plan["state"] != "planned":
            supervisor.attention("Workflow worktree creation is uncertain or failed; inspect the retained path before recovery")
            return False
        try:
            def ensure_registered() -> None:
                from .workflow_worktree_receipts import register
                with worktrees.usage_lock(Path(plan["path"])):
                    register(supervisor.registry.log_dir, supervisor.run_id, plan)
            await asyncio.to_thread(ensure_registered)
            created = await asyncio.to_thread(worktrees.create, plan, on_creating=lambda p: supervisor.update(lambda r: r.update(worktree=p), "worktree_creating"))
            supervisor.update(lambda r: r.update(worktree=created, repo_path=created["path"], source_repo_path=created["source_repo_path"]), "worktree_created")
        except (worktrees.WorktreeError, ValueError, OSError) as exc:
            supervisor.update(lambda r: r.update(worktree={**r["worktree"], "state": "failed", "error": str(exc)}, status="failed", failure_reason="Workflow worktree creation failed: " + str(exc)), "worktree_failed")
            cleanup(lambda: release_run(supervisor.store, supervisor.run(refresh_atomic=True)))
            return False
    if plan and not Path(supervisor.run(refresh_atomic=True)["repo_path"]).is_dir():
        supervisor.attention("Workflow worktree has been removed; restore it before resuming")
        return False
    return True


def _unresolved(storage: Any, run: dict[str, Any], seen: set[str] | None = None) -> bool:
    seen = set() if seen is None else seen
    rid = run["workflow_run_id"]
    if rid in seen:
        return True
    seen.add(rid)
    for activation in run.get("activations", []):
        if any(t.get("status") in ACTIVE for t in activation.get("tasks", [])):
            return True
        if any(ra.get("state") in {"queued", "granted"} for ra in _attempts(activation)):
            return True
        child_id = (activation.get("invocation") or {}).get("child_workflow_run_id")
        if child_id:
            try:
                child = storage.get_run(child_id, refresh_atomic=True)
            except (OSError, ValueError):
                return True
            if child.get("status") not in TERMINAL or _unresolved(storage, child, seen):
                return True
    return False


def release_run(storage: Any, run: dict[str, Any]) -> None:
    from .task_resources import ledger_for
    held = run.get("resources") or {}
    if held.get("reservation_id") and not held.get("released") and run.get("status") in TERMINAL and not _unresolved(storage, run):
        # Keep durable held evidence until the ledger release is published. A repair
        # that races this path restores a release-requested lender conservatively.
        ledger_for(storage.root / "tasks").release(held["reservation_id"])
        storage.update_run(run["workflow_run_id"], lambda r: r["resources"].update(released=True), "run_resources_released")


def reconcile(storage: Any, run: dict[str, Any], *, owner_dead: bool) -> dict[str, Any]:
    from .task_resources import ledger_for
    released = []
    changes = []
    for activation in run.get("activations", []):
        for ra in _attempts(activation):
            if ra.get("released"):
                continue
            task = next((t for t in activation["tasks"] if (t.get("resource_attempt") or {}).get("id") == ra["id"]), None)
            if (task and task.get("status") in TERMINAL) or (owner_dead and task is None and ra.get("state") in {"queued", "granted"}):
                changes.append((activation["id"], ra["id"], task is None))
                released.append(ra["reservation_id"])
    held = run.get("resources") or {}
    reset_run = owner_dead and held.get("state") == "queued"
    unknown_worktree = owner_dead and (run.get("worktree") or {}).get("state") == "creating"
    if reset_run:
        released.append(held["reservation_id"])
    stale_flags = owner_dead and ("resource_wait" in run or any("resource_held_waiting_for_slot" in a or "resource_wait" in a for a in run.get("activations", [])))
    if changes or reset_run or unknown_worktree or stale_flags:
        def update(r: dict[str, Any]) -> None:
            for aid, raid, never_started in changes:
                a = next(a for a in r["activations"] if a["id"] == aid)
                candidates = [a["resource_attempt"]] if a.get("resource_attempt") else []
                candidates += [t["resource_attempt"] for t in a["tasks"] if t.get("resource_attempt")]
                for ra in candidates:
                    if ra["id"] == raid:
                        ra.update(released=True)
                        if never_started:
                            ra["state"] = "not_started"
                if never_started and not any(t.get("status") in ACTIVE for t in a["tasks"]):
                    a["status"] = "not_started"
                a.pop("resource_wait", None)
                a.pop("resource_held_waiting_for_slot", None)
            if owner_dead:
                for activation in r["activations"]:
                    activation.pop("resource_held_waiting_for_slot", None)
                    activation.pop("resource_wait", None)
            if unknown_worktree:
                r["worktree"]["state"] = "unknown"
                r.update(status="needs_attention", attention_reason="Workflow worktree creation is uncertain; cancel, inspect and clean the retained path manually")
            if reset_run:
                r["resources"]["state"] = "not_started"
            r.pop("resource_wait", None)
        run = storage.update_run(run["workflow_run_id"], update, "resources_reconciled")
        for rid in released:
            cleanup(lambda: ledger_for(storage.root / "tasks").release(rid))
    cleanup(lambda: release_run(storage, run))
    return run


def owner_resolvers(log_dir: Path) -> dict[str, Any]:
    def resolve(entry: resources.Entry) -> str:
        from .workflows import WorkflowStore
        storage = WorkflowStore(log_dir.parent)
        run = storage.get_run(entry.owner.workflow_run_id, refresh_atomic=True)
        if entry.owner.kind == "workflow_run":
            return resources.SETTLED if run["status"] in TERMINAL and not _unresolved(storage, run) else resources.KEEP
        for activation in run["activations"]:
            for ra in _attempts(activation):
                if ra["id"] != entry.owner.activation_id:
                    continue
                if ra["state"] == "not_started":
                    return resources.NEVER_STARTED
                task = next((t for t in activation["tasks"] if (t.get("resource_attempt") or {}).get("id") == ra["id"]), None)
                return resources.SETTLED if task and task["status"] in TERMINAL else resources.KEEP
        return resources.KEEP
    return {"workflow_run": resolve, "workflow_attempt": resolve}


def collect_evidence(log_dir: Path) -> list[dict[str, Any]]:
    from .workflows import WorkflowStore
    if not (log_dir.parent / "workflow-runs").exists():
        return []
    storage = WorkflowStore(log_dir.parent)
    specs = []
    for run in storage.list_runs(strict=True):
        held = run.get("resources") or {}
        if held.get("state") in {"granted", "held"}:
            spec = {"reservation_id": held["reservation_id"], "names": held["names"], "owner": {"kind": "workflow_run", "workflow_run_id": run["workflow_run_id"]}}
            lender = held.get("borrowed_from") or (run.get("resource_scope") or {}).get("reservation_id")
            if lender:
                spec["borrows_from"] = lender
            if held.get("released") or run["status"] in TERMINAL and not _unresolved(storage, run):
                spec["release_requested"] = True
            specs.append(spec)
        for activation in run["activations"]:
            for ra in _attempts(activation):
                if ra["state"] not in {"granted", "attached"}:
                    continue
                spec = {"reservation_id": ra["reservation_id"], "names": ra["names"], "owner": {"kind": "workflow_attempt", "workflow_run_id": run["workflow_run_id"], "activation_id": ra["id"]}}
                if ra.get("borrowed_from"):
                    spec["borrows_from"] = ra["borrowed_from"]
                task = next((t for t in activation["tasks"] if (t.get("resource_attempt") or {}).get("id") == ra["id"]), None)
                if ra.get("released") or task and task["status"] in TERMINAL:
                    spec["release_requested"] = True
                specs.append(spec)
    return specs


def worktree_preview(repo: Path, spec: Any, log_dir: Path, tree: dict[str, Any]) -> dict[str, Any] | None:
    if spec is None:
        return None
    from . import worktrees
    from .workflows import WorkflowError
    agents = [n for entry in tree.get("workflows", {}).values() for n in entry["definition"].get("nodes", []) + entry["definition"].get("on_demand_nodes", []) if n.get("type", "agent") == "agent"]
    if not any(n.get("freedom", "read_only") != "read_only" for n in agents):
        raise WorkflowError("Workflow worktrees require at least one agent node above read_only")
    plan = worktrees.plan("<run_id>", repo.resolve(), spec, log_dir)
    if not spec.get("branch") and not spec.get("detach"):
        plan["branch"] = "harflow/workflow-<run_id>"
    return plan
