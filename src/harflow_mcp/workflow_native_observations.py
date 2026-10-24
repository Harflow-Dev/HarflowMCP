"""Replay task-owner native observations before accepting transport settlement."""
from __future__ import annotations

from typing import Any


def require_transport_owner(supervisor: Any, transport_id: str, *, durable: bool = False) -> None:
    """A stream observer cannot wait indefinitely after its qualified owner is lost."""
    if not durable and getattr(supervisor.registry, "execution_mode", "in_process") != "supervised":
        return
    from .task_supervision import attachable
    if not attachable(supervisor.registry._log_dir, transport_id):
        raise ValueError("Native transport owner is lost or uncertain")


def require_transport_settled(supervisor: Any, transport_id: str, *, durable: bool = False) -> None:
    """Exit, owner settlement and output drain are separate from the replay barrier."""
    if not durable and getattr(supervisor.registry, "execution_mode", "in_process") != "supervised":
        return
    from . import store
    record = store.read(supervisor.registry._log_dir, transport_id)
    if record is None or record.status not in store.TERMINAL_RECORD_STATUSES or record.exit_code is None or not record.output_drained or not record.finished_at:
        raise ValueError("Native transport exit and output drain were not observed")


class Replay:
    def __init__(self, supervisor: Any, transport_id: str, observe: Any):
        self.supervisor = supervisor
        self.transport_id = transport_id
        self.observe = observe
        self.cursor = 0

    def poll(self, *, terminal: bool = False) -> None:
        from .task_observations import read
        while True:
            records, final = read(self.supervisor.registry._log_dir, self.transport_id, after=self.cursor)
            for entry in records:
                self.observe(entry["event"])
                self.cursor = entry["sequence"]
                self.supervisor._task_update(self.transport_id, self.transport_id, {"native_observation_cursor": self.cursor, "native_observation_generation": entry["generation"]})
            if final is None or self.cursor == final or not records:
                break
        if terminal:
            if final is None or final != self.cursor:
                raise ValueError("Native transport final observation barrier is missing or incomplete")
            self.supervisor._task_update(self.transport_id, self.transport_id, {"native_observation_watermark": final})


async def recover(supervisor: Any, activation: dict[str, Any]) -> dict[str, Any] | None:
    """Join an existing transport observer; never request a replacement child."""
    import asyncio
    from .state_ownership import owned_job
    child = next(t for t in activation["tasks"] if t.get("execution_kind") == "native_subagent" and t.get("status") in {"running", "reserved", "uncertain"})
    transport_id = child["transport_task_id"]
    jobs = getattr(supervisor.tree, "native_recovery_jobs", None)
    if jobs is None:
        jobs = supervisor.tree.native_recovery_jobs = {}
    key = (supervisor.run_id, transport_id)
    if key not in jobs:
        jobs[key] = owned_job(supervisor.state_layout, _recover_transport(supervisor, transport_id), name="harflow-native-recovery")
    results = await asyncio.shield(jobs[key])
    return results.get(child["task_id"])


def native_adoption_tasks(supervisor: Any, run: dict[str, Any], transport_id: str) -> set[tuple[str, str]]:
    """Return no exemption when native adoption authority cannot be established."""
    from .store import TaskDocumentError
    from .workflows import WorkflowError
    try:
        return _qualified_native_adoption_tasks(supervisor, run, transport_id)
    except (WorkflowError, TaskDocumentError, OSError, ValueError, KeyError, TypeError, AttributeError):
        return set()


def _qualified_native_adoption_tasks(supervisor: Any, run: dict[str, Any], transport_id: str) -> set[tuple[str, str]]:
    """Qualify the exact reciprocal native child set this observer will adopt."""
    from . import store
    from .task_supervision import attachable, read_launch
    from .workflow_recovery import inventory
    root_id = (run.get("parent_link") or {}).get("root_workflow_run_id", run["workflow_run_id"])
    root = supervisor.store.get_run(root_id, refresh_atomic=True)
    linked = {item["workflow_run_id"]: item for item in inventory(supervisor.store, root)}
    current = linked.get(run["workflow_run_id"])
    if current is None:
        raise ValueError("Native adoption run is not linked to the scheduler root")
    controls = [(a, t) for a in current["activations"] if a.get("role") == "native_control" and a["id"] == transport_id for t in a["tasks"] if t["task_id"] == transport_id]
    if len(controls) != 1:
        raise ValueError("Native adoption has no unique owning control transport")
    transport = controls[0][1]
    children = [t for a in current["activations"] for t in a["tasks"] if t.get("execution_kind") == "native_subagent" and t.get("transport_task_id") == transport_id]
    references = list(transport.get("native_owner_execution_ids", []))
    if transport.get("native_owner_execution_id"):
        references.append(transport["native_owner_execution_id"])
    child_ids = {child["task_id"] for child in children}
    if not children or len(child_ids) != len(children) or len(references) != len(set(references)) or set(references) != child_ids:
        raise ValueError("Native adoption child and control transport links do not match")
    record = store.read(supervisor.registry._log_dir, transport_id)
    launch = read_launch(supervisor.registry._log_dir, transport_id)
    if record is None or not record.owner or not record.parent_task_id or not record.session_id or launch is None or launch.get("state") not in {"claimed", "spawned", "settled"} or launch.get("owner") != record.owner or launch.get("backend") != record.backend or launch.get("parent_task_id") != record.parent_task_id or not attachable(supervisor.registry._log_dir, transport_id):
        raise ValueError("Native adoption transport owner is unavailable or uncertain")
    if transport.get("native_observation_generation") not in {None, launch["generation"]}:
        raise ValueError("Native adoption transport generation changed")
    for child in children:
        context = child.get("native_replay_state")
        if not isinstance(context, dict) or not isinstance(child.get("dispatch_nonce"), str) or not child["dispatch_nonce"] or child.get("owner_task_id") != record.parent_task_id or child.get("owner_session_id") != record.session_id or context.get("owner_session_id") != record.session_id:
            raise ValueError("Native adoption child identity does not match its owning transport")
    return {(current["workflow_run_id"], child_id) for child_id in child_ids}


async def _recover_transport(supervisor: Any, transport_id: str) -> dict[str, Any]:
    import asyncio
    import copy
    import time
    from . import backends, store
    from .backends.native import adapter
    from .workflows import CheckoutLease, update_task_state
    from .workflow_invocation import tree_write_strength
    run = supervisor.run(refresh_atomic=True)
    children = [(a["id"], copy.deepcopy(t)) for a in run["activations"] for t in a["tasks"] if t.get("execution_kind") == "native_subagent" and t.get("transport_task_id") == transport_id]
    task = supervisor.registry.attach(transport_id)
    record = store.read(supervisor.registry._log_dir, transport_id)
    if task is None or record is None:
        raise ValueError("Native transport owner metadata is unavailable")
    native = adapter(backends.get(record.backend))
    if native is None or any(not isinstance(child.get("native_replay_state"), dict) for _, child in children):
        raise ValueError("Native adapter or durable replay context is unavailable")
    states = {child["task_id"]: copy.deepcopy(child["native_replay_state"]) for _, child in children}
    def publish(aid: str, child: dict[str, Any], updates: list[dict[str, Any]]) -> None:
        state = states[child["task_id"]]
        for raw in updates:
            update = dict(raw)
            kind = update.pop("native_update")
            def apply(r: dict[str, Any]) -> None:
                a = next(a for a in r["activations"] if a["id"] == aid)
                target = next(t for t in a["tasks"] if t["task_id"] == child["task_id"])
                if kind == "started":
                    native_id = update["native_child_id"]
                    if target.get("native_child_id") not in {None, native_id} or any(t.get("native_child_id") == native_id and t.get("owner_session_id") == child["owner_session_id"] and t["task_id"] != child["task_id"] for a in r["activations"] for t in a["tasks"]):
                        raise ValueError("Native replay child identity changed or was reused")
                    if not target.get("native_terminal"):
                        update_task_state(a, target, {"status": "running", "dispatch_stage": "child_confirmed", "native_child_id": native_id})
                    a["native_child_id"] = native_id
                elif kind == "settled":
                    result = {"task_id": child["task_id"], "status": update["status"], "summary": update.get("summary", ""), "execution_kind": "native_subagent", "native_child_id": state.get("native_child_id"), "parent_task_id": transport_id, "observed_model": update.get("observed_model")}
                    if update["status"] != "completed":
                        result.update(execution_failure=update.get("summary", "Native child failed"), blocked_failure=True, optional_failure_eligible=False)
                    for field in ("failure_diagnostic", "permission_denials"):
                        if update.get(field):
                            result[field] = copy.deepcopy(update[field])
                    if result.get("failure_diagnostic", {}).get("category") == "usage_limit":
                        result["blocked_failure"] = False
                    if target.get("result", {}).get("permission_denials"):
                        from .workflow_native import merge_denials
                        result["permission_denials"] = merge_denials(target["result"]["permission_denials"], result.get("permission_denials", []))
                    state["result"] = result
                    observed = {**copy.deepcopy(update.get("observed_metadata", {})), "model": update.get("observed_model"), "native_child_id": state.get("native_child_id")}
                    update_task_state(a, target, {"status": update["status"], "dispatch_stage": "child_settled", "native_terminal": True, "result": result, "harness_metadata": {**child.get("harness_metadata", {}), "observed": observed, "verification_status": "observed_native_child"}})
                elif kind == "permissions" and target.get("result"):
                    from .workflow_native import merge_denials
                    target["result"]["permission_denials"] = merge_denials(target["result"].get("permission_denials", []), update["permission_denials"])
                    state["result"] = copy.deepcopy(target["result"])
            if kind in {"started", "settled", "permissions"}:
                supervisor.update(apply, "native_observation_recovered")
    def observe(event: dict[str, Any]) -> None:
        for aid, child in children:
            state = states[child["task_id"]]
            publish(aid, child, native.observe(event, child["dispatch_nonce"], state))
    replay = Replay(supervisor, transport_id, observe)
    held = [(supervisor.run_id, child["task_id"]) for _, child in children] + [(supervisor.run_id, transport_id)]
    root_id = (run.get("parent_link") or {}).get("root_workflow_run_id", supervisor.run_id)
    lease_root = supervisor.store.get_run(root_id)
    settled = False
    try:
        adopted_native_tasks = native_adoption_tasks(supervisor, run, transport_id)
        if not adopted_native_tasks:
            raise ValueError("Native recovery cannot establish exact child adoption authority")
        async with CheckoutLease(supervisor.store, run["repo_path"], tree_write_strength(lease_root, record.freedom), pool=supervisor.checkout_leases, owned_run_ids=getattr(supervisor.tree, "recovered_run_ids", set()), adopted_native_tasks=adopted_native_tasks):
            while not task.done.is_set():
                require_transport_owner(supervisor, transport_id, durable=True)
                replay.poll()
                deadlines = [c["timeout_deadline"] for _, c in children if c.get("timeout_deadline") is not None]
                if supervisor.run(refresh_atomic=True)["status"] == "cancelling" or deadlines and time.time() >= min(deadlines):
                    supervisor._task_update(transport_id, transport_id, {"native_cancel_requested_at": time.time()})
                    await supervisor.registry.cancel_cascade(transport_id, workflow_control=True)
                try:
                    await asyncio.wait_for(task.done.wait(), .25)
                except TimeoutError:
                    pass
            require_transport_settled(supervisor, transport_id, durable=True)
            replay.poll(terminal=True)
            snapshot = task.snapshot()
            finalize = getattr(native, "finalize", None)
            if snapshot["status"] == "completed" and finalize is not None and not any(state.get("invalid") for state in states.values()):
                from .state_ownership import run_owned_worker
                for aid, child in children:
                    state = states[child["task_id"]]
                    publish(aid, child, await run_owned_worker(supervisor.state_layout, finalize, child["dispatch_nonce"], state))
            if snapshot["status"] != "completed" or any("result" not in state or state.get("invalid") for state in states.values()):
                raise ValueError("Native parent or child lacks conclusive settlement evidence")
            supervisor._task_update(transport_id, transport_id, {"status": snapshot["status"], "result": snapshot, "finished_at": time.time()})
            supervisor.update(lambda r: next(a for a in r["activations"] if a["id"] == transport_id).update(status=snapshot["status"]), "native_control_settled")
            override = getattr(supervisor, "orchestrator_override", None)
            owner_id = override[0] if override else supervisor.run_id
            def record_session(owner: dict[str, Any]) -> None:
                previous = owner.get("sessions", {}).get("orchestrator", {})
                if previous.get("task_id") not in {record.parent_task_id, transport_id, *(c.get("owner_task_id") for _, c in children)}:
                    raise ValueError("Native owning session changed before recovered settlement")
                owner.setdefault("sessions", {})["orchestrator"] = {**previous, "task_id": transport_id, "session_id": record.session_id}
            supervisor.store.update_run(owner_id, record_session, "native_owner_session_recorded")
            settled = True
            return {tid: state["result"] for tid, state in states.items()}
    except Exception as exc:
        from .state_ownership import StateOwnershipError
        from .workflow_run_documents import WorkflowRunDocumentError
        from .workflow_receipt_documents import WorkflowReceiptDocumentError
        if isinstance(exc, (StateOwnershipError, WorkflowRunDocumentError, WorkflowReceiptDocumentError)):
            raise
        for aid, child in children:
            supervisor._task_update(aid, child["task_id"], {"status": "uncertain", "error": str(exc)})
        supervisor.attention("Native recovery requires reconciliation; no replacement dispatched")
        return {}
    finally:
        if settled:
            for key in held:
                if key in supervisor.tree.held:
                    supervisor.tree.release_slot(control=supervisor.tree.held.pop(key))


def recoverable(log_dir: Any, child: dict[str, Any]) -> bool:
    from . import backends, store
    from .backends.native import adapter
    from .task_supervision import attachable
    record = store.read(log_dir, child.get("transport_task_id", ""))
    return bool(record is not None and adapter(backends.get(record.backend)) is not None and isinstance(child.get("native_replay_state"), dict) and attachable(log_dir, record.task_id))
