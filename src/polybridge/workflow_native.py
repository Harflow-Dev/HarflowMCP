"""Durable, sequential native nodes owned by a managed orchestrator session.

Native attempts have execution IDs and child evidence, never TaskRecords/PIDs.
Only the parent transport is a registry task. Unsupported requests fall back
before launch; after launch any missing evidence remains uncertain.
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any

POLICY = "native_workers_plus_control_v1"


def scheduling_policy(definition: dict[str, Any], tree: dict[str, Any] | None = None) -> str:
    definitions = [definition] + [entry["definition"] for entry in (tree or {}).get("workflows", {}).values()]
    return POLICY if any(n.get("execution_mode") == "prefer_subagent" for d in definitions for n in d.get("nodes", [])) else "legacy"


def merge_denials(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = copy.deepcopy(existing)
    for denial in incoming:
        if denial not in result:
            result.append(copy.deepcopy(denial))
    return result


def presentation(attempt: dict[str, Any]) -> dict[str, Any]:
    return {key: attempt[key] for key in ("execution_kind", "execution_fallback_reason", "owner_task_id", "native_child_id", "activity_level", "can_cancel_child", "can_resume_child") if key in attempt}


def _fallback(supervisor: Any, activation: dict[str, Any], reason: str) -> tuple[bool, None]:
    supervisor.update(lambda r: next(a for a in r["activations"] if a["id"] == activation["id"]).update(execution_kind="headless", execution_fallback_reason=reason), "native_ineligible", reason)
    activation["execution_fallback_reason"] = reason
    return False, None


async def dispatch_native(supervisor: Any, node: dict[str, Any], assignment: str, activation: dict[str, Any]) -> tuple[bool, dict[str, Any] | None]:
    from . import backends, events, store as task_store
    from .backends.native import adapter
    from .workflows import CheckoutLease, DispatchNotStarted, run_effective_freedom, _candidate_key
    from .workflow_invocation import tree_write_strength
    run = supervisor.run()
    refuse = None
    if run.get("scheduling_policy") != POLICY:
        refuse = "Historical runs retain their original scheduling policy"
    elif run.get("execution_contract") != "delegation" or run.get("runner_policy") != "guided":
        refuse = "Native execution requires a guided managed orchestrator"
    elif node.get("session_mode") != "fresh" or activation.get("resume_task_id") or activation.get("continue_previous") or activation.get("resume_question_id"):
        refuse = "Native child resume is not certified; preserving the requested headless session mode"
    elif run.get("parent_link") or any(n.get("type") == "parallel_start" for n in run["definition"]["nodes"]):
        refuse = "Parallel and nested native execution are not yet certified"
    if refuse:
        return _fallback(supervisor, activation, refuse)
    owner = run.get("sessions", {}).get("orchestrator", {})
    record = task_store.read(supervisor.registry._log_dir, owner.get("task_id", "")) if owner.get("task_id") else None
    if record is None or record.status != "completed" or not record.session_id or record.freedom != "read_only" or record.repo_path != run["repo_path"]:
        return _fallback(supervisor, activation, "A settled compatible owning orchestrator session is unavailable")
    candidate = node["agent"]
    if candidate["backend"] != record.backend:
        return _fallback(supervisor, activation, "The node harness differs from its actual owning orchestrator")
    backend = backends.get(record.backend)
    native = adapter(backend)
    if native is None:
        return _fallback(supervisor, activation, "This harness has no certified native execution adapter")
    settings = {"freedom": run_effective_freedom(run, node), "network": False if run["network"] is False else node.get("network", run["network"]), "session_mode": "fresh"}
    import asyncio
    reason = await asyncio.to_thread(native.eligible, record, candidate, settings)
    if reason:
        return _fallback(supervisor, activation, reason)
    execution_id, transport_id, nonce = uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex
    child = {"task_id": execution_id, "execution_kind": "native_subagent", "status": "reserved", "dispatch_stage": "preparing", "candidate": copy.deepcopy(candidate), "freedom": settings["freedom"], "network": settings["network"], "repo_path": run["repo_path"], "session_mode": "fresh", "reserved_at": time.time(), "dispatch_nonce": nonce, "owner_task_id": record.task_id, "owner_session_id": record.session_id, "owner_session_generation": owner.get("task_id"), "transport_task_id": transport_id, "assignment_sha256": hashlib.sha256(assignment.encode()).hexdigest(), "assignment_prompt": activation.get("assignment_prompt", assignment), "activity_level": native.activity_level, "can_cancel_child": False, "can_resume_child": False, "harness_metadata": {"requested": copy.deepcopy(candidate), "effective": {**settings, "backend": record.backend, "model": candidate.get("model"), "reasoning_effort": candidate.get("reasoning_effort"), "settings_source": "certified_native_adapter"}, "observed": None, "verification_status": "configured_not_observed"}}
    continue_running = lambda: supervisor.tree.tree_running(supervisor.run())
    control_held = worker_held = False
    uncertain = False
    log = None
    observers = getattr(supervisor.registry, "_workflow_native_observers", None)
    if observers is None:
        observers = supervisor.registry._workflow_native_observers = {}
    state: dict[str, Any] = {"assignment": assignment, "owner_session_id": record.session_id, "expected_model": record.model}
    try:
        # Cross-owner turns queue without consuming workers or checkout leases.
        await supervisor.tree.acquire_slot(continue_running, control=True)
        control_held = True
        await supervisor.tree.acquire_slot(continue_running)
        worker_held = True
        async with CheckoutLease(supervisor.store, run["repo_path"], tree_write_strength(run, settings["freedom"]), continue_running, pool=supervisor.checkout_leases):
            def reserve(r: dict[str, Any]) -> None:
                a = next(a for a in r["activations"] if a["id"] == activation["id"])
                a["tasks"].append(copy.deepcopy(child))
                a.update(presentation(child))
                r["activations"].append({"id": transport_id, "node_id": "orchestrator", "role": "native_control", "status": "running", "created_at": time.time(), "tasks": [{"task_id": transport_id, "status": "reserved", "dispatch_stage": "preparing", "candidate": {"backend": record.backend, "model": record.model, "reasoning_effort": record.reasoning_effort}, "freedom": "read_only", "repo_path": run["repo_path"], "native_owner_execution_id": execution_id}]})
            supervisor.update(reserve, "native_reserved", {"execution_id": execution_id, "nonce": nonce})
            log = events.EventLog(events.events_path(supervisor.registry._log_dir, execution_id), execution_id)

            def observe(event: dict[str, Any]) -> None:
                updates = native.observe(event, nonce, state)
                for update in updates:
                    kind = update.pop("native_update")
                    if kind == "started":
                        def start_child(r: dict[str, Any]) -> None:
                            from .workflows import update_task_state
                            if any(t.get("native_child_id") == update["native_child_id"] and t.get("owner_session_id") == record.session_id and t.get("task_id") != execution_id for a in r["activations"] for t in a.get("tasks", [])):
                                state["invalid"] = "Native child identity was reused across executions of the owning session"
                                raise ValueError(state["invalid"])
                            a = next(a for a in r["activations"] if a["id"] == activation["id"])
                            attempt = next(t for t in a["tasks"] if t["task_id"] == execution_id)
                            update_task_state(a, attempt, {"status": "running", "dispatch_stage": "child_confirmed", "native_child_id": update["native_child_id"], "started_at": time.time()})
                            a["native_child_id"] = update["native_child_id"]
                        supervisor.update(start_child, "native_child_started")
                    elif kind == "settled":
                        result = {"task_id": execution_id, "status": update["status"], "summary": update.get("summary", ""), "execution_kind": "native_subagent", "native_child_id": state.get("native_child_id"), "parent_task_id": transport_id}
                        if update["status"] != "completed":
                            result.update(execution_failure=update.get("summary", "Native child failed"), blocked_failure=True, optional_failure_eligible=False)
                        result["observed_model"] = update.get("observed_model")
                        if update.get("permission_denials"):
                            result["permission_denials"] = copy.deepcopy(update["permission_denials"])
                        state["result"] = result
                        supervisor._task_update(activation["id"], execution_id, {"status": update["status"], "dispatch_stage": "child_settled", "native_terminal": True, "result": result, "finished_at": time.time(), "harness_metadata": {**child["harness_metadata"], "observed": {"model": update.get("observed_model"), "native_child_id": state.get("native_child_id")}, "verification_status": "observed_native_child"}})
                    elif kind == "permissions":
                        # Parent terminal evidence includes child refusals. Keep
                        # it conservatively on the child before graph decisions.
                        if "result" in state:
                            state["result"]["permission_denials"] = merge_denials(state["result"].get("permission_denials", []), update["permission_denials"])
                            supervisor._task_update(activation["id"], execution_id, {"result": state["result"]})
                    elif kind == "activity":
                        log.write(update.pop("event_kind"), update)
            observers[transport_id] = observe
            supervisor._task_update(activation["id"], execution_id, {"dispatch_stage": "launch_requested", "launch_requested_at": time.time()})
            supervisor._task_update(transport_id, transport_id, {"dispatch_stage": "spawn_requested"})
            parent = supervisor.registry.get(record.task_id)
            prompt = native.prompt(assignment, nonce)
            if parent is not None:
                task = await supervisor.registry.resume(parent, prompt, task_id=transport_id, display_prompt=f"Native subagent: {node.get('title', node['id'])}", native_subagent=True, max_turns=candidate.get("max_turns"))
            else:
                task = await supervisor.registry.resume_record(record, prompt, task_id=transport_id, display_prompt=f"Native subagent: {node.get('title', node['id'])}", native_subagent=True, max_turns=candidate.get("max_turns"))
            supervisor._task_update(transport_id, transport_id, {"status": "running", "dispatch_stage": "spawn_confirmed"})
            deadline = time.monotonic() + node["timeout_seconds"] if node.get("timeout_seconds") else None
            cancel_requested = False
            while not task.done.is_set():
                import asyncio
                if not cancel_requested and (supervisor.run()["status"] == "cancelling" or deadline is not None and time.monotonic() >= deadline):
                    # Individual cancellation is unavailable. Root/timeout cancellation
                    # stops the charged parent transport, but cannot invent child death.
                    cancel_requested = True
                    await supervisor.registry.cancel_cascade(transport_id, workflow_control=True)
                    deadline = None
                try:
                    await asyncio.wait_for(task.done.wait(), .25)
                except TimeoutError:
                    pass
            snapshot = task.snapshot()
            if snapshot.get("permission_denials") and "result" in state:
                state["result"]["permission_denials"] = merge_denials(state["result"].get("permission_denials", []), snapshot["permission_denials"])
                supervisor._task_update(activation["id"], execution_id, {"result": state["result"]})
            supervisor._task_update(transport_id, transport_id, {"status": snapshot["status"], "result": snapshot, "finished_at": time.time()})
            supervisor.update(lambda r: next(a for a in r["activations"] if a["id"] == transport_id).update(status=snapshot["status"]), "native_control_settled")
            if snapshot["status"] != "completed" or "result" not in state or state.get("invalid") or getattr(supervisor.registry, "_workflow_native_failures", {}).get(transport_id):
                uncertain = "result" not in state or bool(state.get("invalid")) or bool(getattr(supervisor.registry, "_workflow_native_failures", {}).get(transport_id))
                supervisor._task_update(activation["id"], execution_id, {"status": "uncertain" if uncertain else state["result"]["status"], "error": "Native child or parent turn did not conclusively settle"})
                supervisor.attention("Native execution requires reconciliation; no headless duplicate dispatched")
                return True, None
            supervisor.update(lambda r: r["sessions"].__setitem__("orchestrator", {"candidate": owner["candidate"], "task_id": transport_id, "session_id": record.session_id}), "native_owner_session_recorded")
            # Worker session is intentionally absent: native resume is not supported.
            return True, state["result"]
    except DispatchNotStarted:
        return True, None
    except Exception as exc:
        latest = next((t for a in supervisor.run()["activations"] if a["id"] == activation["id"] for t in a["tasks"] if t["task_id"] == execution_id), None)
        if latest is not None:
            uncertain = latest.get("dispatch_stage") != "preparing"
            supervisor._task_update(activation["id"], execution_id, {"status": "uncertain" if uncertain else "not_started", "error": str(exc)})
        supervisor.attention(f"Native dispatch requires reconciliation: {exc}")
        return True, None
    finally:
        observers.pop(transport_id, None)
        if log is not None:
            log.close()
        if worker_held:
            if uncertain:
                supervisor.tree.hold_slot(supervisor.run_id, execution_id)
            else:
                supervisor.tree.release_slot()
        if control_held:
            if uncertain:
                supervisor.tree.hold_slot(supervisor.run_id, transport_id, control=True)
            else:
                supervisor.tree.release_slot(control=True)
