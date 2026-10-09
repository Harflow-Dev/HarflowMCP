"""One owning control turn for a bounded batch of certified native children."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import time
import uuid
from typing import Any


async def dispatch_batch(supervisor: Any, node: dict[str, Any], assignment: str, activation: dict[str, Any], prepared: dict[str, Any]) -> tuple[bool, dict[str, Any] | None]:
    tree = supervisor.tree
    pending = getattr(tree, "native_batches", None)
    if pending is None:
        pending = tree.native_batches = {}
    key = (supervisor.run_id, prepared["owner_run"]["workflow_run_id"], json.dumps(prepared["settings"], sort_keys=True), node.get("timeout_seconds") or None)
    queue = pending.get(key)
    future = asyncio.get_running_loop().create_future()
    request = {"supervisor": supervisor, "node": node, "assignment": assignment, "activation": activation, "prepared": prepared, "future": future}
    if queue is None:
        queue = pending[key] = []
        # Collect siblings made ready by the same graph transition without
        # holding control permits or checkout leases while they prepare.
        asyncio.get_running_loop().call_later(.01, lambda: asyncio.create_task(_drain(tree, key)))
    queue.append(request)
    return await asyncio.shield(future)


async def _drain(tree: Any, key: Any) -> None:
    requests = tree.native_batches.pop(key, [])
    if not requests:
        return
    limit = min(max(1, requests[0]["prepared"]["run"]["definition"]["max_parallel"]), tree.worker_capacity or 64, getattr(requests[0]["prepared"]["native"], "max_batch_size", 1))
    for index in range(0, len(requests), limit):
        batch = requests[index:index + limit]
        try:
            results = await _execute(batch)
        except BaseException as exc:
            # Never leave a queued dispatcher waiting forever after a failed
            # controller. _execute persists launched uncertainty before raising.
            for request in batch:
                if not request["future"].done():
                    request["future"].set_exception(exc)
            continue
        for request, result in zip(batch, results, strict=True):
            if not request["future"].done():
                request["future"].set_result(result)


async def _execute(requests: list[dict[str, Any]]) -> list[tuple[bool, dict[str, Any] | None]]:
    from . import backends, events, store
    from .tasks import SessionBusyError, SessionUnknownError, RepoUnavailableError
    from .workflows import CheckoutLease, DispatchNotStarted, update_task_state
    from .workflow_invocation import tree_write_strength
    from .workflow_native import _prepare_native, _fallback, presentation, merge_denials
    from .workflow_context import account_legacy_prompt
    from .workflow_prompt_delivery import delivery_metadata

    first = requests[0]
    supervisor, tree = first["supervisor"], first["supervisor"].tree
    continue_running = lambda: all(r["supervisor"].tree.tree_running(r["supervisor"].run()) for r in requests)
    control_held = False
    acquired = 0
    uncertain = False
    transport_id = uuid.uuid4().hex
    observers = getattr(supervisor.registry, "_workflow_native_observers", None)
    if observers is None:
        observers = supervisor.registry._workflow_native_observers = {}
    children: list[dict[str, Any]] = []
    logs: list[Any] = []
    reserved = False
    launched = False
    owner_run = first["prepared"]["owner_run"]
    try:
        await tree.acquire_slot(continue_running, control=True)
        control_held = True
        # Re-read the owning session after preceding batches/control turns have
        # settled. Never resume the stale task captured while queueing.
        valid = []
        fallbacks = {}
        for index, request in enumerate(requests):
            prepared = await _prepare_native(request["supervisor"], request["node"], request["assignment"], request["activation"])
            if isinstance(prepared, str):
                fallbacks[index] = _fallback(request["supervisor"], request["activation"], prepared)
            else:
                request["prepared"] = prepared
                valid.append((index, request))
        if not valid:
            return [fallbacks[index] for index in range(len(requests))]
        prepared = valid[0][1]["prepared"]
        run, owner_run, owner, record, native = (prepared[k] for k in ("run", "owner_run", "owner", "record", "native"))
        entries = []
        for index, request in valid:
            await tree.acquire_slot(continue_running)
            acquired += 1
            settings = request["prepared"]["settings"]
            execution_id, nonce = uuid.uuid4().hex, uuid.uuid4().hex
            entries.append({"assignment": request["assignment"], "nonce": nonce, "settings": settings})
            child = {"task_id": execution_id, "execution_kind": "native_subagent", "status": "reserved", "dispatch_stage": "preparing", "candidate": copy.deepcopy(request["prepared"]["candidate"]), "freedom": settings["freedom"], "network": settings["network"], "repo_path": run["repo_path"], "session_mode": "fresh", "reserved_at": time.time(), "dispatch_nonce": nonce, "owner_task_id": record.task_id, "owner_session_id": record.session_id, "owner_session_generation": owner.get("task_id"), "transport_task_id": transport_id, "assignment_sha256": hashlib.sha256(request["assignment"].encode()).hexdigest(), "assignment_prompt": request["activation"].get("assignment_prompt", request["assignment"]), "activity_level": native.activity_level, "can_cancel_child": False, "can_resume_child": False, "harness_metadata": {"requested": copy.deepcopy(request["prepared"]["candidate"]), "effective": {**settings, "backend": record.backend, "settings_source": "certified_native_adapter"}, "observed": None, "verification_status": "configured_not_observed"}}
            state = {"assignment": request["assignment"], "owner_session_id": record.session_id, "expected_repo": record.repo_path, "expected_model": settings.get("model"), "expected_freedom": settings["freedom"], "expected_network": settings["network"], "expected_reasoning_effort": settings.get("reasoning_effort"), "owner_freedom": record.freedom, "native_settings": settings}
            children.append({"index": index, "request": request, "child": child, "state": state, "nonce": nonce})
        root_id = (run.get("parent_link") or {}).get("root_workflow_run_id", run["workflow_run_id"])
        lease_run = run if root_id == run["workflow_run_id"] else supervisor.store.get_run(root_id)
        async with CheckoutLease(supervisor.store, run["repo_path"], tree_write_strength(lease_run, record.freedom), continue_running, pool=supervisor.checkout_leases):
            def reserve(r: dict[str, Any]) -> None:
                for item in children:
                    a = next(a for a in r["activations"] if a["id"] == item["request"]["activation"]["id"])
                    a["tasks"].append(copy.deepcopy(item["child"]))
                    a.update(presentation(item["child"]))
                    a.pop("execution_fallback_reason", None)
                r["activations"].append({"id": transport_id, "node_id": "orchestrator", "role": "native_control", "status": "running", "created_at": time.time(), "tasks": [{"task_id": transport_id, "status": "reserved", "dispatch_stage": "preparing", "candidate": {"backend": record.backend, "model": record.model, "reasoning_effort": record.reasoning_effort}, "freedom": record.freedom, "network": record.network, "repo_path": run["repo_path"], "native_owner_execution_ids": [i["child"]["task_id"] for i in children]}]})
            supervisor.update(reserve, "native_batch_reserved", {"transport_task_id": transport_id, "count": len(children)})
            for item in children:
                item["request"]["activation"].pop("execution_fallback_reason", None)
            reserved = True
            for item in children:
                item["state"]["batch_entries"] = entries
                log = events.EventLog(events.events_path(supervisor.registry._log_dir, item["child"]["task_id"]), item["child"]["task_id"])
                item["log"] = log
                logs.append(log)

            def publish(item: dict[str, Any], updates: list[dict[str, Any]]) -> None:
                state, child = item["state"], item["child"]
                activation_id, execution_id = item["request"]["activation"]["id"], child["task_id"]
                for raw in updates:
                    update = dict(raw)
                    kind = update.pop("native_update")
                    if kind == "started":
                        def started(r: dict[str, Any]) -> None:
                            if any(t.get("native_child_id") == update["native_child_id"] and t.get("owner_session_id") == record.session_id and t.get("task_id") != execution_id for a in r["activations"] for t in a.get("tasks", [])):
                                state["invalid"] = "Native child identity was reused across executions"
                                raise ValueError(state["invalid"])
                            a = next(a for a in r["activations"] if a["id"] == activation_id)
                            attempt = next(t for t in a["tasks"] if t["task_id"] == execution_id)
                            update_task_state(a, attempt, {"status": "running", "dispatch_stage": "child_confirmed", "native_child_id": update["native_child_id"], "started_at": time.time()})
                            a["native_child_id"] = update["native_child_id"]
                        supervisor.update(started, "native_child_started")
                    elif kind == "settled":
                        result = {"task_id": execution_id, "status": update["status"], "summary": update.get("summary", ""), "execution_kind": "native_subagent", "native_child_id": state.get("native_child_id"), "parent_task_id": transport_id, "observed_model": update.get("observed_model")}
                        if update["status"] != "completed":
                            result.update(execution_failure=update.get("summary", "Native child failed"), blocked_failure=True, optional_failure_eligible=False)
                        if update.get("permission_denials"):
                            result["permission_denials"] = copy.deepcopy(update["permission_denials"])
                        state["result"] = result
                        observed = {**copy.deepcopy(update.get("observed_metadata", {})), "model": update.get("observed_model"), "native_child_id": state.get("native_child_id")}
                        supervisor._task_update(activation_id, execution_id, {"status": update["status"], "dispatch_stage": "child_settled", "native_terminal": True, "result": result, "finished_at": time.time(), "harness_metadata": {**child["harness_metadata"], "observed": observed, "verification_status": "observed_native_child"}})
                    elif kind == "permissions" and "result" in state:
                        state["result"]["permission_denials"] = merge_denials(state["result"].get("permission_denials", []), update["permission_denials"])
                        supervisor._task_update(activation_id, execution_id, {"result": state["result"]})
                    elif kind == "activity":
                        item["log"].write(update.pop("event_kind"), update)

            def observe(event: dict[str, Any]) -> None:
                for item in children:
                    publish(item, native.observe(event, item["nonce"], item["state"]))

            observers[transport_id] = observe
            for item in children:
                supervisor._task_update(item["request"]["activation"]["id"], item["child"]["task_id"], {"dispatch_stage": "launch_requested", "launch_requested_at": time.time()})
            supervisor._task_update(transport_id, transport_id, {"dispatch_stage": "spawn_requested"})
            prompt = native.prompt_batch(entries)
            accounting = account_legacy_prompt(prompt, role="native_control", checkpoint=transport_id, session_mode="resume", classification="native_control")
            accounting["compatibility_reasons"] = ["native batch retains full assignments"]
            supervisor._task_update(transport_id, transport_id, {"context_delivery": delivery_metadata({}, accounting)})
            parent = supervisor.registry.get(record.task_id)
            resume = supervisor.registry.resume if parent is not None else supervisor.registry.resume_record
            task = await resume(parent if parent is not None else record, prompt, task_id=transport_id, display_prompt=f"Native subagent batch: {len(children)} nodes", native_subagent=True, native_settings={**prepared["settings"], "batch_size": len(children)}, max_turns=record.max_turns)
            launched = True
            supervisor._task_update(transport_id, transport_id, {"status": "running", "dispatch_stage": "spawn_confirmed"})
            deadlines = [time.monotonic() + i["request"]["node"]["timeout_seconds"] for i in children if i["request"]["node"].get("timeout_seconds")]
            deadline = min(deadlines, default=None)
            cancelled = False
            while not task.done.is_set():
                if not cancelled and (supervisor.run()["status"] == "cancelling" or deadline is not None and time.monotonic() >= deadline):
                    cancelled = True
                    await supervisor.registry.cancel_cascade(transport_id, workflow_control=True)
                try:
                    await asyncio.wait_for(task.done.wait(), .25)
                except TimeoutError:
                    pass
            snapshot = task.snapshot()
            supervisor._task_update(transport_id, transport_id, {"status": snapshot["status"], "result": snapshot, "finished_at": time.time()})
            supervisor.update(lambda r: next(a for a in r["activations"] if a["id"] == transport_id).update(status=snapshot["status"]), "native_control_settled")
            if snapshot["status"] == "completed" and not getattr(supervisor.registry, "_workflow_native_failures", {}).get(transport_id):
                finalize = getattr(native, "finalize", None)
                if finalize is not None:
                    for item in children:
                        publish(item, await asyncio.to_thread(finalize, item["nonce"], item["state"]))
            results = dict(fallbacks)
            for item in children:
                state = item["state"]
                if snapshot["status"] != "completed" or "result" not in state or state.get("invalid") or getattr(supervisor.registry, "_workflow_native_failures", {}).get(transport_id):
                    uncertain = True
                    supervisor._task_update(item["request"]["activation"]["id"], item["child"]["task_id"], {"status": "uncertain", "error": "Native batch child or owning turn did not conclusively settle"})
                    results[item["index"]] = (True, None)
                else:
                    results[item["index"]] = (True, state["result"])
            if uncertain:
                supervisor.attention("Native batch requires reconciliation; no headless duplicate dispatched")
                # A batch is one control turn. Do not advance a sibling while
                # another child in that turn has ambiguous ownership.
                return [(True, None) for _ in requests]
            supervisor.store.update_run(owner_run["workflow_run_id"], lambda r: r["sessions"].__setitem__("orchestrator", {"candidate": owner["candidate"], "task_id": transport_id, "session_id": record.session_id}), "native_owner_session_recorded")
            return [results[index] for index in range(len(requests))]
    except DispatchNotStarted:
        return [(True, None) for _ in requests]
    except BaseException as exc:
        no_spawn = isinstance(exc, (SessionBusyError, SessionUnknownError, RepoUnavailableError, backends.NestedDispatchRefused, backends.UnsupportedCapability)) or getattr(exc, "polybridge_not_started", False) is True
        uncertain = reserved and (launched or not no_spawn)
        if reserved:
            for item in children:
                supervisor._task_update(item["request"]["activation"]["id"], item["child"]["task_id"], {"status": "uncertain" if uncertain else "not_started", "dispatch_stage": "uncertain" if uncertain else "not_started", "error": str(exc)})
            supervisor._task_update(transport_id, transport_id, {"status": "uncertain" if uncertain else "not_started", "error": str(exc)})
        supervisor.attention(f"Native batch requires reconciliation: {exc}" if uncertain else f"Native batch did not start: {exc}")
        if isinstance(exc, asyncio.CancelledError):
            raise
        return [(True, None) for _ in requests]
    finally:
        observers.pop(transport_id, None)
        for log in logs:
            log.close()
        for item in children:
            if acquired:
                if uncertain:
                    tree.hold_slot(supervisor.run_id, item["child"]["task_id"])
                else:
                    tree.release_slot()
                acquired -= 1
        while acquired:
            tree.release_slot()
            acquired -= 1
        if control_held:
            if uncertain:
                tree.hold_slot(supervisor.run_id, transport_id, control=True)
            else:
                tree.release_slot(control=True)
