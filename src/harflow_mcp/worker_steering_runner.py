"""One explicitly authorized, finite Fresh worker response; never a retry loop."""
from __future__ import annotations

import asyncio
import copy
import json
import time
from pathlib import Path

from .workflow_steering import SteeringError, _episode, _history, pending_turn
from .worker_steering import _parts, current_target, commit_worker_result


def _turn(supervisor, turn_id):
    return next(t for t in supervisor.run(refresh_atomic=True)["steering"]["turns"] if t["turn_id"] == turn_id)


def _capture(supervisor, turn_id, snapshot=None, failure=None, unknown=False, error_message=None):
    from .orchestrator_steering import capture
    turn = _turn(supervisor, turn_id)
    if unknown and turn["state"] == "unknown" and turn.get("outcome") == failure:
        prior = turn.get("observation")
        if snapshot is None and prior is None or snapshot is not None and prior is not None and all(snapshot.get(k) == v for k, v in prior.items()):
            return
    diagnostic = error_message
    if snapshot is None and not unknown:
        permitted = {"steering_budget_unsupported", "steering_source_unavailable", "provider_not_started", "workflow_terminal", "request_expired", "cancelled", "steering_target_stale", "dismissed"}
        if failure not in permitted:
            diagnostic = (str(failure) + (": " + diagnostic if diagnostic else ""))[:2000]
            failure = "request_expired" if failure == "interaction_revoked" else "provider_not_started"
    target = turn["target"]
    def update(run):
        a = next((a for a in run["activations"] if a["id"] == target["activation_id"]), None)
        if a is None:
            return
        attempt = next((t for t in a["tasks"] if t.get("steering_turn_id") == turn_id), None)
        if attempt is not None:
            attempt.update(status="uncertain" if unknown else snapshot["status"] if snapshot else "not_started")
            if snapshot is not None:
                attempt["result"] = copy.deepcopy(snapshot)
            elif not unknown:
                q = next((q for q in a.get("questions", []) if q.get("reply_task_id") == turn["task_id"] and q.get("answer_delivery_state") == "reserved"), None)
                if q:
                    q["answer_delivery_state"] = "pending"
                    q.pop("reply_task_id", None)
    with supervisor.store.tree_mutation(supervisor.run_id):
        if not (snapshot is None and not unknown and turn["state"] in {"queued", "reserved"}):
            supervisor.store.update_run(target["workflow_run_id"], update, "worker_steering_task_captured")
        capture(supervisor, turn_id, snapshot, failure=failure, unknown=unknown)
        if diagnostic:
            supervisor.update(lambda r: next(t for t in r["steering"]["turns"] if t["turn_id"] == turn_id).update(dispatch_error=str(diagnostic)[:2000]), "worker_steering_dispatch_error")


async def _observe_boundary(supervisor, turn):
    from .steering_runner import _wait_observed
    for boundary in turn.get("boundary_tasks", []):
        child = supervisor.store.get_run(boundary["workflow_run_id"], refresh_atomic=True)
        a = next(a for a in child["activations"] if a["id"] == boundary["activation_id"])
        attempt = next(t for t in a["tasks"] if t["task_id"] == boundary["task_id"])
        snapshot = attempt.get("result")
        if attempt["status"] in {"reserved", "running", "uncertain"}:
            task = supervisor.registry.attach(boundary["task_id"])
            if task is None or task.task_id != boundary["task_id"]:
                return False
            deadline = attempt.get("timeout_deadline")
            while not task.done.is_set():
                if supervisor.run(refresh_atomic=True).get("status") == "cancelling" or deadline and time.time() >= deadline:
                    await asyncio.wait_for(supervisor.registry.cancel_cascade(task.task_id, workflow_control=True), timeout=20)
                    break
                try:
                    await asyncio.wait_for(task.done.wait(), timeout=.25)
                except TimeoutError:
                    pass
            await _wait_observed(supervisor, task)
            snapshot = task.snapshot()
        if not snapshot or snapshot.get("task_id") != boundary["task_id"] or type(snapshot.get("exit_code")) is not int or snapshot.get("output_drained") is not True or snapshot.get("outcome_unknown") or snapshot.get("status") not in {"completed", "failed", "cancelled"}:
            return False
        def observed(run):
            activation = next(a for a in run["activations"] if a["id"] == boundary["activation_id"])
            next(t for t in activation["tasks"] if t["task_id"] == boundary["task_id"]).update(status=snapshot["status"], result=copy.deepcopy(snapshot))
            activation.update(worker_result_required=True, steering_superseded=True)
        supervisor.store.update_run(boundary["workflow_run_id"], observed, "worker_steering_boundary_observed")
        # A restarted ordinary dispatcher no longer owns its finally block.
        # Release only this positively settled execution's existing resource proof
        # through the normal coordinator before requesting a new response's slot.
        from .workflow_resources import reconcile
        reconcile(supervisor.store, supervisor.store.get_run(boundary["workflow_run_id"], refresh_atomic=True), owner_dead=False)
    return True


async def _run_owned(supervisor, run_id):
    from .tasks import get_backend
    from .steering_runner import _wait_observed
    from .steering_schema import captured_failure
    supervisor.run_id = run_id
    root = supervisor.run(refresh_atomic=True)
    turn = pending_turn(root)
    if turn is None:
        captured = next((t for t in root["steering"]["turns"] if t["state"] == "captured" and t["target"]["role"] == "worker"), None)
        if captured:
            commit_worker_result(supervisor, captured["turn_id"])
        return
    if turn["target"]["role"] != "worker":
        return
    if turn["state"] == "unknown" and turn.get("outcome") == "steering_effect_recovery_unknown":
        # Retry only the exact durable effect/delivery, never provider attachment or issuance.
        commit_worker_result(supervisor, turn["turn_id"])
        return
    tid = turn["turn_id"]
    if turn["state"] == "queued":
        if not await _observe_boundary(supervisor, turn):
            _capture(supervisor, tid, failure="steering_delivery_unknown", unknown=True)
            return
        supervisor.update(lambda r: next(t for t in r["steering"]["turns"] if t["turn_id"] == tid).update(state="reserved"), "worker_steering_boundary_ready")
        turn = _turn(supervisor, tid)
    task = None
    if turn["state"] != "reserved":
        task = supervisor.registry.attach(turn["task_id"])
        if task is None or task.task_id != turn["task_id"]:
            _capture(supervisor, tid, failure="steering_delivery_unknown", unknown=True)
            return
    else:
        with supervisor.store.tree_mutation(run_id):
            root = supervisor.run(refresh_atomic=True)
            turn = _turn(supervisor, tid)
            ep = _episode(root, turn)
            if not ep["hold"] or ep.get("dismiss_requested") or root.get("status") in {"cancelled", "cancelling"}:
                _capture(supervisor, tid, failure="interaction_revoked")
                commit_worker_result(supervisor, tid)
                return
            try:
                current = current_target(supervisor.store, root, turn["commit_target"])
            except SteeringError as exc:
                _capture(supervisor, tid, failure=exc.code)
                commit_worker_result(supervisor, tid)
                return
            if current != turn["commit_target"]:
                _capture(supervisor, tid, failure="steering_target_stale")
                commit_worker_result(supervisor, tid)
                return
            run, _, activation, node, token = _parts(supervisor.store, root, turn["commit_target"])
            candidate = turn["candidate"]
            backend = get_backend(candidate["backend"])
            if not turn["allow_fresh_context"] or not backend.capabilities.supports_turn_cap:
                _capture(supervisor, tid, failure="steering_budget_unsupported")
                commit_worker_result(supervisor, tid)
                return
            def reserve(r):
                t = next(t for t in r["steering"]["turns"] if t["turn_id"] == tid)
                if t["state"] != "reserved":
                    raise SteeringError("steering_busy", "Worker response already issued")
                t.update(state="spawn_intent")
                if node.get("timeout_seconds"):
                    t["response_deadline"] = time.time() + node["timeout_seconds"]
                _episode(r, t)["state"] = "discussing"
                _history(r, "spawn_intent", turn_id=tid, task_id=t["task_id"])
            supervisor.update(reserve, "worker_steering_spawn_intent")
            def associate(r):
                a = next(a for a in r["activations"] if a["id"] == activation["id"])
                if not any(t["task_id"] == turn["task_id"] for t in a["tasks"]):
                    a["tasks"].append({"task_id": turn["task_id"], "status": "reserved", "candidate": copy.deepcopy(candidate), "steering_turn_id": tid})
                qid = a.get("worker_steering_question_id")
                question = next((q for q in a.get("questions", []) if q["question_id"] == qid), None)
                if question and question.get("answer_delivery_state") == "pending":
                    question.update(answer_delivery_state="reserved", reply_task_id=turn["task_id"])

            supervisor.store.update_run(run["workflow_run_id"], associate, "worker_steering_task_reserved")
        # No locks span resource acquisition, providers or observation.
        from .workflow_delegation import worker_prompt
        prompt_token = token or {**activation.get("token", {}), "assignment_prompt": activation["assignment_prompt"], "assigned_task_ids": activation.get("assigned_task_ids", [])}
        prompt = worker_prompt(run, node, prompt_token, root=supervisor.store.root)
        wrapper = {"episode_id": turn["episode_id"], "turn_id": tid, "commit_target_ref": turn["commit_target_ref"], "steering_revision": turn["steering_revision"], "interaction_epoch": turn["commit_target"]["interaction_epoch"], "checkpoint_revision": turn["commit_target"]["checkpoint_revision"], "goal_revision": turn["commit_target"]["goal_revision"], "role": "worker", "command": {"status": "succeeded|failed|blocked|asking", "result": {}, "evidence": []}}
        question = next((q for q in activation.get("questions", []) if q["question_id"] == activation.get("worker_steering_question_id") and q.get("status") == "answered" and q.get("answer_delivery_state") == "pending"), None)
        if question:
            prompt += "\nExact answer to your previous question (question identity preserved):\n" + json.dumps({k: question[k] for k in ("question_id", "question", "answer")})
        prompt += "\nThis is one explicitly requested worker steering response. Configured tools have real effects under the existing permissions. Ordinary prose waits for the caller. To submit the original role result return ONLY HARFLOW_COMMIT_V1 on its own line, the exact JSON wrapper below with command replaced by the complete worker result, then END_HARFLOW_COMMIT_V1 on its own line. No automatic repair or additional response follows.\n" + json.dumps(wrapper) + "\nExact caller request:\n" + turn["text"]
        kwargs = {k: copy.deepcopy(candidate[k]) for k in ("git_https_rewrite", "group_report_output") if k in candidate}
        original = next(t for t in activation["tasks"] if t["task_id"] == turn["target"]["task_id"])
        from .task_supervision import read_task_record
        source = read_task_record(supervisor.registry.log_dir, turn["target"]["task_id"])
        if source is None or source.backend != candidate["backend"] or source.model != candidate.get("model") or source.reasoning_effort != candidate.get("reasoning_effort") or source.repo_path != original.get("repo_path", run["repo_path"]):
            _capture(supervisor, tid, failure="steering_source_unavailable")
            commit_worker_result(supervisor, tid)
            return
        kwargs.update({k: copy.deepcopy(getattr(source, k)) for k in ("model", "reasoning_effort", "background_idle_seconds", "caller_approvals", "caller_write_approvals", "allowed_commands", "observe_repos", "freedom", "network", "interaction_owner")})
        kwargs.update(max_turns=min(turn["harness_turn_cap"], source.max_turns or turn["harness_turn_cap"]), task_id=turn["task_id"], display_prompt=turn["text"], title=run["name"] + " · worker conversation")
        if source.worktree:
            kwargs["worktree_inherit"] = copy.deepcopy(source.worktree)
        if source.group:
            kwargs["group"] = source.group
        kwargs["scratch_chain_id"] = source.scratch_chain_id or source.task_id
        if source.extra_writable_paths:
            from .extra_paths import restore_paths, revalidate_paths
            grants = restore_paths(source.extra_writable_paths)
            revalidate_paths(grants)
            kwargs["extra_writable_paths"] = [g.path for g in grants]
        start_called = False
        try:
            names = node.get("resources")
            if names:
                from .workflow_resources import scope_for_run
                from .task_resources import ledger_for
                scope = scope_for_run(supervisor.store, run)
                if scope is not None:
                    ledger_for(supervisor.registry.log_dir).check_borrow(names, scope)
                    kwargs["resource_inherit"] = {"names": names, "borrowed_from": scope.reservation_id}
                else:
                    kwargs["resources"] = names
            start_called = True
            task = await supervisor.registry.start(prompt, Path(source.repo_path), backend=backend, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            positive = not start_called or getattr(exc, "harflow_not_started", False) is True
            _capture(supervisor, tid, failure=getattr(exc, "code", "provider_not_started") if positive else "steering_delivery_unknown", unknown=not positive, error_message=str(exc))
            if positive:
                commit_worker_result(supervisor, tid)
            return
        supervisor.update(lambda r: next(t for t in r["steering"]["turns"] if t["turn_id"] == tid).update(state="running"), "worker_steering_spawn_observed")
    try:
        return await _observe_response(supervisor, tid, task, node if "node" in locals() else None)
    except asyncio.CancelledError:
        raise
    except Exception:
        _capture(supervisor, tid, failure="capture_unknown", unknown=True)
        return


async def _observe_response(supervisor, tid, task, node):
    from .steering_runner import _wait_observed
    # Deadline is persisted before issuance and is never replenished on attachment.
    turn = _turn(supervisor, tid)
    deadline = turn.get("response_deadline")
    while not task.done.is_set():
        if supervisor.run(refresh_atomic=True).get("status") == "cancelling" or deadline and time.time() >= deadline:
            await asyncio.wait_for(supervisor.registry.cancel_cascade(task.task_id, workflow_control=True), timeout=20)
            break
        try:
            await asyncio.wait_for(task.done.wait(), timeout=.25)
        except TimeoutError:
            pass
    await _wait_observed(supervisor, task)
    return _finish_response(supervisor, tid, task.snapshot())


def _finish_response(supervisor, tid, snapshot):
    from .steering_schema import captured_failure
    # Match the normal monitor's optional negative flags; drain remains positive evidence.
    snapshot = {**snapshot, **{flag: snapshot.get(flag, False) for flag in ("output_drained", "timed_out", "outcome_unknown", "is_error")}}
    turn = _turn(supervisor, tid)
    if snapshot.get("task_id") != turn["task_id"]:
        _capture(supervisor, tid, failure="capture_unknown", unknown=True)
        return
    failure = captured_failure(snapshot, turn["task_id"])
    _capture(supervisor, tid, snapshot, failure=failure, unknown=failure == "capture_unknown")
    if failure == "capture_unknown":
        return
    target = turn["target"]
    def observed(r):
        a = next(a for a in r["activations"] if a["id"] == target["activation_id"])
        t = next(t for t in a["tasks"] if t["task_id"] == turn["task_id"])
        t.update(status=snapshot["status"], result=copy.deepcopy(snapshot))
        q = next((q for q in a.get("questions", []) if q.get("reply_task_id") == turn["task_id"] and q.get("answer_delivery_state") == "reserved"), None)
        if q and captured_failure(snapshot, turn["task_id"]) is None:
            q.update(answer_delivery_state="delivered", delivered_at=time.time())
    supervisor.store.update_run(target["workflow_run_id"], observed, "worker_steering_task_observed")
    commit_worker_result(supervisor, tid)
    from .steering_lifecycle import maintain
    maintain(supervisor.store, supervisor.run_id)


async def run_worker_turn(supervisor, run_id):
    from .workflows import CheckoutLease
    from .workflow_invocation import tree_write_strength
    supervisor.run_id = run_id
    root = supervisor.run(refresh_atomic=True)
    async with CheckoutLease(supervisor.store, root["repo_path"], tree_write_strength(root, root.get("freedom", "write_in_repo")), lambda: True, pool=supervisor.checkout_leases, owned_run_ids={run_id}):
        await _run_owned(supervisor, run_id)
