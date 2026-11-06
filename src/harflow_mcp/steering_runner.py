"""Pinned one-turn builder execution and durable final classification."""
from __future__ import annotations

import asyncio
import copy
import json
import os
import time
import uuid
from pathlib import Path

from .steering_output import classify_final
from .steering_schema import captured_failure
from .workflow_steering import (SteeringError, _episode, _history, _source_check, target, pending_turn, _digest)

async def _wait_observed(supervisor, task):
    """Preserve ordinary real cancellation without treating steering as cancel."""
    cancellation_requested = False
    while not task.done.is_set():
        if supervisor.run(refresh_atomic=True)["status"] == "cancelling" and not cancellation_requested:
            cancellation_requested = True
            await asyncio.wait_for(supervisor.registry.cancel_cascade(task.task_id, workflow_control=True), timeout=20)
        try:
            await asyncio.wait_for(task.done.wait(), timeout=0.25)
        except TimeoutError:
            pass


def _settle(supervisor, turn_id, snapshot=None, *, failure=None, unknown=False, error_message=None):
    """Persist final/evidence BEFORE validation in one independent durable mutation."""
    def capture(run):
        turn = next(t for t in run["steering"]["turns"] if t["turn_id"] == turn_id)
        ep = _episode(run, turn)
        if turn["state"] in {"captured", "settled"}:
            return
        turn["state"] = "unknown" if unknown else "captured"
        turn["outcome"] = failure
        if snapshot is None and not unknown:
            turn["not_started"] = True
        if error_message:
            turn["dispatch_error"] = str(error_message)[:2000]
        if snapshot is not None:
            turn["observation"] = {key: copy.deepcopy(snapshot.get(key)) for key in ("status", "exit_code", "output_drained", "timed_out", "outcome_unknown", "is_error", "execution_failure", "failure_diagnostic", "session_id", "usage", "num_turns", "total_cost_usd")}
            turn["observation"]["task_id"] = snapshot.get("task_id")
            for flag in ("output_drained", "timed_out", "outcome_unknown", "is_error"):
                turn["observation"][flag] = snapshot.get(flag, False)
            turn["usage"] = {"provenance": "observed" if snapshot.get("usage") is not None else "unavailable", "usage": snapshot.get("usage"), "cost_usd": snapshot.get("total_cost_usd"), "num_turns": snapshot.get("num_turns"), "reserved_harness_turns": turn["harness_turn_cap"]}
            raw = snapshot.get("summary")
            try:
                raw_size = len(raw.encode("utf-8", errors="strict")) if isinstance(raw, str) else None
            except UnicodeError:
                raw_size = None
                turn["outcome"] = "ambiguous_output"
            if raw_size is not None and raw_size <= 64 * 1024:
                turn["final"] = raw
            elif raw is not None:
                turn["outcome"] = turn.get("outcome") or "output_too_large"
                turn["final_loss"] = {"original_bytes": raw_size, "retained": False}
        ep["state"] = "delivery_unknown" if unknown else "classifying"
        if ep.get("dismiss_requested") and not unknown:
            ep.update(state="dismissed", hold=False)
        _history(run, "final_captured", turn_id=turn_id, episode_id=ep["episode_id"], text=turn.get("final"), outcome=turn.get("outcome"), observation=turn.get("observation"))
        ep["latest_reply_ref"] = "history:" + str(run["steering"]["sequence"])
        if run["status"] in {"cancelling", "cancelled"}:
            if not unknown:
                run["status"] = "cancelled"
                ep.update(state="target_terminal", hold=False)
                turn["outcome"] = "cancelled"
        else:
            run.update(status="paused", attention_reason="Conversation is waiting for caller; use steering capability")
        run.update(supervisor_pid=None, supervisor_identity=None)
        for a in run["activations"]:
            if a.get("steering_turn_id") == turn_id:
                a["status"] = "uncertain" if unknown else "completed" if not failure else "failed"
                a["tasks"][0].update(status="uncertain" if unknown else (snapshot or {}).get("status", "not_started"), result={**{k: copy.deepcopy(v) for k, v in turn.get("observation", {}).items() if v is not None}, "task_id": turn["task_id"], "steering_reply_ref": ep["latest_reply_ref"]})
    supervisor.update(capture, "steering_reply_captured")


def _classify_commit(supervisor, turn_id):
    def commit(run):
        turn = next(t for t in run["steering"]["turns"] if t["turn_id"] == turn_id)
        ep = _episode(run, turn)
        if turn["state"] != "captured":
            return
        failure = turn.get("outcome") or captured_failure(turn.get("observation"), turn["task_id"])
        classification = {"kind": "refused", "code": failure} if failure else classify_final(turn.get("final"))
        if run["status"] in {"cancelled", "cancelling"}:
            classification = {"kind": "refused", "code": "workflow_terminal"}
        elif time.time() - ep["created_at"] > 1800:
            classification = {"kind": "refused", "code": "request_expired"}
            if ep["hold"]:
                ep["state"] = "expired"
        code = classification.get("code")
        if classification["kind"] == "command":
            wrapper = classification["command"]
            expected = {"episode_id": ep["episode_id"], "turn_id": turn["turn_id"], "commit_target_ref": turn["commit_target_ref"],
                        "steering_revision": turn["steering_revision"], "interaction_epoch": turn["commit_target"]["interaction_epoch"],
                        "checkpoint_revision": turn["commit_target"]["checkpoint_revision"], "goal_revision": turn["commit_target"]["goal_revision"], "role": "builder"}
            if set(wrapper) != set(expected) | {"command"} or any(type(wrapper.get(k)) is not type(v) or wrapper.get(k) != v for k, v in expected.items()):
                code = "role_command_invalid"
            elif run["interaction_epoch"] != expected["interaction_epoch"] or ep["steering_revision"] != expected["steering_revision"]:
                code = "interaction_revoked"
            elif turn.get("witness_stale") or target(run) != turn["commit_target"]:
                code = "steering_target_stale"
            else:
                command = wrapper["command"]
                if type(command) is not dict or set(command) != {"definition", "expected_draft_revision"} or type(command["expected_draft_revision"]) is not int or command["expected_draft_revision"] != run.get("draft_revision", 0):
                    code = "role_command_invalid"
                else:
                    try:
                        _source_check(supervisor.store, run)
                        from .workflows import validate_definition
                        definition = validate_definition({**command["definition"], "name": run["name"], "routing_mode": "explicit"})
                        definition["draft"] = True
                        definition["revision"] = run.get("source_revision", 0)
                        definition.pop("updated_at", None)
                        run.update(status="completed", generated_definition=definition, builder_draft=definition, draft_revision=run.get("draft_revision", 0) + 1)
                        run.pop("attention_reason", None)
                        ep.update(state="committed", hold=False)
                        run["steering"]["commits"].append({"turn_id": turn_id, "episode_id": ep["episode_id"], "commit_target_ref": turn["commit_target_ref"], "definition_digest": _digest(definition)})
                    except (ValueError, TypeError, KeyError) as exc:
                        code = exc.code if isinstance(exc, SteeringError) else "role_command_invalid"
        if classification["kind"] == "conversation" and ep["hold"]:
            ep["state"] = "awaiting_caller"
        if code:
            ep["state"] = "command_refused" if ep["hold"] and ep["state"] != "expired" else ep["state"]
        turn.update(state="settled", outcome=code or ("committed" if ep["state"] == "committed" else "conversation"))
        _history(run, "classified", turn_id=turn_id, episode_id=ep["episode_id"], outcome=turn["outcome"])
    supervisor.update(commit, "steering_classified")


async def _observe_boundary(supervisor, issuance):
    """A restarted supervisor observes the exact previously issued ordinary task."""
    active = [t for a in supervisor.run()["activations"] for t in a["tasks"]
              if t["task_id"] == issuance["boundary_task_id"] and t["status"] in {"reserved", "running", "uncertain"}]
    if not active:
        return
    try:
        boundary_task = supervisor.registry.attach(issuance["boundary_task_id"])
        if boundary_task is None or boundary_task.task_id != issuance["boundary_task_id"]:
            return
        await _wait_observed(supervisor, boundary_task)
        snapshot = boundary_task.snapshot()
    except asyncio.CancelledError:
        raise
    except Exception:
        snapshot = None
    qualified = bool(snapshot and snapshot.get("status") in {"completed", "failed", "cancelled"}
                     and type(snapshot.get("exit_code")) is int and snapshot.get("output_drained") is True
                     and not snapshot.get("outcome_unknown"))
    def observed(run):
        turn = next(t for t in run["steering"]["turns"] if t["turn_id"] == issuance["turn_id"])
        if turn["state"] != "queued":
            return
        ep = _episode(run, turn)
        if not qualified:
            if ep["state"] != "delivery_unknown":
                ep.update(state="delivery_unknown", reason="Boundary capture or settlement is unavailable")
                _history(run, "boundary_unknown", turn_id=turn["turn_id"], task_id=issuance["boundary_task_id"])
            return
        for activation in run["activations"]:
            attempt = next((t for t in activation["tasks"] if t["task_id"] == issuance["boundary_task_id"]), None)
            if attempt is not None:
                attempt.update(status=snapshot["status"], result=copy.deepcopy(snapshot))
                activation.update(status="completed" if snapshot["status"] == "completed" else "failed", steering_superseded=True)
                candidate = attempt.get("candidate", turn["candidate"])
                from .workflows import _candidate_key
                run["sessions"]["builder"] = {"candidate": "builder:" + _candidate_key(candidate),
                                              "task_id": issuance["boundary_task_id"], "session_id": snapshot.get("session_id")}
                _history(run, "boundary_settlement_observed", turn_id=turn["turn_id"], task_id=issuance["boundary_task_id"])
                break
    supervisor.update(observed, "steering_boundary_settlement_observed")


async def _run_builder_turn_owned(supervisor, run_id):
    """Pinned candidate execution, no fallback, repair, feedback pump or implicit turns."""
    from . import identity
    from .tasks import get_backend, SessionUnknownError
    from .workflows import CheckoutLease, _candidate_key
    supervisor.run_id = run_id
    run = supervisor.run(refresh_atomic=True)
    issuance = pending_turn(run)
    if issuance is None:
        # A crash after capture is classified from persisted bytes, never redispatched.
        captured = next((t for t in run.get("steering", {}).get("turns", []) if t["state"] == "captured"), None)
        if captured:
            _classify_commit(supervisor, captured["turn_id"])
        return
    tid = issuance["turn_id"]
    task = None
    if issuance["state"] == "queued":
        await _observe_boundary(supervisor, issuance)
        def boundary(run):
            turn = next(t for t in run["steering"]["turns"] if t["turn_id"] == tid)
            ep = _episode(run, turn)
            if any(t["status"] in {"reserved", "running", "uncertain"} for a in run["activations"] for t in a["tasks"]):
                return
            if time.time() - ep["created_at"] > 1800 and not ep.get("dismiss_requested"):
                turn.update(state="settled", outcome="request_expired")
                ep.update(state="expired")
                run.update(status="paused", attention_reason="Steering episode expired; explicitly dismiss or retarget")
                return
            if run["status"] in {"cancelled", "cancelling"}:
                turn.update(state="settled", outcome="workflow_terminal")
                ep.update(state="target_terminal", hold=False)
            elif ep.get("dismiss_requested"):
                turn.update(state="settled", outcome="dismissed")
                ep.update(state="dismissed", hold=False)
                run.update(status="paused")
            elif target(run) != turn["commit_target"] or not ep["hold"]:
                turn.update(state="settled", outcome="steering_target_stale")
                ep.update(state="stale")
                run.update(status="paused", attention_reason="Steering target changed at the builder boundary; explicitly retarget")
            else:
                observed = run.get("sessions", {}).get("builder")
                if observed and observed.get("task_id") == turn["boundary_task_id"]:
                    turn["previous_session"] = copy.deepcopy(observed)
                turn["state"] = "reserved"
                ep["state"] = "turn_reserved"
                run["status"] = "starting"
            _history(run, "boundary_observed", turn_id=tid, state=turn["state"])
        supervisor.update(boundary, "steering_boundary_observed")
        issuance = next(t for t in supervisor.run()["steering"]["turns"] if t["turn_id"] == tid)
        if issuance["state"] != "reserved":
            return
    if issuance["state"] != "reserved":
        try:
            task = supervisor.registry.attach(issuance["task_id"])
        except (ValueError, OSError):
            task = None
        if task is None:
            _settle(supervisor, tid, failure="steering_delivery_unknown", unknown=True)
            return
    else:
        def reserve(run):
            turn = next(t for t in run["steering"]["turns"] if t["turn_id"] == tid)
            if turn["state"] != "reserved":
                raise SteeringError("steering_busy", "Issuance already claimed")
            ep = _episode(run, turn)
            if time.time() - ep["created_at"] > 1800 and not ep.get("dismiss_requested"):
                turn.update(state="settled", outcome="request_expired")
                ep.update(state="expired")
                run.update(status="paused", attention_reason="Steering episode expired; explicitly dismiss or retarget")
                return
            if run["status"] in {"cancelled", "cancelling"}:
                turn.update(state="settled", outcome="workflow_terminal")
                ep.update(state="target_terminal", hold=False)
                return
            if ep.get("dismiss_requested"):
                turn.update(state="settled", outcome="dismissed")
                ep.update(state="dismissed", hold=False)
                run.update(status="paused")
                return
            if not ep["hold"] or target(run) != turn["commit_target"]:
                turn.update(state="settled", outcome="steering_target_stale")
                ep.update(state="stale")
                run.update(status="paused")
                return
            turn["state"] = "spawn_intent"
            ep["state"] = "discussing"
            run.update(status="running", supervisor_pid=os.getpid(), supervisor_identity=identity.own_identity())
            run["activations"].append({"id": turn["turn_id"], "steering_turn_id": tid, "node_id": "builder", "role": "builder", "status": "running", "tasks": [{"task_id": turn["task_id"], "status": "reserved", "candidate": turn["candidate"]}], "created_at": time.time(), "token": None})
            _history(run, "spawn_intent", turn_id=tid, task_id=turn["task_id"])
        supervisor.update(reserve, "steering_spawn_intent")
        current = supervisor.run()
        issuance = next(t for t in current["steering"]["turns"] if t["turn_id"] == tid)
        if issuance["state"] != "spawn_intent":
            return
        wrapper = {"episode_id": issuance["episode_id"], "turn_id": tid, "commit_target_ref": issuance["commit_target_ref"],
                   "steering_revision": issuance["steering_revision"], "interaction_epoch": issuance["commit_target"]["interaction_epoch"],
                   "checkpoint_revision": issuance["commit_target"]["checkpoint_revision"], "goal_revision": issuance["commit_target"]["goal_revision"], "role": "builder",
                   "command": {"definition": "OBJECT", "expected_draft_revision": current.get("draft_revision", 0)}}
        prompt = ("You are the read-only workflow builder in a single explicitly requested conversational turn. Do not dispatch agents, write files, save workflows or answer approval questions. Ordinary prose is persisted and waits for the caller. To propose a complete valid unsaved workflow, return ONLY HARFLOW_COMMIT_V1 on its own line, this strict JSON wrapper, then END_HARFLOW_COMMIT_V1 on its own line. Replace definition with the complete object. No automatic correction turn follows refusal.\n" + json.dumps(wrapper) +
                  "\nThe apply_workflow_draft preview tool remains available. If used, copy the latest returned commit_target_ref and commit_target checkpoint_revision plus expected draft revision into your final wrapper. Preview updates never constitute a final proposal.\nCurrent authoritative draft:\n" + json.dumps(current.get("builder_draft", {})) + "\nExact caller request:\n" + issuance["text"])
        candidate = issuance["candidate"]
        try:
            backend = get_backend(candidate["backend"])
            if not backend.capabilities.supports_turn_cap:
                raise SteeringError("steering_budget_unsupported", "Selected adapter no longer supports finite turns")
            previous = issuance.get("previous_session")
            from .task_supervision import read_task_record
            record = read_task_record(supervisor.registry.log_dir, previous["task_id"]) if previous else None
            compatible = bool(previous and record and previous.get("candidate") == "builder:" + _candidate_key(candidate)
                              and record.backend == candidate["backend"] and record.repo_path == current["repo_path"]
                              and record.freedom == "read_only" and record.session_id == previous.get("session_id")
                              and record.model == candidate.get("model") and record.reasoning_effort == candidate.get("reasoning_effort")
                              and record.network == current.get("network") and record.workflow_builder)
            if compatible:
                try:
                    from .conversation_workflows import commit_profile_options
                    profile_options=commit_profile_options(supervisor.run(refresh_atomic=True), issuance)
                    from .conversation_workflows import bind_commit_input
                    bind_commit_input(supervisor,issuance,prompt,profile_options,issuance["harness_turn_cap"])
                    task = await supervisor.registry.resume_record(record, prompt, **profile_options, max_turns=issuance["harness_turn_cap"], task_id=issuance["task_id"], display_prompt=issuance["text"], workflow_builder=True, require_unchanged_session=True, network=current.get("network"))
                except SessionUnknownError:
                    if not issuance["allow_fresh_context"]:
                        raise
                    task = None
            if task is None:
                if not issuance["allow_fresh_context"]:
                    raise SteeringError("steering_source_unavailable", "Retained session cannot be verified; Fresh requires explicit consent")
                task = await supervisor.registry.start(prompt, Path(current["repo_path"]), backend=candidate["backend"], freedom="read_only", network=current.get("network"), model=candidate.get("model"), reasoning_effort=candidate.get("reasoning_effort"), max_turns=issuance["harness_turn_cap"], caller_approvals=False, workflow_builder=True, task_id=issuance["task_id"], display_prompt=issuance["text"], title=current["name"] + " · builder conversation")
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            positive = isinstance(exc, (SteeringError, SessionUnknownError)) or getattr(exc, "harflow_not_started", False) is True
            _settle(supervisor, tid, failure=getattr(exc, "code", "provider_not_started") if positive else "steering_delivery_unknown", unknown=not positive, error_message=str(exc))
            if positive:
                _classify_commit(supervisor, tid)
            return
        def launched(run):
            turn = next(t for t in run["steering"]["turns"] if t["turn_id"] == tid)
            if turn["state"] not in {"spawn_intent", "running"}:
                return
            turn["state"] = "running"
            next(a for a in run["activations"] if a.get("steering_turn_id") == tid)["tasks"][0]["status"] = "running"
            _history(run, "spawn_observed", turn_id=tid, task_id=task.task_id)
        supervisor.update(launched, "steering_spawn_observed")
    # Task.done belongs exclusively to the normal monitor; never synthesize settlement.
    try:
        await _wait_observed(supervisor, task)
        snapshot = task.snapshot()
    except asyncio.CancelledError:
        raise
    except Exception:
        _settle(supervisor, tid, failure="capture_unknown", unknown=True)
        return
    observed = {**snapshot, **{flag: snapshot.get(flag, False) for flag in ("output_drained", "timed_out", "outcome_unknown", "is_error")}}
    failure = captured_failure(observed, issuance["task_id"])
    from .conversation_workflows import commit_context_mismatch
    if commit_context_mismatch(supervisor.run(refresh_atomic=True),issuance,snapshot):
        failure='steering_source_unavailable'
    if snapshot.get("task_id") != issuance["task_id"] or failure == "capture_unknown":
        _settle(supervisor, tid, snapshot, failure="capture_unknown", unknown=True)
        return
    _settle(supervisor, tid, snapshot, failure=failure)
    def session(run):
        run["sessions"]["builder"] = {"candidate": "builder:" + _candidate_key(issuance["candidate"]), "task_id": task.task_id, "session_id": snapshot.get("session_id")}
    supervisor.update(session, "steering_session_observed")
    _classify_commit(supervisor, tid)



async def run_builder_turn(supervisor, run_id):
    """Hold the existing checkout lease through observed process exit and capture.

    Uncertain durable task associations continue to block incompatible checkouts
    through CheckoutLease's existing orphan-owner checks after this owner exits.
    """
    from .workflows import CheckoutLease
    from .workflow_invocation import tree_write_strength
    supervisor.run_id = run_id
    run = supervisor.run(refresh_atomic=True)
    if pending_turn(run) is None:
        return await _run_builder_turn_owned(supervisor, run_id)
    async with CheckoutLease(supervisor.store, run["repo_path"], tree_write_strength(run, "read_only"),
                             lambda: True, pool=supervisor.checkout_leases,
                             owned_run_ids={run_id}):
        return await _run_builder_turn_owned(supervisor, run_id)
