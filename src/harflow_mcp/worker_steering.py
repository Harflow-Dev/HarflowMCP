"""Exact worker activation authority for explicitly requested steering responses.

All mutations here require the caller's existing root tree mutation lock.
"""
from __future__ import annotations

import copy
import json
import time

from .workflow_steering import SteeringError, _digest

ACTIVE = {"reserved", "running", "uncertain"}
TERMINAL = {"completed", "failed", "cancelled", "cancelling"}


def _parts(storage, root, witness):
    from .orchestrator_steering import records
    tree = records(storage, root)
    entry = tree.get(witness["workflow_run_id"])
    if entry is None:
        raise SteeringError("steering_target_stale", "Worker is outside the qualified invocation tree")
    run, path = entry
    if run.get("execution_contract") != "delegation":
        raise SteeringError("steering_role_unsupported", "Worker steering requires the delegation result contract")
    activation = next((a for a in run["activations"] if a["id"] == witness["activation_id"]), None)
    if activation is None or activation.get("role") != "node" or activation.get("invocation"):
        raise SteeringError("steering_role_unsupported", "Target must be an issued agent worker activation")
    node = activation.get("on_demand_node") or next((n for n in run["definition"]["nodes"] if n["id"] == activation["node_id"]), None)
    token = next((t for t in run["pending"] if t["id"] == witness.get("token_id")), None)
    if node is None or node.get("type", "agent") != "agent":
        raise SteeringError("steering_role_unsupported", "Worker node is unavailable")
    if not activation.get("on_demand_node") and (token is None or token.get("execution_activation_id") != activation["id"]):
        raise SteeringError("steering_target_stale", "Worker assignment no longer owns this token")
    if activation.get("on_demand_node"):
        call = next((c for c in run.get("on_demand_calls", []) if c["id"] == activation.get("on_demand_call_id")), None)
        if not call or call.get("execution_id") != activation["id"] or call.get("assignment") != activation.get("assignment_prompt") or call.get("node_id") != activation["node_id"]:
            raise SteeringError("steering_source_unavailable", "Specialist activation has no exact original call")
        origin = tree.get(call.get("checkpoint", {}).get("workflow_run_id"))
        if origin is None:
            raise SteeringError("steering_source_unavailable", "Specialist origin checkpoint is outside this tree")
        from .workflow_on_demand import checkpoint_record
        try:
            checkpoint = checkpoint_record(origin[0], call)
            if call.get("status") in ACTIVE and (checkpoint.get("pending_on_demand_call_id") != call["id"] or call["id"] not in checkpoint.get("on_demand_applied_call_ids", [])):
                raise ValueError("Specialist is not the exact pending origin call")
        except (ValueError, KeyError, TypeError):
            raise SteeringError("steering_target_stale", "Specialist original checkpoint changed") from None
    return run, path, activation, node, token


def assignment(activation, node, token):
    return {"assignment_prompt": activation.get("assignment_prompt"), "assigned_task_ids": activation.get("assigned_task_ids", []),
            "token": activation.get("token"), "node": node, "input_result_refs": activation.get("input_result_refs", []),
            "on_demand_call_id": activation.get("on_demand_call_id"), "worker_question": {k: q.get(k) for q in activation.get("questions", []) if q["question_id"] == activation.get("worker_steering_question_id") for k in ("question_id", "question", "status", "answer")}, "current_assignment": {k: (token or {}).get(k) for k in ("assignment_prompt", "assigned_task_ids", "execution_activation_id")}}


def current_target(storage, root, witness):
    run, path, a, node, token = _parts(storage, root, witness)
    if root.get("status") in TERMINAL or run.get("status") in TERMINAL or a.get("node_result") or token and token.get("execution_complete"):
        raise SteeringError("steering_target_stale", "Worker result was already accepted")
    tasks = [t for t in a.get("tasks", []) if not t.get("steering_turn_id")]
    if not tasks or not a.get("assignment_prompt"):
        raise SteeringError("steering_source_unavailable", "Worker lacks an issued immutable assignment")
    latest = tasks[-1]
    bound_assignment = assignment(a, node, token)
    bound_assignment["goal_authority"] = [{"workflow_run_id": authority["workflow_run_id"], "goal_revision": authority.get("goal_revision", 0), "goal": authority.get("goal"), "parent_goal": authority.get("parent_goal")} for authority in [root if rid == root["workflow_run_id"] else storage.get_run(rid, refresh_atomic=True) for rid in path]]
    bound_assignment["root_interaction"] = {k: root.get(k) for k in ("input_question", "pending_inputs", "orchestrator_usage_recovery", "worker_usage_recovery")}
    bound_assignment["root_suspension"] = {"status": root.get("status"), "attention_reason": root.get("attention_reason")} if root.get("status") not in {"running", "starting", "paused"} else None
    bound_assignment["suspension"] = {"status": run.get("status"), "attention_reason": run.get("attention_reason")} if run.get("status") not in {"running", "starting", "paused"} else None
    if a.get("on_demand_call_id"):
        call = next(c for c in run["on_demand_calls"] if c["id"] == a["on_demand_call_id"])
        bound_assignment["specialist_origin"] = {k: call.get(k) for k in ("id", "execution_id", "checkpoint", "origin", "assignment", "source_workflow_run_id", "local_node_id", "orchestration_activation_id")}
        from .workflow_on_demand import checkpoint_record
        origin = root if call["checkpoint"]["workflow_run_id"] == root["workflow_run_id"] else storage.get_run(call["checkpoint"]["workflow_run_id"], refresh_atomic=True)
        checkpoint = checkpoint_record(origin, call)
        bound_assignment["specialist_checkpoint"] = {k: checkpoint.get(k) for k in ("decision_id", "decision_activation_id", "question_id", "status", "question", "answer", "answer_delivery_state", "pending_on_demand_call_id", "on_demand_count", "on_demand_applied_call_ids")}
    return {"root_workflow_run_id": root["workflow_run_id"], "workflow_run_id": run["workflow_run_id"], "path": path,
            "role": "worker", "node_id": a["node_id"], "token_id": token["id"] if token else None,
            "activation_id": a["id"], "task_id": latest["task_id"], "decision_id": a.get("on_demand_call_id"),
            "checkpoint_revision": run["steering"]["checkpoint_revision"], "goal_revision": run.get("goal_revision", 0),
            "definition_digest": _digest(run["definition"])[7:], "assignment_revision": 0,
            "assignment_digest": _digest(bound_assignment), "draft_revision": None, "source_revision": None,
            "source_workflow_id": None, "source_definition_digest": None, "preview_hash": None,
            "interaction_epoch": root["interaction_epoch"], "control_revision": root["steering"]["control_revision"]}


def targets(storage, root):
    from .orchestrator_steering import records
    result = []
    for run, _ in records(storage, root).values():
        if run.get("execution_contract") != "delegation":
            continue
        for a in run.get("activations", []):
            if a.get("role") != "node" or a.get("invocation"):
                continue
            token = next((t for t in run.get("pending", []) if t.get("execution_activation_id") == a["id"]), None)
            try:
                result.append(current_target(storage, root, {"workflow_run_id": run["workflow_run_id"], "activation_id": a["id"], "token_id": token["id"] if token else None}))
            except SteeringError:
                pass
    return result


def validate_target(value, root_id):
    from .steering_schema import TARGET_KEYS, _require, _id, _counter, _digest as digest
    _require(type(value) is dict and set(value) == TARGET_KEYS, "worker target fields")
    _require(value["role"] == "worker" and value["root_workflow_run_id"] == root_id, "worker role/root")
    path = value["path"]
    _require(type(path) is list and 1 <= len(path) <= 64 and path[0] == root_id and path[-1] == value["workflow_run_id"] and len(set(path)) == len(path), "worker path")
    for key in ("workflow_run_id", "activation_id", "task_id"):
        _id(value[key], key)
    if value["decision_id"] is not None:
        _require(type(value["node_id"]) is str and value["node_id"].count(":") == 1, "specialist qualified node")
        source_id, local_id = value["node_id"].split(":")
        _id(source_id, "specialist source run")
        _id(local_id, "specialist local node")
    else:
        _id(value["node_id"], "worker node")
    for item in path:
        _id(item, "worker path")
    for key in ("token_id", "decision_id"):
        if value[key] is not None:
            _id(value[key], key)
    _require((value["token_id"] is None) != (value["decision_id"] is None), "worker connected token or specialist call")
    for key in ("checkpoint_revision", "goal_revision", "assignment_revision", "interaction_epoch", "control_revision"):
        _counter(value[key], key)
    digest(value["assignment_digest"], "worker assignment")
    digest("sha256:" + value["definition_digest"], "worker definition")
    _require(all(value[k] is None for k in ("draft_revision", "source_revision", "source_workflow_id", "source_definition_digest", "preview_hash")), "worker inapplicable fields")


def validate_turn(turn, run):
    validate_target(turn["target"], run["workflow_run_id"])
    validate_target(turn["commit_target"], run["workflow_run_id"])
    from .steering_schema import _timestamp, _require, _id
    if "response_deadline" in turn:
        _timestamp(turn["response_deadline"], "worker response deadline")
    boundary = turn.get("boundary_tasks")
    _require(type(boundary) is list and len(boundary) <= 64, "worker boundary tasks")
    for item in boundary:
        _require(type(item) is dict and set(item) == {"workflow_run_id", "activation_id", "task_id"}, "worker boundary identity")
        for key in item:
            _id(item[key], "worker boundary " + key)
        _require(item["workflow_run_id"] == turn["target"]["workflow_run_id"] and item["activation_id"] == turn["target"]["activation_id"], "worker boundary scope")
    _require(turn["boundary_task_id"] == (boundary[-1]["task_id"] if boundary else None), "worker primary boundary")
    if turn.get("previous_session") is not None or turn.get("allow_fresh_context") is not True:
        raise ValueError("Worker steering requires explicitly consented finite Fresh response")


def admission(storage, root, request):
    witness = request["target"]
    if witness != current_target(storage, root, witness):
        raise SteeringError("steering_target_stale", "Worker assignment, goal or ownership changed")
    run, _, a, node, token = _parts(storage, root, witness)
    if root.get("status") in TERMINAL:
        raise SteeringError("workflow_terminal", "Terminal workflow cannot be reopened")
    pending_question = a.get("pending_question_id") or a.get("resume_question_id") or (token or {}).get("question_id")
    if pending_question:
        q = next((q for q in a.get("questions", []) if q["question_id"] == pending_question), None)
        if not q or q["question_id"] != a.get("worker_steering_question_id") or q.get("status") != "answered" or q.get("answer_delivery_state") != "pending" or not isinstance(q.get("answer"), str) or not q["answer"].strip():
            raise SteeringError("steering_question_pending", "Resolve the exact worker question; an uncertain answer is never redelivered")
    if run.get("input_question") or run.get("pending_inputs"):
        raise SteeringError("steering_question_pending", "Resolve the actual worker question or pending answer first")
    tasks = [t for t in a["tasks"] if not t.get("steering_turn_id")]
    latest = tasks[-1]
    if latest.get("execution_kind") == "native_subagent":
        raise SteeringError("steering_budget_unsupported", "Native worker lacks qualified finite per-response cap")
    if not request.get("allow_fresh_context"):
        raise SteeringError("steering_budget_unsupported", "Resume per-response scope is unqualified; explicitly consent to Fresh context")
    candidate = copy.deepcopy(latest["candidate"])
    from .tasks import get_backend
    if not get_backend(candidate["backend"]).capabilities.supports_turn_cap:
        raise SteeringError("steering_budget_unsupported", "Worker adapter cannot enforce a finite Fresh response cap")
    boundary = []
    for task in tasks:
        if task["status"] == "uncertain":
            raise SteeringError("steering_delivery_unknown", "Existing worker generation requires positive settlement")
        if task["status"] in ACTIVE:
            boundary.append({"workflow_run_id": run["workflow_run_id"], "activation_id": a["id"], "task_id": task["task_id"]})
    return candidate, None, boundary, run["workflow_run_id"]


def claim(storage, root, turn):
    """Admission winner permanently prevents ordinary redispatch/reconnection."""
    witness = turn["commit_target"]
    def apply(run):
        a = next(a for a in run["activations"] if a["id"] == witness["activation_id"])
        a["worker_result_required"] = True
        a["worker_steering_assignment_digest"] = witness["assignment_digest"]
        a["steering_superseded"] = True
    if witness["workflow_run_id"] == root["workflow_run_id"]:
        apply(root)
    else:
        storage.update_run(witness["workflow_run_id"], apply, "worker_steering_claimed")


def ordinary_blocked(activation):
    return bool(activation and activation.get("worker_result_required"))


def _receipt_parts(storage, root, witness):
    """Read an applied-effect claim without requiring its old pending checkpoint."""
    from .orchestrator_steering import records
    tree = records(storage, root)
    run, path = tree[witness["workflow_run_id"]]
    activation = next(a for a in run["activations"] if a["id"] == witness["activation_id"])
    if activation.get("role") != "node" or activation.get("node_id") != witness["node_id"]:
        raise SteeringError("steering_source_unavailable", "Worker effect claim target contradicts its activation")
    node = activation.get("on_demand_node") or next(n for n in run["definition"]["nodes"] if n["id"] == activation["node_id"])
    token = next((t for t in run["pending"] if t["id"] == witness.get("token_id")), None)
    return run, path, activation, node, token


def worker_effect_proven(run, activation, turn, command=None, *, require_delivery=False):
    """An idempotency digest is a claim; captured bytes and the actual effect prove it."""
    from .steering_output import classify_final
    from .steering_schema import captured_failure
    from .workflow_delegation import normalize_result
    try:
        if captured_failure(turn.get("observation"), turn["task_id"]) is not None:
            return False
        parsed = classify_final(turn.get("final"))
        if parsed["kind"] != "command":
            return False
        wrapper = parsed["command"]
        expected = {k: turn[k] for k in ("episode_id", "turn_id", "commit_target_ref", "steering_revision")}
        expected.update(role="worker", **{k: turn["commit_target"][k] for k in ("interaction_epoch", "checkpoint_revision", "goal_revision")})
        if set(wrapper) != set(expected) | {"command"} or any(type(wrapper.get(k)) is not type(v) or wrapper[k] != v for k, v in expected.items()):
            return False
        captured = wrapper["command"]
        if command is not None and command != captured:
            return False
        command = captured
        if type(command) is not dict or set(command) != {"status", "result", "evidence"}:
            return False
        if (run["workflow_run_id"] != turn["target"]["workflow_run_id"] or activation["id"] != turn["target"]["activation_id"]
                or activation.get("node_id") != turn["target"]["node_id"]
                or activation.get("worker_steering_receipts", {}).get(turn["turn_id"]) != _digest(command)):
            return False
        node = activation.get("on_demand_node") or next(n for n in run["definition"]["nodes"] if n["id"] == activation["node_id"])
        value = normalize_result(node, {"summary": json.dumps(command)}, activation.get("assigned_task_ids", []), guided=run.get("runner_policy") == "guided")
        if value["status"] == "asking":
            return any(q.get("origin_task_id") == turn["task_id"] and q.get("progress") == value["result"]
                       and q.get("evidence") == value["evidence"] and q.get("question") == value["result"]["question"]
                       and q.get("raw_output") == turn["final"] and q.get("origin_candidate") == turn["candidate"]
                       for q in activation.get("questions", []))
        if activation.get("node_result") != value or activation.get("raw_output") != turn["final"]:
            return False
        if activation.get("on_demand_call_id"):
            call = next((c for c in run.get("on_demand_calls", []) if c["id"] == activation["on_demand_call_id"]), None)
            return bool(call and call["id"] == turn["target"]["decision_id"] and call.get("execution_id") == activation["id"]
                        and call.get("result") == value and call.get("task_id") == turn["task_id"] and call.get("dispatch_state") == "settled"
                        and call.get("status") == ("succeeded" if value["status"] == "succeeded" else "failed")
                        and (not require_delivery or call.get("result_delivered") is True))
        if activation.get("result") != value or activation.get("status") != ("completed" if value["status"] == "succeeded" else "failed"):
            return False
        checkpoints = list(run.get("pending", [])) + [a.get("token") or {} for a in run.get("activations", [])]
        return any((t.get("execution_activation_id") == activation["id"] and t.get("execution_complete") is True and t.get("result") == value)
                   or (activation["id"] in t.get("input_result_refs", []) + t.get("structural_completion_refs", []) and t.get("context") == value)
                   for t in checkpoints)
    except (ValueError, TypeError, KeyError, StopIteration):
        return False


def commit_worker_result(supervisor, turn_id):
    """Validate a whole framed result, then commit once into its original activation."""
    from .steering_output import classify_final
    from .steering_schema import captured_failure
    from .workflow_delegation import normalize_result, finish_result
    from .workflow_steering import _episode, _history
    root_id = supervisor.run_id
    with supervisor.store.tree_mutation(root_id):
        root = supervisor.run(refresh_atomic=True)
        turn = next(t for t in root["steering"]["turns"] if t["turn_id"] == turn_id)
        if turn["state"] != "captured" and not (turn["state"] == "unknown" and turn.get("outcome") == "steering_effect_recovery_unknown"):
            return
        ep = _episode(root, turn)
        code = (turn.get("outcome") if turn.get("outcome") != "steering_effect_recovery_unknown" else None) or captured_failure(turn.get("observation"), turn["task_id"])
        classification = {"kind": "refused", "code": code} if code else classify_final(turn.get("final"))
        code = classification.get("code")
        if classification["kind"] == "command":
            wrapper = classification["command"]
            expected = {"episode_id": ep["episode_id"], "turn_id": turn_id, "commit_target_ref": turn["commit_target_ref"],
                        "steering_revision": turn["steering_revision"], "interaction_epoch": turn["commit_target"]["interaction_epoch"],
                        "checkpoint_revision": turn["commit_target"]["checkpoint_revision"], "goal_revision": turn["commit_target"]["goal_revision"], "role": "worker"}
            effect_claimed = False
            try:
                if set(wrapper) != set(expected) | {"command"} or any(type(wrapper.get(k)) is not type(v) or wrapper.get(k) != v for k, v in expected.items()):
                    raise SteeringError("role_command_invalid", "Worker commit wrapper does not match exact issuance")
                effect_claimed = True  # Unavailable source cannot prove absence of a prior effect.
                run, _, a, node, token = _receipt_parts(supervisor.store, root, turn["commit_target"])
                prior_receipt = a.get("worker_steering_receipts", {}).get(turn_id)
                effect_claimed = prior_receipt is not None
                if prior_receipt is None:
                    run, _, a, node, token = _parts(supervisor.store, root, turn["commit_target"])
                if prior_receipt is None and (not ep["hold"] or ep.get("dismiss_requested") or root["interaction_epoch"] != expected["interaction_epoch"] or ep["steering_revision"] != turn["steering_revision"]):
                    raise SteeringError("interaction_revoked", "Worker conversation authority changed")
                if prior_receipt is None and current_target(supervisor.store, root, turn["commit_target"]) != turn["commit_target"]:
                    raise SteeringError("steering_target_stale", "Worker assignment changed before result acceptance")
                command = wrapper["command"]
                if type(command) is not dict or set(command) != {"status", "result", "evidence"}:
                    raise SteeringError("role_command_invalid", "Worker result requires exact status/result/evidence fields")
                value = normalize_result(node, {"summary": json.dumps(command)}, a.get("assigned_task_ids", []), guided=run.get("runner_policy") == "guided")
                if effect_claimed and not worker_effect_proven(run, a, turn, command):
                    raise SteeringError("steering_source_unavailable", "Stored worker receipt lacks its exact captured result effect")
                # Persist result bytes only through the original role validator. No repair loop.
                def accept(r):
                    current = next(x for x in r["activations"] if x["id"] == a["id"])
                    receipts = current.setdefault("worker_steering_receipts", {})
                    if turn_id in receipts:
                        if receipts[turn_id] != _digest(command) or not worker_effect_proven(r, current, turn, command):
                            raise SteeringError("steering_source_unavailable", "Worker result receipt contradicts its captured effect")
                        return
                    if not current.get("worker_result_required"):
                        raise SteeringError("steering_target_stale", "Worker activation no longer requires this result")
                    if value["status"] == "asking":
                        if token is None:
                            raise SteeringError("steering_role_unsupported", "Specialist asking steering requires explicit on-demand question support")
                        from .workflow_clarification import record_question
                        class AtomicSupervisor:
                            def run(self):
                                return r
                            def update(self, mutate, event, detail=None):
                                mutate(r)
                            def attention(self, message):
                                raise SteeringError("role_command_invalid", message)
                        question_id = record_question(AtomicSupervisor(), node, token, a["id"], value, {"summary": turn["final"], "task_id": turn["task_id"]})
                        if question_id is None:
                            raise SteeringError("role_command_invalid", "Worker question was not accepted")
                        current["worker_steering_question_id"] = question_id
                    elif token is not None:
                        current.pop("worker_result_required", None)
                        finish_result(r, node, token["id"], a["id"], value, {"summary": turn["final"], "task_id": turn["task_id"]})
                    else:
                        call = next(c for c in r.get("on_demand_calls", []) if c["id"] == a["on_demand_call_id"] and c["execution_id"] == a["id"])
                        if call.get("status") not in {"reserved", "running", "uncertain"}:
                            raise SteeringError("steering_target_stale", "Specialist call already settled")
                        status = "succeeded" if value["status"] == "succeeded" else "failed"
                        call.update(status=status, finished_at=time.time(), result=copy.deepcopy(value), failure=None if status == "succeeded" else str(value["result"]), task_id=turn["task_id"], dispatch_state="settled")
                        current.update(status="completed" if status == "succeeded" else "failed", node_result=copy.deepcopy(value), raw_output=turn["final"])
                        current.pop("worker_result_required", None)
                    receipts[turn_id] = _digest(command)
                if value["status"] == "asking" and not effect_claimed:
                    if token is None:
                        raise SteeringError("steering_role_unsupported", "Specialist asking steering requires explicit on-demand question support")
                    if len(a.get("questions", [])) >= node.get("max_context_questions", 10):
                        raise SteeringError("role_command_invalid", "Worker context question allowance is exhausted; submit an explicit terminal result")
                # Once a durable effect write is attempted, an exception is ambiguous
                # until its receipt/effect can be positively read on the next observation.
                effect_claimed = True
                if run["workflow_run_id"] == root_id:
                    # Save root effect and receipt together, never from a detached copy.
                    supervisor.update(accept, "worker_steering_result_accepted")
                else:
                    supervisor.store.update_run(run["workflow_run_id"], accept, "worker_steering_result_accepted")
                effect_claimed = True
                if a.get("on_demand_call_id"):
                    from .workflow_on_demand import reflect_result
                    owner = supervisor.store.get_run(run["workflow_run_id"], refresh_atomic=True)
                    call = next(c for c in owner["on_demand_calls"] if c["id"] == a["on_demand_call_id"])
                    supervisor.store.update_run(call["checkpoint"]["workflow_run_id"], lambda r: reflect_result(r, call), "on_demand_result_delivered", call["id"])
                    supervisor.store.update_run(run["workflow_run_id"], lambda r: next(c for c in r["on_demand_calls"] if c["id"] == call["id"]).update(result_delivered=True), "on_demand_result_acknowledged", call["id"])
            except (ValueError, TypeError, KeyError, StopIteration, OSError) as exc:
                code = getattr(exc, "code", "role_command_invalid")
                if effect_claimed:
                    # An applied/contradictory claim cannot become an ordinary no-effect refusal.
                    # Preserve it across stale or unavailable origin delivery for explicit repair.
                    def unknown(r):
                        t = next(t for t in r["steering"]["turns"] if t["turn_id"] == turn_id)
                        if t["state"] == "unknown" and t.get("outcome") == "steering_effect_recovery_unknown" and t.get("dispatch_error") == code:
                            return
                        t.update(state="unknown", outcome="steering_effect_recovery_unknown", dispatch_error=code)
                        _episode(r, t).update(state="delivery_unknown", hold=True)
                        from .orchestrator_steering import _park
                        _park(r, supervisor.store)
                        _history(r, "effect_recovery_unknown", turn_id=turn_id, outcome=code)
                    supervisor.update(unknown, "worker_steering_effect_recovery_unknown")
                    return
        # Delivery is authorized by this positively captured explicit response.
        # Clear pending IDs after result CAS, never before its checkpoint check.
        def clear_delivered(r):
            a = next((a for a in r["activations"] if a["id"] == turn["target"]["activation_id"]), None)
            if a is None:
                return
            q = next((q for q in a.get("questions", []) if q.get("reply_task_id") == turn["task_id"] and q.get("answer_delivery_state") == "delivered"), None)
            if q:
                for key in ("pending_question_id", "resume_question_id"):
                    if a.get(key) == q["question_id"]:
                        a.pop(key, None)
                token = next((t for t in r.get("pending", []) if t.get("execution_activation_id") == a["id"]), None)
                if token and token.get("question_id") == q["question_id"]:
                    token.pop("question_id", None)
        if captured_failure(turn.get("observation"), turn["task_id"]) is None:
            supervisor.store.update_run(turn["target"]["workflow_run_id"], clear_delivered, "worker_steering_answer_delivered")
        def settle(r):
            t = next(t for t in r["steering"]["turns"] if t["turn_id"] == turn_id)
            episode = _episode(r, t)
            committed = classification["kind"] == "command" and not code
            t.update(state="settled", outcome=code or ("committed" if committed else "conversation"))
            episode.update(state="committed" if committed else "command_refused" if code else "awaiting_caller", hold=False if committed else episode["hold"])
            from .orchestrator_steering import _park, finish_dismissal
            if episode.get("dismiss_requested") and not committed:
                finish_dismissal(supervisor.store, r, episode)
            if committed:
                if r.get("status") == "paused" and r.get("attention_reason") == "Conversation is waiting for caller; use steering capability" and r["interaction_epoch"] == t["commit_target"]["interaction_epoch"] and r["steering"]["control_revision"] == t["commit_target"]["control_revision"]:
                    r["status"] = "running"
                    r.pop("attention_reason", None)
            else:
                _park(r, supervisor.store)
            if committed:
                r["steering"]["commits"].append({"turn_id": turn_id, "episode_id": episode["episode_id"], "commit_target_ref": t["commit_target_ref"], "definition_digest": _digest(classification["command"]["command"])})
            _history(r, "classified", turn_id=turn_id, episode_id=episode["episode_id"], outcome=t["outcome"])
        supervisor.update(settle, "worker_steering_classified")
