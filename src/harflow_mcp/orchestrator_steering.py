"""Root-owned conversational orchestration and shared scheduling fences.

A held conversation is a scheduling dependency, never a pretend workflow pause.
All authority checks run under WorkflowStore's tree mutation lock.
"""
from __future__ import annotations

import copy
import time
import uuid

from .workflow_steering import SteeringError, _digest

NEW_EFFECTS = {"dispatch_reserved", "native_reserved", "native_batch_reserved", "invocation_reserved", "child_decision_reopened", "decision_attempt_reserved", "decision_accepted", "orchestrator_inspection", "on_demand_reserved", "on_demand_checkpoint_applied", "decision_checkpoint"}
DECISIONS = {"decision_attempt_reserved", "decision_accepted", "orchestrator_inspection", "on_demand_reserved", "on_demand_checkpoint_applied", "decision_checkpoint"}


def records(storage, root):
    """Only bounded reciprocal current invocation links establish a target path."""
    rid = root["workflow_run_id"]
    if root.get("orchestrator_session_owner_run_id", rid) != rid or root.get("orchestrator_config", root["definition"].get("orchestrator")) != root["definition"].get("orchestrator"):
        raise SteeringError("steering_source_unavailable", "Root orchestrator owner/configuration contradicts its admitted source")
    result = {rid: (root, [rid])}
    owners = {rid: rid}
    todo = [root]
    while todo:
        parent = todo.pop()
        for activation in parent.get("activations", []):
            invocation = activation.get("invocation")
            if not invocation:
                continue
            cid = invocation.get("child_workflow_run_id")
            if cid in result or len(result) >= 64:
                raise SteeringError("steering_source_unavailable", "Invocation links are cyclic or exceed the qualified bound")
            if type(cid) is not str or not cid:
                raise SteeringError("steering_source_unavailable", "Invocation child identity is missing")
            try:
                child = storage.get_run(cid, refresh_atomic=True)
            except (OSError, ValueError, KeyError) as exc:
                raise SteeringError("steering_source_unavailable", "Invocation child source is unavailable") from exc
            link = child.get("parent_link") or {}
            if (link.get("workflow_run_id") != parent["workflow_run_id"] or link.get("execution_id") != activation["id"]
                    or link.get("root_workflow_run_id") != rid or link.get("node_id") != activation["node_id"]
                    or link.get("depth") != len(result[parent["workflow_run_id"]][1]) + 1):
                raise SteeringError("steering_source_unavailable", "Invocation edge is not exactly reciprocal")
            if "steering" not in child:
                raise SteeringError("steering_source_unavailable", "Child lacks qualified shared scheduling fence")
            from .workflow_references import definition_sha256, workflow_identity, subtree
            node = next((n for n in parent["definition"]["nodes"] if n["id"] == activation["node_id"]), None)
            workflow_id = ((node or {}).get("workflow_ref") or {}).get("workflow_id")
            tree = parent.get("dependency_tree") or {}
            pinned = tree.get("workflows", {}).get(workflow_id)
            mode = (node or {}).get("orchestrator_mode", "child")
            if activation.get("role") != "node" or child.get("kind") != "workflow" or node is None or node.get("type") != "workflow" or not pinned or mode not in {"current", "child"}:
                raise SteeringError("steering_source_unavailable", "Invocation lacks its exact configured pinned workflow source")
            expected_hash = definition_sha256(pinned["definition"])
            expected_revision = pinned.get("revision")
            expected_edge = {"from": workflow_identity(parent["definition"]), "node_id": node["id"], "to": workflow_id, "orchestrator_mode": mode}
            if (pinned.get("definition_sha256") != expected_hash or workflow_identity(pinned["definition"]) != workflow_id
                    or pinned["definition"].get("revision", 0) != expected_revision
                    or expected_edge not in tree.get("edges", [])
                    or invocation.get("workflow_id") != workflow_id or invocation.get("revision") != expected_revision
                    or invocation.get("definition_sha256") != expected_hash or invocation.get("orchestrator_mode") != mode
                    or child.get("definition") != pinned["definition"] or child.get("revision") != expected_revision
                    or child.get("definition_hash") != expected_hash or child.get("workflow_id") != workflow_id
                    or child.get("orchestrator_mode") != mode or child.get("dependency_tree") != subtree(tree, workflow_id)):
                raise SteeringError("steering_source_unavailable", "Child definition, revision, invocation or pinned source contradicts the configured workflow")
            expected_owner = owners[parent["workflow_run_id"]] if mode == "current" else cid
            owner_run = child if expected_owner == cid else result[expected_owner][0]
            if (child.get("orchestrator_session_owner_run_id", cid) != expected_owner
                    or mode == "current" and child.get("orchestrator_config") != owner_run["definition"]["orchestrator"]
                    or mode == "child" and child.get("orchestrator_config", child["definition"]["orchestrator"]) != child["definition"]["orchestrator"]):
                raise SteeringError("steering_source_unavailable", "Child session owner/configuration is not the configured nearest owner")
            owners[cid] = expected_owner
            result[cid] = (child, result[parent["workflow_run_id"]][1] + [cid])
            todo.append(child)
    return result


def _semantic(run):
    value = {k: run.get(k) for k in ("pending", "joins", "tasks", "decisions", "goal_revision", "goal_assessment", "goal_replanning_required", "definition_hash", "input_question", "pending_inputs", "interaction_owner", "interaction_handover")}
    value["definition"] = run.get("definition")
    value["dependency_tree"] = run.get("dependency_tree")
    value["results"] = [{**{k: a.get(k) for k in ("id", "node_id", "node_result", "assignment_prompt", "assigned_task_ids")}, "invocation": {k: v for k, v in a.get("invocation", {}).items() if k not in {"stage", "started_at", "elapsed_seconds", "timeout_elapsed_seconds", "active_since", "finished_at"}}} for a in run.get("activations", []) if a.get("role") == "node"]
    return value


def target_for(storage, root, workflow_run_id, token_id):
    tree = records(storage, root)
    if workflow_run_id not in tree:
        raise SteeringError("steering_source_unavailable", "Requested run is not a qualified descendant")
    run, path = tree[workflow_run_id]
    token = next((t for t in run["pending"] if t["id"] == token_id), None)
    if token is None or not token.get("decision_id"):
        raise SteeringError("steering_target_stale", "No current orchestrator decision at this exact token")
    if run.get("execution_contract") != "delegation" or run.get("runner_policy") != "guided":
        raise SteeringError("steering_role_unsupported", "Orchestrator steering requires guided delegation")
    activation = next((a for a in run["activations"] if a["id"] == token.get("decision_activation_id")), None)
    return {"root_workflow_run_id": root["workflow_run_id"], "workflow_run_id": workflow_run_id,
            "path": path, "role": "orchestrator", "node_id": token["node_id"], "token_id": token_id,
            "activation_id": activation["id"] if activation else None,
            "task_id": activation["tasks"][-1]["task_id"] if activation and activation["tasks"] else None,
            "decision_id": token["decision_id"], "checkpoint_revision": run["steering"]["checkpoint_revision"],
            "goal_revision": run.get("goal_revision", 0), "definition_digest": _digest(run["definition"])[7:],
            "assignment_revision": None, "assignment_digest": _digest({k: _semantic(v[0]) for k, v in tree.items()}),
            "draft_revision": None, "source_revision": None, "source_workflow_id": None,
            "source_definition_digest": None, "preview_hash": None,
            "interaction_epoch": root["interaction_epoch"], "control_revision": root["steering"]["control_revision"]}


def targets(storage, root):
    result = []
    for run, _ in records(storage, root).values():
        if run.get("status") in {"completed", "failed", "cancelled", "cancelling"}:
            continue
        if run.get("execution_contract") != "delegation" or run.get("runner_policy") != "guided":
            continue
        for token in run["pending"]:
            if token.get("decision_id"):
                result.append(target_for(storage, root, run["workflow_run_id"], token["id"]))
    return result


def current_target(storage, root, witness):
    return target_for(storage, root, witness["workflow_run_id"], witness["token_id"])


def target_matches(storage, root, witness):
    try:
        return current_target(storage, root, witness) == witness
    except SteeringError:
        return False


def fence(storage, run, event, detail=None):
    """Shared linearization: called before mutations under the root tree lock."""
    if event not in NEW_EFFECTS:
        return
    rid = (run.get("parent_link") or {}).get("root_workflow_run_id") or run["workflow_run_id"]
    root = run if rid == run["workflow_run_id"] else storage.get_run(rid, refresh_atomic=True)
    permit = detail.get("steering_intent") if isinstance(detail, dict) else None
    if permit and event in {"on_demand_reserved", "on_demand_checkpoint_applied"}:
        issued = next((t for t in root.get("steering", {}).get("turns", []) if t["turn_id"] == permit.get("turn_id")), None)
        if issued and root["interaction_epoch"] == issued["commit_target"]["interaction_epoch"] and _episode(root, issued)["steering_revision"] == issued["steering_revision"] and issued.get("action_commit", {}).get("state") == "intent" and issued["action_commit"]["call_id"] == permit.get("call_id") == detail.get("call_id") and issued["action_commit"]["execution_id"] == permit.get("execution_id") and permit.get("root_id") == rid:
            return
    if event == "dispatch_reserved" and isinstance(detail, dict):
        # Only an accepted exact specialist reservation may pass the hold.
        for a in run.get("activations", []):
            cid = a.get("on_demand_call_id")
            if cid and any(t.get("action_commit", {}).get("call_id") == cid and t["action_commit"].get("state") == "intent" for t in root.get("steering", {}).get("turns", [])):
                call = next((c for c in run.get("on_demand_calls", []) if c["id"] == cid), None)
                if call and call["execution_id"] == a["id"] == detail.get("activation_id") and call["status"] == "running" and not a["tasks"]:
                    return
    for episode in root.get("steering", {}).get("episodes", []):
        if not episode["hold"]:
            continue
        target = episode["target"]
        if target["role"] != "orchestrator":
            continue
        path = target["path"]
        # Block new root decisions and the exact target subtree, allowing already
        # issued unrelated effects and their observation/recovery to settle.
        affected = run["workflow_run_id"] in path[-1:]
        if not affected and run.get("parent_link"):
            affected = target["workflow_run_id"] in records(storage, root)[run["workflow_run_id"]][1]
        issuance = next((t for t in root.get("steering", {}).get("turns", []) if t["episode_id"] == episode["episode_id"]), None)
        shared_owner = issuance and (run.get("orchestrator_session_owner_run_id") or run["workflow_run_id"]) == issuance.get("session_owner_run_id")
        orchestration_event = event in DECISIONS or event == "dispatch_reserved" and isinstance(detail, dict) and detail.get("steering_role") == "orchestrator"
        if affected or orchestration_event and (run["workflow_run_id"] == rid or shared_owner):
            from .workflows import DispatchNotStarted
            raise DispatchNotStarted("Conversation hold blocks this new orchestration or execution reservation")


def scheduling_held(storage, run, token):
    rid = (run.get("parent_link") or {}).get("root_workflow_run_id") or run["workflow_run_id"]
    root = run if rid == run["workflow_run_id"] else storage.get_run(rid, refresh_atomic=True)
    for ep in root.get("steering", {}).get("episodes", []):
        if not ep["hold"] or ep["target"]["role"] != "orchestrator":
            continue
        if run["workflow_run_id"] == rid or ep["target"]["workflow_run_id"] in records(storage, root)[run["workflow_run_id"]][1]:
            # Existing native/worker/invocation work may be observed. A retained
            # ordinary decision is also observed, but never accepted under hold.
            issued = [a for a in run["activations"] if (a.get("token") or {}).get("id") == token["id"]]
            if any(any(t["status"] in {"reserved", "running", "uncertain"} for t in a["tasks"]) for a in issued):
                return False
            for a in issued:
                cid = (a.get("invocation") or {}).get("child_workflow_run_id")
                if cid and any(any(t["status"] in {"reserved", "running", "uncertain"} for aa in child["activations"] for t in aa["tasks"]) for child, path in records(storage, root).values() if cid in path):
                    return False
            return True
    return False


def current_execution_reserved(run, token):
    """Only the current accepted assignment's actual node reservation counts."""
    decision_id = token.get("accepted_decision_id")
    if not decision_id:
        return False
    candidates = []
    assignment_keys = ("assignment_prompt", "assigned_task_ids", "execution_session_mode", "resume_task_id", "retry_of_execution_id")
    for activation in run["activations"]:
        issued = activation.get("token") or {}
        if (activation.get("role") != "node" or activation.get("node_id") != token["node_id"]
                or issued.get("id") != token["id"] or issued.get("accepted_decision_id") != decision_id
                or token.get("execution_activation_id") not in {None, activation["id"]}
                or activation.get("resolved_by_execution_id") or activation.get("retry_of_execution_id") == activation["id"]
                or any(issued.get(k) != token.get(k) for k in assignment_keys)):
            continue
        actual_tasks = any(t.get("status") in {"reserved", "running", "uncertain"} and type(t.get("task_id")) is str for t in activation.get("tasks", []))
        invocation = activation.get("invocation")
        actual_invocation = bool(invocation and token.get("execution_activation_id") == activation["id"] and invocation.get("stage") in {"preparing", "created", "running", "waiting"} and invocation.get("child_workflow_run_id"))
        if actual_tasks or actual_invocation:
            candidates.append(activation)
    return len(candidates) == 1


def admission(storage, root, request):
    """Validate exact checkpoint and bind every existing shared owner task."""
    witness = request["target"]
    if root["status"] in {"completed", "failed", "cancelled", "cancelling"}:
        raise SteeringError("workflow_terminal", "Terminal workflow roots cannot be reopened")
    if witness != current_target(storage, root, witness):
        raise SteeringError("steering_target_stale", "Orchestration target or tree choices changed")
    tree = records(storage, root)
    run = tree[witness["workflow_run_id"]][0]
    token = next(t for t in run["pending"] if t["id"] == witness["token_id"])
    if token.get("accepted_decision_id") or token.get("selected_connections") or token.get("assignments"):
        raise SteeringError("steering_boundary_pending", "Accepted work must reach its next observed decision checkpoint")
    if run["status"] in {"completed", "failed", "cancelled", "cancelling"}:
        raise SteeringError("workflow_terminal", "Terminal workflow cannot be reopened")
    if run.get("input_question") or run.get("pending_inputs") or run.get("orchestrator_usage_recovery"):
        raise SteeringError("steering_question_pending", "Resolve the actual interaction before steering")
    for affected, path in tree.values():
        if witness["workflow_run_id"] not in path:
            continue
        for pending in affected["pending"]:
            if pending["id"] == token["id"] and affected["workflow_run_id"] == run["workflow_run_id"]:
                continue
            accepted = pending.get("accepted_decision_id") or pending.get("selected_connections") or pending.get("assignments")
            already_reserved = current_execution_reserved(affected, pending)
            if accepted and not already_reserved:
                raise SteeringError("steering_boundary_pending", "Affected accepted sibling work has not reached its durable execution reservation")
    owner_id = run.get("orchestrator_session_owner_run_id") or run["workflow_run_id"]
    owner = tree.get(owner_id, (None,))[0]
    if owner is None:
        raise SteeringError("steering_source_unavailable", "Orchestrator session owner is not qualified in this tree")
    candidate = copy.deepcopy(run.get("orchestrator_config") or run["definition"]["orchestrator"])
    from .workflows import _candidate_key
    previous = owner.get("sessions", {}).get("orchestrator")
    if previous and previous.get("candidate") != "orchestrator:" + _candidate_key(candidate):
        raise SteeringError("steering_source_unavailable", "Retained orchestrator candidate changed")
    boundary = []
    for descendant, _ in tree.values():
        shared_owner = descendant.get("orchestrator_session_owner_run_id") or descendant["workflow_run_id"]
        if shared_owner != owner_id:
            continue
        for activation in descendant["activations"]:
            if activation.get("role") != "orchestrator" or activation.get("steering_turn_id"):
                continue
            for attempt in activation["tasks"]:
                if attempt["status"] in {"reserved", "running", "uncertain"}:
                    boundary.append({"workflow_run_id": descendant["workflow_run_id"], "activation_id": activation["id"], "task_id": attempt["task_id"]})
    if any(next(t for a in tree[b["workflow_run_id"]][0]["activations"] for t in a["tasks"] if t["task_id"] == b["task_id"])["status"] == "uncertain" for b in boundary):
        raise SteeringError("steering_delivery_unknown", "Existing shared owner attempt is uncertain")
    if not previous and not boundary and not request.get("allow_fresh_context"):
        raise SteeringError("steering_source_unavailable", "No retained orchestrator session; explicitly permit Fresh context")
    return candidate, previous, boundary, owner_id


def execution_required(node, token):
    return node.get("type") in {"agent", "workflow"} and not token.get("execution_complete") and (not token.get("assignment_prompt") or bool(token.get("requires_assignment")))


def _turn(run, turn_id):
    return next(t for t in run["steering"]["turns"] if t["turn_id"] == turn_id)


def _episode(run, turn):
    return next(e for e in run["steering"]["episodes"] if e["episode_id"] == turn["episode_id"])


def _park(run, storage=None):
    # Keep the root runnable until issued child and sibling owners settle. The
    # scheduler uses the independent hold to stop new conflicting reservations.
    if run["status"] not in {"running", "starting", "paused"}:
        return
    run["attention_reason"] = "Conversation is waiting for caller; use steering capability"
    if storage is not None and not any(t["status"] in {"reserved", "running", "uncertain"} for child, _ in records(storage, run).values() for a in child["activations"] for t in a["tasks"]):
        run["status"] = "paused"


def finish_dismissal(storage, run, episode):
    """Retain the scheduling fence until every issued task positively settles."""
    active = any(t["status"] in {"reserved", "running", "uncertain"}
                 for child, _ in records(storage, run).values()
                 for activation in child["activations"] for t in activation["tasks"])
    episode.update(state="dismiss_pending_settlement" if active else "dismissed", hold=active, dismiss_requested=True)
    if not active:
        _park(run, storage)


def capture(supervisor, turn_id, snapshot=None, *, failure=None, unknown=False):
    """Persist exact final and positive outcome evidence before classification."""
    from .workflow_steering import _history
    def mutate(run):
        t = _turn(run, turn_id)
        if t["state"] in {"captured", "settled"}:
            return
        ep = _episode(run, t)
        t.update(state="unknown" if unknown else "captured", outcome=failure)
        if snapshot is None and not unknown:
            t["not_started"] = True
        if snapshot is not None:
            fields = "task_id status exit_code output_drained timed_out outcome_unknown is_error execution_failure failure_diagnostic session_id usage num_turns total_cost_usd".split()
            t["observation"] = {k: copy.deepcopy(snapshot.get(k)) for k in fields}
            if snapshot.get("task_id") != t["task_id"]:
                t["observed_task_id"] = snapshot.get("task_id")
                t["observation"]["task_id"] = t["task_id"]
                t.update(state="unknown", outcome="capture_unknown")
                unknown_capture = True
            else:
                unknown_capture = False
            for flag in ("output_drained", "timed_out", "outcome_unknown", "is_error"):
                t["observation"][flag] = snapshot.get(flag, False)
            raw = snapshot.get("summary")
            try:
                valid = type(raw) is str and len(raw.encode("utf-8", errors="strict")) <= 65536
            except UnicodeError:
                valid = False
            if valid:
                t["final"] = raw
            else:
                t["outcome"] = failure or "output_too_large" if raw is not None else "empty_final"
            t["usage"] = {"provenance": "observed" if snapshot.get("usage") is not None else "unavailable", "usage": snapshot.get("usage"), "cost_usd": snapshot.get("total_cost_usd"), "num_turns": snapshot.get("num_turns"), "reserved_harness_turns": t["harness_turn_cap"]}
        ep["state"] = "delivery_unknown" if unknown or snapshot is not None and unknown_capture else "classifying"
        _history(run, "final_captured", turn_id=turn_id, episode_id=ep["episode_id"], text=t.get("final"), outcome=t.get("outcome"), observation=t.get("observation"))
        ep["latest_reply_ref"] = "history:" + str(run["steering"]["sequence"])
        for a in run["activations"]:
            if a.get("steering_turn_id") == turn_id:
                a["status"] = "uncertain" if unknown else "completed"
                a["tasks"][0].update(status="uncertain" if unknown else (snapshot or {}).get("status", "not_started"), result={**{k: v for k, v in t.get("observation", {}).items() if v is not None}, "task_id": t["task_id"]})
        _park(run, supervisor.store)
    supervisor.update(mutate, "steering_reply_captured")


def _wrapper(turn, episode):
    return {"episode_id": episode["episode_id"], "turn_id": turn["turn_id"], "commit_target_ref": turn["commit_target_ref"],
            "steering_revision": turn["steering_revision"], "interaction_epoch": turn["commit_target"]["interaction_epoch"],
            "checkpoint_revision": turn["commit_target"]["checkpoint_revision"], "goal_revision": turn["commit_target"]["goal_revision"], "role": "orchestrator"}


def classify_commit(supervisor, turn_id):
    """Classification creates a durable root intent; effects require its receipt."""
    from .steering_output import classify_final
    from .steering_schema import captured_failure
    from .workflow_steering import _history
    def classify(run):
        t = _turn(run, turn_id)
        if t["state"] != "captured" or t.get("action_commit"):
            return
        ep = _episode(run, t)
        failure = t.get("outcome") or captured_failure(t.get("observation"), t["task_id"])
        result = {"kind": "refused", "code": failure} if failure else classify_final(t.get("final"))
        code = result.get("code")
        if ep.get("dismiss_requested"):
            code = "dismissed"
        elif time.time() - ep["created_at"] > 1800:
            code = "request_expired"
        elif run["status"] in {"completed", "failed", "cancelled", "cancelling"}:
            code = "workflow_terminal"
        elif result["kind"] == "conversation" and (run["interaction_epoch"] != t["commit_target"]["interaction_epoch"] or ep["steering_revision"] != t["steering_revision"] or not target_matches(supervisor.store, run, t["commit_target"])):
            code = "steering_target_stale"
        elif result["kind"] == "command":
            wrapper = result["command"]
            expected = _wrapper(t, ep)
            if set(wrapper) != set(expected) | {"command"} or any(type(wrapper.get(k)) is not type(v) or wrapper.get(k) != v for k, v in expected.items()):
                code = "role_command_invalid"
            elif run["interaction_epoch"] != expected["interaction_epoch"] or ep["steering_revision"] != expected["steering_revision"]:
                code = "interaction_revoked"
            elif not target_matches(supervisor.store, run, t["commit_target"]):
                code = "steering_target_stale"
            else:
                command = wrapper["command"]
                if type(command) is not dict:
                    code = "role_command_invalid"
                else:
                    tree = records(supervisor.store, run)
                    target = tree[t["target"]["workflow_run_id"]][0]
                    token = next(x for x in target["pending"] if x["id"] == t["target"]["token_id"])
                    node = next(n for n in target["definition"]["nodes"] if n["id"] == token["node_id"])
                    execute = execution_required(node, token)
                    from .workflow_delegation import validate_decision
                    try:
                        if target["definition"].get("context_delivery") == "optimized_v1":
                            from .workflow_context import acknowledged_receipt
                            if acknowledged_receipt(t.get("context_delivery", {}), command.get("context_ack")) is None:
                                raise SteeringError("role_command_invalid", "Current issued context acknowledgement is required")
                        if command.get("action") == "call_on_demand":
                            from .workflow_on_demand import validate_action
                            validate_action(supervisor.store, target, token, command)
                        elif command.get("action") == "inspect":
                            from .workflow_delegation import validate_inspection_request
                            validate_inspection_request(target, command, token)
                        else:
                            validate_decision(target, node, token, command, execute, root=supervisor.store.root)
                        if run["status"] == "paused":
                            run["status"] = "running"
                        t["action_commit"] = {"state": "intent", "command": copy.deepcopy(command), "call_id": uuid.uuid4().hex, "execution_id": uuid.uuid4().hex}
                        _history(run, "command_intent", turn_id=turn_id, episode_id=ep["episode_id"], action=command.get("action"))
                        return
                    except (ValueError, TypeError, KeyError):
                        code = "role_command_invalid"
        if code:
            ep["state"] = "dismissed" if code == "dismissed" else "expired" if code == "request_expired" else "stale" if code == "steering_target_stale" else "command_refused"
            if code == "dismissed":
                finish_dismissal(supervisor.store, run, ep)
        else:
            ep["state"] = "awaiting_caller"
        t.update(state="settled", outcome=code or "conversation")
        _history(run, "classified", turn_id=turn_id, episode_id=ep["episode_id"], outcome=t["outcome"])
        _park(run, supervisor.store)
    supervisor.update(classify, "steering_classified")
    turn = _turn(supervisor.run(refresh_atomic=True), turn_id)
    if turn.get("action_commit"):
        return apply_commit(supervisor, turn_id)
    return None


def _target_supervisor(supervisor, target_id):
    from .workflows import WorkflowSupervisor
    child = WorkflowSupervisor(supervisor.registry, supervisor.store, tree=supervisor.tree)
    child.run_id = target_id
    return child


def apply_commit(supervisor, turn_id):
    """Complete only this root intent; child receipts make replay effect-free."""
    from .workflow_steering import _history
    from .workflow_delegation import accept_decision_locked, inspect_decision
    root_id = supervisor.run_id
    with supervisor.store.tree_mutation(root_id):
        root = supervisor.run(refresh_atomic=True)
        turn = _turn(root, turn_id)
        intent = turn.get("action_commit")
        if not intent or intent["state"] in {"acknowledged", "refused"}:
            return None
        target_id = turn["target"]["workflow_run_id"]
        child = _target_supervisor(supervisor, target_id)
        command = intent["command"]
        def accept(run):
            receipts = run.setdefault("steering_action_receipts", {})
            if turn_id in receipts:
                expected_receipt = {"root_workflow_run_id": root_id, "turn_id": turn_id, "command_digest": _digest(command), "state": "applied"}
                if receipts[turn_id] != expected_receipt:
                    raise SteeringError("steering_source_unavailable", "Child action receipt disagrees with root command intent")
                return
            # Root intent remains durable and held through child acceptance.
            current_root = run if target_id == root_id else supervisor.run(refresh_atomic=True)
            if current_target(supervisor.store, current_root, turn["commit_target"]) != turn["commit_target"]:
                raise SteeringError("steering_target_stale", "Target changed before exact action acceptance")
            token = next(t for t in run["pending"] if t["id"] == turn["target"]["token_id"])
            node = next(n for n in run["definition"]["nodes"] if n["id"] == token["node_id"])
            activation = next((a for a in run["activations"] if a["id"] == turn_id), None)
            if activation is None:
                activation = {"id": turn_id, "node_id": node["id"], "role": "orchestrator", "status": "running", "tasks": [{"task_id": turn["task_id"], "status": "completed", "candidate": turn["candidate"], "result": {k: copy.deepcopy(v) for k, v in turn["observation"].items() if v is not None}, **({"context_delivery": copy.deepcopy(turn["context_delivery"])} if turn.get("context_delivery") else {})}], "created_at": time.time(), "token": copy.deepcopy(token), "decision_id": token["decision_id"], "decision_goal_revision": run.get("goal_revision")}
                run["activations"].append(activation)
            activation.update(decision_id=token["decision_id"], decision_goal_revision=run.get("goal_revision"))
            token["decision_activation_id"] = turn_id
            if run["status"] == "paused":
                run["status"] = "running"
            # Existing validators and context acknowledgements remain authoritative.
            class AtomicSupervisor:
                store, tree = child.store, child.tree
                context_baselines = child.context_baselines
                def _task_update(self, activation_id, task_id, values):
                    next(t for a in run["activations"] if a["id"] == activation_id for t in a["tasks"] if t["task_id"] == task_id).update(values)
                run_id = child.run_id
                def run(self, **kwargs):
                    return run
                def update(self, mutate, event, detail=None):
                    mutate(run)
                    return run
            atomic = AtomicSupervisor()
            from .workflow_prompt_delivery import accept_ack
            accept_ack(atomic, turn_id, command)
            if command["action"] == "inspect":
                prior_attempts = token.get("decision_attempts", 0)
                inspect_decision(atomic, command, token, turn_id)
                token["decision_attempts"] = prior_attempts
            elif command["action"] != "call_on_demand":
                accept_decision_locked(atomic, run, node, token, activation, command, execution_required(node, token), turn["final"], [])
                if activation.get("decision_ignored"):
                    raise SteeringError("steering_target_stale", "Original validator did not accept the current action")
            receipts[turn_id] = {"root_workflow_run_id": root_id, "turn_id": turn_id, "command_digest": _digest(command), "state": "applied"}
        try:
            child.update(accept, "steering_action_applied", {"turn_id": turn_id})
        except (ValueError, TypeError, KeyError) as exc:
            def refused(run):
                t = _turn(run, turn_id)
                t["action_commit"]["state"] = "refused"
                t.update(state="settled", outcome=getattr(exc, "code", "role_command_invalid"))
                _episode(run, t)["state"] = "command_refused"
                _history(run, "classified", turn_id=turn_id, outcome=t["outcome"])
            supervisor.update(refused, "steering_classified")
            return None
        if command["action"] == "call_on_demand":
            # The asynchronous coordinator consumes this exact durable intent.
            return turn_id
        acknowledge(supervisor, turn_id)
    return None


def acknowledge(supervisor, turn_id):
    from .workflow_steering import _history
    def ack(run):
        t = _turn(run, turn_id)
        if t["action_commit"]["state"] == "acknowledged":
            return
        t["action_commit"]["state"] = "acknowledged"
        t.update(state="settled", outcome="committed")
        ep = _episode(run, t)
        ep.update(state="committed", hold=False)
        run["steering"]["commits"].append({"turn_id": turn_id, "episode_id": ep["episode_id"], "commit_target_ref": t["commit_target_ref"], "definition_digest": _digest(t["action_commit"]["command"])})
        _history(run, "classified", turn_id=turn_id, episode_id=ep["episode_id"], outcome="committed")
    supervisor.update(ack, "steering_commit_acknowledged")


async def _observe_boundaries(supervisor, turn_id):
    """Observe the exact generations sharing the pinned owner, never replace."""
    from .steering_runner import _wait_observed
    from .workflow_steering import _history
    turn = _turn(supervisor.run(refresh_atomic=True), turn_id)
    for boundary in turn.get("boundary_tasks", []):
        child_id = boundary["workflow_run_id"]
        child = supervisor.store.get_run(child_id, refresh_atomic=True)
        activation = next(a for a in child["activations"] if a["id"] == boundary["activation_id"])
        attempt = next(t for t in activation["tasks"] if t["task_id"] == boundary["task_id"])
        snapshot = attempt.get("result")
        if attempt["status"] in {"reserved", "running", "uncertain"}:
            task = supervisor.registry.attach(boundary["task_id"])
            if task is None or task.task_id != boundary["task_id"]:
                return False
            await _wait_observed(supervisor, task)
            snapshot = task.snapshot()
        if (not snapshot or snapshot.get("task_id") != boundary["task_id"] or snapshot.get("status") not in {"completed", "failed", "cancelled"}
                or type(snapshot.get("exit_code")) is not int or snapshot.get("output_drained") is not True or snapshot.get("outcome_unknown")):
            return False
        with supervisor.store.tree_mutation(supervisor.run_id):
            def observed(run):
                a = next(a for a in run["activations"] if a["id"] == boundary["activation_id"])
                t = next(t for t in a["tasks"] if t["task_id"] == boundary["task_id"])
                t.update(status=snapshot["status"], result=copy.deepcopy(snapshot))
                a.update(status="completed", steering_superseded=True, decision_disposition="superseded_by_steering", raw_output=snapshot.get("summary", ""))
            supervisor.store.update_run(child_id, observed, "steering_boundary_settlement_observed")
            def retained(run):
                t = _turn(run, turn_id)
                from .workflows import _candidate_key
                t["previous_session"] = {"candidate": "orchestrator:" + _candidate_key(t["candidate"]), "task_id": boundary["task_id"], "session_id": snapshot.get("session_id")}
                _history(run, "boundary_settlement_observed", turn_id=turn_id, task_id=boundary["task_id"], observation={k: snapshot.get(k) for k in ("status", "exit_code", "output_drained", "session_id")})
            supervisor.update(retained, "steering_boundary_owner_observed")
    return True


def _handoff(supervisor, turn_id):
    """Root intent -> exact child receipt -> root acknowledgement, under tree lock."""
    with supervisor.store.tree_mutation(supervisor.run_id):
        root = supervisor.run(refresh_atomic=True)
        t = _turn(root, turn_id)
        target_id = t["target"]["workflow_run_id"]
        def receipt(run):
            receipts = run.setdefault("steering_hold_receipts", {})
            value = {"root_workflow_run_id": supervisor.run_id, "turn_id": turn_id, "target_digest": _digest(t["target"])}
            if turn_id in receipts and receipts[turn_id] != value:
                raise SteeringError("steering_source_unavailable", "Child receipt disagrees with root intent")
            receipts[turn_id] = value
        supervisor.store.update_run(target_id, receipt, "steering_hold_receipted")
        supervisor.update(lambda r: _turn(r, turn_id)["handoff"].update(state="acknowledged"), "steering_hold_acknowledged")


async def run_turn(supervisor, root_id):
    """Execute only one explicit issuance; prose never schedules another turn."""
    import asyncio
    import json
    from pathlib import Path
    from .workflow_steering import pending_turn, _history
    from .steering_runner import _wait_observed
    from .steering_schema import captured_failure
    from .tasks import get_backend, SessionUnknownError
    from .workflows import _candidate_key, CheckoutLease
    supervisor.run_id = root_id
    root = supervisor.run(refresh_atomic=True)
    turn = pending_turn(root) or next((t for t in root["steering"]["turns"] if t["state"] == "captured"), None)
    if turn is None:
        return
    tid = turn["turn_id"]
    if turn.get("action_commit"):
        pending_action = apply_commit(supervisor, tid)
        if pending_action:
            await _run_on_demand(supervisor, tid)
        return
    if turn["state"] == "captured":
        pending_action = classify_commit(supervisor, tid)
        if pending_action:
            await _run_on_demand(supervisor, tid)
        return
    _handoff(supervisor, tid)
    if turn["state"] == "queued":
        if not await _observe_boundaries(supervisor, tid):
            def unknown(run):
                _episode(run, _turn(run, tid)).update(state="delivery_unknown")
            supervisor.update(unknown, "steering_boundary_unknown")
            return
        def ready(run):
            t = _turn(run, tid)
            ep = _episode(run, t)
            if current_target(supervisor.store, run, t["commit_target"]) != t["commit_target"]:
                t.update(state="settled", outcome="steering_target_stale")
                ep.update(state="stale")
            else:
                t["state"] = "reserved"
                ep["state"] = "turn_reserved"
            _history(run, "boundary_observed", turn_id=tid, state=t["state"])
        supervisor.update(ready, "steering_boundary_observed")
        turn = _turn(supervisor.run(refresh_atomic=True), tid)
        if turn["state"] != "reserved":
            return
    if turn["state"] != "reserved":
        task = supervisor.registry.attach(turn["task_id"])
        if task is None or task.task_id != turn["task_id"]:
            capture(supervisor, tid, failure="steering_delivery_unknown", unknown=True)
            return
    else:
        def reserve(run):
            t = _turn(run, tid)
            ep = _episode(run, t)
            if t["state"] != "reserved":
                raise SteeringError("steering_busy", "Conversation issuance already claimed")
            code = "dismissed" if ep.get("dismiss_requested") else "request_expired" if time.time() - ep["created_at"] > 1800 else None
            if code is None and (run["interaction_epoch"] != t["commit_target"]["interaction_epoch"] or ep["steering_revision"] != t["steering_revision"]):
                code = "steering_target_stale"
            if code is None and current_target(supervisor.store, run, t["commit_target"]) != t["commit_target"]:
                code = "steering_target_stale"
            if code:
                t.update(state="settled", outcome=code)
                if code == "dismissed":
                    finish_dismissal(supervisor.store, run, ep)
                else:
                    ep.update(state="stale", hold=True)
                return
            t["state"] = "spawn_intent"
            ep["state"] = "discussing"
            import os
            from . import identity
            run.update(supervisor_pid=os.getpid(), supervisor_identity=identity.own_identity())
            run["activations"].append({"id": tid, "steering_turn_id": tid, "node_id": t["target"]["node_id"], "role": "orchestrator", "status": "running", "tasks": [{"task_id": t["task_id"], "status": "reserved", "candidate": t["candidate"]}], "created_at": time.time(), "token": None})
            _history(run, "spawn_intent", turn_id=tid, task_id=t["task_id"])
        supervisor.update(reserve, "steering_spawn_intent")
        root = supervisor.run(refresh_atomic=True)
        turn = _turn(root, tid)
        if turn["state"] != "spawn_intent":
            return
        target = records(supervisor.store, root)[turn["target"]["workflow_run_id"]][0]
        token = next(t for t in target["pending"] if t["id"] == turn["target"]["token_id"])
        node = next(n for n in target["definition"]["nodes"] if n["id"] == token["node_id"])
        from .workflow_delegation import decision_context
        context = decision_context(target, node, token, execution_required(node, token), root=supervisor.store.root)
        wrapper = {**_wrapper(turn, _episode(root, turn)), "command": {"decision_id": token["decision_id"], "action": "CONFIGURED_ACTION", "reason": "REASON"}}
        receipt = None
        context_text = json.dumps(context)
        if target["definition"].get("context_delivery") == "optimized_v1":
            from .workflow_context import render_decision_context, context_ack
            from .workflow_prompt_delivery import project_orchestrator_context, orchestrator_input_manifests
            owner_prefix = turn["session_owner_run_id"] + ":orchestrator:" + _candidate_key(turn["candidate"])
            rendered = render_decision_context(project_orchestrator_context(target, context, root=supervisor.store.root), session_owner=owner_prefix + ":" + turn["task_id"], scope=target["workflow_run_id"], baseline=None, force_bootstrap=True, evidence_manifests=orchestrator_input_manifests(target, context))
            receipt = rendered.receipt
            receipt["owner_prefix"] = owner_prefix
            context_text = rendered.prompt
            wrapper["command"]["context_ack"] = context_ack(receipt)
            def issued_context(run):
                t = _turn(run, tid)
                t["context_delivery"] = copy.deepcopy(receipt)
                next(a for a in run["activations"] if a.get("steering_turn_id") == tid)["tasks"][0]["context_delivery"] = copy.deepcopy(receipt)
            supervisor.update(issued_context, "steering_context_issued")
        prompt = ("You are the workflow orchestrator in one explicitly requested read-only conversation. Do not execute tools, dispatch work, modify files, or answer approvals. Ordinary prose waits for an explicit caller reply. To commit one normal configured decision, return only HARFLOW_COMMIT_V1 on its own line, the complete strict JSON wrapper below, and END_HARFLOW_COMMIT_V1 on its own line. Replace command with an original valid decision from the issued choices. No repair, fallback or automatic conversation follows.\n" + json.dumps(wrapper) + "\nCurrent original decision context:\n" + context_text + "\nExact caller request:\n" + turn["text"])
        candidate = turn["candidate"]
        task = None
        spawned = False
        try:
            if candidate.get("caller_approvals") or not get_backend(candidate["backend"]).capabilities.supports_turn_cap:
                raise SteeringError("steering_budget_unsupported", "Selected adapter cannot enforce the pinned finite cap")
            previous = turn.get("previous_session")
            from .task_supervision import read_task_record
            record = read_task_record(supervisor.registry.log_dir, previous["task_id"]) if previous else None
            compatible = bool(previous and record and previous.get("candidate") == "orchestrator:" + _candidate_key(candidate)
                              and record.backend == candidate["backend"] and record.repo_path == target["repo_path"] and record.freedom == "read_only"
                              and record.session_id == previous.get("session_id") and record.model == candidate.get("model")
                              and record.reasoning_effort == candidate.get("reasoning_effort") and record.network == target.get("network"))
            async with CheckoutLease(supervisor.store, target["repo_path"], False, lambda: True, pool=None, owned_run_ids=set(records(supervisor.store, root))):
                if compatible:
                    task = await supervisor.registry.resume_record(record, prompt, max_turns=turn["harness_turn_cap"], task_id=turn["task_id"], display_prompt=turn["text"], require_unchanged_session=True, network=target.get("network"))
                else:
                    if not turn["allow_fresh_context"]:
                        raise SteeringError("steering_source_unavailable", "Pinned session is unavailable; explicit Fresh consent is required")
                    task = await supervisor.registry.start(prompt, Path(target["repo_path"]), backend=candidate["backend"], freedom="read_only", network=target.get("network"), model=candidate.get("model"), reasoning_effort=candidate.get("reasoning_effort"), max_turns=turn["harness_turn_cap"], caller_approvals=False, task_id=turn["task_id"], display_prompt=turn["text"], title=target["name"] + " · orchestrator conversation")
                spawned = True
                def launched(run):
                    t = _turn(run, tid)
                    t["state"] = "running"
                    next(a for a in run["activations"] if a.get("steering_turn_id") == tid)["tasks"][0]["status"] = "running"
                    _history(run, "spawn_observed", turn_id=tid, task_id=task.task_id)
                supervisor.update(launched, "steering_spawn_observed")
                await _wait_observed(supervisor, task)
                snapshot = task.snapshot()
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except Exception as exc:
            positive = not spawned and (isinstance(exc, (SteeringError, SessionUnknownError)) or getattr(exc, "harflow_not_started", False) is True)
            capture(supervisor, tid, failure=getattr(exc, "code", "provider_not_started") if positive else "steering_delivery_unknown", unknown=not positive)
            if positive:
                classify_commit(supervisor, tid)
            return
    if turn["state"] != "reserved" and task is not None:
        target = records(supervisor.store, supervisor.run(refresh_atomic=True))[turn["target"]["workflow_run_id"]][0]
        async with CheckoutLease(supervisor.store, target["repo_path"], False, lambda: True, pool=None, owned_run_ids=set(records(supervisor.store, supervisor.run(refresh_atomic=True)))):
            await _wait_observed(supervisor, task)
            snapshot = task.snapshot()
    observed = {**snapshot, **{flag: snapshot.get(flag, False) for flag in ("output_drained", "timed_out", "outcome_unknown", "is_error")}}
    failure = captured_failure(observed, turn["task_id"])
    capture(supervisor, tid, snapshot, failure=failure, unknown=failure == "capture_unknown")
    if failure == "capture_unknown":
        return
    with supervisor.store.tree_mutation(root_id):
        owner_id = turn["session_owner_run_id"]
        binding = {"candidate": "orchestrator:" + _candidate_key(turn["candidate"]), "task_id": turn["task_id"], "session_id": snapshot.get("session_id")}
        supervisor.store.update_run(owner_id, lambda r: r.setdefault("sessions", {}).__setitem__("orchestrator", binding), "steering_session_observed")
    pending_action = classify_commit(supervisor, tid)
    if pending_action:
        await _run_on_demand(supervisor, tid)


async def _run_on_demand(supervisor, turn_id):
    root = supervisor.run(refresh_atomic=True)
    t = _turn(root, turn_id)
    child = _target_supervisor(supervisor, t["target"]["workflow_run_id"])
    run = child.run(refresh_atomic=True)
    token = next(tk for tk in run["pending"] if tk["id"] == t["target"]["token_id"])
    node = next(n for n in run["definition"]["nodes"] if n["id"] == token["node_id"])
    from .workflow_on_demand import call
    await call(child, node, token, token, t["action_commit"]["command"], turn_id, steering_intent={"root_id": supervisor.run_id, "turn_id": turn_id, "call_id": t["action_commit"]["call_id"], "execution_id": t["action_commit"]["execution_id"]})
    owner = run.get("orchestrator_session_owner_run_id") or run["workflow_run_id"]
    reservation = next((c for c in supervisor.store.get_run(owner).get("on_demand_calls", []) if c["id"] == t["action_commit"]["call_id"]), None)
    if reservation:
        acknowledge(supervisor, turn_id)


def settle_dismissals(supervisor):
    root = supervisor.run(refresh_atomic=True)
    if any(t["status"] in {"reserved", "running", "uncertain"} for run, _ in records(supervisor.store, root).values() for a in run["activations"] for t in a["tasks"]):
        return
    def settle(run):
        for ep in run["steering"]["episodes"]:
            if ep["state"] == "dismiss_pending_settlement" and not any(t["episode_id"] == ep["episode_id"] and t["state"] in {"queued", "reserved", "spawn_intent", "running", "unknown", "captured"} for t in run["steering"]["turns"]):
                finish_dismissal(supervisor.store, run, ep)
    supervisor.update(settle, "steering_dismissal_settled")
