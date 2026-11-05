"""Root-locked admission, authority, receipt and conversation issuance CAS."""
from __future__ import annotations

import copy
import time
import uuid

from .workflow_steering import (SteeringError, _authorize, _source_check, _history, target, held,
                               REQUESTS, EPISODES, EPISODE_TURNS, ROOT_TURNS, UNSETTLED)


def admit(storage, run, principal, monitor, request, digest, binding):
    scheduled = False
    result = None
    _authorize(run, principal, monitor)
    s = run["steering"]
    previous = s["requests"].get(request["request_id"])
    if previous:
        if previous["payload_digest"] != digest or previous.get("principal_binding") != binding:
            raise SteeringError("request_payload_conflict", "Request identity already has a different payload")
        result = {**copy.deepcopy(previous), "replayed": True}
        result.pop("principal_binding", None)
        old_turn = next((t for t in s["turns"] if t["request_id"] == request["request_id"]), None)
        scheduled = bool(old_turn and old_turn["state"] in {"queued", "reserved", "spawn_intent", "running", "captured", "unknown"})
        return result, scheduled
    if len(s["requests"]) >= REQUESTS or len(s["history"]) >= 500:
        raise SteeringError("steering_queue_full", "Durable steering receipt capacity reached")
    if request["action"] in {"open", "reply", "retarget"}:
        _source_check(storage, run)
    if request["expected_interaction_epoch"] != run["interaction_epoch"]:
        raise SteeringError("interaction_revoked", "Interaction epoch changed")
    if request["expected_control_revision"] != s["control_revision"]:
        raise SteeringError("steering_revision_stale", "Control revision changed")
    action = request["action"]
    ep = None
    turn = None
    if action in {"take_owner", "release_owner"}:
        if not monitor:
            raise SteeringError("steering_not_owner", "Ownership changes require a verified native human")
        if action == "release_owner":
            from . import identity
            admitted = run.get("interaction_caller") or {}
            if admitted.get("kind") != "external_parent" or identity.check_detail(admitted["identity"])[0] != "alive":
                raise SteeringError("caller_undecidable", "Original external generation is not still qualified")
        s["owner"] = "monitor" if action == "take_owner" else "caller"
        run["interaction_epoch"] += 1
        for item in s["episodes"]:
            if item["hold"]:
                item["steering_revision"] += 1
    else:
        if action != "dismiss" and request["target"] != target(run, storage, request["target"]):
            raise SteeringError("steering_target_stale", "Draft/source/authority snapshot changed")
        if action != "open":
            ep = next((item for item in s["episodes"] if item["episode_id"] == request["episode_id"]), None)
            if ep is None or ep["steering_revision"] != request["expected_steering_revision"]:
                raise SteeringError("steering_revision_stale", "Episode revision changed")
            if run.get("kind") != "builder" and action == "reply" and request["target"] != ep["target"]:
                raise SteeringError("steering_target_stale", "Reply cannot change this episode target; explicitly retarget after a fresh observation")
            if action == "dismiss" and request["target"] != ep["target"]:
                try:
                    current = target(run, storage, request["target"])
                except SteeringError:
                    current = None
                if current != request["target"]:
                    raise SteeringError("steering_target_stale", "Dismiss must identify this episode or its current exact target")
        if action == "dismiss":
            ep["steering_revision"] += 1
            ep["dismiss_requested"] = ep["state"] in UNSETTLED
            ep["state"] = "dismiss_pending_settlement" if ep["dismiss_requested"] else "dismissed"
            ep["hold"] = ep["state"] != "dismissed"
            if not ep["hold"]:
                if run.get("kind") != "builder":
                    from .orchestrator_steering import records
                    unsettled = any(t["status"] in {"reserved", "running", "uncertain"} for child, _ in records(storage, run).values() for a in child["activations"] for t in a["tasks"])
                    if unsettled:
                        ep.update(state="dismiss_pending_settlement", hold=True, dismiss_requested=True)
                    else:
                        from .orchestrator_steering import _park
                        _park(run, storage)
        else:
            if any(item["state"] in UNSETTLED for item in s["episodes"]) or any(t["state"] == "captured" for t in s["turns"]):
                raise SteeringError("steering_busy", "Observe the existing issuance; no replacement is supported")
            orchestration = run.get("kind") != "builder"
            boundary_tasks, owner_id = [], run["workflow_run_id"]
            if orchestration:
                from .orchestrator_steering import admission
                candidate, previous_session, boundary_tasks, owner_id = admission(storage, run, request)
                active_tasks = [{"task_id": b["task_id"], "status": "running"} for b in boundary_tasks]
            else:
                active_tasks = [t for a in run["activations"] for t in a["tasks"] if t["status"] in {"reserved", "running", "uncertain"}]
            if any(t["status"] == "uncertain" for t in active_tasks):
                raise SteeringError("steering_delivery_unknown", "Existing builder attempt requires observation before admission")
            queued = bool(active_tasks)
            if run.get("status") in {"cancelled", "cancelling"}:
                raise SteeringError("workflow_terminal", "Cancelled builders cannot be reopened")
            if run.get("input_question") or run.get("pending_inputs") or run.get("orchestrator_usage_recovery"):
                raise SteeringError("steering_question_pending", "Resolve the existing interaction first")
            retained = sum(len(t.get("text", "").encode()) + len(t.get("final", "").encode()) for t in s["turns"])
            if retained + len(request["text"].encode()) + 64 * 1024 > 1024 * 1024:
                raise SteeringError("steering_queue_full", "Root retained conversation text capacity reached")
            if len(s["turns"]) >= ROOT_TURNS:
                raise SteeringError("steering_budget_exhausted", "Root conversation issuance limit reached")
            if action == "open" and held(run):
                raise SteeringError("steering_busy", "Reply, retarget or dismiss the existing held episode")
            if action in {"open", "retarget"}:
                if len(s["episodes"]) >= EPISODES:
                    raise SteeringError("steering_queue_full", "Episode capacity reached")
                if action == "retarget":
                    if request.get("supersedes_episode_id") != ep["episode_id"]:
                        raise SteeringError("malformed_command", "Retarget must explicitly identify predecessor")
                    ep.update(state="stale", hold=False, steering_revision=ep["steering_revision"] + 1)
                ep = {"episode_id": uuid.uuid4().hex, "steering_revision": 1, "state": "turn_reserved", "hold": True,
                      "created_at": time.time(), "target": {}, "latest_reply_ref": None}
                if action == "retarget":
                    ep["supersedes_episode_id"] = request["supersedes_episode_id"]
                s["episodes"].append(ep)
            else:
                if ep["state"] not in {"awaiting_caller", "command_refused"} or time.time() - ep["created_at"] > 1800:
                    raise SteeringError("request_expired", "Episode is not open for another turn; explicitly retarget")
                ep["steering_revision"] += 1
            if sum(t["episode_id"] == ep["episode_id"] for t in s["turns"]) >= EPISODE_TURNS:
                raise SteeringError("steering_budget_exhausted", "Episode conversation issuance limit reached")
            if not orchestration:
                candidate = copy.deepcopy(run["definition"]["orchestrator"])
            from .tasks import get_backend
            if candidate.get("caller_approvals") or not get_backend(candidate["backend"]).capabilities.supports_turn_cap:
                raise SteeringError("steering_budget_unsupported", "Selected builder adapter cannot enforce a finite harness-turn cap")
            if not orchestration:
                previous_session = run.get("sessions", {}).get("builder")
            from .workflows import _candidate_key
            if previous_session and previous_session.get("candidate") != ("orchestrator:" if orchestration else "builder:") + _candidate_key(candidate) and not request.get("allow_fresh_context", False):
                raise SteeringError("steering_source_unavailable", "Retained builder candidate changed; explicitly permit Fresh context")
            if not previous_session and not queued and not request.get("allow_fresh_context", False):
                raise SteeringError("steering_source_unavailable", "No retained builder session; explicitly permit Fresh context")
            s["control_revision"] += 1
            witness = target(run, storage, request["target"])
            turn = {"turn_id": uuid.uuid4().hex, "task_id": uuid.uuid4().hex, "episode_id": ep["episode_id"],
                    "request_id": request["request_id"], "state": "queued" if queued else "reserved", "target": copy.deepcopy(witness),
                    "commit_target": copy.deepcopy(witness), "commit_target_ref": "commit-target:" + uuid.uuid4().hex,
                    "candidate": candidate, "text": request["text"], "created_at": time.time(),
                    "allow_fresh_context": request.get("allow_fresh_context", False), "usage": {"provenance": "unavailable", "reserved_harness_turns": min(candidate.get("max_turns") or 8, 8)},
                    "boundary_task_id": active_tasks[-1]["task_id"] if queued else None, "harness_turn_cap": min(candidate.get("max_turns") or 8, 8), "previous_session": copy.deepcopy(previous_session), "steering_revision": ep["steering_revision"], "preview_receipts": []}
            if orchestration:
                turn.update(boundary_tasks=boundary_tasks, session_owner_run_id=owner_id, handoff={"intent_id": turn["turn_id"], "state": "intent", "workflow_run_id": witness["workflow_run_id"]})
            s["turns"].append(turn)
            ep.update(state="queued_boundary" if queued else "turn_reserved", target=witness, hold=True)
            if not queued and not orchestration:
                run["status"] = "starting"
            run["attention_reason"] = "Conversation is waiting for caller; use steering capability"
            scheduled = True
    if not scheduled:
        s["control_revision"] += 1
    _history(run, "requested", request_id=request["request_id"], action=action, episode_id=ep["episode_id"] if ep else None,
             text=request.get("text"), reason=request.get("reason"))
    result = {"request_id": request["request_id"], "payload_digest": digest, "episode_id": ep["episode_id"] if ep else None,
              "steering_revision": ep["steering_revision"] if ep else None, "interaction_epoch": run["interaction_epoch"],
              "control_revision": s["control_revision"], "sequence": s["sequence"], "state": ep["state"] if ep else s["owner"],
              "turn_id": turn["turn_id"] if turn else None, "task_id": turn["task_id"] if turn else None,
              "accepted": True, "scheduled": scheduled, "replayed": False,
              "next_action": "Observe the same issuance" if scheduled else "Refresh steering status"}
    s["requests"][request["request_id"]] = {**copy.deepcopy(result), "principal_binding": binding}
    return result, scheduled
