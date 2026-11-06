"""Process-bound, durable one-request/one-turn root-builder conversations.

The ledger is authority; detached supervisors never retry an uncertain issuance.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any

from .steering_output import classify_final, strict_loads
from .workflows import WorkflowError

ROOT_TURNS = 32
EPISODE_TURNS = 8
REQUESTS = 256
EPISODES = 32
TEXT_BYTES = 32 * 1024
HISTORY_BYTES = 256 * 1024
STATES = {"expired", "target_terminal", "classifying", "queued_boundary", "turn_reserved", "discussing", "delivery_unknown", "awaiting_caller", "command_refused", "committed", "dismissed", "stale", "dismiss_pending_settlement"}
UNSETTLED = {"classifying", "queued_boundary", "turn_reserved", "discussing", "delivery_unknown", "dismiss_pending_settlement"}


class SteeringError(WorkflowError):
    def __init__(self, code, message, **data):
        self.code = code
        self.data = {"request_id": None, "episode_id": None, "current_revision": None,
                     "current_interaction_epoch": None, "current_target_ref": None,
                     "receipt_ref": None, "retryable": False, "next_action": "Refresh steering status", **data}
        super().__init__(message)


def _digest(value):
    return "sha256:" + hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def initialize(run):
    """Only engine admission may install this qualified ledger."""
    run["interaction_epoch"] = 0
    run["steering"] = {"schema_version": 1, "control_revision": 0, "checkpoint_revision": 0,
                       "sequence": 0, "episodes": [], "requests": {}, "turns": [], "commits": [],
                       "history": [], "owner": run.get("interaction_owner", "caller")}


from .steering_schema import validate_ledger


def semantic(run):
    result = {key: run.get(key) for key in ("builder_draft", "draft_revision", "source_revision", "source_saved_definition", "source_name", "definition_hash", "goal_revision", "interaction_owner", "interaction_handover", "input_question", "pending_inputs", "orchestrator_usage_recovery")}
    if run.get("kind") != "builder":
        from .orchestrator_steering import _semantic
        result["orchestration"] = _semantic(run)
    result["ordinary_builder_identity"] = ordinary_builder_identity(run)
    result["definition_digest"] = _digest(run.get("definition"))
    return result


def ordinary_builder_identity(run):
    ordinary = [a for a in run.get("activations", []) if a.get("role") == "builder" and not a.get("steering_turn_id")]
    latest = ordinary[-1] if ordinary else None
    return {"activation_id": latest["id"] if latest else None, "task_id": latest["tasks"][-1]["task_id"] if latest and latest["tasks"] else None}


def semantic_changed(before, run):
    if "steering" in run and before != semantic(run):
        run["steering"]["checkpoint_revision"] += 1
        if before.get("interaction_owner") != run.get("interaction_owner") or before.get("interaction_handover") != run.get("interaction_handover"):
            run["interaction_epoch"] += 1
            run["steering"]["control_revision"] += 1
            run["steering"]["owner"] = run.get("interaction_owner", "caller")


def held(run):
    from .conversation_workflows import held as delivery_held
    return delivery_held(run) or any(ep["hold"] for ep in run.get("steering", {}).get("episodes", []))


def _transport(storage, monitor):
    # Deny managed callers before looking up the requested root, including env-only evidence.
    from . import lineage, takeover, workflow_interactions
    refusal = takeover.env_agent_refusal(None)
    if refusal:
        raise SteeringError("managed_caller_forbidden", "Managed callers cannot steer workflows")
    detection = lineage.detect_caller_detail(storage.root / "tasks")
    if detection.caller is not None:
        raise SteeringError("managed_caller_forbidden", "Managed callers cannot steer workflows")
    if detection.undecidable is not None:
        raise SteeringError("caller_undecidable", "Caller ancestry is unavailable")
    if monitor:
        refusal = takeover.caller_refusal(storage.root / "tasks")
        if refusal:
            raise SteeringError("caller_undecidable" if refusal[0] == "caller_undecidable" else "managed_caller_forbidden", refusal[1])
        return None
    current = workflow_interactions.capture_transport_parent()
    if current["kind"] != "external_parent":
        raise SteeringError("caller_undecidable", "External application parent cannot be qualified")
    return current


def _authorize(run, principal, monitor):
    if run.get("parent_link") or run.get("kind") not in {"builder", "workflow"}:
        raise SteeringError("steering_role_unsupported", "Steering must address a qualified builder or workflow root")
    if "steering" not in run:
        raise SteeringError("steering_source_unavailable", "Historical run lacks qualified steering admission")
    if monitor:
        return
    admitted = run.get("interaction_caller")
    from . import identity
    if run["steering"]["owner"] != "caller" or admitted != principal or not admitted or admitted.get("kind") != "external_parent":
        raise SteeringError("steering_not_owner", "Caller does not match original process binding")
    if identity.check_detail(admitted["identity"])[0] != "alive":
        raise SteeringError("caller_undecidable", "Original caller generation is not positively alive")


def _storage(root):
    from .workflows import WorkflowStore
    return WorkflowStore(root)


def target(run, storage=None, witness=None):
    if run.get("kind") != "builder":
        from .orchestrator_steering import current_target, targets
        if storage is None:
            raise SteeringError("steering_source_unavailable", "Orchestrator targets require the qualified tree store")
        if witness is not None:
            from .steering_schema import _target as qualify_target
            try:
                qualify_target(witness, run["workflow_run_id"])
                if witness["role"] == "worker":
                    from .worker_steering import current_target as worker_target
                    return worker_target(storage, run, witness)
                if witness["role"] != "orchestrator":
                    raise ValueError("Wrong steering role")
            except (ValueError, TypeError):
                raise SteeringError("malformed_command", "A complete issued orchestrator target is required") from None
            return current_target(storage, run, witness)
        choices = targets(storage, run)
        if not choices:
            raise SteeringError("steering_target_stale", "No current orchestrator checkpoint")
        return choices[0]
    s = run["steering"]
    return {"root_workflow_run_id": run["workflow_run_id"], "workflow_run_id": run["workflow_run_id"],
            "path": [run["workflow_run_id"]], "role": "builder", "node_id": "builder", "token_id": None,
            **ordinary_builder_identity(run), "decision_id": None,
            "checkpoint_revision": s["checkpoint_revision"], "goal_revision": run.get("goal_revision", 0),
            "definition_digest": _digest(run["definition"]).removeprefix("sha256:"), "assignment_revision": None, "assignment_digest": None,
            "draft_revision": run.get("draft_revision", 0), "source_revision": run.get("source_revision"),
            "source_workflow_id": (run.get("source_saved_definition") or {}).get("workflow_id"), "source_definition_digest": _digest(run.get("source_saved_definition")),
            "preview_hash": _digest(run.get("builder_draft", {})), "interaction_epoch": run["interaction_epoch"],
            "control_revision": s["control_revision"]}


def _history(run, kind, **fields):
    s = run["steering"]
    s["sequence"] += 1
    s["history"].append({"sequence": s["sequence"], "kind": kind, "timestamp": time.time(), **fields})


def _eligibility(run, monitor, storage=None):
    ledger = run["steering"]
    actions, refusals = [], []
    if monitor:
        actions.append("take_owner")
        from . import identity
        admitted = run.get("interaction_caller") or {}
        if admitted.get("kind") == "external_parent" and identity.check_detail(admitted["identity"])[0] == "alive":
            actions.append("release_owner")
    active = next((ep for ep in reversed(ledger["episodes"]) if ep["hold"]), None)
    if active:
        actions.append("dismiss")
    if run["status"] in ({"cancelled", "cancelling"} if run.get("kind") == "builder" else {"completed", "failed", "cancelled", "cancelling"}):
        return actions, [{"code": "workflow_terminal", "message": "Cancelled builders cannot issue conversations"}]
    if active and (active["state"] in UNSETTLED or any(t["state"] == "captured" for t in ledger["turns"])):
        return actions, [{"code": "steering_busy", "message": "Observe the same issuance; no replacement is supported"}]
    from .tasks import get_backend
    if run.get("kind") == "builder":
        candidate = run["definition"]["orchestrator"]
        if candidate.get("caller_approvals") or not get_backend(candidate["backend"]).capabilities.supports_turn_cap:
            return actions, [{"code": "steering_budget_unsupported", "message": "Selected adapter/settings cannot enforce the finite harness cap"}]
    if run.get("input_question") or run.get("pending_inputs") or run.get("orchestrator_usage_recovery"):
        return actions, [{"code": "steering_question_pending", "message": "Use the existing interaction endpoint first"}]
    if ledger["schema_version"] == 1 and (len(ledger["turns"]) >= ROOT_TURNS or len(ledger["requests"]) >= REQUESTS or len(ledger["history"]) >= 500 or len(ledger["episodes"]) >= EPISODES):
        return actions, [{"code": "steering_budget_exhausted", "message": "Legacy steering storage requires explicit epoch-scoped upgrade"}]
    if active:
        if active["state"] in {"awaiting_caller", "command_refused"} and time.time() - active["created_at"] <= 1800 and (ledger["schema_version"] == 2 or sum(t["episode_id"] == active["episode_id"] for t in ledger["turns"]) < EPISODE_TURNS):
            actions.append("reply")
        actions.append("retarget")
    else:
        actions.append("open")
    return actions, refusals


def get_workflow_steering(workflow_run_id, *, root=None, monitor=False):
    storage = _storage(root)
    principal = _transport(storage, monitor)
    run = storage.get_run(workflow_run_id, refresh_atomic=True)
    _authorize(run, principal, monitor)
    s = run["steering"]
    actions, refusals = _eligibility(run, monitor, storage)
    if run.get("kind") == "builder":
        issued_targets = [target(run)]
    else:
        from .orchestrator_steering import targets, admission
        issued_targets = targets(storage, run)
        from .worker_steering import targets as worker_targets
        issued_targets += worker_targets(storage, run)
        eligible = []
        target_refusals = []
        for issued in issued_targets:
            try:
                selected_admission = admission
                if issued["role"] == "worker":
                    from .worker_steering import admission as selected_admission
                candidate, _, _, _ = selected_admission(storage, run, {"target": issued, "allow_fresh_context": True})
                from .tasks import get_backend
                if (candidate.get("caller_approvals") and issued["role"] != "worker") or not get_backend(candidate["backend"]).capabilities.supports_turn_cap:
                    raise SteeringError("steering_budget_unsupported", "Target adapter/settings cannot enforce the finite harness cap")
                eligible.append(issued)
            except SteeringError as exc:
                target_refusals.append({"code": exc.code, "message": str(exc)})
        if not eligible:
            actions = [a for a in actions if a not in {"open", "reply", "retarget"}]
            refusals.extend(target_refusals or [{"code": "steering_target_stale", "message": "No current orchestration decision checkpoint"}])
    from .workflow_replan import targets as replan_targets, projection as replan_projection
    return {"steering_version": 1, "root_workflow_run_id": workflow_run_id, "owner": s["owner"],
            "interaction_epoch": run["interaction_epoch"], "control_revision": s["control_revision"],
            "targets": issued_targets, "episodes": copy.deepcopy(s["episodes"]), "sequence": s["sequence"],
            "pending_steering_count": sum(ep["hold"] for ep in s["episodes"]),
            "budgets": {"root_issuances": s.get("cumulative", {}).get("issuances", len(s["turns"])), "root_limit": ROOT_TURNS if s["schema_version"] == 1 else None, "episode_limit": EPISODE_TURNS if s["schema_version"] == 1 else None,
                        "provenance": "durable_nonrefundable_issuance", "harness_turn_cap_maximum": 8},
            "replan_targets": replan_targets(storage, run), "replans": [replan_projection(record) for record in s.get("replans", [])],
            "replan_accounting": copy.deepcopy(s.get("replan_accounting")), "replan_history_incomplete": s.get("replan_history_incomplete", False), "normal_execution_history_incomplete": s.get("normal_execution_history_incomplete", False), "replan_history_floor": s.get("replan_history_floor", 0),
            "eligible_actions": actions + (["upgrade"] if s["schema_version"] == 1 else []), "refusals": refusals,
            "request_schema_version": 2 if s["schema_version"] == 2 else 1,
            "protocol_upgrade": {"schema_version": 2, "ledger_epoch": 0, "action": "upgrade"} if s["schema_version"] == 1 else None,
            **({"ledger_epoch": s["ledger_epoch"], "history_floor": s["history_floor"], "cumulative": copy.deepcopy(s["cumulative"])} if s["schema_version"] == 2 else {}),
            "recovery": "Replay the exact request to observe the same issuance; uncertain delivery is never reissued"}


def get_workflow_steering_history(workflow_run_id, cursor=None, limit=50, *, root=None, monitor=False):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise SteeringError("malformed_command", "History limit must be 1 through 100")
    storage = _storage(root)
    principal = _transport(storage, monitor)
    run = storage.get_run(workflow_run_id, refresh_atomic=True)
    _authorize(run, principal, monitor)
    offset = 0
    if cursor is not None:
        try:
            rid, raw = cursor.split(":")
            if rid != workflow_run_id or not raw.isdecimal():
                raise ValueError()
            offset = int(raw)
        except (ValueError, AttributeError):
            raise SteeringError("malformed_command", "History cursor does not belong to this root") from None
    items = []
    size = 0
    for item in run["steering"]["history"]:
        if item["sequence"] <= offset:
            continue
        encoded = len(json.dumps(item, ensure_ascii=False).encode())
        if len(items) >= limit or size + encoded > HISTORY_BYTES:
            break
        items.append(copy.deepcopy(item)); size += encoded
    end = items[-1]["sequence"] if items else offset
    more = end < run["steering"]["sequence"]
    sequences = [h["sequence"] for h in run["steering"]["history"]]
    incomplete = bool(run["steering"].get("history_floor", 0) or any(h.get("kind") == "history_gap" for h in run["steering"]["history"]) or any(b != a + 1 for a, b in zip(sequences, sequences[1:])))
    gap = offset < run["steering"].get("history_floor", 0) or any(i.get("kind") == "history_gap" for i in items) or any(b != a + 1 for a, b in zip([offset] + [i["sequence"] for i in items], [i["sequence"] for i in items]))
    return {"root_workflow_run_id": workflow_run_id, "items": items, "has_more": more,
            "next_cursor": f"{workflow_run_id}:{end}" if more else None, "history_incomplete": incomplete, "gap": gap}


def _request(request):
    if type(request) is not dict:
        raise SteeringError("malformed_command", "Request must be a JSON object")
    try:
        encoded = json.dumps(request, ensure_ascii=True, allow_nan=False)
        if len(encoded.encode()) > 128 * 1024:
            raise ValueError("Request exceeds 128 KiB")
        value = strict_loads(encoded)
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise SteeringError("malformed_command", "Request must be bounded finite UTF-8 JSON") from None
    common = ({"ledger_epoch"} if value.get("schema_version") == 2 else set()) | {"schema_version", "request_id", "action", "expected_interaction_epoch", "expected_control_revision"}
    action = value.get("action")
    owner = action in {"take_owner", "release_owner", "upgrade"}
    required = common | ({"reason"} if owner else {"episode_id", "expected_steering_revision", "target"})
    allowed = required | (set() if owner else {"text", "allow_fresh_context", "reason", "supersedes_episode_id"})
    if not required <= set(value) or set(value) - allowed or value.get("schema_version") not in {1, 2} or type(value.get("schema_version")) is not int or action not in {"open", "reply", "retarget", "dismiss", "take_owner", "release_owner", "upgrade"}:
        raise SteeringError("malformed_command", "Invalid steering request fields")
    try:
        if str(uuid.UUID(value["request_id"])) != value["request_id"]:
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise SteeringError("malformed_command", "request_id must be a canonical UUID") from None
    for key in (("ledger_epoch",) if value["schema_version"] == 2 else ()) + ("expected_interaction_epoch", "expected_control_revision"):
        if type(value[key]) is not int or value[key] < 0:
            raise SteeringError("malformed_command", "Expected revisions must be nonnegative integers")
    if action == "upgrade" and (value["schema_version"] != 2 or value["ledger_epoch"] != 0):
        raise SteeringError("malformed_command", "Upgrade requires schema_version2 and ledger_epoch0")
    if owner:
        if type(value["reason"]) is not str or not value["reason"].strip() or len(value["reason"].encode()) > TEXT_BYTES:
            raise SteeringError("malformed_command", "Ownership changes require bounded reason")
    else:
        if action != "retarget" and "supersedes_episode_id" in value or action != "dismiss" and "reason" in value:
            raise SteeringError("malformed_command", "Fields do not belong to this steering action")
        if action == "open":
            if value["episode_id"] is not None or value["expected_steering_revision"] is not None:
                raise SteeringError("malformed_command", "Open requires null episode/revision")
        elif type(value["episode_id"]) is not str or type(value["expected_steering_revision"]) is not int or value["expected_steering_revision"] < 1:
            raise SteeringError("malformed_command", "Episode/revision required")
        if action in {"open", "reply", "retarget"}:
            if type(value.get("text")) is not str or not value["text"].strip() or len(value["text"].encode()) > TEXT_BYTES:
                raise SteeringError("malformed_command", "Steering text must be nonblank and at most 32 KiB")
        elif "text" in value:
            raise SteeringError("malformed_command", "Dismiss does not accept text")
        if type(value.get("allow_fresh_context", False)) is not bool:
            raise SteeringError("malformed_command", "allow_fresh_context must be boolean")
        if type(value["target"]) is not dict:
            raise SteeringError("malformed_command", "A complete issued target is required")
    if not owner:
        value.setdefault("allow_fresh_context", False)
    return value


async def steer_workflow(workflow_run_id, request, *, root=None, monitor=False):
    storage = _storage(root)
    principal = await asyncio.to_thread(_transport, storage, monitor)
    request = _request(request)
    initial = storage.get_run(workflow_run_id, refresh_atomic=True)
    _authorize(initial, principal, monitor)
    key = f'{request["ledger_epoch"]}:{request["request_id"]}' if request["schema_version"] == 2 else request["request_id"]
    admitted_epoch = None
    if initial.get("steering", {}).get("replan_accounting") and key not in initial["steering"]["requests"]:
        from .steering_window import check_namespace
        check_namespace(initial["steering"], request)
        admitted_epoch = request.get("ledger_epoch")
        from .replan_retention import maintain as reserve_history
        reserve_history(storage, workflow_run_id, reserve_text=2 * len(request.get("text", "").encode()) + (64 * 1024 if request["action"] in {"open", "reply", "retarget"} else 0))
    digest = _digest({k: v for k, v in request.items() if k != "request_id"})
    scheduled = False
    result = None
    binding = _digest({"state_root": str(storage.root), "principal": "native_human" if monitor else principal, "epoch": request["expected_interaction_epoch"]})
    def mutate(run):
        nonlocal result, scheduled
        from .steering_admission import admit
        result, scheduled = admit(storage, run, principal, monitor, request, digest, binding, admitted_epoch=admitted_epoch)
    storage.update_run(workflow_run_id, mutate, "steering_requested")
    from .steering_lifecycle import maintain
    maintain(storage, workflow_run_id)
    if scheduled:
        from .workflows import _launch
        _launch(storage, workflow_run_id)
    return result


def _episode(run, turn):
    return next(ep for ep in run["steering"]["episodes"] if ep["episode_id"] == turn["episode_id"])


def pending_turn(run):
    return next((t for t in run.get("steering", {}).get("turns", []) if t["state"] in {"queued", "reserved", "spawn_intent", "running", "unknown"}), None)


def preview_witness(run, task_id):
    """Called after the exact own preview mutation and checkpoint increment, under root CAS."""
    turn = next((t for t in run.get("steering", {}).get("turns", []) if t["task_id"] == task_id and t["state"] in {"spawn_intent", "running"}), None)
    if turn is None:
        return
    # A preview cannot repair an already stale external draft or revoked epoch.
    before = turn["commit_target"]
    now = target(run)
    if before["draft_revision"] != now["draft_revision"] - 1 or before["checkpoint_revision"] != now["checkpoint_revision"] - 1 or any(before[k] != now[k] for k in before if k not in {"draft_revision", "checkpoint_revision", "preview_hash"}):
        turn["witness_stale"] = True
        return
    turn["preview_receipts"].append({"draft_revision": now["draft_revision"], "preview_hash": now["preview_hash"]})
    turn["commit_target"] = now
    turn["commit_target_ref"] = "commit-target:" + uuid.uuid4().hex


def preview_response(run, task_id):
    turn = next((t for t in run.get("steering", {}).get("turns", []) if t["task_id"] == task_id), None)
    if not turn:
        return {}
    return {"commit_target_ref": turn["commit_target_ref"], "commit_target": copy.deepcopy(turn["commit_target"])}


def _source_check(storage, run):
    if run.get("source_name") and run.get("source_saved_definition") is not None:
        try:
            current = storage.get(run["source_name"])
        except (OSError, ValueError, KeyError):
            raise SteeringError("steering_source_unavailable", "Saved builder source cannot be verified", next_action="Start a new builder from the refreshed source") from None
        if current != run["source_saved_definition"]:
            raise SteeringError("steering_target_stale", "Saved builder source changed", next_action="Start a new builder from the refreshed source")


async def run_builder_turn(supervisor, run_id):
    from .steering_runner import run_builder_turn as execute
    try:
        return await execute(supervisor, run_id)
    finally:
        from .steering_lifecycle import maintain
        maintain(supervisor.store, run_id)


def summary(run):
    """Small additive projection; never exposes private bindings or caller prose."""
    ledger = run.get("steering")
    if not ledger:
        return {}
    episodes = ledger["episodes"]
    latest = episodes[-1] if episodes else None
    return {"steering_summary": {"steering_version": 1, "owner": ledger["owner"],
             "interaction_epoch": run["interaction_epoch"], "control_revision": ledger["control_revision"],
             "sequence": ledger["sequence"], "episode_id": latest["episode_id"] if latest else None,
             "state": latest["state"] if latest else "idle", "steering_revision": latest["steering_revision"] if latest else None,
             "hold": held(run), "latest_reply_ref": latest["latest_reply_ref"] if latest else None, "replan_history_incomplete": ledger.get("replan_history_incomplete", False), "normal_execution_history_incomplete": ledger.get("normal_execution_history_incomplete", False)},
            "pending_steering_count": sum(ep["hold"] for ep in episodes)}


async def run_orchestrator_turn(supervisor, run_id):
    from .orchestrator_steering import run_turn
    try:
        return await run_turn(supervisor, run_id)
    finally:
        from .steering_lifecycle import maintain
        maintain(supervisor.store, run_id)
