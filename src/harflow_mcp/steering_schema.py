"""Pure fail-closed qualification of persisted builder steering authority."""
from __future__ import annotations

import json
import math
import re
import uuid

STATES = {"expired", "target_terminal", "classifying", "queued_boundary", "turn_reserved", "discussing", "delivery_unknown", "awaiting_caller", "command_refused", "committed", "dismissed", "stale", "dismiss_pending_settlement"}
TARGET_KEYS = set("root_workflow_run_id workflow_run_id path role node_id token_id activation_id task_id decision_id checkpoint_revision goal_revision definition_digest assignment_revision assignment_digest draft_revision source_revision source_workflow_id source_definition_digest preview_hash interaction_epoch control_revision".split())


def _require(condition, field):
    if not condition:
        raise ValueError("Malformed steering " + field)


def _counter(value, field, positive=False):
    _require(type(value) is int and value >= (1 if positive else 0), field)


def _id(value, field):
    from .workflow_run_documents import identifier
    try:
        identifier(value)
    except ValueError:
        raise ValueError("Malformed steering " + field) from None


def _digest(value, field):
    _require(type(value) is str and re.fullmatch(r"sha256:[a-f0-9]{64}", value), field)


def _timestamp(value, field):
    _require(type(value) in {int, float} and math.isfinite(value) and value > 0, field)


def _target(value, root_id):
    _require(type(value) is dict and set(value) == TARGET_KEYS, "target fields")
    if value["role"] == "worker":
        from .worker_steering import validate_target
        validate_target(value, root_id)
        return
    if value["role"] == "orchestrator":
        _require(value["root_workflow_run_id"] == root_id and type(value["path"]) is list and 1 <= len(value["path"]) <= 64 and value["path"][0] == root_id and value["path"][-1] == value["workflow_run_id"] and len(set(value["path"])) == len(value["path"]), "orchestrator target path")
        for item in value["path"]:
            _id(item, "path run")
        for key in ("node_id", "token_id", "decision_id"):
            _id(value[key], "orchestrator " + key)
        for key in ("activation_id", "task_id"):
            if value[key] is not None:
                _id(value[key], key)
        for key in ("checkpoint_revision", "goal_revision", "interaction_epoch", "control_revision"):
            _counter(value[key], key)
        _require(type(value["definition_digest"]) is str and re.fullmatch(r"[a-f0-9]{64}", value["definition_digest"]), "definition digest")
        _digest(value["assignment_digest"], "orchestrator tree digest")
        for key in ("assignment_revision", "draft_revision", "source_revision", "source_workflow_id", "source_definition_digest", "preview_hash"):
            _require(value[key] is None, "orchestrator inapplicable " + key)
        return
    _require(value["root_workflow_run_id"] == root_id == value["workflow_run_id"] and value["path"] == [root_id] and value["role"] == "builder" and value["node_id"] == "builder", "target root/role")
    for key in ("token_id", "decision_id", "assignment_revision", "assignment_digest"):
        _require(value[key] is None, "unsupported target " + key)
    for key in ("activation_id", "task_id"):
        if value[key] is not None:
            _id(value[key], "target " + key)
    for key in ("checkpoint_revision", "goal_revision", "draft_revision", "interaction_epoch", "control_revision"):
        _counter(value[key], "target " + key)
    if value["source_revision"] is not None:
        _counter(value["source_revision"], "target source_revision")
    _require(value["source_workflow_id"] is None or type(value["source_workflow_id"]) is str and bool(value["source_workflow_id"]), "target source_workflow_id")
    # Existing definition_hash is unprefixed; target preserves its exact authority.
    _require(type(value["definition_digest"]) is str and re.fullmatch(r"[a-f0-9]{64}", value["definition_digest"]), "target definition_digest")
    for key in ("source_definition_digest", "preview_hash"):
        _digest(value[key], key)


def captured_failure(observation, task_id=None):
    """Positive capture is required again when durable bytes become authority."""
    if type(observation) is not dict or task_id is not None and observation.get("task_id") != task_id:
        return "capture_unknown"
    if (type(observation.get("exit_code")) is not int or observation.get("output_drained") is not True
            or observation.get("outcome_unknown") is not False
            or type(observation.get("timed_out")) is not bool or type(observation.get("is_error")) is not bool
            or observation.get("execution_failure") is not None and type(observation.get("execution_failure")) is not str):
        return "capture_unknown"
    if observation.get("status") == "cancelled":
        return "cancelled"
    if observation["timed_out"]:
        return "timeout"
    diagnostic = observation.get("failure_diagnostic")
    if type(diagnostic) is dict and diagnostic.get("category") == "usage_limit":
        return "usage_limit"
    if diagnostic is not None:
        return "provider_failure"
    if (observation.get("status") != "completed" or observation["exit_code"] != 0
            or observation["is_error"] or observation.get("execution_failure")):
        return "provider_failure"
    return None


def _observation(turn):
    observation = turn.get("observation")
    if "observation" not in turn:
        return
    fields = set("task_id status exit_code output_drained timed_out outcome_unknown is_error execution_failure failure_diagnostic session_id usage num_turns total_cost_usd".split())
    _require(type(observation) is dict and set(observation) == fields, "observation fields")
    _require(observation["task_id"] == turn["task_id"], "observation task binding")
    _require(observation["status"] in {"starting", "running", "completed", "failed", "cancelled"}, "observation status")
    _require(observation["exit_code"] is None or type(observation["exit_code"]) is int, "observation exit code")
    for key in ("output_drained", "timed_out", "outcome_unknown", "is_error"):
        _require(type(observation[key]) is bool, "observation " + key)
    for key in ("execution_failure", "session_id"):
        _require(observation[key] is None or type(observation[key]) is str and len(observation[key].encode()) <= 4096, "observation " + key)
    for key in ("usage", "failure_diagnostic"):
        _require(observation[key] is None or type(observation[key]) is dict and len(json.dumps(observation[key], allow_nan=False).encode()) <= 16384, "observation " + key)
    if observation["num_turns"] is not None:
        _counter(observation["num_turns"], "observed num_turns")
    cost = observation["total_cost_usd"]
    _require(cost is None or type(cost) in {int, float} and math.isfinite(cost) and cost >= 0, "observed cost")
    usage = turn["usage"]
    _require(usage.get("provenance") == ("observed" if observation["usage"] is not None else "unavailable"), "usage provenance")
    for stored, observed in (("usage", "usage"), ("cost_usd", "total_cost_usd"), ("num_turns", "num_turns")):
        _require(usage.get(stored) == observation[observed] and type(usage.get(stored)) is type(observation[observed]), "usage observation binding")
    if turn["state"] in {"captured", "settled"} and turn.get("outcome") in {None, "conversation", "committed"}:
        _require(captured_failure(observation, turn["task_id"]) is None and type(turn.get("final")) is str, "positive successful final capture")


def validate_ledger(value, run):
    required = set("schema_version control_revision checkpoint_revision sequence episodes requests turns commits history owner".split())
    modern = type(value) is dict and value.get("schema_version") == 2
    if modern:
        from .steering_window import WINDOW_FIELDS
        required |= WINDOW_FIELDS
        for extension in ("replan_accounting", "replan_history_incomplete", "normal_execution_history_incomplete", "replan_history_floor", "replan_compaction_pending"):
            if extension in value:
                required.add(extension)
        if "replans" in value:
            required.add("replans")
        if "compaction_pending" in value:
            required.add("compaction_pending")
    _require(type(value) is dict and set(value) == required and type(value["schema_version"]) is int and value["schema_version"] in {1, 2}, "schema")
    if modern:
        for key in ("ledger_epoch", "closed_epoch", "history_floor"):
            _counter(value[key], key)
        _require(value["closed_epoch"] < value["ledger_epoch"], "closed namespace")
        cumulative = value["cumulative"]
        _require(type(cumulative) is dict and set(cumulative) in ({"issuances", "retired_turns", "observed_cost_usd", "observed_num_turns", "unknown_usage", "reserved_harness_turns"}, {"issuances", "retired_turns", "observed_cost_usd", "observed_num_turns", "unknown_usage", "reserved_harness_turns", "unknown_cost", "unknown_num_turns"}), "cumulative fields")
        for key in set(cumulative) - {"observed_cost_usd"}:
            _counter(cumulative[key], "cumulative " + key)
        _require(type(cumulative["observed_cost_usd"]) in {int, float} and math.isfinite(cumulative["observed_cost_usd"]) and cumulative["observed_cost_usd"] >= 0, "cumulative cost")
        _require(cumulative["issuances"] == cumulative["retired_turns"] + len(value["turns"]), "cumulative issuance")
    _counter(run.get("interaction_epoch"), "interaction_epoch")
    _require(value["owner"] in {"caller", "monitor"}, "owner")
    for key in ("control_revision", "checkpoint_revision", "sequence"):
        _counter(value[key], key)
    for key, limit in (("episodes", 32), ("turns", 32), ("commits", 32), ("history", 512)):
        _require(type(value[key]) is list and (modern or len(value[key]) <= limit) and all(type(v) is dict for v in value[key]), key)
    _require(type(value["requests"]) is dict and (modern or len(value["requests"]) <= 256), "requests")
    root_id = run["workflow_run_id"]
    episodes = {}
    for ep in value["episodes"]:
        fields = set("episode_id steering_revision state hold created_at target latest_reply_ref".split())
        _require(fields <= set(ep) and not set(ep) - fields - {"dismiss_requested", "reason", "supersedes_episode_id"}, "episode fields")
        _id(ep["episode_id"], "episode_id")
        _require(ep["episode_id"] not in episodes, "duplicate episode")
        episodes[ep["episode_id"]] = ep
        _counter(ep["steering_revision"], "episode revision", True)
        _timestamp(ep["created_at"], "episode created_at")
        _require(ep["state"] in STATES and type(ep["hold"]) is bool, "episode state/hold")
        _require(ep["latest_reply_ref"] is None or type(ep["latest_reply_ref"]) is str and re.fullmatch(r"history:[1-9][0-9]*", ep["latest_reply_ref"]), "reply ref")
        if "dismiss_requested" in ep:
            _require(type(ep["dismiss_requested"]) is bool, "dismiss_requested")
        if "supersedes_episode_id" in ep:
            _require(ep["supersedes_episode_id"] in episodes and ep["supersedes_episode_id"] != ep["episode_id"], "successor episode link")
        _target(ep["target"], root_id)
    turns = {}
    tasks = set()
    per_episode = {}
    for turn in value["turns"]:
        required_turn = set("turn_id task_id episode_id request_id state target commit_target commit_target_ref candidate text created_at allow_fresh_context usage previous_session steering_revision preview_receipts harness_turn_cap boundary_task_id".split())
        optional = {"revoked_by_replan_id", "witness_stale", "observation", "final", "final_loss", "outcome", "dispatch_error", "not_started", "boundary_tasks", "session_owner_run_id", "handoff", "action_commit", "context_delivery", "observed_task_id", "response_deadline"}
        _require(required_turn <= set(turn) and not set(turn) - required_turn - optional, "turn fields")
        _id(turn["turn_id"], "turn_id"); _id(turn["task_id"], "task_id")
        _require(turn["turn_id"] not in turns and turn["task_id"] not in tasks, "duplicate turn/task")
        turns[turn["turn_id"]] = turn; tasks.add(turn["task_id"])
        _require(turn["episode_id"] in episodes and turn["request_id"] in value["requests"], "turn links")
        per_episode[turn["episode_id"]] = per_episode.get(turn["episode_id"], 0) + 1
        _require(modern or per_episode[turn["episode_id"]] <= 8, "episode turn budget")
        _require(turn["state"] in {"queued", "reserved", "spawn_intent", "running", "unknown", "captured", "settled"}, "turn state")
        _target(turn["target"], root_id); _target(turn["commit_target"], root_id)
        worker = turn["target"]["role"] == "worker"
        orchestration = turn["target"]["role"] == "orchestrator"
        if worker:
            from .worker_steering import validate_turn
            validate_turn(turn, run)
        extension = {"boundary_tasks", "session_owner_run_id", "handoff", "action_commit", "context_delivery"}
        if orchestration:
            _require({"boundary_tasks", "session_owner_run_id", "handoff"} <= set(turn), "orchestrator issuance fields")
            _id(turn["session_owner_run_id"], "session owner")
            _require(type(turn["boundary_tasks"]) is list and len(turn["boundary_tasks"]) <= 64, "boundary tasks")
            boundary_ids = set()
            for boundary in turn["boundary_tasks"]:
                _require(type(boundary) is dict and set(boundary) == {"workflow_run_id", "activation_id", "task_id"}, "boundary fields")
                for key in boundary:
                    _id(boundary[key], "boundary " + key)
                _require(boundary["task_id"] not in boundary_ids, "duplicate boundary generation")
                boundary_ids.add(boundary["task_id"])
            _require(turn["boundary_task_id"] == (turn["boundary_tasks"][-1]["task_id"] if turn["boundary_tasks"] else None), "boundary primary task")
            handoff = turn["handoff"]
            _require(type(handoff) is dict and set(handoff) == {"intent_id", "state", "workflow_run_id"} and handoff["intent_id"] == turn["turn_id"] and handoff["workflow_run_id"] == turn["target"]["workflow_run_id"] and handoff["state"] in {"intent", "acknowledged"}, "hold handoff")
            if "context_delivery" in turn:
                from .workflow_context import CONTEXT_DELIVERY_VERSION
                receipt = turn["context_delivery"]
                allowed = {"version", "scope", "session_owner", "revision", "digest", "base_revision", "bootstrap_digest", "delivery_mode", "section_state", "owner_prefix"}
                _require(type(receipt) is dict and set(receipt) == allowed and type(receipt["version"]) is int and receipt["version"] == CONTEXT_DELIVERY_VERSION and receipt["scope"] == turn["target"]["workflow_run_id"] and receipt["delivery_mode"] == "bootstrap" and receipt["base_revision"] is None and type(receipt["section_state"]) is dict and type(receipt["owner_prefix"]) is str and type(receipt["session_owner"]) is str, "issued context receipt")
                _counter(receipt["revision"], "context revision", True)
                for key in ("digest", "bootstrap_digest"):
                    _require(type(receipt[key]) is str and re.fullmatch(r"[a-f0-9]{64}", receipt[key]), "context " + key)
            if "action_commit" in turn:
                intent = turn["action_commit"]
                _require(type(intent) is dict and set(intent) == {"state", "command", "call_id", "execution_id"} and intent["state"] in {"intent", "acknowledged", "refused"}, "action intent fields")
                _id(intent["call_id"], "action call"); _id(intent["execution_id"], "action execution")
                _require(type(intent["command"]) is dict and intent["command"].get("decision_id") == turn["target"]["decision_id"] and intent["command"].get("action") in {"continue", "complete", "failed", "needs_input", "inspect", "call_on_demand"}, "action intent command")
                _require(captured_failure(turn.get("observation"), turn["task_id"]) is None and type(turn.get("final")) is str, "action positive capture")
                from .steering_output import classify_final
                classified = classify_final(turn["final"])
                _require(classified["kind"] == "command" and classified["command"].get("command") == intent["command"] and classified["command"].get("turn_id") == turn["turn_id"] and classified["command"].get("commit_target_ref") == turn["commit_target_ref"], "action captured command binding")
                wrapper = classified["command"]
                expected_wrapper = {"episode_id": turn["episode_id"], "turn_id": turn["turn_id"], "commit_target_ref": turn["commit_target_ref"], "steering_revision": turn["steering_revision"], "role": "orchestrator", **{k: turn["commit_target"][k] for k in ("interaction_epoch", "checkpoint_revision", "goal_revision")}}
                _require(set(wrapper) == set(expected_wrapper) | {"command"} and all(type(wrapper[k]) is type(v) and wrapper[k] == v for k, v in expected_wrapper.items()), "action wrapper witness binding")
                if turn.get("context_delivery"):
                    from .workflow_context import acknowledged_receipt
                    _require(acknowledged_receipt(turn["context_delivery"], intent["command"].get("context_ack")) is not None, "action context acknowledgement")
                _require(intent["state"] != "acknowledged" or turn["state"] == "settled" and turn.get("outcome") == "committed", "action acknowledged outcome")
        elif not worker:
            _require(not set(turn) & extension, "builder orchestrator fields")
        _require(type(turn["commit_target_ref"]) is str and re.fullmatch(r"commit-target:[a-f0-9]{32}", turn["commit_target_ref"]), "commit_target_ref")
        _counter(turn["steering_revision"], "turn revision", True)
        _require(turn["steering_revision"] <= episodes[turn["episode_id"]]["steering_revision"], "turn future revision")
        _counter(turn["harness_turn_cap"], "harness_turn_cap", True)
        _require(turn["harness_turn_cap"] <= 8, "harness_turn_cap budget")
        _require(type(turn["allow_fresh_context"]) is bool and type(turn["text"]) is str and bool(turn["text"].strip()) and len(turn["text"].encode()) <= 32768, "turn input")
        _timestamp(turn["created_at"], "turn created_at")
        if "response_deadline" in turn:
            _timestamp(turn["response_deadline"], "response deadline")
        candidate = turn["candidate"]
        allowed_candidate = set("backend model reasoning_effort max_turns background_idle_seconds caller_approvals caller_write_approvals allowed_commands observe_repos git_https_rewrite group_report_output fallbacks instructions".split())
        _require(type(candidate) is dict and type(candidate.get("backend")) is str and bool(candidate["backend"]) and not set(candidate) - allowed_candidate, "candidate")
        for key in ("caller_approvals", "caller_write_approvals", "git_https_rewrite", "group_report_output"):
            if key in candidate:
                _require(type(candidate[key]) is bool, "candidate " + key)
        if candidate.get("max_turns") is not None:
            _counter(candidate["max_turns"], "candidate max_turns", True)
        for key in ("model", "reasoning_effort", "instructions"):
            if key in candidate:
                _require(candidate[key] is None or type(candidate[key]) is str, "candidate " + key)
        _require(type(turn["usage"]) is dict and turn["usage"].get("provenance") in {"unavailable", "observed"} and type(turn["usage"].get("reserved_harness_turns")) is int and turn["usage"].get("reserved_harness_turns") == turn["harness_turn_cap"], "usage reservation")
        if turn["boundary_task_id"] is not None:
            _id(turn["boundary_task_id"], "boundary task")
        previous = turn["previous_session"]
        if previous is not None:
            _require(type(previous) is dict and set(previous) == {"candidate", "task_id", "session_id"} and type(previous["candidate"]) is str, "previous session")
            _id(previous["task_id"], "previous task")
            _require(previous["session_id"] is None or type(previous["session_id"]) is str, "previous session id")
        _require(type(turn["preview_receipts"]) is list and len(turn["preview_receipts"]) <= 128, "preview receipts")
        last_revision = turn["target"]["draft_revision"]
        for preview in turn["preview_receipts"]:
            _require(type(preview) is dict and set(preview) == {"draft_revision", "preview_hash"}, "preview receipt")
            _counter(preview["draft_revision"], "preview revision")
            _require(preview["draft_revision"] == last_revision + 1, "preview order")
            _digest(preview["preview_hash"], "preview hash")
            last_revision = preview["draft_revision"]
        _require(turn["commit_target"]["draft_revision"] == last_revision, "preview witness revision")
        if "final" in turn:
            _require(type(turn["final"]) is str and len(turn["final"].encode()) <= 65536, "final")
        if "witness_stale" in turn:
            _require(type(turn["witness_stale"]) is bool, "witness_stale")
        if "outcome" in turn:
            _require(turn["outcome"] is None or type(turn["outcome"]) is str and 0 < len(turn["outcome"]) <= 100, "outcome")
        if "dispatch_error" in turn:
            _require(type(turn["dispatch_error"]) is str and len(turn["dispatch_error"]) <= 2000, "dispatch error")
        if "revoked_by_replan_id" in turn:
            _id(turn["revoked_by_replan_id"], "revoking replan")
            _require(modern and turn["state"] == "settled" and turn.get("outcome") == "request_expired" and turn.get("not_started") is True, "replan revocation shape")
            _require(turn["target"].get("workflow_run_id") == root_id and turn["target"].get("root_workflow_run_id") == root_id and turn["target"].get("path") == [root_id], "root replan revocation")
            record = next((r for r in value.get("replans", []) if r["replan_id"] == turn["revoked_by_replan_id"]), None)
            if record is not None:
                witness = record["withdrawal_witness"]
                _require(witness["queued_turn_id"] == turn["turn_id"] and witness["queued_task_id"] == turn["task_id"] and witness["activation_id"] == turn["target"].get("activation_id") and record["target"] == turn["target"], "exact replan revocation provenance")
        if "not_started" in turn:
            _require(turn["not_started"] is True, "positive not-started proof")
        if "observed_task_id" in turn:
            _require(turn["observed_task_id"] is None or type(turn["observed_task_id"]) is str, "observed task identity")
            _require(turn["observed_task_id"] != turn["task_id"] and turn["state"] == "unknown" and turn.get("outcome") == "capture_unknown", "mismatched capture refusal")
        _observation(turn)
        if turn["state"] in {"captured", "settled"} and "observation" not in turn:
            before_spawn = not any(a.get("steering_turn_id") == turn["turn_id"] for a in run["activations"])
            permitted_prestart = before_spawn and turn.get("outcome") in {"steering_target_stale", "request_expired", "dismissed", "workflow_terminal"}
            permitted_notstarted = turn.get("not_started") is True and turn.get("outcome") in {"steering_budget_unsupported", "steering_source_unavailable", "provider_not_started", "workflow_terminal", "request_expired", "cancelled"}
            _require(permitted_prestart or permitted_notstarted, "missing capture or positive not-started evidence")
        if turn["state"] == "settled":
            _require(type(turn.get("outcome")) is str, "settled outcome")
    for key, receipt in value["requests"].items():
        try:
            identity_key = key.split(":", 1)[-1] if modern else key
            valid_uuid = str(uuid.UUID(identity_key)) == identity_key
        except (ValueError, TypeError, AttributeError):
            valid_uuid = False
        required_receipt = set("request_id payload_digest episode_id steering_revision interaction_epoch control_revision sequence state turn_id task_id accepted scheduled replayed next_action principal_binding".split())
        if "retired_replan" in receipt:
            _require(receipt["retired_replan"] is True and "replan_id" in receipt, "retired replan receipt")
            required_receipt.add("retired_replan")
        if "replan_id" in receipt:
            _id(receipt["replan_id"], "request replan")
            required_receipt.add("replan_id")
        if modern:
            required_receipt.add("ledger_epoch")
            _counter(receipt.get("ledger_epoch"), "receipt ledger epoch")
            _require(receipt["ledger_epoch"] <= value["ledger_epoch"], "receipt future epoch")
            _require(key == f'{receipt["ledger_epoch"]}:{identity_key}' or receipt["ledger_epoch"] == 0 and key == identity_key, "scoped receipt namespace")
            if "retired_turn" in receipt:
                _require(receipt["retired_turn"] is True, "retired turn proof")
                required_receipt.add("retired_turn")
        _require(valid_uuid and type(receipt) is dict and set(receipt) == required_receipt and receipt["request_id"] == identity_key, "request identity/fields")
        _digest(receipt["payload_digest"], "payload digest"); _digest(receipt["principal_binding"], "principal binding")
        for name in ("interaction_epoch", "control_revision", "sequence"):
            _counter(receipt[name], "receipt " + name)
        for name in ("accepted", "scheduled", "replayed"):
            _require(type(receipt[name]) is bool, "receipt " + name)
        _require(receipt["accepted"] and not receipt["replayed"], "receipt accepted identity")
        if receipt["episode_id"] is not None:
            _require(receipt["episode_id"] in episodes, "receipt episode")
            _counter(receipt["steering_revision"], "receipt revision", True)
        if receipt["turn_id"] is not None and not receipt.get("retired_turn"):
            _require(receipt["turn_id"] in turns and turns[receipt["turn_id"]]["request_id"] == key and turns[receipt["turn_id"]]["task_id"] == receipt["task_id"], "receipt turn")
    if modern and "compaction_pending" in value:
        pending = value["compaction_pending"]
        _require(type(pending) is dict and set(pending) == {"turn_ids", "effect_turn_ids"}, "compaction intent fields")
        for key in pending:
            _require(type(pending[key]) is list and len(pending[key]) <= len(turns) and len(set(pending[key])) == len(pending[key]), "compaction intent ids")
            for tid in pending[key]:
                _require(tid in turns and turns[tid]["state"] == "settled", "compaction settled proof")
                _require(not turns[tid].get("action_commit") or turns[tid]["action_commit"]["state"] == "acknowledged", "compaction unresolved effect")
                _require(turns[tid]["task_id"] not in {v.get("task_id") for v in run.get("sessions", {}).values() if type(v) is dict}, "compaction current session")
        _require(set(pending["effect_turn_ids"]) <= set(pending["turn_ids"]), "compaction effect subset")
        for tid in pending["effect_turn_ids"]:
            _require(not turns[tid].get("action_commit") or turns[tid]["action_commit"]["state"] == "acknowledged", "compaction acknowledged effect")
    if "replan_accounting" in value:
        account = value["replan_accounting"]
        _require(type(account) is dict and set(account) == {"accepted", "retired_details", "retired_never_started", "provenance"} and account["provenance"] == "durable_explicit_replan_and_positive_no_start", "replan accounting")
        for key in ("accepted", "retired_details", "retired_never_started"):
            _counter(account[key], "replan " + key)
        _require(account["accepted"] == account["retired_details"] + len(value.get("replans", [])) and account["retired_never_started"] <= account["retired_details"], "replan cumulative binding")
        for key in ("replan_history_incomplete", "normal_execution_history_incomplete"):
            _require(type(value.get(key)) is bool, "normal history gaps")
        _counter(value.get("replan_history_floor"), "replan history floor")
    if "replan_compaction_pending" in value:
        pending = value["replan_compaction_pending"]
        _require(type(pending) is dict and set(pending) == {"replan_ids", "predecessor_ids", "certificates"} and all(type(pending[k]) is list and len(pending[k]) == len(set(pending[k])) for k in ("replan_ids", "predecessor_ids")) and type(pending["certificates"]) is list, "replan compaction intent")
        certificate_ids = set()
        certificate_predecessors = set()
        for ref in pending["certificates"]:
            _require(type(ref) is dict and set(ref) == {"replan_id", "predecessor_execution_id", "certificate_digest"}, "pending certificate reference")
            _id(ref["replan_id"], "pending replan certificate")
            _id(ref["predecessor_execution_id"], "pending predecessor certificate")
            _digest(ref["certificate_digest"], "pending certificate digest")
            _require(ref["replan_id"] not in certificate_ids, "duplicate pending certificate")
            certificate_ids.add(ref["replan_id"])
            certificate_predecessors.add(ref["predecessor_execution_id"])
        _require(set(pending["replan_ids"]) <= certificate_ids and set(pending["predecessor_ids"]) <= certificate_predecessors, "complete planned permanent certificate references")
        planned = [r for r in value.get("replans", []) if r["replan_id"] in pending["replan_ids"]]
        _require(len(planned) == len(pending["replan_ids"]) and all(r["phase"] == "replan_committed" and r.get("quiescence") for r in planned) and all(any(a["id"] == aid and a.get("status") == "not_started" and a.get("worker_result_required") is True for a in run["activations"]) for aid in pending["predecessor_ids"]), "exact replan prune intent")
    for receipt in value["requests"].values():
        if receipt.get("replan_id"):
            retained = any(r["replan_id"] == receipt["replan_id"] for r in value.get("replans", []))
            _require(retained != bool(receipt.get("retired_replan")), "explicit retained or retired replan receipt")
    if "replans" in value:
        from .replan_schema import records
        records(value["replans"], run)
        for record in value["replans"]:
            revoked = turns.get(record["withdrawal_witness"]["queued_turn_id"])
            _require(revoked is not None and revoked.get("revoked_by_replan_id") == record["replan_id"], "retained replan response revocation marker")
    committed = set()
    for commit in value["commits"]:
        _require(set(commit) == {"turn_id", "episode_id", "commit_target_ref", "definition_digest"} and commit["turn_id"] in turns and commit["turn_id"] not in committed, "commit identity")
        turn = turns[commit["turn_id"]]; committed.add(commit["turn_id"])
        _require(turn["episode_id"] == commit["episode_id"] and turn["commit_target_ref"] == commit["commit_target_ref"], "commit witness")
        _digest(commit["definition_digest"], "committed definition digest")
    sequence = value["history_floor"] if modern else 0
    for entry in value["history"]:
        _counter(entry.get("sequence"), "history sequence", True)
        _require((entry["sequence"] > sequence if modern else entry["sequence"] == sequence + 1) and type(entry.get("kind")) is str, "history order")
        _timestamp(entry.get("timestamp"), "history timestamp")
        sequence = entry["sequence"]
    _require(sequence == value["sequence"], "history tail")
    for turn in turns.values():
        if "observation" in turn:
            captures = [h for h in value["history"] if h.get("kind") == "final_captured" and h.get("turn_id") == turn["turn_id"]]
            _require(bool(captures) and captures[-1].get("observation") == turn["observation"] and captures[-1].get("text") == turn.get("final"), "captured history binding")
    _require(sum(t["state"] in {"queued", "reserved", "spawn_intent", "running", "unknown", "captured"} for t in turns.values()) <= 1, "concurrent issuance")
    _require(sum(len(t["text"].encode()) + len(t.get("final", "").encode()) for t in turns.values()) <= 1024 * 1024, "retained text budget")
    _require(len(json.dumps(value, ensure_ascii=False).encode()) <= 4 * 1024 * 1024, "metadata budget")
