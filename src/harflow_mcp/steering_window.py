"""Bounded steering storage. Epoch closure and pruning are separate durable writes.

Call ``close_namespace`` in a committed update before ``compact``. An old epoch
can replay a retained exact receipt, but can never admit a missing request.
"""
from __future__ import annotations

import copy
import json

TURN_WINDOW = 32
RECEIPT_WINDOW = 64
TEXT_LIMIT = 1024 * 1024
METADATA_LIMIT = 4 * 1024 * 1024
WINDOW_FIELDS = {"ledger_epoch", "history_floor", "closed_epoch", "cumulative"}


def upgrade(run):
    """Qualify v1 first; preserve every existing proof and close its namespace."""
    ledger = run["steering"]
    if ledger["schema_version"] == 2:
        return
    from .steering_schema import validate_ledger
    validate_ledger(ledger, run)
    ledger.update(schema_version=2, ledger_epoch=1, closed_epoch=0,
                  history_floor=0, cumulative={"issuances": len(ledger["turns"]),
                  "retired_turns": 0, "observed_cost_usd": 0.0,
                  "observed_num_turns": 0, "unknown_usage": 0,
                  "reserved_harness_turns": 0, "unknown_cost": 0, "unknown_num_turns": 0})
    for receipt in ledger["requests"].values():
        receipt["ledger_epoch"] = 0


def close_namespace(run):
    """Advance namespace atomically; caller MUST persist before pruning."""
    ledger = run["steering"]
    if ledger["schema_version"] != 2:
        upgrade(run)
        return
    ledger["closed_epoch"] = ledger["ledger_epoch"]
    ledger["ledger_epoch"] += 1


def protected_turn_ids(run, settled_effect_turn_ids=frozenset(), protected_task_ids=frozenset()):
    ledger = run["steering"]
    sessions = {v.get("task_id") for v in run.get("sessions", {}).values() if isinstance(v, dict)} | set(protected_task_ids)
    protected = {r["withdrawal_witness"]["queued_turn_id"] for r in ledger.get("replans", [])}
    for turn in ledger["turns"]:
        # Effect claims and crosslinks outlive conversation retention. Never infer
        # settlement from a terminal-looking TaskRecord or a missing task.
        if (turn["state"] != "settled" or turn["task_id"] in sessions
                or (turn.get("action_commit") and (turn["action_commit"].get("state") != "acknowledged" or turn["turn_id"] not in settled_effect_turn_ids))
                or (any(c["turn_id"] == turn["turn_id"] for c in ledger["commits"]) and run.get("kind") != "builder" and turn["turn_id"] not in settled_effect_turn_ids)):
            protected.add(turn["turn_id"])
    return protected


def retired_task_ids(run):
    """Candidates only. Persist managed bindings before pruning activations."""
    ledger = run["steering"]
    protected = protected_turn_ids(run)
    settled = [t for t in ledger["turns"] if t["turn_id"] not in protected]
    return {t["task_id"] for t in settled[:-TURN_WINDOW]}


def _size(value):
    return len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode())


def retained_text_bytes(run):
    ledger = run['steering']
    from .conversation_workflows import extension_text_bytes
    return (extension_text_bytes(run)+sum(len(t.get('text', '').encode()) + len(t.get('final', '').encode()) for t in ledger['turns'])
            + sum(len(h[key].encode()) for h in ledger['history'] for key in ('text','reason') if isinstance(h.get(key), str))
            + sum(len(r['reason'].encode()) for r in ledger.get('replans', [])))


def compact(run, *, managed_task_ids=frozenset(), settled_effect_turn_ids=frozenset(), protected_task_ids=frozenset(), retirement_turn_ids=None, reserve_text_bytes=96 * 1024):
    """Prune positively settled closed namespaces, retaining all live proofs.

    ``managed_task_ids`` is positive evidence of separately persisted ownership,
    not permission to delete TaskRecords. No activation is retired without it.
    Raises on protected-byte pressure; storage caller must discard failed mutation.
    """
    ledger = run["steering"]
    from .conversation_workflows import compact_conversations
    compact_conversations(run,reserve_text_bytes=reserve_text_bytes,reserve_metadata_bytes=128*1024,defer_pressure=True)
    if ledger["schema_version"] != 2:
        return
    protected = protected_turn_ids(run, settled_effect_turn_ids, protected_task_ids)
    removable = [t for t in ledger["turns"] if t["turn_id"] not in protected
                 and (retirement_turn_ids is None or t["turn_id"] in retirement_turn_ids)
                 and ledger["requests"][t["request_id"]]["ledger_epoch"] <= ledger["closed_epoch"]
                 and (not any(a.get("steering_turn_id") == t["turn_id"] or any(task.get("steering_turn_id") == t["turn_id"] for task in a.get("tasks", [])) for a in run.get("activations", []))
                      or t["task_id"] in managed_task_ids)]
    remove = set()
    def text_size():
        return retained_text_bytes(run)
    def retire(turn):
        remove.add(turn["turn_id"])
        ledger["turns"].remove(turn)
        ledger["commits"][:] = [c for c in ledger["commits"] if c["turn_id"] != turn["turn_id"]]
        cumulative = ledger["cumulative"]
        cumulative["retired_turns"] += 1
        usage = turn["usage"]
        cumulative["unknown_cost"] = cumulative.get("unknown_cost", 0) + (usage.get("cost_usd") is None)
        cumulative["unknown_num_turns"] = cumulative.get("unknown_num_turns", 0) + (usage.get("num_turns") is None)
        cumulative["reserved_harness_turns"] += usage["reserved_harness_turns"]
        if usage.get("provenance") != "observed":
            cumulative["unknown_usage"] += 1
        # Cost and response-turn observations are independent of token usage.
        # A provider may disclose either metric while token accounting is absent.
        if usage.get("cost_usd") is not None:
            cumulative["observed_cost_usd"] += usage["cost_usd"]
        if usage.get("num_turns") is not None:
            cumulative["observed_num_turns"] += usage["num_turns"]
    # Retain at most 32 unprotected settled turns; protected evidence is additional.
    recent_settled = {t['turn_id'] for t in [t for t in ledger['turns'] if t['state'] == 'settled'][-TURN_WINDOW:]}
    for turn in removable:
        if turn['turn_id'] not in recent_settled:
            retire(turn)
    # Raw history duplicates captures and request prose. Preserve history for all
    # retained turns and latest episode reply; absolute sequence holes expose gaps.
    def trim(*, byte_pressure=False):
        linked = {t["turn_id"] for t in ledger["turns"]}
        used_requests = {t["request_id"] for t in ledger["turns"]} | {r["request_key"] for r in ledger.get("replans", [])}
        receipts = ledger["requests"]
        candidates = [k for k, r in receipts.items() if k not in used_requests and r["ledger_epoch"] <= ledger["closed_epoch"]]
        for key in candidates[:len(candidates) if byte_pressure else max(0, len(receipts) - RECEIPT_WINDOW)]:
            del receipts[key]
        # Receipts retain their episode and nullable turn identity; removed turns
        # leave an explicit receipt-only replay witness.
        for r in receipts.values():
            if r["turn_id"] in remove:
                r["retired_turn"] = True
        episodes = {t["episode_id"] for t in ledger["turns"]} | {r["episode_id"] for r in receipts.values()}
        delivery_links={b['steering_episode_id'] for b in ledger.get('conversation_delivery',{}).values() if b.get('steering_episode_id')}
        for episode in ledger['episodes']:
            if episode['episode_id'] in delivery_links and episode['episode_id'] not in episodes and not episode['hold']:
                # Role/ownership retirement above already proved that no inline
                # turn remains necessary. Keep only the delivery handoff link.
                episode['latest_reply_ref']=None
        episodes |= delivery_links
        ledger["episodes"][:] = [ep for ep in ledger["episodes"] if ep["hold"] or ep["episode_id"] in episodes]
        for ep in ledger["episodes"]:
            if ep.get("supersedes_episode_id") not in {e["episode_id"] for e in ledger["episodes"]}:
                ep.pop("supersedes_episode_id", None)
        replan_ids = {r["replan_id"] for r in ledger.get("replans", [])}
        retained_request_ids = {receipts[key]["request_id"] for key in used_requests if key in receipts}
        reply_sequences = {int(ep["latest_reply_ref"].split(":")[1]) for ep in ledger["episodes"] if ep.get("latest_reply_ref")}
        ledger["history"][:] = [h for h in ledger["history"] if h.get("turn_id") in linked or h.get("request_id") in retained_request_ids or h.get("replan_id") in replan_ids or h["sequence"] in reply_sequences or (h["sequence"] > ledger["sequence"] - 64 and not isinstance(h.get("text"), str) and not isinstance(h.get("reason"), str))]
        if ledger["history"] and ledger["history"][-1]["sequence"] < ledger["sequence"]:
            ledger["history"].append({"sequence": ledger["sequence"], "kind": "history_gap", "timestamp": __import__("time").time()})
        floor = ledger["history"][0]["sequence"] - 1 if ledger["history"] else ledger["sequence"]
        ledger["history_floor"] = max(ledger["history_floor"], floor)
        run["activations"][:] = [a for a in run.get("activations", []) if a.get("steering_turn_id") not in remove]
        for activation in run.get("activations", []):
            activation["tasks"] = [t for t in activation.get("tasks", []) if not (t.get("steering_turn_id") in remove and t["task_id"] in managed_task_ids)]
    trim()
    if text_size() > TEXT_LIMIT - reserve_text_bytes or _size(run) > METADATA_LIMIT - 128 * 1024:
        trim(byte_pressure=True)
    for turn in removable:
        if text_size() <= TEXT_LIMIT - reserve_text_bytes and _size(run) <= METADATA_LIMIT - 128 * 1024:
            break
        trim(byte_pressure=True)
        if text_size() <= TEXT_LIMIT - reserve_text_bytes and _size(run) <= METADATA_LIMIT - 128 * 1024:
            break
        if turn in ledger["turns"]:
            retire(turn)
            trim(byte_pressure=True)
    if text_size() > TEXT_LIMIT - reserve_text_bytes or _size(run) > METADATA_LIMIT - 128 * 1024:
        from .workflow_steering import SteeringError
        raise SteeringError("steering_queue_full", "Protected steering evidence cannot fit bounded storage; observe unresolved issuances")
    return remove


def check_namespace(ledger, request):
    from .workflow_steering import SteeringError
    if ledger["schema_version"] == 1 and request.get("schema_version", 1) == 2:
        raise SteeringError("steering_source_unavailable", "Legacy ledger requires qualified epoch-scoped upgrade before new-format requests")
    if ledger["schema_version"] == 2 and (request.get("schema_version", 1) != 2 or request.get("ledger_epoch") != ledger["ledger_epoch"]):
        raise SteeringError("request_expired", "Request namespace expired; refresh status and explicitly create a new epoch-scoped request")


def retirement_bindings(run, *, settled_effect_turn_ids=frozenset()):
    """Bounded candidates for the owner-receipt writer before root compaction."""
    protected = protected_turn_ids(run, settled_effect_turn_ids)
    tasks = {t["task_id"] for t in run["steering"]["turns"] if t["turn_id"] not in protected}
    return {t["task_id"]: {"root_workflow_run_id": run["workflow_run_id"],
            "turn_id": t["turn_id"], "ledger_epoch": run["steering"]["requests"][t["request_id"]]["ledger_epoch"],
            "candidate": copy.deepcopy(t["candidate"]),
            "session_head": copy.deepcopy(run.get("sessions", {}))}
            for t in run["steering"]["turns"] if t["task_id"] in tasks}
