"""Independent retention transition model; no providers or user state are used.

The oracle is the set of admitted epoch/request identities and independently
summed usage, not production reservation/normalization helpers.
"""
import copy
import hashlib
import json
import uuid

import pytest

from harflow_mcp import steering_window as window
from harflow_mcp.workflow_steering import SteeringError


def digest(value):
    return "sha256:" + hashlib.sha256(str(value).encode()).hexdigest()


def empty_model():
    return {"workflow_run_id": "root", "kind": "workflow", "status": "paused", "sessions": {}, "activations": [],
        "steering": {"schema_version": 2, "ledger_epoch": 1, "closed_epoch": 0, "history_floor": 0,
            "control_revision": 0, "checkpoint_revision": 0, "sequence": 0, "owner": "caller",
            "episodes": [], "requests": {}, "turns": [], "commits": [], "history": [],
            "cumulative": {"issuances": 0, "retired_turns": 0, "observed_cost_usd": 0.0,
                           "observed_num_turns": 0, "unknown_usage": 0, "reserved_harness_turns": 0}}}


def append_response(run, number, *, state="settled", final="Bounded final", text="Explicit message", known=True, committed=False, activation=True):
    ledger = run["steering"]
    epoch = ledger["ledger_epoch"]
    rid = str(uuid.UUID(int=number + 1))
    tid, pid, eid = f"turn-{number}", f"provider-{number}", f"episode-{number}"
    turn = {"turn_id": tid, "task_id": pid, "episode_id": eid, "request_id": rid,
        "state": state, "text": text, "final": final,
        "usage": {"provenance": "observed" if known else "unavailable", "cost_usd": 0.25 if known else None,
                  "num_turns": 2 if known else None, "reserved_harness_turns": 8}}
    ledger["turns"].append(turn)
    ledger["cumulative"]["issuances"] += 1
    ledger["requests"][rid] = {"request_id": rid, "ledger_epoch": epoch, "turn_id": tid, "task_id": pid,
        "episode_id": eid, "payload_digest": digest(number), "accepted": True, "scheduled": True,
        "replayed": False, "state": "turn_reserved", "sequence": ledger["sequence"] + 1}
    for kind in ("requested", "final_captured", "classified"):
        ledger["sequence"] += 1
        ledger["history"].append({"sequence": ledger["sequence"], "kind": kind, "turn_id": tid,
            "request_id": rid, "episode_id": eid, "text": final if kind == "final_captured" else text if kind == "requested" else None})
    ledger["episodes"].append({"episode_id": eid, "hold": state != "settled", "state": "dismissed" if state == "settled" else "delivery_unknown",
        "steering_revision": 1, "latest_reply_ref": "history:" + str(ledger["sequence"] - 1)})
    if activation:
        run["activations"].append({"id": tid, "role": "orchestrator", "steering_turn_id": tid,
            "status": "completed" if state == "settled" else "uncertain", "tasks": [{"task_id": pid, "status": "completed" if state == "settled" else "uncertain"}]})
    if committed:
        turn["action_commit"] = {"state": "acknowledged", "command": {"action": "inspect"}}
        ledger["commits"].append({"turn_id": tid, "episode_id": eid, "commit_target_ref": "target-" + tid,
            "definition_digest": digest(number)})
    return turn


def all_task_ids(run):
    return {t["task_id"] for t in run["steering"]["turns"]}


def compact_closed(run):
    window.close_namespace(run)
    return window.compact(run, managed_task_ids=all_task_ids(run))


def test_six_hundred_explicit_identities_use_rolling_storage_not_a_count_quota():
    run = empty_model()
    identities = set()
    for n in range(600):
        turn = append_response(run, n)
        identities.add((run["steering"]["ledger_epoch"], turn["request_id"]))
        compact_closed(run)
        assert run["steering"]["cumulative"]["issuances"] == n + 1
        assert len(run["steering"]["turns"]) <= 32
        assert len(run["steering"]["requests"]) <= 64
        assert len(run["activations"]) <= 32
    assert len(identities) == 600
    ledger = run["steering"]
    assert ledger["cumulative"]["retired_turns"] + len(ledger["turns"]) == 600
    assert ledger["history_floor"] > 0
    assert ledger["sequence"] == 1800
    assert [h["sequence"] for h in ledger["history"]] == sorted({h["sequence"] for h in ledger["history"]})


def test_compaction_requires_managed_binding_before_removing_task_associations():
    run = empty_model()
    for n in range(40):
        append_response(run, n)
    window.close_namespace(run)
    before = copy.deepcopy(run["activations"])
    window.compact(run)
    assert run["activations"] == before
    assert len(run["steering"]["turns"]) == 40
    window.compact(run, managed_task_ids=all_task_ids(run))
    assert len(run["activations"]) == len(run["steering"]["turns"]) == 32


def test_unknown_oldest_turn_and_its_receipt_survive_many_settled_responses():
    run = empty_model()
    unknown = copy.deepcopy(append_response(run, 0, state="unknown", known=False))
    receipt = copy.deepcopy(run["steering"]["requests"][unknown["request_id"]])
    for n in range(1, 100):
        append_response(run, n)
        compact_closed(run)
    ledger = run["steering"]
    assert next(t for t in ledger["turns"] if t["turn_id"] == unknown["turn_id"]) == unknown
    assert ledger["requests"][unknown["request_id"]] == receipt
    assert any(a.get("steering_turn_id") == unknown["turn_id"] for a in run["activations"])
    assert len(ledger["turns"]) <= 33
    assert len(ledger["history"]) < 150


def test_retired_epoch_cannot_admit_missing_uuid_even_after_lost_compaction_response():
    run = empty_model()
    old_epoch = run["steering"]["ledger_epoch"]
    window.close_namespace(run)
    after_close = copy.deepcopy(run)
    for restored in (run, after_close):
        with pytest.raises(SteeringError) as refused:
            window.check_namespace(restored["steering"], {"schema_version": 2, "ledger_epoch": old_epoch,
                "request_id": str(uuid.uuid4())})
        assert refused.value.code == "request_expired"
        assert not restored["steering"]["turns"]
        window.check_namespace(restored["steering"], {"schema_version": 2, "ledger_epoch": old_epoch + 1,
            "request_id": str(uuid.uuid4())})


def test_usage_checkpoint_never_refunds_retired_turns_or_treats_unknown_as_zero():
    run = empty_model()
    for n in range(80):
        append_response(run, n, known=n % 3 != 0)
    compact_closed(run)
    ledger = run["steering"]
    retained = ledger["turns"]
    cumulative = ledger["cumulative"]
    assert cumulative["issuances"] == 80
    assert cumulative["retired_turns"] + len(retained) == 80
    assert cumulative["reserved_harness_turns"] + sum(t["usage"]["reserved_harness_turns"] for t in retained) == 640
    assert cumulative["observed_cost_usd"] + sum(t["usage"]["cost_usd"] or 0 for t in retained) == 53 * 0.25
    assert cumulative["unknown_usage"] + sum(t["usage"]["provenance"] != "observed" for t in retained) == 27


def test_byte_pressure_reclaims_settled_receipts_and_episode_text_before_backpressure():
    run = empty_model()
    for n in range(80):
        append_response(run, n, text="x" * (32 * 1024), final="y" * (64 * 1024))
        compact_closed(run)
        assert len(json.dumps(run).encode()) <= 4 * 1024 * 1024
    assert run["steering"]["cumulative"]["issuances"] == 80
    assert sum(len(t["text"]) + len(t["final"]) for t in run["steering"]["turns"]) <= 1024 * 1024
