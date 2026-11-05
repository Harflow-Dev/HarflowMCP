"""Pure synthetic retention and namespace oracles; no provider or task registry."""
import copy

import pytest

from harflow_mcp import steering_window as window
from harflow_mcp.workflow_steering import SteeringError


def ledger(count=600):
    turns = [{"turn_id": f"t{i}", "task_id": f"task{i}", "episode_id": "episode", "request_id": f"0:r{i}",
              "state": "settled", "text": "hello", "final": "reply", "usage": {"provenance": "observed", "reserved_harness_turns": 8, "cost_usd": .25, "num_turns": 1}}
             for i in range(count)]
    return {"workflow_run_id": "root", "activations": [], "sessions": {}, "steering": {
        "schema_version": 2, "ledger_epoch": 1, "closed_epoch": 0, "history_floor": 0,
        "sequence": count, "turns": turns, "commits": [], "episodes": [{"episode_id": "episode", "hold": True, "latest_reply_ref": f"history:{count}"}],
        "requests": {t["request_id"]: {"request_id": t["request_id"], "ledger_epoch": 0, "turn_id": t["turn_id"], "episode_id": "episode"} for t in turns},
        "history": [{"sequence": i + 1, "kind": "reply", "turn_id": f"t{i}"} for i in range(count)],
        "cumulative": {"issuances": count, "retired_turns": 0, "observed_cost_usd": 0., "observed_num_turns": 0, "unknown_usage": 0, "reserved_harness_turns": 0}}}


def test_windows_do_not_bound_lifetime_count():
    run = ledger()
    window.compact(run)
    s = run["steering"]
    assert len(s["turns"]) == 32
    assert len(s["requests"]) == 64
    assert s["cumulative"]["issuances"] == 600
    assert s["cumulative"]["retired_turns"] == 568
    assert s["cumulative"]["observed_cost_usd"] == 142
    assert s["history_floor"] > 0


def test_unknown_and_current_session_are_protected():
    run = ledger()
    run["steering"]["turns"][0]["state"] = "unknown"
    run["sessions"]["worker"] = {"task_id": "task1"}
    window.compact(run)
    assert {"t0", "t1"} <= {t["turn_id"] for t in run["steering"]["turns"]}


def test_namespace_closure_never_reopens_old_missing_request():
    run = ledger()
    old = {"schema_version": 2, "ledger_epoch": 1}
    window.check_namespace(run["steering"], old)
    window.close_namespace(run)
    with pytest.raises(SteeringError, match="namespace expired"):
        window.check_namespace(run["steering"], old)
    window.check_namespace(run["steering"], {"schema_version": 2, "ledger_epoch": 2})


def test_activation_cannot_retire_without_durable_managed_binding():
    run = ledger(40)
    run["activations"] = [{"steering_turn_id": t["turn_id"]} for t in run["steering"]["turns"]]
    before = copy.deepcopy(run)
    window.compact(run)
    assert len(run["activations"]) == 40
    assert run["steering"]["turns"] == before["steering"]["turns"]
    window.compact(run, managed_task_ids=window.retired_task_ids(run))
    assert len(run["activations"]) == 32


def test_explicit_upgrade_protocol_shape_and_epoch_required():
    import uuid
    from harflow_mcp.workflow_steering import _request
    request = {"schema_version": 2, "ledger_epoch": 0, "request_id": str(uuid.uuid4()), "action": "upgrade",
               "expected_interaction_epoch": 0, "expected_control_revision": 0, "reason": "Adopt bounded epoch-scoped replay"}
    assert _request(request) == request
    with pytest.raises(SteeringError):
        _request({k: v for k, v in request.items() if k != "ledger_epoch"})
    with pytest.raises(SteeringError):
        _request({**request, "ledger_epoch": 1})


def test_expired_text_tail_has_explicit_absolute_gap_marker():
    run = ledger(40)
    run["steering"]["sequence"] = 41
    run["steering"]["history"].append({"sequence": 41, "kind": "requested", "text": "Expired request prose"})
    window.compact(run)
    assert run["steering"]["history"][-1]["sequence"] == 41
    assert run["steering"]["history"][-1]["kind"] == "history_gap"


def test_missing_cost_and_response_turn_counts_remain_unknown_after_retirement():
    run = ledger(40)
    run["steering"]["turns"][0]["usage"].update(cost_usd=None, num_turns=None)
    window.compact(run)
    assert run["steering"]["cumulative"]["unknown_cost"] == 1
    assert run["steering"]["cumulative"]["unknown_num_turns"] == 1


def test_known_metrics_survive_missing_token_usage_after_retirement():
    run = ledger(40)
    run['steering']['turns'][0]['usage'].update(provenance='unavailable', usage=None, cost_usd=.25, num_turns=2)
    window.compact(run)
    totals = run['steering']['cumulative']
    assert totals['observed_cost_usd'] == 2.0
    assert totals['observed_num_turns'] == 9
    assert totals['unknown_usage'] == 1
    assert totals['unknown_cost'] == totals['unknown_num_turns'] == 0
