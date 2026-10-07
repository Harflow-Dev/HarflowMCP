"""Deterministic context delivery checks; no harness or model calls."""
import copy
import json

import pytest

from polybridge.workflow_context import (
    account_legacy_prompt, acknowledged_receipt, compact_recent_decisions,
    context_ack, render_decision_context, validate_context_ack,
)


def context():
    return {"workflow_run_id": "run", "decision_id": "d1", "original_request": "original objective",
            "workflow_purpose": "purpose", "workflow_graph": {"nodes": ["start", "end"]},
            "technical_plan": "plan", "technical_plan_execution_id": "plan-execution",
            "current_stage": {"phase": "routing", "node_id": "start"},
            "valid_continuations": [{"continuation_id": "execute:1", "requires_prompt": True}],
            "checklist": [{"id": "task", "status": "pending"}], "recovery_instructions": "caller consent",
            "input_results": [{"result_ref": "execution", "node_result": {"technical_plan": "plan"}}],
            "recent_decisions": [{"decision_id": "previous", "action": "continue", "reason": "why",
                                  "next": [{"continuation_id": "edge", "prompt": "old assignment" * 2000,
                                            "branch_assignments": [{"continuation_id": "branch", "prompt": "old branch"}]}]}]}


def test_compaction_is_lossless_for_decisions_and_does_not_mutate_source():
    source = context()
    before = copy.deepcopy(source)
    result = render_decision_context(source, session_owner=None, scope="run")
    parsed = json.loads(result.prompt)
    assert source == before
    assert "old assignment" not in result.prompt
    assert "old branch" not in result.prompt
    decisions = parsed["checkpoint"]["recent_decisions"]
    assert decisions[0]["reason"] == "why"
    assert decisions[0]["next"][0]["branch_assignments"] == [{"continuation_id": "branch"}]
    assert parsed["checkpoint"]["input_results"][0]["node_result"]["technical_plan"]["same_as"] == "technical_plan"
    assert parsed["checkpoint"]["technical_plan"] == "plan"
    assert result.accounting["serialized_bytes"] == len(result.prompt.encode("utf-8"))


def test_acknowledged_resume_omits_only_stable_bootstrap():
    initial = render_decision_context(context(), session_owner="run:candidate:session", scope="run")
    baseline = acknowledged_receipt(initial.receipt, context_ack(initial.receipt))
    next_context = context()
    next_context.update(decision_id="d2", checklist=[{"id": "task", "status": "done"}])
    resumed = render_decision_context(next_context, session_owner="run:candidate:session", scope="run", baseline=baseline)
    payload = json.loads(resumed.prompt)
    assert "bootstrap" not in payload
    assert payload["checkpoint"]["decision_id"] == "d2"
    assert payload["checkpoint"]["recovery_instructions"] == "caller consent"
    assert payload["checkpoint"]["checklist"][0]["status"] == "done"
    assert resumed.receipt["base_revision"] == 1
    assert resumed.receipt["revision"] == 2
    assert resumed.accounting["delivery_mode"] == "delta"


@pytest.mark.parametrize("change", ["session", "scope", "unacknowledged", "repair", "fresh", "graph"])
def test_incompatible_or_repaired_resume_bootstraps(change):
    source = context()
    initial = render_decision_context(source, session_owner="session", scope="scope")
    baseline = acknowledged_receipt(initial.receipt, context_ack(initial.receipt))
    kwargs = {"session_owner": "session", "scope": "scope", "baseline": baseline}
    if change == "session": kwargs["session_owner"] = "other"
    if change == "scope": kwargs["scope"] = "other"
    if change == "unacknowledged": baseline.pop("acknowledged")
    if change == "repair": kwargs["classification"] = "repair"
    if change == "fresh": kwargs["session_owner"] = None
    if change == "graph": source["workflow_graph"]["nodes"].append("changed")
    result = render_decision_context(source, **kwargs)
    assert result.receipt["delivery_mode"] == "bootstrap"
    assert result.receipt["base_revision"] is None


@pytest.mark.parametrize("field", ["version", "scope", "session_owner", "revision", "digest", "base_revision"])
def test_ack_must_match_every_receipt_field(field):
    receipt = render_decision_context(context(), session_owner="session", scope="scope").receipt
    ack = context_ack(receipt)
    assert validate_context_ack(ack, receipt)
    ack[field] = "invalid"
    assert not validate_context_ack(ack, receipt)
    assert acknowledged_receipt(receipt, ack) is None


def test_budget_preserves_authority_and_unavailable_evidence_with_visible_overflow():
    source = context()
    source["input_results"] = [{"result_ref": "execution", "evidence": "🙂" * 40000}]
    rendered = render_decision_context(source, session_owner=None, scope="scope")
    assert rendered.accounting["budget_overflow_bytes"] > 0
    assert rendered.accounting["compatibility_reasons"]
    assert json.loads(rendered.prompt)["checkpoint"]["input_results"] == source["input_results"]
    assert "caller consent" in rendered.prompt
    assert "execute:1" in rendered.prompt
    bounded = render_decision_context(source, session_owner=None, scope="scope", evidence_manifests={"execution": {"reader": "assigned-input", "cursor": None}})
    assert bounded.accounting["budget_overflow_bytes"] == 0
    manifest = json.loads(bounded.prompt)["checkpoint"]["input_results"][0]
    assert manifest["serialized_bytes"] > 65536
    assert manifest["inline_sha256"]
    assert manifest["retrieval"]["reader"] == "assigned-input"


def test_guidance_is_current_on_delta_and_conditionally_includes_parallel_and_inspection():
    source = context()
    first = render_decision_context(source, session_owner="session", scope="scope")
    assert "branch_assignments" not in json.loads(first.prompt)["guidance"]
    baseline = acknowledged_receipt(first.receipt, context_ack(first.receipt))
    source["valid_continuations"][0]["branch_continuations"] = ["branch"]
    source.update(inspections_remaining=1, settled_executions=[{"execution_id": "execution"}])
    next_turn = render_decision_context(source, session_owner="session", scope="scope", baseline=baseline)
    assert next_turn.receipt["delivery_mode"] == "delta"
    payload = json.loads(next_turn.prompt)
    assert "branch_assignments" in payload["guidance"]
    assert "inspect" in payload["examples"]
    assert payload["delivery"]["acknowledgement"] == context_ack(next_turn.receipt)


def test_deterministic_rendering_and_legacy_measurement():
    kwargs = {"session_owner": None, "scope": "scope"}
    assert render_decision_context(context(), **kwargs) == render_decision_context(context(), **kwargs)
    assert account_legacy_prompt("🙂", role="worker")["serialized_bytes"] == 4
    assert account_legacy_prompt("🙂", role="worker")["serialized_characters"] == 1
    with pytest.raises(ValueError): render_decision_context(context(), **kwargs, budget_bytes=0)
    assert compact_recent_decisions([{"reason": "why", "prompt": "secret"}]) == [{"reason": "why"}]


def test_partial_or_extra_ack_never_establishes_a_baseline():
    receipt = render_decision_context(context(), session_owner="session", scope="scope").receipt
    assert acknowledged_receipt(receipt, None) is None
    ack = context_ack(receipt)
    ack.pop("digest")
    assert acknowledged_receipt(receipt, ack) is None
    ack = {**context_ack(receipt), "extra": True}
    assert acknowledged_receipt(receipt, ack) is None
    ack = context_ack(receipt)
    ack["version"] = True  # bool equals integer 1 in Python; types still must agree.
    assert acknowledged_receipt(receipt, ack) is None


def test_truncated_plan_does_not_claim_full_evidence_equivalence():
    source = context()
    source["technical_plan_truncated"] = True
    result = render_decision_context(source, session_owner=None, scope="scope")
    assert json.loads(result.prompt)["checkpoint"]["input_results"][0]["node_result"]["technical_plan"] == "plan"
    assert "recent_decisions" in result.accounting["context_fields"]
    assert "workflow_graph" in result.accounting["context_fields"]
