"""Completion offers expose only actions that the current checkpoint can accept."""

import pytest

from harflow_mcp.workflow_delegation import _completion_offer, decision_prompt, validate_decision


def _run(**updates):
    value = {"pending": [{"id": "token"}], "joins": {}, "activations": [], "runner_policy": "guided", "definition": {"context_delivery": "optimized_v1", "nodes": [{"id": "end", "type": "end"}], "connections": []}}
    value.update(updates)
    return value


def test_clean_end_checkpoint_offers_complete():
    actions, blockers = _completion_offer(_run(), {"id": "end", "type": "end"}, {"id": "token"}, False, [], [])
    assert actions == ["failed", "needs_input", "complete"]
    assert blockers == []


@pytest.mark.parametrize("updates, node, execute, failure", [
    ({}, {"id": "work", "type": "agent"}, False, []),
    ({"pending": [{"id": "a"}, {"id": "b"}]}, {"id": "end", "type": "end"}, False, []),
    ({"joins": {"pair": {"join_id": "join"}}}, {"id": "end", "type": "end"}, False, []),
    ({}, {"id": "end", "type": "end"}, True, []),
    ({}, {"id": "end", "type": "end"}, False, [{"id": "failed-execution"}]),
    ({"goal_replanning_required": True}, {"id": "end", "type": "end"}, False, []),
])
def test_completion_is_omitted_with_actionable_blocker(updates, node, execute, failure):
    actions, blockers = _completion_offer(_run(**updates), node, {"id": "token"}, execute, failure, [])
    assert "complete" not in actions
    assert blockers


@pytest.mark.parametrize("optimized", [True, False])
def test_guidance_and_examples_do_not_offer_rejected_complete(optimized):
    context = {"decision_id": "d1", "valid_actions": ["failed", "needs_input"], "completion_blockers": ["A revised goal requires current-revision replanning before completion"]}
    text = decision_prompt(context)
    assert '"action": "complete"' not in text
    assert "completion_blockers" in text


def test_guidance_examples_omit_unavailable_inspect_and_specialist_actions():
    context = {"decision_id": "d1", "valid_actions": ["failed", "needs_input"], "completion_blockers": ["pending"]}
    text = decision_prompt(context)
    assert '"action": "inspect"' not in text
    assert '"action": "call_on_demand"' not in text


def test_validation_still_accepts_current_revision_goal_replan_complete():
    run = _run(goal={"objective": "objective"}, goal_replanning_required=True, goal_revision=2, goal_replanning_revision=2,
                activations=[{"id": "exec-1", "role": "node", "status": "completed", "tasks": [], "node_result": {"status": "succeeded", "evidence": ["verified"]}}])
    node = {"id": "end", "type": "end"}
    token = {"id": "token", "decision_id": "d1"}
    decision = {"decision_id": "d1", "action": "complete", "reason": "Replanned goal is complete", "goal_assessment": {"revision": 2, "status": "satisfied", "reason": "Evidence supports completion", "evidence_refs": ["exec-1"]}, "goal_replan": {"revision": 2, "plan": "Keep completed work and verify the revised objective."}}
    selected, updated, error = validate_decision(run, node, token, decision, False)
    assert error is None
    assert selected == []
    assert isinstance(updated, dict)
