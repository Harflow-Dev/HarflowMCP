"""Structured child conversation choices cannot grant worker or permission authority."""

import pytest

from polybridge import workflow_delegation as d
from polybridge import workflow_child_sessions as sessions
from polybridge import workflows as w
from test_workflow_delegation import storage
from test_workflow_run_node_execution import child_graph, parent_graph


def setup_run(storage, tmp_path, policy="agent_decides", mode="child"):
    child = storage.save("child", child_graph())
    definition = parent_graph(child["workflow_id"], mode=mode)
    if policy is not None:
        definition["nodes"][1]["child_session_policy"] = policy
    from polybridge.workflow_references import resolve_dependencies
    definition = w.validate_definition(definition)
    tree = resolve_dependencies(storage, definition=definition)
    run = storage.create_run(definition, "Request", tmp_path, dependency_tree=tree)
    run["runner_policy"] = "guided"
    node = run["definition"]["nodes"][0]
    token = {"id": "checkpoint", "decision_id": "decision", "node_id": "start", "stack": []}
    return run, node, token


def decision(**fields):
    return {"decision_id": "decision", "action": "continue", "reason": "Delegate", "next": [{"continuation_id": "to-call", "prompt": "New assignment", **fields}]}


@pytest.mark.parametrize("mode", ["fresh", "resume"])
def test_agent_choice_is_preserved_in_assignment(storage, tmp_path, monkeypatch, mode):
    run, node, token = setup_run(storage, tmp_path)
    monkeypatch.setattr(sessions, "offer", lambda *a, **k: {"eligible_sessions": [{"session_ref": "opaque"}], "unavailable_reason": ""})
    fields = {"child_session_mode": mode, "child_session_reason": "Useful context"}
    if mode == "resume":
        fields["child_session_ref"] = "opaque"
    normalized, warnings = d.normalize_decision(run, node, token, decision(**fields), root=storage.root)
    _, assignments, _ = d.validate_decision(run, node, token, normalized, False, root=storage.root)
    assert not warnings
    assert all(assignments["to-call"][key] == value for key, value in fields.items())


@pytest.mark.parametrize("fields", [{}, {"child_session_mode": "fresh"}, {"child_session_mode": "resume", "child_session_reason": "Reuse", "child_session_ref": "invented"}, {"child_session_mode": "fresh", "child_session_reason": "New", "child_session_ref": "opaque"}])
def test_agent_choice_requires_reason_and_issued_reference(storage, tmp_path, monkeypatch, fields):
    run, node, token = setup_run(storage, tmp_path)
    monkeypatch.setattr(sessions, "offer", lambda *a, **k: {"eligible_sessions": [{"session_ref": "opaque"}], "unavailable_reason": ""})
    with pytest.raises(w.WorkflowError):
        d.validate_decision(run, node, token, decision(**fields), False, root=storage.root)


def test_fixed_resume_unavailability_never_assigns_fresh(storage, tmp_path, monkeypatch):
    run, node, token = setup_run(storage, tmp_path, policy="resume")
    monkeypatch.setattr(sessions, "offer", lambda *a, **k: {"eligible_sessions": [], "unavailable_reason": "Latest source is uncertain"})
    with pytest.raises(w.WorkflowError, match="Latest source is uncertain"):
        d.validate_decision(run, node, token, decision(), False, root=storage.root)
    with pytest.raises(w.WorkflowError, match="fixed child"):
        d.validate_decision(run, node, token, decision(child_session_mode="fresh"), False, root=storage.root)


def test_current_mode_ignores_retained_preference_and_refuses_choice(storage, tmp_path):
    run, node, token = setup_run(storage, tmp_path, policy="resume", mode="current")
    choice = d.continuations(run, node, token, False, root=storage.root)[0]
    assert choice["child_session_policy"] == "inactive"
    assert "eligible_sessions" not in choice
    _, assignments, _ = d.validate_decision(run, node, token, decision(), False, root=storage.root)
    assert not any(key.startswith("child_session") for key in assignments["to-call"])
    with pytest.raises(w.WorkflowError, match="Unexpected continuation"):
        d.validate_decision(run, node, token, decision(child_session_mode="resume"), False, root=storage.root)


def test_child_choice_cannot_override_permissions(storage, tmp_path):
    run, node, token = setup_run(storage, tmp_path)
    with pytest.raises(w.WorkflowError, match="cannot override"):
        d.normalize_decision(run, node, token, decision(child_session_mode="fresh", child_session_reason="New", network=True), root=storage.root)


def test_optimized_child_guidance_preserves_contract_and_decision_reason():
    from polybridge.workflow_context import compact_recent_decisions, render_decision_context
    context = {"decision_id": "decision", "valid_continuations": [{"continuation_id": "child", "workflow": {}, "requires_prompt": True, "child_session_policy": "agent_decides"}]}
    import json
    prompt = json.loads(render_decision_context(context, session_owner="session", scope="new-child").prompt)
    assert "Fixed Child Resume never falls back Fresh" in prompt["guidance"]
    assert "child_session_ref" in prompt["guidance"]
    source = decision(child_session_mode="resume", child_session_reason="Keep useful context", child_session_ref="opaque")
    compact = compact_recent_decisions([source])[0]["next"][0]
    assert compact["child_session_reason"] == "Keep useful context"
    assert compact["child_session_ref"] == "opaque"
    assert "prompt" not in compact


@pytest.mark.parametrize("mode", ["fresh", "resume"])
def test_omitted_child_policy_defaults_agent_decides_with_both_choices(storage, tmp_path, monkeypatch, mode):
    run, node, token = setup_run(storage, tmp_path, policy=None)
    target = run["definition"]["nodes"][1]
    assert target["child_session_policy"] == "agent_decides"
    # Unsaved/older definitions also use the same omitted-value contract.
    target.pop("child_session_policy")
    monkeypatch.setattr(sessions, "offer", lambda *a, **k: {"eligible_sessions": [{"session_ref": "opaque"}], "unavailable_reason": ""})
    choice = d.continuations(run, node, token, False, root=storage.root)[0]
    assert choice["child_session_policy"] == "agent_decides"
    fields = {"child_session_mode": mode, "child_session_reason": "Explicit default-policy choice"}
    if mode == "resume":
        fields["child_session_ref"] = "opaque"
    _, assignments, _ = d.validate_decision(run, node, token, decision(**fields), False, root=storage.root)
    assert assignments["to-call"]["child_session_mode"] == mode
    with pytest.raises(w.WorkflowError, match="requires child_session_mode"):
        d.validate_decision(run, node, token, decision(), False, root=storage.root)


def test_explicit_fresh_is_retained(storage, tmp_path):
    run, node, token = setup_run(storage, tmp_path, policy="fresh")
    assert run["definition"]["nodes"][1]["child_session_policy"] == "fresh"
    _, assignments, _ = d.validate_decision(run, node, token, decision(), False, root=storage.root)
    assert assignments["to-call"]["child_session_mode"] == "fresh"


@pytest.mark.parametrize("mode", ["fresh", "resume"])
def test_direct_child_execute_issues_workflow_choice_fields(storage, tmp_path, monkeypatch, mode):
    run, _, token = setup_run(storage, tmp_path, policy=None)
    node = run["definition"]["nodes"][1]
    token.update(node_id=node["id"])
    monkeypatch.setattr(sessions, "offer", lambda *a, **k: {"eligible_sessions": [{"session_ref": "opaque"}], "unavailable_reason": ""})
    choice = d.continuations(run, node, token, True, root=storage.root)[0]
    assert choice["workflow"]["workflow_id"] == node["workflow_ref"]["workflow_id"]
    assert choice["child_session_policy"] == "agent_decides"
    entry = {"continuation_id": choice["continuation_id"], "prompt": "Converged child", "child_session_mode": mode, "child_session_reason": "Choose the child conversation"}
    if mode == "resume":
        entry["child_session_ref"] = "opaque"
    _, assignments, _ = d.validate_decision(run, node, token, {"decision_id": token["decision_id"], "action": "continue", "reason": "Converged inputs", "next": [entry]}, True, root=storage.root)
    assert assignments[choice["continuation_id"]]["child_session_mode"] == mode
