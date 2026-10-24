"""Observable section delivery protocol checks; no model or paid calls."""

import json

from harflow_mcp.workflow_context import (
    acknowledged_receipt,
    context_ack,
    render_decision_context,
)
from harflow_mcp.workflow_inspection import instruction_page
from harflow_mcp.workflow_prompt_delivery import project_orchestrator_context
from harflow_mcp import ctl


def _context():
    return {
        "workflow_run_id": "run",
        "decision_id": "d1",
        "original_request": "ship the objective",
        "workflow_purpose": "purpose",
        "workflow_graph": {"nodes": [
            {"id": "start", "type": "start"},
            {"id": "worker", "type": "agent", "purpose": "collect evidence"},
            {"id": "end", "type": "end"},
        ]},
        "technical_plan": "plan",
        "checklist": [{"id": "one", "status": "pending"}],
        "recent_decisions": [{"decision_id": "old", "action": "continue", "reason": "because"}],
        "valid_continuations": [{"continuation_id": "worker", "requires_prompt": True}],
        "node_instructions": {"worker": "Collect evidence verbatim; preserve constraints."},
    }


def _acknowledge(rendered):
    return acknowledged_receipt(rendered.receipt, context_ack(rendered.receipt))


def _payload(rendered):
    return json.loads(rendered.prompt)


def test_first_delivery_is_full_and_receipt_has_bounded_section_metadata():
    rendered = render_decision_context(_context(), session_owner="candidate:session", scope="run")
    payload = _payload(rendered)
    assert rendered.receipt["version"] == 2
    assert rendered.receipt["section_state"]["bootstrap"]["mode"] == "full"
    assert rendered.receipt["section_state"]["technical_plan"]["mode"] == "full"
    assert payload["checkpoint"]["technical_plan"] == "plan"
    assert payload["checkpoint"]["node_instructions"]["worker"] == _context()["node_instructions"]["worker"]
    assert rendered.receipt["section_state"]["checklist"]["length"] == len(_context()["checklist"])


def test_unchanged_sections_become_references_and_reconstruct_exactly():
    source = _context()
    first = render_decision_context(source, session_owner="candidate:session", scope="run")
    baseline = _acknowledge(first)
    source["decision_id"] = "d2"
    resumed = render_decision_context(source, session_owner="candidate:session", scope="run", baseline=baseline)
    payload = _payload(resumed)
    assert resumed.receipt["section_state"]["technical_plan"]["mode"] == "reference"
    assert payload["checkpoint"]["technical_plan"]["mode"] == "reference"
    assert payload["checkpoint"]["checklist"]["mode"] == "reference"
    assert payload["checkpoint"]["node_instructions"]["worker"]["mode"] == "reference"
    assert resumed.receipt["section_state"]["technical_plan"]["digest"] == first.receipt["section_state"]["technical_plan"]["digest"]


def test_changed_section_is_full_while_unrelated_section_stays_reference():
    source = _context()
    first = render_decision_context(source, session_owner="candidate:session", scope="run")
    baseline = _acknowledge(first)
    source["decision_id"] = "d2"
    source["technical_plan"] = "revised plan"
    resumed = render_decision_context(source, session_owner="candidate:session", scope="run", baseline=baseline)
    payload = _payload(resumed)
    assert resumed.receipt["section_state"]["technical_plan"]["mode"] == "full"
    assert payload["checkpoint"]["technical_plan"] == "revised plan"
    assert payload["checkpoint"]["checklist"]["mode"] == "reference"


def test_list_append_contains_only_new_items_and_exact_prefix_metadata():
    source = _context()
    source["recent_decisions"] = [{"decision_id": "a", "action": "continue", "reason": "r"}]
    first = render_decision_context(source, session_owner="candidate:session", scope="run")
    baseline = _acknowledge(first)
    source["decision_id"] = "d2"
    source["recent_decisions"].append({"decision_id": "b", "action": "continue", "reason": "s"})
    resumed = render_decision_context(source, session_owner="candidate:session", scope="run", baseline=baseline)
    section = resumed.receipt["section_state"]["recent_decisions"]
    encoded = _payload(resumed)["checkpoint"]["recent_decisions"]
    assert section["mode"] == "append"
    assert encoded["base_revision"] == first.receipt["section_state"]["recent_decisions"]["revision"]
    assert encoded["items"] == [source["recent_decisions"][-1]]


def test_prefix_replacement_is_full_and_sliding_window_does_not_append():
    source = _context()
    source["checklist"] = [{"id": str(i), "status": "pending"} for i in range(3)]
    first = render_decision_context(source, session_owner="candidate:session", scope="run")
    baseline = _acknowledge(first)
    source["decision_id"] = "d2"
    source["checklist"] = [{"id": "new", "status": "pending"}] + source["checklist"]
    resumed = render_decision_context(source, session_owner="candidate:session", scope="run", baseline=baseline)
    assert resumed.receipt["section_state"]["checklist"]["mode"] == "full"
    assert _payload(resumed)["checkpoint"]["checklist"] == source["checklist"]


def test_mismatched_session_or_repair_forces_full_sections():
    source = _context()
    first = render_decision_context(source, session_owner="candidate:session", scope="run")
    baseline = _acknowledge(first)
    source["decision_id"] = "d2"
    for kwargs in (
        {"session_owner": "other", "classification": "normal"},
        {"session_owner": "candidate:session", "classification": "protocol_repair"},
    ):
        rendered = render_decision_context(source, scope="run", baseline=baseline, **kwargs)
        assert rendered.receipt["delivery_mode"] == "bootstrap"
        assert all(meta["mode"] == "full" for meta in rendered.receipt["section_state"].values())


def test_acknowledgement_is_exact_and_preserves_section_receipt():
    rendered = render_decision_context(_context(), session_owner="candidate:session", scope="run")
    ack = context_ack(rendered.receipt)
    acknowledged = acknowledged_receipt(rendered.receipt, ack)
    assert acknowledged["section_state"] == rendered.receipt["section_state"]
    assert set(ack) == {"version", "scope", "session_owner", "revision", "digest", "base_revision"}


def test_instruction_inspection_pages_verbatim_current_run_text():
    text = "constraint 🙂 " * 80
    run = {"workflow_run_id": "run", "definition": {"nodes": [{"id": "worker", "instructions": text}]}}
    first = instruction_page(run, {"node_id": "worker", "view": "instructions", "limit": 31})
    chunks = [first["chunk"]]
    page = first
    while page["next_cursor"]:
        page = instruction_page(run, {"node_id": "worker", "view": "instructions", "limit": 31, "cursor": page["next_cursor"]})
        chunks.append(page["chunk"])
    assert "".join(chunks) == text
    assert first["content_sha256"] == page["content_sha256"]
    assert page["has_more"] is False


def test_orchestrator_projection_warns_on_missing_purpose_and_keeps_worker_text():
    run = {"definition": {"nodes": [
        {"id": "worker", "type": "agent", "instructions": "Exact specialist constraints", "agent": {"backend": "codex"}},
    ]}}
    context = {"workflow_graph": {"nodes": [{"id": "worker", "type": "agent", "instructions": "Exact specialist constraints"}]}, "valid_continuations": [{"node_id": "worker", "requires_prompt": True}]}
    projected = project_orchestrator_context(run, context)
    assert projected["workflow_graph"]["nodes"][0]["purpose"] == "Exact specialist constraints"
    assert projected["context_warnings"]
    assert projected["node_instructions"]["worker"] == "Exact specialist constraints"


def test_dynamic_authority_stays_inline_after_stable_sections_are_acknowledged():
    source = _context()
    first = render_decision_context(source, session_owner="candidate:session", scope="run")
    baseline = _acknowledge(first)
    source.update(decision_id="d2", valid_continuations=[{"continuation_id": "new", "requires_prompt": True}], current_stage={"node_id": "worker"}, sessions={"worker": "fresh"})
    resumed = render_decision_context(source, session_owner="candidate:session", scope="run", baseline=baseline)
    checkpoint = _payload(resumed)["checkpoint"]
    assert checkpoint["valid_continuations"] == source["valid_continuations"]
    assert checkpoint["current_stage"] == source["current_stage"]
    assert checkpoint["sessions"] == source["sessions"]
    assert checkpoint["checklist"]["mode"] == "reference"


def test_ack_baseline_mismatch_forces_all_known_sections_full():
    source = _context()
    first = render_decision_context(source, session_owner="candidate:session", scope="run")
    baseline = _acknowledge(first)
    cases = [
        {"baseline": None},
        {"baseline": {**baseline, "version": 1}},
        {"baseline": {**baseline, "scope": "other"}},
        {"baseline": {**baseline, "session_owner": "other"}},
        {"baseline": {**baseline, "bootstrap_digest": "changed"}},
    ]
    for case in cases:
        rendered = render_decision_context(source, session_owner="candidate:session", scope="run", baseline=case["baseline"])
        assert rendered.receipt["delivery_mode"] == "bootstrap"
        assert all(meta["mode"] == "full" for meta in rendered.receipt["section_state"].values())


def test_reference_accounting_distinguishes_source_and_delivered_bytes():
    source = _context()
    first = render_decision_context(source, session_owner="candidate:session", scope="run")
    baseline = _acknowledge(first)
    source["decision_id"] = "d2"
    resumed = render_decision_context(source, session_owner="candidate:session", scope="run", baseline=baseline)
    metadata = resumed.accounting["section_delivery"]["technical_plan"]
    encoded = json.dumps(_payload(resumed)["checkpoint"]["technical_plan"], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert metadata["source_bytes"] == len(json.dumps("plan", ensure_ascii=False).encode())
    assert metadata["delivered_bytes"] == len(encoded.encode())
    assert metadata["source_bytes"] < metadata["delivered_bytes"]


def test_parallel_candidate_projection_keeps_both_entry_instructions_across_resume():
    definition = {
        "nodes": [
            {"id": "left", "type": "agent", "instructions": "LEFT exact 🙂 constraints", "agent": {"backend": "codex"}},
            {"id": "right", "type": "agent", "instructions": "RIGHT exact\nconstraints", "agent": {"backend": "codex"}},
        ]
    }
    run = {"definition": definition}
    context = {
        "workflow_graph": {"nodes": [{"id": "left", "type": "agent"}, {"id": "right", "type": "agent"}]},
        "valid_continuations": [{"continuation_id": "split", "branch_continuations": [
            {"node_id": "left", "requires_prompt": True}, {"node_id": "right", "requires_prompt": True}
        ]}],
    }
    projected = project_orchestrator_context(run, context)
    assert projected["node_instructions"] == {"left": "LEFT exact 🙂 constraints", "right": "RIGHT exact\nconstraints"}


def test_ctl_validation_reports_missing_purpose_warning(tmp_path, capsys, monkeypatch, request):
    from harflow_mcp import state_ownership
    from harflow_mcp.state_layout import resolve_layout
    state_root = tmp_path / "state"
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(state_root))
    state_root.mkdir()
    session = state_ownership.state_session(resolve_layout(root=state_root))
    session.__enter__()
    request.addfinalizer(lambda: session.__exit__(None, None, None))
    definition = {
        "name": "warning",
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "worker", "type": "agent", "instructions": "Do the work", "agent": {"backend": "codex"}},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "worker"},
            {"id": "finish", "source": "worker", "target": "end"},
        ],
    }
    path = tmp_path / "definition.json"
    path.write_text(json.dumps(definition))
    assert ctl.main(["workflow-validate", "--definition", str(path), "--json"]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert result["valid"] is True
    assert any("purpose" in warning for warning in result.get("warnings", []))
    definition["nodes"][1]["purpose"] = "Validate the evidence"
    path.write_text(json.dumps(definition))
    assert ctl.main(["workflow-validate", "--definition", str(path), "--json"]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert result["valid"] is True
    assert not any("purpose" in warning for warning in result.get("warnings", []))
