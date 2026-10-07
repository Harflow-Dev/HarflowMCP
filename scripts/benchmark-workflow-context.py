#!/usr/bin/env python3
"""Deterministic injected-context measurements. No model calls or token estimates.

Run: uv run python scripts/benchmark-workflow-context.py
Output: JSON report containing serialized bytes, characters, and section breakdowns.
Fixture manifests model authorized immutable retrieval; they are not executable references.
"""
from __future__ import annotations
import copy
import json

from polybridge.workflow_context import acknowledged_receipt, context_ack, render_decision_context
from polybridge.workflow_delegation import decision_prompt


def fixtures():
    small = {"workflow_run_id": "fixture", "decision_id": "d1", "original_request": "Implement the requested change", "workflow_purpose": "Deterministic fixture", "workflow_graph": {"nodes": [{"id": "start", "type": "start"}, {"id": "work", "type": "agent"}, {"id": "end", "type": "end"}], "connections": []}, "routing_mode": "explicit", "routing_rules": "Choose issued continuations", "current_stage": {"node_id": "start", "phase": "routing"}, "valid_continuations": [{"continuation_id": "execute:work", "requires_prompt": True}], "recent_decisions": [], "input_results": [], "checklist": [], "inspections_remaining": 20, "settled_executions": []}
    yield "small", small, {}
    history = copy.deepcopy(small)
    history["recent_decisions"] = [{"decision_id": f"old-{i}", "action": "continue", "reason": "Verified outcome", "next": [{"continuation_id": "execute:work", "prompt": "HISTORICAL ASSIGNMENT " * 1000}]} for i in range(10)]
    yield "large-history", history, {}
    parallel = copy.deepcopy(history)
    parallel["valid_continuations"][0]["branch_continuations"] = [{"continuation_id": "left"}, {"continuation_id": "right"}]
    parallel["input_results"] = [{"result_ref": ref, "raw_output": "雪🙂 evidence " * 8000} for ref in ("left", "right")]
    manifests = {ref: {"workflow_run_id": "fixture", "execution_id": ref, "content_sha256": "fixture-digest", "total_characters": 96000} for ref in ("left", "right")}
    yield "parallel", parallel, manifests
    child = copy.deepcopy(history)
    child["workflow_scope"] = {"boundary": "child", "parent_workflow_run_id": "fixture", "orchestrator_mode": "current", "assignment": "Child objective"}
    yield "child", child, {}
    clarification = copy.deepcopy(history)
    clarification["recovery_instructions"] = "Caller clarified the requested acceptance criterion"
    clarification["recent_decisions"].append({"decision_id": "question", "action": "needs_input", "question": "Acceptance criterion?", "answer": "Keep compatibility", "reason": "Clarification required"})
    yield "clarification", clarification, {}
    yield "repair", history, {}
    yield "resume", history, {}


def benchmark():
    report = []
    for name, context, manifests in fixtures():
        legacy = decision_prompt(context)
        initial = render_decision_context(context, session_owner="fixture-session", scope="fixture", evidence_manifests=manifests)
        options = {"session_owner": "fixture-session", "scope": "fixture", "evidence_manifests": manifests}
        if name in {"resume", "repair"}:
            options["baseline"] = acknowledged_receipt(initial.receipt, context_ack(initial.receipt))
        if name == "repair":
            options["classification"] = "repair"
        rendered = render_decision_context(context, **options)
        report.append({"fixture": name, "legacy": {"serialized_bytes": len(legacy.encode()), "serialized_characters": len(legacy)}, "optimized": rendered.accounting, "bytes_saved": len(legacy.encode()) - len(rendered.prompt.encode())})
    return {"measurement": "injected context only", "model_calls": 0, "token_counts": None, "fixtures": report}


if __name__ == "__main__":
    print(json.dumps(benchmark(), ensure_ascii=False, indent=2))
