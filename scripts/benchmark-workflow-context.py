#!/usr/bin/env python3
"""Deterministic injected-context measurements. No model calls or token estimates.

Run: uv run python scripts/benchmark-workflow-context.py
Output: JSON report containing serialized bytes, characters, and section breakdowns.
Fixture manifests model authorized immutable retrieval; they are not executable references.
"""
from __future__ import annotations
import argparse
import copy
from pathlib import Path
import json

from harflow_mcp.workflow_context import acknowledged_receipt, context_ack, render_decision_context
from harflow_mcp.workflow_delegation import decision_prompt


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
    sections = copy.deepcopy(history)
    sections["technical_plan"] = "Verified implementation requirements and constraints. " * 300
    sections["checklist"] = [{"id": str(i), "status": "pending", "description": "Retain exact acceptance evidence " * 20} for i in range(20)]
    sections["node_instructions"] = {"work": "Verbatim worker instructions " * 300}
    yield "acknowledged-sections", sections, {}


def benchmark(baseline_report=None):
    report = []
    for name, context, manifests in fixtures():
        legacy = decision_prompt(context)
        initial = render_decision_context(context, session_owner="fixture-session", scope="fixture", evidence_manifests=manifests)
        options = {"session_owner": "fixture-session", "scope": "fixture", "evidence_manifests": manifests}
        if name in {"resume", "repair", "acknowledged-sections"}:
            options["baseline"] = acknowledged_receipt(initial.receipt, context_ack(initial.receipt))
        if name == "repair":
            options["classification"] = "repair"
        if name == "acknowledged-sections":
            context = copy.deepcopy(context)
            context["checklist"].append({"id": "new", "status": "pending", "description": "New acceptance criterion"})
            context["decision_id"] = "d2"
        rendered = render_decision_context(context, **options)
        legacy = decision_prompt(context)
        report.append({"fixture": name, "legacy": {"serialized_bytes": len(legacy.encode()), "serialized_characters": len(legacy)}, "optimized": rendered.accounting, "bytes_saved": len(legacy.encode()) - len(rendered.prompt.encode())})
    if baseline_report is not None:
        before = {row["fixture"]: row for row in baseline_report["fixtures"]}
        for row in report:
            baseline = before.get(row["fixture"])
            if baseline is None:
                row["comparison"] = {"kind": "new scenario; no baseline comparison"}
            elif baseline["legacy"] != row["legacy"]:
                row["comparison"] = {"kind": "incomparable fixture payload; baseline legacy sizes differ"}
            else:
                old = baseline["optimized"]
                row["comparison"] = {"kind": "same fixture", "baseline_prompt_version": old["prompt_version"],
                                     "baseline_serialized_bytes": old["serialized_bytes"],
                                     "current_serialized_bytes": row["optimized"]["serialized_bytes"],
                                     "bytes_saved_from_baseline": old["serialized_bytes"] - row["optimized"]["serialized_bytes"]}
    return {"measurement": "injected context only", "model_calls": 0, "token_counts": None, "cache_counts": None, "cost_usd": None, "prompt_version": 2, "fixtures": report}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-report", type=Path, help="Prior JSON report from the same fixtures")
    args = parser.parse_args()
    baseline = json.loads(args.baseline_report.read_text()) if args.baseline_report else None
    print(json.dumps(benchmark(baseline), ensure_ascii=False, indent=2))
