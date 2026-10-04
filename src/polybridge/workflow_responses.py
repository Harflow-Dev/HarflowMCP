"""Bounded public workflow projections; durable records remain complete."""
from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

BUDGET = 24 * 1024
VIEWS = {"executions": "activations", "decisions": "decisions", "checklist": "tasks", "technical_plan": "technical_plan", "definition": "definition", "question": "input_question", "reason": "reason", "wait_reason": "wait_reason", "checkout_wait": "checkout_wait", "summary": "summary", "failure_reason": "failure_reason", "attention_reason": "attention_reason"}


def history_page(runs: list[dict[str, Any]], offset: int = 0, limit: int = 100) -> dict[str, Any]:
    """Bound Monitor history polling independently of full run and worker outputs."""
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("offset must be nonnegative; limit must be 1–100")
    # Current work remains visible even with a large, newer settled history.
    ordered = sorted(runs, key=lambda r: (r.get("status") not in {"completed", "failed", "cancelled"} or r.get("settling", False), r.get("created_at", 0)), reverse=True)
    entries = []
    for run in ordered[offset:offset + limit]:
        row = compact(run)
        row["repo_path"] = str(run.get("repo_path", ""))[:1000]
        row["prompt"] = str(run.get("prompt", ""))[:2000]
        row["prompt_truncated"] = len(str(run.get("prompt", ""))) > 2000
        row["decisions"] = [{"reason": str(run["decisions"][-1].get("reason", ""))[:2000]}] if run.get("decisions") else []
        definition = run.get("definition", {})
        row["backends"] = sorted({str(candidate.get("backend", ""))[:100] for candidate in [definition.get("orchestrator", {})] + [n.get("agent", {}) for n in definition.get("nodes", [])] if candidate.get("backend")})[:20]
        row["node_labels"] = {str(n.get("id", ""))[:100]: str(n.get("title") or n.get("id", ""))[:200] for n in run.get("definition", {}).get("nodes", [])[:100]}
        row["activations"] = [{key: str(a[key])[:100] if isinstance(a[key], str) else a[key] for key in ("id", "node_id", "role", "status", "created_at") if key in a} | {"tasks": [{"task_id": str(t.get("task_id", ""))[:100], "status": str(t.get("status", ""))[:100]} for t in a.get("tasks", [])[-10:]]} for a in run.get("activations", [])[-40:]]
        row["activations_truncated"] = len(run.get("activations", [])) > 40 or any(len(a.get("tasks", [])) > 10 for a in run.get("activations", []))
        if len(json.dumps(row, ensure_ascii=True).encode()) > 48 * 1024:
            row.pop("node_labels", None)
            row["activations"] = []
            row["activations_truncated"] = True
        if entries and len(json.dumps(entries + [row], ensure_ascii=True).encode()) > 256 * 1024:
            break
        entries.append(row)
    following = offset + len(entries)
    return {"runs": entries, "next_offset": following if following < len(ordered) else None}


def compact(run: dict[str, Any]) -> dict[str, Any]:
    keys = ("workflow_run_id", "name", "revision", "kind", "status", "created_at", "updated_at", "interaction_owner", "settling", "input_decision_id", "execution_contract")
    result = {key: run[key] for key in keys if key in run and isinstance(run[key], (str, int, float, bool, type(None)))}
    result["response_version"] = 1
    result["details"] = {"tool": "get_workflow_run_detail", "views": list(VIEWS)}
    activations = run.get("activations", [])
    result["progress"] = {"executions": len(activations), "active": sum(a.get("status") in {"running", "starting", "reserved", "uncertain"} for a in activations), "settled": sum(a.get("status") in {"completed", "failed", "cancelled", "blocked", "succeeded", "timed_out"} for a in activations)}
    for key in ("input_question", "reason", "wait_reason", "failure_reason", "attention_reason", "summary"):
        if key in run:
            text = str(run[key])
            result[key] = text[:2000]
            result[key + "_truncated"] = len(text) > 2000
    wait = run.get("checkout_wait")
    if isinstance(wait, dict):
        owner = wait.get("owner") or {}
        result["checkout_wait"] = {"reason": str(wait.get("reason", ""))[:300], "repo_path": str(wait.get("repo_path", ""))[:300], "owner": {key: str(owner.get(key, ""))[:100] for key in ("workflow_run_id", "task_id")}}
    result["current_stage"] = [{"execution_id": str(a.get("id", ""))[:100], "node_id": str(a.get("node_id", ""))[:100], "role": str(a.get("role", ""))[:100], "status": str(a.get("status", ""))[:100]} for a in activations if a.get("status") in {"running", "starting", "reserved", "uncertain"}][:8]
    errors = [{"execution_id": a.get("id"), "decision_id": (a.get("decision_diagnostic") or {}).get("decision_id", a.get("decision_id")), "error": str(a["result_error"])[:1000], "diagnostic": {key: str((a.get("decision_diagnostic") or {}).get(key, ""))[:100] for key in ("attempt", "category", "field_path", "field", "path")}} for a in activations if a.get("result_error")]
    result["decision_errors"] = errors[-3:]
    # User-authored identity fields can also be large; never permit them to defeat the budget.
    for key, value in list(result.items()):
        if isinstance(value, str) and len(value) > 2000:
            result[key] = value[:2000]
            result[key + "_truncated"] = True
    if len(json.dumps(result, ensure_ascii=True).encode()) > BUDGET - 2048:
        result.pop("decision_errors", None)
        for key, value in list(result.items()):
            if isinstance(value, str) and len(value) > 200:
                result[key] = value[:200]
                result[key + "_truncated"] = True
    if len(json.dumps(result, ensure_ascii=True).encode()) > BUDGET - 2048:
        result["current_stage"] = []
        result.pop("checkout_wait", None)
        for key, value in list(result.items()):
            if isinstance(value, str) and len(value) > 64:
                result[key] = value[:64]
                result[key + "_truncated"] = True
    return result


def detail(run: dict[str, Any], view: str, cursor: str | None = None, limit: int = 8000) -> dict[str, Any]:
    """Lossless JSON chunks bound to run, view and immutable content digest."""
    if view not in VIEWS:
        raise ValueError("Unknown workflow detail view")
    if type(limit) is not int or not 1 <= limit <= 8000:
        raise ValueError("Detail limit must be between 1 and 8000 characters")
    serialized = json.dumps(run.get(VIEWS[view]), ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(serialized.encode()).hexdigest()
    identity = [run["workflow_run_id"], view, digest]
    offset = 0
    if cursor:
        try:
            decoded = json.loads(base64.urlsafe_b64decode(cursor))
            if decoded["identity"] != identity or type(decoded["offset"]) is not int or not 0 <= decoded["offset"] < len(serialized):
                raise ValueError()
            offset = decoded["offset"]
        except (ValueError, KeyError, TypeError, UnicodeError) as exc:
            raise ValueError("Invalid or stale workflow detail cursor") from exc
    chunk = serialized[offset:offset + limit]
    end = offset + len(chunk)
    next_cursor = base64.urlsafe_b64encode(json.dumps({"identity": identity, "offset": end}).encode()).decode() if end < len(serialized) else None
    return {"response_version": 1, "workflow_run_id": run["workflow_run_id"], "view": view, "encoding": "json", "content_sha256": digest, "offset": offset, "total_characters": len(serialized), "chunk": chunk, "next_cursor": next_cursor}
