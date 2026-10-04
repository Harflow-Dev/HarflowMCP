"""Bounded public workflow projections; durable records remain complete."""
from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

BUDGET = 24 * 1024
VIEWS = {"executions": "activations", "decisions": "decisions", "checklist": "tasks", "checklist_disposition": "checklist_disposition", "technical_plan": "technical_plan", "definition": "definition", "question": "input_question", "reason": "reason", "wait_reason": "wait_reason", "checkout_wait": "checkout_wait", "summary": "summary", "failure_reason": "failure_reason", "attention_reason": "attention_reason", "builder_draft": "builder_draft", "generated_definition": "generated_definition"}


PUBLIC_VIEWS = frozenset(VIEWS)

# These lossless views are fetched only when their digest changes. Executions have
# individual identities so a new checkpoint does not resend old worker results.
MONITOR_FIELDS = ("definition", "tasks", "checklist_disposition", "technical_plan", "decisions", "pending", "joins", "released_parallel_groups", "exhausted_retry_edges", "input_question", "reason", "wait_reason", "checkout_wait", "summary", "failure_reason", "attention_reason", "builder_draft", "generated_definition", "editing_definition", "source_saved_definition", "prompt")
VIEWS.update({field: field for field in MONITOR_FIELDS})
VIEWS["execution_index"] = "execution_index"


def _serialize(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _digest(value: Any) -> str:
    return hashlib.sha256(_serialize(value).encode()).hexdigest()


def _view_value(run: dict[str, Any], view: str) -> Any:
    if view == "execution_index":
        return [{"id": a["id"], "digest": _digest(a)} for a in run.get("activations", [])]
    if view.startswith("execution:"):
        execution_id = view.removeprefix("execution:")
        for activation in run.get("activations", []):
            if activation.get("id") == execution_id:
                return activation
        raise ValueError("Unknown workflow execution")
    if view not in VIEWS:
        raise ValueError("Unknown workflow detail view")
    return run.get(VIEWS[view])


def monitor(run: dict[str, Any]) -> dict[str, Any]:
    """Bound selected-run polling; authoritative content lives in paged views."""
    result = compact(run)
    for key in ("workflow_name", "source_name", "source_revision", "builder_followup"):
        if key in run:
            result[key] = run[key][:2000] if isinstance(run[key], str) else run[key]
    result["monitor_digests"] = {field: _digest(run.get(field)) for field in MONITOR_FIELDS}
    result["monitor_digests"]["execution_index"] = _digest(_view_value(run, "execution_index"))
    # Digest references must fit the same transport budget as status metadata.
    if len(json.dumps(result, ensure_ascii=True).encode()) > BUDGET:
        for key in ("decision_errors", "checkout_wait", "current_stage"):
            result.pop(key, None)
        for key, value in list(result.items()):
            if isinstance(value, str) and len(value) > 64:
                result[key] = value[:64]
                result[key + "_truncated"] = True
    return result


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
    keys = ("workflow_run_id", "name", "revision", "kind", "status", "created_at", "updated_at", "interaction_owner", "settling", "input_decision_id", "execution_contract", "draft_revision")
    result = {key: run[key] for key in keys if key in run and isinstance(run[key], (str, int, float, bool, type(None)))}
    disposition = run.get("checklist_disposition")
    if isinstance(disposition, dict):
        result["checklist_disposition"] = {key: str(disposition[key])[:100] for key in ("status", "execution_id", "decision_id") if key in disposition}
        reason = str(disposition.get("reason", ""))
        result["checklist_disposition"].update(reason=reason[:256], reason_truncated=len(reason) > 256)
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
    if type(limit) is not int or not 1 <= limit <= 8000:
        raise ValueError("Detail limit must be between 1 and 8000 characters")
    serialized = _serialize(_view_value(run, view))
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


def monitor_snapshot(run, run_id, cache, cursor=None):
    """Local CLI-only lossless paging of a stable Monitor transport snapshot."""
    from .workflow_monitor_snapshot import snapshot
    captured = run | {"monitor_digests": monitor(run)["monitor_digests"]} if run is not None else None
    return snapshot(captured, run_id, cache, cursor)
