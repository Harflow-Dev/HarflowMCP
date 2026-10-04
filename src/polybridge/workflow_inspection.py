"""Bounded inspection of durable, fully settled workflow executions."""
from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

LIVE = {"reserved", "uncertain", "running", "starting"}
SETTLED = {"completed", "failed", "cancelled", "blocked", "timed_out", "succeeded"}


def execution(run: dict[str, Any], execution_id: str) -> dict[str, Any]:
    activation = next((a for a in run.get("activations", []) if a.get("id") == execution_id and a.get("role") == "node"), None)
    if activation is None:
        raise ValueError("Unknown node execution in this workflow run")
    if activation.get("status") not in SETTLED or any(t.get("status") in LIVE for t in activation.get("tasks", [])):
        raise ValueError("Node execution must be fully settled before inspection")
    if run.get("execution_contract") == "delegation" and activation.get("status") in {"completed", "failed", "succeeded", "blocked", "timed_out"}:
        result = activation.get("node_result")
        if not isinstance(result, dict) or result.get("status") not in {"succeeded", "failed", "blocked"}:
            raise ValueError("Node execution must have a final normalized result before inspection")
    return activation


def selected_task(activation: dict[str, Any], task_id: str | None) -> dict[str, Any] | None:
    tasks = activation.get("tasks", [])
    if task_id is None:
        return tasks[-1] if tasks else None
    task = next((t for t in tasks if t.get("task_id") == task_id), None)
    if task is None:
        raise ValueError("Selected task does not belong to this node execution")
    return task


def result_page(run: dict[str, Any], execution_id: str, *, task_id: str | None = None, cursor: str | None = None, limit: int = 16000) -> dict[str, Any]:
    """Return lossless JSON text chunks, bound to execution, attempt, and immutable content."""
    if type(limit) is not int or not 1 <= limit <= 32000:
        raise ValueError("limit must be between 1 and 32000 characters")
    activation = execution(run, execution_id)
    task = selected_task(activation, task_id)
    # Explicit attempt selection returns that candidate's outcome; default returns the normalized node contract.
    payload = {"node_result": activation.get("node_result"), "raw_output": activation.get("raw_output", "")} if task_id is None else {"task_result": task.get("result", {}), "raw_output": task.get("raw_output", task.get("result", {}).get("summary", ""))}
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(serialized.encode()).hexdigest()
    identity = [run["workflow_run_id"], execution_id, task_id, digest]
    offset = 0
    if cursor is not None:
        try:
            decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
            if not isinstance(decoded, dict) or decoded.get("identity") != identity or type(decoded.get("offset")) is not int or not 0 <= decoded["offset"] < len(serialized):
                raise ValueError()
            offset = decoded["offset"]
        except (ValueError, TypeError, KeyError, UnicodeError) as exc:
            raise ValueError("Invalid or stale result cursor") from exc
    chunk = serialized[offset:offset + limit]
    end = offset + len(chunk)
    next_cursor = base64.urlsafe_b64encode(json.dumps({"identity": identity, "offset": end}, separators=(",", ":")).encode()).decode() if end < len(serialized) else None
    return {"workflow_run_id": run["workflow_run_id"], "execution_id": execution_id, "node_id": activation["node_id"], "status": activation["status"], "task_id": task.get("task_id") if task else None, "attempts": [{"task_id": t.get("task_id"), "status": t.get("status")} for t in activation.get("tasks", [])], "encoding": "json", "content_sha256": digest, "offset": offset, "total_characters": len(serialized), "chunk": chunk, "next_cursor": next_cursor, "has_more": next_cursor is not None}


def decorate_tasks(entries: list[dict[str, Any]], log_dir: Any) -> list[dict[str, Any]]:
    """Build one association index per listing; avoid scanning every run for every task."""
    from .workflows import WorkflowStore
    index = {}
    storage = WorkflowStore(root=log_dir.parent)
    for run in storage.list_runs():
        for activation in run.get("activations", []):
            for task in activation.get("tasks", []):
                index[task["task_id"]] = {"workflow_run_id": run["workflow_run_id"], "workflow_node_id": activation["node_id"], "workflow_execution_id": activation["id"], "workflow_role": activation["role"], "execution_contract": run.get("execution_contract"), "interaction_owner": run.get("interaction_owner", "caller"), "workflow_status": run["status"] if "status" in run else None, "workflow_settling": run.get("settling", False), "workflow_name": run.get("name")}
                if run.get("execution_contract") == "delegation" and activation.get("role") == "node" and isinstance(activation.get("assignment_prompt"), str):
                    assignment = task.get("assignment_prompt", activation["assignment_prompt"])
                    index[task["task_id"]].update(display_prompt=assignment, prompt=assignment)
    # Follow-up and spawned task records inherit ownership without inheriting the
    # ancestor's assignment prompt or pretending to be that execution.
    changed = True
    inherited = ("workflow_run_id", "workflow_name", "workflow_status", "workflow_settling", "interaction_owner", "execution_contract")
    while changed:
        changed = False
        for entry in entries:
            task_id = entry.get("task_id")
            if task_id in index:
                continue
            ancestor = next((index.get(entry.get(key)) for key in ("parent_task_id", "spawned_by") if entry.get(key) in index), None)
            if ancestor is not None:
                index[task_id] = {key: ancestor[key] for key in inherited if key in ancestor}
                changed = True
    for entry in entries:
        task_id = entry.get("task_id")
        if task_id not in index and (entry.get("parent_task_id") or entry.get("spawned_by")):
            association = storage.task_owner(task_id)
            if association is not None:
                index[task_id] = {key: association[key] for key in inherited if key in association}
    return [{**entry, **index.get(entry.get("task_id"), {})} for entry in entries]


def managed_reader(log_dir: Any) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """CLI caller verification without constructing a registry or running maintenance."""
    from . import lineage
    from .workflows import WorkflowStore
    caller = lineage.detect_caller(log_dir)
    if caller is None:
        import os
        detection = lineage.detect_caller_detail(log_dir)
        if detection.undecidable is not None:
            raise ValueError("Workflow caller authority is undecidable: " + str(detection.undecidable))
        caller = detection.caller
        if caller is None and os.environ.get(lineage.ENV_TASK_ID):
            raise ValueError("Workflow caller task identity cannot be verified")
        if caller is None:
            return None
    storage = WorkflowStore(root=log_dir.parent)
    association = storage.task_owner(caller.record.task_id, strict=True)
    if association is None or association.get("role") == "builder":
        return None
    run = storage.get_run(association["workflow_run_id"])
    return (association, run) if run.get("execution_contract") == "delegation" else None


def guard_task_read(task_id: str, managed: tuple[dict[str, Any], dict[str, Any]] | None) -> None:
    if managed is None:
        return
    association, run = managed
    activation = next((a for a in run.get("activations", []) if any(t.get("task_id") == task_id for t in a.get("tasks", []))), None)
    if activation is None:
        raise ValueError("Managed agents may only read tasks from their own workflow run")
    if association["role"] == "node":
        if activation["id"] != association["activation_id"]:
            raise ValueError("Worker nodes may only inspect their own execution")
    else:
        execution(run, activation["id"])


def filter_task_reads(entries: list[dict[str, Any]], managed: tuple[dict[str, Any], dict[str, Any]] | None) -> list[dict[str, Any]]:
    if managed is None:
        return entries
    allowed = set()
    for activation in managed[1].get("activations", []):
        for task in activation.get("tasks", []):
            try:
                guard_task_read(task["task_id"], managed)
                allowed.add(task["task_id"])
            except ValueError:
                pass
    return [entry for entry in entries if entry["task_id"] in allowed]


def inspect_request(run: dict[str, Any], root: Any, request: dict[str, Any]) -> dict[str, Any]:
    """Runner-side inspection, with no MCP server, registry, or agent tool dependency."""
    from .events import events_path, read_page
    if not isinstance(request, dict):
        raise ValueError("Inspection request must be an object")
    allowed = {"execution_id", "task_id", "view", "cursor", "limit", "before_seq", "after_seq"}
    if set(request) - allowed:
        raise ValueError("Unknown inspection request fields")
    execution_id = request.get("execution_id")
    if not isinstance(execution_id, str) or not execution_id:
        raise ValueError("Inspection requires execution_id")
    task_id = request.get("task_id")
    if task_id is not None and (not isinstance(task_id, str) or not task_id):
        raise ValueError("task_id must be a nonempty string")
    view = request.get("view", "result")
    limit = request.get("limit")
    if view == "result":
        if request.get("before_seq") is not None or request.get("after_seq") is not None:
            raise ValueError("Result view uses cursor, not event sequence cursors")
        if request.get("cursor") is not None and not isinstance(request["cursor"], str):
            raise ValueError("cursor must be a string")
        return result_page(run, execution_id, task_id=task_id, cursor=request.get("cursor"), limit=16000 if limit is None else limit)
    if view != "activity":
        raise ValueError("view must be result or activity")
    if request.get("cursor") is not None:
        raise ValueError("Activity uses before_seq/after_seq rather than result cursors")
    limit = 50 if limit is None else limit
    if type(limit) is not int or not 1 <= limit <= 200:
        raise ValueError("Activity limit must be between 1 and 200")
    before = request.get("before_seq")
    after = request.get("after_seq")
    if before is not None and after is not None:
        raise ValueError("cannot specify both before_seq and after_seq")
    if any(value is not None and (type(value) is not int or value < 0) for value in (before, after)):
        raise ValueError("Activity cursors must be nonnegative integers")
    activation = execution(run, execution_id)
    task = selected_task(activation, task_id)
    if task is None:
        raise ValueError("Node execution has no dispatched task activity")
    page = read_page(events_path(root / "tasks", task["task_id"]), limit=limit, before_seq=before, after_seq=after)
    return {"workflow_run_id": run["workflow_run_id"], "execution_id": execution_id, "node_id": activation["node_id"], "task_id": task["task_id"], "events": page.events, "has_more": page.has_more, "next_before_seq": page.next_before_seq, "next_after_seq": page.next_after_seq, "skipped_oversized": page.skipped_oversized}
