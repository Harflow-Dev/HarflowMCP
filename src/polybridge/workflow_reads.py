"""Shared bounded workflow reads without MCP initialization or task maintenance.

Transport adapters retain their own envelope and error mapping. Authority comes from the
same verified readers as task inspection, never from a CLI Monitor flag.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Awaitable, Callable

from .catalog import bounded_request
from .read_metrics import span

READ_ACTIONS = frozenset({"list", "list_runs", "list_run_page", "get_run_header", "get", "status", "detail"})

def _project(function, *args):
    with span("projection"):
        return function(*args)


async def _read(function, *args, **kwargs):
    """Time disk reads and their projection, including thread scheduling."""
    with span("projection"):
        return await asyncio.to_thread(function, *args, **kwargs)


async def bounded_caller(directory: Path):
    """Verify frozen Monitor snapshot readers without replaying retained metadata."""
    with span("authority"):
        from . import lineage, store
        from .catalog import Catalog
        if not Catalog(directory, store.RECORD_SUFFIX).ready():
            await asyncio.to_thread(store.bootstrap_catalog, directory)
            raise ValueError("Workflow caller authority indexing is incomplete; retry after bounded indexing progresses")
        detection = await asyncio.to_thread(lineage.detect_catalog_caller, directory)
        if detection.undecidable is not None:
            raise ValueError("Workflow caller authority is undecidable: " + str(detection.undecidable))
        return detection.caller


def managed_run_summary(run: dict[str, Any]) -> dict[str, Any]:
    """Expose routing state without bypassing settled-only result inspection."""
    keys = ("workflow_run_id", "name", "kind", "status", "definition", "revision", "prompt", "execution_contract", "tasks", "checklist_disposition", "decisions", "transitions", "settling", "input_question", "input_decision_id", "interaction_owner", "reason", "instructions", "created_at", "updated_at")
    summary = {key: run[key] for key in keys if key in run}
    if isinstance(run.get("technical_plan"), str):
        summary.update(technical_plan=run["technical_plan"][:16000], technical_plan_truncated=len(run["technical_plan"]) > 16000, technical_plan_execution_id=run.get("technical_plan_execution_id"))
    summary["activations"] = [{key: activation[key] for key in ("id", "node_id", "role", "status", "created_at", "finished_at") if key in activation} | {"tasks": [{"task_id": task["task_id"], "status": task["status"]} for task in activation.get("tasks", [])]} for activation in run.get("activations", [])]
    return summary


@bounded_request
async def call(action: str, *, directory: Path, reader: Callable[[], Awaitable[Any]] | None = None, **kwargs: Any) -> Any:
    """Read with an aggregate metadata budget and preserved managed-role restrictions."""
    with span("import"):
        from . import workflows
        from .workflow_inspection import managed_reader
    if action not in READ_ACTIONS:
        raise ValueError("Unsupported workflow read action: " + action)
    bounded_read = kwargs.pop("_bounded_read", False)
    if reader is None:
        async def reader():
            return await asyncio.to_thread(managed_reader, directory)
    with span("authority"):
        if action in {'list_run_page', 'get_run_header'}:
            from .workflow_inspection import managed_page_reader, page_indexing_response
            ready, managed = await asyncio.to_thread(managed_page_reader, directory)
            if not ready:
                return page_indexing_response(directory)
        elif bounded_read and action in {"status", "detail", "list"}:
            from .workflow_inspection import managed_page_reader, page_indexing_response
            ready, managed = await asyncio.to_thread(managed_page_reader, directory)
            if not ready:
                pending = await asyncio.to_thread(page_indexing_response, directory)
                if action == 'list':
                    pending['workflows'] = []
                return pending
        else:
            managed = await reader()
    if managed is not None:
        association, owned_run = managed
        if action in {'list_run_page', 'get_run_header'}:
            if association['role'] not in {'builder', 'orchestrator'}:
                raise ValueError('Worker nodes cannot inspect workflow context')
            if action == 'get_run_header' and kwargs['run_id'] != owned_run['workflow_run_id']:
                raise ValueError('Managed callers may only inspect their own workflow run')
            from .workflow_responses import compact
            header = _project(compact, owned_run)
            if action == 'get_run_header':
                return header
            active = owned_run.get('status') not in workflows.TERMINAL
            return {'items': [header] if not kwargs.get('active_only') or active else [], 'next_cursor': None, 'has_more': False, 'bootstrap_pending': False, 'total_active_count': int(active), 'related_headers': []}
        if association["role"] == "builder":
            if action in {"status", "inspect", "detail"} and kwargs["run_id"] != owned_run["workflow_run_id"]:
                raise ValueError("Builders may only inspect their own workflow run")
            if action == "status":
                from .workflow_responses import compact
                return _project(compact, owned_run)
            if action == "detail" and kwargs["view"] in {"builder_draft", "generated_definition"}:
                from .workflow_responses import detail
                return _project(detail, owned_run, kwargs["view"], kwargs.get("cursor"), kwargs.get("limit", 8000))
            if action == "list_runs":
                from .workflow_responses import compact
                return [_project(compact, owned_run)]
            raise ValueError("Builders may only inspect their own draft and builder run")
        if association["role"] != "orchestrator":
            raise ValueError("Worker nodes cannot inspect workflow context")
        if action in {"status", "inspect", "detail"} and kwargs["run_id"] != owned_run["workflow_run_id"]:
            raise ValueError("Orchestrators may only inspect their own workflow run")
        if action == "get":
            if kwargs["name"] != owned_run["name"]:
                raise ValueError("Orchestrators may only inspect their current workflow graph")
            return owned_run["definition"]
        if action == "list":
            return [owned_run["definition"]]
        if action == "list_runs":
            return [managed_run_summary(owned_run)]
        if action == "detail":
            from .workflow_responses import PUBLIC_VIEWS
            if kwargs["view"] == "executions" or kwargs["view"] not in PUBLIC_VIEWS:
                raise ValueError("Use inspect_workflow_node for settled execution details")
            from .workflow_responses import detail
            return _project(detail, owned_run, kwargs["view"], kwargs.get("cursor"), kwargs.get("limit", 8000))
        if action == "status":
            return managed_run_summary(owned_run)
    store_ = workflows.WorkflowStore()
    if action == "list":
        return await _read(store_.list)
    if action == "list_runs":
        return await _read(store_.list_runs)
    if action == 'get_run_header':
        return await _read(store_.get_run_header, kwargs['run_id'])
    if action == 'list_run_page':
        related_id = kwargs.pop('related_run_id', None)
        if related_id is not None:
            related = await _read(store_.related_run_headers, [related_id])
            page = {'items': [], 'next_cursor': None, 'has_more': False, 'bootstrap_pending': False, 'related_headers': related}
            from .catalog import apply_read_state
            if related.catalog_state:
                apply_read_state(page, related.catalog_state)
            return page
        page = await _read(store_.list_run_page, **kwargs)
        ids = [item[key] for item in page['items'] for key in ('parent_workflow_run_id', 'orchestrator_session_owner_run_id') if item.get(key)]
        import json
        budget = min(64 * 1024, max(0, 256 * 1024 - len(json.dumps(page, ensure_ascii=True).encode()) - 2048))
        page['related_headers'] = await _read(store_.related_run_headers, ids, byte_budget=budget)
        from .catalog import apply_read_state
        if page['related_headers'].catalog_state:
            apply_read_state(page, page['related_headers'].catalog_state)
        return page
    if action == "get":
        return await _read(store_.get, kwargs["name"])
    if action == "detail":
        from .workflow_responses import detail
        run = await _read(store_.get_run, kwargs["run_id"])
        return _project(detail, run, kwargs["view"], kwargs.get("cursor"), kwargs.get("limit", 8000))
    if action == "status":
        return await _read(store_.get_run, kwargs["run_id"])


async def cli_read(action: str, args: Any, directory: Path) -> Any:
    """Preserve CLI projections and human-only frozen Monitor snapshot access."""
    async def read(action, **kwargs):
        return await call(action, directory=directory, **kwargs)
    async def read_page(limit, cursor, active_only, related_run_id, run_ids):
        return await read("list_run_page", limit=limit, cursor=cursor, active_only=active_only, related_run_id=related_run_id, run_ids=run_ids)
    if action == 'list-page':
        return await read_page(args.limit, args.cursor, args.active_only, args.related_run_id, args.run_ids.split(',') if args.run_ids is not None else None)
    if action in {"list", "list-runs"}:
        entries = await read(action.replace("-", "_"), **({"_bounded_read": True} if action == "list" and args.monitor_view else {}))
        if isinstance(entries, dict) and 'catalog_state' in entries:
            return entries
        if action == "list-runs":
            from .workflow_responses import history_page
            return _project(history_page, entries, args.offset, args.limit)
        return {"workflows" if action == "list" else "runs": entries}
    if action == "get":
        return await read("get", name=args.name)
    if action == "detail":
        if args.monitor_view:
            from .workflow_responses import monitor_detail
            if await bounded_caller(directory) is not None:
                raise ValueError("Monitor detail snapshots are only available to the local Monitor")
            run = None if args.cursor else await read("status", run_id=args.workflow_run_id, _bounded_read=True)
            if run is not None and run.get('catalog_state', {}).get('status') in {'preparing', 'blocked'}:
                return run
            return _project(monitor_detail, run, args.workflow_run_id, args.view, directory.parent / "monitor_snapshots", args.cursor)
        return await read("detail", run_id=args.workflow_run_id, view=args.view, cursor=args.cursor)
    if action == "status" and args.monitor_view:
        from .workflow_responses import monitor, monitor_snapshot
        from .workflow_cancellation import monitor_projection
        from .workflows import WorkflowStore
        if args.snapshot:
            if await bounded_caller(directory) is not None:
                raise ValueError("Monitor snapshots are only available to the local Monitor")
            run = None if args.cursor else await read("status", run_id=args.workflow_run_id, _bounded_read=True)
            if run is not None and run.get('catalog_state', {}).get('status') in {'preparing', 'blocked'}:
                return run
            if run is not None:
                run = await asyncio.to_thread(_project, monitor_projection, WorkflowStore(), run)
            return _project(monitor_snapshot, run, args.workflow_run_id, directory.parent / "monitor_snapshots", args.cursor)
        run = await read("status", run_id=args.workflow_run_id, _bounded_read=True)
        if run.get('catalog_state', {}).get('status') in {'preparing', 'blocked'}:
            return run
        return _project(monitor, await asyncio.to_thread(_project, monitor_projection, WorkflowStore(), run))
    return await read(action, run_id=args.workflow_run_id)
