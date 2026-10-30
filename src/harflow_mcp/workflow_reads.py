"""Shared bounded workflow reads without MCP initialization or task maintenance.

Transport adapters retain their own envelope and error mapping. Authority comes from the
same verified readers as task inspection, never from a CLI Monitor flag.
"""
from __future__ import annotations

from .state_access import task_operation, task_layout
from .state_ownership import StateOwnershipError

import asyncio
from pathlib import Path
from typing import Any, Awaitable, Callable

from . import id_resolution
from .catalog import bounded_request
from .read_metrics import span

BOUNDED_READ_ACTIONS = frozenset({"list", "list_runs", "list_run_page", "get_run_header", "get", "status", "detail"})
READ_ONLY_CLI_ACTIONS = {
    "validate": "validate", "list": "list", "list-page": "list_run_page",
    "get": "get", "list-runs": "list_runs", "status": "status", "detail": "detail",
    "wait": "wait", "inspect": "inspect", "assigned-input": "assigned_input",
    "artifacts": "artifacts", "preview": "preview", "notifications": "notifications",
}
READ_ACTIONS = BOUNDED_READ_ACTIONS | frozenset(READ_ONLY_CLI_ACTIONS.values())
READ_ONLY_CLI_COMMANDS = frozenset("workflow-" + name for name in READ_ONLY_CLI_ACTIONS)

RUN_STATUSES = ("pending", "starting", "building", "running", "pausing", "paused", "needs_input", "needs_attention", "cancelling", "completed", "failed", "cancelled")
RUN_SCOPES = ("all", "root", "nested")
ACCEPTED_RUN_FILTERS = ("workflow_id", "repo", "status", "since", "before", "title_contains", "scope")


def _timestamp(name: str, value: Any) -> float:
    from datetime import datetime
    if not isinstance(value, str) or not value or len(value.encode()) > 200:
        raise ValueError(f"{name} must be an ISO8601 timestamp with timezone of at most 200 UTF-8 bytes")
    try:
        date = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if date.tzinfo is None:
            raise ValueError()
        return date.timestamp()
    except (ValueError, OverflowError):
        raise ValueError(f"{name} must be an ISO8601 timestamp with timezone") from None


def validate_run_filters(filters: Any) -> dict[str, Any]:
    """Normalize run listing filters; unset values and scope=all drop out.

    ``since`` is inclusive and ``before`` exclusive, both against ``created_at``. ``title_contains``
    is case-folded against the run name. ``status`` is a list of known run statuses.
    """
    from .task_reads import _unknown_names
    accepted = ", ".join(ACCEPTED_RUN_FILTERS)
    if not isinstance(filters, dict):
        raise ValueError(f"run listing filters must be an object; accepted filters: {accepted}")
    unknown = [key for key in filters if key not in ACCEPTED_RUN_FILTERS]
    if unknown:
        raise ValueError(_unknown_names("run listing filters", unknown, accepted, vocabulary=set(ACCEPTED_RUN_FILTERS), accepted_label="accepted filters"))
    result: dict[str, Any] = {}
    for key, value in filters.items():
        if value is None:
            continue
        if key in {"since", "before"}:
            result[key] = _timestamp(key, value)
        elif key == "status":
            names = [value] if isinstance(value, str) else value
            if not isinstance(names, list) or not names or not all(isinstance(name, str) for name in names):
                raise ValueError("status must be a nonempty list of run statuses: " + ", ".join(RUN_STATUSES))
            bad = [name for name in names if name not in RUN_STATUSES]
            if bad:
                raise ValueError(_unknown_names("run statuses", bad, ", ".join(RUN_STATUSES), vocabulary=set(RUN_STATUSES)))
            result[key] = sorted(set(names))
        elif key == "scope":
            if value not in RUN_SCOPES:
                raise ValueError("scope must be one of: " + ", ".join(RUN_SCOPES))
            if value != "all":
                result[key] = value
        else:
            limit = 4096 if key == "repo" else 200
            if not isinstance(value, str) or not value.strip() or len(value.encode()) > limit:
                raise ValueError(f"{key} must be a nonblank string of at most {limit} UTF-8 bytes")
            result[key] = value.casefold() if key == "title_contains" else value
    return result


def run_matches(filters: dict[str, Any], meta: dict[str, Any], status: Any, created_at: float) -> bool:
    """The catalog's filter semantics for one run, evaluated against its lossless metadata."""
    for key, value in filters.items():
        if key == "workflow_id" and meta["workflow_id"] != value:
            return False
        if key == "repo" and meta["repo_path"] != value:
            return False
        if key == "title_contains" and value not in meta["name_folded"]:
            return False
        if key == "scope" and meta["is_root"] != (value == "root"):
            return False
        if key == "status" and status not in value:
            return False
        if key == "since" and not created_at >= value:
            return False
        if key == "before" and not created_at < value:
            return False
    return True


def _project(function, *args):
    with span("projection"):
        return function(*args)


async def _read(function, *args, **kwargs):
    """Time disk reads and their projection, including thread scheduling."""
    with span("projection"):
        return await asyncio.to_thread(function, *args, **kwargs)


@task_operation(parameter="directory")
async def bounded_caller(directory: Path):
    """Verify frozen Monitor snapshot readers without replaying retained metadata."""
    with span("authority"):
        from . import lineage, store
        from .catalog import Catalog
        if not Catalog(directory, store.RECORD_SUFFIX, layout=task_layout(directory)).ready():
            await asyncio.to_thread(store.bootstrap_catalog, directory)
            raise ValueError("Workflow caller authority indexing is incomplete; retry after bounded indexing progresses")
        detection = await asyncio.to_thread(lineage.detect_catalog_caller, directory)
        if detection.undecidable is not None:
            raise ValueError("Workflow caller authority is undecidable: " + str(detection.undecidable))
        return detection.caller


MANAGED_SUMMARY_FIELDS = ("workflow_run_id", "name", "kind", "status", "goal", "goal_revision", "goal_assessment", "goal_replanning_required", "parent_goal", "definition", "revision", "prompt", "execution_contract", "tasks", "checklist_disposition", "decisions", "transitions", "settling", "input_question", "input_decision_id", "interaction_owner", "reason", "instructions", "created_at", "updated_at", "on_demand_nodes", "on_demand_calls", "on_demand_state", "approval_summary", "input_question_items", "pending_inputs", "pending_attention", "pending_input_count", "pending_attention_count", "input_delivery_receipt", "attention_reason", "input_source", "interaction_state", "messages", "monitor_controls")
MANAGED_SUMMARY_DERIVED = ("technical_plan", "technical_plan_truncated", "technical_plan_execution_id", "activations")


def managed_run_summary(run: dict[str, Any]) -> dict[str, Any]:
    """Expose current routing state; goal history remains in paged detail reads."""
    keys = MANAGED_SUMMARY_FIELDS
    summary = {key: run[key] for key in keys if key in run}
    if isinstance(run.get("technical_plan"), str):
        summary.update(technical_plan=run["technical_plan"][:16000], technical_plan_truncated=len(run["technical_plan"]) > 16000, technical_plan_execution_id=run.get("technical_plan_execution_id"))
    summary["activations"] = [{key: activation[key] for key in ("id", "node_id", "role", "status", "created_at", "finished_at") if key in activation} | {"tasks": [{"task_id": task["task_id"], "status": task["status"]} for task in activation.get("tasks", [])]} for activation in run.get("activations", [])]
    return summary


@bounded_request
@task_operation(parameter="directory")
async def call(action: str, *, directory: Path, reader: Callable[[], Awaitable[Any]] | None = None, **kwargs: Any) -> Any:
    """Read with an aggregate metadata budget and preserved managed-role restrictions."""
    with span("import"):
        from . import workflows
        from .workflow_inspection import managed_reader
    if action not in BOUNDED_READ_ACTIONS:
        raise ValueError("Unsupported workflow read action: " + action)
    bounded_read = kwargs.pop("_bounded_read", False)
    selected = None
    if action == "list_run_page":
        # Refuse bad or conflicting arguments before any authority or catalog work.
        filters = validate_run_filters(kwargs.get("filters") or {})
        if filters and (kwargs.get("run_ids") is not None or kwargs.get("related_run_id") is not None):
            raise ValueError("Run filters cannot be combined with run_ids or related_run_id; filter a page of the whole history, or list the named runs without filters")
        if kwargs.get("fields") is not None:
            from .workflow_responses import RUN_LIST_FIELDS, validate_run_fields
            selected = validate_run_fields(kwargs["fields"], RUN_LIST_FIELDS, "run header")
        kwargs.pop("fields", None)
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
    resolved = await _resolve_run_arguments(action, directory, managed, kwargs)
    result = await _perform(action, directory, managed, kwargs)
    if selected is not None:
        result = _project_page(result, selected)
    return id_resolution.echo(result, **resolved)


def _project_page(page: dict[str, Any], selected: frozenset[str]) -> dict[str, Any]:
    """Project each header after the page is fully built, so values equal the unprojected page's."""
    from .workflow_responses import project_run
    page = dict(page)
    # Keep the links related_headers are joined by, whenever the page carries related headers.
    joins = frozenset({"parent_workflow_run_id", "orchestrator_session_owner_run_id"}) if page.get("related_headers") else frozenset()
    page["items"] = [project_run(item, selected | joins) for item in page["items"]]
    if page.get("related_headers"):
        page["related_headers"] = [project_run(item, selected) for item in page["related_headers"]]
    return page


async def _resolve_run_arguments(action: str, directory: Path, managed: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """Resolve run id prefixes once, in place; the reads below only ever see full ids."""
    root = directory.parent
    resolved: dict[str, Any] = {}
    async def one(value: str, **options: Any):
        return await asyncio.to_thread(id_resolution.resolve_run, root, value, managed=managed, **options)
    async def leniently(value: str):
        try:
            return await one(value)
        except id_resolution.IdResolutionError as exc:
            # A listed id that does not exist is dropped by the page, as it always was.
            if exc.code != "unknown_workflow_run" or not id_resolution.is_canonical_uuid(value):
                raise
            return id_resolution.Resolution(value, None)
    if action in {"status", "detail", "get_run_header"}:
        resolved["primary"] = await one(kwargs["run_id"])
        kwargs["run_id"] = resolved["primary"].id
    elif action == "list_run_page":
        if kwargs.get("related_run_id") is not None:
            resolved["related_run_id"] = await leniently(kwargs["related_run_id"])
            kwargs["related_run_id"] = resolved["related_run_id"].id
        if kwargs.get("run_ids") is not None:
            resolved["run_ids"] = [await leniently(value) for value in kwargs["run_ids"]]
            kwargs["run_ids"] = [item.id for item in resolved["run_ids"]]
    return resolved


async def _perform(action: str, directory: Path, managed: Any, kwargs: dict[str, Any]) -> Any:
    with span("import"):
        from . import workflows
    if managed is not None:
        association, owned_run = managed
        owned_run = await _read(on_demand_projection, workflows.WorkflowStore(directory.parent), owned_run)
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
            filters = validate_run_filters(kwargs.get('filters') or {})
            visible = not kwargs.get('active_only') or active
            if filters:
                # Lossless metadata of the authorized run, not the compact header.
                from .workflow_responses import run_meta
                visible = visible and run_matches(filters, run_meta(owned_run), owned_run.get('status'), float(owned_run.get('created_at', 0)))
            page = {'items': [header] if visible else [], 'next_cursor': None, 'has_more': False, 'bootstrap_pending': False, 'total_active_count': None if filters else int(active), 'related_headers': []}
            if filters:
                page['scan_incomplete'] = False
            return page
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
    store_ = workflows.WorkflowStore(directory.parent)
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
        run = await _read(store_.get_run, kwargs["run_id"], refresh_atomic=True)
        run = await _read(on_demand_projection, store_, run)
        return _project(detail, run, kwargs["view"], kwargs.get("cursor"), kwargs.get("limit", 8000))
    if action == "status":
        run = await _read(store_.get_run, kwargs["run_id"], refresh_atomic=True)
        return await _read(on_demand_projection, store_, run)


@task_operation(parameter="directory")
async def cli_read(action: str, args: Any, directory: Path) -> Any:
    """Preserve CLI projections and human-only frozen Monitor snapshot access."""
    async def read(action, **kwargs):
        return await call(action, directory=directory, **kwargs)
    async def read_page(limit, cursor, active_only, related_run_id, run_ids, filters, fields):
        return await read("list_run_page", limit=limit, cursor=cursor, active_only=active_only, related_run_id=related_run_id, run_ids=run_ids, filters=filters, fields=fields)
    if action == 'list-page':
        filters = {key: getattr(args, key, None) for key in ACCEPTED_RUN_FILTERS}
        if filters["status"] is not None:
            filters["status"] = filters["status"].split(",")
        return await read_page(args.limit, args.cursor, args.active_only, args.related_run_id, args.run_ids.split(',') if args.run_ids is not None else None, filters, args.fields.split(",") if args.fields is not None else None)
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
            # Only the verified local Monitor reaches here, so the run resolves as a direct caller.
            resolution = await asyncio.to_thread(id_resolution.resolve_run, directory.parent, args.workflow_run_id, managed=None)
            run = None if args.cursor else await read("status", run_id=resolution.id, _bounded_read=True)
            if run is not None and run.get('catalog_state', {}).get('status') in {'preparing', 'blocked'}:
                return run
            return id_resolution.echo(_project(monitor_detail, run, resolution.id, args.view, directory.parent / "monitor_snapshots", args.cursor), primary=resolution)
        return await read("detail", run_id=args.workflow_run_id, view=args.view, cursor=args.cursor)
    selected = None
    if action == "status" and getattr(args, "fields", None) is not None:
        from .workflow_responses import run_status_fields, validate_run_fields
        if args.monitor_view:
            raise ValueError("--fields is not available with --monitor-view")
        selected = validate_run_fields(args.fields.split(","), run_status_fields(), "run status")
    if action == "status" and args.monitor_view:
        from .workflow_responses import monitor, monitor_snapshot
        from .workflow_cancellation import monitor_projection
        from .workflows import WorkflowStore
        if args.snapshot:
            if await bounded_caller(directory) is not None:
                raise ValueError("Monitor snapshots are only available to the local Monitor")
            resolution = await asyncio.to_thread(id_resolution.resolve_run, directory.parent, args.workflow_run_id, managed=None)
            run = None if args.cursor else await read("status", run_id=resolution.id, _bounded_read=True)
            if run is not None and run.get('catalog_state', {}).get('status') in {'preparing', 'blocked'}:
                return run
            if run is not None:
                run = await asyncio.to_thread(_project, monitor_projection, WorkflowStore(directory.parent), run)
            return id_resolution.echo(_project(monitor_snapshot, run, resolution.id, directory.parent / "monitor_snapshots", args.cursor), primary=resolution)
        run = await read("status", run_id=args.workflow_run_id, _bounded_read=True)
        if run.get('catalog_state', {}).get('status') in {'preparing', 'blocked'}:
            return run
        return _project(monitor, await asyncio.to_thread(_project, monitor_projection, WorkflowStore(directory.parent), run))
    if action == "status" and getattr(args, "fields", None) is not None:
        from .workflow_responses import project_run
        return project_run(await read(action, run_id=args.workflow_run_id), selected)
    return await read(action, run_id=args.workflow_run_id)


def on_demand_projection(storage: Any, run: dict[str, Any]) -> dict[str, Any]:
    from .workflow_on_demand import projection
    from .approval_runtime import workflow_summary
    view = {} if run.get("kind") == "builder" else projection(storage, run)
    result = {**run, **view, "approval_summary": workflow_summary(storage.root / "tasks", run["workflow_run_id"])}
    if run.get("status") in {"running", "building", "starting", "pending"}:
        # Older records may retain a resolved suspension. Derive any live
        # descendant attention below, rather than displaying its stale reason.
        for key in ("attention_reason", "attention_source", "input_question", "input_question_items", "input_decision_id", "input_source", "optional_review_skip_available"):
            result.pop(key, None)
    if run.get("dependency_tree") or any(a.get("invocation") for a in run.get("activations", [])):
        from .workflow_inputs import project
        waiting = project(storage, run)
        result.update(waiting)
        if run.get("status") not in {"completed", "failed", "cancelled", "cancelling"}:
            inputs, attention = waiting["pending_inputs"], waiting["pending_attention"]
            if inputs:
                first = inputs[0]
                result.update(status="needs_input", input_question=first["question"], input_question_items=first["question_items"], input_decision_id=first["decision_id"], optional_review_skip_available=first["optional_review_skip_available"], input_source={key: first[key] for key in ("workflow_run_id", "workflow_name", "path")})
            elif attention and run.get("status") == "running":
                result["status"] = "needs_attention"
            if inputs or attention:
                names = []
                for item in [*inputs, *attention]:
                    label = f"{item['workflow_name']} ({item['node_title']})"
                    if item.get('kind') == 'usage_limit':
                        candidate = " / ".join(str(item[key]) for key in ('backend', 'model') if item.get(key))
                        label += " [usage_limit" + (f": {candidate}" if candidate else "") + "]"
                    names.append(label)
                result["attention_reason"] = f"{len(names)} workflow runs need input or attention: " + ", ".join(names)
    from .workflow_interactions import projection as interaction_projection
    result.update(interaction_projection(storage, result))
    from .workflow_messages import projection as message_projection
    result["messages"] = message_projection(result)
    return result
