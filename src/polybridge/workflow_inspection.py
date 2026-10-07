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
    if task_id is None:
        from .workflow_delegation import result_evidence
        payload.update(result_evidence(activation, run))
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
    """Resolve requested task receipts and ancestors; never scan run history."""
    import logging
    from . import store as task_store
    from .workflows import WorkflowStore, _identifier
    storage = WorkflowStore(root=log_dir.parent)
    by_id = {entry["task_id"]: entry for entry in entries if entry.get("task_id")}
    run_cache: dict[str, dict[str, dict[str, Any]]] = {}
    resolved: dict[str, dict[str, Any] | None] = {}
    inherited = ("workflow_run_id", "workflow_name", "workflow_status", "workflow_settling", "interaction_owner", "execution_contract")

    def run_index(run_id: str) -> dict[str, dict[str, Any]]:
        if run_id in run_cache:
            return run_cache[run_id]
        index: dict[str, dict[str, Any]] = {}
        run_cache[run_id] = index
        run = storage.get_run(_identifier(run_id))
        from .workflow_invocation import tree_state
        state = tree_state(storage, run)
        for activation in run.get("activations", []):
            for task in activation.get("tasks", []):
                association = {"workflow_run_id": run["workflow_run_id"], "workflow_node_id": activation["node_id"], "workflow_execution_id": activation["id"], "workflow_role": activation["role"], "execution_contract": run.get("execution_contract"), "interaction_owner": run.get("interaction_owner", "caller"), "workflow_status": run.get("status"), "workflow_settling": run.get("settling", False), "workflow_name": run.get("name"), "root_workflow_run_id": state["root_workflow_run_id"], "workflow_tree_status": state["root_status"], "workflow_tree_settling": state["settling"], "suspended_via_root": state.get("suspended_via_root", False)}
                if run.get("orchestrator_session_owner_run_id") and activation.get("role") == "orchestrator":
                    association["workflow_session_owner_run_id"] = run["orchestrator_session_owner_run_id"]
                if run.get("execution_contract") == "delegation" and activation.get("role") == "node" and isinstance(activation.get("assignment_prompt"), str):
                    assignment = task.get("assignment_prompt", activation["assignment_prompt"])
                    association.update(display_prompt=assignment, prompt=assignment)
                if activation.get("role") == "node" and activation.get("result_error") and task["task_id"] == activation.get("tasks", [])[-1]["task_id"]:
                    association["workflow_result_error"] = activation["result_error"]
                index[task["task_id"]] = association
        return index

    def owner(task_id: str, visited: set[str]) -> dict[str, Any] | None:
        if task_id in resolved:
            return resolved[task_id]
        if task_id in visited or len(visited) >= 64:
            return None
        visited = visited | {task_id}
        association = None
        try:
            receipt = storage.owners / f"{_identifier(task_id)}.json"
            if receipt.exists():
                from .bounded_io import read_receipt
                run_id = read_receipt(receipt)["workflow_run_id"]
                association = run_index(run_id).get(task_id)
                if association is None:
                    raise ValueError("Task receipt does not match its workflow run")
            else:
                entry = by_id.get(task_id)
                ancestors = (entry.get("parent_task_id"), entry.get("spawned_by")) if entry is not None else ()
                if not any(ancestors):
                    record = task_store.read(log_dir, task_id)
                    ancestors = (record.parent_task_id, record.spawned_by) if record is not None else ()
                for ancestor in dict.fromkeys(ancestors):
                    if ancestor:
                        parent = owner(ancestor, visited)
                        if parent is not None:
                            association = {key: parent[key] for key in inherited if key in parent}
                            break
        except (OSError, ValueError, KeyError, TypeError):
            logging.getLogger(__name__).warning("Workflow decoration unavailable for %s", task_id, exc_info=True)
        resolved[task_id] = association
        return association

    return [{**entry, **(owner(entry["task_id"], set()) or {})} if entry.get("task_id") else entry for entry in entries]


def managed_page_reader(log_dir: Any) -> tuple[bool, tuple[dict[str, Any], dict[str, Any]] | None]:
    """Prepare bounded caller index, then preserve managed read authority without scans."""
    from . import lineage, store
    from .catalog import Catalog
    from .workflows import WorkflowStore
    task_catalog = Catalog(log_dir, store.RECORD_SUFFIX)
    with task_catalog.connect() as db:
        db.execute("DELETE FROM state WHERE key='authority_preparation'")
    if not task_catalog.ready():
        store.bootstrap_catalog(log_dir)
        # Do not combine a bootstrap decode batch with managed ownership decoding.
        return False, None
    detection = lineage.detect_catalog_caller(log_dir)
    if detection.undecidable is not None:
        raise ValueError('Workflow caller authority is undecidable: ' + detection.undecidable)
    if detection.caller is None:
        return True, None
    storage = WorkflowStore(root=log_dir.parent)
    receipt = storage.owners / f'{detection.caller.record.task_id}.json'
    if not receipt.exists():
        # Missing ownership is not evidence that a verified agent is unmanaged.
        # Resolve only through the bounded run catalog, never list_runs().
        if not storage._ownership_catalog().ready():
            page = storage.list_run_page()
            preparation = page.get('catalog_state', storage._ownership_catalog().state())
            if preparation['status'] == 'ready':
                preparation.update(status='preparing', source='caller_authority', reason='Validating workflow caller ownership after metadata indexing')
            with task_catalog.connect() as db:
                db.execute("INSERT OR REPLACE INTO state VALUES ('authority_preparation',?)", (json.dumps(preparation),))
            return False, None
        queue, visited, association = [(detection.caller.record.task_id, set())], set(), None
        with storage._ownership_catalog().connect() as ownership_db, Catalog(log_dir, store.RECORD_SUFFIX).connect() as task_db:
            while queue and len(visited) < 64:
                identifier, path = queue.pop(0)
                if identifier in path:
                    raise ValueError('Workflow caller ownership ancestry is cyclic')
                if identifier in visited:
                    continue
                visited.add(identifier)
                row = ownership_db.execute('SELECT payload FROM associations WHERE id=?', (identifier,)).fetchone()
                if row is not None:
                    association = json.loads(row[0])
                    break
                task_row = task_db.execute('SELECT payload FROM callers WHERE id=?', (identifier,)).fetchone()
                if task_row is not None:
                    task = json.loads(task_row[0])
                    queue.extend((task[key], path | {identifier}) for key in ('parent_task_id', 'spawned_by') if task.get(key))
            if queue and association is None:
                raise ValueError('Workflow caller ownership ancestry exceeds its bounded limit')
        if association is None:
            return True, None
    else:
        from .bounded_io import read_receipt
        receipt_value = read_receipt(receipt)
        association = {'workflow_run_id': receipt_value['workflow_run_id']}
    # Decode once, with the same aggregate budget as the subsequent page reader.
    # Receipt verification must not hide a second unbudgeted run decode.
    from .catalog import DeferredRead
    budget = Catalog(storage.runs, '.json')
    budget.share_budget(task_catalog)
    def load_run(identifier, *, _metadata_budget=None):
        run_value = storage.get_run(identifier, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=_metadata_budget)
        return run_value, float(run_value['created_at']), run_value.get('status') not in {'completed', 'failed', 'cancelled'}
    load_run.bounded_metadata = True
    try:
        loaded = budget._load_bounded(association['workflow_run_id'], load_run)
    except DeferredRead:
        preparation = {'status': 'preparing', 'source': 'caller_authority', 'reason': 'Preparing bounded workflow ownership metadata', 'pending_records': 1, 'blocked_records': 0}
        with task_catalog.connect() as db:
            db.execute("INSERT OR REPLACE INTO state VALUES ('authority_preparation',?)", (json.dumps(preparation),))
        return False, None
    if loaded is None:
        raise ValueError('Workflow ownership run is unavailable')
    run = loaded[0]
    if run.get('needs_direct_lookup'):
        preparation = {'status': 'blocked', 'source': 'caller_authority', 'reason': 'Workflow ownership metadata exceeds per-record read limit', 'pending_records': 0, 'blocked_records': 1, 'blocker_types': ['oversized_metadata']}
        with task_catalog.connect() as db:
            db.execute("INSERT OR REPLACE INTO state VALUES ('authority_preparation',?)", (json.dumps(preparation),))
        return False, None
    run.pop('_source_identity', None)
    if receipt.exists():
        matched = next((activation for activation in run.get('activations', []) if any(task.get('task_id') == detection.caller.record.task_id for task in activation.get('tasks', []))), None)
        if matched is None:
            raise ValueError('Workflow ownership receipt no longer matches its run')
        association = {'workflow_run_id': run['workflow_run_id'], 'workflow_node_id': matched['node_id'], 'workflow_role': matched['role'], 'node_id': matched['node_id'], 'role': matched['role'], 'activation_id': matched['id'], 'execution_contract': run.get('execution_contract'), 'status': run['status']}
    return True, (association, run) if association.get('role') == 'builder' or run.get('execution_contract') == 'delegation' else None


def page_indexing_response(log_dir: Any) -> dict[str, Any]:
    """No identifiers are exposed while bounded caller authority is incomplete."""
    from .catalog import Catalog
    from . import store
    state = Catalog(log_dir, store.RECORD_SUFFIX).state()
    if state['status'] == 'ready':
        with Catalog(log_dir, store.RECORD_SUFFIX).connect() as db:
            saved = db.execute("SELECT value FROM state WHERE key='authority_preparation'").fetchone()
        if saved:
            state = json.loads(saved[0])
        else:
            state.update(status='preparing', source='caller_authority', reason='Validating workflow caller ownership after metadata indexing')
    incomplete = state['status'] == 'blocked'
    result = {'items': [], 'related_headers': [], 'next_cursor': None, 'has_more': False,
              'bootstrap_pending': state['status'] == 'preparing', 'history_incomplete': incomplete,
              'authority_incomplete': incomplete, 'counts_complete': False,
              'catalog_state': state}
    if incomplete:
        result['note'] = state['reason']
    return result


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
    if association is None:
        return None
    run = storage.get_run(association["workflow_run_id"])
    return (association, run) if association.get("role") == "builder" or run.get("execution_contract") == "delegation" else None


def guard_task_read(task_id: str, managed: tuple[dict[str, Any], dict[str, Any]] | None) -> None:
    if managed is None:
        return
    association, run = managed
    activation = next((a for a in run.get("activations", []) if any(t.get("task_id") == task_id for t in a.get("tasks", []))), None)
    if activation is None:
        raise ValueError("Managed agents may only read tasks from their own workflow run")
    if association["role"] == "builder":
        if activation.get("role") != "builder":
            raise ValueError("Builders may only inspect their own builder tasks")
    elif association["role"] == "node":
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


def _authorized_linked_run(run: dict[str, Any], root: Any, target_id: str) -> dict[str, Any]:
    """Authorize inspection of a linked child run through a verified parent chain."""
    from .workflows import WorkflowStore
    store = WorkflowStore(root)
    current_id = target_id
    target = None
    for _ in range(8):  # Bounded by the nesting depth limit.
        try:
            current = store.get_run(current_id)
        except (OSError, ValueError, KeyError) as exc:
            raise ValueError("Unknown linked child workflow run") from exc
        if target is None:
            target = current
        link = current.get("parent_link") or {}
        parent_id = link.get("workflow_run_id")
        if not parent_id:
            raise ValueError("Requested run is not part of this workflow tree")
        try:
            parent = store.get_run(parent_id)
        except (OSError, ValueError, KeyError) as exc:
            raise ValueError("Linked parent run is unavailable") from exc
        activation = next((a for a in parent.get("activations", []) if a.get("id") == link.get("execution_id")), None)
        invocation = (activation or {}).get("invocation") or {}
        if invocation.get("child_workflow_run_id") != current_id:
            raise ValueError("Linked child run does not match its persisted invocation")
        tree = parent.get("dependency_tree") or {}
        parent_workflow_id = parent.get("workflow_id") or tree.get("root_workflow_id")
        edge = next((e for e in tree.get("edges", []) if e.get("from") == parent_workflow_id and e.get("node_id") == link.get("node_id") and e.get("to") == current.get("workflow_id")), None)
        if edge is None:
            raise ValueError("Linked child run has no edge in the pinned dependency tree")
        if parent_id == run["workflow_run_id"]:
            return target
        current_id = parent_id
    raise ValueError("Linked workflow chain exceeds the nesting limit")


def inspect_request(run: dict[str, Any], root: Any, request: dict[str, Any]) -> dict[str, Any]:
    """Runner-side inspection, with no MCP server, registry, or agent tool dependency."""
    from .events import events_path, read_page
    if not isinstance(request, dict):
        raise ValueError("Inspection request must be an object")
    allowed = {"execution_id", "task_id", "view", "cursor", "limit", "before_seq", "after_seq", "workflow_run_id"}
    if set(request) - allowed:
        raise ValueError("Unknown inspection request fields")
    execution_id = request.get("execution_id")
    if not isinstance(execution_id, str) or not execution_id:
        raise ValueError("Inspection requires execution_id")
    task_id = request.get("task_id")
    if task_id is not None and (not isinstance(task_id, str) or not task_id):
        raise ValueError("task_id must be a nonempty string")
    target_run = run
    target_id = request.get("workflow_run_id")
    if target_id is not None:
        if not isinstance(target_id, str) or not target_id:
            raise ValueError("workflow_run_id must be a nonempty string")
        if target_id != run["workflow_run_id"]:
            # Only a verified parent_link chain up to a direct child is inspectable.
            target_run = _authorized_linked_run(run, root, target_id)
    view = request.get("view", "result")
    limit = request.get("limit")
    if view == "result":
        if request.get("before_seq") is not None or request.get("after_seq") is not None:
            raise ValueError("Result view uses cursor, not event sequence cursors")
        if request.get("cursor") is not None and not isinstance(request["cursor"], str):
            raise ValueError("cursor must be a string")
        return result_page(target_run, execution_id, task_id=task_id, cursor=request.get("cursor"), limit=16000 if limit is None else limit)
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
    activation = next((a for a in target_run.get("activations", []) if a.get("id") == execution_id and a.get("role") == "node"), None)
    if activation is None:
        raise ValueError("Unknown node execution in this workflow run")
    task = selected_task(activation, task_id)
    if task is None or task.get("execution_kind") != "native_subagent":
        activation = execution(target_run, execution_id)
    if task is None:
        raise ValueError("Node execution has no dispatched task activity")
    page = read_page(events_path(root / "tasks", task["task_id"]), limit=limit, before_seq=before, after_seq=after)
    return {"workflow_run_id": target_run["workflow_run_id"], "execution_id": execution_id, "node_id": activation["node_id"], "task_id": task["task_id"], "events": page.events, "has_more": page.has_more, "next_before_seq": page.next_before_seq, "next_after_seq": page.next_after_seq, "skipped_oversized": page.skipped_oversized}


def guard_saved_workflow_authority(caller: Any, value: Any) -> None:
    """An ordinary agent may save only capabilities inside its recorded envelope.

    ``value`` is a definition or a resolved dependency tree; a tree checks every
    pinned node candidate at its saved freedom, and an orchestrator only for the
    root and definitions reached through Child-mode edges.
    """
    from . import workflows, backends
    from .backends.base import FREEDOMS
    from .backends.base import check_nested_enforcement
    record = caller.record
    if record.freedom not in FREEDOMS:
        raise ValueError("Saving a workflow requires known caller freedom")
    if isinstance(value, dict) and "workflows" in value and "root_workflow_id" in value:
        child_reached = {edge["to"] for edge in value.get("edges", []) if edge.get("orchestrator_mode", "child") == "child"}
        for ident, entry in value["workflows"].items():
            definition = entry["definition"]
            configs = []
            if ident == value["root_workflow_id"] or ident in child_reached:
                configs.append((definition["orchestrator"], "read_only", None))
            configs.extend((node["agent"], workflows.effective_freedom(node, "unrestricted", permission_policy="saved_node"), node.get("network")) for node in definition["nodes"] if node["type"] == "agent")
            _check_configs(record, configs)
        return
    definition = value
    configs = [(definition["orchestrator"], "read_only", None)]
    configs.extend((node["agent"], workflows.effective_freedom(node, "unrestricted", permission_policy="saved_node"), node.get("network")) for node in definition["nodes"] if node["type"] == "agent")
    _check_configs(record, configs)


def _check_configs(record: Any, configs: list[tuple[dict[str, Any], str, bool | None]]) -> None:
    from . import backends
    from .backends.base import FREEDOMS, check_nested_enforcement
    for config, freedom, network in configs:
        if FREEDOMS.index(freedom) > FREEDOMS.index(record.freedom):
            raise ValueError("Saved workflow access cannot exceed the caller's freedom")
        if network is True and record.network is False:
            raise ValueError("Saved workflow network cannot exceed the caller's network restriction")
        for candidate in [config, *config.get("fallbacks", [])]:
            backend_ = backends.get(candidate["backend"])
            try:
                enforcement = backend_.enforcement(freedom, network)
            except backends.NestedDispatchRefused:
                raise
            except backends.UnsupportedCapability:
                continue
            # A runnable weaker candidate remains forbidden even if another is safe.
            check_nested_enforcement(record.enforcement or {}, enforcement, parent_backend=record.backend, child_backend=backend_.name, parent_repo=record.repo_path, child_repo=record.repo_path)
