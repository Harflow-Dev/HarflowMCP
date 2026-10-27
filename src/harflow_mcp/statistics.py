"""Finished retained-history cohorts with bounded snapshot pagination."""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import json
import math
import time

from . import accounting
from .state_ownership import StateOwnershipError, assert_owned
from .statistics_index import StatisticsIndex, IndexLimit, check_deadline, diagnostic
from .workflow_completion import qualified_finished_at, TERMINAL_METADATA_STATUSES

PROJECTION_VERSION = 1
PAGE_BYTES = 256 * 1024
CURSOR_BYTES = 4096


class StatisticsError(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def timestamp(value):
    if type(value) in (float, int):
        return float(value) if math.isfinite(value) else None
    if type(value) is str:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.timestamp() if parsed.tzinfo is not None else None
        except (ValueError, OverflowError):
            pass
    return None


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _generation(sources, keys):
    observations = []
    for row in sources:
        if row["key"] not in keys:
            continue
        payload = row.get("payload") or (row.get("error") or {}).get("context") or {}
        if row["kind"] == "task" and payload.get("status") not in TERMINAL_METADATA_STATUSES:
            identity = {key: payload.get(key) for key in ("task_id", "parent_task_id", "continuation_of_task_id",
                        "chain_root_task_id", "spawned_by", "root_task_id")}
            observations.append((row["key"], "nonterminal_ancestry", identity))
        else:
            observations.append((row["key"], row["witness"], payload.get("sequence")))
    return _digest(observations)


def _confirm(index, sources, lower, upper, generation):
    if not index.validate_sources(sources):
        raise StatisticsError("snapshot_expired", "Statistics sources changed during observation")
    refreshed = project(sources, lower, upper, deadline=index.deadline)
    if not refreshed["ownership_complete"] or _generation(sources, refreshed["relevant_keys"]) != generation:
        raise StatisticsError("snapshot_expired", "Statistics cohort ownership changed during observation")


def _common(run, root, start, end, kind, identity):
    return {"id": identity, "kind": kind, "cohort_started_at": root.get("created_at", start) if root else start,
            "started_at": start, "finished_at": end, "status": run.get("status"),
            "repo_path": run.get("repo_path", ""), "title": run.get("title") or run.get("name"),
            "root_workflow_run_id": root.get("workflow_run_id") if root else None,
            "workflow_run_id": run.get("workflow_run_id"), "workflow_id": run.get("workflow_id"),
            "workflow_revision": run.get("revision"), "workflow_name": run.get("name")}


def project(sources, since_epoch, before_epoch, *, deadline=None):
    """Project qualified flat source rows without replay, process probes or indexed status reads."""
    def check():
        if deadline is not None:
            check_deadline(deadline)
    by_kind = {kind: {} for kind in ("task", "run", "owner")}
    source_map = {}
    for row in sources:
        check()
        source_map[row["key"]] = row
        payload = row.get("payload") or (row.get("error") or {}).get("context")
        if payload:
            by_kind[row["kind"]][row["id"]] = payload
    tasks, runs, receipts = (by_kind[k] for k in ("task", "run", "owner"))
    ownership, attempts, diagnostics = {}, {}, []
    ownership_issues = []
    fatal_coverage = False
    relevant = set()

    def problem(code, message, *scope):
        nonlocal fatal_coverage
        value = diagnostic(code, message)
        if scope:
            ownership_issues.append((set(scope), value))
        else:
            fatal_coverage = True
            if len(diagnostics) < 16:
                diagnostics.append(value)

    # A receipt and the run's actual activation must agree. Run attempts also
    # identify ownership when an older writer did not create a receipt.
    for run_id, run in runs.items():
        check()
        for activation in run.get("activations", []):
            for attempt in activation.get("tasks", []):
                task_id = attempt["task_id"]
                candidate = (run_id, activation, attempt)
                if task_id in attempts and attempts[task_id][0] != run_id:
                    problem("ownership_ambiguous", "Task appears in multiple workflow runs", "task:" + task_id, "run:" + run_id, "run:" + attempts[task_id][0])
                else:
                    attempts[task_id] = candidate
                    ownership[task_id] = run_id
    for task_id, receipt in receipts.items():
        check()
        run_id = receipt["workflow_run_id"]
        if task_id in ownership and ownership[task_id] != run_id:
            problem("ownership_ambiguous", "Receipt conflicts with workflow activation", "task:" + task_id, "run:" + run_id)
        if run_id not in runs:
            problem("ownership_unavailable", "Receipt workflow run is unavailable", "task:" + task_id, "run:" + run_id)
        elif task_id not in attempts:
            problem("ownership_ambiguous", "Receipt no longer matches workflow activation", "task:" + task_id, "run:" + run_id)
        ownership[task_id] = run_id

    def root_for(run_id):
        seen, declared_roots = set(), set()
        while run_id in runs and run_id not in seen:
            check()
            seen.add(run_id)
            run = runs[run_id]
            parent = run.get("parent_link")
            if not parent:
                if declared_roots and declared_roots != {run_id}:
                    problem("ownership_ambiguous", "Workflow parent and root identities conflict")
                    return None
                return run
            if parent.get("root_workflow_run_id"):
                declared_roots.add(parent["root_workflow_run_id"])
            run_id = parent.get("workflow_run_id")
        problem("ownership_unavailable", "Workflow root ancestry is unavailable or cyclic")
        return None

    roots = {}
    for run_id in runs:
        roots[run_id] = root_for(run_id)
    for task_id, receipt in receipts.items():
        root = roots.get(receipt["workflow_run_id"])
        if root and receipt.get("root_workflow_run_id", root["workflow_run_id"]) != root["workflow_run_id"]:
            problem("ownership_ambiguous", "Receipt root identity conflicts with workflow ancestry", "task:" + task_id, "run:" + receipt["workflow_run_id"])

    # Nested harness dispatches inherit the owning run, including ancestors
    # outside the requested date window. Resolve once with cycle detection.
    resolved = dict(ownership)
    ancestry = {}
    unknown_owners = set()
    def task_owner(task_id, seen=None):
        check()
        if task_id in resolved:
            return resolved[task_id]
        seen = set() if seen is None else seen
        if len(seen) >= 128:
            unknown_owners.add(task_id)
            problem("ownership_unavailable", "Task ancestry depth exceeds bounded projection", *("task:" + key for key in seen | {task_id}))
            return None
        if task_id in seen:
            unknown_owners.add(task_id)
            problem("ownership_ambiguous", "Task ancestry cycle", *("task:" + key for key in seen | {task_id}))
            return None
        seen.add(task_id)
        task = tasks.get(task_id)
        if task is None:
            unknown_owners.add(task_id)
            problem("ownership_unavailable", "Task ancestor metadata is unavailable", *("task:" + key for key in seen))
            return None
        parents = [task.get(k) for k in ("spawned_by", "parent_task_id", "continuation_of_task_id") if task.get(k)]
        if task.get("root_task_id") and task["root_task_id"] != task_id:
            parents.append(task["root_task_id"])
        inherited = set()
        linked = set()
        for parent in parents:
            if parent == task_id:
                unknown_owners.add(task_id)
                problem("ownership_ambiguous", "Task is its own ancestor", "task:" + task_id)
                continue
            linked.add(parent)
            owner = task_owner(parent, set(seen))
            if parent in unknown_owners:
                unknown_owners.add(task_id)
            if owner:
                inherited.add(owner)
            linked.update(ancestry.get(parent, set()))
        if len(inherited) > 1:
            unknown_owners.add(task_id)
            problem("ownership_ambiguous", "Task ancestry has conflicting workflow ownership", "task:" + task_id)
        resolved[task_id] = next(iter(inherited), None)
        ancestry[task_id] = linked
        return resolved[task_id]

    for task_id in tasks:
        task_owner(task_id)

    def include_run_ancestry(run_id):
        visited = set()
        while run_id in runs and run_id not in visited:
            visited.add(run_id)
            relevant.add("run:" + run_id)
            parent = runs[run_id].get("parent_link") or {}
            run_id = parent.get("workflow_run_id") or parent.get("root_workflow_run_id")

    items = []
    finished_roots = {}
    for root in roots.values():
        check()
        if root is None:
            continue
        run_id = root["workflow_run_id"]
        if run_id in finished_roots or root.get("status") not in TERMINAL_METADATA_STATUSES:
            continue
        start, end = timestamp(root.get("created_at")), timestamp(qualified_finished_at(root))
        if start is None or not since_epoch <= start < before_epoch:
            continue
        relevant.add("run:" + run_id)
        if end is None or end < start:
            problem("completion_unavailable", "Finished root lacks qualified completion time")
            continue
        finished_roots[run_id] = root
        item = _common(root, root, start, end, "workflow", "workflow:" + run_id)
        item["wall_seconds"] = end - start
        items.append(item)
    for task_id, task in tasks.items():
        check()
        if task.get("status") not in TERMINAL_METADATA_STATUSES:
            continue
        owner_id = resolved.get(task_id)
        root = roots.get(owner_id) if owner_id else None
        if task_id in unknown_owners:
            start = timestamp(task.get("started_at"))
            if start is None or since_epoch <= start < before_epoch or root and root["workflow_run_id"] in finished_roots:
                relevant.add("task:" + task_id)
                relevant.update("task:" + key for key in ancestry.get(task_id, set()))
            continue
        if owner_id and (root is None or root["workflow_run_id"] not in finished_roots):
            start = timestamp(task.get("started_at"))
            if root is None and (start is None or since_epoch <= start < before_epoch):
                relevant.update({"task:" + task_id, "run:" + owner_id})
            continue
        start, end = timestamp(task.get("started_at")), timestamp(task.get("finished_at"))
        if not owner_id and start is None:
            relevant.add("task:" + task_id)
            problem("completion_unavailable", "Terminal task cohort start is unavailable")
            continue
        if not owner_id and not since_epoch <= start < before_epoch:
            continue
        relevant.add("task:" + task_id)
        if start is None or end is None or end < start:
            problem("completion_unavailable", "Finished task lacks bounded completion timestamps")
            continue
        for ancestor in ancestry.get(task_id, set()):
            relevant.add("task:" + ancestor)
            if "owner:" + ancestor in source_map:
                relevant.add("owner:" + ancestor)
        if "owner:" + task_id in source_map:
            relevant.add("owner:" + task_id)
        run = runs.get(owner_id, {})
        if owner_id:
            include_run_ancestry(owner_id)
        item = _common({**task, **{k: run.get(k) for k in ("workflow_run_id", "workflow_id", "revision", "name")}}, root,
                       start, end, "execution", "execution:" + task_id)
        activation, attempt = attempts.get(task_id, (None, {}, {}))[1:]
        evidence = task.get("accounting") or accounting.unavailable(task_id, "legacy_missing_accounting")
        intervals = [v for v in evidence["qualified_wait_intervals"] if v["execution_id"] == task_id]
        item.update(accounting.duration(start, end, intervals))
        kind = "continuation" if task.get("continuation_of_task_id") else "resume" if task.get("parent_task_id") else "initial"
        if activation.get("role") == "native_control":
            kind = "control"
        item.update(task_id=task_id, parent_task_id=task.get("parent_task_id"),
                    continuation_of_task_id=task.get("continuation_of_task_id"), chain_root_task_id=task.get("chain_root_task_id"),
                    node_id=activation.get("node_id"), activation_id=activation.get("id"),
                    requested_harness=task.get("backend"), requested_model=task.get("model"),
                    accounting=evidence, execution_kind=kind, duration_additive=True,
                    usage_additive=evidence["additive"], owner_task_id=None)
        if attempt.get("execution_kind") in {"native", "native_subagent"}:
            item.update(execution_kind="native", duration_additive=False, usage_additive=False, owner_task_id=attempt.get("transport_task_id") or attempt.get("owner_task_id"))
            if item["owner_task_id"]:
                item["accounting"] = accounting.unavailable(item["owner_task_id"], "native_owner_contribution")
        items.append(item)

    # Native logical children have no TaskRecord; their recorded lifecycle is
    # still a distinct execution. Transport usage is a reference, never a copy.
    for task_id, (run_id, activation, attempt) in attempts.items():
        check()
        if task_id in tasks or attempt.get("status") not in TERMINAL_METADATA_STATUSES:
            continue
        if attempt.get("execution_kind") not in {"native", "native_subagent"}:
            root = roots.get(run_id)
            if root and root["workflow_run_id"] in finished_roots:
                problem("execution_unavailable", "Finished workflow execution metadata is unavailable")
            continue
        root = roots.get(run_id)
        if root is None or root["workflow_run_id"] not in finished_roots:
            continue
        start, end = timestamp(attempt.get("started_at")), timestamp(attempt.get("finished_at"))
        if start is None or end is None or end < start:
            problem("completion_unavailable", "Native execution lacks qualified lifecycle timestamps")
            continue
        include_run_ancestry(run_id)
        run = runs[run_id]
        owner = attempt.get("transport_task_id") or attempt.get("owner_task_id")
        candidate = attempt.get("candidate") or {}
        item = _common(run, root, start, end, "execution", "native:" + run_id + ":" + task_id)
        item.update(accounting.duration(start, end))
        item.update(task_id=task_id, parent_task_id=None, continuation_of_task_id=None, chain_root_task_id=None,
                    node_id=activation.get("node_id"), activation_id=activation.get("id"),
                    requested_harness=candidate.get("backend"), requested_model=candidate.get("model"),
                    accounting=accounting.unavailable(owner or task_id, "native_owner_contribution"),
                    execution_kind="native", duration_additive=False, usage_additive=False, owner_task_id=owner)
        if owner in tasks:
            relevant.add("task:" + owner)
        items.append(item)
    for scope, value in ownership_issues:
        if scope & relevant:
            fatal_coverage = True
            if len(diagnostics) < 16:
                diagnostics.append(value)
    for row in sources:
        error = row.get("error")
        if not error:
            continue
        context = error.get("context")
        # Only freshly qualified current scope can establish irrelevance. An
        # old cache payload is never proof that a replaced source stayed active.
        known_time = context is not None and (context.get("status") == "running" or timestamp(context.get("started_at")) is not None)
        irrelevant = known_time and (context.get("status") == "running" or row["key"] not in relevant)
        if not irrelevant:
            fatal_coverage = True
        if len(diagnostics) < 16:
            diagnostics.append({key: error[key] for key in ("code", "message")})
    items = [item for item in items if not source_map.get("task:" + str(item.get("task_id")), {}).get("error")]
    items.sort(key=lambda i: (i["cohort_started_at"], i["kind"], i["id"]))
    return {"items": items, "relevant_keys": relevant, "ownership_complete": not fatal_coverage,
            "diagnostics": diagnostics}


def _retention():
    import os
    from .retention import retention_days
    raw = os.environ.get("HARFLOW_RETENTION_DAYS")
    if raw is not None:
        try:
            if int(raw) < 0:
                return {"state": "unknown", "days": None}
        except ValueError:
            return {"state": "unknown", "days": None}
    days = retention_days()
    return {"state": "disabled" if days is None else "days", "days": days}


def _base():
    return {"projection_version": PROJECTION_VERSION, "state": "unavailable", "snapshot_id": None,
            "generation": None, "observed_at": datetime.now(timezone.utc).isoformat(), "items": [],
            "next_cursor": None, "has_more": False, "history_complete": False, "ownership_complete": False,
            "accounting_coverage": {"eligible_executions": 0, "available_executions": 0},
            "retained_history_only": True, "retention": _retention(), "diagnostics": []}


def _decode_cursor(cursor):
    try:
        if type(cursor) is not str or len(cursor.encode()) > CURSOR_BYTES:
            raise ValueError
        payload = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True))
        if type(payload) is not dict or set(payload) != {"v", "root", "since", "before", "generation", "offset"}:
            raise ValueError
        if payload["v"] != PROJECTION_VERSION or type(payload["offset"]) is not int or payload["offset"] < 0:
            raise ValueError
        return payload
    except (ValueError, TypeError, UnicodeError):
        raise StatisticsError("invalid_cursor", "Invalid statistics cursor") from None


def statistics_page(layout, *, since, before, limit=100, cursor=None):
    """Expose one page only to a verified direct caller with owned compatible state."""
    from .takeover import caller_refusal
    refusal = caller_refusal(layout.tasks_directory)
    if refusal:
        raise StatisticsError(refusal[0], "Statistics require a verified direct caller: " + refusal[1])
    lower, upper = timestamp(since), timestamp(before)
    if type(since) is not str or type(before) is not str or lower is None or upper is None or lower >= upper or type(limit) is not int or not 1 <= limit <= 100:
        raise StatisticsError("invalid_params", "Statistics require ordered timezone-qualified bounds and limit 1..100")
    old = _decode_cursor(cursor) if cursor is not None else None
    binding = {"v": PROJECTION_VERSION, "root": str(layout.root), "since": lower, "before": upper}
    if old and any(old.get(k) != v for k, v in binding.items()):
        raise StatisticsError("invalid_cursor", "Statistics cursor belongs to another query or state root")
    result = _base()
    index = StatisticsIndex(layout)
    try:
        assert_owned(layout)
        prepared = index.prepare()
        if prepared["state"] != "ready":
            if old:
                raise StatisticsError("snapshot_expired", "Statistics source coverage changed; restart the page walk")
            result.update(state=prepared["state"], diagnostics=prepared["diagnostics"])
            return result
        sources = prepared["sources"]
        projection = project(sources, lower, upper, deadline=index.deadline)
        if not projection["ownership_complete"]:
            result["diagnostics"] = projection["diagnostics"]
            return result
        keys = projection["relevant_keys"]
        generation = _generation(sources, keys)
        if old and old["generation"] != generation:
            raise StatisticsError("snapshot_expired", "Retained statistics history changed; restart the page walk")
        _confirm(index, sources, lower, upper, generation)
        executions = [i for i in projection["items"] if i["kind"] == "execution"]
        result.update(state="ready", snapshot_id=generation, generation=generation, history_complete=True,
                      ownership_complete=True, accounting_coverage={"eligible_executions": len(executions),
                      "available_executions": sum(i["accounting"]["total_tokens"] is not None and i["usage_additive"] for i in executions)},
                      diagnostics=projection["diagnostics"])
        offset = old["offset"] if old else 0
        items = projection["items"]
        if offset > len(items):
            raise StatisticsError("invalid_cursor", "Statistics cursor order is invalid")
        while offset < len(items) and len(result["items"]) < limit:
            check_deadline(index.deadline)
            candidate = items[offset]
            if len(json.dumps({**result, "items": result["items"] + [candidate]}, separators=(",", ":")).encode()) > PAGE_BYTES - CURSOR_BYTES:
                if not result["items"]:
                    raise IndexLimit("page_limit", "One statistics fact exceeds the bounded page")
                break
            result["items"].append(candidate)
            offset += 1
        result["has_more"] = offset < len(items)
        if result["has_more"]:
            value = {**binding, "generation": generation, "offset": offset}
            result["next_cursor"] = base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode()
            if len(result["next_cursor"]) > CURSOR_BYTES:
                raise IndexLimit("page_limit", "Statistics cursor byte budget exceeded")
        _confirm(index, sources, lower, upper, generation)
        check_deadline(index.deadline)
        # Include serialization in the foreground budget and enforce the actual
        # returned page's byte bound, including disclosures and its opaque cursor.
        if len(json.dumps(result, allow_nan=False).encode()) > PAGE_BYTES:
            raise IndexLimit("page_limit", "Statistics page byte budget exceeded")
        check_deadline(index.deadline)
        result["observed_at"] = datetime.now(timezone.utc).isoformat()
        return result
    except (IndexLimit, StateOwnershipError, OSError) as exc:
        result = _base()
        result["diagnostics"] = [diagnostic(getattr(exc, "code", "statistics_unavailable"), str(exc))]
        return result
