"""Provider usage-limit hits and blockers of workflow runs, read from persisted run evidence.

Evidence is only ``activations[].tasks[].result.failure_diagnostic`` whose category is
``usage_limit`` (headless attempts and Codex-native children alike) and the
``orchestrator_usage_recovery`` / ``worker_usage_recovery`` checkpoints. ``evidence`` runs while a run
header is projected; the rows are stored in the catalog's ``run_limit_hits`` and ``run_limit_blocking``
side tables. ``recent`` answers from those tables over bounded windows and never retries, switches a
provider, continues a run or reads a task log. ``node_result``, ``evidence[]`` copies and the
``usage_recovery_history`` text are never evidence; the history only annotates a matching hit.
"""
from __future__ import annotations

import math
import time
from pathlib import Path
from typing import Any

from .backends.workflow_diagnostics import valid_reset_value
from .limit_hits import CATEGORY, _iso, bounded_diagnostic, reset_state
from .state_access import task_operation

SCAN_LIMIT = 500
CURRENT_LIMIT = 100
MAX_KEYS = 50
MAX_UNAVAILABLE = 20
RUN_HIT_LIMIT = 100  # newest attempts kept per run
RUN_KEY_LIMIT = 20  # further distinct keys kept per run, newest first
MAX_RUN_ROWS = RUN_HIT_LIMIT + RUN_KEY_LIMIT
TREE_LIMIT = 65
READ_BYTES = 4 * 1024 * 1024  # the catalog loaders' per-document read bound
CANDIDATE_LIMIT = 200  # child links examined to find the verified descendants
TERMINAL_STATUSES = frozenset({'completed', 'failed', 'cancelled', 'cancelling'})  # cancel never clears a recovery checkpoint
ACCEPTED_FILTERS = ('workflow_id', 'repo', 'since', 'before', 'root_workflow_run_id', 'current_only')


class Refused(ValueError):
    """The caller may not read workflow limit hits."""


def refuse_managed(managed: tuple[dict[str, Any], dict[str, Any]] | None, filters: dict[str, Any] | None) -> None:
    """Authority first: workers and builders are refused, and a managed caller may not name a tree root."""
    if managed is None:
        return
    if managed[0].get('role') != 'orchestrator':
        raise Refused('Workflow limit hits are available to direct callers and workflow orchestrators; workers and builders cannot read them')
    if (filters or {}).get('root_workflow_run_id') is not None:
        raise Refused('root_workflow_run_id is not supported for managed callers; results are already scoped to your own run and its descendants')


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def _text(value: Any, limit: int = 200) -> str | None:
    return value[:limit] if isinstance(value, str) and value else None


def _identity(task: dict[str, Any], result: dict[str, Any]) -> tuple[str, str | None, str | None]:
    """``(backend, requested model, observed model)`` of an attempt, from its own recorded fields."""
    def mapping(source: dict[str, Any], key: str) -> dict[str, Any]:
        return source[key] if isinstance(source.get(key), dict) else {}
    candidate, metadata = mapping(task, 'candidate'), mapping(task, 'harness_metadata')
    requested, effective, observed = mapping(metadata, 'requested'), mapping(metadata, 'effective'), mapping(metadata, 'observed')
    backend = _text(candidate.get('backend')) or _text(result.get('backend')) or _text(effective.get('backend')) or _text(requested.get('backend')) or ''
    model = _text(candidate.get('model')) or _text(requested.get('model')) or _text(result.get('model'))
    seen = _text(result.get('observed_model')) or _text(observed.get('model')) or _text(result.get('active_model'))
    return backend, model, seen


def _observed(run: dict[str, Any], activation: dict[str, Any], task: dict[str, Any]) -> tuple[float, str] | None:
    """When the attempt settled, with the field that says so; the first usable field wins."""
    for source, value in (('finished_at', task.get('finished_at')), ('started_at', task.get('started_at')),
                          ('activation_finished_at', activation.get('finished_at')), ('activation_created_at', activation.get('created_at')),
                          ('run_updated_at', run.get('updated_at'))):
        stamp = _number(value)
        if stamp is not None:
            return stamp, source
    return None


def evidence(run: dict[str, Any], meta: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any] | None, dict[str, Any] | None]:
    """The ``run_limit_hits`` rows of a run document, its ``run_limit_blocking`` row and, for a run over the
    retention cap, its ``run_limit_meta`` row; each else none.

    One row per evidence-bearing attempt, keyed by the attempt's task id: retries, resumed attempts and
    fallback candidates are separate rows, and so are native children that share a transport (each
    carries its own ``transport_task_id``). Nothing in a run document marks a child's diagnostic as a
    copy of its transport's rejection, so no rows are merged. A run with more than ``RUN_HIT_LIMIT`` hits
    keeps its newest ``RUN_HIT_LIMIT`` plus the newest hit of up to ``RUN_KEY_LIMIT`` further (backend,
    requested model, observed model) keys, at most ``MAX_RUN_ROWS`` rows, and records that once in its meta row.
    """
    run_id = run['workflow_run_id']
    root_id = _text((run.get('parent_link') or {}).get('root_workflow_run_id')) or run_id
    workflow_id, repo_path = meta.get('workflow_id'), meta.get('repo_path') or ''
    found: list[dict[str, Any]] = []
    for activation in run.get('activations') or ():
        if not isinstance(activation, dict) or not isinstance(activation.get('tasks'), list):
            continue
        for attempt, task in enumerate(activation['tasks'], 1):
            if not isinstance(task, dict) or not isinstance(task.get('task_id'), str) or not task['task_id']:
                continue
            result = task.get('result') if isinstance(task.get('result'), dict) else {}
            diagnostic = result.get('failure_diagnostic')
            if not isinstance(diagnostic, dict) or diagnostic.get('category') != CATEGORY:
                continue
            observed = _observed(run, activation, task)
            if observed is None:
                continue
            backend, requested, seen = _identity(task, result)
            payload: dict[str, Any] = {'task_id': task['task_id'], 'transport_task_id': _text(task.get('transport_task_id')),
                                       'execution_id': _text(activation.get('id')), 'node_id': _text(activation.get('node_id')),
                                       'role': _text(activation.get('role')), 'attempt': attempt, 'observed_at_source': observed[1],
                                       'diagnostic': bounded_diagnostic(diagnostic)}
            if valid_reset_value(diagnostic.get('reset_at')):
                payload['reset_at'] = diagnostic['reset_at']
            found.append({'hit_key': task['task_id'], 'root_id': root_id, 'workflow_id': workflow_id, 'repo_path': repo_path, 'backend': backend,
                          'requested_model': requested, 'observed_model': seen, 'finished_at': observed[0], 'payload': payload})
    found.sort(key=lambda hit: (hit['finished_at'], hit['hit_key']), reverse=True)
    _annotate(run, found)
    meta = None
    if len(found) > RUN_HIT_LIMIT:
        # Keep the newest hits plus the newest hit of up to RUN_KEY_LIMIT further keys, so the latest hit per
        # key stays correct unless a run has more keys than that. The row count per run is at most MAX_RUN_ROWS.
        kept, keys = found[:RUN_HIT_LIMIT], {_key(hit) for hit in found[:RUN_HIT_LIMIT]}
        dropped_key = False
        for hit in found[RUN_HIT_LIMIT:]:
            if _key(hit) not in keys:
                if len(kept) < MAX_RUN_ROWS:
                    keys.add(_key(hit))
                    kept.append(hit)
                else:
                    dropped_key = True
        found = kept
        meta = {'root_id': root_id, 'workflow_id': workflow_id, 'repo_path': repo_path, 'keys_exceeded': dropped_key}
    return found, _blocking(run, root_id, workflow_id, repo_path, found), meta


def _key(hit: dict[str, Any]) -> tuple[str, str | None, str | None]:
    return hit['backend'], hit['requested_model'], hit['observed_model']


def _annotate(run: dict[str, Any], hits: list[dict[str, Any]]) -> None:
    """Mark the hit each accepted recovery answered; ``hits`` are newest first.

    History is walked in acceptance order. An entry answers the newest still-unannotated hit of the same
    backend, requested model and diagnostic (and execution, when the entry names one) that was observed
    no later than the answer. When two such hits tie in time the match is uncertain and nothing is marked.
    """
    history = run.get('usage_recovery_history')
    entries = [entry for entry in history if isinstance(entry, dict) and isinstance(entry.get('accepted_answer'), str)
               and isinstance(entry.get('failure_diagnostic'), dict) and _number(entry.get('accepted_at')) is not None] if isinstance(history, list) else []
    for entry in sorted(entries, key=lambda entry: _number(entry['accepted_at'])):
        diagnostic = bounded_diagnostic(entry['failure_diagnostic'])
        candidate = entry['failed_candidate'] if isinstance(entry.get('failed_candidate'), dict) else {}
        execution_id = _text(entry.get('execution_id'))
        matches = [hit for hit in hits if 'recovery' not in hit['payload'] and hit['finished_at'] <= _number(entry['accepted_at'])
                   and hit['payload']['diagnostic'] == diagnostic and hit['backend'] == (_text(candidate.get('backend')) or '')
                   and hit['requested_model'] == _text(candidate.get('model')) and (execution_id is None or hit['payload']['execution_id'] == execution_id)]
        if matches and not (len(matches) > 1 and matches[1]['finished_at'] == matches[0]['finished_at']):
            matches[0]['payload']['recovery'] = {'accepted': True, 'answer': entry['accepted_answer'][:100]}


def _blocking(run: dict[str, Any], root_id: str, workflow_id: Any, repo_path: str, hits: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The run's pending usage-limit recovery unless the run is finished or cancelling; orchestrator first."""
    if run.get('status') in TERMINAL_STATUSES:
        return None
    recovery, worker = run.get('orchestrator_usage_recovery'), False
    if not isinstance(recovery, dict):
        recovery, worker = run.get('worker_usage_recovery'), True
    if not isinstance(recovery, dict):
        return None
    diagnostic = recovery['failure_diagnostic'] if isinstance(recovery.get('failure_diagnostic'), dict) else {}
    execution_id = _text(recovery.get('execution_id')) if worker else None
    candidate = recovery['failed_candidate'] if isinstance(recovery.get('failed_candidate'), dict) else {}
    backend, model = _text(candidate.get('backend')), _text(candidate.get('model'))
    if worker:
        # A worker checkpoint names its execution; the failed candidate is its newest limited attempt.
        source = next((hit for hit in hits if hit['payload']['execution_id'] == execution_id), None)
        if source is not None:
            backend, model = source['backend'] or None, source['requested_model']
    payload: dict[str, Any] = {'decision_id': _text(recovery.get('decision_id')), 'role': 'worker' if worker else _text(recovery.get('role')) or 'orchestrator',
                               'node_id': _text(recovery.get('node_id')) if worker else None, 'execution_id': execution_id,
                               'run_status': _text(run.get('status')), 'backend': backend, 'requested_model': model, 'diagnostic': bounded_diagnostic(diagnostic)}
    if valid_reset_value(diagnostic.get('reset_at')):
        payload['reset_at'] = diagnostic['reset_at']
    return {'root_id': root_id, 'workflow_id': workflow_id, 'repo_path': repo_path, 'payload': payload}


def validate_filters(filters: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize the filters; ``since`` and ``before`` become epoch bounds and unset values drop out."""
    from .task_reads import _unknown_names
    filters = dict(filters or {})
    unknown = [key for key in filters if key not in ACCEPTED_FILTERS]
    if unknown:
        raise ValueError(_unknown_names('workflow limit hit filters', unknown, ', '.join(ACCEPTED_FILTERS), vocabulary=set(ACCEPTED_FILTERS), accepted_label='accepted filters'))
    current_only = filters.pop('current_only', False)
    if current_only is not None and not isinstance(current_only, bool):
        raise ValueError('current_only must be a boolean')
    root = filters.pop('root_workflow_run_id', None)
    if root is not None and (not isinstance(root, str) or not root.strip() or len(root.encode()) > 200):
        raise ValueError('root_workflow_run_id must be a nonblank string of at most 200 UTF-8 bytes')
    from .workflow_reads import validate_run_filters
    result = validate_run_filters(filters)
    if root is not None:
        result['root_workflow_run_id'] = root
    if current_only:
        result['current_only'] = True
    return result


def pending(index_state: dict[str, Any]) -> dict[str, Any]:
    """The reply while the caller's authority or the run index is not ready: nothing is exposed."""
    return {'current': [], 'latest': [], 'unavailable': [],
            'scan': {'examined': 0, 'limit': SCAN_LIMIT, 'current_examined': 0, 'current_limit': CURRENT_LIMIT, 'complete': False},
            'index_state': index_state}


def _detail_ref(run_id: str, execution_id: Any) -> dict[str, Any] | None:
    if not isinstance(execution_id, str) or not execution_id:
        return None
    return {'tool': 'get_workflow_run_detail', 'workflow_run_id': run_id, 'view': 'execution:' + execution_id}


def _hit_item(row: tuple[Any, ...], now: float) -> dict[str, Any]:
    stamp, run_id, backend, requested, observed, root_id, payload = row
    item = {'root_workflow_run_id': root_id, 'workflow_run_id': run_id, 'node_id': payload.get('node_id'), 'role': payload.get('role'),
            'execution_id': payload.get('execution_id'), 'attempt': payload.get('attempt'), 'task_id': payload.get('task_id'),
            'transport_task_id': payload.get('transport_task_id'), 'backend': backend, 'requested_model': requested, 'observed_model': observed,
            'observed_at': _iso(stamp), 'observed_at_source': payload.get('observed_at_source'), 'diagnostic': payload.get('diagnostic', {}),
            'reset': reset_state(payload, now)}
    if 'recovery' in payload:
        item['recovery'] = payload['recovery']
    ref = _detail_ref(run_id, payload.get('execution_id'))
    if ref is not None:
        item['detail_ref'] = ref
    return item


def _current_item(run_id: str, root_id: str, payload: dict[str, Any], now: float) -> dict[str, Any]:
    item = {'root_workflow_run_id': root_id, 'workflow_run_id': run_id, 'run_status': payload.get('run_status'), 'role': payload.get('role'),
            'node_id': payload.get('node_id'), 'execution_id': payload.get('execution_id'), 'decision_id': payload.get('decision_id'),
            'backend': payload.get('backend'), 'requested_model': payload.get('requested_model'), 'diagnostic': payload.get('diagnostic', {}),
            'reset': reset_state(payload, now)}
    ref = _detail_ref(run_id, payload.get('execution_id'))
    if ref is not None:
        item['detail_ref'] = ref
    return item


def _edge_verified(parent: dict[str, Any], activation: dict[str, Any], child: dict[str, Any], root: str) -> bool:
    """Verify one parent-to-child edge from the two documents alone.

    The parent is already verified, so the walk needs no chain: the child's ``parent_link`` must name this
    parent and the invoking execution, its root must be the caller's tree root, and the parent's pinned
    dependency tree must hold the edge for the invoking node (the same facts ``_authorized_linked_run``
    checks for each hop).
    """
    link = child.get('parent_link') or {}
    if link.get('workflow_run_id') != parent.get('workflow_run_id') or link.get('execution_id') != activation.get('id'):
        return False
    if (_text(link.get('root_workflow_run_id')) or link.get('workflow_run_id')) != root:
        return False
    tree = parent.get('dependency_tree') or {}
    parent_workflow_id = parent.get('workflow_id') or tree.get('root_workflow_id')
    return any(edge.get('from') == parent_workflow_id and edge.get('node_id') == link.get('node_id') and edge.get('to') == child.get('workflow_id')
               for edge in tree.get('edges', []) if isinstance(edge, dict))


def _descendants(store: Any, own: dict[str, Any], root: Path) -> tuple[list[str], list[dict[str, Any]], bool] | None:
    """The caller's own run and the descendants its persisted invocations name, each verified.

    Candidates come from the ``invocation.child_workflow_run_id`` of the caller's verified runs (at most
    ``CANDIDATE_LIMIT`` of them). Each child is read once with the loaders' byte bound and its single edge
    to the already-verified parent is checked (``_edge_verified``) before it counts toward the
    ``TREE_LIMIT`` verified members, so an unverifiable link can never displace a verified one. A child
    named by a verified invocation that fails the check, or cannot be read, makes the answer incomplete.
    Returns ``(run ids, unavailable runs, incomplete)``, or None while a named run has no settled catalog
    row (absent from the index, pending or changed), so membership is never reported complete early.
    """
    own_id = own['workflow_run_id']
    own_root = _text((own.get('parent_link') or {}).get('root_workflow_run_id')) or own_id
    if store.child_states([own_id])[own_id][0] == 'unresolved':
        return None
    members, seen, queue = [own_id], {own_id}, [own]
    unavailable: list[dict[str, Any]] = []
    incomplete, examined = False, 0
    while queue:
        run = queue.pop(0)
        links: dict[str, dict[str, Any]] = {}
        for activation in run.get('activations') or ():
            child = (activation.get('invocation') or {}).get('child_workflow_run_id') if isinstance(activation, dict) else None
            if isinstance(child, str) and child and child not in seen:
                seen.add(child)
                links[child] = activation
        children = list(links)
        if examined + len(children) > CANDIDATE_LIMIT:
            children, incomplete = children[:CANDIDATE_LIMIT - examined], True
        examined += len(children)
        states = store.child_states(children) if children else {}
        for child in children:
            state, reason, code = states[child]
            if state == 'unresolved':
                return None
            if state == 'blocked':
                unavailable.append({'workflow_run_id': child, 'reason': reason, 'code': code})
                continue
            if state == 'missing':
                continue
            try:
                target = store.get_run(child, metadata_byte_limit=READ_BYTES)
            except (OSError, ValueError, KeyError) as exc:
                unavailable.append({'workflow_run_id': child, 'reason': str(exc)[:200], 'code': 'workflow_run_unavailable'})
                continue
            if not _edge_verified(run, links[child], target, own_root):
                incomplete = True  # named by a verified invocation but not a verifiable child
                continue
            if len(members) >= TREE_LIMIT:
                incomplete = True
                continue
            members.append(child)
            queue.append(target)
    return members, unavailable[:MAX_UNAVAILABLE], incomplete or len(unavailable) > MAX_UNAVAILABLE


@task_operation
def recent(log_dir: Path, managed: tuple[dict[str, Any], dict[str, Any]] | None, filters: dict[str, Any] | None = None, *, now: float | None = None) -> dict[str, Any]:
    """Blocking recoveries and the latest hit per (backend, requested model, observed model).

    ``current`` lists runs blocked on a usage-limit recovery, at most ``CURRENT_LIMIT``. ``latest`` is
    the newest hit of each (backend, requested model, observed model) among the newest ``SCAN_LIMIT``
    hit rows of the window the indexed bound selects (see ``Catalog.run_limit_hit_window``), at most
    ``MAX_KEYS`` keys. A direct caller reads every run; a managed orchestrator reads its own run and
    the descendants its persisted invocation links verify, and a worker or builder is refused before
    any lookup. ``scan.complete`` is false when a window or key cap ended the answer early, while an in-scope
    run kept fewer attempts than it recorded, while runs are unavailable, or while the index is not ready.
    """
    from . import workflows
    filters = validate_filters(filters)
    now = time.time() if now is None else now
    refuse_managed(managed, filters)
    store = workflows.WorkflowStore(root=log_dir.parent)
    run_ids: list[str] | None = None
    truncated = False
    index_state: dict[str, Any] | None = None
    unavailable: list[dict[str, Any]] = []
    unavailable_capped = False
    if managed is not None:
        scope = None
        for _ in range(2):
            scope = _descendants(store, managed[1], log_dir.parent)
            if scope is not None:
                break
            store.prepare_run_catalog()
        if scope is None:
            return pending({'status': 'preparing', 'source': 'workflow_catalog', 'reason': 'Preparing bounded metadata index'})
        run_ids, unavailable, truncated = scope
        index_state = {'status': 'ready', 'source': 'workflow_catalog', 'pending_records': 0, 'blocked_records': 0}
    index, load = store.limit_hit_catalog()
    with index.connect() as db:
        if managed is None:
            index.bootstrap(db, load)
            index_state = index.state(db)
            sources, unavailable_capped = index.incomplete_sources(db, MAX_UNAVAILABLE)
            unavailable = [{'workflow_run_id': identifier, 'reason': reason, 'code': code} for identifier, reason, code in sources]
        current_rows, current_capped = index.run_limit_blocking_window(db, CURRENT_LIMIT, filters, run_ids)
        hit_rows, examined, capped = ([], 0, False) if filters.get('current_only') else index.run_limit_hit_window(db, SCAN_LIMIT, filters, run_ids)
        run_truncated = index.run_limit_truncated(db, filters, run_ids, any_truncation='since' in filters or 'before' in filters)
    latest: dict[tuple[str, str | None, str | None], dict[str, Any]] = {}
    keys_capped = False
    for row in hit_rows:
        key = (row[2], row[3], row[4])
        if key in latest:
            continue
        if len(latest) >= MAX_KEYS:
            keys_capped = True
            break
        latest[key] = _hit_item(row, now)
    complete = index_state['status'] == 'ready' and not (capped or keys_capped or current_capped or truncated or run_truncated or unavailable or unavailable_capped)
    return {'current': [_current_item(run_id, root_id, payload, now) for run_id, root_id, payload in current_rows], 'latest': list(latest.values()),
            'unavailable': unavailable,
            'scan': {'examined': examined, 'limit': SCAN_LIMIT, 'current_examined': len(current_rows), 'current_limit': CURRENT_LIMIT, 'complete': complete},
            'index_state': index_state}
