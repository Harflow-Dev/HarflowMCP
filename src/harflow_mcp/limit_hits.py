"""Recent provider usage-limit hits, read from persisted task evidence.

Evidence is only a persisted ``TaskRecord.failure_diagnostic`` whose category is ``usage_limit``;
provider errors and prose are never read. ``evidence`` runs while a task header is projected and the
result is stored in the catalog's ``task_limit_hits`` side table. ``recent`` answers from that table
over a bounded window of the newest rows and never retries, falls back or reads a task log.
"""
from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .backends.workflow_diagnostics import valid_reset_value
from .state_access import task_operation, task_layout

CATEGORY = 'usage_limit'
SCAN_LIMIT = 500
MAX_KEYS = 50
ACCEPTED_FILTERS = ('backend', 'model', 'since', 'repo', 'group')
_DIAGNOSTIC_TEXT = {'source': 100, 'reason': 500, 'provider_code': 100}


def _stamp(value: Any) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
    except (ValueError, OverflowError, OSError):
        return None
    return stamp if math.isfinite(stamp) else None


def _iso(stamp: float) -> str | None:
    try:
        return datetime.fromtimestamp(stamp, timezone.utc).isoformat()
    except (ValueError, OverflowError, OSError):
        return None


def bounded_diagnostic(diagnostic: dict[str, Any]) -> dict[str, Any]:
    """The usage-limit diagnostic fields a hit keeps, each bounded; shared with the workflow hits."""
    kept: dict[str, Any] = {'category': CATEGORY}
    for key, limit in _DIAGNOSTIC_TEXT.items():
        if isinstance(diagnostic.get(key), str):
            kept[key] = diagnostic[key][:limit]
    if type(diagnostic.get('http_status')) is int:
        kept['http_status'] = diagnostic['http_status']
    return kept


def evidence(record: Any) -> dict[str, Any] | None:
    """The side-table row for a persisted usage-limit diagnostic, else None.

    ``observed_at`` is the record's ``finished_at`` when it parses, else ``started_at``; a record
    with neither cannot be ordered and carries no hit.
    """
    diagnostic = record.failure_diagnostic
    if not isinstance(diagnostic, dict) or diagnostic.get('category') != CATEGORY:
        return None
    source, stamp = 'finished_at', _stamp(record.finished_at)
    if stamp is None:
        source, stamp = 'started_at', _stamp(record.started_at)
    if stamp is None:
        return None
    kept = bounded_diagnostic(diagnostic)
    group = record.group if isinstance(record.group, str) else None
    title = record.title if isinstance(record.title, str) else None
    payload = {'task_id': record.task_id, 'backend': record.backend, 'requested_model': record.model,
               'title': title[:1000] if title is not None else None,
               'group': group[:1000] if group is not None else None,
               'observed_at_source': source, 'diagnostic': kept}
    if valid_reset_value(diagnostic.get('reset_at')):
        payload['reset_at'] = diagnostic['reset_at']
    return {'observed_at': stamp, 'backend': record.backend, 'model': record.model if isinstance(record.model, str) else None,
            'grp': group if group is not None and len(group) <= 128 else None,
            'repo_path': record.repo_path if isinstance(record.repo_path, str) else '', 'payload': payload}


def reset_state(payload: dict[str, Any], now: float) -> dict[str, Any]:
    """``{state, raw?, at?}``: ``future``, ``elapsed`` or ``unknown`` against ``now``.

    A numeric reset is epoch seconds and an ISO8601 reset carries its offset. Anything that does not
    pass ``valid_reset_value`` is ``unknown`` with no ``raw``. ``elapsed`` only says the stated time
    has passed; it does not say the provider is available again.
    """
    raw = payload.get('reset_at')
    if not valid_reset_value(raw):
        return {'state': 'unknown'}
    at = float(raw) if type(raw) in (int, float) else _stamp(raw)
    iso = _iso(at) if at is not None else None
    if iso is None:
        return {'raw': raw, 'state': 'unknown'}
    return {'raw': raw, 'at': iso, 'state': 'future' if at > now else 'elapsed'}


def validate_filters(backend: Any = None, model: Any = None, since: Any = None, repo: Any = None, group: Any = None) -> dict[str, Any]:
    """Normalize the hit filters; ``since`` becomes an epoch bound and unset values drop out."""
    from .task_reads import validate_filters as task_filters
    values = {'backend': backend, 'model': model, 'since': since, 'repo': repo, 'group': group}
    if model is not None and (not isinstance(model, str) or not model or len(model.encode()) > 200):
        raise ValueError('model must be a nonempty string of at most 200 UTF-8 bytes')
    result = task_filters({key: value for key, value in values.items() if key != 'model'})
    if model is not None:
        result['model'] = model
    return result


@task_operation
def recent(log_dir: Path, filters: dict[str, Any] | None = None, *, now: float | None = None) -> dict[str, Any]:
    """The latest hit per (backend, requested model) among the newest ``SCAN_LIMIT`` persisted hits.

    ``scan.examined`` counts hit rows matching ``since``, ``backend`` and ``model``, before the repo and
    group filters;
    ``scan.complete`` is false when more hits exist than the window holds, when more than
    ``MAX_KEYS`` keys matched, or while the catalog is not ready.
    """
    from . import store
    from .catalog import Catalog
    filters = validate_filters(**(filters or {}))
    now = time.time() if now is None else now

    def load(identifier: str, *, _metadata_budget=None):
        return store._catalog_load(log_dir, identifier, _metadata_budget=_metadata_budget)
    load.bounded_metadata = True
    catalog = Catalog(log_dir, store.RECORD_SUFFIX, layout=task_layout(log_dir))
    with catalog.connect() as db:
        catalog.bootstrap(db, load)
        state = catalog.state(db)
        rows, examined, capped = catalog.limit_hit_window(db, SCAN_LIMIT, filters)
    seen: dict[tuple[str, str | None], dict[str, Any]] = {}
    truncated = False
    for stamp, backend, model, payload in rows:
        key = (backend, model)
        if key in seen:
            continue
        if len(seen) >= MAX_KEYS:
            truncated = True
            break
        iso = _iso(stamp)
        seen[key] = {'backend': backend, 'requested_model': model, 'task_id': payload.get('task_id'),
                     'title': payload.get('title'), 'group': payload.get('group'), 'observed_at': iso,
                     'observed_at_source': payload.get('observed_at_source'), 'diagnostic': payload.get('diagnostic', {}),
                     'reset': reset_state(payload, now)}
    complete = state['status'] == 'ready' and not capped and not truncated
    return {'hits': list(seen.values()), 'scan': {'examined': examined, 'limit': SCAN_LIMIT, 'complete': complete},
            'index_state': state}
