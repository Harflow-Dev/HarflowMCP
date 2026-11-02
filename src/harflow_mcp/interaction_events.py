"""Read-only, bounded cycles over durable interaction episodes for native notifications.

Each completed cycle replays retained events with stable IDs. Consumers persist their dedup set;
there is deliberately no timestamp watermark that could skip an atomic write during a scan.
"""
from __future__ import annotations

import base64
import heapq
import json
import os
import re
import time
from pathlib import Path
from datetime import datetime
from types import SimpleNamespace

from . import approvals, store, workflow_notification_points as points
from .bounded_io import read_receipt
from .state_access import task_operation, task_layout
from .state_ownership import StateOwnershipError
from .workflow_run_documents import WorkflowRunDocumentError
from .task_documents import TaskDocumentError
from .workflow_receipt_documents import WorkflowReceiptDocumentError

SOURCE_LIMIT = 25
METADATA_LIMIT = 8 * 1024 * 1024
PAYLOAD_LIMIT = 128 * 1024


def _cursor(state):
    return base64.urlsafe_b64encode(json.dumps(state, separators=(',', ':')).encode()).decode()


def _parse(cursor):
    if cursor is None:
        return {'v': 1, 'phase': 'workflow', 'after': '', 'source': '', 'event': ''}
    try:
        if not isinstance(cursor, str) or len(cursor) > 4096:
            raise ValueError
        state = json.loads(base64.b64decode(cursor, altchars=b'-_', validate=True))
        if (set(state) != {'v', 'phase', 'after', 'source', 'event'} or state['v'] != 1
                or state['phase'] not in ('workflow', 'task')
                or any(not isinstance(state[key], str) or len(state[key]) > 512 for key in ('after', 'source', 'event'))):
            raise ValueError
        for key in ('after', 'source'):
            if state[key] and (not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,99}', state[key])):
                raise ValueError
        return state
    except (ValueError, TypeError, UnicodeError, KeyError):
        raise ValueError('Invalid interaction-events cursor') from None


def _sources(directory, suffix, after):
    try:
        with os.scandir(directory) as entries:
            return heapq.nsmallest(SOURCE_LIMIT + 1, (entry.name[:-len(suffix)] for entry in entries
                                  if entry.name.endswith(suffix) and entry.name[:-len(suffix)] > after))
    except FileNotFoundError:
        return []


def _event_key(event):
    # Per-source chronological ordering also handles opaque recovery checkpoint IDs.
    # A dynamically observed dead scheduler has no death time: keep its cursor key
    # stable while leaving the public observation timestamp truthful.
    timestamp = 0 if event.get('observed_at_read') else event['occurred_at']
    phase = 0 if event['state'] == 'pending' else 1
    return f'{timestamp:024.6f}:{phase}:{event["event_id"]}'


def _workflow_events(run, *, monitor_available=True):
    source = run['workflow_run_id']
    root = run.get('root_workflow_run_id') or (run.get('parent_link') or {}).get('root_workflow_run_id') or source
    common = {'workflow_run_id': root, 'source_workflow_run_id': source, 'task_id': None,
              'title': points._text(run.get('name'), 200)[0], 'monitor_available': monitor_available}
    events = []
    question_ids = {p.get('snapshot', {}).get('attention', {}).get('input_decision_id') for p in run.get('notification_points', []) if isinstance(p.get('snapshot'), dict) and isinstance(p['snapshot'].get('attention'), dict)}
    for point in run.get('notification_points', []):
        snapshot = point.get('snapshot') or {}
        attention = snapshot.get('attention')
        if not isinstance(attention, dict):
            continue
        origin = attention.get('source') or {}
        if origin.get('workflow_run_id') not in (None, source):
            continue  # The child owns this episode, including its answer identity.
        question = attention.get('kind') == 'needs_input'
        interaction_id = attention.get('input_decision_id') if question else point['key']
        kind = 'question' if question else 'recovery'
        events.append(dict(common, event_id=f'workflow:{source}:{interaction_id}:0:pending',
                           interaction_id=interaction_id, kind=kind, state='pending',
                           occurred_at=point.get('observed_at'), question=attention.get('question') or attention.get('reason') or 'Workflow needs attention'))
    for point in run.get('terminal_notification_points', []):
        if point.get('status') == 'failed':
            snapshot = point.get('snapshot') or {}
            result = snapshot.get('result') or {}
            events.append(dict(common, event_id=f'workflow:{source}:{point["key"]}:0:pending',
                               interaction_id=point['key'], kind='recovery', state='pending',
                               occurred_at=point.get('observed_at'), question=result.get('failure_reason') or 'Workflow failed; review recovery'))
    # Legacy or very recent writes may have current state but no frozen point yet.
    key = points.point_key(dict(run, parent_link=None))
    if key and key.startswith('attention:') and not any(e['interaction_id'] == ((run.get('input_decision_id') if run.get('status') == 'needs_input' else key)) for e in events):
        snapshot = points.bounded_snapshot(run)
        attention = snapshot.get('attention') or {}
        if (attention.get('source') or {}).get('workflow_run_id') in (None, source):
            question = attention.get('kind') == 'needs_input'
            events.append(dict(common, event_id=f'workflow:{source}:{attention.get("input_decision_id") if question else key}:0:pending', interaction_id=attention.get('input_decision_id') if question else key,
                               kind='question' if question else 'recovery', state='pending', occurred_at=run.get('updated_at'),
                               question=attention.get('question') or attention.get('reason') or 'Workflow needs attention'))
    from .workflow_attention import SCHEDULER, checkpoint_id
    from . import identity
    if not run.get('parent_link') and run.get('status') in SCHEDULER:
        owner = run.get('supervisor_identity')
        if owner and identity.identity_check(owner) == 'dead':
            token = checkpoint_id(None, run)
            if token:
                events.append(dict(common, event_id=f'workflow:{source}:{token}:0:pending',
                                   interaction_id=token, kind='recovery', state='pending',
                                   occurred_at=time.time(), question='Workflow scheduler stopped; review recovery', observed_at_read=True))
    for receipt in run.get('interaction_history', []):
        if receipt.get('workflow_run_id') not in (None, source):
            continue
        action = receipt.get('action', '')
        kind = 'question' if receipt['interaction_id'] in question_ids or not str(receipt['interaction_id']).startswith(('attention:', 'scheduler:')) else 'recovery'
        if str(receipt['interaction_id']).startswith('scheduler:') and not any(e['interaction_id'] == receipt['interaction_id'] and e['state'] == 'pending' for e in events):
            events.append(dict(common, event_id=f'workflow:{source}:{receipt["interaction_id"]}:0:pending',
                               interaction_id=receipt['interaction_id'], kind='recovery', state='pending',
                               occurred_at=receipt['answered_at'], question='Workflow scheduler stopped; recovery was requested'))
        events.append(dict(common, event_id=f'workflow:{source}:{receipt["interaction_id"]}:1:answered',
                           interaction_id=receipt['interaction_id'], kind=kind, state='answered',
                           occurred_at=receipt['answered_at'], answer=points._text(receipt.get('answer'), 2000)[0],
                           answered_by=receipt['answered_by'], action=action, answer_truncated=receipt.get('answer_truncated', False) or len(receipt.get('answer', '').encode('utf-8')) > 2000))
    return sorted(events, key=_event_key)



def _read_run(log_dir, identifier, budget):
    # Reuse descriptor-qualified reads without the constructor's mkdir/catalog side effects.
    from .workflows import WorkflowStore
    storage = WorkflowStore.__new__(WorkflowStore)
    storage.root = Path(log_dir).parent
    storage.runs = storage.root / 'workflow-runs'
    storage.state_layout = task_layout(Path(log_dir))
    return storage._read_run(identifier, metadata_byte_limit=4 * 1024 * 1024,
                             metadata_budget=budget, refresh_atomic=True)[0]


def _qualified_root(log_dir, run, budget):
    """Revalidate reciprocal invocation ancestry before routing a child event to its root."""
    current = run
    visited = set()
    claimed = (run.get('parent_link') or {}).get('root_workflow_run_id') or run['workflow_run_id']
    while current.get('parent_link'):
        if current['workflow_run_id'] in visited or len(visited) >= 16:
            raise ValueError('Workflow ancestry exceeds bound')
        visited.add(current['workflow_run_id'])
        link = current['parent_link']
        parent_id = link['workflow_run_id']
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,99}', parent_id):
            raise ValueError('Invalid parent identity')
        parent = _read_run(log_dir, parent_id, budget)
        activation = next((item for item in parent.get('activations', []) if item.get('id') == link.get('execution_id')), {})
        if ((activation.get('invocation') or {}).get('child_workflow_run_id') != current['workflow_run_id']
                or link.get('root_workflow_run_id') != claimed):
            raise ValueError('Workflow ancestry is not reciprocal')
        current = parent
    if current['workflow_run_id'] != claimed:
        raise ValueError('Workflow root differs')
    return current


def _monitor_available(root):
    from .workflow_interactions import owner_state
    return root.get('interaction_owner') == 'monitor' or owner_state(root, None)[0] in ('alive', 'dead')


def _approval_events(log_dir, task_id, budget):
    events = []
    available = [max(0, budget.metadata_limit - budget.metadata_bytes)]
    before = available[0]
    rows = approvals._read(log_dir, approvals._path(log_dir, task_id), task_id, source_budget=available)["requests"]
    budget.metadata_bytes += before - available[0]
    budget.metadata_decodes += 1
    for row in rows:
        if row.get('system_rejection'):
            continue
        common = {'workflow_run_id': row.get('root_workflow_run_id') or row.get('workflow_run_id'),
                  'source_workflow_run_id': row.get('workflow_run_id'), 'task_id': task_id,
                  'title': 'Permission request', 'kind': 'approval', 'interaction_id': row['request_id'],
                  'monitor_available': True}
        key = f'approval:{task_id}:{row["generation"]}:{row["request_id"]}'
        events.append(dict(common, event_id=key + ':0:pending', state='pending', occurred_at=row['created_at'],
                           question='Allow ' + points._text(row.get('tool_name'), 100)[0] + '?'))
        if row.get('decision') in ('approve', 'reject'):
            events.append(dict(common, event_id=key + ':1:answered', state='answered', occurred_at=row.get('decided_at') or row['created_at'],
                               answer=row['decision'], answered_by=row.get('responder', 'caller')))
    return sorted(events, key=_event_key)


def page(log_dir: Path, cursor: str | None = None, limit: int = 50):
    """Return one read-only page, leaving an absent or empty state root untouched."""
    _parse(cursor)
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    root = Path(log_dir).parent
    if not root.exists() or not any(root.iterdir()):
        return {"events": [], "next_cursor": None, "cycle_complete": True, "history_incomplete": False, "gaps": []}
    return _page(Path(log_dir), cursor, limit)


@task_operation
def _page(log_dir: Path, cursor: str | None = None, limit: int = 50):
    """Scan at most 25 source documents; complete cycles replay stable event IDs.

    Metadata reads and response bytes are bounded. Missing/unreadable sources and retained-history
    eviction are explicit gaps. Filenames alone are enumerated to select the next bounded slice;
    this path creates no catalogs, directories, subscriptions or delivery state.
    """
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('limit must be between 1 and 100')
    state = _parse(cursor)
    log_dir = Path(log_dir)
    events, gaps = [], []
    reads = 0
    budget = SimpleNamespace(metadata_limit=METADATA_LIMIT, metadata_bytes=0, metadata_decodes=0)
    while reads < SOURCE_LIMIT:
        workflow = state['phase'] == 'workflow'
        directory, suffix = (log_dir.parent / 'workflow-runs', '.json') if workflow else (log_dir, store.RECORD_SUFFIX)
        sources = [state['source']] if state['source'] else _sources(directory, suffix, state['after'])
        if not sources:
            if workflow:
                state = {'v': 1, 'phase': 'task', 'after': '', 'source': '', 'event': ''}
                continue
            return {'events': events, 'next_cursor': None, 'cycle_complete': True,
                    'history_incomplete': bool(gaps), 'gaps': gaps}
        identifier = sources[0]
        reads += 1
        try:
            if workflow:
                if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,99}', identifier):
                    raise ValueError('Invalid workflow source')
            else:
                store.validate_task_id(identifier)
            path = directory / (identifier + suffix)
            size = path.stat(follow_symlinks=False).st_size
            if not workflow:
                try:
                    size += approvals._path(log_dir, identifier).stat(follow_symlinks=False).st_size
                except FileNotFoundError:
                    pass
            if budget.metadata_bytes + size > METADATA_LIMIT and reads > 1:
                break
            if workflow:
                run = _read_run(log_dir, identifier, budget)
                if run.get('notification_evicted') or run.get('notification_point_error') is not None:
                    gaps.append({'source': identifier, 'reason': 'retained_history_incomplete'})
                if run.get('interaction_history_truncated') or len(run.get('interaction_history', [])) >= 100:
                    gaps.append({'source': identifier, 'reason': 'answer_history_at_retention_limit'})
                root = _qualified_root(log_dir, run, budget)
                candidates = _workflow_events(run, monitor_available=_monitor_available(root)) if run.get('kind') != 'builder' else []
                if any(point.get('snapshot') is None for point in run.get('notification_points', [])):
                    gaps.append({'source': identifier, 'reason': 'pending_snapshot_unavailable'})
            else:
                candidates = _approval_events(log_dir, identifier, budget)
                record = store._read(log_dir, identifier, include_prompt=False, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=budget)
                if record and record.status in ('completed', 'failed') and not store.outcome_unobserved(record):
                    try:
                        membership = read_receipt(log_dir.parent / 'workflow-owners' / (identifier + '.json'))
                    except FileNotFoundError:
                        membership = {}
                    candidates.append({'event_id': f'task:{identifier}:{record.status}', 'interaction_id': identifier,
                                       'kind': 'task', 'state': record.status, 'task_id': identifier,
                                       'workflow_run_id': membership.get('root_workflow_run_id') or membership.get('workflow_run_id'),
                                       'source_workflow_run_id': membership.get('workflow_run_id'),
                                       'title': points._text(record.title or identifier, 200)[0],
                                       'occurred_at': datetime.fromisoformat(record.finished_at or record.started_at).timestamp()})
                candidates.sort(key=_event_key)
        except StateOwnershipError:
            raise
        except (OSError, ValueError, TypeError, KeyError, AttributeError, TaskDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError):
            gaps.append({'source': identifier, 'reason': 'source_unavailable'})
            candidates = []
        for event in candidates:
            if _event_key(event) <= state['event']:
                continue
            if len(events) >= limit or len(json.dumps(events + [event], ensure_ascii=False).encode()) > PAYLOAD_LIMIT:
                state['source'] = identifier
                return {'events': events, 'next_cursor': _cursor(state), 'cycle_complete': False,
                        'history_incomplete': bool(gaps), 'gaps': gaps}
            events.append(event)
            state['event'] = _event_key(event)
        state.update(after=identifier, source='', event='')
    return {'events': events, 'next_cursor': _cursor(state), 'cycle_complete': False,
            'history_incomplete': bool(gaps), 'gaps': gaps}
