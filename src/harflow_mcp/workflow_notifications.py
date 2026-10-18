"""Durable root-workflow notification subscriptions and frozen delivery intents."""
from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from contextlib import closing
from pathlib import Path

from . import notifications as transport
from . import workflow_notification_points as points
from .workflows import WorkflowStore, _identifier


def initialize(db):
    db.execute('''CREATE TABLE IF NOT EXISTS workflow_subscriptions (
        id TEXT PRIMARY KEY, workflow_run_id TEXT NOT NULL, destination TEXT NOT NULL,
        events TEXT NOT NULL, state TEXT NOT NULL, from_sequence INTEGER NOT NULL,
        last_point_sequence INTEGER NOT NULL, run_mtime_ns INTEGER, run_size INTEGER,
        created REAL NOT NULL, updated REAL NOT NULL, UNIQUE(workflow_run_id,destination))''')
    db.execute('''CREATE TABLE IF NOT EXISTS workflow_deliveries (
        id TEXT PRIMARY KEY, subscription_id TEXT NOT NULL, event_key TEXT NOT NULL,
        event TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        due REAL NOT NULL DEFAULT 0, lease TEXT, lease_until REAL NOT NULL DEFAULT 0,
        payload BLOB NOT NULL, error TEXT, created REAL NOT NULL, updated REAL NOT NULL,
        UNIQUE(subscription_id,event_key))''')


def _events(events):
    if events is None:
        return ['terminal', 'attention']
    if not isinstance(events, list) or not events or any(e not in ('terminal', 'attention') for e in events) or len(set(events)) != len(events):
        raise ValueError('events must contain terminal, attention, or both')
    return sorted(events)


def _storage(log_dir):
    return WorkflowStore(root=Path(log_dir).parent)


def _run(log_dir, run_id):
    from .catalog import METADATA_BYTES, metadata_request
    with metadata_request() as budget:
        run = _storage(log_dir).get_run(_identifier(run_id), metadata_byte_limit=METADATA_BYTES, metadata_budget=budget)
    if run.get('parent_link'):
        raise ValueError('Subscribe to the root workflow run: ' + str(run.get('root_workflow_run_id') or run['parent_link'].get('workflow_run_id')))
    if run.get('kind') == 'builder':
        raise ValueError('Builder runs do not support workflow notifications')
    return run


def _public(row):
    return {k: (json.loads(row[k]) if k == 'events' else row[k]) for k in ('id', 'workflow_run_id', 'destination', 'events', 'state', 'from_sequence', 'last_point_sequence')}


def subscribe(log_dir, workflow_run_id, notify, events=None, *, reservation=False):
    transport.preflight(notify)
    _identifier(workflow_run_id)
    selected = _events(events)
    run = None if reservation else _run(log_dir, workflow_run_id)
    cursor = int(run.get('sequence', 0)) if run else -1
    current = points.point_key(run) if run else None
    all_points = _points(run) if run else []
    matching = [p['sequence'] for p in all_points if p['key'] == current]
    if current:
        cursor = min(matching, default=cursor) - 1
    now = time.time()
    with transport._db(log_dir) as db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT * FROM workflow_subscriptions WHERE workflow_run_id=? AND destination=?', (workflow_run_id, notify)).fetchone()
        if row:
            changed = json.loads(row['events']) != selected or row['state'] in ('unsubscribed', 'expired')
            db.execute("UPDATE workflow_subscriptions SET events=?,state=CASE WHEN state IN ('unsubscribed','expired') THEN 'active' ELSE state END,from_sequence=CASE WHEN state IN ('unsubscribed','expired') THEN ? ELSE from_sequence END,last_point_sequence=CASE WHEN state IN ('unsubscribed','expired') THEN ? ELSE last_point_sequence END,run_mtime_ns=NULL,run_size=NULL,updated=? WHERE id=?", (json.dumps(selected), cursor, cursor, now, row['id']))
        else:
            count = db.execute('SELECT (SELECT count(*) FROM subscriptions)+(SELECT count(*) FROM workflow_subscriptions)').fetchone()[0]
            if count >= transport.MAX_SUBSCRIPTIONS:
                raise ValueError('notification subscription capacity reached')
            db.execute('INSERT INTO workflow_subscriptions(id,workflow_run_id,destination,events,state,from_sequence,last_point_sequence,created,updated) VALUES(?,?,?,?,?,?,?,?,?)', (str(uuid.uuid4()), workflow_run_id, notify, json.dumps(selected), 'reserved' if reservation else 'active', cursor, cursor, now, now))
            changed = False
        result = _public(db.execute('SELECT * FROM workflow_subscriptions WHERE workflow_run_id=? AND destination=?', (workflow_run_id, notify)).fetchone())
        result['updated'] = changed
        return result


def activate(log_dir, run_id):
    _run(log_dir, run_id)
    with transport._db(log_dir) as db:
        db.execute("UPDATE workflow_subscriptions SET state='active',updated=? WHERE workflow_run_id=? AND state='reserved'", (time.time(), run_id))


def discard_unstarted(log_dir, run_id):
    if (_storage(log_dir).runs / f'{_identifier(run_id)}.json').exists():
        return False
    with transport._db(log_dir) as db:
        return bool(db.execute("DELETE FROM workflow_subscriptions WHERE workflow_run_id=? AND state='reserved'", (run_id,)).rowcount)


def status(log_dir, workflow_run_id):
    _identifier(workflow_run_id)
    connection = transport._existing_db(log_dir, 'ro')
    if connection is None:
        return {'subscriptions': [], 'deliveries': []}
    with closing(connection) as db:
        db.row_factory = sqlite3.Row
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='workflow_subscriptions'").fetchone():
            return {'subscriptions': [], 'deliveries': []}
        subscriptions = [_public(r) for r in db.execute('SELECT * FROM workflow_subscriptions WHERE workflow_run_id=? ORDER BY created LIMIT ?', (workflow_run_id, transport.MAX_SUBSCRIPTIONS))]
        deliveries = [{k: r[k] for k in ('id', 'subscription_id', 'event_key', 'event', 'state', 'attempts', 'error')} for r in db.execute('SELECT d.* FROM workflow_deliveries d JOIN workflow_subscriptions s ON d.subscription_id=s.id WHERE s.workflow_run_id=? ORDER BY d.created DESC LIMIT 50', (workflow_run_id,))]
        return {'subscriptions': subscriptions, 'deliveries': deliveries}


def unsubscribe(log_dir, workflow_run_id, notify=None):
    _identifier(workflow_run_id)
    with transport._db(log_dir) as db:
        db.execute('BEGIN IMMEDIATE')
        db.execute("UPDATE workflow_subscriptions SET state='unsubscribed',updated=? WHERE workflow_run_id=? AND (? IS NULL OR destination=?)", (time.time(), workflow_run_id, notify, notify))
        db.execute("UPDATE workflow_deliveries SET state='superseded',lease=NULL,lease_until=0,updated=? WHERE subscription_id IN (SELECT id FROM workflow_subscriptions WHERE workflow_run_id=? AND (? IS NULL OR destination=?)) AND state IN ('pending','retry','delivering')", (time.time(), workflow_run_id, notify, notify))
    return status(log_dir, workflow_run_id)


def _points(run):
    return sorted([p for name in ('notification_points', 'terminal_notification_points') for p in run.get(name, []) if isinstance(p, dict) and isinstance(p.get('key'), str) and type(p.get('sequence')) is int], key=lambda p: p['sequence'])


def _synthetic(run, source):
    key = points.point_key(run)
    if key is None:
        return None
    return {'key': key, 'event': points.event_for(key), 'status': run.get('status'), 'sequence': run.get('sequence', 0), 'observed_at': run.get('updated_at'), 'snapshot': points.bounded_snapshot(run), 'snapshot_source': source, 'requires_tree_check': points.event_for(key) == points.SETTLED and points.has_started_invocation(run)}


def _artifacts(log_dir, run_id):
    from . import artifacts
    from .catalog import metadata_request
    result = {'observed_at': time.time(), 'items': [], 'truncated': False, 'detail_ref': {'tool': 'get_workflow_artifacts', 'workflow_run_id': run_id}}
    try:
        with metadata_request():
            page = artifacts.workflow_page(log_dir, run_id, cursor=None, limit=10)
        result['items'] = [{k: item.get(k) if k not in ('node_id', 'execution_id', 'task_id') else (item.get('workflow') or {}).get(k, item.get(k)) for k in ('artifact_id', 'kind', 'path', 'node_id', 'execution_id', 'task_id', 'availability')} for item in page.get('items', [])]
        result['truncated'] = bool(page.get('next_cursor') or page.get('has_more') or page.get('owners')) or page.get('state') == 'unavailable'
    except Exception:
        result['truncated'] = True
    return result


def _payload(log_dir, subscription, point, missed):
    identifier = str(uuid.uuid4())
    snapshot = point.get('snapshot')
    # A missing historical snapshot must never be rebuilt from a later run state.
    data = dict(snapshot) if isinstance(snapshot, dict) else {k: None for k in ('name', 'kind', 'goal_revision', 'definition', 'attention', 'result')}
    data.update(version=1, notification_id=identifier, subscription_id=subscription['id'], event=point['event'], event_key=point['key'], workflow_run_id=subscription['workflow_run_id'], status=point.get('status', data.get('status')), sequence=point['sequence'], observed_at=point.get('observed_at'), artifacts=_artifacts(log_dir, subscription['workflow_run_id']), diagnostics={'workflow_run_record': subscription['workflow_run_id'] + '.json', 'status_tool': 'get_workflow_status'})
    for key in ('snapshot_source', 'snapshot_error', 'snapshot_evicted', 'requires_tree_check'):
        if key in point:
            data[key] = point[key]
    if missed:
        data['points_missed'] = True
    body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
    if len(body) > transport.MAX_PAYLOAD - 128:
        data['artifacts']['items'] = []
        data['artifacts']['truncated'] = True
        body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
    if len(body) > transport.MAX_PAYLOAD - 128:
        for field in ('attention', 'result'):
            data[field] = None
        body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
    if len(body) > transport.MAX_PAYLOAD - 128:
        raise ValueError('workflow notification payload exceeds bound')
    return identifier, body


def _detect(log_dir, subscription):
    storage = _storage(log_dir)
    path = storage.runs / (subscription['workflow_run_id'] + '.json')
    try:
        stat = path.stat()
    except FileNotFoundError:
        if subscription['state'] == 'reserved' and subscription['created'] < time.time() - 5 * transport.LEASE_SECONDS:
            discard_unstarted(log_dir, subscription['workflow_run_id'])
        return
    if subscription['state'] == 'active' and (stat.st_mtime_ns, stat.st_size) == (subscription['run_mtime_ns'], subscription['run_size']):
        return
    run = _run(log_dir, subscription['workflow_run_id'])
    retained = _points(run)
    key = points.point_key(run)
    if key and not any(p['key'] == key for p in retained):
        source = 'reconciled_state' if retained or run.get('notification_point_error') is not None else 'legacy_state'
        point = _synthetic(run, source)
        if point:
            retained.append(point)
    selected = json.loads(subscription['events'])
    cursor = subscription['last_point_sequence']
    watermarks = run.get('notification_evicted') or {}
    missed = any(watermarks.get(e, -1) > cursor for e in selected)
    intents = []
    for point in sorted(retained, key=lambda p: p['sequence']):
        if point['sequence'] <= cursor:
            continue
        category = 'terminal' if point['event'] == points.SETTLED else 'attention'
        if category in selected:
            intents.append((point, *_payload(log_dir, subscription, point, missed)))
    with transport._db(log_dir) as db:
        db.execute('BEGIN IMMEDIATE')
        current = db.execute('SELECT * FROM workflow_subscriptions WHERE id=?', (subscription['id'],)).fetchone()
        if not current or current['state'] not in ('active', 'reserved'):
            return
        attention_count = db.execute("SELECT count(*) FROM workflow_deliveries WHERE subscription_id=? AND event=?", (subscription['id'], points.ATTENTION)).fetchone()[0]
        state = 'active'
        now = time.time()
        for point, identifier, body in intents:
            if point['event'] == points.ATTENTION and attention_count >= 50:
                continue
            inserted = db.execute("INSERT OR IGNORE INTO workflow_deliveries(id,subscription_id,event_key,event,state,payload,created,updated) VALUES(?,?,?,?,'pending',?,?,?)", (identifier, subscription['id'], point['key'], point['event'], body, now, now)).rowcount
            if inserted and point['event'] == points.ATTENTION:
                attention_count += 1
            if point['key'] in ('terminal:completed', 'terminal:cancelled'):
                state = 'settled'
        last = max([cursor] + [p['sequence'] for p in retained])
        activity = current['updated']
        # Discovery time is not activity: an offline worker must not renew an old episode.
        for point in retained:
            observed = point.get('observed_at')
            if (point['sequence'] > current['last_point_sequence']
                    and type(observed) in (int, float) and math.isfinite(observed)):
                activity = max(activity, min(now, max(0, observed)))
        db.execute('UPDATE workflow_subscriptions SET state=?,last_point_sequence=?,run_mtime_ns=?,run_size=?,updated=? WHERE id=?', (state, last, stat.st_mtime_ns, stat.st_size, activity, subscription['id']))


def _defer(log_dir, identifier):
    now = time.time()
    with transport._db(log_dir) as db:
        db.execute("UPDATE workflow_deliveries SET updated=? WHERE id=? AND state IN ('pending','retry','delivering') AND lease_until<=?", (now, identifier, now))


def tick(log_dir):
    now = time.time()
    with transport._db(log_dir) as db:
        db.execute("UPDATE workflow_deliveries SET state='expired',lease=NULL,error='delivery expired' WHERE state IN ('pending','retry','delivering') AND created<? AND lease_until<=?", (now - transport.RETENTION_SECONDS, now))
        db.execute("UPDATE workflow_deliveries SET state='failed',lease=NULL,error='delivery attempts exhausted' WHERE state IN ('pending','retry','delivering') AND attempts>=? AND lease_until<=?", (transport.MAX_ATTEMPTS, now))
        db.execute("DELETE FROM workflow_deliveries WHERE state IN ('delivered','failed','expired','superseded') AND updated<?", (now - transport.RETENTION_SECONDS,))
        db.execute("DELETE FROM workflow_subscriptions WHERE state IN ('settled','expired','unsubscribed') AND updated<? AND NOT EXISTS (SELECT 1 FROM workflow_deliveries d WHERE d.subscription_id=workflow_subscriptions.id)", (now - transport.RETENTION_SECONDS,))
        subscriptions = db.execute("SELECT * FROM workflow_subscriptions WHERE state IN ('active','reserved') ORDER BY updated LIMIT ?", (transport.MAX_SUBSCRIPTIONS,)).fetchall()
    for subscription in subscriptions:
        try:
            _detect(log_dir, subscription)
        except Exception:
            continue
    with transport._db(log_dir) as db:
        # Detect durable progress before applying idle expiry: the server may have been offline.
        db.execute("UPDATE workflow_subscriptions SET state='expired' WHERE state='active' AND updated<?", (time.time() - transport.RETENTION_SECONDS,))
        candidates = db.execute("SELECT d.*,s.workflow_run_id,s.destination FROM workflow_deliveries d JOIN workflow_subscriptions s ON s.id=d.subscription_id WHERE d.state IN ('pending','retry','delivering') AND s.state NOT IN ('unsubscribed','reserved') AND d.due<=? AND d.lease_until<=? ORDER BY d.updated LIMIT ?", (now, now, transport.BATCH)).fetchall()
    for row in candidates:
        body = row['payload']
        data = json.loads(body)
        try:
            run = _run(log_dir, row['workflow_run_id'])
            current_key = points.point_key(run)
        except Exception:
            _defer(log_dir, row['id'])
            continue
        if row['event'] == points.ATTENTION and current_key != row['event_key']:
            with transport._db(log_dir) as db:
                db.execute("UPDATE workflow_deliveries SET state='superseded',updated=? WHERE id=? AND lease_until<=?", (time.time(), row['id'], time.time()))
            continue
        if data.get('requires_tree_check'):
            settled = 'unconfirmed' if current_key != row['event_key'] else points.tree_settlement(_storage(log_dir), run, now=time.time(), observed_at=data.get('observed_at'))
            if settled == 'pending':
                _defer(log_dir, row['id'])
                continue
            if settled == 'unconfirmed':
                data['tree_settlement_unconfirmed'] = True
                body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
        lease = str(uuid.uuid4())
        now = time.time()
        with transport._db(log_dir) as db:
            claimed = db.execute("UPDATE workflow_deliveries SET state='delivering',lease=?,lease_until=?,payload=?,attempts=attempts+1 WHERE id=? AND state IN ('pending','retry','delivering') AND lease_until<=? AND due<=? AND attempts<? AND EXISTS(SELECT 1 FROM workflow_subscriptions s WHERE s.id=subscription_id AND s.state!='unsubscribed') RETURNING attempts", (lease, now + transport.LEASE_SECONDS, body, row['id'], now, now, transport.MAX_ATTEMPTS)).fetchone()
        if claimed is None:
            continue
        error = None
        try:
            transport._deliver(transport.preflight(row['destination']), row['id'], body)
        except Exception:
            error = 'delivery attempt failed'
        attempts = claimed['attempts']
        state = 'delivered' if error is None else ('failed' if attempts >= transport.MAX_ATTEMPTS else 'retry')
        with transport._db(log_dir) as db:
            db.execute('UPDATE workflow_deliveries SET state=?,updated=?,due=?,lease=NULL,lease_until=0,error=? WHERE id=? AND lease=?', (state, time.time(), time.time() + min(3600, 2 ** attempts), error, row['id'], lease))
