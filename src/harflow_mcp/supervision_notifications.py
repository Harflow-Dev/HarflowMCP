"""Independent frozen supervision-loss deliveries using the notification transport."""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections import OrderedDict

from . import notifications as transport, store, supervision
from .state_ownership import StateOwnershipError

NODE_LIMIT = 64
RUN_LIMIT = 64
_SCAN_CURSORS = OrderedDict()


def loss(record, log_dir):
    """Return confirmed loss evidence, excluding histories with an observed outcome."""
    observed = record.status in transport.TERMINAL and not store.outcome_unobserved(record)
    evidence = supervision.evidence(record, store.log_path(log_dir, record.task_id),
                                    observed_terminal=observed)
    if evidence['capture_state'] != 'lost_with_owner':
        return None
    generation = hashlib.sha256(json.dumps(record.owner, sort_keys=True,
                                           separators=(',', ':')).encode()).hexdigest()
    return generation, evidence


def _node(record, evidence, **membership):
    return {'task_id': record.task_id, **membership, 'supervision': evidence,
            'last_activity': transport._text(record.terminal_summary, 256)}


def _insert(log_dir, subscription, kind, generation, nodes, *, truncated=False):
    now = time.time()
    identifier = str(uuid.uuid4())
    event = 'task.supervision_lost' if kind == 'task' else 'workflow.supervision_lost'
    data = {'version': 1, 'notification_id': identifier, 'event': event,
            'subscription_id': subscription['id'], 'owner_generation': generation,
            'affected_nodes': nodes, 'affected_nodes_truncated': truncated,
            'recovered': True, 'outcome_observed': False,
            'next_step': 'Inspect recovery activity and repository changes; cancel before dispatching a replacement.'}
    key = 'task_id' if kind == 'task' else 'workflow_run_id'
    data[key] = subscription[key]
    # Reduce node count rather than exposing unrestricted records or exceeding transport budgets.
    while True:
        body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
        if len(body) <= transport.MAX_PAYLOAD:
            break
        if len(data['affected_nodes']) <= 1:
            raise ValueError('supervision notification payload exceeds bound')
        data['affected_nodes'] = data['affected_nodes'][:-1]
        data['affected_nodes_truncated'] = True
    with transport._db(log_dir) as db:
        db.execute('BEGIN IMMEDIATE')
        table = 'subscriptions' if kind == 'task' else 'workflow_subscriptions'
        current = db.execute('SELECT state FROM ' + table + ' WHERE id=?', (subscription['id'],)).fetchone()
        if current is None or current['state'] in ('unsubscribed', 'expired', 'reserved'):
            return
        db.execute("INSERT OR IGNORE INTO supervision_deliveries(id,subscription_id,subscription_kind,task_id,destination,event_key,event,state,payload,created,updated) VALUES(?,?,?,?,?,?,?,'pending',?,?,?)",
                   (identifier, subscription['id'], kind, nodes[0]['task_id'], subscription['destination'],
                    generation + (':' + nodes[0]['task_id'] if kind == 'workflow' else ''), event, body, now, now))


def _workflow_nodes(log_dir, root, *, subscription_id=None):
    """Resume a bounded traversal of verified child links and owned attempts."""
    from .catalog import METADATA_BYTES, metadata_request
    from .bounded_io import read_receipt
    from .workflow_notifications import _storage
    storage = _storage(log_dir)
    from .workflow_references import MAX_WORKFLOW_NESTING_DEPTH
    key = (str(log_dir), 'walk:' + root['workflow_run_id'], subscription_id)
    # Retain offsets, not documents or a frontier proportional to workflow width.
    frames = _SCAN_CURSORS.get(key) or [dict(run_id=root['workflow_run_id'], activation=0, task=0, child=False)]
    candidates = {}
    truncated = False

    def read_child(parent, activation, child_id, budget):
        child = storage.get_run(child_id, metadata_byte_limit=METADATA_BYTES, metadata_budget=budget)
        if not isinstance(child, dict):
            raise ValueError('Workflow child is unavailable')
        link = child.get('parent_link') or {}
        if not isinstance(link, dict):
            raise ValueError('Workflow child link is invalid')
        if (child.get('workflow_run_id') == child_id
                and (activation.get('invocation') or {}).get('child_workflow_run_id') == child_id
                and link.get('workflow_run_id') == parent['workflow_run_id']
                and link.get('execution_id') == activation.get('id')
                and link.get('root_workflow_run_id') == root['workflow_run_id']):
            return child
        return None

    with metadata_request() as budget:
        runs = [root]
        # Requalify retained ancestry before accepting any resumed candidates.
        for depth in range(1, len(frames)):
            previous = frames[depth - 1]
            activations = runs[-1].get('activations', [])
            index = previous['activation']
            activation = activations[index] if index < len(activations) else {}
            try:
                child = read_child(runs[-1], activation, frames[depth]['run_id'], budget)
            except StateOwnershipError:
                raise
            except (OSError, ValueError, TypeError):
                child = None
            if child is None:
                frames = frames[:depth]
                truncated = True
                break
            runs.append(child)
        for _ in range(RUN_LIMIT):
            if not frames or len(candidates) >= NODE_LIMIT:
                break
            frame, run = frames[-1], runs[-1]
            activations = run.get('activations', [])
            if frame['activation'] >= len(activations):
                frames.pop()
                runs.pop()
                continue
            activation = activations[frame['activation']]
            attempts = activation.get('tasks', [])
            if frame['task'] < len(attempts):
                attempt = attempts[frame['task']]
                frame['task'] += 1
                task_id = attempt.get('task_id')
                if task_id:
                    candidates[task_id] = (run['workflow_run_id'], activation)
                continue
            if frame['child']:
                frame.update(activation=frame['activation'] + 1, task=0, child=False)
                continue
            frame['child'] = True
            child_id = (activation.get('invocation') or {}).get('child_workflow_run_id')
            if not child_id:
                continue
            if len(frames) >= MAX_WORKFLOW_NESTING_DEPTH or child_id in {f['run_id'] for f in frames}:
                truncated = True
                continue
            try:
                child = read_child(run, activation, child_id, budget)
                if child is not None:
                    frames.append(dict(run_id=child_id, activation=0, task=0, child=False))
                    runs.append(child)
            except StateOwnershipError:
                raise
            except (OSError, ValueError, TypeError):
                # Skip this sibling now and retry on the next full traversal.
                truncated = True
    _SCAN_CURSORS[key] = frames
    _SCAN_CURSORS.move_to_end(key)
    while len(_SCAN_CURSORS) > 2048:
        _SCAN_CURSORS.popitem(last=False)
    selected = list(candidates)
    nodes = []
    for task_id in selected:
        run_id, activation = candidates[task_id]
        try:
            receipt = read_receipt(storage.owners / (store.validate_task_id(task_id) + '.json'))
            if (receipt.get('workflow_run_id') != run_id
                    or receipt.get('workflow_node_id') != activation.get('node_id')
                    or receipt.get('workflow_execution_id') != activation.get('id')):
                continue
            record = store.read(log_dir, task_id, include_prompt=False, metadata_byte_limit=262144)
            found = loss(record, log_dir) if record else None
            if found:
                generation, evidence = found
                nodes.append((generation, _node(record, evidence, workflow_run_id=run_id,
                              node_id=transport._text(activation.get('node_id'), 100),
                              execution_id=transport._text(activation.get('id'), 100))))
        except StateOwnershipError:
            raise
        except (OSError, ValueError, TypeError):
            continue
    return nodes, truncated or bool(frames)


def _scan(db, log_dir, table, states):
    key = (str(log_dir), table)
    cursor = _SCAN_CURSORS.get(key, '')
    rows = db.execute('SELECT * FROM ' + table + ' WHERE state IN (' + states + ') AND id>? ORDER BY id LIMIT ?',
                      (cursor, transport.BATCH)).fetchall()
    if not rows and cursor:
        rows = db.execute('SELECT * FROM ' + table + ' WHERE state IN (' + states + ') ORDER BY id LIMIT ?',
                          (transport.BATCH,)).fetchall()
    _SCAN_CURSORS[key] = rows[-1]['id'] if rows else ''
    _SCAN_CURSORS.move_to_end(key)
    while len(_SCAN_CURSORS) > 2048:
        _SCAN_CURSORS.popitem(last=False)
    return rows


def detect(log_dir):
    """Bounded subscription reconciliation independent of completion subscription state."""
    from .workflow_notifications import _run
    with transport._db(log_dir) as db:
        tasks = _scan(db, log_dir, 'subscriptions', "'pending','retry','delivering'")
        workflows = _scan(db, log_dir, 'workflow_subscriptions', "'active'")
    for subscription in tasks:
        try:
            record = store.read(log_dir, subscription['task_id'], include_prompt=False, metadata_byte_limit=262144)
            found = loss(record, log_dir) if record else None
            if found:
                generation, evidence = found
                _insert(log_dir, subscription, 'task', generation, [_node(record, evidence)])
        except StateOwnershipError:
            raise
        except (OSError, ValueError, TypeError):
            continue
    for subscription in workflows:
        try:
            nodes, truncated = _workflow_nodes(log_dir, _run(log_dir, subscription['workflow_run_id']),
                                                subscription_id=subscription['id'])
            for generation, node in nodes:
                # Each affected task owns an independent durable episode, so newly visible
                # nodes from the same owner cannot be hidden by an earlier frozen payload.
                _insert(log_dir, subscription, 'workflow', generation, [node], truncated=truncated)
        except StateOwnershipError:
            raise
        except (OSError, ValueError, TypeError):
            continue


def tick(log_dir):
    detect(log_dir)
    now = time.time()
    with transport._db(log_dir) as db:
        db.execute("UPDATE supervision_deliveries SET state='expired',lease=NULL,error='delivery expired' WHERE state IN ('pending','retry','delivering') AND created<? AND lease_until<=?", (now - transport.RETENTION_SECONDS, now))
        db.execute("UPDATE supervision_deliveries SET state='failed',lease=NULL,error='delivery attempts exhausted' WHERE state IN ('pending','retry','delivering') AND attempts>=? AND lease_until<=?", (transport.MAX_ATTEMPTS, now))
        db.execute("DELETE FROM supervision_deliveries WHERE state IN ('delivered','failed','expired','superseded') AND updated<? AND NOT EXISTS(SELECT 1 FROM subscriptions s WHERE supervision_deliveries.subscription_kind='task' AND s.id=subscription_id AND s.state NOT IN ('unsubscribed','expired')) AND NOT EXISTS(SELECT 1 FROM workflow_subscriptions s WHERE supervision_deliveries.subscription_kind='workflow' AND s.id=subscription_id AND s.state NOT IN ('unsubscribed','expired'))", (now - transport.RETENTION_SECONDS,))
        candidates = db.execute("SELECT * FROM supervision_deliveries WHERE state IN ('pending','retry','delivering') AND due<=? AND lease_until<=? ORDER BY updated LIMIT ?", (now, now, transport.BATCH)).fetchall()
    for row in candidates:
        lease = str(uuid.uuid4())
        now = time.time()
        table = 'subscriptions' if row['subscription_kind'] == 'task' else 'workflow_subscriptions'
        with transport._db(log_dir) as db:
            claimed = db.execute("UPDATE supervision_deliveries SET state='delivering',lease=?,lease_until=?,attempts=attempts+1 WHERE id=? AND state IN ('pending','retry','delivering') AND due<=? AND lease_until<=? AND attempts<? AND EXISTS(SELECT 1 FROM " + table + " s WHERE s.id=subscription_id AND s.state NOT IN ('unsubscribed','expired','reserved')) RETURNING attempts", (lease, now + transport.LEASE_SECONDS, row['id'], now, now, transport.MAX_ATTEMPTS)).fetchone()
        if claimed is None:
            continue
        error = None
        try:
            transport._deliver(transport.preflight(row['destination']), row['id'], row['payload'])
        except StateOwnershipError:
            raise
        except Exception:
            error = 'delivery attempt failed'
        attempts = claimed['attempts']
        state = 'delivered' if error is None else 'failed' if attempts >= transport.MAX_ATTEMPTS else 'retry'
        with transport._db(log_dir) as db:
            db.execute('UPDATE supervision_deliveries SET state=?,updated=?,due=?,lease=NULL,lease_until=0,error=? WHERE id=? AND lease=?', (state, time.time(), time.time() + min(3600, 2 ** attempts), error, row['id'], lease))
