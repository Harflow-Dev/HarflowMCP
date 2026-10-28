"""Durable, at-least-once completion webhooks to administrator-configured destinations."""
from __future__ import annotations

from .state_access import task_operation
from .task_outcomes import COMMIT_ATTRIBUTION, endpoint_projection
from .notification_database import NotificationDatabaseError
from .state_ownership import StateOwnershipError

import asyncio
import hashlib
import hmac
import http.client
import ipaddress
import json
import os
import re
import socket
import sqlite3
import ssl
import subprocess
import sys
import time
import threading
import uuid
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit

from . import control, store

MAX_SUBSCRIPTIONS = 1000
MAX_PINNED_TASKS = 10000
MAX_ATTEMPTS = 6
BATCH = 16
LEASE_SECONDS = 60
RETENTION_SECONDS = 30 * 86400
REQUEST_SECONDS = 10
MAX_PAYLOAD = 16384
TERMINAL = {'completed', 'failed', 'cancelled', 'timed_out'}


def preflight(notify: str) -> dict:
    """Validate an alias against trusted server configuration, without network IO."""
    if not isinstance(notify, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', notify):
        raise ValueError('notify must be a configured destination alias')
    try:
        config = json.loads(os.environ.get('HARFLOW_NOTIFY_DESTINATIONS', '{}'))[notify]
        if not isinstance(config, dict):
            raise ValueError
        url = config['url']
        if not isinstance(url, str):
            raise ValueError
        parsed = urlsplit(url)
        private = config.get('allow_private', False)
        if type(private) is not bool or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.fragment:
            raise ValueError
        if parsed.scheme != 'https' and not (private and parsed.scheme == 'http'):
            raise ValueError
        if len(url) > 2048 or any(ord(c) < 33 or ord(c) > 126 for c in url):
            raise ValueError
        port = parsed.port
        if port == 0:
            raise ValueError
        secret = config.get('secret_env')
        if secret is not None and (not isinstance(secret, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,127}', secret)):
            raise ValueError
        return {'alias': notify, 'url': url, 'allow_private': private, 'secret_env': secret}
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        raise ValueError('notification destination unavailable or invalid') from None


def _db(log_dir: Path):
    from .notification_database import open_database
    return open_database(log_dir, create=True, mode='rw')


def _public(row):
    return {key: row[key] for key in ('id', 'task_id', 'destination', 'state', 'attempts', 'error')}


@task_operation
def subscribe(log_dir: Path, task_id: str, notify: str, *, reservation: bool = False) -> dict:
    preflight(notify)
    store.validate_task_id(task_id)
    with control.record_lock_sync(log_dir, task_id):
        if not reservation and store.read(log_dir, task_id, include_prompt=False) is None:
            raise ValueError('Task not found')
        now = time.time()
        with _db(log_dir) as db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute('SELECT * FROM subscriptions WHERE task_id=? AND destination=?', (task_id, notify)).fetchone()
            if existing:
                return _public(existing)
            if db.execute('SELECT (SELECT count(*) FROM subscriptions)+(SELECT count(*) FROM workflow_subscriptions)').fetchone()[0] >= MAX_SUBSCRIPTIONS:
                raise ValueError('notification subscription capacity reached')
            db.execute('INSERT INTO subscriptions(id,task_id,destination,state,created,updated) VALUES(?,?,?,?,?,?)',
                       (str(uuid.uuid4()), task_id, notify, 'pending', now, now))
            return _public(db.execute('SELECT * FROM subscriptions WHERE task_id=? AND destination=?', (task_id, notify)).fetchone())

def _existing_db(log_dir: Path, mode: str):
    """Qualify existing storage without initializing or recreating it."""
    from .notification_database import open_database
    return open_database(log_dir, create=False, mode=mode)


@task_operation
def status(log_dir: Path, task_id: str) -> list[dict]:
    store.validate_task_id(task_id)
    connection = _existing_db(log_dir, 'ro')
    if connection is None:
        return []
    with closing(connection) as db:
        db.row_factory = sqlite3.Row
        return [_public(row) for row in db.execute('SELECT * FROM subscriptions WHERE task_id=? ORDER BY created LIMIT ?', (task_id, MAX_SUBSCRIPTIONS))]


@task_operation
def discard_unstarted(log_dir: Path, task_id: str) -> bool:
    """Confirm removal of an untouched reservation while no task record exists."""
    with control.record_lock_sync(log_dir, task_id):
        if store.record_path(log_dir, task_id).exists():
            return False
        with _db(log_dir) as db:
            db.execute('BEGIN IMMEDIATE')
            removed = db.execute("DELETE FROM subscriptions WHERE task_id=? AND state='pending' AND attempts=0 AND payload IS NULL", (task_id,)).rowcount
            remaining = db.execute('SELECT count(*) FROM subscriptions WHERE task_id=?', (task_id,)).fetchone()[0]
            return removed > 0 and remaining == 0


@task_operation
def unsubscribe(log_dir: Path, task_id: str, destination: str | None = None) -> list[dict]:
    store.validate_task_id(task_id)
    connection = _existing_db(log_dir, 'rw')
    if connection is None:
        return []
    with closing(connection) as db, db:
        db.execute("UPDATE subscriptions SET state='unsubscribed',updated=?,lease=NULL WHERE task_id=? AND (? IS NULL OR destination=?) AND state IN ('pending','retry','delivering')", (time.time(), task_id, destination, destination))
        db.execute("UPDATE supervision_deliveries SET state='superseded',lease=NULL,lease_until=0,updated=? WHERE subscription_kind='task' AND subscription_id IN (SELECT id FROM subscriptions WHERE task_id=? AND (? IS NULL OR destination=?)) AND state IN ('pending','retry','delivering')", (time.time(), task_id, destination, destination))
    return status(log_dir, task_id)


@task_operation
def pinned_tasks(log_dir: Path) -> set[str]:
    connection = _existing_db(log_dir, 'ro')
    if connection is None:
        return set()
    with closing(connection) as db:
        query = "SELECT task_id FROM subscriptions WHERE state IN ('pending','retry','delivering')"
        if db.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='supervision_deliveries'").fetchone():
            # Every loss episode owns exactly one affected task; duplicate subscriptions
            # and deliveries must not consume the distinct-task pin budget.
            query += " UNION SELECT task_id FROM supervision_deliveries WHERE state IN ('pending','retry','delivering')"
        else:
            query = 'SELECT DISTINCT task_id FROM (' + query + ')'
        rows = db.execute(query + ' LIMIT ?', (MAX_PINNED_TASKS + 1,)).fetchall()
        if len(rows) > MAX_PINNED_TASKS:
            # Retention callers abort deletion when a complete pin set is unavailable.
            raise ValueError('notification task pin budget exceeded; retention must defer deletion')
        pinned = set()
        for row in rows:
            pinned.add(store.validate_task_id(row[0]))
        return pinned



def _text(value, limit=1024):
    return value.encode('utf-8')[:limit].decode('utf-8', 'ignore') if isinstance(value, str) else None


def _payload(row, record, *, recovered=False, evidence=None):
    # Only durable data; never resolve inferred deaths or replay prompt/raw output logs.
    outcome = record.outcome if isinstance(record.outcome, dict) else {}
    projection = {'pushed': outcome.get('pushed') if type(outcome.get('pushed')) is bool else None}
    projection.update(endpoint_projection(outcome))
    for key in ('base_commit', 'head_commit', 'committed_diff_stat', 'publication_evidence'):
        if key in outcome:
            projection[key] = _text(outcome[key], 512)
    if isinstance(outcome.get('local_upstream'), dict):
        projection['local_upstream'] = {key: _text(outcome['local_upstream'].get(key), 256)
                                        for key in ('ref', 'ahead_behind', 'provenance')}
    if isinstance(outcome.get('new_commits'), list):
        projection['attribution'] = COMMIT_ATTRIBUTION
        projection['new_commits_truncated'] = bool(outcome.get('new_commits_truncated') or len(outcome['new_commits']) > 10)
        projection['new_commits'] = [_text(item, 128) for item in outcome['new_commits'][:10] if isinstance(item, str)]
    diagnostic = record.failure_diagnostic if isinstance(record.failure_diagnostic, dict) else {}
    failure = {key: _text(diagnostic[key], 256) for key in ('category', 'reason', 'source', 'reset_at') if key in diagnostic}
    if diagnostic.get('category') == 'background_abandoned_at_idle_bound':
        for key in ('background_idle_seconds', 'elapsed_background_wait_seconds', 'successful_agent_result'):
            value = diagnostic.get(key)
            if type(value) in (int, float, bool):
                failure[key] = value
        latest = diagnostic.get('latest_result')
        if isinstance(latest, dict):
            failure['latest_result'] = {'is_error': latest.get('is_error') if type(latest.get('is_error')) is bool else None,
                                        'summary': _text(latest.get('summary'), 512),
                                        'subtype': _text(latest.get('subtype'), 128)}
        if isinstance(diagnostic.get('background_tasks'), list):
            failure['background_tasks'] = [
                {'id': _text(item.get('id'), 128), 'description': _text(item.get('description'), 128),
                 'elapsed_seconds': item.get('elapsed_seconds') if type(item.get('elapsed_seconds')) in (int, float) else None}
                for item in diagnostic['background_tasks'][:16] if isinstance(item, dict)]
    data = {'version': 1, 'notification_id': row['id'], 'event': 'task.settled',
            'task_id': record.task_id, 'title': _text(record.title), 'group': _text(record.group),
            'status': record.status, 'exit_code': record.exit_code, 'finished_at': record.finished_at,
            'recovered': recovered, 'outcome_observed': record.exit_code is not None,
            'evidence': evidence or {'source': 'observed_process_exit'},
            'output_drained': record.output_drained, 'summary': _text(getattr(record, 'terminal_summary', None), 4096),
            'outcome': projection, 'failure_diagnostic': failure, 'diagnostics': {'task_record': record.task_id + store.RECORD_SUFFIX}}
    body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
    if len(body) > MAX_PAYLOAD:
        data['summary'] = _text(data['summary'], 512)
        data['outcome'] = {'pushed': projection['pushed']}
        data['title'] = _text(data['title'], 128)
        data['group'] = _text(data['group'], 128)
        body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
    if len(body) > MAX_PAYLOAD:
        raise ValueError('payload limit')
    return body


def _resolve(host, port):
    # Resolver libraries offer no deadline; isolate DNS so a wedged resolver can be killed.
    script = "import socket,json,sys; print(json.dumps(socket.getaddrinfo(sys.argv[1],int(sys.argv[2]),type=socket.SOCK_STREAM)))"
    result = subprocess.run([sys.executable, '-I', '-c', script, host, str(port)],
                            capture_output=True, timeout=2, check=True)
    if len(result.stdout) > 16384:
        raise ValueError('resolution exceeded limit')
    return json.loads(result.stdout)


class _BoundedResponse(http.client.HTTPResponse):
    def begin(self):
        original = self.fp
        class Reader:
            budget = 16384
            def readline(self, limit=-1):
                data = original.readline(min(limit if limit >= 0 else self.budget + 1, self.budget + 1))
                self.budget -= len(data)
                if self.budget < 0:
                    raise ValueError('response headers exceeded limit')
                return data
            def flush(self):
                original.flush()
            def close(self):
                original.close()
        self.fp = Reader()
        return super().begin()


def _deliver(config, identifier: str, body: bytes):
    """Resolve once per attempt, validate every address, then try numeric addresses in order."""
    parsed = urlsplit(config['url'])
    port = parsed.port if parsed.port is not None else (443 if parsed.scheme == 'https' else 80)
    if port == 0:
        raise ValueError('destination port rejected')
    deadline = time.monotonic() + REQUEST_SECONDS
    addresses = _resolve(parsed.hostname, port)
    if not addresses or len(addresses) > 32:
        raise ValueError('destination resolution rejected')
    for family, kind, proto, _, address in addresses:
        ip = ipaddress.ip_address(address[0])
        if not config['allow_private'] and not ip.is_global:
            raise ValueError('destination address rejected')
    def remaining():
        budget = deadline - time.monotonic()
        if budget <= 0:
            raise TimeoutError('delivery deadline exceeded')
        return budget

    def watchdog(target):
        # Bind the socket: a cancelled timer may already be executing during fallback.
        def abort():
            try:
                target.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        timer = threading.Timer(remaining(), abort)
        timer.daemon = True
        timer.start()
        return timer

    sock = None
    timer = None
    try:
        for index, (family, kind, proto, _, address) in enumerate(addresses):
            budget = remaining()
            sock = None
            timer = None
            try:
                sock = socket.socket(family, kind, proto)
                timer = watchdog(sock)
                # Reserve time for later validated addresses if this route blackholes.
                sock.settimeout(budget / (len(addresses) - index))
                sock.connect(tuple(address))
            except OSError:
                if timer is not None:
                    timer.cancel()
                if sock is not None:
                    sock.close()
                sock = None
                if index == len(addresses) - 1:
                    raise
                continue
            break
        if parsed.scheme == 'https':
            timer.cancel()
            sock = ssl.create_default_context().wrap_socket(
                sock, server_hostname=parsed.hostname, do_handshake_on_connect=False)
            timer = watchdog(sock)
            sock.settimeout(remaining())
            sock.do_handshake()
        sock.settimeout(remaining())
        timestamp = str(int(time.time()))
        headers = {'Content-Type': 'application/json', 'Content-Length': str(len(body)),
                   'X-HarflowMCP-Notification-ID': identifier, 'X-HarflowMCP-Timestamp': timestamp}
        if config['secret_env']:
            secret = os.environ.get(config['secret_env'])
            if not secret:
                raise ValueError('signing secret unavailable')
            signed = identifier.encode() + b'.' + timestamp.encode() + b'.' + body
            headers['X-HarflowMCP-Signature'] = 'sha256=' + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
        connection = http.client.HTTPConnection(parsed.hostname, port, timeout=remaining())
        connection.response_class = _BoundedResponse
        connection.sock = sock
        try:
            connection.request('POST', (parsed.path or '/') + ('?' + parsed.query if parsed.query else ''), body, headers)
            response = connection.getresponse()
            # Bodies are never needed; do not read attacker-controlled response bytes.
            if not 200 <= response.status < 300:
                raise ValueError('endpoint rejected delivery')
        finally:
            connection.close()
        if time.monotonic() > deadline:
            raise TimeoutError
    finally:
        if timer is not None:
            timer.cancel()
        if sock is not None:
            sock.close()


def _settlement_payload(log_dir, candidate, record):
    if not store.outcome_unobserved(record):
        if record.status in TERMINAL and record.finished_at:
            return _payload(candidate, record)
        return None
    from . import supervision
    evidence = supervision.evidence(record, store.log_path(log_dir, record.task_id))
    if evidence['owner_state'] != 'dead' or evidence['process_state'] != 'dead':
        return None
    status, _note, state, _tail = store.resolve_status(log_dir, record)
    if status not in TERMINAL:
        return None
    # An inferred settlement is a notification projection; never write a task outcome.
    projected = replace(record, status=status, exit_code=None,
                        terminal_summary=state.summary or record.terminal_summary,
                        failure_diagnostic=state.failure_diagnostic or record.failure_diagnostic)
    return _payload(candidate, projected, recovered=True,
                    evidence={'source': 'recovered_status_resolution', 'supervision': evidence,
                              'execution_outcome': 'unobserved'})


@task_operation
def _tick(log_dir: Path):
    now = time.time()
    with _db(log_dir) as db:
        db.execute("UPDATE subscriptions SET state='failed',updated=?,lease=NULL,error='delivery attempts exhausted' WHERE attempts>=? AND state IN ('pending','retry','delivering') AND lease_until<=?", (now, MAX_ATTEMPTS, now))
        db.execute("UPDATE subscriptions SET state='expired',updated=?,lease=NULL,error='subscription expired' WHERE state IN ('pending','retry','delivering') AND created<? AND lease_until<=?", (now, now - RETENTION_SECONDS, now))
        db.execute("DELETE FROM subscriptions WHERE state IN ('delivered','failed','expired','unsubscribed') AND updated<?", (now - RETENTION_SECONDS,))
        candidates = db.execute("SELECT * FROM subscriptions WHERE state IN ('pending','retry','delivering') AND due<=? AND lease_until<=? ORDER BY updated LIMIT ?", (now, now, BATCH)).fetchall()
    for candidate in candidates:
        with _db(log_dir) as db:
            db.execute('UPDATE subscriptions SET updated=? WHERE id=?', (time.time(), candidate['id']))
        try:
            if store.read(log_dir, candidate["task_id"], include_prompt=False, metadata_byte_limit=262144) is None:
                continue
        except (store.TaskDocumentError, OSError, ValueError):
            continue
        body = candidate['payload']
        if body is None:
            try:
                record = store.read(log_dir, candidate['task_id'], include_prompt=False, metadata_byte_limit=262144)
            except (OSError, ValueError, TypeError, store.TaskDocumentError):
                continue
            if record is None:
                continue
            try:
                body = _settlement_payload(log_dir, candidate, record)
            except (ValueError, TypeError):
                continue
            if body is None:
                continue
        lease = str(uuid.uuid4())
        now = time.time()
        with _db(log_dir) as db:
            claimed = db.execute("UPDATE subscriptions SET state='delivering',lease=?,lease_until=?,payload=?,attempts=attempts+1 WHERE id=? AND state IN ('pending','retry','delivering') AND lease_until<=? AND due<=? AND attempts<? RETURNING attempts", (lease, now + LEASE_SECONDS, body, candidate['id'], now, now, MAX_ATTEMPTS)).fetchone()
        if claimed is None:
            continue
        error = None
        try:
            _deliver(preflight(candidate['destination']), candidate['id'], body)
        except StateOwnershipError:
            raise
        except Exception:
            error = 'delivery attempt failed'  # Never persist URL, secret, response, or exception text.
        attempts = claimed['attempts']
        state = 'delivered' if error is None else ('failed' if attempts >= MAX_ATTEMPTS else 'retry')
        with _db(log_dir) as db:
            db.execute('UPDATE subscriptions SET state=?,updated=?,due=?,lease=NULL,lease_until=0,error=? WHERE id=? AND lease=?', (state, time.time(), time.time() + min(3600, 2 ** attempts), error, candidate['id'], lease))


@task_operation
async def tick(log_dir: Path):
    from .supervision_notifications import tick as supervision_tick
    await asyncio.to_thread(supervision_tick, Path(log_dir))
    await asyncio.to_thread(_tick, Path(log_dir))
    from .workflow_notifications import tick as workflow_tick
    await asyncio.to_thread(workflow_tick, Path(log_dir))


async def worker(log_dir: Path):
    """Run only inside a live server; cancellation leaves durable leases recoverable."""
    while True:
        try:
            await tick(log_dir)
        except StateOwnershipError:
            raise
        except Exception:
            pass  # Notification bookkeeping must never change execution status.
        await asyncio.sleep(2)
