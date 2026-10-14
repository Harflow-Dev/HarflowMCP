import json
import sqlite3
import time
from pathlib import Path

import pytest

from polybridge import notifications as n, store


@pytest.fixture
def setup(tmp_path, monkeypatch):
    logs = tmp_path / 'tasks'
    logs.mkdir()
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'test': {'url': 'https://example.com/hook', 'secret_env': 'TEST_SECRET'}}))
    monkeypatch.setenv('TEST_SECRET', 'never-persist-this')
    return logs


def record(logs, status='completed', exit_code=0, finished_at='2026-10-14T00:00:00+00:00'):
    r = store.TaskRecord(task_id='a' * 32, backend='codex', session_id=None,
                         repo_path='/private/project', started_at='2026-10-14T00:00:00+00:00',
                         status=status, exit_code=exit_code, finished_at=finished_at,
                         prompt='PRIVATE PROMPT', output_drained=False)
    store.write(logs, r)
    return r


def test_discard_confirms_only_removed_reservation(setup):
    assert n.discard_unstarted(setup, 'absent') is False
    n.subscribe(setup, 'reserved', 'test', reservation=True)
    assert n.discard_unstarted(setup, 'reserved') is True
    assert n.status(setup, 'reserved') == []
    assert n.discard_unstarted(setup, 'reserved') is False


@pytest.mark.parametrize('change', ["state='retry'", 'attempts=1', "payload=X'01'"])
def test_discard_retains_touched_reservation(setup, change):
    n.subscribe(setup, 'reserved', 'test', reservation=True)
    with n._db(setup) as db:
        db.execute(f'UPDATE subscriptions SET {change}')
    assert n.discard_unstarted(setup, 'reserved') is False
    assert len(n.status(setup, 'reserved')) == 1


def test_discard_retains_reservation_with_durable_record(setup):
    r = record(setup)
    n.subscribe(setup, r.task_id, 'test', reservation=True)
    assert n.discard_unstarted(setup, r.task_id) is False
    assert len(n.status(setup, r.task_id)) == 1


@pytest.mark.parametrize('state', ['completed', 'failed', 'cancelled', 'timed_out'])
def test_delivery(setup, monkeypatch, state):
    r = record(setup, state)
    sub = n.subscribe(setup, r.task_id, 'test')
    sent = []
    monkeypatch.setattr(n, '_deliver', lambda c, i, b: sent.append(json.loads(b)))
    n._tick(setup)
    assert n.status(setup, r.task_id)[0]['state'] == 'delivered'
    assert sent[0]['notification_id'] == sub['id']
    assert sent[0]['status'] == state
    assert sent[0]['output_drained'] is False
    assert 'PRIVATE PROMPT' not in json.dumps(sent)
    assert 'never-persist-this' not in (setup.parent / 'notifications.sqlite3').read_bytes().decode(errors='ignore')
    assert n.subscribe(setup, r.task_id, 'test')['id'] == sub['id']
    n._tick(setup)
    assert len(sent) == 1


@pytest.mark.parametrize('state,code,finished', [('running', None, None), ('failed', None, 'now'), ('completed', 0, None)])
def test_requires_durable_settlement(setup, monkeypatch, state, code, finished):
    r = record(setup, state, code, finished)
    n.subscribe(setup, r.task_id, 'test')
    monkeypatch.setattr(n, '_deliver', lambda *a: pytest.fail('premature notification'))
    n._tick(setup)
    assert n.status(setup, r.task_id)[0]['attempts'] == 0


def test_retry_restart_lease_and_unsubscribe(setup, monkeypatch):
    r = record(setup)
    n.subscribe(setup, r.task_id, 'test')
    def fail(*a):
        raise RuntimeError('secret URL')
    monkeypatch.setattr(n, '_deliver', fail)
    n._tick(setup)
    assert n.status(setup, r.task_id)[0]['state'] == 'retry'
    assert 'secret' not in str(n.status(setup, r.task_id))
    with n._db(setup) as db:
        db.execute("UPDATE subscriptions SET state='delivering',lease='crashed',lease_until=?,due=0", (time.time() + 30,))
    monkeypatch.setattr(n, '_deliver', lambda *a: None)
    n._tick(setup)
    assert n.status(setup, r.task_id)[0]['state'] == 'delivering'
    with n._db(setup) as db:
        db.execute('UPDATE subscriptions SET lease_until=0')
    n._tick(setup)
    assert n.status(setup, r.task_id)[0]['state'] == 'delivered'


def test_unsubscribe_and_pin(setup):
    r = record(setup)
    n.subscribe(setup, r.task_id, 'test')
    assert r.task_id in n.pinned_tasks(setup)
    assert n.unsubscribe(setup, r.task_id)[0]['state'] == 'unsubscribed'
    assert not n.pinned_tasks(setup)


@pytest.mark.parametrize('url', ['http://example.com', 'https://u:p@example.com', 'https://example.com/#x', 'file:///tmp/a'])
def test_reject_urls(setup, monkeypatch, url):
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'test': {'url': url}}))
    with pytest.raises(ValueError):
        n.preflight('test')
    with pytest.raises(ValueError):
        n.preflight('https://example.com')


def test_private_resolution_rejected(setup, monkeypatch):
    monkeypatch.setattr(n, '_resolve', lambda *a: [(2, 1, 6, '', ('127.0.0.1', 443))])
    with pytest.raises(ValueError, match='address rejected'):
        n._deliver(n.preflight('test'), 'id', b'{}')


def test_retry_exhaustion(setup, monkeypatch):
    r = record(setup)
    n.subscribe(setup, r.task_id, 'test')
    monkeypatch.setattr(n, '_deliver', lambda *a: (_ for _ in ()).throw(ValueError()))
    for _ in range(n.MAX_ATTEMPTS + 1):
        with n._db(setup) as db:
            db.execute('UPDATE subscriptions SET due=0')
        n._tick(setup)
    row = n.status(setup, r.task_id)[0]
    assert row['state'] == 'failed'
    assert row['attempts'] == n.MAX_ATTEMPTS

@pytest.mark.parametrize('response,passes', [(b'HTTP/1.1 204 No Content\r\n\r\n', True),
    (b'HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1/secret\r\nContent-Length: 0\r\n\r\n', False),
    (b'HTTP/1.1 200 OK\r\nX-Large: ' + b'a' * 17000 + b'\r\n\r\n', False)], ids=['success', 'redirect', 'oversize'])
def test_local_transport_signature_and_response_bounds(setup, monkeypatch, response, passes):
    import socket
    import threading
    import hashlib
    import hmac
    listener = socket.socket()
    try:
        listener.bind(('127.0.0.1', 0))
    except PermissionError:
        pytest.skip('sandbox denies loopback listener')
    listener.listen(1)
    listener.settimeout(2)
    captured = []
    def serve():
        conn, _ = listener.accept()
        with conn:
            data = b''
            while b'\r\n\r\n' not in data:
                data += conn.recv(4096)
            captured.append(data)
            conn.sendall(response)
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    port = listener.getsockname()[1]
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'test': {'url': f'http://127.0.0.1:{port}/hook', 'allow_private': True, 'secret_env': 'TEST_SECRET'}}))
    monkeypatch.setattr(n, '_resolve', lambda *a: [(2, 1, 6, '', ('127.0.0.1', port))])
    try:
        if passes:
            n._deliver(n.preflight('test'), 'stable-id', b'{}')
        else:
            with pytest.raises(ValueError):
                n._deliver(n.preflight('test'), 'stable-id', b'{}')
        thread.join(2)
        head = captured[0].split(b'\r\n\r\n')[0].decode()
        headers = dict(line.split(': ', 1) for line in head.split('\r\n')[1:])
        signed = b'stable-id.' + headers['X-Polybridge-Timestamp'].encode() + b'.{}'
        assert headers['X-Polybridge-Signature'] == 'sha256=' + hmac.new(b'never-persist-this', signed, hashlib.sha256).hexdigest()
        assert 'never-persist-this' not in head
    finally:
        listener.close()

@pytest.mark.parametrize('summary', ['猫' * 10000, '\x00' * 10000], ids=['utf8','controls'])
def test_payload_adversarial_summary(setup, summary):
    r = record(setup)
    from types import SimpleNamespace
    from dataclasses import asdict
    r = SimpleNamespace(**asdict(r))
    r.terminal_summary = summary
    r.outcome = {'pushed': None, 'publication_evidence': None, 'new_commits': ['a' * 1000] * 100}
    body = n._payload({'id': 'stable'}, r)
    assert len(body) <= n.MAX_PAYLOAD
    assert json.loads(body)['outcome']['pushed'] is None


def test_each_claim_gets_full_lease_after_slow_deliveries(setup, monkeypatch):
    from dataclasses import replace
    base = record(setup)
    for index in range(8):
        r = replace(base, task_id=f'task-{index}')
        store.write(setup, r)
        n.subscribe(setup, r.task_id, 'test')
    clock = [time.time()]
    monkeypatch.setattr(n.time, 'time', lambda: clock[0])
    leases = []
    def deliver(config, identifier, body):
        with n._db(setup) as db:
            row = db.execute('SELECT lease_until FROM subscriptions WHERE id=?', (identifier,)).fetchone()
        leases.append(row['lease_until'] - clock[0])
        clock[0] += 11
    monkeypatch.setattr(n, '_deliver', deliver)
    n._tick(setup)
    assert leases == [n.LEASE_SECONDS] * 8


def test_subscribe_requires_record_unless_reserving(setup):
    with pytest.raises(ValueError, match='Task not found'):
        n.subscribe(setup, 'missing', 'test')
    assert n.subscribe(setup, 'reserved', 'test', reservation=True)['state'] == 'pending'


def test_retention_rechecks_new_subscription_after_pin_snapshot(setup, monkeypatch):
    from dataclasses import replace
    from datetime import datetime, timedelta, timezone
    from polybridge import retention
    old = (datetime.now(timezone.utc) - timedelta(days=40)).isoformat()
    r = replace(record(setup), started_at=old, finished_at=old, output_drained=True)
    store.write(setup, r)
    original = store.read_all
    def snapshot_then_subscribe(log_dir):
        records = original(log_dir)
        n.subscribe(log_dir, r.task_id, 'test')
        return records
    monkeypatch.setattr(store, 'read_all', snapshot_then_subscribe)
    retention.sweep(setup, 30, datetime.now(timezone.utc))
    assert store.read(setup, r.task_id) is not None
    assert n.status(setup, r.task_id)[0]['state'] == 'pending'
