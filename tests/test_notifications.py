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


def test_expiry_preserves_active_delivery_lease(setup, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    clock = [time.time()]
    monkeypatch.setattr(n.time, 'time', lambda: clock[0])
    r = record(setup)
    subscription = n.subscribe(setup, r.task_id, 'test')
    boundary = clock[0] + n.RETENTION_SECONDS
    clock[0] = boundary - 1
    started = threading.Event()
    release = threading.Event()

    def deliver(*args):
        started.set()
        assert release.wait(5)

    monkeypatch.setattr(n, '_deliver', deliver)
    with ThreadPoolExecutor(max_workers=1) as executor:
        delivery = executor.submit(n._tick, setup)
        try:
            assert started.wait(5)
            with n._db(setup) as db:
                claimed = dict(db.execute('SELECT * FROM subscriptions WHERE id=?', (subscription['id'],)).fetchone())
            clock[0] = boundary + 1
            n._tick(setup)
            with n._db(setup) as db:
                current = dict(db.execute('SELECT * FROM subscriptions WHERE id=?', (subscription['id'],)).fetchone())
            assert current['state'] == 'delivering'
            assert current['lease'] == claimed['lease']
            assert current['lease_until'] == claimed['lease_until']
            assert current['attempts'] == 1
            assert r.task_id in n.pinned_tasks(setup)
        finally:
            release.set()
        delivery.result(timeout=5)
    assert n.status(setup, r.task_id)[0]['state'] == 'delivered'
    assert not n.pinned_tasks(setup)


@pytest.mark.parametrize('state', ['pending', 'retry', 'delivering'])
def test_expiry_after_lease_elapsed(setup, monkeypatch, state):
    clock = [time.time()]
    monkeypatch.setattr(n.time, 'time', lambda: clock[0])
    r = record(setup)
    n.subscribe(setup, r.task_id, 'test')
    with n._db(setup) as db:
        db.execute('UPDATE subscriptions SET state=?,lease=?,lease_until=?',
                   (state, 'abandoned', clock[0] + n.LEASE_SECONDS))
    clock[0] += n.RETENTION_SECONDS + 1
    monkeypatch.setattr(n, '_deliver', lambda *args: pytest.fail('expired subscription delivered'))
    n._tick(setup)
    assert n.status(setup, r.task_id)[0]['state'] == 'expired'
    with n._db(setup) as db:
        assert db.execute('SELECT lease FROM subscriptions').fetchone()['lease'] is None
    assert not n.pinned_tasks(setup)


@pytest.mark.parametrize('url', ['http://example.com', 'https://u:p@example.com', 'https://example.com/#x', 'file:///tmp/a'])
def test_reject_urls(setup, monkeypatch, url):
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'test': {'url': url}}))
    with pytest.raises(ValueError):
        n.preflight('test')
    with pytest.raises(ValueError):
        n.preflight('https://example.com')


@pytest.mark.parametrize('url', [123, 1.5, True, None, [], {}], ids=['int', 'float', 'bool', 'null', 'list', 'dict'])
def test_reject_non_string_configured_urls(setup, monkeypatch, url):
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'test': {'url': url}}))
    with pytest.raises(ValueError, match='notification destination unavailable or invalid') as error:
        n.preflight('test')
    assert 'Traceback' not in str(error.value)
    assert str(url) not in str(error.value)


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
@pytest.mark.parametrize('fallback', [False, True])
def test_local_transport_signature_and_response_bounds(setup, monkeypatch, response, passes, fallback):
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
    listener.settimeout(n.REQUEST_SECONDS + 2)
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
    refused = socket.socket()
    refused.bind(('127.0.0.1', 0))  # Reserve a port without listening: connect is refused.
    addresses = [(2, 1, 6, '', ('127.0.0.1', port))]
    if fallback:
        addresses.insert(0, (2, 1, 6, '', refused.getsockname()))
    monkeypatch.setattr(n, '_resolve', lambda *a: addresses)
    try:
        if passes:
            n._deliver(n.preflight('test'), 'stable-id', b'{}')
        else:
            with pytest.raises(ValueError):
                n._deliver(n.preflight('test'), 'stable-id', b'{}')
        thread.join(2)
        assert len(captured) == 1
        head = captured[0].split(b'\r\n\r\n')[0].decode()
        headers = dict(line.split(': ', 1) for line in head.split('\r\n')[1:])
        signed = b'stable-id.' + headers['X-Polybridge-Timestamp'].encode() + b'.{}'
        assert headers['X-Polybridge-Signature'] == 'sha256=' + hmac.new(b'never-persist-this', signed, hashlib.sha256).hexdigest()
        assert 'never-persist-this' not in head
    finally:
        refused.close()
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



def test_status_absent_does_not_create_directories(tmp_path):
    logs = tmp_path / 'missing' / 'tasks'
    assert n.status(logs, 'absent') == []
    assert not logs.parent.exists()


def test_status_existing_database_does_not_mutate(setup):
    n.subscribe(setup, 'reserved', 'test', reservation=True)
    path = setup.parent / 'notifications.sqlite3'
    path.chmod(0o400)
    before = (path.read_bytes(), path.stat().st_mode, path.stat().st_mtime_ns)
    files = set(path.parent.iterdir())
    assert n.status(setup, 'reserved')[0]['state'] == 'pending'
    assert (path.read_bytes(), path.stat().st_mode, path.stat().st_mtime_ns) == before
    assert set(path.parent.iterdir()) == files


def test_status_corrupt_database_fails_without_mutation(setup):
    path = setup.parent / 'notifications.sqlite3'
    path.write_bytes(b'corrupt database')
    path.chmod(0o400)
    before = (path.read_bytes(), path.stat().st_mode, path.stat().st_mtime_ns)
    with pytest.raises(sqlite3.DatabaseError):
        n.status(setup, 'reserved')
    assert (path.read_bytes(), path.stat().st_mode, path.stat().st_mtime_ns) == before


def test_status_database_removed_before_connect_is_not_recreated(setup, monkeypatch):
    n.subscribe(setup, 'reserved', 'test', reservation=True)
    path = setup.parent / 'notifications.sqlite3'
    connect = sqlite3.connect
    def remove_then_connect(*args, **kwargs):
        path.unlink()
        return connect(*args, **kwargs)
    monkeypatch.setattr(sqlite3, 'connect', remove_then_connect)
    assert n.status(setup, 'reserved') == []
    assert not path.exists()


def fake_transport(monkeypatch, *, failures=(), request_error=False, response_error=False):
    from types import SimpleNamespace
    sockets, timers, requests = [], [], []
    clock = [0.0]
    class Socket:
        def __init__(self, *args):
            self.closed = False
            self.shutdowns = 0
            self.timeouts = []
            sockets.append(self)
        def settimeout(self, value):
            self.timeouts.append(value)
        def connect(self, address):
            self.address = address
            if len(sockets) in failures:
                clock[0] += self.timeouts[-1]
                raise TimeoutError('unreachable')
        def shutdown(self, how):
            self.shutdowns += 1
        def close(self):
            self.closed = True
    class Timer:
        def __init__(self, interval, callback):
            self.interval, self.callback = interval, callback
            self.cancelled = False
            timers.append(self)
        def start(self):
            pass
        def cancel(self):
            self.cancelled = True
    class Connection:
        def __init__(self, host, port, timeout):
            assert host == 'receiver.example'
        def request(self, *args):
            requests.append(args)
            if request_error:
                raise OSError('write failed')
        def getresponse(self):
            if response_error:
                raise OSError('response lost')
            return SimpleNamespace(status=204)
        def close(self):
            self.sock.close()
    monkeypatch.setattr(n.socket, 'socket', Socket)
    monkeypatch.setattr(n.threading, 'Timer', Timer)
    monkeypatch.setattr(n.http.client, 'HTTPConnection', Connection)
    monkeypatch.setattr(n.time, 'monotonic', lambda: clock[0])
    return sockets, timers, requests, clock


def transport_config():
    return {'url': 'http://receiver.example/hook', 'allow_private': False, 'secret_env': None}


def transport_addresses():
    return [(n.socket.AF_INET6, 1, 6, '', ('2606:4700:4700::1111', 80, 0, 0)),
            (n.socket.AF_INET, 1, 6, '', ('1.1.1.1', 80))]


def test_transport_fallback_order_deadline_and_timer_binding(monkeypatch):
    sockets, timers, requests, clock = fake_transport(monkeypatch, failures=(1,))
    monkeypatch.setattr(n, '_resolve', lambda *args: transport_addresses())
    n._deliver(transport_config(), 'id', b'{}')
    assert [sock.address for sock in sockets] == [row[4] for row in transport_addresses()]
    assert sockets[0].timeouts == [n.REQUEST_SECONDS / 2]
    assert sockets[1].timeouts == [n.REQUEST_SECONDS / 2, n.REQUEST_SECONDS / 2]
    assert [timer.interval for timer in timers] == [n.REQUEST_SECONDS, n.REQUEST_SECONDS / 2]
    assert len(requests) == 1
    assert all(sock.closed for sock in sockets)
    assert all(timer.cancelled for timer in timers)
    timers[0].callback()  # A timer racing cancellation must only touch its own socket.
    assert [sock.shutdowns for sock in sockets] == [1, 0]
    assert clock[0] == n.REQUEST_SECONDS / 2


def test_transport_mixed_addresses_rejected_before_connect(monkeypatch):
    sockets, _, requests, _ = fake_transport(monkeypatch)
    monkeypatch.setattr(n, '_resolve', lambda *args: transport_addresses() + [(2, 1, 6, '', ('127.0.0.1', 80))])
    with pytest.raises(ValueError, match='address rejected'):
        n._deliver(transport_config(), 'id', b'{}')
    assert sockets == requests == []


@pytest.mark.parametrize('phase', ['request', 'response'])
def test_transport_does_not_fallback_after_request_started(monkeypatch, phase):
    sockets, timers, requests, _ = fake_transport(monkeypatch, request_error=phase == 'request', response_error=phase == 'response')
    monkeypatch.setattr(n, '_resolve', lambda *args: transport_addresses())
    with pytest.raises(OSError):
        n._deliver(transport_config(), 'id', b'{}')
    assert len(sockets) == len(requests) == 1
    assert sockets[0].closed and timers[0].cancelled


def test_transport_total_deadline_includes_resolution_and_connects(monkeypatch):
    sockets, timers, requests, clock = fake_transport(monkeypatch, failures=(1, 2))
    def resolve(*args):
        clock[0] += 2
        return transport_addresses()
    monkeypatch.setattr(n, '_resolve', resolve)
    with pytest.raises(TimeoutError):
        n._deliver(transport_config(), 'id', b'{}')
    assert [sock.timeouts for sock in sockets] == [[4], [4]]
    assert clock[0] == n.REQUEST_SECONDS
    assert requests == []
    assert all(sock.closed for sock in sockets)
    assert all(timer.cancelled for timer in timers)


def test_transport_expired_resolution_never_connects(monkeypatch):
    sockets, _, requests, clock = fake_transport(monkeypatch)
    def resolve(*args):
        clock[0] += n.REQUEST_SECONDS
        return transport_addresses()
    monkeypatch.setattr(n, '_resolve', resolve)
    with pytest.raises(TimeoutError, match='deadline'):
        n._deliver(transport_config(), 'id', b'{}')
    assert sockets == requests == []


def test_tls_handshake_has_bound_socket_watchdog_and_no_fallback(monkeypatch):
    from types import SimpleNamespace
    sockets, timers, requests, clock = fake_transport(monkeypatch)
    monkeypatch.setattr(n, '_resolve', lambda *args: transport_addresses())
    wrapped = SimpleNamespace(shutdowns=0, closed=False)
    def shutdown(how):
        wrapped.shutdowns += 1
    def close():
        wrapped.closed = True
    def settimeout(value):
        assert value == n.REQUEST_SECONDS - 2
    def handshake():
        timers[-1].callback()
        assert wrapped.shutdowns == 1
        raise n.ssl.SSLCertVerificationError('invalid certificate')
    wrapped.shutdown, wrapped.close = shutdown, close
    wrapped.settimeout, wrapped.do_handshake = settimeout, handshake
    def wrap(sock, *, server_hostname, do_handshake_on_connect):
        assert sock is sockets[0]
        assert server_hostname == 'receiver.example'
        assert do_handshake_on_connect is False
        assert timers[0].cancelled
        clock[0] += 2
        return wrapped
    monkeypatch.setattr(n.ssl, 'create_default_context', lambda: SimpleNamespace(wrap_socket=wrap))
    config = transport_config() | {'url': 'https://receiver.example/hook'}
    with pytest.raises(n.ssl.SSLCertVerificationError):
        n._deliver(config, 'id', b'{}')
    assert len(sockets) == 1 and requests == []
    assert wrapped.closed and all(timer.cancelled for timer in timers)
    assert timers[-1].interval == n.REQUEST_SECONDS - 2
    timers[0].callback()
    assert sockets[0].shutdowns == 1 and wrapped.shutdowns == 1


def test_transport_falls_back_when_address_family_unavailable(monkeypatch):
    import errno
    sockets, _, requests, _ = fake_transport(monkeypatch)
    original = n.socket.socket
    families = []
    def socket_for_family(family, kind, proto):
        families.append(family)
        if family == n.socket.AF_INET6:
            raise OSError(errno.EAFNOSUPPORT, 'IPv6 unavailable')
        return original(family, kind, proto)
    monkeypatch.setattr(n.socket, 'socket', socket_for_family)
    monkeypatch.setattr(n, '_resolve', lambda *args: transport_addresses())
    n._deliver(transport_config(), 'id', b'{}')
    assert families == [n.socket.AF_INET6, n.socket.AF_INET]
    assert len(sockets) == len(requests) == 1
    assert sockets[0].closed


@pytest.mark.parametrize('destination', [None, 'hook'])
def test_unsubscribe_absent_does_not_create_storage(tmp_path, monkeypatch, destination):
    monkeypatch.delenv('PB_NOTIFY_DESTINATIONS', raising=False)
    logs = tmp_path / 'missing' / 'tasks'
    assert n.unsubscribe(logs, 'absent', destination) == []
    assert not logs.parent.exists()


def test_unsubscribe_database_removed_before_connect_is_not_recreated(setup, monkeypatch):
    n.subscribe(setup, 'reserved', 'test', reservation=True)
    path = setup.parent / 'notifications.sqlite3'
    connect = sqlite3.connect
    def remove_then_connect(*args, **kwargs):
        path.unlink()
        return connect(*args, **kwargs)
    monkeypatch.setattr(sqlite3, 'connect', remove_then_connect)
    assert n.unsubscribe(setup, 'reserved') == []
    assert not path.exists()


def test_unsubscribe_existing_connect_failure_is_not_hidden(setup, monkeypatch):
    n.subscribe(setup, 'reserved', 'test', reservation=True)
    def fail_connect(*args, **kwargs):
        raise sqlite3.OperationalError('unavailable')
    monkeypatch.setattr(sqlite3, 'connect', fail_connect)
    with pytest.raises(sqlite3.OperationalError, match='unavailable'):
        n.unsubscribe(setup, 'reserved')


def test_unsubscribe_corrupt_database_is_not_initialized(setup):
    path = setup.parent / 'notifications.sqlite3'
    path.write_bytes(b'corrupt database')
    before = path.read_bytes()
    with pytest.raises(sqlite3.DatabaseError):
        n.unsubscribe(setup, 'reserved')
    assert path.read_bytes() == before
