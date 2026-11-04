"""External owned-process probes. No engine tasks or engine implementation imports."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile

import pytest

from conformance.s08.observer import Observer, LIMIT

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='Darwin kernel observer only')


@pytest.fixture
def observer(tmp_path):
    artifact_parent = Path(tempfile.mkdtemp(prefix='s08-preflight-', dir='/private/tmp'))
    value = Observer(artifact_parent / 'o').start()
    yield value
    value.cleanup(timeout=2)
    value.close()


def rows(observer):
    return [json.loads(line) for line in (observer.directory / 'observer.jsonl').read_text().splitlines()]


def probe(observer, script):
    return subprocess.Popen([sys.executable, '-u', '-c', script,
                             observer.socket_path, observer.nonce], cwd=ROOT,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)


def finish(child):
    # This fallback targets only this test's directly owned, unreaped child.
    if child.poll() is None:
        child.kill()
    return child.communicate(timeout=4)


CHAIN = r'''
import os, signal, subprocess, sys
from conformance.s08.client import register
path, nonce=sys.argv[1:3]
index=int(sys.argv[3]) if len(sys.argv)>3 else 0
role=('cli','ctl-fork','supervisor','fake')[index]
handle=register(path, nonce, role, control=index != 0)
if index<3:
    child=subprocess.Popen([sys.executable,'-u','-c',sys.argv[4] if len(sys.argv)>4 else SCRIPT,path,nonce,str(index+1),sys.argv[4] if len(sys.argv)>4 else SCRIPT])
    child.wait(timeout=12)
    handle.close()
else:
    print('ready',flush=True)
    signal.pause()
'''
CHAIN = 'SCRIPT=' + repr(CHAIN) + '\n' + CHAIN



def test_kernel_identity_exit_and_connection_scoped_cleanup(observer):
    child = probe(observer, CHAIN)
    try:
        assert observer.wait_roles({'cli', 'ctl-fork', 'supervisor', 'fake'}, 5), rows(observer)
        records = observer.records
        assert {row['role'] for row in records} == {'cli', 'ctl-fork', 'supervisor', 'fake'}
        assert len({row['token'] for row in records}) == 4
        assert all(not row['exited'] for row in records)
        root = next(row for row in records if row['role'] == 'cli')
        assert root['pid'] == child.pid and root['ppid'] == os.getpid()
        prior = root
        for role in ('ctl-fork', 'supervisor', 'fake'):
            current = next(row for row in records if row['role'] == role)
            assert current['ppid'] == prior['pid']
            prior = current
        assert observer.kill_participant('foreign-token') is False
        assert observer.kill_participant(root['token']) is False
        fake = next(row for row in records if row['role'] == 'fake')
        assert observer.kill_participant(fake['token']) is True
        assert observer.wait_exited(fake['token'], 4), rows(observer)
        assert observer.wait_for(lambda values: all(row['exited'] for row in values), 4)
        assert observer.cleanup(2) is True
        child.communicate(timeout=4)
        assert child.returncode == 0
        events = rows(observer)
        for record in records:
            assert any(event['event'] == 'spawn' and event['token'] == record['token'] for event in events)
            assert any(event['event'] == 'exit' and event['token'] == record['token'] for event in events)
        assert observer.directory.exists()
    finally:
        observer.cleanup(2)
        finish(child)


@pytest.mark.parametrize('change', [dict(nonce='wrong'), dict(pid=1), dict(source_profile='foreign'),
                                  dict(role='fake'), dict(role='unknown'), dict(pid=True), dict(control=True)])
def test_registration_refuses_unknown_identity_or_authority(observer, change):
    request = dict(nonce=observer.nonce, source_profile='s08', role='cli', pid=os.getpid(), control=False)
    request.update(change)
    with socket.socket(socket.AF_UNIX) as conn:
        conn.settimeout(3)
        conn.connect(observer.socket_path)
        conn.sendall(json.dumps(request).encode() + b'\n')
        assert conn.recv(4096) == b''
    assert observer.records == []
    assert observer.cleanup(.2) is False
    assert any(row['event'] == 'registration-refused' for row in rows(observer))
    assert observer.directory.exists()


@pytest.mark.parametrize('payload', [b'{bad}\n', b'[]\n', b'{}\n{}\n', b'x' * (LIMIT + 1), b'{"nonce":'], ids=['malformed','array','extra','oversize','truncated'])
def test_malformed_oversized_and_truncated_registration_retained_as_failure(observer, payload):
    with socket.socket(socket.AF_UNIX) as conn:
        conn.settimeout(3)
        conn.connect(observer.socket_path)
        conn.sendall(payload)
        conn.shutdown(socket.SHUT_WR)
        assert conn.recv(4096) == b''
    assert observer.records == []
    assert observer.cleanup(.2) is False
    assert any(row['event'] == 'registration-refused' for row in rows(observer))


def test_ack_requires_successful_kernel_exit_watch(observer, monkeypatch):
    class RefusingQueue:
        def control(self, changes, *args):
            if changes:
                raise OSError('independent simulated watch registration refusal')
            return []
        def close(self): pass
    original = observer._queue
    monkeypatch.setattr(observer, '_queue', RefusingQueue())
    child = probe(observer, '''
import sys
from conformance.s08.client import register
try:
    register(sys.argv[1],sys.argv[2],'cli',control=False)
except Exception:
    sys.exit(31)
sys.exit(92)
''')
    try:
        _, err = child.communicate(timeout=5)
        assert child.returncode == 31, err
        assert observer.records == []
        assert observer.cleanup(.2) is False
    finally:
        finish(child)
        monkeypatch.setattr(observer, '_queue', original)


def test_no_exit_event_cannot_certify_cleanup(observer, monkeypatch):
    class NoEvents:
        def __init__(self, original): self.original = original
        def control(self, changes, *args):
            if changes:
                return self.original.control(changes, *args)
            return []
        def close(self): pass
    original = observer._queue
    monkeypatch.setattr(observer, '_queue', NoEvents(original))
    child = probe(observer, '''
import sys
from conformance.s08.client import register
register(sys.argv[1],sys.argv[2],'cli',control=False)
''')
    try:
        assert observer.wait_roles({'cli'}, 4), rows(observer)
        child.communicate(timeout=4)
        token = observer.records[0]['token']
        assert observer.wait_exited(token, .1) is False
        assert observer.cleanup(.1) is False
        assert observer.records[0]['exited'] is False
    finally:
        finish(child)
        monkeypatch.setattr(observer, '_queue', original)


def test_live_connection_limit_is_failure_and_retains_evidence(observer, monkeypatch):
    from conformance.s08 import observer as module
    monkeypatch.setattr(module, 'MAX_CONNECTIONS', 2)
    connections = []
    try:
        for _ in range(3):
            conn = socket.socket(socket.AF_UNIX)
            conn.settimeout(3)
            conn.connect(observer.socket_path)
            connections.append(conn)
        assert connections[-1].recv(4096) == b''
        assert len(observer._connections) <= 2
        assert observer.cleanup(.2) is False
        assert any(row['event'] == 'registration-refused' and 'bound' in row['reason'] for row in rows(observer))
    finally:
        for conn in connections:
            conn.close()


def test_participant_limit_prevents_ack(observer, monkeypatch):
    from conformance.s08 import observer as module
    monkeypatch.setattr(module, 'MAX_PARTICIPANTS', 0)
    child = probe(observer, '''
import sys
from conformance.s08.client import register
try:
    register(sys.argv[1],sys.argv[2],'cli',control=False)
except Exception:
    sys.exit(31)
sys.exit(92)
''')
    try:
        _, err = child.communicate(timeout=5)
        assert child.returncode == 31, err
        assert observer.records == []
        assert observer.cleanup(.2) is False
    finally:
        finish(child)


def test_actual_fork_hook_registers_while_parent_alive(observer):
    child = probe(observer, '''
import os,sys,threading
from conformance.s08.client import register,install_fork_hook
h=register(sys.argv[1],sys.argv[2],'cli',control=False)
assert len(threading.enumerate()) == 1
install_fork_hook(h)
pid=os.fork()
if pid == 0:
    print('child-entry',flush=True)
    os._exit(0)
_, status=os.waitpid(pid,0)
assert os.waitstatus_to_exitcode(status) == 0
h.close()
''')
    try:
        assert observer.wait_roles({'cli','ctl-fork'}, 4), rows(observer)
        out, err = child.communicate(timeout=4)
        assert child.returncode == 0, err
        assert out == b'child-entry\n'
        records = observer.records
        cli = next(row for row in records if row['role'] == 'cli')
        fork = next(row for row in records if row['role'] == 'ctl-fork')
        assert cli['pid'] == child.pid and fork['ppid'] == child.pid
        assert observer.wait_exited(fork['token'], 3)
        assert observer.wait_exited(cli['token'], 3)
        assert observer.cleanup(1) is True
    finally:
        finish(child)


def test_ancestry_spawn_failure_closes_preallocated_selector(monkeypatch):
    from conformance.s08 import observer as module
    events = []
    class Selector:
        def __init__(self): events.append('selector-created')
        def close(self): events.append('selector-closed')
    def spawn(*args, **kwargs):
        events.append('spawn-attempt')
        raise OSError('independent simulated spawn failure')
    monkeypatch.setattr(module.selectors,'DefaultSelector',Selector)
    monkeypatch.setattr(module.subprocess,'Popen',spawn)
    with pytest.raises(OSError,match='simulated spawn failure'):
        module.ancestry(123)
    assert events == ['selector-created','spawn-attempt','selector-closed']


@pytest.mark.parametrize('failure', ['bind','selector-registration','thread-start'])
def test_partial_observer_failure_closes_all_acquired_resources_and_retains_ledger(monkeypatch, failure):
    import builtins
    from conformance.s08 import observer as module
    parent = Path(tempfile.mkdtemp(prefix='s08-allocation-',dir='/private/tmp'))
    closed = []
    ledgers = []
    def ledger_open(*args, **kwargs):
        value = builtins.open(*args, **kwargs)
        ledgers.append(value)
        return value
    class Queue:
        def close(self): closed.append('queue')
    class Socket:
        def bind(self, path):
            if failure == 'bind':
                raise OSError('independent bind failure')
        def listen(self, bound): pass
        def setblocking(self, blocking): pass
        def close(self): closed.append('socket')
    class Selector:
        def register(self, *args):
            if failure == 'selector-registration':
                raise OSError('independent selector-registration failure')
        def close(self): closed.append('selector')
    class Thread:
        def __init__(self, *args, **kwargs): pass
        def start(self): raise RuntimeError('independent thread-start failure')
    monkeypatch.setattr(module,'open',ledger_open,raising=False)
    monkeypatch.setattr(module.select,'kqueue',Queue)
    monkeypatch.setattr(module.socket,'socket',lambda *args: Socket())
    monkeypatch.setattr(module.selectors,'DefaultSelector',Selector)
    monkeypatch.setattr(module.threading,'Thread',Thread)
    with pytest.raises((OSError,RuntimeError),match=failure + ' failure'):
        value = module.Observer(parent / 'o')
        value.start()
    assert len(ledgers) == 1 and ledgers[0].closed
    assert set(closed) == ({'socket','queue'} if failure == 'bind' else {'socket','queue','selector'})
    assert len(closed) == len(set(closed))
    assert (parent / 'o/observer.jsonl').is_file()
