"""Observer client safety checks; these never launch an engine or provider."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from conformance.s08 import client

ROOT = Path(__file__).resolve().parents[2]


class SocketStub:
    def __init__(self, response):
        self.response = bytearray(response)
        self.closed = False
        self.sent = b''
    def recv(self, n):
        if not self.response:
            return b''
        value = self.response[:n]
        del self.response[:n]
        return bytes(value)
    def connect(self, path):
        self.path = path
    def settimeout(self, value):
        self.timeout = value
    def sendall(self, value):
        self.sent += value
    def shutdown(self, how):
        pass
    def close(self):
        self.closed = True


@pytest.mark.parametrize('raw', [b'', b'{', b'{}', b'x' * (client.LIMIT + 1)])
def test_truncated_or_oversize_response_is_refused(raw):
    with pytest.raises((RuntimeError, ValueError)):
        client._read(SocketStub(raw))


@pytest.mark.parametrize('reply', [{}, {'ok': False, 'token': 't'}, {'ok': True}, {'ok': 1, 'token': 't'}])
def test_registration_requires_positive_ack_and_token(monkeypatch, reply):
    sock = SocketStub(json.dumps(reply).encode() + b'\n')
    monkeypatch.setattr(client.socket, 'socket', lambda *args: sock)
    with pytest.raises(RuntimeError):
        client.register('/not-a-real-socket', 'nonce', 'cli', control=False)
    assert sock.closed


def test_control_false_starts_no_receiver_before_stock_fork(monkeypatch):
    sock = SocketStub(b'{"ok":true,"token":"owned"}\n')
    monkeypatch.setattr(client.socket, 'socket', lambda *args: sock)
    def forbidden(*args, **kwargs):
        pytest.fail('a receiver thread started before stock fork')
    monkeypatch.setattr(client.threading, 'Thread', forbidden)
    handle = client.register('/test-only', 'nonce', 'cli', control=False)
    sent = json.loads(sock.sent)
    assert sent['pid'] == os.getpid()
    assert sent['nonce'] == 'nonce' and sent['control'] is False
    assert sock.closed
    handle.close()


def test_fork_hook_refuses_live_receiver():
    class Handle:
        control = True
    with pytest.raises(ValueError):
        client.install_fork_hook(Handle())


def test_failed_child_registration_exits_explicitly_in_owned_interpreter():
    # Python ignores raised at-fork callback exceptions; a failing child must
    # exit before executing the caller's post-fork engine entry point.
    script = '''
import os
from conformance.s08 import client
class Handle:
    control=False
    path='/missing-s08-observer'
    nonce='test'
    source_profile='s08'
    def close(self): pass
client.install_fork_hook(Handle())
pid=os.fork()
if pid == 0:
    os._exit(93)
_, status=os.waitpid(pid, 0)
print(os.waitstatus_to_exitcode(status), flush=True)
'''
    result = subprocess.run([sys.executable, '-c', script], cwd=ROOT,
                            capture_output=True, timeout=8)
    assert result.returncode == 126, result.stderr
    assert result.stdout == b''


def test_normal_close_does_not_signal_own_process(monkeypatch):
    signals = []
    monkeypatch.setattr(client.os, 'kill', lambda *args: signals.append(args))
    sock = SocketStub(b'')
    handle = client.Handle(sock, '/test-only', 'nonce', 's08', 'token', False)
    handle.close()
    handle._receive()
    assert signals == []


@pytest.mark.parametrize('command', [b'', b'{"command":"other","token":"token"}\n',
                                     b'{"command":"self-kill","token":"foreign"}\n',
                                     b'{"command":"self-kill","token":"token"}\n'])
def test_unexpected_eof_or_command_only_signals_current_process(monkeypatch, command):
    signals = []
    monkeypatch.setattr(client.os, 'kill', lambda *args: signals.append(args))
    sock = SocketStub(command)
    handle = client.Handle(sock, '/test-only', 'nonce', 's08', 'token', False)
    handle._receive()
    assert signals == [(os.getpid(), client.signal.SIGKILL)]
    handle.close()
