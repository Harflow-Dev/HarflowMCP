"""Bounded CLI/owner lifetime rendezvous, separate from external ancestry authority.

Kernel peer PID plus a nonce binds this cooperative same-UID exchange. It does not
protect against malicious same-UID processes or replace observer registration.
"""
import json
import os
from pathlib import Path
import secrets
import socket
import struct
import time

try:
    from conformance.s08.observer import ancestry
except ModuleNotFoundError:
    from observer import ancestry

LIMIT = 4096
DEADLINE = 3


def read_frame(sock, deadline):
    data = bytearray()
    while len(data) <= LIMIT:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('parent rendezvous deadline')
        sock.settimeout(remaining)
        chunk = sock.recv(1)
        if not chunk:
            raise RuntimeError('parent rendezvous EOF')
        if chunk == b'\n':
            value = json.loads(data)
            if not isinstance(value, dict):
                raise ValueError('parent rendezvous frame shape')
            return value
        data.extend(chunk)
    raise ValueError('parent rendezvous frame exceeded')


class ParentRendezvous:
    def __init__(self):
        self.path = str(Path(os.environ['TMPDIR']) / ('p-' + secrets.token_hex(6)))
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.bound = False
        self.nonce = secrets.token_hex(16)
        self.intents = []
        self.ledger = None
        try:
            if len(os.fsencode(self.path)) > 103:
                raise ValueError('parent rendezvous socket path exceeds Darwin bound')
            self.socket.bind(self.path)
            self.bound = True
            os.chmod(self.path, 0o600)
            self.socket.listen(4)
            command_nonce = os.environ['CONFORMANCE_NONCE']
            if not command_nonce.isalnum() or len(command_nonce) > 64:
                raise ValueError('invalid command capture nonce')
            self.ledger = open(Path(os.environ['TMPDIR']) / ('rendezvous-' + command_nonce + '.jsonl'), 'x')
            os.environ['S09_PARENT_RENDEZVOUS'] = self.path
            os.environ['S09_PARENT_NONCE'] = self.nonce
        except BaseException:
            self.close()
            raise

    def audit(self, event, args):
        if event != 'subprocess.Popen':
            return
        executable, argv, _cwd, _env = args
        # Observe only the exact stock owner shape; never patch argv, env or Popen.
        if (isinstance(argv, (list, tuple)) and len(argv) == 7
                and argv[:4] == [os.environ['S08_PYTHON_SHIM'], '-m',
                                'harflow_mcp.workflows', 'supervise']
                and isinstance(argv[4], str) and argv[4]
                and argv[5:] == ['--root', os.environ['HARFLOW_STATE_ROOT']]
                and executable == os.environ['S08_PYTHON_SHIM']):
            if len(self.intents) >= 4:
                raise RuntimeError('workflow-owner launch intent bound')
            intent = {'run_id': argv[4], 'root': argv[6]}
            self.intents.append(intent)
            self._record('intent', **intent)

    def _record(self, event, **fields):
        self.ledger.write(json.dumps({'event': event, 'command_nonce': os.environ['CONFORMANCE_NONCE'],
            'parent_pid': os.getpid(), 'nonce': self.nonce, **fields}, sort_keys=True) + '\n')
        self.ledger.flush()
        os.fsync(self.ledger.fileno())

    def wait(self):
        deadline = time.monotonic() + DEADLINE
        pending = list(self.intents)
        while pending:
            self.socket.settimeout(max(.001, deadline - time.monotonic()))
            conn, _ = self.socket.accept()
            with conn:
                pid = struct.unpack('i', conn.getsockopt(0, 2, 4))[0]
                frame = read_frame(conn, deadline)
                intent = {'run_id': frame.get('run_id'), 'root': frame.get('root')}
                # The shared ps probe has a fixed one-second budget. Reserve it
                # within this rendezvous deadline, rather than extending it.
                if deadline - time.monotonic() < 1:
                    raise TimeoutError('insufficient parent identity probe budget')
                if (set(frame) != {'nonce', 'pid', 'token', 'run_id', 'root'}
                        or frame['nonce'] != self.nonce or intent not in pending
                        or type(frame['pid']) is not int or frame['pid'] != pid
                        or not isinstance(frame['token'], str) or len(frame['token']) != 32
                        or any(c not in '0123456789abcdef' for c in frame['token'])
                        or ancestry(pid)['ppid'] != os.getpid()):
                    raise ValueError('parent rendezvous identity mismatch')
                if time.monotonic() >= deadline:
                    raise TimeoutError('parent identity probe exceeded rendezvous deadline')
                conn.settimeout(deadline - time.monotonic())
                self._record('owner-acknowledged', peer_pid=pid, token=frame['token'], **intent)
                conn.sendall(json.dumps({'ok': True, 'nonce': self.nonce, **intent}).encode() + b'\n')
                pending.remove(intent)

    def close(self):
        self.socket.close()
        if self.ledger is not None:
            self.ledger.close()
        if self.bound:
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass
            self.bound = False
        for name in ('S09_PARENT_RENDEZVOUS', 'S09_PARENT_NONCE'):
            os.environ.pop(name, None)


def acknowledge_owner(handle, run_id, root):
    deadline = time.monotonic() + DEADLINE
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(DEADLINE)
        conn.connect(os.environ['S09_PARENT_RENDEZVOUS'])
        nonce = os.environ['S09_PARENT_NONCE']
        conn.sendall(json.dumps({'nonce': nonce, 'pid': os.getpid(),
                               'token': handle.token, 'run_id': run_id, 'root': root}).encode() + b'\n')
        if read_frame(conn, deadline) != {'ok': True, 'nonce': nonce, 'run_id': run_id, 'root': root}:
            raise ValueError('parent rendezvous acknowledgement mismatch')
    os.environ.pop('S09_PARENT_RENDEZVOUS', None)
    os.environ.pop('S09_PARENT_NONCE', None)
