"""External Darwin process ledger. No engine state and no saved-PID signals.

LOCAL_PEERPID supplies kernel identity, not authenticity against malicious peers
with the same UID. Only registered ancestry is covered; escaped/unknown processes
are not claimed contained. Evidence is always retained.
"""
import json
import os
from pathlib import Path
import secrets
import select
import selectors
import socket
import struct
import subprocess
import sys
import threading
import time

LIMIT = 4096
MAX_PARTICIPANTS = 256
MAX_CONNECTIONS = 32


def ancestry(pid):
    """Bounded output from an owned ps child, never a broad process inventory."""
    selector = selectors.DefaultSelector()
    try:
        child = subprocess.Popen(['/bin/ps', '-p', str(pid), '-o', 'ppid=,pgid=,lstart='],
                                 stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL, env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
    except BaseException:
        selector.close()
        raise
    data = bytearray()
    try:
        selector.register(child.stdout, selectors.EVENT_READ)
        end = time.monotonic() + 1
        while True:
            remaining = end - time.monotonic()
            if remaining <= 0:
                raise RuntimeError('ps deadline')
            if not selector.select(remaining):
                raise RuntimeError('ps deadline')
            chunk = os.read(child.stdout.fileno(), 512)
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > 1024:
                raise RuntimeError('ps output exceeded')
        if child.wait(timeout=max(.001, end - time.monotonic())) != 0:
            raise RuntimeError('ps failed')
        fields = data.decode('ascii').strip().split(None, 2)
        if len(fields) != 3 or not fields[0].isdigit() or not fields[1].isdigit():
            raise RuntimeError('ps identity missing')
        return dict(ppid=int(fields[0]), pgid=int(fields[1]), lstart=fields[2])
    finally:
        if child.poll() is None:
            child.kill()  # Only our unreaped, directly owned probe.
        child.wait(timeout=1)
        child.stdout.close()
        selector.close()


class Observer:
    def __init__(self, artifact_dir, *, controller_pid=None, source_profile='s08', allow_workflows=False):
        if sys.platform != 'darwin' or not hasattr(select, 'kqueue'):
            raise RuntimeError('S08 observer unsupported: Darwin kqueue required')
        self.directory = Path(artifact_dir)
        self.directory.mkdir(mode=0o700, parents=False, exist_ok=False)
        self.socket_path = str(self.directory / 'observer.sock')
        if len(os.fsencode(self.socket_path)) > 103:
            raise ValueError('Darwin UDS path exceeds 103 bytes; use a fresh short artifact path')
        self.nonce = secrets.token_hex(16)
        self.controller_pid = os.getpid() if controller_pid is None else controller_pid
        if self.controller_pid != os.getpid():
            raise ValueError('controller must be this observer process')
        self.source_profile = source_profile
        self.allow_workflows = allow_workflows
        self._condition = threading.Condition(threading.RLock())
        self._records, self._connections = {}, {}
        self._failed = False
        self._stop = threading.Event()
        self._thread = None
        # Construction can fail before a context manager owns this instance.
        # Close every resource already acquired without deleting its evidence.
        acquired = []
        try:
            self._ledger = open(self.directory / 'observer.jsonl', 'x', encoding='utf-8')
            acquired.append(self._ledger)
            self._queue = select.kqueue()
            acquired.append(self._queue)
            self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            acquired.append(self._listener)
            self._listener.bind(self.socket_path)
            self._listener.listen(MAX_CONNECTIONS)
            self._listener.setblocking(False)
            self._selector = selectors.DefaultSelector()
            acquired.append(self._selector)
            self._selector.register(self._listener, selectors.EVENT_READ)
            self._append('observer-start', controller_pid=self.controller_pid,
                         transport='SAME_UID; not malicious same-UID authentication')
        except BaseException:
            for resource in reversed(acquired):
                resource.close()
            raise

    def _append(self, event, **fields):
        row = dict(event=event, event_id=secrets.token_hex(16), nonce=self.nonce,
                   monotonic=time.monotonic(), **fields)
        line = json.dumps(row, sort_keys=True)
        if len(line) > LIMIT * 2:
            raise RuntimeError('ledger metadata exceeded')
        self._ledger.write(line + '\n')
        self._ledger.flush()
        os.fsync(self._ledger.fileno())

    def start(self):
        if self._thread is not None:
            raise RuntimeError('observer already started')
        self._thread = threading.Thread(target=self._serve, daemon=True)
        try:
            self._thread.start()
        except BaseException:
            self._thread = None
            self.close()
            raise
        return self

    @property
    def records(self):
        with self._condition:
            return [dict(row) for row in self._records.values()]

    def _register(self, conn, request):
        pid = struct.unpack('i', conn.getsockopt(0, 2, 4))[0]  # SDK sys/un.h LOCAL_PEERPID
        if not isinstance(request, dict) or set(request) != {'nonce', 'role', 'pid', 'control', 'source_profile'}:
            raise ValueError('registration shape')
        if request['nonce'] != self.nonce or request['source_profile'] != self.source_profile:
            raise ValueError('nonce or source profile')
        if type(request['pid']) is not int or request['pid'] != pid or type(request['control']) is not bool:
            raise ValueError('claimed PID or control')
        role = request['role']
        roles = ('cli', 'ctl-fork', 'supervisor', 'fake', 'descendant')
        if self.allow_workflows:
            roles += ('workflow-owner',)
        if role not in roles:
            raise ValueError('role')
        if (role == 'cli') == request['control']:
            raise ValueError('CLI must be thread-free; other roles require control')
        identity = ancestry(pid)
        parents = {'ctl-fork': ('cli',), 'supervisor': ('ctl-fork',),
                   'fake': ('supervisor',), 'descendant': ('fake',)}
        if self.allow_workflows:
            parents['workflow-owner'] = ('cli',)
            parents['supervisor'] += ('workflow-owner',)
            parents['fake'] += ('workflow-owner',)
        with self._condition:
            if len(self._records) >= MAX_PARTICIPANTS or any(r['pid'] == pid and not r['exited'] for r in self._records.values()):
                raise ValueError('participant bound or duplicate')
            parent = next((r for r in self._records.values() if r['pid'] == identity['ppid'] and not r['exited']), None)
            valid = (role == 'cli' and (identity['ppid'] == self.controller_pid or (parent and parent['role'] == 'fake')))
            if not valid and not (role in parents and parent and parent['role'] in parents[role]):
                raise ValueError('unknown or wrong parent')
            token = secrets.token_hex(16)
            # This succeeds while the client is blocked waiting for our ACK.
            event = select.kevent(pid, filter=select.KQ_FILTER_PROC,
                                  flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_ONESHOT,
                                  fflags=select.KQ_NOTE_EXIT)
            self._queue.control([event], 0, 0)
            record = dict(token=token, registration_nonce=secrets.token_hex(16), pid=pid,
                          role=role, control=request['control'], exited=False, **identity)
            self._append('spawn', **record)
            self._records[token] = record
            self._connections[conn]['token'] = token
            self._condition.notify_all()
            conn.sendall(json.dumps(dict(ok=True, token=token)).encode() + b'\n')
            self._connections[conn]['registered'] = True

    def _drop(self, conn):
        with self._condition:
            self._selector.unregister(conn)
            self._connections.pop(conn, None)
            conn.close()

    def _serve(self):
        try:
            while not self._stop.is_set():
                for key, _ in self._selector.select(.05):
                    if key.fileobj is self._listener:
                        conn, _ = self._listener.accept()
                        conn.setblocking(False)
                        if len(self._connections) >= MAX_CONNECTIONS:
                            with self._condition:
                                self._failed = True
                                self._append('registration-refused', reason='connection bound')
                            conn.close()
                            continue
                        with self._condition:
                            self._connections[conn] = dict(buffer=bytearray(), end=time.monotonic() + 3, registered=False)
                        self._selector.register(conn, selectors.EVENT_READ)
                        continue
                    conn = key.fileobj
                    state = self._connections[conn]
                    try:
                        chunk = conn.recv(LIMIT + 1)
                        if not chunk:
                            if not state['registered']:
                                with self._condition:
                                    self._failed = True
                                    self._append('registration-refused', reason='EOF before registration')
                            self._drop(conn)
                            continue
                        if state['registered']:
                            raise ValueError('unexpected participant data')
                        state['buffer'].extend(chunk)
                        if len(state['buffer']) > LIMIT:
                            raise ValueError('frame exceeded')
                        if b'\n' in state['buffer']:
                            line, extra = state['buffer'].split(b'\n', 1)
                            if extra:
                                raise ValueError('extra frame')
                            self._register(conn, json.loads(line))
                    except (OSError, ValueError, RuntimeError) as exc:
                        with self._condition:
                            self._failed = True
                            self._append('registration-refused', reason=str(exc)[:200])
                        if conn in self._connections:
                            self._drop(conn)
                for conn, state in list(self._connections.items()):
                    if not state['registered'] and time.monotonic() >= state['end']:
                        with self._condition:
                            self._failed = True
                            self._append('registration-refused', reason='registration deadline')
                        self._drop(conn)
                for event in self._queue.control(None, MAX_PARTICIPANTS, 0):
                    with self._condition:
                        matches = [r for r in self._records.values() if r['pid'] == event.ident and not r['exited']]
                        if not event.fflags & select.KQ_NOTE_EXIT or len(matches) != 1:
                            self._failed = True
                            self._append('watch-error', pid=event.ident)
                        else:
                            record = matches[0]
                            record['exited'] = True
                            record['exit_observed_monotonic'] = time.monotonic()
                            self._append('exit', **record)
                        self._condition.notify_all()
        except BaseException as exc:
            with self._condition:
                self._failed = True
                self._append('observer-error', reason=type(exc).__name__)
                self._condition.notify_all()

    def wait_for(self, predicate, timeout=3):
        end = time.monotonic() + timeout
        with self._condition:
            while not predicate(self.records):
                remaining = end - time.monotonic()
                if remaining <= 0 or self._failed:
                    return False
                self._condition.wait(remaining)
            return True

    def wait_roles(self, roles, timeout=3):
        return self.wait_for(lambda rows: set(roles) <= {r['role'] for r in rows}, timeout)

    def wait_exited(self, token, timeout=3):
        return self.wait_for(lambda rows: any(r['token'] == token and r['exited'] for r in rows), timeout)

    def kill_participant(self, token):
        with self._condition:
            record = self._records.get(token)
            if record is None or record['exited'] or not record['control']:
                return False
            conn = next((c for c, s in self._connections.items() if s.get('token') == token), None)
            if conn is None:
                return False
            try:
                conn.sendall(json.dumps(dict(command='self-kill', token=token)).encode() + b'\n')
                self._append('control-termination-request', **record)
                return True
            except OSError:
                return False

    def cleanup(self, timeout=3):
        end = time.monotonic() + timeout
        for row in self.records:
            if not row['exited'] and row['control']:
                self.kill_participant(row['token'])
        exited = self.wait_for(lambda rows: all(r['exited'] for r in rows), max(0, end - time.monotonic()))
        with self._condition:
            success = exited and not self._failed
            self._append('cleanup', success=success, artifacts_retained=True)
            return success

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=4)
            if self._thread.is_alive():
                raise RuntimeError('observer thread did not stop; artifacts retained')
        for conn in list(self._connections):
            self._drop(conn)
        self._selector.close()
        self._listener.close()
        self._queue.close()
        self._ledger.close()
