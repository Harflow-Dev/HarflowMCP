"""Operating-system observer for participants the harness owns.

Participants (the fake agent) register over a Unix socket. The observer takes the PID from the kernel
(LOCAL_PEERPID), walks the ancestry with `ps`, and arms kqueue exit watches on the participant and on every
non-harness ancestor *before* it acknowledges (the ancestry rendezvous), so a later exit of the engine's
owner process is observed without any hook inside the engine and without signalling a saved PID. Darwin
only. LOCAL_PEERPID is kernel identity, not authentication against a hostile same-UID process.
"""
import errno
import json
import os
from pathlib import Path
import re
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
MAX_CHAIN = 16
MAX_PARTICIPANTS = 64


def probe(pid):
    """One bounded `ps` row for a pid: ppid, pgid, lstart and command, or None when it is gone."""
    done = subprocess.run(['/bin/ps', '-p', str(pid), '-o', 'ppid=,pgid=,lstart=,command='], capture_output=True, timeout=2,
                          env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
    text = done.stdout.decode('ascii', 'replace').strip()
    if done.returncode != 0 or not text:
        return None
    fields = text.split(None, 7)
    if len(fields) < 8 or not fields[0].isdigit() or not fields[1].isdigit():
        raise RuntimeError('ps row unreadable')
    return {'ppid': int(fields[0]), 'pgid': int(fields[1]), 'lstart': ' '.join(fields[2:7]), 'command': fields[7][:256]}


def chain_of(pid, stop_pid):
    """Ancestors of `pid` up to (excluding) `stop_pid` or PID 1, nearest first, each with its identity.

    Returns `(participant, chain, completeness)`. `completeness` is `complete` when the walk reached the harness
    or PID 1/0, `truncated` when the hop bound stopped it, and `broken` when an ancestor vanished mid-walk."""
    chain, current = [], probe(pid)
    if current is None:
        raise RuntimeError('participant vanished before ancestry')
    parent = current['ppid']
    while parent not in (0, 1, stop_pid):
        if len(chain) >= MAX_CHAIN:
            return current, chain, 'truncated'
        row = probe(parent)
        if row is None:
            return current, chain, 'broken'
        chain.append({'pid': parent, **row})
        parent = row['ppid']
    return current, chain, 'complete'


PS_ROW = re.compile(r'^\s*(\d+)\s+(\d+)\s+(\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})\s*(.*)$')


def parse_process_table(text):
    """Rows of `ps -axo pid=,ppid=,lstart=,command=` as `{(pid, lstart): command}`; tolerant of BSD and procps layouts."""
    table = {}
    for line in text.splitlines():
        match = PS_ROW.match(line)
        if match:
            table[(int(match.group(1)), ' '.join(match.group(3).split()))] = match.group(4)
    return table


def process_table(limit=4 * 1024 * 1024):
    """Every process as `{(pid, lstart): command}` from one bounded `ps`; raises when it cannot be read."""
    done = subprocess.run(['/bin/ps', '-axo', 'pid=,ppid=,lstart=,command='], capture_output=True, timeout=10,
                          env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'})
    if done.returncode != 0 or len(done.stdout) > limit:
        raise RuntimeError('process table unreadable or over its bound')
    table = parse_process_table(done.stdout.decode('utf-8', 'replace'))
    if not table:
        raise RuntimeError('process table empty')
    return table


class ProcessWatch:
    """Weak, text-based hint, **not** proof that a request started nothing: processes that appeared during a window
    and whose command line names one of the markers. A detached process whose command line names neither (for
    example `/bin/sleep 300`) is invisible to it, which is why it can never satisfy a no-spawn assertion."""

    def __init__(self, markers):
        self.markers = tuple(m for m in markers if m)
        self.before = process_table()

    def new_matching(self):
        now = process_table()
        return sorted((pid, cmd[:200]) for (pid, lstart), cmd in now.items()
                      if (pid, lstart) not in self.before and any(m in cmd for m in self.markers))


class OsObserver:
    def __init__(self, directory):
        if sys.platform != 'darwin' or not hasattr(select, 'kqueue'):
            raise RuntimeError('lifecycle observer unsupported: Darwin kqueue required')
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700)
        self.socket_path = str(self.directory / 'o.sock')
        if len(os.fsencode(self.socket_path)) > 103:
            raise ValueError('Darwin UDS path exceeds 103 bytes; use a shorter artifact path')
        self.nonce = secrets.token_hex(16)
        self.controller_pid = os.getpid()
        self._condition = threading.Condition(threading.RLock())
        self._records, self._watched = {}, {}
        self._failed = False
        self._stop = threading.Event()
        self._queue = select.kqueue()
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(self.socket_path)
        self._listener.listen(16)
        self._listener.setblocking(False)
        self._selector = selectors.DefaultSelector()
        self._selector.register(self._listener, selectors.EVENT_READ)
        self._ledger = open(self.directory / 'observer.jsonl', 'x')
        self._thread = None

    def _append(self, event, **fields):
        self._ledger.write(json.dumps({'event': event, 'monotonic': time.monotonic(), **fields}, sort_keys=True) + '\n')
        self._ledger.flush()

    def start(self):
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        return self

    @property
    def records(self):
        with self._condition:
            return [{**r, 'chain': [dict(c) for c in r['chain']]} for r in self._records.values()]

    def _arm(self, pid, lstart):
        """Arm an exit watch, then re-probe the start identity so a reused PID cannot be watched by mistake.

        Only ESRCH means "already gone", and that is logged. Any other failure is a failed observation."""
        key = (pid, lstart)
        if key in self._watched:
            return
        self._watched[key] = {'pid': pid, 'lstart': lstart, 'exited': False}
        event = select.kevent(pid, filter=select.KQ_FILTER_PROC, flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_ONESHOT,
                              fflags=select.KQ_NOTE_EXIT)
        try:
            self._queue.control([event], 0, 0)
        except OSError as exc:
            if exc.errno != errno.ESRCH:
                self._failed = True
                self._append('watch-error', pid=pid, errno=exc.errno)
                raise ValueError('exit watch could not be armed') from exc
            self._watched[key]['exited'] = True
            self._append('exit', pid=pid, lstart=lstart, via='esrch-at-arm')
            return
        again = probe(pid)
        if again is not None and again['lstart'] != lstart:
            self._failed = True
            self._append('identity-changed', pid=pid, before=lstart, after=again['lstart'])
            raise ValueError('process identity changed while arming its watch')

    def _register(self, conn, request):
        pid = struct.unpack('i', conn.getsockopt(0, 2, 4))[0]  # LOCAL_PEERPID
        if not isinstance(request, dict) or set(request) != {'nonce', 'role', 'pid'} or request['nonce'] != self.nonce \
                or type(request['pid']) is not int or request['pid'] != pid or request['role'] not in ('fake',):
            raise ValueError('registration shape, nonce or claimed pid')
        me, chain, completeness = chain_of(pid, self.controller_pid)
        with self._condition:
            if len(self._records) >= MAX_PARTICIPANTS:
                raise ValueError('participant bound')
            token = secrets.token_hex(16)
            self._arm(pid, me['lstart'])
            for hop in chain:
                self._arm(hop['pid'], hop['lstart'])
            record = {'token': token, 'pid': pid, 'role': request['role'], **me, 'chain': chain, 'chain_completeness': completeness}
            self._records[token] = record
            self._append('register', **{k: v for k, v in record.items() if k != 'chain'}, chain=chain)
            self._condition.notify_all()
        conn.setblocking(True)
        conn.sendall(json.dumps({'ok': True, 'token': token, 'ppid': me['ppid'], 'lstart': me['lstart']}).encode() + b'\n')
        conn.setblocking(False)

    def _serve(self):
        buffers = {}
        try:
            while not self._stop.is_set():
                for key, _ in self._selector.select(.05):
                    if key.fileobj is self._listener:
                        conn, _ = self._listener.accept()
                        conn.setblocking(False)
                        buffers[conn] = bytearray()
                        self._selector.register(conn, selectors.EVENT_READ)
                        continue
                    conn = key.fileobj
                    chunk = conn.recv(LIMIT + 1)
                    if not chunk:
                        self._selector.unregister(conn)
                        conn.close()
                        buffers.pop(conn, None)
                        continue
                    buffers[conn].extend(chunk)
                    if len(buffers[conn]) > LIMIT:
                        raise ValueError('frame exceeded')
                    if b'\n' in buffers[conn]:
                        line = bytes(buffers[conn]).split(b'\n', 1)[0]
                        try:
                            self._register(conn, json.loads(line))
                        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
                            with self._condition:
                                self._failed = True
                                self._append('refused', reason=str(exc)[:200])
                            self._selector.unregister(conn)
                            conn.close()
                            buffers.pop(conn, None)
                for event in self._queue.control(None, 64, 0):
                    with self._condition:
                        if event.filter != select.KQ_FILTER_PROC or not event.fflags & select.KQ_NOTE_EXIT or event.flags & select.KQ_EV_ERROR:
                            self._failed = True
                            self._append('watch-error', pid=event.ident, filter=event.filter, fflags=event.fflags, flags=event.flags)
                            self._condition.notify_all()
                            continue
                        for watched in self._watched.values():
                            if watched['pid'] == event.ident and not watched['exited']:
                                watched['exited'] = True
                                self._append('exit', pid=event.ident, lstart=watched['lstart'])
                        self._condition.notify_all()
        except BaseException as exc:
            with self._condition:
                self._failed = True
                self._append('observer-error', reason=type(exc).__name__)
                self._condition.notify_all()

    # --- queries ---------------------------------------------------------------------------------

    def exited(self, pid, lstart):
        with self._condition:
            watched = self._watched.get((pid, lstart))
            return bool(watched and watched['exited'])

    def wait_for(self, predicate, timeout=5):
        end = time.monotonic() + timeout
        with self._condition:
            while not predicate():
                remaining = end - time.monotonic()
                if remaining <= 0 or self._failed:
                    return False
                self._condition.wait(min(remaining, .2))
            return True

    def wait_registered(self, known=(), timeout=5):
        known = set(known)
        if not self.wait_for(lambda: any(t not in known for t in self._records), timeout):
            return None
        with self._condition:
            fresh = [r for t, r in self._records.items() if t not in known]
        return fresh[0] if len(fresh) == 1 else None

    def wait_exited(self, pid, lstart, timeout=5):
        return self.wait_for(lambda: self.exited(pid, lstart), timeout)

    @property
    def failed(self):
        return self._failed

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=4)
        for key in list(self._selector.get_map().values()):
            if key.fileobj is not self._listener:
                key.fileobj.close()
        self._selector.close()
        self._listener.close()
        self._queue.close()
        self._ledger.close()
