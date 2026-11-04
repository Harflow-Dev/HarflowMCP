"""Bounded POSIX supervision. Signals precede leader reaping; no containment claim."""
import hashlib
import json
import os
from pathlib import Path
import selectors
import select
import signal
import subprocess
import time
import uuid


def _quiescent_group(pgid):
    """Bounded OS corroboration while the owned leader remains unreaped."""
    selector = selectors.DefaultSelector()
    try:
        probe = subprocess.Popen(['/bin/ps', '-axo', 'pid=,pgid=,stat='], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env={'PATH': '/usr/bin:/bin'})
    except BaseException:
        selector.close()
        raise
    buffer = bytearray()
    deadline = time.monotonic() + 1
    complete = False
    try:
        selector.register(probe.stdout, selectors.EVENT_READ)
        while time.monotonic() < deadline and len(buffer) <= 1048576:
            for key, _ in selector.select(0.05):
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    complete = True
                    break
                buffer.extend(chunk)
            if complete:
                break
        if not complete or len(buffer) > 1048576:
            return False
        if probe.wait(timeout=1) != 0:
            return False
        rows = [line.split() for line in buffer.decode('ascii').splitlines()]
        if not rows or any(len(row) != 3 or not row[0].isdigit() or not row[1].isdigit() for row in rows):
            return False
        members = [row for row in rows if row[1] == str(pgid)]
        return bool(members) and all(row[2].startswith('Z') for row in members)
    except (OSError, UnicodeError, subprocess.TimeoutExpired):
        return False
    finally:
        try:
            probe.kill()
        except (ProcessLookupError, PermissionError):
            pass
        probe.wait(timeout=1)
        probe.stdout.close()
        selector.close()


def run_process(argv, *, cwd, env, artifact_dir, timeout=5, capture_limit=1048576, nonce=None):
    if isinstance(timeout, bool) or isinstance(capture_limit, bool) or timeout <= 0 or capture_limit <= 0:
        raise ValueError('positive bounds required')
    folder = Path(artifact_dir)
    folder.mkdir(exist_ok=False)
    nonce = nonce or uuid.uuid4().hex
    environment = dict(env)
    environment['CONFORMANCE_NONCE'] = nonce
    started = time.monotonic()
    ledger = {'nonce': nonce, 'argv': list(map(str, argv)), 'cwd': str(cwd),
              'environment_sha256': hashlib.sha256(json.dumps(environment, sort_keys=True).encode()).hexdigest(),
              'started_monotonic': started, 'cleanup': 'unknown', 'timed_out': False, 'capture_exceeded': False}
    streams = {}
    selector = selectors.DefaultSelector()
    try:
        process = subprocess.Popen(argv, cwd=cwd, env=environment, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    except BaseException:
        selector.close()
        raise
    ledger['pid'] = process.pid
    ledger['pgid'] = process.pid
    watcher = None
    try:
        with open(folder / 'spawn.json', 'x') as stream:
            json.dump(ledger, stream, sort_keys=True)
        if not hasattr(os, 'waitid'):
            watcher = select.kqueue()
            watcher.control([select.kevent(process.pid, filter=select.KQ_FILTER_PROC, flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT, fflags=select.KQ_NOTE_EXIT)], 0, 0)
        for name in ('stdout', 'stderr'):
            pipe = getattr(process, name)
            os.set_blocking(pipe.fileno(), False)
            selector.register(pipe, selectors.EVENT_READ, name)
            streams[name] = {'bytes_seen': 0, 'captured_bytes': 0, 'sha256': hashlib.sha256(),
                             'path': str(folder / (name + '.bin')), 'file': open(folder / (name + '.bin'), 'xb')}
        # waitid WNOWAIT observes without reaping, keeping the owned PGID reserved.
        while True:
            now = time.monotonic()
            exited = (os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) if watcher is None else (watcher.control(None, 1, 0) or None))
            if now - started >= timeout:
                ledger['timed_out'] = True
                break
            if exited is not None:
                break
            for key, _ in selector.select(min(0.05, max(0, timeout - (now - started)))):
                raw = os.read(key.fd, 65536)
                if not raw:
                    selector.unregister(key.fileobj)
                    continue
                row = streams[key.data]
                row['bytes_seen'] += len(raw)
                keep = raw[:max(0, capture_limit - row['captured_bytes'])]
                row['file'].write(keep)
                row['sha256'].update(keep)
                row['captured_bytes'] += len(keep)
                if row['bytes_seen'] > capture_limit:
                    ledger['capture_exceeded'] = True
            if ledger['capture_exceeded']:
                break
        # Always signal before any wait/poll; even an exited leader can leave pipe holders.
        ledger['signals_finished'] = True
        try:
            os.killpg(process.pid, signal.SIGKILL)
            ledger['cleanup'] = 'group_signalled'
        except ProcessLookupError:
            ledger['cleanup'] = 'group_absent'
        except OSError as exc:
            ledger['cleanup_error'] = str(exc)
            try:
                if exited is not None and _quiescent_group(process.pid):
                    ledger['cleanup'] = 'group_quiescent_os_observed'
            except OSError as probe_exc:
                ledger['cleanup_probe_error'] = str(probe_exc)
        drain_deadline = time.monotonic() + 1
        while selector.get_map() and time.monotonic() < drain_deadline:
            for key, _ in selector.select(0.05):
                raw = os.read(key.fd, 65536)
                if not raw:
                    selector.unregister(key.fileobj)
                    continue
                row = streams[key.data]
                row['bytes_seen'] += len(raw)
                keep = raw[:max(0, capture_limit - row['captured_bytes'])]
                row['file'].write(keep)
                row['sha256'].update(keep)
                row['captured_bytes'] += len(keep)
                if row['bytes_seen'] > capture_limit:
                    ledger['capture_exceeded'] = True
        ledger['pipes_drained'] = not bool(selector.get_map())
        try:
            ledger['returncode'] = process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            ledger['returncode'] = None
            ledger['cleanup'] = 'unknown'
        ledger['ok'] = (ledger.get('returncode') == 0 and ledger['pipes_drained'] and
                        not ledger['timed_out'] and not ledger['capture_exceeded'] and ledger['cleanup'] != 'unknown')
    except Exception as exc:
        ledger['error'] = type(exc).__name__ + ': ' + str(exc)
        ledger['ok'] = False
    finally:
        if not ledger.get('signals_finished'):
            try:
                os.killpg(process.pid, signal.SIGKILL)
                ledger['cleanup'] = 'group_signalled'
            except ProcessLookupError:
                ledger['cleanup'] = 'group_absent'
            except OSError as exc:
                ledger['cleanup_error'] = str(exc)
            try:
                ledger['returncode'] = process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                ledger['cleanup'] = 'unknown'
        if 'returncode' not in ledger:
            try:
                ledger['returncode'] = process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                ledger['returncode'] = None
                ledger['cleanup'] = 'unknown'
        if watcher is not None:
            watcher.close()
        selector.close()
        for name, row in streams.items():
            row['file'].close()
            getattr(process, name).close()
            del row['file']
            row['sha256'] = row['sha256'].hexdigest()
        ledger['streams'] = streams
        ledger['ended_monotonic'] = time.monotonic()
        with open(folder / 'ledger.json', 'x') as stream:
            json.dump(ledger, stream, sort_keys=True, indent=2)
    return ledger
