"""Helpers for the scripted-fake self-checks: install the fake like the seam does, run it, read the recorded bundle.

Standard library only. Nothing here imports the engine (`src/harflow_mcp`). Every process the tests start gets
`PATH=<install>/bin:/usr/bin:/bin`, so the only `claude` that can ever be resolved is the installed fake.
"""
import json
import os
from pathlib import Path
import select
import shutil
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[2]
SOURCE = REPO / 'conformance' / 'claude_fake'
BUNDLE = SOURCE / 'recordings'
PARTICIPANT = REPO / 'conformance' / 'lifecycle' / 'participant.py'
MARKER = 'HARFLOW_FAKE_CLAUDE:'
SESSION = '11111111-2222-4333-8444-555555555555'
TIMEOUT = 120  # the first exec of a freshly created script can stall 4-30 s on this host

COMMON = ['-p', '--output-format', 'stream-json', '--verbose', '--include-partial-messages', '--permission-mode', 'bypassPermissions']
LIVE = [*COMMON, '--input-format', 'stream-json']
PARENT = 'import subprocess, sys\nraise SystemExit(subprocess.run(sys.argv[1:]).returncode)\n'


def live(*extra, session=('--session-id', SESSION)):
    return [*LIVE, *session, *extra]


def marker(**spec):
    return MARKER + ' ' + json.dumps(spec)


def frame(text):
    return (json.dumps({'type': 'user', 'message': {'role': 'user', 'content': [{'type': 'text', 'text': text}]}}) + '\n').encode()


# --- the recorded bundle, read independently of the fake ---------------------------------------------------

class Rec:
    def __init__(self, name):
        folder = BUNDLE / name
        self.name = name
        self.run = json.loads((folder / 'run.json').read_text())
        self.raw = [line for line in (folder / 'stdout.jsonl').read_bytes().split(b'\n') if line]
        self.lines = [json.loads(line) for line in self.raw]
        self.stderr = (folder / 'stderr.txt').read_bytes()
        self.cwd = self.run['cwd']
        argv = self.run['argv']
        self.session = next((argv[i + 1] for i, a in enumerate(argv) if a in ('--session-id', '--resume')), None)

    def turns(self):
        out, start = [], 0
        for index, value in enumerate(self.lines):
            if value.get('type') == 'result':
                out.append((start, index + 1))
                start = index + 1
        return out

    def expect(self, start=0, stop=None, session=None, cwd=None, model=None):
        """The documented substitutions applied independently: session id, cwd, and the `system/init` model."""
        def walk(item):
            if isinstance(item, str):
                if cwd:
                    item = item.replace(self.cwd, cwd)
                return item.replace(self.session, session) if session else item
            if isinstance(item, list):
                return [walk(x) for x in item]
            if isinstance(item, dict):
                return {k: walk(v) for k, v in item.items()}
            return item
        out = []
        for value in self.lines[start:stop]:
            value = walk(value)
            if model and value.get('type') == 'system' and value.get('subtype') == 'init':
                value['model'] = model
            out.append(value)
        return out

    def first_assistant_split(self):
        return next(i for i, v in enumerate(self.lines) if v.get('type') == 'assistant') + 1


def parse_lines(data):
    return [json.loads(line) for line in data.split(b'\n') if line]


def read_rows(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def ledgers(directory):
    return sorted(Path(directory).glob('*.jsonl')) if Path(directory).exists() else []


# --- the installed fake -------------------------------------------------------------------------------------

class Proc:
    def __init__(self, popen):
        self.p = popen
        self._buffer = b''

    @property
    def pid(self):
        return self.p.pid

    def read_lines(self, count, timeout=TIMEOUT):
        """Block until `count` stdout lines have arrived (without consuming more than needed from the pipe's line view)."""
        fd, end = self.p.stdout.fileno(), time.monotonic() + timeout
        while self._buffer.count(b'\n') < count:
            left = end - time.monotonic()
            if left <= 0:
                raise AssertionError(f'timed out waiting for {count} stdout lines, have {self._buffer!r}')
            if select.select([fd], [], [], min(left, .2))[0]:
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                self._buffer += chunk
        lines, rest = [], self._buffer
        for _ in range(count):
            if b'\n' not in rest:
                break
            line, rest = rest.split(b'\n', 1)
            lines.append(json.loads(line))
        self._buffer = rest
        return lines

    def send(self, data):
        self.p.stdin.write(data)
        self.p.stdin.flush()

    def close_stdin(self):
        if self.p.stdin is not None:
            try:
                self.p.stdin.close()
            except (BrokenPipeError, OSError, ValueError):
                pass
            self.p.stdin = None  # communicate() must not flush a closed file

    def finish(self, timeout=TIMEOUT, close=True):
        """Wait for exit; returns `(returncode, remaining stdout lines, stderr bytes)`. `close=False` leaves stdin open so a
        signal or gate that was just delivered cannot race a stdin EOF (EOF ends a waiting fake)."""
        if close:
            self.close_stdin()
            out, err = self.p.communicate(timeout=timeout)
            return self.p.returncode, parse_lines(self._buffer + out), err
        fd, end, out = self.p.stdout.fileno(), time.monotonic() + timeout, b''
        while time.monotonic() < end:
            if select.select([fd], [], [], .2)[0]:
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                out += chunk
        code = self.p.wait(timeout=max(1, end - time.monotonic()))
        err = self.p.stderr.read()
        self.close_stdin()
        return code, parse_lines(self._buffer + out), err

    def alive(self):
        return self.p.poll() is None

    def kill(self):
        if self.p.poll() is None:
            self.p.kill()
        try:
            self.p.communicate(timeout=10)
        except Exception:
            pass


class Installed:
    """The fake installed the way `PathFakeSeam` installs it: `bin/claude` launcher, `fake/` folder, `fake.json`."""

    def __init__(self, root, command=None):
        self.root = Path(root)
        self.bin = self.root / 'bin'
        self.fake = self.root / 'fake'
        self.launcher = self.bin / 'claude'
        self.command = command or [str(self.launcher)]
        self.procs = []

    @classmethod
    def install(cls, root):
        self = cls(root)
        self.bin.mkdir(parents=True)
        shutil.copytree(SOURCE / 'recordings', self.fake / 'recordings', ignore=shutil.ignore_patterns('.DS_Store', '__pycache__'))
        shutil.copyfile(SOURCE / 'fake_claude.py', self.fake / 'fake_claude.py')
        shutil.copyfile(PARTICIPANT, self.fake / 'participant.py')
        self.launcher.write_text(f'#!{sys.executable}\nimport runpy\nrunpy.run_path({str(self.fake / "fake_claude.py")!r}, run_name="__main__")\n')
        self.launcher.chmod(0o700)
        self.configure()
        return self

    def clone(self, root):
        """A tamperable copy, run as `python fake_claude.py` so no new launcher file has to be executed."""
        root = Path(root)
        (root / 'bin').mkdir(parents=True)
        shutil.copytree(self.fake, root / 'fake')
        clone = Installed(root, [sys.executable, str(root / 'fake' / 'fake_claude.py')])
        for name in ('cwd', 'real_cwd', 'control_dir', 'stop_file', 'work', 'ledger_dir'):
            if hasattr(self, name):
                setattr(clone, name, getattr(self, name))
        return clone

    def configure(self, **config):
        (self.fake / 'fake.json').write_text(json.dumps(config))

    def set(self, **overrides):
        """Configure with this test's folders (set by the `fake` fixture) plus overrides; `None` drops a key."""
        config = {'ledger_dir': str(self.ledger_dir), 'control_dir': str(self.control_dir), 'workspace': str(self.work),
                  'stop_file': str(self.stop_file), 'lifetime': 90}
        config.update(overrides)
        self.configure(**{k: v for k, v in config.items() if v is not None})

    def env(self):
        return {'PATH': f'{self.bin}:/usr/bin:/bin', 'LC_ALL': 'C.UTF-8'}

    def guard(self):
        found = shutil.which('claude', path=self.env()['PATH'])
        if self.command[0] != str(self.launcher):
            assert found is None, f'claude resolves to {found} on the restricted PATH'
            return
        assert found == str(self.launcher), f'claude resolves to {found}, not the installed fake'

    def spawn(self, args, cwd, parent=False, **kwargs):
        self.guard()
        command = [*self.command, *args]
        if parent:
            command = [sys.executable, '-c', PARENT, *command]
            kwargs.setdefault('start_new_session', True)
        popen = subprocess.Popen(command, cwd=cwd, env=self.env(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
        proc = Proc(popen)
        self.procs.append(proc)
        return proc

    def run_raw(self, args, cwd, stdin=b'', timeout=TIMEOUT, **kwargs):
        self.guard()
        done = subprocess.run([*self.command, *args], cwd=cwd, env=self.env(), input=stdin, capture_output=True, timeout=timeout, **kwargs)
        return done.returncode, done.stdout, done.stderr

    def run(self, args, cwd, stdin=b'', timeout=TIMEOUT, **kwargs):
        """One-shot run to completion: `(returncode, stdout lines, stderr bytes, raw stdout)`."""
        code, out, err = self.run_raw(args, cwd, stdin, timeout, **kwargs)
        return code, parse_lines(out), err, out

    def cleanup(self):
        for proc in self.procs:
            proc.kill()
