"""A scripted stand-in for the `claude` CLI that speaks the stream-json protocol (standard library only, Python 3.11+).

Installed as `<case>/bin/claude` through a short launcher (`#!<python> -IB`) and placed first on the case `PATH`; the engine then
reaches it exactly as it reaches the real CLI. It never reads environment selectors: its only configuration is
`fake.json` beside this file. Recorded behaviour is replayed from the scrubbed bundle in `recordings/`;
failure modes a real CLI cannot produce on demand (`nonzero`, `hold`, `escape`) are scripted and marked as such
in the ledger. See README.md in this directory for the script marker, the modes, `fake.json` and the ledger.

Exit codes: 0 and 1 follow the recorded CLI (1 also answers a refused argument vector); 70 is a fake-internal
refusal (bundle tamper, malformed script, bound exceeded, registration failure, lifetime bound); 74 is a write
error on stdout or stderr. The process dies by signal on SIGTERM (default action; the real CLI's behaviour is
unmeasured).
"""
import os
import sys

HERE = os.path.dirname(os.path.realpath(__file__))
EXIT_REFUSED, EXIT_FAKE, EXIT_WRITE = 1, 70, 74


def write_all(fd, data):
    """The one write routine: loops over partial writes. Returns `(written, error)`; `error` is None on success."""
    written = 0
    try:
        while written < len(data):
            written += os.write(fd, data[written:])
    except OSError as exc:
        return written, f'{type(exc).__name__}: errno {exc.errno}'
    return written, None


def _version_probe():
    """`claude --version` and nothing else: print the recorded line, touch nothing, register nowhere, write no ledger."""
    with open(os.path.join(HERE, 'recordings', 'version.txt'), 'rb') as stream:
        data = stream.read(256)
    os._exit(EXIT_WRITE if write_all(1, data)[1] else 0)


if sys.argv[1:] == ['--version']:
    _version_probe()

import hashlib  # noqa: E402  (after the probe fast path on purpose)
import json  # noqa: E402
import re  # noqa: E402
import select  # noqa: E402
import signal  # noqa: E402
import time  # noqa: E402

MARKER = 'HARFLOW_FAKE_CLAUDE:'
FRAME_BOUND = 64 * 1024
INPUT_BOUND = 1024 * 1024
RAW_BUDGET = 1024 * 1024
MAX_GAP_SECONDS = 2.0
MAX_LIFETIME = 600

BOOLEAN_FLAGS = frozenset(('-p', '--print', '--verbose', '--include-partial-messages', '--forward-subagent-text',
                           '--strict-mcp-config', '--no-session-persistence', '--version'))
VALUE_FLAGS = frozenset(('--output-format', '--input-format', '--permission-mode', '--disallowedTools', '--allowedTools',
                         '--max-turns', '--model', '--effort', '--session-id', '--resume', '--agents', '--settings',
                         '--append-system-prompt', '--add-dir', '--permission-prompts', '--max-budget-usd',
                         '--setting-sources', '--mcp-config', '--tools'))
REPEATABLE_FLAGS = frozenset(('--add-dir',))
PERMISSION_MODES = frozenset(('default', 'plan', 'acceptEdits', 'bypassPermissions'))

MODES = ('complete', 'multiturn', 'tool_use', 'permission_denied', 'error_model', 'interrupt', 'nonzero', 'hold', 'escape')
SCRIPTED_MODES = frozenset(('nonzero', 'hold', 'escape'))
MODE_KEYS = {'hold': {'gate', 'survive_eof'}, 'escape': {'gate', 'survive_eof', 'descendant_holds_pipes'},
             'nonzero': {'exit_code'}}
# mode -> (live recording, one-shot recording); `complete`, `hold` and `escape` use `r05_resume` when --resume is given.
RECORDED = {'complete': ('r02_live_single', 'r01_oneshot'), 'hold': ('r02_live_single', 'r01_oneshot'),
            'escape': ('r02_live_single', 'r01_oneshot'), 'multiturn': ('r03_live_multiturn',) * 2,
            'tool_use': ('r06_tool_use',) * 2, 'permission_denied': ('r07_permission_denied',) * 2,
            'error_model': ('r08_error_bad_model',) * 2, 'nonzero': ('r08_error_bad_model',) * 2,
            'interrupt': ('r09_interrupt',) * 2}
RESUME_RECORDING = 'r05_resume'
INTERRUPT_RECORDING = 'r09_interrupt'
TOKEN = re.compile(r'[0-9a-f]{32}')


class Refusal(Exception):
    """An argument vector the CLI would refuse: one stderr line, exit 1."""


class FakeError(Exception):
    """A fake-internal refusal: one stderr line, exit 70."""


# --- argument vector -----------------------------------------------------------------------------------------

class Parsed:
    def __init__(self):
        self.options, self.positional = {}, []

    def value(self, flag, default=None):
        values = self.options.get(flag)
        return values[-1] if values else default

    def has(self, flag):
        return flag in self.options


def parse_argv(argv):
    parsed, index = Parsed(), 0
    while index < len(argv):
        token = argv[index]
        index += 1
        if token == '--':
            parsed.positional.extend(argv[index:])
            break
        if token == '--permission-prompt-tool':
            raise Refusal('error: approval transport is not faked: ' + token)
        if token in BOOLEAN_FLAGS:
            parsed.options.setdefault('-p' if token == '--print' else token, []).append(True)
        elif token in VALUE_FLAGS:
            if index >= len(argv):
                raise Refusal(f"error: option '{token}' argument missing")
            if token in parsed.options and token not in REPEATABLE_FLAGS:
                raise Refusal(f"error: option '{token}' specified more than once")
            parsed.options.setdefault(token, []).append(argv[index])
            index += 1
        elif token.startswith('-') and len(token) > 1:
            raise Refusal(f"error: unknown option '{token}'")
        else:
            parsed.positional.append(token)
    if len(parsed.positional) > 1:
        raise Refusal('error: more than one positional prompt')
    if parsed.has('--version'):
        raise Refusal("error: option '--version' cannot be combined with other arguments")
    if not parsed.has('-p'):
        raise Refusal('error: interactive mode is not faked; use -p')
    if parsed.has('--session-id') and parsed.has('--resume'):
        raise Refusal('error: --session-id and --resume conflict')
    if parsed.value('--output-format') != 'stream-json':
        raise Refusal('error: only --output-format stream-json is faked')
    if not parsed.has('--verbose'):
        raise Refusal('error: --output-format=stream-json requires --verbose with -p')
    if parsed.value('--input-format', 'text') not in ('text', 'stream-json'):
        raise Refusal("error: unknown --input-format value")
    if parsed.has('--permission-mode') and parsed.value('--permission-mode') not in PERMISSION_MODES:
        raise Refusal('error: unknown --permission-mode value')
    turns = parsed.value('--max-turns')
    if turns is not None and not (turns.isdigit() and int(turns) >= 1):
        raise Refusal('error: --max-turns needs a positive integer')
    budget = parsed.value('--max-budget-usd')
    if budget is not None:
        try:
            ok = float(budget) > 0
        except ValueError:
            ok = False
        if not ok:
            raise Refusal('error: --max-budget-usd needs a positive amount')
    return parsed


# --- configuration, ledger, bundle ---------------------------------------------------------------------------

def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def read_config():
    path = os.path.join(HERE, 'fake.json')
    if not os.path.exists(path):
        return {}
    with open(path, 'rb') as stream:
        config = json.loads(stream.read(65536))
    if not isinstance(config, dict):
        raise FakeError('fake.json is not an object')
    lifetime = config.get('lifetime')
    if type(lifetime) is not int or not 1 <= lifetime <= MAX_LIFETIME:
        raise FakeError(f'fake.json lifetime must be an integer from 1 to {MAX_LIFETIME}')
    return config


def verify_bundle():
    """The bundle must equal SHA256SUMS exactly (no missing, extra or altered file). Returns the SHA256SUMS digest."""
    root = os.path.join(HERE, 'recordings')
    with open(os.path.join(root, 'SHA256SUMS'), 'rb') as stream:
        sums = stream.read()
    listed = {}
    for line in sums.decode('utf-8').splitlines():
        digest, _, name = line.partition('  ')
        if not re.fullmatch('[0-9a-f]{64}', digest) or not name or name.startswith('/') or '..' in name.split('/'):
            raise FakeError('recording bundle: unreadable SHA256SUMS')
        listed[name] = digest
    found = {}
    for folder, _, names in os.walk(root, followlinks=False):
        for name in names:
            path = os.path.join(folder, name)
            relative = os.path.relpath(path, root).replace(os.sep, '/')
            if relative == 'SHA256SUMS':
                continue
            if os.path.islink(path):
                raise FakeError('recording bundle: symbolic link ' + relative)
            with open(path, 'rb') as stream:
                found[relative] = sha256_bytes(stream.read())
    if found != listed:
        raise FakeError('recording bundle differs from SHA256SUMS')
    return sha256_bytes(sums)


class Recording:
    def __init__(self, root, name):
        folder = os.path.join(root, name)
        with open(os.path.join(folder, 'run.json'), 'rb') as stream:
            self.run = json.loads(stream.read())
        with open(os.path.join(folder, 'stdout.jsonl'), 'rb') as stream:
            data = stream.read()
        rows = data.split(b'\n')
        if rows and rows[-1] == b'':
            rows.pop()
        self.lines = [(row + b'\n', json.loads(row)) for row in rows]
        with open(os.path.join(folder, 'stderr.txt'), 'rb') as stream:
            self.stderr = stream.read()
        self.t_ms = self.run['t_ms']
        self.exit_code = self.run['exit_code']
        self.cwd = self.run['cwd']
        argv = self.run['argv']
        self.session = next((argv[i + 1] for i, item in enumerate(argv[:-1]) if item in ('--session-id', '--resume')), None)
        if len(self.t_ms) != len(self.lines):
            raise FakeError('recording bundle: timeline does not match ' + name)

    def turns(self):
        """Index ranges of the turns, each ending at a `result` line."""
        ranges, start = [], 0
        for index, (_, value) in enumerate(self.lines):
            if value.get('type') == 'result':
                ranges.append((start, index + 1))
                start = index + 1
        if start < len(self.lines):
            ranges.append((start, len(self.lines)))
        return ranges


class Ledger:
    """Untrusted corroboration written by the fake itself, one JSON object per flushed line."""

    def __init__(self, directory, ident):
        os.makedirs(directory, exist_ok=True)
        self.path = os.path.join(directory, f'{os.getpid()}-{sha256_bytes(ident.encode())[:16]}.jsonl')
        self.stream = open(self.path, 'x', buffering=1)
        self.budget = RAW_BUDGET
        self.closed = False

    def add(self, event, **fields):
        if self.closed:
            return
        try:
            self.stream.write(json.dumps({'event': event, 'monotonic': time.monotonic(), **fields}, sort_keys=True) + '\n')
            self.stream.flush()
        except (OSError, ValueError):
            self.closed = True

    def io(self, direction, data, written=None, error=None):
        record = {'direction': direction, 'bytes': len(data), 'sha256': sha256_bytes(data)}
        if len(data) <= self.budget:
            self.budget -= len(data)
            record['raw_hex'] = data.hex()
        else:
            record['digest_only'] = True
        if written is not None:
            record['written'] = written
        if error:
            record['error'] = error
        self.add('io', **record)

    def close(self):
        if not self.closed:
            self.closed = True
            try:
                self.stream.close()
            except OSError:
                pass


# --- marker --------------------------------------------------------------------------------------------------

def parse_marker(text):
    """The script of a run: `{'mode': 'complete'}` unless the first prompt text carries the marker and one JSON object."""
    if MARKER not in text:
        return {'mode': 'complete'}
    rest = text.split(MARKER, 1)[1].lstrip()
    try:
        spec, _ = json.JSONDecoder().raw_decode(rest)
    except ValueError as exc:
        raise FakeError('malformed script marker: ' + str(exc)[:80])
    if not isinstance(spec, dict):
        raise FakeError('script marker is not an object')
    mode = spec.get('mode', 'complete')
    if mode not in MODES:
        raise FakeError('unknown script mode')
    unknown = set(spec) - {'mode'} - MODE_KEYS.get(mode, set())
    if unknown:
        raise FakeError('unknown script key for mode ' + mode + ': ' + sorted(unknown)[0][:40])
    if mode in ('hold', 'escape'):
        if not isinstance(spec.get('gate'), str) or not os.path.isabs(spec['gate']):
            raise FakeError('script gate must be an absolute path')
        for key in ('survive_eof', 'descendant_holds_pipes'):
            if key in spec and not isinstance(spec[key], bool):
                raise FakeError(f'script key {key} must be a boolean')
    if mode == 'nonzero':
        code = spec.get('exit_code', 7)
        if type(code) is not int or not 1 <= code <= 255:
            raise FakeError('script exit_code must be an integer from 1 to 255')
    return spec


def first_text(frame):
    message = frame.get('message') if isinstance(frame, dict) else None
    content = message.get('content') if isinstance(message, dict) else None
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get('type') == 'text' and isinstance(block.get('text'), str):
                return block['text']
    return ''


# --- the run -------------------------------------------------------------------------------------------------

class Run:
    def __init__(self, parsed, config):
        self.parsed, self.config = parsed, config
        self.ledger = None
        self.participant = None
        self.ack = None
        self.stdin_open = True
        self.buffer = bytearray()
        self.stdin_total = 0
        self.turn_open = False
        self.interrupted = False
        self.kill_done = False
        self.pace = float(config.get('pace', 0) or 0)
        self.session_flag = '--resume' if parsed.has('--resume') else '--session-id' if parsed.has('--session-id') else None
        self.session = parsed.value(self.session_flag) if self.session_flag else None
        self.model = parsed.value('--model')
        self.cwd = os.getcwd()
        self.root = os.path.join(HERE, 'recordings')
        self.recordings = {}
        self.spec = {'mode': 'complete'}
        self.gate = None

    # --- output ---------------------------------------------------------------------------------------------

    def write(self, fd, data):
        direction = 'stdout' if fd == 1 else 'stderr'
        written, error = write_all(fd, data)
        if self.ledger is not None:
            self.ledger.io(direction, data, written=written, error=error)
        if error:
            self.finish(EXIT_WRITE, note='write error on ' + direction)

    def finish(self, code, note=None):
        if self.ledger is not None:
            self.ledger.add('exit', code=code, **({'note': note} if note else {}))
            self.ledger.close()
        os._exit(code)

    def fail(self, message, code=EXIT_FAKE):
        self.write(2, (f'harflow fake claude: {message}\n').encode())
        self.finish(code, note=message)

    # --- recordings -----------------------------------------------------------------------------------------

    def recording(self, name):
        if name not in self.recordings:
            self.recordings[name] = Recording(self.root, name)
        return self.recordings[name]

    def base_name(self, mode, live):
        if mode in ('complete', 'hold', 'escape') and self.parsed.has('--resume'):
            return RESUME_RECORDING
        return RECORDED[mode][0 if live else 1]

    def render(self, rec, index):
        raw, value = rec.lines[index]
        session = self.session if rec.session and self.session else None
        cwd = self.cwd

        def walk(item):
            if isinstance(item, str):
                if cwd != rec.cwd:
                    item = item.replace(rec.cwd, cwd)
                return item.replace(rec.session, session) if session and session != rec.session else item
            if isinstance(item, list):
                return [walk(entry) for entry in item]
            if isinstance(item, dict):
                return {key: walk(entry) for key, entry in item.items()}
            return item
        changed = walk(value)
        if self.model and value.get('type') == 'system' and value.get('subtype') == 'init' and 'model' in changed:
            changed['model'] = self.model
        if changed == value:
            return raw
        return (json.dumps(changed, ensure_ascii=False, separators=(',', ':')) + '\n').encode('utf-8')

    def emit(self, rec, start, stop):
        for index in range(start, stop):
            if self.pace > 0 and index > 0:
                gap = max(0, rec.t_ms[index] - rec.t_ms[index - 1]) / 1000 * self.pace
                self.sleep(min(gap, MAX_GAP_SECONDS))
            value = rec.lines[index][1]
            if value.get('type') == 'system' and value.get('subtype') == 'init':
                self.turn_open = True
            self.write(1, self.render(rec, index))
            if value.get('type') == 'result':
                self.turn_open = False

    def sleep(self, seconds):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            self.check_control()
            time.sleep(min(.05, max(0, end - time.monotonic())))

    # --- control --------------------------------------------------------------------------------------------

    def check_control(self):
        if self.interrupted:
            self.interrupted = False
            if self.ledger is not None:
                self.ledger.add('signal', signum=int(signal.SIGINT), turn_open=self.turn_open)
            if self.turn_open:
                rec = self.recording(INTERRUPT_RECORDING)
                after = int(rec.run['signals'][0][1])
                post = [i for i, t in enumerate(rec.t_ms) if t > after]
                self.emit(rec, post[0], len(rec.lines))
                self.finish(rec.exit_code)
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGINT)
        stop = self.config.get('stop_file')
        if stop and os.path.exists(stop):
            self.finish(0, note='stop file')
        control, ack = self.config.get('control_dir'), self.ack
        if control and ack and not self.kill_done and TOKEN.fullmatch(str(ack.get('token', ''))):
            if os.path.exists(os.path.join(control, ack['token'] + '.kill-owner')):
                self.kill_done = True
                sent = self.participant.kill_parent(ack)
                if self.ledger is not None:
                    self.ledger.add('kill-owner', requested=True, sent=bool(sent))

    def read_some(self):
        data = os.read(0, 65536)
        if self.ledger is not None and data:
            self.ledger.io('stdin', data)
        if not data:
            self.stdin_open = False
            return b''
        self.stdin_total += len(data)
        if self.stdin_total > INPUT_BOUND:
            self.fail('input exceeded its total bound')
        return data

    def read_frame(self):
        """One newline-terminated input frame, or None at EOF."""
        while True:
            newline = self.buffer.find(b'\n')
            if newline >= 0:
                frame = bytes(self.buffer[:newline])
                del self.buffer[:newline + 1]
                if len(frame) > FRAME_BOUND:
                    self.fail('input frame exceeded its bound')
                return frame
            if len(self.buffer) > FRAME_BOUND:
                self.fail('input frame exceeded its bound')
            if not self.stdin_open:
                return None
            self.check_control()
            if select.select([0], [], [], .1)[0]:
                self.buffer.extend(self.read_some())

    def read_all_stdin(self):
        while self.stdin_open:
            self.check_control()
            if select.select([0], [], [], .1)[0]:
                self.buffer.extend(self.read_some())
        return bytes(self.buffer)

    def hold(self, gate, survive_eof):
        """Wait for the gate file; stdin EOF ends the run (exit 0) unless `survive_eof`."""
        while not os.path.exists(gate):
            self.check_control()
            if self.stdin_open:
                if select.select([0], [], [], .1)[0]:
                    chunk = self.read_some()
                    self.buffer.extend(chunk)
                    del self.buffer[:max(0, len(self.buffer) - FRAME_BOUND)]
                    if not chunk and not survive_eof:
                        self.finish(0, note='stdin EOF while holding')
            else:
                time.sleep(.1)

    # --- escaped descendant ---------------------------------------------------------------------------------

    def spawn_descendant(self, gate, hold_pipes):
        pid = os.fork()
        if pid:
            if self.ledger is not None:
                self.ledger.add('descendant', pid=pid)
            return
        try:
            os.setsid()
            if not hold_pipes:
                null = os.open(os.devnull, os.O_RDWR)
                for fd in (0, 1, 2):
                    os.dup2(null, fd)
            lifetime = self.config.get('lifetime', 60)
            self.participant.bound_lifetime(lifetime)
            socket_path = self.config.get('observer_socket')
            if socket_path:
                self.participant.register(socket_path, self.config['nonce'], role='descendant')
            stop = self.config.get('stop_file')
            while not os.path.exists(gate) and not (stop and os.path.exists(stop)):
                time.sleep(.1)
        finally:
            os._exit(0)

    # --- modes ----------------------------------------------------------------------------------------------

    def gate_path(self):
        workspace = self.config.get('workspace')
        if not workspace:
            self.fail('no workspace configured for a gated mode')
        gate = os.path.realpath(self.spec['gate'])
        root = os.path.realpath(workspace)
        if os.path.commonpath([gate, root]) != root or gate == root:
            self.fail('gate outside the case workspace')
        return gate

    def play(self, live, turn):
        """One turn of the selected mode. Returns True when the run goes on to read more frames."""
        mode = self.spec['mode']
        rec = self.recording(self.base_name(mode, live))
        if mode == 'multiturn':
            turns = rec.turns()
            start, stop = turns[min(turn, len(turns) - 1)]
            self.emit(rec, start, stop)
            return True
        if mode in ('error_model', 'nonzero'):
            self.emit(rec, 0, len(rec.lines))
            if rec.stderr:
                self.write(2, rec.stderr)
            self.finish(self.spec.get('exit_code', 7) if mode == 'nonzero' else rec.exit_code)
        if mode == 'interrupt':
            after = int(rec.run['signals'][0][1])
            self.emit(rec, 0, len([t for t in rec.t_ms if t <= after]))
            while True:  # the turn is open: SIGINT, EOF, the stop file or the lifetime bound ends it
                self.check_control()
                if self.stdin_open:
                    if select.select([0], [], [], .1)[0] and not self.read_some():
                        self.finish(0, note='stdin EOF while interrupt pending')
                else:
                    time.sleep(.1)
        if mode in ('hold', 'escape'):
            split = next(i for i, (_, v) in enumerate(rec.lines) if v.get('type') == 'assistant') + 1
            self.emit(rec, 0, split)
            gate = self.gate
            if mode == 'escape':
                self.spawn_descendant(gate, bool(self.spec.get('descendant_holds_pipes', False)))
            self.hold(gate, bool(self.spec.get('survive_eof', False)))
            self.emit(rec, split, len(rec.lines))
            self.finish(rec.exit_code)
        self.emit(rec, 0, len(rec.lines))
        return True

    def end_code(self, live):
        mode = self.spec['mode']
        return self.recording(self.base_name(mode, live)).exit_code

    def run_live(self):
        turn = 0
        while True:
            frame = self.read_frame()
            if frame is None:
                if turn == 0:
                    self.finish(0, note='stdin closed before any frame')
                self.finish(self.end_code(True))
            try:
                value = json.loads(frame)
            except ValueError:
                self.fail('input frame is not JSON')
            if turn == 0:
                if not isinstance(value, dict) or value.get('type') != 'user':
                    self.fail('first input frame is not a user message')
                self.configure(first_text(value), True)
            elif not isinstance(value, dict) or value.get('type') != 'user':
                continue
            self.play(True, turn)
            turn += 1

    def run_oneshot(self):
        if self.parsed.positional:
            text = self.parsed.positional[0]
        else:
            text = self.read_all_stdin().decode('utf-8', 'replace')
        self.configure(text, False)
        self.play(False, 0)
        self.finish(self.end_code(False))

    def configure(self, text, live):
        try:
            self.spec = parse_marker(text)
        except FakeError as exc:
            if self.ledger is not None:
                self.ledger.add('marker-error', reason=str(exc)[:120])
            self.fail(str(exc))
        mode = self.spec['mode']
        if mode in ('hold', 'escape'):
            self.gate = self.gate_path()  # validated before any output
        if self.ledger is not None:
            self.ledger.add('mode', mode=mode, scripted=mode in SCRIPTED_MODES, recording=self.base_name(mode, live),
                            spec={k: v for k, v in self.spec.items()})

    def go(self):
        live = self.parsed.value('--input-format') == 'stream-json'
        signal.signal(signal.SIGINT, lambda *_: setattr(self, 'interrupted', True))
        self.run_live() if live else self.run_oneshot()


def load_participant():
    sys.dont_write_bytecode = True
    """The observer client, located from this file's own path (the launcher runs isolated: no script directory on sys.path)."""
    import importlib.util
    path = os.path.join(HERE, 'participant.py')
    if not os.path.exists(path):  # running from the repository, before installation
        path = os.path.join(os.path.dirname(HERE), 'lifecycle', 'participant.py')
    spec = importlib.util.spec_from_file_location('participant', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def refuse(config, argv, message):
    """A refused argument vector: ledger record (kind refused) when a ledger is configured, never a registration."""
    line = (message + '\n').encode()
    written, error = write_all(2, line)
    code = EXIT_WRITE if error else EXIT_REFUSED
    directory = config.get('ledger_dir')
    if directory:
        try:
            ledger = Ledger(directory, f'refused-{time.time_ns()}')
            ledger.add('start', kind='refused', pid=os.getpid(), argv=argv, cwd=os.getcwd(), reason=message)
            ledger.io('stderr', line, written=written, error=error)
            ledger.add('exit', code=code)
            ledger.close()
        except OSError:
            pass
    os._exit(code)


def die(message):
    os._exit(EXIT_WRITE if write_all(2, f'harflow fake claude: {message}\n'.encode())[1] else EXIT_FAKE)


def main(argv):
    try:
        config = read_config()
    except (OSError, ValueError, FakeError) as exc:
        die('fake.json unreadable or invalid: ' + str(exc)[:100])
    try:
        parsed = parse_argv(argv)
    except Refusal as exc:
        refuse(config, argv, str(exc))
    run = Run(parsed, config)
    try:
        participant = run.participant = load_participant()
        participant.bound_lifetime(config.get('lifetime', 60))
        bundle = verify_bundle()
        if config.get('ledger_dir'):
            # Opened before registering, so a failed registration still leaves a ledger.
            run.ledger = Ledger(config['ledger_dir'], 'unregistered-' + str(time.time_ns()))
            with open(os.path.join(HERE, os.path.basename(__file__)), 'rb') as stream:
                own = sha256_bytes(stream.read())
            run.ledger.add('start', kind='agent', registered=False, pid=os.getpid(), ppid=os.getppid(), argv=argv, cwd=run.cwd,
                           session_id=run.session, session_flag=run.session_flag, bundle_sha256=bundle, fake_sha256=own,
                           python=sys.version.split()[0])
        if config.get('observer_socket'):
            try:
                run.ack = participant.register(config['observer_socket'], config['nonce'], role='fake')
            except Exception as exc:
                if run.ledger is not None:
                    run.ledger.add('registration', ok=False, error=type(exc).__name__)
                raise FakeError('observer registration failed: ' + type(exc).__name__)
            if run.ledger is not None:
                run.ledger.add('registration', ok=True, ack_token=run.ack.get('token'), lstart=run.ack.get('lstart'),
                               ppid=run.ack.get('ppid'))
        run.go()
    except FakeError as exc:
        run.fail(str(exc))
    except Exception as exc:
        run.fail('internal error: ' + type(exc).__name__ + ': ' + str(exc)[:120])
    run.finish(0)


if __name__ == '__main__':
    main(sys.argv[1:])
