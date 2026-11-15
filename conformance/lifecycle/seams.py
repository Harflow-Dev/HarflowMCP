"""How the lifecycle cases obtain a running fake agent: the open fake-backend seam.

`NoSeam` is the only seam that exists for a pinned engine today: it has no fake, so every case that needs a
running agent is *blocked*, never passed. `PythonInjectionSeam` re-uses the S08 controlled-source profile (an
in-process backend replacement plus a shim) so the case logic can be proven against Python 0.0.11; it is not a
pinned binary, uses extra environment, and its runs are never eligible for acceptance. The proposed real seam is
written up in docs/decisions/E2-lifecycle-conformance-seam.md. `PathFakeSeam` is that seam: the scripted fake
`claude` of `conformance/claude_fake` installed first on the case `PATH`, observed by `OsObserver`, with the engine
launching it at explicit `--freedom unrestricted`. It adds no environment key and no engine selector.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time

from ..oracles import ConformanceError, strict_json
from .observer import OsObserver, ProcessWatch
from .target import ENV_ALLOWLIST


class SeamRequired(ConformanceError):
    """The case needs a running fake agent and the selected seam has none."""
    partial = None


class SpawnEvidenceUnavailable(SeamRequired):
    """No independent evidence that a refused request started nothing can be collected, so the pass is withheld.
    `partial` carries the checks that did succeed."""


def require(condition, message, tag='baseline'):
    if not condition:
        raise ConformanceError(f'{message} [{tag}]')


class NoSeam:
    name = 'none'
    acceptance_eligible = True
    fake_available = False
    description = 'pinned engine, no fake agent: only cases that never start an agent can run'

    def __init__(self, engine, workspace, markers=()):
        self.engine, self.workspace = engine, Path(workspace)
        self.markers = tuple(m for m in markers if m)
        self.operations = []
        self.evidence = {}

    @property
    def extra_env_keys(self):
        return list(self.engine.extra_env_keys) if self.engine is not None else []

    def call(self, argv, *, refused=False):
        reply = self.engine.command([*argv, '--json'])
        doc = reply['document'] or {}
        self.operations.append({'argv': list(argv), 'exit_code': reply['exit_code'], 'document': doc, 'process': reply['process']})
        require((reply['exit_code'] != 0 and isinstance(doc.get('error'), dict)) if refused
                else (reply['exit_code'] == 0 and 'error' not in doc), 'unexpected public refusal/success: ' + argv[0], 'contract')
        return doc

    def extra_refusals(self):
        return []

    def task_ids(self):
        return [row['task_id'] for row in self.call(['list'])['tasks']]

    def watch_spawns(self):
        """No-spawn evidence for a refusal is **unavailable** without an observable agent.

        A text-based process-table diff cannot see a detached process whose command line names neither marker
        (for example `/bin/sleep 300`), and macOS offers no unprivileged descendant tracking (kqueue NOTE_TRACK
        fails with EOPNOTSUPP, measured). The diff is therefore recorded only as a weak hint, and `verify`
        always withholds the pass by raising `SpawnEvidenceUnavailable`."""
        try:
            watch = ProcessWatch(self.markers)
        except Exception as exc:  # the hint is optional; its absence changes nothing
            watch, hint_error = None, f'{type(exc).__name__}: {exc}'[:200]
        else:
            hint_error = None

        def verify():
            hint = watch.new_matching() if watch is not None else None
            self.evidence['spawn_check'] = {'method': 'ps-snapshot-diff (weak hint, text-based)', 'markers': list(self.markers),
                                            'new_matching': hint, 'hint_error': hint_error, 'proof': False}
            raise SpawnEvidenceUnavailable('no independent descendant tracking: the no-spawn assertion is unverified and the pass is withheld')
        return verify

    def launch(self, *args, **kwargs):
        raise SeamRequired('this case starts an agent and the selected seam provides no fake')

    resume = launch

    def close(self):
        return self.engine.clean() if self.engine is not None else True


class PythonInjectionSeam:
    """Python-only validation seam built on the S08 profile. Never acceptance evidence."""
    name = 'python-injection'
    acceptance_eligible = False
    fake_available = True
    description = 'S08 controlled-source fixture injection (shim, in-process backend replacement, extra environment)'

    def __init__(self, repo, commit, python, output):
        from ..s08.profile import SourceProfile
        from ..s08.scenarios import Case
        self.engine = None
        self.evidence = {}
        self.profile = SourceProfile(repo, commit, Path(output), python, tasks_enabled=True)
        self.profile.__enter__()
        try:
            self.case = Case(self.profile)
            self.case.init_git()
        except BaseException:
            self.close()
            raise
        self.workspace = self.profile.workspace

    @property
    def extra_env_keys(self):
        return sorted(set(self.profile.env) - ENV_ALLOWLIST)

    @property
    def operations(self):
        return self.case.operations

    def call(self, argv, *, refused=False):
        return self.case.call(list(argv), refused=refused)

    def extra_refusals(self):
        w = self.workspace
        return [['run', '--backend', 'conformance_fake', '--repo', str(w / 'missing'), '--prompt', 'must not spawn'],
                ['run', '--backend', 'conformance_fake', '--repo', str(w), '--prompt', 'must not spawn', '--max-turns', '1']]

    def task_ids(self):
        for _ in range(40):
            result = self.call(['task-list-page'])['result']
            if not result.get('bootstrap_pending'):
                return [row['task_id'] for row in result['items']]
            time.sleep(.25)
        raise ConformanceError('task listing never left preparation')

    def fakes_spawned(self):
        return len([r for r in self.profile.observer.records if r['role'] == 'fake'])

    def watch_spawns(self):
        before = self.fakes_spawned()

        def verify():
            ledgers = len(list((self.profile.output / 'fake-ledger').iterdir()))
            self.evidence['spawn_check'] = {'method': 'observer-registrations (the fake agent only)', 'fakes_before': before,
                                            'fakes_after': self.fakes_spawned(), 'ledgers': ledgers, 'proof': 'agents only'}
            require(self.fakes_spawned() == before and ledgers == 0, 'a refused request started an agent (fake) or wrote fake evidence', 'contract')
        return verify

    def launch(self, mode='complete', **spec):
        item = self.case.launch(mode, **spec)
        return {**item, 'owner': item['supervisor']}

    def resume(self, launch):
        item = self.case.resume(launch)
        return {**item, 'owner': item['supervisor']}

    def exited(self, participant, timeout=5):
        return self.profile.observer.wait_exited(participant['token'], timeout)

    def alive(self, participant):
        return not next(r for r in self.profile.observer.records if r['token'] == participant['token'])['exited']

    def kill_owner(self, launch):
        return self.profile.observer.kill_participant(launch['owner']['token'])

    def raw_evidence(self, launch, completed=False):
        return self.case.raw(launch, completed=completed)

    def close(self):
        try:
            self.profile.__exit__(None, None, None)
            return True
        except ConformanceError:
            return False


FAKE_SOURCE = Path(__file__).resolve().parents[1] / 'claude_fake'
PARTICIPANT_SOURCE = Path(__file__).resolve().parent / 'participant.py'
FORBIDDEN_LAUNCH_FLAGS = ('--caller-approvals', '--allowed-commands')
MARKER = 'HARFLOW_FAKE_CLAUDE:'
OBSERVER_SOCKET_LIMIT = 103
MAX_LIFETIME = 600
# Digest of conformance/claude_fake/recordings/SHA256SUMS: the recording bundle this harness is pinned to. Changing the
# bundle means changing this constant in the same commit.
EXPECTED_SHA256SUMS = '819705a090020a0b9d7bbd67df98621f33f4bc9496278df31b4b7664084faffd'
TOKEN = re.compile('[0-9a-f]{32}')


def sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree_digests(root, skip=()):
    """`{relative path: sha256}` of every regular file under `root`, skipping top-level folders named in `skip`."""
    root, found = Path(root), {}
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root)
        if relative.parts[0] in skip or path.is_dir():
            continue
        if path.is_symlink():
            raise ConformanceError('installed fake contains a symbolic link: ' + str(relative))
        found[str(relative)] = sha256_file(path)
    return found


def read_ledger(path):
    """Parse one fake ledger. Returns rows, start, exit, completeness, `digest_only` and `trailing` (bytes after the last
    newline: an unterminated final line, kept and reported, never silently dropped); a malformed terminated record
    raises. Every row carrying raw bytes is re-checked against its recorded length and digest. The ledger is untrusted
    corroboration."""
    raw = Path(path).read_bytes()
    cut = raw.rfind(b'\n') + 1
    body, trailing = raw[:cut], raw[cut:]
    rows = [strict_json(line) for line in body.splitlines() if line.strip()]
    digest_only = 0
    for row in rows:
        if row.get('event') != 'io':
            continue
        if row.get('digest_only'):
            digest_only += 1
            continue
        data = bytes.fromhex(row['raw_hex'])
        require(len(data) == row['bytes'] and hashlib.sha256(data).hexdigest() == row['sha256'], 'fake ledger byte/digest mismatch', 'contract')
    start = next((r for r in rows if r.get('event') == 'start'), None)
    exits = [r for r in rows if r.get('event') == 'exit']
    return {'rows': rows, 'start': start, 'exit': exits[-1] if exits else None, 'complete': bool(exits), 'digest_only': digest_only,
            'raw': raw, 'trailing': trailing}


def ledger_bytes(rows, direction):
    chunks = [bytes.fromhex(r['raw_hex']) for r in rows if r.get('event') == 'io' and r.get('direction') == direction and 'raw_hex' in r]
    return b''.join(chunks)


class PathFakeSeam(NoSeam):
    """The scripted fake `claude` first on the case `PATH`, observed by the OS observer.

    Everything the engine sees is the real shape of a launch: `claude` is found on `PATH` like the real CLI, started
    with the engine's own argv at `--freedom unrestricted`, and fed a stream-json user frame. The seam adds no
    environment key (the case environment is the closed `ENV_ALLOWLIST` set), so `extra_env_keys` stays empty."""
    name = 'path-fake'
    acceptance_eligible = True
    fake_available = True
    description = 'installed scripted fake claude first on the case PATH, OS-observed, run at explicit --freedom unrestricted'

    def __init__(self, engine, folder, *, lifetime=60, pace=0):
        folder = Path(folder).absolute()
        super().__init__(engine, folder / 'workspace')
        if type(lifetime) is not int or not 1 <= lifetime <= MAX_LIFETIME:
            raise ConformanceError(f'fake lifetime must be an integer from 1 to {MAX_LIFETIME}')
        self.folder, self.lifetime, self.pace = folder, lifetime, pace
        self.bin_dir = folder / 'bin'
        self.fake_dir = folder / 'fake'
        self.launcher = self.bin_dir / 'claude'
        self.ledger_dir = self.fake_dir / 'ledger'
        self.control_dir = self.fake_dir / 'control'
        self.stop_file = self.control_dir / 'stop'
        self.observer = None
        self._short = None
        self._created = []
        self.gates = []
        self.launches = []
        self.evidence = {'seam': self.name}
        try:
            self._install()
        except BaseException:
            self._abort()
            raise

    # --- installation -------------------------------------------------------------------------------------

    def _install(self):
        env = self.engine.env
        expected = f'{self.bin_dir}:/usr/bin:/bin'
        if env.get('PATH') != expected:
            raise ConformanceError('case PATH is not the fake bin directory followed by the system directories')
        if re.search(r'\s', sys.executable):
            raise ConformanceError('the harness interpreter path contains whitespace, which a shebang cannot carry')
        recordings = FAKE_SOURCE / 'recordings'
        if sha256_file(recordings / 'SHA256SUMS') != EXPECTED_SHA256SUMS:
            raise ConformanceError('the recording bundle does not match the pinned SHA256SUMS digest [baseline]')
        listed = {name: digest for digest, _, name in (line.partition('  ') for line in (recordings / 'SHA256SUMS').read_text().splitlines())}
        actual = {k: v for k, v in tree_digests(recordings).items() if k != 'SHA256SUMS' and not k.endswith('.DS_Store')}
        if listed != actual:
            raise ConformanceError('the recording bundle differs from its SHA256SUMS [baseline]')
        if self.launcher.exists() or self.fake_dir.exists():
            raise ConformanceError('the fake is already installed in this case')
        obs = self.folder / 'obs'
        if len(os.fsencode(str(obs / 'o.sock'))) > OBSERVER_SOCKET_LIMIT:
            self._short = Path(tempfile.mkdtemp(prefix='hfo'))
            obs = self._short / 'o'
        self.observer = OsObserver(obs).start()
        self._observer_dir = obs
        self._init_git()
        self.fake_dir.mkdir()
        self._created.append(self.fake_dir)
        for directory in (self.ledger_dir, self.control_dir):
            directory.mkdir()
        shutil.copyfile(FAKE_SOURCE / 'fake_claude.py', self.fake_dir / 'fake_claude.py')
        shutil.copyfile(PARTICIPANT_SOURCE, self.fake_dir / 'participant.py')
        shutil.copytree(FAKE_SOURCE / 'recordings', self.fake_dir / 'recordings', ignore=shutil.ignore_patterns('.DS_Store', '__pycache__'))
        config = {'observer_socket': self.observer.socket_path, 'nonce': self.observer.nonce, 'ledger_dir': str(self.ledger_dir),
                  'control_dir': str(self.control_dir), 'workspace': str(self.workspace), 'lifetime': self.lifetime, 'pace': self.pace,
                  'stop_file': str(self.stop_file)}
        (self.fake_dir / 'fake.json').write_text(json.dumps(config, sort_keys=True, indent=1) + '\n')
        launcher = f'#!{sys.executable} -IB\nimport runpy\nrunpy.run_path({str(self.fake_dir / "fake_claude.py")!r}, run_name="__main__")\n'
        self.launcher.write_text(launcher)
        self._created.append(self.launcher)
        self.launcher.chmod(stat.S_IRWXU)
        self._prewarm()
        interpreter = Path(os.path.realpath(sys.executable))
        self._interpreter = (str(interpreter), sha256_file(interpreter))
        self.fake_identity = {'files': tree_digests(self.fake_dir, skip=('ledger', 'control')), 'launcher_sha256': sha256_file(self.launcher),
                              'interpreter': {'path': sys.executable, 'resolved': str(interpreter), 'python': sys.version, 'sha256': sha256_file(interpreter)},
                              'bundle_sha256': sha256_file(self.fake_dir / 'recordings' / 'SHA256SUMS')}
        self.evidence['fake_identity'] = self.fake_identity
        self._guard()

    def _prewarm(self):
        """Run the probe once through the launcher. The first execution of a new script file can stall for many seconds
        under host execution policy (measured: 4 to 30 s on macOS); doing it here keeps that cost out of the launch
        window and the registration deadline. `--version` alone never registers and writes no ledger."""
        done = subprocess.run([str(self.launcher), '--version'], env={'PATH': '/usr/bin:/bin'}, capture_output=True, timeout=180, check=False)
        if done.returncode != 0 or done.stdout != (self.fake_dir / 'recordings' / 'version.txt').read_bytes():
            raise ConformanceError('the installed launcher did not answer the version probe')

    def _init_git(self):
        """The engine requires a git repository; initialize an empty one in the case workspace only (no hooks, no global config)."""
        git = self.workspace / '.git'
        if git.exists():
            raise ConformanceError('the case workspace is already a git repository')
        env = {'PATH': '/usr/bin:/bin', 'HOME': str(self.folder / 'home'), 'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null',
               'GIT_CONFIG_SYSTEM': '/dev/null', 'GIT_TERMINAL_PROMPT': '0'}
        self._created.append(git)
        done = subprocess.run(['/usr/bin/git', '-c', 'core.hooksPath=/dev/null', 'init', '-q', str(self.workspace)], env=env,
                              capture_output=True, timeout=15, check=False)
        if done.returncode != 0:
            raise ConformanceError('fixture git init failed')

    def _abort(self):
        """Initialization failed: stop the observer and remove only what this seam created."""
        if self.observer is not None:
            try:
                self.observer.close()
            except Exception:
                pass
        for path in reversed(self._created):
            try:
                shutil.rmtree(path) if path.is_dir() else path.unlink()
            except OSError:
                pass
        if self._short is not None:
            shutil.rmtree(self._short, ignore_errors=True)

    def _guard(self):
        """Stop condition: `claude` must resolve to the installed fake and the installed bytes must be unchanged."""
        found = shutil.which('claude', path=self.engine.env['PATH'])
        if found is None or os.path.abspath(found) != str(self.launcher):
            raise ConformanceError('claude does not resolve to the installed fake: refusing to reach a real CLI [baseline]')
        if sha256_file(self.launcher) != self.fake_identity['launcher_sha256'] or \
                tree_digests(self.fake_dir, skip=('ledger', 'control')) != self.fake_identity['files']:
            raise ConformanceError('installed fake differs from its recorded identity [baseline]')
        resolved = os.path.realpath(sys.executable)
        if (resolved, sha256_file(resolved)) != self._interpreter:
            raise ConformanceError('the launcher interpreter differs from its recorded identity [baseline]')

    # --- engine calls -------------------------------------------------------------------------------------

    def call(self, argv, *, refused=False):
        self._guard()
        return super().call(argv, refused=refused)

    def extra_refusals(self):
        return [['run', '--backend', 'claude', '--freedom', 'unrestricted', '--repo', str(self.workspace / 'missing'), '--prompt', 'must not spawn']]

    # --- launches -----------------------------------------------------------------------------------------

    def _prompt(self, mode, spec):
        return '  Harflow lifecycle caf\u00e9 \u65e5\u672c\u8a9e\nHARFLOW_FAKE_CLAUDE: ' + json.dumps({'mode': mode, **spec}, ensure_ascii=False)

    def _registered(self, known, flag, session_id, timeout=15):
        """The one fresh `fake` registration whose ledger names this task's session, session flag and observer token (R12)."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            for record in self.observer.records:
                if record['token'] in known or record['role'] != 'fake':
                    continue
                for path in sorted(self.ledger_dir.glob('*.jsonl')):
                    try:
                        ledger = read_ledger(path)
                    except (ConformanceError, ValueError):
                        continue
                    start = ledger['start']
                    registered = any(r.get('event') == 'registration' and r.get('ok') is True and r.get('ack_token') == record['token']
                                     for r in ledger['rows'])
                    if registered and start.get('kind') == 'agent' and start.get('pid') == record['pid'] \
                            and start.get('session_flag') == flag and start.get('session_id') == session_id:
                        return record, path
            time.sleep(.05)
        raise ConformanceError('no fresh fake registration matches the task session [contract]')

    def _owner(self, fake):
        """The registered parent: the observer's first chain hop, which must be the fake's ppid and armed (R11)."""
        chain = fake['chain']
        require(bool(chain) and chain[0]['pid'] == fake['ppid'] and self.observer.is_armed(chain[0]['pid'], chain[0]['lstart']),
                'fake parent not observed as the first armed chain hop', 'contract')
        return chain[0]

    def _begin(self, argv, prompt, mode, spec, flag, composed=False):
        if any(str(a).startswith(FORBIDDEN_LAUNCH_FLAGS) for a in argv):
            raise ConformanceError('the path fake is never launched with caller approvals or allowed commands')
        known = {r['token'] for r in self.observer.records}
        doc = self.call(argv)
        task_id = doc.get('result', {}).get('task_id')
        require(isinstance(task_id, str) and bool(task_id), 'launch lacks public task identity', 'contract')
        status = self.call(['status', task_id])['task']
        session_id = status.get('session_id')
        require(isinstance(session_id, str) and bool(session_id), 'task lacks a public session id', 'contract')
        fake, ledger = self._registered(known, flag, session_id)
        item = {'task_id': task_id, 'prompt': prompt, 'mode': mode, 'spec': spec,
                'session_id': session_id, 'fake': fake, 'owner': self._owner(fake), 'ledger': str(ledger), 'composed': composed}
        gate = spec.get('gate')
        if isinstance(gate, str) and gate not in self.gates:
            self.gates.append(gate)
        self.launches.append(item)
        return item

    def launch(self, mode='complete', *, flags=(), **spec):
        prompt = self._prompt(mode, spec)
        return self._begin(['run', '--backend', 'claude', '--freedom', 'unrestricted', '--repo', str(self.workspace), *flags, '--prompt', prompt],
                           prompt, mode, spec, '--session-id')

    def resume(self, launch):
        prompt = self._prompt('complete', {})
        return self._begin(['resume', launch['task_id'], prompt], prompt, 'complete', {}, '--resume', composed=True)

    # --- observation --------------------------------------------------------------------------------------

    def exited(self, participant, timeout=5):
        return self.observer.wait_exited(participant['pid'], participant['lstart'], timeout)

    def alive(self, participant):
        return not self.observer.exited(participant['pid'], participant['lstart'])

    def fakes_spawned(self):
        return len([r for r in self.observer.records if r['role'] == 'fake'])

    def descendants(self, launch, timeout=5):
        """Registered descendants of this fake (role `descendant`), waiting a bounded time for the first one."""
        fake = launch['fake']
        self.observer.wait_for(lambda: any(r['role'] == 'descendant' and r['ppid'] == fake['pid'] for r in self.observer.records), timeout)
        return [r for r in self.observer.records if r['role'] == 'descendant' and r['ppid'] == fake['pid']]

    def _ledger_kinds(self):
        kinds = {}
        for path in sorted(self.ledger_dir.glob('*.jsonl')):
            try:
                start = read_ledger(path)['start']
            except (ConformanceError, ValueError):
                kinds[path.name] = 'malformed'
                continue
            kinds[path.name] = (start or {}).get('kind', 'unclassifiable') if start else 'unclassifiable'
        return kinds

    def watch_spawns(self):
        before_tokens = {r['token'] for r in self.observer.records if r['role'] == 'fake'}
        before = self._ledger_kinds()

        def verify():
            after = self._ledger_kinds()
            tokens = {r['token'] for r in self.observer.records if r['role'] == 'fake'}
            fresh = {n: k for n, k in after.items() if n not in before}
            refused = sorted(n for n, k in fresh.items() if k == 'refused')
            bad = {n: k for n, k in fresh.items() if k != 'refused'}
            self.evidence['spawn_check'] = {'method': 'observer-registrations and agent ledgers (the fake agent only)',
                                            'fakes_before': len(before_tokens), 'fakes_after': len(tokens), 'new_agent_or_unclassified_ledgers': bad,
                                            'refused_argv_ledgers': refused, 'proof': 'agents only'}
            if not (tokens == before_tokens and not bad):
                time.sleep(0)  # a failure is final; only a pass needs the settle below
            else:
                time.sleep(.5)  # bounded settle: catch a launch that registers or writes its ledger just after the first look
                after = self._ledger_kinds()
                tokens = {r['token'] for r in self.observer.records if r['role'] == 'fake'}
                fresh = {n: k for n, k in after.items() if n not in before}
                refused = sorted(n for n, k in fresh.items() if k == 'refused')
                bad = {n: k for n, k in fresh.items() if k != 'refused'}
                self.evidence['spawn_check'].update(fakes_after=len(tokens), new_agent_or_unclassified_ledgers=bad, refused_argv_ledgers=refused, settle_seconds=.5)
            require(tokens == before_tokens and not bad, 'a refused request started an agent (fake), or wrote an agent or unclassifiable ledger', 'contract')
        return verify

    def kill_owner(self, launch):
        token = launch['fake']['token']
        require(bool(TOKEN.fullmatch(token)), 'fake token malformed', 'baseline')
        (self.control_dir / (token + '.kill-owner')).write_bytes(b'')
        path = Path(launch['ledger'])
        end = time.monotonic() + 5
        while time.monotonic() < end:
            for row in read_ledger(path)['rows']:
                if row.get('event') == 'kill-owner':
                    return bool(row.get('sent'))
            time.sleep(.05)
        return False

    def raw_evidence(self, launch, completed=False):
        path = Path(launch['ledger'])
        ledger = read_ledger(path)
        rows, start = ledger['rows'], ledger['start']
        require(start is not None and start.get('kind') == 'agent' and start.get('pid') == launch['fake']['pid'], 'fake ledger start does not match the observed fake', 'contract')
        require(any(r.get('event') == 'registration' and r.get('ok') is True and r.get('ack_token') == launch['fake']['token'] for r in rows),
                'fake ledger lacks the observed registration', 'contract')
        require(not (completed and ledger['trailing']), 'fake ledger ends in an unterminated line', 'contract')
        require(start.get('session_id') == launch['session_id'], 'ledger session differs from the public session', 'contract')
        stdin = ledger_bytes(rows, 'stdin')
        first = stdin.split(b'\n', 1)[0]
        frame = strict_json(first)
        texts = [b.get('text') for b in frame['message']['content'] if isinstance(b, dict) and b.get('type') == 'text']
        require(bool(texts) and (texts[0] == launch['prompt'] or (launch['composed'] and texts[0].endswith(launch['prompt']))),
                'raw first input frame lost the exact caller bytes', 'contract')
        public = self.call(['task-prompt', launch['task_id']])['result']['prompt']
        require(public.get('complete') is True and (public.get('text') == launch['prompt'] if not launch['composed'] else
                isinstance(public.get('text'), str) and public['text'].endswith(launch['prompt']) and public['text'] in texts[0]),
                'public prompt differs from caller bytes', 'contract')
        status = self.call(['status', launch['task_id']])['task']
        outputs = [strict_json(line) for line in ledger_bytes(rows, 'stdout').splitlines() if line.strip()]
        require(any(o.get('type') == 'system' and o.get('subtype') == 'init' and o.get('session_id') == status.get('session_id') for o in outputs),
                'raw init does not carry the public session', 'contract')
        if completed:
            require(ledger['complete'], 'fake ledger has no exit entry', 'contract')
            results = [o for o in outputs if o.get('type') == 'result']
            require(bool(results), 'no result line was written by the fake', 'contract')
            if isinstance(results[-1].get('result'), str) and not results[-1].get('is_error'):
                require(status.get('summary') == results[-1]['result'], 'public result differs from the written result line', 'contract')
        fake, owner = launch['fake'], launch['owner']
        evidence = {'path': str(path), 'sha256': hashlib.sha256(ledger['raw']).hexdigest(), 'bytes': len(ledger['raw']), 'untrusted': True,
                    'corroborated_pid': fake['pid'], 'ledger_complete': ledger['complete'], 'trailing_bytes': len(ledger['trailing']), 'digest_only_rows': ledger['digest_only'],
                    'frames': len(outputs), 'mode': launch['mode'],
                    'owner': {'pid': owner['pid'], 'lstart': owner['lstart'], 'command': owner['command']},
                    'observed_chain': [{'pid': h['pid'], 'ppid': h['ppid'], 'pgid': h['pgid']} for h in fake['chain']],
                    'chain_completeness': fake['chain_completeness'], 'supervision': status.get('supervision')}
        self.evidence.setdefault('launches', []).append({'task_id': launch['task_id'], 'mode': launch['mode'], 'ledger': evidence['path']})
        return evidence

    # --- cleanup proof ------------------------------------------------------------------------------------

    def close(self):
        """Release every gate, stop every waiting fake, then require that every armed participant is observed exited."""
        ok = True
        try:
            for gate in self.gates:
                try:
                    Path(gate).write_bytes(b'cleanup release\n')
                except OSError:
                    ok = False
            self.stop_file.write_bytes(b'')
            bound = self.lifetime + 5
            end = time.monotonic() + bound
            while time.monotonic() < end:
                pending = [w for w in self.observer.watched_processes() if not w['exited']]
                if not pending or self.observer.failed:
                    break
                time.sleep(.1)
            watched = self.observer.watched_processes()
            left = [w for w in watched if not w['exited']]
            self.evidence['cleanup'] = {'watched': len(watched), 'left_alive': [{'pid': w['pid'], 'lstart': w['lstart']} for w in left],
                                        'observer_failed': self.observer.failed}
            ok = ok and not left and not self.observer.failed
        finally:
            observer_ledger = self._observer_dir / 'observer.jsonl'
            try:
                self.observer.close()
                if observer_ledger.exists():
                    shutil.copyfile(observer_ledger, self.folder / 'observer.jsonl')
            finally:
                if self._short is not None:
                    shutil.rmtree(self._short, ignore_errors=True)
        return bool(super().close()) and ok
