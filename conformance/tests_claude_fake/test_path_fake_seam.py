"""Self-checks for `PathFakeSeam` (install, identity, guards, launch matching, owner selection, cleanup proof) and its wiring.

The seam talks to the engine only through `engine.command`; here a stub engine stands in: it starts the installed fake the way
the engine would (a parent process, the live argv, the prompt as the first stdin frame) and answers `status`/`task-prompt`.
No engine code is imported and no provider is reachable: every process gets `PATH=<case>/bin:/usr/bin:/bin` and the launcher is
always the installed fake. The seam needs the Darwin observer, so everything here is Darwin-gated.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from types import SimpleNamespace

import pytest

import fakekit
from conformance.lifecycle import cases, observer as observer_module, runner, seams, target
from conformance.lifecycle.target import ENV_ALLOWLIST
from conformance.oracles import ConformanceError
from conformance.runner import isolated_environment
from fakekit import REPO, live

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='the lifecycle observer needs Darwin kqueue and LOCAL_PEERPID; Linux lifecycle is unavailable')

GATED = 'import os, subprocess, sys, time\nwhile not os.path.exists(sys.argv[1]): time.sleep(.01)\nraise SystemExit(subprocess.run(sys.argv[2:]).returncode)\n'
NEVER_EXITS = ('import sys, time\nsys.path.insert(0, {repo!r})\nfrom conformance.lifecycle import participant\n'
               'participant.register({sock!r}, {nonce!r}, role={role!r})\ntime.sleep(300)\n')


class StubEngine:
    """What the seam needs from `Engine`: `env`, `extra_env_keys`, `command`, `clean`. It also plays the engine's part of a launch."""

    def __init__(self, env, root):
        self.env, self.extra_env_keys, self.root = env, (), Path(root)
        self.calls, self.procs, self.sessions, self.prompts = [], [], {}, {}
        self.clean_result, self.summary, self.depth, self.on_spawn = True, 'OK', 1, None
        self.launcher = None
        self.next = 0

    def clean(self):
        return self.clean_result

    def command(self, argv):
        argv = [a for a in argv if a != '--json']
        self.calls.append(argv)
        doc = {}
        if argv[0] == 'run':
            doc = {'result': {'task_id': self.start(argv[argv.index('--prompt') + 1], '--session-id', None)}}
        elif argv[0] == 'resume':
            doc = {'result': {'task_id': self.start('Continue.\n' + argv[2], '--resume', argv[1])}}
            self.summary = 'PELICAN'  # the resume recording's result text
        elif argv[0] == 'status':
            doc = {'task': {'session_id': self.sessions[argv[1]], 'summary': self.summary, 'supervision': {'capture_state': 'live'}}}
        elif argv[0] == 'task-prompt':
            doc = {'result': {'prompt': {'complete': True, 'text': self.prompts[argv[1]]}}}
        elif argv[0] == 'list':
            doc = {'tasks': []}
        return {'document': doc, 'exit_code': 0, 'process': {'returncode': 0}}

    def start(self, prompt, flag, task_id):
        if task_id is None:
            self.next += 1
            task_id, session = f't{self.next}', str(uuid.uuid4())
            self.sessions[task_id] = session
        session = self.sessions[task_id]
        self.prompts[task_id] = prompt
        inner = [sys.executable, '-I', str(self.launcher), *live(session=(flag, session))]
        go = self.root / f'go-{uuid.uuid4().hex}'
        command = [sys.executable, '-I', '-c', GATED, str(go), *([sys.executable, '-I', '-c', fakekit.PARENT] if self.depth == 2 else []), *inner]
        proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env={'PATH': self.env['PATH']},
                                start_new_session=True)
        self.procs.append(proc)
        if self.on_spawn:
            self.on_spawn(proc)
        go.write_bytes(b'')
        proc.stdin.write(fakekit.frame(prompt))
        proc.stdin.flush()
        spec = json.JSONDecoder().raw_decode(prompt.split('HARFLOW_FAKE_CLAUDE:', 1)[1].lstrip())[0] if 'HARFLOW_FAKE_CLAUDE:' in prompt else {}
        if spec.get('mode', 'complete') not in ('hold', 'escape'):
            proc.stdin.close()  # the engine's pump closes live stdin once the turn is over
        return task_id

    def kill_all(self):
        for proc in self.procs:
            if proc.poll() is None:
                proc.kill()
            try:
                proc.communicate(timeout=10)
            except Exception:
                pass


@pytest.fixture
def short_case():
    path = Path(tempfile.mkdtemp(prefix='cfs'))
    yield path
    shutil.rmtree(path, ignore_errors=True)


class Harness:
    def __init__(self, folder, monkeypatch, prewarm, lifetime=60):
        self.folder = folder
        env = isolated_environment(folder)
        self.engine = StubEngine(env, folder)
        if not prewarm:
            monkeypatch.setattr(seams.PathFakeSeam, '_prewarm', lambda self: None)
        self.seam = seams.PathFakeSeam(self.engine, folder, lifetime=lifetime)
        self.engine.launcher = self.seam.launcher
        self.closed = False

    def close(self):
        self.engine.kill_all()
        if not self.closed:
            self.closed = True
            try:
                self.seam.close()
            except Exception:
                pass


@pytest.fixture
def make(short_case, monkeypatch):
    made = []

    def factory(prewarm=False, lifetime=60, folder=None):
        harness = Harness(folder or short_case, monkeypatch, prewarm, lifetime)
        made.append(harness)
        return harness
    yield factory
    for harness in made:
        harness.close()


@pytest.fixture(scope='module')
def real(tmp_path_factory):
    """One seam with the real prewarm (the first launcher execution can stall 4-35 s here); reused by the read-only checks."""
    folder = Path(tempfile.mkdtemp(prefix='cfr'))
    mp = pytest.MonkeyPatch()
    harness = Harness(folder, mp, prewarm=True)
    yield harness
    harness.close()
    mp.undo()
    shutil.rmtree(folder, ignore_errors=True)


# --- install and identity -------------------------------------------------------------------------------------

def test_install_places_a_pinned_fake_first_on_the_path_with_no_extra_environment(real):
    seam, engine = real.seam, real.engine
    launcher = seam.launcher
    assert seam.name == 'path-fake' and seam.acceptance_eligible is True and seam.extra_env_keys == [] and seam.fake_available is True
    assert set(engine.env) <= ENV_ALLOWLIST and engine.env['PATH'] == f'{seam.bin_dir}:/usr/bin:/bin'
    assert shutil.which('claude', path=engine.env['PATH']) == str(launcher)
    assert launcher.read_text().splitlines()[0] == f'#!{sys.executable} -IB' and os.access(launcher, os.X_OK)
    assert (seam.workspace / '.git').is_dir() and seam.workspace == real.folder / 'workspace'
    config = json.loads((seam.fake_dir / 'fake.json').read_text())
    assert set(config) == {'observer_socket', 'nonce', 'ledger_dir', 'control_dir', 'workspace', 'lifetime', 'pace', 'stop_file'}
    assert config['lifetime'] == 60 and config['workspace'] == str(seam.workspace) and config['observer_socket'] == seam.observer.socket_path
    assert config['stop_file'] == str(seam.stop_file) and Path(config['control_dir']) == seam.control_dir
    identity = seam.fake_identity
    for name in ('fake_claude.py', 'participant.py', 'fake.json', 'recordings/SHA256SUMS', 'recordings/r02_live_single/stdout.jsonl'):
        assert identity['files'][name] == hashlib.sha256((seam.fake_dir / name).read_bytes()).hexdigest()
    assert not any(name.startswith(('ledger', 'control')) for name in identity['files'])
    assert identity['files']['fake_claude.py'] == hashlib.sha256((fakekit.SOURCE / 'fake_claude.py').read_bytes()).hexdigest()
    assert identity['bundle_sha256'] == seams.EXPECTED_SHA256SUMS == hashlib.sha256((fakekit.BUNDLE / 'SHA256SUMS').read_bytes()).hexdigest()
    assert identity['launcher_sha256'] == hashlib.sha256(launcher.read_bytes()).hexdigest()
    assert identity['interpreter']['path'] == sys.executable and identity['interpreter']['python'] == sys.version
    assert identity['interpreter']['sha256'] == hashlib.sha256(Path(os.path.realpath(sys.executable)).read_bytes()).hexdigest()
    assert seam.evidence['fake_identity'] is identity
    assert seam.extra_refusals() == [['run', '--backend', 'claude', '--freedom', 'unrestricted', '--repo', str(seam.workspace / 'missing'),
                                      '--prompt', 'must not spawn']]
    done = subprocess.run([str(launcher), '--version'], env={'PATH': '/usr/bin:/bin'}, capture_output=True, timeout=60)
    assert done.stdout == b'2.1.322 (Claude Code)\n' and seam.fakes_spawned() == 0 and list(seam.ledger_dir.iterdir()) == []


def mutate_append(path):
    original = path.read_bytes()
    path.write_bytes(original + b'#drift\n')
    return lambda: path.write_bytes(original)


def mutate_extra(path):
    path.write_text('x')
    return lambda: path.unlink()


def mutate_symlink(path):
    path.symlink_to('/etc/hosts')
    return lambda: path.unlink()


@pytest.mark.parametrize('relative,mutate,message', [
    ('fake/fake_claude.py', mutate_append, 'differs from its recorded identity'),
    ('fake/participant.py', mutate_append, 'differs from its recorded identity'),
    ('fake/fake.json', mutate_append, 'differs from its recorded identity'),
    ('fake/recordings/r02_live_single/stdout.jsonl', mutate_append, 'differs from its recorded identity'),
    ('fake/recordings/SHA256SUMS', mutate_append, 'differs from its recorded identity'),
    ('bin/claude', mutate_append, 'differs from its recorded identity'),
    ('fake/recordings/extra.txt', mutate_extra, 'differs from its recorded identity'),
    ('fake/stray.py', mutate_extra, 'differs from its recorded identity'),
    ('fake/recordings/link', mutate_symlink, 'symbolic link'),
])
def test_hash_drift_of_any_installed_file_refuses_every_engine_call(real, relative, mutate, message):
    seam, engine = real.seam, real.engine
    assert seam.call(['list']) == {'tasks': []}
    before = len(engine.calls)
    restore = mutate(real.folder / relative)
    try:
        with pytest.raises(ConformanceError, match=message):
            seam.call(['list'])
        with pytest.raises(ConformanceError, match=message):
            seam.launch()
    finally:
        restore()
    assert len(engine.calls) == before, 'no engine command may run on a drifted install'
    assert seam.call(['list']) == {'tasks': []}


def test_ledger_and_control_files_are_not_drift(real):
    seam = real.seam
    (seam.ledger_dir / 'x.jsonl').write_text('{}\n')
    (seam.control_dir / 'tok.kill-owner').write_bytes(b'')
    try:
        assert seam.call(['list']) == {'tasks': []}
    finally:
        (seam.ledger_dir / 'x.jsonl').unlink()
        (seam.control_dir / 'tok.kill-owner').unlink()


def test_interpreter_identity_is_part_of_the_guard(real, monkeypatch):
    seam = real.seam
    monkeypatch.setattr(seam, '_interpreter', (seam._interpreter[0], '0' * 64))
    with pytest.raises(ConformanceError, match='interpreter differs'):
        seam.call(['list'])


def test_which_guard_refuses_a_path_that_does_not_resolve_to_the_installed_fake(real, tmp_path):
    seam, engine = real.seam, real.engine
    before, path = len(engine.calls), engine.env['PATH']
    shim = tmp_path / 'shim'
    shim.mkdir()
    (shim / 'claude').write_text('#!/bin/sh\nexit 99\n')
    (shim / 'claude').chmod(0o700)
    try:
        for bad in ('/usr/bin:/bin', f'{shim}:{seam.bin_dir}:/usr/bin:/bin'):
            engine.env['PATH'] = bad
            with pytest.raises(ConformanceError, match='does not resolve to the installed fake'):
                seam.call(['list'])
    finally:
        engine.env['PATH'] = path
    assert len(engine.calls) == before
    assert seam.call(['list']) == {'tasks': []}


@pytest.mark.parametrize('flags', [('--caller-approvals',), ('--allowed-commands', 'git status'), ('--allowed-commands=git',), ('--caller-approvals=x',)])
def test_launch_specs_with_caller_approvals_or_allowed_commands_are_refused_before_any_call(real, flags):
    before = len(real.engine.calls)
    with pytest.raises(ConformanceError, match='never launched with caller approvals or allowed commands'):
        real.seam.launch(flags=flags)
    assert len(real.engine.calls) == before and real.seam.fakes_spawned() == 0


# --- transactional initialization -----------------------------------------------------------------------------

class RecordingObserver(observer_module.OsObserver):
    instances = []

    def __init__(self, directory):
        super().__init__(directory)
        self.closed = False
        RecordingObserver.instances.append(self)

    def close(self):
        self.closed = True
        super().close()


def construct(folder, monkeypatch, env, **kwargs):
    RecordingObserver.instances = []
    monkeypatch.setattr(seams, 'OsObserver', RecordingObserver)
    return seams.PathFakeSeam(StubEngine(env, folder), folder, **kwargs)


def test_a_failure_after_installation_started_closes_the_observer_and_removes_only_what_it_created(short_case, monkeypatch):
    def boom(self):
        raise ConformanceError('prewarm failed')
    monkeypatch.setattr(seams.PathFakeSeam, '_prewarm', boom)
    env = isolated_environment(short_case)
    (short_case / 'sentinel.txt').write_text('keep')
    (short_case / 'workspace' / 'precious.txt').write_text('keep')
    with pytest.raises(ConformanceError, match='prewarm failed'):
        construct(short_case, monkeypatch, env)
    [observer] = RecordingObserver.instances
    assert observer.closed
    assert not (short_case / 'fake').exists() and not (short_case / 'bin' / 'claude').exists() and not (short_case / 'workspace' / '.git').exists()
    assert (short_case / 'sentinel.txt').read_text() == 'keep' and (short_case / 'workspace' / 'precious.txt').read_text() == 'keep'
    assert (short_case / 'bin').is_dir(), 'the harness-created bin directory is not the seam\'s to remove'


def test_preexisting_state_is_refused_and_never_removed(short_case, monkeypatch):
    monkeypatch.setattr(seams.PathFakeSeam, '_prewarm', lambda self: None)
    env = isolated_environment(short_case)
    (short_case / 'workspace' / '.git').mkdir()
    (short_case / 'workspace' / '.git' / 'HEAD').write_text('keep')
    RecordingObserver.instances = []
    monkeypatch.setattr(seams, 'OsObserver', RecordingObserver)
    with pytest.raises(ConformanceError, match='already a git repository'):
        seams.PathFakeSeam(StubEngine(env, short_case), short_case)
    assert (short_case / 'workspace' / '.git' / 'HEAD').read_text() == 'keep' and not (short_case / 'fake').exists()
    assert all(o.closed for o in RecordingObserver.instances)
    shutil.rmtree(short_case / 'workspace' / '.git')
    (short_case / 'fake').mkdir()
    with pytest.raises(ConformanceError, match='already installed'):
        seams.PathFakeSeam(StubEngine(env, short_case), short_case)
    assert (short_case / 'fake').is_dir() and not list((short_case / 'fake').iterdir())


def test_installation_preconditions_fail_before_anything_is_created(short_case, monkeypatch):
    monkeypatch.setattr(seams.PathFakeSeam, '_prewarm', lambda self: None)
    env = isolated_environment(short_case)
    wrong_path = StubEngine({**env, 'PATH': '/usr/bin:/bin'}, short_case)
    with pytest.raises(ConformanceError, match='case PATH is not the fake bin directory'):
        seams.PathFakeSeam(wrong_path, short_case)
    for lifetime in (0, 601, True, '5', 1.5):
        with pytest.raises(ConformanceError, match='lifetime'):
            seams.PathFakeSeam(StubEngine(env, short_case), short_case, lifetime=lifetime)
    monkeypatch.setattr(seams, 'EXPECTED_SHA256SUMS', '0' * 64)
    with pytest.raises(ConformanceError, match='pinned SHA256SUMS'):
        seams.PathFakeSeam(StubEngine(env, short_case), short_case)
    monkeypatch.undo()
    monkeypatch.setattr(sys, 'executable', '/path with space/python')
    with pytest.raises(ConformanceError, match='whitespace'):
        seams.PathFakeSeam(StubEngine(env, short_case), short_case)
    assert not (short_case / 'fake').exists() and not (short_case / 'bin' / 'claude').exists()


# --- launches through the seam --------------------------------------------------------------------------------

def test_launch_matches_the_fresh_registration_by_ledger_session_and_selects_the_first_hop_as_owner(make):
    h = make()
    seam = h.seam
    launch = seam.launch()
    assert launch['task_id'] == 't1' and launch['session_id'] == h.engine.sessions['t1'] and launch['mode'] == 'complete'
    fake, owner = launch['fake'], launch['owner']
    assert fake['role'] == 'fake' and owner['pid'] == fake['ppid'] == fake['chain'][0]['pid'] and seam.observer.is_armed(owner['pid'], owner['lstart'])
    assert fake['chain_completeness'] == 'complete'
    assert Path(launch['ledger']).is_file() and launch['ledger'].startswith(str(seam.ledger_dir))
    run = next(c for c in h.engine.calls if c[0] == 'run')
    assert run[:7] == ['run', '--backend', 'claude', '--freedom', 'unrestricted', '--repo', str(seam.workspace)]
    assert run[-2] == '--prompt' and run[-1].startswith('  Harflow lifecycle caf') and 'HARFLOW_FAKE_CLAUDE: {"mode": "complete"}' in run[-1]
    assert seam.exited(fake, 60) and not seam.alive(fake) and seam.fakes_spawned() == 1
    evidence = seam.raw_evidence(launch, completed=True)
    assert evidence['ledger_complete'] and evidence['corroborated_pid'] == fake['pid'] and evidence['untrusted'] is True
    assert evidence['owner']['pid'] == owner['pid'] and evidence['chain_completeness'] == 'complete' and evidence['frames'] == 12
    assert evidence['supervision'] == {'capture_state': 'live'} and seam.evidence['launches'][0]['task_id'] == 't1'


def test_probe_then_launch_gives_exactly_one_fake_registration(make):
    h = make()
    seam = h.seam
    done = subprocess.run([sys.executable, '-I', str(seam.launcher), '--version'], env={'PATH': h.engine.env['PATH']}, capture_output=True, timeout=120)
    assert done.returncode == 0 and seam.fakes_spawned() == 0
    refused = subprocess.run([sys.executable, '-I', str(seam.launcher), '--nonsense'], env={'PATH': h.engine.env['PATH']}, capture_output=True, timeout=120)
    assert refused.returncode == 1 and seam.fakes_spawned() == 0
    launch = seam.launch()
    assert seam.fakes_spawned() == 1 and launch['fake']['role'] == 'fake'


def test_resume_starts_a_second_registration_in_the_same_session(make):
    h = make()
    seam = h.seam
    first = seam.launch()
    assert seam.exited(first['fake'], 60)
    seam.raw_evidence(first, completed=True)
    assert h.engine.summary == 'OK'
    second = seam.resume(first)
    assert second['session_id'] == first['session_id'] and second['fake']['token'] != first['fake']['token'] and second['composed'] is True
    assert seam.fakes_spawned() == 2 and seam.exited(second['fake'], 60)
    assert seam.raw_evidence(second, completed=True)['ledger_complete']
    start = json.loads(Path(second['ledger']).read_text().splitlines()[0])
    assert start['session_flag'] == '--resume' and start['session_id'] == first['session_id']


def test_registration_matching_rejects_known_tokens_other_sessions_and_other_flags(make):
    h = make()
    seam = h.seam
    launch = seam.launch()
    known = {r['token'] for r in seam.observer.records}
    for known_tokens, flag, session in ((known, '--session-id', launch['session_id']), (set(), '--resume', launch['session_id']),
                                        (set(), '--session-id', str(uuid.uuid4()))):
        with pytest.raises(ConformanceError, match='no fresh fake registration'):
            seam._registered(known_tokens, flag, session, timeout=1)
    record, path = seam._registered(set(), '--session-id', launch['session_id'], timeout=5)
    assert record['token'] == launch['fake']['token'] and str(path) == launch['ledger']


def test_a_descendant_is_never_matched_as_the_launch_and_is_not_counted_as_a_fake(make):
    h = make()
    seam = h.seam
    gate = seam.workspace / 'gate'
    launch = seam.launch('escape', gate=str(gate))
    [descendant] = seam.descendants(launch)
    assert descendant['role'] == 'descendant' and descendant['ppid'] == launch['fake']['pid'] and seam.fakes_spawned() == 1
    assert seam.alive(descendant) and seam.alive(launch['fake']) and gate.as_posix() in seam.gates
    with pytest.raises(ConformanceError, match='no fresh fake registration'):
        seam._registered({launch['fake']['token']}, '--session-id', launch['session_id'], timeout=1)
    gate.write_bytes(b'')
    assert seam.exited(descendant, 30) and seam.exited(launch['fake'], 30)


# --- owner selection (R11) ------------------------------------------------------------------------------------

def test_owner_must_be_the_first_armed_chain_hop_equal_to_the_recorded_parent(make):
    seam = make().seam
    hop = {'pid': os.getpid(), 'lstart': 'Mon Jan  1 00:00:00 2001', 'command': 'x', 'ppid': 1, 'pgid': 1}
    for fake in ({'chain': [], 'ppid': 5}, {'chain': [{**hop, 'pid': 7}], 'ppid': 5}, {'chain': [hop], 'ppid': hop['pid']}):
        with pytest.raises(ConformanceError, match='fake parent not observed as the first armed chain hop'):
            seam._owner(fake)


def test_a_broken_chain_beyond_the_first_hop_is_accepted_and_recorded_verbatim(make, monkeypatch):
    h = make()
    h.engine.depth = 2
    blocked = set()
    real_probe = observer_module.probe
    monkeypatch.setattr(observer_module, 'probe', lambda pid: None if pid in blocked else real_probe(pid))
    h.engine.on_spawn = lambda proc: blocked.add(proc.pid)  # the outermost ancestor vanishes while the fake registers
    launch = h.seam.launch()
    assert launch['fake']['chain_completeness'] == 'broken' and len(launch['fake']['chain']) == 1
    assert launch['owner']['pid'] == launch['fake']['ppid'] != next(iter(blocked))
    assert h.seam.exited(launch['fake'], 60)
    assert h.seam.raw_evidence(launch)['chain_completeness'] == 'broken'


def test_a_break_at_the_first_hop_fails_the_launch(make, monkeypatch):
    h = make()
    blocked = set()
    real_probe = observer_module.probe
    monkeypatch.setattr(observer_module, 'probe', lambda pid: None if pid in blocked else real_probe(pid))
    h.engine.on_spawn = lambda proc: blocked.add(proc.pid)  # depth 1: the process that is the fake's parent cannot be probed
    with pytest.raises(ConformanceError, match='fake parent not observed as the first armed chain hop'):
        h.seam.launch()


# --- kill-owner, raw evidence, spawn watching -----------------------------------------------------------------

def test_kill_owner_signals_the_recorded_parent_through_the_fake(make):
    h = make()
    seam = h.seam
    launch = seam.launch('hold', gate=str(seam.workspace / 'never'), survive_eof=True)
    assert seam.kill_owner(launch) is True
    assert seam.exited(launch['owner'], 15) and seam.alive(launch['fake'])
    assert [r['event'] for r in fakekit.read_rows(launch['ledger']) if r['event'] == 'kill-owner'] == ['kill-owner']


def test_kill_owner_reports_false_after_the_parent_changed(make):
    h = make()
    seam = h.seam
    launch = seam.launch('hold', gate=str(seam.workspace / 'never'), survive_eof=True)
    h.engine.procs[0].kill()  # the owner (the fake's parent) dies; the fake is reparented
    assert seam.exited(launch['owner'], 15)
    deadline = time.monotonic() + 15
    while subprocess.run(['/bin/ps', '-o', 'ppid=', '-p', str(launch['fake']['pid'])], capture_output=True).stdout.strip() != b'1' and time.monotonic() < deadline:
        time.sleep(.1)
    assert seam.kill_owner(launch) is False


def test_raw_evidence_refuses_a_ledger_or_public_view_that_disagrees(make, tmp_path):
    h = make()
    seam = h.seam
    launch = seam.launch()
    assert seam.exited(launch['fake'], 60)
    seam.raw_evidence(launch, completed=True)
    with pytest.raises(ConformanceError, match='public prompt differs|lost the exact caller bytes'):
        seam.raw_evidence({**launch, 'prompt': launch['prompt'] + ' tampered'})
    with pytest.raises(ConformanceError, match='ledger session differs'):
        seam.raw_evidence({**launch, 'session_id': str(uuid.uuid4())})
    with pytest.raises(ConformanceError, match='does not match the observed fake'):
        seam.raw_evidence({**launch, 'fake': {**launch['fake'], 'pid': launch['fake']['pid'] + 1}})
    rows = Path(launch['ledger']).read_text().splitlines()
    unregistered = tmp_path / 'unregistered.jsonl'
    unregistered.write_text('\n'.join(r for r in rows if '"event": "registration"' not in r) + '\n')
    with pytest.raises(ConformanceError, match='lacks the observed registration'):
        seam.raw_evidence({**launch, 'ledger': str(unregistered)})
    incomplete = tmp_path / 'incomplete.jsonl'
    incomplete.write_text('\n'.join(r for r in rows if '"event": "exit"' not in r) + '\n')
    assert seam.raw_evidence({**launch, 'ledger': str(incomplete)})['ledger_complete'] is False
    with pytest.raises(ConformanceError, match='no exit entry'):
        seam.raw_evidence({**launch, 'ledger': str(incomplete)}, completed=True)
    partial = tmp_path / 'partial.jsonl'
    partial.write_bytes(Path(launch['ledger']).read_bytes() + b'{"event": "io", "dir')
    assert seam.raw_evidence({**launch, 'ledger': str(partial)})['trailing_bytes'] > 0
    with pytest.raises(ConformanceError, match='unterminated line'):
        seam.raw_evidence({**launch, 'ledger': str(partial)}, completed=True)
    h.engine.summary = 'something else'
    with pytest.raises(ConformanceError, match='public result differs'):
        seam.raw_evidence(launch, completed=True)


def test_watch_spawns_counts_agent_ledgers_and_registrations_but_reports_refused_argv_separately(make):
    h = make()
    seam = h.seam
    env = {'PATH': h.engine.env['PATH']}
    verify = seam.watch_spawns()
    subprocess.run([sys.executable, '-I', str(seam.launcher), '--nonsense'], env=env, capture_output=True, timeout=120)
    verify()
    check = seam.evidence['spawn_check']
    assert len(check['refused_argv_ledgers']) == 1 and check['fakes_before'] == check['fakes_after'] == 0 and check['new_agent_or_unclassified_ledgers'] == {}
    verify = seam.watch_spawns()
    seam.launch()
    with pytest.raises(ConformanceError, match='started an agent'):
        verify()
    assert seam.evidence['spawn_check']['fakes_after'] == 1


# --- cleanup proof --------------------------------------------------------------------------------------------

def test_cleanup_releases_gates_and_proves_every_armed_participant_exited(make):
    h = make()
    seam = h.seam
    launch = seam.launch('escape', gate=str(seam.workspace / 'never'), survive_eof=True)
    [descendant] = seam.descendants(launch)
    assert seam.alive(launch['fake']) and seam.alive(descendant) and seam.alive(launch['owner'])
    assert seam.close() is True
    h.closed = True
    cleanup = seam.evidence['cleanup']
    assert cleanup['left_alive'] == [] and cleanup['observer_failed'] is False and cleanup['watched'] >= 3
    assert (h.folder / 'observer.jsonl').is_file()
    assert seam.observer.exited(descendant['pid'], descendant['lstart']) and seam.observer.exited(launch['fake']['pid'], launch['fake']['lstart'])


def test_cleanup_proof_fails_when_a_participant_is_left_alive(make):
    h = make(lifetime=2)
    seam = h.seam
    stubborn = subprocess.Popen([sys.executable, '-I', '-c', NEVER_EXITS.format(repo=str(REPO), sock=seam.observer.socket_path,
                                                                                 nonce=seam.observer.nonce, role='descendant')])
    try:
        assert seam.observer.wait_for(lambda: seam.observer.records, 30)
        started = time.monotonic()
        assert seam.close() is False
        h.closed = True
        assert time.monotonic() - started < 30, 'cleanup must give up after the lifetime bound'
        cleanup = seam.evidence['cleanup']
        assert [item['pid'] for item in cleanup['left_alive']] == [stubborn.pid]
    finally:
        stubborn.kill()
        stubborn.wait()


def test_cleanup_proof_fails_when_the_observer_failed_or_the_engine_left_work_behind(make):
    h = make()
    seam = h.seam
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.connect(seam.observer.socket_path)
        conn.sendall(json.dumps({'nonce': 'x' * 32, 'role': 'fake', 'pid': os.getpid()}).encode() + b'\n')
        assert conn.recv(16) == b''
    assert seam.observer.wait_for(lambda: seam.observer.failed, 5)
    assert seam.close() is False and seam.evidence['cleanup']['observer_failed'] is True
    h.closed = True
    folder = Path(tempfile.mkdtemp(prefix='cfs'))
    try:
        other = make(folder=folder)
        other.engine.clean_result = False
        assert other.seam.close() is False
        assert other.seam.evidence['cleanup']['left_alive'] == [] and other.seam.evidence['cleanup']['observer_failed'] is False
        other.closed = True
    finally:
        shutil.rmtree(folder, ignore_errors=True)


# --- runner and cases wiring ----------------------------------------------------------------------------------

class UnprovedSeam:
    name, acceptance_eligible, extra_env_keys, workspace = 'path-fake', True, [], Path('/nonexistent')

    def __init__(self, engine, folder):
        self.operations, self.evidence = [], {'seam': 'path-fake'}

    def close(self):
        return False


def stub_target():
    return target.Target(['unused'], {'kind': 'source'}, lambda: None, '0.0.14')


def test_a_case_on_the_path_fake_seam_fails_when_cleanup_is_unproved(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'PathFakeSeam', UnprovedSeam)
    monkeypatch.setattr(runner, 'OBSERVER_AVAILABLE', True)
    entry = runner.run_case('complete', lambda c: {'ok': 1}, SimpleNamespace(seam='path-fake'), tmp_path, stub_target())
    assert entry['status'] == 'failed' and entry['failure']['message'] == 'participant cleanup unproved'
    assert entry['cleanup'] == {'ok': False} and entry['seam_name'] == 'path-fake' and entry['seam_evidence'] == {'seam': 'path-fake'}
    assert entry['seam_acceptance_eligible'] is True and entry['extra_env_keys'] == []


def test_the_unscored_resume_probe_is_reported_beside_the_cases_and_fails_on_unproved_cleanup(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'PathFakeSeam', UnprovedSeam)
    entry = runner.run_probe(lambda c: {'probe': 1}, SimpleNamespace(seam='path-fake'), tmp_path, stub_target())
    assert entry['scored'] is False and entry['status'] == 'failed' and entry['failure']['message'] == 'participant cleanup unproved'
    assert 'resume_probe' not in cases.CASES and len(cases.CASES) == 6 and callable(cases.resume_probe)
    assert runner.assess([{'name': n, 'status': 'passed'} for n in cases.CASES], None)['complete'] is True


@pytest.mark.parametrize('argv', [
    ['--engine-bin', '/x', '--engine-sha256', 'a' * 64, '--seam', 'python-injection'],
    ['--engine-bin', '/x', '--engine-sha256', 'a' * 64, '--seam', 'none', '--resume-probe'],
    ['--source-repo', '/r', '--commit', 'c' * 40, '--python', '/p', '--seam', 'python-injection', '--resume-probe'],
    ['--seam', 'path-fake'],
])
def test_runner_argument_validation_for_the_path_fake_seam(argv, tmp_path):
    with pytest.raises(SystemExit) as raised:
        runner.main([*argv, '--product-version', '0.0.14', '--manifest', 'm', '--manifest-sha256', 'a' * 64, '--output', str(tmp_path / 'out')])
    assert raised.value.code == 2 and not (tmp_path / 'out').exists()
