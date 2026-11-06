"""Self-checks for the generic lifecycle adapter: target identity, closed environment, OS observer and case gating."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace

import pytest

from conformance.lifecycle import cases, observer as observer_module, participant, runner, seams, target
from conformance.oracles import ConformanceError

REPO = Path(__file__).resolve().parents[2]
REAL = [sys.executable, '-I', '-B', '-m', 'harflow_mcp.cli']


@pytest.fixture
def short_dir():
    path = Path(tempfile.mkdtemp(prefix='lc', dir=REPO))
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def observer(short_dir):
    obs = observer_module.OsObserver(short_dir / 'o').start()
    yield obs
    obs.close()


# --- target identity and the closed environment ---------------------------------------------------------

def test_binary_target_is_verified_before_every_use(tmp_path):
    binary = tmp_path / 'engine'
    binary.write_bytes(b'#!/bin/sh\nexit 0\n')
    pinned = target.digest(binary.read_bytes())
    pinned_target = target.binary_target(binary, pinned)
    assert pinned_target.identity['release_binary'] is True and pinned_target.command == [str(binary)]
    binary.write_bytes(b'#!/bin/sh\nexit 1\n')
    with pytest.raises(ConformanceError, match='pinned digest'):
        pinned_target.verify()
    with pytest.raises(ConformanceError):
        target.binary_target('relative/engine', pinned)
    with pytest.raises(ConformanceError):
        target.binary_target(binary, 'abc')


def test_source_launcher_is_never_the_release_binary():
    launcher = target.source_target(['x'], {'kind': 'source'}, lambda: None)
    assert launcher.identity['release_binary'] is False


@pytest.mark.parametrize('key', ['HARFLOW_BACKEND', 'HARFLOW_TASK_ID', 'S08_PYTHON_SHIM', 'PYTHONPATH', 'HARFLOW_ENGINE_BIN', 'ANTHROPIC_API_KEY'])
def test_any_environment_selector_is_refused(tmp_path, key):
    env = {'PATH': '/usr/bin:/bin', 'HARFLOW_STATE_ROOT': str(tmp_path / 's'), key: '1'}
    with pytest.raises(ConformanceError, match='allowlist'):
        target.Engine(target.Target(REAL, {}, lambda: None), env, tmp_path, tmp_path / 'ops')
    # A seam must declare extra keys explicitly; that is what makes its runs ineligible for acceptance.
    engine = target.Engine(target.Target(REAL, {}, lambda: None), env, tmp_path, tmp_path / 'ops2', extra_env_keys=(key,))
    assert key in engine.env


def test_engine_refuses_commands_outside_the_public_set(tmp_path):
    env = {'PATH': '/usr/bin:/bin', 'HARFLOW_STATE_ROOT': str(tmp_path / 's')}
    engine = target.Engine(target.Target(REAL, {}, lambda: None), env, tmp_path, tmp_path / 'ops')
    for argv in (['takeover', 'x', '--json'], ['run', 'x'], []):
        with pytest.raises(ConformanceError):
            engine.command(argv)


# --- the OS observer and the ancestry rendezvous --------------------------------------------------------

CHILD = """
import json, os, sys, time
sys.path.insert(0, {repo!r})
from conformance.lifecycle import participant
ack = participant.register({sock!r}, {nonce!r})
mode = sys.argv[1]
if mode == 'kill-parent':
    open({out!r}, 'w').write(json.dumps({{'sent': participant.kill_parent(ack)}}))
elif mode == 'forged':
    open({out!r}, 'w').write(json.dumps({{'sent': participant.kill_parent(dict(ack, ppid=ack['ppid'] + 1))}}))
else:
    time.sleep(30)
"""
PARENT = """
import subprocess, sys
child = subprocess.Popen([sys.executable, '-c', open({script!r}).read(), sys.argv[1]])
child.wait()
"""


def spawn_tree(obs, short_dir, mode):
    script = short_dir / 'child.py'
    script.write_text(CHILD.format(repo=str(REPO), sock=obs.socket_path, nonce=obs.nonce, out=str(short_dir / 'out.json')))
    parent_script = short_dir / 'parent.py'
    parent_script.write_text(PARENT.format(script=str(script)))
    return subprocess.Popen([sys.executable, str(parent_script), mode], start_new_session=True)


def test_registration_records_the_kernel_pid_and_ancestry_before_acknowledging(observer, short_dir):
    parent = spawn_tree(observer, short_dir, 'wait')
    record = None
    try:
        record = observer.wait_registered(timeout=10)
        assert record is not None and record['role'] == 'fake'
        assert record['ppid'] == parent.pid and [hop['pid'] for hop in record['chain']] == [parent.pid]
        assert record['chain'][0]['lstart'] and 'parent.py' in record['chain'][0]['command']
        assert not observer.exited(record['pid'], record['lstart']) and not observer.exited(parent.pid, record['chain'][0]['lstart'])
        parent.kill()
        parent.wait()
        assert observer.wait_exited(parent.pid, record['chain'][0]['lstart'], timeout=5)
        assert not observer.exited(record['pid'], record['lstart']), 'the registered child must outlive its killed parent'
    finally:
        parent.kill()
        # The sleeping child was reparented when its parent died; it is this test's own process.
        if record is not None and not observer.exited(record['pid'], record['lstart']):
            os.kill(record['pid'], 9)


def test_a_participant_can_end_only_the_parent_the_observer_recorded(observer, short_dir):
    parent = spawn_tree(observer, short_dir, 'kill-parent')
    record = observer.wait_registered(timeout=10)
    assert record is not None
    ancestor = record['chain'][0]
    assert observer.wait_exited(ancestor['pid'], ancestor['lstart'], timeout=5), 'owner exit was not observed through the armed ancestor watch'
    assert json.loads((short_dir / 'out.json').read_text()) == {'sent': True}
    assert parent.wait(timeout=5) != 0  # killed by signal


def test_a_forged_parent_identity_signals_nothing(observer, short_dir):
    parent = spawn_tree(observer, short_dir, 'forged')
    try:
        assert observer.wait_registered(timeout=10) is not None
        assert parent.wait(timeout=10) == 0  # the parent exited normally because the child sent no signal
        assert json.loads((short_dir / 'out.json').read_text()) == {'sent': False}
    finally:
        parent.kill()


def test_registration_with_a_wrong_nonce_or_pid_is_refused(observer, short_dir):
    import socket
    for request in ({'nonce': 'x' * 32, 'role': 'fake', 'pid': os.getpid()}, {'nonce': observer.nonce, 'role': 'fake', 'pid': os.getpid() + 1},
                    {'nonce': observer.nonce, 'role': 'engine', 'pid': os.getpid()}):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(3)
            conn.connect(observer.socket_path)
            conn.sendall(json.dumps(request).encode() + b'\n')
            assert conn.recv(64) == b''
    assert observer.wait_for(lambda: observer.failed, timeout=3)
    assert observer.records == []


# --- case gating ---------------------------------------------------------------------------------------

class StubSeam(seams.NoSeam):
    def __init__(self, tmp_path, **overrides):
        self.workspace = tmp_path
        self.operations = []
        self.engine = None
        self.__dict__.update(overrides)


def refuse_everything(seam):
    def call(argv, *, refused=False):
        doc = {'error': {'code': 'invalid_params', 'message': 'no'}}
        seam.operations.append({'argv': argv})
        if not refused:
            raise ConformanceError('unexpected success')
        return doc
    return call


def test_refusals_pass_only_when_nothing_is_spawned_or_recorded(tmp_path):
    seam = StubSeam(tmp_path)
    seam.call = refuse_everything(seam)
    seam.task_ids = lambda: []
    assert cases.refusals(cases.Lifecycle(seam))['refusals'] >= 3
    for name, override, message in (('a task record', {'task_ids': lambda: ['t']}, 'task record'),
                                    ('a spawned fake', {'fakes_spawned': lambda: 1}, 'spawned a fake'),
                                    ('fake evidence', {'ledgers_written': lambda: 1}, 'fake evidence')):
        broken = StubSeam(tmp_path)
        broken.call = refuse_everything(broken)
        broken.task_ids = lambda: []
        broken.__dict__.update(override)
        with pytest.raises(ConformanceError, match=message):
            cases.refusals(cases.Lifecycle(broken))


def test_a_refusal_that_succeeds_fails_the_case(tmp_path):
    seam = StubSeam(tmp_path)

    def accepting(argv, *, refused=False):
        if refused:
            raise ConformanceError('unexpected public refusal/success: ' + argv[0])
        return {}
    seam.call = accepting
    with pytest.raises(ConformanceError):
        cases.refusals(cases.Lifecycle(seam))


@pytest.mark.parametrize('name', ['complete', 'nonzero', 'detached-resume', 'cancel', 'owner-death'])
def test_agent_cases_are_blocked_not_passed_without_a_seam(tmp_path, name):
    seam = seams.NoSeam(None, tmp_path)
    with pytest.raises(seams.SeamRequired):
        cases.CASES[name](cases.Lifecycle(seam))


def test_acceptance_rules():
    passed = [{'name': n, 'status': 'passed'} for n in cases.CASES]
    assert runner.assess(passed, None) == {'ok': True, 'failed': [], 'blocked': [], 'complete': True}
    blocked = [dict(e, status='blocked') if e['name'] != 'refusals' else e for e in passed]
    summary = runner.assess(blocked, None)
    assert summary['ok'] and not summary['complete'] and len(summary['blocked']) == 5
    failed = [dict(passed[0], status='failed')] + passed[1:]
    assert not runner.assess(failed, None)['ok']
    assert not runner.assess(passed, ['refusals'])['complete'] and not runner.assess([], None)['ok']


def test_refusals_run_end_to_end_against_python_source_without_a_seam(tmp_path):
    launcher = target.Target(REAL, {'kind': 'source', 'release_binary': False}, lambda: None)
    from conformance.runner import isolated_environment
    folder = tmp_path / 'case'
    folder.mkdir()
    env = isolated_environment(folder)
    seam = seams.NoSeam(target.Engine(launcher, env, folder / 'workspace', folder / 'ops'), folder / 'workspace')
    evidence = cases.refusals(cases.Lifecycle(seam))
    assert evidence['refusals'] == 4
    assert [op['exit_code'] for op in seam.operations][:3] == [1, 1, 1]
