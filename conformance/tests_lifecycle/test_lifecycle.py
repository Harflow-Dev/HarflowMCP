"""Self-checks for the generic lifecycle adapter: target identity, closed environment, OS observer and case gating."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace

import pytest

from conformance.lifecycle import cases, observer as observer_module, runner, seams, target
from conformance.oracles import ConformanceError, required_capabilities, validate_reply

REPO = Path(__file__).resolve().parents[2]
REAL = [sys.executable, '-I', '-B', '-m', 'harflow_mcp.cli']
VERSION = '0.0.11'
darwin = pytest.mark.skipif(sys.platform != 'darwin', reason='the lifecycle observer needs Darwin kqueue and LOCAL_PEERPID; Linux lifecycle is unavailable')


def make_target(identity=None):
    return target.Target(REAL, identity or {'kind': 'source'}, lambda: None, VERSION)


@pytest.fixture
def short_dir():
    path = Path(tempfile.mkdtemp(prefix='lc'))  # a system temp directory, never the repository
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def observer(short_dir):
    obs = observer_module.OsObserver(short_dir / 'o').start()
    yield obs
    obs.close()


# --- target identity, product line and the closed environment ----------------------------------------------

def test_binary_target_is_verified_before_every_use(tmp_path):
    binary = tmp_path / 'engine'
    binary.write_bytes(b'#!/bin/sh\nexit 0\n')
    pinned = target.digest(binary.read_bytes())
    pinned_target = target.binary_target(binary, pinned, VERSION)
    assert pinned_target.identity['pinned_binary'] is True and pinned_target.identity['product_version'] == VERSION
    assert pinned_target.command == [str(binary)]
    binary.write_bytes(b'#!/bin/sh\nexit 1\n')
    with pytest.raises(ConformanceError, match='pinned digest'):
        pinned_target.verify()
    for bad in (('relative/engine', pinned), (binary, 'abc')):
        with pytest.raises(ConformanceError):
            target.binary_target(*bad, VERSION)


def test_release_match_needs_independent_digest_and_commit(tmp_path):
    identity = {'pinned_binary': True, 'sha256': 'a' * 64}
    assert target.release_match(identity, 'a' * 64, 'c' * 40)
    assert not target.release_match(identity, 'b' * 64, 'c' * 40)
    assert not target.release_match(identity, 'a' * 64, None)
    assert not target.release_match(identity, None, 'c' * 40)
    assert not target.release_match({'pinned_binary': False, 'sha256': 'a' * 64}, 'a' * 64, 'c' * 40)


def test_source_launcher_is_never_a_pinned_binary():
    assert target.source_target(['x'], {'kind': 'source'}, lambda: None, VERSION).identity['pinned_binary'] is False


@pytest.mark.parametrize('key', ['HARFLOW_BACKEND', 'HARFLOW_TASK_ID', 'S08_PYTHON_SHIM', 'PYTHONPATH', 'HARFLOW_ENGINE_BIN', 'ANTHROPIC_API_KEY'])
def test_any_environment_selector_is_refused(tmp_path, key):
    env = {'PATH': '/usr/bin:/bin', 'HARFLOW_STATE_ROOT': str(tmp_path / 's'), key: '1'}
    with pytest.raises(ConformanceError, match='allowlist'):
        target.Engine(make_target(), env, tmp_path, tmp_path / 'ops')
    engine = target.Engine(make_target(), env, tmp_path, tmp_path / 'ops2', extra_env_keys=(key,))
    assert key in engine.env and engine.extra_env_keys == (key,)


def test_engine_refuses_commands_outside_the_public_set(tmp_path):
    env = {'PATH': '/usr/bin:/bin', 'HARFLOW_STATE_ROOT': str(tmp_path / 's')}
    engine = target.Engine(make_target(), env, tmp_path, tmp_path / 'ops')
    for argv in (['takeover', 'x', '--json'], ['run', 'x'], []):
        with pytest.raises(ConformanceError):
            engine.command(argv)


def test_product_line_is_part_of_target_identity_not_a_hidden_pin(tmp_path):
    from conformance.runner import isolated_environment
    folder = tmp_path / 'case'
    folder.mkdir()
    wrong = target.Target(REAL, {'kind': 'source'}, lambda: None, '9.9.9')
    engine = target.Engine(wrong, isolated_environment(folder), folder / 'workspace', folder / 'ops')
    with pytest.raises(ConformanceError):
        engine.command(['list', '--json'])


def test_shared_reply_validation_and_capabilities():
    assert required_capabilities(['task-events', 'x']) == ('ctl.v1', 'task-event-page.v1')
    assert required_capabilities(['approval-respond', '--monitor']) == ('ctl.v1', 'caller-approvals.monitor-respond.v1')
    envelope = {'product': 'harflow', 'format': 'harflow.ctl', 'v': 1, 'product_version': '0.0.11', 'result': {'events': []}}
    assert validate_reply(['task-events', 'x'], envelope) is envelope
    with pytest.raises(ConformanceError):
        validate_reply(['task-events', 'x'], {**envelope, 'result': {}})


# --- the OS observer and the ancestry rendezvous (Darwin) -------------------------------------------------

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


@darwin
def test_registration_records_the_kernel_pid_and_ancestry_before_acknowledging(observer, short_dir):
    parent = spawn_tree(observer, short_dir, 'wait')
    record = None
    try:
        record = observer.wait_registered(timeout=10)
        assert record is not None and record['role'] == 'fake' and record['chain_completeness'] == 'complete'
        assert record['ppid'] == parent.pid and [hop['pid'] for hop in record['chain']] == [parent.pid]
        assert record['chain'][0]['lstart'] and 'parent.py' in record['chain'][0]['command']
        assert not observer.exited(record['pid'], record['lstart']) and not observer.exited(parent.pid, record['chain'][0]['lstart'])
        parent.kill()
        parent.wait()
        assert observer.wait_exited(parent.pid, record['chain'][0]['lstart'], timeout=5)
        assert not observer.exited(record['pid'], record['lstart']), 'the registered child must outlive its killed parent'
    finally:
        parent.kill()
        if record is not None and not observer.exited(record['pid'], record['lstart']):
            os.kill(record['pid'], 9)  # the sleeping child was reparented; it is this test's own process


@darwin
def test_a_participant_can_end_only_the_parent_the_observer_recorded(observer, short_dir):
    parent = spawn_tree(observer, short_dir, 'kill-parent')
    record = observer.wait_registered(timeout=10)
    assert record is not None
    ancestor = record['chain'][0]
    assert observer.wait_exited(ancestor['pid'], ancestor['lstart'], timeout=5), 'owner exit was not observed through the armed ancestor watch'
    assert json.loads((short_dir / 'out.json').read_text()) == {'sent': True}
    assert parent.wait(timeout=5) != 0


@darwin
def test_a_forged_parent_identity_signals_nothing(observer, short_dir):
    parent = spawn_tree(observer, short_dir, 'forged')
    try:
        assert observer.wait_registered(timeout=10) is not None
        assert parent.wait(timeout=10) == 0
        assert json.loads((short_dir / 'out.json').read_text()) == {'sent': False}
    finally:
        parent.kill()


@darwin
def test_registration_with_a_wrong_nonce_or_pid_is_refused(observer):
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


@darwin
def test_only_esrch_means_already_exited_and_it_is_logged(observer, short_dir):
    dead = subprocess.Popen([sys.executable, '-c', 'pass'])
    dead.wait()
    observer._arm(dead.pid, 'Thu Jan  1 00:00:00 1970')  # gone before the watch: ESRCH
    assert observer.exited(dead.pid, 'Thu Jan  1 00:00:00 1970')
    assert 'esrch-at-arm' in (short_dir / 'o' / 'observer.jsonl').read_text()
    assert not observer.failed


def test_chain_truncation_and_breakage_are_recorded(monkeypatch):
    rows = {n: {'ppid': n + 1, 'pgid': 1, 'lstart': 'x', 'command': 'c'} for n in range(2, 60)}
    monkeypatch.setattr(observer_module, 'probe', lambda pid: rows.get(pid))
    _, chain, completeness = observer_module.chain_of(2, stop_pid=10_000)
    assert completeness == 'truncated' and len(chain) == observer_module.MAX_CHAIN
    short = {2: rows[2], 3: rows[3]}
    monkeypatch.setattr(observer_module, 'probe', lambda pid: short.get(pid))
    _, chain, completeness = observer_module.chain_of(2, stop_pid=10_000)
    assert completeness == 'broken' and [h['pid'] for h in chain] == [3]
    reached = {2: {'ppid': 1, 'pgid': 1, 'lstart': 'x', 'command': 'c'}}
    monkeypatch.setattr(observer_module, 'probe', lambda pid: reached.get(pid))
    assert observer_module.chain_of(2, stop_pid=10_000)[2] == 'complete'


def test_participant_lifetime_bound_ends_a_forgotten_fake():
    code = f"import sys; sys.path.insert(0, {str(REPO)!r}); from conformance.lifecycle import participant; participant.bound_lifetime(1); import time; time.sleep(30)"
    done = subprocess.run([sys.executable, '-c', code], timeout=15)
    assert done.returncode == 70


def test_process_watch_sees_a_lingering_process_naming_a_marker(tmp_path):
    marker = f'marker-{os.getpid()}-{tmp_path.name}'
    watch = observer_module.ProcessWatch([marker])
    assert watch.new_matching() == []
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)', marker])
    try:
        found = watch.new_matching()
        assert [pid for pid, _ in found] == [child.pid]
    finally:
        child.kill()
        child.wait()


# --- case gating --------------------------------------------------------------------------------------------

class StubSeam(seams.NoSeam):
    def __init__(self, tmp_path, verify=None, **overrides):
        self.workspace, self.operations, self.engine, self.evidence, self.markers = tmp_path, [], None, {}, ()
        self._verify = verify or (lambda: None)
        self.__dict__.update(overrides)

    def watch_spawns(self):
        return self._verify


def refuse_everything(seam):
    def call(argv, *, refused=False):
        seam.operations.append({'argv': argv})
        if not refused:
            raise ConformanceError('unexpected success')
        return {'error': {'code': 'invalid_params', 'message': 'no'}}
    return call


def test_refusals_pass_only_with_independent_evidence_and_no_side_effects(tmp_path):
    seam = StubSeam(tmp_path)
    seam.call, seam.task_ids = refuse_everything(seam), lambda: []
    assert cases.refusals(cases.Lifecycle(seam))['refusals'] >= 3
    leaked = StubSeam(tmp_path)
    leaked.call, leaked.task_ids = refuse_everything(leaked), lambda: ['t']
    with pytest.raises(ConformanceError, match='task record'):
        cases.refusals(cases.Lifecycle(leaked))

    def spawned():
        raise ConformanceError('a refused request left processes running')
    started = StubSeam(tmp_path, verify=spawned)
    started.call, started.task_ids = refuse_everything(started), lambda: []
    with pytest.raises(ConformanceError, match='left processes'):
        cases.refusals(cases.Lifecycle(started))


def test_unreadable_process_evidence_withholds_the_refusal_pass(tmp_path, monkeypatch):
    def unreadable(*_):
        raise RuntimeError('process table unreadable or over its bound')
    monkeypatch.setattr(observer_module, 'process_table', unreadable)
    seam = seams.NoSeam(None, tmp_path)
    with pytest.raises(RuntimeError, match='unreadable'):
        seam.watch_spawns()


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
    with pytest.raises(seams.SeamRequired):
        cases.CASES[name](cases.Lifecycle(seams.NoSeam(None, tmp_path)))


def test_noseam_close_reports_real_cleanup_evidence(tmp_path):
    from conformance.runner import isolated_environment
    folder = tmp_path / 'c'
    folder.mkdir()
    engine = target.Engine(make_target(), isolated_environment(folder), folder / 'workspace', folder / 'ops')
    assert seams.NoSeam(engine, folder / 'workspace').close() is True
    engine.records.append({'pipes_drained': True, 'cleanup': 'unknown'})
    assert seams.NoSeam(engine, folder / 'workspace').close() is False


def test_cleanup_is_recorded_per_case_and_failure_is_not_masked(tmp_path, monkeypatch):
    def boom(self):
        raise RuntimeError('cleanup exploded')
    monkeypatch.setattr(seams.NoSeam, 'close', boom)
    args = SimpleNamespace(seam='none')
    entry = runner.run_case('refusals', lambda c: {'ok': 1}, args, tmp_path, make_target())
    assert entry['status'] == 'failed' and entry['cleanup']['ok'] is False and 'exploded' in entry['cleanup']['error']
    assert entry['extra_env_keys'] == [] and entry['seam_acceptance_eligible'] is True


def test_a_failed_constructor_is_a_recorded_failure(tmp_path, monkeypatch):
    def broken(*_args, **_kwargs):
        raise RuntimeError('constructor failed')
    monkeypatch.setattr(runner, 'PythonInjectionSeam', broken)
    monkeypatch.setattr(runner, 'OBSERVER_AVAILABLE', True)
    entry = runner.run_case('refusals', lambda c: {}, SimpleNamespace(seam='python-injection', source_repo='x', commit='y', python='z'), tmp_path, None)
    assert entry['status'] == 'failed' and 'constructor failed' in entry['failure']['message']


def test_unavailable_platform_is_a_failure_not_a_pass(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'OBSERVER_AVAILABLE', False)
    entry = runner.run_case('refusals', lambda c: {}, SimpleNamespace(seam='python-injection', source_repo='x', commit='y', python='z'), tmp_path, None)
    assert entry['status'] == 'failed' and 'unavailable on this platform' in entry['failure']['message']


def test_vocabulary_distinguishes_no_failures_all_passed_complete_and_subsets():
    passed = [{'name': n, 'status': 'passed'} for n in cases.CASES]
    assert runner.assess(passed, None) == {'no_failures': True, 'all_passed': True, 'failed': [], 'blocked': [], 'subset': False, 'complete': True}
    blocked = [dict(e, status='blocked') if e['name'] != 'refusals' else e for e in passed]
    summary = runner.assess(blocked, None)
    assert summary['no_failures'] and not summary['all_passed'] and not summary['complete'] and len(summary['blocked']) == 5
    subset = runner.assess(passed[:1], ['refusals'])
    assert subset['all_passed'] and subset['subset'] and not subset['complete']
    failed = [dict(passed[0], status='failed')] + passed[1:]
    assert not runner.assess(failed, None)['no_failures'] and not runner.assess([], None)['no_failures']


def test_refusals_run_end_to_end_against_python_source_without_a_seam(tmp_path):
    from conformance.runner import isolated_environment
    folder = tmp_path / 'case'
    folder.mkdir()
    env = isolated_environment(folder)
    seam = seams.NoSeam(target.Engine(make_target(), env, folder / 'workspace', folder / 'ops'), folder / 'workspace', [str(folder)])
    evidence = cases.refusals(cases.Lifecycle(seam))
    assert evidence['refusals'] == 4 and evidence['spawn_check']['new_matching'] == []
    assert [op['exit_code'] for op in seam.operations][:3] == [1, 1, 1]
