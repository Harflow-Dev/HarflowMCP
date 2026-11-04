"""Exercise only subprocesses created by these tests; no engine or provider state."""
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import time

import pytest

from conformance.process import run_process

FAKE = Path(__file__).resolve().parents[1] / 'fake_harness.py'


def run(tmp_path, mode, **kwargs):
    observation = tmp_path / 'observation.json'
    argv = [sys.executable, str(FAKE), mode, '--observation', str(observation)]
    result = run_process(argv, cwd=tmp_path, env={'PATH': '/usr/bin:/bin'},
                         artifact_dir=tmp_path / 'capture', nonce='independent-test-nonce', **kwargs)
    return result, observation, argv


def assert_captures(result):
    for row in result['streams'].values():
        raw = Path(row['path']).read_bytes()
        assert len(raw) == row['captured_bytes']
        assert hashlib.sha256(raw).hexdigest() == row['sha256']
        assert row['bytes_seen'] >= len(raw)


def assert_reaped(result):
    with pytest.raises(ChildProcessError):
        os.waitpid(result['pid'], os.WNOHANG)


def test_parent_observed_success_identity_capture_and_exit(tmp_path):
    result, observation, argv = run(tmp_path, 'success')
    corroboration = json.loads(observation.read_bytes())
    assert result['argv'] == argv
    assert result['cwd'] == str(tmp_path)
    assert result['pid'] != os.getpid()
    assert result['pgid'] == result['pid'] == corroboration['pid']
    assert result['nonce'] == corroboration['nonce'] == 'independent-test-nonce'
    assert corroboration['argv'] == argv[2:]
    assert result['returncode'] == 0 and result['ok'] is True
    assert result['pipes_drained'] is True
    assert b'deterministic fake output' in Path(result['streams']['stdout']['path']).read_bytes()
    assert Path(result['streams']['stderr']['path']).read_bytes() == b''
    assert_captures(result)
    assert_reaped(result)


def test_observed_nonzero_cannot_be_success(tmp_path):
    result, _, _ = run(tmp_path, 'nonzero')
    assert result['returncode'] == 7
    assert result['ok'] is False
    assert_captures(result)
    assert_reaped(result)


def test_hang_is_bounded_and_owned_leader_is_reaped(tmp_path):
    start = time.monotonic()
    result, _, _ = run(tmp_path, 'hang', timeout=0.3)
    assert time.monotonic() - start < 4
    assert result['timed_out'] and not result['ok']
    assert result['returncode'] == -signal.SIGKILL
    assert result['pipes_drained']
    assert_reaped(result)


@pytest.mark.parametrize('fd', [1, 2])
def test_stdout_and_stderr_have_independent_hard_bounds(tmp_path, fd):
    program = 'import os\nwhile True: os.write(%d, b"x" * 4096)' % fd
    result = run_process([sys.executable, '-c', program], cwd=tmp_path, env={},
                         artifact_dir=tmp_path / 'capture', timeout=2, capture_limit=8192)
    assert result['capture_exceeded'] and not result['ok']
    stream = result['streams']['stdout' if fd == 1 else 'stderr']
    assert stream['bytes_seen'] > 8192
    assert stream['captured_bytes'] == 8192
    assert_captures(result)
    assert_reaped(result)


def test_pipe_holder_is_killed_before_leader_reaped(tmp_path):
    start = time.monotonic()
    result, _, _ = run(tmp_path, 'pipe-holder', timeout=1)
    assert time.monotonic() - start < 4
    assert result['pipes_drained']
    assert result['cleanup'] == 'group_signalled'
    assert_reaped(result)


def test_post_spawn_setup_exception_still_reaps_owned_child(tmp_path, monkeypatch):
    import conformance.process as supervisor
    def fail_setup(*args):
        raise OSError('injected owned pipe setup failure')
    monkeypatch.setattr(supervisor.os, 'set_blocking', fail_setup)
    result = run_process([sys.executable, '-c', 'import signal; signal.pause()'],
                         cwd=tmp_path, env={}, artifact_dir=tmp_path / 'capture', timeout=1)
    spawn = json.loads((tmp_path / 'capture/spawn.json').read_bytes())
    assert spawn['pid'] == result['pid']
    assert result['ok'] is False
    assert 'injected owned pipe setup failure' in result['error']
    assert_reaped(result)


def test_all_group_signals_precede_leader_reap(tmp_path, monkeypatch):
    import conformance.process as supervisor
    events = []
    actual_killpg = supervisor.os.killpg
    actual_wait = supervisor.subprocess.Popen.wait
    def signal_group(*args, **kwargs):
        events.append('signal')
        return actual_killpg(*args, **kwargs)
    def reap(*args, **kwargs):
        events.append('reap')
        return actual_wait(*args, **kwargs)
    monkeypatch.setattr(supervisor.os, 'killpg', signal_group)
    monkeypatch.setattr(supervisor.subprocess.Popen, 'wait', reap)
    result, _, _ = run(tmp_path, 'hang', timeout=0.2)
    assert result['ok'] is False
    assert 'signal' in events and 'reap' in events
    assert max(i for i,x in enumerate(events) if x == 'signal') < min(i for i,x in enumerate(events) if x == 'reap')
    assert_reaped(result)


def test_cleanup_failure_never_signals_after_leader_reap(tmp_path, monkeypatch):
    import conformance.process as supervisor
    events = []
    actual_wait = supervisor.subprocess.Popen.wait
    def refused_signal(*args, **kwargs):
        assert 'reap' not in events, 'process group signalled after owned leader was reaped'
        events.append('signal')
        raise PermissionError('injected signal refusal')
    def reap(*args, **kwargs):
        value = actual_wait(*args, **kwargs)
        events.append('reap')
        return value
    monkeypatch.setattr(supervisor, '_quiescent_group', lambda pgid: False)
    monkeypatch.setattr(supervisor.os, 'killpg', refused_signal)
    monkeypatch.setattr(supervisor.subprocess.Popen, 'wait', reap)
    result, _, _ = run(tmp_path, 'success', timeout=1)
    assert not result['ok']
    assert_reaped(result)


@pytest.mark.parametrize('boundary', ['supervisor', 'ps-helper'])
def test_selector_allocation_failure_cannot_create_child(tmp_path, monkeypatch, boundary):
    import conformance.process as supervisor
    launches = []
    def fail_selector():
        raise OSError('injected selector allocation failure')
    def forbidden_spawn(*args, **kwargs):
        launches.append(args)
        raise AssertionError('child created before selector allocation')
    monkeypatch.setattr(supervisor.selectors, 'DefaultSelector', fail_selector)
    monkeypatch.setattr(supervisor.subprocess, 'Popen', forbidden_spawn)
    with pytest.raises(OSError, match='injected selector allocation failure'):
        if boundary == 'supervisor':
            run_process([sys.executable, '-c', 'raise SystemExit(0)'], cwd=tmp_path, env={},
                        artifact_dir=tmp_path / 'capture')
        else:
            supervisor._quiescent_group(os.getpid())
    assert launches == []
