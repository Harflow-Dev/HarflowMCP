from dataclasses import replace
import pytest
from polybridge import backends
from polybridge.backends.codex import EXTRA_PATHS_VERSION, EXTRA_PATH_PAIRS, UnsafeInvocationError
from polybridge.backends.base import Invocation, UnsupportedCapability
from polybridge.extra_paths import normalize_paths


def build(backend, repo, freedom='write_in_repo', network=None, resume=False):
    method = backend.build_resume_argv if resume else backend.build_start_argv
    return method('hello', repo=repo, freedom=freedom, network=network, session_id='session' if resume else None,
                  model=None, max_turns=None, reasoning_effort=None)


@pytest.fixture
def qualified(monkeypatch, fake_backend_clis):
    # Exercise the real executable fingerprint without requiring an installed CLI.
    # The version probe is stubbed below; invoking the stand-in would fail loudly.
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'darwin')
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: EXTRA_PATHS_VERSION)
    return backends.get('codex')


@pytest.mark.parametrize('freedom', ['write_in_repo', 'publish'])
@pytest.mark.parametrize('network', [None, False, True])
@pytest.mark.parametrize('resume', [False, True])
def test_bounded_shapes(qualified, tmp_path, freedom, network, resume):
    grants = normalize_paths([str(tmp_path)])
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path, freedom, network, resume), grants, freedom, network)
    qualified.assert_extra_writable_paths(invocation, grants, freedom, network)
    assert all(invocation.argv.count(pair) == 1 for pair in EXTRA_PATH_PAIRS)
    assert invocation.argv.index('--add-dir') < invocation.argv.index('--')
    if resume:
        assert invocation.argv.index('--add-dir') < invocation.argv.index('resume')
    enforcement = qualified.extra_path_enforcement(qualified.enforcement(freedom, network), grants, tmp_path)
    assert enforcement.writable_roots == ('the working directory', grants[0].path)
    assert enforcement.as_dict()['extra_path_certification'] == qualified.extra_path_support()['certification']


@pytest.mark.parametrize('name', ['claude', 'opencode', 'vibe', 'antigravity'])
def test_other_harnesses_reject(name, tmp_path):
    backend = backends.get(name)
    grants = normalize_paths([str(tmp_path)])
    with pytest.raises(UnsupportedCapability):
        backend.with_extra_writable_paths(Invocation([]), grants, 'write_in_repo', False)
    with pytest.raises(UnsupportedCapability):
        backend.assert_extra_writable_paths(Invocation([]), grants, 'write_in_repo', False)
    with pytest.raises(UnsupportedCapability):
        backend.extra_path_enforcement(backend.enforcement('write_in_repo'), grants, tmp_path)


def test_version_platform_native_and_freedom_rejected(qualified, monkeypatch, tmp_path):
    grants = normalize_paths([str(tmp_path)])
    invocation = build(qualified, tmp_path)
    for freedom in ['read_only', 'unrestricted']:
        with pytest.raises(UnsupportedCapability):
            qualified.with_extra_writable_paths(invocation, grants, freedom)
    with pytest.raises(UnsupportedCapability):
        qualified.with_extra_writable_paths(replace(invocation, native_subagent=True), grants, 'write_in_repo')
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: 'codex-cli other')
    with pytest.raises(UnsupportedCapability):
        qualified.with_extra_writable_paths(invocation, grants, 'write_in_repo')
    # Empty grants never query runtime qualification.
    assert qualified.with_extra_writable_paths(invocation, (), 'write_in_repo') is invocation
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: EXTRA_PATHS_VERSION)
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'linux')
    with pytest.raises(UnsupportedCapability):
        qualified.with_extra_writable_paths(invocation, grants, 'write_in_repo')


def test_tampering_and_scratch(qualified, tmp_path):
    grants = normalize_paths([str(tmp_path)])
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path), grants, 'write_in_repo')
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    invocation = qualified.with_writable_directory(invocation, scratch, 'write_in_repo')
    qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')
    with pytest.raises(UnsafeInvocationError):
        qualified.assert_extra_writable_paths(invocation, (), 'write_in_repo')
    for pair in EXTRA_PATH_PAIRS:
        argv = list(invocation.argv)
        index = argv.index(pair)
        del argv[index-1:index+1]
        with pytest.raises(UnsafeInvocationError):
            qualified.assert_safe(replace(invocation, argv=argv), 'write_in_repo')
    argv = list(invocation.argv)
    argv[argv.index('--'):argv.index('--')] = ['--add-dir', '/etc']
    with pytest.raises(UnsafeInvocationError):
        qualified.assert_safe(replace(invocation, argv=argv), 'write_in_repo')
    with pytest.raises(UnsafeInvocationError):
        qualified.assert_safe(replace(invocation, extra_writable_paths=()), 'write_in_repo')


@pytest.mark.parametrize('name', ['claude', 'opencode', 'vibe', 'antigravity'])
def test_direct_unsupported_validator_rejects_metadata(name):
    with pytest.raises(UnsupportedCapability):
        backends.get(name).assert_safe(Invocation([], extra_writable_paths=('/etc',)), 'write_in_repo')


def test_launch_rechecks_version_and_identity(qualified, monkeypatch, tmp_path):
    grantdir = tmp_path / 'grant'
    grantdir.mkdir()
    grants = normalize_paths([str(grantdir)])
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path), grants, 'write_in_repo')
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: 'changed')
    with pytest.raises(UnsupportedCapability):
        qualified.qualify_extra_writable_paths(invocation, grants, 'write_in_repo')
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: EXTRA_PATHS_VERSION)
    grantdir.rmdir()
    grantdir.write_text('changed')
    with pytest.raises(ValueError):
        qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')


def test_one_probe_and_pure_final_runtime_check(qualified, monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(backends, 'version', lambda _, **kwargs: calls.append(1) or EXTRA_PATHS_VERSION)
    runtime = ['/original']
    monkeypatch.setattr(qualified, '_extra_path_runtime', lambda: tuple(runtime))
    grants = normalize_paths([str(tmp_path)])
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path), grants, 'write_in_repo')
    qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')
    assert calls == [1]
    runtime[0] = '/replacement'
    with pytest.raises(UnsafeInvocationError, match='runtime identity'):
        qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')
    assert calls == [1]


def test_runtime_changed_during_version_probe_refused(qualified, monkeypatch, tmp_path):
    runtime = ['/original']
    monkeypatch.setattr(qualified, '_extra_path_runtime', lambda: tuple(runtime))
    def probe(_, **kwargs):
        runtime[0] = '/replacement'
        return EXTRA_PATHS_VERSION
    monkeypatch.setattr(backends, 'version', probe)
    with pytest.raises(UnsafeInvocationError, match='during qualification'):
        qualified.with_extra_writable_paths(build(qualified, tmp_path), normalize_paths([str(tmp_path)]), 'write_in_repo')


def test_runtime_snapshot_tracks_path_and_symlink_target(qualified, monkeypatch, tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    first = tmp_path / 'first'
    second = tmp_path / 'second'
    for executable in (first, second):
        executable.write_text('#!/bin/sh\n')
        executable.chmod(0o755)
    link = bin_dir / qualified.binary
    link.symlink_to(first)
    monkeypatch.setenv('PATH', str(bin_dir))
    initial = qualified._extra_path_runtime()
    link.unlink()
    link.symlink_to(second)
    assert qualified._extra_path_runtime() != initial
    unchanged_target = qualified._extra_path_runtime()
    second.write_text('#!/bin/sh\n# replacement content\n')
    assert qualified._extra_path_runtime() != unchanged_target


def test_relative_path_probe_and_execution_bind_same_absolute_alias(tmp_path):
    """Isolate cwd/PATH changes so concurrent tests retain their original environment."""
    import os
    import subprocess
    import sys
    from pathlib import Path
    server = tmp_path / 'server'
    repo = tmp_path / 'repo'
    server.mkdir()
    repo.mkdir()
    certified = server / 'certified'
    certified.write_text('#!/bin/sh\nif [ "$1" = "--version" ]; then\n echo "' + EXTRA_PATHS_VERSION + '"\nelse\n echo qualified-server\nfi\n')
    certified.chmod(0o755)
    (server / 'codex').symlink_to(certified)
    malicious = repo / 'codex'
    malicious.write_text('#!/bin/sh\necho wrong-repo-binary\n')
    malicious.chmod(0o755)
    script = '''
import os, subprocess, sys
from dataclasses import replace
from pathlib import Path
from polybridge.backends.codex import CodexBackend, UnsafeInvocationError
import polybridge.backends.codex as codex
from polybridge.extra_paths import normalize_paths
server, repo = map(Path, sys.argv[1:])
os.chdir(server)
os.environ['PATH'] = '.'
codex.sys.platform = 'darwin'
backend = CodexBackend()
plain = backend.build_start_argv('Task', repo=repo, freedom='write_in_repo', session_id=None, model=None, max_turns=None, reasoning_effort=None)
grants = normalize_paths([str(repo)])
invocation = backend.with_extra_writable_paths(plain, grants, 'write_in_repo')
# Preserve the selected alias: resolving this symlink would hide updates to it.
assert invocation.argv[0] == str(server / 'codex')
assert invocation.extra_path_runtime[1] == str(server / 'certified')
assert subprocess.run(invocation.argv, cwd=repo, capture_output=True, text=True, check=True).stdout.strip() == 'qualified-server'
for binary in ('codex', str(repo / 'codex'), str(server / 'certified')):
    altered = replace(invocation, argv=[binary, *invocation.argv[1:]])
    try:
        backend.assert_extra_writable_paths(altered, grants, 'write_in_repo')
    except UnsafeInvocationError:
        pass
    else:
        raise AssertionError('executable substitution was accepted')
try:
    backend.assert_safe(replace(plain, argv=[str(server / 'codex'), *plain.argv[1:]]), 'write_in_repo')
except UnsafeInvocationError:
    pass
else:
    raise AssertionError('legacy executable allowance changed')
'''
    env = {**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')}
    result = subprocess.run([sys.executable, '-c', script, str(server), str(repo)], env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


def test_probe_uses_selected_absolute_binary_without_mutating_backend(qualified, monkeypatch, tmp_path):
    selected = str(tmp_path / 'codex-alias')
    monkeypatch.setattr(qualified, '_extra_path_runtime', lambda: (selected,))
    observed = []
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: observed.append(kwargs['executable']) or EXTRA_PATHS_VERSION)
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path), normalize_paths([str(tmp_path)]), 'write_in_repo')
    assert observed == [selected]
    assert invocation.argv[0] == selected
    assert qualified.binary == 'codex'
