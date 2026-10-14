import os
from pathlib import Path
import struct

import pytest

from polybridge import runtime_loaders, runtime_paths


@pytest.fixture(autouse=True)
def isolated_configuration(monkeypatch, tmp_path_factory):
    # Synthetic loader fixtures must reach loader qualification independently
    # of the developer's active plugins or platform-managed preferences.
    monkeypatch.setenv('CODEX_HOME', str(tmp_path_factory.mktemp('loader-config')))
    monkeypatch.setattr('polybridge.backends.codex_config._mdm_absence',
                        lambda: ('managed_preferences_absent',))


def image(path, dependencies=(), rpaths=()):
    commands = []
    for kind, name in [(0xc, x) for x in dependencies] + [(0x8000001c, x) for x in rpaths]:
        value = name.encode() + b'\0'
        offset = 12 if kind == 0x8000001c else 24
        size = (offset + len(value) + 7) // 8 * 8
        commands.append(struct.pack('<III', kind, size, offset) + bytes(offset - 12) + value + bytes(size - offset - len(value)))
    body = b''.join(commands)
    path.write_bytes(struct.pack('<8I', 0xfeedfacf, 0x100000c, 0, 2, len(commands), len(body), 0, 0) + body)
    return path


def test_native_dependency_closure_and_missing_candidates(tmp_path):
    lib = image(tmp_path / 'lib.dylib')
    missing = tmp_path / 'first'
    executable = image(tmp_path / 'native', ['@rpath/lib.dylib'], [str(missing), '@loader_path'])
    files, roots = runtime_loaders.qualify_loaders([executable])
    assert lib in files
    assert missing / 'lib.dylib' in files
    assert missing in roots


def test_dependency_grant_is_rejected(tmp_path, monkeypatch):
    library = image(tmp_path / 'mutable.dylib')
    executable = image(tmp_path / 'native', [str(library)])
    monkeypatch.setattr(runtime_paths.shutil, 'which', lambda name: str(executable))
    monkeypatch.setattr(runtime_paths.sys, 'executable', str(executable))
    with pytest.raises(ValueError, match='protected server runtime paths'):
        runtime_paths.assert_runtime_outside_writes([(library, 'file')])


def test_unknown_shell_helper_rejected_without_execution(tmp_path, monkeypatch):
    shell = tmp_path / 'git'
    sentinel = tmp_path / 'executed'
    shell.write_text(f'#!/bin/sh\ntouch {sentinel}\n')
    monkeypatch.setattr(runtime_paths.shutil, 'which', lambda name: str(shell))
    with pytest.raises(ValueError, match='script loader'):
        runtime_paths.assert_runtime_outside_writes([(tmp_path / 'executed', 'file')])
    assert not sentinel.exists()


@pytest.mark.parametrize('kind', ['malformed', 'unresolved', 'unknown'])
def test_unqualified_images_rejected(tmp_path, kind):
    executable = tmp_path / 'native'
    if kind == 'malformed':
        executable.write_bytes(struct.pack('<8I', 0xfeedfacf, 0, 0, 2, 1, 8, 0, 0) + bytes(8))
    elif kind == 'unresolved':
        image(executable, [str(tmp_path / 'absent.dylib')])
    else:
        executable.write_bytes(b'unknown')
    with pytest.raises(ValueError):
        runtime_loaders.qualify_loaders([executable])


def test_loader_override_rejected(tmp_path, monkeypatch):
    executable = image(tmp_path / 'native')
    monkeypatch.setenv('DYLD_INSERT_LIBRARIES', str(tmp_path / 'injected.dylib'))
    with pytest.raises(ValueError, match='environment overrides'):
        runtime_loaders.qualify_loaders([executable])


def test_system_name_requires_observed_readonly_volume(tmp_path, monkeypatch):
    executable = image(tmp_path / 'native', ['/usr/lib/definitely-missing-probe.dylib'])
    monkeypatch.setattr(runtime_loaders.os, 'statvfs', lambda path: type('Flags', (), {'f_flag': 0})())
    with pytest.raises(ValueError, match='unresolved'):
        runtime_loaders.qualify_loaders([executable])


def test_truncated_architecture_count_is_value_error(tmp_path):
    executable = tmp_path / 'truncated'
    executable.write_bytes(b'\xca\xfe\xba\xbe\0')
    with pytest.raises(ValueError):
        runtime_loaders.qualify_loaders([executable])


def test_truncated_thin_header_is_value_error(tmp_path):
    executable = tmp_path / 'truncated-thin'
    executable.write_bytes(b'\xcf\xfa\xed\xfe' + bytes(8))
    with pytest.raises(ValueError, match='truncated runtime Mach-O header'):
        runtime_loaders.qualify_loaders([executable])


@pytest.mark.parametrize('env_shebang', [False, True])
def test_discovered_interpreter_dependency_refused_before_version(tmp_path, monkeypatch, env_shebang):
    from polybridge import backends
    from polybridge.backends.codex import UnsafeInvocationError
    from polybridge.extra_paths import normalize_paths

    binaries = tmp_path / 'bin'
    binaries.mkdir()
    codex = image(binaries / 'codex')
    node = image(binaries / 'node', [str(tmp_path / 'outside.dylib')])
    dependency = image(tmp_path / 'outside.dylib')
    script = binaries / 'vibe'
    script.write_text('#!/usr/bin/env node\n' if env_shebang else f'#!{node}\n')
    env = image(binaries / 'env')
    original_chain = runtime_paths._chain
    # Isolate the env launcher, while preserving all namespace and loader checks.
    def chain(path):
        if path == Path('/usr/bin/env'):
            protected, target = original_chain(env)
            return protected | {path}, target
        return original_chain(path)
    monkeypatch.setattr(runtime_paths, '_chain', chain)
    original_qualify = runtime_loaders.qualify_loaders
    monkeypatch.setattr(runtime_loaders, 'qualify_loaders',
                        lambda paths: original_qualify([env if path == Path('/usr/bin/env') else path for path in paths]))
    choices = {'codex': codex, 'vibe': script, 'node': node}
    monkeypatch.setattr(runtime_paths.shutil, 'which', lambda name: str(choices[name]) if name in choices else None)
    monkeypatch.setattr(runtime_paths.sys, 'executable', str(codex))
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'darwin')
    calls = []
    monkeypatch.setattr(backends, 'version', lambda *a, **kw: calls.append(1))
    repo = tmp_path / 'repo'
    repo.mkdir()
    backend = backends.get('codex')
    plain = backend.build_start_argv('task', repo=repo, freedom='write_in_repo', session_id=None,
                                     model=None, max_turns=None, reasoning_effort=None)
    with pytest.raises(UnsafeInvocationError, match='protected server runtime paths'):
        backend.with_extra_writable_paths(plain, normalize_paths([str(dependency)]), 'write_in_repo')
    assert calls == []


def test_later_rpath_candidate_dependency_refused_before_version(tmp_path, monkeypatch):
    from polybridge import backends
    from polybridge.backends.codex import UnsafeInvocationError
    from polybridge.extra_paths import normalize_paths

    binaries = tmp_path / 'bin'
    first = tmp_path / 'first'
    later = tmp_path / 'later'
    for directory in (binaries, first, later):
        directory.mkdir()
    dependency = image(tmp_path / 'outside.dylib')
    wrong_arch = image(first / 'candidate.dylib')
    data = bytearray(wrong_arch.read_bytes())
    struct.pack_into('<I', data, 4, 0x1000007)  # x86_64 instead of synthetic arm64.
    wrong_arch.write_bytes(data)
    image(later / 'candidate.dylib', [str(dependency)])
    executable = image(binaries / 'codex', ['@rpath/candidate.dylib'], [str(first), str(later)])
    monkeypatch.setattr(runtime_paths.shutil, 'which', lambda name: str(executable) if name == 'codex' else None)
    monkeypatch.setattr(runtime_paths.sys, 'executable', str(executable))
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'darwin')
    calls = []
    monkeypatch.setattr(backends, 'version', lambda *a, **kw: calls.append(1))
    repo = tmp_path / 'repo'
    repo.mkdir()
    backend = backends.get('codex')
    plain = backend.build_start_argv('task', repo=repo, freedom='write_in_repo', session_id=None,
                                     model=None, max_turns=None, reasoning_effort=None)
    with pytest.raises(UnsafeInvocationError, match='protected server runtime paths'):
        backend.with_extra_writable_paths(plain, normalize_paths([str(dependency)]), 'write_in_repo')
    assert calls == []


def test_existing_unqualified_later_rpath_candidate_fails_closed(tmp_path):
    first = tmp_path / 'first'
    later = tmp_path / 'later'
    first.mkdir()
    later.mkdir()
    image(first / 'candidate.dylib')
    (later / 'candidate.dylib').write_bytes(b'not an image')
    executable = image(tmp_path / 'native', ['@rpath/candidate.dylib'], [str(first), str(later)])
    with pytest.raises(ValueError, match='unsupported runtime executable loader layout'):
        runtime_loaders.qualify_loaders([executable])
