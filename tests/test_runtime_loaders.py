import os
from pathlib import Path
import struct

import pytest

from polybridge import runtime_loaders, runtime_paths


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
