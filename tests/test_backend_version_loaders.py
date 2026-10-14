from types import SimpleNamespace

from polybridge import backends


def test_macos_script_metadata_never_executes(tmp_path, monkeypatch):
    script = tmp_path / 'cli'
    script.write_text('#!/bin/sh\nexit 99\n')
    monkeypatch.setattr(backends.sys, 'platform', 'darwin')
    monkeypatch.setattr(backends.shutil, 'which', lambda binary: str(script))
    monkeypatch.setattr(backends.subprocess, 'run', lambda *a, **kw: (_ for _ in ()).throw(AssertionError('script executed')))
    assert backends.version(backends.get('codex')) is None


def test_macos_native_metadata_uses_exact_alias(tmp_path, monkeypatch):
    native = tmp_path / 'native'
    native.write_bytes(b'\xcf\xfa\xed\xfe')
    alias = tmp_path / 'alias'
    alias.symlink_to(native)
    monkeypatch.setattr(backends.sys, 'platform', 'darwin')
    monkeypatch.setattr(backends.shutil, 'which', lambda binary: str(alias))
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(stdout='version 1\n')
    monkeypatch.setattr(backends.subprocess, 'run', run)
    assert backends.version(backends.get('codex'), executable=str(alias)) == 'version 1'
    assert calls == [[str(alias), '--version']]


def test_nonmacos_script_metadata_retains_probe(tmp_path, monkeypatch):
    script = tmp_path / 'cli'
    script.write_text('#!/bin/sh\n')
    monkeypatch.setattr(backends.sys, 'platform', 'linux')
    monkeypatch.setattr(backends.shutil, 'which', lambda binary: str(script))
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(stdout='version 2\n')
    monkeypatch.setattr(backends.subprocess, 'run', run)
    assert backends.version(backends.get('codex')) == 'version 2'
    assert calls == [['codex', '--version']]
