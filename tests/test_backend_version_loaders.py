from types import SimpleNamespace
import sys
import pytest

from polybridge import backends


@pytest.mark.parametrize('platform', ['darwin', 'linux'])
def test_ordinary_script_version_and_native_owner_eligibility(tmp_path, monkeypatch, platform):
    from polybridge.backends.claude_native import CERTIFIED_VERSION
    from polybridge.workflow_native_policy import plan_owner_contracts
    from polybridge import workflows
    from test_workflow_delegation import graph
    wrapper = tmp_path / 'claude'
    wrapper.write_text('#!/bin/sh\nif [ "$1" = "--version" ]; then printf "%s\\n" "' + CERTIFIED_VERSION + ' fixture"; else exit 99; fi\n')
    wrapper.chmod(0o755)
    monkeypatch.setattr(sys, 'platform', platform)
    monkeypatch.setenv('PATH', str(tmp_path))
    assert backends.version(backends.get('claude')) == CERTIFIED_VERSION + ' fixture'
    definition = graph('review')
    candidate = {'backend': 'claude', 'model': 'claude-sonnet-4-6', 'max_turns': 100}
    definition['orchestrator'] = candidate
    definition['nodes'][1].update(agent=candidate, freedom='read_only', network=False,
                                   execution_mode='prefer_subagent', session_mode='fresh')
    definition = workflows.validate_definition(definition)
    plan = plan_owner_contracts(definition, network=False)
    owner = next(iter(plan['owners'].values()))
    outcomes = next(iter(owner['candidates'].values()))['nodes']
    assert outcomes[0]['execution_kind'] == 'native_subagent'


def test_macos_native_metadata_uses_exact_alias(tmp_path, monkeypatch):
    native = tmp_path / 'native'
    native.write_bytes(b'\xcf\xfa\xed\xfe')
    alias = tmp_path / 'alias'
    alias.symlink_to(native)
    monkeypatch.setattr(sys, 'platform', 'darwin')
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
    monkeypatch.setattr(sys, 'platform', 'linux')
    monkeypatch.setattr(backends.shutil, 'which', lambda binary: str(script))
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(stdout='version 2\n')
    monkeypatch.setattr(backends.subprocess, 'run', run)
    assert backends.version(backends.get('codex')) == 'version 2'
    assert calls == [['codex', '--version']]
