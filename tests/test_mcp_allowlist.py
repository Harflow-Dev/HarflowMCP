import json
import os
from pathlib import Path

import pytest
import tomlkit
from polybridge import mcp_allowlist as rules


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    from types import SimpleNamespace
    monkeypatch.setattr(rules.shutil, 'which', lambda _: '/fake/opencode')
    monkeypatch.setattr(rules.subprocess, 'run', lambda *a, **k: SimpleNamespace(stdout='1.18.34', returncode=0))
    for key in ('CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'XDG_CONFIG_HOME', 'VIBE_HOME'):
        monkeypatch.delenv(key, raising=False)
    return tmp_path


@pytest.mark.parametrize('backend', ['codex','claude','antigravity','opencode','vibe'])
def test_scoped_roundtrip_and_private_backup(home, backend):
    path = rules.config_path(backend)
    path.parent.mkdir(parents=True)
    original = {'other': 'untouched'}
    if backend == 'opencode': original['mcp'] = {'polybridge': {'type': 'local', 'command': ['polybridge-server']}}
    if backend == 'vibe': original['mcp_servers'] = [{'name': 'polybridge', 'transport': 'stdio', 'command': 'polybridge-server'}]
    path.write_text(tomlkit.dumps(original) if backend in ('codex','vibe') else json.dumps(original))
    path.chmod(0o600)
    result = rules.edit(backend, allow='polybridge/apply_workflow_draft')
    assert result['entries'] == ['polybridge/apply_workflow_draft']
    assert rules.edit(backend)['entries'] == result['entries']
    assert path.with_suffix(path.suffix+'.polybridge-backup').stat().st_mode & 0o777 == 0o600
    data = tomlkit.parse(path.read_text()) if backend in ('codex','vibe') else json.loads(path.read_text())
    assert data['other'] == 'untouched'
    assert rules.edit(backend, remove='polybridge/apply_workflow_draft')['entries'] == []


def test_codex_server_approval_is_scoped(home):
    path=rules.config_path('codex');path.parent.mkdir()
    path.write_text('# keep\n[mcp_servers.polybridge.tools.apply_workflow_draft]\napproval_mode="prompt"\n[mcp_servers.other]\ndefault_tools_approval_mode="prompt"\n')
    rules.edit('codex',allow='polybridge/*')
    data=tomlkit.parse(path.read_text())
    assert data['mcp_servers']['polybridge']['tools']['apply_workflow_draft']['approval_mode']=='prompt'
    rules.edit('codex',remove='polybridge/*')
    assert tomlkit.parse(path.read_text())['mcp_servers']['polybridge']['tools']['apply_workflow_draft']['approval_mode']=='prompt'
    assert data['mcp_servers']['other']['default_tools_approval_mode']=='prompt'
    assert '# keep' in path.read_text()


def test_claude_bare_server_alias(home):
    path=rules.config_path('claude');path.parent.mkdir()
    path.write_text('{"permissions":{"allow":["mcp__polybridge","Bash(ls)"],"deny":["mcp__other__write"]}}')
    assert rules.edit('claude')['entries']==['polybridge/*']
    rules.edit('claude',remove='polybridge/*')
    assert json.loads(path.read_text())['permissions']=={'allow':['Bash(ls)'],'deny':['mcp__other__write']}


def test_jsonc_other_comments_and_order_preserved(home):
    path=rules.config_path('opencode').with_suffix('.jsonc');path.parent.mkdir(parents=True)
    path.write_text('{\n// URL context\n"url":"https://example.org",\n"mcp":{"polybridge":{}},\n"permission":{"*":"ask"},\n// final comment\n}')
    rules.edit('opencode',allow='polybridge/*')
    assert '// URL context' in path.read_text() and '// final comment' in path.read_text()
    assert list(json.loads(__import__('polybridge.clients.jsonc',fromlist=['strip']).strip(path.read_text()))['permission'])==['*','polybridge_*']


@pytest.mark.parametrize('kind',['config','backup','lock'])
def test_symlink_refused_without_touching_target(home,kind):
    path=rules.config_path('codex');path.parent.mkdir()
    target=home/'target';target.write_text('secret')
    selected=path if kind=='config' else path.with_suffix(path.suffix+('.polybridge-backup' if kind=='backup' else '.polybridge.lock'))
    selected.symlink_to(target)
    with pytest.raises(ValueError,match='symlink'): rules.edit('codex',allow='polybridge/*')
    assert target.read_text()=='secret'


@pytest.mark.parametrize('entry',['*/*','polybridge/','polybridge/a b','polybridge/x;rm','../tool'])
def test_invalid_entries_do_not_create_config(home,entry):
    with pytest.raises(ValueError): rules.edit('codex',allow=entry)
    assert not rules.config_path('codex').exists()


def test_vibe_wildcard_refused(home):
    with pytest.raises(ValueError,match='individual'): rules.edit('vibe',allow='polybridge/*')
    assert not rules.config_path('vibe').exists()


def test_cli_read_and_write_human_guard(home, monkeypatch, capsys):
    from polybridge import ctl, server
    from polybridge import takeover
    monkeypatch.setattr(takeover, "caller_refusal", lambda *args: None)
    class Human:
        log_dir = home / "tasks"
        async def _detect_caller(self): return None
    monkeypatch.setattr(server, '_reg', lambda: Human())
    assert ctl.main(['mcp-allowlist','--backend','codex','--allow','polybridge/*','--json']) == 0
    assert json.loads(capsys.readouterr().out)['result']['entries'] == ['polybridge/*']
    monkeypatch.setattr(takeover, "caller_refusal", lambda *args: ("agent_caller", "managed"))
    class Agent:
        log_dir = home / "tasks"
        async def _detect_caller(self): return object()
    monkeypatch.setattr(server, '_reg', lambda: Agent())
    before=rules.config_path('codex').read_bytes()
    assert ctl.main(['mcp-allowlist','--backend','codex','--remove','polybridge/*','--json']) != 0
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'human_only'
    assert rules.config_path('codex').read_bytes() == before


def test_opencode_v2_refuses_mutation(home, monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(rules.shutil, 'which', lambda _: '/fake/opencode')
    monkeypatch.setattr(rules.subprocess, 'run', lambda *a, **k: SimpleNamespace(stdout='2.0.1', returncode=0))
    assert rules.edit('opencode', allow='polybridge/*')['supported'] is False
    assert not rules.config_path('opencode').exists()


@pytest.mark.parametrize('stdout,code',[('',0),('unknown',0),('1.2.0',1)])
def test_unknown_opencode_version_refuses_mutation(home, monkeypatch, stdout, code):
    from types import SimpleNamespace
    monkeypatch.setattr(rules.subprocess,'run',lambda *a,**k:SimpleNamespace(stdout=stdout,returncode=code))
    assert not rules.edit('opencode',allow='polybridge/*')['supported']
    assert not rules.config_path('opencode').exists()
