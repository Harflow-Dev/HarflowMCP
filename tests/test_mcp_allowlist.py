import asyncio
import gc
import json
import os
import warnings
from pathlib import Path

import pytest
import tomlkit
from harflow_mcp import mcp_allowlist as rules
from harflow_mcp.backends import mcp_approval as native_rules


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    from types import SimpleNamespace
    monkeypatch.setattr(native_rules.shutil, 'which', lambda _: '/fake/opencode')
    monkeypatch.setattr(native_rules.subprocess, 'run', lambda *a, **k: SimpleNamespace(stdout='1.18.34', returncode=0))
    for key in ('CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'XDG_CONFIG_HOME', 'VIBE_HOME'):
        monkeypatch.delenv(key, raising=False)
    return tmp_path


@pytest.mark.parametrize('backend', ['codex','claude','antigravity','opencode','vibe'])
def test_scoped_roundtrip_and_private_backup(home, backend):
    path = rules.config_path(backend)
    path.parent.mkdir(parents=True)
    original = {'other': 'untouched'}
    if backend == 'codex': original['mcp_servers'] = {'harflow_mcp': {'command': 'harflow-server'}}
    if backend == 'opencode': original['mcp'] = {'harflow_mcp': {'type': 'local', 'command': ['harflow-server']}}
    if backend == 'vibe': original['mcp_servers'] = [{'name': 'harflow_mcp', 'transport': 'stdio', 'command': 'harflow-server'}]
    path.write_text(tomlkit.dumps(original) if backend in ('codex','vibe') else json.dumps(original))
    path.chmod(0o600)
    result = rules.edit(backend, allow='harflow_mcp/apply_workflow_draft')
    assert result['entries'] == ['harflow_mcp/apply_workflow_draft']
    assert rules.edit(backend)['entries'] == result['entries']
    assert path.with_suffix(path.suffix+'.harflow-backup').stat().st_mode & 0o777 == 0o600
    data = tomlkit.parse(path.read_text()) if backend in ('codex','vibe') else json.loads(path.read_text())
    assert data['other'] == 'untouched'
    assert rules.edit(backend, remove='harflow_mcp/apply_workflow_draft')['entries'] == []


def test_codex_server_approval_is_scoped(home):
    path=rules.config_path('codex');path.parent.mkdir()
    path.write_text('# keep\n[mcp_servers.harflow]\ncommand="harflow-server"\n[mcp_servers.harflow.tools.apply_workflow_draft]\napproval_mode="prompt"\n[mcp_servers.other]\ndefault_tools_approval_mode="prompt"\n')
    rules.edit('codex',allow='harflow/*')
    data=tomlkit.parse(path.read_text())
    assert data['mcp_servers']['harflow']['tools']['apply_workflow_draft']['approval_mode']=='prompt'
    rules.edit('codex',remove='harflow/*')
    assert tomlkit.parse(path.read_text())['mcp_servers']['harflow']['tools']['apply_workflow_draft']['approval_mode']=='prompt'
    assert data['mcp_servers']['other']['default_tools_approval_mode']=='prompt'
    assert '# keep' in path.read_text()


def test_claude_bare_server_alias(home):
    path=rules.config_path('claude');path.parent.mkdir()
    path.write_text('{"permissions":{"allow":["mcp__harflow_mcp","Bash(ls)"],"deny":["mcp__other__write"]}}')
    assert rules.edit('claude')['entries']==['harflow_mcp/*']
    rules.edit('claude',remove='harflow_mcp/*')
    assert json.loads(path.read_text())['permissions']=={'allow':['Bash(ls)'],'deny':['mcp__other__write']}


def test_jsonc_other_comments_and_order_preserved(home):
    path=rules.config_path('opencode').with_suffix('.jsonc');path.parent.mkdir(parents=True)
    path.write_text('{\n// URL context\n"url":"https://example.org",\n"mcp":{"harflow_mcp":{}},\n"permission":{"*":"ask"},\n// final comment\n}')
    rules.edit('opencode',allow='harflow_mcp/*')
    assert '// URL context' in path.read_text() and '// final comment' in path.read_text()
    assert list(json.loads(__import__('harflow_mcp.clients.jsonc',fromlist=['strip']).strip(path.read_text()))['permission'])==['*','harflow_mcp_*']


@pytest.mark.parametrize('kind',['config','backup','lock'])
def test_symlink_refused_without_touching_target(home,kind):
    path=rules.config_path('codex');path.parent.mkdir()
    target=home/'target';target.write_text('secret')
    selected=path if kind=='config' else path.with_suffix(path.suffix+('.harflow-backup' if kind=='backup' else '.harflow.lock'))
    selected.symlink_to(target)
    with pytest.raises(ValueError,match='symlink'): rules.edit('codex',allow='harflow_mcp/*')
    assert target.read_text()=='secret'


@pytest.mark.parametrize('entry',['*/*','harflow_mcp/','harflow_mcp/a b','harflow_mcp/x;rm','../tool'])
def test_invalid_entries_do_not_create_config(home,entry):
    with pytest.raises(ValueError): rules.edit('codex',allow=entry)
    assert not rules.config_path('codex').exists()


def test_vibe_wildcard_refused(home):
    with pytest.raises(ValueError,match='individual'): rules.edit('vibe',allow='harflow_mcp/*')
    assert not rules.config_path('vibe').exists()


def test_cli_read_and_write_human_guard(home, monkeypatch, capsys):
    from harflow_mcp import ctl, server
    from harflow_mcp import takeover
    monkeypatch.setattr(takeover, "caller_refusal", lambda *args: None)
    class Human:
        log_dir = home / "tasks"
        async def _detect_caller(self): return None
    monkeypatch.setattr(server, '_reg', lambda: Human())
    path = rules.config_path('codex')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('[mcp_servers.harflow]\ncommand="harflow-server"\n')
    assert ctl.main(['mcp-allowlist','--backend','codex','--allow','harflow/*','--json']) == 0
    assert json.loads(capsys.readouterr().out)['result']['entries'] == ['harflow/*']
    monkeypatch.setattr(takeover, "caller_refusal", lambda *args: ("agent_caller", "managed"))
    class Agent:
        log_dir = home / "tasks"
        async def _detect_caller(self): return object()
    monkeypatch.setattr(server, '_reg', lambda: Agent())
    before=rules.config_path('codex').read_bytes()
    assert ctl.main(['mcp-allowlist','--backend','codex','--remove','harflow/*','--json']) != 0
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'human_only'
    assert rules.config_path('codex').read_bytes() == before


@pytest.mark.parametrize('notification_source', ['environment', 'database'])
def test_cli_allowlist_never_installs_a_registry_or_notification_worker(
    home, monkeypatch, capsys, notification_source,
):
    from harflow_mcp import ctl, notifications, server, takeover
    from harflow_mcp.tasks import TaskRegistry

    registry = TaskRegistry(log_dir=home / '.harflow' / 'tasks')
    monkeypatch.setattr(server, '_registry', None)
    monkeypatch.delenv('HARFLOW_NOTIFY_DESTINATIONS', raising=False)
    if notification_source == 'environment':
        monkeypatch.setenv('HARFLOW_NOTIFY_DESTINATIONS', json.dumps({
            'hook': {'url': 'https://example.com/hook'},
        }))
    else:
        # The database is a legitimate current-root fixture.  Initialize the
        # root before creating it so the test exercises notification behavior,
        # rather than the state-layout refusal for a marked-but-lockless root.
        from harflow_mcp import state_ownership
        from harflow_mcp.state_layout import resolve_layout
        with state_ownership.state_session(resolve_layout(root=registry.log_dir.parent)):
            pass
        database = registry.log_dir.parent / 'notifications.sqlite3'
        database.parent.mkdir(parents=True, exist_ok=True)
        database.touch()
    monkeypatch.setattr(takeover, 'caller_refusal', lambda *args: None)
    result = {'entries': ['harflow_mcp/*'], 'detail': 'updated'}
    monkeypatch.setattr(rules, 'edit', lambda *args, **kwargs: result)
    started = []

    async def worker(logs):
        started.append(logs)
        await asyncio.Future()

    monkeypatch.setattr(notifications, 'worker', worker)
    gc.collect()  # Flush unrelated objects before checking this synchronous call.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        assert ctl.main([
            'mcp-allowlist', '--backend', 'codex', '--allow', 'harflow_mcp/*', '--json',
        ]) == 0
        gc.collect()
    assert json.loads(capsys.readouterr().out)['result'] == result
    assert [str(item.message) for item in caught] == []
    # The allowlist edit needs no registry: none is installed, so no worker can be started.
    assert server._registry is None
    assert started == []

    async def later_startup():
        server._start_notification_worker(registry)
        first = registry._notification_worker
        try:
            server._start_notification_worker(registry)
            await asyncio.sleep(0)
            assert registry._notification_worker is first
            assert started == [registry.log_dir]
        finally:
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first

    asyncio.run(later_startup())


def test_opencode_v2_refuses_mutation(home, monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(native_rules.shutil, 'which', lambda _: '/fake/opencode')
    monkeypatch.setattr(native_rules.subprocess, 'run', lambda *a, **k: SimpleNamespace(stdout='2.0.1', returncode=0))
    assert rules.edit('opencode', allow='harflow_mcp/*')['supported'] is False
    assert not rules.config_path('opencode').exists()


@pytest.mark.parametrize('stdout,code',[('',0),('unknown',0),('1.2.0',1)])
def test_unknown_opencode_version_refuses_mutation(home, monkeypatch, stdout, code):
    from types import SimpleNamespace
    monkeypatch.setattr(native_rules.subprocess,'run',lambda *a,**k:SimpleNamespace(stdout=stdout,returncode=code))
    assert not rules.edit('opencode',allow='harflow_mcp/*')['supported']
    assert not rules.config_path('opencode').exists()


@pytest.mark.parametrize('action', ['allow', 'remove'])
@pytest.mark.parametrize('entry', ['harflow_mcp/*', 'harflow_mcp/apply_workflow_draft'])
@pytest.mark.parametrize('existing', [False, True])
def test_codex_approval_cannot_register_missing_server(home, action, entry, existing):
    path = rules.config_path('codex')
    if existing:
        path.parent.mkdir(parents=True)
        path.write_text('[mcp_servers.other]\ncommand="other-server"\n')
    before = path.read_bytes() if existing else None
    with pytest.raises(ValueError, match='Register this MCP server'):
        rules.edit('codex', **{action: entry})
    assert (path.read_bytes() if path.exists() else None) == before
    assert not path.with_suffix(path.suffix + '.harflow-backup').exists()


def test_codex_registered_http_server_preserves_transport(home):
    path = rules.config_path('codex')
    path.parent.mkdir(parents=True)
    path.write_text('[mcp_servers.harflow]\nurl="https://example.org/mcp"\n')
    rules.edit('codex', allow='harflow/*')
    assert tomlkit.parse(path.read_text())['mcp_servers']['harflow']['url'] == 'https://example.org/mcp'


def test_generic_allowlist_has_no_backend_specific_branching():
    import ast
    tree = ast.parse(Path(rules.__file__).read_text())
    names = {"claude", "codex", "vibe", "opencode", "antigravity"}
    native_literals = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in names]
    assert native_literals == [], "Native approval paths and formats belong to Backend adapter hooks"


def test_new_backend_approval_adapter_needs_no_generic_editor_changes(home, monkeypatch):
    from types import SimpleNamespace
    from harflow_mcp.backends import BACKENDS

    class CustomApproval:
        detail_prefix = ""
        def config_path(self): return home / "custom-harness.json"
        def unsupported_reason(self): return None
        def parse(self, raw): return json.loads(raw or "{}")
        def entries(self, data): return data.get("allow", [])
        def update(self, raw, data, entry, *, allow):
            data["allow"] = [entry] if allow else []
            return json.dumps(data)

    monkeypatch.setitem(BACKENDS, "custom-harness", SimpleNamespace(mcp_approval=CustomApproval()))
    assert rules.edit("custom-harness", allow="harflow_mcp/tool")["entries"] == ["harflow_mcp/tool"]
    assert rules.edit("custom-harness")["entries"] == ["harflow_mcp/tool"]
    assert rules.edit("custom-harness", remove="harflow_mcp/tool")["entries"] == []


def test_registered_backend_approval_adapter_cli_roundtrip_and_human_guard(home, monkeypatch, capsys):
    from types import SimpleNamespace
    from harflow_mcp import ctl, server, takeover
    from harflow_mcp.backends import BACKENDS

    class CustomApproval:
        detail_prefix = ''
        def config_path(self): return home / 'registered-harness.json'
        def unsupported_reason(self): return None
        def parse(self, raw): return json.loads(raw or '{}')
        def entries(self, data): return data.get('allow', [])
        def update(self, raw, data, entry, *, allow):
            data['allow'] = [entry] if allow else []
            return json.dumps(data)

    monkeypatch.setitem(BACKENDS, 'registered-harness', SimpleNamespace(mcp_approval=CustomApproval()))
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=home/'tasks'))
    monkeypatch.setattr(takeover, 'caller_refusal', lambda _: None)
    base=['mcp-allowlist','--backend','registered-harness','--json']
    for extra, expected in [(['--allow','harflow_mcp/tool'], ['harflow_mcp/tool']), ([], ['harflow_mcp/tool']), (['--remove','harflow_mcp/tool'], [])]:
        assert ctl.main(base+extra)==0
        assert json.loads(capsys.readouterr().out)['result']['entries']==expected
    monkeypatch.setattr(takeover, 'caller_refusal', lambda _: ('agent_caller','managed'))
    before=(home/'registered-harness.json').read_bytes()
    assert ctl.main(base+['--allow','harflow_mcp/tool'])==1
    assert json.loads(capsys.readouterr().out)['error']['code']=='human_only'
    assert (home/'registered-harness.json').read_bytes()==before
