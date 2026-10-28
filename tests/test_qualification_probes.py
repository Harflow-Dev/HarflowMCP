"""Production probe safety and opt-in no-paid real CLI verification."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import pytest
from harflow_mcp.backends.qualification import Identity, ProbeContext, identify
from harflow_mcp.backends.qualification_probes import _environment, _sandbox, _provider_prefix, claude_probe, codex_probe


def test_probe_environment_does_not_inherit_credentials_config_or_loader(tmp_path, monkeypatch):
    monkeypatch.setenv('ANTHROPIC_API_KEY', 'must-not-copy')
    monkeypatch.setenv('OPENAI_API_KEY', 'must-not-copy')
    monkeypatch.setenv('HTTP_PROXY', 'must-not-copy')
    monkeypatch.setenv('NODE_OPTIONS', 'must-not-copy')
    monkeypatch.setenv('DYLD_INSERT_LIBRARIES', 'must-not-copy')
    environment = _environment(tmp_path)
    assert not any('must-not-copy' in value for value in environment.values())
    assert all(name not in environment for name in ('ANTHROPIC_API_KEY', 'OPENAI_API_KEY', 'HTTP_PROXY', 'NODE_OPTIONS', 'DYLD_INSERT_LIBRARIES'))
    assert Path(environment['HOME']).is_relative_to(tmp_path)
    assert Path(environment['CLAUDE_CODE_TMPDIR']).is_relative_to(tmp_path)


@pytest.mark.parametrize('probe', [claude_probe, codex_probe])
def test_unsupported_platform_refuses_before_launch(tmp_path, probe):
    context = ProbeContext(Identity('fixture', '0.0.0', '/never/execute', '0' * 64, 'unsupported'), 'caller_approval_tools', str(tmp_path))
    result = probe(context)
    assert not result.ok and 'no qualified OS isolation' in result.reason
    assert not result.features


@pytest.mark.parametrize('probe', [claude_probe, codex_probe])
def test_unknown_feature_refuses(tmp_path, probe):
    context = ProbeContext(Identity('fixture', '0.0.0', '/never/execute', '0' * 64, sys.platform), 'not-a-feature', str(tmp_path))
    result = probe(context)
    assert not result.ok and 'unsupported' in result.reason


@pytest.mark.cli_integration
@pytest.mark.skipif(not os.environ.get('HARFLOW_CLI_INTEGRATION') or sys.platform != 'darwin', reason='opt-in real macOS OS isolation')
def test_actual_probe_sandbox_denies_outside_writes_and_nonloopback_bind(tmp_path):
    tmp_path = tmp_path.resolve()
    executable = Path('/usr/bin/true')
    context = ProbeContext(Identity('fixture', '0', str(executable), hashlib.sha256(executable.read_bytes()).hexdigest(), 'darwin'), 'caller_approval_tools', str(tmp_path))
    environment = _environment(tmp_path)
    prefix = _sandbox(context, tmp_path, environment)
    outside = tmp_path.parent / ('qualification-denied-' + tmp_path.name)
    try:
        result = subprocess.run([*prefix, '/usr/bin/touch', str(outside)], env=environment, capture_output=True, timeout=5)
        assert result.returncode != 0 and not outside.exists()
        # Binding any address is denied; qualification providers bind loopback in the parent.
        code = 'import socket; s=socket.socket(); s.bind(("0.0.0.0",0))'
        result = subprocess.run([*prefix, sys.executable, '-c', code], env=environment, capture_output=True, timeout=5)
        assert result.returncode != 0 and b'Operation not permitted' in result.stderr
    finally:
        if outside.exists():
            outside.unlink()


@pytest.mark.cli_integration
@pytest.mark.skipif(not os.environ.get('HARFLOW_CLI_INTEGRATION'), reason='opt-in no-paid real CLI probes')
@pytest.mark.parametrize('backend_name,probe', [('claude', claude_probe), ('codex', codex_probe)])
def test_production_permission_probe_against_real_cli(tmp_path, backend_name, probe):
    import shutil
    from harflow_mcp.backends import get
    if sys.platform != 'darwin' or not shutil.which(backend_name):
        pytest.skip('qualified OS isolation and installed CLI required')
    identity = identify(get(backend_name))
    from harflow_mcp.backends.qualification import FeatureSpec, _probe
    result = _probe(FeatureSpec('caller_approval_tools', 'production-fixture', probe=probe, timeout_seconds=180), identity)
    assert result.ok, result.reason
    assert result.features['caller_approval_tools']['caller_approval_tools']
    assert result.features['caller_write_tools']['caller_write_tools']


def test_probe_network_allowance_names_only_the_generated_provider_port():
    prefix = ['/usr/bin/sandbox-exec', '-p', '(version 1)(allow default)(deny network*)']
    result = _provider_prefix(prefix, 'http://127.0.0.1:12345')
    assert 'localhost:12345' in result[2] and 'localhost:*' not in result[2]
    with pytest.raises(ValueError):
        _provider_prefix(prefix, 'https://provider.example.invalid:443')


def _scripted_claude(monkeypatch, tmp_path, mode, feature):
    """Synthetic CLI/provider exchange drives production correlation and evidence checks."""
    from contextlib import contextmanager
    import json
    from harflow_mcp.backends import qualification_probes as probes
    active = {}
    @contextmanager
    def fixture_provider(kind, decide):
        assert kind == 'anthropic'
        requests = []
        active.update(decide=decide, requests=requests)
        yield 'http://127.0.0.1:12345', requests, []
    def drive(argv, *, base, env, initial, on_event):
        grants = '--allowedTools' in argv
        if grants and mode == 'provider_failure':
            raise ValueError('synthetic grant provider unavailable')
        on_event({'type': 'system', 'subtype': 'init', 'claude_code_version': '9.9.9'})
        answered = []
        for number in range(32):
            body = {'messages': [{'content': list(answered)}]}
            active['requests'].append(body)
            blocks, stop = active['decide'](body, number + 1)
            if stop == 'end_turn':
                frames, done = on_event({'type': 'result', 'subtype': 'success', 'is_error': False})
                assert done and not frames
                return
            call = blocks[0]
            identifier, tool, arguments = call['id'], call['name'], call['input']
            if grants:
                allowed = identifier in {'fetch', 'pull', 'switch', 'allowed'}
                if identifier == 'allowed' and mode == 'grant_failure':
                    allowed = False
                if identifier == 'switch' and allowed:
                    subprocess.run(['/usr/bin/git', 'switch', '-c', 'qualification/prep'], cwd=base / 'repo', check=True, capture_output=True)
                text = {'denied': 'Permission to use Bash with command git commit -m qualification has been denied.',
                        'chained': 'This Bash command contains multiple operations. The following part requires approval: python3 -m calendar'}.get(identifier, 'This command requires approval')
                if not allowed and mode == 'changed_denial_text':
                    text = 'A new refusal format that has not been qualified'
            else:
                reason = 'other' if tool == 'Read' else 'safetyCheck'
                if mode == 'changed_path_reason':
                    reason = 'unknown-new-reason'
                frames, _done = on_event({'type': 'control_request', 'request_id': 'request-' + identifier,
                    'request': {'subtype': 'can_use_tool', 'tool_name': tool, 'tool_use_id': identifier,
                                'input': arguments, 'decision_reason_type': reason}})
                response = json.loads(frames[0])['response']['response']
                allowed = response['behavior'] == 'allow'
                text = 'fixture result' if allowed else 'Caller rejected this exact action'
                if allowed:
                    if tool == 'Bash':
                        Path(arguments['command'].removeprefix('/usr/bin/touch ')).touch()
                    elif tool == 'Write':
                        Path(arguments['file_path']).write_text(arguments['content'])
                    elif tool == 'Edit':
                        path = Path(arguments['file_path'])
                        path.write_text(path.read_text().replace(arguments['old_string'], arguments['new_string']))
                    elif tool == 'Read':
                        text = Path(arguments['file_path']).read_text()
            answered.append({'type': 'tool_result', 'tool_use_id': identifier, 'is_error': not allowed, 'content': text})
        raise AssertionError('synthetic CLI did not settle')
    monkeypatch.setattr(probes, 'serve', fixture_provider)
    monkeypatch.setattr(probes, '_drive', drive)
    monkeypatch.setattr(probes, '_sandbox', lambda *_: ['/fake-sandbox', '-p', 'deny'])
    context = ProbeContext(Identity('claude', '9.9.9', '/fake-claude', '0' * 64, sys.platform), feature, str(tmp_path))
    return probes.claude_probe(context)


@pytest.mark.parametrize('mode', ['provider_failure', 'grant_failure', 'changed_denial_text'])
def test_claude_approval_success_survives_unrelated_grant_or_text_failure(tmp_path, monkeypatch, mode):
    result = _scripted_claude(monkeypatch, tmp_path, mode, 'caller_approval_tools')
    assert result.ok, result.reason
    assert result.facts == {'caller_approval_tools': ['Bash', 'Read']}
    assert result.features['caller_write_tools'] == {'caller_write_tools': ['Write', 'Edit']}
    if mode == 'provider_failure':
        assert set(result.failures) == {'allowed_commands', 'denial_texts'}
    elif mode == 'grant_failure':
        assert 'allowed_commands' in result.failures
        assert result.features['denial_texts'] == {'denial_texts': True}
    else:
        assert result.failures.keys() == {'denial_texts'}
        assert result.features['allowed_commands'] == {'allowed_commands': True}
    for name, facts in result.features.items():
        assert set(facts) == {name}, 'unrelated feature facts must not leak into a success row'


def test_failed_requested_feature_keeps_independent_sibling_successes(tmp_path, monkeypatch):
    result = _scripted_claude(monkeypatch, tmp_path, 'changed_denial_text', 'denial_texts')
    assert not result.ok and result.facts == {}
    assert 'denial_texts' in result.failures
    assert result.features['caller_approval_tools'] == {'caller_approval_tools': ['Bash', 'Read']}
    assert result.features['allowed_commands'] == {'allowed_commands': True}


def test_path_prompt_reason_drift_does_not_poison_approved_actions(tmp_path, monkeypatch):
    result = _scripted_claude(monkeypatch, tmp_path, 'changed_path_reason', 'path_safety_prompts')
    assert not result.ok and 'path_safety_prompts' in result.failures
    assert 'caller_approval_tools' in result.features and 'caller_write_tools' in result.features


def test_codex_command_success_survives_independent_patch_protocol_failure(tmp_path, monkeypatch):
    from harflow_mcp.backends import qualification_probes as probes
    def group(context, base, env, prefix, feature):
        if feature == 'caller_write_tools':
            raise ValueError('new patch request schema')
        return {feature: ['commandExecution'], 'platforms': ['darwin']}
    monkeypatch.setattr(probes, '_codex_group', group)
    context = ProbeContext(Identity('codex', '9.9.9', '/fake', '0' * 64, sys.platform), 'caller_approval_tools', str(tmp_path))
    result = probes._codex(context, tmp_path, {}, [])
    assert result.ok and 'caller_write_tools' in result.failures
    assert result.features == {'caller_approval_tools': {'caller_approval_tools': ['commandExecution'], 'platforms': ['darwin']}}
