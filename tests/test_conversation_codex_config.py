"""Effective config witness fixtures never invoke a provider CLI."""
import copy
import json
import sys
import pytest
from harflow_mcp.backends.conversation import ConversationRefused
from harflow_mcp.backends.conversation_codex_config import filter_configuration, read_configuration
from harflow_mcp.backends.conversation_native import CodexConversationPort


def config():
    return {'config': {'secret': 'not-retained', 'sandbox_workspace_write': {
        'writable_roots': ['/cache'], 'network_access': False,
        'exclude_slash_tmp': True, 'exclude_tmpdir_env_var': False}},
        'origins': {'sandbox_workspace_write': {'name': {'type': 'user', 'file': '/config.toml', 'profile': None}, 'version': 'generation'}}}


def test_filter_retains_exact_workspace_and_source_only():
    result = filter_configuration(config())
    assert 'secret' not in json.dumps(result)
    assert result['workspace']['writable_roots'] == ['/cache']
    for key in config()['config']['sandbox_workspace_write']:
        malformed = config()
        del malformed['config']['sandbox_workspace_write'][key]
        with pytest.raises(ConversationRefused): filter_configuration(malformed)
    with pytest.raises(ConversationRefused): filter_configuration({'config': config()['config'], 'origins': {}})


@pytest.mark.parametrize('agent', [
    'codex/0.166.0',
    'Codex Desktop/0.166.0 (Mac OS 26.6.2; arm64) dumb (harflow_config_witness; 1)',
])
def test_helper_only_config_requests_and_physical_eof(tmp_path, agent):
    script = tmp_path / 'peer.py'
    script.write_text('import sys,json\n'
        'for line in sys.stdin:\n'
        ' f=json.loads(line);m=f["method"]\n'
        f' if m=="initialize": r={{"userAgent":{agent!r}}}\n'
        ' elif m=="initialized": continue\n'
        ' elif m=="config/read":\n'
        f'  assert f["params"]=={{"cwd":{str(tmp_path)!r},"includeLayers":False}}\n'
        f'  r={config()!r}\n'
        ' else: raise RuntimeError("unexpected model/thread request")\n'
        ' print(json.dumps({"id":f["id"],"result":r}),flush=True)\n')
    assert read_configuration([sys.executable, str(script)], cwd=str(tmp_path), env={}, version='0.166.0') == filter_configuration(config())


@pytest.mark.parametrize('changed', [False, True])
def test_driver_rechecks_baseline_before_thread(changed):
    witness = filter_configuration(config())
    profile = {'version':'0.166.0','cwd':'/repo','sandbox':'workspace-write','approval_policy':'never',
        'configuration_witness':{'cwd':'/repo','configuration':witness},
        'config':{'sandbox_workspace_write':{**witness['workspace'],'writable_roots':['/cache','/scratch']}}}
    port = CodexConversationPort(profile)
    init = json.loads(port.outgoing_frames()[0])
    port.observe({'id':init['id'],'result':{'userAgent':'codex/0.166.0'}}, sequence=1)
    frames = [json.loads(f) for f in port.outgoing_frames()]
    assert [f['method'] for f in frames] == ['initialized','config/read']
    observed = config()
    if changed: observed['config']['sandbox_workspace_write']['writable_roots'].append('/unexpected')
    port.observe({'id':frames[-1]['id'],'result':observed}, sequence=2)
    outgoing = [json.loads(f) for f in port.outgoing_frames()]
    if changed:
        assert port.unknown and not outgoing
    else:
        assert outgoing[0]['method']=='thread/start'
        assert outgoing[0]['params']['config']['sandbox_workspace_write']['writable_roots']==['/cache','/scratch']


def test_converter_preserves_effective_roots_and_original_baseline(tmp_path):
    from harflow_mcp.backends.codex import CodexBackend
    from harflow_mcp.backends.conversation_native import conversation_profile
    backend = CodexBackend()
    source = backend.build_start_argv('literal', repo=tmp_path, freedom='write_in_repo',
        session_id=None, model=None, max_turns=None, reasoning_effort=None, network=False)
    source = backend.with_writable_directory(source, tmp_path / 'scratch', 'write_in_repo')
    witness = {'cwd':str(tmp_path), 'configuration':filter_configuration(config())}
    profile = conversation_profile(backend, source, version='0.166.0', freedom='write_in_repo',
        network=False, configuration_witness=witness)
    assert profile['configured_writable_roots'] == ['/cache']
    assert profile['config']['sandbox_workspace_write']['writable_roots'] == ['/cache',str(tmp_path/'scratch')]
    assert not any('writable_roots=' in token for token in profile['argv'])
    assert profile['config']['sandbox_workspace_write']['exclude_slash_tmp'] is True


@pytest.mark.parametrize('agent', [
    'Codex Desktop/0.167.0 (Mac OS 26.6.2; arm64)',
    'other prefix Codex Desktop/0.166.0 (Mac OS)',
    'Codex Desktop/0.166.0suffix (Mac OS)',
])
def test_helper_mismatched_or_unrecognized_agent_preserves_refusal(tmp_path, agent):
    script = tmp_path / 'wrong_version.py'
    script.write_text('import sys,json\n'
        'for line in sys.stdin:\n'
        ' f=json.loads(line)\n'
        f' print(json.dumps({{"id":f["id"],"result":{{"userAgent":{agent!r}}}}}),flush=True)\n')
    with pytest.raises(ConversationRefused, match='configuration witness version mismatch'):
        read_configuration([sys.executable, str(script)], cwd=str(tmp_path), env={}, version='0.166.0')
