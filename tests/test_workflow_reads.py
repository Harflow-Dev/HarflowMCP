"""Read transport extraction preserves authority and avoids MCP initialization."""
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from polybridge import ctl, lineage, server, workflow_inspection, workflow_reads


@pytest.mark.parametrize('command', [
    ['workflow-list'], ['workflow-list', '--monitor-view'], ['workflow-list-runs'],
    ['workflow-list-page'], ['workflow-get', 'missing'], ['workflow-status', 'missing'],
    ['workflow-status', 'missing', '--monitor-view'],
    ['workflow-status', 'missing', '--monitor-view', '--snapshot'],
    ['workflow-detail', 'missing', '--view', 'definition'],
    ['workflow-detail', 'missing', '--view', 'definition', '--monitor-view'],
])
def test_cli_reads_never_initialize_server_or_registry(tmp_path, command):
    code = '''
import sys
from polybridge import ctl, tasks
class ForbiddenRegistry:
    def __init__(self, *args, **kwargs):
        raise AssertionError("read constructed TaskRegistry")
tasks.TaskRegistry = ForbiddenRegistry
ctl.main(__import__('json').loads(sys.argv[1]) + ['--json'])
assert 'polybridge.server' not in sys.modules
assert not any(name == 'mcp' or name.startswith('mcp.') for name in sys.modules)
'''
    environment = {**os.environ, 'HOME': str(tmp_path), 'PYTHONPATH': str(Path('src').resolve()), 'PB_OPEN_MONITOR': '0'}
    environment.pop('PB_TASK_ID', None)
    result = subprocess.run([sys.executable, '-c', code, json.dumps(command)], env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['v'] == 9


@pytest.mark.parametrize('role', ['builder', 'orchestrator', 'node'])
@pytest.mark.parametrize('action,kwargs', [
    ('get', {'name': 'feature'}), ('list', {}), ('list_runs', {}),
    ('status', {'run_id': 'owned'}), ('status', {'run_id': 'other'}),
    ('detail', {'run_id': 'owned', 'view': 'definition'}),
    ('detail', {'run_id': 'owned', 'view': 'executions'}),
    ('detail', {'run_id': 'owned', 'view': 'builder_draft'}),
])
async def test_mcp_and_cli_shared_authority_parity(monkeypatch, tmp_path, role, action, kwargs):
    run = {'workflow_run_id': 'owned', 'name': 'feature', 'kind': 'builder' if role == 'builder' else 'workflow',
           'status': 'running', 'definition': {}, 'activations': [], 'tasks': [], 'execution_contract': 'delegation'}
    managed = ({'role': role}, run)
    async def reader():
        return managed
    monkeypatch.setattr(server, '_managed_workflow_reader', reader)
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=tmp_path / 'tasks'))
    monkeypatch.setattr(workflow_inspection, 'managed_reader', lambda _: managed)
    async def outcome(awaitable):
        try:
            return ('result', await awaitable)
        except Exception as exc:
            return ('error', str(exc))
    assert await outcome(server._workflow_call(action, **kwargs)) == await outcome(workflow_reads.call(action, directory=tmp_path / 'tasks', **kwargs))


@pytest.mark.parametrize('identity,undecidable,message', [
    ('unverified', None, 'Workflow caller task identity cannot be verified'),
    (None, 'ps denied', 'Workflow caller authority is undecidable: ps denied'),
])
async def test_ordinary_cli_authority_fails_closed(monkeypatch, tmp_path, identity, undecidable, message):
    if identity:
        monkeypatch.setenv(lineage.ENV_TASK_ID, identity)
    else:
        monkeypatch.delenv(lineage.ENV_TASK_ID, raising=False)
    monkeypatch.setattr(lineage, 'detect_caller', lambda _: None)
    monkeypatch.setattr(lineage, 'detect_caller_detail', lambda _: SimpleNamespace(caller=None, undecidable=undecidable))
    with pytest.raises(ValueError, match=message):
        await workflow_reads.call('list', directory=tmp_path / 'tasks')


@pytest.mark.parametrize('command', [
    ['workflow-status', 'owned', '--monitor-view', '--snapshot'],
    ['workflow-detail', 'owned', '--view', 'definition', '--monitor-view'],
])
def test_monitor_flag_does_not_grant_human_snapshot_authority(monkeypatch, tmp_path, capsys, command):
    from polybridge.catalog import Catalog
    from polybridge import store
    directory = tmp_path / 'tasks'
    store.bootstrap_catalog(directory)
    assert Catalog(directory, store.RECORD_SUFFIX).ready()
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: directory)
    monkeypatch.setattr(lineage, 'detect_catalog_caller', lambda _: SimpleNamespace(caller=object(), undecidable=None))
    assert ctl.main(command + ['--json']) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload['error']['code'] == 'workflow_error'
    assert 'only available to the local Monitor' in payload['error']['message']


@pytest.mark.parametrize('role', ['builder', 'orchestrator', 'node'])
@pytest.mark.parametrize('bounded', [False, True])
async def test_real_ownership_receipt_enforces_expected_read_scope(monkeypatch, tmp_path, role, bounded):
    """Exercise both authority readers without replacing ownership resolution."""
    from polybridge import store, workflows
    monkeypatch.setenv('HOME', str(tmp_path))
    storage = workflows.WorkflowStore()
    definition = storage.save('owned-graph', {
        'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'end', 'type': 'end'}],
        'connections': [{'id': 'finish', 'source': 'start', 'target': 'end'}],
    })
    run = storage.create_run(definition, 'synthetic assignment', tmp_path)
    run_id = run['workflow_run_id']
    def reserve(value):
        value.update(status='running', execution_contract='delegation', activations=[{
            'id': 'owned-activation', 'node_id': 'owned-node', 'role': role,
            'status': 'running', 'tasks': [{'task_id': 'owned-caller', 'status': 'reserved'}],
        }])
    storage.update_run(run_id, reserve, 'synthetic_authority')
    directory = storage.root / 'tasks'
    store.bootstrap_catalog(directory)
    caller = SimpleNamespace(record=SimpleNamespace(task_id='owned-caller'))
    monkeypatch.setattr(lineage, 'detect_caller', lambda _: caller)
    monkeypatch.setattr(lineage, 'detect_catalog_caller', lambda _: SimpleNamespace(caller=caller, undecidable=None))
    # The only stub is verified process detection. Durable receipt/run matching is real.
    assert storage.task_owner('owned-caller', strict=True)['role'] == role
    if role == 'node':
        with pytest.raises(ValueError, match='Worker nodes cannot inspect workflow context'):
            await workflow_reads.call('status', directory=directory, run_id=run_id, _bounded_read=bounded)
        with pytest.raises(ValueError, match='Worker nodes cannot inspect workflow context'):
            await workflow_reads.call('list_run_page', directory=directory, limit=100)
    else:
        result = await workflow_reads.call('status', directory=directory, run_id=run_id, _bounded_read=bounded)
        assert result['workflow_run_id'] == run_id
        assert result['status'] == 'running'
        message = 'Builders' if role == 'builder' else 'Orchestrators'
        with pytest.raises(ValueError, match=message + ' may only inspect their own workflow run'):
            await workflow_reads.call('status', directory=directory, run_id='unowned', _bounded_read=bounded)
        page = await workflow_reads.call('list_run_page', directory=directory, limit=100)
        assert [item['workflow_run_id'] for item in page['items']] == [run_id]
        assert page['next_cursor'] is None


@pytest.mark.parametrize('bounded', [False, True])
async def test_verified_human_reads_real_saved_definition(monkeypatch, tmp_path, bounded):
    from polybridge import store, workflows
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.delenv(lineage.ENV_TASK_ID, raising=False)
    storage = workflows.WorkflowStore()
    saved = storage.save('human-graph', {
        'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'end', 'type': 'end'}],
        'connections': [{'id': 'finish', 'source': 'start', 'target': 'end'}],
    })
    directory = storage.root / 'tasks'
    store.bootstrap_catalog(directory)
    detection = SimpleNamespace(caller=None, undecidable=None)
    monkeypatch.setattr(lineage, 'detect_caller', lambda _: None)
    monkeypatch.setattr(lineage, 'detect_caller_detail', lambda _: detection)
    monkeypatch.setattr(lineage, 'detect_catalog_caller', lambda _: detection)
    assert await workflow_reads.call('list', directory=directory, _bounded_read=bounded) == [saved]
