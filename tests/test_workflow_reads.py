"""Read transport extraction preserves authority and avoids MCP initialization."""
import json
import os
import subprocess
import sys
import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import ctl, lineage, server, workflow_inspection, workflow_reads, workflows
from harflow_mcp import workflow_run_documents as documents
from harflow_mcp import state_ownership
from harflow_mcp.state_layout import resolve_layout


@pytest.fixture
def tmp_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("workflow-reads") / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        yield root


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
from harflow_mcp import ctl, tasks
class ForbiddenRegistry:
    def __init__(self, *args, **kwargs):
        raise AssertionError("read constructed TaskRegistry")
tasks.TaskRegistry = ForbiddenRegistry
ctl.main(__import__('json').loads(sys.argv[1]) + ['--json'])
assert 'harflow_mcp.server' not in sys.modules
assert not any(name == 'mcp' or name.startswith('mcp.') for name in sys.modules)
'''
    environment = {**os.environ, 'HOME': str(tmp_path), 'PYTHONPATH': str(Path('src').resolve()), 'HARFLOW_OPEN_MONITOR': '0'}
    environment.pop('HARFLOW_TASK_ID', None)
    result = subprocess.run([sys.executable, '-c', code, json.dumps(command)], env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['v'] == 1


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
    monkeypatch.setattr(lineage, 'detect_caller', lambda *args, **kwargs: None)
    monkeypatch.setattr(lineage, 'detect_caller_detail', lambda *args, **kwargs: SimpleNamespace(caller=None, undecidable=undecidable))
    with pytest.raises(ValueError, match=message):
        await workflow_reads.call('list', directory=tmp_path / 'tasks')


@pytest.mark.parametrize('command', [
    ['workflow-status', 'owned', '--monitor-view', '--snapshot'],
    ['workflow-detail', 'owned', '--view', 'definition', '--monitor-view'],
])
def test_monitor_flag_does_not_grant_human_snapshot_authority(monkeypatch, tmp_path, capsys, command):
    from harflow_mcp.catalog import Catalog
    from harflow_mcp import store
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
    from harflow_mcp import store, workflows
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
            'status': 'running', 'created_at': 0.0, 'tasks': [{'task_id': 'owned-caller', 'status': 'reserved'}],
        }])
    storage.update_run(run_id, reserve, 'synthetic_authority')
    directory = storage.root / 'tasks'
    store.bootstrap_catalog(directory)
    caller = SimpleNamespace(record=SimpleNamespace(task_id='owned-caller'))
    monkeypatch.setattr(lineage, 'detect_caller', lambda *args, **kwargs: caller)
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
    from harflow_mcp import store, workflows
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
    monkeypatch.setattr(lineage, 'detect_caller', lambda *args, **kwargs: None)
    monkeypatch.setattr(lineage, 'detect_caller_detail', lambda *args, **kwargs: detection)
    monkeypatch.setattr(lineage, 'detect_catalog_caller', lambda _: detection)
    assert await workflow_reads.call('list', directory=directory, _bounded_read=bounded) == [saved]


@pytest.mark.parametrize('action,kwargs', [
    ('status', {'run_id': 'run-id'}),
    ('detail', {'run_id': 'run-id', 'view': 'prompt'}),
])
async def test_direct_status_and_detail_read_final_atomic_replacement(tmp_path, monkeypatch, action, kwargs):
    storage = workflows.WorkflowStore(tmp_path)
    definition = storage.save('atomic-read', {
        'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'end', 'type': 'end'}],
        'connections': [{'id': 'finish', 'source': 'start', 'target': 'end'}],
    })
    run = storage.create_run(definition, 'atomic read', tmp_path)
    run_id = run['workflow_run_id']
    path = storage.runs / f'{run_id}.json'
    original_fdopen = workflows.os.fdopen
    replaced = False

    class Stream:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): self.stream.__enter__(); return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def read(self, *args, **read_kwargs):
            nonlocal replaced
            data = self.stream.read(*args, **read_kwargs)
            if not replaced:
                replaced = True
                fresh = copy.deepcopy(run)
                fresh['name'] = 'final atomic source'
                fresh['prompt'] = 'final atomic prompt'
                temporary = path.with_suffix('.workflow-read-refresh.tmp')
                temporary.write_text(json.dumps(documents.encode(fresh)))
                temporary.replace(path)
            return data

    monkeypatch.setattr(workflows.os, 'fdopen', lambda fd, *args, **read_kwargs: Stream(original_fdopen(fd, *args, **read_kwargs)))

    async def no_managed_caller():
        return None

    kwargs['run_id'] = run_id
    result = await workflow_reads.call(action, directory=storage.root / 'tasks', reader=no_managed_caller, **kwargs)
    assert result['workflow_run_id'] == run_id
    if action == 'status':
        assert result['name'] == 'final atomic source'
    else:
        assert json.loads(result['chunk']) == 'final atomic prompt'


async def test_managed_orchestrator_goal_history_is_only_paged(tmp_path):
    directory = tmp_path / "tasks"
    directory.mkdir()
    history = [{'revision': i + 1, 'goal': 'é' * 32768, 'created_at': i} for i in range(16)]
    run = {'workflow_run_id': 'owned', 'name': 'feature', 'kind': 'workflow',
           'status': 'paused', 'goal': history[-1]['goal'], 'goal_revision': 16,
           'goal_history': history, 'goal_assessment': {'revision': 16, 'status': 'unmet'},
           'parent_goal': {'workflow_run_id': 'parent', 'revision': 2, 'goal': 'parent constraint'},
           'definition': {}, 'activations': [], 'tasks': [], 'execution_contract': 'delegation'}
    async def reader():
        return ({'role': 'orchestrator'}, run)
    for action in ('status', 'list_runs'):
        result = await workflow_reads.call(action, directory=directory, reader=reader,
                                           **({'run_id': 'owned'} if action == 'status' else {}))
        summary = result if action == 'status' else result[0]
        assert 'goal_history' not in summary
        for key in ('goal', 'goal_revision', 'goal_assessment', 'parent_goal'):
            assert summary[key] == run[key]
    chunks = []
    cursor = None
    while True:
        page = await workflow_reads.call('detail', directory=directory, reader=reader,
                                         run_id='owned', view='goal_history', cursor=cursor, limit=8000)
        assert len(page['chunk']) <= 8000
        chunks.append(page['chunk'])
        cursor = page['next_cursor']
        if cursor is None:
            break
    assert len(chunks) > 1
    assert json.loads(''.join(chunks)) == history


async def test_server_workflow_wrappers_use_selected_registry_root(tmp_path, monkeypatch):
    """All server workflow wrappers must stay on the registry-selected root."""
    from harflow_mcp import workflows
    from harflow_mcp.tasks import TaskRegistry

    selected = tmp_path / "root-a"
    ambient = tmp_path / "root-b"
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(ambient))
    definition = {
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "end", "type": "end"},
        ],
        "connections": [{"id": "finish", "source": "start", "target": "end"}],
    }
    selected_store = workflows.WorkflowStore(root=selected)
    ambient_store = workflows.WorkflowStore(root=ambient)
    selected_definition = selected_store.save("only-a", definition)
    ambient_store.save("only-b", definition)
    run = selected_store.create_run(selected_definition, "selected run", tmp_path)
    registry = TaskRegistry(log_dir=selected / "tasks", owner={})
    monkeypatch.setattr(server, "_registry", registry)

    listed = await server.list_workflows()
    assert [item["name"] for item in listed] == ["only-a"]
    assert (await server.get_workflow("only-a"))["name"] == "only-a"
    runs = await server.list_workflow_runs()
    assert [item["workflow_run_id"] for item in runs["runs"]] == [run["workflow_run_id"]]
    status = await server.get_workflow_status(run["workflow_run_id"])
    assert status["workflow_run_id"] == run["workflow_run_id"]

    await server.save_workflow("saved-a", definition)
    assert (selected / "workflows" / "saved-a.json").is_file()
    assert not (ambient / "workflows" / "saved-a.json").exists()
    assert [item["name"] for item in ambient_store.list()] == ["only-b"]


async def test_server_node_reads_forward_selected_root(monkeypatch, tmp_path):
    from harflow_mcp import workflow_node_reads
    from harflow_mcp.tasks import TaskRegistry

    selected = tmp_path / "root-a"
    ambient = tmp_path / "root-b"
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(ambient))
    registry = TaskRegistry(log_dir=selected / "tasks", owner={})
    monkeypatch.setattr(server, "_registry", registry)

    async def no_managed_reader():
        return None

    observed = {}
    monkeypatch.setattr(server, "_managed_workflow_reader", no_managed_reader)

    def inspect(*args, **kwargs):
        observed["inspect"] = kwargs["root"]
        return {"chunk": "{}", "has_more": False}

    def assigned(*args, **kwargs):
        observed["assigned"] = kwargs["root"]
        return {"chunk": "{}", "has_more": False}

    monkeypatch.setattr(workflow_node_reads, "inspect_node", inspect)
    monkeypatch.setattr(workflow_node_reads, "assigned_input_node", assigned)
    await server.inspect_workflow_node("run", "execution")
    await server.read_workflow_assigned_input("run", "execution")
    assert observed == {"inspect": selected, "assigned": selected}


async def test_goal_history_detail_does_not_expand_worker_authority(tmp_path):
    directory = tmp_path / 'tasks'
    directory.mkdir()
    async def reader():
        return ({'role': 'node'}, {'workflow_run_id': 'owned'})
    with pytest.raises(ValueError, match='Worker nodes cannot inspect workflow context'):
        await workflow_reads.call('detail', directory=directory, reader=reader,
                                  run_id='owned', view='goal_history')
