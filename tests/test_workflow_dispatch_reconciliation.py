import json
from types import SimpleNamespace

import pytest

from harflow_mcp import ctl, workflows as w
from harflow_mcp import state_ownership
from harflow_mcp.state_layout import resolve_layout
from test_workflows import definition


@pytest.fixture(autouse=True)
def owned_cli_state(tmp_path, monkeypatch):
    root = tmp_path / "state"
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))
    with state_ownership.state_session(resolve_layout(root=root)):
        pass


def reserved(storage, tmp_path, stage='spawn_requested'):
    run = storage.create_run(w.validate_definition(definition()), 'go', tmp_path)
    def seed(value):
        task = {'task_id': 'missing', 'status': 'uncertain'}
        if stage is not None:
            task['dispatch_stage'] = stage
        value.update(status='needs_attention', activations=[{'id':'execution', 'node_id':'work', 'role':'node', 'status':'running', 'created_at': 0.0, 'tasks':[task]}])
    return storage.update_run(run['workflow_run_id'], seed, 'fixture')


@pytest.mark.parametrize('stage,expected', [('preparing','not_started'), ('spawn_requested','uncertain'), (None,'uncertain')])
def test_pre_spawn_recovery_requires_durable_proof(tmp_path, monkeypatch, stage, expected):
    storage = w.WorkflowStore(tmp_path / "state")
    run = reserved(storage, tmp_path, stage)
    monkeypatch.setattr('harflow_mcp.workflow_delegation.reconcile_delegation', lambda r: None)
    recovered = storage.reconcile_run(run['workflow_run_id'])
    assert recovered['activations'][0]['tasks'][0]['status'] == expected


def test_human_abandon_records_reason_without_scheduling(tmp_path, monkeypatch):
    storage = w.WorkflowStore(tmp_path / "state")
    run = reserved(storage, tmp_path)
    result = storage.abandon_dispatch(run['workflow_run_id'], 'execution', 'missing', 'Verified no process survived', True)
    assert result['status'] == 'needs_attention'
    assert result['settling'] is False
    task = result['activations'][0]['tasks'][0]
    assert task['reconciliation']['confirmed_no_process'] is True
    assert task['status'] == 'not_started'
    assert result['attempt_grants'] == {}
    event = json.loads((storage.runs / (run['workflow_run_id']+'.jsonl')).read_text().splitlines()[-1])
    assert event['event'] == 'dispatch_abandoned'


@pytest.mark.parametrize('case', ['live', 'record', 'reason', 'confirm', 'wrong_task'])
def test_abandon_refuses_without_positive_human_reconciliation(tmp_path, monkeypatch, case):
    storage = w.WorkflowStore(tmp_path / "state")
    run = reserved(storage, tmp_path)
    if case == 'live':
        monkeypatch.setattr(w, '_supervisor_present', lambda r: True)
    if case == 'record':
        monkeypatch.setattr(w.task_store, 'read', lambda *a: SimpleNamespace(status='running'))
    from harflow_mcp import scratch
    path = scratch.create(storage.root / 'tasks', 'missing')
    (path / 'artifact').write_text('keep unresolved evidence')
    before = (storage.runs/(run['workflow_run_id']+'.json')).read_bytes()
    with pytest.raises(w.WorkflowError):
        storage.abandon_dispatch(run['workflow_run_id'], 'execution', 'wrong' if case=='wrong_task' else 'missing', '' if case=='reason' else 'Confirmed stopped', case!='confirm')
    assert (storage.runs/(run['workflow_run_id']+'.json')).read_bytes() == before
    assert (path / 'artifact').read_text() == 'keep unresolved evidence'


def test_abandon_cli_refuses_agent_caller(monkeypatch, capsys, tmp_path):
    from harflow_mcp import server, takeover
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=tmp_path / "state" / "tasks"))
    monkeypatch.setattr(takeover, 'caller_refusal', lambda p: ('agent_caller', 'managed task'))
    result = ctl.main(['workflow-abandon-dispatch','run','execution','task','--reason','checked','--confirm-no-process','--json'])
    assert result != 0
    assert 'Only a verified human' in capsys.readouterr().out


@pytest.mark.parametrize('proven', [True, False])
async def test_dispatch_failure_distinguishes_proven_not_started(tmp_path, monkeypatch, proven):
    from test_workflows import FakeRegistry
    monkeypatch.setattr(w.backends, 'is_installed', lambda b: True)
    storage = w.WorkflowStore(tmp_path / "state")
    d = definition()
    d['nodes'][1]['freedom'] = 'read_only'
    run = storage.create_run(w.validate_definition(d), 'go', tmp_path)
    registry = FakeRegistry(tmp_path / "state", [])
    async def fail(*a, **kw):
        exc = OSError('exec refused')
        if proven:
            exc.harflow_not_started = True
        raise exc
    registry.start = fail
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = run['workflow_run_id']
    storage.update_run(run['workflow_run_id'], lambda r: r.update(status='running'), 'fixture')
    node = run['definition']['nodes'][1]
    activation = supervisor._activation('work', 'node')
    assert await supervisor._dispatch(node, 'assignment', 'node', activation) is None
    task = storage.get_run(run['workflow_run_id'])['activations'][0]['tasks'][0]
    assert task['status'] == ('not_started' if proven else 'uncertain')
    assert task['dispatch_stage'] == 'spawn_requested'


@pytest.mark.parametrize('duplicate', [True, False])
async def test_registry_marks_only_proven_exec_refusal(tmp_path, monkeypatch, duplicate):
    from harflow_mcp import tasks
    from harflow_mcp.backends.codex import CodexBackend
    registry = tasks.TaskRegistry(log_dir=tmp_path / 'state' / 'tasks', open_monitor=False)
    backend = CodexBackend()
    invocation = backend.build_start_argv('work', repo=tmp_path, freedom='read_only', session_id=None, model=None, max_turns=None, reasoning_effort=None)
    async def failed_exec(*a, **kw):
        raise OSError('binary unavailable')
    monkeypatch.setattr(tasks.asyncio, 'create_subprocess_exec', failed_exec)
    if duplicate:
        monkeypatch.setattr(registry, 'get', lambda task_id: object())
    with pytest.raises(ValueError if duplicate else OSError) as caught:
        await registry._spawn(invocation, backend=backend, prompt='work', repo_path=tmp_path,
            session_id=None, freedom='read_only', max_turns=None, model=None,
            reasoning_effort=None, task_id='known')
    assert caught.value.harflow_not_started is True
    assert not (tmp_path/'state'/'tasks'/'known.meta.json').exists()

@pytest.mark.parametrize('operation', ['reconcile', 'abandon'])
def test_recordless_answer_recovery_updates_question_delivery(tmp_path, monkeypatch, operation):
    storage = w.WorkflowStore(tmp_path / "state")
    run = reserved(storage, tmp_path, 'preparing')
    def question(r):
        a = r['activations'][0]
        a['questions'] = [{'question_id': 'question', 'reply_task_id': 'missing', 'status': 'answered', 'answer_delivery_state': 'reserved'}]
    storage.update_run(run['workflow_run_id'], question, 'fixture')
    monkeypatch.setattr('harflow_mcp.workflow_delegation.reconcile_delegation', lambda r: None)
    if operation == 'reconcile':
        result = storage.reconcile_run(run['workflow_run_id'])
    else:
        result = storage.abandon_dispatch(run['workflow_run_id'], 'execution', 'missing', 'Confirmed stopped', True)
    assert result['activations'][0]['questions'][0]['answer_delivery_state'] == 'not_started'

@pytest.mark.parametrize('state', ['not_started', 'reserved', 'running', 'uncertain', 'completed'])
def test_delegation_repairs_existing_stale_answer_delivery(state):
    from harflow_mcp import workflow_delegation as d
    from test_workflow_delegation import graph
    question = {'question_id': 'q', 'origin_task_id': 'origin', 'reply_task_id': 'reply', 'status': 'answered', 'answer_delivery_state': 'reserved'}
    reply = {'task_id': 'reply', 'status': state}
    if state == 'completed':
        reply['result'] = {'status': 'completed', 'summary': 'observed reply'}
    activation = {'id': 'a', 'node_id': 'work', 'role': 'node', 'status': 'waiting_for_answer', 'token': {'id': 't'}, 'pending_question_id': 'q', 'questions': [question], 'tasks': [reply]}
    token = {'id': 't', 'node_id': 'work', 'execution_activation_id': 'a'}
    run = {'activations': [activation], 'pending': [token], 'definition': w.validate_definition(graph())}
    d.reconcile_delegation(run)
    assert question['answer_delivery_state'] == ('settled' if state == 'completed' else state)
    if state == 'completed':
        assert token['recovered_result'] == reply['result']
    else:
        assert 'recovered_result' not in token


def test_abandon_removes_recordless_scratch_only_after_durable_confirmation(tmp_path, monkeypatch):
    from harflow_mcp import scratch
    storage = w.WorkflowStore(tmp_path / "state")
    run = reserved(storage, tmp_path)
    path = scratch.create(storage.root / 'tasks', 'missing')
    (path / 'artifact').write_text('ambiguous dispatch artifact')
    remove = scratch.remove
    def after_commit(log_dir, task_id):
        persisted = storage.get_run(run['workflow_run_id'])
        assert persisted['activations'][0]['tasks'][0]['status'] == 'not_started'
        assert persisted['activations'][0]['tasks'][0]['reconciliation']['confirmed_no_process']
        remove(log_dir, task_id)
    monkeypatch.setattr(scratch, 'remove', after_commit)
    storage.abandon_dispatch(run['workflow_run_id'], 'execution', 'missing', 'Confirmed stopped', True)
    assert not path.exists()


def test_abandon_failed_persistence_preserves_ambiguous_scratch(tmp_path, monkeypatch):
    from harflow_mcp import scratch
    storage = w.WorkflowStore(tmp_path / "state")
    run = reserved(storage, tmp_path)
    path = scratch.create(storage.root / 'tasks', 'missing')
    (path / 'artifact').write_text('keep until confirmed')
    def fail(*args, **kwargs):
        raise OSError('Persistence unavailable')
    monkeypatch.setattr(storage, 'update_run', fail)
    with pytest.raises(OSError, match='Persistence unavailable'):
        storage.abandon_dispatch(run['workflow_run_id'], 'execution', 'missing', 'Confirmed stopped', True)
    assert (path / 'artifact').read_text() == 'keep until confirmed'


def test_abandon_cleanup_error_does_not_reverse_durable_reconciliation(tmp_path, monkeypatch, caplog):
    from harflow_mcp import scratch
    storage = w.WorkflowStore(tmp_path / "state")
    run = reserved(storage, tmp_path)
    def fail(*args):
        raise OSError('Scratch cleanup unavailable')
    monkeypatch.setattr(scratch, 'remove', fail)
    result = storage.abandon_dispatch(run['workflow_run_id'], 'execution', 'missing', 'Confirmed stopped', True)
    assert result['activations'][0]['tasks'][0]['status'] == 'not_started'
    assert 'scratch' in caplog.text.lower()
