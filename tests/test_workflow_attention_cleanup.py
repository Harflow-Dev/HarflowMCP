"""Resolved suspension presentation is retired without losing evidence."""
import copy

import pytest

from harflow_mcp import workflow_invocation as inv, workflow_reads, workflows as w
from test_workflow_delegation import Registry, graph, run_flow, storage  # noqa: F401
from test_workflow_optional_refusal import ANSWER, suspended
from test_workflow_run_node_recovery import finished_child, seed_parent, store, write_run  # noqa: F401

FIELDS = dict(attention_reason='Old permission refusal', attention_source={'workflow_run_id': 'old'},
              input_question='Allow retry?', input_question_items=['yes'], input_decision_id='old',
              input_source={'workflow_run_id': 'old'}, optional_review_skip_available=True)


@pytest.fixture(autouse=True)
def owned_child_scope(request):
    if 'store' in request.fixturenames:
        from harflow_mcp.state_ownership import state_session
        with state_session(request.getfixturevalue('store').state_layout):
            yield
    else:
        yield


@pytest.mark.parametrize('before', ['needs_input', 'needs_attention', 'paused', 'failed'])
@pytest.mark.parametrize('after', ['running', 'building', 'starting', 'pending'])
def test_reactivation_retires_only_carried_suspension_fields(storage, tmp_path, before, after):
    run = storage.create_run(w.validate_definition(graph()), 'Request', tmp_path)
    rid = run['workflow_run_id']
    history = {'old': {'accepted_at': 1}}
    storage.update_run(rid, lambda r: r.update(status=before, **copy.deepcopy(FIELDS), accepted_inputs=history), 'suspend')
    result = storage.update_run(rid, lambda r: r.update(status=after, input_question='Fresh question'), 'reactivate')
    assert result['input_question'] == 'Fresh question'
    assert not (set(FIELDS) - {'input_question'}) & result.keys()
    assert result['accepted_inputs'] == history
    assert 'attention_checkpoint' not in result


def test_unrelated_and_new_suspensions_keep_presentation(storage, tmp_path):
    run = storage.create_run(w.validate_definition(graph()), 'Request', tmp_path)
    rid = run['workflow_run_id']
    storage.update_run(rid, lambda r: r.update(status='needs_attention', **copy.deepcopy(FIELDS)), 'suspend')
    polled = storage.update_run(rid, lambda r: r.update(instructions='Inspecting'), 'poll')
    assert all(polled[key] == value for key, value in FIELDS.items())
    fresh = storage.update_run(rid, lambda r: r.update(status='needs_input', attention_reason='New refusal'), 'new_input')
    assert fresh['attention_reason'] == 'New refusal'


async def test_permission_answer_clears_presentation_and_preserves_refusal_evidence(storage, tmp_path, monkeypatch):
    run, registry = await suspended(storage, tmp_path)
    assert run['attention_reason']
    history = copy.deepcopy(run['activations'])
    resumed = storage.control(run['workflow_run_id'], 'resume', ANSWER,
                              decision_id=run['input_decision_id'], allow_optional_review_skip=True)
    assert resumed['status'] == 'running'
    assert not set(FIELDS) & resumed.keys()
    assert resumed['activations'] == history
    assert run['input_decision_id'] in resumed['accepted_inputs']
    assert resumed['optional_skip_authorizations']
    from types import SimpleNamespace
    from harflow_mcp import server
    async def caller():
        return None
    async def read(action, **kwargs):
        return await workflow_reads.call(action, directory=storage.root / 'tasks', reader=caller, **kwargs)
    monkeypatch.setattr(server, '_workflow_call', read)
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=storage.root / 'tasks'))
    waiting = await server.wait_for_workflow(run['workflow_run_id'], timeout_seconds=0)
    assert waiting['status'] == 'running'
    assert waiting['stop_reason'] == 'timeout'
    assert 'attention_reason' not in waiting
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    assert storage.get_run(run['workflow_run_id'])['status'] == 'completed'


async def test_failed_decision_recovery_clears_attention_preserves_checkpoint(storage, tmp_path):
    def invalid(context, registry):
        return {'decision_id': 'wrong', 'action': 'failed', 'reason': 'Invalid decision'}
    run, _ = await run_flow(storage, tmp_path, registry=Registry(storage.root, invalid))
    assert run['status'] == 'failed'
    storage.update_run(run['workflow_run_id'], lambda r: r.update(attention_reason='Previous refusal'), 'old_reason')
    recovered = storage.control(run['workflow_run_id'], 'recover', 'Inspected; retry')
    assert recovered['status'] == 'running'
    assert 'attention_reason' not in recovered
    assert recovered['pending'][0]['decision_id'] == run['pending'][0]['decision_id']
    assert recovered['activations'] == run['activations']


def test_forwarded_child_attention_resume_clears_both_runs(store, tmp_path):
    parent = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = finished_child(store, parent, parent['activations'][0], status='paused')
    child['attention_reason'] = 'Child needs confirmation'
    write_run(store, child)
    inv.publish_attention(store, child)
    result = store.control(parent['workflow_run_id'], 'resume', 'Resume child', 2)
    for run in (result, store.get_run(child['workflow_run_id'])):
        assert run['status'] == 'running'
        assert not set(FIELDS) & run.keys()
    assert store.get_run(child['workflow_run_id'])['transition_grant'] == 2


@pytest.mark.parametrize('child_status', ['running', 'needs_input', 'needs_attention'])
def test_legacy_projection_clears_stale_reason_but_preserves_live_child_attention(store, tmp_path, child_status):
    parent = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = finished_child(store, parent, parent['activations'][0], status=child_status)
    child.update(input_decision_id='live', input_question='Current child question', attention_reason='Current child failure')
    write_run(store, child)
    stale = store.update_run(parent['workflow_run_id'], lambda r: r.update(**copy.deepcopy(FIELDS)), 'legacy')
    view = workflow_reads.on_demand_projection(store, stale)
    if child_status == 'running':
        assert view['status'] == 'running'
        assert not set(FIELDS) & view.keys()
    else:
        assert view['status'] == child_status
        assert child['name'] in view['attention_reason']
        assert view['attention_reason'] != FIELDS['attention_reason']
        if child_status == 'needs_input':
            assert view['input_question'] == 'Current child question'
    assert store.get_run(parent['workflow_run_id'])['attention_reason'] == FIELDS['attention_reason']
