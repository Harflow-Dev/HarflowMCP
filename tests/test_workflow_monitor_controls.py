"""Human controls follow engine evidence, never launch origin."""
import copy
import pytest
from harflow_mcp import identity, workflow_monitor_controls as controls
from harflow_mcp.workflow_cancellation import monitor_projection
from harflow_mcp.workflow_responses import monitor
from test_workflow_interactions import pending

@pytest.mark.parametrize('state,reason,allowed', [('alive','start_time_match',False), ('undecidable','ps_failed',False), ('dead','legacy_markers_not_seen',False), ('dead','pid_absent',True)])
def test_caller_control_requires_qualified_death(pending, monkeypatch, state, reason, allowed):
    storage, run = pending
    monkeypatch.setattr(identity, 'check_detail', lambda evidence: (state, reason))
    before = copy.deepcopy(run)
    actions = controls.projection(storage, run)
    for action in ('resume', 'additional_attempts', 'revise_goal'):
        assert actions[action]['allowed'] is allowed
    assert not actions['pause']['allowed']
    assert run == before
    assert 'interaction_handover' not in storage.get_run(run['workflow_run_id'])


def test_monitor_checkpoint_and_failed_recovery(pending):
    storage, run = pending
    run.update(interaction_owner='monitor', status='needs_input', input_decision_id='decision', optional_review_skip_available=True)
    actions = controls.projection(storage, run)
    assert actions['resume']['allowed'] and actions['skip_optional_review']['allowed']
    assert not actions['recover']['allowed']
    run['status'] = 'failed'
    actions = controls.projection(storage, run)
    assert actions['recover']['allowed'] and actions['additional_attempts']['allowed']
    assert not actions['resume']['allowed'] and not actions['skip_optional_review']['allowed']


def test_child_exposes_root_route_without_applying_child_answer(pending):
    storage, run = pending
    child = {**run, 'workflow_run_id':'child', 'parent_link':{'root_workflow_run_id':run['workflow_run_id']}}
    actions = controls.projection(storage, child)
    assert set(actions) == set(controls.ACTIONS)
    assert all(not a['allowed'] and 'root' in a['reason'] for a in actions.values())
    assert all(a['target_workflow_run_id'] == run['workflow_run_id'] for a in actions.values())


@pytest.mark.parametrize('update,expected', [({'settling':True},'settling'), ({'activations':[{'status':'uncertain','tasks':[]}]},'Unresolved')])
def test_refresh_disables_controls_when_execution_changes(pending, update, expected):
    storage, run = pending
    run['interaction_owner'] = 'monitor'
    assert controls.projection(storage, run)['resume']['allowed']
    run.update(update)
    action = controls.projection(storage, run)['resume']
    assert not action['allowed'] and expected in action['reason']


def test_unknown_scheduler_keeps_reason_and_compact_evidence(pending, monkeypatch):
    storage, run = pending
    run.update(interaction_owner='monitor', supervisor_identity={'pid':42,'start_time':'unknown','markers':[]})
    monkeypatch.setattr(identity, 'identity_check', lambda evidence:'undecidable')
    action = monitor(monitor_projection(storage, run))['monitor_controls']['resume']
    assert not action['allowed'] and 'uncertain' in action['reason']
    assert action['target_workflow_run_id'] == run['workflow_run_id']


def test_message_handover_and_returning_caller_refusal(pending, monkeypatch):
    from harflow_mcp import workflow_interactions as i, workflows
    storage, run = pending
    monkeypatch.setattr(identity, 'check_detail', lambda evidence:('dead','pid_absent'))
    storage.update_run(run['workflow_run_id'], lambda value:i.authorize(storage,value,'monitor','message',pending=True), 'message')
    claimed = storage.get_run(run['workflow_run_id'])
    assert claimed['interaction_owner'] == 'monitor'
    i.validate_handover(claimed['interaction_handover'])
    with pytest.raises(workflows.WorkflowError, match='original caller'):
        i.authorize(storage,claimed,'caller','message',pending=True)


@pytest.mark.parametrize('status', ['starting', 'running', 'cancelling'])
def test_interrupted_scheduler_recovery_matches_control_without_attempt_grant(pending, monkeypatch, status):
    from harflow_mcp import workflows
    storage, run = pending
    rid = run['workflow_run_id']
    run = storage.update_run(rid, lambda value: value.update(
        status=status, interaction_owner='monitor',
        supervisor_identity={'pid': 999999, 'start_time': 'gone', 'markers': []}), 'fixture')
    monkeypatch.setattr(identity, 'identity_check', lambda evidence: 'dead')
    launches = []
    monkeypatch.setattr(workflows, '_launch', lambda storage, identifier: launches.append(identifier))
    before = copy.deepcopy(run)
    actions = controls.projection(storage, run)
    assert actions['recover']['allowed']
    assert not actions['additional_attempts']['allowed']
    assert 'cannot grant' in actions['additional_attempts']['reason']
    assert storage.get_run(rid) == before
    with pytest.raises(workflows.WorkflowError, match='cannot grant additional attempts'):
        storage.control(rid, 'recover', 'Restore scheduler', additional_attempts=1, interaction_owner='monitor')
    recovered = storage.control(rid, 'recover', 'Restore scheduler', interaction_owner='monitor')
    assert recovered['scheduler_recovery']['reason'] == 'Restore scheduler'
    assert recovered.get('attempt_grants') == before.get('attempt_grants')
    assert recovered.get('transition_grant') == before.get('transition_grant')
    assert launches == [rid]


@pytest.mark.parametrize('scheduler_state', ['alive', 'undecidable'])
def test_interrupted_scheduler_requires_qualified_death(pending, monkeypatch, scheduler_state):
    storage, run = pending
    run = storage.update_run(run['workflow_run_id'], lambda value: value.update(
        status='running', interaction_owner='monitor',
        supervisor_identity={'pid': 999999, 'start_time': 'unknown', 'markers': []}), 'fixture')
    monkeypatch.setattr(identity, 'identity_check', lambda evidence: scheduler_state)
    action = controls.projection(storage, run)['recover']
    assert not action['allowed'] and 'confirmed owner death' in action['reason']


def test_interrupted_scheduler_rejects_unresolved_task_ownership(pending, monkeypatch):
    storage, run = pending
    run = storage.update_run(run['workflow_run_id'], lambda value: value.update(
        status='running', interaction_owner='monitor',
        supervisor_identity={'pid': 999999, 'start_time': 'gone', 'markers': []},
        activations=[{'id': 'active', 'node_id': 'work', 'role': 'node', 'status': 'running',
                      'created_at': 0.0, 'tasks': [{'task_id': 'missing', 'status': 'uncertain',
                                                 'dispatch_stage': 'spawn_requested'}]}]), 'fixture')
    monkeypatch.setattr(identity, 'identity_check', lambda evidence: 'dead')
    action = controls.projection(storage, run)['recover']
    assert not action['allowed'] and 'task owner is lost or uncertain' in action['reason']


@pytest.fixture
def child_checkpoint(pending, tmp_path):
    from harflow_mcp import state_ownership
    from test_workflow_run_node_execution import child_graph, parent_graph
    from test_workflow_run_node_recovery import seed_parent, finished_child, write_run
    storage, _ = pending
    with state_ownership.state_session(storage.state_layout):
        definition = storage.save('child', child_graph('child'))
        storage.save('parent', parent_graph(definition['workflow_id']))
        root = seed_parent(storage, tmp_path, 'parent', stage='waiting')
        child = finished_child(storage, root, root['activations'][0], status='needs_input')
        child.update(input_question='Question?', input_decision_id='question')
        write_run(storage, child)
        sibling = {'id': 'sibling', 'node_id': 'call', 'role': 'node', 'status': 'running',
                   'created_at': 0.0, 'tasks': [{'task_id': 'sibling-task', 'status': 'running'}]}
        root = storage.update_run(root['workflow_run_id'], lambda value: value.update(
            interaction_owner='monitor', activations=[*value['activations'], sibling]), 'fixture')
        yield storage, root, child


def test_child_checkpoint_answer_ignores_unrelated_running_sibling(child_checkpoint, monkeypatch):
    from harflow_mcp import workflow_reads, workflows
    storage, root, child = child_checkpoint
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    before = copy.deepcopy(storage.get_run(root['workflow_run_id']))
    projected = workflow_reads.on_demand_projection(storage, root)
    assert projected['status'] == 'needs_input' and projected['settling']
    actions = controls.projection(storage, projected)
    assert actions['resume']['allowed']
    assert actions['resume']['target_workflow_run_id'] == root['workflow_run_id']
    assert not actions['skip_optional_review']['allowed']
    assert storage.get_run(root['workflow_run_id']) == before
    storage.control(root['workflow_run_id'], 'resume', 'Answer', decision_id='question', interaction_owner='monitor')
    answered = storage.get_run(child['workflow_run_id'])
    assert answered['status'] == 'running' and answered['instructions'] == 'Answer'
    assert storage.get_run(root['workflow_run_id'])['activations'][-1]['tasks'][0]['status'] == 'running'


def test_child_checkpoint_still_checks_its_own_active_tasks(child_checkpoint):
    from harflow_mcp import workflow_reads
    storage, root, child = child_checkpoint
    storage.update_run(child['workflow_run_id'], lambda value: value['activations'].append({
        'id': 'child-active', 'node_id': 'work', 'role': 'node', 'status': 'running',
        'created_at': 0.0, 'tasks': [{'task_id': 'child-task', 'status': 'running'}]}), 'fixture')
    projected = workflow_reads.on_demand_projection(storage, root)
    action = controls.projection(storage, projected)['resume']
    assert not action['allowed'] and 'settling' in action['reason']


def test_child_checkpoint_refresh_rejects_stale_decision(child_checkpoint):
    from harflow_mcp import workflow_reads
    storage, root, child = child_checkpoint
    projected = workflow_reads.on_demand_projection(storage, root)
    storage.update_run(child['workflow_run_id'], lambda value: value.update(input_decision_id='new-question'), 'fixture')
    action = controls.projection(storage, projected)['resume']
    assert not action['allowed'] and 'checkpoint changed' in action['reason']
