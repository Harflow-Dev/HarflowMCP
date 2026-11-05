"""Restricted replan protocol and ownership tests using synthetic launch owners."""
import copy

import pytest

from harflow_mcp import workflow_replan as replan, workflow_steering as steering, task_supervision
from harflow_mcp import resources
from test_workflow_replan_faults import pending_replan, replan_request, positive_owner_ack
from test_worker_steering_faults import fresh, EffectRegistry, worker_origin


@pytest.mark.parametrize('field,value', [('schema_version', 1), ('ledger_epoch', True), ('action', 'cancel'), ('reason', ''), ('request_id', 'not-a-uuid')])
async def test_closed_request_refuses_before_any_withdrawal(pending_replan, field, value):
    storage, rid, _ = pending_replan
    request = replan_request(storage, rid)
    request[field] = value
    with pytest.raises(steering.SteeringError):
        await replan.replan_workflow_steering(rid, request, root=storage.root)
    assert not fresh(storage, rid)['steering'].get('replans')
    assert not list((storage.root / 'tasks').glob('*.withdraw.json'))


@pytest.mark.parametrize('field,value', [('backend', 'different'), ('repo_path', '/different'), ('freedom', 'read_only'), ('network', True)])
async def test_launch_configuration_mismatch_refuses_without_revoking_response(pending_replan, field, value):
    storage, rid, launch = pending_replan
    request = replan_request(storage, rid)
    before = copy.deepcopy(fresh(storage, rid)['steering']['turns'])
    task_supervision._write(storage.root / 'tasks' / (launch['task_id'] + '.launch.json'), {**launch, field: value})
    with pytest.raises(steering.SteeringError):
        await replan.replan_workflow_steering(rid, request, root=storage.root)
    assert fresh(storage, rid)['steering']['turns'] == before
    assert not list((storage.root / 'tasks').glob('*.withdraw.json'))


async def test_owner_receipt_mismatch_refuses_before_any_intent(pending_replan):
    from harflow_mcp.workflows import _write
    from harflow_mcp.workflow_receipt_io import read
    from harflow_mcp.workflow_receipt_documents import encode
    storage, rid, launch = pending_replan
    request = replan_request(storage, rid)
    path = storage.owners / (launch['task_id'] + '.json')
    owner, _ = read(path)
    owner['workflow_execution_id'] = 'another-execution'
    from harflow_mcp.state_ownership import state_session
    with state_session(storage.state_layout):
        _write(path, encode(owner, launch['task_id']), layout=storage.state_layout)
    with pytest.raises(steering.SteeringError):
        await replan.replan_workflow_steering(rid, request, root=storage.root)
    assert not fresh(storage, rid)['steering'].get('replans')
    assert not list((storage.root / 'tasks').glob('*.withdraw.json'))


async def test_retired_original_callback_cannot_change_certified_settlement(pending_replan):
    from harflow_mcp.workflows import WorkflowSupervisor
    from harflow_mcp.steering_lifecycle import worker_sources
    storage, rid, launch = pending_replan
    await replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    positive_owner_ack(storage, launch)
    replan.reconcile(storage, rid)
    before = fresh(storage, rid)
    original = before['activations'][0]
    assert original['worker_result_required'] is True
    registry = EffectRegistry(storage)
    supervisor = WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    supervisor._task_update(original['id'], launch['task_id'], {'status': 'completed', 'result': {'summary': 'late'}})
    node = next(n for n in before['definition']['nodes'] if n['id'] == original['node_id'])
    assert await supervisor._dispatch(node, 'Late callback', 'node', original) is None
    assert fresh(storage, rid)['activations'][0] == original
    assert not worker_sources(storage, fresh(storage, rid))
    assert not registry.effects


def test_scoped_workflow_resource_release_checks_exact_resource_attempt(tmp_path):
    from harflow_mcp.state_ownership import state_session
    from harflow_mcp.state_layout import resolve_layout
    with state_session(resolve_layout(root=tmp_path)):
        ledger = resources.ResourceLedger(tmp_path)
        entry = ledger.request(['simulator'], resources.Owner('workflow_attempt', workflow_run_id='root', activation_id='resource-attempt'))
        with pytest.raises(resources.ResourceOwnerMismatch):
            ledger.release_owned_workflow_attempt(entry.reservation_id, 'root', 'different-attempt')
        assert ledger.get(entry.reservation_id) is not None
        assert ledger.release_owned_workflow_attempt(entry.reservation_id, 'root', 'resource-attempt') == 'released'
        assert ledger.release_owned_workflow_attempt(entry.reservation_id, 'root', 'resource-attempt') == 'absent'


@pytest.mark.parametrize('corruption', ['missing_ack', 'wrong_ack_task', 'wrong_ack_generation', 'original_running', 'lost_blocker', 'wrong_task', 'candidate_changed', 'pending_old_execution'])
async def test_committed_replan_corruption_never_becomes_authority(pending_replan, corruption):
    from harflow_mcp.workflow_run_documents import WorkflowRunCandidateError
    storage, rid, launch = pending_replan
    await replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    positive_owner_ack(storage, launch)
    replan.reconcile(storage, rid)
    def corrupt(run):
        record = run['steering']['replans'][0]
        old = next(a for a in run['activations'] if a['id'] == record['withdrawal_witness']['activation_id'])
        new = next(a for a in run['activations'] if a['id'] == record['planned_activation_id'])
        if corruption == 'missing_ack': record.pop('withdrawal_ack')
        elif corruption == 'wrong_ack_task': record['withdrawal_ack']['task_id'] = 'different'
        elif corruption == 'wrong_ack_generation': record['withdrawal_ack']['generation'] = 'f' * 32
        elif corruption == 'original_running': old['tasks'][0]['status'] = 'running'
        elif corruption == 'lost_blocker': old['worker_result_required'] = False
        elif corruption == 'wrong_task': new['planned_task_id'] = 'different'
        elif corruption == 'candidate_changed': new['execution_candidate']['max_turns'] += 1
        elif corruption == 'pending_old_execution': run['pending'][0]['execution_activation_id'] = old['id']
    with pytest.raises(WorkflowRunCandidateError):
        storage.update_run(rid, corrupt, 'test_corrupt_replan_authority')
    assert fresh(storage, rid)['steering']['replans'][0]['phase'] == 'replan_committed'


async def test_public_replan_proof_omits_private_directory_identity(pending_replan):
    storage, rid, launch = pending_replan
    await replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    positive_owner_ack(storage, launch)
    replan.reconcile(storage, rid)
    public = steering.get_workflow_steering(rid, root=storage.root)['replans'][0]
    assert public['withdrawal_ack']['outcome'] == 'withdrawn'
    assert public['withdrawal_ack']['origin'] == 'owner_prestart'
    assert not {'root', 'directory_binding', 'directory_identity', 'proof'} & public['withdrawal_ack'].keys()


@pytest.mark.parametrize('committed', [False, True])
async def test_late_original_execute_node_is_quiet_after_replan_intent(pending_replan, committed):
    from harflow_mcp.workflows import WorkflowSupervisor
    from harflow_mcp.workflow_delegation import execute_node
    storage, rid, launch = pending_replan
    original_run = fresh(storage, rid)
    old_token = copy.deepcopy(original_run['pending'][0])
    node = next(n for n in original_run['definition']['nodes'] if n['id'] == old_token['node_id'])
    await replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    if committed:
        positive_owner_ack(storage, launch)
        replan.reconcile(storage, rid)
    before = fresh(storage, rid)
    registry = EffectRegistry(storage)
    supervisor = WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    attention = []
    supervisor.attention = attention.append
    await execute_node(supervisor, node, old_token)
    assert fresh(storage, rid) == before
    assert attention == []
    assert registry.effects == []
