"""Synthetic recovery/qualification oracles, without providers or live roots."""
import copy
from types import SimpleNamespace
from contextlib import nullcontext

import pytest

from harflow_mcp import steering_lifecycle as lifecycle
from harflow_mcp import workflow_receipt_documents as documents
from harflow_mcp.resources import ResourceLedger
from test_steering_window_core import ledger


def synthetic(count=40):
    root = ledger(count)
    root['kind'] = 'builder'
    root['status'] = 'needs_attention'
    root['steering']['compaction_pending'] = {'turn_ids': [f't{i}' for i in range(8)], 'effect_turn_ids': []}
    root['steering_action_receipts'] = {f't{i}': {'root_workflow_run_id': 'root'} for i in range(8)}
    root['activations'] = [{'id': 'ordinary', 'tasks': [], 'worker_steering_receipts': {f't{i}': 'proof' for i in range(count)}}]
    for turn in root['steering']['turns']:
        turn['candidate'] = {'backend': 'synthetic'}
        turn['target'] = {'workflow_run_id': 'root', 'activation_id': 'ordinary'}
    events = []
    def update(rid, mutate, event):
        mutate(root)
        events.append(event)
    storage = SimpleNamespace(root='/synthetic', tree_mutation=lambda _: nullcontext(), get_run=lambda *a, **k: copy.deepcopy(root), update_run=update)
    return root, storage, events


def test_pending_recovery_reuses_closed_namespace_and_reclaims_root_receipts(monkeypatch):
    root, storage, events = synthetic()
    monkeypatch.setattr(ResourceLedger, 'snapshot', lambda _: [])
    monkeypatch.setattr(lifecycle, '_write_binding', lambda *a: True)
    # Invoke undecorated core: no filesystem is part of this model oracle.
    lifecycle.maintain.__wrapped__(storage, 'root')
    assert root['steering']['ledger_epoch'] == 1
    assert 'steering_namespace_closed' not in events
    assert 'compaction_pending' not in root['steering']
    assert root['steering_action_receipts'] == {}
    assert len(root['activations'][0]['worker_steering_receipts']) == 32
    assert len(root['steering']['turns']) == 32
    lifecycle.maintain.__wrapped__(storage, 'root')
    assert root['steering']['ledger_epoch'] == 1


def test_pending_resource_owner_never_loses_proof(monkeypatch):
    root, storage, events = synthetic()
    before = copy.deepcopy(root)
    monkeypatch.setattr(ResourceLedger, 'snapshot', lambda _: [{'holders': [{'owner': {'task_id': 'task0'}}], 'queue': []}])
    lifecycle.maintain.__wrapped__(storage, 'root')
    assert root == before and not events


@pytest.mark.parametrize('patch', [{'v': True}, {'ledger_epoch': True}, {'task_id': 'different'}, {'role': 'root'}, {'extra': 1}])
def test_binding_decoder_refuses_unqualified_optional_authority(patch):
    binding = {'v': 1, 'root_workflow_run_id': 'root', 'task_id': 'task', 'turn_id': 'turn', 'ledger_epoch': 0, 'role': 'worker'}
    receipt = {'workflow_run_id': 'root', 'root_workflow_run_id': 'root', 'steering_binding': {**binding, **patch}}
    with pytest.raises(documents.WorkflowReceiptDocumentError):
        documents.encode(receipt, 'task')


def test_recovery_never_detaches_newly_settled_child_outside_durable_plan(monkeypatch):
    root, storage, events = synthetic(50)
    monkeypatch.setattr(ResourceLedger, 'snapshot', lambda _: [])
    bound = []
    monkeypatch.setattr(lifecycle, '_write_binding', lambda storage, root, turn, tree: bound.append(turn['turn_id']) or True)
    lifecycle.maintain.__wrapped__(storage, 'root')
    assert len(root['steering']['turns']) == 42
    assert set(bound) == {f't{i}' for i in range(8)}
    assert 't8' in {t['turn_id'] for t in root['steering']['turns']}


from test_workflow_steering_service import setup


def test_status_reports_legacy_limits_until_explicit_upgrade(setup):
    from harflow_mcp.workflow_steering import get_workflow_steering
    from harflow_mcp.steering_window import upgrade
    storage, rid, _ = setup
    legacy = get_workflow_steering(rid, root=storage.root)
    assert legacy['budgets']['root_limit'] == 32
    assert legacy['budgets']['episode_limit'] == 8
    assert 'upgrade' in legacy['eligible_actions']
    storage.update_run(rid, upgrade, 'test_explicit_upgrade')
    current = get_workflow_steering(rid, root=storage.root)
    assert current['budgets']['root_limit'] is None
    assert current['budgets']['episode_limit'] is None


@pytest.mark.parametrize('field,value', [
    ('worker_result_required', 1), ('steering_superseded', 'yes'),
    ('worker_steering_assignment_digest', 'unqualified'),
    ('worker_steering_question_id', '../question'),
    ('worker_steering_receipts', []), ('worker_steering_receipts', {'../turn': 'sha256:' + 'a' * 64}),
    ('worker_steering_receipts', {'turn': 'wrong'})])
def test_activation_rejects_malformed_optional_worker_authority(field, value):
    from harflow_mcp.workflow_run_documents import _activation
    activation = {'id': 'activation', 'node_id': 'node', 'role': 'node', 'status': 'running', 'created_at': 1.0, 'tasks': [], field: value}
    with pytest.raises(ValueError):
        _activation(activation, 'activation')


def test_attempt_rejects_malformed_steering_turn_identity():
    from harflow_mcp.workflow_run_documents import _attempt
    with pytest.raises(ValueError):
        _attempt({'task_id': 'task', 'status': 'completed', 'steering_turn_id': '../turn'}, 'attempt')


def test_workflow_attempt_resource_owner_uses_resource_attempt_id(monkeypatch):
    root, storage, events = synthetic()
    root['activations'][0]['resource_attempt'] = {'id': 'resource-attempt-one', 'state': 'granted'}
    before = copy.deepcopy(root)
    # This is workflow_resources.acquire_attempt's real owner shape: activation_id
    # names the resource attempt, never the original execution activation.
    monkeypatch.setattr(ResourceLedger, 'snapshot', lambda _: [{'holders': [{'owner': {
        'kind': 'workflow_attempt', 'workflow_run_id': 'root', 'activation_id': 'resource-attempt-one'}}], 'queue': []}])
    lifecycle.maintain.__wrapped__(storage, 'root')
    assert root == before and not events


def test_task_resource_attempt_owner_protects_matching_activation_family():
    root, storage, _ = synthetic()
    root['activations'][0]['tasks'] = [{'task_id': 'task0', 'resource_attempt': {'id': 'resource-attempt-one', 'state': 'attached'}}]
    snapshot = [{'holders': [], 'queue': [{'owner': {'kind': 'workflow_attempt', 'workflow_run_id': 'root', 'activation_id': 'resource-attempt-one'}}]}]
    pins, unknown = lifecycle.resource_task_pins(storage, snapshot, {'root': (root, ['root'])})
    assert not unknown
    assert 'task0' in pins and 'task39' in pins


def test_unrelated_workflow_attempt_never_globally_pins_current_tree(monkeypatch):
    root, storage, events = synthetic()
    monkeypatch.setattr(ResourceLedger, 'snapshot', lambda _: [{'holders': [{'owner': {
        'kind': 'workflow_attempt', 'workflow_run_id': 'unrelated-root', 'activation_id': 'unrelated-resource-attempt'}}], 'queue': []}])
    monkeypatch.setattr(lifecycle, '_write_binding', lambda *a: True)
    lifecycle.maintain.__wrapped__(storage, 'root')
    assert len(root['steering']['turns']) == 32
    assert 'compaction_pending' not in root['steering']
