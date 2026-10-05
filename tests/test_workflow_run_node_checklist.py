"""Parent checklist completion requires explicit, assigned child evidence."""
import pytest

from polybridge import workflow_delegation as d
from polybridge import workflows as w
from test_workflow_run_node_execution import run_tree, child_runs, invocation_activation
from test_workflow_run_node_recovery import store


async def test_completed_child_allows_explicit_parent_completion_for_assigned_task(store, tmp_path):
    run, _ = await run_tree(store, tmp_path, 'parent')
    activation = invocation_activation(run)
    activation['assigned_task_ids'] = ['parent-item']
    run['tasks'] = [{'id': 'parent-item', 'status': 'pending'}, {'id': 'other', 'status': 'pending'}]
    node = next(n for n in run['definition']['nodes'] if n['id'] == 'call')
    token = {'id': 'checkpoint', 'execution_complete': True, 'execution_activation_id': activation['id'], 'decision_id': 'decision', 'stack': []}
    decision = {'decision_id': 'decision', 'action': 'continue', 'reason': 'Child evidence satisfies the assigned item', 'next': [{'continuation_id': 'done'}], 'task_updates': [{'task_id': 'parent-item', 'status': 'completed', 'reason': 'Verified child outcome'}]}
    assert d.completion_evidence(run, node, token, 'parent-item') is activation
    assert d.completion_evidence(run, node, token, 'other') is None
    d.validate_decision(run, node, token, decision, False, root=store.root)
    # Child completion alone never mutates the parent checklist.
    assert run['tasks'][0]['status'] == 'pending'
    decision['task_updates'][0]['task_id'] = 'other'
    with pytest.raises(w.WorkflowError, match='assigned task'):
        d.validate_decision(run, node, token, decision, False, root=store.root)
    assert child_runs(store, run)[0]['status'] == 'completed'


@pytest.mark.parametrize('kind', ['child_failed', 'permission', 'uncertain', 'cancelled', 'timeout'])
async def test_unsuccessful_child_cannot_complete_parent_task(store, tmp_path, kind):
    run, _ = await run_tree(store, tmp_path, 'parent')
    activation = invocation_activation(run)
    activation['assigned_task_ids'] = ['parent-item']
    activation['node_result']['result']['child_outcome']['kind'] = kind
    node = next(n for n in run['definition']['nodes'] if n['id'] == 'call')
    token = {'execution_complete': True, 'execution_activation_id': activation['id']}
    assert d.completion_evidence(run, node, token, 'parent-item') is None
