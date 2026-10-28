"""Caller reads derive waiting state from verified descendants without root writes."""
import copy
import uuid

import pytest

from harflow_mcp import workflow_reads, workflow_responses
from test_workflow_input_tree import owned_workflow_scope, question
from test_workflow_run_node_recovery import finished_child, seed_parent, store, write_run


@pytest.mark.parametrize('count', [1, 2, 3])
def test_running_root_reports_all_waiting_descendants(store, tmp_path, count):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    for index in range(count):
        activation = copy.deepcopy(root['activations'][0])
        if index:
            activation['id'] = uuid.uuid4().hex
            activation['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
            store.update_run(root['workflow_run_id'], lambda r: r['activations'].append(activation), 'fixture')
        child = question(store, root, activation, decision=f'decision-{index}')
        store.update_run(child['workflow_run_id'], lambda r: r.update(input_question='Complete clarification?\n' * 200), 'fixture')
    raw = store.get_run(root['workflow_run_id'])
    assert raw['status'] == 'running'
    # Singular compatibility fields must all describe the selected question.
    raw['optional_review_skip_available'] = True
    status = workflow_responses.compact(workflow_reads.on_demand_projection(store, raw))
    assert status['status'] == 'needs_input'
    assert status['pending_input_count'] == count
    assert status['optional_review_skip_available'] is False
    assert {entry['decision_id'] for entry in status['pending_inputs']} == {f'decision-{i}' for i in range(count)}
    assert all(len(entry['question']) > 2000 for entry in status['pending_inputs'])
    assert all('waiting_seconds' in entry for entry in status['pending_inputs'])
    assert str(count) in status['attention_reason']
    assert store.get_run(root['workflow_run_id'])['status'] == 'running'


def test_running_root_reports_nested_attention_with_duration(store, tmp_path):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    parent = finished_child(store, root, root['activations'][0], status='running')
    activation = copy.deepcopy(root['activations'][0])
    activation['id'] = uuid.uuid4().hex
    activation['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
    parent['activations'] = [activation]
    write_run(store, parent)
    child = finished_child(store, parent, activation, status='needs_attention')
    child['parent_link']['root_workflow_run_id'] = root['workflow_run_id']
    child['attention_reason'] = 'Need caller guidance'
    write_run(store, child)
    status = workflow_responses.compact(workflow_reads.on_demand_projection(store, root))
    assert status['status'] == 'needs_attention'
    assert status['pending_attention_count'] == 1
    assert status['pending_attention'][0]['path'] == [root['workflow_run_id'], parent['workflow_run_id'], child['workflow_run_id']]
    assert status['pending_attention'][0]['waiting_seconds'] >= 0
