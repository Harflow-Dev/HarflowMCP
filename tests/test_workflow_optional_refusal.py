"""Caller answers can discard an optional review refusal, without retrying it."""
import asyncio
import copy
import json

import pytest

from polybridge import workflow_delegation as d, workflows as w
from test_workflow_delegation import Registry, run_flow, storage  # noqa: F401
from test_workflow_explicit_parallel import explicit
from test_workflow_traversal import policy

ANSWER = 'Proceed without this reviewer; reconcile the completed reviews.'
DENIAL = {'tool': 'file_system.bash', 'command': 'git -C /repo diff base...head'}
BLOCKED = {'status': 'blocked', 'result': {'blocker_category': 'permission', 'reason': 'git -C refused'}, 'evidence': []}


def definition():
    graph = explicit()
    next(n for n in graph['nodes'] if n['id'] == 'right').update(optional=True, title='right', role='review', agent={'backend': 'vibe'})
    graph['nodes'].append({'id': 'final', 'type': 'agent', 'title': 'final', 'role': 'review'})
    next(e for e in graph['connections'] if e['id'] == 'merge-end').update(target='final')
    graph['connections'].append({'id': 'final-end', 'source': 'final', 'target': 'end'})
    return graph


def choose(context, registry):
    if context['current_stage']['node_id'] == 'right' and context['current_stage']['phase'] == 'routing':
        skips = [c for c in context['valid_continuations'] if c['kind'] == 'skip_optional_review']
        if skips:
            return {'decision_id': context['decision_id'], 'action': 'continue', 'reason': 'Caller accepted incomplete review coverage', 'next': [{'continuation_id': skips[0]['continuation_id']}]}
        if any(e['node_id'] == 'right' and e['status'] == 'blocked' for e in context['settled_executions']):
            return {'decision_id': context['decision_id'], 'action': 'needs_input', 'question': 'Proceed without the refused reviewer?', 'reason': 'Review unavailable'}
    return policy(context, registry)


async def suspended(storage, tmp_path):
    outputs = {'right': {'summary': json.dumps(BLOCKED), 'permission_denials': [DENIAL]}, 'final': {'status': 'succeeded', 'result': {'verdict': 'approved'}, 'evidence': ['Reconciled available reviews']}}
    return await run_flow(storage, tmp_path, definition(), Registry(storage.root, choose, outputs), guided=True)


async def test_answer_and_explicit_skip_complete_final_review_without_replay(storage, tmp_path):
    run, registry = await suspended(storage, tmp_path)
    assert run['status'] == 'needs_input', (run.get('attention_reason'), [(c['current_stage'], c['settled_executions']) for c in registry.contexts])
    with pytest.raises(w.WorkflowError, match='failed'):
        storage.control(run['workflow_run_id'], 'recover', instructions=ANSWER)
    resumed = storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, decision_id=run['input_decision_id'])
    token = next(t for t in resumed['pending'] if t['node_id'] == 'right')
    node = next(n for n in resumed['definition']['nodes'] if n['id'] == 'right')
    choices = d.continuations(resumed, node, token, False, root=storage.root)
    assert any(c['kind'] == 'skip_optional_review' for c in choices)
    assert not any(c['kind'] == 'retry_execution' for c in choices)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id']), 5)
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed', final.get('attention_reason')
    workers = [a for a in final['activations'] if a['role'] == 'node']
    assert sorted(a['node_id'] for a in workers) == ['final', 'left', 'left2', 'right']
    refused = next(a for a in workers if a['node_id'] == 'right')
    assert refused['optional_failure']
    assert refused['node_result'] == BLOCKED | {'result': BLOCKED['result'] | {'failure_kind': 'permission'}}
    assert refused['tasks'][0]['result']['permission_denials'] == [DENIAL]
    assert final['attempt_grants'] == {}
    assert final['definition'] == run['definition']
    assert not final['joins']
    reconciler = next(a for a in workers if a['node_id'] == 'final')
    assert refused['id'] in reconciler['input_result_refs']
    assert refused['optional_skip']['caller_reason'] == ANSWER
    assert refused['optional_skip']['source_decision_id'] == run['input_decision_id']
    prompt = next(p for p, k in registry.calls if k['title'].endswith(' · final'))
    assert 'permission' in prompt and refused['id'] in prompt


@pytest.mark.parametrize('unsafe', ['required', 'task_role', 'authority', 'unknown', 'cancelled', 'no_tasks', 'no_denials', 'no_join', 'wrong_execution'])
async def test_answer_cannot_offer_skip_without_settled_optional_review_scope(storage, tmp_path, unsafe):
    run, _ = await suspended(storage, tmp_path)
    def change(r):
        a = next(a for a in r['activations'] if a['role'] == 'node' and a['node_id'] == 'right')
        node = next(n for n in r['definition']['nodes'] if n['id'] == 'right')
        if unsafe == 'required': node['optional'] = False
        elif unsafe == 'task_role': node['role'] = 'task'
        elif unsafe == 'authority': a['node_result']['result'].update(blocker_category='authority', failure_kind='authority')
        elif unsafe == 'unknown': a['tasks'][0]['result']['outcome_unknown'] = True
        elif unsafe == 'cancelled': a['tasks'][0]['status'] = 'cancelled'
        elif unsafe == 'no_tasks': a['tasks'] = []
        elif unsafe == 'no_denials': a['tasks'][0]['result']['permission_denials'] = []
        elif unsafe == 'no_join': r['joins'] = {}
        elif unsafe == 'wrong_execution': next(t for t in r['pending'] if t['node_id'] == 'right')['execution_activation_id'] = 'unrelated'
    storage.update_run(run['workflow_run_id'], change, 'unsafe_fixture')
    resumed = storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, decision_id=run['input_decision_id'])
    token = next(t for t in resumed['pending'] if t['node_id'] == 'right')
    node = next(n for n in resumed['definition']['nodes'] if n['id'] == 'right')
    assert not any(c['kind'] == 'skip_optional_review' for c in d.continuations(resumed, node, token, False, root=storage.root))


async def test_skip_requires_current_answer_and_exclusive_structural_decision(storage, tmp_path):
    run, _ = await suspended(storage, tmp_path)
    with pytest.raises(w.WorkflowError, match='current input decision_id'):
        storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, decision_id='stale')
    token = next(t for t in run['pending'] if t['node_id'] == 'right')
    node = next(n for n in run['definition']['nodes'] if n['id'] == 'right')
    assert not any(c['kind'] == 'skip_optional_review' for c in d.continuations(run, node, token, False, root=storage.root))
    resumed = storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, decision_id=run['input_decision_id'])
    token = next(t for t in resumed['pending'] if t['node_id'] == 'right')
    choices = d.continuations(resumed, node, token, False, root=storage.root)
    skip = next(c for c in choices if c['kind'] == 'skip_optional_review')
    decision = {'decision_id': token['decision_id'], 'action': 'continue', 'reason': 'Discard refused review', 'next': [{'continuation_id': skip['continuation_id']}]}
    d.validate_decision(resumed, node, token, decision, False, root=storage.root)
    for extra in ({'prompt': 'Retry'}, {'session_mode': 'fresh'}):
        invalid = copy.deepcopy(decision); invalid['next'][0].update(extra)
        with pytest.raises(w.WorkflowError):
            d.validate_decision(resumed, node, token, invalid, False, root=storage.root)
    invalid = copy.deepcopy(decision); invalid['next'].append({'continuation_id': 'right-merge'})
    with pytest.raises(w.WorkflowError):
        d.validate_decision(resumed, node, token, invalid, False, root=storage.root)


@pytest.mark.parametrize('kind', ['protocol', 'harness'])
async def test_settled_failed_refusal_can_be_discarded_with_answer(storage, tmp_path, kind):
    run, registry = await suspended(storage, tmp_path)
    def change(r):
        a = next(a for a in r['activations'] if a['role'] == 'node' and a['node_id'] == 'right')
        a['node_result'] = {'status': 'failed', 'result': {'failure_kind': kind, 'reason': 'Refused before returning a valid review'}, 'evidence': []}
        if kind == 'harness':
            a['tasks'][0].update(status='failed')
            a['tasks'][0]['result'].update(status='failed', exit_code=1)
    storage.update_run(run['workflow_run_id'], change, 'settled_refusal_fixture')
    resumed = storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, decision_id=run['input_decision_id'])
    token = next(t for t in resumed['pending'] if t['node_id'] == 'right')
    node = next(n for n in resumed['definition']['nodes'] if n['id'] == 'right')
    assert [c['kind'] for c in d.continuations(resumed, node, token, False, root=storage.root)] == ['skip_optional_review']
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id']), 5)
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed'
    refused = next(a for a in final['activations'] if a['role'] == 'node' and a['node_id'] == 'right')
    assert refused['node_result']['status'] == 'failed'
    assert refused['node_result']['result']['failure_kind'] == kind
    assert refused['tasks'][0]['result']['permission_denials'] == [DENIAL]


async def test_skip_denied_protocol_repair_discards_its_failed_ancestor(storage, tmp_path):
    attempts = 0
    def worker(prompt, kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return {'summary': '{broken review'}
        return {'summary': json.dumps(BLOCKED), 'permission_denials': [DENIAL]}
    outputs = {'right': worker, 'final': {'status': 'succeeded', 'result': {'verdict': 'approved'}, 'evidence': ['Reconciled completed reviews']}}
    run, registry = await run_flow(storage, tmp_path, definition(), Registry(storage.root, choose, outputs), guided=True)
    assert run['status'] == 'needs_input'
    reviews = [a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'right']
    assert len(reviews) == 2
    assert reviews[0]['node_result']['result']['failure_kind'] == 'protocol'
    assert reviews[1]['retry_of_execution_id'] == reviews[0]['id']
    storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, decision_id=run['input_decision_id'])
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id']), 5)
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed', final.get('attention_reason')
    reviews = [a for a in final['activations'] if a['role'] == 'node' and a['node_id'] == 'right']
    assert len(reviews) == 2 and all(a['optional_failure'] for a in reviews)
    assert reviews[0]['node_result']['result']['failure_kind'] == 'protocol'
    assert reviews[0]['raw_output'] == '{broken review'
    assert reviews[1]['tasks'][0]['result']['permission_denials'] == [DENIAL]
    assert attempts == 2
    assert sum(a['role'] == 'node' and a['node_id'] == 'final' for a in final['activations']) == 1


@pytest.mark.parametrize('unsafe', ['unrelated', 'different_node', 'authority', 'unknown', 'cancelled', 'nonprotocol'])
def test_protocol_ancestor_discard_stays_within_safe_retry_chain(unsafe):
    previous = {'id': 'prior', 'role': 'node', 'node_id': 'right', 'status': 'failed', 'tasks': [{'status': 'completed', 'result': {'status': 'completed'}}], 'node_result': {'status': 'failed', 'result': {'failure_kind': 'protocol'}}}
    current = {'id': 'current', 'retry_of_execution_id': 'prior'}
    run = {'activations': [previous, current]}
    token = {'failed_execution_refs': ['prior', 'current']}
    node = {'id': 'right'}
    assert d.optional_review_protocol_ancestors(run, node, current, token) == [previous]
    if unsafe == 'unrelated': current.pop('retry_of_execution_id')
    elif unsafe == 'different_node': previous['node_id'] = 'left'
    elif unsafe == 'authority': previous['node_result']['result']['blocker_category'] = 'authority'
    elif unsafe == 'unknown': previous['tasks'][0]['result']['outcome_unknown'] = True
    elif unsafe == 'cancelled': previous['tasks'][0]['status'] = 'cancelled'
    elif unsafe == 'nonprotocol': previous['node_result']['result']['failure_kind'] = 'harness'
    assert d.optional_review_protocol_ancestors(run, node, current, token) == []
