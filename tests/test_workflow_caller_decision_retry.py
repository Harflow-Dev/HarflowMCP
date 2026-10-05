"""A caller answer resolves a worker decision without granting tools or extra attempts."""
import asyncio
import copy

import pytest

from polybridge import workflow_delegation as d, workflows as w
from test_workflow_delegation import Registry, default_decision, graph, run_flow, storage

ANSWER = '  POST APPROVED\nKeep the exact body.\r\n  '
BLOCKED = {'status': 'blocked', 'result': {'blocker_category': 'authority', 'question': 'Existing review differs; post anyway?', 'reason': 'Caller must decide'}, 'evidence': []}


def choose(context, registry):
    retry = next((c for c in context['valid_continuations'] if c['kind'] == 'retry_execution'), None)
    if retry:
        return {'decision_id': context['decision_id'], 'action': 'continue', 'reason': 'Caller resolved the decision', 'next': [{'continuation_id': retry['continuation_id'], 'prompt': 'Perform the approved assignment', 'session_mode': 'fresh'}]}
    workers = [e for e in context['settled_executions'] if e['node_id'] == 'work']
    if workers and workers[-1]['status'] == 'blocked':
        return {'decision_id': context['decision_id'], 'action': 'needs_input', 'reason': 'Need caller decision', 'question': 'Post anyway?'}
    return default_decision(context, registry)


@pytest.mark.parametrize('checkpoint', ['work', 'end'])
@pytest.mark.parametrize('max_attempts,grant', [(2, 0), (1, 1)])
async def test_answer_retries_decision_block_with_exact_context_and_completes(storage, tmp_path, max_attempts, grant, checkpoint):
    definition = graph(); definition['nodes'][1].update(max_attempts=max_attempts, freedom='write_in_repo')
    count = 0
    def worker(prompt, kwargs):
        nonlocal count
        count += 1
        if count == 1:
            return copy.deepcopy(BLOCKED)
        assert ANSWER in prompt
        assert kwargs['freedom'] == 'write_in_repo'
        return {'status': 'succeeded', 'result': {'posted': True}, 'evidence': ['Observed completion']}
    run, registry = await run_flow(storage, tmp_path, definition, Registry(storage.root, choose, {'work': worker}), guided=True)
    assert run['status'] == 'needs_input'
    if checkpoint == 'end':
        def move(r):
            r['pending'][0].update(node_id='end', completion_source_node_id='work')
        storage.update_run(run['workflow_run_id'], move, 'end_checkpoint')
    resumed = storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, additional_attempts=grant, decision_id=run['input_decision_id'])
    blocked = next(a for a in resumed['activations'] if a['role'] == 'node')
    assert resumed['blocker_retry_authorizations'][blocked['id']]['reason'] == ANSWER
    token = resumed['pending'][0]; node = next(n for n in resumed['definition']['nodes'] if n['id'] == token['node_id'])
    retry = next(c for c in d.continuations(resumed, node, token, False, root=storage.root) if c['kind'] == 'retry_execution')
    d.validate_decision(resumed, node, token, {'decision_id': token['decision_id'], 'action': 'continue', 'reason': 'Answer supplied', 'next': [{'continuation_id': retry['continuation_id'], 'prompt': 'Post approved review', 'session_mode': 'fresh'}]}, False, root=storage.root)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id']), 5)
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed', final.get('attention_reason')
    workers = [a for a in final['activations'] if a['role'] == 'node']
    assert len(workers) == 2 and workers[1]['attempt_in_visit'] == 2
    assert final['attempt_grants'].get('work', 0) == grant
    assert workers[0]['resolved_by_execution_id'] == workers[1]['id']


async def test_answer_does_not_grant_an_exhausted_execution_attempt(storage, tmp_path):
    definition = graph(); definition['nodes'][1]['max_attempts'] = 1
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, choose, {'work': copy.deepcopy(BLOCKED)}), guided=True)
    resumed = storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, decision_id=run['input_decision_id'])
    token = resumed['pending'][0]; node = resumed['definition']['nodes'][1]
    choices = d.continuations(resumed, node, token, False, root=storage.root)
    assert not any(c['kind'] == 'retry_execution' for c in choices)
    with pytest.raises(w.WorkflowError, match='Invalid'):
        d.validate_decision(resumed, node, token, {'decision_id': token['decision_id'], 'action': 'continue', 'reason': 'Try without budget', 'next': [{'continuation_id': 'retry:' + token['execution_activation_id'], 'prompt': 'Post', 'session_mode': 'fresh'}]}, False, root=storage.root)
    assert resumed['attempt_grants'] == {}


@pytest.mark.parametrize('grant', [0, 1])
@pytest.mark.parametrize('unsafe', ['permission_denial', 'permission_category', 'unknown', 'failed_harness', 'error_harness', 'nonzero_exit', 'cancelled_task', 'no_tasks'])
async def test_caller_answer_never_unblocks_permission_or_uncertain_execution(storage, tmp_path, unsafe, grant):
    run, _ = await run_flow(storage, tmp_path, graph(), Registry(storage.root, choose, {'work': copy.deepcopy(BLOCKED)}), guided=True)
    def change(r):
        a = next(a for a in r['activations'] if a['role'] == 'node')
        if unsafe == 'permission_denial': a['tasks'][0]['result']['permission_denials'] = [{'reason': 'Tool approval refused'}]
        elif unsafe == 'permission_category': a['node_result']['result'].update(blocker_category='permission', failure_kind='permission')
        elif unsafe == 'unknown': a['tasks'][0]['result']['outcome_unknown'] = True
        elif unsafe == 'failed_harness': a['tasks'][0]['result']['status'] = 'failed'
        elif unsafe == 'error_harness': a['tasks'][0]['result']['is_error'] = True
        elif unsafe == 'nonzero_exit': a['tasks'][0]['result']['exit_code'] = 1
        elif unsafe == 'cancelled_task': a['tasks'][0]['status'] = 'cancelled'
        else: a['tasks'] = []
    storage.update_run(run['workflow_run_id'], change, 'unsafe_fixture')
    resumed = storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, additional_attempts=grant, decision_id=run['input_decision_id'])
    token = resumed['pending'][0]
    assert not any(c['kind'] == 'retry_execution' for c in d.continuations(resumed, resumed['definition']['nodes'][1], token, False, root=storage.root))


async def test_answer_authorizes_only_the_current_suspended_decision(storage, tmp_path):
    run, _ = await run_flow(storage, tmp_path, graph(), Registry(storage.root, choose, {'work': copy.deepcopy(BLOCKED)}), guided=True)
    current = next(a for a in run['activations'] if a['role'] == 'node')
    unrelated = copy.deepcopy(current)
    unrelated.update(id='unrelated-execution', token={'id': 'unrelated-token'})
    storage.update_run(run['workflow_run_id'], lambda r: r['activations'].append(unrelated), 'unrelated_fixture')
    with pytest.raises(w.WorkflowError, match='current input decision_id'):
        storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, decision_id='stale')
    resumed = storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, decision_id=run['input_decision_id'])
    assert set(resumed['blocker_retry_authorizations']) == {current['id']}
    authorization = resumed['blocker_retry_authorizations'][current['id']]
    assert authorization['source_decision_id'] == run['input_decision_id']
    assert authorization['authorization_kind'] == 'caller_answer'
    assert not d.retry_eligible(unrelated, guided=True, run=resumed)
