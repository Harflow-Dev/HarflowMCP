"""Refusal reconciliation preserves harness evidence without rewriting worker results."""
import asyncio
import copy
import json

from polybridge import workflow_delegation as d, workflow_inspection as inspection, workflows as w
from test_workflow_delegation import storage  # noqa: F401
from test_workflow_optional_refusal import ANSWER, DENIAL, suspended


def refused(run):
    return next(a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'right')


async def test_final_reviewer_receives_exact_denial_and_explicit_skip_context(storage, tmp_path):
    run, registry = await suspended(storage, tmp_path)
    assert run['optional_review_skip_available'] is True
    storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER,
                    decision_id=run['input_decision_id'], allow_optional_review_skip=True)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id']), 5)
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed'
    source = refused(final)
    prompt = next(p for p, k in registry.calls if k['title'].endswith(' · final'))
    inputs = json.loads(prompt.split('Input results:\n')[1])
    evidence = next(i for i in inputs if i['execution_id'] == source['id'])
    assert evidence['permission_denials'] == [{'task_id': source['tasks'][0]['task_id'], 'permission_denials': [DENIAL]}]
    assert evidence['optional_skip']['caller_reason'] == ANSWER
    assert evidence['optional_skip']['orchestrator_reason'] == 'Caller accepted incomplete review coverage'
    assert evidence['optional_skip']['source_decision_id'] == run['input_decision_id']
    assert evidence['node_result']['evidence'] == []
    assert source['node_result']['evidence'] == []
    assert all('permission_denials' not in i and 'optional_skip' not in i for i in inputs if i['node_id'] != 'right')


async def test_legacy_implicit_grant_cannot_offer_optional_skip(storage, tmp_path):
    run, _ = await suspended(storage, tmp_path)
    source = refused(run)
    token = next(t for t in run['pending'] if t['node_id'] == 'right')
    node = next(n for n in run['definition']['nodes'] if n['id'] == 'right')
    run['optional_skip_authorizations'] = {source['id']: {'node_id': node['id'], 'source_decision_id': token['decision_id'], 'reason': ANSWER}}
    assert not any(c['kind'] == 'skip_optional_review' for c in d.continuations(run, node, token, False))
    run['optional_skip_authorizations'][source['id']]['allow_optional_review_skip'] = True
    assert [c['kind'] for c in d.continuations(run, node, token, False)] == ['skip_optional_review']


def evidence_run():
    return {'workflow_run_id': 'test', 'definition': {'nodes': [{'id': 'right', 'role': 'review'}]}, 'activations': [
        {'id': 'review', 'role': 'node', 'node_id': 'right', 'status': 'failed',
         'tasks': [{'task_id': 'first', 'status': 'completed', 'result': {'status': 'completed', 'permission_denials': [copy.deepcopy(DENIAL)]}},
                   {'task_id': 'second', 'status': 'failed', 'result': {'status': 'failed', 'permission_denials': [{'tool': 'read_file', 'path': '/other'}]}}],
         'node_result': {'status': 'blocked', 'result': {'blocker_category': 'permission'}, 'evidence': []},
         'optional_skip': {'caller_reason': ANSWER, 'orchestrator_reason': 'Proceed', 'scope': 'protocol_retry_ancestor', 'skipped_by_execution_id': 'later'}}]}


def test_multiple_attempts_and_protocol_ancestor_context_stay_attributed():
    run = evidence_run()
    before = copy.deepcopy(run)
    item = d.result_inputs(run, ['review'], preview=False)[0]
    assert [d['task_id'] for d in item['permission_denials']] == ['first', 'second']
    assert item['permission_denials'][0]['permission_denials'] == [DENIAL]
    assert item['permission_denials'][1]['permission_denials'] == [{'tool': 'read_file', 'path': '/other'}]
    assert item['optional_skip']['scope'] == 'protocol_retry_ancestor'
    assert item['optional_skip']['skipped_by_execution_id'] == 'later'
    item['permission_denials'][0]['permission_denials'][0]['command'] = 'changed'
    assert run == before


def test_large_sidecar_is_bounded_and_losslessly_inspectable():
    run = evidence_run()
    source = run['activations'][0]
    source['tasks'][0]['result']['permission_denials'][0]['command'] = 'x' * 20000
    source['optional_skip']['caller_reason'] = 'y' * 20000
    preview = d.result_inputs(run, ['review'])[0]
    assert preview['truncated'] is True
    assert len(preview['result_preview']) <= 16000
    assert 'inspect' in preview
    assert 'permission_denials' not in preview and 'optional_skip' not in preview
    full = d.result_inputs(run, ['review'], preview=False)[0]
    assert full['permission_denials'][0]['permission_denials'][0]['command'] == 'x' * 20000
    assert full['optional_skip']['caller_reason'] == 'y' * 20000
    chunks = []
    cursor = None
    while True:
        page = inspection.result_page(run, 'review', cursor=cursor, limit=1000)
        assert len(page['chunk']) <= 1000
        chunks.append(page['chunk'])
        cursor = page['next_cursor']
        if cursor is None:
            break
    inspected = json.loads(''.join(chunks))
    assert inspected['node_result'] == source['node_result']
    assert inspected['permission_denials'] == full['permission_denials']
    assert inspected['optional_skip'] == full['optional_skip']
    selected = json.loads(inspection.result_page(run, 'review', task_id='second')['chunk'])
    assert selected['task_result'] == source['tasks'][1]['result']
    assert 'optional_skip' not in selected


def test_nested_invocation_preserves_refusal_context_and_preview_budget(monkeypatch, tmp_path):
    from polybridge import workflow_inspection, workflow_invocation
    leaf = evidence_run()
    monkeypatch.setattr(workflow_inspection, '_authorized_linked_run', lambda run, root, target_id: leaf)
    nodes = {'child': {'role': 'review'}}
    activation = {'id': 'invoked', 'node_id': 'child', 'status': 'failed', 'tasks': [], 'invocation': {}}
    ref = {'workflow_run_id': leaf['workflow_run_id'], 'execution_id': 'review'}
    value = {'status': 'blocked', 'result': {'child_outcome': {'final_result_refs': [ref]}}, 'evidence': []}
    activation['node_result'] = value
    full = d.invocation_result_input({}, activation, value, nodes, False, tmp_path)
    entry = full['child_results'][0]
    assert entry['result_ref'] == ref
    assert entry['permission_denials'] == d.result_evidence(leaf['activations'][0])['permission_denials']
    assert entry['optional_skip']['caller_reason'] == ANSWER
    leaf['activations'][0]['optional_skip']['caller_reason'] = 'z' * 20000
    preview = d.invocation_result_input({}, activation, value, nodes, True, tmp_path)
    child = preview['child_result_previews'][0]
    assert child['truncated'] and child['inspect']
    assert len(child['result_preview']) <= workflow_invocation.INVOCATION_PREVIEW_REF_LIMIT
    assert sum(len(item.get('result_preview', '')) for item in preview['child_result_previews']) <= workflow_invocation.INVOCATION_PREVIEW_BUDGET


async def test_final_reviewer_gets_protocol_retry_ancestor_skip_context(storage, tmp_path):
    from test_workflow_delegation import Registry, run_flow
    from test_workflow_optional_refusal import BLOCKED, choose, definition
    attempts = 0
    def worker(prompt, kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return {'summary': '{broken review'}
        return {'summary': json.dumps(BLOCKED), 'permission_denials': [DENIAL]}
    outputs = {'right': worker, 'final': {'status': 'succeeded', 'result': {'verdict': 'approved'}, 'evidence': []}}
    run, registry = await run_flow(storage, tmp_path, definition(), Registry(storage.root, choose, outputs), guided=True)
    storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER,
                    decision_id=run['input_decision_id'], allow_optional_review_skip=True)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id']), 5)
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed'
    reviews = [a for a in final['activations'] if a['role'] == 'node' and a['node_id'] == 'right']
    prompt = next(p for p, k in registry.calls if k['title'].endswith(' · final'))
    inputs = json.loads(prompt.split('Input results:\n')[1])
    current = next(i for i in inputs if i['execution_id'] == reviews[1]['id'])
    prior = next(i for i in current['optional_skip_ancestors'] if i['execution_id'] == reviews[0]['id'])
    assert prior['optional_skip']['scope'] == 'protocol_retry_ancestor'
    assert prior['optional_skip']['skipped_by_execution_id'] == current['execution_id']
    assert prior['optional_skip']['caller_reason'] == ANSWER
    assert current['permission_denials'][0]['task_id'] == reviews[1]['tasks'][-1]['task_id']
    assert current['permission_denials'][0]['permission_denials'] == [DENIAL]


def test_ancestor_context_does_not_scan_unrelated_skipped_executions():
    run = evidence_run()
    current = run['activations'][0]
    previous = copy.deepcopy(current)
    previous.update(id='prior', optional_skip={'scope': 'protocol_retry_ancestor', 'skipped_by_execution_id': 'review', 'caller_reason': 'actual'})
    current['retry_of_execution_id'] = 'prior'
    unrelated = copy.deepcopy(previous)
    unrelated.update(id='unrelated', optional_skip={'scope': 'protocol_retry_ancestor', 'skipped_by_execution_id': 'review', 'caller_reason': 'unrelated'})
    run['activations'].extend([previous, unrelated])
    item = d.result_inputs(run, ['review'], preview=False)[0]
    assert [i['execution_id'] for i in item['optional_skip_ancestors']] == ['prior']
    inspected = json.loads(inspection.result_page(run, 'review')['chunk'])
    assert inspected['optional_skip_ancestors'] == item['optional_skip_ancestors']
    assert 'unrelated' not in json.dumps(item)
