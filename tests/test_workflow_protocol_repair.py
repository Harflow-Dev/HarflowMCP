"""Automatic correction stays with the original harness and node budget."""
import pytest
from test_workflow_delegation import Registry, parallel, run_flow, storage


async def test_guided_protocol_repair_precedes_orchestrator_and_retains_sibling(storage, tmp_path):
    counts = {'left': 0, 'right': 0}
    def left(prompt, kwargs):
        counts['left'] += 1
        if counts['left'] == 1:
            return {'summary': '{broken', 'permission_denials': [{'tool': 'file_system.bash', 'command': 'denied'}]}
        assert 'Protocol correction' in prompt
        assert 'Malformed JSON contract' in prompt
        assert '{broken' in prompt
        assert 'Do not use tools' in prompt
        assert kwargs['display_prompt'] == 'Focused assignment for left'
        return {'status': 'succeeded', 'result': {'summary': 'Corrected'}, 'evidence': []}
    def right(prompt, kwargs):
        counts['right'] += 1
        return {'status': 'succeeded', 'result': {}, 'evidence': []}
    registry = Registry(storage.root, outputs={'left': left, 'right': right})
    run, registry = await run_flow(storage, tmp_path, parallel(), registry, guided=True)
    assert run['status'] == 'completed'
    assert counts == {'left': 2, 'right': 1}
    attempts = [a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'left']
    assert len(attempts) == 2
    assert attempts[0]['raw_output'] == '{broken'
    assert attempts[0]['tasks'][0]['result']['permission_denials']
    assert attempts[1]['tasks'][0]['resume_task_id'] == attempts[0]['tasks'][0]['task_id']
    assert attempts[1]['tasks'][0]['candidate'] == attempts[0]['tasks'][0]['candidate']
    assert attempts[1]['tasks'][0]['freedom'] == attempts[0]['tasks'][0]['freedom']
    assert not any(c['current_stage']['node_id'] == 'left' and c['input_results'] and c['input_results'][0]['status'] == 'failed' for c in registry.contexts)


@pytest.mark.parametrize('optional', [False, True])
async def test_protocol_correction_exhausts_shared_attempt_budget(storage, tmp_path, optional):
    from test_workflow_delegation import default_decision
    definition = parallel(optional=optional)
    definition['nodes'][1]['max_attempts'] = 2
    def policy(context, registry):
        if any(item['node_id'] == 'left' and item['status'] == 'failed' for item in context['input_results']) and not optional:
            return {'decision_id': context['decision_id'], 'action': 'needs_input', 'question': 'Repair budget exhausted', 'reason': 'Malformed final persists'}
        return default_decision(context, registry)
    registry = Registry(storage.root, policy, {'left': {'summary': '{broken', 'permission_denials': [{'tool': 'denied'}]}})
    run, _ = await run_flow(storage, tmp_path, definition, registry, guided=True)
    assert len([a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'left']) == 2
    assert run['status'] == ('completed' if optional else 'needs_input')


@pytest.mark.parametrize('unsafe', ['outcome_unknown', 'cancelled', 'permission', 'authority', 'uncertain'])
def test_protocol_repair_never_reexecutes_unsafe_outcomes(unsafe):
    from polybridge import workflow_delegation as d
    outcome = {'status': 'completed'}
    if unsafe == 'outcome_unknown':
        outcome[unsafe] = True
    elif unsafe == 'cancelled':
        outcome['status'] = unsafe
    else:
        outcome['failure_kind'] = unsafe
    activation = {'id': 'a', 'result_error': 'Malformed JSON contract', 'role': 'node', 'node_id': 'work', 'status': 'failed', 'tasks': [{'status': 'completed', 'candidate': {'backend': 'codex'}, 'result': outcome}], 'node_result': {'status': 'failed', 'result': {'failure_kind': 'protocol'}}}
    run = {'runner_policy': 'guided', 'activations': [activation]}
    assert not d.protocol_repair_eligible(run, {'id': 'work', 'max_attempts': 2}, activation)


async def test_protocol_repair_without_session_stays_fresh_on_same_candidate(storage, tmp_path):
    from test_workflow_delegation import graph
    definition = graph()
    definition['nodes'][1]['agent']['fallbacks'] = [{'backend': 'claude'}]
    count = 0
    def worker(prompt, kwargs):
        nonlocal count
        count += 1
        if count == 1:
            return {'summary': '{broken', 'session_id': None}
        return {'status': 'succeeded', 'result': {}, 'evidence': []}
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, outputs={'work': worker}), guided=True)
    attempts = [a for a in run['activations'] if a['role'] == 'node']
    assert run['status'] == 'completed'
    assert len(attempts) == 2
    assert attempts[1]['tasks'][0]['session_mode'] == 'fresh'
    assert attempts[1]['tasks'][0]['candidate']['backend'] == 'codex'
    assert attempts[1]['tasks'][0]['candidate']['fallbacks'] == []


async def test_existing_paused_malformed_result_repairs_before_routing(storage, tmp_path, monkeypatch):
    from polybridge import workflow_delegation as d, workflows as w
    from test_workflow_delegation import default_decision, graph
    original_eligible = d.protocol_repair_eligible
    monkeypatch.setattr(d, 'protocol_repair_eligible', lambda *args: False)
    count = 0
    def worker(prompt, kwargs):
        nonlocal count
        count += 1
        return {'summary': '{broken'} if count == 1 else {'status': 'succeeded', 'result': {}, 'evidence': []}
    def policy(context, registry):
        if context['input_results'] and context['input_results'][0]['status'] == 'failed':
            return {'decision_id': context['decision_id'], 'action': 'needs_input', 'question': 'Paused malformed result', 'reason': 'Await repair'}
        return default_decision(context, registry)
    registry = Registry(storage.root, policy, {'work': worker})
    run, _ = await run_flow(storage, tmp_path, graph(), registry, guided=True)
    assert run['status'] == 'needs_input'
    assert count == 1
    old_context_count = len(registry.contexts)
    monkeypatch.setattr(d, 'protocol_repair_eligible', original_eligible)
    storage.control(run['workflow_run_id'], 'resume', instructions='Continue correcting the malformed final', decision_id=run['input_decision_id'])
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed'
    assert count == 2
    assert all(not c['input_results'] or c['input_results'][0]['status'] != 'failed' for c in registry.contexts[old_context_count:])


def test_protocol_repair_uses_node_grant_and_does_not_change_historical_policy():
    from polybridge import workflow_delegation as d
    activation = {'id': 'a', 'result_error': 'Malformed JSON contract', 'role': 'node', 'node_id': 'work', 'status': 'failed', 'tasks': [{'status': 'completed', 'candidate': {'backend': 'codex'}, 'result': {'status': 'completed'}}], 'node_result': {'status': 'failed', 'result': {'failure_kind': 'protocol'}}}
    node = {'id': 'work', 'max_attempts': 1}
    run = {'runner_policy': 'guided', 'activations': [activation]}
    assert not d.protocol_repair_eligible(run, node, activation)
    run['attempt_grants'] = {'work': 1}
    assert d.protocol_repair_eligible(run, node, activation)
    run.pop('runner_policy')
    assert not d.protocol_repair_eligible(run, node, activation)


async def test_valid_worker_reported_protocol_failure_is_not_automatically_corrected(storage, tmp_path):
    from test_workflow_delegation import default_decision, graph
    def policy(context, registry):
        if context['input_results'] and context['input_results'][0]['status'] == 'failed':
            return {'decision_id': context['decision_id'], 'action': 'needs_input', 'question': 'Worker reported a failure', 'reason': 'Valid result requires adjudication'}
        return default_decision(context, registry)
    output = {'status': 'failed', 'result': {'failure_kind': 'protocol', 'reason': 'Upstream protocol rejected request'}, 'evidence': ['Observed rejection']}
    run, _ = await run_flow(storage, tmp_path, graph(), Registry(storage.root, policy, {'work': output}), guided=True)
    attempts = [a for a in run['activations'] if a['role'] == 'node']
    assert run['status'] == 'needs_input'
    assert len(attempts) == 1
    assert 'result_error' not in attempts[0]
    from polybridge import workflows as w
    storage.control(run['workflow_run_id'], 'resume', instructions='Adjudicate the reported failure', decision_id=run['input_decision_id'])
    registry = Registry(storage.root, policy, {'work': output})
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    resumed = storage.get_run(run['workflow_run_id'])
    assert len([a for a in resumed['activations'] if a['role'] == 'node']) == 1
