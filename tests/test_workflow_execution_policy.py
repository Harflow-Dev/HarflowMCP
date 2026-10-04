from polybridge import workflow_execution_policy as p


def test_visit_budget_counts_retries_but_not_previous_visits():
    acts = [{'id': str(i), 'role': 'node', 'node_id': 'w', 'token': {'id': visit}, 'tasks': [{'status': 'completed'}]} for i, visit in enumerate(['old', 'new', 'new'])]
    run = {'execution_policy': 'visit', 'activations': acts}
    assert p.attempts_used(run, {'id': 'w'}, {'id': 'new'}) == 2
    assert p.attempts_used(run, {'id': 'w'}, {'id': 'future'}) == 0
    run.pop('execution_policy')
    assert p.attempts_used(run, {'id': 'w'}, {'id': 'future'}) == 3


def test_recovery_uses_original_visit_budget():
    source = {'id': 'failed', 'role': 'node', 'node_id': 'w', 'token': {'id': 'original'}, 'tasks': [{'status': 'completed'}]}
    run = {'execution_policy': 'visit', 'activations': [source]}
    assert p.attempts_used(run, {'id': 'w'}, {'id': 'end', 'retry_of_execution_id': 'failed'}) == 1

import copy
import pytest
from test_workflow_delegation import Registry, graph, run_flow, storage, default_decision


def serial(mode='continue_previous'):
    definition = graph()
    second = copy.deepcopy(definition['nodes'][1])
    second.update(id='second', session_mode=mode)
    definition['nodes'].insert(2, second)
    definition['connections'][1]['target'] = 'second'
    definition['connections'].append({'id': 'end', 'source': 'second', 'target': 'end'})
    return definition


@pytest.mark.parametrize('mode', ['continue_previous', 'agent_decides'])
async def test_serial_session_continuity(storage, tmp_path, mode):
    def policy(context, registry):
        decision = default_decision(context, registry)
        for entry in decision.get('next', []):
            if entry.get('prompt', '').endswith('second'):
                entry['session_mode'] = 'continue_previous'
        return decision
    run, registry = await run_flow(storage, tmp_path, serial(mode), Registry(storage.root, policy), guided=True)
    assert run['status'] == 'completed'
    nodes = [a for a in run['activations'] if a['role'] == 'node']
    assert nodes[1]['tasks'][0]['resume_task_id'] == nodes[0]['tasks'][0]['task_id']
    assert nodes[1]['tasks'][0]['session_mode'] == 'resume'


@pytest.mark.parametrize('mismatch', ['model', 'reasoning_effort', 'freedom', 'network'])
async def test_serial_session_mismatch_bootstraps_fresh(storage, tmp_path, mismatch):
    definition = serial()
    second = definition['nodes'][2]
    if mismatch in {'model', 'reasoning_effort'}:
        second['agent'][mismatch] = 'gpt-6-astra' if mismatch == 'model' else 'low'
    else:
        second[mismatch] = 'read_only' if mismatch == 'freedom' else True
    run, _ = await run_flow(storage, tmp_path, definition, guided=True)
    assert run['status'] == 'completed'
    nodes = [a for a in run['activations'] if a['role'] == 'node']
    assert nodes[1]['tasks'][0]['session_mode'] == 'fresh'


async def test_loop_visit_does_not_exhaust_node_attempts(storage, tmp_path):
    definition = graph()
    definition['nodes'][1]['max_attempts'] = 1
    definition['connections'].append({'id': 'again', 'source': 'work', 'target': 'work', 'condition': 'Repeat once', 'max_retries': 1})
    # Count the actual worker executions instead of coupling policy to stored runner internals.
    count = 0
    def work(prompt, kwargs):
        nonlocal count
        count += 1
        return {'status': 'succeeded', 'result': {}, 'evidence': []}
    def route(context, registry):
        decision = default_decision(context, registry)
        if context['current_stage']['node_id'] == 'work':
            chosen = 'again' if count == 1 else 'finish'
            decision['next'] = [e for e in decision['next'] if e['continuation_id'] == chosen]
        return decision
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, route, {'work': work}), guided=True)
    assert run['status'] == 'completed'
    assert count == 2


@pytest.mark.parametrize('settles', [True, False])
async def test_timeout_settles_before_fallback(storage, tmp_path, monkeypatch, settles):
    from polybridge import workflows as w
    definition = graph()
    definition['nodes'][1].update(timeout_seconds=1, agent={'backend': 'codex', 'fallbacks': [{'backend': 'claude'}]})
    notices = []
    class SlowRegistry(Registry):
        def workflow_notice(self, task_id, text):
            notices.append((task_id, text))
        async def start(self, prompt, repo, **kwargs):
            task = await super().start(prompt, repo, **kwargs)
            if 'Assignment:\n' in prompt and kwargs['backend'].name == 'codex':
                task.done.clear()
                task.result['status'] = 'running'
            return task
        async def cancel_cascade(self, task_id, **kwargs):
            task = self.tasks[task_id]
            if settles:
                task.result['status'] = 'cancelled'
                task.done.set()
                return {}
            else:
                raise RuntimeError('Cancellation outcome unknown')
    registry = SlowRegistry(storage.root)
    run, _ = await run_flow(storage, tmp_path, definition, registry, guided=True)
    worker = next(a for a in run['activations'] if a['role'] == 'node')
    if settles:
        assert run['status'] == 'completed'
        assert worker['tasks'][0]['result']['timed_out']
        assert len(notices) == 1 and 'attempt stopped before fallback' in notices[0][1]
        assert worker['tasks'][1]['session_mode'] == 'fresh'
        assert worker['tasks'][1]['candidate']['backend'] == 'claude'
    else:
        assert run['status'] == 'needs_attention'
        assert len(worker['tasks']) == 1
        assert worker['tasks'][0]['status'] == 'uncertain'
        assert notices == []


def test_caller_grant_retries_settled_block_without_permission_escalation():
    from polybridge import workflow_delegation as d
    activation = {'id': 'blocked', 'role': 'node', 'node_id': 'work', 'status': 'failed', 'tasks': [{'status': 'completed', 'result': {'status': 'completed', 'permission_denials': [{'reason': 'command shape refused'}]}}], 'node_result': {'status': 'blocked', 'result': {'blocker_category': 'missing_context'}}}
    run = {'blocker_retry_authorizations': {'blocked': {'node_id': 'work', 'reason': 'Use the supplied file instead'}}}
    assert d.retry_eligible(activation, guided=True, run=run)
    activation['tasks'][0]['result']['outcome_unknown'] = True
    assert not d.retry_eligible(activation, guided=True, run=run)


async def test_resolved_end_block_grant_offers_retry_and_preserves_access(storage, tmp_path):
    from polybridge import workflows as w, workflow_delegation as d
    definition = graph()
    def policy(context, registry):
        if context['current_stage']['node_id'] in {'work', 'end'} and any(r['status'] == 'blocked' for r in context['input_results']):
            return {'decision_id': context['decision_id'], 'action': 'needs_input', 'reason': 'Caller must supply missing payload', 'question': 'Provide payload'}
        return default_decision(context, registry)
    registry = Registry(storage.root, policy, {'work': {'status': 'blocked', 'result': {'blocker_category': 'missing_context'}, 'evidence': []}})
    run, _ = await run_flow(storage, tmp_path, definition, registry, guided=True)
    assert run['status'] == 'needs_input'
    # Move the saved checkpoint to End to reproduce the publication-stage caller report.
    def end_checkpoint(r):
        token = r['pending'][0]
        token['node_id'] = 'end'
        token['completion_source_node_id'] = 'work'
    storage.update_run(run['workflow_run_id'], end_checkpoint, 'end_fixture')
    saved_access = run['definition']['nodes'][1]['freedom']
    run = storage.control(run['workflow_run_id'], 'resume', 'Use the complete supplied payload', 1, decision_id=run['input_decision_id'])
    token = run['pending'][0]
    end = run['definition']['nodes'][-1]
    choices = d.continuations(run, end, token, False, root=storage.root)
    retry = next(c for c in choices if c['kind'] == 'retry_execution')
    assert retry['recovery_from_checkpoint']
    assert retry['attempts_remaining'] > 0
    assert run['definition']['nodes'][1]['freedom'] == saved_access


async def test_fallback_pre_spawn_refusal_preserves_started_activation(storage, tmp_path, monkeypatch):
    from polybridge import workflows as w
    definition = graph()
    definition['nodes'][1]['agent']['fallbacks'] = [{'backend': 'claude'}]
    run = storage.create_run(w.validate_definition(definition), 'go', tmp_path)
    registry = Registry(storage.root)
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = run['workflow_run_id']
    storage.update_run(run['workflow_run_id'], lambda r: r.update(status='running'), 'fixture')
    activation = supervisor._activation('work', 'node')
    def previous_failure(r):
        r['activations'][0]['tasks'].append({'task_id': 'previous', 'status': 'failed', 'candidate': {'backend': 'codex'}, 'result': {'status': 'failed', 'stderr_tail': 'usage limit reached', 'summary': 'usage limit reached'}})
    storage.update_run(run['workflow_run_id'], previous_failure, 'fixture')
    activation = supervisor.run()['activations'][0]
    backend = w.backends.get('claude')
    def refused(*args, **kwargs):
        raise OSError('Settings unavailable before spawn')
    monkeypatch.setattr(type(backend), 'enforcement', refused)
    # Skip the already settled first candidate, reproducing the restored fallback state.
    storage.update_run(run['workflow_run_id'], lambda r: r['suppressed_candidates'].append('work:' + w._candidate_key({'backend': 'codex'})), 'fixture')
    assert await supervisor._dispatch(run['definition']['nodes'][1], 'assignment', 'node', activation) is None
    restored = supervisor.run()['activations'][0]
    assert restored['tasks'][-1]['status'] == 'not_started'
    assert restored['status'] != 'not_started'


@pytest.mark.parametrize('value', [-1, True, False, 1.5, {}, '900'])
def test_invalid_timeout_definition_is_rejected(value):
    from polybridge import workflows as w
    definition = graph()
    definition['nodes'][1]['timeout_seconds'] = value
    with pytest.raises(w.WorkflowError, match='timeout_seconds'):
        w.validate_definition(definition)


async def test_expired_serial_session_starts_fresh_without_duplication(storage, tmp_path):
    from polybridge.tasks import SessionUnknownError
    class ExpiredRegistry(Registry):
        async def resume(self, previous, prompt, **kwargs):
            if 'Assignment:\n' in prompt and 'Focused assignment for second' in prompt:
                raise SessionUnknownError('Expired predecessor session')
            return await super().resume(previous, prompt, **kwargs)
    run, _ = await run_flow(storage, tmp_path, serial(), ExpiredRegistry(storage.root), guided=True)
    assert run['status'] == 'completed'
    second = next(a for a in run['activations'] if a['node_id'] == 'second' and a['role'] == 'node')
    assert [t['status'] for t in second['tasks']] == ['not_started', 'completed']
    assert second['tasks'][-1]['session_mode'] == 'fresh'


async def test_serial_resume_outage_fallback_is_fresh(storage, tmp_path):
    definition = serial()
    definition['nodes'][2]['agent']['fallbacks'] = [{'backend': 'claude'}]
    class OutageRegistry(Registry):
        async def resume(self, previous, prompt, **kwargs):
            task = await super().resume(previous, prompt, **kwargs)
            if 'Assignment:\n' in prompt and 'Focused assignment for second' in prompt:
                task.result.update(status='failed', stderr_tail=['usage_limit_reached'], summary='usage limit reached')
            return task
    run, _ = await run_flow(storage, tmp_path, definition, OutageRegistry(storage.root), guided=True)
    assert run['status'] == 'completed'
    second = next(a for a in run['activations'] if a['node_id'] == 'second' and a['role'] == 'node')
    assert second['tasks'][-1]['candidate']['backend'] == 'claude'
    assert second['tasks'][-1]['session_mode'] == 'fresh'


def test_previous_session_never_substitutes_historical_candidate(tmp_path):
    from polybridge import workflows as w
    definition = w.validate_definition(serial())
    def execution(eid, backend):
        return {'id': eid, 'role': 'node', 'node_id': 'work', 'status': 'completed', 'tasks': [{'task_id': eid, 'candidate': {'backend': backend}, 'status': 'completed', 'result': {'session_id': 'session-' + eid}, 'repo_path': str(tmp_path), 'freedom': 'publish', 'network': None}]}
    run = {'definition': definition, 'activations': [execution('old', 'codex'), execution('current', 'claude')], 'repo_path': str(tmp_path), 'network': None, 'freedom': 'publish', 'permission_policy': 'saved_node'}
    assert p.previous_session(run, definition['nodes'][2], {'input_result_refs': ['current']}) == []


@pytest.mark.parametrize('indicator', ['sigkill_survivors', 'survivors', 'not_signalled', 'owner_still_settling', 'cascade_incomplete', 'unconverged', 'not_recorded'])
async def test_timeout_cascade_failure_never_falls_back(storage, tmp_path, indicator):
    definition = graph()
    definition['nodes'][1].update(timeout_seconds=1, agent={'backend': 'codex', 'fallbacks': [{'backend': 'claude'}]})
    class IncompleteRegistry(Registry):
        async def start(self, prompt, repo, **kwargs):
            task = await super().start(prompt, repo, **kwargs)
            if 'Assignment:\n' in prompt and kwargs['backend'].name == 'codex':
                task.done.clear()
                task.result['status'] = 'running'
            return task
        async def cancel_cascade(self, task_id, **kwargs):
            self.tasks[task_id].result['status'] = 'cancelled'
            self.tasks[task_id].done.set()
            return {indicator: True if indicator == 'cascade_incomplete' else ['descendant']}
    run, _ = await run_flow(storage, tmp_path, definition, IncompleteRegistry(storage.root), guided=True)
    assert run['status'] == 'needs_attention'
    worker = next(a for a in run['activations'] if a['role'] == 'node')
    assert len(worker['tasks']) == 1
    assert worker['tasks'][0]['status'] == 'uncertain'


async def test_incompatible_network_candidate_is_rejected_before_spawn(storage, tmp_path):
    definition = graph()
    definition['nodes'][1].update(freedom='read_only', network=True, agent={'backend': 'codex', 'fallbacks': [{'backend': 'claude'}]})
    run, registry = await run_flow(storage, tmp_path, definition, guided=True)
    assert run['status'] == 'completed'
    execution = next(a for a in run['activations'] if a['role'] == 'node')
    assert execution['tasks'][0]['status'] == 'not_started'
    assert 'network' in execution['tasks'][0]['error']
    assert execution['tasks'][1]['candidate']['backend'] == 'claude'
    worker_calls = [kwargs for prompt, kwargs in registry.calls if 'Assignment:\n' in prompt]
    assert len(worker_calls) == 1 and worker_calls[0]['backend'].name == 'claude'


def test_review_block_and_generic_three_way_gate_have_existing_contracts():
    from polybridge import workflow_delegation as d
    import json
    for role, value in [('review', {'status': 'blocked', 'result': {'blocker_category': 'missing_context'}, 'evidence': []}), ('task', {'status': 'succeeded', 'result': {'verdict': 'retry'}, 'evidence': []})]:
        node = {'role': role}
        assert d.normalize_result(node, {'summary': json.dumps(value)}, [], guided=True) == value


async def test_revised_technical_plan_preserves_original_execution_result(storage, tmp_path):
    definition = serial('fresh')
    for node in definition['nodes'][1:3]:
        node['role'] = 'planning'
    outputs = {nid: {'status': 'succeeded', 'result': {'technical_plan': plan, 'tasks': [{'id': 'same', 'title': 'Implement'}]}, 'evidence': []} for nid, plan in [('work', '# Original approach'), ('second', '# Revised approach')]}
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, outputs=outputs), guided=True)
    assert run['status'] == 'completed'
    executions = [a for a in run['activations'] if a['role'] == 'node']
    assert run['technical_plan'] == '# Revised approach'
    assert run['technical_plan_execution_id'] == executions[1]['id']
    assert executions[0]['node_result']['result']['technical_plan'] == '# Original approach'
