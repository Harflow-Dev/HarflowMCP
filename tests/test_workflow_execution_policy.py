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

async def test_serial_fresh_reason_names_permission_mismatch(storage, tmp_path):
    definition = serial()
    definition['nodes'][2]['freedom'] = 'read_only'
    run, registry = await run_flow(storage, tmp_path, definition, guided=True)
    execution = next(a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'second')
    assert 'freedom differs' in execution['session_reason']
    assert any('freedom differs' in str(c['valid_continuations']) for c in registry.contexts)

async def test_loop_continue_previous_reuses_target_session(storage, tmp_path):
    definition = serial()
    definition['connections'].append({'id': 'loop', 'source': 'second', 'target': 'work', 'condition': 'Repeat once', 'max_retries': 1})
    definition['nodes'][1]['session_mode'] = 'continue_previous'
    def route(context, registry):
        decision = default_decision(context, registry)
        if context['current_stage']['node_id'] == 'second':
            looped = any(c['current_stage']['node_id'] == 'second' for c in registry.contexts[:-1])
            chosen = 'end' if looped else 'loop'
            decision['next'] = [e for e in decision['next'] if e['continuation_id'] == chosen]
        return decision
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, route), guided=True)
    assert run['status'] == 'completed'
    executions = [a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'work']
    assert len(executions) == 2
    assert executions[1]['tasks'][0]['resume_task_id'] == executions[0]['tasks'][0]['task_id']

async def test_builder_cancel_before_activation_has_no_start_lookup(storage, tmp_path):
    from polybridge import workflows as w
    run = storage.create_run({'name': 'builder', 'orchestrator': {'backend': 'codex'}}, 'Generate', tmp_path, kind='builder', freedom='read_only')
    storage.control(run['workflow_run_id'], 'cancel')
    registry = Registry(storage.root)
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    assert storage.get_run(run['workflow_run_id'])['status'] == 'cancelled'
    assert registry.calls == []

async def test_worker_scratch_path_is_literal_and_internal(storage, tmp_path):
    from polybridge import scratch
    run, registry = await run_flow(storage, tmp_path, guided=True)
    prompt, kwargs = next((p, k) for p, k in registry.calls if 'Assignment:\n' in p)
    path = str(scratch.directory(registry._log_dir, kwargs['task_id']).resolve())
    assert 'Task scratch directory (absolute): ' + path in prompt
    assert path not in kwargs['display_prompt']
    assert 'publish-review' not in prompt and 'GitHub' not in prompt

async def test_recovered_timeout_uses_unused_fallback_without_repeat(storage, tmp_path):
    from polybridge import workflows as w
    definition = graph()
    definition['nodes'][1].update(timeout_seconds=1, agent={'backend': 'codex', 'fallbacks': [{'backend': 'claude'}]})
    run = storage.create_run(w.validate_definition(definition), 'Request', tmp_path)
    def seed(r):
        token = {'id': 'visit', 'node_id': 'work', 'stack': [], 'assignment_prompt': 'Original assignment', 'execution_session_mode': 'fresh', 'execution_activation_id': 'execution'}
        r.update(status='running', pending=[token], execution_initialized=True)
        r['activations'] = [{'id': 'execution', 'role': 'node', 'node_id': 'work', 'status': 'running', 'token': copy.deepcopy(token), 'assignment_prompt': 'Original assignment', 'tasks': [{'task_id': 'old', 'candidate': {'backend': 'codex'}, 'status': 'failed', 'result': {'status': 'failed', 'timed_out': True, 'timeout_seconds': 1, 'summary': 'Node timed out after 1 seconds'}}]}]
    storage.update_run(run['workflow_run_id'], seed, 'crash_fixture')
    registry = Registry(storage.root)
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    restored = storage.get_run(run['workflow_run_id'])
    assert restored['status'] == 'completed'
    workers = [k for prompt, k in registry.calls if 'Assignment:\n' in prompt]
    assert len(workers) == 1 and workers[0]['backend'].name == 'claude'
    assert len([a for a in restored['activations'] if a['role'] == 'node']) == 1

async def test_continue_previous_retry_uses_own_session_not_predecessor(storage, tmp_path):
    from polybridge import workflow_delegation as d
    definition = serial()
    run, _ = await run_flow(storage, tmp_path, definition, guided=True)
    node = run['definition']['nodes'][2]
    execution = next(a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'second')
    execution['node_result'] = {'status': 'blocked', 'result': {'blocker_category': 'missing_context'}, 'evidence': []}
    token = {'id': 'retry-choice', 'node_id': 'second', 'execution_complete': True, 'execution_activation_id': execution['id'], 'result': execution['node_result'], 'input_result_refs': [run['activations'][1]['id']]}
    choices = d.continuations(run, node, token, False, root=storage.root)
    retry = next(c for c in choices if c['kind'] == 'retry_execution')
    execution = next(a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'second')
    assert retry['available_sessions'][0]['task_id'] == execution['tasks'][-1]['task_id']

async def test_timeout_persists_deadline_before_cancellation(storage, tmp_path):
    definition = graph()
    definition['nodes'][1]['timeout_seconds'] = 1
    class ObservedRegistry(Registry):
        async def start(self, prompt, repo, **kwargs):
            task = await super().start(prompt, repo, **kwargs)
            if 'Assignment:\n' in prompt:
                task.done.clear()
                task.result['status'] = 'running'
            return task
        async def cancel_cascade(self, task_id, **kwargs):
            run = storage.list_runs()[0]
            task_record = next(t for a in run['activations'] for t in a['tasks'] if t['task_id'] == task_id)
            assert task_record['timeout_deadline'] <= task_record['timeout_requested_at']
            task = self.tasks[task_id]
            task.result['status'] = 'cancelled'
            task.done.set()
            return {}
    run, _ = await run_flow(storage, tmp_path, definition, ObservedRegistry(storage.root), guided=True)
    assert next(a for a in run['activations'] if a['role'] == 'node')['tasks'][0]['result']['timed_out']

async def test_restart_of_timed_live_task_requires_attention(storage, tmp_path, monkeypatch):
    from polybridge import workflows as w
    run = storage.create_run(w.validate_definition(graph()), 'Request', tmp_path)
    def seed(r):
        r.update(status='running', pending=[{'id': 'visit', 'node_id': 'work', 'stack': []}], execution_initialized=True)
        r['activations'] = [{'id': 'execution', 'role': 'node', 'node_id': 'work', 'status': 'running', 'token': {'id': 'visit'}, 'tasks': [{'task_id': 'lost-owner', 'candidate': {'backend': 'codex'}, 'status': 'running', 'dispatch_stage': 'spawn_confirmed', 'timeout_deadline': 1}]}]
    storage.update_run(run['workflow_run_id'], seed, 'crash_fixture')
    registry = Registry(storage.root)
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    restored = storage.get_run(run['workflow_run_id'])
    assert restored['status'] == 'needs_attention'
    assert 'persisted timeout deadline' in restored['attention_reason']
    assert registry.calls == []

async def test_timeout_allows_sigkill_grace_before_fallback(storage, tmp_path):
    import asyncio
    from polybridge import workflows as w
    definition = graph()
    definition['nodes'][1].update(timeout_seconds=1, agent={'backend': 'codex', 'fallbacks': [{'backend': 'claude'}]})
    class GraceRegistry(Registry):
        async def start(self, prompt, repo, **kwargs):
            task = await super().start(prompt, repo, **kwargs)
            if 'Assignment:\n' in prompt and kwargs['backend'].name == 'codex':
                task.done.clear()
                task.result['status'] = 'running'
            return task
        async def cancel_cascade(self, task_id, **kwargs):
            await asyncio.sleep(5.1)  # A process ignoring SIGTERM needs the full SIGKILL grace.
            self.tasks[task_id].result['status'] = 'cancelled'
            self.tasks[task_id].done.set()
            return {}
    run = storage.create_run(w.validate_definition(definition), 'Request', tmp_path)
    registry = GraceRegistry(storage.root)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id']), 12)
    restored = storage.get_run(run['workflow_run_id'])
    assert restored['status'] == 'completed'
    execution = next(a for a in restored['activations'] if a['role'] == 'node')
    assert execution['tasks'][0]['result']['timed_out']
    assert execution['tasks'][1]['candidate']['backend'] == 'claude'

def test_timeout_intent_restores_only_authoritative_settled_cancellation(storage, tmp_path, monkeypatch):
    from polybridge import workflows as w, store
    definition = w.validate_definition(graph())
    definition['nodes'][1]['timeout_seconds'] = 1
    run = storage.create_run(definition, 'Request', tmp_path)
    store.write(storage.root / 'tasks', store.TaskRecord(task_id='cancelled-task', backend='codex', session_id='session', repo_path=str(tmp_path), started_at='2026-10-05T00:00:00Z', freedom='write_in_repo', status='cancelled'))
    def seed(r):
        r['pending'] = [{'id': 'visit', 'node_id': 'work', 'stack': [], 'execution_activation_id': 'execution'}]
        r['activations'] = [{'id': 'execution', 'role': 'node', 'node_id': 'work', 'status': 'running', 'token': {'id': 'visit'}, 'tasks': [{'task_id': 'cancelled-task', 'candidate': {'backend': 'codex'}, 'status': 'uncertain', 'timeout_deadline': 1, 'timeout_requested_at': 2}]}]
    storage.update_run(run['workflow_run_id'], seed, 'crash_fixture')
    monkeypatch.setattr(w, 'task_liveness', lambda *a: {'process_alive': False, 'outcome_known': True})
    restored = storage.reconcile_run(run['workflow_run_id'])
    assert restored['activations'][0]['tasks'][0]['result']['timed_out']
    assert restored['pending'][0]['recovered_failed_result']['timed_out']


@pytest.mark.parametrize('protocol_repair', [False, True])
async def test_successful_retry_forwards_its_session_to_next_serial_node(storage, tmp_path, protocol_repair):
    calls = 0
    def work(prompt, kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {'summary': 'malformed final'} if protocol_repair else {'status': 'failed', 'result': {'summary': 'Retry this execution'}, 'evidence': []}
        return {'status': 'succeeded', 'result': {'summary': 'Succeeded'}, 'evidence': []}
    run, _ = await run_flow(storage, tmp_path, serial(), Registry(storage.root, outputs={'work': work}), guided=True)
    assert run['status'] == 'completed'
    workers = [a for a in run['activations'] if a['role'] == 'node']
    latest = next(a for a in reversed(workers) if a['node_id'] == 'work')
    downstream = next(a for a in workers if a['node_id'] == 'second')
    assert downstream['tasks'][0]['resume_task_id'] == latest['tasks'][-1]['task_id']
    assert downstream['tasks'][0]['session_mode'] == 'resume'


async def test_forward_continuity_ignores_previous_target_history_after_source_retry(storage, tmp_path):
    from polybridge import workflow_delegation as d
    run, _ = await run_flow(storage, tmp_path, serial(), guided=True)
    source = next(a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'work')
    source_node = next(n for n in run['definition']['nodes'] if n['id'] == 'work')
    token = {'id': 'forward', 'node_id': 'work', 'decision_id': 'decision', 'execution_complete': True, 'execution_activation_id': source['id'], 'retry_of_execution_id': 'older-source', 'result': source['node_result']}
    choice = next(c for c in d.continuations(run, source_node, token, False, root=storage.root) if c['node_id'] == 'second')
    assert choice['available_sessions'][0]['execution_id'] == source['id']
    _, assignments, _ = d.validate_decision(run, source_node, token, {'decision_id': 'decision', 'action': 'continue', 'reason': 'Continue after successful retry', 'next': [{'continuation_id': choice['continuation_id'], 'prompt': 'Next focused assignment'}]}, False, root=storage.root)
    assert assignments[choice['continuation_id']]['resume_source_execution_id'] == source['id']
