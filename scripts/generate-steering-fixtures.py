#!/usr/bin/env python3
"""Generate ST02/ST03-A/ST03-B producer specimens from isolated service calls and a synthetic provider.

No backend CLI/provider is invoked. Admission/requests/status/history/final classification use
production application code with fixed clocks/identities and a fixture-only in-process task.
Owns ctl-steering-* specimens, steering version-info variants and their manifest entries.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
import uuid

from harflow_mcp import identity, state_ownership, workflow_steering as steering, workflows
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CAPABILITIES, CTL_FORMAT, PRODUCT_VERSION, VERSION_INFO_FORMAT, envelope

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'
AT = 1793836800.0


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()


class FixtureRegistry:
    """Only this fake can produce a provider result in this generator."""
    def __init__(self, root, final="Let us discuss the configured branches first.", **changes):
        self.final = final
        self.changes = changes
        self.log_dir = self._log_dir = root / 'tasks'

    async def start(self, prompt, repo, **kwargs):
        done = asyncio.Event()
        done.set()
        value = {'task_id': kwargs['task_id'], 'status': 'completed', 'exit_code': 0,
                 'output_drained': True, 'summary': self.final() if callable(self.final) else self.final,
                 'session_id': 'fixture-session', 'usage': {'input_tokens': 10, 'output_tokens': 8},
                 'total_cost_usd': None, 'num_turns': 1, **self.changes}
        return SimpleNamespace(task_id=kwargs['task_id'], done=done, snapshot=lambda: value)


async def specimens(base):
    from itertools import count
    ids = count(100)
    output = {}
    descriptor = {'name': 'steering-example', 'orchestrator': {'backend': 'claude'}, 'nodes': [], 'connections': []}
    with ExitStack() as stack:
        stack.enter_context(patch.object(steering, '_transport', return_value=None))
        stack.enter_context(patch.object(workflows, '_launch'))
        stack.enter_context(patch('time.time', return_value=AT))
        stack.enter_context(patch('uuid.uuid4', side_effect=lambda: uuid.UUID(int=next(ids))))
        stack.enter_context(patch.object(identity, 'own_identity', return_value={'pid': 6789, 'start_time': 'fixture-generation', 'markers': []}))
        stack.enter_context(state_ownership.state_session(resolve_layout(root=base)))
        storage = workflows.WorkflowStore(base)
        repo = base / 'repo'
        repo.mkdir()
        run = storage.create_run(descriptor, 'Discuss this workflow', repo, freedom='read_only', kind='builder', interaction_owner='monitor')
        rid = run['workflow_run_id']
        storage.update_run(rid, lambda r: r.update(status='completed', builder_draft=descriptor, draft_revision=0), 'fixture')
        def put(name, result):
            output['ctl-steering-' + name + '.json'] = (envelope(CTL_FORMAT, {'result': result}), 0)
        put('builder-ready', steering.get_workflow_steering(rid, root=base, monitor=True))
        request = {'schema_version': 1, 'request_id': str(uuid.UUID(int=1)), 'action': 'open',
                   'expected_interaction_epoch': 0, 'expected_control_revision': 0,
                   'episode_id': None, 'expected_steering_revision': None,
                   'target': steering.target(storage.get_run(rid)),
                   'text': 'Can we discuss the division of work first?', 'allow_fresh_context': True}
        put('builder-admitted', await steering.steer_workflow(rid, request, root=base, monitor=True))
        put('builder-replayed', await steering.steer_workflow(rid, request, root=base, monitor=True))
        changed = {**request, 'text': 'Different payload under the same request identity'}
        try:
            await steering.steer_workflow(rid, changed, root=base, monitor=True)
        except steering.SteeringError as exc:
            output['ctl-steering-request-conflict.json'] = (envelope(CTL_FORMAT, {'error': {'code': exc.code, 'message': str(exc), 'data': exc.data}}), 1)
        supervisor = workflows.WorkflowSupervisor(FixtureRegistry(base), storage)
        await steering.run_builder_turn(supervisor, rid)
        put('builder-awaiting-caller', steering.get_workflow_steering(rid, root=base, monitor=True))
        put('builder-history', steering.get_workflow_steering_history(rid, root=base, monitor=True))
        current = steering.get_workflow_steering(rid, root=base, monitor=True)
        ep = current['episodes'][-1]
        reply = {**request, 'request_id': str(uuid.UUID(int=2)), 'action': 'reply',
                 'expected_control_revision': current['control_revision'],
                 'expected_interaction_epoch': current['interaction_epoch'],
                 'episode_id': ep['episode_id'], 'expected_steering_revision': ep['steering_revision'],
                 'target': current['targets'][0], 'text': 'Please propose the next draft.'}
        await steering.steer_workflow(rid, reply, root=base, monitor=True)
        await steering.run_builder_turn(workflows.WorkflowSupervisor(FixtureRegistry(base, 'HARFLOW_COMMIT_V1\n{bad json\nEND_HARFLOW_COMMIT_V1'), storage), rid)
        put('builder-command-refused', steering.get_workflow_steering(rid, root=base, monitor=True))
        current = steering.get_workflow_steering(rid, root=base, monitor=True)
        ep = current['episodes'][-1]
        proposal_request = {**reply, 'request_id': str(uuid.UUID(int=4)),
            'expected_control_revision': current['control_revision'],
            'expected_steering_revision': ep['steering_revision'], 'target': current['targets'][0]}
        await steering.steer_workflow(rid, proposal_request, root=base, monitor=True)
        turn = storage.get_run(rid)['steering']['turns'][-1]
        proposal = {'name': 'steering-example', 'orchestrator': {'backend': 'claude'},
            'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'end', 'type': 'end'}],
            'connections': [{'id': 'done', 'source': 'start', 'target': 'end'}]}
        wrapper = {key: turn[key] for key in ('episode_id', 'turn_id', 'commit_target_ref', 'steering_revision')}
        wrapper.update({key: turn['commit_target'][key] for key in ('interaction_epoch', 'checkpoint_revision', 'goal_revision')})
        wrapper.update(role='builder', command={'definition': proposal, 'expected_draft_revision': turn['commit_target']['draft_revision']})
        framed = 'HARFLOW_COMMIT_V1\n' + json.dumps(wrapper) + '\nEND_HARFLOW_COMMIT_V1'
        await steering.run_builder_turn(workflows.WorkflowSupervisor(FixtureRegistry(base, framed), storage), rid)
        put('builder-committed', steering.get_workflow_steering(rid, root=base, monitor=True))
        active = storage.create_run(descriptor, 'Active builder', repo, freedom='read_only', kind='builder', interaction_owner='monitor')
        aid = active['workflow_run_id']
        storage.update_run(aid, lambda r: r.update(status='running', builder_draft=descriptor, draft_revision=0,
            activations=[{'id': 'old-activation', 'node_id': 'builder', 'role': 'builder', 'status': 'running',
                          'created_at': AT, 'tasks': [{'task_id': 'old-task', 'status': 'running'}]}]), 'fixture')
        queued = {**request, 'request_id': str(uuid.UUID(int=3)), 'target': steering.target(storage.get_run(aid))}
        put('builder-queued', await steering.steer_workflow(aid, queued, root=base, monitor=True))
        put('builder-summary', steering.summary(storage.get_run(rid)))
    return output


async def orchestrator_specimens(base):
    from itertools import count
    from harflow_mcp import orchestrator_steering
    ids = count(1000)
    output = {}
    with ExitStack() as stack:
        stack.enter_context(patch.object(steering, '_transport', return_value=None))
        stack.enter_context(patch.object(workflows, '_launch'))
        stack.enter_context(patch('time.time', return_value=AT))
        stack.enter_context(patch('uuid.uuid4', side_effect=lambda: uuid.UUID(int=next(ids))))
        stack.enter_context(patch.object(identity, 'own_identity', return_value={'pid': 6789, 'start_time': 'fixture-generation', 'markers': []}))
        stack.enter_context(state_ownership.state_session(resolve_layout(root=base)))
        storage = workflows.WorkflowStore(base)
        repo = base / 'repo'
        repo.mkdir()
        definition = workflows.validate_definition({'name': 'orchestrator-steering-example',
            'orchestrator': {'backend': 'claude'},
            'nodes': [{'id': 'start', 'type': 'start', 'always_route': True},
                      {'id': 'work', 'type': 'agent', 'instructions': 'Configured review', 'agent': {'backend': 'claude'}},
                      {'id': 'end', 'type': 'end'}],
            'connections': [{'id': 'begin', 'source': 'start', 'target': 'work'},
                            {'id': 'finish', 'source': 'work', 'target': 'end'}]})
        run = storage.create_run(definition, 'Discuss configured execution', repo, freedom='read_only', interaction_owner='monitor')
        rid = run['workflow_run_id']
        storage.update_run(rid, lambda r: r.update(status='running', execution_initialized=True,
            pending=[{'id': 'fixture-token', 'node_id': 'start', 'stack': [], 'context': {},
                      'decision_id': 'fixture-decision', 'decision_attempts': 0,
                      'on_demand_count': 0, 'on_demand_results': []}]), 'fixture')
        def put(name, result):
            output['ctl-steering-orchestrator-' + name + '.json'] = (envelope(CTL_FORMAT, {'result': result}), 0)
        def request(action='open', number=10):
            status = steering.get_workflow_steering(rid, root=base, monitor=True)
            episode = status['episodes'][-1] if status['episodes'] else None
            return {'schema_version': 1, 'request_id': str(uuid.UUID(int=number)), 'action': action,
                'expected_interaction_epoch': status['interaction_epoch'], 'expected_control_revision': status['control_revision'],
                'episode_id': episode['episode_id'] if episode else None,
                'expected_steering_revision': episode['steering_revision'] if episode else None,
                'target': status['targets'][0], 'text': 'Discuss this configured choice.', 'allow_fresh_context': True}
        put('ready', steering.get_workflow_steering(rid, root=base, monitor=True))
        first = request()
        put('admitted', await steering.steer_workflow(rid, first, root=base, monitor=True))
        put('replayed', await steering.steer_workflow(rid, first, root=base, monitor=True))
        await orchestrator_steering.run_turn(workflows.WorkflowSupervisor(FixtureRegistry(base), storage), rid)
        awaiting = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert awaiting['episodes'][-1]['state'] == 'awaiting_caller'
        put('awaiting-caller', awaiting)
        await steering.steer_workflow(rid, request('reply', 11), root=base, monitor=True)
        await orchestrator_steering.run_turn(workflows.WorkflowSupervisor(FixtureRegistry(base, '{"decision_id":"fixture-decision","action":"continue"}'), storage), rid)
        refused = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert refused['episodes'][-1]['state'] == 'command_refused'
        put('command-refused', refused)
        await steering.steer_workflow(rid, request('reply', 12), root=base, monitor=True)
        turn = storage.get_run(rid)['steering']['turns'][-1]
        wrapper = {key: turn[key] for key in ('episode_id', 'turn_id', 'commit_target_ref', 'steering_revision')}
        wrapper.update({key: turn['commit_target'][key] for key in ('interaction_epoch', 'checkpoint_revision', 'goal_revision')})
        wrapper.update(role='orchestrator', command={'decision_id': 'fixture-decision', 'action': 'continue',
            'reason': 'Use the configured review.', 'next': [{'continuation_id': 'begin', 'prompt': 'Review configured work.', 'session_mode': 'fresh'}]})
        framed = 'HARFLOW_COMMIT_V1\n' + json.dumps(wrapper) + '\nEND_HARFLOW_COMMIT_V1'
        await orchestrator_steering.run_turn(workflows.WorkflowSupervisor(FixtureRegistry(base, framed), storage), rid)
        committed = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert committed['episodes'][-1]['state'] == 'committed'
        assert len(storage.get_run(rid)['decisions']) == 1
        put('committed', committed)
        put('history', steering.get_workflow_steering_history(rid, root=base, monitor=True))
        put('summary', steering.summary(storage.get_run(rid)))
        def checkpoint():
            isolated_repo = base / ('scenario-' + uuid.uuid4().hex)
            isolated_repo.mkdir()
            created = storage.create_run(definition, 'Another configured checkpoint', isolated_repo, freedom='read_only', interaction_owner='monitor')
            current_id = created['workflow_run_id']
            storage.update_run(current_id, lambda r: r.update(status='running', execution_initialized=True,
                pending=[{'id': 'fixture-token', 'node_id': 'start', 'stack': [], 'context': {},
                          'decision_id': 'fixture-decision', 'decision_attempts': 0,
                          'on_demand_count': 0, 'on_demand_results': []}]), 'fixture')
            return current_id
        rid = checkpoint()
        storage.update_run(rid, lambda r: (r['activations'].append({'id': 'fixture-ordinary', 'node_id': 'start', 'role': 'orchestrator', 'status': 'running', 'created_at': AT,
            'token': r['pending'][0].copy(), 'decision_id': 'fixture-decision',
            'tasks': [{'task_id': 'fixture-boundary', 'status': 'running', 'candidate': {'backend': 'claude'}}]}),
            r['pending'][0].update(decision_activation_id='fixture-ordinary')), 'fixture')
        put('queued', await steering.steer_workflow(rid, request(number=13), root=base, monitor=True))
        rid = checkpoint()
        await steering.steer_workflow(rid, request(number=14), root=base, monitor=True)
        await orchestrator_steering.run_turn(workflows.WorkflowSupervisor(FixtureRegistry(base, output_drained=False), storage), rid)
        unknown = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert unknown['episodes'][-1]['state'] == 'delivery_unknown'
        put('capture-unknown', unknown)
        rid = checkpoint()
        await steering.steer_workflow(rid, request(number=15), root=base, monitor=True)
        storage.update_run(rid, lambda r: r.update(goal='Changed goal', goal_revision=1), 'fixture_goal_change')
        await orchestrator_steering.run_turn(workflows.WorkflowSupervisor(FixtureRegistry(base), storage), rid)
        stale = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert stale['episodes'][-1]['state'] == 'stale'
        put('stale', stale)
        # Real pinned reciprocal child records, still no process/provider invocation.
        from harflow_mcp import workflow_invocation, workflow_references, workflow_context
        child_definition = {**definition, 'name': 'nested-steering-child', 'context_delivery': 'optimized_v1'}
        saved = storage.save('nested-steering-child', child_definition)
        parent_definition = workflows.validate_definition({'name': 'nested-steering-parent', 'routing_mode': 'explicit',
            'orchestrator': {'backend': 'claude'},
            'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'call', 'type': 'workflow',
                'instructions': 'Run the configured child.', 'workflow_ref': {'workflow_id': saved['workflow_id']},
                'orchestrator_mode': 'child'}, {'id': 'end', 'type': 'end'}],
            'connections': [{'id': 'enter', 'source': 'start', 'target': 'call'}, {'id': 'finish', 'source': 'call', 'target': 'end'}]})
        nested_repo = base / 'nested-repo'
        nested_repo.mkdir()
        parent = storage.create_run(parent_definition, 'Root configured goal', nested_repo, freedom='read_only',
            dependency_tree=workflow_references.resolve_dependencies(storage, definition=parent_definition), interaction_owner='monitor')
        rid, child_id = parent['workflow_run_id'], uuid.uuid4().hex
        parent_token = {'id': 'parent-token', 'node_id': 'call', 'stack': [], 'context': {},
            'assignment_prompt': 'Configured child assignment.', 'execution_activation_id': 'child-invocation'}
        pinned_child = parent['dependency_tree']['workflows'][saved['workflow_id']]
        invocation = {'child_workflow_run_id': child_id, 'stage': 'running', 'workflow_id': saved['workflow_id'],
            'workflow_name': 'nested-steering-child', 'revision': pinned_child['revision'],
            'definition_sha256': pinned_child['definition_sha256'],
            'orchestrator_mode': 'child', 'timeout_seconds': None, 'timeout_elapsed_seconds': 0}
        activation = {'id': 'child-invocation', 'node_id': 'call', 'role': 'node', 'status': 'running',
            'created_at': AT, 'tasks': [], 'token': parent_token.copy(), 'invocation': invocation}
        parent = storage.update_run(rid, lambda r: r.update(status='running', execution_initialized=True,
            pending=[parent_token], activations=[activation]), 'fixture')
        node = next(n for n in parent_definition['nodes'] if n['id'] == 'call')
        child = workflow_invocation.child_run_record(storage, parent, activation, node, invocation)
        child.update(status='running', execution_initialized=True, pending=[{'id': 'fixture-token', 'node_id': 'start',
            'stack': [], 'context': {}, 'decision_id': 'fixture-decision', 'decision_attempts': 0,
            'on_demand_count': 0, 'on_demand_results': []}])
        workflow_invocation.write_child_run(storage, child)
        put('nested-ready', steering.get_workflow_steering(rid, root=base, monitor=True))
        put('nested-admitted', await steering.steer_workflow(rid, request(number=16), root=base, monitor=True))
        def nested_frame():
            turn = storage.get_run(rid)['steering']['turns'][-1]
            wrapper = {key: turn[key] for key in ('episode_id', 'turn_id', 'commit_target_ref', 'steering_revision')}
            wrapper.update({key: turn['commit_target'][key] for key in ('interaction_epoch', 'checkpoint_revision', 'goal_revision')})
            wrapper.update(role='orchestrator', command={'decision_id': 'fixture-decision', 'action': 'continue',
                'reason': 'Use configured child work.', 'next': [{'continuation_id': 'begin', 'prompt': 'Review the child assignment.', 'session_mode': 'fresh'}],
                'context_ack': workflow_context.context_ack(turn['context_delivery'])})
            return 'HARFLOW_COMMIT_V1\n' + json.dumps(wrapper) + '\nEND_HARFLOW_COMMIT_V1'
        await orchestrator_steering.run_turn(workflows.WorkflowSupervisor(FixtureRegistry(base, nested_frame), storage), rid)
        result = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert result['episodes'][-1]['state'] == 'committed'
        assert len(storage.get_run(child_id)['decisions']) == 1 and not storage.get_run(rid)['decisions']
        put('nested-committed', result)
        put('nested-history', steering.get_workflow_steering_history(rid, root=base, monitor=True))
    return output


class FixtureWorkerRegistry(FixtureRegistry):
    """Synthetic provider with durable task bytes for production ownership readers."""
    def __init__(self, root, final="Discuss the exact assignment before submitting a result.", **changes):
        super().__init__(root, final, **changes)
        self.tasks = {}

    async def start(self, prompt, repo, **kwargs):
        from harflow_mcp import store
        task = await super().start(prompt, repo, **kwargs)
        value = task.snapshot()
        value.update(timed_out=False, outcome_unknown=False, is_error=False)
        self.tasks[task.task_id] = task
        store.write(self.log_dir, store.TaskRecord(task_id=task.task_id, backend=kwargs['backend'].name,
            session_id=value['session_id'], repo_path=str(repo), started_at='2026-11-05T00:00:00Z',
            freedom=kwargs['freedom'], network=kwargs.get('network'), model=kwargs.get('model'),
            reasoning_effort=kwargs.get('reasoning_effort'), max_turns=kwargs['max_turns'],
            status=value['status'], exit_code=value['exit_code'], output_drained=value['output_drained']))
        return task

    def attach(self, task_id):
        return self.tasks.get(task_id)


async def worker_specimens(base):
    """Actual explicit v2 upgrade, worker service and bounded history projection."""
    from itertools import count
    from harflow_mcp import store
    from harflow_mcp.worker_steering_runner import run_worker_turn
    ids = count(2000)
    output = {}
    with ExitStack() as stack:
        stack.enter_context(patch.object(steering, '_transport', return_value=None))
        stack.enter_context(patch.object(workflows, '_launch'))
        stack.enter_context(patch('time.time', return_value=AT))
        stack.enter_context(patch('uuid.uuid4', side_effect=lambda: uuid.UUID(int=next(ids))))
        stack.enter_context(patch.object(identity, 'own_identity', return_value={'pid': 6789, 'start_time': 'fixture-generation', 'markers': []}))
        stack.enter_context(state_ownership.state_session(resolve_layout(root=base)))
        storage = workflows.WorkflowStore(base)
        definition = workflows.validate_definition({'name': 'worker-steering-example',
            'orchestrator': {'backend': 'claude'},
            'nodes': [{'id': 'start', 'type': 'start'},
                      {'id': 'work', 'type': 'agent', 'role': 'implementation',
                       'instructions': 'Perform only the immutable assigned work.',
                       'agent': {'backend': 'claude', 'model': 'sonnet', 'max_turns': 17},
                       'freedom': 'write_in_repo', 'execution_mode': 'headless'},
                      {'id': 'end', 'type': 'end'}],
            'connections': [{'id': 'begin', 'source': 'start', 'target': 'work'},
                            {'id': 'finish', 'source': 'work', 'target': 'end'}]})
        def put(name, result):
            output['ctl-steering-' + name + '.json'] = (envelope(CTL_FORMAT, {'result': result}), 0)
        def checkpoint(label):
            repo = base / label
            repo.mkdir()
            run = storage.create_run(definition, 'Complete the exact assigned task.', repo,
                freedom='write_in_repo', interaction_owner='monitor')
            rid = run['workflow_run_id']
            provider_id = 'ordinary-' + label
            execution_id = 'execution-' + label
            token = {'id': 'token-' + label, 'node_id': 'work', 'stack': [], 'context': {},
                'assignment_prompt': 'Implement only fixture-task.', 'assigned_task_ids': ['fixture-task'],
                'input_result_refs': [], 'execution_activation_id': execution_id,
                'execution_session_mode': 'fresh', 'accepted_decision_id': 'accepted-' + label}
            candidate = next(n['agent'] for n in definition['nodes'] if n['id'] == 'work')
            ordinary = {'task_id': provider_id, 'status': 'completed', 'exit_code': 0,
                'output_drained': True, 'timed_out': False, 'outcome_unknown': False, 'is_error': False,
                'summary': 'Unaccepted ordinary output.', 'session_id': 'ordinary-session-' + label,
                'usage': {'input_tokens': 9, 'output_tokens': 4}, 'num_turns': 7, 'total_cost_usd': 0.01}
            activation = {'id': execution_id, 'node_id': 'work', 'role': 'node', 'status': 'running',
                'created_at': AT, 'token': token.copy(), 'assignment_prompt': token['assignment_prompt'],
                'assigned_task_ids': ['fixture-task'], 'input_result_refs': [],
                'execution_session_mode': 'fresh', 'visit_id': token['id'], 'attempt_in_visit': 1,
                'tasks': [{'task_id': provider_id, 'status': 'completed', 'candidate': candidate,
                    'repo_path': str(repo), 'freedom': 'write_in_repo', 'network': None, 'result': ordinary}]}
            storage.update_run(rid, lambda r: r.update(status='running', runner_policy='guided',
                execution_initialized=True, pending=[token], activations=[activation],
                tasks=[{'id': 'fixture-task', 'title': 'Bounded assigned work', 'description': 'Preserve the immutable assignment.', 'status': 'pending', 'source_activation_id': 'fixture-plan'}]), 'fixture_worker_checkpoint')
            store.write(base / 'tasks', store.TaskRecord(task_id=provider_id, backend='claude',
                session_id=ordinary['session_id'], repo_path=str(repo), started_at='2026-11-05T00:00:00Z',
                freedom='write_in_repo', model='sonnet', max_turns=17, status='completed', exit_code=0, output_drained=True))
            return rid
        async def upgrade(rid):
            status = steering.get_workflow_steering(rid, root=base, monitor=True)
            return await steering.steer_workflow(rid, {'schema_version': 2, 'ledger_epoch': 0,
                'request_id': str(uuid.uuid4()), 'action': 'upgrade',
                'expected_interaction_epoch': status['interaction_epoch'],
                'expected_control_revision': status['control_revision'],
                'reason': 'Use the explicitly approved bounded epoch-scoped steering protocol.'}, root=base, monitor=True)
        def request(rid, action='open'):
            status = steering.get_workflow_steering(rid, root=base, monitor=True)
            episode = status['episodes'][-1] if action != 'open' else None
            return {'schema_version': 2, 'ledger_epoch': status['ledger_epoch'], 'request_id': str(uuid.uuid4()),
                'action': action, 'expected_interaction_epoch': status['interaction_epoch'],
                'expected_control_revision': status['control_revision'],
                'episode_id': episode['episode_id'] if episode else None,
                'expected_steering_revision': episode['steering_revision'] if episode else None,
                'target': next(t for t in status['targets'] if t['role'] == 'worker'),
                'text': 'Discuss this exact worker assignment.', 'allow_fresh_context': True}
        rid = checkpoint('main')
        put('window-upgrade-admitted', await upgrade(rid))
        put('window-upgraded-status', steering.get_workflow_steering(rid, root=base, monitor=True))
        put('worker-ready', steering.get_workflow_steering(rid, root=base, monitor=True))
        put('worker-admitted', await steering.steer_workflow(rid, request(rid), root=base, monitor=True))
        registry = FixtureWorkerRegistry(base)
        await run_worker_turn(workflows.WorkflowSupervisor(registry, storage), rid)
        awaiting = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert awaiting['episodes'][-1]['state'] == 'awaiting_caller'
        put('worker-awaiting-caller', awaiting)
        put('worker-history', steering.get_workflow_steering_history(rid, root=base, monitor=True))
        # Exceed the former 32 lifetime issuance limit through real explicit requests.
        for _ in range(39):
            await steering.steer_workflow(rid, request(rid, 'reply'), root=base, monitor=True)
            await run_worker_turn(workflows.WorkflowSupervisor(registry, storage), rid)
        window_status = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert window_status['budgets']['root_issuances'] == 40
        assert len(storage.get_run(rid)['steering']['turns']) <= 33
        put('window-status', window_status)
        put('window-history', steering.get_workflow_steering_history(rid, root=base, monitor=True))
        await steering.steer_workflow(rid, request(rid, 'reply'), root=base, monitor=True)
        turn = storage.get_run(rid)['steering']['turns'][-1]
        wrapper = {key: turn[key] for key in ('episode_id', 'turn_id', 'commit_target_ref', 'steering_revision')}
        wrapper.update({key: turn['commit_target'][key] for key in ('interaction_epoch', 'checkpoint_revision', 'goal_revision')})
        wrapper.update(role='worker', command={'status': 'succeeded',
            'result': {'summary': 'Completed only the immutable assignment.', 'completed_task_ids': ['fixture-task']},
            'evidence': ['Synthetic provider observed one finite response.']})
        registry.final = 'HARFLOW_COMMIT_V1\n' + json.dumps(wrapper) + '\nEND_HARFLOW_COMMIT_V1'
        await run_worker_turn(workflows.WorkflowSupervisor(registry, storage), rid)
        committed = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert committed['episodes'][-1]['state'] == 'committed'
        assert len([a for a in storage.get_run(rid)['activations'] if a['role'] == 'node']) == 1
        put('worker-committed', committed)
        rid = checkpoint('unknown')
        await upgrade(rid)
        await steering.steer_workflow(rid, request(rid), root=base, monitor=True)
        await run_worker_turn(workflows.WorkflowSupervisor(FixtureWorkerRegistry(base, output_drained=False), storage), rid)
        unknown = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert unknown['episodes'][-1]['state'] == 'delivery_unknown'
        put('worker-capture-unknown', unknown)
        rid = checkpoint('stale')
        await upgrade(rid)
        await steering.steer_workflow(rid, request(rid), root=base, monitor=True)
        storage.update_run(rid, lambda r: r.update(goal='A revised caller goal.', goal_revision=1), 'fixture_goal_change')
        await run_worker_turn(workflows.WorkflowSupervisor(FixtureWorkerRegistry(base), storage), rid)
        stale = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert stale['episodes'][-1]['state'] == 'command_refused'
        assert storage.get_run(rid)['steering']['turns'][-1]['outcome'] == 'steering_target_stale'
        put('worker-stale', stale)
    return output


def generate():
    with tempfile.TemporaryDirectory() as temporary:
        documents = asyncio.run(specimens(Path(temporary) / 'state'))
        documents.update(asyncio.run(orchestrator_specimens(Path(temporary) / 'orchestrator-state')))
        documents.update(asyncio.run(worker_specimens(Path(temporary) / 'worker-state')))
    manifest = json.loads((BUNDLE / 'manifest.json').read_text())
    output = {}
    for name, (document, code) in documents.items():
        output[BUNDLE / name] = canonical(document)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        manifest['cases'][name] = {'encoding': 'json', 'expected_product_version': PRODUCT_VERSION,
                                  'format': CTL_FORMAT, 'required_capabilities': (['workflow-steering-worker.v1', 'workflow-steering-window.v1'] if name.startswith('ctl-steering-worker-') else ['workflow-steering-window.v1'] if name.startswith('ctl-steering-window-') else ['workflow-steering-orchestrator.v1'] if name.startswith('ctl-steering-orchestrator-') else ['workflow-steering-builder.v1']),
                                  'valid': True, 'exit_code': code}
    version = json.loads((BUNDLE / 'version-info.json').read_text())
    version.update(product_version=PRODUCT_VERSION, capabilities=list(CAPABILITIES))
    output[BUNDLE / 'version-info.json'] = canonical(version)
    manifest['files']['version-info.json'] = hashlib.sha256(output[BUNDLE / 'version-info.json']).hexdigest()
    for capability in ('workflow-steering-read.v1', 'workflow-steering-builder.v1', 'workflow-steering-orchestrator.v1', 'workflow-steering-worker.v1', 'workflow-steering-window.v1'):
        assert capability in CAPABILITIES
        name = 'invalid-steering-missing-' + capability.rsplit('.', 1)[0] + '.json'
        doc = {**version, 'capabilities': [item for item in version['capabilities'] if item != capability]}
        output[BUNDLE / name] = canonical(doc)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        manifest['cases'][name] = {'encoding': 'json', 'expected_product_version': PRODUCT_VERSION,
                                  'format': VERSION_INFO_FORMAT, 'required_capabilities': [capability], 'valid': False}
    output[BUNDLE / 'manifest.json'] = (json.dumps(manifest, indent=2, ensure_ascii=False) + '\n').encode()
    output[BUNDLE / 'manifest.sha256'] = (hashlib.sha256(output[BUNDLE / 'manifest.json']).hexdigest() + '  manifest.json\n').encode()
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    outputs = generate()
    if args.check:
        mismatches = [str(path.relative_to(ROOT)) for path, content in outputs.items() if not path.exists() or path.read_bytes() != content]
        if mismatches:
            raise SystemExit('Steering fixtures differ: ' + ', '.join(mismatches))
    else:
        for path, content in outputs.items():
            path.write_bytes(content)
