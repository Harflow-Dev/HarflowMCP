#!/usr/bin/env python3
"""Canonical restricted ST04 specimens from isolated production service calls.

No CLI, subprocess or provider runs. Synthetic launch owners and completed outer
jobs supply explicit observations; these specimens do not qualify real runtimes.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import asyncio
import copy
import hashlib
from itertools import count
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
from unittest.mock import patch
import uuid

from harflow_mcp import identity, state_ownership, task_supervision, task_withdrawal
from harflow_mcp import workflow_replan, workflow_steering as steering, workflows
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, envelope

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'
AT = 1793836800.0
DISPATCHER = {'pid': 6789, 'start_time': 'fixture-dispatcher-generation', 'markers': []}
OWNER = {'pid': 6790, 'start_time': 'fixture-launch-owner-generation', 'markers': []}


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()


async def specimens(base):
    base = base.resolve()
    identifiers = count(4000)
    output = {}
    with ExitStack() as stack:
        stack.enter_context(patch.object(steering, '_transport', return_value=None))
        stack.enter_context(patch.object(workflows, '_launch'))
        stack.enter_context(patch.object(identity, 'own_identity', return_value=DISPATCHER))
        stack.enter_context(patch.object(identity, 'identity_check', return_value='alive'))
        stack.enter_context(patch.object(identity, 'check_detail', return_value=('alive', 'synthetic-generation')))
        stack.enter_context(patch('time.time', return_value=AT))
        stack.enter_context(patch('uuid.uuid4', side_effect=lambda: uuid.UUID(int=next(identifiers))))
        stack.enter_context(state_ownership.state_session(resolve_layout(root=base)))
        (base / 'tasks').mkdir(exist_ok=True)
        storage = workflows.WorkflowStore(base)
        repo = base / 'repo'
        repo.mkdir()
        definition = workflows.validate_definition({'name': 'prestart-replan-example', 'context_delivery': 'legacy',
            'orchestrator': {'backend': 'claude'}, 'nodes': [
                {'id': 'start', 'type': 'start'},
                {'id': 'work', 'type': 'agent', 'role': 'task', 'instructions': 'Execute the configured assignment.',
                 'execution_mode': 'headless', 'session_mode': 'fresh', 'freedom': 'read_only', 'max_attempts': 3,
                 'agent': {'backend': 'claude', 'model': 'sonnet', 'max_turns': 17}},
                {'id': 'end', 'type': 'end'}],
            'connections': [{'id': 'begin', 'source': 'start', 'target': 'work'},
                            {'id': 'finish', 'source': 'work', 'target': 'end'}]})
        node = next(n for n in definition['nodes'] if n['id'] == 'work')
        def fresh(rid):
            return storage.get_run(rid, refresh_atomic=True)
        def put(name, result):
            output['ctl-steering-replan-' + name + '.json'] = (envelope(CTL_FORMAT, {'result': result}), 0)
        async def create():
            run = storage.create_run(definition, 'Configured original goal', repo, freedom='read_only', interaction_owner='monitor')
            rid = run['workflow_run_id']
            await steering.steer_workflow(rid, {'schema_version': 2, 'ledger_epoch': 0,
                'request_id': str(uuid.uuid4()), 'action': 'upgrade', 'expected_interaction_epoch': 0,
                'expected_control_revision': 0, 'reason': 'Use the qualified epoch-scoped protocol.'}, root=base, monitor=True)
            activation_id = uuid.uuid4().hex
            token = {'id': uuid.uuid4().hex, 'node_id': 'work', 'stack': [], 'context': {},
                'assignment_prompt': 'Review the configured source without changing its scope.', 'assigned_task_ids': [],
                'input_result_refs': [], 'execution_activation_id': activation_id, 'execution_session_mode': 'fresh',
                'accepted_decision_id': 'fixture-accepted-decision'}
            def setup(value):
                # Fixed synthetic source metadata keeps opaque configuration hashes portable.
                # No filesystem/provider operation is performed against this virtual repository.
                value.update(repo_path='/fixture/repo', status='running', execution_initialized=True,
                    supervisor_identity=copy.deepcopy(DISPATCHER), pending=[token], activations=[{
                        'id': activation_id, 'node_id': 'work', 'role': 'node', 'status': 'running', 'created_at': AT,
                        'token': copy.deepcopy(token), 'assignment_prompt': token['assignment_prompt'], 'assigned_task_ids': [],
                        'input_result_refs': [], 'execution_session_mode': 'fresh', 'visit_id': token['id'],
                        'attempt_in_visit': 1, 'tasks': []}])
            storage.update_run(rid, setup, 'fixture_replan_checkpoint')
            return rid
        source_jobs = {}
        async def seed_pending(rid):
            current = fresh(rid)
            activation_id = current['pending'][0]['execution_activation_id']
            activation = next(a for a in current['activations'] if a['id'] == activation_id)
            task_id = activation.get('planned_task_id') or uuid.uuid4().hex
            candidate = copy.deepcopy(node['agent'])
            def pending(value):
                a = next(a for a in value['activations'] if a['id'] == activation_id)
                a.update(status='running', tasks=[{'task_id': task_id, 'status': 'running', 'candidate': candidate,
                    'execution_kind': 'headless', 'session_mode': 'fresh', 'resume_task_id': None,
                    'dispatch_stage': 'spawn_requested', 'repo_path': '/fixture/repo', 'freedom': 'read_only', 'network': None,
                    'harness_metadata': {'provenance': 'validated_launch_configuration', 'verification_status': 'configured_not_observed',
                        'requested': copy.deepcopy(candidate), 'effective': {'backend': 'claude', 'model': 'sonnet', 'freedom': 'read_only'}, 'observed': None}}])
                value.update(status='running', supervisor_identity=copy.deepcopy(DISPATCHER))
            storage.update_run(rid, pending, 'fixture_pending_launch')
            info = (base / 'tasks').stat()
            launch = {'format': task_supervision.FORMAT, 'product': 'harflow', 'product_version': PRODUCT_VERSION, 'v': 1,
                'task_id': task_id, 'generation': uuid.uuid4().hex, 'root': str(base),
                'directory_identity': [info.st_dev, info.st_ino], 'directory_binding': {'path': str((base / 'tasks').resolve()), 'inode': info.st_ino},
                'state': 'waiting_for_resources', 'request_sha256': 'd' * 64, 'configuration_sha256': 'e' * 64,
                'backend': 'claude', 'session_id': str(uuid.uuid4()), 'parent_task_id': None, 'chain_root_task_id': None,
                'owner': copy.deepcopy(OWNER), 'repo_path': '/fixture/repo', 'freedom': 'read_only', 'network': None}
            task_supervision._write(base / 'tasks' / (task_id + '.launch.json'), launch)
            from harflow_mcp.replan_retention import new_outer_job, dispatcher_context
            ready, finish, acknowledged = asyncio.Event(), asyncio.Event(), asyncio.Event()
            supervisor = SimpleNamespace(store=storage, run_id=rid)
            async def source_node(token):
                origin = dispatcher_context(supervisor, activation_id, task_id)
                storage.update_run(rid, lambda value: next(a for a in value['activations'] if a['id'] == activation_id)['tasks'][-1].update(origin), 'fixture_outer_job_origin')
                ready.set()
                await finish.wait()
            supervisor._node = source_node
            coroutine, done = new_outer_job(supervisor, activation['token'], DISPATCHER)
            task = asyncio.create_task(coroutine)
            done.bind(task)
            def complete(owned):
                try:
                    done(owned)
                finally:
                    acknowledged.set()
            task.add_done_callback(complete)
            source_jobs[rid] = (finish, task, acknowledged)
            await ready.wait()
            return launch
        async def queue(rid):
            view = steering.get_workflow_steering(rid, root=base, monitor=True)
            target = next(t for t in view['targets'] if t['role'] == 'worker')
            await steering.steer_workflow(rid, {'schema_version': 2, 'ledger_epoch': view['ledger_epoch'],
                'request_id': str(uuid.uuid4()), 'action': 'open', 'expected_interaction_epoch': view['interaction_epoch'],
                'expected_control_revision': view['control_revision'], 'episode_id': None, 'expected_steering_revision': None,
                'target': target, 'text': 'Discuss this exact pending assignment.', 'allow_fresh_context': True}, root=base, monitor=True)
        def request(rid):
            view = steering.get_workflow_steering(rid, root=base, monitor=True)
            target = view['replan_targets'][0]
            ep = view['episodes'][-1]
            return {'schema_version': 2, 'action': 'replan', 'ledger_epoch': view['ledger_epoch'],
                'request_id': str(uuid.uuid4()), 'episode_id': ep['episode_id'], 'expected_steering_revision': ep['steering_revision'],
                'expected_control_revision': view['control_revision'], 'expected_interaction_epoch': view['interaction_epoch'],
                'reason': 'Withdraw only this never-started launch; keep its assignment.', **target}
        def acknowledge(launch):
            registry = SimpleNamespace(log_dir=base / 'tasks', _owner=copy.deepcopy(OWNER),
                _launch_generations={launch['task_id']: launch['generation']})
            try:
                task_withdrawal.check_prestart(registry, launch['task_id'])
            except task_withdrawal.LaunchWithdrawn as stopped:
                return task_withdrawal.settle_withdrawn(registry, launch['task_id'], stopped)
            raise AssertionError('Fixture owner did not observe its exact withdrawal')
        async def quiescent(rid):
            finish, task, acknowledged = source_jobs.pop(rid)
            finish.set()
            await task
            await acknowledged.wait()

        rid = await create()
        launch = await seed_pending(rid)
        await queue(rid)
        put('ready', steering.get_workflow_steering(rid, root=base, monitor=True))
        first = request(rid)
        put('admitted', await workflow_replan.replan_workflow_steering(rid, first, root=base, monitor=True))
        put('settling', steering.get_workflow_steering(rid, root=base, monitor=True))
        acknowledge(launch)
        workflow_replan.reconcile(storage, rid)
        await quiescent(rid)
        put('committed', steering.get_workflow_steering(rid, root=base, monitor=True))
        put('replayed', await workflow_replan.replan_workflow_steering(rid, first, root=base, monitor=True))
        for _ in range(39):
            launch = await seed_pending(rid)
            await queue(rid)
            await workflow_replan.replan_workflow_steering(rid, request(rid), root=base, monitor=True)
            acknowledge(launch)
            workflow_replan.reconcile(storage, rid)
            await quiescent(rid)
        window = steering.get_workflow_steering(rid, root=base, monitor=True)
        assert window['replan_accounting']['accepted'] == 40 and len(window['replans']) <= 32
        put('window-status', window)
        put('window-history', steering.get_workflow_steering_history(rid, root=base, monitor=True))
        try:
            await workflow_replan.replan_workflow_steering(rid, first, root=base, monitor=True)
        except steering.SteeringError as exc:
            output['ctl-steering-replan-expired.json'] = (envelope(CTL_FORMAT, {'error': {'code': exc.code, 'message': str(exc), 'data': exc.data}}), 1)
        else:
            raise AssertionError('Old request should have expired across the shared receipt window')
        rid = await create()
        launch = await seed_pending(rid)
        await queue(rid)
        await workflow_replan.replan_workflow_steering(rid, request(rid), root=base, monitor=True)
        task_supervision.mark(base / 'tasks', launch['task_id'], launch['generation'], state='spawned')
        registry = SimpleNamespace(log_dir=base / 'tasks', _owner=copy.deepcopy(OWNER),
            _launch_generations={launch['task_id']: launch['generation']})
        task_withdrawal.acknowledge_issued(registry, launch['task_id'])
        workflow_replan.reconcile(storage, rid)
        put('spawn-race-unknown', steering.get_workflow_steering(rid, root=base, monitor=True))
        await quiescent(rid)
    return output


def generate():
    with tempfile.TemporaryDirectory() as temporary:
        documents = asyncio.run(specimens(Path(temporary) / 'state'))
    manifest = json.loads((BUNDLE / 'manifest.json').read_text())
    output = {}
    for name, (document, code) in documents.items():
        output[BUNDLE / name] = canonical(document)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        manifest['cases'][name] = {'encoding': 'json', 'expected_product_version': PRODUCT_VERSION, 'format': CTL_FORMAT,
            'required_capabilities': ['workflow-steering-replan.v1', 'workflow-steering-worker.v1', 'workflow-steering-window.v1'],
            'valid': True, 'exit_code': code}
    output[BUNDLE / 'manifest.json'] = (json.dumps(manifest, indent=2, ensure_ascii=False) + '\n').encode()
    output[BUNDLE / 'manifest.sha256'] = (hashlib.sha256(output[BUNDLE / 'manifest.json']).hexdigest() + '  manifest.json\n').encode()
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    outputs = generate()
    if args.check:
        different = [str(p.relative_to(ROOT)) for p, data in outputs.items() if not p.exists() or p.read_bytes() != data]
        if different:
            raise SystemExit('Replan fixtures differ: ' + ', '.join(different))
    else:
        for path, data in outputs.items():
            path.write_bytes(data)
