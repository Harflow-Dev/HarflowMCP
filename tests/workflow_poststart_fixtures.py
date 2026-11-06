"""Unpaid workflow fixtures; provider entry points are forbidden, never emulated."""
from __future__ import annotations

import asyncio
import copy
import json
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from harflow_mcp import workflows as w
from harflow_mcp.state_layout import resolve_layout


def graph():
    return {
        'name': 'poststart-checkpoints', 'orchestrator': {'backend': 'codex'},
        'nodes': [
            {'id': 'start', 'type': 'start', 'prompt': 'Bounded purpose'},
            {'id': 'work', 'type': 'agent', 'role': 'task', 'instructions': 'Immutable node instructions',
             'max_attempts': 3, 'agent': {'backend': 'codex'}},
            {'id': 'end', 'type': 'end'},
        ],
        'connections': [{'id': 'begin', 'source': 'start', 'target': 'work'},
                        {'id': 'finish', 'source': 'work', 'target': 'end'}],
    }


class RejectRegistry:
    """Every ordinary registry effect is a test failure, including discovery attach."""
    def __init__(self, root):
        self.log_dir = self._log_dir = root / 'tasks'
        self.state_layout = resolve_layout(log_dir=self.log_dir)
        self.calls = []

    async def start(self, *args, **kwargs):
        self.calls.append(('start', args, kwargs))
        raise AssertionError('ordinary provider start is forbidden')

    async def resume(self, *args, **kwargs):
        self.calls.append(('resume', args, kwargs))
        raise AssertionError('ordinary provider resume is forbidden')

    async def cancel_cascade(self, *args, **kwargs):
        self.calls.append(('cancel', args, kwargs))
        raise AssertionError('ordinary provider cancellation is forbidden')

    def get(self, task_id):
        return None

    def attach(self, *args, **kwargs):
        self.calls.append(('attach', args, kwargs))
        raise AssertionError('ordinary task attachment is forbidden')


class AtomicEffects:
    """Independent atomic fence/effect oracle, with deterministic boundary cuts."""
    synthetic = True
    adapter_id = 'workflow-checkpoint-test-v1'

    def __init__(self):
        self.profile = dict(name='synthetic-only', version='1', digest='a' * 64,
                            expected_time_budgets=dict(stop=1, grace=2, execution=3, background=4))
        self.effect_verifier = dict(name='synthetic-enforced-absence', version='1', digest='b' * 64)
        self.inventory = [dict(actor='original', generation='generation-1', obligation='required', settlement='satisfied')]
        self.observing = asyncio.Event()
        self.settled = asyncio.Event()
        self.task_snapshot = None
        self.lock = threading.Lock()
        self.generation = 1
        self.evidence_revision = 0
        self.effects = []
        self.before_effect = None
        self.lost_ack = False

    def invalidate(self, *, root, generation, evidence_revision):
        with self.lock:
            if generation < self.generation or (generation == self.generation and evidence_revision < self.evidence_revision):
                raise RuntimeError('oracle refuses fence reversal')
            self.generation, self.evidence_revision = generation, evidence_revision

    def execute(self, kind, command_id, task_id, expected_generation, root, expected_evidence_revision):
        if self.before_effect:
            self.before_effect()
        with self.lock:
            if (expected_generation, expected_evidence_revision) != (self.generation, self.evidence_revision):
                raise RuntimeError('oracle refuses stale effect')
            if any(e['command_id'] == command_id for e in self.effects):
                raise AssertionError('duplicate physical effect')
            receipt = dict(kind=kind, command_id=command_id, task_id=task_id, root=root,
                           generation=expected_generation, evidence_revision=expected_evidence_revision)
            self.effects.append(receipt)
            if self.lost_ack:
                raise RuntimeError('oracle lost acknowledgement after effect')
            return copy.deepcopy(receipt)

    async def observe(self, task_id):
        self.observing.set()
        await self.settled.wait()
        return {'task_id': task_id, **(copy.deepcopy(self.task_snapshot) if self.task_snapshot else outcome())}


def host_observations(stack):
    """Only caller/host observations are replaced; storage and checkpoints stay real."""
    from harflow_mcp import identity, lineage, workflow_interactions
    principal = {'kind': 'external_parent', 'identity': {'pid': 12345, 'start_time': 'test-owner-generation', 'markers': []}}
    stack.enter_context(patch.object(lineage, 'detect_caller_detail', return_value=SimpleNamespace(caller=None, undecidable=None)))
    stack.enter_context(patch.object(workflow_interactions, 'capture_transport_parent', return_value=copy.deepcopy(principal)))
    stack.enter_context(patch.object(workflow_interactions, '_transport_caller', principal))
    stack.enter_context(patch.object(identity, 'check_detail', return_value=('alive', 'matched')))
    stack.enter_context(patch.object(identity, 'identity_check', return_value='alive'))
    stack.enter_context(patch.object(w.backends, 'is_installed', return_value=True))
    # Discovery is forbidden, even when a caller accidentally asks for a launch.
    stack.enter_context(patch.object(w, '_launch', side_effect=AssertionError('automatic workflow launch forbidden')))
    return principal


def create_workflow(root: Path, *, definition=None, assigned_task_ids=None):
    storage = w.WorkflowStore(root / 'state')
    run = storage.create_run(w.validate_definition(definition or graph()), 'Original exact request', root,
                             freedom='read_only', interaction_owner='caller')
    registry = RejectRegistry(storage.root)
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = run['workflow_run_id']
    assigned_task_ids = list(assigned_task_ids or [])
    if assigned_task_ids:
        supervisor.update(lambda run: run['tasks'].extend(
            dict(id=task_id, title=task_id, description='Fixture checklist task', status='pending')
            for task_id in assigned_task_ids), 'fixture_checklist')
    # Accept the assignment through the real delegation validator and transaction.
    token = dict(id='worker-token', node_id='work', stack=[], branch_ids={}, input_result_refs=[],
                 assigned_task_ids=[], requires_assignment=True, decision_id='original-decision',
                 decision_attempts=1)
    storage.update_run(supervisor.run_id,
                       lambda r: r.update(status='running', pending=[copy.deepcopy(token)],
                                          supervisor_identity={'pid': 12345, 'start_time': 'fixture-supervisor', 'markers': []}),
                       'fixture_worker_checkpoint')
    node = next(n for n in supervisor.run()['definition']['nodes'] if n['id'] == 'work')
    decision_activation = supervisor._activation('work', 'orchestrator', copy.deepcopy(token))
    def bind_decision(run):
        activation = next(a for a in run['activations'] if a['id'] == decision_activation['id'])
        activation.update(decision_id=token['decision_id'], decision_goal_revision=run.get('goal_revision'))
        run['pending'][0]['decision_activation_id'] = activation['id']
    storage.update_run(supervisor.run_id, bind_decision, 'fixture_decision_binding')
    current = supervisor.run()
    decision_activation = next(a for a in current['activations'] if a['id'] == decision_activation['id'])
    token = current['pending'][0]
    decision = dict(decision_id=token['decision_id'], action='continue', reason='Explicit bounded assignment',
                    next=[dict(continuation_id='execute:' + token['id'], prompt='Immutable accepted assignment', session_mode='fresh', assigned_task_ids=assigned_task_ids)])
    from harflow_mcp import workflow_delegation as delegation
    supervisor.update(lambda run: delegation.accept_decision_locked(
        supervisor, run, node, token, decision_activation, decision, True, json.dumps(decision), []),
        'decision_accepted')
    token = supervisor.run()['pending'][0]
    return storage, registry, supervisor, node, token


def result(summary='Observed normal result'):
    return {'status': 'succeeded', 'result': {'summary': summary}, 'evidence': ['independent synthetic observation']}


def outcome(summary='Observed normal result'):
    return {'status': 'completed', 'summary': json.dumps(result(summary)), 'exit_code': 0, 'output_drained': True}


def control_documents(run, *, inventory=None):
    """Candidate envelopes bind actual launch identity; expected proofs stay independent."""
    import uuid
    from poststart_control_fixtures import documents, digest
    contain, certificate, replacement = documents()
    ledger = run['poststart']['ledger']
    for document in (contain, certificate, replacement):
        document['identity'] = copy.deepcopy(ledger['identity'])
        document['ledger_epoch'] = ledger['enrollment']['epoch']
    contain['request_id'] = str(uuid.uuid4())
    contain['profile'] = copy.deepcopy(ledger['profile'])
    certificate['contain_request_id'] = replacement['contain_request_id'] = contain['request_id']
    certificate['effect_verifier'] = copy.deepcopy(ledger['effect_verifier'])
    certificate['inventory'] = copy.deepcopy(inventory if inventory is not None else [dict(actor='original', generation='generation-1', obligation='required', settlement='satisfied')])
    replacement['request_id'] = str(uuid.uuid4())
    replacement['effect_verifier'] = copy.deepcopy(ledger['effect_verifier'])
    replacement['certificate_digest'] = digest(certificate)
    replacement['expected_remaining_attempts'] = ledger['remaining_attempts']
    return contain, certificate, replacement


def coordinator(storage, supervisor, scope):
    from harflow_mcp.poststart_store import Enrollment
    from harflow_mcp.workflow_poststart import WorkflowPoststartStore, WorkflowPoststartCoordinator
    handle = Enrollment(**supervisor.run()['poststart']['enrollment'])
    backend = WorkflowPoststartStore(storage, supervisor.run_id, scope).reopen(handle)
    return WorkflowPoststartCoordinator(backend, handle, scope.adapter)


def active_sibling(storage, supervisor, *, assigned_task_ids=None):
    """A separate actual branch with an accepted assignment and synthetic task row."""
    from harflow_mcp import workflow_delegation as delegation
    node = next(n for n in supervisor.run()['definition']['nodes'] if n['id'] == 'side')
    token = dict(id='side-token', node_id='side', stack=[], branch_ids={}, input_result_refs=[],
                 assigned_task_ids=[], requires_assignment=True, decision_id='side-decision', decision_attempts=1)
    supervisor.update(lambda run: run['pending'].append(copy.deepcopy(token)), 'fixture_side_checkpoint')
    decision_activation = supervisor._activation('side', 'orchestrator', copy.deepcopy(token))
    def bind(run):
        activation = next(a for a in run['activations'] if a['id'] == decision_activation['id'])
        activation.update(decision_id=token['decision_id'], decision_goal_revision=run.get('goal_revision'))
        next(t for t in run['pending'] if t['id'] == token['id'])['decision_activation_id'] = activation['id']
    supervisor.update(bind, 'fixture_side_decision_binding')
    decision_activation = next(a for a in supervisor.run()['activations'] if a['id'] == decision_activation['id'])
    token = next(t for t in supervisor.run()['pending'] if t['id'] == token['id'])
    decision = dict(decision_id='side-decision', action='continue', reason='Independent branch assignment',
                    next=[dict(continuation_id='execute:' + token['id'], prompt='Independent branch work', session_mode='fresh', assigned_task_ids=list(assigned_task_ids or []))])
    supervisor.update(lambda run: delegation.accept_decision_locked(
        supervisor, run, node, token, decision_activation, decision, True, json.dumps(decision), []), 'decision_accepted')
    token = next(t for t in supervisor.run()['pending'] if t['id'] == token['id'])
    worker = supervisor._activation('side', 'node', copy.deepcopy(token))
    def reserve(run):
        activation = next(a for a in run['activations'] if a['id'] == worker['id'])
        activation.update(assignment_prompt=token['assignment_prompt'], assigned_task_ids=token.get('assigned_task_ids', []),
                          execution_session_mode='fresh', visit_id=token['id'], attempt_in_visit=1,
                          execution_candidate=copy.deepcopy(node['agent']))
        activation['tasks'].append(dict(task_id='independent-side-task', status='running', candidate=copy.deepcopy(node['agent'])))
        next(t for t in run['pending'] if t['id'] == token['id'])['execution_activation_id'] = activation['id']
    supervisor.update(reserve, 'fixture_side_task')
    return node, token, worker


def admit_worker_turn(storage, supervisor, principal, activation_id, token_id, *, event='fixture_ordinary_worker_steering'):
    """Use actual steering admission/CAS; this intentionally never starts its provider."""
    import uuid
    from harflow_mcp import steering_admission, worker_steering, workflow_steering
    run = supervisor.run()
    witness = worker_steering.current_target(storage, run,
        dict(workflow_run_id=supervisor.run_id, activation_id=activation_id, token_id=token_id))
    request = dict(schema_version=2, ledger_epoch=run['steering']['ledger_epoch'],
                   request_id=str(uuid.uuid4()), action='open',
                   expected_interaction_epoch=run['interaction_epoch'],
                   expected_control_revision=run['steering']['control_revision'],
                   expected_steering_revision=None, episode_id=None, target=witness,
                   text='Explicit finite ordinary worker conversation', allow_fresh_context=True)
    accepted = []
    def admit(current):
        accepted.append(steering_admission.admit(storage, current, principal, False, request,
            workflow_steering._digest(request), workflow_steering._digest(principal)))
    supervisor.update(admit, event)
    return accepted[0]
