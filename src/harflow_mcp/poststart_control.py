"""Detached synthetic producer. No runtime adapter is registered or qualified here."""
from __future__ import annotations
import copy
from dataclasses import dataclass
import uuid
from . import poststart_schema as schema
from .poststart_store import PoststartStore, validate_context


def poststart_control(*args, **kwargs):
    """Production facade: unconditional refusal before any state/effect access."""
    raise schema.PoststartError('poststart_unsupported')

@dataclass(frozen=True)
class Handoff:
    token: str
    command_id: str
    kind: str
    generation: int
    evidence_revision: int

class SyntheticCoordinator:
    def __init__(self, store, handle, adapter):
        schema.require(type(store) is PoststartStore and getattr(adapter, 'synthetic', None) is True and getattr(adapter, 'adapter_id', None) == handle.adapter_id, 'synthetic_adapter_required')
        self.store = store.reopen(handle)
        self.handle = handle
        self.adapter = adapter
        self._handoffs = {}

    def snapshot(self):
        value = self.store.snapshot()
        value['candidate_only'] = True
        value['runtime_authorized'] = False
        return value

    def _binding(self, state, request):
        schema.require(request['identity'] == state['identity'] and request['ledger_epoch'] == self.handle.epoch, 'source_binding')

    def _checks(self, state, replacement=False):
        keys = ['authority', 'enrolled', 'single_provider', 'scope_exact', 'qualified_profile', 'qualified_budgets', 'configuration', 'claims', 'budget']
        keys += ['effect_safe'] if replacement else ['cancel_authority']
        schema.require(all(state['context'][k] is True for k in keys), 'context_refused')

    def _replay(self, state, request):
        old = state['receipts'].get(request['request_id'])
        if old is not None:
            schema.require(old['request'] == request, 'idempotency_conflict')
            return old['receipt']
        return None

    def contain(self, request):
        request = schema.validate_document(request, 'contain_request')
        def apply(state):
            replay = self._replay(state, request)
            if replay is not None:
                return replay
            self._binding(state, request)
            self._checks(state)
            schema.require(state['result'] is None, 'result_accepted')
            schema.require(state['intent'] is None, 'intent_unresolved')
            schema.require(request['profile'] == state['profile'], 'profile_binding')
            schema.require(len(schema.canonical(state)) + len(schema.canonical(request)) * 3 + self.store.RESERVE <= self.store.metadata_limit, 'metadata_capacity')
            schema.require(self.store.text_size(state) + self.store.text_size(request) * 3 + self.store.RESERVE <= 1048576, 'text_capacity')
            state.update(intent=request, fence=True, future_issuance_fence=True, phase='cancellation_requested', reservation=self.store.RESERVE)
            if state['identity']['task'] not in state['retired_tasks']:
                state['retired_tasks'].append(state['identity']['task'])
            receipt = dict(request_id=request['request_id'], accepted=True, phase='cancellation_requested', runtime_authorized=False)
            state['receipts'][request['request_id']] = dict(request=request, receipt=receipt)
            return receipt
        return self.store.transact(apply)

    def _prepare(self, kind):
        token = str(uuid.uuid4())
        def apply(state):
            schema.require(state['intent'] is not None, 'no_intent')
            self._checks(state, replacement=kind == 'dispatch')
            if kind == 'dispatch':
                self._ready(state)
                schema.require(state['replacement'] is not None, 'no_replacement')
            schema.require(kind not in state['commands'], 'delivery_unknown')
            command_id = schema.control_identifier(state['enrollment'], state['intent'], kind)
            task_id = state['identity']['task'] if kind == 'stop' else state['replacement']['task_id']
            command = dict(command_id=command_id, kind=kind, task_id=task_id, root=self.handle.root, generation=state['context']['generation'], evidence_revision=state['evidence_revision'], status='prepared', receipt=None)
            state['commands'][kind] = command
            return Handoff(token, command_id, kind, command['generation'], command['evidence_revision'])
        # A write exception, including uncertain post-rename commit, never mints a handoff.
        handoff = self.store.transact(apply)
        self._handoffs[token] = handoff
        return handoff

    def prepare_stop(self):
        return self._prepare('stop')

    def prepare_dispatch(self):
        return self._prepare('dispatch')

    def deliver(self, handoff):
        schema.require(type(handoff) is Handoff and self._handoffs.pop(handoff.token, None) == handoff, 'handoff_unavailable')
        def claim(state):
            command = state['commands'].get(handoff.kind)
            schema.require(command is not None and command['command_id'] == handoff.command_id and command['status'] == 'prepared', 'command_binding')
            schema.require(state['context']['generation'] == handoff.generation and state['evidence_revision'] == handoff.evidence_revision, 'stale_handoff')
            self._checks(state, replacement=handoff.kind == 'dispatch')
            if handoff.kind == 'dispatch':
                self._ready(state)
            command['status'] = 'unknown'
            return command
        command = self.store.transact(claim)
        # No store/tree lock survives the durable claim. Adapter must atomically check
        # expected_generation at its own effect boundary, never by a prior snapshot.
        return self.adapter.execute(kind=command['kind'], command_id=command['command_id'], task_id=command['task_id'], root=command['root'], expected_generation=command['generation'], expected_evidence_revision=command['evidence_revision'])

    def acknowledge(self, command_id, receipt):
        def apply(state):
            matches = [c for c in state['commands'].values() if c['command_id'] == command_id]
            schema.require(len(matches) == 1, 'command_binding')
            command = matches[0]
            expected = {k: command[k] for k in ('command_id', 'kind', 'task_id', 'generation', 'root', 'evidence_revision')}
            schema.closed(receipt, expected)
            schema.counter(receipt['generation'])
            schema.counter(receipt['evidence_revision'])
            schema.require(receipt == expected, 'receipt_binding')
            schema.require(command['status'] in ('unknown', 'acknowledged'), 'effect_not_claimed')
            if command['status'] == 'acknowledged':
                schema.require(command['receipt'] == receipt, 'receipt_conflict')
            command.update(status='acknowledged', receipt=copy.deepcopy(receipt))
            return receipt
        return self.store.transact(apply)

    def _invalidate(self, snapshot, *, generation, evidence_revision):
        # Atomic adapter fence precedes ledger CAS, outside every store lock. A
        # failed CAS may leave the adapter ahead; that conservatively blocks effects.
        self._handoffs.clear()
        self.adapter.invalidate(root=self.handle.root, generation=generation, evidence_revision=evidence_revision)

    def observe(self, certificate):
        certificate = schema.validate_document(certificate, 'certificate')
        snapshot = self.store.snapshot()
        self._binding(snapshot, certificate)
        schema.require('stop' in snapshot['commands'], 'no_stop_delivery')
        schema.require(snapshot['intent'] is not None and certificate['contain_request_id'] == snapshot['intent']['request_id'] and certificate['effect_verifier'] == snapshot['effect_verifier'], 'evidence_binding')
        self._invalidate(snapshot, generation=snapshot['context']['generation'], evidence_revision=snapshot['evidence_revision'] + 1)
        def apply(state):
            self._binding(state, certificate)
            schema.require('stop' in state['commands'], 'no_stop_delivery')
            schema.require(state['intent'] is not None and certificate['contain_request_id'] == state['intent']['request_id'], 'intent_binding')
            schema.require(certificate['effect_verifier'] == state['effect_verifier'], 'verifier_binding')
            state['evidence_revision'] += 1
            state.update(evidence=certificate, certificate=None, release=None, phase='settlement_unknown')
            return state['evidence_revision']
        result = self.store.transact(apply, expected_version=snapshot['version'])
        self._handoffs.clear()
        return result

    def certify(self):
        def apply(state):
            schema.require(state['evidence'] is not None and schema.evidence_complete(state['evidence']), 'settlement_unknown')
            schema.require('stop' in state['commands'], 'no_stop_delivery')
            self._checks(state, replacement=True)
            value = schema.document_digest(state['evidence'])
            state.update(certificate=dict(digest=value, revision=state['evidence_revision'], generation=state['context']['generation']), phase='ready_to_replan')
            return value
        return self.store.transact(apply)

    def _ready(self, state):
        certificate = state['certificate']
        schema.require(certificate is not None and certificate['revision'] == state['evidence_revision'] and certificate['generation'] == state['context']['generation'] and state['evidence'] is not None and schema.evidence_complete(state['evidence']), 'stale_certificate')
        schema.require(state['release'] is not None and state['release']['revision'] == state['evidence_revision'] and state['release']['generation'] == state['context']['generation'], 'claims_unreleased')

    def release(self):
        def apply(state):
            schema.require(state['certificate'] is not None and state['certificate']['revision'] == state['evidence_revision'] and state['certificate']['generation'] == state['context']['generation'], 'stale_certificate')
            self._checks(state, replacement=True)
            receipt = dict(revision=state['evidence_revision'], generation=state['context']['generation'], old_claims_settled=True, replacement_exclusion=True)
            state['release'] = receipt
            return receipt
        return self.store.transact(apply)

    def replace(self, request):
        request = schema.validate_document(request, 'replacement_request')
        def apply(state):
            replay = self._replay(state, request)
            if replay is not None:
                return replay
            self._binding(state, request)
            self._checks(state, replacement=True)
            self._ready(state)
            schema.require(state['replacement'] is None and request['contain_request_id'] == state['intent']['request_id'], 'replacement_conflict')
            schema.require(request['certificate_digest'] == state['certificate']['digest'] and request['effect_verifier'] == state['effect_verifier'], 'certificate_binding')
            schema.require(request['expected_remaining_attempts'] == state['remaining_attempts'] and state['remaining_attempts'] > 0, 'attempt_budget')
            lineage = {key: schema.control_identifier(state['enrollment'], request, key) for key in ('decision_id', 'attempt_id', 'task_id', 'launch_id', 'activation_id')}
            lineage.update(supersedes=state['identity']['task'], request_id=request['request_id'], accepted=True, runtime_authorized=False)
            state['replacement_proofs'][request['request_id']] = dict(request_digest=schema.document_digest(request), contain_intent=copy.deepcopy(state['intent']), evidence=copy.deepcopy(state['evidence']), certificate=copy.deepcopy(state['certificate']), release=copy.deepcopy(state['release']), context=copy.deepcopy(state['context']), remaining_before=state['remaining_attempts'], ordinal=state['control_issuances'] + 1)
            state['remaining_attempts'] -= 1
            state['control_issuances'] += 1
            state.update(replacement=lineage, phase='replacement_committed')
            state['receipts'][request['request_id']] = dict(request=request, receipt=lineage)
            return lineage
        return self.store.transact(apply)

    def update_context(self, context):
        validate_context(context)
        snapshot = self.store.snapshot()
        schema.require(context['generation'] > snapshot['context']['generation'], 'stale_context')
        self._invalidate(snapshot, generation=context['generation'], evidence_revision=snapshot['evidence_revision'])
        def apply(state):
            schema.require(context['generation'] > state['context']['generation'], 'stale_context')
            state.update(context=copy.deepcopy(context), certificate=None, release=None)
            return context
        result = self.store.transact(apply, expected_version=snapshot['version'], preserve_reserve=True)
        self._handoffs.clear()
        return result

    def accept_result(self, value, *, identity):
        schema.validate_identity(identity)
        identity = copy.deepcopy(identity)
        schema.require(value is not None, 'result_required')
        schema.bounded(value)
        def apply(state):
            schema.require(identity == state['identity'], 'result_source_binding')
            schema.require(not state['fence'], 'result_fenced')
            schema.require(state['result'] is None or state['result'] == value, 'result_conflict')
            state['result'] = copy.deepcopy(value)
            return value
        return self.store.transact(apply)

    def sibling_result(self, identifier, value):
        schema.bounded(identifier)
        schema.bounded(value)
        def apply(state):
            schema.require(type(identifier) is str and (identifier not in state['siblings'] or state['siblings'][identifier] == value), 'sibling_conflict')
            state['siblings'][identifier] = copy.deepcopy(value)
            return value
        return self.store.transact(apply, preserve_reserve=True)

    def advance_source(self, identity, context):
        """Continue only the exact acknowledged successor; never mint allowance."""
        schema.validate_identity(identity)
        validate_context(context)
        snapshot = self.store.snapshot()
        schema.require(context['generation'] > snapshot['context']['generation'], 'stale_context')
        def apply(state):
            replacement = state['replacement']
            schema.require(replacement is not None and state['commands'].get('dispatch', {}).get('status') == 'acknowledged', 'successor_unknown')
            self._ready(state)
            expected = copy.deepcopy(state['identity'])
            for field, key in (('task', 'task_id'), ('attempt', 'attempt_id'), ('launch_id', 'launch_id'), ('activation', 'activation_id')):
                expected[field] = replacement[key]
            # New enrollment generations are witnesses from the adapter, while all
            # assignment/goal/grant/configuration identity remains fixed.
            for field in ('owner_generation', 'containment_generation', 'process_generation'):
                expected[field] = identity[field]
            schema.require(identity == expected and context['generation'] > state['context']['generation'] and context['enrolled'] is True, 'successor_binding')
            archived = copy.deepcopy(state)
            archived['history'] = []
            state['history'].append(archived)
            state.update(identity=copy.deepcopy(identity), context=copy.deepcopy(context), intent=None, evidence=None, certificate=None, release=None, replacement=None, commands={}, evidence_revision=0, fence=False, future_issuance_fence=False, phase='running', reservation=0, result=None)
            return identity
        apply(copy.deepcopy(snapshot))
        self._invalidate(snapshot, generation=context['generation'], evidence_revision=0)
        result = self.store.transact(apply, expected_version=snapshot['version'])
        self._handoffs.clear()
        return result
