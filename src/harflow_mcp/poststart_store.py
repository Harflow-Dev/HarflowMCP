"""Owned, durable, synthetic-only poststart ledger transactions."""
from __future__ import annotations
import copy
from dataclasses import dataclass, asdict
import hashlib
import json
import os
import stat
import uuid
from . import poststart_schema as schema
from .workflows import _write
from .state_ownership import state_session

CONTEXT_KEYS = set('authority cancel_authority enrolled single_provider scope_exact qualified_profile qualified_budgets effect_safe budget configuration claims generation'.split())

def validate_context(context):
    schema.closed(context, CONTEXT_KEYS)
    schema.counter(context['generation'])
    schema.require(all(type(v) is bool for k, v in context.items() if k != 'generation'), 'context_boolean')

@dataclass(frozen=True)
class Enrollment:
    directory: str
    root: str
    epoch: int
    mode: str
    adapter_id: str
    nonce: str
    initial_attempt_allowance: int
    immutable_digest: str
    initial_source_digest: str

class PoststartStore:
    RESERVE = 524288
    def __init__(self, workflow_store, *, before_replace=None, after_replace=None, metadata_limit=4194304):
        self.workflow_store = workflow_store
        self.directory = workflow_store.root / 'poststart-control'
        self.before_replace = before_replace
        self.after_replace = after_replace
        schema.require(type(metadata_limit) is int and self.RESERVE < metadata_limit <= 4194304, 'metadata_limit')
        self.metadata_limit = metadata_limit
        self.handle = None

    def _paths(self, root):
        schema.require(not self.directory.is_symlink(), 'unsafe_directory')
        name = hashlib.sha256(root.encode()).hexdigest()
        return self.directory / (name + '.anchor.json'), self.directory / (name + '.json')

    def _read(self, path):
        # One operation reads at most a 128 KiB anchor plus 4 MiB ledger.
        limit = 131072 if path.name.endswith('.anchor.json') else 4194304
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(descriptor, 'rb') as stream:
                st = os.fstat(stream.fileno())
                schema.require(stat.S_ISREG(st.st_mode) and st.st_nlink == 1 and st.st_size <= limit, 'unsafe_record')
                raw = stream.read(limit + 1)
            schema.require(len(raw) <= limit, 'read_budget')
            def pairs(items):
                result = {}
                for key, item in items:
                    schema.require(key not in result, 'duplicate_key')
                    result[key] = item
                return result
            return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=lambda _: schema.require(False, 'number'))
        except (OSError, UnicodeError, ValueError, RecursionError) as exc:
            raise schema.PoststartError('record_unavailable') from exc

    def _write(self, path, value):
        _write(path, value, layout=self.workflow_store.state_layout, before_replace=self.before_replace, after_replace=self.after_replace)

    def initialize(self, identity, *, adapter_id, profile, effect_verifier, assignment, remaining_attempts, context):
        schema.validate_identity(identity)
        schema.validate_profile(profile)
        schema.validate_verifier(effect_verifier)
        schema.require(profile['digest'] == identity['profile_digest'] and effect_verifier['digest'] == identity['verifier_digest'], 'enrollment_binding')
        schema.counter(remaining_attempts)
        validate_context(context)
        schema.bounded(assignment)
        schema.require(schema.document_digest(assignment) == identity['assignment_digest'], 'assignment_binding')
        schema.require(type(adapter_id) is str and bool(adapter_id), 'adapter_identity')
        handle = Enrollment(str(self.workflow_store.root.resolve()), identity['root'], 0, 'synthetic', adapter_id, str(uuid.uuid4()), remaining_attempts, schema.document_digest(dict(identity={k: v for k, v in identity.items() if k not in {'task', 'attempt', 'launch_id', 'activation', 'owner_generation', 'containment_generation', 'process_generation'}}, profile=profile, effect_verifier=effect_verifier, assignment=assignment)), schema.document_digest(identity))
        anchor, path = self._paths(identity['root'])
        with state_session(self.workflow_store.state_layout), self.workflow_store.lock('poststart:' + identity['root']):
            schema.require(not self.directory.is_symlink(), 'unsafe_directory')
            self.directory.mkdir(mode=0o700, exist_ok=True)
            schema.require(not anchor.exists() and not path.exists(), 'already_enrolled')
            # Exclusive anchor first: interrupted initialization must never reset authority.
            with anchor.open('x', encoding='utf-8') as stream:
                json.dump(asdict(handle), stream)
                stream.flush()
                os.fsync(stream.fileno())
            fd = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            state = dict(format='harflow.poststart.synthetic', v=1, enrollment=asdict(handle), version=0, identity=copy.deepcopy(identity), profile=copy.deepcopy(profile), effect_verifier=copy.deepcopy(effect_verifier), assignment=copy.deepcopy(assignment), context=copy.deepcopy(context), remaining_attempts=remaining_attempts, original_charged=True, original_usage=None, model_decisions=0, control_issuances=0, result=None, siblings={}, accepted_decision='preserved', intent=None, history=[], retired_tasks=[], fence=False, future_issuance_fence=False, evidence_revision=0, evidence=None, certificate=None, release=None, replacement=None, commands={}, receipts={}, replacement_proofs={}, phase='running', reservation=0)
            self._validate(state, handle)
            self._write(path, state)
        self.handle = handle
        return handle

    def reopen(self, handle):
        schema.require(type(handle) is Enrollment and handle.mode == 'synthetic' and handle.directory == str(self.workflow_store.root.resolve()), 'enrollment_mismatch')
        with state_session(self.workflow_store.state_layout), self.workflow_store.lock('poststart:' + handle.root):
            anchor, path = self._paths(handle.root)
            schema.require(self._read(anchor) == asdict(handle), 'anchor_mismatch')
            self._validate(self._read(path), handle)
        self.handle = handle
        return self

    def _validate(self, state, handle, identities=None, *, check_chain=True):
        try:
            self._validate_inner(state, handle, identities, check_chain)
        except (TypeError, KeyError, AttributeError, OverflowError, RecursionError, UnicodeError) as exc:
            raise schema.PoststartError('malformed_ledger') from exc

    def _validate_inner(self, state, handle, identities, check_chain):
        keys = set('format v enrollment version identity profile effect_verifier assignment context remaining_attempts original_charged original_usage model_decisions control_issuances result siblings accepted_decision intent history retired_tasks fence future_issuance_fence evidence_revision evidence certificate release replacement commands receipts replacement_proofs phase reservation'.split())
        schema.closed(state, keys)
        schema.require(state['format'] == 'harflow.poststart.synthetic' and type(state['v']) is int and state['v'] == 1 and state['enrollment'] == asdict(handle), 'ledger_binding')
        schema.validate_identity(state['identity'])
        schema.require(state['identity']['root'] == handle.root, 'root_binding')
        schema.validate_profile(state['profile'])
        schema.validate_verifier(state['effect_verifier'])
        validate_context(state['context'])
        for key in ('version', 'remaining_attempts', 'model_decisions', 'control_issuances', 'evidence_revision', 'reservation'):
            schema.counter(state[key])
        schema.require(state['original_charged'] is True and state['model_decisions'] == 0, 'accounting')
        replacement_count = sum(entry.get('request', {}).get('kind') == 'replacement_request' for entry in state['receipts'].values())
        schema.require(state['control_issuances'] == replacement_count and state['remaining_attempts'] == handle.initial_attempt_allowance - replacement_count, 'cumulative_accounting')
        immutable = dict(identity={k: v for k, v in state['identity'].items() if k not in {'task', 'attempt', 'launch_id', 'activation', 'owner_generation', 'containment_generation', 'process_generation'}}, profile=state['profile'], effect_verifier=state['effect_verifier'], assignment=state['assignment'])
        schema.require(schema.document_digest(immutable) == handle.immutable_digest, 'immutable_enrollment')
        schema.require(schema.document_digest(state['assignment']) == state['identity']['assignment_digest'], 'assignment_binding')
        schema.require(type(state['history']) is list and type(state['retired_tasks']) is list and type(state['commands']) is dict and type(state['receipts']) is dict and type(state['siblings']) is dict, 'ledger_collection')
        schema.require(type(state['fence']) is bool and type(state['future_issuance_fence']) is bool, 'fence')
        schema.require(state['phase'] in {'running', 'cancellation_requested', 'settlement_unknown', 'ready_to_replan', 'replacement_committed', 'completed'}, 'phase')
        if state['intent'] is not None:
            schema.validate_document(json.dumps(state['intent'], ensure_ascii=False, separators=(',', ':'), allow_nan=False), 'contain_request')
        if state['evidence'] is not None:
            schema.validate_document(json.dumps(state['evidence'], ensure_ascii=False, separators=(',', ':'), allow_nan=False), 'certificate')
        schema.require(state['original_usage'] is None, 'unknown_usage_preserved')
        schema.require(state['accepted_decision'] == 'preserved', 'accepted_decision')
        schema.require(state['profile']['digest'] == state['identity']['profile_digest'] and state['effect_verifier']['digest'] == state['identity']['verifier_digest'], 'profile_binding')
        schema.require(len(set(state['retired_tasks'])) == len(state['retired_tasks']) and all(type(t) is str for t in state['retired_tasks']), 'retirement')
        if state['intent'] is not None:
            schema.require(state['fence'] and state['future_issuance_fence'] and state['identity']['task'] in state['retired_tasks'] and state['reservation'] == self.RESERVE, 'intent_fences')
            schema.require(state['intent']['identity'] == state['identity'] and state['intent']['ledger_epoch'] == handle.epoch and state['intent']['profile'] == state['profile'], 'intent_binding')
        else:
            schema.require(not state['fence'] and not state['future_issuance_fence'] and state['reservation'] == 0 and not state['commands'] and state['replacement'] is None, 'idle_state')
        if state['evidence'] is not None:
            schema.require(state['intent'] is not None and state['evidence']['identity'] == state['identity'] and state['evidence']['ledger_epoch'] == handle.epoch and state['evidence']['contain_request_id'] == state['intent']['request_id'] and state['evidence']['effect_verifier'] == state['effect_verifier'], 'evidence_binding')
        if state['certificate'] is not None:
            schema.closed(state['certificate'], {'digest', 'revision', 'generation'})
            schema.counter(state['certificate']['revision'])
            schema.counter(state['certificate']['generation'])
            schema.digest(state['certificate']['digest'])
            schema.require(state['evidence'] is not None and schema.evidence_complete(state['evidence']) and state['certificate'] == dict(digest=schema.document_digest(state['evidence']), revision=state['evidence_revision'], generation=state['context']['generation']), 'certificate_binding')
        if state['release'] is not None:
            schema.closed(state['release'], {'revision', 'generation', 'old_claims_settled', 'replacement_exclusion'})
            schema.counter(state['release']['revision'])
            schema.counter(state['release']['generation'])
            schema.require(state['release']['old_claims_settled'] is True and state['release']['replacement_exclusion'] is True, 'release_boolean')
            schema.require(state['certificate'] is not None and state['release'] == dict(revision=state['evidence_revision'], generation=state['context']['generation'], old_claims_settled=True, replacement_exclusion=True), 'release_binding')
        if identities is None:
            identities = [state['identity']] + [item['identity'] for item in state['history']]
        for request_id, entry in state['receipts'].items():
            schema.identifier(request_id)
            schema.closed(entry, {'request', 'receipt'})
            request = schema.validate_document(json.dumps(entry['request'], ensure_ascii=False, separators=(',', ':'), allow_nan=False))
            schema.require(request['kind'] in ('contain_request', 'replacement_request') and request['request_id'] == request_id and request['ledger_epoch'] == handle.epoch and request['identity']['root'] == handle.root and request['identity'] in identities, 'receipt_request')
            schema.require(type(entry['receipt']) is dict, 'receipt')
            if request['kind'] == 'contain_request':
                schema.require(entry['receipt'] == dict(request_id=request_id, accepted=True, phase='cancellation_requested', runtime_authorized=False) and entry['receipt']['accepted'] is True and entry['receipt']['runtime_authorized'] is False, 'contain_receipt')
            else:
                expected = {field: schema.control_identifier(state['enrollment'], request, field) for field in ('decision_id', 'attempt_id', 'task_id', 'launch_id', 'activation_id')}
                expected.update(supersedes=request['identity']['task'], request_id=request_id, accepted=True, runtime_authorized=False)
                schema.require(entry['receipt'] == expected and entry['receipt']['accepted'] is True and entry['receipt']['runtime_authorized'] is False, 'replacement_receipt')
        schema.require(type(state['replacement_proofs']) is dict and set(state['replacement_proofs']) == {key for key, entry in state['receipts'].items() if entry['request']['kind'] == 'replacement_request'}, 'acceptance_proofs')
        ordinals = []
        for request_id, proof in state['replacement_proofs'].items():
            schema.closed(proof, {'request_digest', 'contain_intent', 'evidence', 'certificate', 'release', 'context', 'remaining_before', 'ordinal'})
            request = state['receipts'][request_id]['request']
            schema.counter(proof['remaining_before'])
            schema.counter(proof['ordinal'])
            schema.require(proof['ordinal'] > 0 and proof['remaining_before'] == handle.initial_attempt_allowance - proof['ordinal'] + 1, 'acceptance_accounting')
            ordinals.append(proof['ordinal'])
            contain = schema.validate_document(json.dumps(proof['contain_intent'], ensure_ascii=False, separators=(',', ':'), allow_nan=False), 'contain_request')
            evidence = schema.validate_document(json.dumps(proof['evidence'], ensure_ascii=False, separators=(',', ':'), allow_nan=False), 'certificate')
            validate_context(proof['context'])
            schema.require(all(proof['context'][k] is True for k in ('authority', 'enrolled', 'single_provider', 'scope_exact', 'qualified_profile', 'qualified_budgets', 'configuration', 'claims', 'budget', 'effect_safe')), 'acceptance_context')
            schema.require(schema.evidence_complete(evidence) and contain['identity'] == evidence['identity'] == request['identity'] and contain['request_id'] == evidence['contain_request_id'] == request['contain_request_id'] and contain['ledger_epoch'] == evidence['ledger_epoch'] == request['ledger_epoch'], 'acceptance_source')
            schema.require(state['receipts'].get(contain['request_id'], {}).get('request') == contain, 'acceptance_intent')
            schema.closed(proof['certificate'], {'digest', 'revision', 'generation'})
            schema.counter(proof['certificate']['revision'])
            schema.counter(proof['certificate']['generation'])
            schema.require(proof['certificate']['digest'] == schema.document_digest(evidence) == request['certificate_digest'] and proof['certificate']['generation'] == proof['context']['generation'], 'acceptance_certificate')
            expected_release = dict(revision=proof['certificate']['revision'], generation=proof['context']['generation'], old_claims_settled=True, replacement_exclusion=True)
            schema.require(proof['release'] == expected_release and proof['release']['old_claims_settled'] is True and proof['release']['replacement_exclusion'] is True, 'acceptance_release')
            schema.counter(proof['release']['revision'])
            schema.counter(proof['release']['generation'])
            schema.require(request['expected_remaining_attempts'] == proof['remaining_before'] and request['effect_verifier'] == evidence['effect_verifier'] == state['effect_verifier'] and schema.document_digest(request) == proof['request_digest'], 'acceptance_request')
        schema.require(sorted(ordinals) == list(range(1, state['control_issuances'] + 1)), 'acceptance_ordinals')
        if state['replacement'] is not None:
            replacement = state['replacement']
            schema.closed(replacement, {'decision_id', 'attempt_id', 'task_id', 'launch_id', 'activation_id', 'supersedes', 'request_id', 'accepted', 'runtime_authorized'})
            schema.identifier(replacement['request_id'])
            schema.require(replacement['accepted'] is True and replacement['runtime_authorized'] is False and replacement['supersedes'] == state['identity']['task'], 'replacement_binding')
            for field in ('decision_id', 'attempt_id', 'task_id', 'launch_id', 'activation_id'):
                schema.require(replacement[field] == schema.control_identifier(state['enrollment'], state['receipts'][replacement['request_id']]['request'], field), 'lineage')
            schema.require(state['receipts'].get(replacement['request_id'], {}).get('receipt') == replacement, 'replacement_receipt')
        for kind, command in state['commands'].items():
            schema.require(kind in ('stop', 'dispatch') and state['intent'] is not None, 'command_kind')
            schema.closed(command, {'command_id', 'kind', 'task_id', 'root', 'generation', 'evidence_revision', 'status', 'receipt'})
            schema.require(command['kind'] == kind and command['root'] == handle.root and command['command_id'] == schema.control_identifier(state['enrollment'], state['intent'], kind), 'command_binding')
            schema.counter(command['generation'])
            schema.counter(command['evidence_revision'])
            schema.require(command['generation'] <= state['context']['generation'] and command['evidence_revision'] <= state['evidence_revision'], 'command_revision')
            task = state['identity']['task'] if kind == 'stop' else (state['replacement'] or {}).get('task_id')
            schema.require(command['task_id'] == task and command['status'] in ('prepared', 'unknown', 'acknowledged'), 'command_state')
            expected = {k: command[k] for k in ('command_id', 'kind', 'task_id', 'generation', 'root', 'evidence_revision')}
            if command['receipt'] is not None:
                schema.closed(command['receipt'], expected)
                schema.counter(command['receipt']['generation'])
                schema.counter(command['receipt']['evidence_revision'])
            schema.require(command['receipt'] == expected if command['status'] == 'acknowledged' else command['receipt'] is None, 'command_receipt')
        raw = schema.canonical(state)
        schema.require(len(raw) <= self.metadata_limit, 'metadata_capacity')
        schema.require(self.text_size(state) <= 1048576, 'text_capacity')
        if check_chain:
            chain = state['history'] + [state]
            schema.require(schema.document_digest(chain[0]['identity']) == handle.initial_source_digest, 'initial_source_binding')
            for predecessor, successor in zip(chain, chain[1:]):
                replacement = predecessor['replacement']
                schema.require(replacement is not None and predecessor['commands'].get('dispatch', {}).get('status') == 'acknowledged', 'history_successor_unknown')
                expected = copy.deepcopy(predecessor['identity'])
                for field, key in (('task', 'task_id'), ('attempt', 'attempt_id'), ('launch_id', 'launch_id'), ('activation', 'activation_id')):
                    expected[field] = replacement[key]
                for field in ('owner_generation', 'containment_generation', 'process_generation'):
                    expected[field] = successor['identity'][field]
                schema.require(successor['identity'] == expected and successor['context']['generation'] > predecessor['context']['generation'], 'history_lineage')
                schema.require(successor['control_issuances'] >= predecessor['control_issuances'] and successor['remaining_attempts'] <= predecessor['remaining_attempts'], 'history_accounting')
        for historical in state['history']:
            schema.require(type(historical) is dict and historical.get('history') == [], 'history_shape')
            self._validate(historical, handle, identities, check_chain=False)

    @staticmethod
    def text_size(value):
        if type(value) is str:
            return len(value.encode('utf-8'))
        if type(value) is dict:
            return sum(PoststartStore.text_size(k) + PoststartStore.text_size(v) for k, v in value.items())
        if type(value) is list:
            return sum(PoststartStore.text_size(item) for item in value)
        return 0

    def snapshot(self):
        schema.require(self.handle is not None, 'not_enrolled')
        with state_session(self.workflow_store.state_layout), self.workflow_store.lock('poststart:' + self.handle.root):
            anchor, path = self._paths(self.handle.root)
            schema.require(self._read(anchor) == asdict(self.handle), 'anchor_mismatch')
            state = self._read(path)
            self._validate(state, self.handle)
            return copy.deepcopy(state)

    def transact(self, operation, *, expected_version=None, preserve_reserve=False):
        schema.require(self.handle is not None, 'not_enrolled')
        with state_session(self.workflow_store.state_layout), self.workflow_store.lock('poststart:' + self.handle.root):
            anchor, path = self._paths(self.handle.root)
            schema.require(self._read(anchor) == asdict(self.handle), 'anchor_mismatch')
            state = self._read(path)
            self._validate(state, self.handle)
            schema.require(expected_version is None or state['version'] == expected_version, 'stale_version')
            previous = copy.deepcopy(state)
            result = operation(state)
            if state != previous:
                state['version'] += 1
                if preserve_reserve:
                    schema.require(len(schema.canonical(state)) + state['reservation'] <= self.metadata_limit and self.text_size(state) + state['reservation'] <= 1048576, 'completion_reserve')
                self._validate(state, self.handle)
                self._write(path, state)
            return copy.deepcopy(result)
