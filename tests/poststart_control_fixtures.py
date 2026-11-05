"""Independent, unpaid synthetic domain for durable poststart conformance."""
from __future__ import annotations

import copy
import hashlib
import json
import threading

# Transcribed from the normative policy, not imported from producer validators.
PROOFS = ('qualified_profile', 'original_enrollment', 'complete_closure',
          'future_issuance_fence', 'exit_reaped', 'stdout_eof', 'stderr_eof',
          'input_reconciled', 'lossless_capture', 'final_watermark', 'writers_quiescent',
          'extents_reconciled', 'handles_settled', 'no_pending_mutation', 'claims_settled',
          'resources_reconciled', 'sessions_reconciled', 'messages_reconciled',
          'release_journal_complete', 'job_quiescent', 'stale_callback_fence',
          'all_obligations_settled')


def digest(document):
    return hashlib.sha256((json.dumps(document, sort_keys=True, indent=2,
                                     ensure_ascii=False, allow_nan=False) + '\n').encode()).hexdigest()


def documents():
    header = dict(format='harflow.steering.poststart.candidate', v=1,
                  status='specified_unimplemented', provenance='synthetic_specification',
                  python_runtime_support=False, rust_adoption=False,
                  runtime_qualified=False, hypothetical=True)
    inventory = [dict(actor='original', generation='generation-1',
                      obligation='required', settlement='satisfied')]
    closure = hashlib.sha256(json.dumps(inventory, sort_keys=True, separators=(',', ':'),
                                       ensure_ascii=False).encode()).hexdigest()
    identity = {key: 'source-' + key for key in (
        'root', 'activation', 'task', 'attempt', 'visit', 'node', 'candidate',
        'owner_generation', 'containment_generation', 'process_generation')}
    identity.update(launch_id='10000000-0000-4000-8000-000000000001', goal_revision=0,
                    interaction_revision=0, control_revision=0, target_revision=0)
    identity.update({key: 'a' * 64 for key in ('definition_digest', 'goal_digest',
                    'assignment_digest', 'configuration_digest', 'grant_digest', 'profile_digest')})
    identity['assignment_digest'] = digest({'instructions': 'Immutable assignment'})
    identity.update(verifier_digest='b' * 64, closure_digest=closure)
    verifier = dict(name='synthetic-enforced-absence', version='1', digest='b' * 64)
    contain = dict(header, kind='contain_request', request_id='10000000-0000-4000-8000-000000000002',
                   ledger_epoch=0, identity=copy.deepcopy(identity),
                   scope='original_launch_complete_owned_closure', reason='Keep supplied prose.  Exactly.',
                   profile=dict(name='synthetic-only', version='1', digest='a' * 64,
                                expected_time_budgets=dict(stop=1, grace=2, execution=3, background=4)))
    certificate = dict(header, kind='certificate', contain_request_id=contain['request_id'],
                       ledger_epoch=0, identity=copy.deepcopy(identity),
                       proofs={key: True for key in PROOFS}, inventory=inventory,
                       effect_verifier=copy.deepcopy(verifier))
    replacement = dict(header, kind='replacement_request',
                       request_id='10000000-0000-4000-8000-000000000003',
                       contain_request_id=contain['request_id'], ledger_epoch=0,
                       identity=copy.deepcopy(identity), certificate_digest=digest(certificate),
                       effect_verifier=verifier, expected_remaining_attempts=2)
    return contain, certificate, replacement


def trusted_context():
    return dict.fromkeys(('authority', 'cancel_authority', 'enrolled', 'single_provider',
                         'scope_exact', 'qualified_profile', 'qualified_budgets', 'effect_safe',
                         'budget', 'configuration', 'claims'), True) | {'generation': 1}


class SyntheticEffects:
    """Independent effect ledger: no processes, registry, signals or callbacks."""
    synthetic = True
    adapter_id = 'independent-tests-v1'

    def __init__(self):
        self._lock = threading.Lock()
        self.generation = 1
        self.evidence_revision = 0
        self.effects = []
        self.before_effect = None
        self.lost_ack = False

    def invalidate(self, *, root, generation, evidence_revision):
        with self._lock:
            if generation < self.generation:
                raise RuntimeError('independent adapter rejected reversed generation')
            if generation == self.generation and evidence_revision < self.evidence_revision:
                raise RuntimeError('independent adapter rejected reversed evidence')
            self.generation = generation
            self.evidence_revision = evidence_revision

    def execute(self, kind, command_id, task_id, expected_generation, root, expected_evidence_revision):
        if self.before_effect is not None:
            self.before_effect()
        with self._lock:
            if self.generation != expected_generation:
                raise RuntimeError('independent adapter rejected stale generation')
            if self.evidence_revision != expected_evidence_revision:
                raise RuntimeError('independent adapter rejected stale evidence')
            if any(item['command_id'] == command_id for item in self.effects):
                raise AssertionError('producer attempted duplicate effect')
            receipt = dict(command_id=command_id, kind=kind, task_id=task_id,
                           generation=expected_generation, root=root, evidence_revision=expected_evidence_revision)
            self.effects.append(receipt)
            if self.lost_ack:
                raise RuntimeError('synthetic lost effect acknowledgement')
            return copy.deepcopy(receipt)
