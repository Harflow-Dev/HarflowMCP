"""Standalone stdlib candidate tests; no product configuration or runtime imports."""
from __future__ import annotations
import copy
import hashlib
import itertools
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from poststart_profile import Rejected, classify, decode, fixtures, run_scenario
from poststart_reference_model import schema_accepts, ProtocolOracle

# Independently transcribed from the approved policy, never imported from validation.
PROOFS = ('qualified_profile', 'no_pending_mutation', 'sessions_reconciled', 'resources_reconciled', 'messages_reconciled', 'original_enrollment', 'complete_closure', 'future_issuance_fence', 'exit_reaped',
          'stdout_eof', 'stderr_eof', 'input_reconciled', 'lossless_capture', 'final_watermark',
          'writers_quiescent', 'extents_reconciled', 'handles_settled', 'claims_settled',
          'release_journal_complete', 'job_quiescent', 'stale_callback_fence', 'all_obligations_settled')
SCRIPT = ROOT / 'scripts/generate-poststart-profile-fixtures.py'


REJECT_CODES = {
    'unknown-obligation-settled.json':'settlement_unknown',
    'reject-float-version.json':'candidate_only',
    'reject-float-revision.json':'invalid_revision',
    'reject-utf8-string-budget.json':'invalid_string',
    'reject-duplicate-actor.json':'duplicate_actor',
    'reject-runtime-header.json':'candidate_only',
    'reject-caller-authority.json':'closed_shape',
    'reject-verifier-binding.json':'verifier_mismatch',
    'reject-profile-binding.json':'profile_mismatch',
    'reject-scope.json':'unsupported_scope',
    'reject-ordinary-frame.json':'closed_shape',
    'reject-reference-syntax.json':'invalid_request_id',
}
SCHEMA_ONLY_ACCEPTS = {
    'reject-float-version.json','reject-float-revision.json',
    'reject-utf8-string-budget.json','reject-duplicate-actor.json',
    'reject-verifier-binding.json','reject-profile-binding.json',
}


def event(kind, **kw):
    return dict(kind=kind, **kw)


def admission(**changes):
    fields = dict(request_id='contain-1', epoch='epoch-1', payload='payload-1', authority=True,
                  capacity=True, result_accepted=False, enrolled=True, single_provider=True,
                  scope_exact=True, qualified_profile=True, qualified_budgets=True, identity='assignment-node-candidate-goal-definition-grants-1')
    fields.update(changes)
    return event('admit', **fields)


def replacement(**changes):
    fields = dict(request_id='replacement-1', epoch='epoch-1', payload='replacement-payload',
                  authority=True, effect_safe=True, remaining_attempts=2,
                  identity='assignment-node-candidate-goal-definition-grants-1')
    fields.update(changes)
    return event('replace', **fields)


def observation(**changes):
    fields = dict(proofs={k: True for k in PROOFS}, inventory_complete=True, obligations_settled=True)
    fields.update(changes)
    return event('observe', **fields)


def dispatch(**changes):
    fields = dict(authority=True, effect_safe=True, budget=True, configuration=True, claims=True)
    fields.update(changes)
    return event('prepare_dispatch', **fields)


def prefix():
    return [admission(), event('prepare_stop'), event('deliver_stop'), event('stop_ack'), observation(), event('certify'), event('release')]


def body_digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


def bind_inventory(doc):
    doc['identity']['closure_digest']=body_digest(doc['inventory'])


class PoststartProfileTests(unittest.TestCase):
    def cli(self, out, check=False):
        return subprocess.run([sys.executable, str(SCRIPT), *(['--check'] if check else []),
                               '--output-dir', str(out)], cwd=ROOT, capture_output=True,
                              text=True, timeout=20, check=False)

    def test_each_positive_proof_is_separately_necessary(self):
        for fact in PROOFS:
            with self.subTest(fact=fact):
                facts = {k: True for k in PROOFS}; facts[fact] = False
                state = run_scenario([admission(), event('prepare_stop'), observation(proofs=facts),
                                      event('certify'), event('release'), replacement(), dispatch()])
                self.assertFalse(state['certificate'])
                self.assertEqual(state['effects']['dispatch'], 0)
        for field in ('inventory_complete', 'obligations_settled'):
            state = run_scenario([admission(), event('prepare_stop'), observation(**{field: False}), event('certify')])
            self.assertFalse(state['certificate'])

    def test_observation_and_certificate_do_not_grant_stop_or_dispatch(self):
        for events in ([observation(), event('certify'), dispatch()], prefix(), prefix()+[dispatch()]):
            state = run_scenario(events)
            self.assertEqual(state['effects']['dispatch'], 0)
        state = run_scenario([observation(), event('prepare_stop')])
        self.assertEqual(state['effects']['stop'], 0)

    def test_result_race_and_siblings_are_preserved(self):
        state = run_scenario([event('sibling_result', id='sibling'), event('accept_result'), admission(), event('prepare_stop')])
        self.assertTrue(state['accepted_result'])
        self.assertEqual(state['effects']['stop'], 0)
        self.assertEqual(state['siblings'], ['sibling'])
        state = run_scenario([admission(), event('accept_result'), event('sibling_result', id='sibling')])
        self.assertFalse(state['accepted_result'])
        self.assertEqual(state['siblings'], ['sibling'])

    def test_two_explicit_steps_exact_identity_and_fresh_authority(self):
        self.assertEqual(run_scenario(prefix())['effects']['dispatch'], 0)
        for changed in ({'identity': 'changed'}, {'epoch': 'epoch-2'}, {'authority': False},
                        {'effect_safe': False}, {'remaining_attempts': 0}):
            state = run_scenario(prefix()+[replacement(**changed), dispatch()])
            self.assertEqual(state['effects']['dispatch'], 0, changed)
        for field in ('authority', 'effect_safe', 'budget', 'configuration', 'claims'):
            state = run_scenario(prefix()+[replacement(), dispatch(**{field: False})])
            self.assertEqual(state['effects']['dispatch'], 0, field)
            self.assertEqual(state['phase'], 'needs_attention')
        state = run_scenario(prefix()+[replacement(), dispatch(), event('deliver_dispatch')])
        self.assertEqual(state['remaining_attempts'], 1)
        self.assertTrue(state['original_attempt_charged'])
        self.assertEqual(state['model_decisions'], 0)

    def test_closed_control_rejects_ordinary_decision_mutations(self):
        for forbidden in ('assignment', 'node', 'candidate', 'goal', 'grants', 'task_updates',
                          'checklist', 'skip', 'root_complete', 'graph_selection', 'backend', 'session'):
            e = replacement(); e[forbidden] = 'changed'
            with self.subTest(field=forbidden), self.assertRaises(Rejected):
                run_scenario(prefix()+[e])

    def assert_oracle(self, events):
        oracle = ProtocolOracle(PROOFS)
        for i, e in enumerate(events, 1):
            oracle.apply(e)
            actual = run_scenario(events[:i])
            projection = {key: actual.get(key) for key in oracle.projection()}
            projection['intent'] = actual['intent'] is not None
            projection['replacement'] = actual['replacement'] is not None
            self.assertEqual(projection, oracle.projection(), (i,events))
            self.assertTrue(actual['original_attempt_charged'])
            self.assertEqual(actual['model_decisions'],0)
        return actual

    def test_exhaustive_boundary_recovery_and_duplicate_effect_ledger(self):
        base = prefix()+[replacement(), dispatch(), event('deliver_dispatch'), event('dispatch_ack')]
        explored = 0
        for cut in range(len(base)+1):
            for faults in itertools.product((False, True), repeat=4):
                disturbance = [event('crash'), event('recover')]
                if faults[0]: disturbance += [admission()]
                if faults[1]: disturbance += [event('prepare_stop'), event('deliver_stop')]
                if faults[2]: disturbance += [replacement()]
                if faults[3]: disturbance += [dispatch(),event('deliver_dispatch')]
                self.assert_oracle(base[:cut]+disturbance+base[cut:]+[event('old_callback')])
                explored += 1
        # 12 cuts * 16 subsets of independently lost/replayed control steps.
        self.assertEqual(explored, 192)

    def test_reordered_races_match_independent_oracle_at_every_step(self):
        explored = 0
        for order in itertools.permutations([admission(), event('accept_result'),
                                             event('prepare_stop'), event('deliver_stop')]):
            self.assert_oracle(list(order)+[event('crash'),event('recover'),event('prepare_stop'),event('deliver_stop')])
            explored += 1
        for order in itertools.permutations([replacement(),dispatch(),event('deliver_dispatch'),event('dispatch_ack')]):
            self.assert_oracle(prefix()+list(order)+[event('crash'),event('recover'),dispatch(),event('deliver_dispatch')])
            explored += 1
        self.assertEqual(explored,48)

    def test_newer_incomplete_evidence_invalidates_ready_and_linked_proof(self):
        for fact in ('writers_quiescent','claims_settled','future_issuance_fence'):
            facts=dict.fromkeys(PROOFS,True); facts[fact]=False
            for stage in ([],[replacement()],[replacement(),dispatch()]):
                events=prefix()+stage
                events += [observation(proofs=facts),replacement(),dispatch(),event('deliver_dispatch')]
                state=self.assert_oracle(events)
                self.assertFalse(state['certificate'])
                self.assertEqual(state['effects']['dispatch'],0)

    def test_context_changes_after_ready_link_and_prepare_invalidate_delivery(self):
        for key in ('authority','effect_safe','budget','configuration','claims'):
            fields=dict(authority=True,effect_safe=True,budget=True,configuration=True,claims=True)
            fields[key]=False
            for stage in ([],[replacement()],[replacement(),dispatch()]):
                events=prefix()+stage+[event('context_change',**fields),replacement(),dispatch(),event('deliver_dispatch')]
                state=self.assert_oracle(events)
                self.assertEqual(state['effects']['dispatch'],0,(key,stage))

    def test_trusted_authority_before_admission_cannot_be_overridden(self):
        fields=dict(authority=False,effect_safe=True,budget=True,configuration=True,claims=True)
        state=self.assert_oracle([event('context_change',**fields),admission(),event('prepare_stop'),event('deliver_stop')])
        self.assertEqual(state['effects']['stop'],0)

    def test_schema_structure_and_bounded_semantics_are_separate_layers(self):
        from poststart_profile.schemas import schemas
        schema=schemas()['certificate.schema.json']
        raw=fixtures()['certificate.json'][0]
        original=json.loads(raw)
        for mutate in ('unicode_bytes','duplicate_actor','float_version','float_revision'):
            doc=copy.deepcopy(original)
            if mutate=='unicode_bytes':
                doc['effect_verifier']['name']='é'*2049
            elif mutate=='float_version':
                doc['v']=1.0
            elif mutate=='float_revision':
                doc['identity']['goal_revision']=1.0
            else:
                row=copy.deepcopy(doc['inventory'][0]); row['generation']='different-generation'
                doc['inventory'].append(row)
            self.assertTrue(schema_accepts(doc,schema))
            with self.assertRaises(Rejected): decode(json.dumps(doc).encode())
        for raw in (b'{"n":'+b'9'*10000+b'}', b'{"n":'+b'9'*1000+b'}', b'{"n":"\\ud800"}',
                    b'{"n":Infinity}', b'{"n":-Infinity}'):
            result=classify(raw)
            self.assertIs(result['candidate_valid'],False)
            self.assertIs(result['runtime_authorized'],False)

    def test_settlement_closure_permanently_revokes_private_stop_delivery(self):
        # Positive settlement can arrive while signal delivery is unknown. That
        # evidence closes the old domain and revokes a captured signal handoff.
        for suffix in ([],[replacement()],[replacement(),dispatch(),event('deliver_dispatch')]):
            events=[admission(),event('prepare_stop'),observation(),event('certify'),event('release')]
            state=self.assert_oracle(events+suffix+[event('deliver_stop'),event('crash'),event('recover'),event('deliver_stop')])
            self.assertEqual(state['effects']['stop'],0)
            self.assertEqual(state['effects']['dispatch'],int(bool(suffix) and len(suffix)==3))

    def test_blank_reason_and_unknown_obligation_labels_are_rejected(self):
        corpus=fixtures()
        contain=json.loads(corpus['contain-request.json'][0])
        for blank in ('',' ','\t\n'):
            doc=copy.deepcopy(contain); doc['reason']=blank
            self.assertFalse(classify(json.dumps(doc).encode())['candidate_valid'])
        certificate=json.loads(corpus['certificate.json'][0])
        doc=copy.deepcopy(certificate); doc['inventory'][0]['obligation']='unclassified-nonsense'
        bind_inventory(doc)
        with self.assertRaises(Rejected) as rejected: decode(json.dumps(doc).encode())
        self.assertEqual(rejected.exception.code,'invalid_obligation')
        for obligation in ('required','incidental','unknown'):
            doc=copy.deepcopy(certificate); doc['inventory'][0]['obligation']=obligation
            doc['inventory'][0]['settlement']='pending'; bind_inventory(doc)
            self.assertEqual(classify(json.dumps(doc).encode())['code'],'settlement_unknown')

    def test_unknown_stop_and_launch_never_get_fresh_effect_or_identity(self):
        state = run_scenario([admission(), event('prepare_stop'), event('deliver_stop'), event('crash'), event('recover'), event('prepare_stop')])
        self.assertEqual(state['effects']['stop'], 1)
        self.assertEqual(state['stop_status'], 'unknown')
        first = run_scenario(prefix()+[replacement(), dispatch(), event('deliver_dispatch')])
        replay = run_scenario(prefix()+[replacement(), dispatch(), event('deliver_dispatch'), event('crash'), replacement(), dispatch(), event('deliver_dispatch')])
        self.assertEqual(first['lineage'], replay['lineage'])
        self.assertEqual(replay['effects'], {'stop': 1, 'dispatch': 1})
        for field in ('epoch', 'payload', 'authority', 'request_id'):
            new = replacement(**{field: False if field == 'authority' else 'conflict'})
            state = run_scenario(prefix()+[replacement(), new, dispatch()])
            self.assertEqual(state['lineage'], run_scenario(prefix()+[replacement()])['lineage'])
            self.assertIn('stale_or_conflicting', state['verdicts'])

    def test_capacity_pressure_preserves_issued_history_and_completion(self):
        events = prefix()+[event('capacity_pressure') for _ in range(70)]
        state = run_scenario(events+[admission(), replacement(), dispatch(), event('deliver_dispatch')])
        self.assertEqual(state['intent']['request_id'], 'contain-1')
        self.assertTrue(state['certificate'])
        self.assertEqual(state['effects'], {'stop': 1, 'dispatch': 1})
        self.assertIn('completion_space_reserved', state['journal'])
        refused = run_scenario([admission(capacity=False), event('prepare_stop')])
        self.assertIsNone(refused['intent'])
        self.assertEqual(refused['effects']['stop'], 0)
        # Backpressure does not erase an accepted intent or block exact replay.
        self.assertIn('accepted_replay', state['verdicts'])

    def test_admission_authority_scope_and_original_enrollment_fail_before_effects(self):
        for field in ('authority', 'capacity', 'enrolled', 'single_provider', 'scope_exact', 'qualified_profile', 'qualified_budgets'):
            state = run_scenario([admission(**{field:False}), event('prepare_stop'), replacement(), dispatch()])
            self.assertEqual(state['effects'], {'stop':0, 'dispatch':0}, field)
        for field in ('epoch', 'payload', 'authority', 'request_id', 'identity'):
            changed = False if field=='authority' else 'conflicting'
            state = run_scenario([admission(), admission(**{field:changed}), event('prepare_stop'), event('deliver_stop')])
            self.assertIn('stale_or_conflicting', state['verdicts'])
            self.assertEqual(state['intent']['request_id'], 'contain-1')
            self.assertEqual(state['effects']['stop'],1)

    def test_decoder_encoding_and_exact_byte_inventory_depth_boundaries(self):
        valid = fixtures()['certificate.json'][0]
        for raw in (b'{"kind":"certificate","kind":"scenario"}', b'{"n":NaN}', b'\xff',
                    b'{"n":"\\ud800"}', b'['*18+b'0'+b']'*18):
            with self.subTest(raw=raw[:60]), self.assertRaises(Rejected):
                decode(raw)
        for depth, code in ((16, 'unknown_kind'), (17, 'depth_limit')):
            nested = 0
            for _ in range(depth): nested = {'n':nested}
            with self.assertRaises(Rejected) as rejected: decode(json.dumps(nested).encode())
            self.assertEqual(rejected.exception.code, code)
        doc = json.loads(valid)
        doc['effect_verifier']['name'] = 'x'*4096
        decode(json.dumps(doc).encode())
        doc['effect_verifier']['name'] += 'x'
        with self.assertRaises(Rejected): decode(json.dumps(doc).encode())
        padded = valid + b' '*(131072-len(valid))
        self.assertEqual(decode(padded), decode(valid))
        with self.assertRaises(Rejected): decode(padded+b' ')
        doc = json.loads(valid)
        actor = doc['inventory'][0]
        doc['inventory'] = [dict(actor, actor=f'actor-{i}') for i in range(100)]
        bind_inventory(doc)
        decode(json.dumps(doc).encode())
        doc['inventory'].append(dict(actor, actor='actor-100'))
        with self.assertRaises(Rejected): decode(json.dumps(doc).encode())

    def test_complete_capture_and_obligation_disposition_are_independent(self):
        raw = fixtures()['certificate.json'][0]
        base = json.loads(raw)
        for settlement in ('pending', 'unknown'):
            doc = copy.deepcopy(base)
            doc['inventory'][0]['settlement'] = settlement
            bind_inventory(doc)
            self.assertFalse(classify(json.dumps(doc).encode())['candidate_valid'])
        doc = copy.deepcopy(base)
        doc['inventory'][0]['settlement'] = 'failed'
        bind_inventory(doc)
        self.assertTrue(classify(json.dumps(doc).encode())['candidate_valid'])
        for fact in PROOFS:
            doc = copy.deepcopy(base); doc['proofs'][fact] = False
            self.assertFalse(classify(json.dumps(doc).encode())['candidate_valid'], fact)
        for fact in PROOFS:
            doc = copy.deepcopy(base); del doc['proofs'][fact]
            with self.assertRaises(Rejected): decode(json.dumps(doc).encode())

    def test_profile_times_are_explicit_positive_finite_without_defaults(self):
        raw = fixtures()['contain-request.json'][0]
        base = json.loads(raw)
        for key in ('stop', 'grace', 'execution', 'background'):
            for value in (0, -1, True, None, float('inf'), float('nan')):
                doc = copy.deepcopy(base); doc['profile']['expected_time_budgets'][key] = value
                self.assertFalse(classify(json.dumps(doc).encode())['candidate_valid'])
            doc = copy.deepcopy(base); del doc['profile']['expected_time_budgets'][key]
            self.assertFalse(classify(json.dumps(doc).encode())['candidate_valid'])
        self.assertEqual(classify(json.dumps(base).encode())['code'], 'structural_valid_untrusted')
        self.assertNotIn('fixture_only',base['profile'])

    def test_fixture_manifest_reproducibility_source_pins_and_schema_agreement(self):
        pins = [p for p in (ROOT/'contracts/v1').rglob('*') if p.is_file()]
        pins += [ROOT/'src/harflow_mcp/server.py', ROOT/'src/harflow_mcp/backends/base.py']
        before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in pins if p.exists()}
        with tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as td:
            a,b = Path(td)/'a', Path(td)/'b'
            for out in (a,b):
                result = self.cli(out); self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual({p.name:p.read_bytes() for p in a.iterdir()}, {p.name:p.read_bytes() for p in b.iterdir()})
            manifest_raw = (a/'manifest.json').read_bytes(); m = json.loads(manifest_raw)
            for key, val in {'status':'specified_unimplemented','provenance':'synthetic_specification',
                             'python_runtime_support':False,'rust_adoption':False,'runtime_qualified':False}.items():
                self.assertEqual(m[key], val)
            self.assertEqual((a/'manifest.sha256').read_text(), hashlib.sha256(manifest_raw).hexdigest()+'\n')
            rows = m['files']
            self.assertEqual({p.name for p in a.iterdir()}, {'manifest.json','manifest.sha256'}|{r['filename'] for r in rows})
            for row in rows:
                data = (a/row['filename']).read_bytes()
                self.assertEqual(len(data), row['bytes'])
                self.assertEqual(hashlib.sha256(data).hexdigest(), row['sha256'])
            for row in m['specifications']:
                self.assertEqual(hashlib.sha256((ROOT/row['path']).read_bytes()).hexdigest(), row['sha256'])
            schemas = {p.name:json.loads(p.read_bytes()) for p in a.glob('*.schema.json')}
            self.assertEqual(len(schemas), 3)
            for name,(raw, expected) in fixtures().items():
                try: doc = json.loads(raw)
                except (ValueError, UnicodeError): continue
                if not isinstance(doc,dict) or doc.get('kind') not in ('certificate','contain_request','replacement_request','scenario'): continue
                schema = schemas['certificate.schema.json' if doc['kind']=='certificate' else 'scenario.schema.json' if doc['kind']=='scenario' else 'request.schema.json']
                try: decode(raw); structurally_valid=True
                except (Rejected, UnicodeError): structurally_valid=False
                self.assertEqual(schema_accepts(doc,schema), structurally_valid or name in SCHEMA_ONLY_ACCEPTS, name)
            self.assertEqual(self.cli(a,True).returncode, 0)
        self.assertEqual(before, {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in pins if p.exists()})

    def test_source_identity_and_verifier_inventory_digest_bindings(self):
        corpus=fixtures()
        contain=json.loads(corpus['contain-request.json'][0])
        certificate=json.loads(corpus['certificate.json'][0])
        replacement_doc=json.loads(corpus['replacement-request.json'][0])
        self.assertEqual(contain['identity'],certificate['identity'])
        self.assertEqual(certificate['identity'],replacement_doc['identity'])
        self.assertNotEqual(contain['request_id'],replacement_doc['request_id'])
        self.assertEqual(contain['request_id'],certificate['contain_request_id'])
        self.assertEqual(contain['request_id'],replacement_doc['contain_request_id'])
        self.assertNotIn('request_id',contain['identity'])
        self.assertNotIn('ledger_epoch',contain['identity'])
        self.assertEqual(certificate['identity']['closure_digest'],body_digest(certificate['inventory']))
        self.assertEqual(contain['profile']['digest'],contain['identity']['profile_digest'])
        self.assertEqual(certificate['effect_verifier']['digest'],certificate['identity']['verifier_digest'])
        self.assertEqual(replacement_doc['effect_verifier']['digest'],replacement_doc['identity']['verifier_digest'])
        self.assertEqual(replacement_doc['certificate_digest'],hashlib.sha256(corpus['certificate.json'][0]).hexdigest())
        canonical_certificate=(json.dumps(certificate,sort_keys=True,indent=2,ensure_ascii=False,allow_nan=False)+'\n').encode()
        self.assertEqual(corpus['certificate.json'][0],canonical_certificate)
        self.assertEqual(classify(json.dumps(contain).encode())['code'],'structural_valid_untrusted')
        self.assertEqual(classify(json.dumps(replacement_doc).encode())['code'],'structural_valid_untrusted')
        for original,path,value in (
            (contain,('profile','digest'),'f'*64),
            (certificate,('effect_verifier','digest'),'f'*64),
            (replacement_doc,('effect_verifier','digest'),'f'*64),
            (certificate,('identity','closure_digest'),'f'*64),
            (certificate,('identity','goal_revision'),True),
            (certificate,('identity','goal_revision'),-1),
            (certificate,('identity','goal_digest'),'not-a-digest'),
            (certificate,('identity','definition_digest'),'not-a-digest'),
            (contain,('request_id',),'not-a-uuid'),
            (contain,('ledger_epoch',),True),
            (replacement_doc,('expected_remaining_attempts',),True),
        ):
            doc=copy.deepcopy(original); current=doc
            for key in path[:-1]: current=current[key]
            current[path[-1]]=value
            with self.subTest(path=path), self.assertRaises(Rejected): decode(json.dumps(doc).encode())
        doc=copy.deepcopy(certificate); doc['inventory'][0]['generation']='tampered-generation'
        with self.assertRaises(Rejected) as rejected: decode(json.dumps(doc).encode())
        self.assertEqual(rejected.exception.code,'inventory_mismatch')

    def test_link_validator_independently_rejects_cross_document_tampering(self):
        from poststart_profile import validate_link
        corpus=fixtures()
        originals=[json.loads(corpus[name][0]) for name in ('contain-request.json','certificate.json','replacement-request.json')]
        encode=lambda docs:[json.dumps(doc,sort_keys=True,indent=2,ensure_ascii=False).encode()+b'\n' for doc in docs]
        accepted=validate_link(*encode(originals))
        self.assertIs(accepted['runtime_authorized'],False)
        for index,path,value,code in (
            (2,('identity','goal_revision'),2,'source_identity_mismatch'),
            (2,('ledger_epoch',),2,'ledger_epoch_mismatch'),
            (2,('contain_request_id',),'00000000-0000-4000-8000-000000000099','contain_reference_mismatch'),
            (1,('contain_request_id',),'00000000-0000-4000-8000-000000000099','contain_reference_mismatch'),
            (2,('request_id',),originals[0]['request_id'],'request_identity_reused'),
            (2,('certificate_digest',),'f'*64,'certificate_digest_mismatch'),
        ):
            docs=copy.deepcopy(originals); current=docs[index]
            for key in path[:-1]: current=current[key]
            current[path[-1]]=value
            with self.subTest(path=path),self.assertRaises(Rejected) as rejected:
                validate_link(*encode(docs))
            self.assertEqual(rejected.exception.code,code)
        # A changed yet internally valid certificate cannot use the old reference.
        docs=copy.deepcopy(originals); docs[1]['proofs']['writers_quiescent']=False
        with self.assertRaises(Rejected) as rejected: validate_link(*encode(docs))
        self.assertEqual(rejected.exception.code,'certificate_digest_mismatch')

    def test_every_manifest_scenario_has_independently_checked_semantic_vector(self):
        with tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as td:
            out=Path(td)/'corpus'; result=self.cli(out)
            self.assertEqual(result.returncode,0,result.stderr)
            manifest=json.loads((out/'manifest.json').read_bytes())
            scenarios=0
            for row in manifest['files']:
                name=row['filename']
                if name.endswith('.schema.json'): continue
                doc=json.loads((out/name).read_bytes())
                expected=manifest['expectations'][name]
                semantic_refusal=name.startswith('missing-') or name=='incidental-pending.json'
                classification={'candidate_valid':not semantic_refusal and name not in REJECT_CODES,
                                'code':REJECT_CODES[name] if name in REJECT_CODES else 'settlement_unknown' if semantic_refusal else
                                       'structural_valid_untrusted' if doc['kind'] in ('contain_request','replacement_request') else 'hypothetical_valid',
                                'runtime_authorized':False}
                self.assertEqual(expected['classification'],classification,name)
                if doc['kind']!='scenario':
                    self.assertIsNone(expected['model']); continue
                actual=self.assert_oracle(doc['events'])
                oracle=ProtocolOracle(PROOFS)
                for e in doc['events']: oracle.apply(e)
                vector=expected['model']
                projection={key:vector.get(key) for key in oracle.projection()}
                projection['intent']=vector['intent'] is not None
                projection['replacement']=vector['replacement'] is not None
                self.assertEqual(projection,oracle.projection(),name)
                self.assertEqual(vector,actual,name)
                # Readable golden state must include provenance and all journals,
                # dispositions and deterministic IDs future consumers need.
                self.assertIs(vector['runtime_authorized'],False)
                self.assertTrue(vector['candidate_only'])
                self.assertIn('journal',vector); self.assertIn('verdicts',vector)
                self.assertIn('phase',vector)
                scenarios += 1
            self.assertEqual(scenarios,15)

    def test_negative_corpus_has_fixed_independent_codes_and_zero_is_untrusted(self):
        corpus=fixtures()
        self.assertTrue(set(REJECT_CODES)<=set(corpus))
        for name,code in REJECT_CODES.items():
            raw,expected=corpus[name]
            fixed={'candidate_valid':False,'code':code,'runtime_authorized':False}
            self.assertEqual(classify(raw),fixed,name)
            self.assertEqual(expected['classification'],fixed,name)
        raw,expected=corpus['zero-budget-untrusted-witness.json']
        fixed={'candidate_valid':True,'code':'structural_valid_untrusted','runtime_authorized':False}
        self.assertEqual(classify(raw),fixed)
        self.assertEqual(expected['classification'],fixed)
        self.assertEqual(json.loads(raw)['expected_remaining_attempts'],0)
        state=self.assert_oracle(prefix()+[replacement(remaining_attempts=0),dispatch(),event('deliver_dispatch')])
        self.assertIsNone(state['replacement'])
        self.assertEqual(state['effects']['dispatch'],0)

    def test_unhashable_kind_values_are_typed_refusals(self):
        corpus=fixtures()
        for kind in ([],{}):
            doc=json.loads(corpus['contain-request.json'][0]); doc['kind']=kind
            raw=json.dumps(doc).encode()
            self.assertEqual(classify(raw),{'candidate_valid':False,'code':'unknown_kind','runtime_authorized':False})
            with self.assertRaises(Rejected) as rejected: decode(raw)
            self.assertEqual(rejected.exception.code,'unknown_kind')
            with self.assertRaises(Rejected) as rejected: run_scenario([{'kind':kind}])
            self.assertEqual(rejected.exception.code,'unknown_event')
            scenario=json.loads(corpus['hypothetical-sequence.json'][0]); scenario['events']=[{'kind':kind}]
            self.assertEqual(classify(json.dumps(scenario).encode()),
                             {'candidate_valid':False,'code':'unknown_event','runtime_authorized':False})

    def test_unknown_obligation_cannot_become_replacement_proof_by_settlement_label(self):
        certificate=json.loads(fixtures()['certificate.json'][0])
        for settlement in ('satisfied','failed','pending','unknown'):
            doc=copy.deepcopy(certificate)
            doc['inventory'][0].update(obligation='unknown',settlement=settlement)
            bind_inventory(doc)
            self.assertTrue(all(doc['proofs'].values()))
            # Unknown is syntactically representable; it never grants readiness.
            self.assertEqual(decode(json.dumps(doc).encode()),doc)
            self.assertEqual(classify(json.dumps(doc).encode()),
                             {'candidate_valid':False,'code':'settlement_unknown','runtime_authorized':False})

    def test_replacement_request_id_cannot_reuse_contain_identity(self):
        state=self.assert_oracle(prefix()+[replacement(request_id='contain-1'),dispatch(),event('deliver_dispatch')])
        self.assertIsNone(state['replacement'])
        self.assertIsNone(state['remaining_attempts'])
        self.assertEqual(state['effects']['dispatch'],0)
        self.assertIn('request_identity_reused',state['verdicts'])

    def test_shared_proof_read_boundary_and_tamper_fail_closed(self):
        from poststart_profile.tooling import read
        with tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as td:
            root=Path(td); proof=root/'proof'; proof.write_bytes(b'x'*8388608)
            self.assertEqual(len(read(proof)),8388608)
            with proof.open('ab') as stream: stream.write(b'x')
            with self.assertRaises(ValueError): read(proof)
            out=root/'corpus'; self.assertEqual(self.cli(out).returncode,0)
            victim=next(p for p in out.glob('*.json') if p.name!='manifest.json')
            raw=victim.read_bytes(); victim.write_bytes(raw+b' ')
            self.assertNotEqual(self.cli(out,True).returncode,0)
            victim.write_bytes(raw)
            digest=out/'manifest.sha256'; digest.write_text('0'*64+'\n')
            self.assertNotEqual(self.cli(out,True).returncode,0)

    def test_output_safety_and_check_is_read_only(self):
        with tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as td:
            out = Path(td)/'corpus'; result=self.cli(out)
            self.assertEqual(result.returncode,0,result.stderr)
            before={p.name:(p.stat().st_ino,p.stat().st_mtime_ns,p.read_bytes()) for p in out.iterdir()}
            self.assertEqual(self.cli(out,True).returncode,0)
            self.assertEqual(before,{p.name:(p.stat().st_ino,p.stat().st_mtime_ns,p.read_bytes()) for p in out.iterdir()})
            extra=out/'unexpected'; extra.write_text('sentinel')
            self.assertNotEqual(self.cli(out,True).returncode,0)
            self.assertNotEqual(self.cli(out).returncode,0)
            self.assertEqual(extra.read_text(),'sentinel'); extra.unlink()
            victim=next(p for p in out.iterdir() if p.suffix=='.json' and p.name!='manifest.json')
            raw=victim.read_bytes(); external=Path(td)/'external'; external.write_bytes(b'sentinel')
            victim.unlink(); victim.symlink_to(external)
            self.assertNotEqual(self.cli(out).returncode,0)
            self.assertEqual(external.read_bytes(),b'sentinel')
            victim.unlink(); victim.write_bytes(raw); external.unlink(); os.link(victim,external)
            self.assertNotEqual(self.cli(out).returncode,0)
            self.assertEqual(external.read_bytes(),raw)
            alias=Path(td)/'alias'; alias.symlink_to(out,target_is_directory=True)
            self.assertNotEqual(self.cli(alias).returncode,0)

if __name__ == '__main__': unittest.main()
