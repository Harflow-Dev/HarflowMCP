"""Standalone unittest; isolated owned roots and synthetic effects only."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from harflow_mcp.poststart_schema import PoststartError, validate_document, decode
from poststart_control_fixtures import PROOFS, SyntheticEffects, digest, documents, trusted_context


class GrammarTests(unittest.TestCase):
    def test_hand_authored_documents_and_canonical_certificate(self):
        contain, certificate, replacement = documents()
        for doc in (contain, certificate, replacement):
            self.assertEqual(validate_document(doc), doc)
        self.assertEqual(replacement['certificate_digest'], digest(certificate))

    def test_closed_grammar_and_integer_tokens(self):
        base, _, replacement = documents()
        cases = []
        for key, value in [('authorized', True), ('assignment', 'changed'), ('qualified', True)]:
            cases.append(base | {key: value})
        for key in ('v', 'ledger_epoch'):
            for value in (True, 1.0, -1, 9007199254740992):
                cases.append(base | {key: value})
        for key in base['identity']:
            if key.endswith('_revision'):
                doc = copy.deepcopy(base)
                doc['identity'][key] = False
                cases.append(doc)
        for value in (True, 2.0, -1):
            cases.append(replacement | {'expected_remaining_attempts': value})
        for doc in cases:
            with self.subTest(doc=doc), self.assertRaises(PoststartError):
                validate_document(doc)

    def test_strict_json_utf8_duplicate_keys_and_bounds(self):
        for raw in (b'{"a":1,"a":2}', b'{"x":NaN}', b'"\xff"', b'"\\ud800"',
                    b'"' + b'a' * 4097 + b'"', b'[' * 18 + b'0' + b']' * 18,
                    b'[' + b','.join([b'0'] * 101) + b']'):
            with self.subTest(raw=raw[:30]), self.assertRaises((PoststartError, UnicodeError)):
                decode(raw)

    def test_malformed_top_level_and_kind_are_controlled_refusals(self):
        for value in (None, [], 1, True, "text", {}, {"kind": []}, {"kind": {}}):
            with self.subTest(value=value), self.assertRaises(PoststartError):
                validate_document(value)
        for count in (1000, 10000):
            with self.subTest(integer_digits=count), self.assertRaises(PoststartError):
                decode(b'{"ledger_epoch":' + b'9' * count + b'}')

    def test_budget_profile_verifier_actor_and_action_bindings(self):
        contain, certificate, replacement = documents()
        invalid = []
        for value in (0, -1, True, float('inf')):
            doc = copy.deepcopy(contain)
            doc['profile']['expected_time_budgets']['stop'] = value
            invalid.append(doc)
        doc = copy.deepcopy(contain)
        doc['profile']['digest'] = 'c' * 64
        invalid.append(doc)
        doc = copy.deepcopy(certificate)
        doc['inventory'].append(copy.deepcopy(doc['inventory'][0]))
        invalid.append(doc)
        invalid.append(replacement | {'request_id': replacement['contain_request_id']})
        doc = copy.deepcopy(replacement)
        doc['effect_verifier']['digest'] = 'c' * 64
        invalid.append(doc)
        for doc in invalid:
            with self.subTest(doc=doc), self.assertRaises((PoststartError, ValueError)):
                validate_document(doc)


    def test_raw_byte_budget_accepts_compact_certificate_despite_pretty_growth(self):
        import hashlib
        _, certificate, _ = documents()
        certificate['inventory'] = [dict(actor=str(i) + 'x' * 1180, generation='g',
            obligation='required', settlement='satisfied') for i in range(100)]
        compact_inventory = json.dumps(certificate['inventory'], sort_keys=True,
                                      separators=(',', ':')).encode()
        certificate['identity']['closure_digest'] = hashlib.sha256(compact_inventory).hexdigest()
        raw = json.dumps(certificate, separators=(',', ':')).encode()
        self.assertLess(len(raw), 131072)
        self.assertGreater(len(json.dumps(certificate, indent=2).encode()), 131072)
        self.assertEqual(validate_document(raw), certificate)
        with self.assertRaises(PoststartError):
            validate_document(raw + b' ' * (131073 - len(raw)))


class DurableTests(unittest.TestCase):
    def setUp(self):
        from harflow_mcp.workflows import WorkflowStore
        from harflow_mcp.poststart_store import PoststartStore
        self.temporary = tempfile.TemporaryDirectory(prefix='poststart-test-')
        self.addCleanup(self.temporary.cleanup)
        self.workflow = WorkflowStore(Path(self.temporary.name) / 'owned')
        self.store = PoststartStore(self.workflow)
        self.contain, self.certificate, self.replacement = documents()
        self.adapter = SyntheticEffects()
        self.handle = self.store.initialize(
            self.contain['identity'], adapter_id=self.adapter.adapter_id,
            profile=self.contain['profile'], effect_verifier=self.certificate['effect_verifier'],
            assignment={'instructions': 'Immutable assignment'}, remaining_attempts=2,
            context=trusted_context())

    def coordinator(self, reopen=False):
        from harflow_mcp.poststart_control import SyntheticCoordinator
        if reopen:
            from harflow_mcp.poststart_store import PoststartStore
            self.store = PoststartStore(self.workflow).reopen(self.handle)
        return SyntheticCoordinator(self.store, self.handle, self.adapter)

    def test_disk_reopen_deep_copy_and_stale_cas(self):
        original = self.store.snapshot()
        altered = self.store.snapshot()
        altered['assignment']['instructions'] = 'tampered'
        self.assertEqual(self.store.snapshot(), original)
        self.store.transact(lambda state: state['siblings'].update(one='accepted'),
                            expected_version=0)
        with self.assertRaises(PoststartError):
            self.store.transact(lambda state: state['siblings'].clear(), expected_version=0)
        self.coordinator(reopen=True)
        self.assertEqual(self.store.snapshot()['siblings'], {'one': 'accepted'})

    def test_anchor_missing_ledger_and_cross_mode_or_adapter_refuse(self):
        from dataclasses import replace
        from harflow_mcp.poststart_store import PoststartStore
        for handle in (replace(self.handle, mode='production'),
                       replace(self.handle, adapter_id='other'), replace(self.handle, epoch=1)):
            with self.subTest(handle=handle), self.assertRaises(PoststartError):
                PoststartStore(self.workflow).reopen(handle)
        anchor, ledger = self.store._paths(self.handle.root)
        saved = ledger.read_bytes()
        ledger.unlink()
        with self.assertRaises(PoststartError):
            PoststartStore(self.workflow).reopen(self.handle)
        with self.assertRaises(PoststartError):
            self.store.initialize(self.contain['identity'], adapter_id=self.adapter.adapter_id,
                profile=self.contain['profile'], effect_verifier=self.certificate['effect_verifier'],
                assignment={'instructions': 'Immutable assignment'}, remaining_attempts=2,
                context=trusted_context())
        ledger.write_bytes(saved)
        anchor.unlink()
        with self.assertRaises(PoststartError):
            PoststartStore(self.workflow).reopen(self.handle)

    def test_corrupt_duplicate_and_nonregular_records_refuse(self):
        from harflow_mcp.poststart_store import PoststartStore
        _, ledger = self.store._paths(self.handle.root)
        saved = ledger.read_bytes()
        for raw in (b'{}', b'{"v":1,"v":1}', b'not json'):
            ledger.write_bytes(raw)
            with self.subTest(raw=raw), self.assertRaises(PoststartError):
                PoststartStore(self.workflow).reopen(self.handle)
        ledger.write_bytes(saved)
        ledger.unlink()
        ledger.symlink_to(Path(self.temporary.name) / 'nonexistent')
        with self.assertRaises(PoststartError):
            PoststartStore(self.workflow).reopen(self.handle)

    def test_before_and_after_atomic_replace_crash_cuts(self):
        def cut():
            raise OSError('injected durable write cut')
        self.store.before_replace = cut
        with self.assertRaises(OSError):
            self.store.transact(lambda state: state['siblings'].update(before='accepted'))
        self.store.before_replace = None
        self.assertEqual(self.store.snapshot()['siblings'], {})
        self.store.after_replace = cut
        with self.assertRaises(OSError):
            self.store.transact(lambda state: state['siblings'].update(after='accepted'))
        self.store.after_replace = None
        self.assertEqual(self.store.snapshot()['siblings'], {'after': 'accepted'})
        self.assertEqual(self.adapter.effects, [])

    def test_valid_shape_profile_verifier_and_assignment_tampering_refuse(self):
        from harflow_mcp.poststart_store import PoststartStore
        _, ledger = self.store._paths(self.handle.root)
        original = json.loads(ledger.read_bytes())
        for field in ('profile', 'effect_verifier'):
            bad = copy.deepcopy(original)
            bad[field]['digest'] = 'c' * 64
            ledger.write_text(json.dumps(bad))
            with self.subTest(field=field), self.assertRaises(PoststartError):
                PoststartStore(self.workflow).reopen(self.handle)
        bad = copy.deepcopy(original)
        bad['assignment']['instructions'] = 'different assignment'
        ledger.write_text(json.dumps(bad))
        with self.assertRaises(PoststartError):
            PoststartStore(self.workflow).reopen(self.handle)

    def test_hardlink_record_refuses(self):
        import os
        from harflow_mcp.poststart_store import PoststartStore
        _, ledger = self.store._paths(self.handle.root)
        os.link(ledger, Path(self.temporary.name) / 'alias')
        with self.assertRaises(PoststartError):
            PoststartStore(self.workflow).reopen(self.handle)

    def ready(self, coordinator):
        coordinator.contain(self.contain)
        handoff = coordinator.prepare_stop()
        receipt = coordinator.deliver(handoff)
        coordinator.acknowledge(receipt['command_id'], receipt)
        coordinator.observe(self.certificate)
        self.assertEqual(coordinator.certify(), digest(self.certificate))
        coordinator.release()

    def test_explicit_two_request_path_and_charge_preservation(self):
        coordinator = self.coordinator()
        coordinator.sibling_result('independent', {'status': 'accepted'})
        self.ready(coordinator)
        ready = coordinator.snapshot()
        self.assertEqual(ready['phase'], 'ready_to_replan')
        self.assertEqual(len(self.adapter.effects), 1)
        self.assertIsNone(ready['replacement'])
        self.assertEqual(ready['remaining_attempts'], 2)
        receipt = coordinator.replace(self.replacement)
        committed = coordinator.snapshot()
        self.assertEqual(committed['phase'], 'replacement_committed')
        self.assertEqual(committed['remaining_attempts'], 1)
        self.assertTrue(committed['original_charged'])
        self.assertIsNone(committed['original_usage'])
        self.assertEqual(committed['model_decisions'], 0)
        self.assertEqual(committed['siblings'], {'independent': {'status': 'accepted'}})
        self.assertEqual(committed['accepted_decision'], 'preserved')
        self.assertEqual(committed['assignment'], {'instructions': 'Immutable assignment'})
        self.assertIn(self.contain['identity']['task'], committed['retired_tasks'])
        self.assertEqual(coordinator.replace(self.replacement), receipt)
        self.assertEqual(coordinator.snapshot()['remaining_attempts'], 1)
        handoff = coordinator.prepare_dispatch()
        effect = coordinator.deliver(handoff)
        coordinator.acknowledge(effect['command_id'], effect)
        self.assertEqual([item['kind'] for item in self.adapter.effects], ['stop', 'dispatch'])
        self.assertEqual(coordinator.snapshot()['remaining_attempts'], 1)
        with self.assertRaises(PoststartError):
            coordinator.accept_result({'from': 'old original'}, identity=self.contain['identity'])
        self.assertIsNone(coordinator.snapshot()['result'])

    def test_replay_conflicts_result_winner_and_reordered_actions(self):
        coordinator = self.coordinator()
        for action in (coordinator.prepare_stop, coordinator.certify, coordinator.release,
                       lambda: coordinator.replace(self.replacement), coordinator.prepare_dispatch):
            with self.subTest(action=action), self.assertRaises(PoststartError):
                action()
        accepted = coordinator.contain(self.contain)
        self.assertEqual(coordinator.contain(self.contain), accepted)
        changed = copy.deepcopy(self.contain)
        changed['reason'] = 'different payload'
        with self.assertRaises(PoststartError):
            coordinator.contain(changed)
        self.assertEqual(self.adapter.effects, [])
        with self.assertRaises(PoststartError):
            coordinator.accept_result('late original', identity=self.contain['identity'])
        self.assertIsNone(coordinator.snapshot()['result'])

    def test_already_accepted_result_wins_without_fences_or_effects(self):
        coordinator = self.coordinator()
        self.assertEqual(coordinator.accept_result({'status': 'accepted'}, identity=self.contain['identity']), {'status': 'accepted'})
        with self.assertRaises(PoststartError):
            coordinator.contain(self.contain)
        state = coordinator.snapshot()
        self.assertEqual(state['result'], {'status': 'accepted'})
        self.assertFalse(state['fence'])
        self.assertIsNone(state['intent'])
        self.assertEqual(self.adapter.effects, [])

    def test_every_positive_admission_fact_is_independent(self):
        coordinator = self.coordinator()
        for key in ('authority', 'cancel_authority', 'enrolled', 'single_provider', 'scope_exact',
                    'qualified_profile', 'qualified_budgets'):
            bad = trusted_context() | {key: False, 'generation': coordinator.snapshot()['context']['generation'] + 1}
            coordinator.update_context(bad)
            before = coordinator.snapshot()
            with self.subTest(key=key), self.assertRaises(PoststartError):
                coordinator.contain(self.contain)
            self.assertEqual(coordinator.snapshot(), before)
            self.assertEqual(self.adapter.effects, [])
        coordinator.update_context(trusted_context() | {'generation': coordinator.snapshot()['context']['generation'] + 1})
        coordinator.contain(self.contain)

    def test_each_missing_proof_unknown_obligation_and_pending_incidental_block(self):
        coordinator = self.coordinator()
        coordinator.contain(self.contain)
        effect = coordinator.deliver(coordinator.prepare_stop())
        coordinator.acknowledge(effect['command_id'], effect)
        for key in PROOFS:
            certificate = copy.deepcopy(self.certificate)
            certificate['proofs'][key] = False
            coordinator.observe(certificate)
            with self.subTest(proof=key), self.assertRaises(PoststartError):
                coordinator.certify()
        # Closure is identity-bound; enroll a separate source for changed inventories.
        for obligation, settlement in (('unknown', 'satisfied'), ('unknown', 'failed'),
                                       ('incidental', 'pending'), ('required', 'unknown')):
            certificate = copy.deepcopy(self.certificate)
            certificate['inventory'][0].update(obligation=obligation, settlement=settlement)
            import hashlib
            closure = hashlib.sha256(json.dumps(certificate['inventory'], sort_keys=True,
                      separators=(',', ':')).encode()).hexdigest()
            certificate['identity']['closure_digest'] = closure
            with self.subTest(obligation=obligation, settlement=settlement), self.assertRaises(PoststartError):
                coordinator.observe(certificate)
        coordinator.observe(self.certificate)
        coordinator.certify()
        coordinator.release()
        coordinator.observe(self.certificate)
        with self.assertRaises(PoststartError):
            coordinator.replace(self.replacement)
        self.assertEqual(coordinator.snapshot()['remaining_attempts'], 2)

    def test_exact_identity_epoch_and_action_bindings(self):
        coordinator = self.coordinator()
        for key, original in self.contain['identity'].items():
            bad = copy.deepcopy(self.contain)
            if key.endswith('_revision'):
                bad['identity'][key] = original + 1
            elif key.endswith('_digest'):
                bad['identity'][key] = 'c' * 64
            elif key == 'launch_id':
                bad['identity'][key] = '20000000-0000-4000-8000-000000000001'
            else:
                bad['identity'][key] = original + '-changed'
            with self.subTest(key=key), self.assertRaises(PoststartError):
                coordinator.contain(bad)
        with self.assertRaises(PoststartError):
            coordinator.contain(self.contain | {'ledger_epoch': 1})
        self.ready(coordinator)
        for key, value in (('request_id', self.contain['request_id']),
                           ('contain_request_id', '20000000-0000-4000-8000-000000000002'),
                           ('certificate_digest', 'c' * 64), ('ledger_epoch', 1),
                           ('expected_remaining_attempts', 3)):
            with self.subTest(key=key), self.assertRaises(PoststartError):
                coordinator.replace(self.replacement | {key: value})
        self.assertEqual(coordinator.snapshot()['remaining_attempts'], 2)

    def test_restart_drops_private_handoff_and_never_resends_unknown_stop(self):
        coordinator = self.coordinator()
        accepted = coordinator.contain(self.contain)
        handoff = coordinator.prepare_stop()
        restarted = self.coordinator(reopen=True)
        self.assertEqual(restarted.contain(self.contain), accepted)
        with self.assertRaises(PoststartError):
            restarted.deliver(handoff)
        with self.assertRaises(PoststartError):
            restarted.prepare_stop()
        self.assertEqual(self.adapter.effects, [])

    def test_lost_stop_ack_is_unknown_and_never_resent(self):
        coordinator = self.coordinator()
        coordinator.contain(self.contain)
        self.adapter.lost_ack = True
        handoff = coordinator.prepare_stop()
        with self.assertRaises(RuntimeError):
            coordinator.deliver(handoff)
        restarted = self.coordinator(reopen=True)
        self.assertEqual(restarted.snapshot()['commands']['stop']['status'], 'unknown')
        with self.assertRaises(PoststartError):
            restarted.prepare_stop()
        with self.assertRaises(PoststartError):
            restarted.deliver(handoff)
        self.assertEqual(len(self.adapter.effects), 1)
        restarted.observe(self.certificate)
        self.assertEqual(restarted.certify(), digest(self.certificate))

    def test_effect_boundary_rejects_delayed_stop_and_dispatch_generation(self):
        coordinator = self.coordinator()
        coordinator.contain(self.contain)
        self.adapter.before_effect = lambda: coordinator.update_context(trusted_context() | {'generation': 2})
        with self.assertRaises(RuntimeError):
            coordinator.deliver(coordinator.prepare_stop())
        self.assertEqual(self.adapter.effects, [])
        self.assertEqual(coordinator.snapshot()['commands']['stop']['status'], 'unknown')

    def test_observation_and_context_change_revoke_handoffs_before_effect(self):
        coordinator = self.coordinator()
        self.ready(coordinator)
        coordinator.replace(self.replacement)
        handoff = coordinator.prepare_dispatch()
        coordinator.observe(self.certificate)
        with self.assertRaises(PoststartError):
            coordinator.deliver(handoff)
        self.assertEqual(len(self.adapter.effects), 1)
        self.assertEqual(coordinator.snapshot()['remaining_attempts'], 1)

    def test_unknown_and_incidental_inventory_never_becomes_readiness(self):
        import hashlib
        from harflow_mcp.workflows import WorkflowStore
        from harflow_mcp.poststart_store import PoststartStore
        from harflow_mcp.poststart_control import SyntheticCoordinator
        for index, (obligation, settlement) in enumerate((('unknown', 'satisfied'),
                ('unknown', 'failed'), ('incidental', 'pending'), ('required', 'unknown'))):
            contain, certificate, _ = documents()
            certificate['inventory'][0].update(obligation=obligation, settlement=settlement)
            closure = hashlib.sha256(json.dumps(certificate['inventory'], sort_keys=True,
                                               separators=(',', ':')).encode()).hexdigest()
            contain['identity']['closure_digest'] = closure
            certificate['identity']['closure_digest'] = closure
            store = PoststartStore(WorkflowStore(Path(self.temporary.name) / str(index)))
            handle = store.initialize(contain['identity'], adapter_id=self.adapter.adapter_id,
                    profile=contain['profile'], effect_verifier=certificate['effect_verifier'],
                    assignment={'instructions': 'Immutable assignment'}, remaining_attempts=2,
                    context=trusted_context())
            coordinator = SyntheticCoordinator(store, handle, SyntheticEffects())
            coordinator.contain(contain)
            receipt = coordinator.deliver(coordinator.prepare_stop())
            coordinator.acknowledge(receipt['command_id'], receipt)
            coordinator.observe(certificate)
            with self.subTest(obligation=obligation, settlement=settlement), self.assertRaises(PoststartError):
                coordinator.certify()
            self.assertEqual(coordinator.snapshot()['phase'], 'settlement_unknown')

    def test_adapter_boundary_rechecks_dispatch_evidence_after_claim(self):
        coordinator = self.coordinator()
        self.ready(coordinator)
        coordinator.replace(self.replacement)
        handoff = coordinator.prepare_dispatch()
        def invalidate_at_boundary():
            coordinator.observe(self.certificate)
        self.adapter.before_effect = invalidate_at_boundary
        with self.assertRaises(RuntimeError):
            coordinator.deliver(handoff)
        self.assertEqual([effect['kind'] for effect in self.adapter.effects], ['stop'])
        self.assertEqual(coordinator.snapshot()['commands']['dispatch']['status'], 'unknown')
        with self.assertRaises(PoststartError):
            coordinator.prepare_dispatch()

    def test_adapter_effects_run_without_control_or_tree_locks(self):
        import fcntl
        import os
        coordinator = self.coordinator()
        observed = []
        def assert_unlocked():
            # Existing named WorkflowStore locks are actual flock files.
            for path in self.workflow.root.glob('.workflow-*.lock'):
                descriptor = os.open(path, os.O_RDWR)
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)
            observed.append(True)
        self.adapter.before_effect = assert_unlocked
        self.ready(coordinator)
        coordinator.replace(self.replacement)
        receipt = coordinator.deliver(coordinator.prepare_dispatch())
        coordinator.acknowledge(receipt['command_id'], receipt)
        self.assertEqual(observed, [True, True])

    def test_concurrent_identical_requests_have_one_fixed_receipt(self):
        from concurrent.futures import ThreadPoolExecutor
        first, second = self.coordinator(), self.coordinator()
        with ThreadPoolExecutor(max_workers=2) as pool:
            receipts = list(pool.map(lambda c: c.contain(copy.deepcopy(self.contain)), (first, second)))
        self.assertEqual(receipts[0], receipts[1])
        self.assertEqual(len(first.snapshot()['receipts']), 1)
        self.assertEqual(self.adapter.effects, [])

    def test_claim_and_prepare_write_cuts_never_mint_or_repeat_effects(self):
        coordinator = self.coordinator()
        coordinator.contain(self.contain)
        def cut():
            raise OSError('injected commit acknowledgement loss')
        self.store.after_replace = cut
        with self.assertRaises(OSError):
            coordinator.prepare_stop()
        self.store.after_replace = None
        self.assertEqual(self.adapter.effects, [])
        restarted = self.coordinator(reopen=True)
        with self.assertRaises(PoststartError):
            restarted.prepare_stop()
        self.assertEqual(restarted.contain(self.contain)['request_id'], self.contain['request_id'])
        self.assertEqual(self.adapter.effects, [])

    def test_production_facade_refuses_without_inspecting_arguments(self):
        from harflow_mcp.poststart_control import poststart_control
        class NoTouch:
            def __getattribute__(self, name):
                raise AssertionError('production touched adapter/storage')
        with self.assertRaisesRegex(PoststartError, 'poststart_unsupported'):
            poststart_control(NoTouch(), adapter=NoTouch(), mode='synthetic')

    def test_successor_lineage_deterministic_reopen_and_second_explicit_control(self):
        import uuid
        coordinator = self.coordinator()
        self.ready(coordinator)
        lineage = coordinator.replace(self.replacement)
        from dataclasses import asdict
        # Independently calculate the documented enrollment + exact request namespace.
        binding = digest({'enrollment': asdict(self.handle), 'request': self.replacement})
        namespace = uuid.uuid5(uuid.NAMESPACE_URL, 'harflow.poststart.synthetic:' + binding)
        for key in ('decision_id', 'attempt_id', 'task_id', 'launch_id', 'activation_id'):
            self.assertEqual(lineage[key], str(uuid.uuid5(namespace, key)))
        self.assertEqual(lineage['supersedes'], self.contain['identity']['task'])
        self.assertEqual(self.coordinator(reopen=True).replace(self.replacement), lineage)
        coordinator = self.coordinator()
        receipt = coordinator.deliver(coordinator.prepare_dispatch())
        coordinator.acknowledge(receipt['command_id'], receipt)
        identity = copy.deepcopy(self.contain['identity'])
        for field, key in (('task', 'task_id'), ('attempt', 'attempt_id'),
                           ('launch_id', 'launch_id'), ('activation', 'activation_id')):
            identity[field] = lineage[key]
        bad = copy.deepcopy(identity)
        bad['candidate'] = 'different'
        with self.assertRaises(PoststartError):
            coordinator.advance_source(bad, trusted_context() | {'generation': 2})
        coordinator.advance_source(identity, trusted_context() | {'generation': 2})
        before_callback = coordinator.snapshot()
        with self.assertRaises(PoststartError):
            coordinator.accept_result({'from': 'late original'}, identity=self.contain['identity'])
        self.assertEqual(coordinator.snapshot(), before_callback)
        state = coordinator.snapshot()
        self.assertEqual(len(state['history']), 1)
        self.assertEqual(state['history'][0]['replacement'], lineage)
        self.assertEqual(state['remaining_attempts'], 1)
        self.assertEqual(state['control_issuances'], 1)
        self.assertEqual(len(state['receipts']), 2)
        next_request = copy.deepcopy(self.contain)
        next_request.update(identity=identity, request_id='10000000-0000-4000-8000-000000000004')
        coordinator.contain(next_request)
        self.assertEqual(coordinator.snapshot()['remaining_attempts'], 1)
        self.assertEqual(len(self.adapter.effects), 2)

    def test_dispatch_lost_ack_keeps_same_lineage_and_charge(self):
        coordinator = self.coordinator()
        self.ready(coordinator)
        lineage = coordinator.replace(self.replacement)
        self.adapter.lost_ack = True
        handoff = coordinator.prepare_dispatch()
        with self.assertRaises(RuntimeError):
            coordinator.deliver(handoff)
        restarted = self.coordinator(reopen=True)
        self.assertEqual(restarted.replace(self.replacement), lineage)
        self.assertEqual(restarted.snapshot()['remaining_attempts'], 1)
        self.assertEqual(restarted.snapshot()['original_usage'], None)
        self.assertEqual(restarted.snapshot()['commands']['dispatch']['status'], 'unknown')
        with self.assertRaises(PoststartError):
            restarted.prepare_dispatch()
        with self.assertRaises(PoststartError):
            restarted.deliver(handoff)
        self.assertEqual([item['kind'] for item in self.adapter.effects], ['stop', 'dispatch'])

    def test_replacement_before_and_after_commit_cuts_charge_exactly_once(self):
        coordinator = self.coordinator()
        self.ready(coordinator)
        def cut():
            raise OSError('cut')
        self.store.before_replace = cut
        with self.assertRaises(OSError):
            coordinator.replace(self.replacement)
        self.store.before_replace = None
        self.assertEqual(coordinator.snapshot()['remaining_attempts'], 2)
        self.assertIsNone(coordinator.snapshot()['replacement'])
        self.store.after_replace = cut
        with self.assertRaises(OSError):
            coordinator.replace(self.replacement)
        self.store.after_replace = None
        state = coordinator.snapshot()
        self.assertEqual(state['remaining_attempts'], 1)
        self.assertEqual(state['control_issuances'], 1)
        self.assertEqual(len(self.adapter.effects), 1)
        receipt = self.coordinator(reopen=True).replace(self.replacement)
        self.assertEqual(receipt, state['replacement'])
        self.assertEqual(self.store.snapshot()['remaining_attempts'], 1)

    def test_claim_commit_unknown_authorizes_no_effect(self):
        coordinator = self.coordinator()
        coordinator.contain(self.contain)
        handoff = coordinator.prepare_stop()
        def cut():
            raise OSError('claim acknowledgement lost')
        self.store.after_replace = cut
        with self.assertRaises(OSError):
            coordinator.deliver(handoff)
        self.store.after_replace = None
        self.assertEqual(self.adapter.effects, [])
        self.assertEqual(coordinator.snapshot()['commands']['stop']['status'], 'unknown')
        with self.assertRaises(PoststartError):
            coordinator.deliver(handoff)
        with self.assertRaises(PoststartError):
            self.coordinator(reopen=True).prepare_stop()

    def test_context_revocation_blocks_replacement_without_refund(self):
        coordinator = self.coordinator()
        self.ready(coordinator)
        for key in ('authority', 'effect_safe', 'budget', 'configuration', 'claims'):
            context = trusted_context() | {key: False,
                'generation': coordinator.snapshot()['context']['generation'] + 1}
            coordinator.update_context(context)
            with self.subTest(key=key), self.assertRaises(PoststartError):
                coordinator.replace(self.replacement)
            self.assertEqual(coordinator.snapshot()['remaining_attempts'], 2)
            context = trusted_context() | {'generation': context['generation'] + 1}
            coordinator.update_context(context)
            coordinator.observe(self.certificate)
            coordinator.certify()
            coordinator.release()
        coordinator.replace(self.replacement)
        coordinator.update_context(trusted_context() | {'authority': False,
            'generation': coordinator.snapshot()['context']['generation'] + 1})
        with self.assertRaises(PoststartError):
            coordinator.prepare_dispatch()
        self.assertEqual(coordinator.snapshot()['remaining_attempts'], 1)

    def test_text_backpressure_before_admission_and_reserved_completion(self):
        coordinator = self.coordinator()
        # 600 KiB of independent accepted history fits the text limit but leaves
        # insufficient reserved completion space for a new control.
        coordinator.sibling_result('large-history', ['h' * 4096] * 100)
        coordinator.sibling_result('more-history', ['h' * 4096] * 50)
        before = coordinator.snapshot()
        with self.assertRaises(PoststartError):
            coordinator.contain(self.contain)
        self.assertEqual(coordinator.snapshot(), before)
        self.assertEqual(self.adapter.effects, [])
        self.store.transact(lambda state: state['siblings'].clear())
        accepted = coordinator.contain(self.contain)
        # Unrelated writes cannot eat the accepted intent's completion reserve.
        with self.assertRaises(PoststartError):
            coordinator.sibling_result('would-consume-reserve', [['h' * 4096] * 75] * 2)
        self.assertEqual(coordinator.contain(self.contain), accepted)
        receipt = coordinator.deliver(coordinator.prepare_stop())
        coordinator.acknowledge(receipt['command_id'], receipt)
        for _ in range(12):
            coordinator.observe(self.certificate)
        self.assertEqual(coordinator.snapshot()['evidence_revision'], 12)
        self.assertEqual(len(coordinator.snapshot()['history']), 0)
        coordinator.certify()
        coordinator.release()
        coordinator.replace(self.replacement)
        self.assertEqual(coordinator.snapshot()['remaining_attempts'], 1)

    def test_exact_replay_remains_available_under_capacity_pressure(self):
        coordinator = self.coordinator()
        accepted = coordinator.contain(self.contain)
        self.store.metadata_limit = self.store.RESERVE + 1
        self.assertEqual(coordinator.contain(self.contain), accepted)
        changed = copy.deepcopy(self.contain)
        changed['reason'] = 'changed'
        with self.assertRaises(PoststartError):
            coordinator.contain(changed)
        self.assertEqual(self.adapter.effects, [])

    def test_fifo_refuses_without_waiting_for_writer(self):
        import os
        from harflow_mcp.poststart_store import PoststartStore
        _, ledger = self.store._paths(self.handle.root)
        ledger.unlink()
        os.mkfifo(ledger)
        with self.assertRaises(PoststartError):
            PoststartStore(self.workflow).reopen(self.handle)

    def test_nested_command_receipt_lineage_and_certificate_corruption_refuse(self):
        from harflow_mcp.poststart_store import PoststartStore
        coordinator = self.coordinator()
        self.ready(coordinator)
        coordinator.replace(self.replacement)
        original = self.store.snapshot()
        _, ledger = self.store._paths(self.handle.root)
        mutations = (
            lambda state: state['commands']['stop'].update(task_id='other-task'),
            lambda state: state['commands']['stop'].update(receipt={}),
            lambda state: state['replacement'].update(launch_id='other-launch'),
            lambda state: state['certificate'].update(revision=999),
            lambda state: state['release'].update(old_claims_settled=False),
            lambda state: state['receipts'][self.contain['request_id']].update(receipt={}),
            lambda state: state.update(original_usage=0),
        )
        for mutate in mutations:
            bad = copy.deepcopy(original)
            mutate(bad)
            ledger.write_text(json.dumps(bad))
            with self.subTest(mutation=mutate), self.assertRaises(PoststartError):
                PoststartStore(self.workflow).reopen(self.handle)
        ledger.write_text(json.dumps(original))

    def test_admission_atomic_cut_publishes_fences_with_receipt_or_nothing(self):
        coordinator = self.coordinator()
        def cut():
            raise OSError('admission cut')
        self.store.before_replace = cut
        with self.assertRaises(OSError):
            coordinator.contain(self.contain)
        self.store.before_replace = None
        state = coordinator.snapshot()
        self.assertIsNone(state['intent'])
        self.assertFalse(state['fence'])
        self.assertEqual(state['receipts'], {})
        self.store.after_replace = cut
        with self.assertRaises(OSError):
            coordinator.contain(self.contain)
        self.store.after_replace = None
        state = coordinator.snapshot()
        self.assertTrue(state['fence'])
        self.assertTrue(state['future_issuance_fence'])
        self.assertEqual(state['reservation'], self.store.RESERVE)
        self.assertEqual(state['intent'], self.contain)
        self.assertEqual(coordinator.contain(self.contain),
                         state['receipts'][self.contain['request_id']]['receipt'])
        self.assertEqual(self.adapter.effects, [])

    def test_claim_failure_before_replace_loses_token_without_effect(self):
        coordinator = self.coordinator()
        coordinator.contain(self.contain)
        handoff = coordinator.prepare_stop()
        def cut():
            raise OSError('before claim replacement')
        self.store.before_replace = cut
        with self.assertRaises(OSError):
            coordinator.deliver(handoff)
        self.store.before_replace = None
        self.assertEqual(coordinator.snapshot()['commands']['stop']['status'], 'prepared')
        self.assertEqual(self.adapter.effects, [])
        with self.assertRaises(PoststartError):
            coordinator.deliver(handoff)
        with self.assertRaises(PoststartError):
            self.coordinator(reopen=True).prepare_stop()

    def test_original_identity_pin_and_malformed_nested_receipts_refuse(self):
        from harflow_mcp.poststart_store import PoststartStore
        _, ledger = self.store._paths(self.handle.root)
        original = self.store.snapshot()
        for field in ('task', 'attempt', 'launch_id', 'activation', 'owner_generation',
                      'containment_generation', 'process_generation'):
            bad = copy.deepcopy(original)
            bad['identity'][field] = ('20000000-0000-4000-8000-000000000001'
                                      if field == 'launch_id' else 'different-source')
            ledger.write_text(json.dumps(bad))
            with self.subTest(field=field), self.assertRaises(PoststartError):
                PoststartStore(self.workflow).reopen(self.handle)
        ledger.write_text(json.dumps(original))
        coordinator = self.coordinator()
        coordinator.contain(self.contain)
        original = self.store.snapshot()
        for value in (None, [], {}, 'not a receipt', {'request': [], 'receipt': []}):
            bad = copy.deepcopy(original)
            bad['receipts'][self.contain['request_id']] = value
            ledger.write_text(json.dumps(bad))
            with self.subTest(value=value), self.assertRaises(PoststartError):
                PoststartStore(self.workflow).reopen(self.handle)
        ledger.write_text(json.dumps(original))

    def test_archived_lineage_cannot_be_dropped_or_rebound(self):
        from harflow_mcp.poststart_store import PoststartStore
        coordinator = self.coordinator()
        self.ready(coordinator)
        lineage = coordinator.replace(self.replacement)
        receipt = coordinator.deliver(coordinator.prepare_dispatch())
        coordinator.acknowledge(receipt['command_id'], receipt)
        identity = copy.deepcopy(self.contain['identity'])
        for field, key in (('task', 'task_id'), ('attempt', 'attempt_id'),
                           ('launch_id', 'launch_id'), ('activation', 'activation_id')):
            identity[field] = lineage[key]
        coordinator.advance_source(identity, trusted_context() | {'generation': 2})
        original = self.store.snapshot()
        _, ledger = self.store._paths(self.handle.root)
        for mutation in ('drop', 'rebind', 'duplicate'):
            bad = copy.deepcopy(original)
            if mutation == 'drop':
                bad['history'].clear()
            elif mutation == 'rebind':
                bad['history'][0]['identity']['task'] = 'other-task'
            else:
                bad['history'].append(copy.deepcopy(bad['history'][0]))
            ledger.write_text(json.dumps(bad))
            with self.subTest(mutation=mutation), self.assertRaises(PoststartError):
                PoststartStore(self.workflow).reopen(self.handle)
        ledger.write_text(json.dumps(original))
        self.assertEqual(coordinator.accept_result({'from': 'successor'}, identity=identity),
                         {'from': 'successor'})

    def test_oversized_anchor_and_ledger_reads_refuse(self):
        from harflow_mcp.poststart_store import PoststartStore
        anchor, ledger = self.store._paths(self.handle.root)
        saved_anchor, saved_ledger = anchor.read_bytes(), ledger.read_bytes()
        anchor.write_bytes(saved_anchor + b' ' * 131072)
        # An unusable ledger ensures the anchor is rejected first, without reading it.
        ledger.unlink()
        ledger.mkdir()
        from unittest.mock import patch
        reopening = PoststartStore(self.workflow)
        with patch.object(reopening, '_read', wraps=reopening._read) as reads:
            with self.assertRaises(PoststartError):
                reopening.reopen(self.handle)
            self.assertEqual([call.args[0] for call in reads.call_args_list], [anchor])
        ledger.rmdir()
        ledger.write_bytes(saved_ledger)
        anchor.write_bytes(saved_anchor)
        ledger.write_bytes(saved_ledger + b' ' * 4194304)
        with self.assertRaises(PoststartError):
            PoststartStore(self.workflow).reopen(self.handle)

    def test_acknowledgement_boolean_counter_tokens_refuse(self):
        coordinator = self.coordinator()
        coordinator.contain(self.contain)
        receipt = coordinator.deliver(coordinator.prepare_stop())
        for field in ('generation', 'evidence_revision'):
            bad = copy.deepcopy(receipt)
            bad[field] = bool(bad[field])
            with self.subTest(field=field), self.assertRaises(PoststartError):
                coordinator.acknowledge(receipt['command_id'], bad)
        self.assertEqual(coordinator.snapshot()['commands']['stop']['status'], 'unknown')
        coordinator.acknowledge(receipt['command_id'], receipt)

    def test_permanent_replacement_request_binding_active_and_archived(self):
        from harflow_mcp.poststart_store import PoststartStore
        coordinator = self.coordinator()
        self.ready(coordinator)
        lineage = coordinator.replace(self.replacement)
        _, ledger = self.store._paths(self.handle.root)
        def assert_tampered_requests_refused():
            original = self.store.snapshot()
            for field, value in (('contain_request_id', '20000000-0000-4000-8000-000000000002'),
                                 ('certificate_digest', 'c' * 64),
                                 ('expected_remaining_attempts', 3)):
                bad = copy.deepcopy(original)
                bad['receipts'][self.replacement['request_id']]['request'][field] = value
                ledger.write_text(json.dumps(bad))
                with self.subTest(field=field, archived=bool(original['history'])), self.assertRaises(PoststartError):
                    PoststartStore(self.workflow).reopen(self.handle)
            ledger.write_text(json.dumps(original))
        assert_tampered_requests_refused()
        # Re-observation revokes current readiness but cannot erase accepted provenance.
        coordinator.observe(self.certificate)
        self.assertIsNone(coordinator.snapshot()['certificate'])
        self.assertEqual(coordinator.replace(self.replacement), lineage)
        changed = self.replacement | {'expected_remaining_attempts': 1}
        with self.assertRaises(PoststartError):
            coordinator.replace(changed)
        coordinator.certify()
        coordinator.release()
        receipt = coordinator.deliver(coordinator.prepare_dispatch())
        coordinator.acknowledge(receipt['command_id'], receipt)
        identity = copy.deepcopy(self.contain['identity'])
        for field, key in (('task', 'task_id'), ('attempt', 'attempt_id'),
                           ('launch_id', 'launch_id'), ('activation', 'activation_id')):
            identity[field] = lineage[key]
        coordinator.advance_source(identity, trusted_context() | {'generation': 2})
        assert_tampered_requests_refused()
        self.assertEqual(coordinator.replace(self.replacement), lineage)

    def test_same_action_uuids_in_two_roots_issue_distinct_commands_and_lineage(self):
        from harflow_mcp.workflows import WorkflowStore
        from harflow_mcp.poststart_store import PoststartStore
        from harflow_mcp.poststart_control import SyntheticCoordinator
        first = self.coordinator()
        self.ready(first)
        first_lineage = first.replace(self.replacement)
        contain, certificate, replacement = documents()
        for doc in (contain, certificate, replacement):
            doc['identity']['root'] = 'independent-second-root'
        replacement['certificate_digest'] = digest(certificate)
        adapter = SyntheticEffects()
        store = PoststartStore(WorkflowStore(Path(self.temporary.name) / 'second-owned'))
        handle = store.initialize(contain['identity'], adapter_id=adapter.adapter_id,
            profile=contain['profile'], effect_verifier=certificate['effect_verifier'],
            assignment={'instructions': 'Immutable assignment'}, remaining_attempts=2,
            context=trusted_context())
        second = SyntheticCoordinator(store, handle, adapter)
        second.contain(contain)
        receipt = second.deliver(second.prepare_stop())
        second.acknowledge(receipt['command_id'], receipt)
        second.observe(certificate)
        second.certify()
        second.release()
        second_lineage = second.replace(replacement)
        for key in ('decision_id', 'attempt_id', 'task_id', 'launch_id', 'activation_id'):
            self.assertNotEqual(first_lineage[key], second_lineage[key])
        self.assertNotEqual(self.adapter.effects[0]['command_id'], adapter.effects[0]['command_id'])
        first_dispatch = first.deliver(first.prepare_dispatch())
        second_dispatch = second.deliver(second.prepare_dispatch())
        self.assertNotEqual(first_dispatch['command_id'], second_dispatch['command_id'])
        self.assertNotEqual(first_dispatch['task_id'], second_dispatch['task_id'])
        self.assertEqual(first.replace(self.replacement), first_lineage)
        self.assertEqual(second.replace(replacement), second_lineage)


if __name__ == '__main__':
    unittest.main()
