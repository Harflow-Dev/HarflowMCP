"""Standalone ST04 tests: genuine workflow CAS and isolated unpaid effect oracles.

Run with ``.venv/bin/python -m unittest discover -s tests -p test_workflow_poststart.py``.
No pytest collection, product conftest, agent CLI, real registry or live state is used.
"""
from __future__ import annotations

import asyncio
import copy
import json
from contextlib import ExitStack
from pathlib import Path
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from harflow_mcp import resources, state_ownership
from harflow_mcp.state_layout import resolve_layout
from workflow_poststart_fixtures import (AtomicEffects, create_workflow, host_observations,
    control_documents, coordinator, result, outcome, graph, active_sibling, admit_worker_turn)


class ResourceTransferTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory(prefix='workflow-poststart-resources-')))
        self.stack.enter_context(state_ownership.state_session(resolve_layout(root=self.root)))
        self.ledger = resources.ResourceLedger(self.root)
        self.old = resources.Owner('workflow_attempt', workflow_run_id='root', activation_id='old-attempt')
        self.claim = self.ledger.request(['checkout:fixture'], self.old, reservation_id='original-reservation')
        self.waiter = self.ledger.request(['checkout:fixture'], resources.Owner('task', task_id='conflicting-waiter'))
        self.assertTrue(self.claim.held)
        self.assertFalse(self.waiter.held)

    def transfer(self, old, new, generation=None, **kwargs):
        return self.ledger.transfer_owned_workflow_attempt(
            self.claim.reservation_id, 'root', old, new,
            expected_names=kwargs.get('names', ['checkout:fixture']),
            expected_generation=self.claim.generation if generation is None else generation)

    def test_same_reservation_stays_exclusive_through_both_handoffs(self):
        coordinator = self.transfer('old-attempt', 'coordinator')
        self.assertEqual(coordinator.reservation_id, self.claim.reservation_id)
        self.assertEqual(coordinator.owner.activation_id, 'coordinator')
        self.assertFalse(self.ledger.poll(self.waiter.reservation_id).held)
        successor = self.transfer('coordinator', 'successor', coordinator.generation)
        self.assertEqual(successor.owner.activation_id, 'successor')
        self.assertEqual(successor.generation, self.claim.generation + 2)
        self.assertFalse(self.ledger.poll(self.waiter.reservation_id).held)
        with self.assertRaises(resources.ResourceOwnerMismatch):
            self.ledger.release_owned_workflow_attempt(self.claim.reservation_id, 'root', 'old-attempt')
        self.assertEqual(self.ledger.release_owned_workflow_attempt(self.claim.reservation_id, 'root', 'successor'), 'released')
        self.assertTrue(self.ledger.poll(self.waiter.reservation_id).held)

    def test_lost_ack_exact_replay_does_not_repeat_transfer_or_grant_waiter(self):
        transferred = self.transfer('old-attempt', 'coordinator')
        # Reopen only the real leaf ledger: the caller lost the transfer response.
        self.ledger = resources.ResourceLedger(self.root)
        replay = self.transfer('old-attempt', 'coordinator')
        self.assertEqual(replay, transferred)
        self.assertFalse(self.ledger.poll(self.waiter.reservation_id).held)
        with self.assertRaises(resources.ResourceOwnerMismatch):
            self.transfer('old-attempt', 'different-coordinator')
        self.assertEqual(self.ledger.get(self.claim.reservation_id), transferred)

    def test_wrong_owner_names_generation_and_missing_claim_fail_closed(self):
        for old, new, generation, names in [
            ('unrelated', 'coordinator', self.claim.generation, ['checkout:fixture']),
            ('old-attempt', 'coordinator', self.claim.generation + 1, ['checkout:fixture']),
            ('old-attempt', 'coordinator', self.claim.generation, ['different']),
            ('old-attempt', 'old-attempt', self.claim.generation, ['checkout:fixture']),
        ]:
            with self.subTest(old=old, generation=generation, names=names):
                with self.assertRaises(resources.ResourceOwnerMismatch):
                    self.transfer(old, new, generation, names=names)
                self.assertEqual(self.ledger.get(self.claim.reservation_id), self.claim)
                self.assertFalse(self.ledger.poll(self.waiter.reservation_id).held)
        self.ledger.release_owned_workflow_attempt(self.claim.reservation_id, 'root', 'old-attempt')
        with self.assertRaises(resources.ResourceOwnerMismatch):
            self.transfer('old-attempt', 'coordinator')

    def test_lent_and_borrowed_topologies_refuse_without_any_owner_change(self):
        borrower = self.ledger.request(['checkout:fixture'], resources.Owner('workflow_attempt', workflow_run_id='root', activation_id='borrowed-attempt'),
                                       scope=resources.Scope(self.claim.reservation_id))
        before = self.ledger.entries()
        with self.assertRaises(resources.ResourceOwnerMismatch):
            self.transfer('old-attempt', 'coordinator')
        self.assertEqual(self.ledger.entries(), before)
        with self.assertRaises(resources.ResourceOwnerMismatch):
            self.ledger.transfer_owned_workflow_attempt(
                borrower.reservation_id, 'root', 'borrowed-attempt', 'coordinator',
                expected_names=['checkout:fixture'], expected_generation=borrower.generation)
        self.assertEqual(self.ledger.entries(), before)

    def test_unknown_coordinator_owner_survives_generic_reaper(self):
        transferred = self.transfer('old-attempt', 'coordinator')
        report = self.ledger.reap({'task': lambda entry: resources.KEEP})
        self.assertIn(transferred.reservation_id, report.kept)
        self.assertEqual(self.ledger.get(transferred.reservation_id), transferred)
        self.assertFalse(self.ledger.poll(self.waiter.reservation_id).held)


class WorkflowCheckpointTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.principal = host_observations(self.stack)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory(prefix='workflow-poststart-checkpoints-')))
        self.storage, self.registry, self.supervisor, self.node, self.token = create_workflow(self.root)
        self.adapter = AtomicEffects()
        self.jobs = []

    async def asyncTearDown(self):
        self.adapter.settled.set()
        for job in self.jobs:
            if not job.done():
                job.cancel()
        if self.jobs:
            await asyncio.gather(*self.jobs, return_exceptions=True)

    async def launch(self):
        from harflow_mcp.workflow_poststart import FixtureScope
        from harflow_mcp.replan_retention import new_outer_job
        self.scope = FixtureScope(self.storage, self.supervisor.run_id, self.registry, self.adapter)
        self.scope.principal = copy.deepcopy(self.principal)
        self.supervisor.poststart_fixture_scope = self.scope
        coro, done = new_outer_job(self.supervisor, self.token,
                                  {'pid': 12345, 'start_time': 'fixture-dispatcher', 'markers': []})
        job = asyncio.create_task(coro)
        done.bind(job)
        job.add_done_callback(done)
        self.jobs.append(job)
        observing = asyncio.create_task(self.adapter.observing.wait())
        completed, _ = await asyncio.wait([job, observing], timeout=5, return_when=asyncio.FIRST_COMPLETED)
        if job in completed:
            observing.cancel()
            await job  # Surface the real checkpoint failure rather than a timeout.
            self.fail('whole job returned without synthetic original observation')
        self.assertTrue(observing.done(), 'fixture original never reached observation')
        observing.cancel()
        self.coordinator = coordinator(self.storage, self.supervisor, self.scope)
        self.contain, self.certificate, self.replacement = control_documents(self.supervisor.run(), inventory=self.adapter.inventory)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        self.assertEqual(self.registry.calls, [])
        return job

    def source(self):
        run = self.supervisor.run(refresh_atomic=True)
        identity = run['poststart']['ledger']['identity']
        activation = next(a for a in run['activations'] if a['id'] == identity['activation'])
        return run, identity, activation

    async def settle(self, job):
        self.adapter.settled.set()
        await asyncio.wait_for(job, 5)
        # Scheduling a call_soon future waits for the already registered done callback.
        future = asyncio.get_running_loop().create_future()
        asyncio.get_running_loop().call_soon(future.set_result, None)
        await future

    async def test_original_enrollment_precedes_effect_and_uses_real_attempt(self):
        observations = []
        def at_effect():
            run = self.supervisor.run(refresh_atomic=True)
            observations.append(copy.deepcopy(run['poststart']))
        self.adapter.before_effect = at_effect
        job = await self.launch()
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]['runtime']['origin_issue']['status'], 'unknown')
        run, identity, activation = self.source()
        self.assertEqual(activation['assignment_prompt'], 'Immutable accepted assignment')
        self.assertEqual(activation['tasks'][0]['task_id'], identity['task'])
        self.assertEqual(run['decisions'][0]['decision_id'], 'original-decision')
        from harflow_mcp.workflow_execution_policy import attempts_used
        self.assertEqual(attempts_used(run, self.node, activation['token']), 1)
        self.assertEqual(run['poststart']['ledger']['remaining_attempts'], 2)
        await self.settle(job)

    async def test_accepted_normal_result_wins_and_updates_ledger_in_same_replace(self):
        job = await self.launch()
        await self.settle(job)
        run, identity, activation = self.source()
        from poststart_control_fixtures import digest
        self.assertEqual(activation['node_result'], result())
        self.assertEqual(run['poststart']['ledger']['result'], digest(result()))
        before = copy.deepcopy(run['poststart'])
        with self.assertRaises(ValueError):
            self.coordinator.contain(self.contain)
        self.assertEqual(self.supervisor.run()['poststart'], before)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])

    async def test_containment_fences_late_task_and_actual_finish_result(self):
        from harflow_mcp import workflow_delegation as delegation
        job = await self.launch()
        self.coordinator.contain(self.contain)
        before, identity, activation = self.source()
        self.supervisor._task_update(identity['activation'], identity['task'],
                                     {'status': 'completed', 'result': outcome('late task callback')})
        self.storage.update_run(self.supervisor.run_id, lambda run: delegation.finish_result(
            run, self.node, self.token['id'], identity['activation'], result('late accepted output'), outcome()),
            'node_result_ready', identity['activation'])
        after = self.supervisor.run()
        self.assertEqual(after['activations'], before['activations'])
        self.assertEqual(after['pending'], before['pending'])
        self.assertEqual(after['tasks'], before['tasks'])
        self.assertIsNone(await self.supervisor._dispatch(self.node, 'No-op task callback grants no issue', 'node', activation))
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        await self.settle(job)
        self.assertIsNone(self.supervisor.run()['poststart']['ledger']['result'])

    async def test_real_normal_grant_is_authoritative_and_stales_original_profile(self):
        job = await self.launch()
        self.storage.update_run(self.supervisor.run_id,
                                lambda run: run['attempt_grants'].update(work=1), 'fixture_normal_grant')
        run = self.supervisor.run()
        self.assertEqual(run['attempt_grants']['work'], 1)
        self.assertTrue(run['poststart']['stale'])
        with self.assertRaises(ValueError):
            self.coordinator.contain(self.contain)
        self.assertIsNone(self.supervisor.run()['poststart']['ledger']['intent'])
        await self.settle(job)

    async def test_production_facade_rejects_real_persisted_enrollment_before_effects(self):
        from harflow_mcp.poststart_control import poststart_control
        job = await self.launch()
        effects = copy.deepcopy(self.adapter.effects)
        with self.assertRaises(ValueError):
            poststart_control(self.supervisor.run_id, self.contain, root=self.storage.root)
        self.assertEqual(self.adapter.effects, effects)
        self.assertEqual(self.registry.calls, [])
        await self.settle(job)

    async def test_fresh_supervisor_without_scope_never_reissues_original(self):
        from harflow_mcp import workflows as w
        job = await self.launch()
        run, identity, activation = self.source()
        fresh = w.WorkflowSupervisor(self.registry, w.WorkflowStore(self.storage.root))
        fresh.run_id = self.supervisor.run_id
        effects = copy.deepcopy(self.adapter.effects)
        try:
            await fresh._dispatch(self.node, 'Persisted synthetic facts cannot grant issuance', 'node', activation)
        except ValueError:
            pass
        self.assertEqual(self.registry.calls, [])
        self.assertEqual(self.adapter.effects, effects)
        await self.settle(job)

    async def test_pending_intent_rejects_real_source_mutations_atomically(self):
        job = await self.launch()
        self.coordinator.contain(self.contain)
        _, identity, _ = self.source()
        def activation(run):
            return next(a for a in run['activations'] if a['id'] == identity['activation'])
        mutations = [
            lambda run: activation(run).update(assignment_prompt='Changed assignment'),
            lambda run: activation(run)['tasks'][0]['candidate'].update(model='different-model'),
            lambda run: activation(run)['token'].update(id='different-visit'),
            lambda run: run.update(goal='Changed goal'),
            lambda run: run.update(freedom='unrestricted'),
            lambda run: run['attempt_grants'].update(work=10),
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                before = (self.storage.runs / (self.supervisor.run_id + '.json')).read_bytes()
                with self.assertRaises(ValueError):
                    self.storage.update_run(self.supervisor.run_id, mutation, 'ordinary_mutation')
                self.assertEqual((self.storage.runs / (self.supervisor.run_id + '.json')).read_bytes(), before)
        await self.settle(job)

    async def test_stop_claim_is_durable_before_effect_and_restart_loses_handoff(self):
        job = await self.launch()
        self.coordinator.contain(self.contain)
        handoff = self.coordinator.prepare_stop()
        reopened = coordinator(self.storage, self.supervisor, self.scope)
        with self.assertRaises(ValueError):
            reopened.deliver(handoff)
        effects = copy.deepcopy(self.adapter.effects)
        with self.assertRaises(ValueError):
            reopened.prepare_stop()
        self.assertEqual(self.adapter.effects, effects)
        at_effect = []
        self.adapter.before_effect = lambda: at_effect.append(self.coordinator.snapshot()['commands']['stop']['status'])
        receipt = self.coordinator.deliver(handoff)
        self.assertEqual(at_effect, ['unknown'])
        self.coordinator.acknowledge(receipt['command_id'], receipt)
        with self.assertRaises(ValueError):
            self.coordinator.deliver(handoff)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop'])
        await self.settle(job)

    async def test_lost_stop_ack_remains_unknown_and_cannot_mint_new_delivery(self):
        job = await self.launch()
        self.coordinator.contain(self.contain)
        handoff = self.coordinator.prepare_stop()
        self.adapter.lost_ack = True
        with self.assertRaises(RuntimeError):
            self.coordinator.deliver(handoff)
        self.assertEqual(self.coordinator.snapshot()['commands']['stop']['status'], 'unknown')
        reopened = coordinator(self.storage, self.supervisor, self.scope)
        for operation in (reopened.prepare_stop, lambda: reopened.deliver(handoff)):
            with self.assertRaises(ValueError):
                operation()
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop'])
        await self.settle(job)

    async def test_every_proof_and_obligation_is_required_and_whole_job_done_is_independent(self):
        from poststart_control_fixtures import PROOFS
        job = await self.launch()
        self.coordinator.contain(self.contain)
        handoff = self.coordinator.prepare_stop()
        receipt = self.coordinator.deliver(handoff)
        self.coordinator.acknowledge(receipt['command_id'], receipt)
        # A complete observer document cannot certify the still-running outer job.
        self.coordinator.observe(self.certificate)
        with self.assertRaises(ValueError):
            self.coordinator.certify()
        self.assertFalse(job.done())
        await self.settle(job)
        for proof in PROOFS:
            with self.subTest(proof=proof):
                certificate = copy.deepcopy(self.certificate)
                certificate['proofs'][proof] = False
                self.coordinator.observe(certificate)
                with self.assertRaises(ValueError):
                    self.coordinator.certify()
                self.assertIsNone(self.coordinator.snapshot()['certificate'])
        self.coordinator.observe(self.certificate)
        self.assertIsInstance(self.coordinator.certify(), str)
        self.assertEqual(self.coordinator.snapshot()['phase'], 'ready_to_replan')
        # Readiness is a report. It has no successor or provider side effect.
        self.assertIsNone(self.coordinator.snapshot()['replacement'])
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop'])

    async def test_simultaneous_actual_result_and_containment_have_one_checkpoint_winner(self):
        from harflow_mcp import workflow_delegation as delegation
        job = await self.launch()
        _, identity, _ = self.source()
        barrier = threading.Barrier(2)
        accepted, errors = [], []
        def contain():
            barrier.wait()
            try:
                accepted.append(self.coordinator.contain(self.contain))
            except ValueError as error:
                errors.append(error)
        def finish():
            barrier.wait()
            self.storage.update_run(self.supervisor.run_id, lambda run: delegation.finish_result(
                run, self.node, self.token['id'], identity['activation'], result(), outcome()),
                'node_result_ready', identity['activation'])
        await asyncio.gather(asyncio.to_thread(contain), asyncio.to_thread(finish))
        ledger = self.coordinator.snapshot()
        self.assertNotEqual(ledger['intent'] is None, ledger['result'] is None)
        self.assertEqual(len(accepted), int(ledger['intent'] is not None))
        self.assertEqual(len(errors), int(ledger['result'] is not None))
        await self.settle(job)
        self.assertEqual([effect['kind'] for effect in self.adapter.effects], ['original'])

    async def test_concurrent_exact_containment_replays_one_intent_and_receipt(self):
        job = await self.launch()
        barrier = threading.Barrier(2)
        def contain():
            barrier.wait()
            return self.coordinator.contain(copy.deepcopy(self.contain))
        receipts = await asyncio.gather(asyncio.to_thread(contain), asyncio.to_thread(contain))
        self.assertEqual(receipts[0], receipts[1])
        ledger = self.coordinator.snapshot()
        self.assertEqual(list(ledger['receipts']), [self.contain['request_id']])
        self.assertEqual(ledger['control_issuances'], 0)
        self.assertEqual(ledger['remaining_attempts'], 2)
        self.assertEqual([effect['kind'] for effect in self.adapter.effects], ['original'])
        await self.settle(job)

    async def test_before_containment_atomic_replace_cut_has_no_intent_or_effect(self):
        from unittest.mock import patch
        job = await self.launch()
        before = copy.deepcopy(self.coordinator.snapshot())
        def cut(*args, **kwargs):
            raise OSError('injected before real run replacement')
        with patch.object(self.storage, '_replace_run', side_effect=cut):
            with self.assertRaises(OSError):
                self.coordinator.contain(self.contain)
        self.assertEqual(self.coordinator.snapshot(), before)
        self.assertEqual([effect['kind'] for effect in self.adapter.effects], ['original'])
        await self.settle(job)

    async def test_after_containment_atomic_replace_ack_loss_has_no_delivery_capability(self):
        from unittest.mock import patch
        job = await self.launch()
        replace = self.storage._replace_run
        def cut(*args, **kwargs):
            replace(*args, **kwargs)
            raise OSError('injected acknowledgement loss after real run replacement')
        with patch.object(self.storage, '_replace_run', side_effect=cut):
            with self.assertRaises(OSError):
                self.coordinator.contain(self.contain)
        # Fresh decoding observes the actual atomic publication, never a cached mirror.
        from harflow_mcp import workflows as w
        fresh = w.WorkflowStore(self.storage.root).get_run(self.supervisor.run_id, refresh_atomic=True)
        self.assertEqual(fresh['poststart']['ledger']['intent'], self.contain)
        self.assertEqual(fresh['poststart']['ledger']['commands'], {})
        self.assertEqual([effect['kind'] for effect in self.adapter.effects], ['original'])
        await self.settle(job)

    async def ready(self):
        job = await self.launch()
        self.coordinator.contain(self.contain)
        handoff = self.coordinator.prepare_stop()
        receipt = self.coordinator.deliver(handoff)
        self.coordinator.acknowledge(receipt['command_id'], receipt)
        await self.settle(job)
        self.coordinator.observe(self.certificate)
        self.coordinator.certify()
        self.coordinator.release()
        return job

    async def successor_job(self):
        from harflow_mcp.replan_retention import new_outer_job
        token = copy.deepcopy(self.supervisor.run()['pending'][0])
        coro, done = new_outer_job(self.supervisor, token,
                                  {'pid': 12345, 'start_time': 'fixture-successor-dispatcher', 'markers': []})
        job = asyncio.create_task(coro)
        done.bind(job)
        job.add_done_callback(done)
        self.jobs.append(job)
        await asyncio.wait_for(job, 5)
        return job

    async def test_replacement_uses_actual_visit_allowance_once_and_real_dispatch_once(self):
        await self.ready()
        original_run, original_identity, original = self.source()
        accepted_decision = copy.deepcopy(original_run['decisions'])
        receipt = self.coordinator.replace(self.replacement)
        self.assertEqual(self.coordinator.replace(copy.deepcopy(self.replacement)), receipt)
        run = self.supervisor.run()
        successor = next(a for a in run['activations'] if a['id'] == receipt['activation_id'])
        self.assertEqual(successor['planned_task_id'], receipt['task_id'])
        self.assertEqual(successor['poststart_supersedes_execution_id'], original_identity['activation'])
        self.assertEqual(successor['assignment_prompt'], original['assignment_prompt'])
        self.assertEqual(run['pending'][0]['execution_activation_id'], receipt['activation_id'])
        self.assertEqual(run['decisions'], accepted_decision)
        from harflow_mcp.workflow_execution_policy import attempts_used
        self.assertEqual(attempts_used(run, self.node, original['token']), 2)
        self.assertEqual(run['poststart']['ledger']['remaining_attempts'], 1)
        self.assertEqual(run['poststart']['ledger']['control_issuances'], 1)
        await self.successor_job()
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop', 'dispatch'])
        self.assertEqual(self.registry.calls, [])
        final = self.supervisor.run()
        self.assertEqual(attempts_used(final, self.node, original['token']), 2)
        self.assertEqual(len([a for a in final['activations'] if a['role'] == 'node']), 2)

    async def test_replacement_commit_before_cut_keeps_allowance_and_tasks_unchanged(self):
        from unittest.mock import patch
        await self.ready()
        before = self.supervisor.run()
        with patch.object(self.storage, '_replace_run', side_effect=OSError('before replacement CAS')):
            with self.assertRaises(OSError):
                self.coordinator.replace(self.replacement)
        after = self.supervisor.run(refresh_atomic=True)
        self.assertEqual(after['activations'], before['activations'])
        self.assertEqual(after['poststart'], before['poststart'])
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop'])

    async def test_replacement_committed_ack_loss_replays_exact_lineage_without_charge(self):
        from unittest.mock import patch
        await self.ready()
        replace = self.storage._replace_run
        def cut(*args, **kwargs):
            replace(*args, **kwargs)
            raise OSError('after replacement CAS acknowledgement loss')
        with patch.object(self.storage, '_replace_run', side_effect=cut):
            with self.assertRaises(OSError):
                self.coordinator.replace(self.replacement)
        run = self.supervisor.run(refresh_atomic=True)
        receipt = copy.deepcopy(run['poststart']['ledger']['replacement'])
        self.assertIsNotNone(receipt)
        self.assertEqual(self.coordinator.replace(self.replacement), receipt)
        after = self.supervisor.run()
        self.assertEqual(after['activations'], run['activations'])
        self.assertEqual(after['poststart']['ledger']['control_issuances'], 1)
        self.assertEqual(after['poststart']['ledger']['remaining_attempts'], 1)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop'])

    def resource_workflow(self):
        definition = graph()
        definition['nodes'][1]['resources'] = ['checkout:fixture']
        self.storage, self.registry, self.supervisor, self.node, self.token = create_workflow(
            self.root, definition=definition)

    async def test_real_workflow_resource_claim_survives_generic_cleanup_and_recovery(self):
        from harflow_mcp import workflow_resources as wr
        self.resource_workflow()
        job = await self.launch()
        self.coordinator.contain(self.contain)
        before, identity, activation = self.source()
        original = copy.deepcopy(activation['resource_attempt'])
        ledger = resources.ResourceLedger(self.storage.root)
        entry = ledger.get(original['reservation_id'])
        self.assertEqual(entry.owner.activation_id, original['id'])
        waiter = ledger.request(['checkout:fixture'], resources.Owner('task', task_id='queued-conflict'))
        self.assertFalse(waiter.held)
        for operation in (lambda: wr.finish_attempt(self.supervisor, activation['id'], original),
                          lambda: wr.abandon_attempt(self.supervisor, activation['id'], original),
                          lambda: wr.reconcile(self.storage, self.supervisor.run(), owner_dead=True)):
            try:
                operation()
            except ValueError:
                pass  # A controlled refusal is safe; releasing this exact grant is not.
            self.assertEqual(ledger.get(original['reservation_id']), entry)
        self.assertEqual(ledger.get(original['reservation_id']), entry)
        self.assertFalse(ledger.poll(waiter.reservation_id).held)
        await self.settle(job)
        self.assertEqual(ledger.get(original['reservation_id']), entry)
        self.assertFalse(ledger.poll(waiter.reservation_id).held)

    async def test_real_resource_old_coordinator_successor_transfer_has_no_grant_gap(self):
        from harflow_mcp import workflow_resources as wr
        self.resource_workflow()
        job = await self.launch()
        original_run, identity, activation = self.source()
        original = copy.deepcopy(activation['resource_attempt'])
        ledger = resources.ResourceLedger(self.storage.root)
        old_entry = ledger.get(original['reservation_id'])
        waiter = ledger.request(['checkout:fixture'], resources.Owner('task', task_id='queued-conflict'))
        self.coordinator.contain(self.contain)
        handoff = self.coordinator.prepare_stop()
        receipt = self.coordinator.deliver(handoff)
        self.coordinator.acknowledge(receipt['command_id'], receipt)
        await self.settle(job)
        self.coordinator.observe(self.certificate)
        self.coordinator.certify()
        self.coordinator.release()
        run = self.supervisor.run()
        journal = run['poststart']['runtime']['resource_handoff']
        coordinator_entry = ledger.get(original['reservation_id'])
        self.assertEqual(journal['state'], 'coordinator_held')
        self.assertEqual(coordinator_entry.owner.activation_id, journal['coordinator_attempt_id'])
        self.assertEqual(coordinator_entry.reservation_id, old_entry.reservation_id)
        self.assertFalse(ledger.poll(waiter.reservation_id).held)
        # Repair evidence must name the actual coordinator, never reconstruct old owner.
        evidence = wr.collect_evidence(self.registry.log_dir)
        matching = [e for e in evidence if e['reservation_id'] == original['reservation_id']]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0]['owner']['activation_id'], journal['coordinator_attempt_id'])
        self.coordinator.replace(self.replacement)
        # Inspect the leaf grant at the actual successor issue boundary.
        observed = []
        self.adapter.before_effect = lambda: observed.append(ledger.get(original['reservation_id']))
        await self.successor_job()
        self.assertEqual(len(observed), 1)
        self.assertTrue(observed[0].held)
        self.assertNotEqual(observed[0].owner.activation_id, old_entry.owner.activation_id)
        self.assertNotEqual(observed[0].owner.activation_id, coordinator_entry.owner.activation_id)
        self.assertEqual(observed[0].reservation_id, old_entry.reservation_id)
        self.assertTrue(ledger.poll(waiter.reservation_id).held)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop', 'dispatch'])

    async def test_fixture_scope_never_substitutes_for_real_caller_authority(self):
        job = await self.launch()
        original = copy.deepcopy(self.scope.principal)
        for principal in [
            {'kind': 'external_parent', 'identity': {'pid': 99999, 'start_time': 'different-caller', 'markers': []}},
            {'kind': 'managed_task', 'identity': original['identity'], 'task_id': 'self-worker', 'session_id': None},
            {'kind': 'unknown', 'identity': None},
        ]:
            with self.subTest(principal=principal):
                self.scope.principal = principal
                with self.assertRaises(ValueError):
                    self.coordinator.contain(self.contain)
                self.assertIsNone(self.coordinator.snapshot()['intent'])
                self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        self.scope.principal = original
        from unittest.mock import patch
        from harflow_mcp import workflow_interactions
        with patch.dict('os.environ', {'HARFLOW_TASK_ID': 'fixture-managed-caller'}):
            with self.assertRaises(ValueError):
                self.coordinator.contain(self.contain)
        changed_transport = {'kind': 'external_parent', 'identity': {'pid': 54321, 'start_time': 'new-transport', 'markers': []}}
        with patch.object(workflow_interactions, 'capture_transport_parent', return_value=changed_transport):
            with self.assertRaises(ValueError):
                self.coordinator.contain(self.contain)
        self.assertIsNone(self.coordinator.snapshot()['intent'])
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        await self.settle(job)

    async def test_refused_capacity_preserves_exact_accepted_replay(self):
        job = await self.launch()
        receipt = self.coordinator.contain(self.contain)
        before = copy.deepcopy(self.coordinator.snapshot())
        self.coordinator.store.metadata_limit = 1
        self.assertEqual(self.coordinator.contain(copy.deepcopy(self.contain)), receipt)
        self.assertEqual(self.coordinator.snapshot(), before)
        changed = copy.deepcopy(self.contain)
        changed['reason'] = 'Different payload under accepted request id'
        with self.assertRaises(ValueError):
            self.coordinator.contain(changed)
        self.assertEqual(self.coordinator.snapshot(), before)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        await self.settle(job)

    async def test_bad_certificate_inventory_profile_and_scope_cannot_certify(self):
        job = await self.launch()
        self.coordinator.contain(self.contain)
        handoff = self.coordinator.prepare_stop()
        receipt = self.coordinator.deliver(handoff)
        self.coordinator.acknowledge(receipt['command_id'], receipt)
        await self.settle(job)
        cases = []
        for field, value in [('activation', 'unrelated-activation'), ('task', 'unrelated-task'),
                             ('profile_digest', 'c' * 64), ('goal_revision', 1)]:
            certificate = copy.deepcopy(self.certificate)
            certificate['identity'][field] = value
            cases.append(certificate)
        certificate = copy.deepcopy(self.certificate)
        certificate['inventory'][0]['settlement'] = 'pending'
        cases.append(certificate)
        certificate = copy.deepcopy(self.certificate)
        certificate['inventory'][0]['obligation'] = 'unknown'
        cases.append(certificate)
        for certificate in cases:
            with self.subTest(certificate=certificate):
                with self.assertRaises(ValueError):
                    self.coordinator.observe(certificate)
                self.assertIsNone(self.coordinator.snapshot()['certificate'])
                self.assertIsNone(self.coordinator.snapshot()['release'])
                self.assertIsNone(self.coordinator.snapshot()['replacement'])
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop'])

    async def origin_claim_cut(self, after):
        from unittest.mock import patch
        replace = self.storage._replace_run
        cut_once = False
        def cut(run_id, document, source):
            nonlocal cut_once
            poststart = document['run'].get('poststart')
            if poststart and poststart['runtime']['origin_issue']['status'] == 'unknown' and not cut_once:
                cut_once = True
                if after:
                    replace(run_id, document, source)
                raise OSError('original claim atomic publication cut')
            return replace(run_id, document, source)
        with patch.object(self.storage, '_replace_run', side_effect=cut):
            with self.assertRaises(OSError):
                await self.launch()
        self.assertTrue(cut_once)
        run, identity, activation = self.source()
        self.assertEqual(run['poststart']['runtime']['origin_issue']['status'], 'unknown' if after else 'prepared')
        self.assertEqual(self.adapter.effects, [])
        self.assertEqual(self.registry.calls, [])
        # Durable evidence cannot reconstruct the lost original-launch permit.
        with self.assertRaises(ValueError):
            await self.supervisor._dispatch(self.node, 'Do not reconstruct original capability', 'node', activation)
        self.assertEqual(self.adapter.effects, [])
        self.assertEqual(self.registry.calls, [])

    async def test_before_original_issue_claim_cut_never_executes_or_reconstructs(self):
        await self.origin_claim_cut(False)

    async def test_after_original_issue_claim_ack_loss_never_executes_or_reconstructs(self):
        await self.origin_claim_cut(True)

    async def test_resource_transfer_ack_loss_replays_exact_leaf_owner_without_gap(self):
        from unittest.mock import patch
        self.resource_workflow()
        job = await self.launch()
        _, identity, activation = self.source()
        original = copy.deepcopy(activation['resource_attempt'])
        ledger = resources.ResourceLedger(self.storage.root)
        waiter = ledger.request(['checkout:fixture'], resources.Owner('task', task_id='queued-conflict'))
        self.coordinator.contain(self.contain)
        handoff = self.coordinator.prepare_stop()
        receipt = self.coordinator.deliver(handoff)
        self.coordinator.acknowledge(receipt['command_id'], receipt)
        await self.settle(job)
        self.coordinator.observe(self.certificate)
        self.coordinator.certify()
        replace = self.storage._replace_run
        cut_once = False
        def cut(run_id, document, source):
            nonlocal cut_once
            journal = document['run']['poststart']['runtime']['resource_handoff']
            if journal and journal['state'] == 'coordinator_held' and not cut_once:
                cut_once = True
                raise OSError('lost exact leaf-transfer acknowledgement')
            return replace(run_id, document, source)
        with patch.object(self.storage, '_replace_run', side_effect=cut):
            with self.assertRaises(OSError):
                self.coordinator.release()
        run = self.supervisor.run(refresh_atomic=True)
        journal = run['poststart']['runtime']['resource_handoff']
        self.assertEqual(journal['state'], 'coordinator_intent')
        entry = ledger.get(original['reservation_id'])
        self.assertEqual(entry.owner.activation_id, journal['coordinator_attempt_id'])
        self.assertFalse(ledger.poll(waiter.reservation_id).held)
        self.coordinator.release()
        self.assertEqual(ledger.get(original['reservation_id']), entry)
        self.assertEqual(self.supervisor.run()['poststart']['runtime']['resource_handoff']['state'], 'coordinator_held')
        self.assertFalse(ledger.poll(waiter.reservation_id).held)

    async def test_original_effect_ack_loss_retains_one_identity_and_no_reissue(self):
        self.adapter.lost_ack = True
        with self.assertRaises(RuntimeError):
            await self.launch()
        run, identity, activation = self.source()
        self.assertEqual(run['poststart']['runtime']['origin_issue']['status'], 'unknown')
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        self.assertEqual(self.adapter.effects[0]['task_id'], identity['task'])
        with self.assertRaises(ValueError):
            await self.supervisor._dispatch(self.node, 'Lost acknowledgement never permits reissue', 'node', activation)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        self.assertEqual(self.registry.calls, [])

    async def test_old_callbacks_cannot_modify_actual_successor_result_or_attention(self):
        from harflow_mcp import workflow_delegation as delegation
        await self.ready()
        _, old_identity, old_activation = self.source()
        self.coordinator.replace(self.replacement)
        await self.successor_job()
        before = self.supervisor.run()
        self.supervisor._task_update(old_identity['activation'], old_identity['task'],
                                     {'status': 'failed', 'result': outcome('late original failure')})
        self.storage.update_run(self.supervisor.run_id, lambda run: delegation.finish_result(
            run, self.node, self.token['id'], old_identity['activation'], result('late original result'), outcome()),
            'node_result_ready', old_identity['activation'])
        after = self.supervisor.run()
        self.assertEqual(after['activations'], before['activations'])
        self.assertEqual(after['pending'], before['pending'])
        self.assertEqual(after.get('attention_reason'), before.get('attention_reason'))
        self.assertEqual(after['sessions'], before['sessions'])
        self.assertEqual(after['poststart'], before['poststart'])
        self.assertIsNone(await self.supervisor._dispatch(self.node, 'Late old-job final issuance', 'node', old_activation))
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop', 'dispatch'])
        self.assertEqual(self.registry.calls, [])

    async def test_entire_outer_job_tail_must_finish_after_dispatch_and_node_return(self):
        reached_tail, release_tail = asyncio.Event(), asyncio.Event()
        actual_node = self.supervisor._node
        async def node_with_owned_tail(token):
            await actual_node(token)
            reached_tail.set()
            await release_tail.wait()
        # Additive boundary instrumentation: all actual node/checkpoint paths run.
        self.supervisor._node = node_with_owned_tail
        job = await self.launch()
        self.coordinator.contain(self.contain)
        handoff = self.coordinator.prepare_stop()
        receipt = self.coordinator.deliver(handoff)
        self.coordinator.acknowledge(receipt['command_id'], receipt)
        self.coordinator.observe(self.certificate)
        self.adapter.settled.set()
        await asyncio.wait_for(reached_tail.wait(), 5)
        self.assertFalse(job.done())
        self.assertFalse(self.supervisor.run()['poststart']['runtime']['job']['done'])
        with self.assertRaises(ValueError):
            self.coordinator.certify()
        with self.assertRaises(ValueError):
            self.coordinator.release()
        self.assertIsNone(self.coordinator.snapshot()['replacement'])
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop'])
        release_tail.set()
        await self.settle(job)
        self.assertTrue(self.supervisor.run()['poststart']['runtime']['job']['done'])
        self.assertIsInstance(self.coordinator.certify(), str)

    async def test_active_independent_sibling_result_is_accepted_while_source_is_held(self):
        from harflow_mcp import workflow_delegation as delegation
        definition = graph()
        definition['nodes'].insert(2, dict(id='side', type='agent', role='task',
            instructions='Independent branch', agent={'backend': 'codex'}))
        definition['connections'] += [dict(id='side-begin', source='start', target='side'),
                                      dict(id='side-end', source='side', target='end')]
        self.storage, self.registry, self.supervisor, self.node, self.token = create_workflow(
            self.root, definition=definition)
        side_node, side_token, side = active_sibling(self.storage, self.supervisor)
        job = await self.launch()
        self.coordinator.contain(self.contain)
        _, identity, original = self.source()
        self.supervisor._task_update(side['id'], 'independent-side-task',
                                     {'status': 'completed', 'result': outcome('Independent sibling result')})
        self.storage.update_run(self.supervisor.run_id, lambda run: delegation.finish_result(
            run, side_node, side_token['id'], side['id'], result('Independent sibling result'), outcome()),
            'node_result_ready', side['id'])
        run, _, after_original = self.source()
        self.assertEqual(after_original, original)
        after_side = next(a for a in run['activations'] if a['id'] == side['id'])
        self.assertEqual(after_side['node_result'], result('Independent sibling result'))
        self.assertTrue(next(t for t in run['pending'] if t['id'] == side_token['id'])['execution_complete'])
        self.assertIsNone(run['poststart']['ledger']['result'])
        await self.settle(job)
        self.assertEqual(next(a for a in self.supervisor.run()['activations'] if a['id'] == side['id'])['node_result'],
                         result('Independent sibling result'))

    async def test_lent_real_resource_domain_refuses_before_any_stop_effect(self):
        self.resource_workflow()
        job = await self.launch()
        _, identity, activation = self.source()
        original = activation['resource_attempt']
        ledger = resources.ResourceLedger(self.storage.root)
        borrower = ledger.request(['checkout:fixture'], resources.Owner('task', task_id='incidental-borrower'),
                                  scope=resources.Scope(original['reservation_id']))
        self.assertTrue(borrower.held)
        with self.assertRaises(ValueError):
            self.coordinator.contain(self.contain)
            handoff = self.coordinator.prepare_stop()
            self.coordinator.deliver(handoff)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        self.assertIsNotNone(ledger.get(original['reservation_id']))
        self.assertIsNotNone(ledger.get(borrower.reservation_id))
        await self.settle(job)

    async def test_replacement_rechecks_real_coordinator_claim_owner_before_charging(self):
        self.resource_workflow()
        await self.ready()
        run, _, activation = self.source()
        journal = run['poststart']['runtime']['resource_handoff']
        ledger = resources.ResourceLedger(self.storage.root)
        entry = ledger.get(journal['reservation_id'])
        changed = ledger.transfer_owned_workflow_attempt(entry.reservation_id, self.supervisor.run_id,
            entry.owner.activation_id, 'unrelated-resource-owner', expected_names=entry.names,
            expected_generation=entry.generation)
        before = self.supervisor.run()
        with self.assertRaises(ValueError):
            self.coordinator.replace(self.replacement)
        after = self.supervisor.run()
        self.assertEqual(after['activations'], before['activations'])
        self.assertEqual(after['poststart']['ledger']['remaining_attempts'], 2)
        self.assertEqual(after['poststart']['ledger']['control_issuances'], 0)
        self.assertEqual(ledger.get(entry.reservation_id), changed)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop'])

    async def test_containment_winning_at_original_atomic_effect_boundary_blocks_issue(self):
        requests = []
        def contain_before_original_effect():
            bound = coordinator(self.storage, self.supervisor, self.scope)
            contain, _, _ = control_documents(self.supervisor.run())
            requests.append(bound.contain(contain))
        self.adapter.before_effect = contain_before_original_effect
        with self.assertRaises(RuntimeError):
            await self.launch()
        run, identity, activation = self.source()
        self.assertEqual(len(requests), 1)
        self.assertIsNotNone(run['poststart']['ledger']['intent'])
        self.assertEqual(run['poststart']['runtime']['origin_issue']['status'], 'unknown')
        self.assertEqual(self.adapter.effects, [])
        self.assertEqual(self.registry.calls, [])
        self.assertEqual(run['poststart']['ledger']['remaining_attempts'], 2)

    async def test_actual_supervisor_restart_cannot_dispatch_from_readiness(self):
        from harflow_mcp import workflows as w
        await self.ready()
        before = self.supervisor.run()
        effects = copy.deepcopy(self.adapter.effects)
        claims = resources.ResourceLedger(self.storage.root).entries()
        fresh_store = w.WorkflowStore(self.storage.root)
        fresh = w.WorkflowSupervisor(self.registry, fresh_store)
        from harflow_mcp import workflow_poststart as poststart
        from unittest.mock import patch
        reached = asyncio.Event()
        actual_guard = poststart.blocked_token
        def observe_actual_guard(run, token):
            blocked = actual_guard(run, token)
            if run['workflow_run_id'] == self.supervisor.run_id and blocked:
                reached.set()
            return blocked
        with patch.object(poststart, 'blocked_token', side_effect=observe_actual_guard):
            restart = asyncio.create_task(fresh.execute(self.supervisor.run_id))
            self.jobs.append(restart)
            observed = asyncio.create_task(reached.wait())
            completed, _ = await asyncio.wait([restart, observed], timeout=5, return_when=asyncio.FIRST_COMPLETED)
            if restart in completed:
                try:
                    await restart
                except ValueError:
                    pass
            else:
                self.assertTrue(observed.done(), 'actual scheduler never inspected the held token')
                restart.cancel()
                await asyncio.gather(restart, return_exceptions=True)
            observed.cancel()
        after = fresh_store.get_run(self.supervisor.run_id, refresh_atomic=True)
        self.assertEqual(self.adapter.effects, effects)
        self.assertEqual(self.registry.calls, [])
        self.assertEqual(resources.ResourceLedger(self.storage.root).entries(), claims)
        self.assertEqual(after['activations'], before['activations'])
        self.assertIsNone(after['poststart']['ledger']['replacement'])
        self.assertEqual(after['poststart']['ledger']['control_issuances'], 0)
        self.assertEqual(after['poststart']['ledger']['remaining_attempts'], 2)

    async def test_actual_join_and_decision_entrypoints_cannot_advance_held_branch(self):
        from harflow_mcp import workflow_delegation as delegation
        definition = graph()
        definition['routing_mode'] = 'explicit'
        definition['nodes'].insert(1, dict(id='fork', type='parallel_start', parallel_group_id='paired-group'))
        definition['nodes'].insert(3, dict(id='side', type='agent', role='task', instructions='Independent branch', agent={'backend': 'codex'}))
        definition['nodes'].insert(4, dict(id='join', type='parallel_end', parallel_group_id='paired-group'))
        definition['connections'] = [
            dict(id='begin', source='start', target='fork'),
            dict(id='left', source='fork', target='work'),
            dict(id='right', source='fork', target='side'),
            dict(id='left-arrival', source='work', target='join'),
            dict(id='right-arrival', source='side', target='join'),
            dict(id='finish', source='join', target='end')]
        self.storage, self.registry, self.supervisor, self.node, self.token = create_workflow(self.root, definition=definition)
        def split(run):
            run['pending'][0].update(stack=['parallel-generation'], branch_ids={'parallel-generation': 'left'})
            run['joins']['parallel-generation'] = dict(join_id='join', split_id='fork', expected=2,
                stack=[], arrived=[], arrival_ids=[], selected_connection_ids=['left', 'right'],
                branch_states={'left': 'active', 'right': 'active'})
        self.supervisor.update(split, 'fixture_selected_parallel_split')
        self.token = copy.deepcopy(self.supervisor.run()['pending'][0])
        job = await self.launch()
        self.coordinator.contain(self.contain)
        before = self.supervisor.run()
        decision_activation = next(a for a in before['activations'] if a['role'] == 'orchestrator')
        with self.assertRaises(ValueError):
            self.storage.update_run(self.supervisor.run_id, lambda run: delegation.accept_decision_locked(
                self.supervisor, run, self.node, self.token, decision_activation,
                {'action': 'complete'}, False, 'Late root completion', []), 'late_decision_acceptance')
        # A captured outer continuation reaching the real join cannot contribute.
        captured_arrival = {**self.token, 'node_id': 'join'}
        self.assertTrue(self.supervisor._arrive_join(captured_arrival))
        await self.supervisor._legacy_node(self.token)
        self.storage.update_run(self.supervisor.run_id, delegation.reconcile_delegation, 'fixture_reconcile_delegation')
        after = self.supervisor.run()
        for key in ('joins', 'pending', 'tasks', 'decisions', 'activations'):
            self.assertEqual(after[key], before[key])
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        await self.settle(job)

    def steering_workflow(self, *, side=False):
        from harflow_mcp.steering_window import upgrade
        definition = graph()
        definition['orchestrator']['backend'] = 'claude'
        definition['nodes'][1]['agent']['backend'] = 'claude'
        if side:
            definition['nodes'].insert(2, dict(id='side', type='agent', role='task', instructions='Independent branch', agent={'backend': 'claude'}))
            definition['connections'] += [dict(id='side-begin', source='start', target='side'),
                                          dict(id='side-end', source='side', target='end')]
        self.storage, self.registry, self.supervisor, self.node, self.token = create_workflow(self.root, definition=definition)
        self.supervisor.update(upgrade, 'fixture_explicit_steering_upgrade')

    async def test_actual_ordinary_worker_steering_before_intent_stales_original_qualification(self):
        self.steering_workflow()
        job = await self.launch()
        _, identity, _ = self.source()
        receipt, scheduled = admit_worker_turn(self.storage, self.supervisor, self.principal,
                                               identity['activation'], self.token['id'])
        self.assertTrue(receipt['accepted'])
        self.assertTrue(scheduled)
        run = self.supervisor.run()
        self.assertTrue(run['poststart']['stale'])
        self.assertFalse(run['poststart']['ledger']['context']['enrolled'])
        self.assertEqual(len(run['steering']['turns']), 1)
        with self.assertRaises(ValueError):
            self.coordinator.contain(self.contain)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        await self.settle(job)

    async def test_affected_actual_steering_admission_is_fenced_even_under_arbitrary_event_names(self):
        self.steering_workflow()
        job = await self.launch()
        self.coordinator.contain(self.contain)
        before, identity, _ = self.source()
        for event in ('ordinary_unfamiliar_event', 'poststart:not_a_steering_exemption'):
            with self.subTest(event=event):
                with self.assertRaises(ValueError):
                    admit_worker_turn(self.storage, self.supervisor, self.principal,
                                      identity['activation'], self.token['id'], event=event)
                after = self.supervisor.run()
                self.assertEqual(after['steering'], before['steering'])
                self.assertEqual(after['activations'], before['activations'])
                self.assertEqual(after['pending'], before['pending'])
        await self.settle(job)

    async def test_disjoint_actual_worker_steering_retains_ordinary_admission(self):
        self.steering_workflow(side=True)
        side_node, side_token, side = active_sibling(self.storage, self.supervisor)
        job = await self.launch()
        self.coordinator.contain(self.contain)
        _, identity, original = self.source()
        receipt, scheduled = admit_worker_turn(self.storage, self.supervisor, self.principal,
            side['id'], side_token['id'], event='ordinary_disjoint_worker_queue')
        self.assertTrue(receipt['accepted'])
        self.assertTrue(scheduled)
        run, _, after_original = self.source()
        self.assertEqual(after_original, original)
        self.assertEqual(run['steering']['turns'][0]['target']['activation_id'], side['id'])
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        await self.settle(job)

    def test_stock_task_registry_and_subclasses_cannot_enter_fixture_scope(self):
        from harflow_mcp.tasks import TaskRegistry
        from harflow_mcp.workflow_poststart import FixtureScope
        class FlaggedRegistry(TaskRegistry):
            synthetic = True
        for registry_type in (TaskRegistry, FlaggedRegistry):
            # Construction is intentionally skipped: no registry, process or owner starts.
            registry = object.__new__(registry_type)
            with self.subTest(registry_type=registry_type), self.assertRaises(ValueError):
                FixtureScope(self.storage, self.supervisor.run_id, registry, self.adapter)
        self.assertEqual(self.adapter.effects, [])

    async def test_persisted_task_dispatcher_and_configuration_corruption_refuses_decode(self):
        from harflow_mcp import workflows as w
        job = await self.launch()
        _, identity, _ = self.source()
        path = self.storage.runs / (self.supervisor.run_id + '.json')
        saved = path.read_bytes()
        for field, value in [('dispatcher_job_id', 'unrelated-job'),
                             ('dispatcher_identity', {'pid': 99999, 'start_time': 'wrong-owner', 'markers': []}),
                             ('freedom', 'unrestricted'), ('repo_path', str(self.root / 'different-repo'))]:
            with self.subTest(field=field):
                corrupt = json.loads(saved)
                activation = next(a for a in corrupt['run']['activations'] if a['id'] == identity['activation'])
                activation['tasks'][0][field] = value
                path.write_text(json.dumps(corrupt), encoding='utf-8')
                try:
                    from harflow_mcp.workflow_run_documents import WorkflowRunDocumentError
                    with self.assertRaises(WorkflowRunDocumentError):
                        w.WorkflowStore(self.storage.root).get_run(self.supervisor.run_id, refresh_atomic=True)
                    self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
                    self.assertEqual(self.registry.calls, [])
                finally:
                    path.write_bytes(saved)
        await self.settle(job)

    def overlapping_workflow(self, *, shared):
        definition = graph()
        definition['nodes'].insert(2, dict(id='side', type='agent', role='task',
            instructions='Independent node with bounded checklist assignment', agent={'backend': 'codex'}))
        definition['connections'] += [dict(id='side-begin', source='start', target='side'),
                                      dict(id='side-end', source='side', target='end')]
        self.storage, self.registry, self.supervisor, self.node, self.token = create_workflow(
            self.root, definition=definition, assigned_task_ids=['shared-checklist-item'])
        return active_sibling(self.storage, self.supervisor,
                              assigned_task_ids=['shared-checklist-item'] if shared else [])

    async def test_original_enrollment_refuses_actual_active_overlapping_checklist_scope(self):
        self.overlapping_workflow(shared=True)
        with self.assertRaises(ValueError):
            await self.launch()
        self.assertNotIn('poststart', self.supervisor.run())
        self.assertEqual(self.adapter.effects, [])
        self.assertEqual(self.registry.calls, [])

    async def test_new_actual_sibling_dependency_stales_enrollment_before_containment(self):
        _, side_token, side = self.overlapping_workflow(shared=False)
        job = await self.launch()
        _, identity, _ = self.source()
        def dependency(run):
            activation = next(a for a in run['activations'] if a['id'] == side['id'])
            activation['input_result_refs'] = [identity['activation']]
        self.supervisor.update(dependency, 'ordinary_sibling_dependency')
        run = self.supervisor.run()
        self.assertTrue(run['poststart']['stale'])
        self.assertTrue(run['poststart']['source']['scope_ambiguous'] is False)
        with self.assertRaises(ValueError):
            self.coordinator.contain(self.contain)
        self.assertIsNone(self.coordinator.snapshot()['intent'])
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        await self.settle(job)

    async def dispatch_claim_cut(self, *, after=False, lost_ack=False):
        from unittest.mock import patch
        from harflow_mcp import workflows as w
        await self.ready()
        receipt = self.coordinator.replace(self.replacement)
        replace = self.storage._replace_run
        cut_once = False
        def cut(run_id, document, source):
            nonlocal cut_once
            dispatch = document['run']['poststart']['ledger']['commands'].get('dispatch')
            if dispatch and dispatch['status'] == 'unknown' and not cut_once:
                cut_once = True
                if after:
                    replace(run_id, document, source)
                raise OSError('successor issue claim publication cut')
            return replace(run_id, document, source)
        if lost_ack:
            self.adapter.lost_ack = True
            with self.assertRaises(RuntimeError):
                await self.successor_job()
        else:
            with patch.object(self.storage, '_replace_run', side_effect=cut):
                with self.assertRaises(OSError):
                    await self.successor_job()
            self.assertTrue(cut_once)
        run = w.WorkflowStore(self.storage.root).get_run(self.supervisor.run_id, refresh_atomic=True)
        ledger = run['poststart']['ledger']
        self.assertEqual(ledger['replacement']['task_id'], receipt['task_id'])
        self.assertEqual(ledger['replacement']['activation_id'], receipt['activation_id'])
        self.assertEqual(ledger['remaining_attempts'], 1)
        self.assertEqual(ledger['control_issuances'], 1)
        successor = next(a for a in run['activations'] if a['id'] == receipt['activation_id'])
        self.assertEqual(successor['planned_task_id'], receipt['task_id'])
        self.assertEqual(ledger['commands']['dispatch']['status'], 'unknown' if after or lost_ack else 'prepared')
        from harflow_mcp.workflow_execution_policy import attempts_used
        self.assertEqual(attempts_used(run, self.node, successor['token']), 2)
        expected_effects = ['original', 'stop'] + (['dispatch'] if lost_ack else [])
        self.assertEqual([e['kind'] for e in self.adapter.effects], expected_effects)
        for supervisor in (self.supervisor, w.WorkflowSupervisor(self.registry, w.WorkflowStore(self.storage.root))):
            supervisor.run_id = self.supervisor.run_id
            with self.assertRaises(ValueError):
                await supervisor._dispatch(self.node, 'Never recreate unknown successor delivery', 'node', successor)
        self.assertEqual([e['kind'] for e in self.adapter.effects], expected_effects)
        self.assertEqual(self.registry.calls, [])
        self.assertEqual(len([a for a in self.supervisor.run()['activations'] if a['role'] == 'node']), 2)

    async def test_before_successor_issue_claim_cut_preserves_exact_planned_charge(self):
        await self.dispatch_claim_cut()

    async def test_after_successor_issue_claim_ack_cut_preserves_exact_planned_charge(self):
        await self.dispatch_claim_cut(after=True)

    async def test_lost_successor_effect_ack_restart_never_repeats_or_recharges(self):
        await self.dispatch_claim_cut(lost_ack=True)

    def append_history_pressure(self, run, *, records):
        from harflow_mcp.workflow_steering import _history
        for _ in range(records):
            _history(run, 'caller_note', text='Preserved ordinary caller history. ' + 'x' * 29950)

    async def test_shared_normal_history_pressure_refuses_containment_before_stop(self):
        from harflow_mcp.steering_window import upgrade
        self.supervisor.update(upgrade, 'fixture_explicit_history_upgrade')
        job = await self.launch()
        self.supervisor.update(lambda run: self.append_history_pressure(run, records=24), 'ordinary_caller_history')
        before = self.supervisor.run()
        self.assertEqual(len(before['steering']['history']), 24)
        with self.assertRaises(ValueError):
            self.coordinator.contain(self.contain)
        self.assertIsNone(self.coordinator.snapshot()['intent'])
        self.assertEqual(self.supervisor.run()['steering'], before['steering'])
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        await self.settle(job)

    async def test_shared_completion_reserve_rejects_history_but_keeps_replay_and_observation(self):
        from harflow_mcp.steering_window import upgrade
        self.supervisor.update(upgrade, 'fixture_explicit_history_upgrade')
        job = await self.launch()
        receipt = self.coordinator.contain(self.contain)
        handoff = self.coordinator.prepare_stop()
        effect_receipt = self.coordinator.deliver(handoff)
        self.coordinator.acknowledge(effect_receipt['command_id'], effect_receipt)
        await self.settle(job)
        before = self.supervisor.run()
        with self.assertRaises(ValueError):
            self.supervisor.update(lambda run: self.append_history_pressure(run, records=20), 'unrelated_caller_history')
        self.assertEqual(self.supervisor.run()['steering'], before['steering'])
        self.assertEqual(self.coordinator.contain(copy.deepcopy(self.contain)), receipt)
        # A refused unrelated write must not strand the adapter ahead of the ledger.
        self.coordinator.observe(self.certificate)
        self.assertIsInstance(self.coordinator.certify(), str)
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop'])

    async def test_fresh_store_normal_grant_fences_already_claimed_original_effect(self):
        from harflow_mcp import workflows as w
        mutations = []
        def grant_at_effect_boundary():
            fresh = w.WorkflowStore(self.storage.root)
            fresh.update_run(self.supervisor.run_id, lambda run: run['attempt_grants'].update(work=1),
                             'ordinary_grant_from_independent_store')
            mutations.append(True)
        self.adapter.before_effect = grant_at_effect_boundary
        with self.assertRaises(RuntimeError):
            await self.launch()
        run = self.supervisor.run(refresh_atomic=True)
        self.assertEqual(mutations, [True])
        self.assertEqual(run['attempt_grants']['work'], 1)
        self.assertTrue(run['poststart']['stale'])
        self.assertIsNone(run['poststart']['ledger']['intent'])
        self.assertEqual(self.adapter.effects, [])
        self.assertEqual(self.registry.calls, [])

    async def test_original_enrollment_rechecks_actual_source_after_real_slot_acquisition(self):
        from unittest.mock import patch
        from harflow_mcp import workflow_resources as wr, workflows as w
        import hashlib
        cases = [('execution_mode', 'prefer_subagent'), ('session_mode', 'resume'),
                 ('candidate_model', 'different-model'), ('grant', 1), ('goal', 'Changed goal after slot')]
        for field, value in cases:
            with self.subTest(field=field):
                self.storage, self.registry, self.supervisor, self.node, self.token = create_workflow(self.root)
                self.adapter = AtomicEffects()
                actual_slot = wr.acquire_slot
                changed = []
                async def mutate_after_actual_slot(supervisor, activation, should_continue, **kwargs):
                    await actual_slot(supervisor, activation, should_continue, **kwargs)
                    def mutate(run):
                        node = next(n for n in run['definition']['nodes'] if n['id'] == 'work')
                        if field == 'grant':
                            run['attempt_grants']['work'] = value
                        elif field == 'goal':
                            run.update(goal=value, goal_revision=run.get('goal_revision', 0) + 1)
                        else:
                            if field == 'candidate_model':
                                node['agent']['model'] = value
                            else:
                                node[field] = value
                            run['definition_hash'] = hashlib.sha256(json.dumps(run['definition'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                    w.WorkflowStore(self.storage.root).update_run(self.supervisor.run_id, mutate, 'authorized_source_change_after_slot')
                    changed.append(True)
                with patch.object(wr, 'acquire_slot', side_effect=mutate_after_actual_slot):
                    with self.assertRaises(ValueError):
                        await self.launch()
                run = self.supervisor.run(refresh_atomic=True)
                self.assertEqual(changed, [True])
                self.assertNotIn('poststart', run)
                current_node = next(n for n in run['definition']['nodes'] if n['id'] == 'work')
                actual = (run['attempt_grants']['work'] if field == 'grant' else run['goal'] if field == 'goal'
                          else current_node['agent']['model'] if field == 'candidate_model' else current_node[field])
                self.assertEqual(actual, value)
                self.assertEqual(self.adapter.effects, [])
                self.assertEqual(self.registry.calls, [])

    async def test_persisted_fixture_taint_blocks_uncontrolled_provider_paths_without_scope(self):
        from harflow_mcp import workflows as w
        job = await self.launch()
        uncontrolled = self.supervisor._activation('work', 'orchestrator', copy.deepcopy(self.token))
        from harflow_mcp.tasks import TaskRegistry
        # Stock registry identity with constructor skipped and every effect replaced
        # by the existing raising sentinel; no real TaskRegistry method may execute.
        registry = object.__new__(TaskRegistry)
        registry._log_dir = self.registry.log_dir
        registry.state_layout = self.registry.state_layout
        for method in ('start', 'resume', 'cancel_cascade', 'get', 'attach'):
            setattr(registry, method, getattr(self.registry, method))
        fresh = w.WorkflowSupervisor(registry, w.WorkflowStore(self.storage.root))
        fresh.run_id = self.supervisor.run_id
        before = self.supervisor.run()
        effects = copy.deepcopy(self.adapter.effects)
        claims = resources.ResourceLedger(self.storage.root).entries()
        calls = [lambda: fresh._dispatch({'id': 'orchestrator', 'title': 'Decision'}, 'No fixture authority', 'orchestrator', uncontrolled),
                 lambda: fresh._node(copy.deepcopy(self.token)),
                 lambda: fresh._execute_node(self.node, copy.deepcopy(self.token))]
        for call in calls:
            with self.subTest(call=call), self.assertRaises(ValueError):
                await call()
            self.assertEqual(self.registry.calls, [])
            self.assertEqual(self.adapter.effects, effects)
            self.assertEqual(resources.ResourceLedger(self.storage.root).entries(), claims)
        after = self.supervisor.run()
        self.assertEqual(after['activations'], before['activations'])
        self.assertEqual(after['pending'], before['pending'])
        await self.settle(job)

    async def test_complete_replacement_chain_settles_old_workflow_rows_without_rewriting_task_evidence(self):
        from harflow_mcp import store, workflows as w, workflow_resources as wr
        job = await self.launch()
        initial, identity, original = self.source()
        record = store.TaskRecord(task_id=identity['task'], backend='codex', session_id=None,
            repo_path=str(self.root), started_at='2026-11-06T00:00:00Z', freedom='read_only',
            status='running', prompt='Immutable original task record evidence', exit_code=None)
        store.write(self.registry.log_dir, record)
        record_before = store.read(self.registry.log_dir, identity['task'])
        record_bytes = store.record_path(self.registry.log_dir, identity['task']).read_bytes()
        self.coordinator.contain(self.contain)
        handoff = self.coordinator.prepare_stop()
        receipt = self.coordinator.deliver(handoff)
        self.coordinator.acknowledge(receipt['command_id'], receipt)
        await self.settle(job)
        self.coordinator.observe(self.certificate)
        self.coordinator.certify()
        self.coordinator.release()
        self.coordinator.replace(self.replacement)
        await self.successor_job()
        final = self.supervisor.run()
        old = next(a for a in final['activations'] if a['id'] == identity['activation'])
        self.assertEqual(old['status'], 'cancelled')
        self.assertEqual(old['tasks'][0]['status'], 'cancelled')
        from poststart_control_fixtures import digest
        proof = final['poststart']['settlements'][self.contain['request_id']]
        self.assertEqual(old['poststart_settlement'], dict(request_id=self.contain['request_id'],
            task_id=identity['task'], certificate_digest=digest(self.certificate),
            evidence_revision=proof['certificate']['revision'], provenance='synthetic_poststart_settlement'))
        self.assertEqual(proof['identity'], identity)
        self.assertEqual(proof['evidence'], self.certificate)
        self.assertEqual(proof['certificate']['digest'], digest(self.certificate))
        self.assertTrue(proof['job']['done'])
        for key in ('id', 'node_id', 'role', 'created_at', 'token', 'visit_id', 'attempt_in_visit',
                    'assignment_prompt', 'assigned_task_ids', 'input_result_refs', 'execution_session_mode'):
            self.assertEqual(old.get(key), original.get(key), key)
        for key in ('task_id', 'candidate', 'repo_path', 'freedom', 'network', 'session_mode',
                    'execution_kind', 'dispatcher_identity', 'dispatcher_job_id'):
            self.assertEqual(old['tasks'][0].get(key), original['tasks'][0].get(key), key)
        for key in ('node_result', 'result', 'raw_output', 'exit_code'):
            self.assertEqual(old.get(key), original.get(key), key)
        self.assertIsNone(old['tasks'][0].get('result'))
        self.assertIsNone(old['tasks'][0].get('exit_code'))
        self.assertEqual(store.read(self.registry.log_dir, identity['task']), record_before)
        self.assertEqual(store.record_path(self.registry.log_dir, identity['task']).read_bytes(), record_bytes)
        self.assertFalse(w._run_settling(final))
        self.assertFalse(wr._unresolved(self.storage, final))
        claims_before = resources.ResourceLedger(self.storage.root).entries()
        wr.release_run(self.storage, final)
        self.assertEqual(resources.ResourceLedger(self.storage.root).entries(), claims_before)

    async def test_resource_journal_cannot_retarget_unrelated_real_grant_active_or_archived(self):
        from harflow_mcp.workflow_run_documents import WorkflowRunDocumentError
        from harflow_mcp import workflows as w
        self.resource_workflow()
        await self.ready()
        ledger = resources.ResourceLedger(self.storage.root)
        unrelated = ledger.request(['unrelated:fixture'], resources.Owner('workflow_attempt',
            workflow_run_id=self.supervisor.run_id, activation_id='unrelated-resource-attempt'))
        path = self.storage.runs / (self.supervisor.run_id + '.json')
        for archived in (False, True):
            if archived:
                self.coordinator.replace(self.replacement)
                await self.successor_job()
            saved = path.read_bytes()
            before_entries = ledger.entries()
            corrupt = json.loads(saved)
            runtime = corrupt['run']['poststart']['runtime']
            if archived:
                runtime = runtime['history'][0]
            journal = runtime['resource_handoff']
            self.assertIsNotNone(journal)
            journal.update(reservation_id=unrelated.reservation_id, names=list(unrelated.names),
                           old_attempt_id=unrelated.owner.activation_id, old_generation=unrelated.generation,
                           coordinator_generation=None, successor_generation=None, successor_attempt_id=None,
                           state='coordinator_intent')
            path.write_text(json.dumps(corrupt), encoding='utf-8')
            try:
                with self.subTest(archived=archived):
                    with self.assertRaises(WorkflowRunDocumentError):
                        w.WorkflowStore(self.storage.root).get_run(self.supervisor.run_id, refresh_atomic=True)
                    with self.assertRaises((WorkflowRunDocumentError, ValueError)):
                        self.coordinator.release()
                    self.assertEqual(ledger.entries(), before_entries)
                    self.assertEqual(ledger.get(unrelated.reservation_id), unrelated)
            finally:
                path.write_bytes(saved)

    async def test_maximum_compact_certificate_profile_never_strands_accepted_intent(self):
        self.adapter.inventory = [dict(actor=('actor-%03d-' % index) + 'x' * 1170,
            generation='generation-1', obligation='required', settlement='satisfied') for index in range(100)]
        job = await self.launch()
        compact = json.dumps(self.certificate, separators=(',', ':'), ensure_ascii=False).encode()
        self.assertGreater(len(compact), 120000)
        self.assertLessEqual(len(compact), 131072)
        try:
            self.coordinator.contain(self.contain)
        except ValueError as error:
            self.assertIn(str(error), {'metadata_capacity', 'text_capacity', 'poststart_capacity', 'completion_capacity', 'poststart_completion_envelope'})
            self.assertIsNone(self.coordinator.snapshot()['intent'])
            self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
            await self.settle(job)
            return
        handoff = self.coordinator.prepare_stop()
        receipt = self.coordinator.deliver(handoff)
        self.coordinator.acknowledge(receipt['command_id'], receipt)
        await self.settle(job)
        self.coordinator.observe(compact)
        self.coordinator.certify()
        self.coordinator.release()
        self.coordinator.replace(self.replacement)
        await self.successor_job()
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original', 'stop', 'dispatch'])
        self.assertEqual(self.registry.calls, [])

    async def test_stale_prelock_observation_cannot_write_beyond_actual_protective_frontier(self):
        from unittest.mock import patch
        from harflow_mcp import workflows as w
        job = await self.launch()
        actual_read = self.storage._read_run
        interleaved = False
        def read_then_interleave(run_id, *args, **kwargs):
            nonlocal interleaved
            captured = actual_read(run_id, *args, **kwargs)
            if run_id == self.supervisor.run_id and not interleaved:
                interleaved = True
                fresh = w.WorkflowStore(self.storage.root)
                fresh.update_run(run_id, lambda run: run.update(instructions='First independent actual mutation'),
                                 'ordinary_inner_checkpoint')
            return captured
        # The outer reader returns its genuine earlier snapshot after an actual
        # independent store has committed. No ledger or checkpoint is fabricated.
        with patch.object(self.storage, '_read_run', side_effect=read_then_interleave):
            with self.assertRaises(ValueError):
                self.storage.update_run(self.supervisor.run_id,
                    lambda run: run.update(instructions='Unprotected outer overwrite'), 'ordinary_outer_checkpoint')
        self.assertTrue(interleaved)
        self.assertEqual(self.supervisor.run()['instructions'], 'First independent actual mutation')
        self.storage.update_run(self.supervisor.run_id,
            lambda run: run.update(instructions='Explicit retry from fresh checkpoint'), 'ordinary_explicit_retry')
        self.assertEqual(self.supervisor.run()['instructions'], 'Explicit retry from fresh checkpoint')
        self.assertEqual([e['kind'] for e in self.adapter.effects], ['original'])
        self.assertEqual(self.registry.calls, [])
        await self.settle(job)


if __name__ == '__main__':
    unittest.main()
