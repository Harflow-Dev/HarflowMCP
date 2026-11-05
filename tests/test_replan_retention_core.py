"""Synthetic lineage/window qualification: fake owners, no provider processes."""
import copy
import uuid
from types import SimpleNamespace

import pytest

from harflow_mcp import workflow_replan as replan, replan_retention as retention, replan_certificates as certificates
from harflow_mcp import task_supervision, workflow_steering as steering
from test_workflow_replan_faults import pending_replan, replan_request, positive_owner_ack
from test_worker_steering_faults import worker_origin, fresh, request as conversation_request

DISPATCHER = {'pid': 909001, 'start_time': 'original-dispatch-generation', 'markers': []}


@pytest.fixture
async def upstream_pending_replan(worker_origin, monkeypatch):
    import hashlib
    from harflow_mcp import store as task_store
    storage, rid = worker_origin
    data = storage.root / 'upstream-input.txt'
    data.write_text('Accepted upstream artifact data')
    artifact = {'execution_id': 'upstream-execution', 'artifact_id': 'artifact-one', 'owner_task_id': 'upstream-provider', 'workflow_run_id': rid, 'node_id': 'producer', 'kind': 'file', 'path': str(data), 'registered_at': 1.0, 'availability_at_assignment': 'available'}
    input_ref = {'workflow_run_id': rid, 'execution_id': 'upstream-execution', 'task_id': 'upstream-provider', 'content_sha256': hashlib.sha256(data.read_bytes()).hexdigest(), 'total_characters': 29, 'encoding': 'utf-8'}
    upstream = {'id': 'upstream-execution', 'node_id': 'producer', 'role': 'node', 'status': 'completed', 'created_at': 1.0,
        'tasks': [{'task_id': 'upstream-provider', 'status': 'completed', 'candidate': {'backend': 'claude'}}],
        'node_result': {'status': 'succeeded', 'result': {'summary': 'Accepted upstream data', 'completed_task_ids': []}, 'evidence': []}}
    def inputs(run):
        a = run['activations'][0]
        for target in (a, a['token'], run['pending'][0]):
            target['authorized_artifact_refs'] = [copy.deepcopy(artifact)]
            target['authorized_input_refs'] = [copy.deepcopy(input_ref)]
        run['activations'].append(copy.deepcopy(upstream))
    storage.update_run(rid, inputs, 'test_accepted_upstream_inputs')
    task_store.write(storage.root / 'tasks', task_store.TaskRecord(task_id='upstream-provider', backend='claude', session_id='upstream-session', repo_path=fresh(storage,rid)['repo_path'], started_at='2026-11-05T00:00:00Z', status='completed', exit_code=0, output_drained=True))
    return await pending_replan.__wrapped__((storage, rid), monkeypatch)


def source_provenance(storage, rid, job_id=None):
    job_id = job_id or uuid.uuid4().hex
    def stamp(run):
        a = next(a for a in run['activations'] if a['id'] == run['pending'][0]['execution_activation_id'])
        a['tasks'][0].update(dispatcher_identity=copy.deepcopy(DISPATCHER), dispatcher_job_id=job_id)
    storage.update_run(rid, stamp, 'test_captured_dispatcher')
    return job_id


def next_pending(storage, rid, prior_launch):
    run = fresh(storage, rid)
    a = next(a for a in run['activations'] if a['id'] == run['pending'][0]['execution_activation_id'])
    candidate = copy.deepcopy(a['execution_candidate'])
    task_id = a['planned_task_id']
    def prepare(value):
        current = next(x for x in value['activations'] if x['id'] == a['id'])
        current['tasks'] = [{'task_id': task_id, 'status': 'running', 'dispatch_stage': 'spawn_requested',
            'candidate': candidate, 'repo_path': value['repo_path'], 'freedom': 'write_in_repo', 'network': False,
            'session_mode': 'fresh', 'execution_kind': 'headless',
            'harness_metadata': {'provenance': 'validated_launch_configuration', 'verification_status': 'configured_not_observed',
                'requested': candidate, 'effective': {'backend': candidate['backend'], 'model': candidate.get('model'), 'freedom': 'write_in_repo'}, 'observed': None}}]
    storage.update_run(rid, prepare, 'test_next_original_launch')
    launch = {**copy.deepcopy(prior_launch), 'task_id': task_id, 'generation': uuid.uuid4().hex, 'request_sha256': uuid.uuid4().hex * 2, 'configuration_sha256': uuid.uuid4().hex * 2, 'state': 'waiting_for_resources', 'session_id': str(uuid.uuid4())}
    launch.pop('withdrawal_proof', None)
    launch.pop('not_started_reason', None)
    task_supervision._write(storage.root / 'tasks' / (task_id + '.launch.json'), launch)
    return launch


async def chain(pending_replan, monkeypatch, count):
    from harflow_mcp import identity
    storage, rid, launch = pending_replan
    original_check = identity.check_detail
    monkeypatch.setattr(identity, 'check_detail', lambda marker: ('dead', 'captured_generation_gone') if marker == DISPATCHER else original_check(marker))
    receipts = []
    for index in range(count):
        source_provenance(storage, rid)
        req = replan_request(storage, rid)
        receipt = await replan.replan_workflow_steering(rid, req, root=storage.root)
        positive_owner_ack(storage, launch)
        replan.reconcile(storage, rid)
        retention.maintain(storage, rid)
        receipts.append((req, receipt))
        if index + 1 < count:
            launch = next_pending(storage, rid, launch)
            await steering.steer_workflow(rid, conversation_request(storage, rid), root=storage.root)
    return storage, rid, receipts


async def test_repeated_never_started_chain_has_bounded_details_and_compact_current_lineage(upstream_pending_replan, monkeypatch):
    initial_storage, initial_rid, _ = upstream_pending_replan
    upstream_before = copy.deepcopy(next(a for a in fresh(initial_storage, initial_rid)['activations'] if a['id'] == 'upstream-execution'))
    artifact_before = (initial_storage.root / 'upstream-input.txt').read_bytes()
    owner_before = (initial_storage.owners / 'upstream-provider.json').read_bytes()
    storage, rid, receipts = await chain(upstream_pending_replan, monkeypatch, 36)
    run = fresh(storage, rid)
    assert len(run['steering']['replans']) == 32
    assert len(run['activations']) == 34
    assert next(a for a in run['activations'] if a['id'] == 'upstream-execution') == upstream_before
    assert (storage.root / 'upstream-input.txt').read_bytes() == artifact_before
    assert (storage.owners / 'upstream-provider.json').read_bytes() == owner_before
    current = next(a for a in run['activations'] if a['id'] == run['pending'][0]['execution_activation_id'])
    assert current['authorized_artifact_refs'] and current['authorized_input_refs']
    assert run['steering']['replan_accounting'] == {'accepted': 36, 'retired_details': 4, 'retired_never_started': 4, 'provenance': 'durable_explicit_replan_and_positive_no_start'}
    assert run['steering']['replan_history_incomplete'] and run['steering']['normal_execution_history_incomplete']
    first_id = receipts[0][1]['replan_id']
    cert = certificates.read(storage, replan_id=first_id)
    assert cert['predecessor_execution_id'] == 'original-execution'
    assert not any(a['id'] == 'original-execution' for a in run['activations'])
    assert run['tasks'] == [{'id': 'task-one', 'title': 'Assigned task', 'description': 'Bounded work', 'status': 'pending'}]
    assert run.get('attempt_grants', {}) == {}
    assert not (storage.root / 'observed-provider-effects.txt').exists()


async def test_live_unknown_dispatcher_without_outer_job_done_protects_predecessor(pending_replan, monkeypatch):
    storage, rid, launch = pending_replan
    source_provenance(storage, rid)
    await replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    positive_owner_ack(storage, launch)
    replan.reconcile(storage, rid)
    from harflow_mcp import identity
    monkeypatch.setattr(identity, 'check_detail', lambda _: ('undecidable', 'cannot_observe'))
    before = fresh(storage, rid)
    retention.maintain(storage, rid)
    assert fresh(storage, rid)['activations'] == before['activations']
    assert 'quiescence' not in fresh(storage, rid)['steering']['replans'][0]


async def test_done_ack_binds_exact_outer_job_origin_not_reused_token(pending_replan):
    storage, rid, launch = pending_replan
    job_id = source_provenance(storage, rid)
    await replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    positive_owner_ack(storage, launch)
    replan.reconcile(storage, rid)
    record = fresh(storage, rid)['steering']['replans'][0]
    wrong = ({'activation_id': 'later-execution', 'task_id': 'later-task', 'replan_id': record['replan_id']},)
    retention.record_job_done(storage, rid, job_id, DISPATCHER, wrong, SimpleNamespace(done=lambda: True))
    assert 'quiescence' not in fresh(storage, rid)['steering']['replans'][0]
    exact = ({'activation_id': 'original-execution', 'task_id': launch['task_id'], 'replan_id': record['replan_id']},)
    retention.record_job_done(storage, rid, job_id, DISPATCHER, exact, SimpleNamespace(done=lambda: False))
    assert 'quiescence' not in fresh(storage, rid)['steering']['replans'][0]
    retention.record_job_done(storage, rid, job_id, DISPATCHER, exact, SimpleNamespace(done=lambda: True))
    assert 'quiescence' not in fresh(storage, rid)['steering']['replans'][0]


async def test_outer_owned_job_waits_for_routing_finally_before_quiescence(pending_replan):
    import asyncio
    storage, rid, launch = pending_replan
    entered, dispatch_returned, routing_done, acknowledged = (asyncio.Event() for _ in range(4))
    supervisor = SimpleNamespace(store=storage, run_id=rid)
    async def node(token):
        stamp = retention.dispatcher_context(supervisor, 'original-execution', launch['task_id'])
        storage.update_run(rid, lambda r: r['activations'][0]['tasks'][0].update(stamp), 'test_actual_job_origin')
        entered.set()
        await dispatch_returned.wait()
        await routing_done.wait()  # Entire process_node/routing/finally still owns callbacks.
    supervisor._node = node
    coroutine, done = retention.new_outer_job(supervisor, fresh(storage, rid)['pending'][0], DISPATCHER)
    task = asyncio.create_task(coroutine)
    def finished(job):
        done(job)
        acknowledged.set()
    done.bind(task)
    task.add_done_callback(finished)
    await entered.wait()
    await replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    positive_owner_ack(storage, launch)
    replan.reconcile(storage, rid)
    dispatch_returned.set()
    assert not task.done()
    assert 'quiescence' not in fresh(storage, rid)['steering']['replans'][0]
    routing_done.set()
    await acknowledged.wait()
    assert task.done()
    assert fresh(storage, rid)['steering']['replans'][0]['quiescence']['origin'] == 'outer_node_job_done'


async def test_seventy_replans_close_old_scope_and_detect_tampered_dual_indexes(pending_replan, monkeypatch):
    import json
    from harflow_mcp.workflows import _write
    from harflow_mcp.state_ownership import state_session
    storage, rid, receipts = await chain(pending_replan, monkeypatch, 70)
    run = fresh(storage, rid)
    assert len(run['steering']['replans']) == 32
    assert len(run['steering']['requests']) <= 65  # At most64 settled plus current protected issuance.
    assert run['steering']['replan_accounting']['accepted'] == 70
    old_request, old_receipt = receipts[0]
    with pytest.raises(steering.SteeringError) as expired:
        await replan.replan_workflow_steering(rid, old_request, root=storage.root)
    assert expired.value.code == 'request_expired'
    certificate = certificates.read(storage, replan_id=old_receipt['replan_id'])
    first, second = certificates._paths(storage, certificate['replan_id'], certificate['predecessor_execution_id'])
    with state_session(storage.state_layout):
        forged = {**certificate, 'replacement_task_id': 'another-task'}
        _write(first, forged, layout=storage.state_layout)
    with pytest.raises(steering.SteeringError):
        certificates.read(storage, replan_id=certificate['replan_id'])
    with state_session(storage.state_layout):
        _write(second, forged, layout=storage.state_layout)
    # Matching two edited indexes still contradict independent source ownership.
    with pytest.raises(steering.SteeringError):
        certificates.read(storage, replan_id=certificate['replan_id'])
    assert not (storage.root / 'observed-provider-effects.txt').exists()


async def test_byte_retired_detail_replays_explicit_receipt_without_dispatch(pending_replan, monkeypatch):
    from harflow_mcp import identity
    storage, rid, launch = pending_replan
    source_provenance(storage, rid)
    req = replan_request(storage, rid)
    receipt = await replan.replan_workflow_steering(rid, req, root=storage.root)
    positive_owner_ack(storage, launch)
    replan.reconcile(storage, rid)
    old_check = identity.check_detail
    monkeypatch.setattr(identity, 'check_detail', lambda marker: ('dead','captured_generation_gone') if marker == DISPATCHER else old_check(marker))
    storage.update_run(rid, lambda r: r.update(instructions='x' * (3860 * 1024)), 'test_inline_byte_pressure')
    retention.maintain(storage, rid)
    assert not fresh(storage, rid)['steering']['replans']
    replay = await replan.replan_workflow_steering(rid, req, root=storage.root)
    assert replay['replayed'] and replay['retired_replan']
    assert replay['replan_id'] == receipt['replan_id']
    assert not (storage.root / 'observed-provider-effects.txt').exists()


async def test_upstream_input_authorizations_are_duplicate_detail_not_old_outputs(upstream_pending_replan, monkeypatch):
    storage, rid, _ = await chain(upstream_pending_replan, monkeypatch, 2)
    run = fresh(storage, rid)
    pins = set()
    assert all(retention._safe_predecessor(run, record, pins) for record in run['steering']['replans'])
    assert (storage.root / 'upstream-input.txt').read_text() == 'Accepted upstream artifact data'
