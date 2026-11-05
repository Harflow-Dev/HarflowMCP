"""Restricted root Fresh prestart withdrawal and same-assignment linked retry.

No signals, graph edits, descendant cancellation or poststart replacement. A
missing record is never no-spawn proof; only the exact owner's positive ack is.
"""
from __future__ import annotations

import asyncio
import copy
import time
import uuid

from .workflow_steering import SteeringError, _digest
from .state_access import storage_operation


def assignment_values(activation):
    return {k: copy.deepcopy(activation.get(k, [] if k != 'assignment_prompt' else '')) for k in ('assignment_prompt','assigned_task_ids','input_result_refs','authorized_artifact_refs','authorized_input_refs')}


def configured_values(run, node, candidate, activation):
    from .workflows import run_effective_freedom
    return {'definition_digest': _digest(run['definition'])[7:], 'node_id': node['id'], 'node_configuration_digest': _digest(node),
        'goal_revision': run.get('goal_revision', 0), 'goal_digest': _digest({'goal': run.get('goal'), 'parent_goal': run.get('parent_goal')}),
        'candidate': {k: copy.deepcopy(v) for k, v in candidate.items() if k != 'fallbacks'}, 'assignment_digest': _digest(assignment_values(activation)),
        'freedom': run_effective_freedom(run, node), 'network': False if run.get('network') is False else node.get('network', run.get('network')),
        'repo_path': run['repo_path'], 'worktree_digest': _digest(run.get('worktree')), 'attempt_grant': run.get('attempt_grants', {}).get(node['id'], 0)}


def ensure_accounting(ledger):
    if 'replan_accounting' not in ledger:
        ledger['replan_accounting'] = {'accepted': len(ledger.get('replans', [])), 'retired_details': 0, 'retired_never_started': 0, 'provenance': 'durable_explicit_replan_and_positive_no_start'}
        for sequence, record in enumerate(ledger.get('replans', []), 1):
            record['sequence'] = sequence
    ledger.setdefault('replan_history_incomplete', False)
    ledger.setdefault('normal_execution_history_incomplete', False)
    ledger.setdefault('replan_history_floor', 0)


def projection(record):
    """Public semantic proof; private directory/root identity stays in storage."""
    keys = ('replan_id', 'phase', 'target', 'withdrawal_witness', 'reason', 'created_at', 'planned_activation_id', 'planned_task_id', 'attempt_in_visit', 'journal', 'diagnostic')
    result = {key: copy.deepcopy(record[key]) for key in keys if key in record and key != 'reason'}
    result['reason_preview'] = record['reason'][:512]
    result['reason_truncated'] = len(record['reason']) > 512
    result['reason_ref'] = record.get('reason_ref', 'replan:' + record['replan_id'])
    if record.get('withdrawal_ack'):
        ack = record['withdrawal_ack']
        proof = ack.get('proof') or {}
        result['withdrawal_ack'] = {key: ack[key] for key in ('outcome', 'task_id', 'generation', 'request_sha256', 'configuration_sha256', 'replan_id')}
        result['withdrawal_ack'].update({key: proof.get(key) for key in ('origin', 'resources_released', 'followups_released')})
    return result


def active_replan(run):
    from .replan_schema import accepted_normal_result
    return next((r for r in run.get('steering', {}).get('replans', []) if r['phase'] != 'replan_committed' and not (r['phase'] == 'settlement_unknown' and accepted_normal_result(run, r['withdrawal_witness']['activation_id']))), None)


def retired_execution(run, activation_id, task_id=None, storage=None):
    for record in run.get('steering', {}).get('replans', []):
        witness = record['withdrawal_witness']
        if witness['activation_id'] == activation_id and (task_id is None or witness['task_id'] == task_id):
            return record
    if storage is not None:
        activation = next((a for a in run['activations'] if a['id'] == activation_id), None)
        if activation and activation.get('superseded_by_execution_id'):
            from .replan_certificates import read
            cert = read(storage, predecessor_execution_id=activation_id)
            if cert['root_workflow_run_id'] == run['workflow_run_id'] and (task_id is None or task_id == cert['predecessor_task_id']):
                return {'replan_id': cert['replan_id'], 'phase': 'replan_committed', 'journal': {'original_withdrawn': True, 'resources_released': True}, 'withdrawal_witness': {'activation_id': activation_id, 'task_id': cert['predecessor_task_id']}}
    return None


def superseded_execution(run, activation, storage=None):
    record = retired_execution(run, activation['id'])
    if record is None and storage is not None and activation.get('superseded_by_execution_id'):
        from .replan_certificates import read
        cert = read(storage, predecessor_execution_id=activation['id'])
        return cert['root_workflow_run_id'] == run['workflow_run_id'] and cert['replacement_execution_id'] == activation['superseded_by_execution_id'] and activation.get('status') == 'not_started' and all(t['status'] == 'not_started' for t in activation['tasks'])
    if not record or record['phase'] != 'replan_committed' or not all(record['journal'].values()):
        return False
    replacement = next((a for a in run['activations'] if a['id'] == record['planned_activation_id']), None)
    return bool(replacement and replacement.get('replan_id') == record['replan_id'] and replacement.get('supersedes_execution_id') == activation['id'] and activation.get('superseded_by_execution_id') == replacement['id'])


def validate_replacement(storage, run, activation, *, live=True):
    """Re-read the permanent exact owner certificate before a linked dispatch."""
    record = next((p for p in run.get('steering', {}).get('replans', []) if p['replan_id'] == activation.get('replan_id') and p['planned_activation_id'] == activation['id']), None)
    if record is None:
        from .replan_certificates import read
        cert = read(storage, replan_id=activation.get('replan_id'))
        if cert['ledger_epoch'] > run['steering']['closed_epoch'] or cert['root_workflow_run_id'] != run['workflow_run_id'] or cert['replacement_execution_id'] != activation['id'] or cert['replacement_task_id'] != activation.get('planned_task_id') or cert['predecessor_execution_id'] != activation.get('supersedes_execution_id') or cert['candidate'] != {k: v for k, v in activation.get('execution_candidate', {}).items() if k != 'fallbacks'} or cert['attempt_in_visit'] != activation.get('attempt_in_visit') or _digest(assignment_values(activation)) != cert['assignment']['digest']:
            raise SteeringError('replan_source_unavailable', 'Current planned activation contradicts compact lineage certificate')
        if live and not activation.get('resume_question_id'):
            node = next(n for n in run['definition']['nodes'] if n['id'] == activation['node_id'])
            if configured_values(run, node, activation['execution_candidate'], activation) != cert['source_configuration']:
                raise SteeringError('replan_source_unavailable', 'Current launch controls contradict compact original configuration')
        return cert
    if not record or record['phase'] != 'replan_committed' or not superseded_execution(run, next(a for a in run['activations'] if a['id'] == record['withdrawal_witness']['activation_id'])):
        raise SteeringError('replan_source_unavailable', 'Linked replacement lacks exact committed supersedes authority')
    if live and not activation.get('resume_question_id'):
        node = next(n for n in run['definition']['nodes'] if n['id'] == activation['node_id'])
        if configured_values(run, node, record['candidate'], activation) != record.get('source_configuration'):
            raise SteeringError('replan_source_unavailable', 'Current launch settings no longer match certified original configuration')
    old = next(a for a in run['activations'] if a['id'] == record['withdrawal_witness']['activation_id'])
    if record.get('source_configuration') and (_digest(assignment_values(old)) != record['source_configuration']['assignment_digest'] or _digest(assignment_values(activation)) != record['source_configuration']['assignment_digest']):
        raise SteeringError('replan_source_unavailable', 'Immutable historical assignment no longer matches certified source provenance')
    source = next(t for t in old['tasks'] if t['task_id'] == record['withdrawal_witness']['task_id'])
    if source['candidate'] != record['candidate'] or source.get('harness_metadata', {}).get('requested') != record['candidate']:
        raise SteeringError('replan_source_unavailable', 'Selected candidate no longer matches certified original launch')
    from .task_withdrawal import read_ack
    from .task_supervision import read_launch, read_task_record
    w = record['withdrawal_witness']
    ack = read_ack(storage.root / 'tasks', w['task_id'], w['launch_generation'])
    launch = read_launch(storage.root / 'tasks', w['task_id'])
    if ack != record.get('withdrawal_ack') or not launch or launch.get('state') != 'not_started' or not launch.get('withdrawal_proof') or read_task_record(storage.root / 'tasks', w['task_id'], include_prompt=False) is not None:
        raise SteeringError('replan_source_unavailable', 'Permanent original no-spawn certificate is unavailable or contradictory')
    if activation.get('execution_candidate') != record['candidate'] or activation.get('planned_task_id') != record['planned_task_id']:
        raise SteeringError('replan_source_unavailable', 'Linked replacement candidate or exact task identity changed')
    return record


def _unissued(storage, task_id):
    from .task_supervision import read_launch, read_task_record
    if read_task_record(storage.root / 'tasks', task_id, include_prompt=False) is not None or read_launch(storage.root / 'tasks', task_id) is not None:
        raise SteeringError('replan_unsupported', 'Queued steering response has an issuance or unresolved launch claim')


def _source(storage, run, target):
    from .worker_steering import current_target
    from .task_supervision import read_launch, read_task_record
    from .workflows import run_effective_freedom
    if run.get('parent_link') or run.get('kind') != 'workflow' or run.get('execution_contract') != 'delegation' or target.get('workflow_run_id') != run['workflow_run_id'] or target.get('path') != [run['workflow_run_id']]:
        raise SteeringError('replan_unsupported', 'Only a root connected delegation worker supports prestart replanning')
    if target != current_target(storage, run, target):
        raise SteeringError('steering_target_stale', 'Worker assignment or checkpoint changed')
    activation = next(a for a in run['activations'] if a['id'] == target['activation_id'])
    if activation.get('on_demand_call_id') or activation.get('invocation'):
        raise SteeringError('replan_unsupported', 'Specialist invocations do not support prestart replanning')
    node = next((n for n in run['definition']['nodes'] if n['id'] == activation['node_id']), None)
    token = next((t for t in run['pending'] if t['id'] == target['token_id']), None)
    if node is None or token is None:
        raise SteeringError('replan_unsupported', 'Connected source node or token is unavailable')
    originals = [t for t in activation['tasks'] if not t.get('steering_turn_id')]
    if (activation.get('on_demand_call_id') or activation.get('invocation') or activation.get('node_result') or len(originals) != 1
            or node.get('execution_mode') != 'headless' or node.get('session_mode') != 'fresh'
            or activation.get('execution_session_mode', 'fresh') != 'fresh' or activation.get('continue_previous') or activation.get('resume_task_id')):
        raise SteeringError('replan_unsupported', 'Replan requires one headless Fresh original attempt, without specialist or Resume authority')
    attempt = originals[0]
    if attempt.get('result') is not None:
        raise SteeringError('replan_unsupported', 'Original execution already has observed result evidence')
    candidate = attempt.get('candidate') or {}
    from .workflows import _candidate_key
    if node['id'] + ':' + _candidate_key(candidate) in run.get('suppressed_candidates', []):
        raise SteeringError('replan_source_unavailable', 'The selected original candidate is suppressed')
    configured = node.get('agent') or {}
    if candidate not in [configured] + configured.get('fallbacks', []):
        raise SteeringError('replan_source_unavailable', 'Original selected candidate is no longer configured on this node')
    from .tasks import get_backend
    if type(candidate.get('max_turns')) is not int or candidate['max_turns'] <= 0 or not get_backend(candidate['backend']).capabilities.supports_turn_cap:
        raise SteeringError('replan_unsupported', 'The original selected candidate must have a qualified positive finite turn cap')
    if attempt.get('execution_kind') != 'headless' or attempt.get('session_mode') != 'fresh' or attempt.get('resume_task_id') is not None:
        raise SteeringError('replan_unsupported', 'Original attempt is not qualified headless Fresh')
    metadata = attempt.get('harness_metadata') or {}
    effective = metadata.get('effective') or {}
    freedom = run_effective_freedom(run, node)
    network = False if run.get('network') is False else node.get('network', run.get('network'))
    if (metadata.get('provenance') != 'validated_launch_configuration' or metadata.get('verification_status') != 'configured_not_observed'
            or metadata.get('observed') is not None or metadata.get('requested') != candidate or effective.get('backend') != candidate.get('backend')
            or effective.get('freedom') != freedom or attempt.get('freedom') != freedom or attempt.get('network') != network
            or attempt.get('repo_path') != run['repo_path']):
        raise SteeringError('replan_source_unavailable', 'Original launch configuration provenance cannot be qualified')
    launch = read_launch(storage.root / 'tasks', attempt['task_id'])
    if read_task_record(storage.root / 'tasks', attempt['task_id'], include_prompt=False) is not None or not launch or launch['state'] not in {'reserved', 'claimed', 'waiting_for_resources', 'granted'}:
        raise SteeringError('replan_unsupported', 'Original provider is already issued or its prestart launch is unavailable')
    for key in ('request_sha256', 'configuration_sha256'):
        if type(launch.get(key)) is not str or len(launch[key]) != 64:
            raise SteeringError('replan_source_unavailable', 'Original opaque launch fingerprints are unavailable')
    if any(launch.get(key) != value for key, value in {'backend': candidate['backend'], 'repo_path': run['repo_path'], 'freedom': freedom, 'network': network}.items()):
        raise SteeringError('replan_source_unavailable', 'Original launch provenance contradicts configured backend, repository or permissions')
    from .workflow_receipt_io import read as read_owner
    try:
        owner, _ = read_owner(storage.owners / (attempt['task_id'] + '.json'))
    except Exception as exc:
        raise SteeringError('replan_source_unavailable', 'Original workflow ownership receipt is unavailable') from exc
    if (owner.get('workflow_run_id') != run['workflow_run_id'] or owner.get('root_workflow_run_id', owner.get('workflow_run_id')) != run['workflow_run_id']
            or owner.get('workflow_execution_id') != activation['id'] or owner.get('workflow_node_id') != node['id'] or owner.get('workflow_role') != 'node'):
        raise SteeringError('replan_source_unavailable', 'Original workflow owner receipt contradicts this exact activation')
    if launch.get('parent_task_id') is not None or launch.get('continuation_of_task_id') is not None or launch.get('resume_task_id') is not None or launch.get('chain_root_task_id') not in {None, attempt['task_id']}:
        raise SteeringError('replan_unsupported', 'Original launch is not Fresh')
    queued = [t for t in run['steering']['turns'] if t['state'] == 'queued' and t['target']['role'] == 'worker' and t['target']['activation_id'] == activation['id'] and t['boundary_task_id'] == attempt['task_id']]
    unsettled = [t for t in run['steering']['turns'] if t['state'] in {'queued', 'reserved', 'spawn_intent', 'running', 'unknown', 'captured'}]
    if len(queued) != 1 or unsettled != queued:
        raise SteeringError('replan_unsupported', 'Exactly one positively queued, unissued response is required')
    response = queued[0]
    if response['target'] != target or response['commit_target'] != target:
        raise SteeringError('steering_target_stale', 'Queued response was issued against another assignment or checkpoint')
    _unissued(storage, response['task_id'])
    if any(t.get('steering_turn_id') == response['turn_id'] for a in run['activations'] for t in a['tasks']):
        raise SteeringError('replan_unsupported', 'Queued response already has an activation task claim')
    episode = next(e for e in run['steering']['episodes'] if e['episode_id'] == response['episode_id'])
    if not episode['hold'] or episode.get('dismiss_requested') or episode['target'] != target:
        raise SteeringError('steering_target_stale', 'Queued conversation no longer owns its exact hold')
    from .workflow_execution_policy import visit_id
    witness = {'activation_id': activation['id'], 'task_id': attempt['task_id'], 'token_id': token['id'], 'visit_id': visit_id(run, token),
        'assignment_digest': target['assignment_digest'], 'launch_generation': launch['generation'], 'request_sha256': launch['request_sha256'], 'configuration_sha256': launch['configuration_sha256'],
        'resolved_configuration_digest': _digest(configured_values(run, node, candidate, activation)),
        'queued_turn_id': response['turn_id'], 'queued_task_id': response['task_id']}
    from .replan_schema import witness as validate_witness
    validate_witness(witness)
    return witness, activation, attempt, candidate, node, token, response, episode


def targets(storage, run):
    if run.get('steering', {}).get('schema_version') != 2 or active_replan(run):
        return []
    from .worker_steering import targets as worker_targets
    result = []
    for target in worker_targets(storage, run):
        try:
            witness, *_ = _source(storage, run, target)
            result.append({'target': target, 'withdrawal_witness': witness})
        except (ValueError, KeyError, TypeError):
            pass
    return result


def _authorize_control(storage, run, principal, monitor):
    from .workflow_steering import _authorize
    _authorize(run, principal, monitor)
    from . import identity, workflow_interactions
    supervisor = run.get('supervisor_identity')
    if not supervisor or identity.identity_check(supervisor) not in {'alive', 'dead'}:
        raise SteeringError('caller_undecidable', 'Supervisor identity is unknown; prestart replan refuses')
    if monitor:
        from .workflow_cancellation import eligibility
        allowed, reason = eligibility(storage, run)
        if not allowed:
            raise SteeringError('replan_not_owner', reason)
    else:
        from .workflows import WorkflowError
        try:
            workflow_interactions.authorize(storage, run, 'caller', 'cancel')
        except WorkflowError as exc:
            raise SteeringError('replan_not_owner', str(exc)) from exc
    if run.get('input_question') or run.get('pending_inputs') or run.get('pending_attention') or run.get('status') not in {'running', 'starting'}:
        raise SteeringError('replan_unsupported', 'An actual question, pause, approval or terminal state must use its existing control')


async def replan_workflow_steering(workflow_run_id, request, *, root=None, monitor=False):
    from .workflow_steering import _storage, _transport
    from .replan_schema import request as validate_request
    from .steering_window import check_namespace
    storage = _storage(root)
    principal = await asyncio.to_thread(_transport, storage, monitor)
    request = validate_request(request)
    digest = _digest({k: v for k, v in request.items() if k != 'request_id'})
    binding = _digest({'state_root': str(storage.root), 'principal': 'native_human' if monitor else principal, 'epoch': request['expected_interaction_epoch']})
    request_key = f'{request["ledger_epoch"]}:{request["request_id"]}'
    initial = storage.get_run(workflow_run_id, refresh_atomic=True)
    admitted_epoch = None
    if request_key not in initial.get('steering', {}).get('requests', {}):
        _authorize_control(storage, initial, principal, monitor)
        check_namespace(initial['steering'], request)
        admitted_epoch = request['ledger_epoch']
        from .replan_retention import maintain as reserve_history
        reserve_history(storage, workflow_run_id, reserve_text=2 * len(request['reason'].encode()))
    result = None
    def admit(run):
        nonlocal result
        from .workflow_steering import _authorize
        _authorize(run, principal, monitor)
        ledger = run['steering']
        previous = ledger['requests'].get(request_key)
        if previous:
            if previous['payload_digest'] != digest or previous['principal_binding'] != binding:
                raise SteeringError('request_payload_conflict', 'Replan request identity already has another payload')
            result = {k: copy.deepcopy(v) for k, v in previous.items() if k != 'principal_binding'} | {'replayed': True}
            return
        _authorize_control(storage, run, principal, monitor)
        if admitted_epoch != request['ledger_epoch']:
            check_namespace(ledger, request)
        if active_replan(run):
            raise SteeringError('replan_busy', 'Observe the existing exact replan intent')
        if request['expected_interaction_epoch'] != run['interaction_epoch'] or request['expected_control_revision'] != ledger['control_revision']:
            raise SteeringError('steering_revision_stale', 'Replan authority revision changed')
        witness, activation, attempt, candidate, node, token, response, ep = _source(storage, run, request['target'])
        if witness != request['withdrawal_witness'] or request['episode_id'] != ep['episode_id'] or request['expected_steering_revision'] != ep['steering_revision']:
            raise SteeringError('steering_target_stale', 'Exact launch, assignment or queued response changed')
        from .workflow_execution_policy import attempts_used
        # Exclude only this positively withdrawn source once settlement is proven.
        count = attempts_used(run, node, token) - 1
        if count < 0 or count >= node['max_attempts'] + run.get('attempt_grants', {}).get(node['id'], 0):
            raise SteeringError('replan_unsupported', 'Unchanged normal attempt allowance cannot admit the linked replacement')
        from .steering_window import retained_text_bytes, TEXT_LIMIT
        if retained_text_bytes(run) + 2 * len(request['reason'].encode()) > TEXT_LIMIT - 96 * 1024:
            raise SteeringError('steering_queue_full', 'Protected shared conversation/replan text cannot fit; no withdrawal was requested')
        ensure_accounting(ledger)
        record = {'replan_id': uuid.uuid4().hex, 'request_key': request_key, 'phase': 'cancel_requested', 'target': copy.deepcopy(request['target']), 'withdrawal_witness': witness,
            'reason': request['reason'], 'created_at': time.time(), 'sequence': ledger['replan_accounting']['accepted'] + 1, 'source_dispatcher_identity': copy.deepcopy(attempt.get('dispatcher_identity')), 'source_job_id': attempt.get('dispatcher_job_id'), 'candidate': copy.deepcopy(candidate), 'planned_activation_id': uuid.uuid4().hex, 'planned_task_id': uuid.uuid4().hex,
            'source_configuration': configured_values(run, node, candidate, activation), 'attempt_in_visit': count + 1, 'journal': {'original_withdrawn': False, 'response_revoked': True, 'resources_released': False, 'messages_released': False}}
        import json
        if len(json.dumps(run, ensure_ascii=False).encode()) + len(json.dumps(record, ensure_ascii=False).encode()) > 4 * 1024 * 1024 - 128 * 1024:
            raise SteeringError('steering_queue_full', 'Protected evidence cannot fit the authorized inline bounds; no withdrawal requested')
        ledger.setdefault('replans', []).append(record)
        ledger['replan_accounting']['accepted'] += 1
        from .replan_retention import bind_origin
        bind_origin(storage, record)
        # Same root CAS as response runner's claim: positive no-issuance only.
        response.update(state='settled', outcome='request_expired', not_started=True, revoked_by_replan_id=record['replan_id'])
        ep.update(state='stale', hold=False, steering_revision=ep['steering_revision'] + 1, reason='Queued response withdrawn by explicit prestart replan')
        activation['withdrawing_replan_id'] = record['replan_id']
        ledger['control_revision'] += 1
        from .workflow_steering import _history
        _history(run, 'replan_requested', replan_id=record['replan_id'], task_id=attempt['task_id'], reason=request['reason'])
        record['reason_ref'] = 'history:' + str(ledger['sequence'])
        result = {'request_id': request['request_id'], 'payload_digest': digest, 'episode_id': ep['episode_id'], 'steering_revision': ep['steering_revision'], 'interaction_epoch': run['interaction_epoch'],
            'control_revision': ledger['control_revision'], 'sequence': ledger['sequence'], 'state': record['phase'], 'turn_id': None, 'task_id': None, 'accepted': True, 'scheduled': True, 'replayed': False,
            'next_action': 'Observe exact prestart withdrawal; no poststart replacement is authorized', 'ledger_epoch': request['ledger_epoch'], 'replan_id': record['replan_id']}
        ledger['requests'][request_key] = {**copy.deepcopy(result), 'principal_binding': binding}
    storage.update_run(workflow_run_id, admit, 'steering_replan_requested')
    if result.get('retired_replan'):
        return {**result, 'phase': 'replan_committed'}
    reconcile(storage, workflow_run_id)
    from .workflows import _launch
    _launch(storage, workflow_run_id)
    from .replan_retention import maintain as maintain_replans
    retention_refusal = None
    try:
        maintain_replans(storage, workflow_run_id)
    except (ValueError, OSError) as exc:
        retention_refusal = str(exc)[:2000]
    current = storage.get_run(workflow_run_id)
    record = next((r for r in current['steering']['replans'] if r['replan_id'] == result['replan_id']), None)
    return {**result, 'phase': record['phase'] if record else 'replan_committed', **({'retired_replan': True} if record is None else {}), **({'history_retention_refusal': retention_refusal} if retention_refusal else {})}


@storage_operation
def reconcile(storage, run_id):
    from .task_withdrawal import request_withdrawal, reconcile_withdrawal
    from .task_supervision import read_launch, read_task_record
    # Persisted intent/fence is immutable. No root lock spans owner identity
    # probing or the withdraw-only transport; fresh root CAS follows the proof.
    with storage.tree_mutation(run_id):
        run = storage.get_run(run_id, refresh_atomic=True)
        record = copy.deepcopy(active_replan(run))
    if record is None:
        return
    witness = record['withdrawal_witness']
    task_id, generation = witness['task_id'], witness['launch_generation']
    try:
        request_withdrawal(storage.root / 'tasks', task_id, generation, witness['request_sha256'], witness['configuration_sha256'], record['replan_id'], root_workflow_run_id=run_id, source_execution_id=witness['activation_id'])
        ack = reconcile_withdrawal(storage.root / 'tasks', task_id, generation)
        with storage.tree_mutation(run_id):
            current = active_replan(storage.get_run(run_id, refresh_atomic=True))
            if current is None or current['replan_id'] != record['replan_id'] or current['withdrawal_witness'] != witness:
                return
            if ack is None:
                storage.update_run(run_id, lambda r: active_replan(r).update(phase='settling'), 'steering_replan_settling')
                return
            launch = read_launch(storage.root / 'tasks', task_id)
            if (ack['outcome'] != 'withdrawn' or launch is None or launch['state'] != 'not_started' or not launch.get('withdrawal_proof') or read_task_record(storage.root / 'tasks', task_id, include_prompt=False) is not None):
                raise ValueError('Original issuance is started or cannot be proven never started')
            _release_and_link(storage, run_id, record, ack)
    except (ValueError, OSError, KeyError, TypeError) as exc:
        with storage.tree_mutation(run_id):
            current = active_replan(storage.get_run(run_id, refresh_atomic=True))
            if current is not None and current['replan_id'] == record['replan_id']:
                storage.update_run(run_id, lambda r: active_replan(r).update(phase='settlement_unknown', diagnostic=str(exc)[:2000]), 'steering_replan_unknown')


def _link_eligible(run, record):
    target = record['target']
    if (run.get('status') not in {'running', 'starting'} or run.get('input_question') or run.get('pending_inputs') or run.get('pending_attention')
            or run['interaction_epoch'] != target['interaction_epoch'] or run.get('goal_revision', 0) != target['goal_revision']
            or run['steering']['control_revision'] != target['control_revision'] + 1
            or run['steering']['checkpoint_revision'] != target['checkpoint_revision']
            or _digest(run['definition'])[7:] != target['definition_digest']):
        raise SteeringError('steering_target_stale', 'Actual workflow authority or checkpoint changed during withdrawal; no replacement is authorized')
    original = next(a for a in run['activations'] if a['id'] == target['activation_id'])
    node = next(n for n in run['definition']['nodes'] if n['id'] == original['node_id'])
    token = next(t for t in run['pending'] if t['id'] == target['token_id'])
    from .workflows import run_effective_freedom
    resolved = _digest(configured_values(run, node, record['candidate'], original))
    from .workflow_execution_policy import attempts_used
    from .workflows import _candidate_key
    used = attempts_used(run, node, token)
    configured = node.get('agent') or {}
    if (record['candidate'] not in [configured] + configured.get('fallbacks', [])
            or node['id'] + ':' + _candidate_key(record['candidate']) in run.get('suppressed_candidates', [])
            or used + 1 != record['attempt_in_visit']
            or used >= node['max_attempts'] + run.get('attempt_grants', {}).get(node['id'], 0)):
        raise SteeringError('steering_target_stale', 'Normal attempt allowance or selected candidate changed during withdrawal')
    if resolved != record['withdrawal_witness']['resolved_configuration_digest'] or token.get('execution_activation_id') != original['id'] or token.get('execution_complete'):
        raise SteeringError('steering_target_stale', 'Assignment or configured launch settings changed during withdrawal')


def _release_and_link(storage, run_id, record, ack):
    from .resources import ResourceLedger
    run = storage.get_run(run_id, refresh_atomic=True)
    witness = record['withdrawal_witness']
    original = next(a for a in run['activations'] if a['id'] == witness['activation_id'])
    attempts = [original.get('resource_attempt')] + [t.get('resource_attempt') for t in original['tasks'] if t['task_id'] == witness['task_id']]
    ledger = ResourceLedger(storage.root)
    released = set()
    for attempt in attempts:
        if attempt and attempt.get('reservation_id') and (attempt['reservation_id'], attempt['id']) not in released:
            released.add((attempt['reservation_id'], attempt['id']))
            outcome = ledger.release_owned_workflow_attempt(attempt['reservation_id'], run_id, attempt['id'])
            if outcome not in {'released', 'absent'}:
                raise ValueError('Workflow resource release remains unresolved: ' + outcome)
    def settle(r):
        current = active_replan(r)
        if current is None or current['replan_id'] != record['replan_id']:
            raise SteeringError('replan_busy', 'Replan identity changed')
        a = next(a for a in r['activations'] if a['id'] == witness['activation_id'])
        old = next(t for t in a['tasks'] if t['task_id'] == witness['task_id'])
        old.update(status='not_started', replan_withdrawn=True)
        a.update(status='not_started')
        for resource_attempt in [a.get('resource_attempt'), old.get('resource_attempt')]:
            if resource_attempt:
                resource_attempt.update(state='not_started', released=True)
        from .workflow_messages import task_state
        task_state(r, witness['task_id'], 'not_started')
        current.update(phase='ready_to_replan', withdrawal_ack=copy.deepcopy(ack))
        current['journal'].update(original_withdrawn=True, resources_released=True, messages_released=True)
    storage.update_run(run_id, settle, 'steering_replan_ready')
    def link(r):
        current = active_replan(r)
        if current is None or current['replan_id'] != record['replan_id'] or not all(current['journal'].values()):
            raise SteeringError('replan_busy', 'Replan settlement journal is incomplete')
        _link_eligible(r, current)
        original = next(a for a in r['activations'] if a['id'] == witness['activation_id'])
        token = next(t for t in r['pending'] if t['id'] == witness['token_id'])
        if token.get('execution_activation_id') != original['id']:
            raise SteeringError('steering_target_stale', 'Original pending token changed before linked replacement')
        if any(a['id'] == current['planned_activation_id'] for a in r['activations']):
            raise SteeringError('replan_source_unavailable', 'Planned replacement already exists without committed linkage')
        replacement = {k: copy.deepcopy(v) for k, v in original.items() if k not in {'id', 'tasks', 'status', 'created_at', 'finished_at', 'worker_result_required', 'steering_superseded', 'worker_steering_receipts', 'worker_steering_assignment_digest', 'resource_attempt', 'node_result', 'result', 'raw_output', 'result_error', 'replan_id', 'withdrawing_replan_id'}}
        replacement.update(id=current['planned_activation_id'], tasks=[], status='running', created_at=time.time(), token=copy.deepcopy(token), replan_id=current['replan_id'], supersedes_execution_id=original['id'], planned_task_id=current['planned_task_id'], execution_candidate=copy.deepcopy(current['candidate']), attempt_in_visit=current['attempt_in_visit'], execution_session_mode='fresh')
        r['activations'].append(replacement)
        original['superseded_by_execution_id'] = replacement['id']
        token.update(execution_activation_id=replacement['id'], execution_candidate=copy.deepcopy(current['candidate']), execution_session_mode='fresh', continue_previous=False)
        token.pop('resume_task_id', None)
        current['phase'] = 'replan_committed'
        r['status'] = 'running'
    storage.update_run(run_id, link, 'steering_replan_committed')


async def run_replan(supervisor, run_id):
    reconcile(supervisor.store, run_id)
    release_retired_permits(supervisor)
    return active_replan(supervisor.store.get_run(run_id, refresh_atomic=True))


def release_retired_permits(supervisor):
    """Free only the original permit after exact positive withdrawal settlement."""
    run = supervisor.run(refresh_atomic=True)
    for key in list(supervisor.tree.held):
        if key[0] != run['workflow_run_id']:
            continue
        proven = any(p['withdrawal_witness']['task_id'] == key[1] and p['journal']['original_withdrawn'] and p['journal']['resources_released'] for p in run.get('steering', {}).get('replans', []))
        if not proven:
            try:
                from .workflow_receipt_io import read as read_owner
                from .replan_certificates import read
                owner, _ = read_owner(supervisor.store.owners / (key[1] + '.json'))
                cert = read(supervisor.store, predecessor_execution_id=owner['workflow_execution_id'])
                proven = cert['predecessor_task_id'] == key[1] and cert['root_workflow_run_id'] == run['workflow_run_id']
            except Exception:
                proven = False
        if proven and key in supervisor.tree.held:
            supervisor.tree.release_slot(control=supervisor.tree.held.pop(key))
