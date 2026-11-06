"""Owner-approved bounded replan detail with permanent compact lineage proofs."""
from __future__ import annotations
import contextvars
import copy
import json
import uuid

from .state_access import storage_operation
from .workflow_steering import SteeringError

_job = contextvars.ContextVar('harflow_outer_node_job', default=None)
_jobs = {}
_outer_tasks = {}
DIRECT_READ_BUDGET = 8 * 1024 * 1024


def dispatcher_context(supervisor, activation_id, task_id):
    context = _job.get()
    if context is None:
        return {}
    key = (activation_id, task_id)
    origin = context['origins'].setdefault(key, {'activation_id': activation_id, 'task_id': task_id, 'replan_id': None})
    _jobs[(str(supervisor.store.root), activation_id, task_id)] = (context, origin)
    return {'dispatcher_identity': copy.deepcopy(context['dispatcher_identity']), 'dispatcher_job_id': context['job_id']}


def bind_origin(storage, record):
    w = record['withdrawal_witness']
    found = _jobs.get((str(storage.root), w['activation_id'], w['task_id']))
    if found:
        context, origin = found
        if context['dispatcher_identity'] == record.get('source_dispatcher_identity') and context['job_id'] == record.get('source_job_id'):
            origin['replan_id'] = record['replan_id']


def bind_current(supervisor, record):
    if record:
        bind_origin(supervisor.store, record)


def new_outer_job(supervisor, token, dispatcher_identity):
    """Return coroutine and callback for the ENTIRE owned per-token job."""
    run_id = supervisor.run_id
    context = {'job_id': uuid.uuid4().hex, 'dispatcher_identity': copy.deepcopy(dispatcher_identity), 'origins': {}}
    async def invoke():
        handle = _job.set(context)
        try:
            return await supervisor._node(token)
        finally:
            _job.reset(handle)
    def done(task):
        # Task.done means all outer routing and finally paths have returned.
        if not task.done() or context.get('task') is not task:
            return
        origins = tuple(copy.deepcopy(value) for value in context['origins'].values())
        try:
            from .workflow_poststart_runtime import record_job_done as record_poststart_job_done
            record_poststart_job_done(supervisor.store, run_id, context['job_id'], context['dispatcher_identity'], origins, task)
            record_job_done(supervisor.store, run_id, context['job_id'], context['dispatcher_identity'], origins, task)
        except Exception:
            import logging
            logging.getLogger(__name__).warning("Outer-job quiescence acknowledgement unavailable; preserving predecessor", exc_info=True)
        finally:
            for origin in origins:
                key = (str(supervisor.store.root), origin['activation_id'], origin['task_id'])
                current = _jobs.get(key)
                if current and current[0] is context:
                    _jobs.pop(key)
            if _outer_tasks.get(context['job_id'], (None,None))[1] is task:
                _outer_tasks.pop(context['job_id'])
    def bind(task):
        context['task'] = task
        _outer_tasks[context['job_id']] = (context, task)
    done.bind = bind
    return invoke(), done


@storage_operation
def record_job_done(storage, run_id, job_id, dispatcher_identity, origins, task):
    registered = _outer_tasks.get(job_id)
    if not task.done() or not registered or registered[1] is not task or registered[0]['dispatcher_identity'] != dispatcher_identity:
        return
    expected = tuple(copy.deepcopy(value) for value in registered[0]['origins'].values())
    if tuple(origins) != expected or not any(o.get('replan_id') for o in origins):
        return
    def mutate(run):
        for record in run.get('steering', {}).get('replans', []):
            w = record['withdrawal_witness']
            exact = next((o for o in origins if o['replan_id'] == record['replan_id'] and o['activation_id'] == w['activation_id'] and o['task_id'] == w['task_id']), None)
            if exact and not record.get('quiescence') and record.get('source_job_id') == job_id and record.get('source_dispatcher_identity') == dispatcher_identity:
                record['quiescence'] = {'v': 1, 'origin': 'outer_node_job_done', 'replan_id': record['replan_id'], 'activation_id': w['activation_id'], 'task_id': w['task_id'], 'dispatcher_identity': copy.deepcopy(dispatcher_identity), 'job_id': job_id}
    storage.update_run(run_id, mutate, 'steering_replan_outer_job_quiescent')
    maintain(storage, run_id)


def _ordinary_references(run, record, removing, *, include_lineage=True):
    old_id = record['withdrawal_witness']['activation_id']
    def contains(value):
        if type(value) is str:
            return value == old_id or value.endswith(':' + old_id)
        if type(value) is list:
            return any(contains(item) for item in value)
        if type(value) is dict:
            return any(contains(item) for item in value.values())
        return False
    for key in ('tasks','decisions','joins','on_demand_calls','invocation_artifact_refs'):
        if contains(run.get(key)):
            return True
    for activation in run['activations']:
        if activation['id'] == old_id:
            continue
        for key in ('input_result_refs','authorized_artifact_refs','authorized_input_refs','node_result','result'):
            if contains(activation.get(key)):
                return True
    # A retained edge uses its immediate replacement as bounded direct proof.
    return include_lineage and any(other['replan_id'] not in removing and other['replan_id'] != record['replan_id'] and other['planned_activation_id'] == old_id for other in run['steering'].get('replans', []))


def _safe_predecessor(run, record, protected_tasks):
    if record['phase'] != 'replan_committed' or not all(record['journal'].values()) or not record.get('quiescence'):
        return False
    old = next((a for a in run['activations'] if a['id'] == record['withdrawal_witness']['activation_id']), None)
    if not old or old.get('status') != 'not_started' or old.get('worker_result_required') is not True or old.get('node_result') or old.get('result') or old.get('artifact_refs'):
        return False
    return all(t['status'] == 'not_started' and not t.get('result') and t['task_id'] not in protected_tasks for t in old['tasks'])


@storage_operation
def maintain(storage, run_id, *, reserve_text=0):
    from . import identity
    from .replan_certificates import publish, read
    from .steering_window import close_namespace, compact, retained_text_bytes, TEXT_LIMIT
    from .steering_lifecycle import _tree, resource_task_pins
    from .resources import ResourceLedger
    # Identity probes do not run under a root/tree/storage lock.
    snapshot = storage.get_run(run_id, refresh_atomic=True)
    if snapshot.get('parent_link') or not (snapshot.get('steering', {}).get('replans') or any(a.get('withdrawing_replan_id') for a in snapshot.get('activations', []))):
        return
    dead = set()
    for record in snapshot['steering'].get('replans', []):
        captured = record.get('source_dispatcher_identity')
        if captured and type(captured.get('start_time')) is str and captured.get('start_time') and record.get('source_job_id') and not record.get('quiescence') and identity.check_detail(captured)[0] == 'dead':
            dead.add(record['replan_id'])
    with storage.tree_mutation(run_id):
        run = storage.get_run(run_id, refresh_atomic=True)
        if dead:
            def observed(value):
                for record in value['steering']['replans']:
                    prior = next((r for r in snapshot['steering']['replans'] if r['replan_id'] == record['replan_id']), None)
                    if record['replan_id'] in dead and prior and not record.get('quiescence') and record.get('source_dispatcher_identity') == prior.get('source_dispatcher_identity') and record.get('source_job_id') == prior.get('source_job_id'):
                        w = record['withdrawal_witness']
                        record['quiescence'] = {'v': 1, 'origin': 'dispatcher_generation_dead', 'replan_id': record['replan_id'], 'activation_id': w['activation_id'], 'task_id': w['task_id'], 'dispatcher_identity': copy.deepcopy(record['source_dispatcher_identity']), 'job_id': record['source_job_id']}
            storage.update_run(run_id, observed, 'steering_replan_dispatcher_dead')
            run = storage.get_run(run_id, refresh_atomic=True)
        ledger = run['steering']
        tree = _tree(storage, run)
        pins = {v.get('task_id') for owner, _ in tree.values() for v in owner.get('sessions', {}).values() if type(v) is dict}
        resource_pins, unknown = resource_task_pins(storage, ResourceLedger(storage.root).snapshot(), tree)
        pins |= resource_pins
        if unknown:
            return
        pending = ledger.get('replan_compaction_pending')
        if pending:
            selected = set(pending['replan_ids'])
        else:
            eligible = [r for r in ledger['replans'] if _safe_predecessor(run, r, pins)]
            excess = max(0, len(eligible) - 32)
            selected = set()
            def fits(planned):
                trial = copy.deepcopy(run)
                chosen = [r for r in trial['steering']['replans'] if r['replan_id'] in planned]
                old = {r['withdrawal_witness']['activation_id'] for r in chosen if not _ordinary_references(run, r, planned)}
                trial['steering']['replans'][:] = [r for r in trial['steering']['replans'] if r['replan_id'] not in planned]
                trial['activations'][:] = [a for a in trial['activations'] if a['id'] not in old]
                close_namespace(trial)
                try:
                    compact(trial, reserve_text_bytes=96 * 1024 + reserve_text)
                except SteeringError:
                    return False
                return retained_text_bytes(trial) + reserve_text <= TEXT_LIMIT - 96 * 1024 and len(json.dumps(trial, ensure_ascii=False).encode()) <= 4 * 1024 * 1024 - 256 * 1024
            for record in eligible:
                if len(selected) >= excess and fits(selected):
                    break
                tentative = selected | {record['replan_id']}
                selected.add(record['replan_id'])
            # Write BOTH permanent indexes before any namespace or inline prune.
            for record in ledger['replans']:
                if record['replan_id'] in selected:
                    publish(storage, run, record)
            predecessor_ids = {r['withdrawal_witness']['activation_id'] for r in ledger['replans'] if r['replan_id'] in selected and not _ordinary_references(run, r, selected)}
            for activation in run['activations']:
                if not activation.get('withdrawing_replan_id') or any(r['withdrawal_witness']['activation_id'] == activation['id'] for r in ledger['replans']):
                    continue
                try:
                    cert = read(storage, predecessor_execution_id=activation['id'])
                    pseudo = {'replan_id': cert['replan_id'], 'phase': 'replan_committed', 'journal': {'ready': True}, 'quiescence': cert['quiescence'], 'withdrawal_witness': {'activation_id': activation['id']}}
                    if _safe_predecessor(run, pseudo, pins) and not _ordinary_references(run, pseudo, selected):
                        predecessor_ids.add(activation['id'])
                except (ValueError, OSError):
                    pass
            if not selected and not predecessor_ids:
                return
            from .workflow_steering import _digest
            certificate_refs = {}
            for record in ledger['replans']:
                if record['replan_id'] in selected:
                    cert = read(storage, replan_id=record['replan_id'])
                    certificate_refs[cert['replan_id']] = {'replan_id': cert['replan_id'], 'predecessor_execution_id': cert['predecessor_execution_id'], 'certificate_digest': _digest(cert)}
            for aid in predecessor_ids:
                cert = read(storage, predecessor_execution_id=aid)
                certificate_refs[cert['replan_id']] = {'replan_id': cert['replan_id'], 'predecessor_execution_id': aid, 'certificate_digest': _digest(cert)}
            plan_value = {'replan_ids': sorted(selected), 'predecessor_ids': sorted(predecessor_ids), 'certificates': list(certificate_refs.values())}
            # Preflight BOTH temporary-plan and eventual-survivor root reads with
            # exactly the reader's aggregate bound before a durable root change.
            proposal = copy.deepcopy(run)
            proposal['steering']['replan_compaction_pending'] = plan_value
            qualify_references(storage, proposal)
            survivors = copy.deepcopy(run)
            removed_records = [r for r in ledger['replans'] if r['replan_id'] in selected]
            survivors['steering']['replans'][:] = [r for r in survivors['steering']['replans'] if r['replan_id'] not in selected]
            for survivor in survivors['steering']['replans']:
                successor = next((r for r in removed_records if r['withdrawal_witness']['activation_id'] == survivor['planned_activation_id']), None)
                if successor:
                    cert = read(storage, replan_id=successor['replan_id'])
                    survivor['successor_certificate'] = {'replan_id': cert['replan_id'], 'predecessor_execution_id': cert['predecessor_execution_id'], 'predecessor_task_id': cert['predecessor_task_id'], 'replacement_execution_id': cert['replacement_execution_id'], 'replacement_task_id': cert['replacement_task_id'], 'certificate_digest': _digest(cert)}
            qualify_references(storage, survivors)
            def plan(value):
                close_namespace(value)
                value['steering']['replan_compaction_pending'] = copy.deepcopy(plan_value)
            storage.update_run(run_id, plan, 'steering_replan_namespace_closed')
            run = storage.get_run(run_id, refresh_atomic=True)
            ledger = run['steering']
        # Resume ONLY the exact durable plan; requalify every permanent edge.
        for record in ledger['replans']:
            if record['replan_id'] in selected:
                certificate = read(storage, replan_id=record['replan_id'])
                if certificate['root_workflow_run_id'] != run_id or not _safe_predecessor(run, record, pins):
                    raise SteeringError('replan_source_unavailable', 'Planned normal-history retirement lost its exact safety proof')
        pending_plan = ledger['replan_compaction_pending']
        for aid in pending_plan['predecessor_ids']:
            cert = read(storage, predecessor_execution_id=aid)
            pseudo = {'replan_id': cert['replan_id'], 'phase': 'replan_committed', 'journal': {'ready': True}, 'quiescence': cert['quiescence'], 'withdrawal_witness': {'activation_id': aid}}
            if cert['root_workflow_run_id'] != run_id or not _safe_predecessor(run, pseudo, pins) or _ordinary_references(run, pseudo, selected):
                raise SteeringError('replan_source_unavailable', 'Planned predecessor gained a protected session/resource/reference or lost its exact proof')
        def prune(value):
            led = value['steering']
            plan = led['replan_compaction_pending']
            if set(plan['replan_ids']) != selected:
                raise SteeringError('replan_source_unavailable', 'Normal-history retirement plan changed')
            records = [r for r in led['replans'] if r['replan_id'] in selected]
            ids = set(plan['predecessor_ids'])
            requests = {r['request_key'] for r in records}
            for key in requests:
                if key in led['requests']:
                    led['requests'][key]['retired_replan'] = True
            from .workflow_steering import _digest
            for survivor in led['replans']:
                if survivor['replan_id'] in selected:
                    continue
                successor = next((r for r in records if r['withdrawal_witness']['activation_id'] == survivor['planned_activation_id']), None)
                if successor:
                    certificate = read(storage, replan_id=successor['replan_id'])
                    survivor['successor_certificate'] = {'replan_id': certificate['replan_id'], 'predecessor_execution_id': certificate['predecessor_execution_id'], 'predecessor_task_id': certificate['predecessor_task_id'], 'replacement_execution_id': certificate['replacement_execution_id'], 'replacement_task_id': certificate['replacement_task_id'], 'certificate_digest': _digest(certificate)}
            value['activations'][:] = [a for a in value['activations'] if a['id'] not in ids]
            led['replans'][:] = [r for r in led['replans'] if r['replan_id'] not in selected]
            accounting = led['replan_accounting']
            accounting['retired_details'] += len(records)
            accounting['retired_never_started'] += len(ids)
            led['replan_history_incomplete'] = True
            led['normal_execution_history_incomplete'] = True
            led['replan_history_floor'] = min((r['sequence'] for r in led['replans']), default=accounting['accepted'] + 1) - 1
            led.pop('replan_compaction_pending')
            from .steering_window import compact
            compact(value, reserve_text_bytes=96 * 1024 + reserve_text)
        storage.update_run(run_id, prune, 'steering_replan_history_compacted')


def qualify_references(storage, run):
    from .replan_certificates import read
    from .workflow_steering import _digest
    budget = {'remaining': DIRECT_READ_BUDGET}
    cache = {}
    def acquire(replan_id):
        if replan_id not in cache:
            cache[replan_id] = read(storage, replan_id=replan_id, budget=budget)
        return cache[replan_id]
    for record in run.get('steering', {}).get('replans', []):
        reference = record.get('successor_certificate')
        if reference:
            certificate = acquire(reference['replan_id'])
            if certificate['root_workflow_run_id'] != run['workflow_run_id'] or _digest(certificate) != reference['certificate_digest'] or any(certificate[key] != reference[key] for key in ('predecessor_execution_id','predecessor_task_id','replacement_execution_id','replacement_task_id')):
                raise SteeringError('replan_source_unavailable', 'Direct successor proof no longer matches its exact permanent lineage')

    retained = {r['replan_id'] for r in run.get('steering', {}).get('replans', [])}
    for turn in run.get('steering', {}).get('turns', []):
        marker = turn.get('revoked_by_replan_id')
        if marker and marker not in retained:
            certificate = acquire(marker)
            target = turn['target']
            if (certificate['root_workflow_run_id'] != run['workflow_run_id'] or certificate['queued_turn_id'] != turn['turn_id']
                    or certificate['queued_task_id'] != turn['task_id'] or certificate['predecessor_execution_id'] != target.get('activation_id')):
                raise SteeringError('replan_source_unavailable', 'Retired response revocation contradicts permanent lineage')

    for reference in run.get('steering', {}).get('replan_compaction_pending', {}).get('certificates', []):
        certificate = acquire(reference['replan_id'])
        if certificate['root_workflow_run_id'] != run['workflow_run_id'] or certificate['predecessor_execution_id'] != reference['predecessor_execution_id'] or _digest(certificate) != reference['certificate_digest']:
            raise SteeringError('replan_source_unavailable', 'Pending prune certificate contradicts its exact durable reference')


def owns_current_execution(token):
    context = _job.get()
    if not context or not any(origin.get('replan_id') for origin in context['origins'].values()):
        return True
    return token.get('execution_activation_id') in {origin['activation_id'] for origin in context['origins'].values()}
