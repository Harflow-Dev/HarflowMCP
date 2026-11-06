"""Private fixture-only poststart authority embedded in the actual run CAS.

Persisted enrollment is evidence, never a capability to execute an adapter.
"""
from __future__ import annotations
import copy
import json
from dataclasses import asdict
import uuid
import weakref
import time
import threading
from . import poststart_schema as s
from .poststart_store import PoststartStore, Enrollment, initial_state, validate_context
from .poststart_control import SyntheticCoordinator

FORMAT = 'harflow.workflow.poststart'
_SCOPES = weakref.WeakValueDictionary()
_FRONTIERS = weakref.WeakKeyDictionary()

def _compact(value):
    return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False)


def _stored_document(value,kind):
    # Stored qualified documents retain their accepted compact-wire byte bound.
    return s.validate_document(_compact(value),kind)


class FixtureScope:
    """Nonserializable, explicitly supplied ownership of one fixture domain."""
    def __init__(self, storage, run_id, registry, adapter):
        from .tasks import TaskRegistry
        s.require(not isinstance(registry,TaskRegistry),'fixture_registry_required')
        s.require(getattr(adapter, 'synthetic', None) is True, 'synthetic_adapter_required')
        self.storage, self.directory = storage, str(storage.root.resolve())
        self.run_id, self.registry, self.adapter = run_id, registry, adapter
        domain=(self.directory,run_id)
        try:
            domains=_FRONTIERS.setdefault(adapter,{})
        except TypeError as exc:
            raise s.PoststartError('fixture_adapter_identity_required') from exc
        self._frontier=domains.setdefault(domain,dict(value=(0,0),lock=threading.RLock()))
        for key,existing in list(_SCOPES.items()):
            if key[:2]==domain:
                s.require(existing.adapter is adapter and existing.registry is registry,'fixture_domain_conflict')
        # Keep every independently held scope weakly: dropping a newer scope must
        # not erase protection still owned by an older scope for this domain.
        _SCOPES[(*domain,id(self))]=self
    def check(self, storage, run_id, registry=None):
        s.require(self.storage is storage and self.directory == str(storage.root.resolve()) and self.run_id == run_id and (registry is None or registry is self.registry), 'fixture_scope_required')
        s.require(getattr(self.adapter, 'synthetic', None) is True, 'synthetic_adapter_required')


def _invalidate_scope(scope, *, generation, evidence_revision):
    """Monotonic protection only; this frontier never mints an effect permit."""
    frontier=scope._frontier
    with frontier['lock']:
        value=max(frontier['value'],(generation,evidence_revision))
        # Remember even an uncertain invalidate acknowledgment conservatively.
        frontier['value']=value
        scope.adapter.invalidate(root=scope.run_id,generation=value[0],evidence_revision=value[1])
        return value


def _activation(run, aid):
    found = [a for a in run['activations'] if a['id'] == aid]
    s.require(len(found) == 1, 'actual_activation_binding')
    return found[0]


_TASK_CONFIGURATION=('candidate','repo_path','freedom','network','session_mode','execution_kind','dispatcher_identity','dispatcher_job_id')
_TASK_STATIC=set(_TASK_CONFIGURATION)-{'dispatcher_identity','dispatcher_job_id'}


def _task_configuration(task):
    return {key:copy.deepcopy(task.get(key)) for key in _TASK_CONFIGURATION}


def _task_job_binding(run,identity,job):
    activation=_activation(run,identity['activation'])
    tasks=[task for task in activation['tasks'] if task['task_id']==identity['task']]
    s.require(len(tasks)==1 and job is not None,'actual_task_job_binding')
    task=tasks[0]
    s.require(task.get('dispatcher_job_id')==job['job_id'] and isinstance(task.get('dispatcher_identity'),dict) and s.document_digest(task['dispatcher_identity'])==job['dispatcher_identity']==identity['owner_generation'],'actual_dispatcher_binding')
    return task


def _execution_refs(activation):
    refs=set()
    for record in (activation,activation.get('token') or {}):
        for key in ('input_result_refs','additional_result_refs','resolved_execution_refs','failed_execution_refs'):
            refs.update(record.get(key) or [])
        refs.update(record.get(key) for key in ('execution_activation_id','retry_of_execution_id','failed_execution_id'))
    refs.discard(None)
    return refs


def _ambiguous_active_scope(run,source):
    source_token=source.get('token') or {}
    source_ids={source['id'],source_token.get('id')}-{None}
    p=run.get('poststart')
    if p:
        for record in [p['ledger']]+p['ledger']['history']:
            source_ids.add(record['identity']['activation'])
            if record['replacement'] is not None:
                source_ids.add(record['replacement']['activation_id'])
    assigned=set(source.get('assigned_task_ids') or []) | set(source_token.get('assigned_task_ids') or [])
    source_refs=_execution_refs(source)
    for other in run['activations']:
        if other['id']==source['id'] or other['id'] in source_ids:
            continue
        active=any(task.get('status') in {'reserved','running','uncertain'} for task in other['tasks']) or bool(other.get('invocation') and other.get('status') in {'pending','running','uncertain'})
        if not active:
            continue
        token=other.get('token') or {}
        other_ids={other['id'],token.get('id')}-{None}
        other_assigned=set(other.get('assigned_task_ids') or []) | set(token.get('assigned_task_ids') or [])
        if other['node_id']==source['node_id'] or source_ids.intersection(other_ids) or assigned.intersection(other_assigned) or _execution_refs(other).intersection(source_ids) or source_refs.intersection(other_ids):
            return True
    return False


def projection(run, identity):
    """Read actual workflow fields; never reconstruct workflow state from ledger."""
    a = _activation(run, identity['activation'])
    tasks = [t for t in a['tasks'] if t.get('task_id', t.get('id')) == identity['task']]
    s.require(len(tasks) == 1, 'actual_task_binding')
    node = next((n for n in run['definition']['nodes'] if n['id'] == a['node_id']), None)
    s.require(node is not None, 'actual_node_binding')
    from .workflow_execution_policy import attempts_used, visit_id
    from .workflow_replan import assignment_values
    token = a.get('token') or {}
    return dict(scope_ambiguous=_ambiguous_active_scope(run,a), run_id=run['workflow_run_id'], activation_id=a['id'], task_id=identity['task'], node_id=a['node_id'], visit_id=visit_id(run, token), token_id=token.get('id'), assignment_prompt=a.get('assignment_prompt'), candidate=a.get('execution_candidate', tasks[0].get('candidate')), task_configuration=_task_configuration(tasks[0]), resource_attempt=copy.deepcopy(a.get('resource_attempt')), task_resource_attempt=copy.deepcopy(tasks[0].get('resource_attempt')), goal=run.get('goal'), goal_revision=run.get('goal_revision'), definition_hash=run.get('definition_hash'), assignment_values=assignment_values(a), session={k:copy.deepcopy(a.get(k)) for k in ('execution_session_mode','resume_task_id','continue_previous')}, definition_digest=s.document_digest(run['definition']), parent_goal=copy.deepcopy(run.get('parent_goal')), configuration={k: copy.deepcopy(run.get(k)) for k in ('repo_path','freedom','network','permission_policy','execution_policy','interaction_owner','interaction_caller','owner_contracts')}, node=copy.deepcopy(node), grants=copy.deepcopy(run.get('attempt_grants', {})), attempts_used=attempts_used(run,node,token), max_attempts=node.get('max_attempts',0)+run.get('attempt_grants',{}).get(node['id'],0), result_digest=s.document_digest(a['node_result']) if a.get('node_result') is not None else None)


def _anchor_projection(value):
    return {k:v for k,v in value.items() if k not in {'attempts_used','result_digest','activation_id','task_id'}}


def _runtime(value, historical=False):
    s.closed(value, {'origin_issue','resource_handoff','job','claim_exclusion','next_job','history'})
    s.require(type(value['claim_exclusion']) is bool,'runtime_claim')
    shapes = dict(next_job={'job_id','dispatcher_identity','activation_id','task_id','done'},origin_issue={'command_id','task_id','status','receipt'},resource_handoff={'reservation_id','names','old_attempt_id','coordinator_attempt_id','successor_attempt_id','old_generation','coordinator_generation','successor_generation','state'},job={'job_id','dispatcher_identity','activation_id','task_id','done'})
    for key, fields in shapes.items():
        item=value[key]
        if item is None:
            continue
        s.closed(item,fields)
        if key in {'job','next_job'}:
            s.require(type(item['done']) is bool,'job_done')
            s.require(all(type(item[k]) is str and item[k] for k in fields-{'done'}),'job_identity')
        elif key=='origin_issue':
            s.require(item['status'] in {'prepared','unknown','acknowledged'},'origin_status')
            s.require(all(type(item[k]) is str and item[k] for k in ('command_id','task_id')),'origin_identity')
            receipt=item['receipt']
            if receipt is not None:
                s.closed(receipt,{'command_id','kind','task_id','generation','root','evidence_revision'})
                s.counter(receipt['generation']);s.counter(receipt['evidence_revision'])
                s.require(receipt['kind']=='original' and receipt['command_id']==item['command_id'] and receipt['task_id']==item['task_id'] and type(receipt['root']) is str,'origin_receipt')
            s.require((item['status']=='acknowledged')==(receipt is not None),'origin_acknowledgement')
        else:
            s.require(item['state'] in {'coordinator_intent','coordinator_held','successor_intent','successor_held','unknown'},'resource_handoff_state')
            s.require(all(type(item[k]) is str and item[k] for k in ('reservation_id','old_attempt_id','coordinator_attempt_id')),'resource_handoff_identity')
            s.require(item['successor_attempt_id'] is None or type(item['successor_attempt_id']) is str and item['successor_attempt_id'],'resource_successor')
            s.counter(item['old_generation'])
            for k in ('coordinator_generation','successor_generation'):
                if item[k] is not None: s.counter(item[k])
            s.require(type(item['names']) is list and 0<len(item['names'])<=100 and all(type(v) is str and v for v in item['names']) and len(set(item['names']))==len(item['names']),'resource_names')
    s.require(type(value['history']) is list,'runtime_history')
    if historical:
        s.require(value['history']==[],'runtime_history_nested')
    else:
        for archive in value['history']:
            _runtime(archive,True)


def validate_settlement(value):
    s.closed(value,{'request_id','task_id','certificate_digest','evidence_revision','provenance'})
    s.identifier(value['request_id']);s.counter(value['evidence_revision']);s.digest(value['certificate_digest'])
    s.require(type(value['task_id']) is str and value['task_id'] and value['provenance']=='synthetic_poststart_settlement','settlement_metadata')


def _ra_anchor(ra):
    if ra is None:
        return None
    s.require(type(ra) is dict,'resource_attempt_binding')
    s.counter(ra.get('poststart_grant_generation'))
    return {key:copy.deepcopy(ra.get(key)) for key in ('id','reservation_id','names','task_id','candidate_index','granted_at','borrowed_from','poststart_grant_generation')}


def _validate_journal(run,runtime,record,source_ra):
    journal=runtime['resource_handoff']
    actual=_activation(run,record['identity']['activation']).get('resource_attempt')
    s.require(_ra_anchor(actual)==_ra_anchor(source_ra),'source_resource_anchor')
    if journal is None:
        if record['intent'] is not None and runtime['claim_exclusion']:
            s.require(source_ra is None,'resource_journal_missing')
        return
    s.require(source_ra is not None and record['intent'] is not None,'resource_journal_source')
    s.require(journal['reservation_id']==source_ra['reservation_id'] and journal['old_attempt_id']==source_ra['id'] and journal['names']==source_ra['names'] and journal['old_generation']==source_ra['poststart_grant_generation'],'resource_journal_anchor')
    expected=str(uuid.uuid5(uuid.UUID(record['intent']['request_id']),'coordinator_resource'))
    s.require(journal['coordinator_attempt_id']==expected,'resource_coordinator_binding')
    old=journal['old_generation'];phase=journal['state']
    if journal['coordinator_generation'] is not None:
        s.require(journal['coordinator_generation']==old+1,'resource_coordinator_generation')
    if journal['successor_generation'] is not None:
        s.require(journal['successor_generation']==old+2,'resource_successor_generation')
    if phase=='coordinator_intent':
        s.require(journal['coordinator_generation'] is None and journal['successor_attempt_id'] is None and journal['successor_generation'] is None,'resource_intent_phase')
    elif phase=='coordinator_held':
        s.require(journal['coordinator_generation']==old+1 and journal['successor_attempt_id'] is None and journal['successor_generation'] is None,'resource_coordinator_phase')
    if journal['successor_attempt_id'] is not None:
        replacement=record['replacement']
        s.require(replacement is not None,'resource_successor_receipt')
        successor_id=str(uuid.uuid5(uuid.UUID(replacement['request_id']),'successor_resource'))
        s.require(journal['successor_attempt_id']==successor_id,'resource_successor_binding')
        successor=_activation(run,replacement['activation_id']).get('resource_attempt')
        if successor is not None:
            s.require(successor['id']==successor_id and successor['reservation_id']==journal['reservation_id'] and successor['names']==journal['names'] and successor.get('poststart_grant_generation')==old+2,'resource_successor_anchor')
        if phase=='successor_held':
            s.require(successor is not None and journal['successor_generation']==old+2,'resource_successor_held')
    if phase in {'successor_intent','successor_held'}:
        s.require(journal['coordinator_generation']==old+1 and journal['successor_attempt_id'] is not None,'resource_successor_phase')


def _validate_settlements(run):
    p=run['poststart'];proofs=p['settlements']
    s.require(type(proofs) is dict,'settlement_proofs')
    for request_id,proof in proofs.items():
        s.identifier(request_id)
        s.closed(proof,{'identity','intent','evidence','certificate','job','resource_attempt'})
        s.validate_identity(proof['identity'])
        intent=_stored_document(proof['intent'],'contain_request')
        evidence=_stored_document(proof['evidence'],'certificate')
        certificate=proof['certificate']
        s.closed(certificate,{'digest','revision','generation'})
        s.counter(certificate['revision']);s.counter(certificate['generation']);s.digest(certificate['digest'])
        s.require(intent['request_id']==request_id==evidence['contain_request_id'] and intent['identity']==evidence['identity']==proof['identity'] and certificate['digest']==s.document_digest(evidence) and s.evidence_complete(evidence),'settlement_certificate_binding')
        s.require(intent['ledger_epoch']==evidence['ledger_epoch']==p['enrollment']['epoch'] and intent['identity']['root']==run['workflow_run_id'] and intent['profile']==p['ledger']['profile'] and evidence['effect_verifier']==p['ledger']['effect_verifier'],'settlement_enrollment_binding')
        job=proof['job']
        s.closed(job,{'job_id','dispatcher_identity','activation_id','task_id','done'})
        s.require(job['done'] is True and job['activation_id']==proof['identity']['activation'] and job['task_id']==proof['identity']['task'],'settlement_job_binding')
        _task_job_binding(run,proof['identity'],job)
        s.require(_ra_anchor(_activation(run,proof['identity']['activation']).get('resource_attempt'))==_ra_anchor(proof['resource_attempt']),'settlement_resource_anchor')
        activation=_activation(run,proof['identity']['activation'])
        metadata=activation.get('poststart_settlement')
        validate_settlement(metadata)
        s.require(metadata==dict(request_id=request_id,task_id=proof['identity']['task'],certificate_digest=certificate['digest'],evidence_revision=certificate['revision'],provenance='synthetic_poststart_settlement'),'settlement_activation_binding')
        s.require(activation['status']=='cancelled' and all(task['status'] not in {'reserved','running','uncertain'} for task in activation['tasks'] if task['task_id']==proof['identity']['task']),'settlement_control_disposition')
        source=next((record for record in [p['ledger']]+p['ledger']['history'] if record['identity']==proof['identity']),None)
        s.require(source is not None and source['receipts'].get(request_id,{}).get('request')==intent,'settlement_retained_intent')
    for activation in run['activations']:
        if 'poststart_settlement' in activation:
            validate_settlement(activation['poststart_settlement'])
            s.require(activation['poststart_settlement']['request_id'] in proofs,'settlement_proof_missing')


def has_markers(run):
    for activation in run.get('activations',[]):
        if any(key.startswith('poststart_') for key in activation):
            return True
        attempts=[activation.get('resource_attempt')]
        for task in activation.get('tasks',[]):
            if str(task.get('dispatch_stage','')).startswith('poststart_') or any(key.startswith('poststart_') for key in task):
                return True
            attempts.append(task.get('resource_attempt'))
        if any(isinstance(ra,dict) and 'poststart_grant_generation' in ra for ra in attempts):
            return True
    return False


def validate_run(run):
    if 'poststart' not in run:
        s.require(not has_markers(run),'poststart_enrollment_missing')
        return
    p=run['poststart']
    s.closed(p,{'format','v','enrollment','ledger','source','stale','original_launch','runtime','settlements'})
    s.require(p['format']==FORMAT and type(p['v']) is int and p['v']==1 and type(p['stale']) is bool,'embedded_format')
    s.closed(p['enrollment'],set(Enrollment.__dataclass_fields__))
    handle=Enrollment(**p['enrollment'])
    s.require(handle.mode=='synthetic' and all(type(getattr(handle,k)) is str and getattr(handle,k) for k in ('directory','root','adapter_id')),'enrollment_shape')
    s.counter(handle.epoch);s.counter(handle.initial_attempt_allowance);s.identifier(handle.nonce)
    s.digest(handle.immutable_digest);s.digest(handle.initial_source_digest)
    # Validation is pure: constructing the validator does not access any storage.
    validator=object.__new__(PoststartStore)
    validator.metadata_limit=4194304
    validator.RESERVE=WorkflowPoststartStore.RESERVE
    validator._validate(p['ledger'],handle)
    s.closed(p['original_launch'],{'activation_id','task_id','job_id','dispatcher_identity'})
    s.require(all(type(v) is str and v for v in p['original_launch'].values()),'launch_marker')
    s.require(handle.root==run['workflow_run_id'],'embedded_root')
    actual=projection(run,p['ledger']['identity'])
    s.closed(p['source'],set(actual))
    s.require(p['source']['run_id']==run['workflow_run_id'],'source_root')
    identity=p['ledger']['identity'];source=p['source']
    s.require(identity['activation']==source['activation_id'] and identity['task']==source['task_id'] and identity['goal_revision']==(source['goal_revision'] if source['goal_revision'] is not None else 0) and identity['node']==source['node_id'] and identity['visit']==source['visit_id'] and identity['candidate']==s.document_digest(source['candidate']),'actual_identity_binding')
    bindings=dict(definition_digest=source['definition_digest'],assignment_digest=s.document_digest(source['assignment_values']),configuration_digest=s.document_digest(source['configuration']),grant_digest=s.document_digest(source['grants']),goal_digest=s.document_digest(dict(goal=source['goal'],goal_revision=source['goal_revision'],parent_goal=source['parent_goal'])))
    s.require(all(identity[k]==v for k,v in bindings.items()),'actual_enrollment_digests')
    s.require(p['ledger']['assignment']==source['assignment_values'],'actual_assignment_binding')
    initial=(p['ledger']['history'] or [p['ledger']])[0]['identity']
    s.require(p['original_launch']['activation_id']==initial['activation'] and p['original_launch']['task_id']==initial['task'],'original_launch_binding')
    if not p['stale']:
        s.require(_anchor_projection(actual)==_anchor_projection(p['source']),'actual_source_changed')
    s.require(p['ledger']['result']==actual['result_digest'],'actual_result_binding')
    if p['stale']:
        s.require(p['ledger']['context']['enrolled'] is False and p['ledger']['context']['configuration'] is False and p['ledger']['context']['budget'] is False,'stale_enrollment_authority')
    expected_used=p['source']['max_attempts']-handle.initial_attempt_allowance+p['ledger']['control_issuances']
    if not p['stale']:
        s.require(actual['attempts_used']==expected_used,'actual_cumulative_accounting')
        s.require(actual['max_attempts']-actual['attempts_used']==p['ledger']['remaining_attempts'],'actual_remaining_accounting')
    _validate_settlements(run)
    _runtime(p['runtime'])
    _validate_journal(run,p['runtime'],p['ledger'],p['source']['resource_attempt'])
    s.require(_ra_anchor(actual['resource_attempt'])==_ra_anchor(actual['task_resource_attempt']),'actual_task_resource_anchor')
    for archive in p['runtime']['history']:
        job=archive['job']
        record=next((record for record in p['ledger']['history'] if job is not None and record['identity']['activation']==job['activation_id'] and record['identity']['task']==job['task_id']),None)
        s.require(record is not None and record['intent'] is not None,'archived_resource_source')
        proof=p['settlements'].get(record['intent']['request_id'])
        s.require(proof is not None,'archived_resource_proof')
        _validate_journal(run,archive,record,proof['resource_attempt'])
    if p['runtime']['job'] is not None:
        s.require(p['runtime']['job']['activation_id']==identity['activation'] and p['runtime']['job']['task_id']==identity['task'],'runtime_job_binding')
        _task_job_binding(run,identity,p['runtime']['job'])
    jobs=[p['runtime']['job']]+[archive['job'] for archive in p['runtime']['history']]
    s.require(any(job is not None and {k:job[k] for k in p['original_launch']}==p['original_launch'] for job in jobs),'original_job_history_binding')
    if p['runtime']['origin_issue'] is not None:
        s.require(p['runtime']['origin_issue']['task_id']==p['original_launch']['task_id'],'origin_task_binding')
    s.require(len(s.canonical({'poststart':p,'steering':run.get('steering',{})}))<=4194304 and PoststartStore.text_size({'poststart':p,'steering':run.get('steering',{})})<=1048576,'embedded_capacity')
    s.require(len(s.canonical(run))<=8388608,'workflow_proof_read_budget')


def retired(run, aid, tid=None):
    p=run.get('poststart')
    if not p:
        return False
    l=p['ledger']
    return (aid==l['identity']['activation'] and l['fence']) or (tid is not None and tid in l['retired_tasks']) or any(aid==h['identity']['activation'] for h in l['history'])


def controlled_activation(run,aid):
    p=run.get('poststart')
    if not p:
        return False
    ledger=p['ledger']
    identities=[ledger['identity']]+[h['identity'] for h in ledger['history']]
    replacements=[ledger.get('replacement')]+[h.get('replacement') for h in ledger['history']]
    return any(aid==i['activation'] for i in identities) or any(r is not None and aid==r['activation_id'] for r in replacements)


def blocked_token(run, token):
    p=run.get('poststart')
    if not p or not p['ledger']['fence']:
        return False
    if not isinstance(token,dict):
        return token==p['source']['token_id']
    old={p['ledger']['identity']['activation']} | {h['identity']['activation'] for h in p['ledger']['history']}
    refs=[]
    for key in ('input_result_refs','additional_result_refs','failed_execution_refs','resolved_execution_refs'):
        refs.extend(token.get(key) or [])
    refs.extend(token.get(key) for key in ('execution_activation_id','retry_of_execution_id','failed_execution_id'))
    replacement=p['ledger']['replacement']
    if replacement and token.get('execution_activation_id')==replacement['activation_id']:
        return False
    return token.get('id')==p['source']['token_id'] or bool(old.intersection(refs))


def auth_stamp(run):
    keys=('interaction_caller','interaction_owner','interaction_epoch','status','input_question','pending_inputs','pending_attention','supervisor_identity','goal_revision','definition','repo_path','freedom','network','permission_policy','attempt_grants','retry_grants','execution_policy')
    return s.document_digest({k:run.get(k) for k in keys})


def _retired_callback(run,event,detail):
    if event not in {'task_state','node_result_ready','result_reconnected'}:
        return False
    aid=detail if isinstance(detail,str) else detail.get('activation_id') if isinstance(detail,dict) else None
    tid=detail.get('task_id') if isinstance(detail,dict) else None
    return retired(run,aid,tid)


def _successor_reservation(run,event,detail):
    if event!='node_assignment_reserved' or not isinstance(detail,dict):
        return False
    p=run.get('poststart');replacement=p['ledger']['replacement'] if p else None
    if replacement is None or detail.get('activation_id')!=replacement['activation_id']:
        return False
    successor=_activation(run,replacement['activation_id'])
    from .workflow_replan import assignment_values
    s.require(successor.get('planned_task_id')==replacement['task_id'] and len(successor['tasks'])==1 and successor['tasks'][0]['task_id']==replacement['task_id'],'successor_reservation_binding')
    s.require(assignment_values(successor)==p['ledger']['assignment'] and successor.get('execution_candidate',successor['tasks'][0].get('candidate'))==p['source']['candidate'],'successor_assignment_binding')
    s.require(any(t['id']==p['source']['token_id'] and t.get('execution_activation_id')==successor['id'] for t in run['pending']),'successor_token_binding')
    return True


def prepare_update(storage,observed,event,detail=None):
    """Protectively fence delayed fixture effects before entering workflow locks."""
    storage._poststart_owned_directory=str(storage.root.resolve())
    if 'poststart' not in observed or event.startswith('poststart:'):
        return
    if _successor_reservation(observed,event,detail):
        return
    if _retired_callback(observed,event,detail):
        return
    domain=(str(storage.root.resolve()),observed['workflow_run_id'])
    adapters=set()
    for key,scope in list(_SCOPES.items()):
        if key[:2]!=domain or id(scope.adapter) in adapters:
            continue
        adapters.add(id(scope.adapter))
        # Domain lookup permits protective invalidation only. It cannot supply
        # the exact storage/registry-bound capability required for any effect.
        ledger=observed['poststart']['ledger']
        _invalidate_scope(scope,generation=ledger['context']['generation']+1,evidence_revision=ledger['evidence_revision'])


_UNSETTLED_TURNS={'queued','reserved','spawn_intent','running','unknown','captured'}


def _affected_steering_target(run,target):
    """Only a positively disjoint local sibling worker is outside the hold."""
    if not isinstance(target,dict) or target.get('role')!='worker':
        return True
    if target.get('workflow_run_id')!=run['workflow_run_id']:
        return True
    p=run['poststart'];ledger=p['ledger']
    identities=[ledger['identity']]+[h['identity'] for h in ledger['history']]
    affected={identity['activation'] for identity in identities}
    for record in [ledger]+ledger['history']:
        if record['replacement'] is not None:
            affected.add(record['replacement']['activation_id'])
    aid=target.get('activation_id')
    if aid in affected or target.get('node_id')==p['source']['node_id'] or target.get('token_id')==p['source']['token_id']:
        return True
    candidates=[a for a in run['activations'] if a['id']==aid]
    if len(candidates)!=1:
        return True
    activation=candidates[0]
    if activation['node_id']==p['source']['node_id']:
        return True
    refs=[]
    for record in (activation,activation.get('token') or {}):
        for key in ('input_result_refs','additional_result_refs','resolved_execution_refs','failed_execution_refs'):
            refs.extend(record.get(key) or [])
        refs.extend(record.get(key) for key in ('execution_activation_id','retry_of_execution_id','failed_execution_id'))
    if affected.intersection(refs):
        return True
    assigned=set(p['source']['assignment_values']['assigned_task_ids'])
    return bool(assigned.intersection(activation.get('assigned_task_ids') or []))


def _affected_steering_progress(before,run):
    previous={turn['turn_id']:turn for turn in before.get('steering',{}).get('turns',[])}
    for turn in run.get('steering',{}).get('turns',[]):
        old=previous.get(turn['turn_id'])
        if old==turn:
            continue
        affected=_affected_steering_target(run,turn.get('target')) or (old is not None and _affected_steering_target(before,old.get('target')))
        if not affected:
            continue
        if old is None:
            return True
        if any(old.get(key)!=turn.get(key) for key in ('task_id','target','commit_target')):
            return True
        if old.get('state') in _UNSETTLED_TURNS or turn.get('state') in _UNSETTLED_TURNS:
            return True
    return False


def before_update(storage,run,event,detail=None):
    if 'poststart' not in run:
        return
    validate_run(run)
    if event.startswith('poststart:'):
        return
    p=run['poststart']
    s.require(p['enrollment']['directory']==getattr(storage,'_poststart_owned_directory',None),'embedded_directory_binding')
    if not _retired_callback(run,event,detail) and not _successor_reservation(run,event,detail):
        domain=(p['enrollment']['directory'],run['workflow_run_id'])
        required=(p['ledger']['context']['generation']+1,p['ledger']['evidence_revision'])
        # Read-only comparison under workflow locks. The matching adapter fence
        # must already have advanced outside those locks; a stale preparation
        # refuses here and can only be retried from a fresh observed run.
        frontiers=[scope._frontier['value'] for key,scope in list(_SCOPES.items()) if key[:2]==domain]
        s.require(bool(frontiers) and max(frontiers)>=required,'fixture_protective_frontier_required')
    aid=(detail or {}).get('activation_id') if isinstance(detail,dict) else None
    tid=(detail or {}).get('task_id') if isinstance(detail,dict) else None
    s.require(_retired_callback(run,event,detail) or not retired(run,aid,tid),'poststart_retired')
    if p['ledger']['intent'] is not None:
        s.require(event not in {'control:resume','control:recover','control:cancel','control:revise_goal','scheduler_recovery_requested','decision_accepted'},'poststart_held')


def after_update(storage,before,run,event,detail=None):
    if 'poststart' not in before:
        if 'poststart' in run:
            validate_run(run)
        return
    p=run.get('poststart')
    s.require(p is not None,'enrollment_removed')
    steering_progress=_affected_steering_progress(before,run)
    if before['poststart']['ledger']['intent'] is not None:
        s.require(not steering_progress,'poststart_future_steering_fenced')
    if event.startswith('poststart:'):
        old=before['poststart']
        s.require(p['enrollment']==old['enrollment'] and p['original_launch']==old['original_launch'],'enrollment_anchor_changed')
        s.require(set(old['settlements'])<=set(p['settlements']),'settlement_proof_removed')
        if p['runtime']!=old['runtime'] and p['ledger']['version']==old['ledger']['version']:
            p['ledger']['version']+=1
        validate_run(run)
        return
    s.require(p==before['poststart'],'embedded_ordinary_mutation')
    old=before['poststart']; actual=projection(run,p['ledger']['identity'])
    if old['ledger']['intent'] is not None:
        # Independent sibling results are permitted, protected source is exact.
        a=p['ledger']['identity']['activation']
        s.require(_activation(before,a)==_activation(run,a) and actual==projection(before,p['ledger']['identity']),'poststart_source_held')
        token_id=p['source']['token_id']
        s.require([t for t in before['pending'] if t['id']==token_id]==[t for t in run['pending'] if t['id']==token_id],'poststart_token_held')
    if _retired_callback(before,event,detail):
        s.require(all(run.get(k)==before.get(k) for k in ('activations','pending','joins','tasks','sessions','status','summary','reason','attention_reason')),'retired_callback_mutated')
        validate_run(run)
        return
    if _successor_reservation(before,event,detail):
        s.require(_successor_reservation(run,event,detail),'successor_reservation_binding')
        replacement=p['ledger']['replacement'];aid=replacement['activation_id']
        prior=_activation(before,aid);current=_activation(run,aid)
        s.require(current['status']=='running' and current.get('attempt_in_visit')==p['source']['max_attempts']-p['enrollment']['initial_attempt_allowance']+p['ledger']['control_issuances'] and auth_stamp(run)==auth_stamp(before),'successor_reservation_authority')
        allowed={'status','visit_id','attempt_in_visit','assignment_prompt','assigned_task_ids','input_result_refs','execution_session_mode','continue_previous','resume_task_id','session_reason','authorized_artifact_refs','protocol_repair_of'}
        s.require({k:v for k,v in prior.items() if k not in allowed}=={k:v for k,v in current.items() if k not in allowed},'successor_reservation_mutation')
        validate_run(run)
        return
    changed=steering_progress or _anchor_projection(actual)!=_anchor_projection(p['source']) or actual['attempts_used']!=p['source']['max_attempts']-p['enrollment']['initial_attempt_allowance']+p['ledger']['control_issuances']
    p['stale']=p['stale'] or changed
    p['ledger']['result']=actual['result_digest']
    if changed:
        p['ledger']['context'].update(configuration=False,budget=False,enrolled=False)
    p['ledger']['context']['generation']+=1
    p['ledger']['certificate']=None
    p['ledger']['release']=None
    if p!=old:
        p['ledger']['version']+=1
    validate_run(run)
    check_capacity(run)


def check_capacity(run, additional_bytes=0, additional_text=0):
    p=run.get('poststart')
    if p:
        reserve=p['ledger']['reservation']
        shared={'poststart':p,'steering':run.get('steering',{})}
        s.require(len(s.canonical(shared))+additional_bytes+reserve<=4194304 and PoststartStore.text_size(shared)+additional_text+reserve<=1048576,'poststart_capacity')
        s.require(len(s.canonical(run))+additional_bytes<=8388608,'workflow_proof_read_budget')


def check_admission_capacity(run,request):
    """Reserve a conservative complete certificate/replacement/archive envelope.

    Two existing payload copies cover retained cumulative ledger/runtime history.
    Seven contain and two replacement copies cover new permanent receipts/proofs.
    Five maximum-size certificate copies cover observation, settlement retention,
    accepted replacement evidence and source archival. Fixed control metadata has
    a separate 64 KiB allowance. This may refuse before the hard document limits.
    """
    p=run['poststart'];ledger=p['ledger']
    request_id='00000000-0000-0000-0000-000000000000'
    if request_id==request['request_id']:
        request_id='00000000-0000-0000-0000-000000000001'
    replacement=dict(s.COMMON,kind='replacement_request',ledger_epoch=ledger['enrollment']['epoch'],identity=ledger['identity'],request_id=request_id,contain_request_id=request['request_id'],certificate_digest='0'*64,effect_verifier=ledger['effect_verifier'],expected_remaining_attempts=ledger['remaining_attempts'])
    _stored_document(replacement,'replacement_request')
    steering=run.get('steering',{})
    fixed_text=5*s.MAX_DOCUMENT+65536
    # Closed evidence has at most100 inventory entries; doubling compact bytes
    # conservatively covers its bounded pretty-print structure and indentation.
    fixed_metadata=10*s.MAX_DOCUMENT+65536
    metadata=2*len(s.canonical(p))+7*len(s.canonical(request))+2*len(s.canonical(replacement))+len(s.canonical(steering))+fixed_metadata
    text=2*PoststartStore.text_size(p)+7*PoststartStore.text_size(request)+2*PoststartStore.text_size(replacement)+PoststartStore.text_size(steering)+fixed_text
    s.require(metadata<=4194304 and text<=1048576,'poststart_completion_envelope')


class WorkflowPoststartStore(PoststartStore):
    RESERVE=786432
    def __init__(self,workflow_store,run_id,scope,**kwargs):
        super().__init__(workflow_store,**kwargs)
        s.require(type(scope) is FixtureScope,'fixture_scope_required')
        scope.check(workflow_store,run_id)
        self.run_id,self.scope=run_id,scope
    def initialize(self,identity,*,adapter_id,profile,effect_verifier,assignment,remaining_attempts,context,original_launch,runtime,enrollment_mutator=None):
        s.validate_identity(identity);s.validate_profile(profile);s.validate_verifier(effect_verifier);validate_context(context);s.bounded(assignment)
        s.require(adapter_id==self.scope.adapter.adapter_id and identity['root']==self.run_id,'enrollment_binding')
        s.require(profile['digest']==identity['profile_digest'] and effect_verifier['digest']==identity['verifier_digest'] and s.document_digest(assignment)==identity['assignment_digest'],'enrollment_binding')
        s.counter(remaining_attempts)
        handle=Enrollment(str(self.workflow_store.root.resolve()),self.run_id,0,'synthetic',adapter_id,str(uuid.uuid4()),remaining_attempts,s.document_digest(dict(identity={k:v for k,v in identity.items() if k not in {'task','attempt','launch_id','activation','owner_generation','containment_generation','process_generation'}},profile=profile,effect_verifier=effect_verifier,assignment=assignment)),s.document_digest(identity))
        def apply(run):
            s.require('poststart' not in run,'already_enrolled')
            if enrollment_mutator is not None: enrollment_mutator(run)
            actual=projection(run,identity)
            s.require(actual['scope_ambiguous'] is False,'poststart_scope_ambiguous')
            s.require(not any(a['id']!=identity['activation'] and a['node_id']==actual['node_id'] and any(t.get('status') in {'reserved','running','uncertain'} for t in a['tasks']) for a in run['activations']),'affected_second_provider')
            s.require(actual['result_digest'] is None and actual['max_attempts']-actual['attempts_used']==remaining_attempts,'actual_enrollment_budget')
            s.require(original_launch['activation_id']==identity['activation'] and original_launch['task_id']==identity['task'],'launch_binding')
            run['poststart']=dict(format=FORMAT,v=1,enrollment=asdict(handle),ledger=initial_state(handle,identity,profile,effect_verifier,assignment,remaining_attempts,context),source=actual,stale=False,original_launch=copy.deepcopy(original_launch),runtime=copy.deepcopy(runtime),settlements={})
            validate_run(run)
        self.workflow_store.update_run(self.run_id,apply,'poststart:enroll')
        self.handle=handle
        return handle
    def reopen(self,handle):
        self.scope.check(self.workflow_store,self.run_id)
        s.require(type(handle) is Enrollment and handle.directory==str(self.workflow_store.root.resolve()) and handle.root==self.run_id,'enrollment_mismatch')
        self.handle=handle
        self.snapshot()
        return self
    def snapshot(self):
        self.scope.check(self.workflow_store,self.run_id)
        run=self.workflow_store.get_run(self.run_id)
        validate_run(run)
        s.require(self.handle is not None and run.get('poststart',{}).get('enrollment')==asdict(self.handle),'enrollment_mismatch')
        return copy.deepcopy(run['poststart']['ledger'])
    def runtime_update(self,mutator):
        def apply(run):
            self.scope.check(self.workflow_store,self.run_id)
            validate_run(run)
            previous=copy.deepcopy(run['poststart']['runtime'])
            mutator(run['poststart']['runtime'])
            if previous!=run['poststart']['runtime']:
                run['poststart']['ledger']['version']+=1
                if run['poststart']['runtime']['next_job'] is not None and previous['next_job'] is None:
                    p=run['poststart'];replacement=p['ledger']['replacement'];job=p['runtime']['next_job']
                    s.require(replacement is not None and replacement['request_id'] in p['ledger']['replacement_proofs'] and job['done'] is False and job['activation_id']==replacement['activation_id'] and job['task_id']==replacement['task_id'],'reserved_successor_job')
                    activation=_activation(run,replacement['activation_id'])
                    s.require(activation.get('planned_task_id')==replacement['task_id'] and any(token['id']==p['source']['token_id'] and token.get('execution_activation_id')==activation['id'] for token in run['pending']),'reserved_successor_job')
                    # This exact linked-job checkpoint is completion of the already
                    # admitted request. It consumes the reserved envelope; the
                    # hard shared limits below still qualify the whole candidate.
            validate_run(run)
        return self.workflow_store.update_run(self.run_id,apply,'poststart:runtime')
    def transact(self,operation,*,expected_version=None,preserve_reserve=False):
        result=[]
        def apply(run):
            self.scope.check(self.workflow_store,self.run_id)
            validate_run(run)
            p=run['poststart'];state=p['ledger']
            s.require(p['enrollment']==asdict(self.handle),'enrollment_mismatch')
            s.require(expected_version is None or state['version']==expected_version,'stale_version')
            previous=copy.deepcopy(state)
            actual=projection(run,state['identity'])
            # Actual remaining allowance is admission authority, never a stale copy.
            if state['intent'] is None and not p['stale']:
                s.require(actual['max_attempts']-actual['attempts_used']==state['remaining_attempts'],'actual_attempt_budget')
            result.append(copy.deepcopy(operation(state)))
            privileged=(state['intent']!=previous['intent'] or state['replacement']!=previous['replacement'] or any(c['status'] in {'prepared','unknown'} and c!=previous['commands'].get(k) for k,c in state['commands'].items()))
            if privileged:
                s.require(not actual['scope_ambiguous'],'poststart_scope_ambiguous')
                s.require(getattr(self.scope,'_auth_stamp',None)==auth_stamp(run),'fresh_authority_required')
            dispatch=state['commands'].get('dispatch')
            if dispatch is not None and dispatch['status'] in {'prepared','unknown'} and dispatch!=previous['commands'].get('dispatch'):
                runtime=p['runtime'];journal=runtime['resource_handoff']
                next_job=runtime['next_job'];replacement=state['replacement']
                s.require(next_job is not None and replacement is not None,'successor_job_binding')
                successor_identity=dict(activation=replacement['activation_id'],task=replacement['task_id'],owner_generation=next_job['dispatcher_identity'])
                successor_task=_task_job_binding(run,successor_identity,next_job)
                successor_cfg=_task_configuration(successor_task)
                s.require({key:successor_cfg[key] for key in _TASK_STATIC}=={key:p['source']['task_configuration'][key] for key in _TASK_STATIC},'successor_task_configuration')
                s.require(runtime['claim_exclusion'] is True,'successor_exclusion_required')
                if journal is not None:
                    s.require(journal['state']=='successor_held','successor_resources_unsettled')
                else:
                    s.require(not _activation(run,state['identity']['activation']).get('resource_attempt') and not p['source']['node'].get('resources'),'successor_resources_unsettled')
            if p['stale']:
                s.require(state['context']['enrolled'] is False and state['context']['configuration'] is False and state['context']['budget'] is False,'stale_enrollment_authority')
            if state!=previous:
                if state['certificate'] is not None and state['certificate']!=previous['certificate']:
                    self._publish_settlement(run,state)
                if previous['intent'] is None and state['intent'] is not None:
                    candidate=copy.deepcopy(run)
                    candidate['poststart']['ledger']=previous
                    check_admission_capacity(candidate,state['intent'])
                    state['context']['generation']+=1
                    check_capacity(run)
                if state['replacement'] != previous['replacement']:
                    if previous['replacement'] is None and state['replacement'] is not None:
                        self._commit_replacement(run, previous, state)
                    else:
                        s.require(previous['replacement'] is not None and state['replacement'] is None and state['identity']!=previous['identity'] and previous['commands'].get('dispatch',{}).get('status')=='acknowledged' and len(state['history'])==len(previous['history'])+1,'replacement_conflict')
                if state['identity'] != previous['identity']:
                    # advance_source is allowed only through existing acknowledged lineage.
                    s.require(previous['replacement'] is not None and state['identity']['activation']==previous['replacement']['activation_id'],'actual_successor_binding')
                    rt=p['runtime'];next_job=rt['next_job']
                    s.require(next_job is not None and next_job['done'] is False and next_job['activation_id']==state['identity']['activation'] and next_job['task_id']==state['identity']['task'],'successor_job_binding')
                    archived=copy.deepcopy(rt);archived['history']=[]
                    rt['history'].append(archived)
                    rt.update(job=next_job,next_job=None,resource_handoff=None,claim_exclusion=False)
                    p['source']=projection(run,state['identity'])
                state['version']+=1
                if preserve_reserve: check_capacity(run)
            validate_run(run)
        self.workflow_store.update_run(self.run_id,apply,'poststart:transaction')
        return result[0]

    def _publish_settlement(self,run,state):
        p=run['poststart'];job=p['runtime']['job']
        s.require(state['intent'] is not None and state['evidence'] is not None and s.evidence_complete(state['evidence']) and job is not None and job['done'] is True and job['activation_id']==state['identity']['activation'] and job['task_id']==state['identity']['task'],'settlement_requires_whole_job')
        _task_job_binding(run,state['identity'],job)
        certificate=state['certificate'];request_id=state['intent']['request_id']
        p['settlements'][request_id]=copy.deepcopy(dict(identity=state['identity'],intent=state['intent'],evidence=state['evidence'],certificate=certificate,job=job,resource_attempt=p['source']['resource_attempt']))
        activation=_activation(run,state['identity']['activation'])
        activation['status']='cancelled'
        for task in activation['tasks']:
            if task['task_id']==state['identity']['task'] and task['status'] not in {'completed','failed','cancelled','not_started'}:
                task['status']='cancelled'
        activation['poststart_settlement']=dict(request_id=request_id,task_id=state['identity']['task'],certificate_digest=certificate['digest'],evidence_revision=certificate['revision'],provenance='synthetic_poststart_settlement')

    def _commit_replacement(self,run,previous,state):
        p=run['poststart']; receipt=state['replacement']
        s.require(not p['stale'],'enrollment_stale')
        original=_activation(run,previous['identity']['activation'])
        actual=projection(run,previous['identity'])
        s.require(actual['max_attempts']-actual['attempts_used']==previous['remaining_attempts'],'actual_attempt_budget')
        pending=[v for v in run['pending'] if v['id']==p['source']['token_id']]
        s.require(len(pending)==1 and pending[0].get('execution_activation_id')==original['id'],'actual_token_binding')
        s.require(not any(a['id']==receipt['activation_id'] or any(t['task_id']==receipt['task_id'] for t in a['tasks']) for a in run['activations']),'actual_lineage_conflict')
        token=copy.deepcopy(pending[0]);token['execution_activation_id']=receipt['activation_id']
        successor={k:copy.deepcopy(original[k]) for k in ('node_id','role','created_at','visit_id','assignment_prompt','assigned_task_ids','input_result_refs','execution_session_mode','continue_previous','resume_task_id','execution_candidate','authorized_artifact_refs','authorized_input_refs') if k in original}
        successor.update(id=receipt['activation_id'],created_at=time.time(),status='pending',token=token,planned_task_id=receipt['task_id'],attempt_in_visit=actual['attempts_used']+1,poststart_request_id=receipt['request_id'],poststart_supersedes_execution_id=original['id'],tasks=[dict(task_id=receipt['task_id'],status='reserved',dispatch_stage='poststart_planned',candidate=copy.deepcopy(actual['candidate']))])
        original['poststart_superseded_by_execution_id']=receipt['activation_id']
        run['activations'].append(successor)
        pending[0]['execution_activation_id']=receipt['activation_id']
        s.require(projection(run,previous['identity'])['attempts_used']==actual['attempts_used']+1,'actual_replacement_charge')


class WorkflowPoststartCoordinator(SyntheticCoordinator):
    def __init__(self,store,handle,adapter):
        s.require(isinstance(store,WorkflowPoststartStore) and adapter is store.scope.adapter,'fixture_adapter_binding')
        super().__init__(store,handle,adapter)
    def _invalidate(self,snapshot,*,generation,evidence_revision):
        self._handoffs.clear()
        value=_invalidate_scope(self.store.scope,generation=generation,evidence_revision=evidence_revision)
        s.require(value==(generation,evidence_revision),'protective_frontier_advanced')
        return value
    def _reconcile_frontier(self):
        snapshot=self.store.snapshot()
        scope=self.store.scope
        with scope._frontier['lock']:
            frontier=scope._frontier['value']
            current=(snapshot['context']['generation'],snapshot['evidence_revision'])
            if frontier<=current:
                return
            generation=max(frontier[0],current[0])+1
            self._invalidate(snapshot,generation=generation,evidence_revision=current[1])
        def apply(state):
            state['context']['generation']=generation
            state['certificate']=None
            state['release']=None
        # No adapter call or fence lock survives into the actual workflow CAS.
        self.store.transact(apply,expected_version=snapshot['version'])
    def observe(self,certificate):
        certificate=s.validate_document(certificate,'certificate')
        snapshot=self.store.snapshot()
        self._binding(snapshot,certificate)
        s.require(snapshot['intent'] is not None and certificate['contain_request_id']==snapshot['intent']['request_id'] and certificate['effect_verifier']==snapshot['effect_verifier'],'evidence_binding')
        self._reconcile_frontier()
        return super().observe(_compact(certificate))
    def _authorize(self,kind,request=None):
        run=self.store.workflow_store.get_run(self.store.run_id)
        authorize=getattr(self.store.scope,'authorize',None)
        s.require(callable(authorize),'caller_authority_required')
        stamp=authorize(kind,run,request)
        s.require(stamp==auth_stamp(run),'caller_authority_binding')
        self.store.scope._auth_stamp=stamp
    def contain(self,request):
        request=s.validate_document(request,'contain_request')
        snapshot=self.store.snapshot()
        replay=self._replay(snapshot,request)
        if replay is not None:
            return copy.deepcopy(replay)
        self._authorize('contain',request)
        run=self.store.workflow_store.get_run(self.store.run_id)
        s.require(not projection(run,snapshot['identity'])['scope_ambiguous'],'poststart_scope_ambiguous')
        check_admission_capacity(run,request)
        self._invalidate(snapshot,generation=snapshot['context']['generation']+1,evidence_revision=snapshot['evidence_revision'])
        return super().contain(_compact(request))
    def replace(self,request):
        request=s.validate_document(request,'replacement_request')
        replay=self._replay(self.store.snapshot(),request)
        if replay is not None:
            return copy.deepcopy(replay)
        self._authorize('replace',request)
        return super().replace(_compact(request))
    def prepare_stop(self):
        self._authorize('stop')
        return super().prepare_stop()
    def prepare_dispatch(self):
        self._authorize('dispatch')
        return super().prepare_dispatch()
    def deliver(self,handoff):
        self._authorize('deliver')
        return super().deliver(handoff)
    def update_context(self,context):
        raise s.PoststartError('workflow_context_is_authoritative')
    def _job_quiescent(self):
        check=getattr(self.store.scope,'runtime_job_quiescent',None)
        s.require(callable(check) and check(self) is True,'whole_job_unsettled')
    def certify(self):
        self._job_quiescent()
        run=self.store.workflow_store.get_run(self.store.run_id)
        job=run['poststart']['runtime']['job']
        s.require(job is not None and job['done'] is True and job['activation_id']==run['poststart']['ledger']['identity']['activation'] and job['task_id']==run['poststart']['ledger']['identity']['task'],'whole_job_unsettled')
        return super().certify()
    def release(self):
        self._job_quiescent()
        snapshot=self.store.snapshot()
        certificate=snapshot['certificate']
        s.require(certificate is not None and certificate['revision']==snapshot['evidence_revision'] and certificate['generation']==snapshot['context']['generation'],'stale_certificate')
        self._checks(snapshot,replacement=True)
        release=getattr(self.store.scope,'runtime_release',None)
        s.require(callable(release),'resource_release_capability_required')
        release(self)
        run=self.store.workflow_store.get_run(self.store.run_id)
        runtime=run['poststart']['runtime'];job=runtime['job'];journal=runtime['resource_handoff']
        s.require(job is not None and job['done'] is True and job['activation_id']==run['poststart']['ledger']['identity']['activation'] and job['task_id']==run['poststart']['ledger']['identity']['task'],'whole_job_unsettled')
        if journal is None:
            source=_activation(run,run['poststart']['ledger']['identity']['activation'])
            s.require(not source.get('resource_attempt') and not run['poststart']['source']['node'].get('resources'),'resource_transfer_unsettled')
        else:
            s.require(journal['state']=='coordinator_held','resource_transfer_unsettled')
        s.require(runtime['claim_exclusion'] is True,'resource_transfer_unsettled')
        return super().release()
