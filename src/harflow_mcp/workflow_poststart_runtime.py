"""Explicit unpaid workflow fixtures; no production adapter or registry fallback."""
from __future__ import annotations

import copy
from dataclasses import asdict
import hashlib
import inspect
import json
import time
import uuid

from . import poststart_schema as s
from .poststart_store import Enrollment
from . import workflow_poststart as p


def bind_scope(supervisor, scope):
    scope.check(supervisor.store, supervisor.run_id, supervisor.registry)
    scope.supervisor = supervisor
    def authorize(kind, run, request=None):
        from .workflow_replan import _authorize_control
        from .workflow_steering import _transport
        s.require(hasattr(scope, 'principal'), 'fixture_principal_required')
        actual=_transport(supervisor.store,getattr(scope,'monitor',False))
        s.require(actual==scope.principal,'fixture_transport_binding')
        _authorize_control(supervisor.store, run, scope.principal, getattr(scope, 'monitor', False))
        journal=run['poststart']['runtime']['resource_handoff']
        if journal is not None and kind in {'replace','dispatch','deliver'}:
            from .resources import ResourceLedger
            ledger=ResourceLedger(supervisor.store.root);entry=ledger.get(journal['reservation_id'])
            s.require(journal['state'] in {'coordinator_held','successor_held'},'resource_handoff_unknown')
            owner=journal['coordinator_attempt_id'] if journal['state']=='coordinator_held' else journal['successor_attempt_id']
            generation=journal['coordinator_generation'] if journal['state']=='coordinator_held' else journal['successor_generation']
            s.require(entry is not None and entry.owner.kind=='workflow_attempt' and entry.owner.workflow_run_id==supervisor.run_id
                and entry.owner.activation_id==owner and entry.generation==generation and tuple(entry.names)==tuple(journal['names'])
                and entry.state=='held' and entry.borrows_from is None
                and not any(e.borrows_from==entry.reservation_id for e in ledger.entries()),'resource_owner_unknown')
        if kind in {'contain','stop','deliver'}:
            source=run['poststart']['ledger']['identity']['activation']
            activation=next(a for a in run['activations'] if a['id']==source)
            attempt=activation.get('resource_attempt')
            if attempt and run['poststart']['runtime']['resource_handoff'] is None:
                from .resources import ResourceLedger
                ledger=ResourceLedger(supervisor.store.root)
                entry=ledger.get(attempt['reservation_id'])
                s.require(entry is not None and entry.state=='held' and entry.owner.kind=='workflow_attempt'
                    and entry.owner.workflow_run_id==supervisor.run_id and entry.owner.activation_id==attempt['id']
                    and tuple(entry.names)==tuple(attempt['names']) and entry.borrows_from is None
                    and not any(e.borrows_from==entry.reservation_id for e in ledger.entries()),'poststart_resource_profile_unsupported')
        return p.auth_stamp(run)
    scope.authorize = authorize
    scope.runtime_release = release_claims
    scope.runtime_job_quiescent = job_quiescent
    return scope


def coordinator(supervisor, scope):
    bind_scope(supervisor, scope)
    run = supervisor.run(refresh_atomic=True)
    handle = Enrollment(**run['poststart']['enrollment'])
    store = p.WorkflowPoststartStore(supervisor.store, supervisor.run_id, scope)
    return p.WorkflowPoststartCoordinator(store, handle, scope.adapter)


def assert_ordinary_dispatch(run, activation_id):
    s.require(not p.controlled_activation(run, activation_id), 'poststart_registry_forbidden')


def require_fixture_scope(supervisor, run):
    if 'poststart' not in run:return
    scope=getattr(supervisor,'poststart_fixture_scope',None)
    s.require(type(scope) is p.FixtureScope,'poststart_fixture_scope_required')
    scope.check(supervisor.store,supervisor.run_id,supervisor.registry)


def claim_protected(run, activation_id):
    value=run.get('poststart')
    if not value:return False
    ledger=value['ledger']
    if any(h['identity']['activation']==activation_id for h in ledger['history']):return True
    if ledger['fence'] and ledger['identity']['activation']==activation_id:return True
    return bool(ledger['replacement'] and ledger['replacement']['activation_id']==activation_id)


def protected_reservation(run,reservation_id):
    for activation in run['activations']:
        if claim_protected(run,activation['id']):
            attempts=[activation.get('resource_attempt')]+[t.get('resource_attempt') for t in activation['tasks']]
            if any(ra and ra['reservation_id']==reservation_id for ra in attempts):return True
    journal=run.get('poststart',{}).get('runtime',{}).get('resource_handoff')
    return bool(journal and journal['reservation_id']==reservation_id)


def resource_specs(run):
    journal=run.get('poststart',{}).get('runtime',{}).get('resource_handoff')
    if journal is None:
        ledger=run.get('poststart',{}).get('ledger',{})
        if ledger.get('intent') is None:return []
        source=next(a for a in run['activations'] if a['id']==ledger['identity']['activation'])
        attempt=source.get('resource_attempt')
        if not attempt:return []
        return [dict(reservation_id=attempt['reservation_id'],names=attempt['names'],
            owner=dict(kind='workflow_attempt',workflow_run_id=run['workflow_run_id'],activation_id=attempt['id']))]
    s.require(journal['state'] in {'coordinator_held','successor_held'},'resource_handoff_unknown')
    owner=journal['coordinator_attempt_id'] if journal['state']=='coordinator_held' else journal['successor_attempt_id']
    return [dict(reservation_id=journal['reservation_id'],names=journal['names'],
                 owner=dict(kind='workflow_attempt',workflow_run_id=run['workflow_run_id'],activation_id=owner))]


def _profile_run(run, node, role, activation):
    s.require(role == 'node' and not run.get('parent_link') and run.get('execution_contract') == 'delegation'
              and node.get('execution_mode') == 'headless' and node.get('session_mode') in {'fresh','agent_decides'}
              and activation.get('execution_session_mode') == 'fresh'
              and not activation.get('invocation') and not activation.get('on_demand_call_id')
              and not activation.get('resume_task_id') and not activation.get('continue_previous')
              and not run.get('worktree'), 'poststart_fixture_profile_unsupported')
    s.require(not activation.get('tasks') and not activation.get('node_result'), 'original_launch_only')
    s.require(not any(a['role'] != 'node' and any(t.get('status') in {'reserved','running','uncertain'}
                  for t in a.get('tasks', [])) for a in run['activations']), 'poststart_second_provider')
    s.require(not any(t['state'] in {'queued','reserved','spawn_intent','running','unknown','captured'}
                  for t in run.get('steering', {}).get('turns', [])), 'poststart_response_unsettled')
    return run


def _enrollment_witness(run,node,activation):
    from .workflow_replan import assignment_values
    return s.document_digest(dict(definition=run['definition'],node=node,assignment=assignment_values(activation),
        goal=run.get('goal'),goal_revision=run.get('goal_revision'),parent_goal=run.get('parent_goal'),
        configuration={k:run.get(k) for k in ('repo_path','freedom','network','permission_policy','execution_policy','attempt_grants','interaction_owner','interaction_caller','owner_contracts')},
        activation={k:activation.get(k) for k in ('id','node_id','token','execution_candidate','execution_session_mode','resume_task_id','continue_previous','invocation','on_demand_call_id')}))


def _identity(run, node, activation, task_id, launch_id, dispatcher, adapter):
    from .workflow_replan import assignment_values
    from .workflow_execution_policy import visit_id
    assignment = assignment_values(activation)
    candidate = copy.deepcopy(activation.get('execution_candidate') or node['agent'])
    configuration = {k: copy.deepcopy(run.get(k)) for k in ('repo_path','freedom','network','permission_policy','execution_policy','interaction_owner','interaction_caller','owner_contracts')}
    inventory = copy.deepcopy(adapter.inventory)
    closure = hashlib.sha256(json.dumps(inventory,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
    identity = dict(root=run['workflow_run_id'], activation=activation['id'], task=task_id, attempt=task_id,
        visit=visit_id(run,activation.get('token') or {}), node=node['id'],candidate=s.document_digest(candidate),
        owner_generation=s.document_digest(dispatcher['dispatcher_identity']),containment_generation=launch_id,
        process_generation=task_id,launch_id=launch_id,definition_digest=s.document_digest(run['definition']),
        goal_digest=s.document_digest({'goal':run.get('goal'),'goal_revision':run.get('goal_revision'),'parent_goal':run.get('parent_goal')}),
        goal_revision=run.get('goal_revision',0),interaction_revision=run.get('interaction_epoch',0),
        control_revision=run.get('steering',{}).get('control_revision',0),target_revision=run.get('steering',{}).get('checkpoint_revision',0),
        assignment_digest=s.document_digest(assignment),configuration_digest=s.document_digest(configuration),
        grant_digest=s.document_digest(run.get('attempt_grants',{})),profile_digest=adapter.profile['digest'],
        verifier_digest=adapter.effect_verifier['digest'],closure_digest=closure)
    return identity, assignment, candidate


async def fixture_dispatch(supervisor, node, prompt, role, activation):
    run = supervisor.run(refresh_atomic=True)
    scope = getattr(supervisor, 'poststart_fixture_scope', None)
    controlled = p.controlled_activation(run, activation['id'])
    if scope is None:
        s.require('poststart' not in run, 'poststart_fixture_scope_required')
        return False, None
    s.require(type(scope) is p.FixtureScope, 'fixture_scope_required')
    bind_scope(supervisor, scope)
    if 'poststart' in run and not controlled:
        return False, None  # Independently active siblings retain ordinary behavior.
    from . import workflow_resources as wr
    from .workflow_execution_policy import attempts_used
    from .replan_retention import dispatcher_context
    acquired = False
    task_id = None
    ra = None
    store = None
    settled = False
    try:
        if 'poststart' not in run:
            declared=next(n for n in run['definition']['nodes'] if n['id']==activation['node_id'])
            s.require({k:v for k,v in node.items() if k!='session_mode'}=={k:v for k,v in declared.items() if k!='session_mode'},'original_dispatch_checkpoint_stale')
            node=declared
            run = _profile_run(run,node,role,activation)
            witness=_enrollment_witness(run,node,activation)
            task_id, launch_id = str(uuid.uuid4()), str(uuid.uuid4())
            dispatcher = dispatcher_context(supervisor,activation['id'],task_id)
            s.require(dispatcher.get('dispatcher_job_id') and dispatcher.get('dispatcher_identity'), 'whole_job_origin_required')
            ra = await wr.acquire_attempt(supervisor,activation,node,0)
            if ra is not None:
                from .resources import ResourceLedger
                leaf=ResourceLedger(supervisor.store.root)
                entry=leaf.get(ra['reservation_id'])
                s.require(entry is not None and entry.state=='held' and entry.owner.kind=='workflow_attempt'
                    and entry.owner.workflow_run_id==supervisor.run_id and entry.owner.activation_id==ra['id']
                    and entry.borrows_from is None and not any(e.borrows_from==entry.reservation_id for e in leaf.entries()),'poststart_resource_profile_unsupported')
                ra['poststart_grant_generation']=entry.generation
            await wr.acquire_slot(supervisor,activation,lambda: supervisor.run(refresh_atomic=True)['status']=='running')
            acquired = True
            run = supervisor.run(refresh_atomic=True)
            activation = next(a for a in run['activations'] if a['id']==activation['id'])
            current_node=next(n for n in run['definition']['nodes'] if n['id']==node['id'])
            _profile_run(run,current_node,role,activation)
            s.require(_enrollment_witness(run,current_node,activation)==witness,'original_enrollment_stale')
            node=current_node
            identity,assignment,candidate = _identity(run,node,activation,task_id,launch_id,dispatcher,scope.adapter)
            maximum = node['max_attempts'] + run.get('attempt_grants',{}).get(node['id'],0)
            # The original reservation is charged by the normal attempts_used rule.
            remaining = maximum - attempts_used(run,node,activation.get('token') or {}) - 1
            s.require(remaining>=0,'attempt_budget')
            marker = dict(activation_id=activation['id'],task_id=task_id,job_id=dispatcher['dispatcher_job_id'],dispatcher_identity=s.document_digest(dispatcher['dispatcher_identity']))
            command_id = str(uuid.uuid5(uuid.UUID(launch_id),'original'))
            runtime = dict(origin_issue=dict(command_id=command_id,task_id=task_id,status='prepared',receipt=None),
                resource_handoff=None,job={**marker,'done':False},claim_exclusion=False,next_job=None,history=[])
            from .workflows import run_effective_freedom
            freedom=run_effective_freedom(run,node)
            network=False if run.get('network') is False else node.get('network',run.get('network'))
            reservation = dict(task_id=task_id,status='reserved',dispatch_stage='poststart_prepared',candidate=candidate,
                repo_path=run['repo_path'],freedom=freedom,network=network,session_mode='fresh',
                execution_kind='headless',**dispatcher)
            if ra is not None:
                wr.attach_attempt(supervisor,activation['id'],ra,reservation)
            def enroll(r):
                a=next(a for a in r['activations'] if a['id']==activation['id'])
                actual_node=next(n for n in r['definition']['nodes'] if n['id']==node['id'])
                _profile_run(r,actual_node,role,a)
                s.require(_enrollment_witness(r,actual_node,a)==witness,'original_enrollment_stale')
                s.require(not a['tasks'] and not a.get('node_result'),'original_launch_only')
                a['tasks'].append(copy.deepcopy(reservation))
                if ra is not None:a['resource_attempt']=copy.deepcopy(ra)
            context = dict.fromkeys(('authority','cancel_authority','enrolled','single_provider','scope_exact','qualified_profile','qualified_budgets','effect_safe','budget','configuration','claims'),True)
            context['generation']=1
            store=p.WorkflowPoststartStore(supervisor.store,supervisor.run_id,scope)
            handle=store.initialize(identity,adapter_id=scope.adapter.adapter_id,profile=scope.adapter.profile,
                effect_verifier=scope.adapter.effect_verifier,assignment=assignment,remaining_attempts=remaining,
                context=context,original_launch=marker,runtime=runtime,enrollment_mutator=enroll)
            # A capability exists only after an acknowledged commit; it is never persisted.
            def claim(rt):
                current=supervisor.run(refresh_atomic=True)['poststart']['ledger']
                s.require(current['intent'] is None and current['context']['generation']==1,'original_issue_fenced')
                s.require(rt['origin_issue']['status']=='prepared','original_issue_unknown')
                rt['origin_issue']['status']='unknown'
            store.runtime_update(claim)
            receipt=scope.adapter.execute(kind='original',command_id=command_id,task_id=task_id,root=supervisor.run_id,
                expected_generation=1,expected_evidence_revision=0)
            expected=dict(command_id=command_id,kind='original',task_id=task_id,root=supervisor.run_id,generation=1,evidence_revision=0)
            s.require(receipt==expected,'original_receipt_binding')
            store.runtime_update(lambda rt:rt['origin_issue'].update(status='acknowledged',receipt=copy.deepcopy(receipt)))
            supervisor._task_update(activation['id'],task_id,{'status':'running','dispatch_stage':'poststart_running'})
        else:
            c=coordinator(supervisor,scope);store=c.store
            ledger=store.snapshot()
            if p.retired(run,activation['id']):return True,None
            replacement=ledger['replacement']
            if replacement and activation['id']==replacement['activation_id']:
                task_id=replacement['task_id']
                s.require('dispatch' not in ledger['commands'],'successor_issue_unknown')
                dispatcher=dispatcher_context(supervisor,activation['id'],task_id)
                s.require(dispatcher.get('dispatcher_job_id') and dispatcher.get('dispatcher_identity'),'whole_job_origin_required')
                marker=dict(activation_id=activation['id'],task_id=task_id,job_id=dispatcher['dispatcher_job_id'],
                    dispatcher_identity=s.document_digest(dispatcher['dispatcher_identity']),done=False)
                store.runtime_update(lambda rt:rt.update(next_job=marker))
                def checkpoint(r):
                    from .workflows import run_effective_freedom
                    row=next(t for a in r['activations'] if a['id']==activation['id'] for t in a['tasks'] if t['task_id']==task_id)
                    s.require(row['status']=='reserved' and row['dispatch_stage']=='poststart_planned','successor_issue_unknown')
                    row.update(repo_path=r['repo_path'],freedom=run_effective_freedom(r,node),
                        network=False if r.get('network') is False else node.get('network',r.get('network')),
                        session_mode='fresh',execution_kind='headless',**dispatcher)
                supervisor.store.update_run(supervisor.run_id,checkpoint,'poststart:successor_launch_checkpoint')
                await _acquire_successor(supervisor,c,node,activation)
                ra=next(a for a in supervisor.run(refresh_atomic=True)['activations'] if a['id']==activation['id']).get('resource_attempt')
                acquired=True
                handoff=c.prepare_dispatch()
                receipt=c.deliver(handoff)
                c.acknowledge(handoff.command_id,receipt)
                successor=copy.deepcopy(ledger['identity'])
                for field,key in (('activation','activation_id'),('task','task_id'),('attempt','attempt_id'),('launch_id','launch_id')):
                    successor[field]=replacement[key]
                successor.update(owner_generation=marker['dispatcher_identity'],containment_generation=replacement['launch_id'],process_generation=task_id)
                context=copy.deepcopy(store.snapshot()['context']);context['generation']+=1
                c.advance_source(successor,context)
            else:
                task_id=ledger['identity']['task']
                s.require(run['poststart']['runtime']['origin_issue']['status']=='acknowledged','original_issue_unknown')
                s.require(False,'original_observer_reopen_required')
        outcome=scope.adapter.observe(task_id)
        if inspect.isawaitable(outcome):outcome=await outcome
        s.require(type(outcome) is dict and outcome.get('task_id')==task_id,'fixture_outcome_binding')
        supervisor._task_update(activation['id'],task_id,{'status':outcome['status'],'result':copy.deepcopy(outcome),'finished_at':time.time()})
        settled=outcome.get('status') in {'completed','failed','cancelled'} and outcome.get('output_drained') is True and type(outcome.get('exit_code')) is int and not outcome.get('outcome_unknown',False)
        return True,outcome
    finally:
        if ra is not None and task_id is not None:
            observed=supervisor.run(refresh_atomic=True)
            if 'poststart' not in observed:
                # The enrollment CAS never published an original issue claim.
                # Keep the normal never-issued cleanup free of private markers.
                clean=copy.deepcopy(ra);clean.pop('poststart_grant_generation',None)
                wr.abandon_attempt(supervisor,activation['id'],clean)
                ra=None
        if acquired and task_id is not None:
            current=supervisor.run(refresh_atomic=True)
            if p.controlled_activation(current,activation['id']) and (current['poststart']['ledger']['intent'] is not None or not settled):
                supervisor.tree.hold_slot(supervisor.run_id,task_id)
            else:
                supervisor.tree.release_slot()
                wr.finish_attempt(supervisor,activation['id'],ra)


def job_quiescent(c):
    run=c.store.workflow_store.get_run(c.store.run_id,refresh_atomic=True)
    job=run['poststart']['runtime']['job'];source=run['poststart']['ledger']['identity']
    if not job or not job['done'] or job['activation_id']!=source['activation'] or job['task_id']!=source['task']:return False
    from .replan_retention import _outer_tasks
    registered=_outer_tasks.get(job['job_id'])
    return registered is None or registered[1].done()


def record_job_done(storage,run_id,job_id,dispatcher_identity,origins,task):
    """Only the registered complete outer task can publish this acknowledgment."""
    from .replan_retention import _outer_tasks
    registered=_outer_tasks.get(job_id)
    if not registered or registered[1] is not task or not task.done() or registered[0]['dispatcher_identity']!=dispatcher_identity:
        return
    expected=tuple(copy.deepcopy(v) for v in registered[0]['origins'].values())
    if tuple(origins)!=expected:return
    run=storage.get_run(run_id,refresh_atomic=True)
    if 'poststart' not in run:return
    def done(r):
        for key in ('job','next_job'):
            job=r['poststart']['runtime'][key]
            if job and job['job_id']==job_id and job['dispatcher_identity']==s.document_digest(dispatcher_identity) and any(
                    o['activation_id']==job['activation_id'] and o['task_id']==job['task_id'] for o in origins):
                job['done']=True
        r['poststart']['ledger']['version']+=1
    storage.update_run(run_id,done,'poststart:outer_job_done')


def release_claims(c):
    s.require(job_quiescent(c),'whole_job_unsettled')
    store=c.store;run=store.workflow_store.get_run(store.run_id,refresh_atomic=True)
    l=run['poststart']['ledger'];a=next(a for a in run['activations'] if a['id']==l['identity']['activation'])
    s.require(l['certificate'] is not None,'certificate_required')
    ra=a.get('resource_attempt')
    if ra is not None and not ra.get('released'):
        from .resources import ResourceLedger
        ledger=ResourceLedger(store.workflow_store.root)
        entry=ledger.get(ra['reservation_id']);journal=run['poststart']['runtime']['resource_handoff']
        if journal is None:
            s.require(entry is not None and entry.owner.activation_id==ra['id'] and entry.owner.workflow_run_id==store.run_id,'resource_owner_unknown')
            journal=dict(reservation_id=ra['reservation_id'],names=list(entry.names),old_attempt_id=ra['id'],
                coordinator_attempt_id=str(uuid.uuid5(uuid.UUID(l['intent']['request_id']),'coordinator_resource')),
                successor_attempt_id=None,old_generation=entry.generation,coordinator_generation=None,successor_generation=None,state='coordinator_intent')
            store.runtime_update(lambda rt:rt.update(resource_handoff=copy.deepcopy(journal)))
        s.require(journal['state'] in {'coordinator_intent','coordinator_held'},'resource_transfer_unknown')
        result=ledger.transfer_owned_workflow_attempt(journal['reservation_id'],store.run_id,journal['old_attempt_id'],journal['coordinator_attempt_id'],expected_names=journal['names'],expected_generation=journal['old_generation'])
        store.runtime_update(lambda rt:rt['resource_handoff'].update(state='coordinator_held',coordinator_generation=result.generation))
    store.runtime_update(lambda rt:rt.update(claim_exclusion=True))


async def _acquire_successor(supervisor,c,node,activation):
    run=supervisor.run(refresh_atomic=True);rt=run['poststart']['runtime'];journal=rt['resource_handoff']
    s.require(rt['claim_exclusion'] is True,'exclusion_required')
    if journal is not None:
        from .resources import ResourceLedger
        ledger=ResourceLedger(supervisor.store.root)
        new_id=str(uuid.uuid5(uuid.UUID(c.store.snapshot()['replacement']['request_id']),'successor_resource'))
        s.require(journal['state'] in {'coordinator_held','successor_intent','successor_held'},'resource_transfer_unknown')
        c.store.runtime_update(lambda rt:rt['resource_handoff'].update(state='successor_intent',successor_attempt_id=new_id))
        result=ledger.transfer_owned_workflow_attempt(journal['reservation_id'],supervisor.run_id,journal['coordinator_attempt_id'],new_id,expected_names=journal['names'],expected_generation=journal['coordinator_generation'])
        def attach(r):
            a=next(a for a in r['activations'] if a['id']==activation['id'])
            ra=dict(id=new_id,reservation_id=result.reservation_id,names=list(result.names),state='attached',released=False,
                task_id=c.store.snapshot()['replacement']['task_id'],poststart_grant_generation=result.generation)
            a['resource_attempt']=copy.deepcopy(ra);a['tasks'][0]['resource_attempt']=copy.deepcopy(ra)
            r['poststart']['runtime']['resource_handoff'].update(state='successor_held',successor_generation=result.generation)
        supervisor.store.update_run(supervisor.run_id,attach,'poststart:successor_claims')
    old=run['poststart']['ledger']['identity']['task']
    held=supervisor.tree.held.pop((supervisor.run_id,old),None)
    if held is None:
        from .workflow_resources import acquire_slot
        await acquire_slot(supervisor,activation,lambda:supervisor.run(refresh_atomic=True)['status']=='running')
