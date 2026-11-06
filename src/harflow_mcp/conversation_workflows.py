"""Conversation bindings inside the existing root steering authority/CAS.

No effect driver or second mailbox lives here. Every mutating helper is called
inside WorkflowStore.update_run, alongside the result/graph mutation it gates.
"""
from __future__ import annotations
from copy import deepcopy
from contextvars import ContextVar
from . import conversation_delivery as delivery
from .conversation_documents import ConversationError, encoded, validate, identifier, MAX_HISTORY, MAX_DOCUMENT_BYTES, COMPLETION_RESERVE_BYTES

MAX_CAPTURE_BYTES = 1024 * 1024
ROOT_TEXT_BYTES = 1024 * 1024
RESPONSE_TEXT_RESERVE = 16 * 1024
EXTENSION = 'conversation_delivery'
_handoff = ContextVar('conversation_explicit_commit_handoff', default=None)
_handoff_source = ContextVar('conversation_explicit_commit_source', default=None)

def _bindings(run):
    return run.get('steering', {}).get(EXTENSION, {})

def validate_bindings(value, run):
    from .steering_schema import _target
    if not isinstance(value,dict) or len(encoded(value)) > MAX_DOCUMENT_BYTES:
        raise ValueError('Malformed conversation bindings')
    for activation_id,binding in value.items():
        if not isinstance(binding,dict) or not {'target','state','accepted','captures'} <= set(binding) or set(binding)-{'target','state','accepted','captures','native_owner','steering_episode_id','capture_history_floor','omitted_captures','commit_source','history_sequence'}:
            raise ValueError('Malformed conversation binding')
        _target(binding['target'],run['workflow_run_id'])
        if binding['target']['activation_id'] != activation_id:
            raise ValueError('Malformed conversation activation')
        validate(binding['state'])
        for counter in ('capture_history_floor','omitted_captures','history_sequence'):
            if counter in binding and (type(binding[counter]) is not int or binding[counter]<0):
                raise ValueError('Malformed capture history counters')
        linked=binding.get('steering_episode_id')
        if linked is not None and not any(ep.get('episode_id')==linked and ep.get('target',{}).get('activation_id')==activation_id
            and ep.get('target',{}).get('role')==binding['target']['role'] for ep in run.get('steering',{}).get('episodes',[])):
            raise ValueError('Malformed conversation steering episode link')
        proof=binding.get('commit_source')
        if proof is not None:
            if not isinstance(proof,dict) or set(proof)!={'conversation_id','head_task_id','generation','turn_id','ledger_epoch'}:
                raise ValueError('Malformed conversation commit source')
            if not binding.get('steering_episode_id') or any(proof[key]!=binding['state'][key] for key in ('conversation_id','head_task_id','generation','turn_id')):
                raise ValueError('Malformed conversation commit source')
            if type(proof['ledger_epoch']) is not int or not 0<=proof['ledger_epoch']<=binding['state']['ledger_epoch']:
                raise ValueError('Malformed conversation commit epoch')
        native=binding.get('native_owner')
        if native is not None:
            if not isinstance(native,dict) or not {'task_id','transport','profile_fingerprint'}<=set(native) or set(native)-{'task_id','transport','profile_fingerprint','profile_name'}:
                raise ValueError('Malformed native conversation owner')
            if native['task_id']!=binding['target']['task_id'] or not isinstance(native['transport'],str) or not isinstance(native['profile_fingerprint'],str) or len(native['profile_fingerprint'])!=64:
                raise ValueError('Malformed native conversation owner')
            if 'profile_name' in native and native['profile_name'] not in (None,'native','shipped-medium35-high'):
                raise ValueError('Malformed native conversation profile')
        if type(binding['accepted']) is not bool or not isinstance(binding['captures'],list) or len(binding['captures']) > MAX_HISTORY or len(encoded(binding['captures'])) > MAX_CAPTURE_BYTES:
            raise ValueError('Malformed conversation capture')
        for capture in binding['captures']:
            if not isinstance(capture,dict) or set(capture) != {'task_id','generation','turn_id','result'}:
                raise ValueError('Malformed conversation capture')
            identifier(capture['task_id']); identifier(capture['turn_id'])
            if type(capture['generation']) is not int or capture['generation'] < 0 or capture['generation']>binding['state']['generation'] or not isinstance(capture['result'],dict):
                raise ValueError('Malformed conversation capture')

    if shared_text_bytes(run)>ROOT_TEXT_BYTES:
        raise ValueError('Conversation root text window exceeded')

def held(run, activation_id=None):
    return any(not b['accepted'] and (delivery.protects_retention(b['state']) or b['state']['cumulative_issuances'] > 0)
               for key,b in _bindings(run).items() if (activation_id is None or key == activation_id)
               and not (_handoff.get() is not None and _handoff.get()[0]==run.get('workflow_run_id'))
               and not b.get('steering_episode_id'))

def _activation(run, activation_id):
    activation = next((a for a in run.get('activations',[]) if a['id'] == activation_id),None)
    if activation is None: raise ConversationError('target_stale')
    return activation

def _accepted(run, activation):
    return (activation.get('status') in {'completed','failed','cancelled'}
            or bool(activation.get('node_result'))
            or any(t.get('execution_activation_id') == activation['id'] and t.get('execution_complete') for t in run.get('pending',[])))

def enroll(run, *, target, state):
    """Trusted launch owner enrolls exact ordinary activation before admission."""
    from .steering_schema import _target
    _target(target,run['workflow_run_id']); validate(state)
    activation=_activation(run,target['activation_id'])
    if run.get('status') in {'completed','failed','cancelled','cancelling'}: raise ConversationError('target_stale')
    if activation.get('conversation_checkpoint'): raise ConversationError('request_expired')
    if _accepted(run,activation): raise ConversationError('target_stale')
    if target['task_id'] != state['head_task_id'] or not any(t['task_id'] == target['task_id'] for t in activation['tasks']):
        raise ConversationError('target_stale')
    expected_role = 'node' if target['role']=='worker' else target['role']
    if activation['role'] != expected_role: raise ConversationError('target_stale')
    if 'steering' not in run: raise ConversationError('authority_unavailable')
    bindings=run['steering'].setdefault(EXTENSION,{})
    old=bindings.get(activation['id'])
    if old:
        if old['target'] != target or old['state'] != state: raise ConversationError('context_conflict')
        return
    bindings[activation['id']]=dict(target=deepcopy(target),state=deepcopy(state),accepted=False,captures=[])
    compact_conversations(run,reserve_text_bytes=pending_capture_reserve(run),reserve_metadata_bytes=pending_metadata_reserve(run))

def _same_authority(issued, current):
    # A sibling completion may advance the root checkpoint. Exact activation,
    # task, assignment, draft and ownership identities remain authoritative.
    ignored={'checkpoint_revision','task_id'}
    if issued.get('role')=='orchestrator': ignored.add('assignment_digest')
    return all(issued.get(key)==current.get(key) for key in set(issued)|set(current) if key not in ignored)

def admit(storage, run, *, principal, monitor, target, request_id, text, mode,
          expected_generation, expected_turn_id, expected_epoch, original_target_task_id=None):
    """Admission plus result hold in one authoritative run mutation."""
    from .workflow_steering import _authorize, _source_check
    _authorize(run,principal,monitor); _source_check(storage,run)
    binding=_bindings(run).get(target.get('activation_id'))
    if binding is None:
        if _activation(run,target['activation_id']).get('conversation_checkpoint'):raise ConversationError('request_expired')
        raise ConversationError('activation_unenrolled')
    activation=_activation(run,target['activation_id'])
    replay=next((m for m in binding['state']['messages'] if m['request_id']==request_id),None)
    if replay:
        state,item=delivery.admit(binding['state'],request_id=request_id,text=text,delivery=mode,
            target_task_id=binding['state']['head_task_id'],original_target_task_id=original_target_task_id or target['task_id'],
            expected_generation=expected_generation,expected_turn_id=expected_turn_id,expected_epoch=expected_epoch)
        return item
    if run.get('status') in {'completed','failed','cancelled','cancelling'}: raise ConversationError('target_stale')
    if binding['accepted'] or _accepted(run,activation): raise ConversationError('target_stale')
    if any(ep['hold'] and ep['target'].get('activation_id')==target['activation_id'] and ep['episode_id']!=binding.get('steering_episode_id') for ep in run['steering']['episodes']):
        raise ConversationError('conversation_bound_to_steering')
    if not _same_authority(binding['target'],target): raise ConversationError('target_stale')
    # Preserve ownership and canvas/source witnesses without treating unrelated
    # sibling graph activity as a new target.
    from .workflow_steering import target as current_target
    now=current_target(run,storage,target)
    if not _same_authority(target,now): raise ConversationError('target_stale')
    if sum(m['status'] in {'queued','claimed','unknown'} for b in _bindings(run).values() for m in b['state']['messages'])>=32:
        raise ConversationError('queue_full')
    state,item=delivery.admit(binding['state'],request_id=request_id,text=text,delivery=mode,
        target_task_id=binding['state']['head_task_id'],expected_generation=expected_generation,
        expected_turn_id=expected_turn_id,expected_epoch=expected_epoch,
        original_target_task_id=original_target_task_id or target['task_id'])
    binding['state']=state
    compact_conversations(run,reserve_text_bytes=pending_capture_reserve(run),reserve_metadata_bytes=pending_metadata_reserve(run))
    return item

def guard_result(run, *, activation_id, task_id, generation, turn_id, result):
    """Return accept/hold/conversation/stale BEFORE ordinary result mutation.

    Only accept authorizes the caller's existing result/graph mutation. Every
    conversational response remains prose until the existing framed steering
    commit path independently validates its authority and draft witnesses.
    """
    binding=_bindings(run).get(activation_id)
    if binding is None:
        activation=next((a for a in run.get('activations',[]) if a['id']==activation_id),None)
        return 'stale' if activation and activation.get('conversation_checkpoint') else 'accept'
    if binding['accepted']: return 'stale'
    state=binding['state']
    if (task_id,generation,turn_id) != (state['head_task_id'],state['generation'],state['turn_id']):
        return 'stale'
    if not isinstance(result,dict) or len(encoded(result)) > 32*1024:
        raise ConversationError('result_too_large')
    capture=dict(task_id=task_id,generation=generation,turn_id=turn_id,result=deepcopy(result))
    state=delivery.observe_boundary(state,generation=generation,turn_id=turn_id)
    binding['state']=state
    append_capture(run,activation_id,capture)
    if delivery.protects_retention(state): return 'hold'
    if state['cumulative_issuances']: return 'conversation'
    binding['accepted']=True
    binding['state']=delivery.close(state)
    return 'accept'


def guard_outcome(run, activation_id, outcome):
    """CAS acceptance seam for existing ordinary result handlers."""
    b=_bindings(run).get(activation_id)
    if b is None:
        activation=next((a for a in run.get('activations',[]) if a['id']==activation_id),None)
        return 'stale' if activation and activation.get('conversation_checkpoint') else 'accept'
    linked=b.get('steering_episode_id')
    if linked:
        from .steering_schema import captured_failure
        turn=next((t for t in run['steering']['turns'] if t['episode_id']==linked
                   and t['task_id']==outcome.get('task_id') and t.get('final')==outcome.get('summary')),None)
        if turn is not None and turn['state']=='captured' and captured_failure(turn.get('observation'),turn['task_id']) is None:
            return 'accept'
    state=b['state']
    task_id=outcome.get('task_id')
    if task_id is None:
        activation=_activation(run,activation_id)
        task_id=b['target']['task_id']
    return guard_result(run,activation_id=activation_id,task_id=task_id,
        generation=state['generation'],turn_id=state['turn_id'],result=_capture(outcome))


def _capture(snapshot, *, settlement=None):
    import hashlib
    summary=(snapshot.get('summary') or '').encode('utf-8')
    if settlement is None:
        settlement=('physical_response' if snapshot.get('status')=='completed' and type(snapshot.get('exit_code')) is int
                    and snapshot['exit_code']==0 and snapshot.get('output_drained') is True else 'unqualified')
    result=dict(task_id=snapshot.get('task_id'),status=snapshot.get('status'),
        exit_code=snapshot.get('exit_code'),session_id=snapshot.get('session_id'),settlement=settlement,
        summary=summary[:16*1024].decode('utf-8',errors='ignore'),
        summary_sha256=hashlib.sha256(summary).hexdigest(),summary_bytes=len(summary))
    # Completion reserve applies to serialized metadata even for escape-heavy
    # output. The complete output hash/byte count still identifies the source.
    while len(encoded(result))>24*1024:
        result['summary']=result['summary'][:max(0,len(result['summary'])//2)]
    return result


def guard_preview(run, task_id):
    for b in _bindings(run).values():
        state=b['state']
        owned = task_id == b['target']['task_id'] or task_id == state['head_task_id'] or any(m['claim'] and m['claim']['destination_task_id']==task_id for m in state['messages'])
        if owned and (delivery.protects_retention(state) or state['cumulative_issuances']):
            raise ConversationError('conversation_preview_requires_explicit_commit')


def enroll_activation(storage, registry, run_id, activation_id, task_id, *, monitor=False):
    """Select a qualified same-run activity; nested runs explicitly refuse."""
    import uuid
    from .conversation_runtime import _record, _qualified
    from .workflow_steering import _transport, _authorize, _source_check
    principal=_transport(storage,monitor)
    initial=storage.get_run(run_id,refresh_atomic=True)
    _authorize(initial,principal,monitor); _source_check(storage,initial)
    selected=_activation(initial,activation_id)
    if selected.get('conversation_checkpoint'): raise ConversationError('request_expired')
    if any(t['task_id']==task_id for t in initial['steering']['turns']): raise ConversationError('steering_commit_not_composer_target')
    source=_record(registry,task_id)
    initial_binding=_bindings(initial).get(activation_id)
    if initial_binding is not None:
        if initial_binding.get('native_owner'):_native_descriptor_matches(source,initial_binding)
        elif source.session_id!=initial_binding['state']['native_context_id']:raise ConversationError('target_stale')
    else:
        _qualified(source)
    result=[]
    def mutate(run):
        _authorize(run,principal,monitor); _source_check(storage,run)
        if run.get('parent_link'): raise ConversationError('nested_conversation_unsupported')
        activation=_activation(run,activation_id)
        old=_bindings(run).get(activation_id)
        if old:
            if not any(t['task_id']==task_id for t in activation['tasks']):raise ConversationError('target_stale')
            if old.get('native_owner'):_native_descriptor_matches(source,old)
            elif source.session_id!=old['state']['native_context_id']:raise ConversationError('target_stale')
            result.append(deepcopy(old['state']));return
        if activation.get('conversation_checkpoint'):raise ConversationError('request_expired')
        if not activation['tasks'] or activation['tasks'][-1]['task_id'] != task_id:
            raise ConversationError('target_stale')
        from .workflow_steering import target as current_target
        role='worker' if activation['role']=='node' else activation['role']
        if role not in {'worker','orchestrator','builder'}: raise ConversationError('target_unsupported')
        witness={'role':role,'workflow_run_id':run_id,'activation_id':activation_id,
                 'token_id':(activation.get('token') or {}).get('id')}
        # Builders have one declared actor. Worker/orchestrator targets reuse
        # their issued immutable assignment/checkpoint validators.
        if role=='builder': target=current_target(run)
        elif role=='worker':
            from .worker_steering import current_target as issued
            target=issued(storage,run,witness)
        else:
            from .orchestrator_steering import current_target as issued
            target=issued(storage,run,witness)
        conversation_id=str(uuid.uuid5(uuid.NAMESPACE_URL,'harflow:workflow:'+run_id+':'+activation_id+':'+task_id))
        state=delivery.new_state(conversation_id,task_id,source.session_id,0,task_id)
        enroll(run,target=target,state=state); result.append(state)
    storage.update_run(run_id,mutate,'conversation_enrolled')
    return result[0]


def message_ack(run_id, activation_id, conversation_id, task_id, item):
    return dict(workflow_run_id=run_id, activation_id=activation_id,
        conversation_id=conversation_id, message_id=item['request_id'], status=item['status'],
        task_id=task_id, resolved_task_id=item['target_task_id'])


def control_ack(run_id, activation_id, conversation_id, operation, receipt):
    return dict(workflow_run_id=run_id, activation_id=activation_id,
        conversation_id=conversation_id, operation=operation, receipt=receipt)


def send_request(storage, registry, request, *, monitor=False):
    from .conversation_requests import validate_send
    from .workflow_steering import _transport, _authorize, _source_check
    request=validate_send(request); target=request['target']; expected=request['expected']
    if target['kind']!='workflow_activation': raise ConversationError('target_unsupported')
    principal=_transport(storage,monitor)
    initial=storage.get_run(target['workflow_run_id'],refresh_atomic=True)
    _authorize(initial,principal,monitor); _source_check(storage,initial)
    initial_binding=_bindings(initial).get(target['activation_id'])
    qualified_source=None
    if initial_binding and initial_binding.get('native_owner'):
        existing=next((m for m in initial_binding['state']['messages'] if m['request_id']==request['request_id']),None)
        if existing is None:
            from .conversation_runtime import _record
            qualified_source=_record(registry,target['task_id'])
            _qualify_native_source(registry,qualified_source,initial_binding)
    result=[]
    def mutate(run):
        if run.get('parent_link'): raise ConversationError('nested_conversation_unsupported')
        b=_bindings(run).get(target['activation_id'])
        if b is None:
            if _activation(run,target['activation_id']).get('conversation_checkpoint'):raise ConversationError('request_expired')
            raise ConversationError('activation_unenrolled')
        if qualified_source is not None: _native_descriptor_matches(qualified_source,b)
        state=b['state']
        if state['conversation_id'] != expected['conversation_id']: raise ConversationError('target_stale')
        replay=next((m for m in state['messages'] if m['request_id']==request['request_id']),None)
        if replay is None and (state['head_task_id'] != target['task_id'] or state['head_task_id'] != expected['head_task_id']):
            raise ConversationError('target_stale')
        if request['delivery']=='now' and not state['boundary'] and not b.get('native_owner'):
            raise ConversationError('interrupt_unsupported')
        item=admit(storage,run,principal=principal,monitor=monitor,target=b['target'],
            request_id=request['request_id'],text=request['text'],mode=request['delivery'],
            expected_generation=expected['generation'],expected_turn_id=expected['turn_id'],
            expected_epoch=expected['ledger_epoch'],original_target_task_id=target['task_id'])
        result.append(message_ack(run['workflow_run_id'], target['activation_id'],
            state['conversation_id'], target['task_id'], item))
    storage.update_run(target['workflow_run_id'],mutate,'conversation_admitted')
    return result[0]


async def drain_observed(supervisor, task, node, role, activation, reservation, snapshot):
    """Same-activation response driver; called before any ordinary JSON parser."""
    import uuid
    from .conversation_runtime import _observed, _qualified
    from . import store as task_store
    aid=activation['id']
    enrolled=_bindings(supervisor.run(refresh_atomic=True)).get(aid)
    if enrolled is None: return False
    if enrolled.get('native_owner'):
        return held(supervisor.run(refresh_atomic=True),aid)
    current_task,current_snapshot=task,snapshot
    for _ in range(33):
        selected=[]; handled=[]
        child_id,turn_id,claim_id=(str(uuid.uuid4()) for _ in range(3))
        def settle_and_claim(run):
            b=_bindings(run).get(aid)
            if b is None: return
            state=b['state']
            if state['head_task_id'] != current_task.task_id: return
            pending=next((m for m in state['messages'] if m['status'] in {'claimed','unknown'}),None)
            if not _observed(current_task,state['native_context_id']):
                if pending: b['state']=delivery.mark_unknown(state,claim_id=pending['claim']['claim_id'])
                handled.append(delivery.protects_retention(state)); return
            if pending:
                claim=pending['claim']
                state=delivery.record_receipt(state,receipt=dict(claim=claim,task_id=current_task.task_id,
                    turn_id=claim['destination_turn_id'],generation=claim['destination_generation'],kind='input_consumed'))
                b['state']=state
            state=delivery.record_response(state,generation=state['generation'],turn_id=state['turn_id'])
            b['state']=state
            if not delivery.protects_retention(state) and not state['cumulative_issuances']:
                b['state']=delivery.observe_boundary(state,generation=state['generation'],turn_id=state['turn_id'])
                handled.append(False); return
            choice=guard_result(run,activation_id=aid,task_id=current_task.task_id,
                generation=state['generation'],turn_id=state['turn_id'],result=_capture(current_snapshot,settlement='physical_response'))
            handled.append(choice!='accept')
            if choice=='accept': return
            if current_snapshot.get('status')!='completed' or current_task.cancel_requested: return
            state,item=delivery.claim_next(b['state'],claim_id=claim_id,
                destination_task_id=child_id,destination_turn_id=turn_id)
            b['state']=state
            if item:
                entry={'task_id':child_id,'status':'reserved','resume_task_id':current_task.task_id,
                       'session_mode':'resume'}
                if 'candidate' in reservation: entry['candidate']=deepcopy(reservation['candidate'])
                _activation(run,aid)['tasks'].append(entry)
                selected.append(item)
        supervisor.update(settle_and_claim,'conversation_response_boundary',{'activation_id':aid})
        if not selected: return any(handled)
        item=selected[0]
        try:
            source=task_store.read(supervisor.registry.log_dir,current_task.task_id)
            _qualified(source)
            child=await supervisor.registry.resume(current_task,item['text'],task_id=child_id,
                consume_followups=False,require_unchanged_session=True,workflow_builder=False)
            if child.task_id!=child_id or child.session_id!=item['claim']['native_context_id']:
                raise ConversationError('destination_mismatch')
            supervisor._task_update(aid,child_id,{'status':'running'})
            child_snapshot=await supervisor._observe_attempt(child,node,role,activation,reservation)
            if child_snapshot is None: raise ConversationError('delivery_unknown')
            supervisor._task_update(aid,child_id,{'status':child_snapshot['status'],'result':child_snapshot})
            current_task,current_snapshot=child,child_snapshot
        except BaseException:
            def unknown(run):
                b=_bindings(run)[aid]
                b['state']=delivery.mark_unknown(b['state'],claim_id=claim_id)
            supervisor.update(unknown,'conversation_delivery_unknown',{'activation_id':aid})
            raise
    return True


def schedule_delivery(storage, registry, run_id, activation_id):
    """Retained owner wake for sends racing response observation/acceptance."""
    import asyncio
    from .state_ownership import owned_job, state_session
    run=storage.get_run(run_id,refresh_atomic=True)
    b=_bindings(run).get(activation_id)
    if b is None or b.get('native_owner'): return None
    task=registry.get(b['state']['head_task_id'])
    if task is None:
        # A disk record alone is not correlated response-consumption proof.
        return None
    try: asyncio.get_running_loop()
    except RuntimeError: return None
    async def wake():
        await task.done.wait()
        from .workflows import WorkflowSupervisor
        supervisor=WorkflowSupervisor(registry,storage); supervisor.run_id=run_id
        current=supervisor.run(refresh_atomic=True)
        activation=_activation(current,activation_id)
        reservation=next(t for t in activation['tasks'] if t['task_id']==task.task_id)
        role=activation['role']
        nodes=current.get('definition',{}).get('nodes',[])
        node=next((n for n in nodes if n['id']==activation['node_id']),{'id':activation['node_id']})
        await drain_observed(supervisor,task,node,role,activation,reservation,task.snapshot())
    with state_session(registry.state_layout):
        job=owned_job(registry.state_layout,wake(),name='workflow-conversation-'+activation_id)
    retained=getattr(registry,'_continuations',None)
    if retained is not None:
        retained.add(job); job.add_done_callback(retained.discard)
    return job


def scheduling_blocked(run, token):
    """Fence only the exact activation/token; independent siblings still run."""
    for aid,b in _bindings(run).items():
        if (token.get('execution_activation_id')==aid or token.get('decision_activation_id')==aid
                or token.get('id')==b['target'].get('token_id')) and held(run,aid):
            return True
    return False


class WorkflowConversationStore:
    """Shared owner ledger port backed only by the authoritative run CAS."""
    def __init__(self,storage,run_id,activation_id,conversation_id):
        self.storage,self.run_id,self.activation_id=storage,run_id,activation_id
        self.conversation_id=conversation_id
    def owns(self):
        run=self.storage.get_run(self.run_id,refresh_atomic=True)
        b=_bindings(run).get(self.activation_id)
        if b is not None:return b['state']['conversation_id']==self.conversation_id
        actor=next((a for a in run.get('activations',[]) if a['id']==self.activation_id),None)
        return bool(actor and actor.get('conversation_checkpoint',{}).get('conversation_id')==self.conversation_id)
    def read(self):
        run=self.storage.get_run(self.run_id,refresh_atomic=True)
        b=_bindings(run).get(self.activation_id)
        if b is None:
            if self.owns():raise ConversationError('request_expired')
            raise ConversationError('conversation_missing')
        if b['state']['conversation_id']!=self.conversation_id: raise ConversationError('context_conflict')
        return deepcopy(b['state'])
    def update(self,mutation):
        result=[]
        def mutate(run):
            if run.get('parent_link'): raise ConversationError('nested_conversation_unsupported')
            b=_bindings(run).get(self.activation_id)
            if b is None: raise ConversationError('conversation_missing')
            state,value=mutation(deepcopy(b['state']))
            validate(state)
            if state['conversation_id']!=self.conversation_id: raise ConversationError('context_conflict')
            b['state']=state
            compact_conversations(run)
            result.append(value)
        self.storage.update_run(self.run_id,mutate,'conversation_native_observation',{'activation_id':self.activation_id})
        return result[0]


def for_task(registry, task, port, *, profile_fingerprint):
    """Engine-only factory. Reserved workflow association owns this native actor."""
    from .workflow_hooks import owner as association_for
    association=association_for(registry.log_dir,task.task_id,strict=True)
    if association is None: return None
    from .workflows import WorkflowStore
    from .workflow_steering import target as current_target
    from .conversation_live_owner import ConversationOwner
    from .backends.base import Accumulator
    from .conversation_documents import CONTRACT
    from .state_access import storage_operation
    import asyncio
    import uuid
    storage=WorkflowStore(root=registry.state_layout.root)
    rid,aid=association['workflow_run_id'],association['activation_id']
    cid=str(uuid.uuid5(uuid.NAMESPACE_URL,'harflow:workflow:'+rid+':'+aid+':'+task.task_id))
    class WorkflowConversationOwner(ConversationOwner):
        def __init__(self):
            self.state_layout=registry.state_layout
            self.task_id,self.initial_text,self.port=task.task_id,task.prompt,port
            self.conversation_id=cid
            self.ledger=WorkflowConversationStore(storage,rid,aid,cid)
            self._terminal=None
            self._response_acc=Accumulator()
            self._response_text=''
            def create(run):
                if run.get('parent_link'): raise ConversationError('nested_conversation_unsupported')
                if aid in _bindings(run): raise ConversationError('conversation_owner_exists')
                activation=_activation(run,aid)
                role='worker' if activation['role']=='node' else activation['role']
                witness={'role':role,'workflow_run_id':rid,'activation_id':aid,
                         'token_id':(activation.get('token') or {}).get('id')}
                if role=='builder': target=current_target(run)
                elif role=='worker':
                    from .worker_steering import current_target as issued
                    target=issued(storage,run,witness)
                elif role=='orchestrator':
                    from .orchestrator_steering import current_target as issued
                    target=issued(storage,run,witness)
                else: raise ConversationError('target_unsupported')
                enroll(run,target=target,state=delivery.new_state(cid,task.task_id,port.session_id,1,task.task_id))
                profile=getattr(port,'profile',None)
                profile_name=(profile.get('profile_name') or 'native') if isinstance(profile,dict) else None
                _bindings(run)[aid]['native_owner']=dict(task_id=task.task_id,transport=port.transport,profile_fingerprint=profile_fingerprint,profile_name=profile_name)
            storage.update_run(rid,create,'conversation_native_enrolled',{'activation_id':aid})
        @storage_operation
        async def observe(self,observation):
            if observation.kind=='input_issued':
                self._response_acc=Accumulator(); self._response_text=''
            events=self.port.normalize_observation(observation,self._response_acc)
            for event in events:
                if event.get('kind')=='assistant_delta':
                    self._response_text+=event.get('text','')
                    if len(self._response_text.encode('utf-8'))>MAX_CAPTURE_BYTES:
                        await asyncio.to_thread(self.ledger.update,lambda state:(delivery.mark_runtime_fault(state,
                            code='response_capture_full',generation=state['generation'],turn_id=state['turn_id']),None))
                        raise ConversationError('response_capture_full')
            await super().observe(observation)
        async def _settle_terminal(self):
            terminal=self._terminal
            if terminal is None or not self.port.can_issue: return
            if terminal.identity!=self.port.identity or terminal.identity.turn_id is None:
                raise ConversationError('native_terminal_mismatch')
            def finished(state):
                if not self._matches(state,terminal.identity): raise ConversationError('turn_mismatch')
                item=next((m for m in state['messages'] if m['status'] in {'claimed','unknown'}),None)
                if item:
                    claim=item['claim']
                    state=delivery.record_receipt(state,receipt=dict(claim=claim,task_id=self.task_id,
                        turn_id=terminal.identity.logical_turn,generation=terminal.identity.generation,kind='input_consumed'))
                state=delivery.record_response(state,generation=terminal.identity.generation,turn_id=terminal.identity.logical_turn)
                return delivery.observe_boundary(state,generation=terminal.identity.generation,turn_id=terminal.identity.logical_turn),None
            # Capture the exact response before next_input advances its head.
            summary=self._response_acc.summary or self._response_text
            def capture(run):
                b=_bindings(run)[aid]
                state,_=finished(b['state']); b['state']=state
                if not state['cumulative_issuances'] and not state['messages']:
                    return
                observation=_capture(dict(task_id=self.task_id,status=terminal.payload.get('status'),
                    session_id=terminal.identity.session_id,summary=summary),settlement='native_response')
                if not state['cumulative_issuances']:
                    observation['summary']=''  # Initial ordinary output remains in the owned task log.
                    observation['summary_omitted']=True
                entry=dict(task_id=self.task_id,generation=terminal.identity.generation,
                           turn_id=terminal.identity.logical_turn,result=observation)
                append_capture(run,aid,entry)
            await asyncio.to_thread(storage.update_run,rid,capture,'conversation_native_response',{'activation_id':aid})
    owner=WorkflowConversationOwner()
    task.conversation=dict(contract=CONTRACT,conversation_id=cid,transport=port.transport,profile_fingerprint=profile_fingerprint)
    return owner


def _native_descriptor_matches(source,binding):
    from .conversation_documents import CONTRACT
    native=binding.get('native_owner')
    expected=dict(contract=CONTRACT,conversation_id=binding['state']['conversation_id'],
        transport=native['transport'],profile_fingerprint=native['profile_fingerprint']) if native else None
    if (native is None or source.task_id!=native['task_id']
            or getattr(source,'conversation',None)!=expected
            or binding['state']['head_task_id']!=source.task_id):
        raise ConversationError('native_owner_mismatch')
    if source.session_id is not None and binding['state']['native_context_id'] is not None and source.session_id!=binding['state']['native_context_id']:
        raise ConversationError('native_owner_mismatch')


def _qualify_native_source(registry,source,binding):
    from .conversation_task_transport import qualify_saved_source
    if qualify_saved_source(registry,source) is not True:
        raise ConversationError('conversation_profile_unqualified')
    _native_descriptor_matches(source,binding)


def commit_conversation(storage,registry,run_id,activation_id,*,expected,steering_request,monitor=False):
    """Explicit operator issuance of an existing capped, same-session commit turn.

    This never treats a native terminal as process exit and never upgrades the
    original source result into graph/draft authority. The issued steering episode
    becomes the sole commit authority; its existing validators remain unchanged.
    """
    from .workflow_steering import _transport,_authorize,_source_check,_request,_digest
    from .conversation_runtime import _record
    from .steering_admission import admit as steering_admit
    request=_request(steering_request)
    if request['action']!='open' or request.get('allow_fresh_context') is not False:
        raise ConversationError('explicit_same_session_commit_required')
    if not isinstance(expected,dict) or set(expected)!={'conversation_id','head_task_id','generation','turn_id','ledger_epoch'}:
        raise ConversationError('invalid_request')
    principal=_transport(storage,monitor)
    initial=storage.get_run(run_id,refresh_atomic=True)
    _authorize(initial,principal,monitor)
    binding=_bindings(initial).get(activation_id)
    retired=_retired(initial,activation_id,expected['conversation_id'])
    if binding is None and retired is None:
        if _activation(initial,activation_id).get('conversation_checkpoint'):raise ConversationError('request_expired')
        raise ConversationError('commit_role_unsupported')
    role=binding['target']['role'] if binding is not None else retired['role']
    if (request['target']['role']!=role or request['target']['activation_id']!=activation_id
            or request['target']['workflow_run_id']!=run_id): raise ConversationError('target_stale')
    digest=_digest({k:v for k,v in request.items() if k!='request_id'})
    principal_binding=_digest({'state_root':str(storage.root),'principal':'native_human' if monitor else principal,
                               'epoch':request['expected_interaction_epoch']})
    request_key=(str(request['ledger_epoch'])+':'+request['request_id']) if request['schema_version']==2 else request['request_id']
    def retained_replay(run):
        b=_bindings(run).get(activation_id) or _retired(run,activation_id,expected['conversation_id'])
        old=run['steering']['requests'].get(request_key)
        if old is None:return None
        if b is None or b.get('commit_source')!=expected or b.get('steering_episode_id')!=old['episode_id']:
            raise ConversationError('request_conflict')
        return steering_admit(storage,run,principal,monitor,request,digest,principal_binding)
    if request_key in initial['steering']['requests']:
        replay=[]
        storage.update_run(run_id,lambda run:replay.append(retained_replay(run)),'conversation_commit_replayed',{'activation_id':activation_id})
        receipt,issue=replay[0]
        if issue:
            from .workflows import _launch
            _launch(storage,run_id)
        return receipt
    if binding is None:raise ConversationError('request_expired')
    _source_check(storage,initial)
    source=_record(registry,expected['head_task_id'])
    if (source.status!='completed' or type(source.exit_code) is not int or source.exit_code!=0
            or source.output_drained is not True
            or role=='builder' and (not source.workflow_builder or source.freedom!='read_only')
            or role=='orchestrator' and source.freedom!='read_only'):
        raise ConversationError('source_not_physically_settled')
    result=[]; scheduled=[]
    def mutate(run):
        _authorize(run,principal,monitor)
        replay=retained_replay(run)
        if replay is not None:
            result.append(replay[0]);scheduled.append(replay[1]);return
        _source_check(storage,run)
        if run.get('parent_link'): raise ConversationError('nested_conversation_unsupported')
        b=_bindings(run).get(activation_id)
        if b is None or b['accepted']: raise ConversationError('target_stale')
        state=b['state']
        if any(state[key]!=expected[key] for key in expected): raise ConversationError('target_stale')
        if not state['closed'] or delivery.protects_retention(state) or not state['cumulative_issuances']:
            raise ConversationError('conversation_not_settled')
        if source.task_id!=state['head_task_id'] or source.session_id!=state['native_context_id']:
            raise ConversationError('context_conflict')
        activation=_activation(run,activation_id)
        candidate=(activation['tasks'][-1].get('candidate') if role=='worker' else run.get('orchestrator_config') or run['definition']['orchestrator'])
        if not candidate: raise ConversationError('source_profile_changed')
        if (source.backend!=candidate['backend'] or str(source.repo_path)!=run['repo_path']
                or source.model!=candidate.get('model') or source.reasoning_effort!=candidate.get('reasoning_effort')
                or source.network!=(activation['tasks'][-1].get('network',run.get('network')) if role=='worker' else run.get('network'))):
            raise ConversationError('source_profile_changed')
        native=b.get('native_owner')
        configured=candidate.get('conversation_profile')
        if native is not None:
            _native_descriptor_matches(source,b)
            if native.get('profile_name') not in {'native','shipped-medium35-high'}:
                raise ConversationError('source_profile_unknown')
            if configured is not None and configured!=native['profile_name']:
                raise ConversationError('source_profile_changed')
        elif configured is not None:
            raise ConversationError('source_profile_unknown')
        from .workflows import _candidate_key
        if role in {'builder','orchestrator'}:
            run.setdefault('sessions',{})[role]=dict(candidate=role+':'+_candidate_key(candidate),
                task_id=source.task_id,session_id=source.session_id)
        token=_handoff.set((run_id,activation_id))
        source_token=_handoff_source.set(source)
        try:
            receipt,issue=steering_admit(storage,run,principal,monitor,request,digest,principal_binding)
        finally:
            _handoff_source.reset(source_token)
            _handoff.reset(token)
        if b.get('steering_episode_id') not in (None,receipt['episode_id']):
            raise ConversationError('commit_already_issued')
        b['steering_episode_id']=receipt['episode_id']
        b['commit_source']=deepcopy(expected)
        result.append(receipt); scheduled.append(issue)
    storage.update_run(run_id,mutate,'conversation_explicit_commit_requested',{'activation_id':activation_id})
    if scheduled[0]:
        from .workflows import _launch
        _launch(storage,run_id)
    return result[0]


def control_request(storage,registry,command,*,monitor=False):
    """Exact idempotent cancel/promotion in the existing activity authority CAS."""
    from .conversation_requests import validate_control
    from .workflow_steering import _transport,_authorize,_source_check
    from .conversation_runtime import _record,_qualified
    command=validate_control(command); target=command['target']; expected=command['expected']
    if target['kind']!='workflow_activation': raise ConversationError('target_unsupported')
    principal=_transport(storage,monitor)
    initial=storage.get_run(target['workflow_run_id'],refresh_atomic=True)
    _authorize(initial,principal,monitor)
    initial_binding=_bindings(initial).get(target['activation_id'])
    if initial_binding is None:
        if _activation(initial,target['activation_id']).get('conversation_checkpoint'):raise ConversationError('request_expired')
        raise ConversationError('activation_unenrolled')
    previous=next((m[key] for m in initial_binding['state']['messages'] for key in ('promotion','cancellation')
                   if m.get(key) and m[key]['request_id']==command['request_id']),None)
    source=None
    if previous is None and command['operation']=='promote_now':
        _source_check(storage,initial)
        source=_record(registry,expected['head_task_id'])
        if initial_binding.get('native_owner'): _qualify_native_source(registry,source,initial_binding)
        else: _qualified(source)
    result=[]
    def mutate(run):
        _authorize(run,principal,monitor)
        if run.get('parent_link'): raise ConversationError('nested_conversation_unsupported')
        b=_bindings(run).get(target['activation_id'])
        if b is None: raise ConversationError('activation_unenrolled')
        state=b['state']
        if state['conversation_id']!=expected['conversation_id']: raise ConversationError('target_stale')
        activation=_activation(run,target['activation_id'])
        if not any(t['task_id']==target['task_id'] for t in activation['tasks']): raise ConversationError('target_stale')
        replay=next((m[key] for m in state['messages'] for key in ('promotion','cancellation')
                     if m.get(key) and m[key]['request_id']==command['request_id']),None)
        if replay is None:
            if any(state[key]!=expected[key] for key in expected): raise ConversationError('target_stale')
            if command['operation']=='promote_now':
                _source_check(storage,run)
                if b.get('native_owner'): _native_descriptor_matches(source,b)
                elif not state['boundary']: raise ConversationError('interrupt_unsupported')
        transition=delivery.cancel_control if command['operation']=='cancel' else delivery.promote_now
        new,receipt=transition(state,request_id=command['request_id'],message_request_id=command['message_request_id'],
            target_task_id=expected['head_task_id'],expected_generation=expected['generation'],
            expected_turn_id=expected['turn_id'],expected_epoch=expected['ledger_epoch'])
        b['state']=new
        result.append(control_ack(run['workflow_run_id'], target['activation_id'],
            new['conversation_id'], command['operation'], receipt))
    storage.update_run(target['workflow_run_id'],mutate,'conversation_control_requested',{'activation_id':target['activation_id']})
    return result[0]


def commit_builder_conversation(storage,registry,run_id,activation_id,**options):
    """Compatibility name for the builder-only caller; role is still exact."""
    return commit_conversation(storage,registry,run_id,activation_id,**options)


def same_session_commit_source(run,activation_id):
    """Engine-local admission context, never a public caller permission boolean."""
    if _handoff.get()!=(run['workflow_run_id'],activation_id): return None
    return _handoff_source.get()


def commit_context_mismatch(run,turn,snapshot):
    linked=any(b.get('steering_episode_id')==turn['episode_id'] for b in _bindings(run).values())
    previous=turn.get('previous_session')
    return bool(linked and turn.get('allow_fresh_context') is False and previous
                and snapshot.get('session_id')!=previous['session_id'])


def commit_profile_options(run,turn):
    """Restore the source owner's actual qualified native profile, never default it."""
    binding=next((b for b in _bindings(run).values() if b.get('steering_episode_id')==turn['episode_id']),None)
    if binding is None: return {}
    native=binding.get('native_owner')
    if native is None:
        if turn['candidate'].get('conversation_profile') is not None:
            raise ConversationError('source_profile_unknown')
        return {}
    profile=native.get('profile_name')
    if profile not in {'native','shipped-medium35-high'}:
        raise ConversationError('source_profile_unknown')
    return {'conversation_profile':profile}


def bind_commit_input(supervisor,turn,prompt,profile_options,cap):
    """Bind exact generated prompt before a linked native capped response issues."""
    if not profile_options:return
    import hashlib
    raw=prompt.encode('utf-8')
    if not raw or len(raw)>1024*1024: raise ConversationError('commit_input_too_large')
    previous=turn.get('previous_session')
    if previous is None or turn.get('allow_fresh_context') is not False:
        raise ConversationError('commit_issuance_mismatch')
    proof=dict(sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),source_task_id=previous['task_id'],
        source_session_id=previous['session_id'],cap=cap,profile_name=profile_options['conversation_profile'])
    def mutate(run):
        issued=next(t for t in run['steering']['turns'] if t['turn_id']==turn['turn_id'])
        if issued['state'] not in {'spawn_intent','running'}:raise ConversationError('commit_issuance_mismatch')
        binding=next((b for b in _bindings(run).values() if b.get('steering_episode_id')==issued['episode_id']),None)
        if binding is None or commit_profile_options(run,issued)!=profile_options:raise ConversationError('commit_profile_mismatch')
        if issued.get('conversation_commit_input') not in (None,proof):raise ConversationError('commit_input_mismatch')
        issued['conversation_commit_input']=proof
    supervisor.update(mutate,'steering_commit_input_bound',{'turn_id':turn['turn_id']})


def extension_text_bytes(run):
    return sum(sum(len(m['text'].encode('utf-8')) for m in b['state']['messages'])
        +sum(len(c['result'].get('summary','').encode('utf-8')) for c in b['captures'])
        for b in _bindings(run).values())


def shared_text_bytes(run):
    from .steering_window import retained_text_bytes
    return retained_text_bytes(run)


def pending_capture_reserve(run):
    reserve=0
    for b in _bindings(run).values():
        if not any(m['status'] in {'queued','claimed','unknown'} for m in b['state']['messages']):continue
        latest=b['captures'][-1] if b['captures'] else None
        credit=(len(latest['result'].get('summary','').encode('utf-8')) if latest and latest['result'].get('settlement') in {'native_response','physical_response'} else 0)
        reserve+=max(0,RESPONSE_TEXT_RESERVE-credit)
    return reserve


def append_capture(run,activation_id,capture):
    b=_bindings(run)[activation_id]
    if capture not in b['captures']:b['captures'].append(capture)
    compact_conversations(run,reserve_text_bytes=pending_capture_reserve(run),reserve_metadata_bytes=pending_metadata_reserve(run))


def compact_conversations(run,*,reserve_text_bytes=0,reserve_metadata_bytes=0,defer_pressure=False):
    """Retire only settled prose; preserve checkpoint/effect/ownership evidence.

    Legacy steering namespace and managed-task retirement remain the existing
    steering lifecycle's responsibility. This function never removes a binding,
    activation, TaskRecord, native identity, pending claim or linked episode.
    """
    if not _bindings(run):return
    text_limit=ROOT_TEXT_BYTES-reserve_text_bytes
    metadata_limit=MAX_DOCUMENT_BYTES-reserve_metadata_bytes
    closed=[]
    for aid,b in _bindings(run).items():
        if _closed_history(run,aid,b):
            b.setdefault('history_sequence',run.get('sequence',0)+1)
            closed.append((b['history_sequence'],aid,b))
    for index,(_,aid,b) in enumerate(sorted(closed,key=lambda row:row[:2])):
        if len(closed)-index<=64 and len(encoded(run))<=metadata_limit:break
        _retire_binding(run,aid,b)
    def pressured():
        return (shared_text_bytes(run)>text_limit or len(encoded(run))>metadata_limit
                or any(len(b['captures'])>MAX_HISTORY for b in _bindings(run).values())
                or history_counts(run)[0]>32 or history_counts(run)[1]>64)
    if not pressured():return
    candidates=[]
    for aid,b in sorted(_bindings(run).items()):
        state=b['state']
        closed=_closed_history(run,aid,b)
        candidates_captures=b['captures'] if closed else b['captures'][:-1]
        for index,capture in enumerate(candidates_captures):
            if capture['result'].get('settlement') in {'native_response','physical_response'}:
                candidates.append((capture['generation'],aid,0,index,capture))
        for message in state['messages']:
            current=(message['claim'] is not None and message['claim']['destination_generation']==state['generation'])
            if message['status'] in {'consumed','cancelled'} and (closed or not current):
                candidates.append((message['sequence'],aid,1,message['sequence'],message))
    changed=set()
    for _,aid,kind,_,entry in sorted(candidates,key=lambda row:row[:4]):
        if not pressured():break
        b=_bindings(run)[aid]
        if kind==0:
            if entry not in b['captures'] or entry is b['captures'][-1] and not _closed_history(run,aid,b):continue
            b['captures'].remove(entry)
            b['capture_history_floor']=max(b.get('capture_history_floor',0),entry['generation'])
            b['omitted_captures']=b.get('omitted_captures',0)+1
        else:
            b['state']['messages'].remove(entry);changed.add(aid)
    for aid in changed:
        state=_bindings(run)[aid]['state'];state['ledger_epoch']+=1;validate(state)
    if pressured() and not defer_pressure:
        raise ConversationError('protected_conversation_window_full')


def _closed_history(run,aid,b):
    state=b['state']
    if not state['closed'] or delivery.protects_retention(state):return False
    actor=next((a for a in run.get('activations',[]) if a['id']==aid),None)
    if actor is None or not any(t['task_id']==state['head_task_id'] for t in actor['tasks']):return False
    if b['accepted'] and state['cumulative_issuances']==0 and _accepted(run,actor):return True
    linked=b.get('steering_episode_id')
    episode=next((ep for ep in run['steering']['episodes'] if ep['episode_id']==linked),None)
    if episode is None or episode['hold'] or episode['state']!='committed':return False
    turns=[t for t in run['steering']['turns'] if t['episode_id']==linked and t['state']=='settled' and t.get('outcome')=='committed']
    if not turns:return False # Archive proof is the existing lifecycle's responsibility.
    if b['target']['role']=='builder':
        return any(c['turn_id']==t['turn_id'] for c in run['steering']['commits'] for t in turns)
    from .steering_lifecycle import _effect_proofs
    proven=_effect_proofs(None,run,{run['workflow_run_id']:(run,[run['workflow_run_id']])})
    return any(t['turn_id'] in proven for t in turns)


def _retired(run,activation_id,conversation_id=None):
    items=run.get('steering',{}).get('conversation_retention',{}).get('tombstones',[])
    return next((t for t in reversed(items) if t['activation_id']==activation_id
                 and (conversation_id is None or t['conversation_id']==conversation_id)),None)


def _retire_binding(run,aid,b):
    state=b['state'];actor=_activation(run,aid)
    retention=run['steering'].setdefault('conversation_retention',dict(sequence=0,history_floor=0,tombstones=[],
        cumulative=dict(retired_conversations=0,retired_issuances=0,retired_settled_responses=0,incomplete_usage_conversations=0,
                        observed_cost_usd=None,usage_complete=True)))
    retention['sequence']+=1;sequence=retention['sequence']
    cumulative=retention['cumulative'];accounting=state['accounting']
    cumulative['retired_conversations']+=1
    cumulative['retired_issuances']+=state['cumulative_issuances']
    cumulative['retired_settled_responses']+=accounting['settled_responses']
    cumulative['incomplete_usage_conversations']+=not accounting['usage_complete']
    if accounting['usage_complete'] and accounting['cost_basis']=='response_local' and cumulative['usage_complete']:
        cumulative['observed_cost_usd']=(cumulative['observed_cost_usd'] or 0)+accounting['observed_cost_usd']
    else:
        cumulative['usage_complete']=False;cumulative['observed_cost_usd']=None
    tombstone=dict(sequence=sequence,activation_id=aid,role=b['target']['role'],conversation_id=state['conversation_id'],
        head_task_id=state['head_task_id'],native_context_id=state['native_context_id'],generation=state['generation'],turn_id=state['turn_id'],closed_epoch=state['ledger_epoch']+1,
        cumulative_issuances=state['cumulative_issuances'],accounting=deepcopy(accounting),
        steering_episode_id=b.get('steering_episode_id'),commit_source=deepcopy(b.get('commit_source')))
    retention['tombstones'].append(tombstone)
    if len(retention['tombstones'])>64:
        dropped=retention['tombstones'][:-64];retention['tombstones']=retention['tombstones'][-64:]
        retention['history_floor']=max(retention['history_floor'],dropped[-1]['sequence'])
    actor['conversation_checkpoint']=dict(conversation_id=state['conversation_id'],head_task_id=state['head_task_id'],
        native_context_id=state['native_context_id'],generation=state['generation'],turn_id=state['turn_id'],
        closed_epoch=state['ledger_epoch']+1,ordinary_source_accepted=b['accepted'],retired_sequence=sequence)
    del run['steering'][EXTENSION][aid]


def _legacy_history_counts(run):
    from .steering_window import protected_turn_ids
    from .steering_lifecycle import _effect_proofs
    ledger=run['steering']
    effects=_effect_proofs(None,run,{run['workflow_run_id']:(run,[run['workflow_run_id']])})
    protected=protected_turn_ids(run,effects)
    settled={t['turn_id'] for t in ledger['turns'] if t['state']=='settled' and t['turn_id'] not in protected}
    by_id={t['turn_id']:t for t in ledger['turns']}
    receipts=sum(r['turn_id'] is None or r.get('retired_turn') or r['turn_id'] in settled
                 for r in ledger['requests'].values())
    return len(settled),receipts


def history_counts(run):
    turns,receipts=_legacy_history_counts(run);response_ids=set()
    for aid,b in _bindings(run).items():
        closed=_closed_history(run,aid,b);state=b['state']
        for index,capture in enumerate(b['captures']):
            if (closed or index!=len(b['captures'])-1) and capture['result'].get('settlement') in {'native_response','physical_response'}:
                response_ids.add((state['conversation_id'],capture['generation']))
        for m in state['messages']:
            current=m['claim'] and m['claim']['destination_generation']==state['generation']
            if m['status'] in {'consumed','cancelled'} and (closed or not current):
                receipts+=1+bool(m['promotion'])+bool(m['cancellation'])
                if m['status']=='consumed':response_ids.add((state['conversation_id'],m['claim']['destination_generation']))
    return turns+len(response_ids),receipts


def validate_retention(value,run):
    if not isinstance(value,dict) or set(value)!={'sequence','history_floor','tombstones','cumulative'}:
        raise ValueError('Malformed conversation retention')
    if any(type(value[k]) is not int or value[k]<0 for k in ('sequence','history_floor')) or value['history_floor']>value['sequence']:
        raise ValueError('Malformed conversation retention sequence')
    cumulative=value['cumulative']
    if not isinstance(cumulative,dict) or set(cumulative)!={'retired_conversations','retired_issuances','retired_settled_responses','incomplete_usage_conversations','observed_cost_usd','usage_complete'}:
        raise ValueError('Malformed conversation retention accounting')
    if any(type(cumulative[k]) is not int or cumulative[k]<0 for k in ('retired_conversations','retired_issuances','retired_settled_responses','incomplete_usage_conversations')) or type(cumulative['usage_complete']) is not bool:
        raise ValueError('Malformed conversation retention accounting')
    if cumulative['retired_conversations']!=value['sequence'] or not isinstance(value['tombstones'],list) or len(value['tombstones'])>64:
        raise ValueError('Malformed conversation tombstones')
    import math
    cost=cumulative['observed_cost_usd']
    if cost is not None and (type(cost) not in (int,float) or not math.isfinite(cost) or cost<0):raise ValueError('Malformed retired cost')
    if not cumulative['usage_complete'] and cost is not None:raise ValueError('Malformed retired usage completeness')
    sequence=value['history_floor']
    for item in value['tombstones']:
        fields={'sequence','activation_id','role','conversation_id','head_task_id','native_context_id','generation','turn_id','closed_epoch','cumulative_issuances','accounting','steering_episode_id','commit_source'}
        if not isinstance(item,dict) or set(item)!=fields or type(item['sequence']) is not int or item['sequence']<=sequence:
            raise ValueError('Malformed conversation tombstone')
        for key in ('activation_id','conversation_id','head_task_id','turn_id'):identifier(item[key])
        if item['native_context_id'] is not None:identifier(item['native_context_id'])
        if item['role'] not in {'builder','worker','orchestrator'}:raise ValueError('Malformed retired role')
        for key in ('generation','closed_epoch','cumulative_issuances'):
            if type(item[key]) is not int or item[key]<0:raise ValueError('Malformed retired counter')
        sample=delivery.new_state(item['conversation_id'],item['head_task_id'],None,item['generation'],item['turn_id'])
        sample['accounting']=deepcopy(item['accounting']);sample['cumulative_issuances']=item['cumulative_issuances'];validate(sample)
        if item['steering_episode_id'] is not None:identifier(item['steering_episode_id'])
        proof=item['commit_source']
        if proof is not None:
            if not isinstance(proof,dict) or set(proof)!={'conversation_id','head_task_id','generation','turn_id','ledger_epoch'}:
                raise ValueError('Malformed retired commit source')
            if any(proof[key]!=item[key] for key in ('conversation_id','head_task_id','generation','turn_id')) or type(proof['ledger_epoch']) is not int or not 0<=proof['ledger_epoch']<item['closed_epoch']:
                raise ValueError('Malformed retired commit source')
        sequence=item['sequence']
    if sequence!=value['sequence']:raise ValueError('Malformed conversation tombstone tail')


def pending_metadata_reserve(run):
    return 32*1024*sum(any(m['status'] in {'queued','claimed','unknown'} for m in b['state']['messages'])
                       for b in _bindings(run).values())


def own_builder_preview_applied(run,task_id):
    """Advance only the initial builder's own successful preview witness.

    External canvas/source/owner changes remain conflicts. Once an explicit
    conversation owns input, ordinary preview mutation is already fenced.
    """
    if run.get('kind')!='builder':return
    from .workflow_steering import target as builder_target,ordinary_builder_identity
    actor=ordinary_builder_identity(run)
    for aid,b in _bindings(run).items():
        state=b['state'];before=b['target']
        if (before['role']!='builder' or actor['activation_id']!=aid or actor['task_id']!=task_id
                or state['head_task_id']!=task_id or state['closed'] or state['cumulative_issuances']
                or delivery.protects_retention(state)):
            continue
        now=builder_target(run)
        if before['draft_revision']+1!=now['draft_revision'] or before['checkpoint_revision']+1!=now['checkpoint_revision']:
            continue
        if any(before[key]!=now[key] for key in before if key not in {'draft_revision','checkpoint_revision','preview_hash'}):
            continue
        b['target']=deepcopy(now)
