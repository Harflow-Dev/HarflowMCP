"""Core tests use explicit native observations, never process-start shortcuts."""
import uuid
from concurrent.futures import ThreadPoolExecutor
import pytest
from harflow_mcp import conversation_delivery as delivery
from harflow_mcp.conversation_documents import ConversationError, validate
from harflow_mcp.conversation_store import ConversationStore
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session

def uid(): return str(uuid.uuid4())
def initial(): return delivery.new_state(uid(),'task-a','native-a',0,'turn-a')
def send(state,text='one',mode='queue',request=None):
    return delivery.admit(state,request_id=request or uid(),text=text,delivery=mode,target_task_id=state['head_task_id'],expected_epoch=state['ledger_epoch'])
def boundary(state): return delivery.observe_boundary(state,generation=state['generation'],turn_id=state['turn_id'])
def receipt(item,n=1):
    claim=item['claim']
    return dict(claim=claim,task_id=claim['destination_task_id'],turn_id=claim['destination_turn_id'],generation=claim['destination_generation'],kind='input_consumed')

def test_fifo_one_response_each_and_now_preserves_older_queue():
    state,a = send(initial(),'first'); state,b = send(state,'second'); state,now = send(state,'urgent','now')
    assert delivery.claim_next(state,claim_id=uid())[1] is None
    state = boundary(state)
    state,claimed = delivery.claim_next(state,claim_id=uid())
    assert claimed['request_id'] == now['request_id']
    assert [m['text'] for m in state['messages'] if m['status']=='queued'] == ['first','second']
    state = delivery.record_receipt(state,receipt=receipt(claimed))
    assert delivery.claim_next(state,claim_id=uid())[1] is None
    state,claimed = delivery.claim_next(boundary(state),claim_id=uid())
    assert claimed['request_id'] == a['request_id']
    state = delivery.record_receipt(state,receipt=receipt(claimed,2))
    state,claimed = delivery.claim_next(boundary(state),claim_id=uid())
    assert claimed['request_id'] == b['request_id']

def test_exact_replays_conflicts_cancel_and_now_busy():
    state,item = send(initial(),mode='now')
    again, replay = send(state,mode='now',request=item['request_id'])
    assert again == state and replay == item
    with pytest.raises(ConversationError,match='request_conflict'): send(state,'different',request=item['request_id'])
    with pytest.raises(ConversationError,match='now_busy'): send(state,'another',mode='now')
    state = delivery.cancel(state,item['request_id'])
    assert delivery.cancel(state,item['request_id']) == state
    state,item = send(state)
    state,claimed = delivery.claim_next(boundary(state),claim_id=uid())
    with pytest.raises(ConversationError,match='already_claimed'): delivery.cancel(state,item['request_id'])


def test_retained_original_epoch_replays_but_changed_epoch_conflicts(monkeypatch):
    monkeypatch.setattr(delivery, 'MAX_HISTORY', 1)
    state, original = send(initial())
    for text in ('settled one', 'settled two'):
        state, item = send(state, text)
        state = delivery.cancel(state, item['request_id'])
    assert state['ledger_epoch'] > 0
    arguments = dict(request_id=original['request_id'], text=original['text'], delivery='queue', target_task_id='task-a')
    assert delivery.admit(state, expected_epoch=0, **arguments)[1] == original
    with pytest.raises(ConversationError, match='request_conflict'):
        delivery.admit(state, expected_epoch=state['ledger_epoch'], **arguments)


def test_text_history_compacts_only_settled_entries_within_one_mebibyte():
    state, pending = send(initial(), 'protected')
    for _ in range(33):
        state, item = send(state, 'x' * (32 * 1024))
        state = delivery.cancel(state, item['request_id'])
    assert sum(len(item['text'].encode()) for item in state['messages']) <= 1024 * 1024
    assert next(item for item in state['messages'] if item['request_id'] == pending['request_id']) == pending
    assert state['ledger_epoch'] > 0

def test_unknown_never_replays_and_matching_late_receipt_recovers():
    state,item = send(initial()); state,claim = delivery.claim_next(boundary(state),claim_id=uid())
    state = delivery.mark_unknown(state,claim_id=claim['claim']['claim_id'])
    state,_ = send(state,'later')
    assert delivery.claim_next(state,claim_id=uid())[1] is None
    wrong = receipt(claim); wrong['claim'] = dict(wrong['claim'],native_context_id='foreign')
    with pytest.raises(ConversationError,match='receipt_mismatch'): delivery.record_receipt(state,receipt=wrong)
    spawn = receipt(claim); spawn['kind'] = 'running'
    with pytest.raises(ConversationError): delivery.record_receipt(state,receipt=spawn)
    state = delivery.record_receipt(state,receipt=receipt(claim))
    assert state['messages'][0]['status'] == 'consumed'
    assert delivery.record_receipt(state,receipt=receipt(claim)) == state
    with pytest.raises(ConversationError,match='turn_mismatch'): delivery.observe_boundary(state,generation=0,turn_id='turn-a')

def test_store_concurrent_admission_and_reopen(tmp_path):
    layout = resolve_layout(root=tmp_path/'state'); state=initial()
    with state_session(layout):
        store=ConversationStore(layout,state['conversation_id']); store.update(lambda old:(state,None))
        def write(n):
            with state_session(layout):
                return store.update(lambda old:send(old,str(n)))
        with ThreadPoolExecutor(max_workers=4) as workers:
            list(workers.map(write,range(16)))
        fresh=ConversationStore(layout,state['conversation_id']).read()
        assert sorted(m['text'] for m in fresh['messages']) == sorted(map(str,range(16)))
        assert [m['sequence'] for m in fresh['messages']] == list(range(1,17))

def test_publication_failure_after_replace_does_not_issue_or_reclaim(tmp_path,monkeypatch):
    layout=resolve_layout(root=tmp_path/'state'); state,item=send(initial()); state=boundary(state)
    with state_session(layout):
        store=ConversationStore(layout,state['conversation_id']); store.update(lambda old:(state,None))
        import harflow_mcp.conversation_store as module
        original=module.os.fsync; count=0
        def fail_directory(fd):
            nonlocal count
            count+=1
            if count==2: raise OSError('directory sync uncertain')
            original(fd)
        monkeypatch.setattr(module.os,'fsync',fail_directory)
        with pytest.raises(OSError): store.update(lambda old:delivery.claim_next(old,claim_id=uid()))
        monkeypatch.setattr(module.os,'fsync',original)
        observed=store.read()
        assert observed['messages'][0]['status']=='claimed'
        assert store.update(lambda old:delivery.claim_next(old,claim_id=uid())) is None

def test_strict_schema_utf8_and_backpressure():
    state=initial()
    for n in range(32): state,_=send(state,str(n))
    with pytest.raises(ConversationError,match='queue_full'): send(state,'overflow')
    with pytest.raises(ConversationError): send(initial(),'é'*(16*1024+1))
    with pytest.raises(ConversationError): validate(dict(initial(),public_authority=True))
    with pytest.raises(ConversationError,match='target_stale'):
        delivery.admit(initial(),request_id=uid(),text='x',delivery='queue',target_task_id='fresh-other',expected_epoch=0)

def test_pending_native_identity_and_immutable_binding():
    state=delivery.new_state(uid(),'task-a',None,0,'turn-a')
    state,_=send(state)
    state=boundary(state)
    assert delivery.claim_next(state,claim_id=uid())[1] is None
    state=delivery.bind_native_context(state,context_id='native-a',generation=0)
    assert delivery.claim_next(state,claim_id=uid())[1] is not None
    with pytest.raises(ConversationError,match='context_conflict'):
        delivery.bind_native_context(state,context_id='fresh-context',generation=0)


def test_driver_fault_is_unknown_and_never_reissued(tmp_path):
    layout=resolve_layout(root=tmp_path/'state'); state,_=send(initial()); state=boundary(state)
    with state_session(layout):
        store=ConversationStore(layout,state['conversation_id']); store.update(lambda old:(state,None))
        effects=[]
        def lost_ack(message):
            effects.append(message['request_id'])
            raise OSError('partial native write')
        with pytest.raises(OSError): delivery.drive_once(store,lost_ack)
        assert store.read()['messages'][0]['status']=='unknown'
        assert delivery.drive_once(store,lost_ack) is None
        assert len(effects)==1


def test_closed_wins_admission_and_invalid_duplicate_json_fails_closed(tmp_path):
    state=delivery.close(boundary(initial()))
    with pytest.raises(ConversationError,match='target_stale'): send(state)
    layout=resolve_layout(root=tmp_path/'state')
    with state_session(layout):
        store=ConversationStore(layout,state['conversation_id']); store.update(lambda old:(state,None))
        store.path.write_text('{"contract":"conversation_delivery_v1","contract":"other"}')
        with pytest.raises(ConversationError,match='ledger_unavailable'): store.read()


def test_reserved_destination_is_active_before_issue_and_receipt_is_exact():
    state,item=send(initial()); state=boundary(state)
    claim_id=uid()
    state,claimed=delivery.claim_next(state,claim_id=claim_id,
        destination_task_id='reserved-child',destination_turn_id='reserved-turn',destination_generation=1)
    assert (state['head_task_id'],state['turn_id'],state['generation'],state['boundary']) == ('reserved-child','reserved-turn',1,False)
    assert claimed['claim']['source_task_id']=='task-a'
    # A new message binds the in-flight response, not its settled predecessor.
    state,later=send(state,'during native issue')
    assert (later['target_task_id'],later['source_generation'],later['source_turn_id']) == ('reserved-child',1,'reserved-turn')
    with pytest.raises(ConversationError,match='target_stale'):
        delivery.admit(state,request_id=uid(),text='late old source',delivery='queue',target_task_id='task-a',expected_epoch=0)
    replay,replayed=delivery.claim_next(state,claim_id=claim_id,destination_task_id='reserved-child')
    assert replay==state and replayed==claimed
    with pytest.raises(ConversationError,match='claim_conflict'):
        delivery.claim_next(state,claim_id=claim_id,destination_task_id='different-child')
    for field,value in [('task_id','arbitrary-successor'),('turn_id','wrong-turn'),('generation',2),('kind','input_accepted')]:
        wrong=dict(receipt(claimed),**{field:value})
        with pytest.raises(ConversationError): delivery.record_receipt(state,receipt=wrong)
    state=delivery.mark_unknown(state,claim_id=claim_id)
    assert state['head_task_id']=='reserved-child'
    assert delivery.claim_next(boundary(state),claim_id=uid())[1] is None
    state=delivery.record_receipt(state,receipt=receipt(claimed))
    assert state['messages'][0]['status']=='consumed'
    assert state['head_task_id']=='reserved-child'


def test_promotion_distinct_idempotent_control_preserves_original_request():
    state,first=send(initial(),'first'); state,selected=send(state,'selected'); state,last=send(state,'last')
    control_id=uid()
    promoted,control=delivery.promote_now(state,request_id=control_id,
        message_request_id=selected['request_id'],target_task_id='task-a',expected_generation=0,expected_turn_id='turn-a',expected_epoch=0)
    assert control['request_id']==control_id
    assert promoted['messages'][1]['delivery']=='queue'
    replay,_=send(promoted,'selected',request=selected['request_id'])
    assert replay==promoted
    replay,replayed=delivery.promote_now(promoted,request_id=control_id,
        message_request_id=selected['request_id'],target_task_id='task-a',expected_epoch=0)
    assert replay==promoted and replayed==control
    with pytest.raises(ConversationError,match='request_conflict'):
        delivery.promote_now(promoted,request_id=control_id,message_request_id=first['request_id'],target_task_id='task-a',expected_epoch=0)
    with pytest.raises(ConversationError,match='now_busy'):
        delivery.promote_now(promoted,request_id=uid(),message_request_id=last['request_id'],target_task_id='task-a',expected_epoch=0)
    with pytest.raises(ConversationError,match='request_conflict'):
        send(promoted,'control collision',request=control_id)
    state,claimed=delivery.claim_next(boundary(promoted),claim_id=uid())
    assert claimed['request_id']==selected['request_id']
    state=delivery.record_receipt(state,receipt=receipt(claimed))
    state,claimed=delivery.claim_next(boundary(state),claim_id=uid())
    assert claimed['request_id']==first['request_id']


def test_compaction_rotates_epoch_and_refuses_evicted_request_replay():
    state=initial(); first=None; last=None
    for n in range(65):
        state,item=send(state,str(n))
        if first is None: first=item
        last=item
        state=delivery.cancel(state,item['request_id'])
    assert len(state['messages'])==64
    assert state['ledger_epoch']==1 and state['cumulative_issuances']==0
    with pytest.raises(ConversationError,match='request_expired'):
        delivery.admit(state,request_id=first['request_id'],text='0',delivery='queue',target_task_id='task-a',expected_epoch=0)
    replay,_=delivery.admit(state,request_id=last['request_id'],text='64',delivery='queue',target_task_id='task-a',expected_epoch=0)
    assert replay==state


def test_original_target_fingerprint_survives_resolved_head_advancement():
    state,item=delivery.admit(initial(),request_id=uid(),text='x',delivery='queue',target_task_id='task-a',original_target_task_id='conversation-origin',expected_epoch=0)
    state,claim=delivery.claim_next(boundary(state),claim_id=uid(),destination_task_id='task-b')
    replay,same=delivery.admit(state,request_id=item['request_id'],text='x',delivery='queue',target_task_id='task-b',original_target_task_id='conversation-origin',expected_epoch=0)
    assert replay==state and same['request_id']==item['request_id']
    with pytest.raises(ConversationError,match='request_conflict'):
        delivery.admit(state,request_id=item['request_id'],text='x',delivery='queue',target_task_id='task-b',original_target_task_id='different-origin',expected_epoch=0)


def test_accounting_dedup_unknown_and_basis_mixing_never_overclaim():
    state=initial()
    state=delivery.record_response(state,generation=0,turn_id='turn-a',cost_usd=2,cost_basis='response_local')
    assert state['accounting']['usage_complete'] and state['accounting']['observed_cost_usd']==2
    assert delivery.record_response(state,generation=0,turn_id='turn-a',cost_usd=2,cost_basis='response_local')==state
    with pytest.raises(ConversationError,match='usage_evidence_conflict'):
        delivery.record_response(state,generation=0,turn_id='turn-a',cost_usd=3,cost_basis='response_local')
    with pytest.raises(ConversationError,match='turn_mismatch'):
        delivery.record_response(state,generation=1,turn_id='other')
    state,_=send(state); state,item=delivery.claim_next(boundary(state),claim_id=uid())
    state=delivery.record_receipt(state,receipt=receipt(item),usage_evidence={'cost_usd':5,'cost_basis':'native_cumulative'})
    assert not state['accounting']['usage_complete'] and state['accounting']['observed_cost_usd'] is None
    assert state['accounting']['settled_responses']==2
    state,_=send(state); state,item=delivery.claim_next(boundary(state),claim_id=uid())
    state=delivery.mark_unknown(state,claim_id=item['claim']['claim_id'])
    assert not state['accounting']['usage_complete'] and state['accounting']['observed_cost_usd'] is None


def test_native_cumulative_totals_are_observed_not_summed():
    state=delivery.record_response(initial(),generation=0,turn_id='turn-a',cost_usd=2,cost_basis='native_cumulative')
    state,_=send(state); state,item=delivery.claim_next(boundary(state),claim_id=uid())
    state=delivery.record_receipt(state,receipt=receipt(item),usage_evidence={'cost_usd':3,'cost_basis':'native_cumulative'})
    assert state['accounting']['observed_cost_usd']==3
    assert state['accounting']['settled_responses']==2


def test_progress_is_generation_bound_observation_not_boundary_authority():
    state,_=send(initial())
    progress=delivery.record_progress(state,generation=0,logical_turn='turn-a',phase='waiting_tools',
        native_terminal='cancelled',pending_tools=1,unsettled_effects=1)
    assert progress['native_progress']['pending_tools']==1
    assert not progress['boundary'] and delivery.claim_next(progress,claim_id=uid())[1] is None
    with pytest.raises(ConversationError,match='turn_mismatch'):
        delivery.record_progress(progress,generation=1,logical_turn='future',phase='responding')
    with pytest.raises(ConversationError):
        delivery.record_progress(progress,generation=0,logical_turn='turn-a',phase='responding',pending_tools=True)
    fault=delivery.mark_runtime_fault(progress,code='native_unknown',generation=0,turn_id='turn-a')
    assert delivery.record_progress(fault,generation=0,logical_turn='turn-a',phase='responding')['native_progress']['phase']=='unknown'
    bounded=delivery.observe_boundary(state,generation=0,turn_id='turn-a')
    next_state,item=delivery.claim_next(bounded,claim_id=uid())
    assert next_state['native_progress']['generation']==1 and next_state['native_progress']['phase']=='issuing'
    assert next_state['native_progress']['logical_turn']==item['claim']['destination_turn_id']


@pytest.mark.parametrize('field',['pending_tools','pending_approvals','unsettled_effects'])
def test_now_tool_limit_refuses_before_intent_but_retained_replay_is_stable(field):
    state,item=send(initial(),mode='now')
    waiting=delivery.record_progress(state,generation=0,logical_turn='turn-a',phase='waiting_tools',**{field:1})
    assert send(waiting,mode='now',request=item['request_id'])[1]==item
    with pytest.raises(ConversationError,match='tool_interruption_unqualified'):
        send(waiting,'another now',mode='now')
    queued,_=send(waiting,'queue stays supported')
    with pytest.raises(ConversationError,match='tool_interruption_unqualified'):
        delivery.promote_now(queued,request_id=uid(),message_request_id=queued['messages'][-1]['request_id'],target_task_id='task-a',expected_epoch=0)
