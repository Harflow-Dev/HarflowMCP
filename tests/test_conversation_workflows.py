"""Real run-CAS admission/result arbitration, without provider execution."""
import copy
import uuid
import pytest
from harflow_mcp import workflows, workflow_steering as steering
from harflow_mcp import conversation_delivery as delivery, conversation_workflows as binding
from harflow_mcp.conversation_documents import ConversationError


def uid(): return str(uuid.uuid4())

@pytest.fixture
def active(tmp_path):
    storage=workflows.WorkflowStore(tmp_path/'state')
    descriptor={'name':'example','orchestrator':{'backend':'claude'},'nodes':[],'connections':[]}
    run=storage.create_run(descriptor,'Build',tmp_path,kind='builder',freedom='read_only',interaction_owner='monitor')
    rid=run['workflow_run_id']
    def setup(r):
        r.update(status='running',activations=[dict(id='activation-a',node_id='builder',role='builder',status='running',created_at=1,tasks=[dict(task_id='task-a',status='running')])])
        binding.enroll(r,target=steering.target(r),state=delivery.new_state(uid(),'task-a','native-a',0,'turn-a'))
    storage.update_run(rid,setup,'test_enrollment')
    return storage,rid


def admit(storage,rid,request=None,target=None):
    request=request or uid()
    result=[]
    def mutate(r):
        result.append(binding.admit(storage,r,principal=None,monitor=True,target=target or steering.target(r),
            request_id=request,text='Discuss before publishing.',mode='queue',expected_generation=0,expected_turn_id='turn-a',expected_epoch=0))
    storage.update_run(rid,mutate,'conversation_admitted')
    return result[0]


def result(storage,rid):
    outcome=[]
    def mutate(r):
        choice=binding.guard_result(r,activation_id='activation-a',task_id='task-a',generation=0,turn_id='turn-a',result={'summary':'ordinary JSON'})
        outcome.append(choice)
        if choice=='accept':
            r['activations'][0]['status']='completed'
            r['generated_definition']={'accepted':True}
    storage.update_run(rid,mutate,'result_cas')
    return outcome[0]


def test_admission_wins_holds_result_and_builder_publication(active):
    storage,rid=active
    item=admit(storage,rid)
    assert result(storage,rid)=='hold'
    run=storage.get_run(rid,refresh_atomic=True)
    assert steering.held(run)
    assert 'generated_definition' not in run
    assert run['steering']['conversation_delivery']['activation-a']['captures'][0]['result']=={'summary':'ordinary JSON'}
    assert run['steering']['conversation_delivery']['activation-a']['state']['messages'][0]['request_id']==item['request_id']
    assert run.get('messages',[])==[] and run.get('builder_messages',[])==[]


def test_result_wins_admission_is_stale(active):
    storage,rid=active
    assert result(storage,rid)=='accept'
    before=storage.get_run(rid)
    with pytest.raises(ConversationError,match='target_stale'): admit(storage,rid)
    assert storage.get_run(rid)==before


def test_draft_and_authority_change_refuse_before_input(active):
    storage,rid=active; target=steering.target(storage.get_run(rid))
    storage.update_run(rid,lambda r:r.update(draft_revision=r.get('draft_revision',0)+1),'external_canvas_change')
    with pytest.raises(ConversationError,match='target_stale'): admit(storage,rid,target=target)
    assert not binding.held(storage.get_run(rid))


def test_unrelated_checkpoint_change_does_not_retarget_activation(active):
    storage,rid=active; target=steering.target(storage.get_run(rid))
    storage.update_run(rid,lambda r:r['steering'].update(checkpoint_revision=r['steering']['checkpoint_revision']+1),'sibling_observation')
    admit(storage,rid,target=target)
    assert binding.held(storage.get_run(rid),'activation-a')
    assert not binding.held(storage.get_run(rid),'independent-sibling')


def test_conversation_response_cannot_publish_ordinary_result(active):
    storage,rid=active
    admit(storage,rid); assert result(storage,rid)=='hold'
    def issue(r):
        b=r['steering']['conversation_delivery']['activation-a']
        state,item=delivery.claim_next(b['state'],claim_id=uid(),destination_task_id='task-b',destination_turn_id='turn-b')
        claim=item['claim']
        b['state']=delivery.record_receipt(state,receipt=dict(claim=claim,task_id='task-b',turn_id='turn-b',generation=1,kind='input_consumed'))
    storage.update_run(rid,issue,'trusted_native_response')
    choice=[]
    def finish(r):
        choice.append(binding.guard_result(r,activation_id='activation-a',task_id='task-b',generation=1,turn_id='turn-b',result={'summary':'{"save":"must remain prose"}'}))
    storage.update_run(rid,finish,'conversation_result')
    assert choice==['conversation']
    run=storage.get_run(rid)
    assert 'generated_definition' not in run
    assert run.get('draft_revision',0)==0


async def test_actual_bridge_claims_resumes_and_observes_before_consumption(active,monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from harflow_mcp import conversation_runtime
    storage,rid=active; admit(storage,rid)
    def task(task_id):
        done=asyncio.Event(); done.set()
        return SimpleNamespace(task_id=task_id,session_id='native-a',done=done,exit_code=0,output_drained=True,
            latest_record_landed=True,persistence_error=None,cancel_requested=False,
            acc=SimpleNamespace(session_id='native-a',normalize_errors=[],result_count=1,terminal=None,turn_open=False,background_open=False))
    old=task('task-a'); calls=[]
    async def resume(source,text,**kwargs):
        # Inspect the real persisted run at the exact effect boundary.
        state=storage.get_run(rid)['steering']['conversation_delivery']['activation-a']['state']
        assert state['messages'][0]['status']=='claimed'
        assert state['head_task_id']==kwargs['task_id']
        assert not state['boundary']
        calls.append((source.task_id,text,kwargs))
        return task(kwargs['task_id'])
    registry=SimpleNamespace(log_dir=storage.root/'tasks',_log_dir=storage.root/'tasks',resume=resume)
    supervisor=workflows.WorkflowSupervisor(registry,storage); supervisor.run_id=rid
    monkeypatch.setattr(conversation_runtime,'_qualified',lambda source:None)
    from harflow_mcp import store as task_store
    monkeypatch.setattr(task_store,'read',lambda *args:SimpleNamespace())
    async def observe(child,*args):
        state=storage.get_run(rid)['steering']['conversation_delivery']['activation-a']['state']
        assert state['messages'][0]['status']=='claimed' # running/spawn was not consumption
        return {'task_id':child.task_id,'status':'completed','summary':'Conversational response','session_id':'native-a','exit_code':0}
    monkeypatch.setattr(supervisor,'_observe_attempt',observe)
    snapshot={'task_id':'task-a','status':'completed','summary':'Original result','session_id':'native-a','exit_code':0}
    handled=await binding.drain_observed(supervisor,old,{},'builder',{'id':'activation-a'},{},snapshot)
    assert handled and len(calls)==1
    run=storage.get_run(rid)
    b=run['steering']['conversation_delivery']['activation-a']
    assert b['state']['messages'][0]['status']=='consumed'
    assert [c['result']['summary'] for c in b['captures']]==['Original result','Conversational response']
    assert 'generated_definition' not in run
    assert run['activations'][0]['status']=='running'
    assert b['state']['accounting']['settled_responses']==2


async def test_live_workflow_owner_uses_only_run_ledger_and_captures_each_native_turn(active):
    from types import SimpleNamespace
    from harflow_mcp.backends.conversation import ConversationIdentity,ConversationObservation
    storage,rid=active
    storage.update_run(rid,lambda r:r['steering'].pop('conversation_delivery'),'replace_fixture_enrollment')
    class Port:
        session_id='native-a'; transport='test-native'; can_issue=False
        identity=ConversationIdentity('native-a','native-turn-one',1,'task-a')
        def normalize_observation(self,observation,acc):
            if observation.kind=='response_terminal': acc.summary=observation.payload['final']
            return []
    port=Port()
    task=SimpleNamespace(task_id='task-a',prompt='original request')
    registry=SimpleNamespace(log_dir=storage.root/'tasks',state_layout=storage.state_layout)
    owner=binding.for_task(registry,task,port,profile_fingerprint='a'*64)
    run=storage.get_run(rid); target=steering.target(run)
    def queue(r):
        binding.admit(storage,r,principal=None,monitor=True,target=target,request_id=uid(),
            text='Second native response',mode='queue',expected_generation=1,expected_turn_id='task-a',expected_epoch=0)
    storage.update_run(rid,queue,'native_queue')
    port.can_issue=True
    await owner.observe(ConversationObservation('response_terminal',port.identity,{'status':'completed','final':'Original native final'}))
    candidate=await owner.next_input(port.identity)
    assert candidate.text=='Second native response'
    state=owner.ledger.read()
    assert state['head_task_id']=='task-a' and state['generation']==2
    assert state['messages'][0]['status']=='claimed'
    port.identity=ConversationIdentity('native-a','native-turn-two',2,candidate.logical_turn)
    await owner.observe(ConversationObservation('input_issued',port.identity,{}))
    await owner.observe(ConversationObservation('response_terminal',port.identity,{'status':'completed','final':'Second native final'}))
    run=storage.get_run(rid)
    b=run['steering']['conversation_delivery']['activation-a']
    assert b['state']['messages'][0]['status']=='consumed'
    assert [entry['result']['summary'] for entry in b['captures']]==['','Second native final']
    import hashlib
    assert b['captures'][0]['result']['summary_omitted'] is True
    assert b['captures'][0]['result']['summary_sha256']==hashlib.sha256(b'Original native final').hexdigest()
    assert not (storage.root/'conversations').exists()
    assert 'generated_definition' not in run


def test_initial_native_fault_pins_empty_state_and_refuses_new_intent():
    state=delivery.new_state(uid(),'task-a','native-a',1,'turn-a')
    state=delivery.mark_runtime_fault(state,code='native_delivery_unknown',generation=1,turn_id='turn-a')
    assert delivery.protects_retention(state)
    with pytest.raises(ConversationError,match='conversation_unknown'):
        delivery.admit(state,request_id=uid(),text='must not issue',delivery='queue',target_task_id='task-a',expected_epoch=0)
    assert delivery.claim_next(delivery.observe_boundary(state,generation=1,turn_id='turn-a'),claim_id=uid())[1] is None
    with pytest.raises(ConversationError,match='conversation_unknown'): delivery.close(state)


async def test_native_protocol_driver_now_fifo_all_in_workflow_cas(active):
    import asyncio,json
    from types import SimpleNamespace
    from harflow_mcp.backends.conversation_native import ACPConversationPort
    from harflow_mcp.conversation_transport import ConversationDriver,ConversationInput
    storage,rid=active
    storage.update_run(rid,lambda r:r['steering'].pop('conversation_delivery'),'replace_fixture_enrollment')
    port=ACPConversationPort(dict(backend='cursor',version='2026.10.22-27dcf5f',cwd='/repo',mode='agent',caller_approvals=False))
    registry=SimpleNamespace(log_dir=storage.root/'tasks',state_layout=storage.state_layout)
    task=SimpleNamespace(task_id='task-a',prompt='original request')
    owner=binding.for_task(registry,task,port,profile_fingerprint='a'*64)
    class Writer:
        def __init__(self): self.frames=asyncio.Queue()
        def write(self,data): self.frames.put_nowait(json.loads(data))
        async def drain(self): pass
        def close(self): pass
    writer=Writer()
    driver=ConversationDriver(port,writer,owner,initial_input=ConversationInput('task-a',task.prompt),idle_timeout=.05,operation_timeout=2,control_interval=.01)
    job=driver.start()
    try:
        init=await asyncio.wait_for(writer.frames.get(),1)
        driver.observe(dict(id=init['id'],result=dict(protocolVersion=1)))
        new=await asyncio.wait_for(writer.frames.get(),1)
        driver.observe(dict(id=new['id'],result=dict(sessionId='native-a',modes=dict(currentModeId='agent',availableModes=[dict(id='agent')]))))
        original=await asyncio.wait_for(writer.frames.get(),1)
        target=steering.target(storage.get_run(rid))
        def queue(r):
            for text,mode in [('older queue','queue'),('selected now','now')]:
                binding.admit(storage,r,principal=None,monitor=True,target=target,request_id=uid(),text=text,mode=mode,expected_generation=1,expected_turn_id='task-a',expected_epoch=0)
        storage.update_run(rid,queue,'native_queue_and_now')
        cancel=await asyncio.wait_for(writer.frames.get(),1)
        assert cancel['method']=='session/cancel'
        state=owner.ledger.read()
        now=next(m for m in state['messages'] if m['delivery']=='now')
        assert now['interrupt_intent']['session_id']=='native-a'
        assert writer.frames.empty()
        driver.observe(dict(id=original['id'],result=dict(stopReason='cancelled')))
        selected=await asyncio.wait_for(writer.frames.get(),1)
        assert selected['params']['prompt'][0]['text']=='selected now'
        assert next(m for m in owner.ledger.read()['messages'] if m['delivery']=='now')['status']=='claimed'
        driver.observe(dict(method='session/update',params=dict(sessionId='native-a',update=dict(sessionUpdate='agent_message_chunk',content=dict(type='text',text='Selected final prose')))))
        driver.observe(dict(id=selected['id'],result=dict(stopReason='end_turn')))
        queued=await asyncio.wait_for(writer.frames.get(),1)
        assert queued['params']['prompt'][0]['text']=='older queue'
        driver.observe(dict(id=queued['id'],result=dict(stopReason='end_turn')))
        await asyncio.wait_for(job,2)
        run=storage.get_run(rid)
        assert [m['status'] for m in owner.ledger.read()['messages']]==['consumed','consumed']
        assert any(c['result']['summary']=='Selected final prose' for c in run['steering']['conversation_delivery']['activation-a']['captures'])
        assert 'generated_definition' not in run and not (storage.root/'conversations').exists()
    finally:
        await driver.close()


async def test_explicit_builder_commit_issues_existing_capped_episode_without_accepting_old_source(active,monkeypatch):
    from types import SimpleNamespace
    from harflow_mcp import conversation_runtime
    storage,rid=active; admit(storage,rid)
    def settle(r):
        b=r['steering']['conversation_delivery']['activation-a']
        state=delivery.observe_boundary(b['state'],generation=0,turn_id='turn-a')
        state,item=delivery.claim_next(state,claim_id=uid(),destination_task_id='task-a')
        claim=item['claim']
        state=delivery.record_receipt(state,receipt=dict(claim=claim,task_id='task-a',turn_id=claim['destination_turn_id'],generation=1,kind='input_consumed'))
        state=delivery.observe_boundary(state,generation=1,turn_id=state['turn_id'])
        b['state']=delivery.close(state)
        r['activations'][0]['tasks'][0]['status']='completed'; r['status']='paused'
    storage.update_run(rid,settle,'physical_native_settlement')
    run=storage.get_run(rid); state=run['steering']['conversation_delivery']['activation-a']['state']
    source=SimpleNamespace(task_id='task-a',status='completed',exit_code=0,output_drained=True,
        workflow_builder=True,freedom='read_only',backend='claude',repo_path=run['repo_path'],model=None,
        reasoning_effort=None,network=run.get('network'),session_id='native-a')
    source.conversation=_mark_native_commit_profile(storage,rid,'activation-a')
    monkeypatch.setattr(conversation_runtime,'_record',lambda *args:source)
    monkeypatch.setattr(steering,'_transport',lambda *args:None)
    launches=[]; monkeypatch.setattr(workflows,'_launch',lambda *args:launches.append(args))
    req=dict(schema_version=1,request_id=uid(),action='open',expected_interaction_epoch=run['interaction_epoch'],
        expected_control_revision=run['steering']['control_revision'],episode_id=None,expected_steering_revision=None,
        target=steering.target(run),allow_fresh_context=False,text='Explicitly propose the final draft in the required frame.')
    expected={key:state[key] for key in ('conversation_id','head_task_id','generation','turn_id','ledger_epoch')}
    receipt=binding.commit_builder_conversation(storage,SimpleNamespace(),rid,'activation-a',expected=expected,steering_request=req,monitor=True)
    final=storage.get_run(rid); b=final['steering']['conversation_delivery']['activation-a']
    assert b['accepted'] is False
    assert b['steering_episode_id']==receipt['episode_id']
    issuance=final['steering']['turns'][0]
    assert issuance['harness_turn_cap']==8 and issuance['allow_fresh_context'] is False
    assert issuance['previous_session']['task_id']=='task-a'
    assert 'generated_definition' not in final and len(launches)==1
    assert steering.held(final)
    before=copy.deepcopy(final)
    source.exit_code=None
    with pytest.raises(ConversationError,match='source_not_physically_settled'):
        binding.commit_builder_conversation(storage,SimpleNamespace(),rid,'activation-a',expected=expected,steering_request=dict(req,request_id=uid()),monitor=True)
    assert storage.get_run(rid)==before
    # Housekeeping may rotate the source conversation epoch after issuance.
    # Retained exact commit replay remains stable and does not requalify a new
    # source effect; changing its original expected epoch conflicts.
    storage.update_run(rid,lambda r:r['steering']['conversation_delivery']['activation-a']['state'].update(ledger_epoch=expected['ledger_epoch']+1),'source_history_epoch_advanced')
    replay=binding.commit_builder_conversation(storage,SimpleNamespace(),rid,'activation-a',expected=expected,steering_request=req,monitor=True)
    assert replay['turn_id']==receipt['turn_id'] and replay['task_id']==receipt['task_id']
    with pytest.raises(ConversationError,match='request_conflict'):
        binding.commit_builder_conversation(storage,SimpleNamespace(),rid,'activation-a',expected=dict(expected,ledger_epoch=expected['ledger_epoch']+1),steering_request=req,monitor=True)

    # The explicitly issued capped response takes the unchanged physical-exit
    # classifier path. Its final frame alone can propose the unsaved draft.
    import asyncio,json
    from harflow_mcp import steering_runner,task_supervision
    source.exit_code=0
    monkeypatch.setattr(task_supervision,'read_task_record',lambda *args:source)
    resumes=[]
    class Registry:
        log_dir=storage.root/'tasks'
        _log_dir=log_dir
        async def resume_record(self,record,prompt,**kwargs):
            resumes.append(kwargs)
            issued=storage.get_run(rid)['steering']['turns'][0]
            witness=issued['commit_target']
            definition={'name':'example','orchestrator':{'backend':'claude'},
                'nodes':[{'id':'start','type':'start'},{'id':'end','type':'end'}],
                'connections':[{'id':'done','source':'start','target':'end'}]}
            wrapper=dict(episode_id=issued['episode_id'],turn_id=issued['turn_id'],
                commit_target_ref=issued['commit_target_ref'],steering_revision=issued['steering_revision'],
                interaction_epoch=witness['interaction_epoch'],checkpoint_revision=witness['checkpoint_revision'],
                goal_revision=witness['goal_revision'],role='builder',command={'definition':definition,'expected_draft_revision':0})
            final='HARFLOW_COMMIT_V1\n'+json.dumps(wrapper)+'\nEND_HARFLOW_COMMIT_V1'
            done=asyncio.Event();done.set()
            return SimpleNamespace(task_id=kwargs['task_id'],done=done,snapshot=lambda:dict(task_id=kwargs['task_id'],
                status='completed',exit_code=0,output_drained=True,summary=final,session_id='native-a'))
        async def start(self,*args,**kwargs):
            raise AssertionError('Fresh is not authorized')
    supervisor=workflows.WorkflowSupervisor(Registry(),storage);supervisor.run_id=rid
    await steering_runner._run_builder_turn_owned(supervisor,rid)
    committed=storage.get_run(rid)
    assert resumes[0]['max_turns']==8 and resumes[0]['require_unchanged_session'] is True
    assert resumes[0]['conversation_profile']=='native'
    assert committed['steering']['episodes'][0]['state']=='committed'
    assert committed['steering']['conversation_delivery']['activation-a']['accepted'] is False
    assert committed['generated_definition']['name']=='example'
    assert storage.list()==[] # proposal is unsaved until explicit Save


# Reuse the independently maintained canonical assignment/checkpoint specimens.
from test_worker_steering_faults import worker_origin
from test_orchestrator_steering_faults import origin as orchestrator_origin


def _close_delivery_for_commit(storage,rid,aid,task_id,session_id,target):
    def mutate(run):
        state=delivery.new_state(uid(),task_id,session_id,0,task_id)
        binding.enroll(run,target=target,state=state)
        b=run['steering']['conversation_delivery'][aid]
        state,_=delivery.admit(state,request_id=uid(),text='Discuss first',delivery='queue',target_task_id=task_id,expected_epoch=0)
        state=delivery.observe_boundary(state,generation=0,turn_id=task_id)
        state,item=delivery.claim_next(state,claim_id=uid(),destination_task_id=task_id)
        claim=item['claim']
        state=delivery.record_receipt(state,receipt=dict(claim=claim,task_id=task_id,turn_id=claim['destination_turn_id'],generation=1,kind='input_consumed'))
        state=delivery.observe_boundary(state,generation=1,turn_id=state['turn_id'])
        b['state']=delivery.close(state)
    storage.update_run(rid,mutate,'closed_conversation_specimen')
    return storage.get_run(rid)['steering']['conversation_delivery'][aid]['state']


async def test_worker_explicit_commit_same_session_frame_is_sole_graph_authority(worker_origin,monkeypatch):
    import asyncio,json
    from types import SimpleNamespace
    from harflow_mcp import worker_steering,worker_steering_runner
    from harflow_mcp import store as task_store
    storage,rid=worker_origin
    run=storage.get_run(rid)
    target=worker_steering.current_target(storage,run,{'workflow_run_id':rid,'activation_id':'original-execution','token_id':'worker-token'})
    state=_close_delivery_for_commit(storage,rid,'original-execution','original-provider','original-session',target)
    descriptor=_mark_native_commit_profile(storage,rid,'original-execution')
    from dataclasses import replace
    source_record=replace(task_store.read(storage.root/'tasks','original-provider'),conversation=descriptor)
    task_store.write(storage.root/'tasks',source_record)
    # A second conversational worker remains independently held; it must not
    # prevent this actor's explicitly issued commit or lose its own history.
    def sibling(r):
        token=copy.deepcopy(r['pending'][0]);token.update(id='sibling-token',execution_activation_id='sibling-execution')
        activation=copy.deepcopy(r['activations'][0]);activation.update(id='sibling-execution',token=token)
        activation['tasks'][0]['task_id']='sibling-provider'
        r['pending'].append(token);r['activations'].append(activation)
    storage.update_run(rid,sibling,'independent_sibling_specimen')
    other_target=worker_steering.current_target(storage,storage.get_run(rid),{'workflow_run_id':rid,'activation_id':'sibling-execution','token_id':'sibling-token'})
    other_state=_close_delivery_for_commit(storage,rid,'sibling-execution','sibling-provider','sibling-session',other_target)
    run=storage.get_run(rid)
    target=worker_steering.current_target(storage,run,target)
    request=dict(schema_version=2,ledger_epoch=run['steering']['ledger_epoch'],request_id=uid(),action='open',
        expected_interaction_epoch=run['interaction_epoch'],expected_control_revision=run['steering']['control_revision'],
        episode_id=None,expected_steering_revision=None,target=target,allow_fresh_context=False,text='Explicitly submit the immutable worker result.')
    expected={key:state[key] for key in ('conversation_id','head_task_id','generation','turn_id','ledger_epoch')}
    class Registry:
        log_dir=storage.root/'tasks';_log_dir=log_dir
        def __init__(self):self.resumes=[]
        async def resume_record(self,source,prompt,**options):
            self.resumes.append(options)
            turn=storage.get_run(rid)['steering']['turns'][-1];witness=turn['commit_target']
            wrapper={key:turn[key] for key in ('episode_id','turn_id','commit_target_ref','steering_revision')}
            wrapper.update({key:witness[key] for key in ('interaction_epoch','checkpoint_revision','goal_revision')})
            wrapper.update(role='worker',command={'status':'succeeded','result':{'summary':'Implementation done.','completed_task_ids':['task-one']},'evidence':[]})
            final='HARFLOW_COMMIT_V1\n'+json.dumps(wrapper)+'\nEND_HARFLOW_COMMIT_V1'
            done=asyncio.Event();done.set()
            return SimpleNamespace(task_id=options['task_id'],done=done,snapshot=lambda:dict(task_id=options['task_id'],status='completed',exit_code=0,
                output_drained=True,summary=final,session_id='original-session'))
        async def start(self,*args,**options):raise AssertionError('Fresh is not authorized')
    registry=Registry()
    receipt=binding.commit_conversation(storage,registry,rid,'original-execution',expected=expected,steering_request=request)
    issued=storage.get_run(rid)
    assert issued['pending'][0].get('execution_complete') is not True
    from harflow_mcp.orchestrator_steering import scheduling_held
    assert not scheduling_held(storage,issued,{'id':'unrelated-sibling-token'})
    supervisor=workflows.WorkflowSupervisor(registry,storage);supervisor.run_id=rid
    await worker_steering_runner.run_worker_turn(supervisor,rid)
    final=storage.get_run(rid)
    assert len(registry.resumes)==1, {k:final['steering']['turns'][-1].get(k) for k in ('state','outcome','dispatch_error')}
    assert registry.resumes[0]['require_unchanged_session'] is True
    assert registry.resumes[0]['max_turns']==8
    assert registry.resumes[0]['conversation_profile']=='native'
    assert final['steering']['episodes'][-1]['state']=='committed'
    assert final['pending'][0]['execution_complete'] is True
    assert final['activations'][0]['node_result']['status']=='succeeded'
    assert final['steering']['conversation_delivery']['original-execution']['accepted'] is False
    assert final['steering']['conversation_delivery']['sibling-execution']['state']==other_state
    assert final['pending'][1].get('execution_complete') is not True
    assert binding._closed_history(final,'original-execution',final['steering']['conversation_delivery']['original-execution'])
    # Positively committed old conversation data can retire under metadata
    # pressure; graph/effect receipts and managed source ownership stay intact.
    def retire(r):
        from harflow_mcp.conversation_documents import encoded,MAX_DOCUMENT_BYTES
        binding.compact_conversations(r,reserve_metadata_bytes=MAX_DOCUMENT_BYTES-len(encoded(r))+1)
    storage.update_run(rid,retire,'closed_steered_binding_retired')
    retired=storage.get_run(rid)
    assert 'original-execution' not in retired['steering']['conversation_delivery']
    assert retired['steering']['conversation_retention']['cumulative']['retired_issuances']==1
    assert retired['steering']['conversation_retention']['cumulative']['observed_cost_usd'] is None
    assert retired['steering']['conversation_delivery']['sibling-execution']['state']==other_state
    assert retired['pending'][0]['execution_complete'] is True
    assert binding.guard_outcome(retired,'original-execution',{'task_id':'original-provider','summary':'Old ordinary result'})=='stale'
    replay=binding.commit_conversation(storage,registry,rid,'original-execution',expected=expected,steering_request=request)
    assert replay['turn_id']==receipt['turn_id'] and len(registry.resumes)==1


async def test_orchestrator_explicit_commit_same_session_frame_advances_only_exact_checkpoint(orchestrator_origin):
    import asyncio,json
    from types import SimpleNamespace
    from harflow_mcp import orchestrator_steering
    from harflow_mcp import store as task_store
    storage,rid=orchestrator_origin
    run=storage.get_run(rid);candidate=run['definition']['orchestrator']
    def origin(r):
        token=r['pending'][0];token['decision_activation_id']='original-orchestrator'
        r['activations'].append(dict(id='original-orchestrator',node_id='start',role='orchestrator',status='running',created_at=1,
            decision_id='checkpoint-decision',decision_goal_revision=r.get('goal_revision'),token=copy.deepcopy(token),
            tasks=[dict(task_id='original-control',status='completed',candidate=candidate)]))
    storage.update_run(rid,origin,'original_control_settled')
    task_store.write(storage.root/'tasks',task_store.TaskRecord(task_id='original-control',backend='claude',session_id='original-session',
        repo_path=run['repo_path'],started_at='2026-11-05T00:00:00Z',freedom='read_only',network=run.get('network'),
        status='completed',exit_code=0,output_drained=True))
    run=storage.get_run(rid)
    target=orchestrator_steering.current_target(storage,run,{'workflow_run_id':rid,'token_id':'checkpoint-token'})
    state=_close_delivery_for_commit(storage,rid,'original-orchestrator','original-control','original-session',target)
    descriptor=_mark_native_commit_profile(storage,rid,'original-orchestrator')
    from dataclasses import replace
    source_record=replace(task_store.read(storage.root/'tasks','original-control'),conversation=descriptor)
    task_store.write(storage.root/'tasks',source_record)
    run=storage.get_run(rid);target=orchestrator_steering.current_target(storage,run,target)
    req=dict(schema_version=1,request_id=uid(),action='open',expected_interaction_epoch=run['interaction_epoch'],
        expected_control_revision=run['steering']['control_revision'],episode_id=None,expected_steering_revision=None,
        target=target,allow_fresh_context=False,text='Explicitly submit one normal checkpoint decision.')
    expected={key:state[key] for key in ('conversation_id','head_task_id','generation','turn_id','ledger_epoch')}
    class Registry:
        log_dir=storage.root/'tasks';_log_dir=log_dir
        def __init__(self):self.resumes=[]
        async def resume_record(self,source,prompt,**options):
            self.resumes.append(options)
            turn=storage.get_run(rid)['steering']['turns'][-1];witness=turn['commit_target']
            wrapper={key:turn[key] for key in ('episode_id','turn_id','commit_target_ref','steering_revision')}
            wrapper.update({key:witness[key] for key in ('interaction_epoch','checkpoint_revision','goal_revision')})
            command={'decision_id':'checkpoint-decision','action':'continue','reason':'Use configured work only.',
                'next':[{'continuation_id':'begin','prompt':'Configured work assignment.','session_mode':'fresh'}]}
            if turn.get('context_delivery'):
                command['context_ack']={key:turn['context_delivery'][key] for key in ('version','scope','session_owner','revision','digest','base_revision')}
            wrapper.update(role='orchestrator',command=command)
            final='HARFLOW_COMMIT_V1\n'+json.dumps(wrapper)+'\nEND_HARFLOW_COMMIT_V1'
            done=asyncio.Event();done.set()
            return SimpleNamespace(task_id=options['task_id'],done=done,snapshot=lambda:dict(task_id=options['task_id'],
                status='completed',exit_code=0,output_drained=True,summary=final,session_id='original-session'))
        async def start(self,*args,**kwargs):raise AssertionError('Fresh is not authorized')
    registry=Registry()
    binding.commit_conversation(storage,registry,rid,'original-orchestrator',expected=expected,steering_request=req)
    before=storage.get_run(rid)
    assert not before['pending'][0].get('selected_connections')
    supervisor=workflows.WorkflowSupervisor(registry,storage);supervisor.run_id=rid
    await orchestrator_steering.run_turn(supervisor,rid)
    final=storage.get_run(rid)
    assert len(registry.resumes)==1 and registry.resumes[0]['require_unchanged_session'] is True
    assert registry.resumes[0]['max_turns']==8
    assert registry.resumes[0]['conversation_profile']=='native'
    assert final['steering']['episodes'][-1]['state']=='committed'
    assert final['pending'][0]['selected_connections']==['begin']
    assert final['steering']['conversation_delivery']['original-orchestrator']['accepted'] is False


def test_workflow_control_cancel_replay_and_closed_history(active,monkeypatch):
    storage,rid=active; item=admit(storage,rid)
    monkeypatch.setattr(steering,'_transport',lambda *args:None)
    state=storage.get_run(rid)['steering']['conversation_delivery']['activation-a']['state']
    command=dict(contract='conversation_delivery_v1',request_id=uid(),operation='cancel',
        target=dict(kind='workflow_activation',workflow_run_id=rid,activation_id='activation-a',task_id='task-a'),
        expected={key:state[key] for key in ('conversation_id','head_task_id','generation','turn_id','ledger_epoch')},message_request_id=item['request_id'])
    first=binding.control_request(storage,None,command,monitor=True)
    def close(r):
        b=r['steering']['conversation_delivery']['activation-a'];state=b['state']
        state=delivery.observe_boundary(state,generation=0,turn_id='turn-a');b['state']=delivery.close(state)
    storage.update_run(rid,close,'closed_after_cancel')
    assert binding.control_request(storage,None,command,monitor=True)==first
    state=storage.get_run(rid)['steering']['conversation_delivery']['activation-a']['state']
    assert state['messages'][0]['status']=='cancelled' and not binding.held(storage.get_run(rid))
    before=storage.get_run(rid)
    with pytest.raises(steering.SteeringError): binding.control_request(storage,None,dict(command,request_id=uid()),monitor=False)
    assert storage.get_run(rid)==before


async def test_workflow_progress_waits_for_tools_without_claiming_boundary(active):
    from types import SimpleNamespace
    from harflow_mcp.backends.conversation import ConversationIdentity,ConversationObservation
    storage,rid=active
    storage.update_run(rid,lambda r:r['steering'].pop('conversation_delivery'),'replace_fixture_enrollment')
    class Port:
        session_id='native-a';transport='test-native';can_issue=False
        identity=ConversationIdentity('native-a','native-turn',1,'task-a')
        terminal='cancelled';pending_tools={'issued-tool'};pending_approvals={};unsettled_effects={'issued-tool'}
        interrupt_requested=True;unknown=None
        def normalize_observation(self,observation,acc): return []
    port=Port();task=SimpleNamespace(task_id='task-a',prompt='original')
    registry=SimpleNamespace(log_dir=storage.root/'tasks',state_layout=storage.state_layout)
    owner=binding.for_task(registry,task,port,profile_fingerprint='a'*64)
    await owner.observe(ConversationObservation('response_terminal',port.identity,{'status':'cancelled'}))
    state=owner.ledger.read()
    assert state['native_progress']['phase']=='waiting_tools'
    assert state['native_progress']['native_terminal']=='cancelled'
    assert state['native_progress']['pending_tools']==1 and state['native_progress']['unsettled_effects']==1
    assert state['boundary'] is False
    port.pending_tools.clear()
    await owner.observe(ConversationObservation('tool_terminal',port.identity,{}))
    assert owner.ledger.read()['native_progress']['unsettled_effects']==1
    assert owner.ledger.read()['boundary'] is False
    owner.ledger.update(lambda state:(delivery.mark_runtime_fault(state,code='response_capture_full',generation=1,turn_id='task-a'),None))
    await owner.observe(ConversationObservation('unknown',port.identity,{'reason':'generic cleanup'}))
    assert owner.ledger.read()['runtime_fault']['code']=='response_capture_full'


def test_orchestrator_sibling_result_does_not_stale_same_active_source(orchestrator_origin):
    from harflow_mcp import orchestrator_steering
    storage,rid=orchestrator_origin
    def setup(r):
        token=r['pending'][0];token['decision_activation_id']='active-orchestrator'
        r['activations'].append(dict(id='active-orchestrator',node_id='start',role='orchestrator',status='running',created_at=1,
            token=copy.deepcopy(token),tasks=[dict(task_id='control-source',status='running')]))
        target=orchestrator_steering.current_target(storage,r,{'workflow_run_id':rid,'token_id':token['id']})
        binding.enroll(r,target=target,state=delivery.new_state(uid(),'control-source','native-context',1,'logical-source'))
    storage.update_run(rid,setup,'active_orchestrator_specimen')
    issued=storage.get_run(rid)['steering']['conversation_delivery']['active-orchestrator']['target']
    storage.update_run(rid,lambda r:r['tasks'].append(dict(id='sibling',title='Independent sibling',description='',status='completed')),'sibling_result_observed')
    fresh=orchestrator_steering.current_target(storage,storage.get_run(rid),issued)
    assert fresh['assignment_digest']!=issued['assignment_digest']
    def admit(r):
        binding.admit(storage,r,principal=None,monitor=True,target=issued,request_id=uid(),text='Same actor after sibling completion.',mode='queue',
            expected_generation=1,expected_turn_id='logical-source',expected_epoch=0)
    storage.update_run(rid,admit,'same_source_admission')
    state=storage.get_run(rid)['steering']['conversation_delivery']['active-orchestrator']['state']
    assert state['messages'][0]['target_task_id']=='control-source'


def test_terminal_workflow_keeps_unknown_empty_native_source_pinned(active):
    storage,rid=active
    def cancel_with_unknown(r):
        b=r['steering']['conversation_delivery']['activation-a']
        b['state']=delivery.mark_runtime_fault(b['state'],code='native_unknown',generation=0,turn_id='turn-a')
        r['status']='cancelled'
        r['activations'][0]['status']='cancelled'
        r['activations'][0]['tasks'][0]['status']='completed'
    storage.update_run(rid,cancel_with_unknown,'unknown_source_during_cancellation')
    assert 'task-a' in storage.pinned_tasks()


def _mark_native_commit_profile(storage,rid,aid,profile='native'):
    descriptor=[]
    def mutate(run):
        b=run['steering']['conversation_delivery'][aid]
        b['native_owner']={'task_id':b['state']['head_task_id'],'transport':'test-native','profile_fingerprint':'a'*64,'profile_name':profile}
        descriptor.append(dict(contract='conversation_delivery_v1',conversation_id=b['state']['conversation_id'],transport='test-native',profile_fingerprint='a'*64))
    storage.update_run(rid,mutate,'fake_native_source_profile')
    return descriptor[0]


def test_candidate_conversation_profile_is_closed_keyed_and_does_not_drop_caps():
    plain=workflows._candidate({'backend':'claude'})
    native=workflows._candidate({'backend':'claude','conversation_profile':'native'})
    capped=workflows._candidate({'backend':'claude','conversation_profile':'native','max_turns':7})
    assert plain['max_turns']==native['max_turns']==100
    assert capped['max_turns']==7
    assert 'conversation_profile' not in workflows._candidate_key(plain)
    assert workflows._candidate_key(plain)!=workflows._candidate_key(native)
    for value in ('other',None,True,{}):
        with pytest.raises(workflows.WorkflowError):workflows._candidate({'backend':'claude','conversation_profile':value})


def test_commit_profile_selects_actual_vibe_high_marker_without_classic_fallback():
    run={'steering':{'conversation_delivery':{'actor':{'steering_episode_id':'issued','native_owner':{'profile_name':'shipped-medium35-high'}}}}}
    assert binding.commit_profile_options(run,{'episode_id':'issued','candidate':{}})=={'conversation_profile':'shipped-medium35-high'}
    run['steering']['conversation_delivery']['actor']['native_owner']['profile_name']=None
    with pytest.raises(ConversationError,match='source_profile_unknown'):
        binding.commit_profile_options(run,{'episode_id':'issued','candidate':{}})


@pytest.mark.parametrize('maximum',[None,7])
async def test_workflow_dispatch_forwards_explicit_profile_without_dropping_cap(tmp_path,monkeypatch,maximum):
    from test_workflows import FakeRegistry
    monkeypatch.setattr(workflows.backends,'is_installed',lambda backend:True)
    candidate={'backend':'claude','conversation_profile':'native'}
    if maximum is not None:candidate['max_turns']=maximum
    candidate=workflows._candidate(candidate,False)
    storage=workflows.WorkflowStore(tmp_path/'dispatch-state')
    run=storage.create_run({'name':'profile-forward','orchestrator':candidate,'nodes':[],'connections':[]},'Create',tmp_path,kind='builder',freedom='read_only')
    rid=run['workflow_run_id'];storage.update_run(rid,lambda r:r.update(status='running'),'fixture_running')
    registry=FakeRegistry(storage.root,['literal response'])
    supervisor=workflows.WorkflowSupervisor(registry,storage);supervisor.run_id=rid
    activation=supervisor._activation('builder','builder')
    outcome=await supervisor._dispatch({'id':'builder','title':'Builder'},'literal request','builder',activation)
    assert outcome['summary']=='literal response'
    assert registry.calls[0][1]['conversation_profile']=='native'
    assert registry.calls[0][1]['max_turns']==(100 if maximum is None else maximum)


def test_native_profile_fresh_steering_refuses_before_issuance(active,monkeypatch):
    storage,rid=active
    monkeypatch.setattr(steering,'_transport',lambda *args:None)
    def configure(r):
        r['steering'].pop('conversation_delivery')
        r['definition']['orchestrator']['conversation_profile']='native'
    storage.update_run(rid,configure,'selected_native_profile')
    run=storage.get_run(rid)
    req=dict(schema_version=1,request_id=uid(),action='open',expected_interaction_epoch=run['interaction_epoch'],
        expected_control_revision=run['steering']['control_revision'],episode_id=None,expected_steering_revision=None,
        target=steering.target(run),allow_fresh_context=True,text='Discuss.')
    from harflow_mcp.steering_admission import admit
    before=copy.deepcopy(run)
    with pytest.raises(steering.SteeringError,match='Fresh steering cannot substitute'):
        storage.update_run(rid,lambda r:admit(storage,r,None,True,req,'digest','binding'),'steering_requested')
    assert storage.get_run(rid)==before


def test_shared_root_window_rolls_beyond_64_responses_without_resetting_accounting(active):
    storage,rid=active
    legacy='L'*(200*1024)
    def initial(r):
        steering._history(r,'retained_legacy_prose',text=legacy)
        b=r['steering']['conversation_delivery']['activation-a'];state=b['state']
        state=delivery.record_response(state,generation=0,turn_id='turn-a')
        b['state']=delivery.observe_boundary(state,generation=0,turn_id='turn-a')
    storage.update_run(rid,initial,'source_boundary')
    first_request=None
    for n in range(80):
        request_id=uid()
        if first_request is None:first_request=request_id
        def response(r):
            b=r['steering']['conversation_delivery']['activation-a'];state=b['state']
            binding.admit(storage,r,principal=None,monitor=True,target=b['target'],request_id=request_id,
                text=str(n)+'M'*(31*1024),mode='queue',expected_generation=state['generation'],expected_turn_id=state['turn_id'],expected_epoch=state['ledger_epoch'])
            state,item=delivery.claim_next(b['state'],claim_id=uid(),destination_task_id='task-a')
            claim=item['claim']
            state=delivery.record_receipt(state,receipt=dict(claim=claim,task_id='task-a',turn_id=claim['destination_turn_id'],generation=claim['destination_generation'],kind='input_consumed'))
            b['state']=delivery.observe_boundary(state,generation=state['generation'],turn_id=state['turn_id'])
            capture=dict(task_id='task-a',generation=state['generation'],turn_id=state['turn_id'],result=binding._capture(
                {'task_id':'task-a','status':'end_turn','session_id':'native-a','summary':str(n)+'R'*(24*1024)},settlement='native_response'))
            binding.append_capture(r,'activation-a',capture)
            assert binding.shared_text_bytes(r)<=1024*1024
        storage.update_run(rid,response,'explicit_response')
    run=storage.get_run(rid);b=run['steering']['conversation_delivery']['activation-a'];state=b['state']
    assert state['cumulative_issuances']==80 and state['accounting']['settled_responses']==81
    assert state['ledger_epoch']>0 and b['omitted_captures']>0 and b['capture_history_floor']>0
    assert not any(m['request_id']==first_request for m in state['messages'])
    assert b['captures'][-1]['generation']==80 and run['steering']['history'][0]['text']==legacy
    with pytest.raises(ConversationError,match='request_expired'):
        delivery.admit(state,request_id=first_request,text='0'+'M'*(31*1024),delivery='queue',target_task_id='task-a',expected_epoch=0)
    # Unknown and queued inputs retain exact caller prose even while old settled
    # history is reclaimed against the same legacy-inclusive window.
    def unknown(r):
        b=r['steering']['conversation_delivery']['activation-a']
        for n in range(3):
            state=b['state'];binding.admit(storage,r,principal=None,monitor=True,target=b['target'],request_id=uid(),
                text=f'protected {n} '+'P'*(30*1024),mode='queue',expected_generation=state['generation'],expected_turn_id=state['turn_id'],expected_epoch=state['ledger_epoch'])
        state,item=delivery.claim_next(b['state'],claim_id=uid(),destination_task_id='task-a')
        b['state']=delivery.mark_unknown(state,claim_id=item['claim']['claim_id'])
        protected={m['request_id']:m['text'] for m in b['state']['messages'] if m['status'] in {'queued','unknown'}}
        binding.compact_conversations(r,reserve_text_bytes=256*1024)
        assert {m['request_id']:m['text'] for m in b['state']['messages'] if m['status'] in {'queued','unknown'}}==protected
    storage.update_run(rid,unknown,'protected_unknown_pressure')


def test_binding_lifetime_rolls_with_closed_namespace_and_managed_checkpoint(worker_origin):
    from harflow_mcp import worker_steering,workflow_delegation
    storage,rid=worker_origin
    original=storage.get_run(rid);template_actor=copy.deepcopy(original['activations'][0]);template_token=copy.deepcopy(original['pending'][0])
    first=None
    for n in range(80):
        aid,tid,token_id=f'life-actor-{n}',f'life-task-{n}',f'life-token-{n}'
        def complete(r):
            token=copy.deepcopy(template_token);token.update(id=token_id,execution_activation_id=aid)
            actor=copy.deepcopy(template_actor);actor.update(id=aid,token=copy.deepcopy(token),status='running',created_at=n+2)
            actor['tasks']=[dict(task_id=tid,status='completed',candidate=template_actor['tasks'][0]['candidate'])]
            r['pending'].append(token);r['activations'].append(actor)
            target=worker_steering.current_target(storage,r,{'workflow_run_id':rid,'activation_id':aid,'token_id':token_id})
            state=delivery.new_state(uid(),tid,f'context-{n}',0,tid)
            binding.enroll(r,target=target,state=state)
            b=r['steering']['conversation_delivery'][aid]
            b['state']=delivery.record_response(b['state'],generation=0,turn_id=tid)
            outcome=dict(task_id=tid,status='completed',exit_code=0,output_drained=True,session_id=f'context-{n}',summary='Original role result')
            value={'status':'succeeded','result':{'summary':'Original role result','completed_task_ids':[]},'evidence':[]}
            workflow_delegation.finish_result(r,next(node for node in r['definition']['nodes'] if node['id']=='work'),token_id,aid,value,outcome)
        storage.update_run(rid,complete,'ordinary_native_actor_completed')
        if first is None:
            row=storage.get_run(rid)['steering']['conversation_delivery'][aid]
            first=(aid,tid,row['state']['conversation_id'])
    run=storage.get_run(rid);retention=run['steering']['conversation_retention']
    assert len(run['steering']['conversation_delivery'])==64
    assert retention['cumulative']['retired_conversations']==16
    assert retention['cumulative']['retired_issuances']==0
    assert retention['cumulative']['retired_settled_responses']==16
    assert retention['cumulative']['observed_cost_usd'] is None and retention['cumulative']['usage_complete'] is False
    assert binding.history_counts(run)[0]<=32
    actor=next(a for a in run['activations'] if a['id']==first[0])
    assert actor['conversation_checkpoint']['conversation_id']==first[2]
    assert actor['tasks'][0]['task_id']==first[1] and actor['node_result']['status']=='succeeded'
    assert binding.guard_outcome(run,first[0],{'task_id':first[1],'summary':'Late old output'})=='stale'
    facade=binding.WorkflowConversationStore(storage,rid,first[0],first[2])
    assert facade.owns()
    with pytest.raises(ConversationError,match='request_expired'):facade.read()


def test_more_than_32_ordinary_actors_are_not_a_conversation_quota(worker_origin):
    from harflow_mcp import worker_steering
    storage,rid=worker_origin
    template=storage.get_run(rid);actor_template=copy.deepcopy(template['activations'][0]);token_template=copy.deepcopy(template['pending'][0])
    def allocate(r):
        for n in range(40):
            aid,tid,token_id=f'parallel-actor-{n}',f'parallel-task-{n}',f'parallel-token-{n}'
            token=copy.deepcopy(token_template);token.update(id=token_id,execution_activation_id=aid)
            actor=copy.deepcopy(actor_template);actor.update(id=aid,token=copy.deepcopy(token),created_at=n+2)
            actor['tasks']=[dict(task_id=tid,status='running',candidate=actor_template['tasks'][0]['candidate'])]
            r['pending'].append(token);r['activations'].append(actor)
            target=worker_steering.current_target(storage,r,{'workflow_run_id':rid,'activation_id':aid,'token_id':token_id})
            binding.enroll(r,target=target,state=delivery.new_state(uid(),tid,f'context-{n}',0,tid))
    storage.update_run(rid,allocate,'parallel_native_actors_reserved')
    run=storage.get_run(rid)
    assert len(run['steering']['conversation_delivery'])==40
    assert binding.pending_capture_reserve(run)==0
    def queue(r):
        for b in list(r['steering']['conversation_delivery'].values())[:32]:
            state=b['state'];binding.admit(storage,r,principal=None,monitor=True,target=b['target'],request_id=uid(),text='x',mode='queue',
                expected_generation=0,expected_turn_id=state['turn_id'],expected_epoch=state['ledger_epoch'])
    storage.update_run(rid,queue,'root_32_messages')
    def overflow(r):
        b=list(r['steering']['conversation_delivery'].values())[32];state=b['state']
        binding.admit(storage,r,principal=None,monitor=True,target=b['target'],request_id=uid(),text='x',mode='queue',expected_generation=0,expected_turn_id=state['turn_id'],expected_epoch=state['ledger_epoch'])
    with pytest.raises(ConversationError,match='queue_full'):storage.update_run(rid,overflow,'root_33rd_message')


def test_root_receipt_window_counts_message_promotion_and_cancellation(active):
    storage,rid=active;first=None
    for n in range(80):
        mid,pid,cid=uid(),uid(),uid()
        if first is None:first=mid
        def controls(r):
            b=r['steering']['conversation_delivery']['activation-a'];state=b['state']
            state,_=delivery.admit(state,request_id=mid,text=f'cancelled {n}',delivery='queue',target_task_id='task-a',expected_epoch=state['ledger_epoch'])
            state,_=delivery.promote_now(state,request_id=pid,message_request_id=mid,target_task_id='task-a',expected_generation=0,expected_turn_id='turn-a',expected_epoch=state['ledger_epoch'])
            state,_=delivery.cancel_control(state,request_id=cid,message_request_id=mid,target_task_id='task-a',expected_generation=0,expected_turn_id='turn-a',expected_epoch=state['ledger_epoch'])
            b['state']=state
        storage.update_run(rid,controls,'cancelled_promoted_message')
    run=storage.get_run(rid);state=run['steering']['conversation_delivery']['activation-a']['state']
    assert binding.history_counts(run)[1]<=64
    assert len(state['messages'])<=21 and state['sequence']==80 and state['cumulative_issuances']==0
    assert state['ledger_epoch']>0 and not any(m['request_id']==first for m in state['messages'])


async def test_builder_own_authorized_preview_refreshes_queue_witness_but_external_edit_does_not(active,monkeypatch):
    from types import SimpleNamespace
    from harflow_mcp import store as task_store
    storage,rid=active
    definition={'name':'example','orchestrator':{'backend':'claude'},'nodes':[{'id':'start','type':'start'},{'id':'end','type':'end'}],
                'connections':[{'id':'done','source':'start','target':'end'}]}
    # Production owner enrollment follows the durable task reservation; the
    # generic fixture installed both in one mutation, so pin its settled witness.
    storage.update_run(rid,lambda r:r['steering']['conversation_delivery']['activation-a'].update(target=steering.target(r)),'qualified_owner_snapshot')
    caller=SimpleNamespace(record=SimpleNamespace(task_id='task-a'))
    monkeypatch.setattr(task_store,'read',lambda *args,**kwargs:SimpleNamespace(status='running'))
    await workflows.apply_workflow_draft(definition,0,caller=caller,root=storage.root)
    run=storage.get_run(rid);b=run['steering']['conversation_delivery']['activation-a']
    assert b['target']['draft_revision']==1 and b['target']==steering.target(run)
    admit(storage,rid) # Queue still works after the original builder's own preview.
    before=storage.get_run(rid)
    with pytest.raises(ConversationError,match='conversation_preview_requires_explicit_commit'):
        await workflows.apply_workflow_draft(definition,1,caller=caller,root=storage.root)
    assert storage.get_run(rid)==before


def test_external_draft_event_cannot_refresh_initial_builder_authority(active):
    storage,rid=active;old=copy.deepcopy(storage.get_run(rid)['steering']['conversation_delivery']['activation-a']['target'])
    storage.update_run(rid,lambda r:r.update(builder_draft={'changed':'external'},draft_revision=1),'external_canvas_edit')
    def own_after_external(r):r.update(builder_draft={'changed':'own after external'},draft_revision=2)
    storage.update_run(rid,own_after_external,'builder_draft_applied',{'task_id':'task-a'})
    run=storage.get_run(rid)
    assert run['steering']['conversation_delivery']['activation-a']['target']==old
    with pytest.raises(ConversationError,match='target_stale'):admit(storage,rid)
