"""Sealed native steering ownership; existing issuance alone authorizes one input."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace
import pytest
from harflow_mcp import conversation_delivery as delivery,conversation_workflows as binding
from harflow_mcp import conversation_commit_owner as sealed,workflow_steering as steering,workflows,store
from harflow_mcp.backends.base import Accumulator
from harflow_mcp.backends.conversation import ConversationIdentity,ConversationObservation
from harflow_mcp.conversation_transport import ConversationInput
from harflow_mcp.conversation_documents import ConversationError
from test_conversation_workflows import active,uid,admit,_mark_native_commit_profile


@pytest.fixture
def issued(active,monkeypatch):
    storage,rid=active;admit(storage,rid)
    def settle(r):
        b=r['steering']['conversation_delivery']['activation-a'];state=b['state']
        state=delivery.observe_boundary(state,generation=0,turn_id='turn-a')
        state,item=delivery.claim_next(state,claim_id=uid(),destination_task_id='task-a')
        claim=item['claim'];state=delivery.record_receipt(state,receipt=dict(claim=claim,task_id='task-a',turn_id=claim['destination_turn_id'],generation=1,kind='input_consumed'))
        b['state']=delivery.close(delivery.observe_boundary(state,generation=1,turn_id=state['turn_id']))
        r['activations'][0]['tasks'][0]['status']='completed';r['status']='paused'
    storage.update_run(rid,settle,'physical_source_settlement')
    descriptor=_mark_native_commit_profile(storage,rid,'activation-a')
    run=storage.get_run(rid)
    source=store.TaskRecord(task_id='task-a',backend='claude',session_id='native-a',repo_path=run['repo_path'],started_at='2026-11-05T00:00:00Z',
        freedom='read_only',network=run.get('network'),status='completed',exit_code=0,output_drained=True,workflow_builder=True,max_turns=100,conversation=descriptor)
    store.write(storage.root/'tasks',source)
    monkeypatch.setattr(steering,'_transport',lambda *args:None);monkeypatch.setattr(workflows,'_launch',lambda *args:None)
    state=run['steering']['conversation_delivery']['activation-a']['state']
    req=dict(schema_version=1,request_id=uid(),action='open',expected_interaction_epoch=run['interaction_epoch'],expected_control_revision=run['steering']['control_revision'],
        episode_id=None,expected_steering_revision=None,target=steering.target(run),allow_fresh_context=False,text='Explicitly produce one framed proposal.')
    registry=SimpleNamespace(log_dir=storage.root/'tasks',_log_dir=storage.root/'tasks',state_layout=storage.state_layout)
    binding.commit_conversation(storage,registry,rid,'activation-a',expected={k:state[k] for k in ('conversation_id','head_task_id','generation','turn_id','ledger_epoch')},steering_request=req,monitor=True)
    turn=storage.get_run(rid)['steering']['turns'][0]
    def reserve(r):
        t=r['steering']['turns'][0];t['state']='spawn_intent'
        r['activations'].append(dict(id=t['turn_id'],node_id='builder',role='builder',status='running',created_at=1,
            steering_turn_id=t['turn_id'],tasks=[dict(task_id=t['task_id'],status='reserved',candidate=t['candidate'])]))
    storage.update_run(rid,reserve,'steering_spawn_intent')
    supervisor=workflows.WorkflowSupervisor(registry,storage);supervisor.run_id=rid
    prompt='Exact engine-generated commit input.'
    binding.bind_commit_input(supervisor,turn,prompt,{'conversation_profile':'native'},8)
    task=SimpleNamespace(task_id=turn['task_id'],parent_task_id='task-a',session_id='native-a',max_turns=8,prompt=prompt,backend='claude',
        repo_path=source.repo_path,model=None,reasoning_effort=None,freedom='read_only',network=source.network,caller_approvals=False,caller_write_approvals=False,
        allowed_commands=[],acc=Accumulator(),conversation=None)
    class Port:
        profile={'profile_name':None,'max_turns':8};session_id='native-a';can_issue=True;identity=None
        def normalize_observation(self,observation,acc):
            if observation.kind=='response_terminal':acc.summary=observation.payload.get('final')
            return []
    return storage,rid,registry,task,Port()


async def test_sealed_owner_selects_exact_issued_input_and_never_opens_queue(issued):
    storage,rid,registry,task,port=issued
    before=storage.get_run(rid)
    owner=sealed.for_task(registry,task,port,profile_fingerprint='b'*64)
    assert isinstance(owner,sealed.SingleResponseOwner) and task.conversation is None
    await owner.before_issue(ConversationInput(task.task_id,task.prompt),None)
    with pytest.raises(ConversationError,match='commit_input_mismatch'):
        await owner.before_issue(ConversationInput(task.task_id,task.prompt),None)
    assert await owner.next_input(None) is None and await owner.poll_control(None) is None
    port.identity=ConversationIdentity('native-a','actual-native-turn',1,task.task_id)
    await owner.observe(ConversationObservation('response_terminal',port.identity,{'final':'A complete bounded frame'}))
    assert task.acc.summary=='A complete bounded frame'
    assert await owner.before_close(port.identity) is None
    assert storage.get_run(rid)==before and not (storage.root/'conversations').exists()


@pytest.mark.parametrize('field,value', [('prompt','changed input'),('parent_task_id','other-source'),('session_id','other-context'),('max_turns',9),('freedom','publish')])
def test_sealed_owner_refuses_wrong_issuance_context_cap_or_grants(issued,field,value):
    storage,rid,registry,task,port=issued
    setattr(task,field,value)
    with pytest.raises(ConversationError):sealed.for_task(registry,task,port,profile_fingerprint='b'*64)
    assert not (storage.root/'conversations').exists()


def test_sealed_owner_rejects_fresh_even_if_task_uuid_matches(issued):
    storage,rid,registry,task,port=issued
    storage.update_run(rid,lambda r:r['steering']['turns'][0].update(allow_fresh_context=True),'corrupt_fresh_commit_variant')
    with pytest.raises(ConversationError,match='commit_issuance_mismatch'):
        sealed.for_task(registry,task,port,profile_fingerprint='b'*64)
