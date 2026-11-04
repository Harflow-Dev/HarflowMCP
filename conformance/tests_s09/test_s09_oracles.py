"""Public guide-derived negative controls; no engine or real process execution."""
import hashlib
import json

import pytest

from conformance.oracles import ConformanceError
from conformance.s09 import oracles as module


def envelope(**fields):
    return dict(product='harflow',format='harflow.ctl',v=1,product_version='0.0.7',**fields)


def test_resource_wait_requires_typed_pending_launch_not_ordinary_snapshot():
    pending=envelope(error={'code':'task_pending_launch','message':'pending','data':{'launch_state':'waiting_for_resources','task_id':'queued'}})
    assert module.resource_pending(pending,'queued')['task_id']=='queued'
    for doc in [envelope(result={'task_id':'queued','status':'queued','launch_state':'waiting_for_resources'}),
                envelope(error={'code':'task_pending_launch','message':'pending','data':{'launch_state':'starting'}}),pending]:
        with pytest.raises(ConformanceError): module.resource_pending(doc,'foreign')


def resource():
    def row(task): return {'reservation_id':'reservation-'+task,'owner':{'kind':'task','task_id':task}}
    return envelope(resources=[{'name':'exclusive','capacity':1,'holders':[row('first')],'queue':[row('second'),row('third')]}])


def test_fifo_needs_public_order_and_external_spawn_events():
    doc=resource()
    module.resource_task_order(doc,'exclusive',['first'],['second','third'])
    events=[{'event':'spawn','role':'fake','pid':pid} for pid in [111,222,333]]
    module.resource_spawn_order(events,[111,222,333])
    for bad in [list(reversed(events)),events[:2],events+[events[-1]],
                [{'event':'fixture-claims-spawn','role':'fake','pid':pid} for pid in [111,222,333]]]:
        with pytest.raises(ConformanceError): module.resource_spawn_order(bad,[111,222,333])
    doc['resources'][0]['queue'].reverse()
    with pytest.raises(ConformanceError): module.resource_task_order(doc,'exclusive',['first'],['second','third'])


@pytest.mark.parametrize('fault',['no-fake','live-fake','self-claim','settling','missing-settling','wrong-status'])
def test_workflow_fake_success_claim_cannot_replace_public_settlement_and_os_exit(fault):
    run={'status':'completed','settling':False}
    observed=[{'role':'fake','pid':111,'exited':True}]
    if fault=='no-fake':observed=[]
    elif fault=='live-fake':observed[0]['exited']=False
    elif fault=='self-claim':observed[0]={'status':'succeeded','exited':True}
    elif fault=='settling':run['settling']=True
    elif fault=='missing-settling':run.pop('settling')
    else:run['status']='running'
    with pytest.raises(ConformanceError):module.workflow_terminal(run,observed)


def page(identifier='one',more=False,cursor=None):
    return envelope(result={'state':'ready','history_complete':True,'ownership_complete':True,
        'retained_history_only':True,'generation':'generation-one','snapshot_id':'snapshot-one',
        'projection_version':1,'accounting_coverage':{'eligible_executions':2,'available_executions':1},
        'cost_coverage':{'eligible_executions':2,'available_executions':1},'retention':{'retained_only':True},
        'items':[{'id':identifier,'kind':'execution'}],'has_more':more,'next_cursor':cursor})


def test_paging_qualifies_full_same_generation_set_and_rejects_partial():
    first,second=page(more=True,cursor='next'),page('two')
    assert [fact['id'] for fact in module.statistics_pages([first,second])]==['one','two']
    with pytest.raises(ConformanceError):module.statistics_pages([first])


@pytest.mark.parametrize('fault',['history','ownership','ready','generation','snapshot','coverage','duplicate','final-cursor','missing-cost-coverage'])
def test_statistics_incomplete_or_cross_generation_totals_never_qualify(fault):
    first,second=page(more=True,cursor='next'),page('two')
    p=second['result']
    if fault=='history':p['history_complete']=False
    elif fault=='ownership':p['ownership_complete']=False
    elif fault=='ready':p['state']='preparing'
    elif fault=='generation':p['generation']='stale-generation'
    elif fault=='snapshot':p['snapshot_id']='other-snapshot'
    elif fault=='coverage':p['cost_coverage']['available_executions']=2
    elif fault=='duplicate':p['items'][0]['id']='one'
    elif fault=='final-cursor':p['next_cursor']='still-pending'
    else:p.pop('cost_coverage')
    with pytest.raises(ConformanceError):module.statistics_pages([first,second])


def test_stale_cursor_error_cannot_become_statistics_page():
    with pytest.raises(ConformanceError):module.statistics_pages([envelope(error={'code':'snapshot_expired','message':'refresh snapshot'})])


def fact():
    return {'kind':'execution','requested_model':None,'cost_usd':None,'cost_provenance':None,
        'cost_additive':False,'observed_model':None,'observed_model_provenance':None}


def test_unknown_cost_is_null_and_requested_identity_does_not_become_observed():
    row=fact()
    assert module.cost_fact(row)==row
    for change in [{'cost_usd':0},{'cost_provenance':'backend_reported'},{'cost_additive':True},
                   {'observed_model':'requested-only'},{'requested_model':'observed-only'}]:
        with pytest.raises(ConformanceError):module.cost_fact({**row,**change})
    row.update(cost_usd=0,cost_provenance='backend_reported',cost_additive=True,
               observed_model='synthetic-observed',observed_model_provenance='harness_initialization_event')
    module.cost_fact(row,reported_cost=0,reported_model='synthetic-observed')
    with pytest.raises(ConformanceError):module.cost_fact(row,reported_cost=.5,reported_model='synthetic-observed')


def managed():
    raw=json.dumps(envelope(error={'code':'agent_caller','message':'managed caller refused'})).encode()
    argv=['/owned/shim','statistics-page','--json']
    capture={'argv':argv,'pid':222,'returncode':1,'pipes_drained':True,'timed_out':False,
        'capture_exceeded':False,'cleanup':'group_quiescent_os_observed',
        'streams':{'stdout':{'captured_bytes':len(raw),'bytes_seen':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}}}
    return [{'role':'cli','pid':222,'ppid':111,'exited':True}],capture,argv,raw


def test_managed_refusal_requires_exact_capture_argv_pid_parent_and_hash():
    rows,capture,argv,raw=managed()
    assert module.correlate_managed(rows,capture,argv,111,raw)['error']['code']=='agent_caller'


@pytest.mark.parametrize('fault',['pid','parent','exit','role','argv','timeout','returncode','hash','bytes','cleanup'])
def test_self_reported_managed_capture_needs_independent_identity(fault):
    rows,capture,argv,raw=managed()
    if fault=='pid':capture['pid']=333
    elif fault=='parent':rows[0]['ppid']=999
    elif fault=='exit':rows[0]['exited']=False
    elif fault=='role':rows[0]['role']='fake'
    elif fault=='argv':capture['argv']=['different']
    elif fault=='timeout':capture['timed_out']=True
    elif fault=='returncode':capture['returncode']=None
    elif fault=='hash':capture['streams']['stdout']['sha256']='0'*64
    elif fault=='bytes':capture['streams']['stdout']['bytes_seen']+=1
    else:capture['cleanup']='unknown'
    with pytest.raises(ConformanceError):module.correlate_managed(rows,capture,argv,111,raw)


def test_repeated_cursor_and_invalid_coverage_cannot_certify_pagination():
    first,second,third=page(more=True,cursor='same'),page('two',True,'same'),page('three')
    with pytest.raises(ConformanceError):module.statistics_pages([first,second,third])
    for count in [-1,True,3]:
        value=page()
        value['result']['cost_coverage']['available_executions']=count
        with pytest.raises(ConformanceError):module.statistics_pages([value])


def control_turn(checkpoint='issued-first',mode='bootstrap'):
    payload={'checkpoint':{'decision_id':checkpoint,'workflow_run_id':'workflow:owned','node_instructions':{'worker':{'mode':'full','value':'exact café 日本語'}}}}
    if mode=='bootstrap':payload['bootstrap']={'context':{'original_request':' exact original Ω\n'}}
    digest=hashlib.sha256(json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')).hexdigest()
    receipt={'checkpoint':checkpoint,'digest':digest,'revision':2 if mode=='delta' else 1,'scope':'workflow:owned','version':2,'session_owner':'issued-owner','base_revision':1 if mode=='delta' else None,'delivery_mode':mode}
    control={**payload,'delivery':{'mode':mode,'acknowledgement':{key:receipt[key] for key in ('version','scope','session_owner','revision','digest','base_revision')}}}
    frames=[('stdout',{'subtype':'init','session_id':'same-session'}),('stdin',{'type':'input','text':json.dumps(control,ensure_ascii=False)})]
    return frames,receipt,control


def test_orchestrator_full_and_delta_turns_bind_distinct_receipts_in_same_session():
    first,r1,_=control_turn()
    second,r2,_=control_turn('issued-second','delta')
    for frames,receipt in [(first,r1),(second,r2)]:
        assert module.workflow_input_matches(frames,'same-session',' exact original Ω\n','orchestrator',receipt)
    assert not module.workflow_input_matches(first,'same-session',' exact original Ω\n','orchestrator',r2)
    assert not module.workflow_input_matches(second,'same-session',' exact original Ω\n','orchestrator',r1)


@pytest.mark.parametrize('fault',['checkpoint','digest','revision','scope','session','original','changed-payload','reference','malformed'])
def test_orchestrator_input_rejects_receipt_payload_or_public_identity_mismatch(fault):
    frames,receipt,control=control_turn()
    session='same-session'
    prompt=' exact original Ω\n'
    if fault=='checkpoint':receipt['checkpoint']='other'
    elif fault=='digest':receipt['digest']='0'*64
    elif fault=='revision':receipt['revision']=2
    elif fault=='scope':receipt['scope']='foreign'
    elif fault=='session':session='foreign-session'
    elif fault=='original':prompt='changed original'
    elif fault=='changed-payload':control['checkpoint']['node_instructions']['worker']['value']='altered'
    elif fault=='reference':control['checkpoint']['node_instructions']['worker']={'mode':'reference','reference':'unresolved'}
    else:frames[1][1]['text']='{broken}'
    if fault in ('changed-payload','reference'):frames[1][1]['text']=json.dumps(control,ensure_ascii=False)
    assert not module.workflow_input_matches(frames,session,prompt,'orchestrator',receipt)


def test_resumed_control_display_label_keeps_independent_root_original_request():
    frames,receipt,_=control_turn()
    assert module.workflow_input_matches(frames,'same-session','Public resumed checkpoint display',
        'orchestrator',receipt,original_request=' exact original Ω\n')


def test_resumed_control_display_label_cannot_replace_root_original_request():
    frames,receipt,_=control_turn()
    assert not module.workflow_input_matches(frames,'same-session','Public resumed checkpoint display',
        'orchestrator',receipt,original_request='wrong root original')


@pytest.mark.parametrize('fault',['ack-owner','ack-version','ack-base','checkpoint-scope','delivery-mode','malformed-delivery','malformed-checkpoint','malformed-ack','malformed-bootstrap'])
def test_control_all_published_ack_and_scope_fields_must_agree(fault):
    frames,receipt,control=control_turn()
    if fault=='ack-owner':control['delivery']['acknowledgement']['session_owner']='foreign-owner'
    elif fault=='ack-version':control['delivery']['acknowledgement']['version']=1
    elif fault=='ack-base':control['delivery']['acknowledgement']['base_revision']=99
    elif fault=='checkpoint-scope':control['checkpoint']['workflow_run_id']='foreign-run'
    elif fault=='delivery-mode':control['delivery']['mode']='foreign-mode'
    elif fault=='malformed-delivery':control['delivery']=[]
    elif fault=='malformed-checkpoint':control['checkpoint']=[]
    elif fault=='malformed-ack':control['delivery']['acknowledgement']=[]
    else:
        control['bootstrap']=[]
        # Qualify the altered bytes so this probes bootstrap shape, not digest drift.
        payload={key:value for key,value in control.items() if key!='delivery'}
        digest=hashlib.sha256(json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        receipt['digest']=control['delivery']['acknowledgement']['digest']=digest
    frames[1][1]['text']=json.dumps(control,ensure_ascii=False)
    assert not module.workflow_input_matches(frames,'same-session',' exact original Ω\n','orchestrator',receipt)


@pytest.mark.parametrize('fault',['unknown-mode','bootstrap-base','missing-bootstrap','delta-null-base','delta-equal-base','delta-extra-bootstrap'])
def test_control_mode_and_base_revision_obey_closed_v2_shapes(fault):
    mode='delta' if fault.startswith('delta') else 'bootstrap'
    frames,receipt,control=control_turn(mode=mode)
    if fault=='unknown-mode':receipt['delivery_mode']=control['delivery']['mode']='unknown'
    elif fault=='bootstrap-base':receipt['base_revision']=control['delivery']['acknowledgement']['base_revision']=1
    elif fault=='missing-bootstrap':control.pop('bootstrap')
    elif fault=='delta-null-base':receipt['base_revision']=control['delivery']['acknowledgement']['base_revision']=None
    elif fault=='delta-equal-base':receipt['base_revision']=control['delivery']['acknowledgement']['base_revision']=receipt['revision']
    else:control['bootstrap']={'context':{'original_request':' exact original Ω\n'}}
    payload={key:value for key,value in control.items() if key!='delivery'}
    digest=hashlib.sha256(json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    receipt['digest']=control['delivery']['acknowledgement']['digest']=digest
    frames[1][1]['text']=json.dumps(control,ensure_ascii=False)
    assert not module.workflow_input_matches(frames,'same-session',' exact original Ω\n','orchestrator',receipt)
