"""Independent delivered-protocol probes, without engine state or subprocesses."""
import ast
import json
from pathlib import Path

import pytest

from conformance.s09 import fake_agent as module


def delivery(**checkpoint):
    return json.dumps({'delivery':{'acknowledgement':{'receipt':'current exact receipt'}},
                      'checkpoint':{'decision_id':'issued-decision','workflow_run_id':'issued-run',**checkpoint}})


def test_complete_requires_current_offer_and_copies_exact_delivery_ack():
    answer=module.decision_from_prompt(delivery(valid_actions=['complete']))
    assert answer['action']=='complete'
    assert answer['decision_id']=='issued-decision'
    assert answer['context_ack']=={'receipt':'current exact receipt'}
    assert 'next' not in answer


def test_continuation_uses_issued_id_and_authored_prompt_verbatim():
    prose=' exact worker prose 日本語\nS09_WORKER_SPEC: {"text":"fixed"}\n'
    answer=module.decision_from_prompt(delivery(valid_actions=['continue'],
        valid_continuations=[{'continuation_id':'issued-next','node_id':'worker','requires_prompt':True}],
        node_instructions={'worker':prose}))
    assert answer['next'][0]['continuation_id']=='issued-next'
    assert answer['next'][0]['session_mode']=='fresh'
    assert answer['next'][0]['prompt'].startswith(prose)
    assert 'issued-run' in answer['next'][0]['prompt']
    assert answer['action']=='continue'


def test_parallel_and_nested_branch_assignments_recurse_only_issued_ids():
    bundle={'continuation_id':'bundle','branch_selection':'orchestrator','branch_continuations':[
        {'continuation_id':'left','branch_continuations':[{'continuation_id':'nested'}]},
        {'continuation_id':'right'}]}
    answer=module.decision_from_prompt(delivery(valid_actions=['continue'],valid_continuations=[bundle]))
    entry=answer['next'][0]
    assert entry['continuation_id']=='bundle'
    assert entry['branch_assignments'][0]['branch_assignments']==[{'continuation_id':'nested'}]
    assert entry['branch_assignments'][1]=={'continuation_id':'right'}
    assert entry['selection_reason']


def test_nested_workflow_session_choice_uses_delivered_policy():
    choice={'continuation_id':'nested','node_id':'child','requires_prompt':True,
            'workflow':'authored-child','child_session_policy':'agent_decides'}
    answer=module.decision_from_prompt(delivery(valid_actions=['continue'],valid_continuations=[choice],node_instructions={'child':'authored child prose'}))
    selected=answer['next'][0]
    assert selected['child_session_mode']=='fresh'
    assert 'session_mode' not in selected


@pytest.mark.parametrize('text',['unknown prose','{}','[]',delivery(valid_actions=[]),
    delivery(valid_actions=['continue'],valid_continuations=[]),
    delivery(valid_actions=['continue'],valid_continuations=[{'node_id':'missing-id'}]),
    delivery(valid_actions=['continue'],valid_continuations=[{'continuation_id':'a'},{'continuation_id':'b'}]),
    delivery(valid_actions=['continue'],valid_continuations=[{'continuation_id':'a','requires_prompt':True,'node_id':'missing'}])])
def test_unknown_unoffered_or_unissued_decisions_fail_closed(text):
    with pytest.raises(ValueError): module.decision_from_prompt(text)


def test_needs_input_requires_authored_answer_in_current_delivery():
    case={'needs_input_question':' exact question?\n','answer_token':'unique-answer-token'}
    first=module.decision_from_prompt(delivery(valid_actions=['needs_input','complete']),case)
    assert first['action']=='needs_input' and first['question']==case['needs_input_question']
    after=module.decision_from_prompt(delivery(valid_actions=['needs_input','complete'],answer='unique-answer-token'),case)
    assert after['action']=='complete'
    with pytest.raises(ValueError):
        module.decision_from_prompt(delivery(valid_actions=['needs_input']),{'needs_input_question':'question'})


def test_parallel_and_recursive_budgets_refuse():
    branch={'continuation_id':'leaf'}
    for index in range(14): branch={'continuation_id':str(index),'branch_continuations':[branch]}
    for choice in [branch,{'continuation_id':'wide','branch_continuations':[{'continuation_id':str(i)} for i in range(33)]}]:
        with pytest.raises(ValueError): module.decision_from_prompt(delivery(valid_actions=['continue'],valid_continuations=[choice]))


def test_spec_extracts_json_and_preserves_authored_text_ignoring_suffix():
    spec=module.parse_spec('prefix S09_WORKER_SPEC: {"text":"exact 日本語\\n"} trailing context','S09_WORKER_SPEC:')
    assert spec=={'text':'exact 日本語\n'}
    with pytest.raises(ValueError):module.parse_spec('unknown','S09_WORKER_SPEC:')
    with pytest.raises(ValueError):module.parse_spec('S09_WORKER_SPEC: []','S09_WORKER_SPEC:')


def test_fake_has_no_engine_state_or_renderer_import_authority():
    tree=ast.parse(Path(module.__file__).read_text())
    names=[]
    for node in ast.walk(tree):
        if isinstance(node,ast.Import): names.extend(alias.name for alias in node.names)
        elif isinstance(node,ast.ImportFrom): names.append(node.module or '')
    assert not any(name=='harflow_mcp' or name.startswith('harflow_mcp.') for name in names)


def test_unmanaged_actor_refuses_before_any_process_or_artifact(tmp_path,monkeypatch):
    monkeypatch.delenv('HARFLOW_TASK_ID',raising=False)
    monkeypatch.setattr(module.Path,'mkdir',lambda *a,**kw:pytest.fail('unmanaged actor allocated artifacts'))
    with pytest.raises(ValueError,match='managed identity'): module._actor({'operation':'statistics-page','since':'2026-01-01T00:00:00Z','before':'2027-01-01T00:00:00Z'},'authored assignment')


def test_full_projected_instruction_section_and_reference_refusal():
    choice={'continuation_id':'issued-next','node_id':'worker','requires_prompt':True}
    full=delivery(valid_actions=['continue'],valid_continuations=[choice],
                  node_instructions={'worker':{'mode':'full','value':'exact public instruction 日本語\n'}})
    assert module.decision_from_prompt(full)['next'][0]['prompt'].startswith('exact public instruction 日本語\n')
    referenced=delivery(valid_actions=['continue'],valid_continuations=[choice],
                        node_instructions={'worker':{'mode':'reference','value':'not delivered prose'}})
    with pytest.raises(ValueError):module.decision_from_prompt(referenced)


@pytest.fixture
def fake_transport(tmp_path,monkeypatch):
    import sys
    from types import SimpleNamespace
    closed=[]
    monkeypatch.setitem(sys.modules,'client',SimpleNamespace(register=lambda *a,**kw:SimpleNamespace(close=lambda:closed.append(True))))
    monkeypatch.setattr(sys,'argv',['fake','--session-id','authored-session'])
    for key,value in {'S08_OBSERVER_SOCKET':'stub','S08_OBSERVER_NONCE':'stub',
        'S08_FAKE_LEDGER':str(tmp_path),'S08_WORKSPACE':str(tmp_path),'S09_CASE_SPEC':'{}'}.items():monkeypatch.setenv(key,value)
    output=[]
    monkeypatch.setattr(module.os,'write',lambda fd,raw:output.append(json.loads(raw)) or len(raw))
    monkeypatch.setattr(module.select,'select',lambda readers,*a:(readers,[],[]))
    def feed(spec):
        raw=json.dumps({'type':'input','text':'S09_WORKER_SPEC: '+json.dumps(spec)}).encode()+b'\n'
        data=iter(bytes([byte]) for byte in raw)
        monkeypatch.setattr(module.os,'read',lambda *a:next(data,b''))
    return feed,output,closed


@pytest.mark.parametrize('spec',[{}, {'reported_cost':0,'reported_model':'synthetic-observed'},
    {'reported_cost':.125,'reported_model':'synthetic-observed'}])
def test_fake_cost_model_fields_are_explicit_synthetic_reports_only(fake_transport,spec):
    feed,output,closed=fake_transport
    feed(spec)
    assert module.main()==0
    if spec:
        assert output[0]['model']==spec['reported_model']
        assert output[-1]['total_cost_usd']==spec['reported_cost']
    else:
        assert 'model' not in output[0]
        assert 'total_cost_usd' not in output[-1]
    assert closed==[True]


@pytest.mark.parametrize('bad',[-1,True,float('inf')])
def test_invalid_synthetic_cost_refuses_before_success(fake_transport,bad):
    feed,output,closed=fake_transport
    feed({'reported_cost':bad})
    with pytest.raises(ValueError):module.main()
    assert not any(frame.get('type')=='result' for frame in output)
    assert closed==[True]


@pytest.mark.parametrize('fault',[None,'timed_out','missing_returncode','unknown_cleanup'])
def test_managed_actor_fresh_handshake_bounded_calls_and_no_unknown_replay(tmp_path,monkeypatch,fault):
    import conformance.process
    monkeypatch.setenv('HARFLOW_TASK_ID','owned-managed-task')
    for key,value in {'S08_ARCHIVE_SRC':str(tmp_path/'src'),'S08_WORKSPACE':str(tmp_path),
                      'S08_PYTHON_SHIM':'/stub/shim','HARFLOW_STATE_ROOT':str(tmp_path/'state')}.items():monkeypatch.setenv(key,value)
    version=dict(product='harflow',format='harflow.version-info',v=1,product_version='0.0.7',component='engine',
        contracts={k:dict(format=v,v=1) for k,v in [('ctl','harflow.ctl'),('setup','harflow.setup'),('events','harflow.event')]},
        capabilities=['ctl.v1','version-info.v1','statistics-page.v1'],os='darwin',architecture='arm64',state_root=str(tmp_path/'state'))
    path=tmp_path/'response'
    calls=[]
    def run(argv,**kwargs):
        calls.append((argv,kwargs))
        path.write_text(json.dumps(version if len(calls)==1 else dict(product='harflow',format='harflow.ctl',v=1,product_version='0.0.7',error={'code':'agent_caller','message':'refused'})))
        row={'timed_out':False,'capture_exceeded':False,'pipes_drained':True,'returncode':0,
             'cleanup':'group_quiescent_os_observed','streams':{'stdout':{'path':str(path)}}}
        if fault=='timed_out':row['timed_out']=True
        elif fault=='missing_returncode':row['returncode']=None
        elif fault=='unknown_cleanup':row['cleanup']='unknown'
        return row
    monkeypatch.setattr(conformance.process,'run_process',run)
    if fault:
        with pytest.raises(ValueError,match='no replay'):module._actor({'operation':'statistics-page','since':'2026-01-01T00:00:00Z','before':'2027-01-01T00:00:00Z'},'authored assignment')
        assert len(calls)==1
    else:
        module._actor({'operation':'statistics-page','since':'2026-01-01T00:00:00Z','before':'2027-01-01T00:00:00Z'},'authored assignment')
        assert [args for args,_ in calls]==[['/stub/shim','version-info','--json'],['/stub/shim','statistics-page','--since','2026-01-01T00:00:00Z','--before','2027-01-01T00:00:00Z','--limit','1','--json']]
    assert all(kw['timeout']==10 and kw['capture_limit']==65536 for _,kw in calls)


def test_needs_input_question_is_scoped_to_delivered_stage():
    spec={'needs_input_question':'exact start question','answer_token':'authored-answer','question_node_id':'start'}
    first=module.decision_from_prompt(delivery(valid_actions=['needs_input','complete'],current_stage={'node_id':'start'}),spec)
    assert first['action']=='needs_input'
    end=module.decision_from_prompt(delivery(valid_actions=['needs_input','complete'],current_stage={'node_id':'end'}),spec)
    assert end['action']=='complete'
    missing=module.decision_from_prompt(delivery(valid_actions=['needs_input','complete']),spec)
    assert missing['action']=='complete'
