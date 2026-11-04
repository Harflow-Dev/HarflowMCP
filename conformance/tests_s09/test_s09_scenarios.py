"""Scenario evidence boundaries exercised with authored public data and stubs only."""
from types import SimpleNamespace

import pytest

from conformance.oracles import ConformanceError
from conformance.s09 import scenarios as module


class Observer:
    def __init__(self): self.records=[]
    def wait_for(self,predicate,timeout): return predicate(self.records)


def case(tmp_path):
    return module.Case(SimpleNamespace(observer=Observer(),workspace=tmp_path,output=tmp_path))


def workflow_case(tmp_path):
    c=case(tmp_path)
    owner={'role':'workflow-owner','pid':100,'token':'owner','exited':True}
    fake={'role':'fake','pid':300,'ppid':100,'token':'fake','exited':True}
    c.p.observer.records=[owner,fake]
    run={'workflow_run_id':'public-run','status':'completed','settling':False,
         'activations':[{'node_id':'node','role':'node','tasks':[{'task_id':'public-task','status':'completed'}]}]}
    c.workflow_status=lambda ident:run
    c.tree=lambda ident:{ident:run}
    status={'status':'completed','session_id':'public-session','exit_code':0}
    prompt={'complete':True,'text':' exact authored prompt 日本語\n'}
    frames=[('stdout',{'type':'system','subtype':'init','session_id':'public-session'}),
            ('stdin',{'type':'input','text':'prefix '+prompt['text']})]
    c.frames=lambda row:(frames,{'untrusted':True,'pid':row['pid']})
    c.call=lambda args:{'task':status} if args[0]=='status' else {'result':{'prompt':prompt}}
    c.terminal=lambda *args:None  # Separate S08 tests qualify public task settlement.
    return c,run,status,prompt,frames


def test_workflow_evidence_requires_unique_raw_public_os_conjunction(tmp_path):
    c,*_=workflow_case(tmp_path)
    evidence=c.workflow_evidence('public-run')
    assert evidence['owner_pids']==[100]
    assert evidence['tasks'][0]['fake_pid']==300
    assert evidence['tasks'][0]['task_id']=='public-task'
    assert evidence['tasks'][0]['raw']['untrusted'] is True


@pytest.mark.parametrize('fault',['unmatched-fake','no-fake','wrong-session','wrong-prompt','incomplete-prompt','unknown-fake-exit','unknown-owner-exit'])
def test_workflow_claims_fail_when_os_prompt_session_or_exit_disagrees(tmp_path,fault):
    c,run,status,prompt,frames=workflow_case(tmp_path)
    if fault=='unmatched-fake':c.p.observer.records.append({'role':'fake','pid':301,'token':'foreign-fake','exited':True})
    elif fault=='no-fake':c.p.observer.records=c.p.observer.records[:1]
    elif fault=='wrong-session':status['session_id']='foreign-session'
    elif fault=='wrong-prompt':prompt['text']='not the delivered prompt'
    elif fault=='incomplete-prompt':prompt['complete']=False
    elif fault=='unknown-fake-exit':c.p.observer.records[1]['exited']=False
    else:c.p.observer.records[0]['exited']=False
    with pytest.raises(ConformanceError):c.workflow_evidence('public-run')


def test_accepted_queued_task_uses_result_task_id_without_claiming_spawn(tmp_path):
    c=case(tmp_path)
    c.call=lambda args:{'result':{'task_id':'queued-task'}}
    item=c.accept(flags=('--resource','exclusive'))
    assert item['task_id']=='queued-task'
    assert c.accepted==[item] and c.launches==[]
    assert 'fake' not in item
    with pytest.raises(ConformanceError,match='OS spawn missing'):c.attach(item)
    c.p.observer.records=[{'role':'supervisor','pid':200,'token':'supervisor'},
                          {'role':'fake','pid':300,'ppid':200,'token':'fake'}]
    assert c.attach(item)['fake']['pid']==300
    assert c.launches==[item]
    with pytest.raises(ConformanceError):c.attach(item)


@pytest.mark.parametrize('result',[{}, {'task':{'task_id':'nested-wrong-shape'}},{'task_id':None},{'task_id':123}])
def test_accept_never_invents_missing_public_task_identity(tmp_path,result):
    c=case(tmp_path)
    c.call=lambda args:{'result':result}
    with pytest.raises(ConformanceError):c.accept()
    assert c.accepted==c.launches==[]


@pytest.mark.parametrize('args,caps',[
    (['statistics-page'],('ctl.v1','statistics-page.v1','statistics-cost-model.v1')),
    (['slot','status','heavy'],('ctl.v1','phase-slots.v1')),
    (['task-prompt','owned'],('ctl.v1','task-prompt.v1')),
    (['workflow-inspect','owned','--view','activity'],('ctl.v1','workflow-live-activity.v1')),
    (['workflow-resume','owned','--attention-checkpoint-id','issued'],('ctl.v1','workflow-attention-checkpoints.v1')),
])
def test_call_declares_exact_public_capabilities_before_operation(tmp_path,args,caps):
    c=case(tmp_path)
    calls=[]
    def command(argv,**kwargs):
        calls.append((argv,kwargs))
        return {'document':{'result':{}},'exit_code':0,'process':{'stub':True}}
    c.p.command=command
    c.call(args)
    assert calls==[([*args,'--json'],{'required_capabilities':caps})]
    assert c.operations[0]['argv']==args


def main_args(preflight):
    import hashlib
    return ['--invocation-id','0123456789abcdef0123456789abcdef','--repo','/never-entered',
            '--commit','a'*40,'--python','/never-executed','--observer-preflight',str(preflight),
            '--observer-preflight-sha256',hashlib.sha256(preflight.read_bytes()).hexdigest(),'--case','definitions']


def redirect_root(tmp_path,monkeypatch):
    from pathlib import Path
    original=Path
    monkeypatch.setattr(module,'Path',lambda value:tmp_path if value=='/private/tmp' else original(value))
    return tmp_path/'h9-0123456789abcd'


def test_main_preflight_hash_mismatch_never_enters_profile(tmp_path,monkeypatch):
    gate=tmp_path/'gate'
    gate.write_bytes(b'external independent gate bytes')
    args=main_args(gate)
    args[args.index('--observer-preflight-sha256')+1]='0'*64
    monkeypatch.setattr(module,'SourceProfile',lambda *a,**kw:pytest.fail('invalid preflight entered profile'))
    with pytest.raises(ConformanceError,match='byte identity mismatch'):module.main(args)


@pytest.mark.parametrize('identity',['short','A'*32,'../'+'a'*29])
def test_main_invalid_invocation_identity_never_enters_profile(tmp_path,monkeypatch,identity):
    gate=tmp_path/'gate'
    gate.write_bytes(b'independent gate')
    args=main_args(gate)
    args[1]=identity
    monkeypatch.setattr(module,'SourceProfile',lambda *a,**kw:pytest.fail('invalid identity entered profile'))
    with pytest.raises(ConformanceError,match='invalid caller invocation identity'):module.main(args)


def test_main_existing_invocation_directory_never_clobbered(tmp_path,monkeypatch):
    gate=tmp_path/'gate'
    gate.write_bytes(b'independent gate')
    root=redirect_root(tmp_path,monkeypatch)
    root.mkdir()
    sentinel=root/'report.json'
    sentinel.write_bytes(b'prior retained report')
    monkeypatch.setattr(module,'SourceProfile',lambda *a,**kw:pytest.fail('collision entered profile'))
    with pytest.raises(FileExistsError):module.main(main_args(gate))
    assert sentinel.read_bytes()==b'prior retained report'


def test_context_and_gate_bytes_exist_before_failed_profile_and_case_cannot_pass(tmp_path,monkeypatch):
    import json
    gate=tmp_path/'gate'
    gate.write_bytes(b'independent observer evidence')
    root=redirect_root(tmp_path,monkeypatch)
    calls=[]
    def denied(*args,**kwargs):
        context=json.loads((root/'context.json').read_text())
        assert context['invocation_id']=='0123456789abcdef0123456789abcdef'
        assert context['source_commit']=='a'*40
        assert (root/'observer-preflight.bin').read_bytes()==gate.read_bytes()
        calls.append(context)
        raise RuntimeError('independent profile boundary refusal')
    monkeypatch.setattr(module,'SourceProfile',denied)
    args=main_args(gate)+['--case','workflow-complete']
    assert module.main(args)==1
    assert len(calls)==1
    report=json.loads((root/'report.json').read_text())
    assert report['passed'] is False and report['C0_MC_04_accepted'] is False
    assert report['stock_backend_qualification'] is False
    assert report['cases'][0]['passed'] is False
    assert report['cases'][0]['failure']['no_replay'] is True
    assert report['not_run']==['workflow-complete']


@pytest.mark.parametrize('exit_code',[None,True,False])
def test_cancelled_workflow_attempt_requires_observed_integer_exit(tmp_path,exit_code):
    c,run,status,*_=workflow_case(tmp_path)
    run['status']='cancelled'
    run['activations'][0]['tasks'][0]['status']='cancelled'
    status.update(status='cancelled',exit_code=exit_code)
    with pytest.raises(ConformanceError,match='outcome contradicts'):c.workflow_evidence('public-run','cancelled')


@pytest.mark.parametrize('code,message',[('workflow_error','usage error'),('io_error','Worker nodes cannot inspect workflow context'),('usage_error','bad argument')])
def test_managed_evidence_rejects_unrelated_refusal(tmp_path,monkeypatch,code,message):
    import json
    from conformance.s09 import oracles
    c=case(tmp_path)
    c.p.shim=tmp_path/'shim'
    directory=tmp_path/'actor-300'/'capture'
    directory.mkdir(parents=True)
    (directory/'ledger.json').write_text(json.dumps({'streams':{'stdout':{'path':str(directory/'stdout.bin')}}}))
    (directory/'stdout.bin').write_bytes(b'stubbed trusted boundary')
    monkeypatch.setattr(module,'correlate_managed',lambda *a:{'error':{'code':code,'message':message}})
    with pytest.raises(ConformanceError):c.managed_evidence({'pid':300},['workflow-status','owned','--json'],code='workflow_error',message='Worker nodes cannot inspect workflow context')


def test_workflow_attempt_and_public_task_status_must_agree(tmp_path):
    c,run,status,*_=workflow_case(tmp_path)
    run['activations'][0]['tasks'][0]['status']='cancelled'
    with pytest.raises(ConformanceError,match='public status mismatch'):c.workflow_evidence('public-run')
