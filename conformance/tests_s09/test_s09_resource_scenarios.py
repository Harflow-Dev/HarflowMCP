"""Resource scenario read-only/uncertainty boundaries with owned stubs only."""
from types import SimpleNamespace

import pytest

from conformance.oracles import ConformanceError
from conformance.s09 import resource_scenarios as module


def doc(**fields):return dict(product='harflow',format='harflow.ctl',v=1,product_version='0.0.7',**fields)


def owner_loss_case(tmp_path,fault=None):
    fake={'role':'fake','pid':300,'token':'fake','exited':False}
    supervisor={'role':'supervisor','pid':200,'token':'supervisor','exited':False}
    rows=[supervisor,fake]
    calls=[]
    statuses=[]
    holder={'task_id':'holder','fake':fake,'supervisor':supervisor}
    queued={'task_id':'queued'}
    def reservation(task):return {'reservation_id':'r-'+task,'owner':{'kind':'task','task_id':task}}
    inventory=doc(resources=[{'name':'s09.owner-loss','capacity':1,'holders':[reservation('holder')],'queue':[]}])
    def accept(**kwargs):
        inventory['resources'][0]['queue']=[reservation('queued')]
        return queued
    def kill(token):
        assert token=='supervisor'
        supervisor['exited']=True
        if fault=='extra-fake':rows.append({'role':'fake','pid':301,'token':'extra','exited':False})
        if fault=='lost-fake':fake['exited']=True
        return True
    def call(args,**kwargs):
        calls.append((args,kwargs))
        if args==['resources']:return inventory
        assert args==['status','queued'] and kwargs=={'refused':True,'code':'task_pending_launch'}
        return doc(error={'code':'task_pending_launch','message':'waiting','data':{'task_id':'queued','launch_state':'waiting_for_resources'}})
    def status(task):
        assert task is holder
        value={'status':'failed' if fault=='failed-status' else 'running','exit_code':None,'output_drained':fault=='drained'}
        if fault=='exit':value['exit_code']=0
        statuses.append(value)
        return value
    def poll(operation,predicate):
        value=operation()
        assert predicate(value)
        return value
    observer=SimpleNamespace(records=rows,kill_participant=kill,wait_exited=lambda token,timeout:token=='supervisor' and supervisor['exited'])
    c=SimpleNamespace(p=SimpleNamespace(observer=observer,workspace=tmp_path),launch=lambda *a,**kw:holder,
        accept=accept,call=call,poll=poll,status=status,raw=lambda task,completed:{'untrusted':True,'completed':completed})
    return c,calls,statuses


def test_owner_loss_repeats_three_public_reads_without_release_or_replacement(tmp_path):
    c,calls,statuses=owner_loss_case(tmp_path)
    result=module.resource_owner_loss(c)
    assert len(statuses)==len(result['observations'])==3
    assert result['reservation_released'] is False and result['replacement_started'] is False
    assert result['raw']=={'untrusted':True,'completed':False}
    assert all(args[0] in ('resources','status') for args,_ in calls)
    assert not (tmp_path/'owner-loss-never-release').exists()
    assert c.p.observer.records[1]['exited'] is False


@pytest.mark.parametrize('fault',['extra-fake','lost-fake','drained','exit','failed-status'])
def test_owner_loss_never_qualifies_spawn_or_fabricated_settlement(tmp_path,fault):
    c,*_=owner_loss_case(tmp_path,fault)
    with pytest.raises(ConformanceError):module.resource_owner_loss(c)


def test_occupancy_does_not_accept_wrong_fifo_or_duplicate_resource():
    def row(task):return {'reservation_id':task,'owner':{'task_id':task}}
    resource={'name':'exclusive','capacity':1,'holders':[row('first')],'queue':[row('third'),row('second')]}
    value=doc(resources=[resource])
    assert module._occupancy(value,'exclusive',['first'],['second','third']) is False
    value['resources'][0]['queue'].reverse()
    assert module._occupancy(value,'exclusive',['first'],['second','third']) is True
    value['resources'].append(resource.copy())
    with pytest.raises(ConformanceError):module._occupancy(value,'exclusive',['first'],['second','third'])
