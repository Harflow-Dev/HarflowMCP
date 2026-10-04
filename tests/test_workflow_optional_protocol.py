"""Malformed optional results are discarded without granting denied capabilities."""
import copy
import json

import pytest

from polybridge import workflows as w, workflow_delegation as d
from test_workflow_delegation import Registry, run_flow, storage
from test_workflow_explicit_parallel import explicit
from test_workflow_traversal import policy


DENIALS = [{'tool':'file_system.bash', 'command':'git diff base...head > /tmp/review.diff'}, {'tool':'file_system.bash', 'command':'env | cut -d= -f1'}]


async def test_optional_malformed_result_with_denials_converges(storage, tmp_path):
    graph = explicit()
    next(n for n in graph['nodes'] if n['id']=='right').update(optional=True, title='right')
    raw = '{"status":"succeeded","result":{"findings":['
    run, registry = await run_flow(storage, tmp_path, graph, Registry(storage.root, policy, {'right': {'summary':raw, 'permission_denials':DENIALS}}), guided=True)
    assert run['status']=='completed', run.get('attention_reason')
    failed = next(a for a in run['activations'] if a['role']=='node' and a['node_id']=='right')
    assert failed['optional_failure']
    assert failed['node_result']['status']=='failed'
    assert failed['node_result']['result']['failure_kind']=='protocol'
    assert failed['raw_output']==raw
    assert failed['tasks'][0]['result']['permission_denials']==DENIALS
    assert not run['joins']
    assert sum(a['role']=='node' and a['node_id']=='right' for a in run['activations'])==3
    assert sum(a['role']=='node' and a['node_id']=='left2' for a in run['activations'])==1


def protocol_activation():
    return {'id':'bad', 'role':'node', 'node_id':'work', 'status':'failed', 'tasks':[{'task_id':'task','status':'completed','result':{'status':'completed','permission_denials':DENIALS}}], 'node_result':{'status':'failed','result':{'failure_kind':'protocol'},'evidence':[]}}


def test_protocol_denials_retry_requires_execution_scoped_caller_grant():
    a=protocol_activation()
    run={'runner_policy':'guided', 'instructions':'Try again', 'attempt_grants':{'work':1}}
    assert not d.retry_eligible(a,guided=True,run=run)
    run['protocol_retry_authorizations']={'bad':{'node_id':'work','reason':'Correct the malformed result under the same permissions'}}
    assert d.retry_eligible(a,guided=True,run=run)
    run['protocol_retry_authorizations']={'other':{'node_id':'work','reason':'unrelated'}}
    assert not d.retry_eligible(a,guided=True,run=run)


@pytest.mark.parametrize('unsafe', ['authority','permission','cancelled','uncertain'])
def test_caller_grant_never_authorizes_unsafe_failure(unsafe):
    a=protocol_activation(); a['node_result']['result']['failure_kind']=unsafe
    run={'runner_policy':'guided','protocol_retry_authorizations':{'bad':{'node_id':'work','reason':'Try same access'}}}
    assert not d.retry_eligible(a,guided=True,run=run)


async def test_resume_grant_authorizes_only_existing_settled_protocol_execution(storage,tmp_path):
    from test_workflow_delegation import graph
    run=storage.create_run(w.validate_definition(graph()),'request',tmp_path)
    a=protocol_activation()
    storage.update_run(run['workflow_run_id'],lambda r:r.update(status='needs_input', input_decision_id='decision', activations=[a]),'fixture')
    resumed=storage.control(run['workflow_run_id'],'resume',instructions='Retry this result under saved access',additional_attempts=1,decision_id='decision')
    assert d.retry_eligible(resumed['activations'][0],guided=True,run=resumed)
    assert resumed['protocol_retry_authorizations']['bad']['node_id']=='work'
    assert resumed['definition']['nodes'][1]['freedom']=='publish'


def test_pause_cannot_erase_an_input_question(storage,tmp_path):
    from test_workflow_delegation import graph
    run=storage.create_run(w.validate_definition(graph()),'request',tmp_path)
    storage.update_run(run['workflow_run_id'],lambda r:r.update(status='needs_input',input_decision_id='question-id',input_question='Which path?'),'fixture')
    paused=storage.control(run['workflow_run_id'],'pause')
    assert paused['status']=='needs_input'
    assert paused['input_question']=='Which path?'
    assert paused['input_decision_id']=='question-id'
    with pytest.raises(w.WorkflowError,match='requires an answer'):
        storage.control(run['workflow_run_id'],'resume')
    with pytest.raises(w.WorkflowError,match='current input decision_id'):
        storage.control(run['workflow_run_id'],'resume',instructions='Continue',decision_id='stale')
