from types import SimpleNamespace
import json
import pytest
from polybridge import server, workflows as w, workflow_inspection as wi, backends
from test_workflows import FakeRegistry, definition

@pytest.mark.parametrize('kind', ['mcp', 'cli'])
async def test_builder_reader_is_scoped_without_delegation_contract(tmp_path, monkeypatch, kind):
    association={'role':'builder','activation_id':'builder-a','workflow_run_id':'builder-run'}
    run={'workflow_run_id':'builder-run','kind':'builder','activations':[]}
    source=SimpleNamespace(record=SimpleNamespace(task_id='builder-task'))
    monkeypatch.setattr(w.WorkflowStore,'task_owner',lambda *a,**k:association)
    monkeypatch.setattr(w.WorkflowStore,'get_run',lambda *a:run)
    if kind=='mcp':
        from polybridge import workflow_hooks
        async def caller():return source
        monkeypatch.setattr(server,'_verified_workflow_caller',caller)
        monkeypatch.setattr(server,'_reg',lambda:SimpleNamespace(log_dir=tmp_path/'tasks'))
        monkeypatch.setattr(workflow_hooks,'owner',lambda *a,**k:association)
        value=await server._managed_workflow_reader()
    else:
        from polybridge import lineage
        monkeypatch.setattr(lineage,'detect_caller',lambda *a:source)
        value=wi.managed_reader(tmp_path/'tasks')
    assert value==(association,run)

async def test_builder_can_read_own_draft_but_not_other_run(monkeypatch):
    run={'workflow_run_id':'owned','name':'example','kind':'builder','status':'running','draft_revision':2,'builder_draft':{'nodes':[]},'activations':[]}
    async def managed():return {'role':'builder'},run
    monkeypatch.setattr(server,'_managed_workflow_reader',managed)
    assert (await server._workflow_call('status',run_id='owned'))['draft_revision']==2
    assert await server._workflow_call('detail',run_id='owned',view='builder_draft')
    with pytest.raises(Exception,match='own workflow'):
        await server._workflow_call('status',run_id='foreign')

async def test_create_builder_autosave_checks_captured_caller(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends,'is_installed',lambda _:True)
    storage=w.WorkflowStore(tmp_path)
    descriptor={'name':'example','orchestrator':{'backend':'codex'},'nodes':[],'connections':[]}
    run=storage.create_run(descriptor,'Create',tmp_path,kind='builder')
    # Captured caller must govern publication, even after the original caller exits.
    from polybridge.store import TaskRecord
    from dataclasses import asdict
    from datetime import datetime, timezone
    caller=TaskRecord(task_id='caller', backend='codex', session_id='s', repo_path=str(tmp_path), prompt='create', started_at=datetime.now(timezone.utc).isoformat(), status='completed', freedom='read_only', network=False, enforcement=backends.get('codex').enforcement('read_only',False).as_dict())
    storage.update_run(run['workflow_run_id'],lambda r:r.update(caller_record=asdict(caller)),'fixture')
    graph=definition();graph['nodes'][1]['freedom']='unrestricted'
    registry=FakeRegistry(tmp_path,[json.dumps(graph)])
    await w.WorkflowSupervisor(registry,storage).build(run['workflow_run_id'])
    settled=storage.get_run(run['workflow_run_id'])
    assert settled['status']=='needs_attention'
    assert "cannot exceed the caller's freedom" in settled['attention_reason']
    assert storage.list()==[]

async def test_builder_followup_with_retained_session_missing_starts_fresh(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends,'is_installed',lambda _:True)
    storage=w.WorkflowStore(tmp_path)
    descriptor={'name':'example','orchestrator':{'backend':'codex'},'nodes':[],'connections':[]}
    run=storage.create_run(descriptor,'Original',tmp_path,kind='builder')
    canvas=definition();canvas['nodes'][1]['instructions']='Durable edited instruction'
    key='builder:'+w._candidate_key({'backend':'codex'})
    storage.update_run(run['workflow_run_id'],lambda r:r.update(builder_followup=True,builder_turn_prompt='New feedback',builder_draft=canvas,draft_revision=7,editing_definition=canvas,sessions={'builder':{'candidate':key,'task_id':'retained-away','session_id':'gone'}}),'fixture')
    registry=FakeRegistry(tmp_path,[json.dumps(canvas)])
    await w.WorkflowSupervisor(registry,storage).build(run['workflow_run_id'])
    settled=storage.get_run(run['workflow_run_id'])
    assert settled['status']=='completed'
    prompt=registry.calls[0][0]
    assert 'New feedback' in prompt and 'Durable edited instruction' in prompt
    assert 'revision 7' in prompt
    assert settled['activations'][0]['tasks'][0]['session_mode']=='fresh'


def test_builder_task_reads_only_own_builder_turns():
    run={'activations':[{'id':'older','role':'builder','status':'completed','tasks':[{'task_id':'old'}]}, {'id':'current','role':'builder','status':'running','tasks':[{'task_id':'live'}]}, {'id':'worker','role':'node','status':'completed','tasks':[{'task_id':'worker'}]}]}
    scope=({'role':'builder','activation_id':'current'},run)
    wi.guard_task_read('old',scope)
    wi.guard_task_read('live',scope)
    for task in ('foreign','worker'):
        with pytest.raises(ValueError):wi.guard_task_read(task,scope)


@pytest.mark.parametrize('view',['executions','definition','decisions','technical_plan'])
async def test_builder_cannot_use_own_run_details_as_unrelated_inspection(monkeypatch,view):
    async def managed():return {'role':'builder'},{'workflow_run_id':'owned'}
    monkeypatch.setattr(server,'_managed_workflow_reader',managed)
    with pytest.raises(Exception,match='own draft'):
        await server._workflow_call('detail',run_id='owned',view=view)
