import copy
import json
from types import SimpleNamespace

import pytest

from polybridge import workflows as w, workflow_hooks, control
from test_workflows import definition


def test_saved_access_and_legacy_ceiling_are_separate(tmp_path):
    storage = w.WorkflowStore(tmp_path)
    node = {"role": "review", "freedom": "unrestricted"}
    legacy = storage.create_run(w.validate_definition(definition()), "go", tmp_path, freedom="read_only")
    current = storage.create_run(w.validate_definition(definition()), "go", tmp_path, permission_policy="saved_node")
    assert w.run_effective_freedom(legacy, node) == "read_only"
    assert w.run_effective_freedom(current, node) == "unrestricted"


async def test_authorized_snapshot_cannot_drift(tmp_path, monkeypatch):
    storage = w.WorkflowStore(tmp_path)
    saved = storage.save("example", definition())
    snapshot = copy.deepcopy(saved)
    changed = copy.deepcopy(saved)
    changed["nodes"][1]["freedom"] = "unrestricted"
    storage.save("example", changed, saved["revision"])
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    async def capture(*a): pass
    monkeypatch.setattr(w, "_capture_caller", capture)
    run = await w.start_workflow("example", "go", tmp_path, root=tmp_path, definition_snapshot=snapshot)
    assert run["revision"] == snapshot["revision"]
    assert run["definition"]["nodes"][1]["freedom"] == snapshot["nodes"][1]["freedom"]
    assert run["permission_policy"] == "saved_node"
    with pytest.raises(w.WorkflowError, match="caller freedom"):
        await w.start_workflow("example", "go", tmp_path, root=tmp_path, freedom="unrestricted")


def test_cancel_is_not_overwritten_by_pause(tmp_path, monkeypatch):
    storage = w.WorkflowStore(tmp_path)
    run = storage.create_run(w.validate_definition(definition()), "go", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="cancelling"), "fixture")
    assert storage.control(run["workflow_run_id"], "pause")["status"] == "cancelling"


@pytest.mark.parametrize("status,settling,allowed", [("running", False, False), ("paused",False,False), ("completed",True,False), ("completed",False,True), ("cancelled",False,True)])
def test_takeover_requires_whole_parent_settled(tmp_path,status,settling,allowed):
    storage=w.WorkflowStore(tmp_path)
    run=storage.create_run(w.validate_definition(definition()),"go",tmp_path)
    storage.update_run(run["workflow_run_id"],lambda r:r.update(status=status,activations=[{"id":"a","node_id":"work","role":"node","status":"completed","tasks":[{"task_id":"child","status":"running" if settling else "completed"}]}]),"fixture")
    if allowed:
        workflow_hooks.refuse_takeover(tmp_path/"tasks","child")
    else:
        with pytest.raises(control.TakeoverRefused,match="entire workflow"):
            workflow_hooks.refuse_takeover(tmp_path/"tasks","child")


def test_corrupt_unrelated_run_does_not_break_human_bookkeeping(tmp_path):
    storage=w.WorkflowStore(tmp_path)
    (storage.runs/"corrupt.json").write_text("{")
    workflow_hooks.pause_for_task(tmp_path/"tasks","ordinary","cancel")
    assert storage.list_runs()==[]
    with pytest.raises(w.WorkflowError,match="ownership"):
        workflow_hooks.refuse_managed(tmp_path/"tasks","ordinary")


@pytest.mark.parametrize("status,settling,allowed", [("running", False, False), ("needs_input", False, False), ("failed", True, False), ("failed", False, True)])
async def test_direct_messages_respect_workflow_assignment_authority(tmp_path, status, settling, allowed):
    from polybridge import inbox
    from polybridge.tasks import TaskRegistry
    storage = w.WorkflowStore(tmp_path)
    run = storage.create_run(w.validate_definition(definition()), "go", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status=status, activations=[{"id": "a", "node_id": "work", "role": "node", "status": "running" if settling else "completed", "tasks": [{"task_id": "child", "status": "running" if settling else "completed"}]}]), "fixture")
    if allowed:
        workflow_hooks.refuse_direct_message(tmp_path / "tasks", "child")
    else:
        registry = TaskRegistry(log_dir=tmp_path / "tasks", owner={}, open_monitor=False)
        with pytest.raises(inbox.SendRefused, match="active workflow"):
            await registry.send_message(SimpleNamespace(task_id="child", live_input=True), "change assignment")
        with pytest.raises(inbox.SendRefused, match="active workflow"):
            await registry.send_to_record("child", "change assignment")
        assert not (tmp_path / "tasks/child.inbox.jsonl").exists()


def test_optional_decoration_survives_missing_owned_run(tmp_path):
    from polybridge.workflow_inspection import decorate_tasks
    storage = w.WorkflowStore(tmp_path)
    (storage.owners / "child.json").write_text(json.dumps({"workflow_run_id": "missing"}))
    entries = [{"task_id": "child", "parent_task_id": "parent", "status": "completed"}]
    assert decorate_tasks(entries, tmp_path / "tasks") == entries
    from polybridge import inbox
    with pytest.raises(inbox.SendRefused, match="ownership"):
        workflow_hooks.refuse_direct_message(tmp_path / "tasks", "child")


async def test_orphan_dead_process_does_not_hold_checkout(tmp_path,monkeypatch):
    storage=w.WorkflowStore(tmp_path)
    run=storage.create_run(w.validate_definition(definition()),"go",tmp_path)
    storage.update_run(run["workflow_run_id"],lambda r:r.update(status="needs_attention",activations=[{"id":"a","node_id":"work","role":"node","status":"running","tasks":[{"task_id":"dead","status":"running","freedom":"write_in_repo"}]}]),"fixture")
    monkeypatch.setattr(w.task_store,"read",lambda *a:SimpleNamespace(status="running"))
    monkeypatch.setattr(w,"task_liveness",lambda *a:{"process_alive":False,"outcome_known":False})
    async with w.CheckoutLease(storage,str(tmp_path),True,wait_seconds=.01): pass


async def test_missing_dispatch_stays_blocking_and_wait_is_bounded(tmp_path):
    storage=w.WorkflowStore(tmp_path)
    run=storage.create_run(w.validate_definition(definition()),"go",tmp_path)
    storage.update_run(run["workflow_run_id"],lambda r:r.update(status="needs_attention",activations=[{"id":"a","node_id":"work","role":"node","status":"uncertain","tasks":[{"task_id":"missing","status":"uncertain","freedom":"write_in_repo"}]}]),"fixture")
    notices=[]
    with pytest.raises(w.DispatchNotStarted,match="reconciliation"):
        async with w.CheckoutLease(storage,str(tmp_path),False,wait_seconds=.01,on_wait=notices.append): pass
    assert notices
    assert storage.get_run(run["workflow_run_id"])["activations"][0]["tasks"][0]["status"]=="uncertain"


async def test_task_mutation_refuses_undecidable_caller(tmp_path,monkeypatch):
    from polybridge import lineage
    from polybridge.tasks import TaskRegistry
    from polybridge.backends import NestedDispatchRefused
    monkeypatch.setattr(lineage,"detect_caller_detail",lambda *a:lineage.Detection(None,"ps denied"))
    registry=TaskRegistry(log_dir=tmp_path/"tasks")
    with pytest.raises(NestedDispatchRefused,match="ps denied"):
        await registry._mutation_caller()


def test_observed_identity_never_comes_from_worker_prose(tmp_path):
    path=tmp_path/"stream.jsonl"
    metadata={"requested":{"model":"alias"},"effective":{"model":"alias"},"observed":None,"verification_status":"configured_not_observed","provenance":"validated_launch_configuration"}
    path.write_text(json.dumps({"type":"assistant","model":"invented","text":"My model is opus"})+"\n")
    snapshot={"backend":"claude","raw_stream_log":str(path)}
    assert w.observed_harness_metadata(snapshot,metadata)["observed"] is None
    path.write_text(json.dumps({"type":"system","subtype":"init","model":"actual-native-model"})+"\n")
    result=w.observed_harness_metadata(snapshot,metadata)
    assert result["observed"]["model"]=="actual-native-model"
    assert result["effective"]["model"]=="alias"
    assert result["verification_status"]=="observed"


def test_dead_unobserved_outcome_cannot_recover_as_success(tmp_path,monkeypatch):
    storage=w.WorkflowStore(tmp_path)
    run=storage.create_run(w.validate_definition(definition()),"go",tmp_path)
    storage.update_run(run["workflow_run_id"],lambda r:r.update(status="needs_attention",activations=[{"id":"a","node_id":"work","role":"node","status":"running","tasks":[{"task_id":"dead","status":"running"}]}]),"fixture")
    monkeypatch.setattr(w.task_store,"read",lambda *a:SimpleNamespace(status="running"))
    monkeypatch.setattr(w,"task_liveness",lambda *a:{"process_alive":False,"outcome_known":False})
    monkeypatch.setattr(w.task_store,"snapshot",lambda *a:{"status":"completed","summary":"old result"})
    from polybridge import workflow_delegation
    monkeypatch.setattr(workflow_delegation,"reconcile_delegation",lambda r:None)
    recovered=storage.reconcile_run(run["workflow_run_id"])["activations"][0]["tasks"][0]
    assert recovered["status"]=="failed"
    assert recovered["result"]["outcome_unknown"] is True


def test_known_ownership_receipt_isolated_from_corrupt_other_run(tmp_path):
    storage=w.WorkflowStore(tmp_path)
    run=storage.create_run(w.validate_definition(definition()),"go",tmp_path)
    storage.update_run(run["workflow_run_id"],lambda r:r.update(status="completed",activations=[{"id":"a","node_id":"work","role":"node","status":"completed","tasks":[{"task_id":"known","status":"completed"}]}]),"fixture")
    (storage.runs/"broken.json").write_text("{")
    assert storage.task_owner("known",strict=True)["workflow_run_id"]==run["workflow_run_id"]
    workflow_hooks.refuse_takeover(tmp_path/"tasks","known")
    with pytest.raises(w.WorkflowError):
        storage.task_owner("unknown",strict=True)


@pytest.mark.parametrize("verdict,alive",[("dead",False),("alive",True),("undecidable",None)])
def test_unobserved_liveness_keeps_unknown_distinct(tmp_path,monkeypatch,verdict,alive):
    from polybridge import identity
    record=SimpleNamespace(status="running",exit_code=None,pid=123,start_time="old",markers=[])
    monkeypatch.setattr(identity,"check_detail",lambda *a:(verdict,"evidence"))
    monkeypatch.setattr(w.task_store,"resolve_status",lambda *a,**k:("failed","unobserved",None,[]))
    result=w.task_liveness(tmp_path,record)
    assert result["process_alive"] is alive
    assert result["outcome_known"] is False


def test_nested_descendant_inherits_workflow_takeover_gate(tmp_path,monkeypatch):
    storage=w.WorkflowStore(tmp_path)
    run=storage.create_run(w.validate_definition(definition()),"go",tmp_path)
    storage.update_run(run["workflow_run_id"],lambda r:r.update(status="running",activations=[{"id":"a","node_id":"work","role":"node","status":"completed","tasks":[{"task_id":"worker","status":"completed"}]}]),"fixture")
    records={"nested":SimpleNamespace(parent_task_id="worker",spawned_by=None)}
    monkeypatch.setattr(w.task_store,"read",lambda root,tid:records.get(tid))
    assert storage.task_owner("nested")["workflow_run_id"]==run["workflow_run_id"]
    with pytest.raises(control.TakeoverRefused,match="entire workflow"):
        workflow_hooks.refuse_takeover(tmp_path/"tasks","nested")


def test_failed_run_cannot_recover_into_taken_over_session(tmp_path,monkeypatch):
    storage=w.WorkflowStore(tmp_path)
    run=storage.create_run(w.validate_definition(definition()),"go",tmp_path)
    storage.update_run(run["workflow_run_id"],lambda r:r.update(status="failed",activations=[{"id":"a","node_id":"work","role":"node","status":"failed","tasks":[{"task_id":"worker","status":"failed"}]}]),"fixture")
    monkeypatch.setattr(w,"_supervisor_present",lambda r:True)
    monkeypatch.setattr(control,"takeover_reservations",lambda root:{"worker":"session"})
    with pytest.raises(w.WorkflowError,match="human takeover"):
        storage.control(run["workflow_run_id"],"recover",instructions="retry")


async def test_checkout_timeout_does_not_leave_false_live_activation(tmp_path,monkeypatch):
    from test_workflows import FakeRegistry
    storage=w.WorkflowStore(tmp_path)
    run=storage.create_run(w.validate_definition(definition()),"go",tmp_path)
    storage.update_run(run["workflow_run_id"],lambda r:r.update(status="running",activations=[{"id":"a","node_id":"work","role":"node","status":"running","tasks":[]}]),"fixture")
    monkeypatch.setattr(w.backends,"is_installed",lambda backend:True)
    async def blocked(self):
        raise w.DispatchNotStarted("Checkout wait exhausted; reconciliation is required")
    monkeypatch.setattr(w.CheckoutLease,"__aenter__",blocked)
    supervisor=w.WorkflowSupervisor(FakeRegistry(tmp_path,[]),storage)
    supervisor.run_id=run["workflow_run_id"]
    node=run["definition"]["nodes"][1]
    assert await supervisor._dispatch(node,"assignment","node",storage.get_run(run["workflow_run_id"])["activations"][0]) is None
    settled=storage.get_run(run["workflow_run_id"])
    assert settled["status"]=="needs_attention"
    assert settled["activations"][0]["status"]=="not_started"
    assert settled["settling"] is False


@pytest.mark.parametrize('unsafe', [{'outcome_unknown': True}, {'permission_denials': ['denied']}])
def test_unsafe_outage_diagnostics_never_authorize_fallback(unsafe):
    assert w.availability_failure({'status': 'failed', 'backend': 'codex', 'stderr_tail': ['rate_limit_exceeded'], **unsafe}) is None


@pytest.mark.parametrize('checks', [['undecidable'], ['alive','undecidable']])
def test_cancel_refuses_uncertain_supervisor_without_run_or_history_mutation(tmp_path, monkeypatch, checks):
    from polybridge import identity
    storage=w.WorkflowStore(tmp_path)
    run=storage.create_run(w.validate_definition(definition()),'Run',tmp_path)
    storage.update_run(run['workflow_run_id'],lambda r:r.update(status='running',supervisor_pid=999,supervisor_identity={'pid':999}),'fixture')
    before={path:path.read_bytes() for path in storage.root.rglob('*') if path.is_file()}
    values=iter(checks)
    monkeypatch.setattr(identity,'identity_check',lambda _:next(values))
    monkeypatch.setattr(w,'_launch',lambda *a:(_ for _ in ()).throw(AssertionError('must not launch')))
    with pytest.raises(w.WorkflowError,match='uncertain.*cancel'):
        storage.control(run['workflow_run_id'],'cancel')
    after={path:path.read_bytes() for path in storage.root.rglob('*') if path.is_file()}
    assert after==before
    assert storage.get_run(run['workflow_run_id'])['status']=='running'


@pytest.mark.parametrize('identity_state,expected_launch', [('dead', True), ('alive', False)])
def test_cancel_verified_supervisor_mutates_and_launches_only_for_dead_owner(tmp_path,monkeypatch,identity_state,expected_launch):
    from polybridge import identity
    storage=w.WorkflowStore(tmp_path)
    run=storage.create_run(w.validate_definition(definition()),'Run',tmp_path)
    storage.update_run(run['workflow_run_id'],lambda r:r.update(status='running',supervisor_pid=999,supervisor_identity={'pid':999}),'fixture')
    monkeypatch.setattr(identity,'identity_check',lambda _:identity_state)
    launched=[]
    monkeypatch.setattr(w,'_launch',lambda *a:launched.append(a))
    result=storage.control(run['workflow_run_id'],'cancel')
    assert result['status']=='cancelling'
    assert bool(launched)==expected_launch
