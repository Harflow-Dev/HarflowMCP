"""Durable node results remain inspectable without exposing live execution state."""
import copy
import json
from types import SimpleNamespace

import pytest

from polybridge import ctl, server
from polybridge.workflow_inspection import execution, result_page


@pytest.fixture
def run():
    return {"workflow_run_id": "run-1", "name": "feature", "definition": {}, "execution_contract": "delegation", "activations": [{"id": "old", "node_id": "review", "role": "node", "status": "completed", "node_result": {"status": "succeeded", "result": {"verdict": "changes_needed", "text": "世界" * 40000}, "evidence": []}, "raw_output": "raw" * 20000, "tasks": [{"task_id": "first", "status": "failed", "result": {"summary": "outage"}}, {"task_id": "second", "status": "completed", "result": {"summary": "success"}}]}, {"id": "live", "node_id": "implement", "role": "node", "status": "running", "tasks": [{"task_id": "third", "status": "running"}]}]}


def indexed_run(monkeypatch, tmp_path, value):
    from polybridge import workflows
    storage = workflows.WorkflowStore(root=tmp_path)
    for activation in value.get("activations", []):
        for task in activation.get("tasks", []):
            (storage.owners / (task["task_id"] + ".json")).write_text(json.dumps({"workflow_run_id": value["workflow_run_id"]}))
    monkeypatch.setattr(workflows.WorkflowStore, "get_run", lambda self, run_id: value)
    monkeypatch.setattr(workflows.WorkflowStore, "list_runs", lambda self: pytest.fail("Decoration must not scan workflow history"))


def test_full_result_chunks_are_lossless(run):
    cursor = None
    chunks = []
    while True:
        page = result_page(run, "old", cursor=cursor, limit=777)
        assert len(page["chunk"]) <= 777
        chunks.append(page["chunk"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    decoded = json.loads("".join(chunks))
    assert decoded == {"node_result": run["activations"][0]["node_result"], "raw_output": run["activations"][0]["raw_output"]}


def test_fallback_attempt_selection_and_membership(run):
    page = result_page(run, "old", task_id="first")
    assert json.loads(page["chunk"])["task_result"]["summary"] == "outage"
    with pytest.raises(ValueError, match="does not belong"):
        result_page(run, "old", task_id="third")


def test_cursor_cannot_cross_execution_attempt_or_changed_content(run):
    page = result_page(run, "old", limit=10)
    with pytest.raises(ValueError, match="cursor"):
        result_page(run, "old", task_id="first", cursor=page["next_cursor"])
    changed = copy.deepcopy(run)
    changed["activations"][0]["raw_output"] += "changed"
    with pytest.raises(ValueError, match="cursor"):
        result_page(changed, "old", cursor=page["next_cursor"])
    with pytest.raises(ValueError, match="cursor"):
        result_page(run, "old", cursor="broken")


@pytest.mark.parametrize("state", ["running", "reserved", "uncertain"])
def test_whole_execution_must_settle_including_fallbacks(run, state):
    run["activations"][0]["tasks"][0]["status"] = state
    with pytest.raises(ValueError, match="fully settled"):
        execution(run, "old")


async def test_orchestrator_can_only_read_settled_same_run_tasks(run, monkeypatch):
    async def reader():
        return {"role": "orchestrator", "activation_id": "decision"}, run
    monkeypatch.setattr(server, "_managed_workflow_reader", reader)
    await server._guard_task_read("first")
    with pytest.raises(Exception, match="fully settled"):
        await server._guard_task_read("third")
    with pytest.raises(Exception, match="own workflow"):
        await server._guard_task_read("foreign")


async def test_workers_read_only_their_own_activation(run, monkeypatch):
    async def reader():
        return {"role": "node", "activation_id": "live"}, run
    monkeypatch.setattr(server, "_managed_workflow_reader", reader)
    await server._guard_task_read("third")
    with pytest.raises(Exception, match="own execution"):
        await server._guard_task_read("first")


async def test_human_reads_unchanged(monkeypatch):
    async def reader():
        return None
    monkeypatch.setattr(server, "_managed_workflow_reader", reader)
    await server._guard_task_read("anything")


def test_cli_inspection_parity(monkeypatch, capsys):
    observed = []
    async def inspect(*args):
        observed.extend(args)
        return {"chunk": "{}", "has_more": False}
    monkeypatch.setattr(server, "inspect_workflow_node", inspect)
    assert ctl.main(["workflow-inspect", "r1", "e1", "--task-id", "t1", "--view", "activity", "--limit", "5", "--after-seq", "7", "--json"]) == 0
    assert observed == ["r1", "e1", "t1", "activity", None, 5, None, 7]
    assert json.loads(capsys.readouterr().out)["result"]["has_more"] is False


def test_cli_recovery_parity(monkeypatch, capsys):
    async def recover(action, **kwargs):
        assert action == "recover"
        assert kwargs == {"run_id": "r1", "instructions": "Inspected the failure", "additional_attempts": 2}
        return {"status": "running"}
    monkeypatch.setattr(server, "_workflow_call", recover)
    assert ctl.main(["workflow-recover", "r1", "--reason", "Inspected the failure", "--additional-attempts", "2", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["result"]["status"] == "running"


async def test_recovery_requires_reason_and_forwards_explicit_grants(monkeypatch):
    async def call(action, **kwargs):
        assert action == "recover"
        assert kwargs == {"run_id": "r", "instructions": "examined", "additional_attempts": 1}
        return kwargs
    monkeypatch.setattr(server, "_workflow_call", call)
    with pytest.raises(Exception, match="nonempty"):
        await server.recover_workflow("r", " ")
    await server.recover_workflow("r", "examined", 1)


async def test_workflow_read_scope_and_worker_graph_denial(run, monkeypatch):
    role = "orchestrator"
    async def reader():
        return {"role": role, "activation_id": "decision"}, run
    monkeypatch.setattr(server, "_managed_workflow_reader", reader)
    assert await server._workflow_call("get", name="feature") == {}
    assert len(await server._workflow_call("list_runs")) == 1
    with pytest.raises(Exception, match="own workflow"):
        await server._workflow_call("status", run_id="other-run")
    with pytest.raises(Exception, match="current workflow graph"):
        await server._workflow_call("get", name="another")
    role = "node"
    with pytest.raises(Exception, match="cannot inspect workflow"):
        await server._workflow_call("get", name="feature")


async def test_wait_returns_suspension_and_settling_without_waiting(monkeypatch):
    async def status(action, **kwargs):
        return {"workflow_run_id": kwargs["run_id"], "status": "needs_input", "settling": True, "input_question": "Which scope?"}
    monkeypatch.setattr(server, "_workflow_call", status)
    result = await server.wait_for_workflow("r1", 30)
    assert result["status"] == "needs_input" and result["settling"]
    assert result["input_question"] == "Which scope?" and not result["timed_out"]


def test_task_listing_metadata_preserves_assignment_and_original_entry(run, monkeypatch, tmp_path):
    from polybridge import workflows
    from polybridge.workflow_inspection import decorate_tasks
    indexed_run(monkeypatch, tmp_path, run)
    entry = {"task_id": "second", "prompt": "Review this focused assignment"}
    decorated = decorate_tasks([entry], tmp_path / "tasks")[0]
    assert decorated["workflow_node_id"] == "review"
    assert decorated["execution_contract"] == "delegation"
    assert decorated["prompt"] == entry["prompt"]
    assert entry == {"task_id": "second", "prompt": "Review this focused assignment"}


async def test_managed_status_is_compact_and_requires_explicit_inspection(run, monkeypatch):
    async def reader():
        return {"role": "orchestrator"}, run
    monkeypatch.setattr(server, "_managed_workflow_reader", reader)
    summary = await server._workflow_call("status", run_id="run-1")
    assert summary["activations"][0]["id"] == "old"
    assert "node_result" not in summary["activations"][0]
    assert "result" not in summary["activations"][0]["tasks"][0]
    assert summary["activations"][1]["tasks"] == [{"task_id": "third", "status": "running"}]


@pytest.mark.parametrize("cursor", ["W10=", "bnVsbA==", "MQ==", "InN0cmluZyI="])
def test_result_cursor_rejects_non_object_json(run, cursor):
    with pytest.raises(ValueError, match="Invalid or stale"):
        result_page(run, "old", cursor=cursor)


@pytest.mark.parametrize("role,expected", [("orchestrator", ["first", "second"]), ("node", ["third"])])
def test_cli_task_list_uses_managed_scope(run, monkeypatch, tmp_path, capsys, role, expected):
    from polybridge import store
    def reader(log_dir):
        return {"role": role, "activation_id": "live"}, run
    from polybridge import workflow_inspection
    monkeypatch.setattr(workflow_inspection, "managed_reader", reader)
    monkeypatch.setattr(ctl, "default_log_dir", lambda: tmp_path / "tasks")
    monkeypatch.setattr(store, "read_all", lambda path: ["first", "second", "third", "foreign"])
    monkeypatch.setattr(store, "brief", lambda path, record: {"task_id": record})
    assert ctl.main(["list", "--json"]) == 0
    assert [t["task_id"] for t in json.loads(capsys.readouterr().out)["tasks"]] == expected


@pytest.mark.parametrize("task_id", ["third", "foreign"])
def test_cli_status_refuses_live_or_foreign_task_for_orchestrator(run, monkeypatch, tmp_path, capsys, task_id):
    def reader(log_dir):
        return {"role": "orchestrator"}, run
    from polybridge import workflow_inspection
    monkeypatch.setattr(workflow_inspection, "managed_reader", reader)
    monkeypatch.setattr(ctl, "default_log_dir", lambda: tmp_path / "tasks")
    assert ctl.main(["status", task_id, "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "workflow_read_refused"


def test_worker_display_prompt_uses_full_assignment_for_each_execution(run, monkeypatch, tmp_path):
    from polybridge import workflows
    from polybridge.workflow_inspection import decorate_tasks
    assignment = "Focused assignment " + "A" * 6000
    run["activations"][0]["assignment_prompt"] = assignment
    run["prompt"] = "Caller original request"
    indexed_run(monkeypatch, tmp_path, run)
    entries = decorate_tasks([{"task_id": "second", "prompt": assignment[:2000]}, {"task_id": "outside", "prompt": "Regular task"}], tmp_path / "tasks")
    assert entries[0]["display_prompt"] == assignment
    assert entries[0]["prompt"] == assignment
    assert entries[1]["prompt"] == "Regular task"
    assert "display_prompt" not in entries[1]
    assert run["prompt"] == "Caller original request"


def test_runner_inspection_needs_no_registry_and_rejects_unsettled_questions(run, tmp_path, monkeypatch):
    from polybridge import tasks
    from polybridge.workflow_inspection import inspect_request
    monkeypatch.setattr(tasks, "TaskRegistry", lambda *a, **k: pytest.fail("Runner inspection must not construct a registry"))
    assert inspect_request(run, tmp_path, {"execution_id": "old", "view": "result"})["execution_id"] == "old"
    assert inspect_request(run, tmp_path, {"execution_id": "old", "view": "activity"})["events"] == []
    run["activations"][0]["status"] = "waiting_for_answer"
    with pytest.raises(ValueError, match="fully settled"):
        inspect_request(run, tmp_path, {"execution_id": "old"})


@pytest.mark.parametrize("payload", [{"execution_id": "old", "limit": 10.5}, {"execution_id": "old", "limit": "10"}, {"execution_id": "old", "view": "activity", "limit": True}, {"execution_id": "old", "view": "activity", "before_seq": 1, "after_seq": 2}, {"execution_id": "old", "workflow_run_id": "foreign"}])
def test_runner_inspection_validates_request(run, tmp_path, payload):
    from polybridge.workflow_inspection import inspect_request
    with pytest.raises(ValueError):
        inspect_request(run, tmp_path, payload)


def test_task_prompt_history_prefers_each_answer_attempt(run, monkeypatch, tmp_path):
    from polybridge import workflows
    from polybridge.workflow_inspection import decorate_tasks
    run["status"] = "running"
    run["interaction_owner"] = "monitor"
    run["activations"][0]["assignment_prompt"] = "Original assignment"
    run["activations"][0]["tasks"][0]["assignment_prompt"] = "Original assignment"
    run["activations"][0]["tasks"][1]["assignment_prompt"] = "Answer to worker clarification"
    indexed_run(monkeypatch, tmp_path, run)
    entries = decorate_tasks([{"task_id": "first"}, {"task_id": "second"}], tmp_path / "tasks")
    assert entries[0]["display_prompt"] == "Original assignment"
    assert entries[1]["display_prompt"] == "Answer to worker clarification"
    assert entries[1]["interaction_owner"] == "monitor"
    assert entries[1]["workflow_status"] == "running"


def test_cli_monitor_start_and_resume_forward_internal_owner(monkeypatch, capsys):
    observed = []
    async def call(action, **kwargs):
        observed.append((action, kwargs))
        return {"status": "running"}
    monkeypatch.setattr(server, "_workflow_call", call)
    assert ctl.main(["workflow-start", "feature", "--repo", "/tmp/repo", "--prompt", "Work", "--monitor", "--json"]) == 0
    assert observed[-1][1]["interaction_owner"] == "monitor"
    assert ctl.main(["workflow-resume", "r1", "--instructions", "Answer", "--decision-id", "d1", "--monitor", "--json"]) == 0
    assert observed[-1] == ("resume", {"run_id": "r1", "instructions": "Answer", "additional_attempts": 0, "decision_id": "d1", "interaction_owner": "monitor"})
    capsys.readouterr()


async def test_managed_agent_cannot_claim_monitor_ownership(monkeypatch, tmp_path):
    from polybridge.tasks import TaskRegistry
    registry = TaskRegistry(log_dir=tmp_path / "tasks", owner={})
    async def caller():
        return SimpleNamespace(record=SimpleNamespace(task_id="managed"))
    monkeypatch.setattr(registry, "_detect_caller", caller)
    monkeypatch.setattr(server, "_registry", registry)
    from polybridge import takeover
    monkeypatch.setattr(takeover, "caller_refusal", lambda *args: ("agent_caller", "managed agent"))
    with pytest.raises(Exception, match="human Monitor"):
        await server._workflow_call("start", interaction_owner="monitor")


async def test_managed_status_bounds_plan_but_preserves_source(run, monkeypatch):
    run["technical_plan"] = "## Technical approach\n" + "X" * 20000
    run["technical_plan_execution_id"] = "old"
    async def reader():
        return {"role": "orchestrator"}, run
    monkeypatch.setattr(server, "_managed_workflow_reader", reader)
    summary = await server._workflow_call("status", run_id="run-1")
    assert len(summary["technical_plan"]) == 16000
    assert summary["technical_plan_truncated"] is True
    assert summary["technical_plan_execution_id"] == "old"
    assert len(run["technical_plan"]) > 16000


async def test_recovered_completed_asking_turn_is_not_a_settled_node(run, monkeypatch, tmp_path):
    from polybridge.workflow_inspection import inspect_request
    activation = run["activations"][0]
    activation.pop("node_result")
    activation["tasks"][-1]["result"] = {"summary": '{"status":"asking","result":{"question":"Need context"},"evidence":[]}'}
    with pytest.raises(ValueError, match="final normalized"):
        inspect_request(run, tmp_path, {"execution_id": "old"})
    async def reader():
        return {"role": "orchestrator"}, run
    monkeypatch.setattr(server, "_managed_workflow_reader", reader)
    with pytest.raises(Exception, match="final normalized"):
        await server._guard_task_read("second")
    assert await server._filter_task_reads([{"task_id": "second"}]) == []


def test_delegation_asking_result_cannot_pass_settlement_gate(run):
    run["activations"][0]["node_result"] = {"status": "asking", "result": {"question": "Need context"}, "evidence": []}
    with pytest.raises(ValueError, match="final normalized"):
        execution(run, "old")


def test_cancelled_execution_activity_remains_inspectable_without_result(run, tmp_path):
    from polybridge.workflow_inspection import inspect_request
    run["activations"][0].pop("node_result")
    run["activations"][0]["status"] = "cancelled"
    assert inspect_request(run, tmp_path, {"execution_id": "old", "view": "activity"})["events"] == []


def test_descendant_projection_inherits_owner_but_not_assignment(monkeypatch, tmp_path):
    from polybridge import workflows
    from polybridge.workflow_inspection import decorate_tasks
    owner = {'workflow_run_id': 'run', 'status': 'running', 'settling': True, 'name': 'Review', 'execution_contract': 'delegation', 'activations': [{'id': 'execution', 'node_id': 'work', 'role': 'node', 'assignment_prompt': 'private ancestor assignment', 'tasks': [{'task_id': 'root'}]}]}
    indexed_run(monkeypatch, tmp_path, owner)
    entries = [{'task_id': 'grandchild', 'parent_task_id': 'child', 'prompt': 'own prompt'}, {'task_id': 'child', 'spawned_by': 'root', 'prompt': 'own child prompt'}]
    projected = decorate_tasks(entries, tmp_path / 'tasks')
    assert all(t['workflow_run_id'] == 'run' and t['workflow_settling'] is True for t in projected)
    assert projected[0]['prompt'] == 'own prompt'
    assert all('workflow_execution_id' not in t for t in projected)


def test_single_descendant_detail_retains_workflow_takeover_gate(monkeypatch, tmp_path):
    from polybridge import workflows, store
    from polybridge.workflow_inspection import decorate_tasks
    from polybridge.store import TaskRecord
    def make_record(**kwargs):
        return TaskRecord(backend="claude", session_id=None, repo_path=str(tmp_path), started_at="2026-10-04T00:00:00Z", prompt="test", **kwargs)
    monkeypatch.setattr(workflows.WorkflowStore, 'list_runs', lambda self: [])
    storage = workflows.WorkflowStore(root=tmp_path)
    indexed_run(monkeypatch, tmp_path, {'workflow_run_id': 'run', 'status': 'running', 'settling': True, 'activations': [{'id': 'execution', 'node_id': 'work', 'role': 'node', 'tasks': [{'task_id': 'root'}]}]})
    store.write(tmp_path / 'tasks', make_record(task_id='child', parent_task_id='root'))
    detail = decorate_tasks([{'task_id': 'grandchild', 'parent_task_id': 'child', 'prompt': 'own assignment'}], tmp_path / 'tasks')[0]
    assert detail['workflow_run_id'] == 'run'
    assert detail['workflow_settling'] is True
    assert detail['prompt'] == 'own assignment'


def test_ordinary_descendants_do_not_scan_run_history(monkeypatch, tmp_path):
    from polybridge import workflows, store
    from polybridge.workflow_inspection import decorate_tasks
    from polybridge.store import TaskRecord
    def make_record(**kwargs):
        return TaskRecord(backend="claude", session_id=None, repo_path=str(tmp_path), started_at="2026-10-04T00:00:00Z", prompt="test", **kwargs)
    calls = []
    def runs(self):
        calls.append(1)
        return []
    monkeypatch.setattr(workflows.WorkflowStore, 'list_runs', runs)
    def forbidden(*args, **kwargs):
        raise AssertionError('Unindexed ordinary tasks must not invoke ownership scan')
    monkeypatch.setattr(workflows.WorkflowStore, 'task_owner', forbidden)
    store.write(tmp_path / 'tasks', make_record(task_id='root'))
    entries = [{'task_id': f'child-{i}', 'parent_task_id': 'root'} for i in range(40)]
    assert decorate_tasks(entries, tmp_path / 'tasks') == entries
    assert calls == []


def test_decorating_hidden_cyclic_lineage_terminates(monkeypatch, tmp_path):
    from polybridge import workflows, store
    from polybridge.workflow_inspection import decorate_tasks
    from polybridge.store import TaskRecord
    def make_record(**kwargs):
        return TaskRecord(backend="claude", session_id=None, repo_path=str(tmp_path), started_at="2026-10-04T00:00:00Z", prompt="test", **kwargs)
    monkeypatch.setattr(workflows.WorkflowStore, 'list_runs', lambda self: [])
    store.write(tmp_path / 'tasks', make_record(task_id='one', parent_task_id='two'))
    store.write(tmp_path / 'tasks', make_record(task_id='two', parent_task_id='one'))
    entries = [{'task_id': 'child', 'parent_task_id': 'one'}]
    assert decorate_tasks(entries, tmp_path / 'tasks') == entries


def test_decoration_loads_only_receipt_runs_once_and_ignores_unrelated_history(monkeypatch, tmp_path):
    from polybridge import workflows
    from polybridge.workflow_inspection import decorate_tasks
    storage = workflows.WorkflowStore(root=tmp_path)
    for number in range(200):
        (storage.runs / f'unrelated-{number}.json').write_text('invalid unrelated history')
    for task in ('one', 'two'):
        (storage.owners / f'{task}.json').write_text(json.dumps({'workflow_run_id': 'requested'}))
    loaded = []
    def get_run(self, run_id):
        loaded.append(run_id)
        return {'workflow_run_id': run_id, 'name': 'Requested', 'status': 'running', 'activations': [{'id': 'execution', 'node_id': 'work', 'role': 'node', 'tasks': [{'task_id': 'one'}, {'task_id': 'two'}]}]}
    monkeypatch.setattr(workflows.WorkflowStore, 'get_run', get_run)
    monkeypatch.setattr(workflows.WorkflowStore, 'list_runs', lambda self: pytest.fail('Full history scan'))
    entries = [{'task_id': 'one'}, {'task_id': 'two'}, {'task_id': 'ordinary'}]
    result = decorate_tasks(entries, tmp_path / 'tasks')
    assert loaded == ['requested']
    assert result[0]['workflow_run_id'] == result[1]['workflow_run_id'] == 'requested'
    assert result[2] == entries[2]


def test_bad_receipt_does_not_break_unrelated_task_decoration(monkeypatch, tmp_path):
    from polybridge import workflows
    from polybridge.workflow_inspection import decorate_tasks
    storage = workflows.WorkflowStore(root=tmp_path)
    (storage.owners / 'broken.json').write_text('invalid')
    entries = [{'task_id': 'broken'}, {'task_id': 'ordinary'}]
    monkeypatch.setattr(workflows.WorkflowStore, 'list_runs', lambda self: pytest.fail('Full history scan'))
    assert decorate_tasks(entries, tmp_path / 'tasks') == entries


def test_compact_checklist_disposition_exposes_authority_with_bounded_reason():
    from polybridge.workflow_responses import compact, detail
    disposition = {'status': 'not_needed', 'reason': 'No implementation checklist is needed. ' * 200, 'execution_id': 'execution', 'decision_id': 'decision'}
    run = {'workflow_run_id': 'run', 'status': 'completed', 'checklist_disposition': disposition}
    result = compact(run)
    assert result['checklist_disposition']['status'] == 'not_needed'
    assert result['checklist_disposition']['reason_truncated']
    assert len(result['checklist_disposition']['reason']) == 256
    page = detail(run, 'checklist_disposition', limit=8000)
    assert disposition['reason'] in page['chunk']
