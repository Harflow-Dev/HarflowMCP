"""Additive workflow transport contracts preserve the task CLI schema."""
import json

import pytest

from polybridge import ctl, server


async def test_start_task_workflow_has_explicit_run_identity(monkeypatch):
    observed = {}
    async def invoke(action, **kwargs):
        observed.update(action=action, **kwargs)
        return {"workflow_run_id": "run-1", "status": "pending"}
    monkeypatch.setattr(server, "_workflow_call", invoke)
    result = await server.start_task("do it", "/tmp/repo", workflow="review")
    assert result["workflow_run_id"] == "run-1"
    assert "task_id" not in result
    assert observed["overrides"] == {}


async def test_workflow_rejects_task_only_options():
    with pytest.raises(Exception, match="group or title"):
        await server.start_task("do it", "/tmp/repo", workflow="review", group="team")


def test_cli_workflow_start_envelope(monkeypatch, capsys):
    observed = {}
    async def start(name, prompt, repo, overrides, freedom, network):
        observed.update(name=name, overrides=overrides, network=network)
        return {"workflow_run_id": "run-1", "status": "pending"}
    monkeypatch.setattr(server, "start_workflow", start)
    assert ctl.main(["run", "--workflow", "review", "--repo", "/tmp/repo", "--prompt", "do it", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {"v": 2, "result": {"workflow_run_id": "run-1", "status": "pending"}}
    assert observed == {"name": "review", "overrides": None, "network": None}


def test_cli_save_reads_file_and_expected_revision(tmp_path, monkeypatch, capsys):
    definition = tmp_path / "definition.json"
    definition.write_text('{"nodes": []}')
    async def save(name, value, revision):
        assert (name, value, revision) == ("draft", {"nodes": []}, 3)
        return {"name": name, "revision": 4}
    monkeypatch.setattr(server, "save_workflow", save)
    assert ctl.main(["workflow-save", "draft", "--definition", str(definition), "--expected-revision", "3", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["result"]["revision"] == 4


def test_cli_requires_backend_without_workflow(capsys):
    assert ctl.main(["run", "--repo", "/tmp/repo", "--prompt", "do it", "--json"]) == 1
    assert "--backend" in capsys.readouterr().out


async def test_wait_workflow_checks_timeout():
    with pytest.raises(Exception, match="between 0 and 300"):
        await server.wait_for_workflow("run-1", 301)


def test_managed_agent_cannot_directly_dispatch(monkeypatch, tmp_path):
    from polybridge import workflow_hooks
    from polybridge.backends.base import NestedDispatchRefused
    monkeypatch.setattr(workflow_hooks, "owner", lambda *args: {"role": "node", "status": "running"})
    with pytest.raises(NestedDispatchRefused) as refused:
        workflow_hooks.refuse_managed(tmp_path, "reserved-1")
    assert refused.value.rule == "workflow_managed"


def test_external_control_does_not_reopen_terminal_workflow(monkeypatch, tmp_path):
    from polybridge import workflow_hooks
    monkeypatch.setattr(workflow_hooks, "owner", lambda *args: {"workflow_run_id": "r1", "status": "completed"})
    workflow_hooks.pause_for_task(tmp_path, "t1", "manual resume")


async def test_workflow_mutation_refuses_managed_caller(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from polybridge import workflow_hooks
    from polybridge.tasks import TaskRegistry
    registry = TaskRegistry(log_dir=tmp_path / "tasks", owner={})
    async def caller():
        return SimpleNamespace(record=SimpleNamespace(task_id="managed-1"))
    monkeypatch.setattr(registry, "_detect_caller", caller)
    monkeypatch.setattr(server, "_reg", lambda: registry)
    monkeypatch.setattr(workflow_hooks, "owner", lambda *args: {"role": "builder"})
    with pytest.raises(Exception, match="direct dispatch and workflow mutations"):
        await server.save_workflow("draft", {})


@pytest.mark.parametrize("action,key", [("list", "workflows"), ("list-runs", "runs")])
def test_cli_workflow_collections_are_result_objects(action, key, monkeypatch, capsys):
    async def invoke(action):
        return [{"name": "review"}]
    monkeypatch.setattr(server, "_workflow_call", invoke)
    assert ctl.main(["workflow-" + action, "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {"v": 2, "result": {key: [{"name": "review"}]}}


async def test_registry_honors_reserved_id_and_refuses_reuse(tmp_path, git_repo, fake_backend_clis):
    from polybridge import backends, store
    from polybridge.tasks import TaskRegistry
    registry = TaskRegistry(log_dir=tmp_path / "tasks", owner={}, open_monitor=False)
    task = await registry.start("test", git_repo, backend=backends.get("claude"), task_id="reserved-id")
    await task.done.wait()
    assert task.task_id == "reserved-id"
    assert store.read(registry.log_dir, "reserved-id") is not None
    with pytest.raises(ValueError, match="already exists"):
        await registry.start("test", git_repo, backend=backends.get("claude"), task_id="reserved-id")


@pytest.fixture
def managed_workflow_run(tmp_path, monkeypatch):
    """Use durable reserved-ID ownership, rather than a fabricated guard verdict."""
    from types import SimpleNamespace
    from polybridge import workflows
    from polybridge.tasks import TaskRegistry
    monkeypatch.setenv("HOME", str(tmp_path))
    storage = workflows.WorkflowStore()
    definition = storage.save("guarded", {
        "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
        "connections": [{"id": "finish", "source": "start", "target": "end"}],
    })
    run = storage.create_run(definition, "task", tmp_path)
    run_id = run["workflow_run_id"]
    def reserve(run):
        run["status"] = "needs_attention"
        run["tasks"] = [{"id": "check-1", "title": "Implement", "status": "pending"}]
        run["activations"] = [{"id": "activation-1", "node_id": "worker", "role": "node", "status": "running", "tasks": [{"task_id": "reserved-worker", "status": "reserved"}]}]
    storage.update_run(run_id, reserve, "test_reserved")
    registry = TaskRegistry(log_dir=storage.root / "tasks", owner={})
    async def caller():
        return SimpleNamespace(record=SimpleNamespace(task_id="reserved-worker"))
    monkeypatch.setattr(registry, "_detect_caller", caller)
    monkeypatch.setattr(server, "_reg", lambda: registry)
    return storage, run_id


@pytest.mark.parametrize("operation", ["save", "resume"])
async def test_reserved_worker_cannot_mutate_workflow_or_checklist(managed_workflow_run, operation):
    from mcp import MCPError
    storage, run_id = managed_workflow_run
    before = storage.get_run(run_id)
    with pytest.raises(MCPError, match="direct dispatch and workflow mutations"):
        if operation == "save":
            await server.save_workflow("guarded", before["definition"], expected_revision=1)
        else:
            await server.resume_workflow(run_id, instructions="Mark check-1 completed", additional_attempts=3)
    assert storage.get_run(run_id) == before
    assert storage.get("guarded")["revision"] == 1


def test_worker_cli_resume_cannot_change_checklist(managed_workflow_run, capsys):
    storage, run_id = managed_workflow_run
    before = storage.get_run(run_id)
    assert ctl.main(["workflow-resume", run_id, "--instructions", "check-1 is completed", "--json"]) == 1
    document = json.loads(capsys.readouterr().out)
    assert "direct dispatch and workflow mutations" in document["error"]["message"]
    assert storage.get_run(run_id) == before


async def test_public_mcp_has_no_checklist_completion_mutation():
    from mcp import Client
    async with Client(server.mcp) as client:
        tools = {t.name: t for t in (await client.list_tools()).tools}
    # Worker's evidence reaches the supervisor through task output. Only an internal
    # orchestrator decision can supply task_updates to change the run checklist.
    assert set(name for name in tools if "workflow" in name) == {
        "list_workflows", "get_workflow", "save_workflow", "delete_workflow",
        "workflow_builder", "start_workflow", "list_workflow_runs",
        "get_workflow_status", "wait_for_workflow", "pause_workflow",
        "resume_workflow", "cancel_workflow",
    }
    for name in ("pause_workflow", "resume_workflow", "cancel_workflow"):
        assert not {"task_updates", "tasks", "completed_task_ids", "status", "completed"} & set(tools[name].input_schema["properties"])


@pytest.mark.parametrize("arguments", [
    ["workflow-task-complete", "run-1", "check-1"],
    ["workflow-resume", "run-1", "--completed-task-ids", "check-1"],
    ["workflow-resume", "run-1", "--task-updates", '[{"task_id":"check-1","status":"completed"}]'],
])
def test_cli_rejects_direct_checklist_completion(arguments, capsys):
    with pytest.raises(SystemExit) as error:
        ctl.main([*arguments, "--json"])
    assert error.value.code == 2
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "usage"


def test_cli_validation_is_authoritative_and_creates_no_workflow_state(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HOME", str(tmp_path))
    definition = tmp_path / "input.json"
    value = {"name": "draft", "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}], "connections": [{"id": "finish", "source": "start", "target": "end"}]}
    definition.write_text(json.dumps(value))
    assert ctl.main(["workflow-validate", "--definition", str(definition), "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {"v": 2, "result": {"valid": True}}
    value["nodes"].append({"id": "orphan", "type": "agent"})
    definition.write_text(json.dumps(value))
    assert ctl.main(["workflow-validate", "--definition", str(definition), "--json"]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert result["valid"] is False
    assert "orphan" in result["error"]
    assert not (tmp_path / ".polybridge").exists()


def test_cli_validation_reports_malformed_input_as_transport_error(tmp_path, capsys):
    path = tmp_path / "invalid.json"
    path.write_text("{broken")
    assert ctl.main(["workflow-validate", "--definition", str(path), "--json"]) == 1
    assert "error" in json.loads(capsys.readouterr().out)


@pytest.mark.parametrize("freedom", ["read_only", "write_in_repo", "publish", "unrestricted"])
def test_cli_workflow_launch_accepts_all_access_ceilings(freedom, monkeypatch, capsys):
    async def start(name, prompt, repo, overrides, actual_freedom, network):
        assert actual_freedom == freedom
        assert network is None
        return {"workflow_run_id": "run-1"}
    monkeypatch.setattr(server, "start_workflow", start)
    assert ctl.main(["workflow-start", "example", "--repo", "/tmp/repo", "--prompt", "task", "--freedom", freedom, "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["result"]["workflow_run_id"] == "run-1"
