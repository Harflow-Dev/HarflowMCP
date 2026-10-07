import json

import pytest

from polybridge.workflow_inspection import assigned_input_page, guard_task_read, make_assigned_input_ref


def fixture_run():
    return {"workflow_run_id": "run", "execution_contract": "delegation", "activations": [
        {"id": "source", "role": "node", "node_id": "before", "status": "completed", "node_result": {"status": "succeeded", "summary": "雪🙂" * 100}, "raw_output": "日本語\n" * 100, "tasks": [{"task_id": "old", "status": "completed", "result": {"summary": "attempt"}}]},
        {"id": "worker", "role": "node", "node_id": "now", "status": "running", "tasks": [{"task_id": "current", "status": "running"}]},
    ]}


def authorize(run, root):
    run["activations"][1]["authorized_input_refs"] = [make_assigned_input_ref(run, root, "source")]
    return ({"role": "node", "activation_id": "worker"}, run)


def test_assigned_input_lossless_unicode_and_generic_guard(tmp_path):
    run = fixture_run()
    managed = authorize(run, tmp_path)
    chunks, cursor = [], None
    while True:
        page = assigned_input_page(managed, tmp_path, "run", "source", cursor=cursor, limit=7)
        chunks.append(page["chunk"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    payload = json.loads("".join(chunks))
    assert payload["node_result"] == run["activations"][0]["node_result"]
    assert payload["raw_output"] == run["activations"][0]["raw_output"]
    with pytest.raises(ValueError, match="own execution"):
        guard_task_read("old", managed)


def test_assigned_input_stale_revoked_attempt_and_cursor(tmp_path):
    run = fixture_run()
    managed = authorize(run, tmp_path)
    page = assigned_input_page(managed, tmp_path, "run", "source", limit=10)
    with pytest.raises(ValueError, match="not assigned"):
        assigned_input_page(managed, tmp_path, "run", "source", task_id="old")
    with pytest.raises(ValueError, match="cursor"):
        assigned_input_page(managed, tmp_path, "run", "source", cursor="bad")
    run["activations"][0]["raw_output"] += "changed"
    with pytest.raises(ValueError, match="stale"):
        assigned_input_page(managed, tmp_path, "run", "source")
    with pytest.raises(ValueError, match="cursor"):
        assigned_input_page(managed, tmp_path, "run", "source", cursor=page["next_cursor"])
    run["activations"][1]["authorized_input_refs"] = []
    with pytest.raises(ValueError, match="not assigned"):
        assigned_input_page(managed, tmp_path, "run", "source")


@pytest.mark.parametrize("status", ["running", "reserved", "uncertain"])
def test_assigned_input_never_reads_unsettled(tmp_path, status):
    run = fixture_run()
    managed = authorize(run, tmp_path)
    run["activations"][0]["status"] = status
    with pytest.raises(ValueError, match="fully settled"):
        assigned_input_page(managed, tmp_path, "run", "source")


def test_assigned_input_caller_authority(tmp_path):
    run = fixture_run()
    managed = authorize(run, tmp_path)
    for caller in [None, ({"role": "orchestrator"}, run), ({"role": "node", "activation_id": "missing"}, run)]:
        with pytest.raises(ValueError):
            assigned_input_page(caller, tmp_path, "run", "source")
    with pytest.raises(ValueError, match="own workflow"):
        assigned_input_page(managed, tmp_path, "other", "source")
    run["activations"][1]["status"] = "completed"
    with pytest.raises(ValueError, match="active worker"):
        assigned_input_page(managed, tmp_path, "run", "source")


def test_assigned_input_requires_verified_child_chain(tmp_path, monkeypatch):
    from polybridge.workflows import WorkflowStore
    run, child = fixture_run(), fixture_run()
    child.update(workflow_run_id="child", workflow_id="child-definition", parent_link={"workflow_run_id": "run", "execution_id": "source", "node_id": "before"})
    run.update(workflow_id="parent-definition", dependency_tree={"edges": [{"from": "parent-definition", "to": "child-definition", "node_id": "before"}]})
    run["activations"][0]["invocation"] = {"child_workflow_run_id": "child"}
    monkeypatch.setattr(WorkflowStore, "get_run", lambda self, ident: {"run": run, "child": child}[ident])
    managed = authorize(run, tmp_path)
    run["activations"][1]["authorized_input_refs"] = [make_assigned_input_ref(run, tmp_path, "source", source_run_id="child")]
    assert assigned_input_page(managed, tmp_path, "run", "source", source_run_id="child")["workflow_run_id"] == "child"
    run["activations"][0]["invocation"] = {}
    with pytest.raises(ValueError, match="persisted invocation"):
        assigned_input_page(managed, tmp_path, "run", "source", source_run_id="child")


@pytest.mark.asyncio
async def test_server_worker_reader_and_inspection_restriction(tmp_path, monkeypatch):
    from polybridge import server
    from polybridge.workflows import WorkflowStore
    run = fixture_run()
    managed = authorize(run, tmp_path)
    async def reader():
        return managed
    monkeypatch.setattr(server, "_managed_workflow_reader", reader)
    monkeypatch.setattr(WorkflowStore, "__init__", lambda self: setattr(self, "root", tmp_path))
    page = await server.read_workflow_assigned_input("run", "source", limit=13)
    assert len(page["chunk"]) == 13
    with pytest.raises(Exception, match="cannot inspect workflow context"):
        await server.inspect_workflow_node("run", "source")


def test_cli_assigned_input_parity(monkeypatch, capsys):
    from polybridge import ctl, server
    seen = []
    async def read(*args):
        seen.append(args)
        return {"chunk": "ok"}
    monkeypatch.setattr(server, "read_workflow_assigned_input", read)
    assert ctl.main(["workflow-assigned-input", "run", "source", "--source-run", "child", "--task-id", "attempt", "--limit", "7", "--cursor", "cursor", "--json"]) == 0
    assert seen == [("run", "source", "child", "attempt", "cursor", 7)]
    assert json.loads(capsys.readouterr().out)["result"]["chunk"] == "ok"
