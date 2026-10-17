"""A2: ctl workflow node reads share the server's checks without importing the server.

`workflow_node_reads` is the single body behind both the MCP tools (`inspect_workflow_node`,
`read_workflow_assigned_input`) and `polybridge-ctl workflow inspect` / `assigned-input`, so the
two surfaces cannot drift; ctl resolves managed authority itself and never imports
`polybridge.server`, never starts retention and never starts the notification worker.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from mcp import MCPError

from polybridge import ctl, lineage, server
from polybridge.tasks import TaskRegistry


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


def _caller(task_id: str = "owned-caller") -> SimpleNamespace:
    return SimpleNamespace(record=SimpleNamespace(task_id=task_id))


def _owned_run(tmp_path: Path, role: str):
    """A real durable run whose `owned-caller` task sits in a `role` activation."""
    from polybridge import workflows

    storage = workflows.WorkflowStore()
    definition = storage.save(
        "owned-graph",
        {
            "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
            "connections": [{"id": "finish", "source": "start", "target": "end"}],
        },
    )
    run = storage.create_run(definition, "synthetic assignment", tmp_path)
    run_id = run["workflow_run_id"]

    def reserve(value):
        value.update(
            status="running",
            execution_contract="delegation",
            activations=[
                {
                    "id": "owned-activation",
                    "node_id": "owned-node",
                    "role": role,
                    "status": "completed",
                    "node_result": {"status": "succeeded", "summary": "done"},
                    "raw_output": "raw output",
                    "tasks": [{"task_id": "owned-caller", "status": "completed", "result": {"summary": "attempt"}}],
                },
                # A settled node execution an orchestrator may inspect (inspection is node-only).
                {
                    "id": "settled-node",
                    "node_id": "earlier",
                    "role": "node",
                    "status": "completed",
                    "node_result": {"status": "succeeded", "summary": "done"},
                    "raw_output": "raw output",
                    "tasks": [{"task_id": "earlier-task", "status": "completed", "result": {"summary": "attempt"}}],
                },
            ],
        )

    storage.update_run(run_id, reserve, "synthetic_authority")
    return storage, run_id


def _use_server_registry(monkeypatch: pytest.MonkeyPatch, directory: Path) -> None:
    """The MCP tools read through `_reg()`; point it at the same durable root as ctl."""
    monkeypatch.setattr(server, "_registry", TaskRegistry(log_dir=directory))


def _ctl_error(capsys) -> dict:
    return json.loads(capsys.readouterr().out)["error"]


def _run_ctl_subprocess(home: Path, argv: list[str]) -> dict:
    code = (
        "import json, sys\n"
        "from polybridge import ctl\n"
        "code = ctl.main(sys.argv[1:])\n"
        "print(json.dumps({'code': code, 'server_imported': 'polybridge.server' in sys.modules,"
        " 'mcp_imported': 'mcp' in sys.modules}))\n"
    )
    env = {key: value for key, value in os.environ.items() if key != "PB_TASK_ID"}
    env["HOME"] = str(home)
    result = subprocess.run(
        [sys.executable, "-c", code, *argv], capture_output=True, text=True, env=env, timeout=60
    )
    return json.loads(result.stdout.splitlines()[-1])


def test_cli_node_reads_never_import_the_server_module(home: Path, tmp_path: Path):
    """Refused reads too: importing `server` is the thing being refused, whatever the outcome."""
    storage, run_id = _owned_run(tmp_path, "orchestrator")
    inspected = _run_ctl_subprocess(
        home, ["workflow-inspect", run_id, "settled-node", "--json"]
    )
    assert inspected["code"] == 0
    assert inspected["server_imported"] is False
    # The happy path stays free of the MCP import too — that cost belongs to the server module.
    assert inspected["mcp_imported"] is False
    refused = _run_ctl_subprocess(home, ["workflow-assigned-input", run_id, "no-such", "--json"])
    assert refused["code"] == 1
    assert refused["server_imported"] is False


def test_install_cli_registry_is_idempotent_and_side_effect_free(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "_registry", None)
    installed = server.install_cli_registry(tmp_path / "tasks")
    assert installed.log_dir == tmp_path / "tasks"
    assert installed._maintenance is None
    assert getattr(installed, "_notification_worker", None) is None
    assert server.install_cli_registry() is installed
    injected = TaskRegistry(log_dir=tmp_path / "injected")
    monkeypatch.setattr(server, "_registry", injected)
    assert server.install_cli_registry() is injected


def test_workflow_and_permission_commands_never_start_maintenance_or_retention(
    home, monkeypatch, capsys, fake_backend_clis
):
    from polybridge import retention

    started = []
    monkeypatch.setattr(TaskRegistry, "start_maintenance", lambda self: started.append("maintenance"))
    monkeypatch.setattr(retention, "maybe_sweep", lambda *args, **kwargs: started.append("sweep"))
    monkeypatch.setattr(server, "_registry", None)

    async def preview(self, repo_path, **kwargs):
        return {"preview": True}

    async def validate(path):
        return path

    monkeypatch.setattr(TaskRegistry, "preview_permissions", preview)
    monkeypatch.setattr(server, "_validate_repo_path", validate)

    assert ctl.main(["workflow-inspect", "no-such-run", "no-such-execution", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "workflow_error"
    assert ctl.main(["workflow-delete", "no-such-workflow", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "workflow_error"
    assert ctl.main(["permission-preview", "--repo", str(home), "--backend", "claude", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["result"] == {"preview": True}

    assert started == []
    # The registry ctl installed carries neither side effect.
    assert server._registry is not None
    assert server._registry._maintenance is None
    assert getattr(server._registry, "_notification_worker", None) is None


@pytest.mark.parametrize("role", ["builder", "orchestrator", "node"])
async def test_ctl_and_mcp_inspect_agree_per_role(home, monkeypatch, tmp_path, capsys, role):
    storage, run_id = _owned_run(tmp_path, role)
    directory = storage.root / "tasks"
    _use_server_registry(monkeypatch, directory)
    monkeypatch.setattr(lineage, "detect_caller", lambda *args, **kwargs: _caller())

    if role == "orchestrator":
        payload = await server.inspect_workflow_node(run_id, "settled-node")
        assert await asyncio.to_thread(ctl.main, ["workflow-inspect", run_id, "settled-node", "--json"]) == 0
        assert json.loads(capsys.readouterr().out)["result"] == payload
        # An orchestrator may not inspect a run it does not own — identically refused.
        with pytest.raises(MCPError) as foreign:
            await server.inspect_workflow_node("someone-elses-run", "owned-activation")
        assert await asyncio.to_thread(
            ctl.main, ["workflow-inspect", "someone-elses-run", "owned-activation", "--json"]
        ) == 1
        assert _ctl_error(capsys)["message"] == str(foreign.value)
        assert "Orchestrators may only inspect their own workflow run" in str(foreign.value)
        return

    with pytest.raises(MCPError) as refusal:
        await server.inspect_workflow_node(run_id, "owned-activation")
    message = (
        "Builders may only inspect their own draft and builder run"
        if role == "builder"
        else "Worker nodes cannot inspect workflow context"
    )
    assert message in str(refusal.value)
    assert await asyncio.to_thread(ctl.main, ["workflow-inspect", run_id, "owned-activation", "--json"]) == 1
    assert _ctl_error(capsys)["message"] == str(refusal.value)


async def test_ctl_and_mcp_assigned_input_agree(home, monkeypatch, tmp_path, capsys):
    from polybridge import workflows
    from polybridge.workflow_inspection import make_assigned_input_ref

    storage = workflows.WorkflowStore()
    definition = storage.save(
        "worker-graph",
        {
            "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
            "connections": [{"id": "finish", "source": "start", "target": "end"}],
        },
    )
    run = storage.create_run(definition, "synthetic assignment", tmp_path)
    run_id = run["workflow_run_id"]

    def reserve(value):
        value.update(
            status="running",
            execution_contract="delegation",
            activations=[
                {
                    "id": "source",
                    "node_id": "before",
                    "role": "node",
                    "status": "completed",
                    "node_result": {"status": "succeeded", "summary": "input"},
                    "raw_output": "the assignment",
                    "tasks": [{"task_id": "old", "status": "completed", "result": {"summary": "attempt"}}],
                },
                {
                    "id": "worker",
                    "node_id": "now",
                    "role": "node",
                    "status": "running",
                    "tasks": [{"task_id": "owned-caller", "status": "running"}],
                },
            ],
        )

    run = storage.update_run(run_id, reserve, "synthetic_authority")
    reference = make_assigned_input_ref(run, storage.root, "source")

    def authorize(value):
        value["activations"][1]["authorized_input_refs"] = [reference]

    storage.update_run(run_id, authorize, "synthetic_authority")

    directory = storage.root / "tasks"
    _use_server_registry(monkeypatch, directory)
    monkeypatch.setattr(lineage, "detect_caller", lambda *args, **kwargs: _caller())

    payload = await server.read_workflow_assigned_input(run_id, "source", limit=13)
    assert await asyncio.to_thread(ctl.main, ["workflow-assigned-input", run_id, "source", "--limit", "13", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["result"] == payload
    assert len(payload["chunk"]) == 13
