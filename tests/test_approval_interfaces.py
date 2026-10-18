"""Public MCP approval interface contract tests."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp.shared.exceptions import MCPError

from polybridge import approvals, server, store
from polybridge.backends import get


def _row(directory: Path) -> dict:
    store.write(directory, store.TaskRecord(
        task_id="task-1", backend="claude", session_id="session-1", repo_path=str(directory),
        started_at="now", status="running", approval_generation="generation-1",
        start_time="task-start", owner={"pid": 5678, "start_time": "bridge-start", "markers": []},
    ))
    task = SimpleNamespace(
        task_id="task-1", proc=SimpleNamespace(pid=1234), start_time="task-start",
        markers=("task-marker",), owner={"pid": 5678, "start_time": "bridge-start", "markers": []},
        approval_generation="generation-1", session_id="session-1", repo_path=directory,
        freedom="write_in_repo",
    )
    return approvals.admit(directory, task, {
        "native_request_id": "native-1", "tool_use_id": "tool-1",
        "action": {"tool_name": "Bash", "input": {"command": "printf exact"}},
    }, {"interaction_owner": "caller", "workflow_run_id": None, "root_workflow_run_id": None,
        "workflow_node_id": None, "workflow_execution_id": None})


class Registry:
    def __init__(self, directory: Path):
        self.log_dir = directory
        self.task = SimpleNamespace(pump_wake=SimpleNamespace(set=lambda: None))

    async def _mutation_caller(self):
        return None

    def get(self, _task_id):
        return self.task


@pytest.mark.asyncio
async def test_public_list_get_respond_roundtrip_and_strict_revision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    row = _row(tmp_path)
    registry = Registry(tmp_path)
    monkeypatch.setattr(server, "_registry", registry)
    monkeypatch.setattr("polybridge.approval_runtime.identity.identity_check", lambda _value: "alive")

    listed = await server.list_approval_requests(task_id="task-1", limit=16)
    assert listed["requests"][0]["request_id"] == row["request_id"]
    assert "action" not in listed["requests"][0]
    full = await server.get_approval_request("task-1", row["request_id"])
    assert full["action"] == row["action"]
    answered = await server.respond_approval_request(
        "task-1", row["request_id"], full["action_digest"], full["owner_revision"], "reject"
    )
    assert answered["decision"] == "reject"
    with pytest.raises(MCPError, match="integer"):
        await server.respond_approval_request(
            "task-1", row["request_id"], full["action_digest"], True, "approve"
        )


def test_claude_caller_approval_opt_in_requires_supported_versioned_backend() -> None:
    backend = get("claude")
    assert backend.capabilities.supports_caller_approvals is True
