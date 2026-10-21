"""Receipt refusal behavior at server and display boundaries."""
from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path

import pytest
from mcp import MCPError

from harflow_mcp import artifacts, ctl, server, state_ownership, store, workflows
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.workflow_receipt_documents import WorkflowReceiptDocumentError


def _receipt_error(run_id: str = "run") -> WorkflowReceiptDocumentError:
    return WorkflowReceiptDocumentError(
        "attempt", "receipt source changed before replacement", workflow_run_id=run_id
    )


async def test_start_task_preserves_receipt_refusal_through_entrypoint(
    monkeypatch, git_repo, fake_backend_clis
):
    async def refuse(*args, **kwargs):
        raise _receipt_error()

    monkeypatch.setattr(server._reg(), "start", refuse)
    with pytest.raises(MCPError) as caught:
        await server.start_task("run", str(git_repo), backend="codex")
    assert caught.value.code == -32602
    assert caught.value.error.data["code"] == "workflow_receipt_incompatible"
    assert "launch" not in str(caught.value).lower()


async def test_start_task_notify_reservation_preserves_typed_receipt_refusal(
    monkeypatch, git_repo, fake_backend_clis
):
    monkeypatch.setenv("HARFLOW_NOTIFY_DESTINATIONS", json.dumps({"hook": {"url": "https://example.com"}}))
    monkeypatch.setattr(server, "_start_notification_worker", lambda _: None)

    def refuse(*args, **kwargs):
        raise _receipt_error()

    monkeypatch.setattr("harflow_mcp.notifications.subscribe", refuse)
    with pytest.raises(MCPError) as caught:
        await server.start_task("run", str(git_repo), backend="codex", notify="hook")
    assert caught.value.error.data["code"] == "workflow_receipt_incompatible"
    assert "notification reservation failed" not in str(caught.value).lower()


async def test_start_task_post_reservation_receipt_refusal_preserves_subscription(
    monkeypatch, git_repo, fake_backend_clis
):
    monkeypatch.setenv("HARFLOW_NOTIFY_DESTINATIONS", json.dumps({"hook": {"url": "https://example.com"}}))
    monkeypatch.setattr(server, "_start_notification_worker", lambda _: None)
    calls = []

    def reserve(directory, task_id, destination, **kwargs):
        calls.append(("subscribe", task_id))
        return {"task_id": task_id, "destination": destination, "state": "pending"}

    def discard(*args, **kwargs):
        calls.append(("discard", args[1] if len(args) > 1 else None))
        raise AssertionError("typed post-reservation refusal must preserve the reservation")

    monkeypatch.setattr("harflow_mcp.notifications.subscribe", reserve)
    monkeypatch.setattr("harflow_mcp.notifications.discard_unstarted", discard)

    async def refuse(*args, **kwargs):
        raise _receipt_error(kwargs.get("task_id", "task"))

    monkeypatch.setattr(server._reg(), "start", refuse)
    with pytest.raises(MCPError) as caught:
        await server.start_task("run", str(git_repo), backend="codex", notify="hook")
    assert caught.value.error.data["code"] == "workflow_receipt_incompatible"
    assert calls and calls[0][0] == "subscribe"
    assert all(call[0] != "discard" for call in calls)


async def test_start_workflow_preserves_receipt_refusal_without_launch_notice(monkeypatch, tmp_path):
    async def refuse(*args, **kwargs):
        raise _receipt_error(kwargs.get("run_id", "run"))

    monkeypatch.setattr(server, "_workflow_call", refuse)
    with pytest.raises(MCPError) as caught:
        await server.start_workflow("saved", "run", str(tmp_path))
    assert caught.value.error.data["code"] == "workflow_receipt_incompatible"
    assert "launch did not return normally" not in str(caught.value).lower()


async def test_start_workflow_notify_reservation_preserves_typed_receipt_refusal(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("HARFLOW_NOTIFY_DESTINATIONS", json.dumps({"hook": {"url": "https://example.com"}}))
    monkeypatch.setattr(server, "_start_notification_worker", lambda _: None)

    def refuse(*args, **kwargs):
        raise _receipt_error()

    monkeypatch.setattr("harflow_mcp.workflow_notifications.subscribe", refuse)
    with pytest.raises(MCPError) as caught:
        await server.start_workflow("saved", "run", str(tmp_path), notify="hook")
    assert caught.value.error.data["code"] == "workflow_receipt_incompatible"
    assert "notification reservation failed" not in str(caught.value).lower()


async def test_start_workflow_post_reservation_receipt_refusal_keeps_created_run_subscription(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("HARFLOW_NOTIFY_DESTINATIONS", json.dumps({"hook": {"url": "https://example.com"}}))
    monkeypatch.setattr(server, "_start_notification_worker", lambda _: None)
    calls = []

    def reserve(directory, run_id, destination, events=None, **kwargs):
        calls.append(("subscribe", run_id))
        return {"workflow_run_id": run_id, "destination": destination, "state": "pending"}

    def discard(*args, **kwargs):
        calls.append(("discard", args[1] if len(args) > 1 else None))
        raise AssertionError("typed post-reservation refusal must preserve the run subscription")

    monkeypatch.setattr("harflow_mcp.workflow_notifications.subscribe", reserve)
    monkeypatch.setattr("harflow_mcp.workflow_notifications.discard_unstarted", discard)
    created = {}

    async def refuse(*args, **kwargs):
        created["run_id"] = kwargs["run_id"]
        raise _receipt_error(kwargs["run_id"])

    monkeypatch.setattr(server, "_workflow_call", refuse)

    class ExistingRun:
        def __init__(self, *args, **kwargs):
            pass

        def get_run(self, run_id, **kwargs):
            return {"workflow_run_id": run_id, "name": "saved", "kind": "workflow", "status": "needs_attention", "sequence": 1, "definition": {}, "activations": [], "created_at": 1, "updated_at": 1}

    monkeypatch.setattr("harflow_mcp.workflows.WorkflowStore", ExistingRun)
    with pytest.raises(MCPError) as caught:
        await server.start_workflow("saved", "run", str(tmp_path), notify="hook")
    assert caught.value.error.data["code"] == "workflow_receipt_incompatible"
    assert "launch did not return normally" not in str(caught.value).lower()
    assert created["run_id"] == calls[0][1]
    assert all(call[0] != "discard" for call in calls)


def _record(task_id: str, *, parent_task_id: str | None = None):
    return store.TaskRecord(
        task_id=task_id,
        backend="codex",
        session_id="session",
        repo_path="/repo",
        started_at="2026-10-01T00:00:00+00:00",
        status="running",
        parent_task_id=parent_task_id,
    )


def _seed_workflow_task(tmp_path: Path):
    storage = workflows.WorkflowStore(tmp_path / "state")
    definition = workflows.validate_definition(
        {
            "name": "receipt",
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "work", "type": "agent", "agent": {"backend": "codex"}},
                {"id": "end", "type": "end"},
            ],
            "connections": [
                {"id": "a", "source": "start", "target": "work"},
                {"id": "b", "source": "work", "target": "end"},
            ],
            "orchestrator": {"backend": "codex"},
        }
    )
    run = storage.create_run(definition, "run", tmp_path)
    task = _record("worker")
    storage.update_run(
        run["workflow_run_id"],
        lambda value: value.update(
            status="running",
            activations=[
                {
                    "id": "execution",
                    "node_id": "node",
                    "role": "agent",
                    "status": "running",
                    "created_at": 0.0,
                    "tasks": [{"task_id": task.task_id, "status": "running"}],
                }
            ],
        ),
        "seed",
    )
    store.write(storage.root / "tasks", task)
    return storage, task


def test_list_page_and_related_headers_diagnose_existing_receipt_refusal(tmp_path):
    storage, task = _seed_workflow_task(tmp_path)
    receipt = storage.owners / f"{task.task_id}.json"
    receipt.write_text("{broken")

    page = store.list_page(storage.root / "tasks", task_ids=[task.task_id])
    item = page["items"][0]
    assert item["workflow_diagnostic"]["code"] == "workflow_receipt_incompatible"
    assert page["ownership_incomplete"] is True

    # A descendant projection uses the same refusal contract and must not silently
    # turn a malformed receipt into an unmanaged task.
    child = replace(task, task_id="child", parent_task_id=task.task_id)
    store.write(storage.root / "tasks", child)
    related = store.list_page(storage.root / "tasks", task_ids=[child.task_id])["related_headers"]
    assert related[0]["workflow_diagnostic"]["code"] == "workflow_receipt_incompatible"


def test_artifact_owner_keeps_existing_malformed_receipt_managed(tmp_path):
    storage, task = _seed_workflow_task(tmp_path)
    receipt = storage.owners / f"{task.task_id}.json"
    receipt.write_text(json.dumps({"product": "other", "format": "other"}))
    with state_ownership.state_session(resolve_layout(root=storage.root)):
        owner = artifacts._workflow_owner(storage.root / "tasks", task.task_id)
    assert owner == {"managed_or_unverifiable": True}


def test_ctl_main_serializes_typed_receipt_refusal_without_generic_read_error(
    tmp_path, monkeypatch, capsys
):
    root = tmp_path / "state"
    log_dir = root / "tasks"
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))
    with state_ownership.state_session(resolve_layout(root=root)):
        store.write(log_dir, _record("guarded"))

    def refuse(_log_dir):
        raise WorkflowReceiptDocumentError(
            "guarded", "receipt source changed before replacement", workflow_run_id="run"
        )

    from harflow_mcp import workflow_inspection

    monkeypatch.setattr(workflow_inspection, "managed_reader", refuse)
    assert ctl.main(["status", "guarded", "--json"]) == 1
    document = json.loads(capsys.readouterr().out)
    assert document["error"]["code"] == "workflow_receipt_incompatible"
    assert document["error"]["data"] == {
        "task_id": "guarded",
        "code": "workflow_receipt_incompatible",
        "reason": "receipt source changed before replacement",
        "workflow_run_id": "run",
    }

