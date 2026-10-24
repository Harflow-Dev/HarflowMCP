"""Opt-in: Read, Write and Edit caller approvals against the installed Claude and a localhost fake API."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from claude_cli_support import isolate, require_claude
from fake_anthropic_api import fake_anthropic_api
from harflow_mcp import approval_runtime, approvals
from harflow_mcp.backends import get
from harflow_mcp.tasks import TaskRegistry

import os

pytestmark = [
    pytest.mark.cli_integration,
    pytest.mark.skipif(not os.environ.get("HARFLOW_CLI_INTEGRATION"), reason="opt in with HARFLOW_CLI_INTEGRATION=1"),
]


async def _next_request(registry: TaskRegistry, task_id: str, seen: list[str]) -> dict:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        for row in approvals.rows(registry.log_dir, task_id):
            if row["request_id"] not in seen and row["disposition"] == "pending" and row["delivery"] == "pending":
                return row
        await asyncio.sleep(0.05)
    raise AssertionError("Claude did not emit a caller approval request")


async def _run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, steps: list[dict], decisions: list[str], *, write: bool = True, prepare=None):
    require_claude("caller_approval_tools", monkeypatch)
    repo = tmp_path / "repo"
    (repo / "trailing ").mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    if prepare:
        prepare(repo, outside)
    with fake_anthropic_api(steps=[{**step, "input": {k: (str(v).replace("{repo}", str(repo)).replace("{outside}", str(outside)) if isinstance(v, str) else v) for k, v in step["input"].items()}} for step in steps]) as (base_url, _requests):
        isolate(monkeypatch, tmp_path, base_url)
        registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks", open_monitor=False)
        task = await registry.start("run the scripted actions", repo, backend=get("claude"), freedom="write_in_repo", caller_approvals=True, caller_write_approvals=write)
        seen: list[str] = []
        shown = []
        try:
            for decision in decisions:
                row = await _next_request(registry, task.task_id, seen)
                full = await approval_runtime.get_request(registry, task.task_id, row["request_id"])
                shown.append(full)
                await approval_runtime.respond_request(registry, task.task_id, row["request_id"], full["action_digest"], full["owner_revision"], decision)
                seen.append(row["request_id"])
            await asyncio.wait_for(task.done.wait(), timeout=30)
        finally:
            if not task.done.is_set():
                await registry.cancel(task.task_id)
        return repo, outside, registry, task, shown


def _rows(registry, task) -> list[dict]:
    return approvals.rows(registry.log_dir, task.task_id)


@pytest.mark.parametrize("decision", ["approve", "reject"])
@pytest.mark.asyncio
async def test_path_safety_read(tmp_path, monkeypatch, decision):
    def prepare(repo, _outside):
        (repo / "trailing " / "file.txt").write_text("hello")
    steps = [{"name": "Read", "id": "r1", "input": {"file_path": "{repo}/trailing /file.txt"}}]
    _repo, _out, registry, task, shown = await _run(tmp_path, monkeypatch, steps, [decision], prepare=prepare)
    assert shown[0]["tool_name"] == "Read" and shown[0]["decision_reason_type"] == "other"
    assert _rows(registry, task)[0]["execution_outcome"] == ("succeeded" if decision == "approve" else "failed")


@pytest.mark.parametrize("decision", ["approve", "reject"])
@pytest.mark.asyncio
async def test_out_of_cwd_write(tmp_path, monkeypatch, decision):
    steps = [{"name": "Write", "id": "w1", "input": {"file_path": "{outside}/x.txt", "content": "written"}}]
    _repo, outside, registry, task, shown = await _run(tmp_path, monkeypatch, steps, [decision])
    assert shown[0]["decision_reason_type"] == "workingDir" and shown[0]["inside_repository"] is False
    assert (outside / "x.txt").exists() is (decision == "approve")
    writes = task.snapshot()["caller_approved_writes"]
    assert [w["write_possible"] for w in writes] == ([True] if decision == "approve" else [])
    assert "written" not in str(writes)


@pytest.mark.parametrize("decision", ["approve", "reject"])
@pytest.mark.asyncio
async def test_out_of_cwd_edit_after_an_approved_read(tmp_path, monkeypatch, decision):
    def prepare(_repo, outside):
        (outside / "e.txt").write_text("before")
    steps = [{"name": "Read", "id": "r1", "input": {"file_path": "{outside}/e.txt"}},
             {"name": "Edit", "id": "e1", "input": {"file_path": "{outside}/e.txt", "old_string": "before", "new_string": "after"}}]
    _repo, outside, _registry, _task, shown = await _run(tmp_path, monkeypatch, steps, ["approve", decision], prepare=prepare)
    assert [s["tool_name"] for s in shown] == ["Read", "Edit"]
    assert (outside / "e.txt").read_text() == ("after" if decision == "approve" else "before")


@pytest.mark.asyncio
async def test_path_safety_write_and_edit(tmp_path, monkeypatch):
    def prepare(repo, _outside):
        (repo / "trailing " / "e.txt").write_text("before")
    steps = [{"name": "Write", "id": "w1", "input": {"file_path": "{repo}/trailing /new.txt", "content": "n"}},
             {"name": "Read", "id": "r1", "input": {"file_path": "{repo}/trailing /e.txt"}},
             {"name": "Edit", "id": "e1", "input": {"file_path": "{repo}/trailing /e.txt", "old_string": "before", "new_string": "after"}}]
    repo, _out, _registry, _task, shown = await _run(tmp_path, monkeypatch, steps, ["approve", "approve", "approve"], prepare=prepare)
    assert shown[0]["decision_reason_type"] == "safetyCheck"
    assert (repo / "trailing " / "new.txt").read_text() == "n" and (repo / "trailing " / "e.txt").read_text() == "after"


@pytest.mark.asyncio
async def test_second_write_to_the_same_directory_prompts_again(tmp_path, monkeypatch):
    steps = [{"name": "Write", "id": "w1", "input": {"file_path": "{outside}/a.txt", "content": "a"}},
             {"name": "Write", "id": "w2", "input": {"file_path": "{outside}/b.txt", "content": "b"}}]
    _repo, outside, _registry, _task, shown = await _run(tmp_path, monkeypatch, steps, ["approve", "reject"])
    assert len(shown) == 2 and (outside / "a.txt").exists() and not (outside / "b.txt").exists()


@pytest.mark.asyncio
async def test_write_without_the_opt_in_is_a_delivered_system_rejection_and_the_run_continues(tmp_path, monkeypatch):
    steps = [{"name": "Write", "id": "w1", "input": {"file_path": "{outside}/x.txt", "content": "w"}}]
    _repo, outside, registry, task, _shown = await _run(tmp_path, monkeypatch, steps, [], write=False)
    (row,) = _rows(registry, task)
    assert (row["system_rejection"], row["decided_by"], row["effective_decision"], row["delivered_decision"], row["delivery"], row["disposition"]) == \
        ("write_approvals_not_enabled", "harflow", "reject", "reject", "sent", "system_rejected")
    assert not (outside / "x.txt").exists() and task.status == "completed"
    assert task.snapshot()["denial_categories"] == ["harflow_policy_rejected"]


@pytest.mark.asyncio
async def test_protected_path_is_refused_even_with_the_opt_in(tmp_path, monkeypatch):
    steps = [{"name": "Write", "id": "w1", "input": {"file_path": "{repo}/.claude/settings.json", "content": "{}"}}]
    repo, _out, registry, task, _shown = await _run(tmp_path, monkeypatch, steps, [])
    (row,) = _rows(registry, task)
    assert row["system_rejection"] == "protected_path" and row["delivered_decision"] == "reject"
    assert not (repo / ".claude" / "settings.json").exists()
