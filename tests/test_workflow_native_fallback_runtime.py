"""Native execution preserves candidate order and actual nested session ownership."""
import asyncio
import json
from dataclasses import replace

import pytest

from polybridge import store, workflows as w
from polybridge.workflow_native_policy import orchestrator_contract
from test_workflow_delegation import Task
from test_workflow_native import NativeRegistry, MODEL, native_events, native_graph
from test_workflow_run_node_execution import parent_graph, run_tree, child_runs, workflow_policy


class RuntimeRegistry(NativeRegistry):
    async def resume(self, previous, prompt, **kwargs):
        if not kwargs.get("native_subagent"):
            return await super().resume(previous, prompt, **kwargs)
        self.native_calls.append(kwargs)
        arguments = json.loads(prompt.split("Agent arguments:\n", 1)[1])
        record = store.read(self._log_dir, previous.task_id)
        events = native_events(arguments["description"], arguments["prompt"], record.session_id)
        events[0]["permissionMode"] = "acceptEdits" if record.freedom == "write_in_repo" else "plan"
        events[2]["task_id"] = "child-" + arguments["description"]
        events[-1]["tool_use_result"]["agentId"] = events[2]["task_id"]
        for event in events:
            self._workflow_native_observers[kwargs["task_id"]](event)
        task = Task(kwargs["task_id"], {"summary": "Parent transport finished", "backend": "claude", "session_id": record.session_id})
        task.kwargs, task.repo = previous.kwargs, previous.repo
        self.tasks[task.task_id] = task
        store.write(self._log_dir, replace(record, task_id=task.task_id))
        return task


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w.backends, "version", lambda backend: "2.1.295 (Claude Code)")
    storage = w.WorkflowStore(tmp_path)
    return storage, RuntimeRegistry(tmp_path)


def fallback_graph():
    definition = native_graph()
    definition["nodes"][1]["freedom"] = "write_in_repo"
    definition["nodes"][1]["agent"] = {"backend": "opencode", "fallbacks": [{"backend": "claude", "model": MODEL}]}
    return w.validate_definition(definition)


async def test_healthy_mismatched_primary_is_headless_despite_native_fallback_plan(setup, tmp_path):
    storage, registry = setup
    run = storage.create_run(fallback_graph(), "Review", tmp_path)
    contract = orchestrator_contract(run)
    assert contract["freedom"] == "write_in_repo"
    assert contract["contributing_nodes"][0]["candidate_position"] == 1
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed", observed.get("attention_reason")
    worker = next(a for a in observed["activations"] if a["role"] == "node")
    assert worker["tasks"][0]["candidate"]["backend"] == "opencode"
    assert worker["execution_kind"] == "headless"
    assert registry.native_calls == []


async def test_unavailable_primary_reaches_matching_native_fallback(setup, tmp_path, monkeypatch):
    storage, registry = setup
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: backend.name != "opencode")
    run = storage.create_run(fallback_graph(), "Review", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed", observed.get("attention_reason")
    worker = next(a for a in observed["activations"] if a["role"] == "node")
    assert worker["tasks"][0]["candidate"]["backend"] == "claude"
    assert worker["execution_kind"] == "native_subagent"
    assert len(registry.native_calls) == 1
    assert all(kwargs["backend"].name != "opencode" for _, kwargs in registry.calls)


@pytest.mark.parametrize("mode", ["current", "child"])
async def test_nested_native_worker_uses_correct_owner_permission_contract(setup, tmp_path, mode):
    storage, registry = setup
    registry.policy = workflow_policy
    child = native_graph()
    child["name"] = "child"
    child["nodes"][1]["freedom"] = "write_in_repo"
    saved_child = storage.save("child", child)
    parent = parent_graph(saved_child["workflow_id"], mode=mode)
    parent["orchestrator"] = {"backend": "claude", "model": MODEL}
    storage.save("parent", parent)
    run, _ = await run_tree(storage, tmp_path, "parent", registry)
    assert run["status"] == "completed", run.get("attention_reason")
    child_run = child_runs(storage, run)[0]
    worker = next(a for a in child_run["activations"] if a["role"] == "node")
    assert worker["execution_kind"] == "native_subagent"
    assert worker["tasks"][0]["freedom"] == "write_in_repo"
    expected_root_access = "write_in_repo" if mode == "current" else "read_only"
    assert orchestrator_contract(run)["freedom"] == expected_root_access
    root_owner = store.read(storage.root / "tasks", run["sessions"]["orchestrator"]["task_id"])
    assert root_owner.freedom == expected_root_access
    if mode == "current":
        assert child_run["orchestrator_session_owner_run_id"] == run["workflow_run_id"]
        assert worker["tasks"][0]["owner_session_id"] == root_owner.session_id
    else:
        child_owner = store.read(storage.root / "tasks", child_run["sessions"]["orchestrator"]["task_id"])
        assert child_owner.freedom == "write_in_repo"
        assert child_owner.session_id != root_owner.session_id
        assert worker["tasks"][0]["owner_session_id"] == child_owner.session_id
