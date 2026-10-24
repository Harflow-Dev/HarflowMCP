"""Fault-injection coverage for durable task and workflow checkpoints."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import replace
import json
from pathlib import Path

import pytest

from harflow_mcp import store, task_supervision, workflows as w
from harflow_mcp.backends import get
from harflow_mcp.tasks import SessionBusyError, TaskRegistry

from test_workflow_delegation import Registry, default_decision, graph, run_flow


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda _backend: True)
    return w.WorkflowStore(tmp_path / "workflow-state")


def _log_dir(root: Path) -> Path:
    return root / "state" / "tasks"


@pytest.mark.asyncio
async def test_reserved_successor_session_blocks_competing_resume_before_spawn(tmp_path, monkeypatch):
    log_dir = _log_dir(tmp_path)
    first = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    second = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    reservation_id = "a" * 32
    source_id = "b" * 32
    failure = OSError("fault before owner metadata")
    failure.harflow_not_started = True
    calls = 0

    def failing_popen(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise failure

    monkeypatch.setattr(task_supervision.subprocess, "Popen", failing_popen)
    with pytest.raises(OSError, match="fault before owner metadata"):
        await first.start("reply source", tmp_path, backend=get("claude"), freedom="read_only", task_id=reservation_id)
    launch = task_supervision.read_launch(log_dir, reservation_id)
    assert launch is not None and launch["state"] == "not_started"
    session_id = launch["session_id"]
    task_supervision.mark(log_dir, reservation_id, launch["generation"], state="reserved")

    source = store.TaskRecord(
        task_id=source_id,
        backend="claude",
        session_id=session_id,
        repo_path=str(tmp_path),
        started_at="2026-10-24T00:00:00+00:00",
        status="completed",
        exit_code=0,
        output_drained=True,
        finished_at="2026-10-24T00:01:00+00:00",
    )
    store.write_landed(log_dir, source)
    assert reservation_id in store.live_session_task_ids(log_dir, session_id)
    assert session_id in store.live_session_ids(log_dir)

    with pytest.raises(SessionBusyError, match="already has a running task"):
        await second.resume_record(source, "reply competing", task_id="c" * 32)
    assert calls == 1
    assert task_supervision.read_launch(log_dir, reservation_id)["state"] == "reserved"


@pytest.mark.asyncio
async def test_settled_decision_result_reenters_same_activation_without_second_harness_call(storage, tmp_path, monkeypatch):
    calls = []

    class CountingRegistry(Registry):
        async def start(self, prompt, repo, **kwargs):
            calls.append((prompt, copy.deepcopy(kwargs)))
            return await super().start(prompt, repo, **kwargs)

    run = storage.create_run(w.validate_definition(graph()), "Request", tmp_path)
    registry = CountingRegistry(storage.root)
    original = storage.update_run
    failed = False

    def crash_before_decision_acceptance(identifier, change, event, detail=None):
        nonlocal failed
        if event == "decision_accepted" and not failed:
            failed = True
            raise OSError("crash before decision acceptance")
        return original(identifier, change, event, detail)

    monkeypatch.setattr(storage, "update_run", crash_before_decision_acceptance)
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert failed
    interrupted = storage.get_run(run["workflow_run_id"])
    retained = interrupted["activations"][0]
    assert retained["status"] == "running"
    assert retained["tasks"][0]["status"] == "completed"
    first_activation_id = retained["id"]
    first_attempt_count = interrupted["pending"][0]["decision_attempts"]

    monkeypatch.setattr(storage, "update_run", original)
    storage.update_run(run["workflow_run_id"], lambda current: current.update(status="running", supervisor_pid=None, supervisor_identity=None), "simulated_restart")
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    completed = storage.get_run(run["workflow_run_id"])
    assert completed["status"] == "completed"
    assert completed["activations"][0]["id"] == first_activation_id
    assert completed["pending"] == []
    # One Start decision and one End decision are expected; replaying the
    # retained Start result must not add a third harness turn.
    assert len([entry for entry in calls if "Decision" in entry[1]["title"]]) == 2
    assert len([entry for entry in calls if entry[1]["title"].endswith(" · work")]) == 1
    assert first_attempt_count == 1


@pytest.mark.asyncio
async def test_question_control_acceptance_replays_once_with_same_question_checkpoint(storage, tmp_path, monkeypatch):
    clarification_calls = []
    worker_calls = 0

    def policy(context, registry):
        if context["current_stage"]["phase"] == "clarification":
            clarification_calls.append(context["worker_question"].copy())
            question = context["worker_question"]
            return {"decision_id": context["decision_id"], "action": "answer", "question_id": question["question_id"], "answer": "Use JSON", "reason": "Confirmed"}
        return default_decision(context, registry)

    def worker(_prompt, _kwargs):
        nonlocal worker_calls
        worker_calls += 1
        if worker_calls == 1:
            return {"status": "asking", "result": {"question": "Which format?", "progress": "Inspected"}, "evidence": ["Observed"]}
        return {"status": "succeeded", "result": {"summary": "Done"}, "evidence": ["Observed"]}

    class FaultyRegistry(Registry):
        async def start(self, prompt, repo, **kwargs):
            return await super().start(prompt, repo, **kwargs)

    registry = FaultyRegistry(storage.root, policy, {"work": worker})
    original = storage.update_run
    failed = False

    def crash_before_question_acceptance(identifier, change, event, detail=None):
        nonlocal failed
        if event == "question_decision_accepted" and not failed:
            failed = True
            raise OSError("crash before question acceptance")
        return original(identifier, change, event, detail)

    monkeypatch.setattr(storage, "update_run", crash_before_question_acceptance)
    await run_flow(storage, tmp_path, registry=registry)
    interrupted = storage.get_run(next(iter(storage.list_runs()))["workflow_run_id"])
    node = next(a for a in interrupted["activations"] if a["role"] == "node")
    question = node["questions"][0]
    question_id = question["question_id"]
    goal_revision = interrupted.get("goal_revision")
    budget = question["decision_attempts"]

    monkeypatch.setattr(storage, "update_run", original)
    storage.update_run(interrupted["workflow_run_id"], lambda current: current.update(status="running", supervisor_pid=None, supervisor_identity=None), "simulated_restart")
    await w.WorkflowSupervisor(registry, storage).execute(interrupted["workflow_run_id"])
    completed = storage.get_run(interrupted["workflow_run_id"])
    final_question = next(a for a in completed["activations"] if a["role"] == "node")["questions"][0]
    assert completed["status"] == "completed"
    assert final_question["question_id"] == question_id
    assert completed.get("goal_revision") == goal_revision
    assert final_question["decision_attempts"] == budget
    assert final_question["answer_delivery_state"] == "settled"
    assert len(clarification_calls) == 1
    assert worker_calls == 2
