"""Independent ST01 crash/race oracles; never launch a real provider process."""
import asyncio
import copy
import threading
import time
import uuid

import pytest

from harflow_mcp import workflow_steering as s, workflows as w
from test_workflow_steering_service import (
    Registry, Task, frame, request, run_turn, setup, valid_definition, wrapper_for,
)


class SimulatedCrash(BaseException):
    """A process interruption, rather than a provider-reported failure."""


def fresh(storage, rid):
    return w.WorkflowStore(storage.root).get_run(rid, refresh_atomic=True)


def crash_after(monkeypatch, event):
    original = w.WorkflowSupervisor.update
    fired = False

    def update(supervisor, mutate, kind, *args, **kwargs):
        nonlocal fired
        result = original(supervisor, mutate, kind, *args, **kwargs)
        if kind == event and not fired:
            fired = True
            raise SimulatedCrash(event)
        return result

    monkeypatch.setattr(w.WorkflowSupervisor, "update", update)


@pytest.mark.parametrize("same_request", [True, False])
async def test_barrier_admissions_have_one_durable_receipt_and_issuance(setup, monkeypatch, same_request):
    storage, rid, _ = setup
    first = request(storage, rid)
    second = copy.deepcopy(first) if same_request else request(storage, rid)
    barrier = threading.Barrier(2, timeout=10)
    original = s._transport

    def qualify(*args):
        result = original(*args)
        barrier.wait()
        return result

    monkeypatch.setattr(s, "_transport", qualify)
    results = await asyncio.gather(
        s.steer_workflow(rid, first, root=storage.root),
        s.steer_workflow(rid, second, root=storage.root), return_exceptions=True,
    )
    monkeypatch.setattr(s, "_transport", original)
    accepted = [value for value in results if isinstance(value, dict)]
    assert len(accepted) == (2 if same_request else 1)
    if same_request:
        assert sorted(value["replayed"] for value in accepted) == [False, True]
        assert accepted[0]["task_id"] == accepted[1]["task_id"]
    else:
        assert [value.code for value in results if isinstance(value, s.SteeringError)] == ["steering_revision_stale"]
    registry = Registry(storage.root)
    await run_turn(storage, rid, registry)
    persisted = fresh(storage, rid)["steering"]
    assert len(registry.calls) == len(persisted["turns"]) == len(persisted["requests"]) == 1
    assert len([item for item in persisted["history"] if item["kind"] == "requested"]) == 1
    assert s.get_workflow_steering(rid, root=storage.root)["budgets"]["root_issuances"] == 1


async def test_restart_after_admission_issues_original_task_once(setup):
    storage, rid, _ = setup
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    # Admission is durable but the detached owner has not run at all.
    restarted = w.WorkflowStore(storage.root)
    registry = Registry(storage.root)
    await run_turn(restarted, rid, registry)
    await s.steer_workflow(rid, req, root=storage.root)
    await run_turn(restarted, rid, registry)
    assert [call[2]["task_id"] for call in registry.calls] == [receipt["task_id"]]
    assert len(fresh(storage, rid)["steering"]["turns"]) == 1


@pytest.mark.parametrize("event,spawns,commits", [
    ("steering_spawn_intent", 0, 0),
    ("steering_spawn_observed", 1, 1),
    ("steering_reply_captured", 1, 1),
    ("steering_classified", 1, 1),
])
async def test_restart_at_durable_boundaries_never_reissues(setup, monkeypatch, event, spawns, commits):
    storage, rid, _ = setup
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    turn = fresh(storage, rid)["steering"]["turns"][0]
    registry = Registry(storage.root, frame(wrapper_for(turn, valid_definition())))
    crash_after(monkeypatch, event)
    with pytest.raises(SimulatedCrash):
        await run_turn(storage, rid, registry)
    # A new supervisor and store must recover from durable evidence, not its own locals.
    restarted = w.WorkflowStore(storage.root)
    await run_turn(restarted, rid, registry)
    await run_turn(restarted, rid, registry)
    assert len(registry.calls) == spawns
    persisted = fresh(storage, rid)["steering"]
    assert len(persisted["turns"]) == len(persisted["requests"]) == 1
    assert persisted["turns"][0]["task_id"] == receipt["task_id"]
    assert len(persisted["commits"]) == commits
    if not spawns:
        assert persisted["episodes"][0]["state"] == "delivery_unknown"
        assert persisted["turns"][0]["usage"]["reserved_harness_turns"] == 8
    else:
        assert persisted["turns"][0]["final"] == registry.final
        assert len([item for item in persisted["history"] if item["kind"] == "classified"]) == 1
    assert s.get_workflow_steering(rid, root=storage.root)["budgets"]["root_issuances"] == 1


async def test_lost_spawn_ack_recovers_same_observed_task_without_replacement(setup):
    storage, rid, _ = setup
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    registry = Registry(storage.root)

    async def lost_ack(prompt, path, **kwargs):
        registry.calls.append((prompt, path, kwargs))
        registry.task = Task(kwargs["task_id"], "The original turn settled.")
        raise OSError("spawn acknowledgement lost")

    registry.start = lost_ack
    await run_turn(storage, rid, registry)
    assert fresh(storage, rid)["steering"]["episodes"][0]["state"] == "delivery_unknown"
    await run_turn(w.WorkflowStore(storage.root), rid, registry)
    replay = await s.steer_workflow(rid, req, root=storage.root)
    await run_turn(storage, rid, registry)
    persisted = fresh(storage, rid)["steering"]
    assert replay["task_id"] == receipt["task_id"]
    assert len(registry.calls) == len(persisted["turns"]) == 1
    assert persisted["turns"][0]["final"] == "The original turn settled."
    assert persisted["episodes"][0]["state"] == "awaiting_caller"
    assert not persisted["commits"]


async def test_active_builder_preview_invalidates_queued_target_without_retarget(setup):
    storage, rid, _ = setup
    old_task = uuid.uuid4().hex
    def active(run):
        run["status"] = "running"
        run["activations"].append({"id": uuid.uuid4().hex, "node_id": "builder", "role": "builder", "status": "running", "created_at": time.time(),
                                   "tasks": [{"task_id": old_task, "status": "running", "candidate": {"backend": "claude"}}], "token": None})
    storage.update_run(rid, active, "test_active_builder")
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    registry = Registry(storage.root)
    await run_turn(storage, rid, registry)
    assert registry.calls == []
    assert fresh(storage, rid)["steering"]["episodes"][0]["state"] == "queued_boundary"
    def old_preview_settles(run):
        run["builder_draft"] = {"name": "example", "nodes": [], "connections": [], "description": "Old builder preview"}
        run["draft_revision"] += 1
        run["activations"][0]["status"] = "completed"
        run["activations"][0]["tasks"][0]["status"] = "completed"
    storage.update_run(rid, old_preview_settles, "test_old_builder_preview")
    await run_turn(storage, rid, registry)
    persisted = fresh(storage, rid)
    assert registry.calls == []
    assert persisted["steering"]["turns"][0]["outcome"] == "steering_target_stale"
    assert persisted["steering"]["episodes"][0]["state"] == "stale"
    assert persisted["steering"]["requests"][req["request_id"]]["task_id"] == receipt["task_id"]
    assert persisted["steering"]["turns"][0]["text"] == req["text"]
    assert persisted["builder_draft"]["description"] == "Old builder preview"
    assert not persisted["steering"]["commits"]
    assert s.get_workflow_steering(rid, root=storage.root)["budgets"]["root_issuances"] == 1


async def test_native_revocation_after_spawn_retains_final_and_usage_without_commit(setup):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    turn = fresh(storage, rid)["steering"]["turns"][0]
    registry = Registry(storage.root, frame(wrapper_for(turn, valid_definition())), usage={"input_tokens": 17}, total_cost_usd=0.25)
    started = asyncio.Event()
    task = Task(turn["task_id"], registry.final, usage={"input_tokens": 17}, total_cost_usd=0.25)
    task.done.clear()
    async def start(prompt, path, **kwargs):
        registry.calls.append((prompt, path, kwargs))
        registry.task = task
        started.set()
        return task
    registry.start = start
    running = asyncio.create_task(run_turn(storage, rid, registry))
    await asyncio.wait_for(started.wait(), 10)
    current = fresh(storage, rid)
    await s.steer_workflow(rid, {"schema_version": 1, "request_id": str(uuid.uuid4()), "action": "take_owner",
                               "expected_interaction_epoch": current["interaction_epoch"],
                               "expected_control_revision": current["steering"]["control_revision"], "reason": "Native owner takes over"}, root=storage.root, monitor=True)
    task.done.set()
    await running
    persisted = fresh(storage, rid)
    issued = persisted["steering"]["turns"][0]
    assert issued["final"] == registry.final
    assert issued["usage"]["usage"] == {"input_tokens": 17}
    assert issued["usage"]["cost_usd"] == 0.25
    assert issued["outcome"] == "interaction_revoked"
    assert not persisted["steering"]["commits"]
    assert "generated_definition" not in persisted
    assert len(registry.calls) == 1
    assert s.get_workflow_steering(rid, root=storage.root, monitor=True)["budgets"]["root_issuances"] == 1


async def test_failed_turns_exhaust_episode_allowance_without_refund(setup):
    storage, rid, _ = setup
    registry = Registry(storage.root, exit_code=1, status="failed")
    for index in range(8):
        ep = fresh(storage, rid)["steering"]["episodes"]
        req = request(storage, rid, action="reply" if ep else "open", ep=ep[0] if ep else None)
        await s.steer_workflow(rid, req, root=storage.root)
        await run_turn(storage, rid, registry)
        await s.steer_workflow(rid, req, root=storage.root)
        assert s.get_workflow_steering(rid, root=storage.root)["budgets"]["root_issuances"] == index + 1
    ep = fresh(storage, rid)["steering"]["episodes"][0]
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, request(storage, rid, action="reply", ep=ep), root=storage.root)
    assert error.value.code == "steering_budget_exhausted"
    persisted = fresh(storage, rid)["steering"]
    assert len(registry.calls) == len(persisted["turns"]) == 8
    assert all(turn["outcome"] == "provider_failure" for turn in persisted["turns"])
    assert all(turn["usage"]["reserved_harness_turns"] == 8 for turn in persisted["turns"])


async def test_unknown_turn_blocks_replacement_and_retains_budget(setup):
    storage, rid, _ = setup
    req = request(storage, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    registry = Registry(storage.root, exit_code=None, status="failed")
    await run_turn(storage, rid, registry)
    await s.steer_workflow(rid, req, root=storage.root)
    await run_turn(storage, rid, registry)
    ep = fresh(storage, rid)["steering"]["episodes"][0]
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, request(storage, rid, action="reply", ep=ep), root=storage.root)
    assert error.value.code == "steering_busy"
    assert len(registry.calls) == len(fresh(storage, rid)["steering"]["turns"]) == 1
    assert s.get_workflow_steering(rid, root=storage.root)["budgets"]["root_issuances"] == 1


async def test_unrequested_builder_has_no_steering_effects(setup):
    storage, rid, _ = setup
    before = fresh(storage, rid)
    registry = Registry(storage.root, "HARFLOW_COMMIT_V1\nlooks like ordinary queued text")
    await run_turn(storage, rid, registry)
    assert registry.calls == []
    assert fresh(storage, rid) == before
    assert storage.list() == []


async def test_concurrent_supervisors_claim_once_and_classify_final_once(setup):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    turn = fresh(storage, rid)["steering"]["turns"][0]
    registry = Registry(storage.root, frame(wrapper_for(turn, valid_definition())))
    task = Task(turn["task_id"], registry.final)
    task.done.clear()
    started, attached = asyncio.Event(), asyncio.Event()

    async def start(prompt, path, **kwargs):
        registry.calls.append((prompt, path, kwargs))
        registry.task = task
        started.set()
        return task

    def attach(task_id):
        assert task_id == task.task_id
        attached.set()
        return task

    registry.start, registry.attach = start, attach
    first = asyncio.create_task(run_turn(storage, rid, registry))
    await asyncio.wait_for(started.wait(), 10)
    second = asyncio.create_task(run_turn(w.WorkflowStore(storage.root), rid, registry))
    await asyncio.wait_for(attached.wait(), 10)
    task.done.set()
    await asyncio.gather(first, second)
    persisted = fresh(storage, rid)["steering"]
    assert len(registry.calls) == len(persisted["requests"]) == len(persisted["turns"]) == 1
    assert len(persisted["commits"]) == 1
    assert persisted["turns"][0]["outcome"] == "committed"
    assert persisted["episodes"][0]["state"] == "committed"
    assert len([event for event in persisted["history"] if event["kind"] == "final_captured"]) == 1
    assert len([event for event in persisted["history"] if event["kind"] == "classified"]) == 1


@pytest.mark.parametrize("capture_known", [True, False])
async def test_queued_restart_observes_exact_old_task_before_one_requested_turn(setup, capture_known):
    from harflow_mcp import store as task_store
    from harflow_mcp.task_supervision import read_task_record

    storage, rid, _ = setup
    old_task_id = uuid.uuid4().hex
    old_final = "Old builder reply is evidence, never a new proposal."
    record = task_store.TaskRecord(
        task_id=old_task_id, backend="claude", session_id="old-boundary-session",
        repo_path=fresh(storage, rid)["repo_path"], started_at="2026-11-05T00:00:00+00:00",
        status="completed", exit_code=0, output_drained=capture_known, terminal_summary=old_final,
        freedom="read_only", workflow_builder=True,
    )
    task_store.write(storage.root / "tasks", record)

    def active(run):
        run["status"] = "running"
        run["activations"].append({
            "id": uuid.uuid4().hex, "node_id": "builder", "role": "builder", "status": "running",
            "created_at": time.time(), "token": None,
            "tasks": [{"task_id": old_task_id, "status": "running", "candidate": {"backend": "claude"}}],
        })

    storage.update_run(rid, active, "test_detached_builder_before_restart")
    before = fresh(storage, rid)
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    registry = Registry(storage.root, "Only the requested conversation runs.")
    observed_ids = []
    resumed_ids = []

    async def resume_record(observed, prompt, **kwargs):
        resumed_ids.append((observed.task_id, kwargs["task_id"]))
        registry.task = Task(kwargs["task_id"], registry.final)
        return registry.task

    registry.resume_record = resume_record

    def attach(task_id):
        observed_ids.append(task_id)
        assert task_id == old_task_id
        observed = read_task_record(registry.log_dir, task_id)
        assert observed is not None and observed.task_id == old_task_id
        return Task(task_id, observed.terminal_summary, status=observed.status,
                    exit_code=observed.exit_code, output_drained=observed.output_drained,
                    session_id=observed.session_id)

    registry.attach = attach
    # The fixture deliberately leaves the workflow attempt running although its
    # detached task has a terminal record, as a dead supervisor would.
    restarted = w.WorkflowStore(storage.root)
    assert fresh(restarted, rid)["activations"][0]["tasks"][0]["status"] == "running"
    await run_turn(restarted, rid, registry)
    await s.steer_workflow(rid, req, root=storage.root)
    await run_turn(restarted, rid, registry)
    persisted = fresh(storage, rid)
    steering = persisted["steering"]
    assert observed_ids and set(observed_ids) == {old_task_id}
    assert len(steering["turns"]) == len(steering["requests"]) == 1
    assert steering["turns"][0]["task_id"] == receipt["task_id"]
    assert steering["turns"][0]["boundary_task_id"] == old_task_id
    assert steering["turns"][0]["text"] == req["text"]
    assert steering["turns"][0]["target"]["preview_hash"] == req["target"]["preview_hash"]
    assert persisted["builder_draft"] == before["builder_draft"]
    assert persisted.get("source_revision") == before.get("source_revision")
    assert "generated_definition" not in persisted
    assert not steering["commits"]
    if capture_known:
        assert registry.calls == []
        assert resumed_ids == [(old_task_id, receipt["task_id"])]
        assert persisted["activations"][0]["tasks"][0]["status"] == "completed"
        assert steering["turns"][0]["final"] == registry.final
        assert steering["episodes"][0]["state"] == "awaiting_caller"
    else:
        assert registry.calls == resumed_ids == []
        assert steering["turns"][0]["state"] == "queued"
        assert steering["episodes"][0]["hold"] is True
        assert persisted["activations"][0]["tasks"][0]["status"] in {"running", "uncertain"}
    assert s.get_workflow_steering(rid, root=storage.root)["budgets"]["root_issuances"] == 1
