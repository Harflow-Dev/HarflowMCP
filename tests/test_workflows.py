import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from polybridge import workflows as w


def definition(role="task"):
    return {"name": "example", "orchestrator": {"backend": "codex"}, "nodes": [{"id": "start", "type": "start"}, {"id": "work", "type": "agent", "role": role, "agent": {"backend": "codex"}, "instructions": "Work"}, {"id": "end", "type": "end"}], "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}]}


class FakeTask:
    def __init__(self, task_id, summary, backend="codex", status="completed", stderr=None):
        self.task_id = task_id
        self.done = asyncio.Event()
        self.done.set()
        self._snapshot = {"task_id": task_id, "session_id": "session-" + task_id, "status": status, "summary": summary, "backend": backend, "stderr_tail": stderr or []}

    def snapshot(self):
        return self._snapshot


class FakeRegistry:
    def __init__(self, root, responses):
        self._log_dir = root / "tasks"
        self.responses = iter(responses)
        self.calls = []
        self.tasks = {}

    async def start(self, prompt, repo_path, **kwargs):
        self.calls.append((prompt, kwargs))
        response = next(self.responses)
        task = FakeTask(kwargs["task_id"], **response) if isinstance(response, dict) else FakeTask(kwargs["task_id"], response)
        self.tasks[task.task_id] = task
        return task

    def get(self, task_id):
        return self.tasks.get(task_id)

    async def resume(self, parent, prompt, **kwargs):
        return await self.start(prompt, Path("."), **kwargs)

    async def cancel_cascade(self, task_id, **kwargs):
        return {}


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda b: True)
    storage = w.WorkflowStore(tmp_path)
    # This suite preserves the historical routing engine and builder regression
    # fixtures. Public delegation control eligibility is tested separately; these
    # snapshots represent the pre-cutover supervisor, not newly launched runs.
    create_run = storage.create_run
    def create_historical(*args, **kwargs):
        run = create_run(*args, **kwargs)
        if run.get("kind") != "builder":
            def legacy(r):
                r.pop("execution_contract", None)
                for node in r["definition"]["nodes"]:
                    if node["type"] == "agent":
                        node["session_mode"] = "resume"
            run = storage.update_run(run["workflow_run_id"], legacy, "historical_test_fixture")
        return run
    monkeypatch.setattr(storage, "create_run", create_historical)
    monkeypatch.setattr(w, "_require_delegation_control", lambda run: None)
    return storage


async def execute(storage, tmp_path, definition_, responses):
    d = w.validate_definition(definition_)
    run = storage.create_run(d, "build it", tmp_path)
    registry = FakeRegistry(storage.root, responses)
    supervisor = w.WorkflowSupervisor(registry, storage)
    await asyncio.wait_for(supervisor.execute(run["workflow_run_id"]), 3)
    return storage.get_run(run["workflow_run_id"]), registry


def test_definition_revisions_and_immutable_run(storage, tmp_path):
    saved = storage.save("example", definition())
    run = storage.create_run(saved, "task", tmp_path)
    with pytest.raises(w.WorkflowError, match="Stale"):
        storage.save("example", definition())
    updated = definition()
    updated["description"] = "new"
    assert storage.save("example", updated, 1)["revision"] == 2
    assert storage.get_run(run["workflow_run_id"])["definition"]["revision"] == 1
    storage.delete("example")
    assert storage.list() == []
    assert storage.get_run(run["workflow_run_id"])["definition"]["name"] == "example"


@pytest.mark.parametrize("name", ["../bad", "", "/absolute", "x/y", "x\\y", " leading", "trailing ", "tab\tname", "line\nname", "nul\0name", ".", "..", "a" * 101])
def test_bad_names_rejected(storage, name):
    with pytest.raises(w.WorkflowError):
        storage.save(name, definition())


def test_invalid_capability_rejected():
    d = definition()
    d["orchestrator"] = {"backend": "vibe", "model": "unsupported"}
    with pytest.raises(w.WorkflowError):
        w.validate_definition(d)


def test_omitted_positions_are_spaced_and_explicit_positions_preserved():
    d = definition()
    d["nodes"][1]["position"] = {"x": 415, "y": 230}
    normalized = w.validate_definition(d)
    assert normalized["nodes"][0]["position"] == {"x": 80, "y": 80}
    assert normalized["nodes"][1]["position"] == {"x": 415, "y": 230}
    assert normalized["nodes"][2]["position"] == {"x": 600, "y": 80}
    assert len({(n["position"]["x"], n["position"]["y"]) for n in normalized["nodes"]}) == 3


async def test_linear_run_reservation_and_association(storage, tmp_path):
    run, registry = await execute(storage, tmp_path, definition(), ["done"])
    assert run["status"] == "completed"
    a = run["activations"][0]
    assert a["tasks"][0]["status"] == "completed"
    assert storage.task_owner(a["tasks"][0]["task_id"])["role"] == "node"
    assert storage.pinned_tasks() == set()
    journal = (storage.runs / f"{run['workflow_run_id']}.jsonl").read_text()
    assert journal.index("dispatch_reserved") < journal.index("task_state")
    assert len(registry.calls) == 1


async def test_planning_then_implementation_orchestrator_only_completion(storage, tmp_path):
    d = definition("planning")
    d["nodes"].insert(2, {"id": "implement", "type": "agent", "role": "implementation", "agent": {"backend": "codex"}})
    d["connections"][1]["target"] = "implement"
    d["connections"].append({"id": "done", "source": "implement", "target": "end"})
    responses = [json.dumps({"tasks": [{"id": "one", "title": "Implement"}]}), json.dumps({"action": "continue", "connections": ["finish"], "reason": "Plan ready"}), json.dumps({"summary": "Implemented and tests pass", "completed_task_ids": ["one"]}), json.dumps({"action": "continue", "connections": ["done"], "reason": "Done", "task_updates": [{"task_id": "one", "status": "completed", "reason": "Implementation verified"}]})]
    run, registry = await execute(storage, tmp_path, d, responses)
    assert run["status"] == "completed"
    assert run["tasks"][0]["status"] == "completed"
    assert run["tasks"][0]["completed_by_activation_id"]
    assert len(run["decisions"]) == 2
    assert "Checklist:" in registry.calls[2][0]


async def test_worker_claim_alone_never_completes(storage, tmp_path):
    d = definition("implementation")
    run = storage.create_run(w.validate_definition(d), "task", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r["tasks"].append({"id": "one", "title": "Task", "status": "pending"}), "fixture")
    registry = FakeRegistry(storage.root, [json.dumps({"summary": "done", "completed_task_ids": ["one"], "task_updates": [{"task_id": "one", "status": "completed"}]}), json.dumps({"action": "continue", "connections": ["finish"], "reason": "done"})])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert storage.get_run(run["workflow_run_id"])["tasks"][0]["status"] == "pending"


async def test_unknown_orchestrator_task_updates_rejected(storage, tmp_path):
    decision = json.dumps({"action": "continue", "connections": ["finish"], "reason": "done", "task_updates": [{"task_id": "unknown", "status": "completed", "reason": "done"}]})
    run, _ = await execute(storage, tmp_path, definition("review"), ["reviewed", decision, decision])
    assert run["status"] == "needs_attention"
    assert not run["decisions"]


async def test_failed_tests_are_not_availability(storage, tmp_path):
    d = definition()
    d["nodes"][1]["agent"]["fallbacks"] = [{"backend": "claude"}]
    run, registry = await execute(storage, tmp_path, d, [{"summary": "Tests failed: rate_limit_exceeded", "status": "failed"}])
    assert run["status"] == "needs_attention"
    assert len(registry.calls) == 1


async def test_ordered_availability_fallback(storage, tmp_path):
    d = definition()
    d["nodes"][1]["agent"]["fallbacks"] = [{"backend": "claude"}, {"backend": "codex", "model": "second"}]
    run, registry = await execute(storage, tmp_path, d, [{"summary": "", "status": "failed", "backend": "codex", "stderr": ["rate_limit_exceeded"]}, {"summary": "", "status": "failed", "backend": "claude", "stderr": ["You've hit your limit"]}, "success"])
    assert run["status"] == "completed"
    assert len(registry.calls) == 3
    assert len(run["activations"]) == 1
    assert len(run["activations"][0]["tasks"]) == 3


def parallel_definition():
    d = definition()
    d["nodes"][1].update(branch_mode="all_matching", join_id="join")
    d["nodes"].extend([{"id": "left", "type": "agent", "agent": {"backend": "codex"}}, {"id": "right", "type": "agent", "agent": {"backend": "codex"}}, {"id": "join", "type": "join"}])
    d["connections"] = [d["connections"][0], {"id": "left-edge", "source": "work", "target": "left", "condition": "left needed"}, {"id": "right-edge", "source": "work", "target": "right", "condition": "right needed"}, {"id": "left-join", "source": "left", "target": "join"}, {"id": "right-join", "source": "right", "target": "join"}, {"id": "finish", "source": "join", "target": "end"}]
    return d


async def test_parallel_join_only_selected_branches(storage, tmp_path):
    decision = json.dumps({"action": "continue", "connections": ["left-edge"], "reason": "Only left needed"})
    run, registry = await execute(storage, tmp_path, parallel_definition(), ["split", decision, "left done"])
    assert run["status"] == "completed"
    assert len(registry.calls) == 3
    assert run["joins"] == {}


def test_cross_branch_merge_rejected():
    d = parallel_definition()
    d["connections"][4]["target"] = "left"
    with pytest.raises(w.WorkflowError, match="Cross-branch"):
        w.validate_definition(d)


async def test_three_attempt_loop_limit(storage, tmp_path):
    d = definition()
    d["connections"].append({"id": "retry", "source": "work", "target": "work", "backward": True, "condition": "retry"})
    decision = json.dumps({"action": "continue", "connections": ["retry"], "reason": "Retry"})
    run, registry = await execute(storage, tmp_path, d, ["attempt1", decision, "attempt2", decision, "attempt3", decision])
    assert run["status"] == "needs_attention"
    assert "Attempt limit" in run["attention_reason"]
    assert sum(a["role"] == "node" for a in run["activations"]) == 3


async def test_pause_after_worker_resume_does_not_repeat(storage, tmp_path, monkeypatch):
    d = w.validate_definition(definition("review"))
    run = storage.create_run(d, "task", tmp_path)
    rid = run["workflow_run_id"]
    registry = FakeRegistry(storage.root, ["review done", json.dumps({"action": "continue", "connections": ["finish"], "reason": "approved"})])
    original = registry.start
    async def start(prompt, repo_path, **kwargs):
        task = await original(prompt, repo_path, **kwargs)
        if len(registry.calls) == 1:
            storage.control(rid, "pause")
        return task
    registry.start = start
    supervisor = w.WorkflowSupervisor(registry, storage)
    await supervisor.execute(rid)
    assert storage.get_run(rid)["status"] == "paused"
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    storage.control(rid, "resume")
    await supervisor.execute(rid)
    assert storage.get_run(rid)["status"] == "completed"
    assert len(registry.calls) == 2


async def test_uncertain_dispatch_is_never_replayed(storage, tmp_path):
    run = storage.create_run(w.validate_definition(definition()), "task", tmp_path)
    rid = run["workflow_run_id"]
    storage.update_run(rid, lambda r: r.update(status="running", activations=[{"id": "a", "node_id": "work", "role": "node", "status": "running", "tasks": [{"task_id": "reserved", "status": "reserved"}]}]), "fixture")
    registry = FakeRegistry(storage.root, [])
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    assert storage.get_run(rid)["status"] == "needs_attention"
    with pytest.raises(w.WorkflowError, match="Unresolved"):
        storage.control(rid, "resume")
    assert registry.calls == []
    storage.update_run(rid, lambda r: r["activations"][0].update(status="failed"), "failed_outer_activation")
    with pytest.raises(w.WorkflowError, match="Unresolved"):
        storage.control(rid, "resume")


async def test_checkout_writer_waits_for_shared_readers(storage):
    first = w.CheckoutLease(storage, "/repo", False)
    await first.__aenter__()
    writer = w.CheckoutLease(storage, "/repo", True)
    pending = asyncio.create_task(writer.__aenter__())
    await asyncio.sleep(.15)
    assert not pending.done()
    await first.__aexit__()
    await asyncio.wait_for(pending, 1)
    await writer.__aexit__()


def test_stdout_availability_envelopes_only(storage):
    log = storage.root / "stream.log"
    snapshot = {"status": "failed", "backend": "codex", "raw_stream_log": str(log)}
    log.write_text(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "usage_limit_reached"}}) + "\n")
    assert w.availability_failure(snapshot) is None
    log.write_text(json.dumps({"type": "turn.failed", "error": {"code": "usage_limit_reached"}}) + "\n")
    assert w.availability_failure(snapshot)
    snapshot["backend"] = "claude"
    log.write_text(json.dumps({"type": "rate_limit_event", "rate_limit_info": {"status": "allowed_warning"}}) + "\n")
    assert w.availability_failure(snapshot) is None
    log.write_text(json.dumps({"type": "rate_limit_event", "rate_limit_info": {"status": "rejected"}}) + "\n")
    assert w.availability_failure(snapshot)


async def test_lease_blocks_orphaned_writer(storage, tmp_path):
    run = storage.create_run(w.validate_definition(definition()), "task", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="needs_attention", activations=[{"id": "a", "node_id": "work", "role": "node", "status": "failed", "tasks": [{"task_id": "unknown", "status": "uncertain", "freedom": "write_in_repo"}]}]), "fixture")
    lease = w.CheckoutLease(storage, str(tmp_path), False)
    pending = asyncio.create_task(lease.__aenter__())
    await asyncio.sleep(.15)
    assert not pending.done()
    storage.update_run(run["workflow_run_id"], lambda r: r["activations"][0]["tasks"][0].update(status="cancelled"), "settled")
    await asyncio.wait_for(pending, 1)
    await lease.__aexit__()


async def test_lease_wait_can_stop_before_spawn(storage, tmp_path):
    first = w.CheckoutLease(storage, str(tmp_path), True)
    await first.__aenter__()
    running = True
    second = w.CheckoutLease(storage, str(tmp_path), True, lambda: running)
    pending = asyncio.create_task(second.__aenter__())
    await asyncio.sleep(.1)
    running = False
    with pytest.raises(w.DispatchNotStarted):
        await pending
    await first.__aexit__()


@pytest.mark.parametrize("raw", [{"backend": "codex", "ignored": "value"}, {"backend": "codex", "fallbacks": "claude"}, {"backend": "codex", "model": 7}])
def test_candidate_unknown_and_invalid_fields_rejected(raw):
    d = definition()
    d["orchestrator"] = raw
    with pytest.raises(w.WorkflowError):
        w.validate_definition(d)


async def test_builder_stores_validated_draft(storage, tmp_path):
    run = storage.create_run({"name": "generated", "orchestrator": {"backend": "codex"}}, "Generate", tmp_path, kind="builder", freedom="read_only")
    registry = FakeRegistry(storage.root, [json.dumps(definition())])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    assert storage.get_run(run["workflow_run_id"])["status"] == "completed"
    assert storage.get("generated")["draft"] is True
    assert registry.calls[0][1]["freedom"] == "read_only"


async def test_builder_never_overwrites_existing_definition(storage, tmp_path):
    storage.save("example", definition())
    run = storage.create_run({"name": "example", "orchestrator": {"backend": "codex"}}, "Generate", tmp_path, kind="builder", freedom="read_only")
    registry = FakeRegistry(storage.root, [json.dumps(definition())])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    assert storage.get_run(run["workflow_run_id"])["status"] == "needs_attention"
    assert storage.get("example")["revision"] == 1


async def test_read_only_caller_clamps_writing_nodes(storage, tmp_path):
    run = storage.create_run(w.validate_definition(definition()), "task", tmp_path, freedom="read_only", network=False)
    registry = FakeRegistry(storage.root, ["done"])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert registry.calls[0][1]["freedom"] == "read_only"
    assert registry.calls[0][1]["network"] is False


async def test_orchestrator_review_reopens_completed_task(storage, tmp_path):
    run = storage.create_run(w.validate_definition(definition("review")), "review", tmp_path)
    rid = run["workflow_run_id"]
    storage.update_run(rid, lambda r: r["tasks"].append({"id": "one", "title": "Task", "status": "completed", "completed_by_activation_id": "prior-implementation"}), "fixture")
    decision = json.dumps({"action": "continue", "connections": ["finish"], "reason": "Review found issue", "task_updates": [{"task_id": "one", "status": "pending", "reason": "Test evidence missing"}]})
    registry = FakeRegistry(storage.root, ["Needs coverage", decision])
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    task = storage.get_run(rid)["tasks"][0]
    assert task["status"] == "pending"
    assert task["completed_by_activation_id"] is None
    assert task["status_decision_activation_id"]


async def test_orchestrator_review_cannot_complete_without_implementation(storage, tmp_path):
    run = storage.create_run(w.validate_definition(definition("review")), "review", tmp_path)
    rid = run["workflow_run_id"]
    storage.update_run(rid, lambda r: r["tasks"].append({"id": "one", "title": "Task", "status": "pending"}), "fixture")
    decision = json.dumps({"action": "continue", "connections": ["finish"], "reason": "Claim", "task_updates": [{"task_id": "one", "status": "completed", "reason": "Claimed complete"}]})
    registry = FakeRegistry(storage.root, ["Reviewed", decision, decision])
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    result = storage.get_run(rid)
    assert result["status"] == "needs_attention"
    assert result["tasks"][0]["status"] == "pending"
    assert not result["decisions"]


def test_invalid_control_attempt_grants(storage, tmp_path):
    run = storage.create_run(w.validate_definition(definition()), "task", tmp_path)
    with pytest.raises(w.WorkflowError):
        storage.control(run["workflow_run_id"], "pause", additional_attempts=True)


async def test_completed_activation_crash_gap_never_reexecutes_worker(storage, tmp_path):
    run = storage.create_run(w.validate_definition(definition("review")), "task", tmp_path)
    rid = run["workflow_run_id"]
    token = {"id": "token", "node_id": "work", "stack": [], "context": {}}
    snapshot = FakeTask("settled-worker", "Reviewed").snapshot()
    activation = {"id": "a", "node_id": "work", "role": "node", "status": "completed", "token": token, "tasks": [{"task_id": "settled-worker", "status": "completed", "result": snapshot}]}
    storage.update_run(rid, lambda r: r.update(status="running", pending=[token], activations=[activation]), "crash_fixture")
    registry = FakeRegistry(storage.root, [json.dumps({"action": "continue", "connections": ["finish"], "reason": "Review approved"})])
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    assert storage.get_run(rid)["status"] == "completed"
    assert len(registry.calls) == 1
    assert "workflow decision agent" in registry.calls[0][0]


async def test_builder_cancel_finalizes_after_active_task_settles(storage, tmp_path, monkeypatch):
    run = storage.create_run({"name": "generated", "orchestrator": {"backend": "codex"}}, "Generate", tmp_path, kind="builder", freedom="read_only")
    rid = run["workflow_run_id"]
    registry = FakeRegistry(storage.root, [{"summary": "", "status": "cancelled"}])
    original = registry.start
    async def start(prompt, repo_path, **kwargs):
        task = await original(prompt, repo_path, **kwargs)
        storage.control(rid, "cancel")
        return task
    registry.start = start
    await w.WorkflowSupervisor(registry, storage).build(rid)
    assert storage.get_run(rid)["status"] == "cancelled"
    assert storage.list() == []


async def test_explicit_resume_retries_malformed_implementation_result(storage, tmp_path, monkeypatch):
    run = storage.create_run(w.validate_definition(definition("implementation")), "Implement", tmp_path)
    rid = run["workflow_run_id"]
    registry = FakeRegistry(storage.root, ["not valid JSON", json.dumps({"summary": "Fixed output", "completed_task_ids": []}), json.dumps({"action": "continue", "connections": ["finish"], "reason": "Complete"})])
    supervisor = w.WorkflowSupervisor(registry, storage)
    await supervisor.execute(rid)
    assert storage.get_run(rid)["status"] == "needs_attention"
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    storage.control(rid, "resume", instructions="Return proper JSON")
    await supervisor.execute(rid)
    assert storage.get_run(rid)["status"] == "completed"
    assert len(registry.calls) == 3


async def test_persisted_decision_routes_without_repeating_orchestrator(storage, tmp_path):
    run = storage.create_run(w.validate_definition(definition("review")), "task", tmp_path)
    rid = run["workflow_run_id"]
    token = {"id": "token", "node_id": "work", "stack": [], "context": {}, "execution_complete": True, "result": {"summary": "Reviewed"}, "selected_connections": ["finish"]}
    storage.update_run(rid, lambda r: r.update(status="running", pending=[token]), "post_decision_crash_fixture")
    registry = FakeRegistry(storage.root, [])
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    assert storage.get_run(rid)["status"] == "completed"
    assert registry.calls == []


async def test_backend_override_clears_primary_options_but_keeps_fallbacks(storage, tmp_path, monkeypatch):
    d = definition()
    d["orchestrator"] = {"backend": "codex", "model": "codex-only", "reasoning_effort": "high", "fallbacks": [{"backend": "claude", "model": "opus"}]}
    storage.save("example", d)
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    run = await w.start_workflow("example", "task", tmp_path, overrides={"backend": "claude"}, root=storage.root)
    candidate = run["definition"]["orchestrator"]
    assert candidate["backend"] == "claude"
    assert "model" not in candidate
    assert "reasoning_effort" not in candidate
    assert candidate["max_turns"] == 100
    assert candidate["fallbacks"][0]["model"] == "opus"
    inherited = await w.start_workflow("example", "task", tmp_path, overrides={"backend": "codex"}, root=storage.root)
    assert inherited["definition"]["orchestrator"]["model"] == "codex-only"
    assert inherited["definition"]["orchestrator"]["reasoning_effort"] == "high"


async def test_backend_override_keeps_explicit_new_primary_options(storage, tmp_path, monkeypatch):
    d = definition()
    d["orchestrator"] = {"backend": "claude", "model": "opus", "reasoning_effort": "high", "max_turns": 8}
    storage.save("example", d)
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    run = await w.start_workflow("example", "task", tmp_path, overrides={"backend": "codex", "model": "new-model", "reasoning_effort": "low"}, root=storage.root)
    candidate = run["definition"]["orchestrator"]
    assert candidate["model"] == "new-model"
    assert candidate["reasoning_effort"] == "low"
    assert "max_turns" not in candidate


@pytest.mark.parametrize("role,response,default_marker", [
    ("planning", json.dumps({"tasks": [{"id": "planned", "title": "Planned task"}], "summary": "Plan"}), "do not implement code"),
    ("implementation", json.dumps({"summary": "Implemented", "completed_task_ids": []}), "observed validation evidence"),
    ("review", "Review complete", "concrete, prioritized findings"),
    ("task", "Check complete", "commands actually executed"),
])
async def test_role_defaults_complement_custom_context_and_preserve_freedom(storage, tmp_path, role, response, default_marker):
    import os
    d = definition(role)
    d["nodes"][1].update(instructions="Custom instruction for this step", freedom="write_in_repo")
    run = storage.create_run(w.validate_definition(d), "Overall user task", tmp_path)
    rid = run["workflow_run_id"]
    token = {"id": "role-token", "node_id": "work", "stack": [], "context": {"summary": "Previous step evidence"}}
    storage.update_run(rid, lambda r: r.update(status="running", supervisor_pid=os.getpid(), instructions="Human recovery note", pending=[token]), "fixture")
    registry = FakeRegistry(storage.root, [response])
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    await supervisor._execute_node(run["definition"]["nodes"][1], token)
    prompt, settings = registry.calls[0]
    for marker in ("Overall user task", "Custom instruction for this step", "Previous step evidence", "Human recovery note", "Role defaults:", default_marker, "Only the orchestrator may change checklist statuses", "Polybridge owns dispatch and routing", "configured permission limits"):
        assert marker in prompt
    assert settings["freedom"] == "write_in_repo"
    assert storage.get_run(rid)["pending"][0]["execution_complete"] is True


def test_unknown_role_default_rejected():
    with pytest.raises(w.WorkflowError, match="Invalid agent role"):
        w.role_prompt("unknown")


@pytest.mark.parametrize("legacy_mode", [None, "choose_one", "all_matching", "auto"])
def test_definition_normalizes_legacy_branch_modes_to_auto(legacy_mode):
    graph = definition()
    if legacy_mode:
        for node in graph["nodes"]:
            node["branch_mode"] = legacy_mode
    assert all(n["branch_mode"] == "auto" for n in w.validate_definition(graph)["nodes"])


async def test_builder_prompts_for_orchestrator_routing_and_inferred_retry_gates(storage, tmp_path):
    run = storage.create_run({"name": "generated", "orchestrator": {"backend": "codex"}}, "Generate", tmp_path, kind="builder", freedom="read_only")
    registry = FakeRegistry(storage.root, [json.dumps(definition())])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    prompt = registry.calls[0][0]
    assert '"branch_mode": "auto"' in prompt
    assert "Ordinary nodes choose exactly one" in prompt
    assert "parallel_group_id" in prompt
    assert "do not set a backward flag" in prompt


def test_reachable_backward_only_branch_requires_forward_exit():
    graph = definition()
    graph["nodes"].append({"id": "trapped", "type": "agent", "agent": {"backend": "codex"}})
    graph["connections"].extend([
        {"id": "enter-trap", "source": "work", "target": "trapped"},
        {"id": "retry-trap", "source": "trapped", "target": "work", "backward": True},
    ])
    with pytest.raises(w.WorkflowError, match="forward path to End: trapped"):
        w.validate_definition(graph)
    graph["connections"].append({"id": "exit-trap", "source": "trapped", "target": "end"})
    assert w.validate_definition(graph)["name"] == "example"


@pytest.mark.parametrize("role,expected", [("planning", "read_only"), ("review", "read_only"), ("implementation", "write_in_repo"), ("task", "publish")])
def test_role_access_defaults(role, expected):
    assert w.validate_definition(definition(role))["nodes"][1]["freedom"] == expected


@pytest.mark.parametrize("role", ["planning", "review", "implementation", "task"])
@pytest.mark.parametrize("freedom", w.FREEDOMS)
def test_role_access_options(role, freedom):
    graph = definition(role)
    graph["nodes"][1]["freedom"] = freedom
    if role == "implementation" and freedom == "read_only":
        with pytest.raises(w.WorkflowError, match="Implementation nodes cannot use read_only"):
            w.validate_definition(graph)
    else:
        assert w.validate_definition(graph)["nodes"][1]["freedom"] == freedom


@pytest.mark.parametrize("requested", w.FREEDOMS)
@pytest.mark.parametrize("ceiling", w.FREEDOMS)
def test_effective_access_never_exceeds_node_or_run_ceiling(requested, ceiling):
    node = {"role": "task", "freedom": requested}
    assert w.effective_freedom(node, ceiling) == w.FREEDOMS[min(w.FREEDOMS.index(requested), w.FREEDOMS.index(ceiling))]


def test_implementation_read_only_launch_rejected(storage, tmp_path):
    with pytest.raises(w.WorkflowError, match="Implementation nodes cannot run read_only"):
        storage.create_run(w.validate_definition(definition("implementation")), "implement", tmp_path, freedom="read_only")
    assert storage.list_runs() == []


@pytest.mark.parametrize("freedom", w.FREEDOMS)
async def test_actual_dispatch_access_and_network_are_independent(storage, tmp_path, freedom):
    graph = definition("task")
    graph["nodes"][1]["freedom"] = freedom
    graph["nodes"][1]["network"] = True
    run = storage.create_run(w.validate_definition(graph), "external task", tmp_path, freedom="unrestricted")
    registry = FakeRegistry(storage.root, ["done"])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    if freedom == "read_only":
        # Codex cannot provide network-enabled read-only; never weaken its sandbox.
        assert registry.calls == []
        outcome = storage.get_run(run["workflow_run_id"])
        assert outcome["status"] == "needs_attention"
        assert outcome["activations"][0]["tasks"][0]["status"] == "not_started"
        return
    assert registry.calls[0][1]["freedom"] == freedom
    assert registry.calls[0][1]["network"] is True
    reservation = storage.get_run(run["workflow_run_id"])["activations"][0]["tasks"][0]
    assert reservation["freedom"] == freedom


async def test_legacy_implementation_read_only_run_needs_attention_without_dispatch(storage, tmp_path):
    run = storage.create_run(w.validate_definition(definition("implementation")), "implement", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(freedom="read_only"), "legacy_fixture")
    registry = FakeRegistry(storage.root, [])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    final = storage.get_run(run["workflow_run_id"])
    assert final["status"] == "needs_attention"
    assert "Implementation nodes cannot run read_only" in final["attention_reason"]
    assert registry.calls == []


async def test_fallback_keeps_effective_access_ceiling(storage, tmp_path):
    graph = definition("task")
    graph["nodes"][1]["freedom"] = "unrestricted"
    graph["nodes"][1]["agent"]["fallbacks"] = [{"backend": "claude"}]
    run = storage.create_run(w.validate_definition(graph), "task", tmp_path, freedom="publish")
    registry = FakeRegistry(storage.root, [
        {"summary": "", "backend": "codex", "status": "failed", "stderr": ["usage_limit_reached"]},
        {"summary": "done", "backend": "claude"},
    ])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert [call[1]["freedom"] for call in registry.calls] == ["publish", "publish"]
    assert {t["freedom"] for a in storage.get_run(run["workflow_run_id"])["activations"] for t in a["tasks"]} == {"publish"}


async def test_orchestrator_stays_read_only_at_unrestricted_ceiling(storage, tmp_path):
    graph = definition("review")
    graph["nodes"][1]["freedom"] = "unrestricted"
    run = storage.create_run(w.validate_definition(graph), "review", tmp_path, freedom="unrestricted")
    registry = FakeRegistry(storage.root, ["done", json.dumps({"action": "complete", "connections": ["finish"], "reason": "done"})])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert [call[1]["freedom"] for call in registry.calls] == ["unrestricted", "read_only"]


def implicit_parallel_definition():
    graph = parallel_definition()
    graph["nodes"] = [n for n in graph["nodes"] if n["type"] != "join"]
    graph["nodes"].append({"id": "merge", "type": "agent", "agent": {"backend": "codex"}, "instructions": "Combine results"})
    graph["nodes"][1].pop("join_id")
    for edge in graph["connections"]:
        if edge["target"] == "join":
            edge["target"] = "merge"
        if edge["source"] == "join":
            edge["source"] = "merge"
    return graph


async def test_implicit_convergence_executes_agent_once_after_all_active_branches(storage, tmp_path):
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both needed"})
    run, registry = await execute(storage, tmp_path, implicit_parallel_definition(), ["split", decision, "left done", "right done", "merged"])
    assert run["status"] == "completed"
    assert run["definition"]["nodes"][1]["join_id"] == "merge"
    assert sum(a["node_id"] == "merge" and a["role"] == "node" for a in run["activations"]) == 1
    assert run["joins"] == {}
    assert "left done" in registry.calls[-1][0] and "right done" in registry.calls[-1][0]


async def test_implicit_convergence_does_not_wait_for_unselected_branch(storage, tmp_path):
    decision = json.dumps({"action": "continue", "connections": ["left-edge"], "reason": "Only left needed"})
    run, registry = await execute(storage, tmp_path, implicit_parallel_definition(), ["split", decision, "left done", "merged"])
    assert run["status"] == "completed"
    assert not any(a["node_id"] == "right" for a in run["activations"])
    assert len(registry.calls) == 4


def nested_same_convergence_definition():
    graph = implicit_parallel_definition()
    left = next(n for n in graph["nodes"] if n["id"] == "left")
    left["branch_mode"] = "all_matching"
    graph["nodes"].extend([{"id": "sub-a", "type": "agent"}, {"id": "sub-b", "type": "agent"}])
    graph["connections"] = [e for e in graph["connections"] if e["source"] != "left"] + [
        {"id": "sub-a-edge", "source": "left", "target": "sub-a"},
        {"id": "sub-b-edge", "source": "left", "target": "sub-b"},
        {"id": "sub-a-merge", "source": "sub-a", "target": "merge"},
        {"id": "sub-b-merge", "source": "sub-b", "target": "merge"},
    ]
    return graph


async def test_nested_splits_can_converge_at_same_agent_once(storage, tmp_path):
    # Mark right read-only to allow scheduling alongside the nested branch; responses
    # are keyed by dispatched prompt so concurrent order is not a test assumption.
    graph = nested_same_convergence_definition()
    run = storage.create_run(w.validate_definition(graph), "task", tmp_path)
    class Registry(FakeRegistry):
        async def start(self, prompt, repo_path, **kwargs):
            self.calls.append((prompt, kwargs))
            if "workflow decision agent" in prompt:
                context = json.loads(prompt.split("Context:\n", 1)[1])
                edges = ["sub-a-edge", "sub-b-edge"] if context["node"]["id"] == "left" else ["left-edge", "right-edge"]
                summary = json.dumps({"action": "continue", "connections": edges, "reason": "Both needed"})
            else:
                summary = "worker result"
            task = FakeTask(kwargs["task_id"], summary)
            self.tasks[task.task_id] = task
            return task
    registry = Registry(storage.root, [])
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 3)
    final = storage.get_run(run["workflow_run_id"])
    assert final["status"] == "completed"
    assert final["joins"] == {}
    assert sum(a["node_id"] == "merge" and a["role"] == "node" for a in final["activations"]) == 1
    assert {n["join_id"] for n in final["definition"]["nodes"] if n["id"] in {"work", "left"}} == {"merge"}


def test_parallel_paths_without_common_convergence_rejected():
    graph = implicit_parallel_definition()
    graph["nodes"].append({"id": "other-end", "type": "end"})
    next(e for e in graph["connections"] if e["source"] == "right")["target"] = "other-end"
    with pytest.raises(w.WorkflowError, match="must converge at one common node"):
        w.validate_definition(graph)


def test_inferred_convergence_is_recomputed_after_canvas_edit():
    graph = w.validate_definition(implicit_parallel_definition())
    graph["nodes"] = [n for n in graph["nodes"] if n["id"] != "merge"]
    graph["connections"] = [e for e in graph["connections"] if e["source"] != "merge"]
    for edge in graph["connections"]:
        if edge["target"] == "merge":
            edge["target"] = "end"
    assert w.validate_definition(graph)["nodes"][1]["join_id"] == "end"


async def test_implicit_end_convergence_finishes_after_selected_branches(storage, tmp_path):
    graph = implicit_parallel_definition()
    graph["nodes"] = [n for n in graph["nodes"] if n["id"] != "merge"]
    graph["connections"] = [e for e in graph["connections"] if e["source"] != "merge"]
    for edge in graph["connections"]:
        if edge["target"] == "merge":
            edge["target"] = "end"
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both"})
    run, registry = await execute(storage, tmp_path, graph, ["split", decision, "left", "right"])
    assert run["status"] == "completed"
    assert run["joins"] == {}
    assert len(registry.calls) == 4


async def test_implicit_convergence_loop_uses_separate_activation_generations(storage, tmp_path):
    graph = implicit_parallel_definition()
    graph["connections"].append({"id": "retry", "source": "merge", "target": "work", "backward": True, "condition": "Retry once"})
    split = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both"})
    retry = json.dumps({"action": "continue", "connections": ["retry"], "reason": "Again"})
    finish = json.dumps({"action": "complete", "connections": ["finish"], "reason": "Done"})
    run, _ = await execute(storage, tmp_path, graph, ["split1", split, "left1", "right1", "merge1", retry, "split2", split, "left2", "right2", "merge2", finish])
    assert run["status"] == "completed"
    assert run["joins"] == {}
    assert sum(a["node_id"] == "merge" and a["role"] == "node" for a in run["activations"]) == 2


def test_automatic_split_can_offer_retry_and_parallel_paths():
    graph = implicit_parallel_definition()
    graph["connections"].append({"id": "retry", "source": "work", "target": "work", "backward": True})
    normalized = w.validate_definition(graph)
    node = next(n for n in normalized["nodes"] if n["id"] == "work")
    assert node["branch_mode"] == "auto"
    assert node["join_id"] == "merge"
    assert next(e for e in normalized["connections"] if e["id"] == "retry")["backward"] is True


def test_natural_loop_inferred_without_backward_flag():
    graph = definition()
    graph["nodes"].insert(2, {"id": "check", "type": "agent", "instructions": "check"})
    graph["connections"][1]["source"] = "check"
    graph["connections"].extend([{"id": "check-work", "source": "work", "target": "check"}, {"id": "retry", "source": "check", "target": "work", "condition": "Check failed"}])
    normalized = w.validate_definition(graph)
    assert next(e for e in normalized["connections"] if e["id"] == "retry")["backward"] is True


def test_canvas_position_and_legacy_flag_do_not_make_forward_arrow_a_retry():
    graph = definition()
    graph["nodes"][0]["position"] = {"x": 900, "y": 80}
    graph["nodes"][1]["position"] = {"x": 80, "y": 80}
    graph["connections"][0]["backward"] = True
    normalized = w.validate_definition(graph)
    assert normalized["connections"][0]["backward"] is False


def test_self_retry_inferred_without_flag():
    graph = definition()
    graph["connections"].append({"id": "retry", "source": "work", "target": "work", "condition": "Retry"})
    normalized = w.validate_definition(graph)
    assert normalized["connections"][-1]["backward"] is True


def test_irreducible_loop_rejected():
    graph = definition()
    graph["nodes"].insert(2, {"id": "other", "type": "agent"})
    graph["connections"].extend([{"id": "other-start", "source": "start", "target": "other"}, {"id": "other-work", "source": "other", "target": "work"}, {"id": "work-other", "source": "work", "target": "other"}, {"id": "other-end", "source": "other", "target": "end"}])
    with pytest.raises(w.WorkflowError, match="Ambiguous loop"):
        w.validate_definition(graph)


async def test_retry_plus_forward_selection_is_corrected(storage, tmp_path):
    graph = definition()
    graph["connections"].append({"id": "retry", "source": "work", "target": "work", "condition": "Retry"})
    invalid = json.dumps({"action": "continue", "connections": ["retry", "finish"], "reason": "Both"})
    valid = json.dumps({"action": "continue", "connections": ["finish"], "reason": "Done"})
    run, registry = await execute(storage, tmp_path, graph, ["work", invalid, valid])
    assert run["status"] == "completed"
    assert len(registry.calls) == 3
    assert len(run["decisions"]) == 1
    assert "must be selected exclusively" in registry.calls[-1][0]


async def test_orchestrator_receives_workflow_guide_and_target_descriptions(storage, tmp_path):
    graph = implicit_parallel_definition()
    for node in graph["nodes"]:
        node["instructions"] = f"Custom instructions for {node['id']}"
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both reviews required"})
    run, registry = await execute(storage, tmp_path, graph, ["split result", decision, "left report", "right report", "merged"])
    assert run["status"] == "completed"
    prompt = registry.calls[1][0]
    context = json.loads(prompt.split("Context:\n", 1)[1])
    assert {n["id"] for n in context["workflow_graph"]["nodes"]} == {n["id"] for n in graph["nodes"]}
    assert len(context["workflow_graph"]["connections"]) == len(graph["connections"])
    assert context["legal_connections"][0]["target_node"]["instructions"].startswith("Custom instructions")
    assert "Blank conditions are available unconditional paths, not a requirement" in prompt
    assert "never invent" in prompt.lower()


async def test_existing_run_snapshot_preserves_historical_branch_mode(storage, tmp_path):
    graph = w.validate_definition(implicit_parallel_definition())
    next(n for n in graph["nodes"] if n["id"] == "work")["branch_mode"] = "choose_one"
    run = storage.create_run(graph, "task", tmp_path)
    invalid = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both"})
    registry = FakeRegistry(storage.root, ["split", invalid, invalid])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "needs_attention"
    assert next(n for n in observed["definition"]["nodes"] if n["id"] == "work")["branch_mode"] == "choose_one"


def feature_flow_definition():
    roles = {"planning": "planning", "implementation": "implementation", "plan_a": "review", "plan_b": "review", "plan_gate": "review", "code_a": "review", "code_b": "review", "code_gate": "review", "unit": "task", "ui": "task"}
    nodes = [{"id": "start", "type": "start"}] + [{"id": nid, "type": "agent", "role": role, "instructions": nid, "agent": {"backend": "codex"}, "branch_mode": "choose_one"} for nid, role in roles.items()] + [{"id": "end", "type": "end"}]
    paths = [("start", "planning", ""), ("planning", "plan_a", "Review plan alongside other reviewer"), ("planning", "plan_b", "Review plan alongside other reviewer"), ("plan_a", "plan_gate", ""), ("plan_b", "plan_gate", ""), ("plan_gate", "planning", "Either plan review requests revision"), ("plan_gate", "implementation", "Both plan reviews approve"), ("implementation", "unit", ""), ("unit", "implementation", "Unit validation failed"), ("unit", "ui", "Unit validation passed"), ("ui", "implementation", "UI validation failed"), ("ui", "code_a", "UI passed: review alongside other reviewer"), ("ui", "code_b", "UI passed: review alongside other reviewer"), ("code_a", "code_gate", ""), ("code_b", "code_gate", ""), ("code_gate", "implementation", "Either code review requests changes"), ("code_gate", "end", "Both code reviews approve with validation")]
    return {"name": "feature-flow", "orchestrator": {"backend": "codex"}, "nodes": nodes, "connections": [{"id": source + "-" + target, "source": source, "target": target, "condition": condition} for source, target, condition in paths]}


class FeatureRegistry(FakeRegistry):
    def __init__(self, root, retry_at=None, always_retry=False):
        super().__init__(root, [])
        self.retry_at = retry_at
        self.always_retry = always_retry
        self.worker_counts = {}
        self.decision_contexts = []

    async def start(self, prompt, repo_path, **kwargs):
        self.calls.append((prompt, kwargs))
        if "workflow decision agent" in prompt:
            context = json.loads(prompt.split("Context:\n", 1)[1])
            self.decision_contexts.append(context)
            nid = context["node"]["id"]
            routing = {"planning": ["planning-plan_a", "planning-plan_b"], "plan_a": ["plan_a-plan_gate"], "plan_b": ["plan_b-plan_gate"], "plan_gate": ["plan_gate-implementation"], "implementation": ["implementation-unit"], "unit": ["unit-ui"], "ui": ["ui-code_a", "ui-code_b"], "code_a": ["code_a-code_gate"], "code_b": ["code_b-code_gate"], "code_gate": ["code_gate-end"]}
            selected = routing[nid]
            if nid == self.retry_at and (self.always_retry or self.worker_counts[nid] == 1):
                selected = [nid + "-implementation"]
            updates = [{"task_id": "feature", "status": "completed", "reason": "Implementation and observed evidence"}] if nid == "implementation" else []
            summary = json.dumps({"action": "continue", "connections": selected, "reason": "Observed workflow evidence", "task_updates": updates})
        else:
            nid = prompt.split("Step instructions: ", 1)[1].split("\n", 1)[0]
            self.worker_counts[nid] = self.worker_counts.get(nid, 0) + 1
            if nid == "planning":
                summary = json.dumps({"tasks": [{"id": "feature", "title": "Implement feature"}], "summary": "Actionable plan"})
            elif nid == "implementation":
                summary = json.dumps({"summary": "Implemented with validation evidence", "completed_task_ids": ["feature"]})
            else:
                summary = "Failed check / changes needed" if nid == self.retry_at and (self.always_retry or self.worker_counts[nid] == 1) else "Passed / approved with observed evidence"
            if nid in {"plan_gate", "code_gate"}:
                prior_context = json.loads(prompt.split("Prior context: ", 1)[1].split("\nRole defaults:", 1)[0])
                assert len(prior_context["branches"]) == 2
        task = FakeTask(kwargs["task_id"], summary)
        self.tasks[task.task_id] = task
        return task


@pytest.mark.parametrize("retry_at", [None, "unit", "ui", "code_gate"])
async def test_feature_flow_infers_parallel_reviews_and_exclusive_retry_gates(storage, tmp_path, retry_at):
    normalized = w.validate_definition(feature_flow_definition())
    assert sum(e["backward"] for e in normalized["connections"]) == 4
    assert {n["id"]: n["join_id"] for n in normalized["nodes"] if n.get("join_id")} == {"planning": "plan_gate", "ui": "code_gate"}
    run = storage.create_run(normalized, "Implement requested feature", tmp_path)
    registry = FeatureRegistry(storage.root, retry_at)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 10)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed"
    assert observed["joins"] == {}
    assert registry.worker_counts["plan_gate"] == 1
    assert registry.worker_counts["implementation"] == (2 if retry_at else 1)
    assert registry.worker_counts["code_gate"] == (2 if retry_at == "code_gate" else 1)
    assert observed["tasks"][0]["status"] == "completed"


async def test_feature_flow_test_failures_obey_three_implementation_attempt_limit(storage, tmp_path):
    run = storage.create_run(w.validate_definition(feature_flow_definition()), "Implement", tmp_path)
    registry = FeatureRegistry(storage.root, "unit", always_retry=True)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 10)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "needs_attention"
    assert "Attempt limit reached for implementation" == observed["attention_reason"]
    assert registry.worker_counts["implementation"] == 3
    assert "ui" not in registry.worker_counts


def test_workflow_turn_defaults_supported_candidates_only_and_explicit_limit_preserved():
    graph = definition()
    graph["orchestrator"] = {"backend": "claude", "fallbacks": [{"backend": "vibe"}, {"backend": "codex"}]}
    graph["nodes"][1]["agent"] = {"backend": "claude", "max_turns": 12, "fallbacks": [{"backend": "vibe"}, {"backend": "codex"}]}
    normalized = w.validate_definition(graph)
    assert normalized["orchestrator"]["max_turns"] == 100
    assert normalized["orchestrator"]["fallbacks"][0]["max_turns"] == 100
    assert "max_turns" not in normalized["orchestrator"]["fallbacks"][1]
    candidate = normalized["nodes"][1]["agent"]
    assert candidate["max_turns"] == 12
    assert candidate["fallbacks"][0]["max_turns"] == 100
    assert "max_turns" not in candidate["fallbacks"][1]
    assert normalized["nodes"][1]["max_attempts"] == 3
    assert normalized["max_transitions"] == 100


def test_explicit_unsupported_turn_cap_still_rejected():
    graph = definition()
    graph["orchestrator"] = {"backend": "codex", "max_turns": 100}
    with pytest.raises(w.WorkflowError, match="no turn cap"):
        w.validate_definition(graph)


def feature_bypass_definition():
    graph = feature_flow_definition()
    graph["connections"].extend([
        {"id": "trivial-bypass", "source": "start", "target": "implementation", "condition": "If changes are trivial"},
        {"id": "no-ui-review-a", "source": "unit", "target": "code_a", "condition": "If no UI changes are involved, review alongside other code reviewer"},
        {"id": "no-ui-review-b", "source": "unit", "target": "code_b", "condition": "If no UI changes are involved, review alongside other code reviewer"},
    ])
    return graph


class BypassFeatureRegistry(FeatureRegistry):
    def __init__(self, root, *, trivial=False, skip_ui=False, retry_at=None):
        super().__init__(root, retry_at)
        self.trivial = trivial
        self.skip_ui = skip_ui

    async def start(self, prompt, repo_path, **kwargs):
        summary = None
        if "workflow decision agent" in prompt:
            context = json.loads(prompt.split("Context:\n", 1)[1])
            nid = context["node"]["id"]
            selected = None
            if nid == "start":
                selected = ["trivial-bypass" if self.trivial else "start-planning"]
            elif nid == "unit" and self.skip_ui:
                selected = ["no-ui-review-a", "no-ui-review-b"]
            elif nid == "implementation" and self.trivial:
                selected = ["implementation-unit"]
            if selected:
                self.decision_contexts.append(context)
                summary = json.dumps({"action": "continue", "connections": selected, "reason": "Conditional bypass with evidence"})
        elif self.trivial and "Step instructions: implementation\n" in prompt:
            self.worker_counts["implementation"] = self.worker_counts.get("implementation", 0) + 1
            summary = json.dumps({"summary": "Trivial implementation completed", "completed_task_ids": []})
        if summary is None:
            return await super().start(prompt, repo_path, **kwargs)
        self.calls.append((prompt, kwargs))
        task = FakeTask(kwargs["task_id"], summary)
        self.tasks[task.task_id] = task
        return task


@pytest.mark.parametrize("trivial,skip_ui,retry_at", [(True, True, None), (False, True, "code_gate"), (False, False, None)])
async def test_conditional_feature_bypasses_and_parallel_review_converge_once(storage, tmp_path, trivial, skip_ui, retry_at):
    normalized = w.validate_definition(feature_bypass_definition())
    assert sum(e["backward"] for e in normalized["connections"]) == 4
    run = storage.create_run(normalized, "Implement requested feature", tmp_path)
    registry = BypassFeatureRegistry(storage.root, trivial=trivial, skip_ui=skip_ui, retry_at=retry_at)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 10)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed"
    assert observed["joins"] == {}
    assert ("planning" not in registry.worker_counts) == trivial
    assert ("ui" not in registry.worker_counts) == skip_ui
    assert registry.worker_counts["implementation"] == (2 if retry_at else 1)
    assert registry.worker_counts["code_gate"] == (2 if retry_at else 1)
    decisions = [c for c in observed["decisions"] if c["node_id"] == "unit"]
    assert len(decisions[0]["connections"]) == (2 if skip_ui else 1)
    assert decisions[0]["selected_join_id"] == ("code_gate" if skip_ui else None)
    journal = (storage.runs / f"{run['workflow_run_id']}.jsonl").read_text()
    assert '"event": "decision"' in journal


def test_selected_overlapping_paths_rejected_but_disjoint_bypass_reviews_allowed():
    graph = w.validate_definition(feature_bypass_definition())
    node = next(n for n in graph["nodes"] if n["id"] == "unit")
    by_edge = {e["id"]: e for e in graph["connections"]}
    assert w.validate_selection(graph, node, [by_edge["no-ui-review-a"], by_edge["no-ui-review-b"]], {}, {}) == "code_gate"
    assert w.validate_selection(graph, node, [by_edge["unit-ui"]], {}, {}) is None
    with pytest.raises(w.WorkflowError, match="overlap before convergence"):
        w.validate_selection(graph, node, [by_edge["unit-ui"], by_edge["no-ui-review-a"]], {}, {})


async def test_unsafe_selected_overlap_gets_one_correction_without_duplicate_agents(storage, tmp_path):
    graph = implicit_parallel_definition()
    graph["connections"].append({"id": "left-right", "source": "left", "target": "right", "condition": "Alternative path"})
    invalid = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both"})
    corrected = json.dumps({"action": "continue", "connections": ["right-edge"], "reason": "Safe alternate route"})
    run, registry = await execute(storage, tmp_path, graph, ["work", invalid, corrected, "right", "merge"])
    assert run["status"] == "completed"
    assert "overlap before convergence" in registry.calls[2][0]
    assert not any(a["role"] == "node" and a["node_id"] == "left" for a in run["activations"])
    assert sum(a["role"] == "node" and a["node_id"] == "right" for a in run["activations"]) == 1


def test_runtime_retry_cannot_escape_any_active_fork_frame():
    graph = feature_bypass_definition()
    graph["connections"].append({"id": "review-retry", "source": "code_a", "target": "implementation", "condition": "Needs fixes"})
    normalized = w.validate_definition(graph)
    node = next(n for n in normalized["nodes"] if n["id"] == "code_a")
    edge = next(e for e in normalized["connections"] if e["id"] == "review-retry")
    assert edge["backward"] is True
    groups = {"outer": {"split_id": "unit", "join_id": "code_gate"}, "inner": {"split_id": "ui", "join_id": "code_gate"}}
    with pytest.raises(w.WorkflowError, match="escape active parallel split unit"):
        w.validate_selection(normalized, node, [edge], {"stack": ["outer", "inner"]}, groups)
    assert w.validate_selection(normalized, node, [edge], {"stack": []}, {}) is None


def test_legacy_fork_missing_split_metadata_fails_closed_if_ambiguous():
    graph = w.validate_definition(feature_bypass_definition())
    graph["connections"].append({"id": "review-retry", "source": "code_a", "target": "implementation", "backward": True})
    node = next(n for n in graph["nodes"] if n["id"] == "code_a")
    edge = graph["connections"][-1]
    with pytest.raises(w.WorkflowError, match="historical parallel split safely"):
        w.validate_selection(graph, node, [edge], {"stack": ["legacy"]}, {"legacy": {"join_id": "code_gate"}})


def test_selected_subset_uses_earlier_convergence_and_direct_barrier_path_is_empty():
    graph = implicit_parallel_definition()
    graph["nodes"].insert(-1, {"id": "earlier", "type": "agent"})
    graph["nodes"].insert(-1, {"id": "third", "type": "agent"})
    for edge in graph["connections"]:
        if edge["source"] in {"left", "right"}:
            edge["target"] = "earlier"
    graph["connections"].extend([{"id": "third-edge", "source": "work", "target": "third"}, {"id": "third-merge", "source": "third", "target": "merge"}, {"id": "earlier-merge", "source": "earlier", "target": "merge"}])
    normalized = w.validate_definition(graph)
    node = next(n for n in normalized["nodes"] if n["id"] == "work")
    assert node["join_id"] == "merge"
    selected = [e for e in normalized["connections"] if e["id"] in {"left-edge", "right-edge"}]
    assert w.validate_selection(normalized, node, selected, {}, {}) == "earlier"
    assert w._forward_reachable(normalized, "earlier", "earlier") == set()


async def test_active_fork_escaping_retry_is_corrected_before_dispatch(storage, tmp_path):
    graph = implicit_parallel_definition()
    graph["connections"].append({"id": "retry-parent", "source": "left", "target": "work", "condition": "Changes needed"})
    run = storage.create_run(w.validate_definition(graph), "task", tmp_path)
    class Registry(FakeRegistry):
        decisions = 0
        async def start(self, prompt, repo_path, **kwargs):
            self.calls.append((prompt, kwargs))
            if "workflow decision agent" in prompt:
                context = json.JSONDecoder().raw_decode(prompt.split("Context:\n", 1)[1])[0]
                if context["node"]["id"] == "work":
                    selected = ["left-edge", "right-edge"]
                else:
                    self.decisions += 1
                    selected = ["retry-parent"] if self.decisions == 1 else ["left-join"]
                summary = json.dumps({"action": "continue", "connections": selected, "reason": "Evidence"})
            else:
                summary = "Worker evidence"
            task = FakeTask(kwargs["task_id"], summary)
            self.tasks[task.task_id] = task
            return task
    registry = Registry(storage.root, [])
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed"
    assert sum(a["role"] == "node" and a["node_id"] == "work" for a in observed["activations"]) == 1
    assert any("Retry cannot escape active parallel split work" in prompt for prompt, _ in registry.calls)


async def test_recovered_selected_retry_is_revalidated_against_active_fork(storage, tmp_path):
    graph = implicit_parallel_definition()
    graph["connections"].append({"id": "retry-parent", "source": "left", "target": "work", "condition": "Changes needed"})
    run = storage.create_run(w.validate_definition(graph), "task", tmp_path)
    rid = run["workflow_run_id"]
    token = {"id": "recovered", "node_id": "left", "stack": ["fork"], "context": {}, "execution_complete": True, "result": {"summary": "Worker already finished"}, "selected_connections": ["retry-parent"]}
    group = {"join_id": "merge", "split_id": "work", "expected": 2, "arrived": [], "stack": []}
    storage.update_run(rid, lambda r: r.update(status="running", pending=[token], joins={"fork": group}), "recovery_fixture")
    registry = FakeRegistry(storage.root, [])
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    observed = storage.get_run(rid)
    assert observed["status"] == "needs_attention"
    assert "Retry cannot escape active parallel split work" in observed["attention_reason"]
    assert registry.calls == []


async def test_selected_subset_earlier_convergence_is_persisted_with_decision(storage, tmp_path):
    graph = implicit_parallel_definition()
    graph["nodes"].extend([{"id": "earlier", "type": "agent"}, {"id": "third", "type": "agent"}])
    for edge in graph["connections"]:
        if edge["source"] in {"left", "right"}:
            edge["target"] = "earlier"
    graph["connections"].extend([{"id": "third-edge", "source": "work", "target": "third"}, {"id": "third-merge", "source": "third", "target": "merge"}, {"id": "earlier-merge", "source": "earlier", "target": "merge"}])
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Only two branches needed"})
    run, _ = await execute(storage, tmp_path, graph, ["work", decision, "left", "right", "earlier", "merge"])
    assert run["status"] == "completed"
    assert run["decisions"][0]["selected_join_id"] == "earlier"
    assert sum(a["role"] == "node" and a["node_id"] == "earlier" for a in run["activations"]) == 1
    assert not any(a["role"] == "node" and a["node_id"] == "third" for a in run["activations"])


async def test_edit_builder_accepts_incomplete_canvas_and_never_saves_proposal(storage, tmp_path, monkeypatch):
    baseline = storage.save("example", definition())
    captured = {"name": "example", "nodes": [{"id": "start", "type": "start"}], "connections": []}
    source = {"name": "example", "revision": 1, "saved_definition": baseline}
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    run = await w.build_workflow("example", "Finish my current unsaved graph", tmp_path, agent={"backend": "codex"}, definition=captured, source=source, root=storage.root)
    proposal = definition()
    proposal.update(name="agent-invented-name", revision=999, description="Proposed edit")
    registry = FakeRegistry(storage.root, [json.dumps(proposal)])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed"
    assert observed["generated_definition"]["name"] == "example"
    assert observed["generated_definition"]["revision"] == 1
    assert observed["generated_definition"]["description"] == "Proposed edit"
    assert observed["editing_definition"] == captured
    assert observed["editing_source"] == source
    assert observed["source_saved_definition"] == baseline
    assert storage.get("example") == baseline
    assert len(storage.list()) == 1
    prompt, settings = registry.calls[0]
    assert settings["freedom"] == "read_only"
    assert "Current canvas:" in prompt
    assert "Preserve existing node and connection IDs" in prompt
    assert "read repository guidance and skills" in prompt
    assert "Read applicable repository AGENTS.md and skill files" in prompt
    assert "especially skills specified in the request" in prompt
    assert "do not implement the task or run the workflow" in prompt
    assert json.JSONDecoder().raw_decode(prompt.split("Current canvas:\n", 1)[1])[0] == captured


async def test_failed_edit_builder_preserves_saved_graph_and_captured_canvas(storage, tmp_path, monkeypatch):
    baseline = storage.save("example", definition())
    captured = {"name": "example", "nodes": [{"id": "unfinished"}], "connections": []}
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    run = await w.build_workflow("example", "Refine", tmp_path, agent={"backend": "codex"}, definition=captured, source={"name": "example", "revision": 1, "saved_definition": baseline}, root=storage.root)
    registry = FakeRegistry(storage.root, ["invalid output"])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "needs_attention"
    assert observed["attention_reason"].startswith("Workflow proposal was invalid:")
    assert "generated_definition" not in observed
    assert observed["editing_definition"] == captured
    assert storage.get("example") == baseline


async def test_unsaved_or_renamed_edit_proposal_has_zero_revision_and_no_file(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    run = await w.build_workflow("new-name", "Refine", tmp_path, agent={"backend": "codex"}, definition={"name": "new-name", "nodes": [], "connections": []}, source={"name": "old-name", "revision": 42, "saved_definition": None}, root=storage.root)
    proposal = definition()
    proposal["revision"] = 100
    registry = FakeRegistry(storage.root, [json.dumps(proposal)])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["generated_definition"]["name"] == "new-name"
    assert observed["generated_definition"]["revision"] == 0
    assert observed["source_name"] == "old-name"
    assert observed["source_revision"] == 42
    assert storage.list() == []


@pytest.mark.parametrize("definition_,source", [
    ({"nodes": "not an array"}, None),
    ({"nodes": ["not an object"]}, None),
    ({"nodes": [], "unexpected": {"value": object()}}, None),
    ({"nodes": [], "value": float("nan")}, None),
    ({"nodes": []}, {"name": "example", "revision": True}),
    ({"nodes": []}, {"name": "../unsafe", "revision": 1}),
    ({"nodes": []}, {"name": "example", "revision": -1}),
    ({"nodes": []}, {"unknown": "option"}),
    (None, {"name": "example", "revision": 1}),
    ({"nodes": [], "text": "x" * (1024 * 1024)}, None),
])
async def test_edit_context_invalid_json_shapes_and_source_metadata_rejected(storage, tmp_path, monkeypatch, definition_, source):
    monkeypatch.setattr(w, "_launch", lambda *a: pytest.fail("Invalid context must not launch"))
    with pytest.raises(w.WorkflowError):
        await w.build_workflow("example", "Refine", tmp_path, agent={"backend": "codex"}, definition=definition_, source=source, root=storage.root)
    assert storage.list_runs() == []


def active_builder(storage, tmp_path, *, role="builder"):
    from polybridge import store, lineage
    run = storage.create_run({"name": "example", "orchestrator": {"backend": "codex"}}, "Build", tmp_path, kind="builder", freedom="read_only")
    tid = "builder-task"
    record = store.TaskRecord(task_id=tid, backend="codex", session_id="session", repo_path=str(tmp_path), started_at=1, status="running")
    store.write(storage.root / "tasks", record)
    activation = {"id": "activation", "node_id": "builder", "role": role, "status": "running", "tasks": [{"task_id": tid, "status": "running"}]}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", activations=[activation], draft_revision=0), "test_active")
    return run["workflow_run_id"], lineage.Caller(record, "ancestry")


async def test_live_builder_apply_scoped_atomic_revision_and_safe_incomplete_canvas(storage, tmp_path):
    run_id, caller = active_builder(storage, tmp_path)
    incomplete = {"name": "wrong", "revision": 987, "nodes": [{"id": "work", "type": "agent"}], "connections": []}
    applied = await w.apply_workflow_draft(incomplete, 0, caller=caller, root=storage.root)
    assert applied["draft_revision"] == 1
    assert applied["builder_draft"]["name"] == "example"
    assert "revision" not in applied["builder_draft"]
    assert storage.list() == []
    with pytest.raises(w.WorkflowError, match="revision conflict"):
        await w.apply_workflow_draft(incomplete, 0, caller=caller, root=storage.root)
    assert storage.get_run(run_id)["draft_revision"] == 1
    journal = (storage.runs / f"{run_id}.jsonl").read_text()
    assert '"event": "builder_draft_applied"' in journal


@pytest.mark.parametrize("state", ["paused", "cancelled", "completed", "needs_attention"])
async def test_live_builder_apply_refuses_settled_or_paused_run(storage, tmp_path, state):
    run_id, caller = active_builder(storage, tmp_path)
    storage.update_run(run_id, lambda r: r.update(status=state), "test_status")
    with pytest.raises(w.WorkflowError, match="no longer active"):
        await w.apply_workflow_draft({"nodes": [], "connections": []}, 0, caller=caller, root=storage.root)


async def test_live_builder_apply_refuses_worker_and_old_activation(storage, tmp_path):
    run_id, caller = active_builder(storage, tmp_path, role="node")
    with pytest.raises(w.WorkflowError, match="Only the active"):
        await w.apply_workflow_draft({}, 0, caller=caller, root=storage.root)
    storage.update_run(run_id, lambda r: (r["activations"][0].update(role="builder"), r["activations"].append({"id": "new", "role": "builder", "status": "running", "tasks": []})), "test_new_turn")
    with pytest.raises(w.WorkflowError, match="no longer active"):
        await w.apply_workflow_draft({}, 0, caller=caller, root=storage.root)


@pytest.mark.parametrize("canvas", [
    {"nodes": [{"id": "same", "type": "agent"}, {"id": "same", "type": "agent"}]},
    {"nodes": [{"type": "agent"}]},
    {"nodes": [{"id": "x", "type": "bad"}]},
    {"nodes": [{"id": "x", "type": "agent", "position": {"x": 1e10, "y": 0}}]},
    {"nodes": [], "connections": [{"id": "edge", "source": "missing", "target": "missing"}]},
])
def test_live_preview_rejects_render_unsafe_snapshots(canvas):
    with pytest.raises(w.WorkflowError):
        w.validate_builder_preview(canvas, "example")


async def test_live_builder_applied_draft_wins_over_stale_final_json(storage, tmp_path):
    from polybridge import store, lineage
    baseline = storage.save("example", definition())
    run = storage.create_run({"name": "example", "orchestrator": {"backend": "codex"}}, "Refine", tmp_path, kind="builder", freedom="read_only")
    storage.update_run(run["workflow_run_id"], lambda r: r.update(editing_definition=baseline, editing_source={"name": "example", "revision": 1, "saved_definition": baseline}, draft_revision=0), "editing")
    class ApplyingRegistry(FakeRegistry):
        async def start(self, prompt, repo_path, **kwargs):
            record = store.TaskRecord(task_id=kwargs["task_id"], backend="codex", session_id="s", repo_path=str(repo_path), started_at=1, status="running")
            store.write(self._log_dir, record)
            newer = definition()
            newer["description"] = "Live graph wins"
            await w.apply_workflow_draft(newer, 0, caller=lineage.Caller(record, "ancestry"), root=storage.root)
            task = await super().start(prompt, repo_path, **kwargs)
            from dataclasses import replace
            store.write(self._log_dir, replace(record, status="completed"))
            return task
    registry = ApplyingRegistry(storage.root, [json.dumps(definition())])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    result = storage.get_run(run["workflow_run_id"])
    assert result["generated_definition"]["description"] == "Live graph wins"
    assert result["builder_draft"] == result["generated_definition"]
    assert result["draft_revision"] == 2
    assert storage.get("example") == baseline


async def test_builder_nonlive_feedback_drains_next_turn_same_session_without_save(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(w, "_supervisor_present", lambda r: bool(r.get("supervisor_pid")))
    baseline = storage.save("example", definition())
    run = storage.create_run({"name": "example", "orchestrator": {"backend": "codex"}}, "Edit", tmp_path, kind="builder", freedom="read_only")
    storage.update_run(run["workflow_run_id"], lambda r: r.update(editing_definition=baseline, editing_source={"name": "example", "revision": 1, "saved_definition": baseline}, draft_revision=0), "editing")
    class FeedbackRegistry(FakeRegistry):
        async def start(self, prompt, repo_path, **kwargs):
            task = await super().start(prompt, repo_path, **kwargs)
            if len(self.calls) == 1:
                result = await w.followup_workflow_builder(run["workflow_run_id"], "Make the second edit", root=storage.root)
                assert result["status"] == "queued_next_turn"
            return task
    second = definition(); second["description"] = "Followup"
    registry = FeedbackRegistry(storage.root, [json.dumps(definition()), json.dumps(second)])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    observed = storage.get_run(run["workflow_run_id"])
    assert len(registry.calls) == 2
    assert "Make the second edit" in registry.calls[1][0]
    assert "max_turns" in registry.calls[1][1]  # same-session resume path
    assert observed["generated_definition"]["description"] == "Followup"
    assert storage.get("example") == baseline


async def test_builder_followup_refuses_unreconciled_dispatch(storage, tmp_path, monkeypatch):
    run_id, caller = active_builder(storage, tmp_path)
    monkeypatch.setattr(w, "_supervisor_present", lambda r: False)
    with pytest.raises(w.WorkflowError, match="reconciliation"):
        await w.followup_workflow_builder(run_id, "Continue", root=storage.root)
    assert not storage.get_run(run_id).get("builder_messages")


@pytest.mark.parametrize("uncertain", [False, True])
async def test_live_builder_feedback_close_refusal_defers_but_unknown_send_needs_attention(storage, tmp_path, monkeypatch, uncertain):
    from polybridge import inbox
    monkeypatch.setattr(w, "_supervisor_present", lambda r: bool(r.get("supervisor_pid")))
    baseline = storage.save("example", definition())
    run = storage.create_run({"name": "example", "orchestrator": {"backend": "codex"}}, "Edit", tmp_path, kind="builder", freedom="read_only")
    storage.update_run(run["workflow_run_id"], lambda r: r.update(editing_definition=baseline, editing_source={"name": "example", "revision": 1}, draft_revision=0), "edit")
    class LiveRegistry(FakeRegistry):
        async def start(self, prompt, repo_path, **kwargs):
            task = await super().start(prompt, repo_path, **kwargs)
            if len(self.calls) == 1:
                task.live_input = True
                task.done.clear()
                await w.followup_workflow_builder(run["workflow_run_id"], "Feedback", root=storage.root)
            return task
        async def send_message(self, task, text):
            task.done.set()
            if uncertain:
                raise RuntimeError("IPC failed")
            raise inbox.SendRefused("closed", code="closed")
    registry = LiveRegistry(storage.root, [json.dumps(definition()), json.dumps(definition())])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    observed = storage.get_run(run["workflow_run_id"])
    if uncertain:
        assert observed["status"] == "needs_attention"
        assert len(registry.calls) == 1
        assert observed["builder_messages"][0]["status"] == "forwarding_uncertain"
    else:
        assert observed["status"] == "completed"
        assert len(registry.calls) == 2
        assert observed["builder_messages"][0]["status"] == "claimed"


async def test_initial_create_queued_feedback_captures_saved_baseline_for_followup(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(w, "_supervisor_present", lambda r: bool(r.get("supervisor_pid")))
    run = storage.create_run({"name": "example", "orchestrator": {"backend": "codex"}}, "Create", tmp_path, kind="builder", freedom="read_only")
    class CreateFeedbackRegistry(FakeRegistry):
        async def start(self, prompt, repo_path, **kwargs):
            task = await super().start(prompt, repo_path, **kwargs)
            if len(self.calls) == 1:
                await w.followup_workflow_builder(run["workflow_run_id"], "Refine after creating", root=storage.root)
            return task
    next_graph = definition(); next_graph["description"] = "Unsaved refinement"
    registry = CreateFeedbackRegistry(storage.root, [json.dumps(definition()), json.dumps(next_graph)])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["source_name"] == "example"
    assert observed["source_revision"] == 1
    assert observed["source_saved_definition"] == storage.get("example")
    assert observed["generated_definition"]["description"] == "Unsaved refinement"
    assert observed["generated_definition"]["revision"] == 1
    assert storage.get("example").get("description", "") == ""


async def test_builder_recovery_surfaces_claimed_live_forwarding_without_replay(storage, tmp_path):
    run = storage.create_run({"name": "example", "orchestrator": {"backend": "codex"}}, "Create", tmp_path, kind="builder", freedom="read_only")
    storage.update_run(run["workflow_run_id"], lambda r: r.update(builder_messages=[{"id": "old", "prompt": "Ambiguous feedback", "status": "forwarding"}]), "seed_crash")
    registry = FakeRegistry(storage.root, [])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "needs_attention"
    assert observed["builder_messages"][0]["status"] == "forwarding_uncertain"
    assert registry.calls == []


@pytest.mark.parametrize("limit", [0, 1, 3])
async def test_retry_connection_budget_counts_actual_traversals(storage, tmp_path, limit):
    d = definition()
    d["nodes"][1]["max_attempts"] = 10
    d["connections"].append({"id": "retry", "source": "work", "target": "work", "condition": "Retry", "max_retries": limit})
    decision = json.dumps({"action": "continue", "connections": ["retry"], "reason": "Needs another attempt"})
    responses = [item for i in range(limit + 1) for item in (f"attempt {i}", decision)]
    run, registry = await execute(storage, tmp_path, d, responses)
    assert run["status"] == "needs_attention"
    assert f"connection retry: {limit} of {limit}" in run["attention_reason"]
    assert run["retry_counts"].get("retry", 0) == limit
    assert sum(a["role"] == "node" for a in run["activations"]) == limit + 1
    assert run["transitions"] == 1 + limit
    assert run["exhausted_retry_edges"] == ["retry"]
    assert len(registry.calls) == (limit + 1) * 2
    context = json.loads(registry.calls[-1][0].split("Context:\n", 1)[1])
    retry = next(e for e in context["legal_connections"] if e["id"] == "retry")
    assert retry["retry_used"] == limit and retry["retry_remaining"] == 0


@pytest.mark.parametrize("invalid", [True, False, -1, 1.0, "3", None])
def test_retry_connection_limit_requires_nonnegative_json_integer(invalid):
    d = definition(); d["connections"][0]["max_retries"] = invalid
    with pytest.raises(w.WorkflowError, match="max_retries"):
        w.validate_definition(d)


async def test_forward_connection_keeps_retry_config_without_limiting_forward_progress(storage, tmp_path):
    d = definition(); d["connections"][0]["max_retries"] = 0
    run, registry = await execute(storage, tmp_path, d, ["Done"])
    assert run["status"] == "completed"
    assert run["definition"]["connections"][0]["max_retries"] == 0
    assert run["retry_counts"] == {}


async def test_recovered_retry_selection_checks_budget_without_partial_transition(storage, tmp_path):
    d = definition(); d["connections"].append({"id": "retry", "source": "work", "target": "work", "max_retries": 1})
    run = storage.create_run(w.validate_definition(d), "Task", tmp_path)
    token = {"id": "recovered", "node_id": "work", "stack": [], "context": {}, "execution_complete": True, "result": {"summary": "Already finished"}, "selected_connections": ["retry"]}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", pending=[token], retry_counts={"retry": 1}, transitions=9), "recovered")
    registry = FakeRegistry(storage.root, [])
    supervisor = w.WorkflowSupervisor(registry, storage); supervisor.run_id = run["workflow_run_id"]
    await supervisor._node(token)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "needs_attention"
    assert observed["transitions"] == 9
    assert observed["pending"] == [token]
    assert observed["retry_counts"] == {"retry": 1}
    assert registry.calls == []


async def test_explicit_retry_resume_grants_only_exhausted_connection(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    d = definition(); d["connections"].extend([{"id": "retry", "source": "work", "target": "work", "max_retries": 0}, {"id": "other", "source": "work", "target": "work", "max_retries": 4}])
    run = storage.create_run(w.validate_definition(d), "Task", tmp_path)
    token = {"id": "t", "node_id": "work", "stack": [], "context": {}, "execution_complete": True, "result": {"summary": "Done"}, "selected_connections": ["retry"]}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="needs_attention", exhausted_retry_edges=["retry"], pending=[token]), "limit")
    resumed = storage.control(run["workflow_run_id"], "resume", additional_attempts=1)
    assert resumed["retry_grants"] == {"retry": 1}
    assert "exhausted_retry_edges" not in resumed
    assert resumed["definition"]["connections"][-2]["max_retries"] == 0
    supervisor = w.WorkflowSupervisor(FakeRegistry(storage.root, []), storage); supervisor.run_id = run["workflow_run_id"]
    await supervisor._node(token)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["retry_counts"] == {"retry": 1}
    assert observed["pending"][0]["via"] == "retry"
    journal = (storage.runs / f"{run['workflow_run_id']}.jsonl").read_text()
    assert '"retry_grants": {"retry": {"additional": 1, "previous": 0, "total": 1}}' in journal


def test_distinct_retry_edges_have_independent_run_budgets():
    run = {"retry_counts": {"one": 2, "two": 0}, "retry_grants": {"one": 1}}
    w.validate_retry_budget(run, [{"id": "two", "backward": True, "max_retries": 1}])
    assert w.retry_budget(run, {"id": "one", "max_retries": 2})["retry_remaining"] == 1
    with pytest.raises(w.RetryLimitReached):
        w.validate_retry_budget(run, [{"id": "one", "backward": True, "max_retries": 1}])


async def test_builder_omitted_repo_uses_owned_workspace_and_preserves_followup_path(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    run = await w.build_workflow("example", "Create reusable workflow", agent={"backend": "codex"}, root=storage.root)
    assert run["repo_path"] == str((storage.root / "builder-workspace").resolve())
    assert Path(run["repo_path"]).stat().st_mode & 0o777 == 0o700
    assert run["freedom"] == "read_only" and run["builder_has_repo_context"] is False
    registry = FakeRegistry(storage.root, [json.dumps(definition())])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    assert "No user repository was supplied" in registry.calls[0][0]
    assert registry.calls[0][1]["display_prompt"] == "Create reusable workflow"
    assert registry.calls[0][1]["workflow_builder"] is True
    await w.followup_workflow_builder(run["workflow_run_id"], "Refine", root=storage.root)
    assert storage.get_run(run["workflow_run_id"])["repo_path"] == run["repo_path"]


async def test_builder_invalid_agent_does_not_create_default_workspace(storage):
    with pytest.raises(w.backends.UnknownBackend):
        await w.build_workflow("example", "Create", agent={"backend": "unknown"}, root=storage.root)
    assert not (storage.root / "builder-workspace").exists()


async def test_registry_display_projection_keeps_full_backend_prompt_and_normalized_timeline(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from polybridge import tasks, store
    registry = tasks.TaskRegistry(log_dir=tmp_path)
    captured = {}
    async def spawn(invocation, **kwargs):
        captured.update(kwargs, argv=invocation.argv)
        return SimpleNamespace()
    monkeypatch.setattr(registry, "_spawn", spawn)
    full = "USER REQUEST\nINJECTED PRIVATE WORKFLOW CONTEXT"
    await registry.start(full, tmp_path, backend=w.backends.get("codex"), freedom="read_only", display_prompt="USER REQUEST")
    assert full in captured["argv"]
    assert captured["prompt"] == full and captured["display_prompt"] == "USER REQUEST"
    task = tasks.Task(task_id="display", backend="codex", session_id="s", repo_path=tmp_path, prompt=full, display_prompt="USER REQUEST", workflow_builder=True, max_turns=5, log_path=tmp_path / "raw", started_at=datetime.now(timezone.utc))
    registry.persist(task)
    assert store.read(tmp_path, "display").prompt == "USER REQUEST"
    assert store.read(tmp_path, "display").workflow_builder is True
    assert task.brief()["workflow_builder"] is True
    events = []
    task.events = SimpleNamespace(write=lambda kind, fields, **kw: events.append((kind, fields)))
    backend = SimpleNamespace(normalize=lambda event, acc: [{"kind": "user_message", "text": full, "source": "initial"}, {"kind": "user_message", "text": "Later human message", "source": "live"}])
    tasks._record_events(task, backend, {}, None)
    assert events[0][1]["text"] == "USER REQUEST"
    assert events[1][1]["text"] == "Later human message"


@pytest.mark.parametrize("recovered", [False, True])
async def test_registry_workflow_followup_display_keeps_full_resume_input(tmp_path, monkeypatch, recovered):
    from datetime import datetime, timezone
    from polybridge import tasks, store
    registry = tasks.TaskRegistry(log_dir=tmp_path)
    parent = tasks.Task(task_id="parent", backend="claude", session_id="session", repo_path=tmp_path, prompt="Previous full prompt", display_prompt="Previous user request", max_turns=5, log_path=tmp_path / "parent.log", started_at=datetime.now(timezone.utc))
    parent.status = "completed"; parent.done.set()
    registry._tasks[parent.task_id] = parent
    registry.persist(parent)
    observed = {}
    async def spawn(invocation, **kwargs):
        observed.update(kwargs, argv=invocation.argv, initial_input=invocation.initial_input)
        return SimpleNamespace()
    monkeypatch.setattr(registry, "_spawn", spawn)
    full = "Actual feedback\nLatest private graph context"
    if recovered:
        await registry.resume_record(store.read(tmp_path, "parent"), full, display_prompt="Actual feedback", workflow_builder=True)
    else:
        await registry.resume(parent, full, display_prompt="Actual feedback", workflow_builder=True)
    assert full in observed["argv"] or full == json.loads(observed["initial_input"].decode())["message"]["content"][0]["text"]
    assert observed["prompt"] == full
    assert observed["display_prompt"] == "Actual feedback"
    assert observed["workflow_builder"] is True


@pytest.mark.parametrize("invalid", [None, True, 7, [], {}])
def test_optional_start_purpose_rejects_nonstring(invalid):
    graph = definition(); graph["nodes"][0]["prompt"] = invalid
    with pytest.raises(w.WorkflowError, match="Start prompt"):
        w.validate_definition(graph)
    with pytest.raises(w.WorkflowError, match="Start prompt"):
        w.validate_builder_preview(graph, "example")


def test_start_purpose_defaults_empty_and_preserves_agent_fields():
    graph = definition(); graph["nodes"][1]["prompt"] = {"existing": "custom extension"}
    canonical = w.validate_definition(graph)
    assert canonical["nodes"][0]["prompt"] == ""
    assert canonical["nodes"][1]["prompt"] == {"existing": "custom extension"}


async def test_start_purpose_injected_alongside_runtime_request_without_display_leak(storage, tmp_path):
    graph = definition("review")
    graph["nodes"][0]["prompt"] = "This workflow independently reviews release changes"
    decision = json.dumps({"action": "complete", "connections": ["finish"], "reason": "Approved"})
    observed, registry = await execute(storage, tmp_path, graph, ["Approved", decision])
    assert observed["status"] == "completed"
    context = json.loads(registry.calls[-1][0].split("Context:\n", 1)[1])
    assert context["workflow_purpose"] == graph["nodes"][0]["prompt"]
    assert context["task"] == "build it"
    assert registry.calls[-1][1]["display_prompt"] == "build it"
    assert "independently reviews" not in registry.calls[-1][1]["display_prompt"]


@pytest.mark.parametrize("resume", [False, True])
def test_codex_builder_nongit_workspace_disables_only_git_guard(tmp_path, resume):
    backend = w.backends.get("codex")
    kwargs = dict(repo=tmp_path, freedom="read_only", model=None, max_turns=None, reasoning_effort=None)
    invocation = backend.build_resume_argv("Refine", session_id="session", **kwargs) if resume else backend.build_start_argv("Create", session_id=None, **kwargs)
    assert invocation.argv.count("--skip-git-repo-check") == 1
    assert invocation.argv[invocation.argv.index("-s") + 1] == "read-only"
    assert 'approval_policy="never"' in invocation.argv
    backend.assert_safe(invocation, "read_only")


@pytest.mark.parametrize("backend_name", ["codex", "claude", "vibe", "opencode", "antigravity"])
def test_all_backend_canonical_invocations_accept_owned_nongit_builder_directory(tmp_path, backend_name):
    backend = w.backends.get(backend_name)
    invocation = backend.build_start_argv("Reply OK", repo=tmp_path, freedom="read_only", session_id="session" if backend.capabilities.chooses_session_id else None, model=None, max_turns=None, reasoning_effort=None)
    backend.assert_safe(invocation, "read_only")
    assert not (tmp_path / ".git").exists()
    if backend_name == "vibe":
        assert "--trust" in invocation.argv
    if backend_name == "codex":
        assert "--skip-git-repo-check" in invocation.argv


async def test_builder_failed_startup_exposes_sanitized_stderr_reason(storage, tmp_path):
    run = storage.create_run({"name": "example", "orchestrator": {"backend": "codex"}}, "Create", tmp_path, kind="builder", freedom="read_only")
    error = "Not inside a trusted directory and --skip-git-repo-check was not specified."
    registry = FakeRegistry(storage.root, [{"summary": "", "status": "failed", "stderr": ["Reading additional input from stdin...", "Reading prompt from stdin...", "WARNING: config entry ignored", error]}])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "needs_attention"
    assert error in observed["attention_reason"]
    assert "Create a Polybridge workflow" not in observed["attention_reason"]


def test_failure_diagnostic_removes_injected_prompt_echo_and_bounds_output():
    prompt = "Internal private workflow context\nComplete graph and instructions"
    result = w.failure_diagnostic({"stderr_tail": ["Error: " + json.dumps(prompt), "X" * 2000]}, prompt)
    assert "Internal private" not in result and "Complete graph" not in result
    assert len(result) <= 500


def test_failed_stderr_survives_registry_persistence_and_recovered_snapshot(tmp_path):
    from datetime import datetime, timezone
    from polybridge import tasks, store
    registry = tasks.TaskRegistry(log_dir=tmp_path)
    task = tasks.Task(task_id="failed", backend="codex", session_id=None, repo_path=tmp_path, prompt="Prompt", max_turns=5, log_path=tmp_path / "failed.jsonl", started_at=datetime.now(timezone.utc))
    task.status = "failed"; task.exit_code = 1; task.finished_at = datetime.now(timezone.utc)
    task.stderr_tail.append("Not inside a trusted directory")
    registry.persist(task)
    record = store.read(tmp_path, "failed")
    assert record.stderr_tail == ["Not inside a trusted directory"]
    assert store.snapshot(tmp_path, record)["stderr_tail"] == record.stderr_tail


def test_authoritative_opencode_auth_failure_reason_excludes_headers_and_tool_claims(tmp_path):
    log = tmp_path / "errors.jsonl"
    log.write_text(json.dumps({"type": "text", "part": {"text": "Invented API error"}}) + "\n" + json.dumps({"type": "error", "error": {"name": "APIError", "data": {"message": "Unauthorized Invalid API Key", "statusCode": 401, "responseHeaders": {"Authorization": "secret"}}}}) + "\n")
    diagnostic = w.failure_diagnostic({"backend": "opencode", "raw_stream_log": str(log), "stderr_tail": []}, "Create workflow")
    assert "authentication failed (HTTP 401)" in diagnostic
    assert "secret" not in diagnostic and "Invented" not in diagnostic


def test_protocol_failure_reason_does_not_use_nested_tool_error_or_assistant_prose(tmp_path):
    log = tmp_path / "tool.jsonl"
    log.write_text(json.dumps({"type": "tool_use", "part": {"error": {"name": "APIError", "data": {"message": "Imaginary failure"}}}}) + "\n")
    assert w.failure_diagnostic({"backend": "opencode", "raw_stream_log": str(log)}, "Request") == ""


def test_natural_workflow_names_keep_exact_storage_identity(storage):
    natural = "Feature Implementation v2"
    saved = storage.save(natural, definition(), 0)
    assert saved["name"] == natural
    assert (storage.definitions / f"{natural}.json").exists()
    assert storage.get(natural) == saved
    storage.save("Feature_Implementation_v2", definition(), 0)
    changed = storage.save(natural, {**saved, "description": "changed"}, 1)
    assert changed["revision"] == 2
    assert storage.get("Feature_Implementation_v2")["revision"] == 1
    assert {d["name"] for d in storage.list()} == {natural, "Feature_Implementation_v2"}
    storage.delete(natural)
    assert storage.get("Feature_Implementation_v2")["revision"] == 1


def test_workflow_spaces_do_not_relax_node_identifiers():
    graph = definition()
    graph["name"] = "Natural Workflow Name"
    assert w.validate_definition(graph)["name"] == graph["name"]
    graph["nodes"][1]["id"] = "work item"
    with pytest.raises(w.WorkflowError, match="Identifiers"):
        w.validate_definition(graph)


@pytest.mark.parametrize("backend,setting", [("codex", {"max_turns": 5}), ("vibe", {"reasoning_effort": "xhigh"})])
async def test_unsupported_primary_settings_use_ordered_fallback(storage, tmp_path, backend, setting):
    graph = definition()
    graph["nodes"][1]["agent"] = {"backend": backend, **setting, "fallbacks": [{"backend": "claude"}]}
    run, registry = await execute(storage, tmp_path, graph, ["done"])
    assert run["status"] == "completed"
    assert len(registry.calls) == 1
    assert registry.calls[0][1]["backend"].name == "claude"
    attempts = run["activations"][0]["tasks"]
    assert attempts[0]["status"] == "not_started"
    assert attempts[1]["freedom"] == attempts[0]["freedom"]


@pytest.mark.parametrize("refusal,expected", [(w.backends.UnsupportedCapability, "completed"), (w.backends.NestedDispatchRefused, "needs_attention")])
async def test_capability_refusal_fallback_never_bypasses_lineage(storage, tmp_path, refusal, expected):
    graph = definition()
    graph["nodes"][1]["agent"]["fallbacks"] = [{"backend": "claude"}]
    run = storage.create_run(w.validate_definition(graph), "task", tmp_path)
    registry = FakeRegistry(storage.root, ["done"])
    original = registry.start
    calls = []
    async def start(prompt, repo, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise refusal("authoritative pre-spawn refusal", rule="backend") if refusal is w.backends.NestedDispatchRefused else refusal("authoritative pre-spawn refusal")
        return await original(prompt, repo, **kwargs)
    registry.start = start
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    result = storage.get_run(run["workflow_run_id"])
    assert result["status"] == expected
    assert len(calls) == (2 if expected == "completed" else 1)
    assert result["activations"][0]["tasks"][0]["status"] == "not_started"


def test_malformed_primary_settings_rejected_even_with_fallback():
    for setting in [{"max_turns": True}, {"reasoning_effort": "made-up"}, {"model": 7}]:
        with pytest.raises(w.WorkflowError):
            w._candidate({"backend": "codex", **setting, "fallbacks": [{"backend": "claude"}]})


@pytest.mark.parametrize("backend", ["claude", "codex", "opencode", "vibe", "antigravity"])
@pytest.mark.parametrize("status", [500, 502, 503, 504, 529])
def test_authoritative_provider_http_outage_fallback(tmp_path, backend, status):
    path = tmp_path / "stream.jsonl"
    event = {"type": "turn.failed" if backend == "codex" else "error", "error": {"status": status}}
    path.write_text(json.dumps(event) + "\n")
    assert w.availability_failure({"status": "failed", "backend": backend, "raw_stream_log": str(path)})


@pytest.mark.parametrize("code", ["overloaded_error", "api_error", "api_connection_error"])
def test_claude_authoritative_provider_error_codes(tmp_path, code):
    path = tmp_path / "stream.jsonl"
    path.write_text(json.dumps({"type": "error", "error": {"type": code, "message": "provider unavailable"}}))
    assert w.availability_failure({"status": "failed", "backend": "claude", "raw_stream_log": str(path)})


@pytest.mark.parametrize("event", [
    {"type": "assistant", "error": {"type": "overloaded_error", "status": 503}},
    {"type": "tool_result", "error": {"status": 503}},
    {"type": "result", "result": "API Error: 503 service unavailable", "is_error": True},
    {"type": "error", "error": {"status": 403, "type": "permission_error"}},
])
def test_provider_fallback_ignores_claims_tools_and_security_refusals(tmp_path, event):
    path = tmp_path / "stream.jsonl"
    path.write_text(json.dumps(event))
    assert w.availability_failure({"status": "failed", "backend": "claude", "raw_stream_log": str(path), "summary": "API Error: 503"}) is None


@pytest.mark.parametrize("diagnostic", ["API Error: 503 Service unavailable", "APIConnectionError: connection refused", "Provider Error: provider timeout"])
def test_provider_transport_stderr_fallback(diagnostic):
    assert w.availability_failure({"status": "failed", "backend": "claude", "stderr_tail": [diagnostic]})


def test_provider_domain_failure_diagnostic_not_transport():
    assert w.availability_failure({"status": "failed", "backend": "claude", "stderr_tail": ["Test failed: expected HTTP 503", "Review says provider timeout"], "summary": "overloaded_error"}) is None


async def test_provider_outage_exhausts_chain_without_permission_change(storage, tmp_path):
    graph = definition()
    graph["nodes"][1]["agent"]["fallbacks"] = [{"backend": "claude"}]
    run, registry = await execute(storage, tmp_path, graph, [
        {"summary": "", "status": "failed", "backend": "codex", "stderr": ["API Error: 503 Service unavailable"]},
        {"summary": "", "status": "failed", "backend": "claude", "stderr": ["API Error: 529 overloaded_error"]},
    ])
    assert run["status"] == "needs_attention"
    assert "All agents unavailable" in run["attention_reason"]
    assert len(registry.calls) == 2
    assert len({call[1]["freedom"] for call in registry.calls}) == 1
    assert len({call[1]["network"] for call in registry.calls}) == 1


@pytest.mark.parametrize("diagnostic", [
    "HTTP Error: 403 Forbidden (request failed after 500 ms)",
    "APIError: 400 Bad Request request ID 500",
    "API Error: 401 Unauthorized; service unavailable",
    "Provider Error: HTTP status 403 connection refused",
    "HTTP Error: 404 missing request 503",
])
def test_provider_stderr_client_status_never_outage_from_later_numbers(diagnostic):
    assert w.availability_failure({"status": "failed", "backend": "claude", "stderr_tail": [diagnostic]}) is None


@pytest.mark.parametrize("diagnostic", ["API Error: HTTP 503 unavailable", "HTTP Error: status code: 502 bad gateway", "Provider Error: status=504 timeout"])
def test_provider_stderr_status_position_recognized(diagnostic):
    assert w.availability_failure({"status": "failed", "backend": "claude", "stderr_tail": [diagnostic]})


@pytest.mark.parametrize("axis", ["x", "y"])
@pytest.mark.parametrize("coordinate", [-1, -0.01])
@pytest.mark.parametrize("preview", [False, True])
def test_negative_canvas_positions_are_rejected(axis, coordinate, preview):
    graph = definition()
    graph["nodes"][1]["position"] = {"x": 0, "y": 0, axis: coordinate}
    with pytest.raises(w.WorkflowError, match="nonnegative"):
        if preview:
            w.validate_builder_preview(graph, "example")
        else:
            w.validate_definition(graph)


@pytest.mark.parametrize("preview", [False, True])
def test_nonnegative_canvas_positions_preserve_zero_and_large_expandable_layout(preview):
    graph = definition()
    graph["nodes"][0]["position"] = {"x": 0, "y": 0}
    graph["nodes"][1]["position"] = {"x": 20000, "y": 30000}
    actual = w.validate_builder_preview(graph, "example") if preview else w.validate_definition(graph)
    assert actual["nodes"][0]["position"] == {"x": 0, "y": 0}
    assert actual["nodes"][1]["position"] == {"x": 20000, "y": 30000}


async def test_builder_receives_canvas_geometry_and_spacing_guidance(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    run = await w.build_workflow("example", "Refine step instructions", agent={"backend": "codex"}, definition=definition(), root=storage.root)
    registry = FakeRegistry(storage.root, [json.dumps(definition())])
    await w.WorkflowSupervisor(registry, storage).build(run["workflow_run_id"])
    prompt = registry.calls[0][0]
    assert w.BUILDER_LAYOUT_GUIDANCE in prompt
    assert "200 x 92" in prompt and "72 x 72" in prompt
    assert "40 points" in prompt and "nonnegative" in prompt
    assert "Adding nodes or refining instructions is not permission to move existing nodes" in prompt


def optional_parallel_definition():
    graph = parallel_definition()
    next(n for n in graph["nodes"] if n["id"] == "left")["optional"] = True
    return graph


@pytest.mark.parametrize("value", [1, "true", None])
def test_optional_node_requires_strict_boolean(value):
    graph = optional_parallel_definition()
    next(n for n in graph["nodes"] if n["id"] == "left")["optional"] = value
    with pytest.raises(w.WorkflowError, match="boolean"):
        w.validate_definition(graph)


def test_optional_sequential_and_all_optional_forks_rejected():
    graph = definition()
    graph["nodes"][1]["optional"] = True
    with pytest.raises(w.WorkflowError, match="safe parallel"):
        w.validate_definition(graph)
    graph = optional_parallel_definition()
    next(n for n in graph["nodes"] if n["id"] == "right")["optional"] = True
    with pytest.raises(w.WorkflowError, match="safe parallel"):
        w.validate_definition(graph)


def test_optional_cannot_bypass_required_successor():
    graph = optional_parallel_definition()
    graph["nodes"].append({"id": "must", "type": "agent", "agent": {"backend": "codex"}})
    next(e for e in graph["connections"] if e["id"] == "left-join")["target"] = "must"
    graph["connections"].append({"id": "must-join", "source": "must", "target": "join"})
    with pytest.raises(w.WorkflowError, match="required successor"):
        w.validate_definition(graph)


async def test_optional_parallel_failure_arrives_join_with_failed_evidence(storage, tmp_path):
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both"})
    run, registry = await execute(storage, tmp_path, optional_parallel_definition(), ["split", decision, {"summary": "failed check", "status": "failed"}, "required success"])
    assert run["status"] == "completed"
    assert len(registry.calls) == 4
    assert run["joins"] == {}
    failed = next(a for a in run["activations"] if a["node_id"] == "left")
    assert failed["status"] == "failed"
    assert failed["optional_failure"] is True
    assert failed["result"]["failure_evidence"]["summary"] == "failed check"


async def test_required_parallel_failure_still_needs_attention(storage, tmp_path):
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both"})
    run, registry = await execute(storage, tmp_path, parallel_definition(), ["split", decision, {"summary": "failed", "status": "failed"}, "required success"])
    assert run["status"] == "needs_attention"


async def test_single_selected_optional_failure_has_no_tolerance(storage, tmp_path):
    decision = json.dumps({"action": "continue", "connections": ["left-edge"], "reason": "Only optional"})
    run, registry = await execute(storage, tmp_path, optional_parallel_definition(), ["split", decision, {"summary": "failed", "status": "failed"}])
    assert run["status"] == "needs_attention"
    assert not any(a.get("optional_failure") for a in run["activations"])


async def test_optional_fallback_exhaustion_continues_required_branch(storage, tmp_path):
    graph = optional_parallel_definition()
    next(n for n in graph["nodes"] if n["id"] == "left")["agent"]["fallbacks"] = [{"backend": "claude"}]
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both"})
    run, registry = await execute(storage, tmp_path, graph, ["split", decision, {"summary": "", "status": "failed", "backend": "codex", "stderr": ["API Error: 503 unavailable"]}, {"summary": "", "status": "failed", "backend": "claude", "stderr": ["API Error: 529 overloaded_error"]}, "required success"])
    assert run["status"] == "completed"
    assert any(a.get("optional_failure") for a in run["activations"])


async def test_optional_invalid_planning_json_does_not_create_or_complete_tasks(storage, tmp_path):
    graph = optional_parallel_definition()
    next(n for n in graph["nodes"] if n["id"] == "left")["role"] = "planning"
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both"})
    run, registry = await execute(storage, tmp_path, graph, ["split", decision, "not JSON", "required done"])
    assert run["status"] == "completed"
    assert run["tasks"] == []
    assert next(a for a in run["activations"] if a["node_id"] == "left")["status"] == "failed"


def test_optional_failure_never_escapes_unsafe_inner_group():
    graph = w.validate_definition(optional_parallel_definition())
    left = next(n for n in graph["nodes"] if n["id"] == "left")
    run = {"definition": graph, "status": "running", "joins": {
        "outer": {"join_id": "join", "selected_targets": ["left", "right"]},
        "inner": {"join_id": "join", "selected_targets": ["left"]},
    }}
    assert w.optional_failure_join(run, left, {"stack": ["outer"]}) == "join"
    assert w.optional_failure_join(run, left, {"stack": ["outer", "inner"]}) is None


async def test_optional_unsupported_settings_then_missing_fallback_can_continue(storage, tmp_path, monkeypatch):
    graph = optional_parallel_definition()
    next(n for n in graph["nodes"] if n["id"] == "left")["agent"] = {"backend": "codex", "max_turns": 5, "fallbacks": [{"backend": "claude"}]}
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: backend.name != "claude")
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both"})
    run, registry = await execute(storage, tmp_path, graph, ["split", decision, "required done"])
    assert run["status"] == "completed"
    failed = next(a for a in run["activations"] if a["node_id"] == "left")
    assert failed["optional_failure"] is True
    assert failed["tasks"][0]["status"] == "not_started"


async def test_optional_enforcement_refusal_does_not_bypass_limits(storage, tmp_path):
    graph = optional_parallel_definition()
    left = next(n for n in graph["nodes"] if n["id"] == "left")
    left.update(freedom="read_only", network=True)
    decision = json.dumps({"action": "continue", "connections": ["left-edge", "right-edge"], "reason": "Both"})
    run, registry = await execute(storage, tmp_path, graph, ["split", decision, "required done"])
    assert run["status"] == "needs_attention"
    assert not any(a.get("optional_failure") for a in run["activations"])


def test_selected_all_optional_subset_requires_actual_required_sibling():
    graph = optional_parallel_definition()
    graph["nodes"].append({"id": "third", "type": "agent", "optional": True, "agent": {"backend": "codex"}})
    graph["connections"].extend([{"id": "third-edge", "source": "work", "target": "third"}, {"id": "third-join", "source": "third", "target": "join"}])
    graph = w.validate_definition(graph)
    work = next(n for n in graph["nodes"] if n["id"] == "work")
    edges = [e for e in graph["connections"] if e["id"] in {"left-edge", "third-edge"}]
    with pytest.raises(w.WorkflowError, match="actually selected required"):
        w.validate_selection(graph, work, edges, {"stack": []}, {})


async def test_recovered_optional_failure_result_does_not_repeat_worker(storage, tmp_path):
    graph = w.validate_definition(optional_parallel_definition())
    run = storage.create_run(graph, "task", tmp_path)
    token = {"id": "optional-token", "node_id": "left", "stack": ["group"], "execution_complete": True, "optional_failure_join": "join", "result": {"optional_failure": "observed failed", "failure_evidence": {"status": "failed"}}}
    arrived = {"id": "required-token", "node_id": "join", "stack": ["group"], "context": {"summary": "required done"}}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", pending=[token], joins={"group": {"join_id": "join", "split_id": "work", "selected_targets": ["left", "right"], "expected": 2, "arrived": [arrived], "stack": []}}), "recovered_fixture")
    registry = FakeRegistry(storage.root, [])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    result = storage.get_run(run["workflow_run_id"])
    assert result["status"] == "completed"
    assert result["joins"] == {}
    assert registry.calls == []


@pytest.mark.parametrize("availability", [False, True])
async def test_reconcile_optional_failure_at_task_snapshot_boundary_no_repeat(storage, tmp_path, availability):
    graph = w.validate_definition(optional_parallel_definition())
    left = next(n for n in graph["nodes"] if n["id"] == "left")
    left["max_attempts"] = 1
    if availability:
        left["agent"]["fallbacks"] = [{"backend": "claude", "max_turns": 100}]
    run = storage.create_run(graph, "task", tmp_path)
    token = {"id": "optional-token", "node_id": "left", "stack": ["group"]}
    arrived = {"id": "required-token", "node_id": "join", "stack": ["group"], "context": {"summary": "required done"}}
    failed = {"status": "failed", "backend": "codex", "summary": "observed failure", "stderr_tail": ["API Error: 503 unavailable"] if availability else []}
    activation = {"id": "activation", "node_id": "left", "role": "node", "status": "running", "token": token, "tasks": [{"task_id": "observed-task", "status": "failed", "candidate": left["agent"], "result": failed}]}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="needs_attention", pending=[token], activations=[activation], joins={"group": {"join_id": "join", "split_id": "work", "selected_targets": ["left", "right"], "expected": 2, "arrived": [arrived], "stack": []}}), "task_snapshot_boundary")
    reconciled = storage.reconcile_run(run["workflow_run_id"])
    assert reconciled["pending"][0]["recovered_failed_result"] == failed
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running"), "explicit_resume")
    registry = FakeRegistry(storage.root, ["fallback success"] if availability else [])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    result = storage.get_run(run["workflow_run_id"])
    assert result["status"] == "completed"
    assert len(registry.calls) == (1 if availability else 0)
    if availability:
        assert registry.calls[0][1]["backend"].name == "claude"
        assert len(result["activations"]) == 1
    else:
        assert result["activations"][0]["status"] == "failed"


def test_reconcile_single_selected_optional_preserves_explicit_retry(storage, tmp_path):
    graph = w.validate_definition(optional_parallel_definition())
    run = storage.create_run(graph, "task", tmp_path)
    token = {"id": "single-token", "node_id": "left", "stack": []}
    failed = {"status": "failed", "backend": "codex", "summary": "observed failure"}
    activation = {"id": "activation", "node_id": "left", "role": "node", "status": "running", "token": token, "tasks": [{"task_id": "failed-task", "status": "failed", "result": failed}]}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="needs_attention", pending=[token], activations=[activation]), "single_failure_boundary")
    recovered = storage.reconcile_run(run["workflow_run_id"])
    assert "recovered_failed_result" not in recovered["pending"][0]
    assert recovered["status"] == "needs_attention"


async def test_recovered_optional_fallback_completed_snapshot_survives_second_interruption(storage, tmp_path):
    graph = w.validate_definition(optional_parallel_definition())
    left = next(n for n in graph["nodes"] if n["id"] == "left")
    left["max_attempts"] = 1
    left["agent"]["fallbacks"] = [{"backend": "claude", "max_turns": 100}]
    run = storage.create_run(graph, "task", tmp_path)
    token = {"id": "token", "node_id": "left", "stack": ["group"]}
    failed = {"status": "failed", "backend": "codex", "summary": "", "stderr_tail": ["API Error: 503 unavailable"]}
    activation = {"id": "activation", "node_id": "left", "role": "node", "status": "running", "token": token, "tasks": [{"task_id": "first", "status": "failed", "candidate": left["agent"], "result": failed}]}
    arrived = {"id": "required", "node_id": "join", "stack": ["group"], "context": {"summary": "required done"}}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", pending=[token], activations=[activation], joins={"group": {"join_id": "join", "split_id": "work", "selected_targets": ["left", "right"], "expected": 2, "arrived": [arrived], "stack": []}}), "first_interruption")
    first = storage.reconcile_run(run["workflow_run_id"])
    registry = FakeRegistry(storage.root, ["fallback done"])
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = run["workflow_run_id"]
    from polybridge import identity
    import os
    storage.update_run(run["workflow_run_id"], lambda r: r.update(supervisor_pid=os.getpid(), supervisor_identity=identity.own_identity()), "supervisor_restored")
    original = supervisor._task_update
    class Interrupted(BaseException):
        pass
    def task_update(aid, tid, values):
        original(aid, tid, values)
        if values.get("status") == "completed":
            raise Interrupted()
    supervisor._task_update = task_update
    with pytest.raises(Interrupted):
        await supervisor._execute_node(left, first["pending"][0])
    second = storage.reconcile_run(run["workflow_run_id"])
    assert second["activations"][0]["status"] == "completed"
    assert "recovered_failed_result" not in second["pending"][0]
    assert second["pending"][0]["recovered_result"]["summary"] == "fallback done"
    fresh = FakeRegistry(storage.root, [])
    await w.WorkflowSupervisor(fresh, storage).execute(run["workflow_run_id"])
    assert storage.get_run(run["workflow_run_id"])["status"] == "completed"
    assert fresh.calls == []
    assert len(registry.calls) == 1
