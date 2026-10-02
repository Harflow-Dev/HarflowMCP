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
    return w.WorkflowStore(tmp_path)


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


@pytest.mark.parametrize("name", ["../bad", "", "/absolute", "x/y"])
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
    assert "orchestrator chooses one or multiple" in prompt
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
    with pytest.raises(w.WorkflowError, match="open split/join boundary"):
        w.validate_definition(graph)


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
    with pytest.raises(w.WorkflowError, match="does not support max_turns"):
        w.validate_definition(graph)
