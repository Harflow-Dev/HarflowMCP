"""Public execution-contract tests for orchestrator-directed delegation."""
import asyncio
import copy
import json
from pathlib import Path

import pytest

from polybridge import workflows as w
from polybridge import workflow_delegation as d


def graph(role="task"):
    return {"name": "delegation", "orchestrator": {"backend": "codex"}, "nodes": [{"id": "start", "type": "start", "prompt": "Overall purpose"}, {"id": "work", "type": "agent", "role": role, "instructions": "Node-only instruction", "agent": {"backend": "codex"}}, {"id": "end", "type": "end"}], "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}]}


def parallel(optional=False):
    g = graph()
    g["nodes"][1]["id"] = "left"
    g["nodes"][1]["optional"] = optional
    g["nodes"].insert(2, {"id": "right", "type": "agent", "instructions": "Other review", "agent": {"backend": "codex"}})
    g["nodes"].insert(3, {"id": "merge", "type": "agent", "instructions": "Reconcile inputs", "agent": {"backend": "codex"}})
    g["connections"] = [{"id": "left", "source": "start", "target": "left"}, {"id": "right", "source": "start", "target": "right"}, {"id": "left-merge", "source": "left", "target": "merge"}, {"id": "right-merge", "source": "right", "target": "merge"}, {"id": "finish", "source": "merge", "target": "end"}]
    return g


class Task:
    def __init__(self, tid, result):
        self.task_id = tid
        self.done = asyncio.Event()
        self.done.set()
        self.result = {"task_id": tid, "session_id": "session-" + tid, "backend": "codex", "status": "completed", **result}

    def snapshot(self):
        return self.result


class Registry:
    def __init__(self, root, policy=None, outputs=None):
        self._log_dir = root / "tasks"
        self.calls = []
        self.contexts = []
        self.policy = policy or default_decision
        self.outputs = outputs or {}
        self.tasks = {}
        self.context_bootstraps = {}

    def decode_context(self, prompt):
        if "Context:\n" in prompt:
            return json.JSONDecoder().raw_decode(prompt.split("Context:\n", 1)[1])[0]
        try:
            envelope = json.JSONDecoder().raw_decode(prompt)[0]
        except (ValueError, TypeError):
            return None
        if not isinstance(envelope, dict) or "checkpoint" not in envelope or "delivery" not in envelope:
            return None
        scope = envelope["delivery"]["acknowledgement"]["scope"]
        if "bootstrap" in envelope:
            self.context_bootstraps[scope] = copy.deepcopy(envelope["bootstrap"]["context"])
        return {**self.context_bootstraps[scope], **envelope["checkpoint"]}

    async def start(self, prompt, repo, **kwargs):
        self.calls.append((prompt, kwargs))
        context = self.decode_context(prompt)
        if context is not None:
            self.contexts.append(context)
            result = self.policy(context, self)
            if prompt.startswith("{"):
                envelope = json.JSONDecoder().raw_decode(prompt)[0]
                result = {**result, "context_ack": copy.deepcopy(envelope["delivery"]["acknowledgement"])}
        else:
            nid = kwargs["title"].split(" · ")[-1]
            result = self.outputs.get(nid, {"status": "succeeded", "result": {"summary": "Done"}, "evidence": ["observed"]})
            if callable(result):
                result = result(prompt, kwargs)
        snapshot = result if "summary" in result and "result" not in result else {"summary": json.dumps(result)}
        snapshot.setdefault("backend", kwargs["backend"].name)
        task = Task(kwargs["task_id"], snapshot)
        task.kwargs = copy.deepcopy(kwargs)
        task.repo = repo
        from polybridge import store
        store.write(self._log_dir, store.TaskRecord(task_id=task.task_id, backend=task.result["backend"], session_id=task.result["session_id"], repo_path=str(repo), started_at="2026-10-04T00:00:00Z", freedom=kwargs.get("freedom", "write_in_repo"), network=kwargs.get("network"), status=task.result["status"]))
        self.tasks[task.task_id] = task
        return task

    async def resume(self, previous, prompt, **kwargs):
        return await self.start(prompt, previous.repo, **{**previous.kwargs, **kwargs})

    def get(self, tid):
        return self.tasks.get(tid)

    async def cancel_cascade(self, *args, **kwargs):
        return {}


def default_decision(context, registry):
    choices = context["valid_continuations"]
    action = "continue" if choices else "complete"
    entries = [{"continuation_id": c["continuation_id"], **({"prompt": "Focused assignment for " + c["node_id"], "session_mode": "fresh"} if c["requires_prompt"] else {})} for c in choices]
    return {"decision_id": context["decision_id"], "action": action, "reason": "Evidence supports continuation", "next": entries}


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    return w.WorkflowStore(tmp_path)


async def run_flow(storage, tmp_path, definition=None, registry=None, *, guided=False):
    run = storage.create_run(w.validate_definition(definition or graph()), "ORIGINAL PRIVATE REQUEST", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda current: current.update(runner_policy="guided") if guided else current.pop("runner_policy", None), "test_runner_policy")
    registry = registry or Registry(storage.root)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    return storage.get_run(run["workflow_run_id"]), registry


async def test_assignment_isolation_display_and_durable_result(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path)
    assert run["status"] == "completed"
    assert run["execution_contract"] == "delegation"
    assert len(registry.contexts) == 3  # Start, worker routing, End completion
    workers = [(p, k) for p, k in registry.calls if "Assignment:\n" in p]
    assert len(workers) == 1
    prompt, kwargs = workers[0]
    assert "ORIGINAL PRIVATE REQUEST" not in prompt
    assert "workflow_graph" not in prompt and "checklist" not in prompt.lower()
    assert "Node-only instruction" in prompt
    assert kwargs["display_prompt"] == "Focused assignment for work"
    activation = next(a for a in run["activations"] if a["role"] == "node")
    assert activation["assignment_prompt"] == kwargs["display_prompt"]
    assert activation["node_result"]["status"] == "succeeded"
    assert json.loads(activation["raw_output"])["result"]["summary"] == "Done"
    assert all(c["original_request"] == "ORIGINAL PRIVATE REQUEST" for c in registry.contexts)
    assert not run["settling"]


@pytest.mark.parametrize("bad", [0, 11, True, "3"])
def test_decision_attempt_setting_rejects_invalid(bad):
    g = graph()
    g["max_decision_attempts"] = bad
    with pytest.raises(w.WorkflowError, match="max_decision_attempts"):
        w.validate_definition(g)


def test_new_defaults_are_agent_decides_and_three_decisions():
    definition = w.validate_definition(graph())
    assert definition["max_decision_attempts"] == 3
    assert definition["nodes"][1]["session_mode"] == "agent_decides"


@pytest.mark.parametrize("mutation", [lambda r: r.update(decision_id="stale"), lambda r: r["next"][0].update(continuation_id="invented"), lambda r: r["next"][0].pop("prompt"), lambda r: r["next"][0].update(additional_result_refs=["foreign"]), lambda r: r["next"][0].update(assigned_task_ids=["unknown"]), lambda r: r.update(action="failed")])
async def test_invalid_decisions_exhaust_without_dispatch(storage, tmp_path, mutation):
    def invalid(context, registry):
        result = default_decision(context, registry)
        mutation(result)
        return result
    run, registry = await run_flow(storage, tmp_path, registry=Registry(storage.root, invalid))
    assert run["status"] == "failed"
    assert "decision attempts" in run["failure_reason"]
    assert len(registry.contexts) == 3
    assert not any(a["role"] == "node" for a in run["activations"])
    assert len({c["decision_id"] for c in registry.contexts}) == 1
    assert all(a["raw_output"] for a in run["activations"])


async def test_invalid_then_valid_resets_at_next_stage(storage, tmp_path):
    def policy(c, registry):
        value = default_decision(c, registry)
        if len(registry.contexts) == 1:
            value["next"][0]["continuation_id"] = "wrong"
        return value
    run, registry = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy))
    assert run["status"] == "completed"
    assert len(registry.contexts) == 4
    assert registry.contexts[0]["decision_id"] == registry.contexts[1]["decision_id"]
    assert registry.contexts[2]["decision_id"] != registry.contexts[1]["decision_id"]
    assert "Correction:" in registry.calls[1][0]


async def test_parallel_barrier_requests_shared_assignment_after_both_results(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path, parallel())
    assert run["status"] == "completed"
    barrier = next(c for c in registry.contexts if c["current_stage"]["node_id"] == "merge" and c["current_stage"]["phase"] == "assignment")
    assert len(barrier["input_results"]) == 2
    assert {v["node_id"] for v in barrier["input_results"]} == {"left", "right"}
    assert len(barrier["valid_continuations"]) == 1
    assert barrier["valid_continuations"][0]["kind"] == "execute"
    arriving = [c for c in registry.contexts if c["current_stage"]["node_id"] in {"left", "right"}]
    assert all(not c["valid_continuations"][0]["requires_prompt"] for c in arriving)
    assert run["transitions"] == 5
    assert len([a for a in run["activations"] if a["node_id"] == "merge" and a["role"] == "node"]) == 1
    merged = next(a for a in run["activations"] if a["node_id"] == "merge" and a["role"] == "node")
    assert len(merged["input_result_refs"]) == 2


async def test_optional_protocol_failure_continues_and_is_visible(storage, tmp_path):
    registry = Registry(storage.root, outputs={"left": {"summary": "plain invalid output"}})
    run, registry = await run_flow(storage, tmp_path, parallel(True), registry)
    assert run["status"] == "completed"
    failed = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "left")
    assert failed["optional_failure"]
    assert failed["node_result"]["result"]["failure_kind"] == "protocol"
    merged = next(c for c in registry.contexts if c["current_stage"]["node_id"] == "merge" and c["current_stage"]["phase"] == "assignment")
    assert any(i["status"] == "failed" for i in merged["input_results"])


async def test_required_protocol_failure_routes_to_input_without_rerun(storage, tmp_path):
    def policy(context, registry):
        if context["current_stage"]["node_id"] == "work":
            assert context["valid_continuations"] == []
            return {"decision_id": context["decision_id"], "action": "needs_input", "reason": "Required worker output invalid", "question": "How should we recover?"}
        return default_decision(context, registry)
    run, registry = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy, {"work": {"summary": "invalid"}}))
    assert run["status"] == "needs_input"
    assert run["input_question"] == "How should we recover?"
    assert len([a for a in run["activations"] if a["role"] == "node"]) == 1
    assert next(a for a in run["activations"] if a["role"] == "node")["raw_output"] == "invalid"
    assert not run["settling"]


async def test_needs_input_resume_preserves_checkpoint_and_answer(storage, tmp_path):
    def policy(c, registry):
        if len(registry.contexts) == 1:
            return {"decision_id": c["decision_id"], "action": "needs_input", "reason": "Clarification", "question": "Which behavior?"}
        return default_decision(c, registry)
    run, registry = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy))
    assert run["status"] == "needs_input"
    checkpoint = run["pending"][0]["decision_id"]
    with pytest.raises(w.WorkflowError, match="answer"):
        storage.control(run["workflow_run_id"], "resume")
    storage.control(run["workflow_run_id"], "resume", instructions="Use the safe behavior", decision_id=run["input_decision_id"])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    result = storage.get_run(run["workflow_run_id"])
    assert result["status"] == "completed"
    assert registry.contexts[1]["decision_id"] == checkpoint
    assert registry.contexts[1]["recovery_instructions"] == "Use the safe behavior"
    assert len([a for a in result["activations"] if a["role"] == "node"]) == 1


async def test_failed_decision_recovery_requires_reason_and_reset(storage, tmp_path):
    def invalid(c, registry):
        return {"decision_id": "wrong", "action": "failed", "reason": "Wrong ID"}
    run, _ = await run_flow(storage, tmp_path, registry=Registry(storage.root, invalid))
    checkpoint = run["pending"][0]["decision_id"]
    with pytest.raises(w.WorkflowError, match="reason"):
        storage.control(run["workflow_run_id"], "recover")
    recovered = storage.control(run["workflow_run_id"], "recover", instructions="Inspected invalid decisions; retry")
    assert recovered["pending"][0]["decision_attempts"] == 0
    assert recovered["pending"][0]["decision_id"] == checkpoint
    registry = Registry(storage.root)
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert storage.get_run(run["workflow_run_id"])["status"] == "completed"
    assert registry.contexts[0]["recovery_instructions"] == "Inspected invalid decisions; retry"


@pytest.mark.parametrize("status", ["completed", "cancelled"])
def test_completed_cancelled_cannot_recover(storage, tmp_path, status):
    run = storage.create_run(w.validate_definition(graph()), "request", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status=status), "fixture")
    with pytest.raises(w.WorkflowError, match="failed delegation"):
        storage.control(run["workflow_run_id"], "recover", instructions="Retry")


def test_historical_control_cannot_reinterpret(storage, tmp_path):
    run = storage.create_run(w.validate_definition(graph()), "request", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: (r.pop("execution_contract"), r.update(status="paused")), "historical")
    with pytest.raises(w.WorkflowError, match="Historical"):
        storage.control(run["workflow_run_id"], "resume", instructions="Resume")


async def test_planning_assigned_descriptors_and_checklist_authority(storage, tmp_path):
    g = graph("planning")
    g["nodes"].insert(2, {"id": "implement", "type": "agent", "role": "implementation", "instructions": "Implement", "agent": {"backend": "codex"}})
    g["connections"][1]["target"] = "implement"
    g["connections"].append({"id": "done", "source": "implement", "target": "end"})
    def policy(c, registry):
        value = default_decision(c, registry)
        if c["current_stage"]["node_id"] == "work":
            value["next"][0]["assigned_task_ids"] = ["one"]
        if c["current_stage"]["node_id"] == "implement":
            assert c["checklist"][0]["status"] == "pending"
            value["task_updates"] = [{"task_id": "one", "status": "completed", "reason": "Observed implementation evidence"}]
        return value
    outputs = {"work": {"status": "succeeded", "result": {"tasks": [{"id": "one", "title": "Implement", "description": "Bounded work"}], "technical_plan": "Implement the parser and validate empty input"}, "evidence": []}, "implement": {"status": "succeeded", "result": {"completed_task_ids": ["one"]}, "evidence": ["Tests passed"]}}
    run, registry = await run_flow(storage, tmp_path, g, Registry(storage.root, policy, outputs))
    assert run["status"] == "completed"
    assert run["tasks"][0]["status"] == "completed"
    prompt = next(p for p, k in registry.calls if k["title"].endswith(" · implement"))
    descriptors = json.loads(prompt.split("Assigned task descriptors:\n")[1].split("\nInput results:")[0])
    assert descriptors == [{"id": "one", "title": "Implement", "description": "Bounded work"}]
    assert "ORIGINAL PRIVATE REQUEST" not in prompt


async def test_unassigned_completion_is_protocol_failure(storage, tmp_path):
    def policy(c, registry):
        if c["current_stage"]["node_id"] == "work":
            return {"decision_id": c["decision_id"], "action": "failed", "reason": "Invalid worker completion"}
        return default_decision(c, registry)
    output = {"status": "succeeded", "result": {"completed_task_ids": ["unassigned"]}, "evidence": []}
    run, _ = await run_flow(storage, tmp_path, graph("implementation"), Registry(storage.root, policy, {"work": output}))
    activation = next(a for a in run["activations"] if a["role"] == "node")
    assert activation["node_result"]["status"] == "failed"
    assert "assigned" in activation["result_error"]


async def test_review_changes_and_failed_check_are_successful_execution(storage, tmp_path):
    registry = Registry(storage.root, outputs={"work": {"status": "succeeded", "result": {"verdict": "changes_needed", "findings": ["Test fails"]}, "evidence": ["Observed"]}})
    run, _ = await run_flow(storage, tmp_path, graph("review"), registry)
    assert run["status"] == "completed"
    assert next(a for a in run["activations"] if a["role"] == "node")["node_result"]["status"] == "succeeded"


def test_full_result_retained_preview_has_provenance():
    run = {"definition": {"nodes": [{"id": "work", "role": "task"}]}, "activations": [{"id": "activation", "role": "node", "node_id": "work", "status": "completed", "tasks": [], "node_result": {"status": "succeeded", "result": {"large": "x" * 50000}, "evidence": []}}]}
    preview = d.result_inputs(run, ["activation"])[0]
    assert preview["truncated"] and preview["status"] == "succeeded" and preview["result_ref"] == "activation"
    assert len(d.result_inputs(run, ["activation"], preview=False)[0]["node_result"]["result"]["large"]) == 50000
    assert len(run["activations"][0]["node_result"]["result"]["large"]) == 50000
    run["activations"][0]["tasks"] = [{"status": "uncertain"}]
    with pytest.raises(w.WorkflowError, match="settled"):
        d.result_inputs(run, ["activation"])


def test_migration_backup_preserves_graph_and_revision(storage, tmp_path):
    original = graph()
    original["nodes"][1]["session_mode"] = "resume"
    saved = storage.save("delegation", original)
    saved.pop("execution_contract")
    w._write(storage.definitions / "delegation.json", saved)
    before = copy.deepcopy(saved)
    result = w.migrate_workflows(tmp_path)
    assert result["migrated"] == ["delegation"]
    assert json.loads(Path(result["backups"][0]).read_text()) == before
    migrated = storage.get("delegation")
    assert migrated["revision"] == before["revision"] + 1
    assert migrated["connections"] == before["connections"]
    assert migrated["nodes"][0] == before["nodes"][0]
    expected = {**before["nodes"][1], "session_mode": "agent_decides"}
    assert migrated["nodes"][1] == expected
    assert w.migrate_workflows(tmp_path)["migrated"] == []


def test_migration_refuses_active_historical_runs(storage, tmp_path):
    saved = storage.save("delegation", graph())
    run = storage.create_run(saved, "task", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.pop("execution_contract"), "historical")
    with pytest.raises(w.WorkflowError, match="Active historical"):
        w.migrate_workflows(tmp_path)
    assert storage.get("delegation") == saved


async def test_invalid_parallel_assignment_dispatches_no_branch(storage, tmp_path):
    def policy(c, registry):
        value = default_decision(c, registry)
        value["next"][-1].pop("prompt", None)
        return value
    run, _ = await run_flow(storage, tmp_path, parallel(), Registry(storage.root, policy))
    assert run["status"] == "failed"
    assert not any(a["role"] == "node" for a in run["activations"])
    assert run["transitions"] == 0


async def test_harness_outage_uses_ordered_fallback_with_same_assignment(storage, tmp_path):
    g = graph()
    g["nodes"][1]["agent"] = {"backend": "claude", "fallbacks": [{"backend": "codex"}]}
    attempts = []
    def output(prompt, kwargs):
        attempts.append(kwargs["backend"].name)
        if len(attempts) == 1:
            return {"status": "failed", "summary": "", "stderr_tail": ["API Error: 503 Service unavailable"], "backend": "claude"}
        return {"status": "succeeded", "result": {"summary": "Fallback performed assignment"}, "evidence": []}
    run, registry = await run_flow(storage, tmp_path, g, Registry(storage.root, outputs={"work": output}))
    assert run["status"] == "completed"
    assert attempts == ["claude", "codex"]
    activation = next(a for a in run["activations"] if a["role"] == "node")
    assert len(activation["tasks"]) == 2
    assert activation["node_result"]["status"] == "succeeded"
    worker_calls = [k for p, k in registry.calls if "Assignment:\n" in p]
    assert all(k["display_prompt"] == activation["assignment_prompt"] for k in worker_calls)


def recovered_fixture(storage, tmp_path, *, status="completed", candidate="codex", summary=None, fallback=False):
    g = graph()
    if fallback:
        g["nodes"][1]["agent"] = {"backend": "claude", "fallbacks": [{"backend": "codex"}]}
    run = storage.create_run(w.validate_definition(g), "Private original", tmp_path)
    node = run["definition"]["nodes"][1]
    token = {"id": "token", "node_id": "work", "stack": [], "context": {}, "assignment_prompt": "Persisted assignment", "assigned_task_ids": [], "input_result_refs": [], "execution_activation_id": "execution"}
    output = summary or json.dumps({"status": "succeeded", "result": {"summary": "Durable result"}, "evidence": []})
    snapshot = {"status": status, "summary": output, "backend": candidate, "task_id": "attempt", "stderr_tail": ["API Error: 503 Service unavailable"] if status == "failed" else []}
    activation = {"id": "execution", "node_id": "work", "role": "node", "status": "running", "assignment_prompt": token["assignment_prompt"], "assigned_task_ids": [], "token": copy.deepcopy(token), "tasks": [{"task_id": "attempt", "candidate": copy.deepcopy(node["agent"]), "status": status, "result": snapshot}]}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", pending=[token], activations=[activation]), "recovered_fixture")
    return run["workflow_run_id"]


async def test_recovered_completed_worker_is_not_repeated(storage, tmp_path):
    rid = recovered_fixture(storage, tmp_path)
    registry = Registry(storage.root)
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    run = storage.get_run(rid)
    assert run["status"] == "completed"
    assert not any("Assignment:\n" in prompt for prompt, _ in registry.calls)
    assert len([a for a in run["activations"] if a["role"] == "node"]) == 1
    assert run["activations"][0]["node_result"]["result"]["summary"] == "Durable result"


async def test_recovered_failed_candidate_continues_fallback_without_replay(storage, tmp_path):
    rid = recovered_fixture(storage, tmp_path, status="failed", candidate="claude", fallback=True)
    registry = Registry(storage.root)
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    run = storage.get_run(rid)
    assert run["status"] == "completed"
    workers = [kwargs for prompt, kwargs in registry.calls if "Assignment:\n" in prompt]
    assert len(workers) == 1 and workers[0]["backend"].name == "codex"
    assert workers[0]["display_prompt"] == "Persisted assignment"
    assert len(run["activations"][0]["tasks"]) == 2
    assert run["activations"][0]["node_result"]["status"] == "succeeded"


async def test_ambiguous_spawn_never_redispatches_or_resumes(storage, tmp_path):
    rid = recovered_fixture(storage, tmp_path, status="reserved")
    registry = Registry(storage.root)
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    run = storage.get_run(rid)
    assert run["status"] == "needs_attention" and run["settling"]
    assert not registry.calls
    assert run["activations"][0]["tasks"][0]["status"] == "uncertain"
    with pytest.raises(w.WorkflowError, match="Unresolved"):
        storage.control(rid, "resume", instructions="Retry")


async def test_parallel_input_suspension_settles_sibling_and_resume_does_not_repeat(storage, tmp_path, monkeypatch):
    right_started = asyncio.Event()
    input_accepted = asyncio.Event()
    release_right = asyncio.Event()
    saw_input = False
    def policy(context, registry):
        nonlocal saw_input
        if context["current_stage"]["node_id"] == "left" and not saw_input:
            saw_input = True
            return {"decision_id": context["decision_id"], "action": "needs_input", "reason": "Inspect branch result", "question": "Continue?"}
        return default_decision(context, registry)
    class ControlledRegistry(Registry):
        async def start(self, prompt, repo, **kwargs):
            if "Context:\n" in prompt:
                c = json.JSONDecoder().raw_decode(prompt.split("Context:\n", 1)[1])[0]
                if c["current_stage"]["node_id"] == "left":
                    await right_started.wait()
            task = await super().start(prompt, repo, **kwargs)
            if kwargs["title"].endswith(" · right"):
                task.done = release_right
                right_started.set()
            return task
    update = storage.update_run
    def observe(*args, **kwargs):
        result = update(*args, **kwargs)
        if result["status"] == "needs_input":
            input_accepted.set()
        return result
    monkeypatch.setattr(storage, "update_run", observe)
    definition = parallel()
    for node in definition["nodes"]:
        if node["type"] == "agent":
            node["freedom"] = "read_only"
    run = storage.create_run(w.validate_definition(definition), "Request", tmp_path)
    registry = ControlledRegistry(storage.root, policy)
    executing = asyncio.create_task(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]))
    await asyncio.wait_for(input_accepted.wait(), 3)
    pending = storage.get_run(run["workflow_run_id"])
    assert pending["status"] == "needs_input" and pending["settling"]
    from polybridge import identity
    monkeypatch.setattr(identity, "identity_check", lambda *args: "alive")
    with pytest.raises(w.WorkflowError, match="Unresolved"):
        storage.control(run["workflow_run_id"], "resume", instructions="Continue")
    release_right.set()
    await asyncio.wait_for(executing, 3)
    suspended = storage.get_run(run["workflow_run_id"])
    assert suspended["status"] == "needs_input" and not suspended["settling"]
    right = next(a for a in suspended["activations"] if a["role"] == "node" and a["node_id"] == "right")
    assert right["node_result"]["status"] == "succeeded"
    storage.control(run["workflow_run_id"], "resume", instructions="Continue after inspection", decision_id=suspended["input_decision_id"])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    completed = storage.get_run(run["workflow_run_id"])
    assert completed["status"] == "completed"
    assert len([a for a in completed["activations"] if a["role"] == "node" and a["node_id"] in {"left", "right"}]) == 2


async def test_old_execution_inventory_remains_discoverable(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path, parallel())
    final_context = registry.contexts[-1]
    assert {a["node_id"] for a in final_context["settled_executions"]} == {"left", "right", "merge"}
    assert all(a["execution_id"] == a["result_ref"] for a in final_context["settled_executions"])
    assert not any("node_result" in a for a in final_context["settled_executions"])
    ref = final_context["settled_executions"][0]["result_ref"]
    assert d.result_inputs(run, [ref], preview=False)[0]["node_result"]["status"] == "succeeded"


@pytest.mark.parametrize("status,extra", [("cancelled", {}), ("failed", {"permission_denials": ["Denied write"]})])
async def test_optional_unsafe_task_outcome_never_bypasses(storage, tmp_path, status, extra):
    registry = Registry(storage.root, outputs={"left": {"status": status, "summary": "", **extra}})
    run, _ = await run_flow(storage, tmp_path, parallel(True), registry)
    assert run["status"] == "needs_attention"
    activation = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "left")
    assert activation["node_result"]["status"] == "blocked"
    assert not activation.get("optional_failure")
    assert not any(a["node_id"] == "merge" for a in run["activations"])


async def test_optional_exhausted_enforcement_refusal_never_bypasses(storage, tmp_path, monkeypatch):
    g = parallel(True)
    g["nodes"][1]["agent"] = {"backend": "claude", "fallbacks": [{"backend": "claude"}]}
    backend = w.backends.get("claude")
    def refuse(*args, **kwargs):
        raise w.backends.UnsupportedCapability("Cannot enforce required access")
    monkeypatch.setattr(type(backend), "enforcement", refuse)
    # Orchestrator is a different harness and remains available.
    run, _ = await run_flow(storage, tmp_path, g)
    assert run["status"] == "needs_attention"
    a = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "left")
    assert a["node_result"]["status"] == "blocked"
    assert not a.get("optional_failure")


@pytest.mark.parametrize("limit", [0, 3])
async def test_explicit_recovery_grant_unlocks_exhausted_retry(storage, tmp_path, limit):
    g = graph()
    g["connections"].append({"id": "retry", "source": "work", "target": "work", "condition": "Repeat once after inspection", "max_retries": limit})
    run = storage.create_run(w.validate_definition(g), "Request", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: (r.pop("runner_policy", None), r.update(retry_counts={"retry": limit})), "fixture")
    def policy(c, registry):
        result = default_decision(c, registry)
        if c["current_stage"]["node_id"] == "work":
            result["next"] = [{"continuation_id": "retry", "prompt": "Repeat the bounded assignment", "session_mode": "fresh"}]
        return result
    registry = Registry(storage.root, policy)
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    failed = storage.get_run(run["workflow_run_id"])
    assert failed["status"] == "failed"
    assert failed["exhausted_retry_edges"] == ["retry"]
    assert len([a for a in failed["activations"] if a["role"] == "node"]) == 1
    storage.control(run["workflow_run_id"], "recover", instructions="Inspected; allow one extra retry", additional_attempts=1)
    did_retry = False
    def recovered_policy(c, registry):
        nonlocal did_retry
        if c["current_stage"]["node_id"] == "work" and not did_retry:
            did_retry = True
            result = default_decision(c, registry)
            result["next"] = [{"continuation_id": "retry", "prompt": "Retry after explicit grant", "session_mode": "fresh"}]
            return result
        result = default_decision(c, registry)
        if c["current_stage"]["node_id"] == "work":
            result["next"] = [{"continuation_id": "finish"}]
        return result
    after = Registry(storage.root, recovered_policy)
    await w.WorkflowSupervisor(after, storage).execute(run["workflow_run_id"])
    complete = storage.get_run(run["workflow_run_id"])
    assert complete["status"] == "completed"
    assert complete["retry_counts"]["retry"] == limit + 1
    assert complete["retry_grants"]["retry"] == 1
    assert len([a for a in complete["activations"] if a["role"] == "node"]) == 2


@pytest.mark.parametrize("control", ["pause", "cancel"])
@pytest.mark.parametrize("late_action", ["continue", "failed", "needs_input"])
async def test_late_decision_cannot_override_human_control(storage, tmp_path, control, late_action):
    started = asyncio.Event()
    release = asyncio.Event()
    def policy(c, registry):
        if late_action == "continue":
            return default_decision(c, registry)
        return {"decision_id": c["decision_id"], "action": late_action, "reason": "Late response", **({"question": "Continue?"} if late_action == "needs_input" else {})}
    class Controlled(Registry):
        async def start(self, prompt, repo, **kwargs):
            task = await super().start(prompt, repo, **kwargs)
            task.done = release
            started.set()
            return task
    run = storage.create_run(w.validate_definition(graph()), "Request", tmp_path)
    registry = Controlled(storage.root, policy)
    execution = asyncio.create_task(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]))
    await asyncio.wait_for(started.wait(), 3)
    storage.control(run["workflow_run_id"], control)
    release.set()
    await asyncio.wait_for(execution, 3)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == ("paused" if control == "pause" else "cancelled")
    assert not observed["decisions"]
    assert not any(a["role"] == "node" for a in observed["activations"])
    assert observed["activations"][0]["decision_ignored"]


def test_cancelled_execution_cannot_be_an_input_reference():
    run = {"definition": {"nodes": [{"id": "work", "role": "task"}]}, "activations": [{"id": "cancelled", "role": "node", "node_id": "work", "status": "cancelled", "tasks": [], "node_result": {"status": "failed", "result": {}, "evidence": []}}]}
    with pytest.raises(w.WorkflowError, match="settled"):
        d.result_inputs(run, ["cancelled"])


async def test_worker_question_resumes_same_execution_and_shows_answer(storage, tmp_path):
    turns = []
    def policy(context, registry):
        if context["current_stage"]["phase"] == "clarification":
            question = context["worker_question"]
            assert question["question"] == "Which parser format?"
            return {"decision_id": context["decision_id"], "action": "answer", "question_id": question["question_id"], "answer": "Use JSON format", "reason": "The overall request specifies JSON"}
        return default_decision(context, registry)
    def worker(prompt, kwargs):
        turns.append((prompt, kwargs))
        if len(turns) == 1:
            return {"status": "asking", "result": {"question": "Which parser format?", "context": "Two supported formats", "progress": "Inspected existing parsers"}, "evidence": ["Existing parser code"]}
        assert "Use JSON format" in prompt and "Inspected existing parsers" in prompt
        assert "ORIGINAL PRIVATE REQUEST" not in prompt
        return {"status": "succeeded", "result": {"summary": "Implemented JSON parser"}, "evidence": ["Tests passed"]}
    run, registry = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy, {"work": worker}))
    assert run["status"] == "completed"
    nodes = [a for a in run["activations"] if a["role"] == "node"]
    assert len(nodes) == 1 and len(nodes[0]["tasks"]) == 2
    q = nodes[0]["questions"][0]
    assert q["origin_task_id"] == nodes[0]["tasks"][0]["task_id"]
    assert q["reply_task_id"] == nodes[0]["tasks"][1]["task_id"]
    assert q["status"] == "answered" and q["answer_delivery_state"] == "settled"
    assert nodes[0]["tasks"][1]["assignment_prompt"] == "Use JSON format"
    assert turns[1][1]["display_prompt"] == "Use JSON format"
    assert nodes[0]["assignment_prompt"] == "Focused assignment for work"


async def test_worker_question_caller_input_requires_matching_decision(storage, tmp_path):
    asked_caller = False
    worker_turns = 0
    def policy(context, registry):
        nonlocal asked_caller
        if context["current_stage"]["phase"] == "clarification":
            if not asked_caller:
                asked_caller = True
                return {"decision_id": context["decision_id"], "action": "needs_input", "reason": "Caller preference needed", "question": "JSON or YAML?"}
            assert context["recovery_instructions"] == "JSON"
            return {"decision_id": context["decision_id"], "action": "answer", "question_id": context["worker_question"]["question_id"], "answer": "JSON", "reason": "Caller answered"}
        return default_decision(context, registry)
    def worker(prompt, kwargs):
        nonlocal worker_turns
        worker_turns += 1
        return {"status": "asking", "result": {"question": "Which format?"}, "evidence": []} if worker_turns == 1 else {"status": "succeeded", "result": {"summary": "Completed"}, "evidence": []}
    run, registry = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy, {"work": worker}))
    assert run["status"] == "needs_input"
    activation = next(a for a in run["activations"] if a["role"] == "node")
    assert activation["status"] == "waiting_for_answer" and "node_result" not in activation
    with pytest.raises(w.WorkflowError, match="settled"):
        d.result_inputs(run, [activation["id"]])
    with pytest.raises(w.WorkflowError, match="decision_id"):
        storage.control(run["workflow_run_id"], "resume", instructions="JSON", decision_id="stale")
    storage.control(run["workflow_run_id"], "resume", instructions="JSON", decision_id=run["input_decision_id"])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert storage.get_run(run["workflow_run_id"])["status"] == "completed"
    assert worker_turns == 2


async def test_asking_without_session_requires_explicit_fresh(storage, tmp_path):
    worker_turns = 0
    def policy(context, registry):
        if context["current_stage"]["phase"] == "clarification":
            assert context["answer_sessions"] == []
            return {"decision_id": context["decision_id"], "action": "answer", "question_id": context["worker_question"]["question_id"], "answer": "Use JSON", "session_mode": "fresh", "reason": "Harness has no resume session"}
        return default_decision(context, registry)
    def worker(prompt, kwargs):
        nonlocal worker_turns
        worker_turns += 1
        if worker_turns == 1:
            return {"summary": json.dumps({"status": "asking", "result": {"question": "Which format?", "progress": "Inspected parser"}, "evidence": []}), "session_id": None}
        assert "Inspected parser" in prompt and "Use JSON" in prompt
        return {"status": "succeeded", "result": {"summary": "Done"}, "evidence": []}
    run, _ = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy, {"work": worker}))
    assert run["status"] == "completed" and worker_turns == 2
    q = next(a for a in run["activations"] if a["role"] == "node")["questions"][0]
    assert q["session_mode"] == "fresh"


async def test_context_question_budget_is_separate_from_node_attempts(storage, tmp_path):
    g = graph()
    g["nodes"][1]["max_context_questions"] = 2
    g["nodes"][1]["max_attempts"] = 1
    def policy(context, registry):
        if context["current_stage"]["phase"] == "clarification":
            return {"decision_id": context["decision_id"], "action": "answer", "question_id": context["worker_question"]["question_id"], "answer": "Continue", "reason": "Clarified"}
        return default_decision(context, registry)
    asking = {"status": "asking", "result": {"question": "More context?"}, "evidence": []}
    run, _ = await run_flow(storage, tmp_path, g, Registry(storage.root, policy, {"work": asking}))
    assert run["status"] == "failed" and "question limit" in run["failure_reason"]
    nodes = [a for a in run["activations"] if a["role"] == "node"]
    assert len(nodes) == 1 and len(nodes[0]["questions"]) == 2 and len(nodes[0]["tasks"]) == 3


async def test_final_json_inspection_does_not_consume_decision_attempt(storage, tmp_path):
    inspected = False
    def policy(context, registry):
        nonlocal inspected
        if context["current_stage"]["node_id"] == "work" and not inspected:
            inspected = True
            return {"decision_id": context["decision_id"], "action": "inspect", "reason": "Read complete worker evidence", "requests": [{"execution_id": context["input_results"][0]["execution_id"], "view": "result"}]}
        if context["inspection_results"]:
            result = context["inspection_results"][0]["response"]
            assert json.loads(result["chunk"])["node_result"]["status"] == "succeeded"
            assert context["inspections_remaining"] == 19
        return default_decision(context, registry)
    g = graph()
    g["max_decision_attempts"] = 1
    run, registry = await run_flow(storage, tmp_path, g, Registry(storage.root, policy))
    assert run["status"] == "completed" and len(registry.contexts) == 4
    assert registry.contexts[1]["decision_id"] == registry.contexts[2]["decision_id"]


async def test_inspection_budget_survives_caller_resume(storage, tmp_path):
    phase = 0
    def policy(context, registry):
        nonlocal phase
        if context["current_stage"]["node_id"] == "work":
            phase += 1
            if phase == 1:
                return {"decision_id": context["decision_id"], "action": "inspect", "requests": [{"execution_id": context["input_results"][0]["execution_id"], "view": "result"}]}
            if phase == 2:
                return {"decision_id": context["decision_id"], "action": "needs_input", "reason": "Need caller input", "question": "Continue?"}
            assert context["inspections_remaining"] == 0
            assert context["inspection_results"]
        return default_decision(context, registry)
    g = graph()
    g["max_inspections"] = 1
    run, registry = await run_flow(storage, tmp_path, g, Registry(storage.root, policy))
    assert run["status"] == "needs_input"
    storage.control(run["workflow_run_id"], "resume", instructions="Continue", decision_id=run["input_decision_id"])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert storage.get_run(run["workflow_run_id"])["status"] == "completed"


async def test_agent_decides_resume_requires_issued_compatible_session(storage, tmp_path):
    def policy(context, registry):
        result = default_decision(context, registry)
        result["next"][0].update(session_mode="resume", resume_task_id="invented")
        return result
    run, _ = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy))
    assert run["status"] == "failed" and not any(a["role"] == "node" for a in run["activations"])


async def test_fixed_resume_bootstraps_first_session_fresh(storage, tmp_path):
    g = graph()
    g["nodes"][1]["session_mode"] = "resume"
    def policy(context, registry):
        result = default_decision(context, registry)
        for entry in result.get("next", []):
            if "prompt" in entry:
                entry["session_mode"] = "resume"
        return result
    run, _ = await run_flow(storage, tmp_path, g, Registry(storage.root, policy))
    assert run["status"] == "completed"
    assert next(a for a in run["activations"] if a["role"] == "node")["execution_session_mode"] == "fresh"


async def test_answer_persisted_before_resume_survives_supervisor_restart(storage, tmp_path):
    def policy(context, registry):
        if context["current_stage"]["phase"] == "clarification":
            return {"decision_id": context["decision_id"], "action": "answer", "question_id": context["worker_question"]["question_id"], "answer": "Use JSON", "reason": "Chosen format"}
        return default_decision(context, registry)
    asking = {"status": "asking", "result": {"question": "Which format?", "progress": "Inspected parser"}, "evidence": []}
    class Interrupted(w.WorkflowSupervisor):
        async def _dispatch(self, node, prompt, role, activation):
            if role == "node" and activation.get("resume_question_id"):
                self.attention("Simulated interruption before answer dispatch reservation")
                return None
            return await super()._dispatch(node, prompt, role, activation)
    run = storage.create_run(w.validate_definition(graph()), "Private original", tmp_path)
    registry = Registry(storage.root, policy, {"work": asking})
    await Interrupted(registry, storage).execute(run["workflow_run_id"])
    paused = storage.get_run(run["workflow_run_id"])
    activation = next(a for a in paused["activations"] if a["role"] == "node")
    q = activation["questions"][0]
    assert q["status"] == "answered" and q["answer_delivery_state"] == "pending"
    assert q["answer"] == "Use JSON" and len(activation["tasks"]) == 1
    origin = activation["tasks"][0]
    w.task_store.write(storage.root / "tasks", w.task_store.TaskRecord(task_id=origin["task_id"], backend="codex", session_id=origin["result"]["session_id"], repo_path=str(tmp_path), started_at="2026-10-03T00:00:00Z", status="completed", exit_code=0))
    storage.control(run["workflow_run_id"], "resume", instructions="Continue the saved answer")
    class Restored(Registry):
        async def resume_record(self, record, prompt, **kwargs):
            if "Context:\n" not in prompt:  # Workers keep isolated assignments; orchestrators retain their own session.
                assert "Use JSON" in prompt and "Inspected parser" in prompt
                assert "Private original" not in prompt
            return await self.start(prompt, Path(record.repo_path), **{**kwargs, "backend": w.backends.get(record.backend), "title": "delegation · work"})
    restored = Restored(storage.root)
    await w.WorkflowSupervisor(restored, storage).execute(run["workflow_run_id"])
    complete = storage.get_run(run["workflow_run_id"])
    assert complete["status"] == "completed"
    assert len([a for a in complete["activations"] if a["role"] == "node"]) == 1
    assert len([p for p, _ in restored.calls if "Assignment:\n" in p]) == 1
    assert not any(c["current_stage"]["phase"] == "clarification" for c in restored.contexts)


async def test_ambiguous_answer_delivery_is_not_replayed(storage, tmp_path):
    def policy(context, registry):
        if context["current_stage"]["phase"] == "clarification":
            return {"decision_id": context["decision_id"], "action": "answer", "question_id": context["worker_question"]["question_id"], "answer": "Use JSON", "reason": "Chosen format"}
        return default_decision(context, registry)
    class Uncertain(Registry):
        async def resume(self, previous, prompt, **kwargs):
            raise OSError("Unknown resume outcome after handoff")
    asking = {"status": "asking", "result": {"question": "Which format?"}, "evidence": []}
    run, registry = await run_flow(storage, tmp_path, registry=Uncertain(storage.root, policy, {"work": asking}))
    assert run["status"] == "needs_attention" and run["settling"]
    activation = next(a for a in run["activations"] if a["role"] == "node")
    q = activation["questions"][0]
    assert q["answer"] == "Use JSON" and q["answer_delivery_state"] == "uncertain"
    before = len(registry.calls)
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert len(registry.calls) == before
    with pytest.raises(w.WorkflowError, match="Unresolved"):
        storage.control(run["workflow_run_id"], "resume", instructions="Continue")


async def test_observed_completed_answer_result_recovers_without_resume_replay(storage, tmp_path):
    turns = 0
    def policy(context, registry):
        if context["current_stage"]["phase"] == "clarification":
            return {"decision_id": context["decision_id"], "action": "answer", "question_id": context["worker_question"]["question_id"], "answer": "Use JSON", "reason": "Chosen format"}
        return default_decision(context, registry)
    def worker(prompt, kwargs):
        nonlocal turns
        turns += 1
        return {"status": "asking", "result": {"question": "Which format?"}, "evidence": []} if turns == 1 else {"status": "succeeded", "result": {"summary": "Durable completed reply"}, "evidence": []}
    class Interrupted(w.WorkflowSupervisor):
        async def _dispatch(self, node, prompt, role, activation):
            result = await super()._dispatch(node, prompt, role, activation)
            if role == "node" and activation.get("resume_question_id"):
                self.attention("Simulated interruption after observed reply result")
                return None
            return result
    run = storage.create_run(w.validate_definition(graph()), "Request", tmp_path)
    registry = Registry(storage.root, policy, {"work": worker})
    await Interrupted(registry, storage).execute(run["workflow_run_id"])
    interrupted = storage.get_run(run["workflow_run_id"])
    node = next(a for a in interrupted["activations"] if a["role"] == "node")
    assert node["questions"][0]["answer_delivery_state"] == "settled" and "node_result" not in node
    storage.control(run["workflow_run_id"], "resume", instructions="Use observed reply")
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    final = storage.get_run(run["workflow_run_id"])
    assert final["status"] == "completed" and turns == 2
    activation = next(a for a in final["activations"] if a["role"] == "node")
    assert activation["node_result"]["result"]["summary"] == "Durable completed reply"


async def test_cancelling_waiting_question_preserves_history_and_settles_node(storage, tmp_path):
    def policy(context, registry):
        if context["current_stage"]["phase"] == "clarification":
            return {"decision_id": context["decision_id"], "action": "needs_input", "question": "Which format?", "reason": "Caller preference required"}
        return default_decision(context, registry)
    asking = {"status": "asking", "result": {"question": "Which format?"}, "evidence": []}
    run, registry = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy, {"work": asking}))
    assert run["status"] == "needs_input"
    storage.control(run["workflow_run_id"], "cancel")
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    cancelled = storage.get_run(run["workflow_run_id"])
    assert cancelled["status"] == "cancelled"
    activation = next(a for a in cancelled["activations"] if a["role"] == "node")
    assert activation["status"] == "cancelled" and activation["questions"][0]["question"] == "Which format?"
    with pytest.raises(w.WorkflowError, match="settled"):
        d.result_inputs(cancelled, [activation["id"]])


@pytest.mark.parametrize("field,value", [("max_context_questions", 0), ("max_context_questions", 101)])
def test_question_limit_validation(field, value):
    definition = graph()
    definition["nodes"][1][field] = value
    with pytest.raises(w.WorkflowError, match=field):
        w.validate_definition(definition)


async def test_planning_requires_technical_plan_and_retains_it_for_later_decisions(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path, graph("planning"), Registry(storage.root, outputs={"work": {"status": "succeeded", "result": {"tasks": [{"id": "one", "title": "Implement"}], "technical_plan": "# Technical plan\nImplement and validate the parser"}, "evidence": []}}))
    assert run["status"] == "completed"
    assert run["technical_plan"] == "# Technical plan\nImplement and validate the parser"
    assert registry.contexts[-1]["technical_plan"] == run["technical_plan"]
    assert registry.contexts[-1]["technical_plan_execution_id"] == run["technical_plan_execution_id"]
    node = next(n for n in run["definition"]["nodes"] if n["type"] == "agent")
    with pytest.raises(w.WorkflowError, match="technical_plan"):
        d.normalize_result(node, {"summary": json.dumps({"status": "succeeded", "result": {"tasks": [{"id": "one", "title": "Implement"}]}, "evidence": []})}, [])


async def test_recovered_asking_output_is_not_prematurely_settled(storage, tmp_path):
    asking = json.dumps({"status": "asking", "result": {"question": "Which format?"}, "evidence": []})
    rid = recovered_fixture(storage, tmp_path, summary=asking)
    classified = storage.reconcile_run(rid)
    assert classified["activations"][0]["status"] == "result_pending"
    assert "node_result" not in classified["activations"][0]
    with pytest.raises(w.WorkflowError, match="settled"):
        d.result_inputs(classified, ["execution"])


async def test_question_resume_outage_requests_fresh_automatically_then_falls_back(storage, tmp_path):
    g = graph()
    g["nodes"][1]["agent"] = {"backend": "claude", "fallbacks": [{"backend": "codex"}]}
    turns = []
    def policy(context, registry):
        if context["current_stage"]["phase"] == "clarification":
            question = context["worker_question"]
            fresh = question["answer_delivery_state"] == "requires_fresh"
            return {"decision_id": context["decision_id"], "action": "answer", "question_id": question["question_id"], "answer": "Use JSON", "reason": "Continue on available harness" if fresh else "Known format", **({"session_mode": "fresh"} if fresh else {})}
        return default_decision(context, registry)
    def worker(prompt, kwargs):
        turns.append(kwargs["backend"].name)
        if len(turns) == 1:
            return {"status": "asking", "result": {"question": "Which format?", "progress": "Inspected parser"}, "evidence": []}
        if len(turns) == 2:
            return {"status": "failed", "summary": "", "backend": "claude", "stderr_tail": ["API Error: 503 Service unavailable"]}
        assert "Inspected parser" in prompt and "Use JSON" in prompt
        return {"status": "succeeded", "result": {"summary": "Fresh fallback completed work"}, "evidence": []}
    run, registry = await run_flow(storage, tmp_path, g, Registry(storage.root, policy, {"work": worker}))
    assert run["status"] == "completed" and turns == ["claude", "claude", "codex"]
    assert not any(decision["action"] == "needs_input" for decision in run["decisions"])
    activation = next(a for a in run["activations"] if a["role"] == "node")
    assert len(activation["questions"]) == 1 and len(activation["tasks"]) == 3
    assert activation["questions"][0]["session_mode"] == "fresh"
    assert len([c for c in registry.contexts if c["current_stage"]["phase"] == "clarification"]) == 2


async def test_selected_resume_outage_reassigns_same_execution_fresh_without_caller(storage, tmp_path):
    g = graph()
    g["nodes"][1]["agent"] = {"backend": "claude", "fallbacks": [{"backend": "codex"}]}
    g["connections"].append({"id": "retry", "source": "work", "target": "work", "condition": "One follow-up", "max_retries": 1})
    turns = []
    routed_retry = False
    def policy(context, registry):
        nonlocal routed_retry
        result = default_decision(context, registry)
        if context["current_stage"]["node_id"] == "work" and context["current_stage"]["phase"] == "routing":
            if not routed_retry:
                routed_retry = True
                choice = next(c for c in context["valid_continuations"] if c["continuation_id"] == "retry")
                assert choice["available_sessions"]
                result["next"] = [{"continuation_id": "retry", "prompt": "Refine the previous result", "session_mode": "resume", "resume_task_id": choice["available_sessions"][0]["task_id"]}]
            else:
                result["next"] = [{"continuation_id": "finish"}]
        if context["current_stage"]["phase"] == "assignment":
            result["next"][0]["session_mode"] = "fresh"
        return result
    def worker(prompt, kwargs):
        turns.append(kwargs["backend"].name)
        if len(turns) == 2:
            return {"status": "failed", "summary": "", "backend": "claude", "stderr_tail": ["API Error: 503 Service unavailable"]}
        if len(turns) == 3:
            assert "Previous implementation evidence" in prompt
        return {"status": "succeeded", "result": {"summary": "Previous implementation evidence"}, "evidence": []}
    run, registry = await run_flow(storage, tmp_path, g, Registry(storage.root, policy, {"work": worker}))
    assert run["status"] == "completed" and turns == ["claude", "claude", "codex"]
    nodes = [a for a in run["activations"] if a["role"] == "node"]
    assert len(nodes) == 2 and len(nodes[1]["tasks"]) == 2
    assert not any(decision["action"] == "needs_input" for decision in run["decisions"])
    assert any(c["current_stage"]["phase"] == "assignment" for c in registry.contexts)


def resume_outage_checkpoint(storage, tmp_path, *, question=False, attempt_mode_metadata=True):
    g = graph()
    g["nodes"][1]["agent"] = {"backend": "claude", "fallbacks": [{"backend": "codex"}]}
    run = storage.create_run(w.validate_definition(g), "Original request", tmp_path)
    node = run["definition"]["nodes"][1]
    candidate = copy.deepcopy(node["agent"])
    source_task = {"task_id": "source", "candidate": candidate, "status": "completed", "repo_path": str(tmp_path), "freedom": "write_in_repo", "network": None, "result": {"task_id": "source", "status": "completed", "backend": "claude", "session_id": "known-session", "summary": json.dumps({"status": "asking" if question else "succeeded", "result": {"question": "Which format?"} if question else {"summary": "Previous work"}, "evidence": []})}}
    failed_task = {"task_id": "failed-reply", "candidate": candidate, "status": "failed", "result": {"task_id": "failed-reply", "backend": "claude", "status": "failed", "summary": "", "stderr_tail": ["API Error: 503 Service unavailable"]}}
    if attempt_mode_metadata:
        failed_task.update(session_mode="resume", resume_task_id="source")
    token = {"id": "token", "node_id": "work", "stack": [], "context": {}, "assignment_prompt": "Refine previous work", "execution_activation_id": "current", "execution_session_mode": "fresh" if question else "resume", "resume_task_id": "source", "input_result_refs": [], "assigned_task_ids": []}
    activation = {"id": "current", "node_id": "work", "role": "node", "status": "running", "assignment_prompt": token["assignment_prompt"], "assigned_task_ids": [], "execution_session_mode": token["execution_session_mode"], "resume_task_id": "source", "token": copy.deepcopy(token), "tasks": [source_task, failed_task] if question else [failed_task]}
    activations = [activation]
    if question:
        activation.update(pending_question_id="question", resume_question_id="question", questions=[{"question_id": "question", "question": "Which format?", "context": "", "progress": {"question": "Which format?"}, "evidence": [], "origin_task_id": "source", "origin_candidate": candidate, "reply_task_id": "failed-reply", "status": "answered", "answer": "Use JSON", "answer_delivery_state": "settled", "session_mode": "resume", "decision_id": "question-decision", "decision_attempts": 1}])
    else:
        token["resume_source_execution_id"] = "previous"
        previous = {"id": "previous", "node_id": "work", "role": "node", "status": "completed", "token": {}, "tasks": [source_task], "node_result": {"status": "succeeded", "result": {"summary": "Previous work"}, "evidence": []}, "raw_output": source_task["result"]["summary"]}
        activations.insert(0, previous)
    identity = "work:" + w._candidate_key(candidate)
    # Emulate both crash points: the task outcome is durable, and the historical
    # first write suppressed the harness, but the Fresh requirement is absent.
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", pending=[token], activations=activations, suppressed_candidates=[identity]), "suppressed_before_requirement_crash")
    return run["workflow_run_id"]


@pytest.mark.parametrize("metadata", [True, False])
async def test_recovered_resume_outage_cannot_fresh_fallback_before_orchestrator(storage, tmp_path, metadata):
    rid = resume_outage_checkpoint(storage, tmp_path, attempt_mode_metadata=metadata)
    def policy(context, registry):
        assert context["current_stage"]["phase"] == "assignment"
        return {"decision_id": context["decision_id"], "action": "failed", "reason": "Decline Fresh fallback"}
    registry = Registry(storage.root, policy)
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    run = storage.get_run(rid)
    assert run["status"] == "failed"
    assert not any("Assignment:\n" in prompt for prompt, _ in registry.calls)
    assert len([a for a in run["activations"] if a["role"] == "node"]) == 2
    assert run["pending"][0]["requires_assignment"]


@pytest.mark.parametrize("metadata", [True, False])
async def test_recovered_answer_resume_outage_cannot_fresh_before_orchestrator(storage, tmp_path, metadata):
    rid = resume_outage_checkpoint(storage, tmp_path, question=True, attempt_mode_metadata=metadata)
    def policy(context, registry):
        assert context["current_stage"]["phase"] == "clarification"
        assert context["worker_question"]["answer_delivery_state"] == "requires_fresh"
        return {"decision_id": context["decision_id"], "action": "failed", "reason": "Decline Fresh answer fallback"}
    registry = Registry(storage.root, policy)
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    run = storage.get_run(rid)
    assert run["status"] == "failed"
    assert not any("Assignment:\n" in prompt for prompt, _ in registry.calls)
    activation = next(a for a in run["activations"] if a["role"] == "node")
    assert len(activation["tasks"]) == 2
    assert activation["questions"][0]["answer_delivery_state"] == "requires_fresh"

async def test_blocked_worker_can_retry_without_graph_loop(storage, tmp_path):
    attempts = 0
    def output(prompt, kwargs):
        nonlocal attempts
        attempts += 1
        return {"status": "blocked" if attempts == 1 else "succeeded", "result": {"summary": "need corrected context"}, "evidence": []}
    def policy(context, registry):
        retries = [c for c in context["valid_continuations"] if c["kind"] == "retry_execution"]
        if retries:
            return {"decision_id": context["decision_id"], "action": "continue", "reason": "Correct context", "next": [{"continuation_id": retries[0]["continuation_id"], "prompt": "Corrected assignment", "session_mode": "fresh"}]}
        return default_decision(context, registry)
    run, registry = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy, {"Work": output, "work": output}))
    assert run["status"] == "completed"
    assert attempts == 2
    workers = [a for a in run["activations"] if a["role"] == "node"]
    assert len(workers) == 2
    assert workers[1]["assignment_prompt"] == "Corrected assignment"
    assert run["transitions"] == 2
    assert workers[0]["id"] in workers[1]["input_result_refs"]


def test_structural_barrier_has_no_session_fields(storage, tmp_path):
    definition = w.validate_definition(parallel())
    run = storage.create_run(definition, "request", tmp_path)
    run["joins"]["fork"] = {"join_id": "merge"}
    token = {"id": "token", "node_id": "left", "stack": ["fork"]}
    node = next(n for n in definition["nodes"] if n["id"] == "left")
    choice = d.continuations(run, node, token, False)[0]
    assert choice["kind"] == "barrier_arrival"
    assert "session_mode" not in choice
    assert "available_sessions" not in choice


@pytest.mark.parametrize("kind", ["permission", "authority", "cancelled", "uncertain"])
def test_unsafe_execution_never_offers_retry(kind):
    assert not d.retry_eligible({"status": "failed", "tasks": [], "node_result": {"status": "blocked", "result": {"failure_kind": kind}}})

async def test_retry_at_parallel_branch_preserves_convergence_and_siblings(storage, tmp_path):
    attempts = {"left": 0, "right": 0}
    def worker(which):
        def output(prompt, kwargs):
            attempts[which] += 1
            return {"status": "blocked" if which == "left" and attempts[which] == 1 else "succeeded", "result": {"summary": which}, "evidence": []}
        return output
    def policy(context, registry):
        retries = [c for c in context["valid_continuations"] if c["kind"] == "retry_execution"]
        if retries:
            return {"decision_id": context["decision_id"], "action": "continue", "reason": "Resolve missing context", "next": [{"continuation_id": retries[0]["continuation_id"], "prompt": "Corrected branch", "session_mode": "fresh"}]}
        return default_decision(context, registry)
    definition = parallel()
    definition['nodes'][1]['title'] = 'left'
    definition['nodes'][2]['title'] = 'right'
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, policy, {'left': worker('left'), 'right': worker('right')}))
    assert run['status'] == 'completed'
    assert attempts == {'left': 2, 'right': 1}
    assert len([a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'merge']) == 1
    assert not run['joins']


async def test_retry_budget_exhaustion_stops_offering_execution(storage, tmp_path):
    definition = graph()
    definition['nodes'][1]['max_attempts'] = 1
    def policy(context, registry):
        if context['current_stage']['node_id'] == 'work':
            assert not any(c['kind'] == 'retry_execution' for c in context['valid_continuations'])
            return {'decision_id': context['decision_id'], 'action': 'failed', 'reason': 'No execution budget remains'}
        return default_decision(context, registry)
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, policy, {'Work': {'status': 'blocked', 'result': {'reason': 'missing context'}, 'evidence': []}}))
    assert run['status'] == 'failed'
    assert len([a for a in run['activations'] if a['role'] == 'node']) == 1


def test_no_retry_or_resume_when_all_candidates_are_suppressed(storage, tmp_path):
    definition = w.validate_definition(graph())
    run = storage.create_run(definition, 'request', tmp_path)
    node = definition['nodes'][1]
    activation = {'id': 'failed-worker', 'role': 'node', 'node_id': 'work', 'status': 'failed', 'tasks': [], 'node_result': {'status': 'failed', 'result': {'failure_kind': 'harness', 'reason': 'All agents unavailable'}, 'evidence': []}}
    run['activations'] = [activation]
    run['suppressed_candidates'] = ['work:' + w._candidate_key(node['agent'])]
    token = {'id': 'token', 'node_id': 'work', 'execution_complete': True, 'execution_activation_id': activation['id'], 'result': activation['node_result']}
    assert not any(c['kind'] == 'retry_execution' for c in d.continuations(run, node, token, False))
    assert d.available_sessions(run, node) == []


async def test_settled_protocol_failure_explicit_retry_recovers_without_repeating_sibling(storage, tmp_path):
    definition = parallel()
    counts = {'left': 0, 'right': 0}
    def worker_left(prompt, kwargs):
        counts['left'] += 1
        if counts['left'] == 1:
            return {'summary': '{"status":"succeeded"},"evidence":[]}'}
        return {'status': 'succeeded', 'result': {'summary': 'Corrected envelope'}, 'evidence': []}
    def worker_right(prompt, kwargs):
        counts['right'] += 1
        return {'status': 'succeeded', 'result': {'summary': 'Sibling retained'}, 'evidence': []}
    def policy(context, registry):
        retry = next((c for c in context['valid_continuations'] if c['kind'] == 'retry_execution'), None)
        if retry:
            assert context['input_results'][0]['retry_eligible']
            return {'decision_id': context['decision_id'], 'action': 'continue', 'reason': 'Explicitly correct malformed result', 'next': [{'continuation_id': retry['continuation_id'], 'prompt': 'Perform the assigned review and return a valid envelope', 'session_mode': 'fresh'}]}
        return default_decision(context, registry)
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, policy, {'left': worker_left, 'right': worker_right}), guided=True)
    assert run['status'] == 'completed'
    assert counts == {'left': 2, 'right': 1}
    failed = next(a for a in run['activations'] if a.get('result_error'))
    assert failed['raw_output'].endswith(',"evidence":[]}')
    assert failed['resolved_by_execution_id']


async def test_guided_parallel_trailing_worker_json_keeps_branches_successful(storage, tmp_path):
    definition = parallel()
    output = {'summary': json.dumps({'status': 'succeeded', 'result': {'summary': 'Review completed'}, 'evidence': ['reviewed']}) + ',"evidence":[]}'}
    run, registry = await run_flow(storage, tmp_path, definition, Registry(storage.root, outputs={'left': output}), guided=True)
    assert run['status'] == 'completed'
    left = next(a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'left')
    assert left['node_result']['status'] == 'succeeded'
    assert left['raw_output'].endswith(',"evidence":[]}')
    assert len([a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'left']) == 1


async def test_guided_exhaustion_resume_does_not_repeat_worker(storage, tmp_path):
    bad = True
    def policy(context, registry):
        result = default_decision(context, registry)
        if bad:
            result['decision_id'] = 'stale'
        return result
    run, registry = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy), guided=True)
    assert run['status'] == 'needs_attention'
    assert 'Decision ID' in run['attention_reason']
    assert not any(a['role'] == 'node' for a in run['activations'])
    bad = False
    storage.control(run['workflow_run_id'], 'resume', instructions='Correct the decision ID')
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    assert storage.get_run(run['workflow_run_id'])['status'] == 'completed'
    assert len([call for call in registry.calls if 'Context:\n' not in call[0]]) == 1


async def test_guided_harmless_decision_fields_persist_warnings(storage, tmp_path):
    def policy(context, registry):
        result = default_decision(context, registry)
        result['display_note'] = 'Presentation only'
        for entry in result.get('next', []):
            entry['label'] = 'Continue'
            choice = next(c for c in context['valid_continuations'] if c['continuation_id'] == entry['continuation_id'])
            if not choice['requires_prompt']:
                entry.update(assigned_task_ids=[], additional_result_refs=[])
        return result
    run, _ = await run_flow(storage, tmp_path, registry=Registry(storage.root, policy), guided=True)
    assert run['status'] == 'completed'
    assert all(decision['protocol_warnings'] for decision in run['decisions'])
    assert all('display_note' not in decision for decision in run['decisions'])


async def test_guided_structural_nonempty_result_context_is_not_silently_dropped(storage, tmp_path):
    definition = graph()
    definition['nodes'].pop(1)
    definition['connections'] = [{'id': 'begin', 'source': 'start', 'target': 'end'}]
    def policy(context, registry):
        result = default_decision(context, registry)
        if context['current_stage']['node_id'] == 'start':
            result['next'][0]['additional_result_refs'] = ['foreign']
        return result
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, policy), guided=True)
    assert run['status'] == 'needs_attention'
    assert 'additional_result_refs' in run['attention_reason']
    assert not any(a['role'] == 'node' for a in run['activations'])


async def test_guided_orchestrator_can_reject_no_checklist_proposal_and_request_replan(storage, tmp_path):
    definition = graph('planning')
    definition['nodes'][1]['require_technical_plan'] = False
    count = 0
    def worker(prompt, kwargs):
        nonlocal count
        count += 1
        if count == 1:
            return {'status': 'succeeded', 'result': {'no_checklist_needed': True, 'checklist_reason': 'Seems simple'}, 'evidence': []}
        assert 'Produce checklist tasks' in prompt
        return {'status': 'succeeded', 'result': {'tasks': [{'id': 'verify', 'title': 'Verify changed behavior'}]}, 'evidence': []}
    def policy(context, registry):
        retry = next((c for c in context['valid_continuations'] if c['kind'] == 'retry_execution'), None)
        if retry:
            assert context['input_results'][0]['node_result']['result']['no_checklist_needed']
            return {'decision_id': context['decision_id'], 'action': 'continue', 'reason': 'The change requires explicit validation tasks', 'next': [{'continuation_id': retry['continuation_id'], 'prompt': 'Produce checklist tasks covering validation', 'session_mode': 'fresh'}]}
        return default_decision(context, registry)
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, policy, {'work': worker}), guided=True)
    assert run['status'] == 'completed'
    assert count == 2
    assert run['tasks'][0]['id'] == 'verify'


async def test_end_required_context_failure_pauses_then_retries_only_failed_node(storage, tmp_path):
    definition = graph()
    definition['nodes'][1]['id'] = 'source'
    definition['nodes'].insert(2, {'id': 'publish', 'type': 'agent', 'role': 'task', 'instructions': 'Use exact complete draft', 'agent': {'backend': 'codex'}})
    definition['connections'] = [{'id': 'begin', 'source': 'start', 'target': 'source'}, {'id': 'publish', 'source': 'source', 'target': 'publish'}, {'id': 'end', 'source': 'publish', 'target': 'end', 'condition': 'Inspect completion or blocked publication at End'}]
    draft = 'approved full draft ' * 3000 + 'COMPLETE DRAFT SENTINEL'
    counts = {'source': 0, 'publish': 0}
    def source(prompt, kwargs):
        counts['source'] += 1
        return {'status': 'succeeded', 'result': {'draft': draft}, 'evidence': []}
    def publish(prompt, kwargs):
        counts['publish'] += 1
        assert draft in prompt
        if counts['publish'] == 1:
            blocked = {'status': 'blocked', 'result': {'blocker_category': 'missing_context', 'posted': False, 'reason': 'Need original full context'}, 'evidence': []}
            return {'summary': json.dumps(blocked), 'status': 'completed', 'permission_denials': [{'tool_name': 'Bash', 'tool_input': {'command': 'echo "$CLAUDE_SESSION_ID"'}}]}
        assert 'Retry only the blocked publication' in prompt
        return {'status': 'succeeded', 'result': {'posted': True}, 'evidence': []}
    def policy(context, registry):
        if context['current_stage']['node_id'] == 'publish' and context['current_stage']['phase'] == 'routing':
            return {'decision_id': context['decision_id'], 'action': 'continue', 'reason': 'Inspect blocked completion at End', 'next': [{'continuation_id': 'end'}]}
        if context['current_stage']['node_id'] == 'end':
            retry = next((c for c in context['valid_continuations'] if c.get('recovery_from_checkpoint')), None)
            if retry:
                return {'decision_id': context['decision_id'], 'action': 'continue', 'reason': 'Caller authorized full-context retry', 'next': [{'continuation_id': retry['continuation_id'], 'prompt': 'Retry only the blocked publication', 'session_mode': 'fresh'}]}
            if any(i['status'] == 'blocked' for i in context['input_results']):
                return {'decision_id': context['decision_id'], 'action': 'failed', 'reason': 'Publication remains blocked and needs caller recovery'}
        return default_decision(context, registry)
    run, registry = await run_flow(storage, tmp_path, definition, Registry(storage.root, policy, {'source': source, 'publish': publish}), guided=True)
    assert run['status'] == 'needs_attention'
    assert counts == {'source': 1, 'publish': 1}
    storage.control(run['workflow_run_id'], 'resume', instructions='Retry with the complete supplied context; prior publication authorization remains valid')
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed'
    assert counts == {'source': 1, 'publish': 2}
    assert final['transitions'] == 3
    failed = next(a for a in final['activations'] if a.get('node_result', {}).get('status') == 'blocked')
    assert failed['resolved_by_execution_id']


async def test_orchestrator_accepts_no_checklist_proposal_with_durable_disposition(storage, tmp_path):
    definition = graph('planning')
    definition['nodes'][1]['require_technical_plan'] = False
    value = {'status': 'succeeded', 'result': {'no_checklist_needed': True, 'checklist_reason': 'This is a scoped reviewer brief'}, 'evidence': []}
    def policy(context, registry):
        result = default_decision(context, registry)
        if context['current_stage']['node_id'] == 'work':
            result['next'] = [{'continuation_id': 'finish'}]
        return result
    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, policy, outputs={'work': value}), guided=True)
    assert run['status'] == 'completed'
    disposition = run['checklist_disposition']
    assert disposition['status'] == 'not_needed'
    assert disposition['reason'] == value['result']['checklist_reason']
    assert disposition['execution_id'] in {a['id'] for a in run['activations'] if a['role'] == 'node'}
    assert disposition['decision_id'] in {d['decision_id'] for d in run['decisions']}


async def test_guided_orchestrator_uses_one_compatible_native_session(storage, tmp_path):
    class SessionRegistry(Registry):
        def __init__(self, root):
            super().__init__(root)
            self.resumed = []
        async def resume(self, previous, prompt, **kwargs):
            self.resumed.append(previous.task_id)
            result = await super().resume(previous, prompt, **kwargs)
            result.result["session_id"] = previous.result["session_id"]
            return result
    registry = SessionRegistry(storage.root)
    run, _ = await run_flow(storage, tmp_path, registry=registry, guided=True)
    assert run["status"] == "completed"
    decisions = [a for a in run["activations"] if a["role"] == "orchestrator"]
    assert len(decisions) == 2
    assert len(registry.resumed) == 1
    first, last = (a["tasks"][0] for a in decisions)
    assert first["session_mode"] == "fresh"
    assert last["session_mode"] == "resume"
    assert last["resume_task_id"] == first["task_id"]
    assert first["result"]["session_id"] == last["result"]["session_id"]


async def test_guided_orchestrator_missing_retained_session_bootstraps_fresh(storage, tmp_path):
    class RetentionRegistry(Registry):
        async def start(self, prompt, repo, **kwargs):
            task = await super().start(prompt, repo, **kwargs)
            if "Assignment:\n" in prompt:
                # Retention removed the earlier orchestrator's record and in-memory owner.
                for prior in list(self.tasks):
                    if prior != task.task_id:
                        self.tasks.pop(prior)
                        w.task_store.record_path(self._log_dir, prior).unlink()
            return task
        async def resume(self, *args, **kwargs):
            pytest.fail("A missing retained session must bootstrap before dispatch")
    run, _ = await run_flow(storage, tmp_path, registry=RetentionRegistry(storage.root), guided=True)
    assert run["status"] == "completed"
    decisions = [a for a in run["activations"] if a["role"] == "orchestrator"]
    assert all(a["tasks"][0]["session_mode"] == "fresh" for a in decisions)


async def test_required_terminal_snapshot_at_single_attempt_recovers_ordered_fallback_in_same_activation(storage, tmp_path):
    rid = recovered_fixture(storage, tmp_path, status='failed', candidate='claude', fallback=True)
    def terminal_snapshot(run):
        run['definition']['nodes'][1]['max_attempts'] = 1
        run['activations'][0]['status'] = 'failed'
    storage.update_run(rid, terminal_snapshot, 'terminal_snapshot_before_dispatch_return')
    recovered = storage.reconcile_run(rid)
    assert recovered['pending'][0]['recovered_failed_result']['status'] == 'failed'
    assert recovered['pending'][0]['execution_activation_id'] == 'execution'
    registry = Registry(storage.root)
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    final = storage.get_run(rid)
    assert final['status'] == 'completed'
    nodes = [a for a in final['activations'] if a['role'] == 'node']
    assert len(nodes) == 1 and nodes[0]['id'] == 'execution'
    assert [task['candidate']['backend'] for task in nodes[0]['tasks']] == ['claude', 'codex']
    workers = [kwargs for prompt, kwargs in registry.calls if 'Assignment:\n' in prompt]
    assert len(workers) == 1 and workers[0]['backend'].name == 'codex'


async def test_required_terminal_nonavailability_snapshot_does_not_redispatch_or_create_activation(storage, tmp_path):
    rid = recovered_fixture(storage, tmp_path, status='failed')
    def terminal_snapshot(run):
        run['definition']['nodes'][1]['max_attempts'] = 1
        run['activations'][0]['status'] = 'failed'
        run['activations'][0]['tasks'][0]['result']['stderr_tail'] = ['Application crashed']
    storage.update_run(rid, terminal_snapshot, 'terminal_snapshot_before_dispatch_return')
    def policy(context, registry):
        assert not any(c['kind'] == 'retry_execution' for c in context['valid_continuations'])
        return {'decision_id': context['decision_id'], 'action': 'needs_input', 'reason': 'Required attempt exhausted without qualifying fallback', 'question': 'Inspect and grant another attempt?'}
    registry = Registry(storage.root, policy)
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    final = storage.get_run(rid)
    assert final['status'] == 'needs_input'
    assert not any('Assignment:\n' in prompt for prompt, _ in registry.calls)
    assert len([a for a in final['activations'] if a['role'] == 'node']) == 1


@pytest.mark.parametrize('checkpoint_type', ['end', 'parallel_end'])
@pytest.mark.parametrize('invalid', [None, 'unassigned', 'unrelated', 'superseded'])
def test_recovered_checkpoint_checklist_evidence(checkpoint_type, invalid, monkeypatch):
    checkpoint = {'id': 'checkpoint', 'type': checkpoint_type}
    worker = {'id': 'work', 'type': 'agent', 'role': 'implementation'}
    previous = {'id': 'old', 'role': 'node', 'node_id': 'work', 'resolved_by_execution_id': 'recovered'}
    recovered = {'id': 'recovered', 'role': 'node', 'node_id': 'work', 'status': 'completed', 'tasks': [], 'retry_of_execution_id': 'old', 'resolved_execution_refs': ['old'], 'assigned_task_ids': ['task'], 'node_result': {'status': 'succeeded', 'result': {'completed_task_ids': ['task']}}}
    token = {'recovered_execution_refs': ['recovered'], 'input_result_refs': ['recovered']}
    run = {'runner_policy': 'guided', 'definition': {'nodes': [checkpoint, worker]}, 'activations': [previous, recovered]}
    if invalid == 'unassigned':
        recovered['assigned_task_ids'] = []
    elif invalid == 'unrelated':
        token['input_result_refs'] = []
    elif invalid == 'superseded':
        recovered['resolved_by_execution_id'] = 'newer'
    evidence = d.completion_evidence(run, checkpoint, token, 'task')
    assert (evidence['id'] if evidence else None) == ('recovered' if invalid is None else None)
    token['decision_id'] = 'decision'
    run['tasks'] = [{'id': 'task'}]
    monkeypatch.setattr(d, 'continuations', lambda *args, **kwargs: [])
    decision = {'decision_id': 'decision', 'action': 'needs_input', 'question': 'Proceed?', 'reason': 'Checkpoint', 'task_updates': [{'task_id': 'task', 'status': 'completed', 'reason': 'Verified'}]}
    if invalid:
        with pytest.raises(w.WorkflowError, match='Completion requires'):
            d.validate_decision(run, checkpoint, token, decision, False)
    else:
        d.validate_decision(run, checkpoint, token, decision, False)



async def test_end_recovered_implementation_completes_checklist_with_actual_provenance(storage, tmp_path):
    definition = graph('implementation')
    definition['connections'][-1]['condition'] = 'Inspect result at End'
    run = storage.create_run(w.validate_definition(definition), 'Recover implementation', tmp_path)
    storage.update_run(run['workflow_run_id'], lambda r: r.update(runner_policy='guided', tasks=[{'id': 'task', 'title': 'Implement', 'status': 'pending'}]), 'test_setup')
    calls = 0
    def worker(prompt, kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {'status': 'failed', 'result': {'reason': 'Implementation needs another attempt'}, 'evidence': []}
        return {'status': 'succeeded', 'result': {'completed_task_ids': ['task']}, 'evidence': ['verified']}
    def policy(context, registry):
        if context['current_stage']['node_id'] == 'end':
            recovery = next((c for c in context['valid_continuations'] if c.get('recovery_from_checkpoint')), None)
            if recovery:
                return {'decision_id': context['decision_id'], 'action': 'continue', 'reason': 'Recover failed implementation', 'next': [{'continuation_id': recovery['continuation_id'], 'prompt': 'Complete task', 'assigned_task_ids': ['task'], 'session_mode': 'fresh'}]}
            return {'decision_id': context['decision_id'], 'action': 'complete', 'reason': 'Recovered implementation verified', 'task_updates': [{'task_id': 'task', 'status': 'completed', 'reason': 'Implementation evidence verified'}]}
        result = default_decision(context, registry)
        if context['current_stage']['node_id'] == 'work' and context['current_stage']['phase'] == 'routing':
            result['next'] = [{'continuation_id': 'finish'}]
        for entry in result.get('next', []):
            if entry.get('prompt'):
                entry['assigned_task_ids'] = ['task']
        return result
    registry = Registry(storage.root, policy, {'work': worker})
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed'
    assert calls == 2
    success = next(a for a in final['activations'] if a['role'] == 'node' and a.get('node_result', {}).get('status') == 'succeeded')
    assert final['tasks'][0]['completed_by_activation_id'] == success['id']


async def test_orchestrator_visible_prompt_uses_request_once_then_checkpoint(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path, guided=True)
    calls = [(p, k) for p, k in registry.calls if 'Context:\n' in p]
    assert len(calls) >= 2
    assert calls[0][1]['display_prompt'] == 'ORIGINAL PRIVATE REQUEST'
    assert all(k['display_prompt'] != 'ORIGINAL PRIVATE REQUEST' for p, k in calls[1:])
    assert all(k['display_prompt'].startswith('Workflow checkpoint:') for p, k in calls[1:])
    assert all('decision_id' not in k['display_prompt'] and 'Context:' not in k['display_prompt'] for p, k in calls)
    assert all(c['original_request'] == 'ORIGINAL PRIVATE REQUEST' for c in registry.contexts)
    turns = [t for a in run['activations'] if a['role'] == 'orchestrator' for t in a['tasks']]
    assert [t['assignment_prompt'] for t in turns] == [k['display_prompt'] for p, k in calls]


async def test_optional_settled_timeout_with_prior_denial_bypasses(storage, tmp_path):
    denial = {"command": "git -C repo rev-parse HEAD", "reason": "Not allowed"}
    registry = Registry(storage.root, outputs={"left": {"status": "failed", "summary": "Node timed out", "timed_out": True, "timeout_seconds": 600, "permission_denials": [denial]}})
    run, _ = await run_flow(storage, tmp_path, parallel(True), registry, guided=True)
    assert run["status"] == "completed"
    activation = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "left")
    assert activation["optional_failure"]
    assert activation["node_result"]["result"]["failure_kind"] == "timeout"
    assert activation["node_result"]["evidence"][0]["permission_denials"] == [denial]
    assert activation["node_result"]["evidence"][0]["timed_out"] is True
    assert not run["joins"]


async def test_resume_optional_timeout_reconciles_old_unmarked_failure(storage, tmp_path, monkeypatch):
    original = d.mark_optional_failure
    monkeypatch.setattr(d, "mark_optional_failure", lambda *args: None)
    def policy(context, registry):
        if context["current_stage"]["node_id"] == "left":
            return {"decision_id": context["decision_id"], "action": "needs_input", "reason": "Old optional timeout was blocked", "question": "Continue?"}
        return default_decision(context, registry)
    registry = Registry(storage.root, policy, outputs={"left": {"status": "failed", "summary": "Timeout", "timed_out": True, "permission_denials": ["Denied incidental read"]}})
    run, _ = await run_flow(storage, tmp_path, parallel(True), registry, guided=True)
    assert run["status"] == "needs_input"
    monkeypatch.setattr(d, "mark_optional_failure", original)
    registry.policy = default_decision
    storage.control(run["workflow_run_id"], "resume", instructions="Continue after timeout", decision_id=run["input_decision_id"])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    run = storage.get_run(run["workflow_run_id"])
    assert run["status"] == "completed"
    assert next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "left")["optional_failure"]


async def test_optional_timeout_explicit_parallel_releases_join(storage, tmp_path):
    from test_workflow_explicit_parallel import explicit
    from test_workflow_traversal import policy
    g = explicit()
    for n in g["nodes"]:
        if n["type"] == "agent":
            n.update(role="task", title=n["id"], agent={"backend": "codex"})
    g["orchestrator"] = {"backend": "codex"}
    next(n for n in g["nodes"] if n["id"] == "right").update(optional=True, title="right")
    registry = Registry(storage.root, policy, {"right": {"status": "failed", "summary": "Timeout", "timed_out": True, "permission_denials": ["Denied read"]}})
    run, _ = await run_flow(storage, tmp_path, g, registry, guided=True)
    assert run["status"] == "completed"
    assert len(run["released_parallel_groups"]) == 1
    assert not run["joins"]
    assert "optional_branch_bypassed" in (storage.root / "workflow-runs" / (run["workflow_run_id"] + ".jsonl")).read_text()


def test_every_issued_barrier_arrival_validates():
    from test_workflow_explicit_parallel import explicit
    g = w.validate_definition(explicit())
    right = next(n for n in g["nodes"] if n["id"] == "right")
    right["optional"] = True
    token = {"id": "branch", "decision_id": "decision", "execution_complete": True, "execution_activation_id": "result", "result": {"status": "failed"}, "stack": ["group"]}
    activation = {"id": "result", "role": "node", "node_id": "right", "status": "failed", "tasks": [], "node_result": {"status": "failed", "result": {"failure_kind": "permission"}}, "optional_failure": False}
    run = {"definition": g, "activations": [activation], "joins": {"group": {"join_id": "merge"}}, "tasks": [], "runner_policy": "guided", "transitions": 0}
    assert not any(c["kind"] == "barrier_arrival" for c in d.continuations(run, right, token, False))
    activation["optional_failure"] = True
    choices = d.continuations(run, right, token, False)
    assert choices
    for choice in choices:
        d.validate_decision(run, right, token, {"decision_id": "decision", "action": "continue", "reason": "Settled optional branch", "next": [{"continuation_id": choice["continuation_id"]}]}, False)


@pytest.mark.parametrize("mutation", ["unknown", "permission", "live", "cancelled"])
def test_settled_timeout_never_overrides_unsafe_terminal_cause(mutation):
    activation = {"status": "failed", "node_result": {"status": "failed", "result": {"failure_kind": "timeout"}}, "tasks": [{"status": "failed", "result": {"status": "failed", "timed_out": True, "permission_denials": ["Denied"]}}]}
    if mutation == "unknown":
        activation["tasks"][0]["result"]["outcome_unknown"] = True
    elif mutation == "permission":
        activation["node_result"]["result"]["failure_kind"] = "permission"
    else:
        activation["tasks"][0]["status"] = mutation if mutation == "cancelled" else "running"
    assert not d.settled_timeout(activation)
    assert not d.retry_eligible(activation)
