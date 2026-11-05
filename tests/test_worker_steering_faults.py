"""Independent worker-steering effect and lifecycle regressions.

Only injected in-process providers run here. The external effect file and original
TaskRecord bytes are independent of the production issuance/attempt counters.
"""
import asyncio
import copy
import json
import threading
import uuid
from types import SimpleNamespace

import pytest

from harflow_mcp import identity, lineage, store as task_store, workflow_interactions
from harflow_mcp import workflow_steering as s, workflows as w
from harflow_mcp.state_layout import resolve_layout


class PowerLoss(BaseException):
    pass


class ObservedTask:
    def __init__(self, task_id, final, **changes):
        self.task_id = task_id
        self.done = asyncio.Event()
        self.done.set()
        self.value = {"task_id": task_id, "status": "completed", "exit_code": 0,
            "output_drained": True, "timed_out": False, "outcome_unknown": False,
            "is_error": False, "failure_diagnostic": None, "summary": final,
            "backend": "claude", "session_id": "response-" + task_id,
            "usage": {"input_tokens": 11, "output_tokens": 7}, "num_turns": 2,
            "total_cost_usd": 0.004, **changes}

    def snapshot(self):
        return copy.deepcopy(self.value)


class EffectRegistry:
    def __init__(self, storage, final="Discuss this assignment before committing.", **changes):
        self.storage = storage
        self.log_dir = self._log_dir = storage.root / "tasks"
        self.state_layout = resolve_layout(log_dir=self.log_dir)
        self.final, self.changes = final, changes
        self.effects, self.tasks = [], {}
        self.before_return = None
        self.effect_path = storage.root / "observed-provider-effects.txt"

    async def start(self, prompt, repo, **options):
        tid = options["task_id"]
        self.effects.append(dict(task_id=tid, prompt=prompt, repo=str(repo), options=copy.deepcopy(options)))
        with self.effect_path.open("a") as stream:
            stream.write(tid + "\n")
        final = self.final() if callable(self.final) else self.final
        task = ObservedTask(tid, final, **self.changes)
        self.tasks[tid] = task
        backend = options["backend"]
        backend = getattr(backend, "name", backend)
        task_store.write(self.log_dir, task_store.TaskRecord(
            task_id=tid, backend=backend, session_id=task.value["session_id"],
            repo_path=str(repo), started_at="2026-11-05T00:00:00Z",
            freedom=options["freedom"], network=options.get("network"),
            model=options.get("model"), reasoning_effort=options.get("reasoning_effort"),
            max_turns=options["max_turns"], status="completed", exit_code=0,
            output_drained=True, allowed_commands=options.get("allowed_commands", [])))
        if self.before_return:
            value = self.before_return()
            if asyncio.iscoroutine(value):
                await value
        return task

    async def resume_record(self, *args, **kwargs):
        pytest.fail("Unqualified vendor Resume cannot replenish the original execution allowance")

    def attach(self, task_id):
        return self.tasks.get(task_id)

    def get(self, task_id):
        return self.tasks.get(task_id)

    async def cancel_cascade(self, *args, **kwargs):
        pytest.fail("Steering cannot interrupt issued work")


def fresh(storage, rid):
    return w.WorkflowStore(storage.root).get_run(rid, refresh_atomic=True)


@pytest.fixture
def worker_origin(tmp_path, monkeypatch):
    principal = {"kind": "external_parent", "identity": {"pid": 42310, "start_time": "qualified-generation", "markers": []}}
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: SimpleNamespace(caller=None, undecidable=None))
    monkeypatch.setattr(workflow_interactions, "capture_transport_parent", lambda: copy.deepcopy(principal))
    monkeypatch.setattr(workflow_interactions, "_transport_caller", principal)
    monkeypatch.setattr(identity, "check_detail", lambda *a, **k: ("alive", "matched"))
    monkeypatch.setattr(w, "_launch", lambda *a, **k: None)
    monkeypatch.setattr(w.backends, "is_installed", lambda *a, **k: True)
    storage = w.WorkflowStore(tmp_path / "state")
    definition = w.validate_definition({"name": "worker-effects", "context_delivery": "legacy",
        "orchestrator": {"backend": "claude"}, "nodes": [
            {"id": "start", "type": "start"},
            {"id": "work", "type": "agent", "role": "implementation", "instructions": "Only the immutable assignment.",
             "freedom": "write_in_repo", "network": False, "max_attempts": 3,
             "agent": {"backend": "claude", "model": "sonnet", "max_turns": 17}},
            {"id": "end", "type": "end"}],
        "connections": [{"id": "begin", "source": "start", "target": "work"},
                        {"id": "finish", "source": "work", "target": "end"}]})
    run = storage.create_run(definition, "Original goal", tmp_path, freedom="write_in_repo", network=False)
    rid = run["workflow_run_id"]
    node = next(n for n in definition["nodes"] if n["id"] == "work")
    token = dict(id="worker-token", node_id="work", stack=[], context={},
        assignment_prompt="Implement exactly task-one.", assigned_task_ids=["task-one"],
        input_result_refs=[], execution_activation_id="original-execution", execution_session_mode="fresh",
        accepted_decision_id="original-decision")
    original = dict(task_id="original-provider", status="completed", exit_code=0, output_drained=True,
        timed_out=False, outcome_unknown=False, is_error=False, summary="Unaccepted ordinary output.",
        backend="claude", session_id="original-session", num_turns=7,
        usage={"input_tokens": 29, "output_tokens": 19}, total_cost_usd=0.1)
    activation = dict(id="original-execution", node_id="work", role="node", status="running",
        created_at=1.0, token=copy.deepcopy(token), assignment_prompt=token["assignment_prompt"],
        assigned_task_ids=["task-one"], input_result_refs=[], execution_session_mode="fresh",
        visit_id="worker-token", attempt_in_visit=1,
        tasks=[dict(task_id="original-provider", status="completed", candidate=copy.deepcopy(node["agent"]),
                    repo_path=str(tmp_path), freedom="write_in_repo", network=False, result=original)])
    def setup(r):
        r.update(status="running", execution_initialized=True, pending=[token], activations=[activation],
            tasks=[dict(id="task-one", title="Assigned task", description="Bounded work", status="pending")])
        from harflow_mcp.steering_window import upgrade
        upgrade(r)
    storage.update_run(rid, setup, "fault_worker_checkpoint")
    task_store.write(storage.root / "tasks", task_store.TaskRecord(task_id="original-provider", backend="claude",
        session_id="original-session", repo_path=str(tmp_path), started_at="2026-11-05T00:00:00Z",
        freedom="write_in_repo", network=False, model="sonnet", max_turns=17,
        status="completed", exit_code=0, output_drained=True))
    return storage, rid


def observation(storage, rid):
    return s.get_workflow_steering(rid, root=storage.root)


def request(storage, rid, *, action="open", text="Discuss the exact assignment.", target=None, **extra):
    view = observation(storage, rid)
    episode = view["episodes"][-1] if action != "open" else None
    selected = target or next(t for t in view["targets"] if t["role"] == "worker")
    result = dict(schema_version=2, ledger_epoch=view["ledger_epoch"], request_id=str(uuid.uuid4()), action=action,
        expected_interaction_epoch=view["interaction_epoch"], expected_control_revision=view["control_revision"],
        episode_id=episode["episode_id"] if episode else None,
        expected_steering_revision=episode["steering_revision"] if episode else None,
        target=selected, allow_fresh_context=True)
    if action != "dismiss":
        result["text"] = text
    return {**result, **extra}


async def run_turn(storage, rid, registry):
    from harflow_mcp.worker_steering_runner import run_worker_turn
    await asyncio.wait_for(run_worker_turn(w.WorkflowSupervisor(registry, storage), rid), 20)
    return fresh(storage, rid)


def framed_result(storage, rid, command=None):
    turn = fresh(storage, rid)["steering"]["turns"][-1]
    wrapper = {k: turn[k] for k in ("episode_id", "turn_id", "commit_target_ref", "steering_revision")}
    wrapper.update({k: turn["commit_target"][k] for k in ("interaction_epoch", "checkpoint_revision", "goal_revision")})
    wrapper.update(role="worker", command=command or {"status": "succeeded",
        "result": {"summary": "Observed bounded work", "completed_task_ids": ["task-one"]}, "evidence": ["fixture-effect"]})
    return "HARFLOW_COMMIT_V1\n" + json.dumps(wrapper) + "\nEND_HARFLOW_COMMIT_V1"


async def test_many_explicit_responses_neither_spend_nor_replenish_normal_allowance(worker_origin):
    storage, rid = worker_origin
    # Public TaskRecord serialization is the independent original execution oracle.
    original = task_store.read(storage.root / "tasks", "original-provider")
    before = copy.deepcopy(original)
    registry = EffectRegistry(storage)
    first = None
    for index in range(40):
        req = request(storage, rid, action="open" if index == 0 else "reply", text=f"Explicit response {index}.")
        first = first or copy.deepcopy(req)
        receipt = await s.steer_workflow(rid, req, root=storage.root)
        run = await run_turn(storage, rid, registry)
        assert registry.effects[-1]["task_id"] == receipt["task_id"]
        assert len(registry.effects) == index + 1
        assert run["pending"][0].get("execution_complete") is not True
        assert len([a for a in run["activations"] if a["role"] == "node"]) == 1
        assert next(a for a in run["activations"] if a["id"] == "original-execution")["attempt_in_visit"] == 1
        assert task_store.read(storage.root / "tasks", "original-provider") == before
    assert registry.effect_path.read_text().splitlines() == [effect["task_id"] for effect in registry.effects]
    assert all(effect["options"]["max_turns"] == 8 for effect in registry.effects)
    assert all(effect["options"]["freedom"] == "write_in_repo" and effect["options"]["network"] is False for effect in registry.effects)
    assert all(effect["options"]["model"] == "sonnet" for effect in registry.effects)
    assert len(fresh(storage, rid)["steering"]["turns"]) <= 33


async def test_worker_result_commits_same_activation_once_and_replay_does_not_repeat_effect(worker_origin):
    storage, rid = worker_origin
    req = request(storage, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    registry = EffectRegistry(storage, lambda: framed_result(storage, rid))
    result = await run_turn(storage, rid, registry)
    activation = next(a for a in result["activations"] if a["id"] == "original-execution")
    assert activation["node_result"]["status"] == "succeeded"
    assert activation["node_result"]["result"]["completed_task_ids"] == ["task-one"]
    assert result["pending"][0]["execution_complete"] is True
    assert len([a for a in result["activations"] if a["role"] == "node"]) == 1
    assert len(registry.effects) == 1
    replay = await s.steer_workflow(rid, req, root=storage.root)
    assert replay["replayed"] is True
    await run_turn(storage, rid, registry)
    assert registry.effect_path.read_text().splitlines() == [registry.effects[0]["task_id"]]


@pytest.mark.parametrize("final,expected", [
    ("The result status was succeeded; let us discuss why.", "conversation"),
    ('Example:\n```json\n{"status":"succeeded","result":{},"evidence":[]}\n```', "conversation"),
    ('{"status":"succeeded","result":{},"evidence":[]}', "commit_frame_required"),
    ('HARFLOW_COMMIT_V1\n{"role":"worker", "command":\nEND_HARFLOW_COMMIT_V1', "malformed_command"),
    ('HARFLOW_COMMIT_V1\n{}\nEND_HARFLOW_COMMIT_V1', "role_command_invalid"),
])
async def test_prose_and_attempted_command_corpus_never_become_normal_result(worker_origin, final, expected):
    storage, rid = worker_origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage, final)
    run = await run_turn(storage, rid, registry)
    assert not next(a for a in run["activations"] if a["id"] == "original-execution").get("node_result")
    assert not run["pending"][0].get("execution_complete")
    assert len(registry.effects) == 1
    assert run["steering"]["turns"][-1]["outcome"] == expected


@pytest.mark.parametrize("changes", [{"exit_code": 2}, {"output_drained": False}, {"outcome_unknown": True}, {"is_error": True}])
async def test_valid_looking_result_never_commits_without_positive_capture(worker_origin, changes):
    storage, rid = worker_origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage, lambda: framed_result(storage, rid), **changes)
    run = await run_turn(storage, rid, registry)
    assert not next(a for a in run["activations"] if a["id"] == "original-execution").get("node_result")
    assert len(registry.effects) == 1
    await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1


@pytest.mark.parametrize("change", ["assignment", "goal", "owner"])
async def test_changed_worker_binding_retains_real_effect_and_refuses_old_result(worker_origin, change):
    storage, rid = worker_origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage, lambda: framed_result(storage, rid))
    def stale():
        def mutate(run):
            if change == "assignment":
                run["pending"][0]["assignment_prompt"] = "A different assignment."
            elif change == "goal":
                run.update(goal="Changed goal", goal_revision=run.get("goal_revision", 0) + 1)
            else:
                run["interaction_epoch"] += 1
        storage.update_run(rid, mutate, "fault_external_change")
    registry.before_return = stale
    result = await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1
    assert not next(a for a in result["activations"] if a["id"] == "original-execution").get("node_result")
    assert result["steering"]["episodes"][-1]["hold"]
    assert result["steering"]["turns"][-1]["usage"]["cost_usd"] == 0.004


@pytest.mark.parametrize("status", ["cancelled", "cancelling", "completed", "failed", "needs_input", "needs_attention"])
@pytest.mark.parametrize("command", [False, True])
async def test_capture_cannot_overwrite_concurrent_real_workflow_state(worker_origin, status, command):
    storage, rid = worker_origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage, (lambda: framed_result(storage, rid)) if command else "Discuss the result first.")
    def change_state():
        def change(run):
            run.update(status=status, attention_reason="Real independent workflow state")
            if status == "needs_input":
                run.update(input_question="Actual question", input_decision_id="real-question")
        storage.update_run(rid, change, "fault_real_workflow_control")
    registry.before_return = change_state
    result = await run_turn(storage, rid, registry)
    assert result["status"] == status
    assert result["attention_reason"] == "Real independent workflow state"
    if status == "needs_input":
        assert result["input_question"] == "Actual question" and result["input_decision_id"] == "real-question"
    assert not next(a for a in result["activations"] if a["id"] == "original-execution").get("node_result")
    assert len(registry.effects) == 1


async def test_dismissed_worker_still_requires_explicit_result_and_cannot_replenish_execution(worker_origin):
    storage, rid = worker_origin
    original = copy.deepcopy(task_store.read(storage.root / "tasks", "original-provider"))
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage)
    await run_turn(storage, rid, registry)
    await s.steer_workflow(rid, request(storage, rid, action="dismiss"), root=storage.root)
    dismissed = fresh(storage, rid)
    assert dismissed["status"] == "paused"
    assert not dismissed["steering"]["episodes"][-1]["hold"]
    with pytest.raises(w.WorkflowError, match="(?i)(worker|result|steering)"):
        storage.control(rid, "resume")
    assert fresh(storage, rid)["status"] == "paused"
    assert len(registry.effects) == 1
    assert task_store.read(storage.root / "tasks", "original-provider") == original
    assert not dismissed["pending"][0].get("execution_complete")


@pytest.mark.parametrize("same", [False, True])
async def test_simultaneous_worker_callers_authorize_one_external_effect(worker_origin, monkeypatch, same):
    storage, rid = worker_origin
    first = request(storage, rid)
    second = copy.deepcopy(first) if same else request(storage, rid)
    barrier = threading.Barrier(2, timeout=10)
    transport = s._transport
    def synchronized(*args):
        value = transport(*args)
        barrier.wait()
        return value
    monkeypatch.setattr(s, "_transport", synchronized)
    values = await asyncio.gather(s.steer_workflow(rid, first, root=storage.root), s.steer_workflow(rid, second, root=storage.root), return_exceptions=True)
    monkeypatch.setattr(s, "_transport", transport)
    receipts = [v for v in values if isinstance(v, dict)]
    assert len(receipts) == (2 if same else 1)
    if same:
        assert sorted(r["replayed"] for r in receipts) == [False, True]
        assert receipts[0]["task_id"] == receipts[1]["task_id"]
    else:
        assert len([v for v in values if isinstance(v, s.SteeringError)]) == 1
    registry = EffectRegistry(storage)
    await run_turn(storage, rid, registry)
    assert registry.effect_path.read_text().splitlines() == [receipts[0]["task_id"]]


async def test_lost_spawn_ack_observes_same_worker_response_without_replacement(worker_origin):
    storage, rid = worker_origin
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    registry = EffectRegistry(storage)
    registry.before_return = lambda: (_ for _ in ()).throw(PowerLoss())
    with pytest.raises(PowerLoss):
        await run_turn(storage, rid, registry)
    assert registry.effect_path.read_text().splitlines() == [receipt["task_id"]]
    registry.before_return = None
    replay = await s.steer_workflow(rid, req, root=storage.root)
    assert replay["replayed"] and replay["task_id"] == receipt["task_id"]
    await run_turn(w.WorkflowStore(storage.root), rid, registry)
    assert len(registry.effects) == 1
    assert fresh(storage, rid)["steering"]["turns"][-1]["state"] == "settled"


async def test_unknown_response_intent_remains_held_without_a_replacement(worker_origin):
    storage, rid = worker_origin
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    def uncertain(run):
        turn = run["steering"]["turns"][-1]
        turn["state"] = "spawn_intent"
        run["steering"]["episodes"][-1]["state"] = "discussing"
    storage.update_run(rid, uncertain, "fault_unknown_intent")
    registry = EffectRegistry(storage)
    await run_turn(storage, rid, registry)
    observed_sequence = fresh(storage, rid)["steering"]["sequence"]
    for _ in range(20):
        assert (await s.steer_workflow(rid, req, root=storage.root))["task_id"] == receipt["task_id"]
        await run_turn(storage, rid, registry)
        assert fresh(storage, rid)["steering"]["sequence"] == observed_sequence
    result = fresh(storage, rid)
    assert not registry.effects and not registry.effect_path.exists()
    assert result["steering"]["episodes"][-1]["hold"]
    assert result["steering"]["turns"][-1]["task_id"] == receipt["task_id"]
    assert result["steering"]["turns"][-1]["usage"]["provenance"] == "unavailable"


async def test_actual_question_frame_uses_new_response_origin_without_completing_worker(worker_origin):
    storage, rid = worker_origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage, lambda: framed_result(storage, rid, {"status": "asking",
        "result": {"question": "Which exact acceptance criterion applies?", "question_items": ["Criterion A", "Criterion B"]}, "evidence": []}))
    result = await run_turn(storage, rid, registry)
    activation = next(a for a in result["activations"] if a["id"] == "original-execution")
    question = next(q for q in activation["questions"] if q["question_id"] == activation["pending_question_id"])
    assert question["question"] == "Which exact acceptance criterion applies?"
    assert question["origin_task_id"] == registry.effects[0]["task_id"] != "original-provider"
    assert not result["pending"][0].get("execution_complete")
    assert not activation.get("node_result")
    with pytest.raises(s.SteeringError) as refused:
        await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    assert refused.value.code == "steering_question_pending"
    assert len(registry.effects) == 1


class MainRegistry(EffectRegistry):
    """Normal control decisions are a separate fake provider/effect oracle."""
    def __init__(self, storage, rid):
        from test_workflow_delegation import Registry as NormalRegistry, default_decision
        super().__init__(storage)
        self.rid = rid
        self.normal = NormalRegistry(storage.root)
        def policy(context, registry):
            if context.get("worker_question"):
                return {"decision_id": context["decision_id"], "action": "answer", "reason": "The actual criterion is known.",
                    "question_id": context["worker_question"]["question_id"], "answer": "Use Criterion A exactly.", "session_mode": "fresh"}
            context = {**context, "valid_continuations": [c for c in context["valid_continuations"] if c["kind"] != "retry_execution"]}
            decision = default_decision(context, registry)
            if context["current_stage"]["node_id"] == "work":
                decision["task_updates"] = [{"task_id": "task-one", "status": "completed", "reason": "Existing assigned worker completion evidence."}]
            return decision
        self.normal.policy = policy

    async def start(self, prompt, repo, **kwargs):
        steering_ids = {t["task_id"] for t in fresh(self.storage, self.rid)["steering"]["turns"]}
        if kwargs["task_id"] in steering_ids:
            return await super().start(prompt, repo, **kwargs)
        return await self.normal.start(prompt, repo, **kwargs)

    async def resume(self, previous, prompt, **kwargs):
        return await self.normal.resume(previous, prompt, **kwargs)

    def get(self, task_id):
        return super().get(task_id) or self.normal.get(task_id)

    def attach(self, task_id):
        return self.get(task_id)


async def owned_main(storage, rid):
    from harflow_mcp import state_ownership
    with state_ownership.state_session(resolve_layout(root=storage.root)):
        await asyncio.wait_for(w._main(SimpleNamespace(root=str(storage.root), run_id=rid)), 20)


@pytest.fixture
def qualified_main_provider(monkeypatch):
    # MainRegistry is an in-process provider with no network access. Qualify its
    # capabilities for normal orchestrator dispatch as well as steering dispatch.
    from harflow_mcp.tasks import get_backend
    backend = get_backend("claude")
    caps = backend.capabilities
    monkeypatch.setattr(type(backend), "capabilities", caps._replace(
        network_control=caps.network_control._replace(can_block=("read_only", "write_in_repo", "publish"))))


async def test_actual_main_prose_waits_then_explicit_result_advances_without_new_worker_attempt(worker_origin, monkeypatch, qualified_main_provider):
    from harflow_mcp import tasks
    storage, rid = worker_origin
    original = copy.deepcopy(task_store.read(storage.root / "tasks", "original-provider"))
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = MainRegistry(storage, rid)
    registry.final = lambda: "Discuss before finishing." if len(registry.effects) == 1 else framed_result(storage, rid)
    monkeypatch.setattr(tasks, "TaskRegistry", lambda **kwargs: registry)
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(storage.root))
    await owned_main(storage, rid)
    waiting = fresh(storage, rid)
    assert waiting["status"] == "paused", waiting.get("attention_reason")
    assert len(registry.effects) == 1 and not registry.normal.calls
    assert not waiting["pending"][0].get("execution_complete")
    await s.steer_workflow(rid, request(storage, rid, action="reply"), root=storage.root)
    await owned_main(storage, rid)
    completed = fresh(storage, rid)
    assert completed["status"] == "completed", completed.get("attention_reason") or completed.get("failure_reason")
    assert len(registry.effects) == 2
    assert not [(p, k) for p, k in registry.normal.calls if "Assignment:\n" in p]
    assert len([a for a in completed["activations"] if a["role"] == "node"]) == 1
    assert task_store.read(storage.root / "tasks", "original-provider") == original
    assert next(a for a in completed["activations"] if a["id"] == "original-execution")["attempt_in_visit"] == 1


async def test_real_question_answer_waits_for_next_explicit_bounded_response(worker_origin, monkeypatch, qualified_main_provider):
    from harflow_mcp import tasks
    storage, rid = worker_origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = MainRegistry(storage, rid)
    registry.final = lambda: framed_result(storage, rid, {"status": "asking", "result": {"question": "Which criterion?"}, "evidence": []}) if len(registry.effects) == 1 else framed_result(storage, rid)
    monkeypatch.setattr(tasks, "TaskRegistry", lambda **kwargs: registry)
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(storage.root))
    await owned_main(storage, rid)
    awaiting = fresh(storage, rid)
    activation = next(a for a in awaiting["activations"] if a["id"] == "original-execution")
    question = activation["questions"][-1]
    assert question["status"] == "answered" and question["answer"] == "Use Criterion A exactly."
    assert question["answer_delivery_state"] == "pending"
    assert question["origin_task_id"] == registry.effects[0]["task_id"]
    assert len(registry.effects) == 1
    assert not [(p, k) for p, k in registry.normal.calls if "Assignment:\n" in p]
    assert not awaiting["pending"][0].get("execution_complete")
    qid = question["question_id"]
    receipt = await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    await owned_main(storage, rid)
    completed = fresh(storage, rid)
    activation = next(a for a in completed["activations"] if a["id"] == "original-execution")
    question = next(q for q in activation["questions"] if q["question_id"] == qid)
    assert question["answer_delivery_state"] == "delivered"
    assert question["reply_task_id"] == receipt["task_id"]
    assert "Use Criterion A exactly." in registry.effects[-1]["prompt"]
    assert qid in registry.effects[-1]["prompt"]
    assert completed["status"] == "completed", completed.get("attention_reason")
    assert len(registry.effects) == 2


def nested_worker_checkpoint(storage, repo):
    from test_orchestrator_steering_faults import nested_checkpoint
    from harflow_mcp.steering_window import upgrade
    rid, cid = nested_checkpoint(storage, repo)
    storage.update_run(rid, upgrade, "fault_nested_window_upgrade")
    child = fresh(storage, cid)
    node = next(n for n in child["definition"]["nodes"] if n["id"] == "work")
    token = dict(id="child-worker-token", node_id="work", stack=[], context={},
        assignment_prompt="Child immutable assignment.", assigned_task_ids=[], input_result_refs=[],
        execution_activation_id="child-execution", execution_session_mode="fresh", accepted_decision_id="child-route")
    snapshot = dict(task_id="child-provider", status="completed", exit_code=0, output_drained=True,
        timed_out=False, outcome_unknown=False, is_error=False, backend="claude", session_id="child-original-session",
        summary="Child unaccepted ordinary output.", usage={"input_tokens": 3}, num_turns=2, total_cost_usd=0.1)
    activation = dict(id="child-execution", node_id="work", role="node", status="running", created_at=1.0,
        token=copy.deepcopy(token), assignment_prompt=token["assignment_prompt"], assigned_task_ids=[], input_result_refs=[],
        execution_session_mode="fresh", visit_id=token["id"], attempt_in_visit=1,
        tasks=[dict(task_id="child-provider", status="completed", candidate=copy.deepcopy(node["agent"]),
                    repo_path=str(repo), freedom="read_only", network=child.get("network"), result=snapshot)])
    storage.update_run(cid, lambda r: r.update(pending=[token], activations=[activation]), "fault_child_worker")
    task_store.write(storage.root / "tasks", task_store.TaskRecord(task_id="child-provider", backend="claude",
        session_id="child-original-session", repo_path=str(repo), started_at="2026-11-05T00:00:00Z",
        freedom="read_only", network=child.get("network"), max_turns=node["agent"].get("max_turns"),
        model=node["agent"].get("model"), reasoning_effort=node["agent"].get("reasoning_effort"),
        status="completed", exit_code=0, output_drained=True))
    return rid, cid


@pytest.mark.parametrize("nested", [False, True])
async def test_crash_after_existing_result_acceptance_before_root_ack_is_observed_once(worker_origin, tmp_path, monkeypatch, nested):
    storage, rid = worker_origin
    target_id, activation_id = rid, "original-execution"
    command = None
    if nested:
        rid, target_id = nested_worker_checkpoint(storage, tmp_path)
        activation_id = "child-execution"
        command = {"status": "succeeded", "result": {"summary": "Child result"}, "evidence": ["child-effect"]}
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    registry = EffectRegistry(storage, lambda: framed_result(storage, rid, command))
    original_update = storage.update_run
    cut = False
    def accepted_then_crash(run_id, mutator, event, *args, **kwargs):
        nonlocal cut
        result = original_update(run_id, mutator, event, *args, **kwargs)
        if event == "worker_steering_result_accepted" and not cut:
            cut = True
            raise PowerLoss()
        return result
    monkeypatch.setattr(storage, "update_run", accepted_then_crash)
    with pytest.raises(PowerLoss):
        await run_turn(storage, rid, registry)
    assert cut
    accepted = fresh(storage, target_id)
    activation = next(a for a in accepted["activations"] if a["id"] == activation_id)
    assert activation["node_result"]["status"] == "succeeded"
    assert len(registry.effects) == 1
    monkeypatch.setattr(storage, "update_run", original_update)
    assert (await s.steer_workflow(rid, req, root=storage.root))["task_id"] == receipt["task_id"]
    await run_turn(w.WorkflowStore(storage.root), rid, registry)
    final = fresh(storage, target_id)
    same = next(a for a in final["activations"] if a["id"] == activation_id)
    assert same["node_result"] == activation["node_result"]
    assert len(same["worker_steering_receipts"]) == 1
    assert len(registry.effects) == 1
    assert fresh(storage, rid)["steering"]["turns"][-1]["outcome"] == "committed"


def specialist_checkpoint(storage, rid):
    from harflow_mcp.workflow_on_demand import runtime_node, catalog
    def configure(run):
        run["definition"] = w.validate_definition({**run["definition"], "on_demand_nodes": [{
            "id": "specialist", "title": "specialist", "instructions": "Only the specialist assignment.",
            "agent": {"backend": "claude", "model": "sonnet", "max_turns": 17},
            "freedom": "write_in_repo", "network": False, "session_mode": "fresh", "execution_mode": "headless"}]})
        run["pending"][0].update(decision_id="specialist-decision", decision_activation_id="specialist-authorizer", on_demand_count=1,
            pending_on_demand_call_id="specialist-call", on_demand_results=[], on_demand_applied_call_ids=["specialist-call"])
    storage.update_run(rid, configure, "fault_configured_specialist")
    run = fresh(storage, rid)
    entry = next(e for e in catalog(storage, run) if e["local_node_id"] == "specialist")
    node = runtime_node(storage, run, entry)
    node["session_mode"] = "fresh"
    assignment = "Inspect exactly the specialist evidence."
    action = dict(decision_id="specialist-decision", action="call_on_demand", node_id=node["id"], assignment=assignment,
        reason="Configured specialist evidence needed.", session_mode="fresh")
    checkpoint = dict(workflow_run_id=rid, decision_id="specialist-decision", token_id="worker-token", question_id=None)
    call = dict(id="specialist-call", call_number=1, node_id=node["id"], source_workflow_run_id=rid, local_node_id="specialist",
        title="specialist", assignment=assignment, reason=action["reason"], checkpoint=checkpoint,
        origin=dict(kind="routing", node_id="work", title="work", workflow_run_id=rid), status="running", started_at=1.0,
        finished_at=None, execution_id="specialist-execution", result_ref=dict(workflow_run_id=rid, execution_id="specialist-execution"),
        task_id="specialist-provider", result=None, failure=None, before_count=0, orchestration_activation_id="specialist-authorizer",
        action=action, dispatch_state="activation_reserved", execution_session_mode="fresh", resume_task_id=None)
    snapshot = dict(task_id="specialist-provider", status="completed", exit_code=0, output_drained=True, timed_out=False,
        outcome_unknown=False, is_error=False, backend="claude", session_id="specialist-session", summary="Ordinary specialist output.")
    activation = dict(id="specialist-execution", node_id=node["id"], role="node", status="running", created_at=1.0,
        token={}, on_demand_call_id=call["id"], on_demand_node=node, assignment_prompt=assignment,
        execution_session_mode="fresh", tasks=[dict(task_id="specialist-provider", candidate=node["agent"], status="completed",
            repo_path=run["repo_path"], freedom="write_in_repo", network=False, result=snapshot)])
    authorizer = dict(id="specialist-authorizer", node_id="work", role="orchestrator", status="completed", created_at=1.0,
        token=copy.deepcopy(run["pending"][0]), tasks=[], decision_id="specialist-decision", decision_disposition="call_on_demand")
    storage.update_run(rid, lambda r: (r.update(on_demand_calls=[call]), r["activations"].extend([authorizer, activation])), "fault_specialist_reservation")
    task_store.write(storage.root / "tasks", task_store.TaskRecord(task_id="specialist-provider", backend="claude",
        session_id="specialist-session", repo_path=run["repo_path"], started_at="2026-11-05T00:00:00Z", freedom="write_in_repo",
        network=False, model="sonnet", max_turns=17, status="completed", exit_code=0, output_drained=True))
    return next(t for t in observation(storage, rid)["targets"] if t["role"] == "worker" and t["activation_id"] == activation["id"])


async def test_specialist_steering_wins_ordinary_settlement_then_delivers_one_exact_result(worker_origin):
    from harflow_mcp import workflow_on_demand
    storage, rid = worker_origin
    target = specialist_checkpoint(storage, rid)
    req = request(storage, rid, target=target)
    await s.steer_workflow(rid, req, root=storage.root)
    workflow_on_demand.settle(storage, rid, "specialist-call", {"task_id": "specialist-provider", "status": "completed",
        "summary": json.dumps({"status": "succeeded", "result": {"summary": "OLD result"}, "evidence": []})})
    held = fresh(storage, rid)
    assert held["on_demand_calls"][0]["status"] == "running"
    assert held["on_demand_calls"][0]["result"] is None
    assert held["pending"][0]["pending_on_demand_call_id"] == "specialist-call"
    assert held["pending"][0]["on_demand_results"] == []
    registry = EffectRegistry(storage, lambda: framed_result(storage, rid, {"status": "succeeded",
        "result": {"summary": "NEW exact specialist result"}, "evidence": ["one-observed-effect"]}))
    result = await run_turn(storage, rid, registry)
    call = result["on_demand_calls"][0]
    assert call["status"] == "succeeded" and call["result"]["result"]["summary"] == "NEW exact specialist result"
    assert call["result_delivered"] is True
    assert len(result["pending"][0]["on_demand_results"]) == 1
    assert len([a for a in result["activations"] if a["id"] == "specialist-execution"]) == 1
    await s.steer_workflow(rid, req, root=storage.root)
    await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1
    assert len(fresh(storage, rid)["pending"][0]["on_demand_results"]) == 1


async def test_normal_monitor_snapshot_omitting_optional_negatives_accepts_result(worker_origin):
    storage, rid = worker_origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage, lambda: framed_result(storage, rid))
    def normal_monitor_shape():
        task = next(iter(registry.tasks.values()))
        del task.value["timed_out"]
        del task.value["outcome_unknown"]
    registry.before_return = normal_monitor_shape
    result = await run_turn(storage, rid, registry)
    activation = next(a for a in result["activations"] if a["id"] == "original-execution")
    assert activation["node_result"]["status"] == "succeeded"
    assert result["steering"]["turns"][-1]["outcome"] == "committed"
    assert len(registry.effects) == 1


@pytest.mark.parametrize("unsupported", ["native", "resume"])
async def test_unsupported_response_budget_refuses_before_effect(worker_origin, unsupported):
    storage, rid = worker_origin
    if unsupported == "native":
        storage.update_run(rid, lambda r: r["activations"][0]["tasks"][0].update(execution_kind="native_subagent"), "fault_native")
    req = request(storage, rid, allow_fresh_context=unsupported != "resume")
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, req, root=storage.root)
    assert error.value.code == "steering_budget_unsupported"
    assert fresh(storage, rid)["steering"]["turns"] == []
    assert not (storage.root / "observed-provider-effects.txt").exists()


async def test_smaller_original_candidate_cap_bounds_each_explicit_response(worker_origin):
    storage, rid = worker_origin
    storage.update_run(rid, lambda r: r["activations"][0]["tasks"][0]["candidate"].update(max_turns=3), "fault_small_cap")
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage)
    await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1
    assert registry.effects[0]["options"]["max_turns"] == 3
    assert task_store.read(storage.root / "tasks", "original-provider").max_turns == 17


@pytest.mark.parametrize("positive", [True, False])
async def test_active_original_generation_is_observed_before_any_response_effect(worker_origin, positive):
    storage, rid = worker_origin
    def active(run):
        attempt = run["activations"][0]["tasks"][0]
        attempt.update(status="running")
        attempt.pop("result")
    storage.update_run(rid, active, "fault_original_still_running")
    req = request(storage, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    assert fresh(storage, rid)["steering"]["turns"][-1]["state"] == "queued"
    registry = EffectRegistry(storage)
    original = ObservedTask("original-provider", "Ordinary result must not win after admission.", outcome_unknown=not positive)
    original.done.clear()
    attached = asyncio.Event()
    ordinary_attach = registry.attach
    def attach(task_id):
        if task_id == "original-provider":
            attached.set()
            return original
        return ordinary_attach(task_id)
    registry.attach = attach
    pending = asyncio.create_task(run_turn(storage, rid, registry))
    await asyncio.wait_for(attached.wait(), 5)
    assert registry.effects == []
    assert not registry.effect_path.exists()
    original.done.set()
    result = await pending
    assert len(registry.effects) == int(positive)
    assert result["steering"]["turns"][-1]["state"] == ("settled" if positive else "unknown")
    assert not result["activations"][0].get("node_result")
    if not positive:
        await s.steer_workflow(rid, req, root=storage.root)
        await run_turn(storage, rid, registry)
        assert registry.effects == []

@pytest.mark.asyncio
@pytest.mark.parametrize('drained', [True, False])
async def test_restarted_legacy_cancel_observes_blocked_worker_without_accepting_result(worker_origin, monkeypatch, drained):
    from dataclasses import replace
    from harflow_mcp import task_supervision
    storage, rid = worker_origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    record = task_store.read(storage.root / 'tasks', 'original-provider')
    record = replace(record, status='cancelled', exit_code=-15, output_drained=drained)
    task_store.write(storage.root / 'tasks', record)
    def cancelling(run):
        run['status'] = 'cancelling'
        original = run['activations'][0]['tasks'][0]
        original['status'] = 'running'
        original.pop('result', None)
    storage.update_run(rid, cancelling, 'fixture_cancel_restart')
    monkeypatch.setattr(w, 'task_liveness', lambda *a, **k: {'process_alive': False, 'outcome_known': True})
    monkeypatch.setattr(task_supervision, 'owner_holds_unsettled', lambda *a: False)
    class CancelRegistry(EffectRegistry):
        async def cancel_cascade(self, task_id, **kwargs):
            assert task_id == 'original-provider' and kwargs == {'workflow_control': True}
            return {'task_id': task_id, 'cascade_incomplete': False}
    registry = CancelRegistry(storage)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(rid), 5)
    final = fresh(storage, rid)
    activation = final['activations'][0]
    assert final['status'] == ('cancelled' if drained else 'needs_attention')
    assert activation['tasks'][0]['status'] == ('cancelled' if drained else 'running')
    assert activation['status'] == ('cancelled' if drained else 'running')
    assert activation['worker_result_required'] is True
    assert 'node_result' not in activation and not final['pending'][0].get('execution_complete')
    assert not activation.get('questions') and not registry.effects
