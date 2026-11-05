"""Worker steering authority fences independent of provider execution."""
import copy
from types import SimpleNamespace

import pytest

from harflow_mcp import workflow_delegation as delegation
from harflow_mcp import workflow_clarification as clarification
from harflow_mcp import worker_steering as worker


def test_ordinary_result_cannot_overwrite_steering_takeover():
    activation = {"id": "execution", "worker_result_required": True, "status": "running", "tasks": []}
    token = {"id": "token", "node_id": "work", "execution_activation_id": "execution"}
    run = {"activations": [activation], "pending": [token]}
    before = copy.deepcopy(run)
    delegation.finish_result(run, {"id": "work", "role": "task"}, "token", "execution", {"status": "succeeded", "result": {}, "evidence": []}, {"summary": "ordinary output"})
    assert run == before


async def test_ordinary_reentry_cannot_replenish_execution_harness():
    run = {"activations": [{"id": "execution", "worker_result_required": True, "node_result": {"status": "succeeded"}}]}
    messages = []
    supervisor = SimpleNamespace(run=lambda **kw: run, attention=messages.append)
    await delegation.execute_node(supervisor, {"id": "work", "type": "agent"}, {"id": "token", "execution_activation_id": "execution"})
    assert len(messages) == 1
    assert "cannot restart" in messages[0]


def test_question_origin_never_reuses_another_completed_response():
    activation = {"id": "execution", "tasks": [{"task_id": "old", "candidate": {}, "status": "completed"}]}
    messages = []
    supervisor = SimpleNamespace(run=lambda: {"activations": [activation]}, attention=messages.append)
    assert clarification.record_question(supervisor, {"id": "work"}, {}, "execution", {"status": "asking", "result": {"question": "Which one?"}, "evidence": []}, {"task_id": "new", "summary": "new question"}) is None
    assert "questions" not in activation
    assert messages


def test_assignment_digest_binds_actual_role_and_immutable_scope():
    activation = {"assignment_prompt": "Edit only A", "assigned_task_ids": ["a"], "token": {"id": "token"}, "input_result_refs": ["plan"]}
    token = {"assignment_prompt": "Edit only A", "assigned_task_ids": ["a"], "execution_activation_id": "execution"}
    node = {"id": "work", "role": "implementation", "freedom": "write_in_repo"}
    baseline = worker._digest(worker.assignment(activation, node, token))
    for changed in ({**node, "role": "review"}, {**node, "freedom": "unrestricted"}):
        assert worker._digest(worker.assignment(activation, changed, token)) != baseline
    assert worker._digest(worker.assignment(activation, node, {**token, "assignment_prompt": "Edit A and B"})) != baseline


def test_admission_winning_question_record_race_suppresses_ordinary_question():
    activation = {"id": "execution", "tasks": [{"task_id": "ordinary", "candidate": {}, "status": "completed"}]}
    run = {"activations": [activation], "pending": [{"id": "token"}]}
    def admission_then_mutate(mutate, event, detail=None):
        activation["worker_result_required"] = True
        mutate(run)
    supervisor = SimpleNamespace(run=lambda: run, update=admission_then_mutate, attention=lambda message: None)
    qid = clarification.record_question(supervisor, {"id": "work"}, {"id": "token"}, "execution", {"status": "asking", "result": {"question": "Old ordinary question?"}, "evidence": []}, {"task_id": "ordinary", "summary": "old question"})
    assert qid is None
    assert "questions" not in activation
    assert "question_id" not in run["pending"][0]


@pytest.mark.parametrize("drained", [True, False])
async def test_restart_boundary_releases_original_resource_only_after_positive_settlement(tmp_path, drained):
    import asyncio
    from harflow_mcp import resources, state_ownership
    from harflow_mcp.state_layout import resolve_layout
    from harflow_mcp.worker_steering_runner import _observe_boundary

    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        ledger = resources.ResourceLedger(root)
        old = ledger.request(["worker-slot"], {"kind": "workflow_attempt", "workflow_run_id": "root", "activation_id": "old-attempt"}, reservation_id="old-reservation")
        response = ledger.request(["worker-slot"], {"kind": "task", "task_id": "explicit-response"}, reservation_id="new-reservation")
        assert old.held and response.state == "queued"
        proof = {"id": "old-attempt", "candidate_index": 0, "state": "attached", "reservation_id": "old-reservation", "names": ["worker-slot"], "task_id": "original"}
        deadline = 9999999999.0
        activation = {"id": "execution", "node_id": "work", "role": "node", "status": "running", "worker_result_required": True,
            "resource_attempt": copy.deepcopy(proof), "tasks": [{"task_id": "original", "status": "running", "resource_attempt": copy.deepcopy(proof), "timeout_deadline": deadline}]}
        run = {"workflow_run_id": "root", "status": "running", "activations": [activation]}
        events = []
        class Storage:
            def get_run(self, run_id, **kwargs):
                assert run_id == "root"
                return run
            def update_run(self, run_id, mutate, event, *args):
                assert run_id == "root"
                mutate(run)
                events.append(event)
                return run
        storage = Storage()
        storage.root = root
        done = asyncio.Event()
        done.set()
        task = SimpleNamespace(task_id="original", done=done, snapshot=lambda: {"task_id": "original", "status": "completed", "exit_code": 0, "output_drained": drained, "outcome_unknown": False})
        registry = SimpleNamespace(attach=lambda task_id: task if task_id == "original" else None)
        supervisor = SimpleNamespace(store=storage, registry=registry, run=lambda **kwargs: run)
        # This owner has no ordinary _dispatch coroutine/finally block to release it.
        observed = await _observe_boundary(supervisor, {"boundary_tasks": [{"workflow_run_id": "root", "activation_id": "execution", "task_id": "original"}]})
        assert observed is drained
        assert activation["tasks"][0]["timeout_deadline"] == deadline
        assert activation["resource_attempt"]["id"] == proof["id"]
        assert activation["resource_attempt"]["reservation_id"] == proof["reservation_id"]
        if drained:
            assert ledger.get("old-reservation") is None
            assert ledger.poll("new-reservation").held
            assert activation["resource_attempt"]["released"] is True
            assert "resources_reconciled" in events
        else:
            assert ledger.get("old-reservation").held
            assert ledger.poll("new-reservation").state == "queued"
            assert "released" not in activation["resource_attempt"]
            assert not events


def applied_effect_model(run_id="root", status="succeeded"):
    import json
    command = {"status": status, "result": {"question": "Which exact criterion?"} if status == "asking" else {"summary": "Actual accepted result"}, "evidence": ["observed"]}
    target = {"role": "worker", "workflow_run_id": run_id, "activation_id": "execution", "node_id": "work", "token_id": "token", "decision_id": None,
        "interaction_epoch": 0, "checkpoint_revision": 1, "goal_revision": 0}
    turn = {"turn_id": "response", "task_id": "provider", "episode_id": "episode", "commit_target_ref": "commit-target:fixture", "steering_revision": 1,
        "candidate": {"backend": "claude"}, "target": target, "commit_target": target.copy(), "state": "captured",
        "observation": {"task_id": "provider", "status": "completed", "exit_code": 0, "output_drained": True, "timed_out": False, "outcome_unknown": False, "is_error": False}}
    wrapper = {key: turn[key] for key in ("turn_id", "episode_id", "commit_target_ref", "steering_revision")}
    wrapper.update(role="worker", command=command, **{key: target[key] for key in ("interaction_epoch", "checkpoint_revision", "goal_revision")})
    turn["final"] = "HARFLOW_COMMIT_V1\n" + json.dumps(wrapper) + "\nEND_HARFLOW_COMMIT_V1"
    activation = {"id": "execution", "node_id": "work", "role": "node", "status": "completed", "node_result": copy.deepcopy(command), "result": copy.deepcopy(command), "raw_output": turn["final"],
        "worker_steering_receipts": {"response": worker._digest(command)}, "tasks": []}
    token = {"id": "token", "execution_activation_id": "execution", "execution_complete": True, "result": copy.deepcopy(command)}
    run = {"workflow_run_id": run_id, "definition": {"nodes": [{"id": "work", "role": "task"}]}, "activations": [activation], "pending": [token]}
    if status == "asking":
        activation["questions"] = [{"question_id": "actual-question", "origin_task_id": "provider", "origin_candidate": turn["candidate"].copy(), "progress": copy.deepcopy(command["result"]), "question": command["result"]["question"], "evidence": command["evidence"].copy(), "raw_output": turn["final"]}]
    return run, activation, turn, command


@pytest.mark.parametrize("run_id", ["root", "child"])
@pytest.mark.parametrize("damage", ["digest_only", "missing_node_result", "wrong_result", "wrong_token_result", "wrong_capture"])
def test_worker_receipt_cannot_prove_missing_or_contradictory_result_effect(run_id, damage):
    run, activation, turn, command = applied_effect_model(run_id)
    assert worker.worker_effect_proven(run, activation, turn, command)
    if damage == "digest_only":
        activation.clear()
        activation.update(id="execution", node_id="work", worker_steering_receipts={"response": worker._digest(command)})
    elif damage == "missing_node_result":
        activation.pop("node_result")
    elif damage == "wrong_result":
        activation["node_result"] = {**command, "result": {"summary": "Different actual effect"}}
    elif damage == "wrong_token_result":
        run["pending"][0]["result"] = {**command, "evidence": []}
    else:
        turn["observation"]["task_id"] = "another-provider"
    assert not worker.worker_effect_proven(run, activation, turn, command)


@pytest.mark.parametrize("run_id", ["root", "child"])
def test_question_effect_receipt_requires_the_exact_response_origin_and_progress(run_id):
    run, activation, turn, command = applied_effect_model(run_id, "asking")
    assert worker.worker_effect_proven(run, activation, turn, command)
    activation["questions"][0]["origin_task_id"] = "original-ordinary-provider"
    assert not worker.worker_effect_proven(run, activation, turn, command)
    activation["questions"][0]["origin_task_id"] = turn["task_id"]
    activation["questions"][0]["progress"] = {"question": "An unrelated question?"}
    assert not worker.worker_effect_proven(run, activation, turn, command)


def test_connected_effect_proof_can_follow_a_preserved_downstream_result_checkpoint():
    run, activation, turn, command = applied_effect_model()
    run["pending"] = []
    assert not worker.worker_effect_proven(run, activation, turn, command)
    run["activations"].append({"id": "end-decision", "token": {"input_result_refs": [activation["id"]], "context": copy.deepcopy(command)}})
    assert worker.worker_effect_proven(run, activation, turn, command)


@pytest.mark.parametrize("run_id", ["root", "child"])
def test_contradictory_replayed_worker_receipt_stays_unknown_and_held(run_id, monkeypatch):
    from contextlib import nullcontext
    from harflow_mcp import orchestrator_steering
    run, activation, turn, command = applied_effect_model(run_id)
    activation.pop("node_result")
    root = {"workflow_run_id": "root", "status": "paused", "interaction_epoch": 17,
        "steering": {"sequence": 0, "history": [], "commits": [], "turns": [turn],
                     "episodes": [{"episode_id": "episode", "steering_revision": 8, "hold": False, "state": "dismissed"}]}}
    node = run["definition"]["nodes"][0]
    monkeypatch.setattr(worker, "_receipt_parts", lambda *args: (run, ["root", run_id], activation, node, run["pending"][0]))
    monkeypatch.setattr(orchestrator_steering, "_park", lambda *args: None)
    store = SimpleNamespace(tree_mutation=lambda *args: nullcontext())
    supervisor = SimpleNamespace(run_id="root", store=store, run=lambda **kwargs: root,
        update=lambda mutate, *args: mutate(root))
    worker.commit_worker_result(supervisor, turn["turn_id"])
    assert turn["state"] == "unknown" and turn["outcome"] == "steering_effect_recovery_unknown"
    assert root["steering"]["episodes"][0]["hold"] is True
    assert root["steering"]["episodes"][0]["state"] == "delivery_unknown"
    assert not root["steering"]["commits"]
    assert "node_result" not in activation


@pytest.mark.parametrize("run_id", ["root", "child"])
def test_applied_specialist_receipt_with_stale_origin_delivery_remains_protected(run_id, monkeypatch):
    from contextlib import nullcontext
    from harflow_mcp import orchestrator_steering
    run, activation, turn, command = applied_effect_model(run_id)
    turn["target"].update(token_id=None, decision_id="specialist-call")
    turn["commit_target"].update(token_id=None, decision_id="specialist-call")
    activation.update(on_demand_call_id="specialist-call", on_demand_node=run["definition"]["nodes"][0])
    call = {"id": "specialist-call", "execution_id": activation["id"], "result": copy.deepcopy(command), "task_id": turn["task_id"],
        "dispatch_state": "settled", "status": "succeeded", "result_delivered": False, "checkpoint": {"workflow_run_id": "root", "decision_id": "changed-origin"}}
    run["on_demand_calls"] = [call]
    assert worker.worker_effect_proven(run, activation, turn, command)
    assert not worker.worker_effect_proven(run, activation, turn, command, require_delivery=True)
    root = {"workflow_run_id": "root", "status": "paused", "interaction_epoch": 0,
        "steering": {"sequence": 0, "history": [], "commits": [], "turns": [turn],
                     "episodes": [{"episode_id": "episode", "steering_revision": 1, "hold": True, "state": "classifying"}]}}
    if run_id == "root":
        root.update(run)
        run = root
    monkeypatch.setattr(worker, "_receipt_parts", lambda *args: (run, ["root", run_id], activation, activation["on_demand_node"], None))
    monkeypatch.setattr(worker, "_parts", lambda *args: pytest.fail("Already applied receipt must be read before the stale ordinary checkpoint"))
    monkeypatch.setattr(orchestrator_steering, "_park", lambda *args: None)
    def update(run_id, mutate, event, *args):
        if event == "on_demand_result_delivered":
            raise ValueError("Original specialist checkpoint changed")
        mutate(run)
        return run
    store = SimpleNamespace(tree_mutation=lambda *args: nullcontext(), get_run=lambda *args, **kwargs: run, update_run=update)
    supervisor = SimpleNamespace(run_id="root", store=store, run=lambda **kwargs: root, update=lambda mutate, *args: mutate(root))
    worker.commit_worker_result(supervisor, turn["turn_id"])
    assert turn["state"] == "unknown" and turn["outcome"] == "steering_effect_recovery_unknown"
    assert root["steering"]["episodes"][0]["hold"] is True
    assert not root["steering"]["commits"]
    assert call["result"] == command and call["result_delivered"] is False


def finite_runner_model(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from harflow_mcp import orchestrator_steering, tasks, task_supervision, worker_steering_runner as runner
    run, activation, turn, _ = applied_effect_model()
    for key in ("node_result", "result", "raw_output", "worker_steering_receipts"):
        activation.pop(key, None)
    activation.update(worker_result_required=True, assignment_prompt="Exact assignment", assigned_task_ids=[])
    run["pending"][0]["execution_complete"] = False
    turn["target"]["task_id"] = turn["commit_target"]["task_id"] = "ordinary-provider"
    turn.pop("observation", None)
    turn.pop("final", None)
    turn.update(state="reserved", text="One explicit response", allow_fresh_context=True, harness_turn_cap=8, candidate={"backend": "fixture"}, boundary_tasks=[])
    activation["tasks"] = [{"task_id": "ordinary-provider", "status": "completed", "candidate": turn["candidate"], "repo_path": str(tmp_path)}]
    run.update(name="fixture", status="running", repo_path=str(tmp_path), interaction_epoch=0,
        steering={"sequence": 0, "history": [], "commits": [], "control_revision": 1, "turns": [turn],
                  "episodes": [{"episode_id": "episode", "steering_revision": 1, "hold": True, "state": "turn_reserved"}]})
    class Storage:
        def get_run(self, *args, **kwargs):
            return run
        def update_run(self, run_id, mutate, *args):
            mutate(run)
            return run
        def tree_mutation(self, *args):
            return nullcontext()
    store = Storage()
    store.root = tmp_path
    source = SimpleNamespace(task_id="ordinary-provider", backend="fixture", model=None, reasoning_effort=None,
        repo_path=str(tmp_path), freedom="write_in_repo", network=None, max_turns=17, background_idle_seconds=None,
        caller_approvals=False, caller_write_approvals=False, allowed_commands=[], observe_repos=[], interaction_owner="caller",
        worktree=None, group=None, scratch_chain_id=None, extra_writable_paths=[])
    monkeypatch.setattr(task_supervision, "read_task_record", lambda *args: source)
    monkeypatch.setattr(tasks, "get_backend", lambda name: SimpleNamespace(capabilities=SimpleNamespace(supports_turn_cap=True)))
    monkeypatch.setattr(delegation, "worker_prompt", lambda *args, **kwargs: "Immutable assignment")
    monkeypatch.setattr(runner, "_parts", lambda *args: (run, ["root"], activation, run["definition"]["nodes"][0], run["pending"][0]))
    monkeypatch.setattr(runner, "current_target", lambda *args: turn["commit_target"])
    monkeypatch.setattr(orchestrator_steering, "_park", lambda *args: None)
    supervisor = SimpleNamespace(run_id="root", store=store, run=lambda **kwargs: run, update=lambda mutate, *args: mutate(run))
    return supervisor, run, activation, turn


async def test_provider_effect_then_value_error_is_unknown_and_never_reissued(tmp_path, monkeypatch):
    from harflow_mcp import worker_steering_runner as runner
    supervisor, run, activation, turn = finite_runner_model(tmp_path, monkeypatch)
    effects = []
    async def start(*args, **kwargs):
        effects.append(kwargs["task_id"])
        raise ValueError("Lost acknowledgment after provider effect")
    supervisor.registry = SimpleNamespace(log_dir=tmp_path / "tasks", start=start, attach=lambda tid: None)
    await runner._run_owned(supervisor, "root")
    assert effects == [turn["task_id"]]
    assert turn["state"] == "unknown"
    assert run["steering"]["episodes"][0]["hold"] is True
    assert activation["tasks"][-1]["status"] == "uncertain"
    await runner._run_owned(supervisor, "root")
    assert effects == [turn["task_id"]]
    assert turn["state"] == "unknown"


@pytest.mark.parametrize("structural", [False, True])
async def test_prestart_terminal_or_missing_target_gets_durable_no_start_receipt(tmp_path, monkeypatch, structural):
    from harflow_mcp import worker_steering_runner as runner
    supervisor, run, activation, turn = finite_runner_model(tmp_path, monkeypatch)
    if structural:
        run["activations"] = []
    else:
        run["status"] = "completed"
    def unavailable(*args):
        raise worker.SteeringError("steering_target_stale", "Target ended or no longer exists")
    monkeypatch.setattr(runner, "current_target", unavailable)
    async def start(*args, **kwargs):
        pytest.fail("A stale prestart target must never launch")
    supervisor.registry = SimpleNamespace(log_dir=tmp_path / "tasks", start=start)
    await runner._run_owned(supervisor, "root")
    assert turn["state"] == "settled" and turn["outcome"] == "steering_target_stale"
    assert turn["not_started"] is True
    assert run["status"] == ("running" if structural else "completed")


def test_unavailable_applied_effect_source_keeps_claim_then_recovers_same_receipt(monkeypatch):
    from contextlib import nullcontext
    from harflow_mcp import orchestrator_steering
    root, activation, turn, command = applied_effect_model()
    root.update(status="paused", interaction_epoch=0, steering={"sequence": 0, "history": [], "commits": [], "control_revision": 1,
        "turns": [turn], "episodes": [{"episode_id": "episode", "steering_revision": 1, "hold": True, "state": "classifying"}]})
    before = copy.deepcopy(activation)
    def unavailable(*args):
        raise OSError("Applied child source is temporarily unreadable")
    monkeypatch.setattr(worker, "_receipt_parts", unavailable)
    monkeypatch.setattr(orchestrator_steering, "_park", lambda *args: None)
    store = SimpleNamespace(tree_mutation=lambda *args: nullcontext(), update_run=lambda rid, mutate, *args: mutate(root))
    supervisor = SimpleNamespace(run_id="root", store=store, run=lambda **kwargs: root, update=lambda mutate, *args: mutate(root))
    worker.commit_worker_result(supervisor, turn["turn_id"])
    assert turn["state"] == "unknown" and root["steering"]["episodes"][0]["hold"] is True
    assert not root["steering"]["commits"]
    history_count = len(root["steering"]["history"])
    worker.commit_worker_result(supervisor, turn["turn_id"])
    assert len(root["steering"]["history"]) == history_count
    monkeypatch.setattr(worker, "_receipt_parts", lambda *args: (root, ["root"], activation, root["definition"]["nodes"][0], root["pending"][0]))
    worker.commit_worker_result(supervisor, turn["turn_id"])
    assert turn["state"] == "settled" and turn["outcome"] == "committed"
    assert activation == before
    assert len(root["steering"]["commits"]) == 1


async def test_local_resource_refusal_normalizes_positive_no_start_without_losing_reason(tmp_path, monkeypatch):
    from harflow_mcp import resources, task_resources, workflow_resources, worker_steering_runner as runner
    supervisor, run, activation, turn = finite_runner_model(tmp_path, monkeypatch)
    run["definition"]["nodes"][0]["resources"] = ["restricted-slot"]
    monkeypatch.setattr(workflow_resources, "scope_for_run", lambda *args: resources.Scope(None))
    def refuse(*args):
        raise resources.ResourceNotDeclaredByScope("restricted-slot is outside the inherited scope")
    monkeypatch.setattr(task_resources, "ledger_for", lambda *args: SimpleNamespace(check_borrow=refuse))
    async def start(*args, **kwargs):
        pytest.fail("A local resource refusal must not call the provider")
    supervisor.registry = SimpleNamespace(log_dir=tmp_path / "tasks", start=start)
    await runner._run_owned(supervisor, "root")
    assert turn["state"] == "settled" and turn["outcome"] == "provider_not_started"
    assert turn["not_started"] is True
    assert "resource_not_declared_by_scope" in turn["dispatch_error"]
    assert "outside the inherited scope" in turn["dispatch_error"]
    assert activation["tasks"][-1]["status"] == "not_started"
    assert activation["worker_result_required"] is True


@pytest.mark.parametrize("dismiss", [False, True])
async def test_prestart_dismissal_or_revoked_hold_settles_same_issuance_without_effect(tmp_path, monkeypatch, dismiss):
    from harflow_mcp import worker_steering_runner as runner
    supervisor, run, activation, turn = finite_runner_model(tmp_path, monkeypatch)
    episode = run["steering"]["episodes"][0]
    episode.update(hold=False, dismiss_requested=dismiss)
    async def start(*args, **kwargs):
        pytest.fail("A prestart dismissal or revoked hold cannot launch")
    supervisor.registry = SimpleNamespace(log_dir=tmp_path / "tasks", start=start)
    await runner._run_owned(supervisor, "root")
    assert turn["state"] == "settled" and turn["outcome"] == "request_expired"
    assert turn["not_started"] is True
    assert turn["dispatch_error"] == "interaction_revoked"
    assert episode["hold"] is False
    assert len(activation["tasks"]) == 1
    assert run["status"] == "running"


def test_exhausted_actual_question_allowance_is_known_refusal_before_effect_claim(monkeypatch):
    from contextlib import nullcontext
    from harflow_mcp import orchestrator_steering
    root, activation, turn, command = applied_effect_model(status="asking")
    activation.pop("worker_steering_receipts")
    activation.pop("node_result")
    activation.pop("result")
    activation.update(worker_result_required=True, questions=[{"question_id": "previous-question", "origin_task_id": "previous-provider"}])
    root["pending"][0].update(execution_complete=False, decision_attempts=7)
    node = root["definition"]["nodes"][0]
    node["max_context_questions"] = 1
    root.update(status="paused", interaction_epoch=0, steering={"sequence": 0, "history": [], "commits": [], "control_revision": 1,
        "turns": [turn], "episodes": [{"episode_id": "episode", "steering_revision": 1, "hold": True, "state": "classifying"}]})
    monkeypatch.setattr(worker, "_receipt_parts", lambda *args: (root, ["root"], activation, node, root["pending"][0]))
    monkeypatch.setattr(worker, "_parts", lambda *args: (root, ["root"], activation, node, root["pending"][0]))
    monkeypatch.setattr(worker, "current_target", lambda *args: turn["commit_target"])
    monkeypatch.setattr(orchestrator_steering, "_park", lambda *args: None)
    monkeypatch.setattr(clarification, "record_question", lambda *args: pytest.fail("Exhausted asking must refuse before the effect writer"))
    store = SimpleNamespace(tree_mutation=lambda *args: nullcontext(), update_run=lambda rid, mutate, *args: mutate(root))
    supervisor = SimpleNamespace(run_id="root", store=store, run=lambda **kwargs: root, update=lambda mutate, *args: mutate(root))
    worker.commit_worker_result(supervisor, turn["turn_id"])
    assert turn["state"] == "settled" and turn["outcome"] == "role_command_invalid"
    assert root["steering"]["episodes"][0]["state"] == "command_refused"
    assert activation["questions"] == [{"question_id": "previous-question", "origin_task_id": "previous-provider"}]
    assert activation["worker_result_required"] is True
    assert "node_result" not in activation and "worker_steering_receipts" not in activation
    assert root["pending"][0]["decision_attempts"] == 7
