"""Independent ST03-A dispatch/capture oracles; no external process is launched.

Registry.effects is the observation oracle, rather than production reservations.
Every final command is authored here; expected parsing outcomes are literal cases.
"""
import asyncio
import copy
import json
import threading
import uuid
from types import SimpleNamespace

import pytest

from harflow_mcp import identity, lineage, workflow_interactions
from harflow_mcp import workflow_steering as s, workflows as w
from harflow_mcp.state_layout import resolve_layout


class PowerLoss(BaseException):
    pass


class ObservedTask:
    def __init__(self, task_id, final, **changes):
        self.task_id = task_id
        self.done = asyncio.Event()
        self.done.set()
        self.observation = {
            "task_id": task_id, "status": "completed", "exit_code": 0,
            "output_drained": True, "timed_out": False, "outcome_unknown": False,
            "is_error": False, "failure_diagnostic": None,
            "summary": final, "session_id": "synthetic-session", "backend": "claude",
            "usage": {"input_tokens": 13, "output_tokens": 7}, "num_turns": 1,
            "total_cost_usd": 0.0001, **changes,
        }

    def snapshot(self):
        return copy.deepcopy(self.observation)


class EffectRegistry:
    """A fake provider with an independently inspectable issuance/effect log."""
    def __init__(self, storage, final="Discuss the configured route first.", **changes):
        self.log_dir = self._log_dir = storage.root / "tasks"
        self.state_layout = resolve_layout(log_dir=self.log_dir)
        self.storage = storage
        self.final = final
        self.changes = changes
        self.effects = []
        self.tasks = {}
        self.before_return = None

    async def start(self, prompt, repo, **kwargs):
        tid = kwargs["task_id"]
        self.effects.append({"task_id": tid, "prompt": prompt, "options": copy.deepcopy(kwargs)})
        final = self.final() if callable(self.final) else self.final
        task = ObservedTask(tid, final, **self.changes)
        self.tasks[tid] = task
        if self.before_return:
            result = self.before_return()
            if asyncio.iscoroutine(result):
                await result
        return task

    def attach(self, task_id):
        return self.tasks.get(task_id)

    def get(self, task_id):
        return self.tasks.get(task_id)

    async def cancel_cascade(self, *args, **kwargs):
        pytest.fail("Conversational steering must not cancel real issued work")


@pytest.fixture
def origin(tmp_path, monkeypatch):
    binding = {"kind": "external_parent", "identity": {"pid": 43210, "start_time": "admitted-generation", "markers": []}}
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: SimpleNamespace(caller=None, undecidable=None))
    monkeypatch.setattr(workflow_interactions, "capture_transport_parent", lambda: copy.deepcopy(binding))
    monkeypatch.setattr(workflow_interactions, "_transport_caller", binding)
    monkeypatch.setattr(identity, "check_detail", lambda *a, **k: ("alive", "matched"))
    monkeypatch.setattr(w, "_launch", lambda *a, **k: None)
    monkeypatch.setattr(w.backends, "is_installed", lambda *a, **k: True)
    storage = w.WorkflowStore(tmp_path / "state")
    definition = w.validate_definition({
        "name": "orchestrator-fault-oracle", "orchestrator": {"backend": "claude"},
        "nodes": [{"id": "start", "type": "start", "always_route": True},
                  {"id": "work", "type": "agent", "agent": {"backend": "claude"}, "instructions": "Configured work only."},
                  {"id": "end", "type": "end"}],
        "connections": [{"id": "begin", "source": "start", "target": "work"},
                        {"id": "finish", "source": "work", "target": "end"}],
    })
    run = storage.create_run(definition, "Original exact goal", tmp_path, freedom="read_only")
    rid = run["workflow_run_id"]
    storage.update_run(rid, lambda r: r.update(
        status="running", execution_initialized=True,
        pending=[{"id": "checkpoint-token", "node_id": "start", "stack": [], "context": {},
                  "decision_id": "checkpoint-decision", "decision_attempts": 0,
                  "on_demand_count": 0, "on_demand_results": []}]), "oracle_checkpoint")
    return storage, rid


def fresh(storage, rid):
    return w.WorkflowStore(storage.root).get_run(rid, refresh_atomic=True)


def observation(storage, rid):
    return s.get_workflow_steering(rid, root=storage.root)


def request(storage, rid, *, action="open", text="Discuss this exact checkpoint.", **extra):
    view = observation(storage, rid)
    ep = view["episodes"][-1] if action != "open" else None
    req = {"schema_version": 1, "request_id": str(uuid.uuid4()), "action": action,
           "expected_interaction_epoch": view["interaction_epoch"],
           "expected_control_revision": view["control_revision"],
           "episode_id": ep["episode_id"] if ep else None,
           "expected_steering_revision": ep["steering_revision"] if ep else None,
           "target": view["targets"][0], "allow_fresh_context": True}
    if action != "dismiss":
        req["text"] = text
    return {**req, **extra}


async def run_turn(storage, rid, registry):
    supervisor = w.WorkflowSupervisor(registry, storage)
    from harflow_mcp import orchestrator_steering
    await asyncio.wait_for(orchestrator_steering.run_turn(supervisor, rid), 20)
    return fresh(storage, rid)


def valid_command(storage, rid):
    turn = fresh(storage, rid)["steering"]["turns"][-1]
    witness = turn["commit_target"]
    wrapper = {key: turn[key] for key in ("episode_id", "turn_id", "commit_target_ref", "steering_revision")}
    wrapper.update({key: witness[key] for key in ("interaction_epoch", "checkpoint_revision", "goal_revision")})
    wrapper.update(role="orchestrator", command={
        "decision_id": "checkpoint-decision", "action": "continue", "reason": "Use the configured work edge.",
        "next": [{"continuation_id": "begin", "prompt": "Configured work assignment.", "session_mode": "fresh"}],
    })
    if turn.get("context_delivery"):
        # Copy the exact issued acknowledgement fields, independently of the
        # production acknowledgement validator/normalizer.
        wrapper["command"]["context_ack"] = {key: copy.deepcopy(turn["context_delivery"][key]) for key in ("version", "scope", "session_owner", "revision", "digest", "base_revision")}
    return "HARFLOW_COMMIT_V1\n" + json.dumps(wrapper) + "\nEND_HARFLOW_COMMIT_V1"


@pytest.mark.parametrize("same_request", [True, False])
async def test_simultaneous_callers_charge_only_one_issuance(origin, monkeypatch, same_request):
    storage, rid = origin
    first = request(storage, rid)
    second = copy.deepcopy(first) if same_request else request(storage, rid)
    barrier = threading.Barrier(2, timeout=10)
    qualify = s._transport
    def together(*args):
        value = qualify(*args)
        barrier.wait()
        return value
    monkeypatch.setattr(s, "_transport", together)
    results = await asyncio.gather(
        s.steer_workflow(rid, first, root=storage.root),
        s.steer_workflow(rid, second, root=storage.root), return_exceptions=True)
    monkeypatch.setattr(s, "_transport", qualify)
    accepted = [r for r in results if isinstance(r, dict)]
    assert len(accepted) == (2 if same_request else 1)
    if same_request:
        assert sorted(r["replayed"] for r in accepted) == [False, True]
        assert accepted[0]["task_id"] == accepted[1]["task_id"]
    else:
        assert len([r for r in results if isinstance(r, s.SteeringError)]) == 1
    registry = EffectRegistry(storage)
    await run_turn(storage, rid, registry)
    assert [effect["task_id"] for effect in registry.effects] == [accepted[0]["task_id"]]
    assert len(fresh(storage, rid)["steering"]["turns"]) == 1


async def test_prose_waits_without_dispatch_or_ordinary_decision_charge(origin):
    storage, rid = origin
    before = fresh(storage, rid)
    req = request(storage, rid, text="Verbatim request.\n  Keep this spacing.")
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    registry = EffectRegistry(storage, "The configured route is available; awaiting your choice.")
    run = await run_turn(storage, rid, registry)
    await run_turn(storage, rid, registry)
    await s.steer_workflow(rid, req, root=storage.root)
    await run_turn(storage, rid, registry)
    assert [e["task_id"] for e in registry.effects] == [receipt["task_id"]]
    assert req["text"] in registry.effects[0]["prompt"]
    assert registry.effects[0]["options"]["max_turns"] == 8
    assert registry.effects[0]["options"]["freedom"] == "read_only"
    assert run["pending"] == before["pending"]
    assert run["decisions"] == [] and run["transitions"] == before["transitions"]
    assert run["steering"]["episodes"][-1]["state"] == "awaiting_caller"
    assert run["steering"]["turns"][-1]["final"] == registry.final
    assert len(run["steering"]["turns"]) == 1


async def test_valid_frame_reserves_configured_route_exactly_once(origin):
    storage, rid = origin
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    registry = EffectRegistry(storage, lambda: valid_command(storage, rid))
    run = await run_turn(storage, rid, registry)
    await run_turn(w.WorkflowStore(storage.root), rid, registry)
    replay = await s.steer_workflow(rid, req, root=storage.root)
    assert replay["task_id"] == receipt["task_id"] and replay["replayed"]
    assert len(registry.effects) == 1
    assert len(run["decisions"]) == 1 and run["decisions"][0]["action"] == "continue"
    assert len(run["steering"]["commits"]) == 1
    assert run["steering"]["episodes"][-1]["state"] == "committed"
    token = run["pending"][0]
    assert token["selected_connections"] == ["begin"]
    assert token["accepted_decision_id"] == "checkpoint-decision"
    assert token["assignments"]["begin"]["assignment_prompt"] == "Configured work assignment."


@pytest.mark.parametrize("final,expected", [
    ('{"decision_id":"checkpoint-decision","action":"continue"}', "command_refused"),
    ('HARFLOW_COMMIT_V1\n{"role":"orchestrator"}\nEND_HARFLOW_COMMIT_V1', "command_refused"),
    ('HARFLOW_COMMIT_V1\n{broken}\nEND_HARFLOW_COMMIT_V1', "command_refused"),
    ('Explanation before\nHARFLOW_COMMIT_V1\n{}\nEND_HARFLOW_COMMIT_V1', "command_refused"),
    ('A quoted example:\n> HARFLOW_COMMIT_V1\n> {}\n> END_HARFLOW_COMMIT_V1\nDiscuss it.', "awaiting_caller"),
    ('Example only:\n```text\nHARFLOW_COMMIT_V1\n{}\nEND_HARFLOW_COMMIT_V1\n```\nDiscussion.', "awaiting_caller"),
])
async def test_role_command_corpus_never_advances_from_prose_or_malformed_frame(origin, final, expected):
    storage, rid = origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage, final)
    run = await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1
    assert not run["decisions"] and not run["steering"]["commits"]
    assert "accepted_decision_id" not in run["pending"][0]
    assert run["steering"]["episodes"][-1]["state"] == expected


@pytest.mark.parametrize("changes", [
    {"status": "failed", "exit_code": 1}, {"exit_code": None},
    {"output_drained": False}, {"outcome_unknown": True}, {"is_error": True},
])
async def test_valid_looking_command_requires_positive_final_capture(origin, changes):
    storage, rid = origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage, lambda: valid_command(storage, rid), **changes)
    run = await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1 and not run["decisions"] and not run["steering"]["commits"]
    assert len(run["steering"]["turns"]) == 1
    assert run["steering"]["turns"][0]["usage"]["reserved_harness_turns"] == 8


async def test_stale_goal_after_spawn_retains_reply_and_usage_without_command_effect(origin):
    storage, rid = origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    final = valid_command(storage, rid)
    registry = EffectRegistry(storage, final)
    registry.before_return = lambda: storage.update_run(rid, lambda r: r.update(goal="Changed observed goal", goal_revision=r.get("goal_revision", 0) + 1), "external_goal_change")
    run = await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1
    assert run["steering"]["turns"][0].get("final") == final, run["steering"]["turns"][0]
    assert run["steering"]["turns"][0]["usage"]["provenance"] == "observed"
    assert not run["decisions"] and not run["steering"]["commits"]
    assert run["steering"]["turns"][0]["outcome"] == "steering_target_stale"


async def test_lost_spawn_ack_observes_same_generation_without_replacement(origin):
    storage, rid = origin
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    registry = EffectRegistry(storage)
    start = registry.start
    async def lost_ack(*args, **kwargs):
        await start(*args, **kwargs)
        raise OSError("Simulated response loss after synthetic task creation")
    registry.start = lost_ack
    run = await run_turn(storage, rid, registry)
    assert run["steering"]["episodes"][-1]["state"] == "delivery_unknown"
    await run_turn(w.WorkflowStore(storage.root), rid, registry)
    await s.steer_workflow(rid, req, root=storage.root)
    run = await run_turn(storage, rid, registry)
    assert [e["task_id"] for e in registry.effects] == [receipt["task_id"]]
    assert run["steering"]["episodes"][-1]["state"] == "awaiting_caller"
    assert len(run["steering"]["turns"]) == 1 and not run["decisions"]


@pytest.mark.parametrize("stop_state,effects", [("spawn_intent", 0), ("running", 1), ("captured", 1), ("settled", 1)])
async def test_power_loss_at_durable_turn_boundaries_never_reissues(origin, monkeypatch, stop_state, effects):
    storage, rid = origin
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    registry = EffectRegistry(storage, lambda: valid_command(storage, rid))
    update = w.WorkflowSupervisor.update
    fired = False
    def interrupt(supervisor, mutator, event, *args, **kwargs):
        nonlocal fired
        result = update(supervisor, mutator, event, *args, **kwargs)
        current = fresh(storage, rid)
        if not fired and current["steering"]["turns"][0]["state"] == stop_state:
            fired = True
            raise PowerLoss(stop_state)
        return result
    monkeypatch.setattr(w.WorkflowSupervisor, "update", interrupt)
    with pytest.raises(PowerLoss):
        await run_turn(storage, rid, registry)
    monkeypatch.setattr(w.WorkflowSupervisor, "update", update)
    await run_turn(w.WorkflowStore(storage.root), rid, registry)
    run = await run_turn(storage, rid, registry)
    assert len(registry.effects) == effects
    assert len(run["steering"]["turns"]) == 1
    assert run["steering"]["turns"][0]["task_id"] == receipt["task_id"]
    if effects:
        assert len(run["decisions"]) == len(run["steering"]["commits"]) == 1
    else:
        assert not run["decisions"] and run["steering"]["episodes"][-1]["hold"]
        assert run["steering"]["episodes"][-1]["state"] == "delivery_unknown"


async def test_unknown_spawn_intent_does_not_release_hold_or_refund(origin):
    storage, rid = origin
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    storage.update_run(rid, lambda r: r["steering"]["turns"][0].update(state="spawn_intent"), "oracle_crash_before_ack")
    registry = EffectRegistry(storage)
    for _ in range(2):
        await run_turn(w.WorkflowStore(storage.root), rid, registry)
        await s.steer_workflow(rid, req, root=storage.root)
    run = fresh(storage, rid)
    assert registry.effects == [] and run["steering"]["episodes"][-1]["hold"]
    assert run["steering"]["turns"][0]["task_id"] == receipt["task_id"]
    assert run["steering"]["turns"][0]["usage"]["reserved_harness_turns"] == 8
    assert len(run["steering"]["turns"]) == 1


async def test_exhausted_episode_never_refunds_malformed_issuances(origin):
    storage, rid = origin
    effects = []
    for index in range(8):
        await s.steer_workflow(rid, request(storage, rid, action="reply" if index else "open"), root=storage.root)
        registry = EffectRegistry(storage, "HARFLOW_COMMIT_V1\n{broken}\nEND_HARFLOW_COMMIT_V1")
        await run_turn(storage, rid, registry)
        effects.extend(registry.effects)
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, request(storage, rid, action="reply"), root=storage.root)
    assert error.value.code == "steering_budget_exhausted"
    assert len(effects) == len(fresh(storage, rid)["steering"]["turns"]) == 8
    assert len({e["task_id"] for e in effects}) == 8
    assert not fresh(storage, rid)["decisions"]


@pytest.mark.parametrize("admission_first", [True, False])
async def test_admission_and_normal_decision_have_one_authoritative_winner(origin, admission_first):
    """The final is held at an async barrier after its real attempt reservation."""
    from harflow_mcp import workflow_delegation as delegation
    storage, rid = origin
    registry = EffectRegistry(storage)
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    dispatched, release = asyncio.Event(), asyncio.Event()
    effects = []
    async def ordinary_final(node, prompt, role, activation):
        effects.append(activation["id"])
        dispatched.set()
        await release.wait()
        return {"status": "completed", "exit_code": 0, "output_drained": True,
                "summary": json.dumps({"decision_id": "checkpoint-decision", "action": "continue",
                                       "reason": "Configured route", "next": [{"continuation_id": "begin", "prompt": "Configured work assignment.", "session_mode": "fresh"}]})}
    supervisor._dispatch = ordinary_final
    run = fresh(storage, rid)
    node = next(n for n in run["definition"]["nodes"] if n["id"] == "start")
    deciding = asyncio.create_task(delegation.decide(supervisor, node, run["pending"][0]))
    try:
        await asyncio.wait_for(dispatched.wait(), 5)
        req = request(storage, rid)
        if admission_first:
            receipt = await s.steer_workflow(rid, req, root=storage.root)
            assert receipt["accepted"]
        release.set()
        await asyncio.wait_for(deciding, 5)
        if not admission_first:
            with pytest.raises(s.SteeringError) as error:
                await s.steer_workflow(rid, req, root=storage.root)
            assert error.value.code in {"steering_target_stale", "steering_boundary_pending"}
        final = fresh(storage, rid)
        assert len(effects) == 1
        assert len(final["decisions"]) == (0 if admission_first else 1)
        assert len(final["steering"]["turns"]) == (1 if admission_first else 0)
        if admission_first:
            assert not final["pending"][0].get("accepted_decision_id")
        else:
            assert final["pending"][0]["accepted_decision_id"] == "checkpoint-decision"
    finally:
        release.set()
        if not deciding.done():
            deciding.cancel()
        await asyncio.gather(deciding, return_exceptions=True)


async def test_committed_route_survives_owner_restart_and_dispatches_worker_once(origin):
    """Crash after the steering commit, before ordinary worker reservation."""
    from test_workflow_delegation import Registry as NormalRegistry
    storage, rid = origin
    req = request(storage, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    conversation = EffectRegistry(storage, lambda: valid_command(storage, rid))
    committed = await run_turn(storage, rid, conversation)
    assert len(committed["decisions"]) == 1
    assert not any(a["role"] == "node" for a in committed["activations"])
    # A fresh owner consumes the durable configured assignment. Its fake registry
    # independently records actual start calls, not merely persisted reservations.
    normal = NormalRegistry(storage.root)
    restarted = w.WorkflowStore(storage.root)
    await asyncio.wait_for(w.WorkflowSupervisor(normal, restarted).execute(rid), 15)
    final = fresh(storage, rid)
    workers = [(prompt, options) for prompt, options in normal.calls if "Assignment:\n" in prompt]
    assert len(workers) == 1
    assert workers[0][1]["display_prompt"] == "Configured work assignment."
    assert all(context["current_stage"]["node_id"] != "start" for context in normal.contexts)
    assert final["status"] == "completed", final.get("attention_reason")
    assert len(final["steering"]["commits"]) == 1
    await s.steer_workflow(rid, req, root=storage.root)
    await run_turn(restarted, rid, conversation)
    assert len(conversation.effects) == 1 and len(workers) == 1


def nested_checkpoint(storage, repo, *, mode="child"):
    """Construct a real pinned reciprocal invocation, without executing it."""
    from harflow_mcp import workflow_invocation as inv, workflow_references as refs
    child_definition = {
        "name": "fault-child", "orchestrator": {"backend": "claude"},
        "nodes": [{"id": "start", "type": "start", "always_route": True},
                  {"id": "work", "type": "agent", "agent": {"backend": "claude"}, "instructions": "Child work only."},
                  {"id": "end", "type": "end"}],
        "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}],
    }
    saved_child = storage.save("fault-child", child_definition)
    parent_definition = w.validate_definition({
        "name": "fault-parent", "routing_mode": "explicit", "orchestrator": {"backend": "claude"},
        "nodes": [{"id": "start", "type": "start"},
                  {"id": "call", "type": "workflow", "instructions": "Child assignment.",
                   "workflow_ref": {"workflow_id": saved_child["workflow_id"]}, "orchestrator_mode": mode},
                  {"id": "end", "type": "end"}],
        "connections": [{"id": "enter", "source": "start", "target": "call"}, {"id": "finish", "source": "call", "target": "end"}],
    })
    parent = storage.create_run(parent_definition, "Root goal", repo, freedom="read_only",
                                dependency_tree=refs.resolve_dependencies(storage, definition=parent_definition))
    rid, cid = parent["workflow_run_id"], uuid.uuid4().hex
    token = {"id": "parent-token", "node_id": "call", "stack": [], "context": {},
             "assignment_prompt": "Child assignment.", "execution_activation_id": "child-invocation"}
    activation = {"id": "child-invocation", "node_id": "call", "role": "node", "status": "running", "created_at": 1.0,
                  "tasks": [], "token": copy.deepcopy(token),
                  "invocation": {"child_workflow_run_id": cid, "stage": "running", "workflow_id": saved_child["workflow_id"],
                                 "workflow_name": "fault-child", "orchestrator_mode": mode, "revision": saved_child["revision"],
                                 "definition_sha256": parent["dependency_tree"]["workflows"][saved_child["workflow_id"]]["definition_sha256"],
                                 "timeout_seconds": None, "timeout_elapsed_seconds": 0}}
    parent = storage.update_run(rid, lambda r: r.update(status="running", pending=[token], activations=[activation], execution_initialized=True), "oracle_parent")
    call = next(n for n in parent_definition["nodes"] if n["id"] == "call")
    child = inv.child_run_record(storage, parent, activation, call, activation["invocation"])
    child.update(status="running", execution_initialized=True, pending=[{
        "id": "checkpoint-token", "node_id": "start", "stack": [], "context": {},
        "decision_id": "checkpoint-decision", "decision_attempts": 0, "on_demand_count": 0, "on_demand_results": [],
    }])
    inv.write_child_run(storage, child)
    return rid, cid


async def test_nested_commit_lost_child_ack_remains_held_and_reconciles_same_decision(origin, tmp_path, monkeypatch):
    storage, _ = origin
    rid, cid = nested_checkpoint(storage, tmp_path)
    view = observation(storage, rid)
    selected = next(t for t in view["targets"] if t["workflow_run_id"] == cid)
    await s.steer_workflow(rid, request(storage, rid, target=selected), root=storage.root)
    registry = EffectRegistry(storage, lambda: valid_command(storage, rid))
    update = w.WorkflowStore.update_run
    fired = False
    def lose_child_ack(store, run_id, mutator, event, *args, **kwargs):
        nonlocal fired
        result = update(store, run_id, mutator, event, *args, **kwargs)
        if not fired and run_id == cid and fresh(storage, cid)["decisions"]:
            fired = True
            raise PowerLoss("Child decision durable; root acknowledgement lost")
        return result
    monkeypatch.setattr(w.WorkflowStore, "update_run", lose_child_ack)
    with pytest.raises(PowerLoss):
        await run_turn(storage, rid, registry)
    root = fresh(storage, rid)
    assert root["steering"]["episodes"][-1]["hold"]
    assert len(fresh(storage, cid)["decisions"]) == 1
    assert not root["steering"]["commits"]
    monkeypatch.setattr(w.WorkflowStore, "update_run", update)
    await run_turn(w.WorkflowStore(storage.root), rid, registry)
    await run_turn(storage, rid, registry)
    root = fresh(storage, rid)
    assert len(registry.effects) == 1
    assert len(fresh(storage, cid)["decisions"]) == len(root["steering"]["commits"]) == 1
    assert root["steering"]["episodes"][-1]["state"] == "committed"


async def test_changed_reciprocal_child_link_cannot_issue_steering(origin, tmp_path):
    storage, _ = origin
    rid, cid = nested_checkpoint(storage, tmp_path)
    view = observation(storage, rid)
    selected = next(t for t in view["targets"] if t["workflow_run_id"] == cid)
    req = request(storage, rid, target=selected)
    storage.update_run(cid, lambda r: r["parent_link"].update(execution_id="foreign-invocation"), "oracle_foreign_link")
    with pytest.raises(s.SteeringError):
        await s.steer_workflow(rid, req, root=storage.root)
    assert not fresh(storage, rid)["steering"]["turns"]
    assert not fresh(storage, cid)["decisions"]


async def test_native_takeover_during_issued_turn_retains_cost_and_refuses_old_commit(origin):
    storage, rid = origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    final = valid_command(storage, rid)
    registry = EffectRegistry(storage, final)
    async def takeover():
        run = fresh(storage, rid)
        owner_request = {"schema_version": 1, "request_id": str(uuid.uuid4()), "action": "take_owner",
                         "expected_interaction_epoch": run["interaction_epoch"],
                         "expected_control_revision": run["steering"]["control_revision"], "reason": "Native adopted conversation."}
        await s.steer_workflow(rid, owner_request, root=storage.root, monitor=True)
    registry.before_return = takeover
    run = await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1 and not run["decisions"]
    assert not run["steering"]["commits"]
    assert run["steering"]["turns"][0].get("final") == final, run["steering"]["turns"][0]
    assert run["steering"]["turns"][0]["usage"]["cost_usd"] == 0.0001
    assert run["steering"]["turns"][0]["outcome"] == "interaction_revoked"


async def test_sibling_result_remains_real_and_stales_issued_command(origin):
    storage, rid = origin
    sibling = {"id": "already-issued-sibling", "node_id": "work", "role": "node", "status": "running", "created_at": 1.0,
               "token": {"id": "sibling-token", "node_id": "work", "stack": [], "context": {}},
               "tasks": [{"task_id": "sibling-task", "status": "running"}]}
    storage.update_run(rid, lambda r: r["activations"].append(sibling), "oracle_existing_sibling")
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    final = valid_command(storage, rid)
    registry = EffectRegistry(storage, final)
    result = {"status": "succeeded", "result": {"summary": "Sibling effect remains observed."}, "evidence": ["independent-sibling-effect"]}
    def sibling_settles():
        def observed(run):
            current = next(a for a in run["activations"] if a["id"] == sibling["id"])
            current.update(status="completed", node_result=result)
            current["tasks"][0].update(status="completed", result={"status": "completed", "exit_code": 0, "output_drained": True})
        storage.update_run(rid, observed, "node_result")
    registry.before_return = sibling_settles
    run = await run_turn(storage, rid, registry)
    retained = next(a for a in run["activations"] if a["id"] == sibling["id"])
    assert retained["node_result"] == result and retained["status"] == "completed"
    assert len(registry.effects) == 1 and not run["decisions"] and not run["steering"]["commits"]
    assert run["steering"]["turns"][0].get("final") == final, run["steering"]["turns"][0]
    assert run["steering"]["turns"][0]["outcome"] == "steering_target_stale"


async def test_admission_after_decision_attempt_before_dispatch_reservation_prevents_spawn(origin):
    from harflow_mcp import workflow_delegation as delegation
    storage, rid = origin
    registry = EffectRegistry(storage)
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    original_dispatch = supervisor._dispatch
    at_claim, release = asyncio.Event(), asyncio.Event()
    async def before_reservation(*args, **kwargs):
        at_claim.set()
        await release.wait()
        return await original_dispatch(*args, **kwargs)
    supervisor._dispatch = before_reservation
    initial = fresh(storage, rid)
    node = next(n for n in initial["definition"]["nodes"] if n["id"] == "start")
    deciding = asyncio.create_task(delegation.decide(supervisor, node, initial["pending"][0]))
    try:
        await asyncio.wait_for(at_claim.wait(), 5)
        reserved = fresh(storage, rid)
        assert len(reserved["activations"]) == 1 and not reserved["activations"][0]["tasks"]
        await s.steer_workflow(rid, request(storage, rid), root=storage.root)
        release.set()
        await asyncio.wait_for(deciding, 5)
        assert registry.effects == []
        assert not fresh(storage, rid)["decisions"]
        assert len(fresh(storage, rid)["steering"]["turns"]) == 1
    finally:
        release.set()
        if not deciding.done():
            deciding.cancel()
        await asyncio.gather(deciding, return_exceptions=True)


async def test_committed_end_completes_without_second_orchestrator_turn(origin):
    from test_workflow_delegation import Registry as NormalRegistry
    storage, rid = origin
    storage.update_run(rid, lambda r: r["pending"][0].update(node_id="end"), "oracle_end_checkpoint")
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    wrapper = json.loads(valid_command(storage, rid).splitlines()[1])
    wrapper["command"] = {"decision_id": "checkpoint-decision", "action": "complete", "reason": "All configured work is settled.", "next": []}
    final = "HARFLOW_COMMIT_V1\n" + json.dumps(wrapper) + "\nEND_HARFLOW_COMMIT_V1"
    conversation = EffectRegistry(storage, final)
    committed = await run_turn(storage, rid, conversation)
    assert committed["status"] == "completed"
    normal = NormalRegistry(storage.root)
    await asyncio.wait_for(w.WorkflowSupervisor(normal, w.WorkflowStore(storage.root)).execute(rid), 10)
    assert normal.calls == []
    assert len(fresh(storage, rid)["decisions"]) == len(fresh(storage, rid)["steering"]["commits"]) == 1
    assert len(conversation.effects) == 1


@pytest.mark.parametrize("drained,expected_spawns", [(True, 1), (False, 0)])
async def test_queued_restart_observes_exact_ordinary_generation_without_replaying_command(origin, drained, expected_spawns):
    storage, rid = origin
    old_tid = "ordinary-generation-task"
    ordinary = {"id": "ordinary-generation-activation", "node_id": "start", "role": "orchestrator", "status": "running", "created_at": 1.0,
                "decision_id": "checkpoint-decision", "decision_goal_revision": 0,
                "token": copy.deepcopy(fresh(storage, rid)["pending"][0]),
                "tasks": [{"task_id": old_tid, "status": "running", "candidate": copy.deepcopy(fresh(storage, rid)["definition"]["orchestrator"])}]}
    def seed(run):
        run["activations"].append(ordinary)
        run["pending"][0]["decision_activation_id"] = ordinary["id"]
    storage.update_run(rid, seed, "oracle_ordinary_issued")
    req = request(storage, rid)
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    assert receipt["state"] == "queued_boundary"
    registry = EffectRegistry(storage)
    legacy_command = json.dumps({"decision_id": "checkpoint-decision", "action": "continue", "reason": "Old response.",
                                 "next": [{"continuation_id": "begin", "prompt": "Must never execute this superseded assignment.", "session_mode": "fresh"}]})
    registry.tasks[old_tid] = ObservedTask(old_tid, legacy_command, output_drained=drained)
    run = await run_turn(w.WorkflowStore(storage.root), rid, registry)
    await run_turn(storage, rid, registry)
    assert len(registry.effects) == expected_spawns
    assert not run["decisions"] and not run["steering"]["commits"]
    assert len(run["steering"]["turns"]) == 1
    if drained:
        observed = next(a for a in run["activations"] if a["id"] == ordinary["id"])
        assert observed["tasks"][0]["result"]["summary"] == legacy_command
        assert run["steering"]["episodes"][-1]["state"] == "awaiting_caller"
    else:
        assert run["steering"]["episodes"][-1]["hold"]
        assert run["steering"]["episodes"][-1]["state"] == "delivery_unknown"


async def test_smaller_configured_harness_cap_is_preserved(origin):
    storage, rid = origin
    storage.update_run(rid, lambda r: r["definition"]["orchestrator"].update(max_turns=2), "oracle_cap")
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage)
    run = await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1
    assert registry.effects[0]["options"]["max_turns"] == 2
    assert run["steering"]["turns"][0]["usage"]["reserved_harness_turns"] == 2


@pytest.mark.parametrize("ack_mode", ["exact", "missing", "wrong"])
async def test_optimized_context_requires_exact_issued_acknowledgement(origin, ack_mode):
    storage, rid = origin
    storage.update_run(rid, lambda r: r["definition"].update(context_delivery="optimized_v1"), "oracle_optimized")
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    def final():
        wrapper = json.loads(valid_command(storage, rid).splitlines()[1])
        assert "context_ack" in wrapper["command"]
        if ack_mode == "missing":
            wrapper["command"].pop("context_ack")
        elif ack_mode == "wrong":
            wrapper["command"]["context_ack"]["digest"] = "0" * 64
        return "HARFLOW_COMMIT_V1\n" + json.dumps(wrapper) + "\nEND_HARFLOW_COMMIT_V1"
    registry = EffectRegistry(storage, final)
    run = await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1
    assert len(run["decisions"]) == len(run["steering"]["commits"]) == (1 if ack_mode == "exact" else 0)
    assert run["steering"]["episodes"][-1]["state"] == ("committed" if ack_mode == "exact" else "command_refused")
    assert run["steering"]["turns"][0]["context_delivery"]["delivery_mode"] == "bootstrap"


class MainRegistry(EffectRegistry):
    """Both control conversation and ordinary fake providers under actual _main."""
    def __init__(self, storage, rid, *, unknown=False):
        from test_workflow_delegation import Registry as NormalRegistry
        super().__init__(storage, **({"output_drained": False} if unknown else {}))
        self.rid = rid
        self.normal = NormalRegistry(storage.root)
        self.final = lambda: "Discuss before deciding." if len(self.effects) == 1 else valid_command(storage, rid)

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
        await w._main(SimpleNamespace(root=str(storage.root), run_id=rid))


async def test_actual_main_paused_reply_commits_and_dispatches_exact_configured_work(origin, monkeypatch):
    from harflow_mcp import tasks
    storage, rid = origin
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(storage.root))
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = MainRegistry(storage, rid)
    monkeypatch.setattr(tasks, "TaskRegistry", lambda **kwargs: registry)
    launches = []
    monkeypatch.setattr(w, "_launch", lambda store, run_id: launches.append(run_id))
    await asyncio.wait_for(owned_main(storage, rid), 15)
    waiting = fresh(storage, rid)
    assert waiting["status"] == "paused" and waiting["steering"]["episodes"][-1]["state"] == "awaiting_caller"
    assert len(registry.effects) == 1 and not registry.normal.calls and launches == []
    receipt = await s.steer_workflow(rid, request(storage, rid, action="reply"), root=storage.root)
    assert launches == [rid]
    launches.clear()
    await asyncio.wait_for(owned_main(storage, rid), 15)
    assert launches == []
    final = fresh(storage, rid)
    workers = [k for p, k in registry.normal.calls if "Assignment:\n" in p]
    assert [e["task_id"] for e in registry.effects] == [t["task_id"] for t in final["steering"]["turns"]]
    assert len(registry.effects) == 2 and registry.effects[-1]["task_id"] == receipt["task_id"]
    assert len(workers) == 1 and workers[0]["display_prompt"] == "Configured work assignment."
    assert final["status"] == "completed", final.get("attention_reason")
    assert len(final["steering"]["commits"]) == 1
    assert all(c["current_stage"]["node_id"] != "start" for c in registry.normal.contexts)


async def test_actual_main_fast_reply_after_classification_is_handed_off_after_unlock(origin, monkeypatch):
    import fcntl
    from harflow_mcp import tasks, orchestrator_steering
    storage, rid = origin
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(storage.root))
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = MainRegistry(storage, rid)
    monkeypatch.setattr(tasks, "TaskRegistry", lambda **kwargs: registry)
    original = orchestrator_steering.run_turn
    children, lock_observations, receipts = [], [], []
    first = True
    def launch(store, run_id):
        with (store.runs / f"{run_id}.supervisor.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                lock_observations.append("busy")
            else:
                lock_observations.append("free")
        children.append(asyncio.create_task(owned_main(store, run_id)))
    async def classify_then_reply(supervisor, run_id):
        nonlocal first
        await original(supervisor, run_id)
        if first:
            first = False
            receipts.append(await s.steer_workflow(rid, request(storage, rid, action="reply"), root=storage.root))
            await asyncio.wait_for(children[0], 5)
            assert len(registry.effects) == 1
    monkeypatch.setattr(w, "_launch", launch)
    monkeypatch.setattr(orchestrator_steering, "run_turn", classify_then_reply)
    await asyncio.wait_for(owned_main(storage, rid), 15)
    await asyncio.wait_for(asyncio.gather(*children), 15)
    run = fresh(storage, rid)
    assert lock_observations == ["busy", "free"]
    assert len(children) == 2 and len(registry.effects) == 2
    assert registry.effects[-1]["task_id"] == receipts[0]["task_id"]
    assert run["status"] == "completed" and len(run["steering"]["commits"]) == 1
    assert len([1 for p, k in registry.normal.calls if "Assignment:\n" in p]) == 1


async def test_actual_main_unknown_generation_never_respawns_owner_loop(origin, monkeypatch):
    from harflow_mcp import tasks
    storage, rid = origin
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(storage.root))
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = MainRegistry(storage, rid, unknown=True)
    monkeypatch.setattr(tasks, "TaskRegistry", lambda **kwargs: registry)
    launches = []
    monkeypatch.setattr(w, "_launch", lambda store, run_id: launches.append(run_id))
    await asyncio.wait_for(owned_main(storage, rid), 15)
    await asyncio.wait_for(owned_main(storage, rid), 15)
    assert launches == [] and len(registry.effects) == 1 and not registry.normal.calls
    assert fresh(storage, rid)["steering"]["episodes"][-1]["state"] == "delivery_unknown"


def action_frame(storage, rid, command):
    wrapper = json.loads(valid_command(storage, rid).splitlines()[1])
    ack = wrapper["command"].get("context_ack")
    wrapper["command"] = copy.deepcopy(command)
    if ack is not None:
        wrapper["command"]["context_ack"] = ack
    return "HARFLOW_COMMIT_V1\n" + json.dumps(wrapper) + "\nEND_HARFLOW_COMMIT_V1"


async def test_steered_inspection_does_not_refund_ordinary_decision_budget(origin):
    from test_workflow_delegation import Registry as NormalRegistry
    storage, rid = origin
    storage.update_run(rid, lambda r: r["pending"][0].update(decision_attempts=2), "oracle_prior_decision_charges")
    req = request(storage, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    command = {"decision_id": "checkpoint-decision", "action": "inspect", "reason": "Read configured instruction.",
               "requests": [{"node_id": "work", "view": "instructions"}]}
    registry = EffectRegistry(storage, lambda: action_frame(storage, rid, command))
    run = await run_turn(storage, rid, registry)
    assert run["pending"][0]["decision_attempts"] == 2
    assert run["pending"][0]["inspection_count"] == 1
    assert len(run["pending"][0]["inspection_results"]) == 1
    assert len(run["steering"]["commits"]) == len(registry.effects) == 1
    await s.steer_workflow(rid, req, root=storage.root)
    await run_turn(storage, rid, registry)
    assert fresh(storage, rid)["pending"][0]["inspection_count"] == 1
    normal = NormalRegistry(storage.root)
    await asyncio.wait_for(w.WorkflowSupervisor(normal, w.WorkflowStore(storage.root)).execute(rid), 15)
    completed = fresh(storage, rid)
    assert completed["status"] == "completed", completed.get("attention_reason")
    assert len([1 for prompt, _ in normal.calls if "Assignment:\n" in prompt]) == 1
    assert sum(a.get("decision_disposition") == "inspect" for a in completed["activations"]) == 1


def configure_specialist(storage, rid):
    def change(run):
        run["definition"] = w.validate_definition({**run["definition"], "on_demand_nodes": [{
            "id": "specialist", "title": "specialist", "instructions": "Return bounded specialist evidence.",
            "agent": {"backend": "claude"}, "freedom": "read_only", "session_mode": "fresh", "execution_mode": "headless",
        }]})
        run["pending"][0]["decision_attempts"] = 2
    storage.update_run(rid, change, "oracle_configured_specialist")


async def test_steered_specialist_is_one_effect_and_does_not_refund_normal_decision_budget(origin):
    storage, rid = origin
    configure_specialist(storage, rid)
    req = request(storage, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    command = {"decision_id": "checkpoint-decision", "action": "call_on_demand", "node_id": "specialist",
               "assignment": "Inspect configured evidence once.", "reason": "Need bounded specialist evidence.", "session_mode": "fresh"}
    registry = MainRegistry(storage, rid)
    registry.final = lambda: action_frame(storage, rid, command)
    run = await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1
    assert len(registry.normal.calls) == 1
    assert run["pending"][0]["decision_attempts"] == 2
    assert run["pending"][0]["on_demand_count"] == 1
    assert len(run["on_demand_calls"]) == len(run["steering"]["commits"]) == 1
    await s.steer_workflow(rid, req, root=storage.root)
    await run_turn(w.WorkflowStore(storage.root), rid, registry)
    assert len(registry.effects) == len(registry.normal.calls) == 1
    assert len(fresh(storage, rid)["on_demand_calls"]) == 1


async def test_unknown_specialist_effect_keeps_exact_reservation_and_never_dispatches_twice(origin, monkeypatch):
    storage, rid = origin
    configure_specialist(storage, rid)
    req = request(storage, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    command = {"decision_id": "checkpoint-decision", "action": "call_on_demand", "node_id": "specialist",
               "assignment": "Observe the configured specialist once.", "reason": "Need evidence.", "session_mode": "fresh"}
    registry = EffectRegistry(storage, lambda: action_frame(storage, rid, command))
    effects = []
    async def unknown_effect(supervisor, node, prompt, role, activation):
        assert role == "node" and "specialist" in node["id"]
        effects.append({"activation_id": activation["id"], "prompt": prompt})
        return {"status": "failed", "outcome_unknown": True, "summary": "Provider delivery was not observed."}
    monkeypatch.setattr(w.WorkflowSupervisor, "_dispatch", unknown_effect)
    await run_turn(storage, rid, registry)
    first = fresh(storage, rid)
    reservation = copy.deepcopy(first["on_demand_calls"][0])
    assert reservation["status"] == "uncertain"
    for _ in range(2):
        await s.steer_workflow(rid, req, root=storage.root)
        await run_turn(w.WorkflowStore(storage.root), rid, registry)
    final = fresh(storage, rid)
    assert len(effects) == len(registry.effects) == 1
    assert len(final["on_demand_calls"]) == len(final["steering"]["turns"]) == 1
    assert final["on_demand_calls"][0]["id"] == reservation["id"]
    assert final["on_demand_calls"][0]["execution_id"] == effects[0]["activation_id"]
    assert final["on_demand_calls"][0]["status"] == "uncertain"
    assert final["steering"]["turns"][0]["usage"]["reserved_harness_turns"] == 8


async def test_actual_main_nested_hold_converses_then_child_and_parent_complete_once(origin, tmp_path, monkeypatch):
    """The issued parent invocation must park and reenter its same held child."""
    from harflow_mcp import tasks
    storage, _ = origin
    rid, cid = nested_checkpoint(storage, tmp_path)
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(storage.root))
    selected = next(t for t in observation(storage, rid)["targets"] if t["workflow_run_id"] == cid)
    await s.steer_workflow(rid, request(storage, rid, target=selected), root=storage.root)
    registry = MainRegistry(storage, rid)
    from test_workflow_delegation import default_decision
    registry.normal.policy = lambda context, current: default_decision(
        {**context, "valid_continuations": [c for c in context["valid_continuations"] if c["kind"] != "retry_execution"]}, current)
    monkeypatch.setattr(tasks, "TaskRegistry", lambda **kwargs: registry)
    launches = []
    monkeypatch.setattr(w, "_launch", lambda store, run_id: launches.append(run_id))
    await asyncio.wait_for(owned_main(storage, rid), 15)
    waiting, child = fresh(storage, rid), fresh(storage, cid)
    assert waiting["status"] in {"paused", "running"}, waiting.get("attention_reason")
    assert child["status"] in {"paused", "running"}, child.get("attention_reason")
    assert waiting["steering"]["episodes"][-1]["state"] == "awaiting_caller"
    assert waiting["steering"]["episodes"][-1]["hold"]
    assert len(registry.effects) == 1 and not registry.normal.calls and launches == []
    assert waiting["activations"][0]["invocation"]["child_workflow_run_id"] == cid
    selected = next(t for t in observation(storage, rid)["targets"] if t["workflow_run_id"] == cid)
    receipt = await s.steer_workflow(rid, request(storage, rid, action="reply", target=selected), root=storage.root)
    assert launches == [rid]
    launches.clear()
    await asyncio.wait_for(owned_main(storage, rid), 15)
    root, child = fresh(storage, rid), fresh(storage, cid)
    assert root["status"] == "completed", root.get("attention_reason") or root.get("failure_reason")
    assert child["status"] == "completed", child.get("attention_reason") or child.get("failure_reason")
    assert launches == []
    assert len(registry.effects) == 2 and registry.effects[-1]["task_id"] == receipt["task_id"]
    workers = [(prompt, options) for prompt, options in registry.normal.calls if "Assignment:\n" in prompt]
    assert len(workers) == 1 and workers[0][1]["display_prompt"] == "Configured work assignment."
    assert len(root["steering"]["commits"]) == 1
    assert root["activations"][0]["invocation"]["child_workflow_run_id"] == cid
    assert len([a for a in root["activations"] if a.get("invocation")]) == 1


def held_request(storage, rid, action):
    """An exact current target may remain readable after a real terminal event."""
    from harflow_mcp import orchestrator_steering
    run = fresh(storage, rid)
    ep = run["steering"]["episodes"][-1]
    old = ep["target"]
    req = {"schema_version": 1, "request_id": str(uuid.uuid4()), "action": action,
           "expected_interaction_epoch": run["interaction_epoch"], "expected_control_revision": run["steering"]["control_revision"],
           "episode_id": ep["episode_id"], "expected_steering_revision": ep["steering_revision"],
           "target": orchestrator_steering.target_for(storage, run, old["workflow_run_id"], old["token_id"])}
    return req


def real_status(storage, rid, status):
    def observed(run):
        run.update(status=status, attention_reason="Real external checkpoint; preserve this reason.")
        if status == "failed":
            run["failure_reason"] = "Observed workflow failure."
        if status == "needs_input":
            run.update(input_question="A real unresolved question?", input_decision_id="real-question-decision")
    storage.update_run(rid, observed, "oracle_real_control")


@pytest.mark.parametrize("phase", ["settled", "reserved", "running"])
@pytest.mark.parametrize("status", ["cancelled", "cancelling", "failed", "completed", "needs_input", "needs_attention"])
async def test_dismiss_preserves_real_terminal_question_and_attention_checkpoints(origin, phase, status):
    storage, rid = origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage)
    async def control_then_dismiss():
        real_status(storage, rid, status)
        await s.steer_workflow(rid, held_request(storage, rid, "dismiss"), root=storage.root)
    if phase == "settled":
        await run_turn(storage, rid, registry)
        await control_then_dismiss()
    elif phase == "reserved":
        await control_then_dismiss()
        await run_turn(storage, rid, registry)
    else:
        registry.before_return = control_then_dismiss
        await run_turn(storage, rid, registry)
    final = fresh(storage, rid)
    assert final["status"] == status
    assert final["attention_reason"] == "Real external checkpoint; preserve this reason."
    if status == "needs_input":
        assert final["input_question"] == "A real unresolved question?"
        assert final["input_decision_id"] == "real-question-decision"
    if status == "failed":
        assert final["failure_reason"] == "Observed workflow failure."
    assert not final["decisions"] and not final["steering"]["commits"]
    assert len(registry.effects) == (0 if phase == "reserved" else 1)


@pytest.mark.parametrize("old_record", ["completed_orchestrator", "completed_historical_worker"])
async def test_accepted_sibling_assignment_requires_current_execution_reservation(origin, old_record):
    storage, rid = origin
    def seed(run):
        sibling = {"id": "accepted-sibling-token", "node_id": "work", "stack": [], "context": {},
                   "decision_id": "accepted-sibling-decision", "accepted_decision_id": "accepted-sibling-decision",
                   "assignment_prompt": "Already accepted sibling assignment.", "execution_activation_id": "new-generation-not-reserved"}
        run["pending"].append(sibling)
        run["activations"].append({"id": "old-sibling-record", "node_id": "work", "role": "orchestrator" if old_record == "completed_orchestrator" else "node",
                                   "status": "completed", "created_at": 1.0, "token": copy.deepcopy(sibling),
                                   "tasks": [{"task_id": "old-sibling-task", "status": "completed"}]})
    storage.update_run(rid, seed, "oracle_accepted_sibling_before_worker_reservation")
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    assert error.value.code == "steering_boundary_pending"
    assert not fresh(storage, rid)["steering"]["turns"]


@pytest.mark.parametrize("change", ["goal", "sibling_result", "owner"])
async def test_prose_on_changed_target_is_stale_and_cannot_slide_to_fresh_reply(origin, change):
    storage, rid = origin
    original_text = "Discuss this original target; preserve all text."
    await s.steer_workflow(rid, request(storage, rid, text=original_text), root=storage.root)
    registry = EffectRegistry(storage, "Historical discussion of the original target.")
    async def changed():
        if change == "goal":
            storage.update_run(rid, lambda r: r.update(goal="New observed goal", goal_revision=r.get("goal_revision", 0) + 1), "oracle_goal_changed")
        elif change == "sibling_result":
            def completed(run):
                run["activations"].append({"id": "observed-sibling", "node_id": "work", "role": "node", "status": "completed", "created_at": 1.0,
                                           "tasks": [], "node_result": {"status": "succeeded", "result": {"summary": "New real evidence"}, "evidence": ["sibling-effect"]}})
            storage.update_run(rid, completed, "oracle_sibling_observed")
        else:
            run = fresh(storage, rid)
            await s.steer_workflow(rid, {"schema_version": 1, "request_id": str(uuid.uuid4()), "action": "take_owner",
                                        "expected_interaction_epoch": run["interaction_epoch"], "expected_control_revision": run["steering"]["control_revision"], "reason": "Native takeover."}, root=storage.root, monitor=True)
    registry.before_return = changed
    await run_turn(storage, rid, registry)
    run = fresh(storage, rid)
    ep, turn = run["steering"]["episodes"][-1], run["steering"]["turns"][-1]
    assert ep["state"] == "stale"
    assert ep["hold"] and turn["text"] == original_text and turn["final"] == registry.final
    assert turn["usage"]["cost_usd"] == 0.0001
    reply = {**held_request(storage, rid, "reply"), "text": "Continue against the new target.", "allow_fresh_context": True}
    with pytest.raises(s.SteeringError):
        await s.steer_workflow(rid, reply, root=storage.root, monitor=change == "owner")
    assert len(fresh(storage, rid)["steering"]["turns"]) == len(registry.effects) == 1


async def test_reply_after_external_goal_change_requires_explicit_retarget(origin):
    storage, rid = origin
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage)
    await run_turn(storage, rid, registry)
    before = fresh(storage, rid)["steering"]["episodes"][-1]["target"]
    storage.update_run(rid, lambda r: r.update(goal="Fresh goal", goal_revision=r.get("goal_revision", 0) + 1), "oracle_goal_changed_after_reply")
    req = request(storage, rid, action="reply")
    assert req["target"] != before
    with pytest.raises(s.SteeringError):
        await s.steer_workflow(rid, req, root=storage.root)
    assert len(fresh(storage, rid)["steering"]["turns"]) == 1


@pytest.mark.parametrize("contradiction", [
    "child_definition", "child_hash", "child_revision", "child_workflow", "child_mode",
    "invocation_workflow", "invocation_revision", "invocation_hash", "invocation_mode",
    "pinned_definition", "configured_workflow", "child_owner", "child_configuration", "child_subtree",
])
async def test_shape_valid_contradictory_nested_source_refuses_before_advertising_or_admitting(origin, tmp_path, contradiction):
    storage, _ = origin
    rid, cid = nested_checkpoint(storage, tmp_path)
    selected = next(t for t in observation(storage, rid)["targets"] if t["workflow_run_id"] == cid)
    req = request(storage, rid, target=selected)
    child_changes = {"child_definition", "child_hash", "child_revision", "child_workflow", "child_mode", "child_owner", "child_configuration", "child_subtree"}
    if contradiction in child_changes:
        def mutate(child):
            if contradiction == "child_definition":
                child["definition"]["nodes"][1]["instructions"] = "Contradictory child source."
            elif contradiction == "child_hash":
                child["definition_hash"] = "0" * 64
            elif contradiction == "child_revision":
                child["revision"] += 1
            elif contradiction == "child_workflow":
                child["workflow_id"] = uuid.uuid4().hex
            elif contradiction == "child_mode":
                child["orchestrator_mode"] = "current"
            elif contradiction == "child_owner":
                child["orchestrator_session_owner_run_id"] = rid
            elif contradiction == "child_configuration":
                child["orchestrator_config"] = {**child["definition"]["orchestrator"], "model": "contradictory-model"}
            else:
                child["dependency_tree"]["root_workflow_id"] = uuid.uuid4().hex
        storage.update_run(cid, mutate, "oracle_contradictory_child_source")
    else:
        def mutate(parent):
            invocation = parent["activations"][0]["invocation"]
            if contradiction == "invocation_workflow":
                invocation["workflow_id"] = uuid.uuid4().hex
            elif contradiction == "invocation_revision":
                invocation["revision"] += 1
            elif contradiction == "invocation_hash":
                invocation["definition_sha256"] = "0" * 64
            elif contradiction == "invocation_mode":
                invocation["orchestrator_mode"] = "current"
            elif contradiction == "pinned_definition":
                parent["dependency_tree"]["workflows"][invocation["workflow_id"]]["definition"]["nodes"][1]["instructions"] = "Contradictory pinned source."
            else:
                next(n for n in parent["definition"]["nodes"] if n["id"] == "call")["workflow_ref"]["workflow_id"] = uuid.uuid4().hex
        storage.update_run(rid, mutate, "oracle_contradictory_parent_source")
    # Both documents remain shape-qualified; source authority is the missing proof.
    fresh(storage, rid)
    fresh(storage, cid)
    with pytest.raises(s.SteeringError) as read_error:
        observation(storage, rid)
    assert read_error.value.code == "steering_source_unavailable"
    with pytest.raises(s.SteeringError) as write_error:
        await s.steer_workflow(rid, req, root=storage.root)
    assert write_error.value.code == "steering_source_unavailable"
    assert not fresh(storage, rid)["steering"]["turns"]
    assert not fresh(storage, cid)["decisions"]


@pytest.mark.parametrize("mode", ["child", "current"])
async def test_exact_real_nested_owner_source_remains_eligible(origin, tmp_path, mode):
    storage, _ = origin
    rid, cid = nested_checkpoint(storage, tmp_path, mode=mode)
    selected = next(t for t in observation(storage, rid)["targets"] if t["workflow_run_id"] == cid)
    receipt = await s.steer_workflow(rid, request(storage, rid, target=selected), root=storage.root)
    registry = EffectRegistry(storage)
    run = await run_turn(storage, rid, registry)
    assert len(registry.effects) == 1 and registry.effects[0]["task_id"] == receipt["task_id"]
    assert run["steering"]["episodes"][-1]["state"] == "awaiting_caller"
    assert run["steering"]["turns"][-1]["session_owner_run_id"] == (rid if mode == "current" else cid)
    assert not run["decisions"] and not fresh(storage, cid)["decisions"]


async def test_nested_child_cannot_borrow_historical_sibling_session_owner(origin, tmp_path):
    from harflow_mcp import workflow_invocation as inv
    storage, _ = origin
    rid, old_child_id = nested_checkpoint(storage, tmp_path)
    storage.update_run(old_child_id, lambda r: r.update(status="completed", pending=[]), "oracle_prior_invocation_finished")
    next_child_id = uuid.uuid4().hex
    def next_invocation(parent):
        old = parent["activations"][0]
        old.update(status="completed")
        old["invocation"]["stage"] = "settled"
        current = copy.deepcopy(old)
        current.update(id="next-child-invocation", status="running")
        current["invocation"].update(stage="running", child_workflow_run_id=next_child_id)
        current["token"]["execution_activation_id"] = current["id"]
        parent["pending"][0]["execution_activation_id"] = current["id"]
        parent["activations"].append(current)
    parent = storage.update_run(rid, next_invocation, "oracle_next_invocation_generation")
    activation = parent["activations"][-1]
    node = next(n for n in parent["definition"]["nodes"] if n["id"] == "call")
    child = inv.child_run_record(storage, parent, activation, node, activation["invocation"])
    child.update(status="running", pending=[{"id": "next-child-token", "node_id": "start", "stack": [], "context": {}, "decision_id": "next-child-decision"}])
    inv.write_child_run(storage, child)
    selected = next(t for t in observation(storage, rid)["targets"] if t["workflow_run_id"] == next_child_id)
    req = request(storage, rid, target=selected)
    storage.update_run(next_child_id, lambda r: r.update(orchestrator_session_owner_run_id=old_child_id), "oracle_sibling_owner_contradiction")
    with pytest.raises(s.SteeringError) as error:
        observation(storage, rid)
    assert error.value.code == "steering_source_unavailable"
    with pytest.raises(s.SteeringError):
        await s.steer_workflow(rid, req, root=storage.root)
    assert not fresh(storage, rid)["steering"]["turns"]


async def test_dismiss_keeps_issued_active_sibling_real_and_does_not_pause_or_cancel_it(origin):
    storage, rid = origin
    sibling = {"id": "live-sibling", "node_id": "work", "role": "node", "status": "running", "created_at": 1.0,
               "token": {"id": "live-sibling-token", "node_id": "work"},
               "tasks": [{"task_id": "live-sibling-task", "status": "running"}]}
    storage.update_run(rid, lambda r: r["activations"].append(sibling), "oracle_issued_live_sibling")
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage)
    await run_turn(storage, rid, registry)
    assert fresh(storage, rid)["status"] == "running"
    await s.steer_workflow(rid, held_request(storage, rid, "dismiss"), root=storage.root)
    final = fresh(storage, rid)
    retained = next(a for a in final["activations"] if a["id"] == sibling["id"])
    assert final["status"] == "running"
    assert retained == sibling
    assert len(registry.effects) == 1 and not final["decisions"]
    assert final["steering"]["episodes"][-1]["state"] == "dismiss_pending_settlement"
    assert final["steering"]["episodes"][-1]["hold"]


@pytest.mark.parametrize("phase", ["reserved", "running_before_capture"])
async def test_dismissal_before_turn_settlement_holds_until_exact_active_sibling_settles(origin, monkeypatch, phase):
    """Dismissing a conversational task cannot reopen sibling scheduling."""
    from harflow_mcp import orchestrator_steering
    storage, rid = origin
    sibling = {"id": "issued-sibling-execution", "node_id": "work", "role": "node", "status": "running", "created_at": 1.0,
               "token": {"id": "issued-sibling-token", "node_id": "work"},
               "tasks": [{"task_id": "issued-sibling-generation", "status": "running"}]}
    storage.update_run(rid, lambda r: r["activations"].append(sibling), "oracle_issued_sibling")
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = EffectRegistry(storage, "The dismissed turn was observed and retained.")
    launches = []
    monkeypatch.setattr(w, "_launch", lambda store, run_id: launches.append(run_id))
    if phase == "reserved":
        await s.steer_workflow(rid, held_request(storage, rid, "dismiss"), root=storage.root)
        await run_turn(storage, rid, registry)
    else:
        running = asyncio.Event()
        update = w.WorkflowSupervisor.update
        def signal_running(supervisor, mutator, event, *args, **kwargs):
            result = update(supervisor, mutator, event, *args, **kwargs)
            if fresh(storage, rid)["steering"]["turns"][-1]["state"] == "running":
                running.set()
            return result
        monkeypatch.setattr(w.WorkflowSupervisor, "update", signal_running)
        start = registry.start
        async def unsettled_provider(*args, **kwargs):
            task = await start(*args, **kwargs)
            task.done.clear()
            return task
        registry.start = unsettled_provider
        turn_owner = asyncio.create_task(run_turn(storage, rid, registry))
        try:
            await asyncio.wait_for(running.wait(), 5)
            assert fresh(storage, rid)["steering"]["turns"][-1]["state"] == "running"
            await s.steer_workflow(rid, held_request(storage, rid, "dismiss"), root=storage.root)
            current = fresh(storage, rid)
            assert current["steering"]["episodes"][-1]["state"] == "dismiss_pending_settlement"
            assert current["steering"]["episodes"][-1]["hold"] and current["status"] == "running"
            registry.tasks[current["steering"]["turns"][-1]["task_id"]].done.set()
            await asyncio.wait_for(turn_owner, 10)
        finally:
            for task in registry.tasks.values():
                task.done.set()
            if not turn_owner.done():
                turn_owner.cancel()
            await asyncio.gather(turn_owner, return_exceptions=True)
    run = fresh(storage, rid)
    assert run["steering"]["turns"][-1]["state"] == "settled"
    assert run["steering"]["episodes"][-1]["state"] == "dismiss_pending_settlement"
    assert run["steering"]["episodes"][-1]["hold"] and run["status"] == "running"
    assert next(a for a in run["activations"] if a["id"] == sibling["id"]) == sibling
    assert len(registry.effects) == (0 if phase == "reserved" else 1)
    assert launches == [] and not run["decisions"]
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    orchestrator_steering.settle_dismissals(supervisor)
    assert fresh(storage, rid)["steering"]["episodes"][-1]["hold"]
    def observed_sibling(run):
        activation = next(a for a in run["activations"] if a["id"] == sibling["id"])
        activation.update(status="completed", node_result={"status": "succeeded", "result": {"summary": "Issued sibling settled."}, "evidence": ["observed-exact-generation"]})
        activation["tasks"][0].update(status="completed", result={"task_id": "issued-sibling-generation", "status": "completed", "exit_code": 0, "output_drained": True, "summary": "Issued sibling settled."})
    storage.update_run(rid, observed_sibling, "oracle_sibling_capture_observed")
    orchestrator_steering.settle_dismissals(supervisor)
    final = fresh(storage, rid)
    assert final["status"] == "paused"
    assert final["steering"]["episodes"][-1]["state"] == "dismissed"
    assert not final["steering"]["episodes"][-1]["hold"]
    assert launches == [] and not final["decisions"]
    assert len(registry.effects) == (0 if phase == "reserved" else 1)
