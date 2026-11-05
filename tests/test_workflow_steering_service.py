"""Deterministic one-request effect oracles with no provider or live state."""
import asyncio
import copy
import json
import uuid
from types import SimpleNamespace

import pytest

from harflow_mcp import identity, lineage, workflow_interactions, workflows as w
from harflow_mcp import workflow_steering as s
from harflow_mcp.state_layout import resolve_layout


@pytest.fixture
def setup(tmp_path, monkeypatch):
    principal = {"kind": "external_parent", "identity": {"pid": 12345, "start_time": "generation-one", "markers": []}}
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: SimpleNamespace(caller=None, undecidable=None))
    monkeypatch.setattr(workflow_interactions, "capture_transport_parent", lambda: copy.deepcopy(principal))
    monkeypatch.setattr(workflow_interactions, "_transport_caller", principal)
    monkeypatch.setattr(identity, "check_detail", lambda *a, **k: ("alive", "matched"))
    monkeypatch.setattr(w, "_launch", lambda *a: None)
    monkeypatch.setattr(w.backends, "is_installed", lambda *a: True)
    storage = w.WorkflowStore(tmp_path / "state")
    descriptor = {"name": "example", "orchestrator": {"backend": "claude"}, "nodes": [], "connections": []}
    run = storage.create_run(descriptor, "Create", tmp_path, kind="builder", freedom="read_only")
    rid = run["workflow_run_id"]
    storage.update_run(rid, lambda r: r.update(status="completed", builder_draft={"name": "example", "nodes": [], "connections": []}, draft_revision=0), "test_setup")
    return storage, rid, principal


def request(storage, rid, *, action="open", ep=None, text="Please discuss this design.", **extra):
    run = storage.get_run(rid)
    result = {"schema_version": 1, "request_id": str(uuid.uuid4()), "action": action,
              "expected_interaction_epoch": run["interaction_epoch"], "expected_control_revision": run["steering"]["control_revision"],
              "expected_steering_revision": ep["steering_revision"] if ep else None, "episode_id": ep["episode_id"] if ep else None,
              "target": s.target(run), "allow_fresh_context": True}
    if action != "dismiss":
        result["text"] = text
    result.update(extra)
    return result


class Task:
    def __init__(self, task_id, final, **changes):
        self.task_id = task_id
        self.done = asyncio.Event(); self.done.set()
        self.value = {"task_id": task_id, "status": "completed", "exit_code": 0, "output_drained": True,
                      "summary": final, "session_id": "test-session", **changes}
    def snapshot(self):
        return self.value


class Registry:
    def __init__(self, root, final="Let's discuss the design.", **changes):
        self.log_dir = self._log_dir = root / "tasks"
        self.state_layout = resolve_layout(log_dir=self.log_dir)
        self.final = final
        self.changes = changes
        self.calls = []
        self.task = None
    async def start(self, prompt, path, **kwargs):
        self.calls.append((prompt, path, kwargs))
        self.task = Task(kwargs["task_id"], self.final, **self.changes)
        return self.task
    def attach(self, task_id):
        return self.task


async def run_turn(storage, rid, registry):
    sup = w.WorkflowSupervisor(registry, storage)
    await s.run_builder_turn(sup, rid)
    return storage.get_run(rid)


async def test_one_request_prose_waits_and_replay_has_no_second_issuance(setup):
    storage, rid, _ = setup
    req = request(storage, rid, text="Words stay verbatim.\n  With spaces.")
    receipt = await s.steer_workflow(rid, req, root=storage.root)
    registry = Registry(storage.root)
    run = await run_turn(storage, rid, registry)
    assert len(registry.calls) == 1
    assert run["status"] == "paused"
    assert run["steering"]["episodes"][0]["state"] == "awaiting_caller"
    assert "generated_definition" not in run
    assert storage.list() == []
    assert registry.calls[0][2]["max_turns"] == 8
    assert registry.calls[0][2]["freedom"] == "read_only"
    assert req["text"] in registry.calls[0][0]
    replay = await s.steer_workflow(rid, req, root=storage.root)
    assert replay["request_id"] == receipt["request_id"] and replay["replayed"]
    assert len(storage.get_run(rid)["steering"]["turns"]) == 1
    await run_turn(storage, rid, registry)
    assert len(registry.calls) == 1


async def test_request_identity_conflict_precedes_stale_target_and_expiry(setup, monkeypatch):
    storage, rid, _ = setup
    req = request(storage, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    monkeypatch.setattr(s.time, "time", lambda: 10**12)
    assert (await s.steer_workflow(rid, req, root=storage.root))["replayed"]
    changed = {**req, "text": "A different request"}
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, changed, root=storage.root)
    assert error.value.code == "request_payload_conflict"


async def test_concurrent_admission_one_winner(setup):
    storage, rid, _ = setup
    first, second = request(storage, rid), request(storage, rid)
    values = await asyncio.gather(s.steer_workflow(rid, first, root=storage.root), s.steer_workflow(rid, second, root=storage.root), return_exceptions=True)
    assert sum(isinstance(v, dict) for v in values) == 1
    assert [v.code for v in values if isinstance(v, s.SteeringError)] == ["steering_revision_stale"]
    assert len(storage.get_run(rid)["steering"]["turns"]) == 1


async def test_managed_denied_before_target_lookup(setup, monkeypatch):
    storage, rid, _ = setup
    req = request(storage, rid)
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: SimpleNamespace(caller=object(), undecidable=None))
    monkeypatch.setattr(w.WorkflowStore, "get_run", lambda *a, **k: pytest.fail("Unauthorized target lookup"))
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow("foreign", req, root=storage.root)
    assert error.value.code == "managed_caller_forbidden"


async def test_changed_process_generation_cannot_read_or_reply(setup, monkeypatch):
    storage, rid, principal = setup
    changed = copy.deepcopy(principal); changed["identity"]["start_time"] = "restarted"
    monkeypatch.setattr(workflow_interactions, "capture_transport_parent", lambda: changed)
    with pytest.raises(s.SteeringError) as error:
        s.get_workflow_steering(rid, root=storage.root)
    assert error.value.code == "steering_not_owner"


async def test_native_takeover_invalidates_inflight_command_without_cancel(setup):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    run = storage.get_run(rid); turn = run["steering"]["turns"][0]
    wrapper = wrapper_for(turn, valid_definition())
    owner = {"schema_version": 1, "request_id": str(uuid.uuid4()), "action": "take_owner",
             "expected_interaction_epoch": run["interaction_epoch"], "expected_control_revision": run["steering"]["control_revision"], "reason": "Native adoption"}
    await s.steer_workflow(rid, owner, root=storage.root, monitor=True)
    registry = Registry(storage.root, frame(wrapper))
    settled = await run_turn(storage, rid, registry)
    assert len(registry.calls) == 0  # Revoked before spawn: no turn can issue.
    assert settled["steering"]["episodes"][0]["state"] == "stale"
    with pytest.raises(s.SteeringError):
        s.get_workflow_steering(rid, root=storage.root)


def valid_definition():
    return {"name": "example", "orchestrator": {"backend": "claude"},
            "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
            "connections": [{"id": "finish", "source": "start", "target": "end"}]}


def wrapper_for(turn, definition):
    target = turn["commit_target"]
    return {"episode_id": turn["episode_id"], "turn_id": turn["turn_id"], "commit_target_ref": turn["commit_target_ref"],
            "steering_revision": turn["steering_revision"], "interaction_epoch": target["interaction_epoch"],
            "checkpoint_revision": target["checkpoint_revision"], "goal_revision": target["goal_revision"], "role": "builder",
            "command": {"definition": definition, "expected_draft_revision": target["draft_revision"]}}


def frame(wrapper):
    return "HARFLOW_COMMIT_V1\n" + json.dumps(wrapper) + "\nEND_HARFLOW_COMMIT_V1"


async def test_frame_proposes_once_without_save(setup):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    turn = storage.get_run(rid)["steering"]["turns"][0]
    registry = Registry(storage.root, frame(wrapper_for(turn, valid_definition())))
    run = await run_turn(storage, rid, registry)
    assert run["steering"]["episodes"][0]["state"] == "committed"
    assert len(run["steering"]["commits"]) == 1
    assert run["generated_definition"]["draft"] is True
    assert run["status"] == "completed" and "attention_reason" not in run
    assert storage.list() == []
    await run_turn(storage, rid, registry)
    assert len(registry.calls) == 1 and len(storage.get_run(rid)["steering"]["commits"]) == 1


@pytest.mark.parametrize("changes,expected,unknown", [({"exit_code": 1, "status": "failed"}, "provider_failure", False),
    ({"output_drained": False}, "capture_unknown", True), ({"exit_code": None}, "capture_unknown", True),
    ({"status": "cancelled"}, "cancelled", False), ({"timed_out": True}, "timeout", False)])
async def test_good_looking_command_requires_positive_observation(setup, changes, expected, unknown):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    turn = storage.get_run(rid)["steering"]["turns"][0]
    registry = Registry(storage.root, frame(wrapper_for(turn, valid_definition())), **changes)
    run = await run_turn(storage, rid, registry)
    assert not run["steering"]["commits"]
    assert run["steering"]["turns"][0]["outcome"] == expected
    assert len(run["steering"]["turns"]) == 1
    assert (run["steering"]["episodes"][0]["state"] == "delivery_unknown") == unknown


async def test_restart_after_spawn_intent_never_reissues(setup):
    storage, rid, _ = setup
    req = request(storage, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    storage.update_run(rid, lambda r: r["steering"]["turns"][0].update(state="spawn_intent"), "simulate_crash")
    registry = Registry(storage.root)
    run = await run_turn(storage, rid, registry)
    assert registry.calls == []
    assert run["steering"]["episodes"][0]["state"] == "delivery_unknown"
    await s.steer_workflow(rid, req, root=storage.root)
    await run_turn(storage, rid, registry)
    assert registry.calls == [] and len(storage.get_run(rid)["steering"]["turns"]) == 1


async def test_held_resume_followup_and_message_now_do_not_bypass(setup):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    await run_turn(storage, rid, Registry(storage.root))
    with pytest.raises(s.SteeringError, match="held conversation"):
        storage.control(rid, "resume", instructions="Continue", additional_attempts=5)
    with pytest.raises(s.SteeringError):
        await w.followup_workflow_builder(rid, "Continue", root=storage.root)
    from harflow_mcp import workflow_messages
    result = workflow_messages.send(storage, rid, "HARFLOW_COMMIT_V1", delivery="now")
    assert result["refusal"]["code"] == "workflow_checkpoint_required"
    assert len(storage.get_run(rid)["steering"]["turns"]) == 1


async def test_external_draft_change_stales_commit(setup):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    turn = storage.get_run(rid)["steering"]["turns"][0]
    registry = Registry(storage.root, frame(wrapper_for(turn, valid_definition())))
    async def start(prompt, path, **kwargs):
        registry.calls.append((prompt, path, kwargs))
        storage.update_run(rid, lambda r: r.update(draft_revision=1), "external_edit")
        return Task(kwargs["task_id"], registry.final)
    registry.start = start
    run = await run_turn(storage, rid, registry)
    assert run["steering"]["turns"][0]["outcome"] == "steering_target_stale"
    assert not run["steering"]["commits"]


async def test_unsupported_cap_refuses_without_issuance(setup):
    storage, rid, _ = setup
    storage.update_run(rid, lambda r: r["definition"]["orchestrator"].update(backend="codex"), "unsupported_candidate")
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    assert error.value.code == "steering_budget_unsupported"
    assert not storage.get_run(rid)["steering"]["turns"]


@pytest.mark.parametrize("external_edit", [False, True])
async def test_active_builder_queues_then_safe_boundary_issues_or_stales(setup, external_edit):
    from harflow_mcp.workflow_builder_finalization import finish_ordinary_builder
    storage, rid, _ = setup
    activation = {"id": "original", "node_id": "builder", "role": "builder", "status": "running", "tasks": [{"task_id": "old-task", "status": "running"}], "created_at": 1.0, "token": None}
    storage.update_run(rid, lambda r: r.update(status="running", activations=[activation]), "old_started")
    receipt = await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    assert receipt["state"] == "queued_boundary" and receipt["scheduled"]
    registry = Registry(storage.root)
    supervisor = w.WorkflowSupervisor(registry, storage); supervisor.run_id = rid
    await s.run_builder_turn(supervisor, rid)
    assert not registry.calls
    finish_ordinary_builder(supervisor, activation, {"summary": json.dumps(valid_definition())})
    assert storage.list() == []
    assert "generated_definition" not in storage.get_run(rid)
    def settle(run):
        run["activations"][0]["tasks"][0]["status"] = "completed"
        run["activations"][0]["status"] = "completed"
        if external_edit:
            run["draft_revision"] += 1
    storage.update_run(rid, settle, "old_exit_observed")
    run = await run_turn(storage, rid, registry)
    assert len(registry.calls) == (0 if external_edit else 1)
    assert run["steering"]["episodes"][0]["state"] == ("stale" if external_edit else "awaiting_caller")
    assert storage.list() == []


@pytest.mark.parametrize("final_kind", ["current", "old", "prose"])
async def test_own_preview_advances_explicit_witness_but_not_issuance(setup, final_kind):
    from harflow_mcp import store
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    initial = copy.deepcopy(storage.get_run(rid)["steering"]["turns"][0])
    registry = Registry(storage.root)
    async def start(prompt, path, **kwargs):
        registry.calls.append((prompt, path, kwargs))
        record = store.TaskRecord(task_id=kwargs["task_id"], backend="claude", session_id="preview-session", status="running", repo_path=str(path), freedom="read_only", started_at="2026-11-05T00:00:00+00:00")
        store.write(storage.root / "tasks", record)
        result = await w.apply_workflow_draft(valid_definition(), 0, caller=lineage.Caller(record, "ancestry"), root=storage.root)
        assert result["draft_revision"] == 1
        assert result["commit_target_ref"] != initial["commit_target_ref"]
        current = storage.get_run(rid)["steering"]["turns"][0]
        assert current["target"] == initial["target"]
        assert current["commit_target"]["draft_revision"] == 1
        final = "The preview is ready for discussion." if final_kind == "prose" else frame(wrapper_for(current if final_kind == "current" else initial, valid_definition()))
        return Task(kwargs["task_id"], final)
    registry.start = start
    run = await run_turn(storage, rid, registry)
    assert len(registry.calls) == 1 and len(run["steering"]["turns"]) == 1
    assert len(run["steering"]["turns"][0]["preview_receipts"]) == 1, run["steering"]["turns"][0].get("dispatch_error")
    assert storage.list() == []
    if final_kind == "current":
        assert run["steering"]["episodes"][0]["state"] == "committed"
    else:
        assert "generated_definition" not in run
        assert run["draft_revision"] == 1
        assert run["steering"]["episodes"][0]["state"] == ("command_refused" if final_kind == "old" else "awaiting_caller")


async def test_dismiss_reserved_issuance_never_spawns_and_clears_only_hold(setup):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    ep = storage.get_run(rid)["steering"]["episodes"][0]
    await s.steer_workflow(rid, request(storage, rid, action="dismiss", ep=ep), root=storage.root)
    registry = Registry(storage.root)
    run = await run_turn(storage, rid, registry)
    assert registry.calls == []
    assert run["steering"]["episodes"][0]["state"] == "dismissed"
    assert not s.held(run) and run["status"] == "paused"
    assert len(run["steering"]["turns"]) == 1


async def test_budget_issuance_is_nonrefundable_after_invalid_frame(setup):
    storage, rid, _ = setup
    for index in range(8):
        run = storage.get_run(rid)
        ep = run["steering"]["episodes"][0] if index else None
        await s.steer_workflow(rid, request(storage, rid, action="reply" if index else "open", ep=ep), root=storage.root)
        registry = Registry(storage.root, "HARFLOW_COMMIT_V1\n{broken}\nEND_HARFLOW_COMMIT_V1")
        await run_turn(storage, rid, registry)
        assert len(registry.calls) == 1
    run = storage.get_run(rid)
    assert len(run["steering"]["turns"]) == 8
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, request(storage, rid, action="reply", ep=run["steering"]["episodes"][0]), root=storage.root)
    assert error.value.code == "steering_budget_exhausted"
    assert len(storage.get_run(rid)["steering"]["turns"]) == 8


@pytest.mark.parametrize("mutate", [lambda l: l["turns"][0].update(harness_turn_cap=True),
    lambda l: l["turns"][0]["commit_target"].update(role="worker"),
    lambda l: l["episodes"][0].update(hold=1),
    lambda l: l["turns"][0].update(request_id="missing"),
    lambda l: l["history"][0].update(sequence=2),
    lambda l: l["requests"][next(iter(l["requests"]))].update(task_id="another")])
async def test_persisted_authority_fails_closed_for_malformed_links(setup, mutate):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    run = storage.get_run(rid)
    mutate(run["steering"])
    from harflow_mcp.workflow_run_documents import qualify_payload, WorkflowRunDocumentError
    with pytest.raises(WorkflowRunDocumentError):
        qualify_payload(run, rid)


async def test_new_ordinary_attempt_invalidates_previously_issued_target(setup):
    storage, rid, _ = setup
    req = request(storage, rid)
    storage.update_run(rid, lambda r: r["activations"].append({"id": "new-ordinary", "node_id": "builder", "role": "builder", "status": "running", "created_at": 1.0, "tasks": [{"task_id": "new-issued-task", "status": "reserved"}]}), "another_attempt_issued")
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, req, root=storage.root)
    assert error.value.code == "steering_target_stale"
    assert not storage.get_run(rid)["steering"]["turns"]


async def test_saved_source_conflict_refuses_continuation_but_dismisses(setup):
    storage, rid, _ = setup
    source = storage.save("example", valid_definition())
    storage.update_run(rid, lambda r: r.update(source_name="example", source_revision=source["revision"], source_saved_definition=source), "bind_source")
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = Registry(storage.root)
    await run_turn(storage, rid, registry)
    storage.save("example", valid_definition(), expected_revision=source["revision"], expected_workflow_id=source["workflow_id"])
    ep = storage.get_run(rid)["steering"]["episodes"][0]
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, request(storage, rid, action="reply", ep=ep), root=storage.root)
    assert error.value.code == "steering_target_stale"
    assert error.value.data["next_action"] == "Start a new builder from the refreshed source"
    await s.steer_workflow(rid, request(storage, rid, action="dismiss", ep=ep), root=storage.root)
    assert not s.held(storage.get_run(rid))
    assert len(registry.calls) == 1
    assert storage.get("example")["revision"] == source["revision"] + 1


async def test_effective_cap_accounting_and_default_request_digest(setup):
    storage, rid, _ = setup
    storage.update_run(rid, lambda r: r["definition"]["orchestrator"].update(max_turns=3), "configured_cap")
    req = request(storage, rid)
    req.pop("allow_fresh_context")
    # Explicit False and omitted are the same canonical request; both initially
    # refuse without a retained session rather than silently creating Fresh.
    req["allow_fresh_context"] = True
    await s.steer_workflow(rid, req, root=storage.root)
    registry = Registry(storage.root)
    run = await run_turn(storage, rid, registry)
    assert registry.calls[0][2]["max_turns"] == 3
    assert run["steering"]["turns"][0]["usage"]["reserved_harness_turns"] == 3
    omitted = {"schema_version": 1, "request_id": str(uuid.uuid4()), "action": "open", "expected_interaction_epoch": 0, "expected_control_revision": 0, "expected_steering_revision": None, "episode_id": None, "target": {}, "text": "Test"}
    assert s._request(omitted) == s._request({**omitted, "allow_fresh_context": False})


@pytest.mark.parametrize("changes", [{"is_error": True}, {"execution_failure": "Provider error"}, {"failure_diagnostic": {"category": "provider_error", "reason": "Provider refused", "source": "stream:result"}}])
async def test_error_metadata_cannot_commit_a_clean_exit(setup, changes):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    turn = storage.get_run(rid)["steering"]["turns"][0]
    run = await run_turn(storage, rid, Registry(storage.root, frame(wrapper_for(turn, valid_definition())), **changes))
    assert run["steering"]["turns"][0]["outcome"] == "provider_failure"
    assert not run["steering"]["commits"]


async def test_cancel_intent_wins_before_final_commit(setup):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    turn = storage.get_run(rid)["steering"]["turns"][0]
    registry = Registry(storage.root)
    async def start(prompt, path, **kwargs):
        registry.calls.append((prompt, path, kwargs))
        storage.update_run(rid, lambda r: r.update(status="cancelling"), "cancel_intent")
        return Task(kwargs["task_id"], frame(wrapper_for(turn, valid_definition())))
    registry.start = start
    run = await run_turn(storage, rid, registry)
    assert run["status"] == "cancelled"
    assert not run["steering"]["commits"] and not s.held(run)
    assert len(registry.calls) == 1


@pytest.mark.parametrize("queued", [False, True])
async def test_expired_grant_never_launches_provider(setup, monkeypatch, queued):
    storage, rid, _ = setup
    if queued:
        storage.update_run(rid, lambda r: r.update(status="running", activations=[{"id": "old", "node_id": "builder", "role": "builder", "status": "running", "created_at": 1.0, "tasks": [{"task_id": "old-task", "status": "running"}]}]), "old_running")
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    ep = storage.get_run(rid)["steering"]["episodes"][0]
    if queued:
        storage.update_run(rid, lambda r: r["activations"][0]["tasks"][0].update(status="completed"), "old_settled")
    monkeypatch.setattr(s.time, "time", lambda: ep["created_at"] + 1801)
    registry = Registry(storage.root)
    run = await run_turn(storage, rid, registry)
    assert not registry.calls
    assert run["steering"]["episodes"][0]["state"] == "expired"
    assert s.held(run) and len(run["steering"]["turns"]) == 1


async def test_expiry_during_provider_turn_preserves_final_but_refuses_commit(setup, monkeypatch):
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    run = storage.get_run(rid); turn = run["steering"]["turns"][0]; ep = run["steering"]["episodes"][0]
    final = frame(wrapper_for(turn, valid_definition()))
    registry = Registry(storage.root)
    async def start(prompt, path, **kwargs):
        registry.calls.append((prompt, path, kwargs))
        monkeypatch.setattr(s.time, "time", lambda: ep["created_at"] + 1801)
        return Task(kwargs["task_id"], final, usage={"input_tokens": 4})
    registry.start = start
    settled = await run_turn(storage, rid, registry)
    assert settled["steering"]["turns"][0]["final"] == final
    assert settled["steering"]["turns"][0]["usage"]["usage"] == {"input_tokens": 4}
    assert settled["steering"]["turns"][0]["outcome"] == "request_expired"
    assert settled["steering"]["episodes"][0]["state"] == "expired" and s.held(settled)
    assert not settled["steering"]["commits"]


@pytest.mark.parametrize("corruption", ["missing", "null_observation", "exit_failure", "undrained", "provider_error", "unknown", "timeout", "boolean_exit", "boolean_turns", "wrong_task", "usage_conflict"])
async def test_corrupt_captured_state_refuses_restart_before_effect(setup, monkeypatch, corruption):
    from harflow_mcp import steering_runner
    from harflow_mcp.workflow_run_documents import WorkflowRunDocumentError
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    turn = storage.get_run(rid)["steering"]["turns"][0]
    registry = Registry(storage.root, frame(wrapper_for(turn, valid_definition())))
    monkeypatch.setattr(steering_runner, "_classify_commit", lambda *a: None)
    await run_turn(storage, rid, registry)
    path = storage.runs / (rid + ".json")
    document = json.loads(path.read_text())
    persisted = document["run"]["steering"]["turns"][0]
    assert persisted["state"] == "captured" and persisted["outcome"] is None
    observation = persisted["observation"]
    if corruption == "missing":
        del persisted["observation"]
    elif corruption == "null_observation":
        persisted["observation"] = None
    elif corruption == "usage_conflict":
        persisted["usage"]["cost_usd"] = 999
    else:
        key, value = {"exit_failure": ("exit_code", 1), "undrained": ("output_drained", False),
                      "provider_error": ("is_error", True), "unknown": ("outcome_unknown", True),
                      "timeout": ("timed_out", True), "boolean_exit": ("exit_code", False),
                      "boolean_turns": ("num_turns", True), "wrong_task": ("task_id", "foreign")}[corruption]
        observation[key] = value
        # Keep history concordant so the positive-observation guard itself must
        # reject forged success rather than relying on the duplicate witness.
        for event in document["run"]["steering"]["history"]:
            if event["kind"] == "final_captured":
                event["observation"] = copy.deepcopy(observation)
    path.write_text(json.dumps(document))
    restarted = w.WorkflowStore(storage.root)
    with pytest.raises(WorkflowRunDocumentError):
        await run_turn(restarted, rid, registry)
    assert len(registry.calls) == 1
    assert not json.loads(path.read_text())["run"]["steering"]["commits"]


async def test_restart_commit_guard_rechecks_positive_capture_even_without_reader(setup, monkeypatch):
    from harflow_mcp import steering_runner
    storage, rid, _ = setup
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    issued = storage.get_run(rid)["steering"]["turns"][0]
    original = steering_runner._classify_commit
    monkeypatch.setattr(steering_runner, "_classify_commit", lambda *a: None)
    await run_turn(storage, rid, Registry(storage.root, frame(wrapper_for(issued, valid_definition()))))
    run = storage.get_run(rid)
    run["steering"]["turns"][0]["observation"]["output_drained"] = False
    # An independent in-memory mutation seam deliberately bypasses qualification;
    # the final acceptance boundary still cannot turn those bytes into a proposal.
    supervisor = SimpleNamespace(update=lambda mutate, event: mutate(run))
    original(supervisor, issued["turn_id"])
    assert not run["steering"]["commits"] and "generated_definition" not in run
    assert run["steering"]["turns"][0]["outcome"] == "capture_unknown"


async def test_positive_not_started_failure_qualifies_without_fabricated_capture(setup):
    storage, rid, _ = setup
    req = request(storage, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    registry = Registry(storage.root)
    async def unsupported(*a, **kw):
        raise s.SteeringError("steering_budget_unsupported", "Runtime positively refused before spawn")
    registry.start = unsupported
    run = await run_turn(storage, rid, registry)
    turn = run["steering"]["turns"][0]
    assert turn["state"] == "settled" and turn["not_started"] is True
    assert turn["outcome"] == "steering_budget_unsupported" and "observation" not in turn
    from harflow_mcp.workflow_run_documents import qualify_payload
    qualify_payload(run, rid)
    await s.steer_workflow(rid, req, root=storage.root)
    assert len(storage.get_run(rid)["steering"]["turns"]) == 1
