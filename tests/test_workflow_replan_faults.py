"""Independent ST04 observations: fake owners only, no provider processes.

The effect and signal log is external to workflow issuance accounting. Original
execution/checklist/decision evidence is snapshotted before each operation.
"""
import copy
import asyncio
import json
import uuid

import pytest

from harflow_mcp import task_supervision, workflow_steering as steering
from test_worker_steering_faults import worker_origin, fresh, request, EffectRegistry, ObservedTask, qualified_main_provider


@pytest.fixture
async def pending_replan(worker_origin, monkeypatch):
    from harflow_mcp import identity
    storage, rid = worker_origin
    log_dir = storage.root / "tasks"
    # Remove fixture's positively completed original. This fixture instead has
    # only an owner-controlled pending launch and no provider TaskRecord.
    (log_dir / "original-provider.meta.json").unlink()
    def pending(run):
        node = next(n for n in run["definition"]["nodes"] if n["id"] == "work")
        node.update(session_mode="fresh", execution_mode="headless")
        attempt = run["activations"][0]["tasks"][0]
        attempt.update(status="running", execution_kind="headless", session_mode="fresh")
        attempt.pop("result", None)
        attempt["harness_metadata"] = dict(provenance="validated_launch_configuration",
            verification_status="configured_not_observed", requested=copy.deepcopy(attempt["candidate"]),
            effective=dict(backend="claude", model="sonnet", freedom="write_in_repo"), observed=None)
        run["supervisor_identity"] = dict(pid=424243, start_time="fixture-supervisor", markers=[])
    storage.update_run(rid, pending, "fault_pending_original")
    info = log_dir.stat()
    launch = dict(format=task_supervision.FORMAT, product="harflow", product_version=task_supervision.PRODUCT_VERSION,
        v=1, task_id="original-provider", generation="c" * 32, root=str(storage.root),
        directory_identity=[info.st_dev, info.st_ino], directory_binding=dict(path=str(log_dir.resolve()), inode=info.st_ino),
        state="waiting_for_resources", request_sha256="d" * 64, configuration_sha256="e" * 64,
        backend="claude", session_id=str(uuid.uuid4()), parent_task_id=None, chain_root_task_id=None,
        owner=dict(pid=424242, start_time="fixture-owner-generation", markers=[]),
        repo_path=fresh(storage, rid)["repo_path"], freedom="write_in_repo", network=False)
    task_supervision._write(log_dir / "original-provider.launch.json", launch)
    monkeypatch.setattr(identity, "identity_check", lambda owner: "alive")
    await steering.steer_workflow(rid, request(storage, rid), root=storage.root)
    return storage, rid, launch


def replan_request(storage, rid):
    from harflow_mcp import workflow_replan
    run = fresh(storage, rid)
    candidates = workflow_replan.targets(storage, run)
    if not candidates:
        from harflow_mcp.worker_steering import targets
        # Surface qualification refusal, rather than an unhelpful empty-list error.
        workflow_replan._source(storage, run, targets(storage, run)[0])
    target = candidates[0]
    view = steering.get_workflow_steering(rid, root=storage.root)
    episode = view["episodes"][-1]
    return dict(schema_version=2, action="replan", ledger_epoch=view["ledger_epoch"], request_id=str(uuid.uuid4()),
        episode_id=episode["episode_id"], expected_steering_revision=episode["steering_revision"],
        expected_control_revision=view["control_revision"], expected_interaction_epoch=view["interaction_epoch"],
        reason="Withdraw the never-started invocation and issue the same assignment.", **target)


async def test_replan_admission_preserves_original_evidence_and_queued_response_charge(pending_replan):
    from harflow_mcp import workflow_replan
    storage, rid, launch = pending_replan
    before = fresh(storage, rid)
    req = replan_request(storage, rid)
    await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    after = fresh(storage, rid)
    assert after["tasks"] == before["tasks"]
    assert after["definition"] == before["definition"]
    assert after.get("attempt_grants", {}) == before.get("attempt_grants", {})
    assert after["pending"][0]["accepted_decision_id"] == before["pending"][0]["accepted_decision_id"]
    assert after["steering"]["cumulative"]["issuances"] == before["steering"]["cumulative"]["issuances"]
    assert len(after["activations"]) == len(before["activations"])
    assert not (storage.root / "observed-provider-effects.txt").exists()
    assert not (storage.root / "tasks" / "original-provider.launch-cancel.json").exists()


@pytest.mark.parametrize("field", ["generation", "request_sha256", "configuration_sha256"])
async def test_changed_launch_provenance_refuses_without_legacy_cancel(pending_replan, field):
    from harflow_mcp import workflow_replan
    storage, rid, launch = pending_replan
    req = replan_request(storage, rid)
    changed = copy.deepcopy(launch)
    changed[field] = "f" * len(changed[field])
    task_supervision._write(storage.root / "tasks" / "original-provider.launch.json", changed)
    with pytest.raises(steering.SteeringError):
        await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    assert not fresh(storage, rid)["steering"].get("replans")
    assert not (storage.root / "tasks" / "original-provider.launch-cancel.json").exists()


def positive_owner_ack(storage, launch):
    from types import SimpleNamespace
    from harflow_mcp import task_withdrawal
    registry = SimpleNamespace(log_dir=storage.root / "tasks", _owner=launch["owner"],
        _launch_generations={launch["task_id"]: launch["generation"]})
    with pytest.raises(task_withdrawal.LaunchWithdrawn) as stopped:
        task_withdrawal.check_prestart(registry, launch["task_id"])
    return task_withdrawal.settle_withdrawn(registry, launch["task_id"], stopped.value)


async def test_positive_withdrawal_links_once_preserving_normal_budget_and_same_visit(pending_replan):
    from harflow_mcp import workflow_replan
    storage, rid, launch = pending_replan
    before = fresh(storage, rid)
    req = replan_request(storage, rid)
    receipt = await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    positive_owner_ack(storage, launch)
    workflow_replan.reconcile(storage, rid)
    committed = fresh(storage, rid)
    record = committed["steering"]["replans"][-1]
    replacement = next(a for a in committed["activations"] if a["id"] == record["planned_activation_id"])
    assert record["phase"] == "replan_committed"
    assert replacement["supersedes_execution_id"] == "original-execution"
    assert replacement["assignment_prompt"] == before["activations"][0]["assignment_prompt"]
    assert replacement["attempt_in_visit"] == 1
    assert replacement["visit_id"] == "worker-token"
    assert committed["pending"][0]["execution_activation_id"] == replacement["id"]
    assert committed["pending"][0]["accepted_decision_id"] == "original-decision"
    assert committed["tasks"] == before["tasks"]
    assert committed.get("attempt_grants", {}) == before.get("attempt_grants", {})
    assert committed["steering"]["cumulative"]["issuances"] == before["steering"]["cumulative"]["issuances"]
    replay = await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    assert replay["replan_id"] == receipt["replan_id"] and replay["replayed"] is True
    workflow_replan.reconcile(storage, rid)
    assert fresh(storage, rid)["activations"] == committed["activations"]


@pytest.mark.parametrize("state", ["completed", "cancelled", "needs_input"])
async def test_real_state_change_during_withdrawal_never_resurrects_or_dispatches(pending_replan, state):
    from harflow_mcp import workflow_replan
    storage, rid, launch = pending_replan
    await workflow_replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    storage.update_run(rid, lambda r: r.update(status=state, attention_reason="Concurrent actual control"), "fault_real_control")
    positive_owner_ack(storage, launch)
    workflow_replan.reconcile(storage, rid)
    result = fresh(storage, rid)
    assert result["status"] == state
    assert result["attention_reason"] == "Concurrent actual control"
    assert len(result["activations"]) == 1
    assert result["activations"][0]["tasks"][0]["status"] == "not_started"
    assert all(result["steering"]["replans"][-1]["journal"].values())
    assert result["steering"]["replans"][-1]["withdrawal_ack"]["outcome"] == "withdrawn"
    assert not (storage.root / "observed-provider-effects.txt").exists()


@pytest.mark.parametrize("cut_event", ["steering_replan_ready", "steering_replan_committed"])
async def test_crash_at_link_boundary_recovers_one_preallocated_activation(pending_replan, monkeypatch, cut_event):
    from harflow_mcp import workflow_replan, workflows
    storage, rid, launch = pending_replan
    req = replan_request(storage, rid)
    receipt = await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    reserved = copy.deepcopy(fresh(storage, rid)["steering"]["replans"][-1])
    positive_owner_ack(storage, launch)
    original = storage.update_run
    class PowerLoss(BaseException):
        pass
    def cut_after_write(run_id, mutator, event, *args, **kwargs):
        result = original(run_id, mutator, event, *args, **kwargs)
        if event == cut_event:
            raise PowerLoss()
        return result
    monkeypatch.setattr(storage, "update_run", cut_after_write)
    with pytest.raises(PowerLoss):
        workflow_replan.reconcile(storage, rid)
    restored = workflows.WorkflowStore(storage.root)
    workflow_replan.reconcile(restored, rid)
    result = fresh(restored, rid)
    assert result["steering"]["replans"][-1]["phase"] == "replan_committed"
    replacements = [a for a in result["activations"] if a.get("supersedes_execution_id") == "original-execution"]
    assert len(replacements) == 1
    assert replacements[0]["id"] == reserved["planned_activation_id"]
    assert replacements[0]["planned_task_id"] == reserved["planned_task_id"]
    assert result["pending"][0]["execution_activation_id"] == replacements[0]["id"]
    replay = await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    assert replay["replan_id"] == receipt["replan_id"]
    assert len(fresh(restored, rid)["activations"]) == 2
    assert not (storage.root / "observed-provider-effects.txt").exists()


async def test_response_claim_winning_before_admission_prevents_original_withdrawal(pending_replan):
    from harflow_mcp import workflow_replan
    storage, rid, launch = pending_replan
    req = replan_request(storage, rid)
    storage.update_run(rid, lambda r: r["steering"]["turns"][-1].update(state="spawn_intent"), "fault_response_claim_won")
    with pytest.raises(steering.SteeringError):
        await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    assert not list((storage.root / "tasks").glob("*.withdraw.json"))
    assert not fresh(storage, rid)["steering"].get("replans")
    assert task_supervision.read_launch(storage.root / "tasks", launch["task_id"])["state"] == "waiting_for_resources"


async def test_fresh_backend_generated_session_id_is_not_resume_authority(pending_replan):
    from harflow_mcp import workflow_replan
    storage, rid, launch = pending_replan
    assert str(uuid.UUID(launch["session_id"])) == launch["session_id"]
    assert launch["parent_task_id"] is None
    req = replan_request(storage, rid)
    accepted = await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    assert accepted["accepted"] is True
    positive_owner_ack(storage, launch)
    workflow_replan.reconcile(storage, rid)
    result = fresh(storage, rid)
    record = result["steering"]["replans"][-1]
    assert record["phase"] == "replan_committed"
    replacement = next(a for a in result["activations"] if a["id"] == record["planned_activation_id"])
    assert replacement["execution_session_mode"] == "fresh"
    assert replacement.get("resume_task_id") is None


async def test_monitor_cannot_withdraw_active_caller_owned_work(pending_replan):
    from harflow_mcp import workflow_replan
    storage, rid, launch = pending_replan
    req = replan_request(storage, rid)
    with pytest.raises(steering.SteeringError) as refused:
        await workflow_replan.replan_workflow_steering(rid, req, root=storage.root, monitor=True)
    assert refused.value.code == "replan_not_owner"
    assert not list((storage.root / "tasks").glob("*.withdraw.json"))
    assert not fresh(storage, rid)["steering"].get("replans")


async def test_unknown_supervisor_identity_refuses_before_withdrawal(pending_replan, monkeypatch):
    from harflow_mcp import identity, workflow_replan
    storage, rid, launch = pending_replan
    req = replan_request(storage, rid)
    monkeypatch.setattr(identity, "identity_check", lambda owner: "undecidable")
    with pytest.raises(steering.SteeringError) as refused:
        await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    assert refused.value.code == "caller_undecidable"
    assert not list((storage.root / "tasks").glob("*.withdraw.json"))
    assert not fresh(storage, rid)["steering"].get("replans")


async def test_original_spawn_after_intent_is_unknown_with_no_replacement_on_replay(pending_replan):
    from types import SimpleNamespace
    from harflow_mcp import task_withdrawal, workflow_replan
    storage, rid, launch = pending_replan
    req = replan_request(storage, rid)
    receipt = await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    effects = ["ORIGINAL_SPAWN"]
    issued = {**launch, "state": "spawned"}
    task_supervision._write(storage.root / "tasks" / "original-provider.launch.json", issued)
    registry = SimpleNamespace(log_dir=storage.root / "tasks", _owner=launch["owner"],
        _launch_generations={launch["task_id"]: launch["generation"]})
    task_withdrawal.acknowledge_issued(registry, launch["task_id"])
    for _ in range(3):
        workflow_replan.reconcile(storage, rid)
        replay = await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
        assert replay["replan_id"] == receipt["replan_id"]
        current = fresh(storage, rid)
        assert current["steering"]["replans"][-1]["phase"] == "settlement_unknown"
        assert len(current["activations"]) == 1
        assert current["pending"][0]["execution_activation_id"] == "original-execution"
    assert effects == ["ORIGINAL_SPAWN"]
    assert not (storage.root / "observed-provider-effects.txt").exists()
    assert not (storage.root / "tasks" / "original-provider.launch-cancel.json").exists()


async def test_waiting_b_runner_cannot_resurrect_response_revoked_by_replan(pending_replan):
    from harflow_mcp import workflows, workflow_replan
    from harflow_mcp.worker_steering_runner import run_worker_turn
    storage, rid, launch = pending_replan
    registry = EffectRegistry(storage)
    ordinary = ObservedTask(launch["task_id"], "Original eventually completed after spawn race.")
    ordinary.done.clear()
    attached = asyncio.Event()
    def attach(task_id):
        assert task_id == launch["task_id"]
        attached.set()
        return ordinary
    registry.attach = attach
    running = asyncio.create_task(run_worker_turn(workflows.WorkflowSupervisor(registry, storage), rid))
    await asyncio.wait_for(attached.wait(), 5)
    req = replan_request(storage, rid)
    await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    revoked = copy.deepcopy(fresh(storage, rid)["steering"]["turns"][-1])
    assert revoked["state"] == "settled" and revoked["not_started"] is True
    # The boundary now becomes positively completed, after revocation won.
    task_supervision._write(storage.root / "tasks" / "original-provider.launch.json", {**launch, "state": "spawned"})
    ordinary.done.set()
    await asyncio.wait_for(running, 5)
    result = fresh(storage, rid)
    assert result["steering"]["turns"][-1] == revoked
    assert registry.effects == []
    assert not registry.effect_path.exists()
    assert len(result["activations"]) == 1


@pytest.mark.parametrize("restart_preparing", [False, True])
async def test_normal_linked_dispatch_uses_planned_id_candidate_and_nonzero_attempt(pending_replan, qualified_main_provider, monkeypatch, restart_preparing):
    from harflow_mcp import workflows, workflow_replan, workflow_delegation
    storage, rid, launch = pending_replan
    await workflow_replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    positive_owner_ack(storage, launch)
    workflow_replan.reconcile(storage, rid)
    linked = fresh(storage, rid)
    record = linked["steering"]["replans"][-1]
    registry = EffectRegistry(storage, json.dumps(dict(status="succeeded", result=dict(summary="Exact linked work", completed_task_ids=["task-one"]), evidence=[])))
    supervisor = workflows.WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    node = next(n for n in linked["definition"]["nodes"] if n["id"] == "work")
    if restart_preparing:
        original_update = storage.update_run
        class PowerLoss(BaseException):
            pass
        def cut_reserved(run_id, mutator, event, *args, **kwargs):
            result = original_update(run_id, mutator, event, *args, **kwargs)
            if event == "dispatch_reserved":
                raise PowerLoss()
            return result
        monkeypatch.setattr(storage, "update_run", cut_reserved)
        with pytest.raises(PowerLoss):
            await workflow_delegation.execute_node(supervisor, node, copy.deepcopy(linked["pending"][0]))
        assert registry.effects == []
        preparing = next(a for a in fresh(storage, rid)["activations"] if a["id"] == record["planned_activation_id"])
        assert len(preparing["tasks"]) == 1
        assert preparing["tasks"][0]["task_id"] == record["planned_task_id"]
        assert preparing["tasks"][0]["dispatch_stage"] == "preparing"
        restored = workflows.WorkflowStore(storage.root)
        restored.reconcile_run(rid)
        recovered = next(a for a in fresh(restored, rid)["activations"] if a["id"] == record["planned_activation_id"])
        assert recovered["tasks"][0]["status"] == "not_started"
        assert recovered["tasks"][0]["dispatch_stage"] == "preparing"
        supervisor = workflows.WorkflowSupervisor(registry, restored)
        supervisor.run_id = rid
    await workflow_delegation.execute_node(supervisor, node, copy.deepcopy(linked["pending"][0]))
    result = fresh(storage, rid)
    replacement = next(a for a in result["activations"] if a["id"] == record["planned_activation_id"])
    assert [effect["task_id"] for effect in registry.effects] == [record["planned_task_id"]]
    assert registry.effects[0]["options"]["model"] == "sonnet"
    assert registry.effects[0]["options"]["max_turns"] == 17
    assert replacement["attempt_in_visit"] == 1 and replacement["visit_id"] == "worker-token"
    assert replacement["node_result"]["status"] == "succeeded"
    assert result["pending"][0]["execution_activation_id"] == replacement["id"]
    assert result["pending"][0]["execution_complete"] is True
    assert len(replacement["tasks"]) == 1


@pytest.mark.parametrize("return_before_ack", [False, True])
async def test_delayed_old_dispatch_failure_cannot_finish_new_same_token(pending_replan, qualified_main_provider, monkeypatch, return_before_ack):
    from harflow_mcp import workflows, workflow_replan, workflow_delegation
    storage, rid, launch = pending_replan
    class FakeOwnerCheckout:
        # The fake owner positively acknowledges withdrawal and has no checkout
        # process. Keep that fixture lease independent of the delayed coroutine.
        def __init__(self, *args, **kwargs):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
    monkeypatch.setattr(workflows, "CheckoutLease", FakeOwnerCheckout)
    # Start an independent scenario before the original dispatcher reserved its
    # row; the fake start below publishes only a pending owner, never a process.
    def before_dispatch(run):
        activation = run["activations"][0]
        activation.update(tasks=[], planned_task_id=launch["task_id"])
        activation.pop("worker_result_required", None)
        activation.pop("steering_superseded", None)
        activation.pop("worker_steering_assignment_digest", None)
        ledger = run["steering"]
        ledger.update(turns=[], episodes=[], requests={}, history=[], sequence=0, history_floor=0)
        ledger["cumulative"]["issuances"] = 0
    storage.update_run(rid, before_dispatch, "fault_before_original_reservation")
    (storage.root / "tasks" / "original-provider.launch.json").unlink()
    entered, release = asyncio.Event(), asyncio.Event()
    old_registry = EffectRegistry(storage)
    async def pending_start(prompt, repo, **options):
        assert options["task_id"] == launch["task_id"]
        task_supervision._write(storage.root / "tasks" / "original-provider.launch.json", launch)
        entered.set()
        await release.wait()
        raise ValueError("delayed original start refusal after certified withdrawal")
    old_registry.start = pending_start
    from harflow_mcp.workflow_invocation import WorkflowTree
    tree = WorkflowTree(storage, rid, permits=1) if return_before_ack else None
    old_supervisor = workflows.WorkflowSupervisor(old_registry, storage, tree=tree)
    old_supervisor.run_id = rid
    initial = fresh(storage, rid)
    node = next(n for n in initial["definition"]["nodes"] if n["id"] == "work")
    stale_token = copy.deepcopy(initial["pending"][0])
    old_call = asyncio.create_task(workflow_delegation.execute_node(old_supervisor, node, stale_token))
    await asyncio.wait_for(entered.wait(), 5)
    await steering.steer_workflow(rid, request(storage, rid), root=storage.root)
    await workflow_replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    if return_before_ack:
        assert fresh(storage, rid)["steering"]["replans"][-1]["journal"]["original_withdrawn"] is False
        release.set()
        await asyncio.wait_for(old_call, 5)
        assert (rid, launch["task_id"]) in tree.held
    positive_owner_ack(storage, launch)
    if return_before_ack:
        await workflow_replan.run_replan(old_supervisor, rid)
        assert (rid, launch["task_id"]) not in tree.held
    else:
        workflow_replan.reconcile(storage, rid)
    linked = fresh(storage, rid)
    record = linked["steering"]["replans"][-1]
    new_registry = EffectRegistry(storage, json.dumps(dict(status="succeeded", result=dict(summary="New bounded result", completed_task_ids=["task-one"]), evidence=[])))
    new_supervisor = workflows.WorkflowSupervisor(new_registry, storage, tree=tree)
    new_supervisor.run_id = rid
    await asyncio.wait_for(workflow_delegation.execute_node(new_supervisor, node, copy.deepcopy(linked["pending"][0])), 5)
    accepted = fresh(storage, rid)
    source = copy.deepcopy(next(a for a in accepted["activations"] if a["id"] == "original-execution"))
    replacement = copy.deepcopy(next(a for a in accepted["activations"] if a["id"] == record["planned_activation_id"]))
    protected = {key: copy.deepcopy(accepted.get(key)) for key in ("pending", "sessions", "attention_reason", "status", "suppressed_candidates", "attempt_grants", "tasks")}
    release.set()
    await asyncio.wait_for(old_call, 5)
    final = fresh(storage, rid)
    assert next(a for a in final["activations"] if a["id"] == "original-execution") == source
    assert next(a for a in final["activations"] if a["id"] == record["planned_activation_id"]) == replacement
    assert {key: final.get(key) for key in protected} == protected
    assert [effect["task_id"] for effect in new_registry.effects] == [record["planned_task_id"]]
    assert old_registry.effects == []


@pytest.mark.parametrize("answer_mode", ["fresh", "resume"])
async def test_linked_worker_question_has_ordinary_distinct_continuation_identity(pending_replan, qualified_main_provider, answer_mode):
    from harflow_mcp import workflows, workflow_replan, workflow_delegation
    storage, rid, launch = pending_replan
    await workflow_replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    positive_owner_ack(storage, launch)
    workflow_replan.reconcile(storage, rid)
    linked = fresh(storage, rid)
    plan = linked["steering"]["replans"][-1]
    class QuestionRegistry(EffectRegistry):
        def __init__(self):
            super().__init__(storage)
            self.worker_ids, self.resume_ids = [], []
        async def start(self, prompt, repo, **options):
            if "A worker needs clarification." in prompt:
                context = json.loads(prompt.split("\nContext:\n", 1)[1])
                self.final = json.dumps(dict(decision_id=context["decision_id"], action="answer", reason="Known criterion.",
                    question_id=context["worker_question"]["question_id"], answer="Use exact criterion A.", session_mode=answer_mode))
            else:
                self.worker_ids.append(options["task_id"])
                self.final = json.dumps(dict(status="asking", result=dict(question="Which criterion?"), evidence=[])) if len(self.worker_ids) == 1 else json.dumps(dict(status="succeeded", result=dict(summary="Answer delivered", completed_task_ids=["task-one"]), evidence=[]))
            return await super().start(prompt, repo, **options)
        async def resume(self, previous, prompt, **options):
            self.resume_ids.append(options["task_id"])
            prior = next(effect for effect in self.effects if effect["task_id"] == previous.task_id)
            merged = {**prior["options"], **options}
            return await self.start(prompt, prior["repo"], **merged)
    registry = QuestionRegistry()
    supervisor = workflows.WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    node = next(n for n in linked["definition"]["nodes"] if n["id"] == "work")
    await asyncio.wait_for(workflow_delegation.execute_node(supervisor, node, copy.deepcopy(linked["pending"][0])), 10)
    result = fresh(storage, rid)
    activation = next(a for a in result["activations"] if a["id"] == plan["planned_activation_id"])
    assert len(registry.worker_ids) == 2
    assert registry.worker_ids[0] == plan["planned_task_id"]
    assert registry.worker_ids[1] != plan["planned_task_id"]
    assert len(set(effect["task_id"] for effect in registry.effects)) == 3
    assert registry.resume_ids == ([registry.worker_ids[1]] if answer_mode == "resume" else [])
    assert activation["node_result"]["status"] == "succeeded"
    assert activation["questions"][0]["status"] == "answered"
    assert activation["questions"][0]["answer_delivery_state"] == "settled"
    assert activation["attempt_in_visit"] == 1
    assert result.get("attempt_grants", {}) == linked.get("attempt_grants", {})
    workers = [effect for effect in registry.effects if effect["task_id"] in registry.worker_ids]
    assert [effect["options"]["max_turns"] for effect in workers] == [17, 17]


async def test_actual_supervisor_legacy_cancel_preserves_certified_original(pending_replan):
    from harflow_mcp import workflows, workflow_replan
    storage, rid, launch = pending_replan
    await workflow_replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    positive_owner_ack(storage, launch)
    workflow_replan.reconcile(storage, rid)
    before = fresh(storage, rid)
    original = copy.deepcopy(next(a for a in before["activations"] if a["id"] == "original-execution"))
    storage.control(rid, "cancel")
    registry = EffectRegistry(storage)
    await asyncio.wait_for(workflows.WorkflowSupervisor(registry, storage).execute(rid), 10)
    result = fresh(storage, rid)
    assert result["status"] == "cancelled"
    assert next(a for a in result["activations"] if a["id"] == "original-execution") == original
    assert result["steering"]["replans"][-1]["phase"] == "replan_committed"
    assert registry.effects == []


@pytest.mark.parametrize("source", ["root", "missing_root", "nested"])
async def test_unmarked_expired_ordinary_turn_never_reads_st04_proof(worker_origin, tmp_path, monkeypatch, source):
    from harflow_mcp import workflows, replan_certificates
    from harflow_mcp.worker_steering_runner import _replan_revoked
    storage, rid = worker_origin
    target = None
    if source == "nested":
        from test_worker_steering_faults import nested_worker_checkpoint
        rid, child_id = nested_worker_checkpoint(storage, tmp_path)
        target = next(t for t in steering.get_workflow_steering(rid, root=storage.root)["targets"] if t["role"] == "worker" and t["workflow_run_id"] == child_id)
    await steering.steer_workflow(rid, request(storage, rid, target=target), root=storage.root)
    def ordinary_expired(run):
        run["steering"]["turns"][-1].update(state="settled", outcome="request_expired", not_started=True)
        run["steering"]["episodes"][-1].update(state="stale", hold=False)
        if source == "missing_root":
            run["activations"] = []
    storage.update_run(rid, ordinary_expired, "fault_ordinary_expiry_without_replan")
    turn = copy.deepcopy(fresh(storage, rid)["steering"]["turns"][-1])
    assert "revoked_by_replan_id" not in turn
    observed_reads = []
    def no_replan_read(*args, **kwargs):
        observed_reads.append((args, kwargs))
        pytest.fail("Ordinary turn expiry is not an ST04 proof obligation")
    monkeypatch.setattr(replan_certificates, "read", no_replan_read)
    supervisor = workflows.WorkflowSupervisor(EffectRegistry(storage), storage)
    supervisor.run_id = rid
    assert _replan_revoked(supervisor, turn["turn_id"], turn) is False
    assert observed_reads == []
    assert fresh(storage, rid)["steering"]["turns"][-1] == turn


async def test_replan_revocation_marker_is_atomic_with_intent_and_retained_proof(pending_replan):
    from harflow_mcp import workflows, workflow_replan
    from harflow_mcp.worker_steering_runner import _replan_revoked, _capture
    storage, rid, launch = pending_replan
    receipt = await workflow_replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    revoked = copy.deepcopy(fresh(storage, rid)["steering"]["turns"][-1])
    assert revoked["revoked_by_replan_id"] == receipt["replan_id"]
    assert revoked["state"] == "settled" and revoked["outcome"] == "request_expired" and revoked["not_started"] is True
    supervisor = workflows.WorkflowSupervisor(EffectRegistry(storage), storage)
    supervisor.run_id = rid
    assert _replan_revoked(supervisor, revoked["turn_id"], revoked) is True
    _capture(supervisor, revoked["turn_id"], snapshot={"task_id": revoked["task_id"], "status": "completed", "summary": "Late capture"}, origin=revoked)
    assert fresh(storage, rid)["steering"]["turns"][-1] == revoked


@pytest.mark.parametrize("corruption", ["missing_marker", "other_marker", "queued_task", "source", "root"])
async def test_retained_revocation_crosslinks_cannot_be_rewritten(pending_replan, corruption):
    from harflow_mcp import workflow_replan, workflows
    from harflow_mcp.workflow_run_documents import WorkflowRunCandidateError
    from harflow_mcp.worker_steering_runner import _capture
    storage, rid, launch = pending_replan
    await workflow_replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    turn = copy.deepcopy(fresh(storage, rid)["steering"]["turns"][-1])
    path = storage.runs / (rid + ".json")
    before = path.read_bytes()
    def corrupt(run):
        record = run["steering"]["replans"][-1]
        if corruption == "missing_marker":
            run["steering"]["turns"][-1].pop("revoked_by_replan_id")
        elif corruption == "other_marker":
            run["steering"]["turns"][-1]["revoked_by_replan_id"] = "different-replan"
        elif corruption == "queued_task":
            record["withdrawal_witness"]["queued_task_id"] = "different-task"
        elif corruption == "source":
            record["withdrawal_witness"]["activation_id"] = "different-source"
        else:
            record["target"]["root_workflow_run_id"] = "different-root"
    with pytest.raises(WorkflowRunCandidateError):
        storage.update_run(rid, corrupt, "fault_revocation_proof_corruption")
    assert path.read_bytes() == before
    supervisor = workflows.WorkflowSupervisor(EffectRegistry(storage), storage)
    supervisor.run_id = rid
    _capture(supervisor, turn["turn_id"], snapshot={"task_id": turn["task_id"], "status": "completed"}, origin=turn)
    assert path.read_bytes() == before
