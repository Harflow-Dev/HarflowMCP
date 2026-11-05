"""Independent live-job retention faults with in-process pending owners only.

Each source has a real asyncio outer job and the production done callback. No
provider is invoked; the independent issuance/effect log must remain empty.
"""
import asyncio
import copy
import json
import uuid
from types import SimpleNamespace

import pytest

from harflow_mcp import task_supervision, workflow_replan, replan_retention, replan_certificates
from harflow_mcp import workflow_steering as steering, workflows
from test_worker_steering_faults import worker_origin, fresh, request, qualified_main_provider, MainRegistry, EffectRegistry, ObservedTask, framed_result, run_turn
from test_workflow_replan_faults import pending_replan, replan_request, positive_owner_ack


async def bind_live_source(storage, rid, *, hold=False):
    """Bind source provenance through the actual owned outer-job context API."""
    ready, released, acknowledged = asyncio.Event(), asyncio.Event(), asyncio.Event()
    run = fresh(storage, rid)
    token = copy.deepcopy(run["pending"][0])
    aid = token["execution_activation_id"]
    activation = next(a for a in run["activations"] if a["id"] == aid)
    tid = next(t["task_id"] for t in activation["tasks"] if not t.get("steering_turn_id"))
    identity = copy.deepcopy(run["supervisor_identity"])
    supervisor = SimpleNamespace(store=storage, run_id=rid)
    async def node(pending):
        metadata = replan_retention.dispatcher_context(supervisor, aid, tid)
        storage.update_run(rid, lambda r: next(t for a in r["activations"] if a["id"] == aid for t in a["tasks"] if t["task_id"] == tid).update(metadata), "fault_bound_owned_job")
        ready.set()
        await released.wait()
    supervisor._node = node
    coro, done = replan_retention.new_outer_job(supervisor, token, identity)
    task = asyncio.create_task(coro)
    done.bind(task)
    def observed_done(completed):
        done(completed)
        acknowledged.set()
    task.add_done_callback(observed_done)
    await asyncio.wait_for(ready.wait(), 5)
    async def finish():
        released.set()
        await asyncio.wait_for(task, 5)
        await asyncio.wait_for(acknowledged.wait(), 5)
    return finish, task


async def next_pending_source(storage, rid, prior_launch):
    run = fresh(storage, rid)
    activation_id = run["pending"][0]["execution_activation_id"]
    activation = next(a for a in run["activations"] if a["id"] == activation_id)
    tid = activation["planned_task_id"]
    candidate = copy.deepcopy(activation["execution_candidate"])
    row = dict(task_id=tid, status="running", execution_kind="headless", session_mode="fresh", candidate=candidate,
        repo_path=run["repo_path"], freedom="write_in_repo", network=False,
        harness_metadata=dict(provenance="validated_launch_configuration", verification_status="configured_not_observed",
            requested=copy.deepcopy(candidate), effective=dict(backend="claude", model="sonnet", freedom="write_in_repo"), observed=None))
    storage.update_run(rid, lambda r: next(a for a in r["activations"] if a["id"] == activation_id)["tasks"].append(row), "fault_next_pending_source")
    launch = {key: copy.deepcopy(value) for key, value in prior_launch.items() if key not in {
        "withdrawal_claim", "withdrawal_proof", "not_started_reason", "error"}}
    launch.update(task_id=tid, generation=uuid.uuid4().hex, session_id=str(uuid.uuid4()), state="waiting_for_resources",
        request_sha256=uuid.uuid4().hex * 2, configuration_sha256=uuid.uuid4().hex * 2)
    task_supervision._write(storage.root / "tasks" / (tid + ".launch.json"), launch)
    await steering.steer_workflow(rid, request(storage, rid), root=storage.root)
    return launch


async def chain(storage, rid, launch, count, *, defer_first=None, reason=None):
    receipts = []
    for index in range(count):
        finish, task = await bind_live_source(storage, rid)
        req = replan_request(storage, rid)
        if reason is not None:
            req["reason"] = reason
        receipt = await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
        positive_owner_ack(storage, launch)
        workflow_replan.reconcile(storage, rid)
        if index == 0 and defer_first is not None:
            defer_first.append(finish)
        else:
            await finish()
        run = fresh(storage, rid)
        record = next(r for r in run["steering"]["replans"] if r["replan_id"] == receipt["replan_id"])
        if index != 0 or defer_first is None:
            assert record["quiescence"]["origin"] == "outer_node_job_done"
        assert record["attempt_in_visit"] == 1
        receipts.append((req, receipt))
        if index + 1 < count:
            launch = await next_pending_source(storage, rid, launch)
    return receipts


async def test_eighty_positive_prestart_replans_use_window_and_permanent_direct_proofs(pending_replan):
    storage, rid, launch = pending_replan
    baseline = fresh(storage, rid)
    receipts = await chain(storage, rid, launch, 80)
    result = fresh(storage, rid)
    assert len(result["steering"]["replans"]) <= 32
    assert len(result["steering"]["requests"]) <= 64
    assert len(result["activations"]) <= 33
    assert result["steering"]["replan_accounting"]["accepted"] == 80
    assert result["steering"]["replan_accounting"]["retired_details"] == 48
    assert result["steering"]["replan_history_incomplete"] is True
    assert result["steering"]["normal_execution_history_incomplete"] is True
    assert result["tasks"] == baseline["tasks"]
    assert result["definition"] == baseline["definition"]
    assert result.get("attempt_grants", {}) == baseline.get("attempt_grants", {})
    assert result["pending"][0]["accepted_decision_id"] == "original-decision"
    assert not (storage.root / "observed-provider-effects.txt").exists()
    first = replan_certificates.read(storage, replan_id=receipts[0][1]["replan_id"])
    assert first["predecessor_execution_id"] == "original-execution"
    assert replan_certificates.read(storage, predecessor_execution_id="original-execution") == first
    assert len(json.dumps(result).encode()) < 4 * 1024 * 1024


async def test_sparse_old_live_job_stays_protected_until_captured_generation_is_dead(pending_replan, monkeypatch):
    from harflow_mcp import identity
    storage, rid, launch = pending_replan
    deferred = []
    receipts = await chain(storage, rid, launch, 40, defer_first=deferred)
    result = fresh(storage, rid)
    first_id = receipts[0][1]["replan_id"]
    oldest = next(r for r in result["steering"]["replans"] if r["replan_id"] == first_id)
    assert not oldest.get("quiescence")
    assert len(result["steering"]["replans"]) == 33
    assert result["steering"]["replan_history_incomplete"] is True
    assert any(a["id"] == "original-execution" for a in result["activations"])
    monkeypatch.setattr(identity, "check_detail", lambda dispatcher: ("undecidable", "fixture unknown"))
    replan_retention.maintain(storage, rid)
    assert any(r["replan_id"] == first_id for r in fresh(storage, rid)["steering"]["replans"])
    monkeypatch.setattr(identity, "check_detail", lambda dispatcher: ("dead", "fixture exact generation dead"))
    storage.update_run(rid, lambda r: r["sessions"].update(work=dict(candidate="claude:fixture", task_id="original-provider", session_id="unresolved-session-claim")), "fault_current_session_claim")
    replan_retention.maintain(storage, rid)
    pinned = fresh(storage, rid)
    assert any(r["replan_id"] == first_id for r in pinned["steering"]["replans"])
    assert pinned["sessions"]["work"]["task_id"] == "original-provider"
    storage.update_run(rid, lambda r: r["sessions"].pop("work"), "fault_session_claim_resolved")
    replan_retention.maintain(storage, rid)
    retired = fresh(storage, rid)
    assert not any(r["replan_id"] == first_id for r in retired["steering"]["replans"])
    assert not any(a["id"] == "original-execution" for a in retired["activations"])
    proof = replan_certificates.read(storage, replan_id=first_id)
    assert proof["quiescence"]["origin"] == "dispatcher_generation_dead"
    await deferred[0]()


@pytest.mark.parametrize("mismatch", ["job_id", "activation_id", "task_id", "replan_id", "dispatcher_generation", "unrelated_done_task"])
async def test_unrelated_done_job_cannot_certify_live_source_quiescence(pending_replan, mismatch):
    storage, rid, launch = pending_replan
    finish, source_task = await bind_live_source(storage, rid)
    await workflow_replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    positive_owner_ack(storage, launch)
    workflow_replan.reconcile(storage, rid)
    record = fresh(storage, rid)["steering"]["replans"][-1]
    assert not record.get("quiescence")
    origin = dict(replan_id=record["replan_id"], activation_id=record["withdrawal_witness"]["activation_id"], task_id=launch["task_id"])
    job_id = record["source_job_id"]
    dispatcher = copy.deepcopy(record["source_dispatcher_identity"])
    if mismatch == "job_id":
        job_id = "different-job"
    elif mismatch == "dispatcher_generation":
        dispatcher["start_time"] = "different-dispatcher-generation"
    elif mismatch == "unrelated_done_task":
        pass  # Even exact copied IDs cannot turn another task into the owner job.
    else:
        origin[mismatch] = "different-identity"
    async def completed():
        return None
    unrelated = asyncio.create_task(completed())
    await unrelated
    replan_retention.record_job_done(storage, rid, job_id, dispatcher, (origin,), unrelated)
    assert not fresh(storage, rid)["steering"]["replans"][-1].get("quiescence")
    assert source_task.done() is False
    await finish()
    assert fresh(storage, rid)["steering"]["replans"][-1]["quiescence"]["origin"] == "outer_node_job_done"


@pytest.mark.parametrize("corruption", ["second_index", "root", "candidate", "owner", "withdrawal_ack"])
async def test_permanent_direct_certificate_rejects_independent_proof_tampering(pending_replan, corruption):
    storage, rid, launch = pending_replan
    receipts = await chain(storage, rid, launch, 1)
    run = fresh(storage, rid)
    record = run["steering"]["replans"][-1]
    cert = replan_certificates.publish(storage, run, record)
    left = storage.root / "replan-lineage" / "replans" / (record["replan_id"] + ".json")
    right = storage.root / "replan-lineage" / "predecessors" / "original-execution.json"
    if corruption == "second_index":
        right.unlink()
    elif corruption in {"root", "candidate"}:
        changed = copy.deepcopy(cert)
        if corruption == "root":
            changed["root_workflow_run_id"] = "different-root"
        else:
            changed["candidate"]["max_turns"] = 999
        task_supervision._write(left, changed)
        task_supervision._write(right, changed)
    elif corruption == "owner":
        path = storage.owners / "original-provider.json"
        document = json.loads(path.read_text())
        document["receipt"]["workflow_execution_id"] = "different-execution"
        task_supervision._write(path, document)
    else:
        path = storage.root / "tasks" / (launch["task_id"] + "." + launch["generation"] + ".withdraw-ack.json")
        document = json.loads(path.read_text())
        document["proof"]["resources_released"] = False
        task_supervision._write(path, document)
    from harflow_mcp.task_withdrawal import WithdrawalError
    with pytest.raises((steering.SteeringError, WithdrawalError)):
        replan_certificates.read(storage, replan_id=record["replan_id"])
    assert not (storage.root / "observed-provider-effects.txt").exists()


async def test_certificate_lookup_reads_fixed_direct_indexes_not_predecessor_chain(pending_replan, monkeypatch):
    storage, rid, launch = pending_replan
    await chain(storage, rid, launch, 3)
    run = fresh(storage, rid)
    certificates = [replan_certificates.publish(storage, run, record) for record in run["steering"]["replans"]]
    observed = []
    original = replan_certificates._read
    def read(path, budget=None):
        observed.append(path)
        return original(path, budget)
    monkeypatch.setattr(replan_certificates, "_read", read)
    actual = replan_certificates.read(storage, predecessor_execution_id="original-execution")
    assert actual == certificates[0]
    assert len(observed) == 3
    assert {path.parent.name for path in observed} == {"replans", "predecessors"}


async def test_partial_certificate_indexes_owner_binding_and_prune_recover_exact_plan(pending_replan, monkeypatch):
    storage, rid, launch = pending_replan
    receipts = await chain(storage, rid, launch, 32)
    last = fresh(storage, rid)["steering"]["replans"][-1]
    # The next predecessor is the previous linked replacement, not an old ID.
    previous = task_supervision.read_launch(storage.root / "tasks", last["withdrawal_witness"]["task_id"])
    launch = await next_pending_source(storage, rid, previous)
    finish, task = await bind_live_source(storage, rid)
    req = replan_request(storage, rid)
    receipt = await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    positive_owner_ack(storage, launch)
    workflow_replan.reconcile(storage, rid)
    original_write = workflows._write
    cut = {"stage": "first_index"}
    def interrupted_write(path, value, *args, **kwargs):
        selected = path.name == receipts[0][1]["replan_id"] + ".json"
        if (cut["stage"] == "first_index" and path.parent.name == "replans" and selected
                or cut["stage"] == "second_index" and path.parent.name == "predecessors" and path.name == "original-execution.json"
                or cut["stage"] == "owner_receipt" and path.parent.name == "workflow-owners" and path.name == "original-provider.json"):
            raise OSError("fixture interruption at " + cut["stage"])
        return original_write(path, value, *args, **kwargs)
    monkeypatch.setattr(workflows, "_write", interrupted_write)
    await finish()  # Done callback records quiescence, then safely retains failed prune.
    for stage in ("second_index", "owner_receipt"):
        cut["stage"] = stage
        with pytest.raises(OSError, match="fixture interruption"):
            replan_retention.maintain(storage, rid)
        current = fresh(storage, rid)
        assert len(current["activations"]) == 34
        assert len(current["steering"]["replans"]) == 33
        assert not current["steering"].get("replan_compaction_pending")
    cut["stage"] = None
    original_update = storage.update_run
    class PowerLoss(BaseException):
        pass
    def namespace_landed(run_id, mutator, event, *args, **kwargs):
        result = original_update(run_id, mutator, event, *args, **kwargs)
        if event == "steering_replan_namespace_closed":
            raise PowerLoss()
        return result
    monkeypatch.setattr(storage, "update_run", namespace_landed)
    with pytest.raises(PowerLoss):
        replan_retention.maintain(storage, rid)
    pending = fresh(storage, rid)["steering"]["replan_compaction_pending"]
    assert pending["replan_ids"] == [receipts[0][1]["replan_id"]]
    assert pending["predecessor_ids"] == ["original-execution"]
    def prune_landed(run_id, mutator, event, *args, **kwargs):
        result = original_update(run_id, mutator, event, *args, **kwargs)
        if event == "steering_replan_history_compacted":
            raise PowerLoss()
        return result
    monkeypatch.setattr(storage, "update_run", prune_landed)
    with pytest.raises(PowerLoss):
        replan_retention.maintain(storage, rid)
    restored = workflows.WorkflowStore(storage.root)
    replan_retention.maintain(restored, rid)
    final = fresh(restored, rid)
    assert len(final["activations"]) == 33
    assert len(final["steering"]["replans"]) == 32
    assert final["steering"]["replan_accounting"]["retired_details"] == 1
    assert not final["steering"].get("replan_compaction_pending")
    replay = await workflow_replan.replan_workflow_steering(rid, req, root=storage.root)
    assert replay["replan_id"] == receipt["replan_id"] and replay["replayed"] is True
    with pytest.raises(steering.SteeringError) as expired:
        await workflow_replan.replan_workflow_steering(rid, receipts[0][0], root=storage.root)
    assert expired.value.code == "request_expired"
    assert len(fresh(restored, rid)["activations"]) == 33
    assert not (storage.root / "observed-provider-effects.txt").exists()


async def test_repeated_linkage_allows_later_ordinary_result_and_routing_progress(pending_replan, qualified_main_provider):
    storage, rid, launch = pending_replan
    receipts = await chain(storage, rid, launch, 3)
    linked = fresh(storage, rid)
    current_id = linked["pending"][0]["execution_activation_id"]
    immutable = [(r["replan_id"], r["planned_activation_id"], r["withdrawal_witness"]["activation_id"]) for r in linked["steering"]["replans"]]
    registry = MainRegistry(storage, rid)
    registry.normal.outputs["work"] = dict(status="succeeded", result=dict(summary="Normal replacement work complete", completed_task_ids=["task-one"]), evidence=["ordinary observed work"])
    await asyncio.wait_for(workflows.WorkflowSupervisor(registry, storage).execute(rid), 10)
    result = fresh(storage, rid)
    assert result["status"] == "completed", result.get("attention_reason")
    replacement = next(a for a in result["activations"] if a["id"] == current_id)
    assert replacement["node_result"]["status"] == "succeeded"
    assert replacement["attempt_in_visit"] == 1
    assert [(r["replan_id"], r["planned_activation_id"], r["withdrawal_witness"]["activation_id"]) for r in result["steering"]["replans"]] == immutable
    worker_calls = [options["task_id"] for prompt, options in registry.normal.calls if "Assignment:\n" in prompt]
    assert worker_calls == [linked["steering"]["replans"][-1]["planned_task_id"]]
    assert result["tasks"] == linked["tasks"]
    assert result.get("attempt_grants", {}) == linked.get("attempt_grants", {})
    assert not any(token.get("execution_activation_id") == current_id for token in result["pending"])


async def test_large_reasons_retire_detail_early_but_retained_receipt_replays_without_dispatch(pending_replan):
    storage, rid, launch = pending_replan
    reason = "Explicit same-assignment withdrawal. " + "r" * (32768 - len("Explicit same-assignment withdrawal. "))
    receipts = await chain(storage, rid, launch, 20, reason=reason)
    run = fresh(storage, rid)
    assert run["steering"]["replan_accounting"]["accepted"] == 20
    assert len(run["steering"]["replans"]) < 20
    assert len(run["steering"]["requests"]) <= 64
    text = [value for turn in run["steering"]["turns"] for key in ("text", "final") if isinstance(value := turn.get(key), str)]
    text += [value for entry in run["steering"]["history"] for key in ("text", "reason") if isinstance(value := entry.get(key), str)]
    text += [record["reason"] for record in run["steering"]["replans"]]
    assert sum(len(value.encode()) for value in text) <= 1024 * 1024
    assert len(json.dumps(run).encode()) < 4 * 1024 * 1024
    before = copy.deepcopy(run["activations"])
    replay = await workflow_replan.replan_workflow_steering(rid, receipts[0][0], root=storage.root)
    assert replay["replan_id"] == receipts[0][1]["replan_id"] and replay["replayed"] is True
    assert replay["retired_replan"] is True
    assert fresh(storage, rid)["activations"] == before
    assert not (storage.root / "observed-provider-effects.txt").exists()


@pytest.mark.parametrize("routing", ["structural", "orchestrator"])
async def test_unknown_earlier_replan_is_discharged_by_actual_result_then_real_routing(pending_replan, qualified_main_provider, routing):
    from harflow_mcp import task_withdrawal, store
    from harflow_mcp.replan_schema import accepted_normal_result
    storage, rid, launch = pending_replan
    if routing == "orchestrator":
        storage.update_run(rid, lambda r: r.pop("runner_policy", None), "fault_legacy_routing_fixture")
    await workflow_replan.replan_workflow_steering(rid, replan_request(storage, rid), root=storage.root)
    task_supervision._write(storage.root / "tasks" / "original-provider.launch.json", {**launch, "state": "spawned"})
    owner = SimpleNamespace(log_dir=storage.root / "tasks", _owner=launch["owner"], _launch_generations={launch["task_id"]: launch["generation"]})
    task_withdrawal.acknowledge_issued(owner, launch["task_id"])
    workflow_replan.reconcile(storage, rid)
    assert workflow_replan.active_replan(fresh(storage, rid))["phase"] == "settlement_unknown"
    observed = ObservedTask("original-provider", "Original bounded provider work completed.")
    store.write(storage.root / "tasks", store.TaskRecord(task_id="original-provider", backend="claude", session_id=observed.value["session_id"],
        repo_path=fresh(storage, rid)["repo_path"], freedom="write_in_repo", network=False, max_turns=17, model="sonnet",
        started_at="2026-11-05T00:00:00Z", status="completed", exit_code=0, output_drained=True))
    storage.update_run(rid, lambda r: r["activations"][0]["tasks"][0].update(status="completed", result=observed.snapshot()), "fault_original_capture")
    await steering.steer_workflow(rid, request(storage, rid), root=storage.root)
    conversation = EffectRegistry(storage, lambda: framed_result(storage, rid))
    await run_turn(storage, rid, conversation)
    accepted = fresh(storage, rid)
    assert accepted_normal_result(accepted, "original-execution") is True
    registry = MainRegistry(storage, rid)
    await asyncio.wait_for(workflows.WorkflowSupervisor(registry, storage).execute(rid), 10)
    routed = fresh(storage, rid)
    assert routed["status"] == "completed", routed.get("attention_reason")
    assert routed["decisions"]
    assert all("execution_id" not in decision and "completion_source_node_id" not in decision for decision in routed["decisions"])
    assert accepted_normal_result(routed, "original-execution") is True
    assert workflow_replan.active_replan(routed) is None
    # Pure qualifier countermodel, using actual earlier producer decisions: an
    # accepted result at a later same-node execution has no routing proof yet.
    model = copy.deepcopy(routed)
    later = copy.deepcopy(next(a for a in accepted["activations"] if a["id"] == "original-execution"))
    later["id"] = "later-same-node-execution"
    later["token"]["execution_activation_id"] = later["id"]
    model["activations"].append(later)
    model["pending"] = []
    assert any(decision["node_id"] == later["node_id"] for decision in model["decisions"])
    assert accepted_normal_result(model, later["id"]) is False
    assert len(conversation.effects) == 1


async def test_writer_read_budget_refusal_preserves_readable_protected_root(pending_replan, monkeypatch):
    from harflow_mcp import steering_window
    storage, rid, launch = pending_replan
    deferred = []
    await chain(storage, rid, launch, 2, defer_first=deferred)
    before = fresh(storage, rid)
    ledger = before["steering"]
    strings = [value for turn in ledger["turns"] for key in ("text", "final") if isinstance(value := turn.get(key), str)]
    strings += [value for event in ledger["history"] for key in ("text", "reason") if isinstance(value := event.get(key), str)]
    strings += [record["reason"] for record in ledger["replans"]]
    current_text = sum(len(value.encode()) for value in strings)
    try:
        with monkeypatch.context() as patch:
            # Force exactly the eligible successor edge into pressure retirement;
            # its protected predecessor needs a direct certificate after pruning.
            patch.setattr(steering_window, "TEXT_LIMIT", 96 * 1024 + current_text - 1)
            patch.setattr(replan_retention, "DIRECT_READ_BUDGET", 1)
            with pytest.raises(steering.SteeringError, match="budget"):
                replan_retention.maintain(storage, rid)
            readable = fresh(storage, rid)
            assert readable["activations"] == before["activations"]
            assert readable["steering"]["replan_accounting"] == before["steering"]["replan_accounting"]
            assert [r["replan_id"] for r in readable["steering"]["replans"]] == [r["replan_id"] for r in ledger["replans"]]
            assert not any(r.get("successor_certificate") for r in readable["steering"]["replans"])
            assert not (storage.root / "observed-provider-effects.txt").exists()
    finally:
        await deferred[0]()


async def test_saved_orphan_prune_plan_rechecks_new_session_pin_before_recovery(pending_replan, monkeypatch):
    from harflow_mcp import steering_window
    storage, rid, launch = pending_replan
    deferred = []
    receipts = await chain(storage, rid, launch, 2, defer_first=deferred)
    second = copy.deepcopy(fresh(storage, rid)["steering"]["replans"][-1])
    orphan_id = second["withdrawal_witness"]["activation_id"]
    orphan_task = second["withdrawal_witness"]["task_id"]
    def force_one_byte_pressure(patch, *, strict=False):
        run = fresh(storage, rid)
        strings = [value for turn in run["steering"]["turns"] for key in ("text", "final") if isinstance(value := turn.get(key), str)]
        strings += [value for entry in run["steering"]["history"] for key in ("text", "reason") if isinstance(value := entry.get(key), str)]
        strings += [r["reason"] for r in run["steering"]["replans"]]
        patch.setattr(steering_window, "TEXT_LIMIT", 96 * 1024 + (1 if strict else sum(len(value.encode()) for value in strings) - 1))
    try:
        with monkeypatch.context() as patch:
            force_one_byte_pressure(patch)
            replan_retention.maintain(storage, rid)
        partial = fresh(storage, rid)
        assert len(partial["steering"]["replans"]) == 1
        assert any(a["id"] == orphan_id for a in partial["activations"])
    finally:
        await deferred[0]()
    original_update = storage.update_run
    class PowerLoss(BaseException):
        pass
    def cut_plan(run_id, mutator, event, *args, **kwargs):
        result = original_update(run_id, mutator, event, *args, **kwargs)
        if event == "steering_replan_namespace_closed":
            raise PowerLoss()
        return result
    with monkeypatch.context() as patch:
        force_one_byte_pressure(patch, strict=True)
        patch.setattr(storage, "update_run", cut_plan)
        with pytest.raises(PowerLoss):
            replan_retention.maintain(storage, rid)
    planned = fresh(storage, rid)
    plan = copy.deepcopy(planned["steering"]["replan_compaction_pending"])
    assert orphan_id in plan["predecessor_ids"]
    assert plan["replan_ids"] == [receipts[0][1]["replan_id"]]
    storage.update_run(rid, lambda r: r["sessions"].update(work=dict(candidate="claude:fixture", task_id=orphan_task,
        session_id="new-current-owner-claim")), "fault_new_pin_after_saved_plan")
    before = fresh(storage, rid)
    with pytest.raises(steering.SteeringError):
        replan_retention.maintain(workflows.WorkflowStore(storage.root), rid)
    after = fresh(storage, rid)
    assert any(a["id"] == orphan_id for a in after["activations"])
    assert after["sessions"] == before["sessions"]
    assert after["steering"]["replan_accounting"] == before["steering"]["replan_accounting"]
    assert after["steering"]["replan_compaction_pending"] == plan
    assert not (storage.root / "observed-provider-effects.txt").exists()


@pytest.mark.parametrize("tamper", ["candidate", "harness"])
async def test_historical_certificate_survives_supported_goal_revision_but_live_dispatch_does_not(pending_replan, qualified_main_provider, tamper):
    from harflow_mcp import workflow_goals, workflow_delegation
    storage, rid, launch = pending_replan
    await chain(storage, rid, launch, 1)
    linked = fresh(storage, rid)
    record = copy.deepcopy(linked["steering"]["replans"][-1])
    registry = EffectRegistry(storage, json.dumps(dict(status="succeeded", result=dict(summary="Accepted normal work", completed_task_ids=["task-one"]), evidence=["actual fake provider result"])))
    supervisor = workflows.WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    node = next(n for n in linked["definition"]["nodes"] if n["id"] == "work")
    await workflow_delegation.execute_node(supervisor, node, copy.deepcopy(linked["pending"][0]))
    settled = fresh(storage, rid)
    replacement = next(a for a in settled["activations"] if a["id"] == record["planned_activation_id"])
    assert replacement["node_result"]["status"] == "succeeded"
    storage.control(rid, "pause")
    revision = fresh(storage, rid).get("goal_revision", 0)
    workflow_goals.revise(storage, rid, "A legitimately revised goal after quiescent work.", revision)
    revised = fresh(storage, rid)
    assert revised["goal_revision"] == revision + 1
    historical = replan_certificates.publish(storage, revised, revised["steering"]["replans"][-1])
    assert historical["goal_revision"] == record["target"]["goal_revision"]
    assert historical["source_configuration"] == record["source_configuration"]
    assert historical["candidate"] == {key: value for key, value in record["candidate"].items() if key != "fallbacks"}
    with pytest.raises(steering.SteeringError):
        workflow_replan.validate_replacement(storage, revised, next(a for a in revised["activations"] if a["id"] == replacement["id"]))
    def corrupt_source(run):
        source = next(a for a in run["activations"] if a["id"] == "original-execution")["tasks"][0]
        if tamper == "candidate":
            source["candidate"]["max_turns"] = 999
        else:
            source["harness_metadata"]["requested"]["max_turns"] = 999
    corrupted = copy.deepcopy(revised)
    corrupt_source(corrupted)
    with pytest.raises(steering.SteeringError):
        replan_certificates.publish(storage, corrupted, corrupted["steering"]["replans"][-1])
    from harflow_mcp.workflow_run_documents import WorkflowRunCandidateError
    with pytest.raises(WorkflowRunCandidateError):
        storage.update_run(rid, corrupt_source, "fault_historical_binding_tamper")
    assert fresh(storage, rid)["activations"] == revised["activations"]
    assert replan_certificates.read(storage, replan_id=record["replan_id"]) == historical
    assert len(registry.effects) == 1


@pytest.mark.parametrize("corruption", [None, "marker", "task", "source", "root", "missing_certificate", "corrupt_certificate"])
async def test_expired_replan_marker_uses_exact_permanent_proof_without_capture_mutation(pending_replan, monkeypatch, corruption):
    from harflow_mcp.worker_steering_runner import _replan_revoked, _capture
    storage, rid, launch = pending_replan
    await chain(storage, rid, launch, 2)
    before = fresh(storage, rid)
    first = before["steering"]["replans"][0]
    origin = copy.deepcopy(next(t for t in before["steering"]["turns"] if t["turn_id"] == first["withdrawal_witness"]["queued_turn_id"]))
    assert origin["revoked_by_replan_id"] == first["replan_id"]
    from harflow_mcp import steering_window
    with monkeypatch.context() as patch:
        patch.setattr(steering_window, "TEXT_LIMIT", 96 * 1024 + 1)
        replan_retention.maintain(storage, rid)
    expired = fresh(storage, rid)
    assert not any(r["replan_id"] == first["replan_id"] for r in expired["steering"]["replans"])
    assert not any(a["id"] == "original-execution" for a in expired["activations"])
    assert not any(t["turn_id"] == origin["turn_id"] for t in expired["steering"]["turns"])
    assert replan_certificates.read(storage, replan_id=first["replan_id"])["queued_turn_id"] == origin["turn_id"]
    # The late producer's saved origin is the only turn witness once detail ages.
    if corruption == "marker":
        origin["revoked_by_replan_id"] = "different-replan"
    elif corruption == "task":
        origin["task_id"] = "different-queued-task"
    elif corruption == "source":
        origin["target"]["activation_id"] = "different-source"
    elif corruption == "root":
        origin["target"]["root_workflow_run_id"] = "different-root"
    elif corruption in {"missing_certificate", "corrupt_certificate"}:
        path = storage.root / "replan-lineage" / "replans" / (first["replan_id"] + ".json")
        if corruption == "missing_certificate":
            path.unlink()
        else:
            path.write_text("{}")
    registry = EffectRegistry(storage)
    supervisor = workflows.WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    persisted = (storage.runs / (rid + ".json")).read_bytes()
    if corruption is None:
        assert _replan_revoked(supervisor, origin["turn_id"], origin) is True
        _capture(supervisor, origin["turn_id"], snapshot={"task_id": origin["task_id"], "status": "completed", "summary": "Late old output"}, origin=origin)
    else:
        with pytest.raises(steering.SteeringError):
            _capture(supervisor, origin["turn_id"], snapshot={"task_id": origin["task_id"], "status": "completed", "summary": "Wrong old output"}, origin=origin)
    assert (storage.runs / (rid + ".json")).read_bytes() == persisted
    assert registry.effects == []
