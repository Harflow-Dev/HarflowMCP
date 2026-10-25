"""G03 responder-channel and additive approval-summary regressions."""

from __future__ import annotations

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from dataclasses import replace

import pytest
from mcp.shared.exceptions import MCPError

from harflow_mcp import approval_runtime, approvals, server, store, wire_contracts

from test_approval_lifecycle import _Registry, _admit, _state
from test_approval_delivery_recheck import claude_world, admit_claude, swap_to_symlink, wire_response
from test_caller_write_approvals import repo  # noqa: F401


def _record(directory: Path, *, owner: str = "caller") -> None:
    store.write(
        directory,
        store.TaskRecord(
            task_id="task-1",
            backend="claude",
            session_id="session-1",
            repo_path=str(directory),
            started_at="now",
            status="running",
            approval_generation="generation-1",
            start_time="task-start",
            owner={"pid": 5678, "start_time": "bridge-start", "markers": []},
            interaction_owner=owner,
        ),
    )


def test_awaiting_decision_count_excludes_answered_reserved_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = _state(tmp_path)
    row = _admit(directory)
    _record(directory)
    monkeypatch.setattr("harflow_mcp.identity.identity_check", lambda _value: "alive")

    before = approvals.summary(directory, "task-1")
    assert before["pending_count"] == 1
    assert before["awaiting_decision_count"] == 1

    answered = approvals.reserve(
        directory,
        "task-1",
        row["request_id"],
        row["action_digest"],
        row["owner_revision"],
        "approve",
        row["generation"],
        responder="monitor",
    )
    after = approvals.summary(directory, "task-1")
    assert after["pending_count"] == 1  # legacy delivery-oriented count is unchanged
    assert after["awaiting_decision_count"] == 0
    assert answered["decided_by"] == "caller"
    assert answered["responder"] == "monitor"


def test_caller_channel_cannot_answer_monitor_owned_row() -> None:
    with pytest.raises(approvals.ApprovalError) as raised:
        approval_runtime._authorize_owner("caller", "monitor")
    assert raised.value.reason == "unauthorized"


@pytest.mark.asyncio
async def test_mcp_rejects_monitor_channel_before_mutation() -> None:
    with pytest.raises(MCPError, match="through harflow-ctl"):
        await server.respond_approval_request(
            "task-1", "request-1", "digest", 1, "approve", interaction_owner="monitor"
        )


def test_caller_approval_capability_is_additive() -> None:
    assert "caller-approvals.monitor-respond.v1" in wire_contracts.CAPABILITIES
    assert wire_contracts.PRODUCT_VERSION == "0.0.2"


def test_typed_reservation_refusals_preserve_reason(tmp_path: Path) -> None:
    directory = _state(tmp_path)
    row = _admit(directory)
    with pytest.raises(approvals.ApprovalError) as raised:
        approvals.reserve(
            directory,
            "task-1",
            row["request_id"],
            "wrong-digest",
            row["owner_revision"],
            "approve",
            row["generation"],
        )
    assert raised.value.reason == "not_current"

    answered = approvals.reserve(
        directory,
        "task-1",
        row["request_id"],
        row["action_digest"],
        row["owner_revision"],
        "reject",
        row["generation"],
    )
    with pytest.raises(approvals.ApprovalError) as raised:
        approvals.reserve(
            directory,
            "task-1",
            row["request_id"],
            answered["action_digest"],
            answered["owner_revision"],
            "approve",
            answered["generation"],
        )
    assert raised.value.reason == "already_answered"


@pytest.mark.asyncio
async def test_monitor_response_reaches_owner_pump_and_is_delivered(repo, monkeypatch):
    world = claude_world(repo, monkeypatch)
    world.registry._mutation_caller = lambda: _none_async()
    row = await admit_claude(world, "Write", repo.repo / "unused")
    _record(world.directory)
    answered = await approval_runtime.respond_request(
        world.registry, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "reject", "monitor"
    )
    assert (answered["decided_by"], answered["responder"], answered["delivery"]) == ("caller", "monitor", "reserved")
    await approval_runtime.pump(world.registry, world.task)
    assert wire_response(world.writes[0]) == {"behavior": "deny", "message": "Caller rejected this exact action"}


@pytest.mark.asyncio
async def test_monitor_owned_response_preserves_decided_by_and_records_monitor_responder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = _state(tmp_path)
    row = _admit(directory, interaction_owner="monitor")
    _record(directory, owner="monitor")
    registry = _Registry(directory)
    monkeypatch.setattr(approval_runtime, "context", lambda *_: {"interaction_owner": "monitor", "root_workflow_run_id": None})
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _value: "alive")
    registry.task.pump_wake = type("Wake", (), {"set": lambda _self: None})()
    result = await approval_runtime.respond_request(
        registry, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "reject", "monitor"
    )
    assert result["decided_by"] == "caller"
    assert result["responder"] == "monitor"


async def _none_async():
    return None


def test_concurrent_caller_and_monitor_reservations_have_one_winner(tmp_path: Path) -> None:
    directory = _state(tmp_path)
    row = _admit(directory)

    def reserve_as(role: str):
        try:
            return approvals.reserve(directory, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"], role)
        except approvals.ApprovalError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve_as, ("caller", "monitor")))
    winners = [result for result in results if not isinstance(result, Exception)]
    losers = [result for result in results if isinstance(result, approvals.ApprovalError)]
    assert len(winners) == 1 and winners[0]["responder"] in {"caller", "monitor"}
    assert len(losers) == 1 and losers[0].reason == "not_current"


def _reserve_process(directory: str, role: str, request_id: str, digest: str, generation: str, barrier, output) -> None:
    barrier.wait()
    try:
        result = approvals.reserve(Path(directory), "task-1", request_id, digest, 1, "approve", generation, role)
    except approvals.ApprovalError as error:
        output.put(("lost", error.reason))
    else:
        output.put(("won", result["responder"]))


@pytest.mark.parametrize("roles", [("caller", "monitor"), ("monitor", "caller")])
@pytest.mark.asyncio
async def test_two_process_responses_have_one_reservation_and_one_pump_delivery(repo, monkeypatch, roles) -> None:
    world = claude_world(repo, monkeypatch)
    row = await admit_claude(world, "Write", repo.repo / "action.txt")
    _record(world.directory)
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    output = context.Queue()
    processes = [context.Process(target=_reserve_process, args=(str(world.directory), role, row["request_id"], row["action_digest"], row["generation"], barrier, output)) for role in roles]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    results = [output.get(timeout=2) for _ in processes]
    assert sum(result[0] == "won" for result in results) == 1
    assert sum(result[0] == "lost" for result in results) == 1
    assert approvals.get(world.directory, "task-1", row["request_id"])["delivery"] == "reserved"
    await approval_runtime.pump(world.registry, world.task)
    await approval_runtime.pump(world.registry, world.task)
    assert len(world.writes) == 1


@pytest.mark.asyncio
async def test_monitor_approve_then_delivery_path_change_becomes_system_rejection(repo, monkeypatch) -> None:
    world = claude_world(repo, monkeypatch)
    world.registry._mutation_caller = lambda: _none_async()
    (repo.repo / "src").mkdir()
    row = await admit_claude(world, "Write", repo.repo / "src" / "action.txt")
    _record(world.directory)
    answered = await approval_runtime.respond_request(
        world.registry, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", "monitor"
    )
    assert answered["responder"] == "monitor"
    swap_to_symlink(repo.repo / "src", repo.repo / ".git" / "hooks")
    await approval_runtime.pump(world.registry, world.task)
    final = approvals.get(world.directory, "task-1", row["request_id"])
    assert final["decision"] == "approve"
    assert final["effective_decision"] == "reject"
    assert final["disposition"] == "system_rejected"
    assert wire_response(world.writes[0])["behavior"] == "deny"


@pytest.mark.asyncio
async def test_post_reservation_read_failure_is_uncertain_not_a_typed_unchanged_refusal(repo, monkeypatch) -> None:
    world = claude_world(repo, monkeypatch)
    world.registry._mutation_caller = lambda: _none_async()
    row = await admit_claude(world, "Write", repo.repo / "action.txt")
    _record(world.directory)
    real_get = approvals.get

    def fail_after_reservation(*_args, **_kwargs):
        raise approvals.ApprovalError("not_current", "post-reservation read failed")

    monkeypatch.setattr(approvals, "get", fail_after_reservation)
    try:
        result = await approval_runtime.respond_request(
            world.registry, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", "monitor"
        )
    except Exception as error:  # transport/read failure is allowed to be uncertain, never a stale unchanged claim
        assert not isinstance(error, approvals.ApprovalError)
        result = None
    finally:
        monkeypatch.setattr(approvals, "get", real_get)
    current = real_get(world.directory, "task-1", row["request_id"])
    assert current["decision"] == "approve" and current["responder"] == "monitor" and current["delivery"] == "reserved"
    if result is not None:
        assert result["decision"] == "approve" and result["responder"] == "monitor"
    await approval_runtime.pump(world.registry, world.task)
    await approval_runtime.pump(world.registry, world.task)
    assert len(world.writes) == 1


@pytest.mark.asyncio
async def test_worker_refusal_does_not_mutate_pending_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = _state(tmp_path)
    row = _admit(directory)
    _record(directory)
    registry = _Registry(directory)
    monkeypatch.setenv("HARFLOW_TASK_ID", "worker-task")
    with pytest.raises(approvals.ApprovalError) as raised:
        await approval_runtime.respond_request(registry, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve")
    assert raised.value.reason == "unauthorized"
    current = approvals.get(directory, "task-1", row["request_id"])
    assert current["decision"] is None and current["delivery"] == "pending"


@pytest.mark.asyncio
async def test_owner_loss_refuses_without_applying_decision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = _state(tmp_path)
    row = _admit(directory)
    _record(directory)
    registry = _Registry(directory)
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _value: "dead")
    with pytest.raises(approvals.ApprovalError) as raised:
        await approval_runtime.respond_request(registry, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve")
    assert raised.value.reason == "ownership_lost"
    current = approvals.get(directory, "task-1", row["request_id"])
    assert current["decision"] is None


@pytest.mark.asyncio
async def test_generation_change_refuses_without_applying_decision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = _state(tmp_path)
    row = _admit(directory)
    _record(directory)
    record = store.read(directory, "task-1")
    assert record is not None
    store.write(directory, replace(record, approval_generation="generation-2"))
    registry = _Registry(directory)
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _value: "alive")
    with pytest.raises(approvals.ApprovalError) as raised:
        await approval_runtime.respond_request(registry, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve")
    assert raised.value.reason == "ownership_lost"
    assert approvals.get(directory, "task-1", row["request_id"])["decision"] is None


@pytest.mark.asyncio
async def test_cleared_environment_with_detected_lineage_refuses_without_mutation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = _state(tmp_path)
    row = _admit(directory)
    _record(directory)
    registry = _Registry(directory, caller=object())
    monkeypatch.delenv("HARFLOW_TASK_ID", raising=False)
    with pytest.raises(approvals.ApprovalError) as raised:
        await approval_runtime.respond_request(registry, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve")
    assert raised.value.reason == "unauthorized"
    assert approvals.get(directory, "task-1", row["request_id"])["decision"] is None


@pytest.mark.asyncio
async def test_boolean_owner_revision_is_unsupported_before_reconciliation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = _state(tmp_path)
    row = _admit(directory)
    _record(directory)
    registry = _Registry(directory)
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _value: "dead")
    with pytest.raises(approvals.ApprovalError) as raised:
        await approval_runtime.respond_request(registry, "task-1", row["request_id"], row["action_digest"], True, "approve")
    assert raised.value.reason == "unsupported"
    assert approvals.get(directory, "task-1", row["request_id"])["decision"] is None


@pytest.mark.asyncio
async def test_undecidable_lineage_refuses_without_mutation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = _state(tmp_path)
    row = _admit(directory)
    _record(directory)
    registry = _Registry(directory)

    async def undecidable():
        error = RuntimeError("lineage unavailable")
        error.rule = "caller_undecidable"
        raise error

    monkeypatch.setattr(registry, "_mutation_caller", undecidable)
    with pytest.raises(approvals.ApprovalError) as raised:
        await approval_runtime.respond_request(registry, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve")
    assert raised.value.reason == "caller_undecidable"
    assert approvals.get(directory, "task-1", row["request_id"])["decision"] is None
