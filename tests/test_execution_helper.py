import asyncio

import pytest

from execution_helper import run_bounded, workflow_snapshot


@pytest.mark.asyncio
async def test_run_bounded_returns_result_and_leaves_no_task():
    before = set(asyncio.all_tasks())
    assert await run_bounded(asyncio.sleep(0, result="ok"), 1) == "ok"
    await asyncio.sleep(0)
    assert set(asyncio.all_tasks()) - before == set()


@pytest.mark.asyncio
async def test_run_bounded_preserves_execution_failure():
    before = set(asyncio.all_tasks())

    async def fail():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await run_bounded(fail(), 1)
    await asyncio.sleep(0)
    assert set(asyncio.all_tasks()) - before == set()


@pytest.mark.asyncio
async def test_timeout_diagnostic_runs_before_cancellation_and_cleanup_finishes():
    cancelled = False
    diagnostic_seen = []

    async def work():
        nonlocal cancelled
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled = True
            raise

    def diagnostic():
        diagnostic_seen.append(cancelled)
        return {"cancelled": cancelled}

    with pytest.raises(TimeoutError) as caught:
        await run_bounded(work(), 0.01, diagnostic=diagnostic)
    assert diagnostic_seen == [False]
    assert cancelled is True
    assert "cancelled" in "\n".join(caught.value.__notes__)


@pytest.mark.asyncio
async def test_run_bounded_timeout_keeps_diagnostic_failure_secondary():
    async def hang():
        await asyncio.Event().wait()

    with pytest.raises(TimeoutError) as caught:
        await run_bounded(hang(), 0.01, diagnostic=lambda: 1 / 0)
    assert "diagnostic_error" in "\n".join(caught.value.__notes__)


@pytest.mark.asyncio
async def test_run_bounded_timeout_survives_diagnostic_value_repr_failure():
    class Unprintable:
        def __repr__(self):
            raise RuntimeError("repr failed")

    async def hang():
        await asyncio.Event().wait()

    with pytest.raises(TimeoutError) as caught:
        await run_bounded(hang(), 0.01, diagnostic=lambda: Unprintable())
    assert "exceeded" in str(caught.value)


@pytest.mark.asyncio
async def test_run_bounded_timeout_keeps_execution_cleanup_error_as_note():
    async def fails_during_cleanup():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as exc:
            raise RuntimeError("cleanup execution failed") from exc

    with pytest.raises(TimeoutError) as caught:
        await run_bounded(fails_during_cleanup(), 0.01)
    assert any("cleanup execution failed" in note for note in caught.value.__notes__)


@pytest.mark.asyncio
async def test_run_bounded_preserves_caller_cancellation():
    before = set(asyncio.all_tasks())
    started = asyncio.Event()

    async def wait_forever():
        started.set()
        await asyncio.Event().wait()

    owner = asyncio.create_task(run_bounded(wait_forever(), 10))
    await started.wait()
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner
    await asyncio.sleep(0)
    assert set(asyncio.all_tasks()) - before == set()


@pytest.mark.asyncio
async def test_run_bounded_second_cancel_interrupts_uncooperative_first_cleanup():
    cancellations = 0
    before = set(asyncio.all_tasks())

    async def stubborn():
        nonlocal cancellations
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellations += 1
            if cancellations == 1:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancellations += 1
                    raise
            raise

    with pytest.raises(TimeoutError):
        await run_bounded(stubborn(), 0.01)
    assert cancellations == 2
    await asyncio.sleep(0)
    assert set(asyncio.all_tasks()) - before == set()


def test_workflow_snapshot_contains_bounded_state_and_counts():
    class Storage:
        def get_run(self, _run_id):
            return {
                "status": "running",
                "sequence": 7,
                "transitions": list(range(30)),
                "pending": [{"node_id": "node-1", "generation": 3}],
                "joins": {"join": {"arrived": ["a"]}},
                "activations": [
                    {"id": "old-active", "node_id": "old", "status": "running", "tasks": [{"task_id": "child-1", "status": "running"}]},
                    {"id": "old-failed", "node_id": "old-failed-node", "status": "failed", "tasks": [{"task_id": "uncertain-child", "status": "uncertain"}]},
                ] + [{"id": str(i), "node_id": "n", "status": "completed", "tasks": [{"task_id": f"child-{i}", "status": "completed"}]} for i in range(30)],
            }

    class Registry:
        calls = list(range(30))
        contexts = list(range(25))
        worker_counts = {"active": 2}

    snapshot = workflow_snapshot(Storage(), "run-1", Registry())
    assert snapshot["run_id"] == "run-1"
    assert snapshot["status"] == "running"
    assert snapshot["sequence"] == 7
    assert snapshot["transitions"]["items"][-1] == 29
    assert snapshot["pending_count"] == 1
    assert snapshot["pending"][0]["node_id"] == "node-1"
    assert snapshot["pending"][0]["generation"] == 3
    assert snapshot["join_count"] == 1
    assert snapshot["joins"][0]["arrived_count"] == 1
    assert snapshot["registry_calls"]["count"] == 30
    assert snapshot["registry_calls"]["items"][-1] == 29
    assert snapshot["contexts"]["count"] == 25
    assert snapshot["contexts"]["items"][-1] == 24
    assert snapshot["worker_counts"] == {"count": 1, "items": [("active", 2)]}
    assert snapshot["activation_count"] == 32
    assert snapshot["activation_status_counts"] == {"running": 1, "failed": 1, "completed": 30}
    assert snapshot["nonterminal_activation_count"] == 2
    assert len(snapshot["activations"]) == 20
    assert snapshot["activations"][0]["id"] == "old-active"
    assert snapshot["activations"][0]["task_count"] == 1
    assert snapshot["activations"][0]["tasks"] == [{"task_id": "child-1", "status": "running"}]
    assert snapshot["activations"][1]["id"] == "old-failed"
    assert snapshot["activations"][1]["tasks"] == [{"task_id": "uncertain-child", "status": "uncertain"}]


@pytest.mark.asyncio
async def test_run_bounded_rejects_non_finite_or_non_positive_budget():
    for budget in (0, -1, float("inf"), float("nan")):
        awaitable = asyncio.sleep(0)
        with pytest.raises(ValueError, match="budget"):
            await run_bounded(awaitable, budget)
        awaitable.close()
