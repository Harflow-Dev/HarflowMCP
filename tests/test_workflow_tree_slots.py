"""Saturated tree harness scheduling and interruption retain every permit."""
import asyncio

import pytest

from harflow_mcp.workflow_invocation import TreeSlots
from harflow_mcp.workflows import DispatchNotStarted


async def test_multiple_waiters_receive_permits_once_in_order():
    slots = TreeSlots(1)
    await slots.acquire(lambda: True)
    completed = []
    active = 0
    maximum = 0

    async def worker(index):
        nonlocal active, maximum
        await slots.acquire(lambda: True)
        active += 1
        maximum = max(maximum, active)
        completed.append(index)
        await asyncio.sleep(0)
        active -= 1
        slots.release()

    workers = [asyncio.create_task(worker(index)) for index in range(5)]
    await asyncio.sleep(0)
    slots.release()
    await asyncio.wait_for(asyncio.gather(*workers), 2)
    assert completed == list(range(5))
    assert maximum == 1
    assert slots.free == 1 and not slots.waiters


@pytest.mark.parametrize('after_handoff', [False, True])
async def test_cancelled_waiter_returns_exactly_one_permit(after_handoff):
    slots = TreeSlots(1)
    await slots.acquire(lambda: True)
    waiter = asyncio.create_task(slots.acquire(lambda: True))
    await asyncio.sleep(0)
    if after_handoff:
        slots.release()
    cancellation_requested = waiter.cancel()
    assert cancellation_requested is True
    assert waiter.cancelling() >= 1
    completed = False
    try:
        await waiter
    except asyncio.CancelledError:
        # If cancellation is delivered before the handoff resumes the waiter,
        # TreeSlots must return the handed-off permit in its cleanup path.
        pass
    else:
        # Python 3.11 can resume the already-completed shield before delivering
        # cancellation. In that legitimate race the waiter owns the permit.
        completed = True
        slots.release()
    if not after_handoff and not completed:
        slots.release()
    await asyncio.wait_for(slots.acquire(lambda: True), 1)
    assert slots.free == 0 and not slots.waiters
    slots.release()
    assert slots.free == 1


async def test_handoff_completes_waiter_and_transfers_exactly_one_permit():
    slots = TreeSlots(1)
    await slots.acquire(lambda: True)
    waiter = asyncio.create_task(slots.acquire(lambda: True))
    await asyncio.sleep(0)
    slots.release()
    assert await waiter is None
    assert slots.free == 0 and not slots.waiters
    slots.release()
    assert slots.free == 1


@pytest.mark.parametrize('after_handoff', [False, True])
async def test_suspension_while_waiting_returns_without_leaking_permit(after_handoff):
    slots = TreeSlots(1)
    running = True
    await slots.acquire(lambda: True)
    waiter = asyncio.create_task(slots.acquire(lambda: running))
    await asyncio.sleep(0)
    running = False
    if after_handoff:
        slots.release()
    with pytest.raises(DispatchNotStarted):
        await asyncio.wait_for(waiter, 1)
    if not after_handoff:
        slots.release()
    await asyncio.wait_for(slots.acquire(lambda: True), 1)
    slots.release()
    assert slots.free == 1 and not slots.waiters
