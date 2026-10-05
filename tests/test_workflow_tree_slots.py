"""Saturated tree harness scheduling and interruption retain every permit."""
import asyncio

import pytest

from polybridge.workflow_invocation import TreeSlots
from polybridge.workflows import DispatchNotStarted


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
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    if not after_handoff:
        slots.release()
    await asyncio.wait_for(slots.acquire(lambda: True), 1)
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
