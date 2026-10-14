import asyncio
from types import SimpleNamespace

import pytest

from polybridge.detached import _wait_started, _release_started


@pytest.mark.asyncio
async def test_wait_started_owns_successors_created_by_queued_continuation():
    registry = SimpleNamespace(_continuations=set(), tasks=[], list=lambda: registry.tasks)
    done = asyncio.Event()
    async def continuation():
        successor = SimpleNamespace(done=done, finished=False)
        registry.tasks.append(successor)
        await asyncio.sleep(0)
    launch = asyncio.create_task(continuation())
    registry._continuations.add(launch)
    launch.add_done_callback(registry._continuations.discard)
    waiter = asyncio.create_task(_wait_started(registry))
    await asyncio.sleep(0.02)
    assert not waiter.done()
    registry.tasks[0].finished = True
    done.set()
    await asyncio.wait_for(waiter, 1)


@pytest.mark.asyncio
async def test_release_stops_pending_continuation_before_returning():
    continuation = asyncio.create_task(asyncio.sleep(100))
    registry = SimpleNamespace(_continuations={continuation}, list=lambda: [])
    await _release_started(registry)
    assert continuation.cancelled()
