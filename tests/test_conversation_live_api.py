"""Public native conversation API through real TaskRegistry and stdio peer."""
import asyncio
from dataclasses import replace
import uuid

import pytest

from harflow_mcp import backends, conversation_api as api, conversation_live_owner as live
from harflow_mcp import state_ownership, store
from harflow_mcp.backends.conversation import ConversationLaunch
from harflow_mcp.backends.conversation_native import ACPConversationPort
from harflow_mcp.backends.conversation_qualification import fingerprint
from harflow_mcp.conversation_documents import ConversationError
from harflow_mcp.conversation_task_transport import ConversationAttachment
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry
from test_conversation_transport_tasks import Backend


@pytest.fixture
def tmp_path(tmp_path_factory):
    workspace = tmp_path_factory.mktemp('live-api')
    with state_ownership.state_session(resolve_layout(root=workspace / 'state')):
        (workspace / 'state' / 'tasks').mkdir()
        yield workspace


async def test_public_native_send_uses_original_process_and_replays_after_physical_close(tmp_path, monkeypatch):
    backend = Backend()
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    terminals, owners = asyncio.Queue(), []
    close_barrier = asyncio.Event()
    before_close_waiting = asyncio.Event()
    def factory(backend, original, policy):
        profile = dict(v=1, backend=backend.name, version='fixture', transport='acp', cwd=str(tmp_path),
            freedom=policy['freedom'], caller_approvals=False, mode='agent',
            initial_prompt=policy['prompt'], argv=original.argv, env=dict(original.env))
        port = ACPConversationPort(profile)
        launch = ConversationLaunch(replace(original, conversation_context=profile), port)
        def owner_factory(task):
            owner = live.for_task(registry, task, port, profile_fingerprint=fingerprint(profile))
            original_observe, original_close = owner.observe, owner.before_close
            async def observe(observation):
                await original_observe(observation)
                if observation.kind == 'response_terminal':
                    terminals.put_nowait(observation)
            async def before_close(previous):
                before_close_waiting.set()
                await close_barrier.wait()
                return await original_close(previous)
            owner.observe, owner.before_close = observe, before_close
            owners.append(owner)
            return owner
        return ConversationAttachment(launch, owner_factory, idle_timeout=.05, operation_timeout=3)
    registry = TaskRegistry(log_dir=tmp_path / 'state' / 'tasks', conversation_launch_factory=factory)
    task = await registry.start('initial assignment', tmp_path, backend=backend, freedom='read_only')
    try:
        await asyncio.wait_for(terminals.get(), 5)
        await asyncio.wait_for(before_close_waiting.wait(), 5)
        prepared = await api.prepare_task(registry, task.task_id)
        command = dict(contract=prepared['contract'], request_id=str(uuid.uuid4()),
            delivery='queue', text='next exact input', target=dict(kind='task', task_id=task.task_id),
            expected=prepared['expected'])
        receipt = await api.send_task(registry, command)
        assert receipt['status'] == 'queued'
        cancellation = dict(contract=prepared['contract'], request_id=str(uuid.uuid4()), operation='cancel',
            target=command['target'], expected=command['expected'], message_request_id=command['request_id'])
        assert (await api.control_task(registry, cancellation))['status'] == 'cancelled'
        priority = {**command, 'request_id': str(uuid.uuid4())}
        priority_receipt = await api.send_task(registry, priority)
        promotion = dict(contract=prepared['contract'], request_id=str(uuid.uuid4()), operation='promote_now',
            target=priority['target'], expected=priority['expected'], message_request_id=priority['request_id'])
        promoted = await api.control_task(registry, promotion)
        assert promoted['status'] == 'promoted'
        assert await api.control_task(registry, promotion) == promoted
        command, receipt = priority, priority_receipt
        assert len(registry._tasks) == 1
        close_barrier.set()
        second = await asyncio.wait_for(terminals.get(), 5)
        assert second.identity.generation == 2
        await asyncio.wait_for(task.done.wait(), 5)
        assert task.status == 'completed' and task.output_drained
        assert task.acc.summary == 'next exact input'
        assert task.conversation_responses_issued == 2
        assert len(registry._tasks) == 1
        assert not owners[0].ledger.read()['runtime_fault']
        replay = await api.send_task(registry, command)
        assert replay['message_id'] == receipt['message_id']
        assert replay['status'] == 'consumed'
        fresh = TaskRegistry(log_dir=registry.log_dir)
        assert (await api.send_task(fresh, command))['status'] == 'consumed'
        assert (await api.control_task(fresh, promotion)) == promoted
        assert (await api.control_task(fresh, cancellation))['status'] == 'cancelled'
        # Descriptor/readback and old receipts cannot qualify a new detached effect.
        another = {**command, 'request_id': str(uuid.uuid4()), 'text': 'no replay'}
        with pytest.raises(ConversationError, match='native_source_unqualified'):
            await api.send_task(fresh, another)
        assert store.read(registry.log_dir, task.task_id).conversation == task.conversation
        recovered = store.snapshot(registry.log_dir, store.read(registry.log_dir, task.task_id))
        assert recovered['summary'] == 'next exact input'
        assert recovered['is_error'] is False
    finally:
        close_barrier.set()
        if task.proc.returncode is None:
            task.proc.kill()
            await task.proc.wait()
        await asyncio.wait_for(asyncio.shield(task.monitor), 5)
