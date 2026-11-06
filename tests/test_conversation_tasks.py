"""Conversation delivery through the real registry and physical settlement monitor."""
import asyncio

import pytest

from harflow_mcp import backends, state_ownership
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry
from conversation_fixtures import ConversationProvider


@pytest.fixture
def tmp_path(tmp_path_factory):
    workspace = tmp_path_factory.mktemp("conversation")
    with state_ownership.state_session(resolve_layout(root=workspace / "state")):
        (workspace / "state" / "tasks").mkdir()
        yield workspace


async def test_response_terminal_does_not_publish_task_done_before_physical_exit(tmp_path, monkeypatch):
    provider = await ConversationProvider(tmp_path / "provider.sock").open()
    monkeypatch.setitem(backends.BACKENDS, provider.name, provider)
    registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks")
    task = None
    try:
        task = await registry.start("original assignment", tmp_path, backend=provider, freedom="read_only")
        turn = await provider.next_turn()
        await turn.result("response finished")
        assert task.proc.returncode is None
        assert not task.done.is_set()
        assert task.status == "running"
        await turn.settle()
        await asyncio.wait_for(task.done.wait(), 10)
        assert task.exit_code == 0
        assert task.output_drained
        assert task.status == "completed"
    finally:
        await provider.close()
        if task is not None and task.proc.returncode is None:
            task.proc.kill()
            await asyncio.wait_for(task.proc.wait(), 5)
        if task is not None and task.monitor is not None:
            await asyncio.wait_for(asyncio.shield(task.monitor), 10)


async def test_same_session_resume_preserves_context_identity_without_fresh_start(tmp_path, monkeypatch):
    provider = await ConversationProvider(tmp_path / "provider.sock").open()
    monkeypatch.setitem(backends.BACKENDS, provider.name, provider)
    registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks")
    started = []
    try:
        first = await registry.start("remember the assignment", tmp_path, backend=provider, freedom="read_only")
        started.append(first)
        initial = await provider.next_turn()
        await initial.result("remembered")
        await initial.settle()
        await asyncio.wait_for(first.done.wait(), 10)
        resumed = await registry.resume(first, "the next message")
        started.append(resumed)
        next_turn = await provider.next_turn()
        assert next_turn.request == {"prompt": "the next message", "session_id": first.session_id, "resumed": True}
        assert resumed.session_id == first.session_id
        assert resumed.task_id != first.task_id
        await next_turn.result("same context")
        await next_turn.settle()
        await asyncio.wait_for(resumed.done.wait(), 10)
        assert resumed.status == "completed"
        assert [request["resumed"] for request in provider.history] == [False, True]
    finally:
        await provider.close()
        for task in started:
            if task.proc.returncode is None:
                task.proc.kill()
                await asyncio.wait_for(task.proc.wait(), 5)
            if task.monitor is not None:
                await asyncio.wait_for(asyncio.shield(task.monitor), 10)


@pytest.fixture
async def conversation(tmp_path, monkeypatch):
    provider = await ConversationProvider(tmp_path / "provider.sock").open()
    monkeypatch.setitem(backends.BACKENDS, provider.name, provider)
    registry = TaskRegistry(log_dir=tmp_path / "state" / "tasks")
    try:
        yield registry, provider
    finally:
        await provider.close()
        for task in list(registry._tasks.values()):
            if task.proc is not None and task.proc.returncode is None:
                task.proc.kill()
                await asyncio.wait_for(task.proc.wait(), 5)
            if task.monitor is not None:
                await asyncio.wait_for(asyncio.shield(task.monitor), 10)
        jobs = list(getattr(registry, '_continuations', ()))
        if jobs:
            await asyncio.wait_for(asyncio.gather(*jobs), 10)


async def queue_message(registry, task_id, text, request_id=None, expected=None):
    import uuid
    from harflow_mcp import conversation_runtime as runtime
    state = expected if expected is not None else await asyncio.to_thread(runtime.enroll, registry, task_id)
    return await asyncio.to_thread(runtime.admit, registry, task_id, text, request_id=request_id or str(uuid.uuid4()),
        expected_epoch=state['ledger_epoch'], expected_head_task_id=state['head_task_id'],
        expected_generation=state['generation'], expected_turn_id=state['turn_id'])


async def finish_turn(registry, task, turn, text):
    from harflow_mcp import conversation_runtime as runtime
    await turn.result(text)
    await turn.settle()
    await asyncio.wait_for(task.done.wait(), 10)
    await asyncio.wait_for(asyncio.shield(task.monitor), 10)
    jobs = list(getattr(registry, "_continuations", ()))
    if jobs:
        await asyncio.wait_for(asyncio.gather(*jobs), 10)
    head = runtime.ledger_for(registry, task).read()["head_task_id"]
    return registry.get(head) if head != task.task_id else None


async def test_queue_drains_one_fifo_message_per_response_and_resolves_late_head(conversation, tmp_path):
    from harflow_mcp import conversation_runtime as runtime
    registry, provider = conversation
    first = await registry.start("assignment", tmp_path, backend=provider, freedom="read_only")
    first_turn = await provider.next_turn()
    queued = await queue_message(registry, first.task_id, "first queued message")
    await queue_message(registry, first.task_id, "second queued message")
    await first_turn.result("initial response")
    assert not first.done.is_set()
    assert provider.calls.empty()
    await first_turn.settle()
    second_turn = await provider.next_turn()
    ledger = runtime.ledger_for(registry, first)
    state = ledger.read()
    second = registry.get(state['head_task_id'])
    assert second is not None
    assert second_turn.request == dict(prompt="first queued message", session_id=first.session_id, resumed=True)
    assert state['messages'][0]['status'] == 'claimed'  # Spawn is not consumption.
    assert state['messages'][1]['status'] == 'queued'
    late = await queue_message(registry, first.task_id, "arrived during first drain")
    assert late['resolved_task_id'] == second.task_id
    assert late['task_id'] == first.task_id
    replay = await queue_message(registry, first.task_id, "first queued message", queued['message_id'], expected=queued)
    assert replay['message_id'] == queued['message_id']
    assert len(ledger.read()['messages']) == 3
    third = await finish_turn(registry, second, second_turn, "first revised response")
    third_turn = await provider.next_turn()
    assert third_turn.request['prompt'] == "second queued message"
    fourth = await finish_turn(registry, third, third_turn, "second revised response")
    fourth_turn = await provider.next_turn()
    assert fourth_turn.request['prompt'] == "arrived during first drain"
    assert await finish_turn(registry, fourth, fourth_turn, "final revised response") is None
    final = ledger.read()
    assert [m['status'] for m in final['messages']] == ['consumed'] * 3
    assert all(request['session_id'] == first.session_id for request in provider.history)
    assert [request['resumed'] for request in provider.history] == [False, True, True, True]


async def test_cancelled_queue_item_is_never_issued(conversation, tmp_path):
    from harflow_mcp import conversation_runtime as runtime
    registry, provider = conversation
    task = await registry.start("assignment", tmp_path, backend=provider, freedom="read_only")
    turn = await provider.next_turn()
    cancelled = await queue_message(registry, task.task_id, "withdraw me")
    runtime.cancel(registry, task.task_id, cancelled['message_id'])
    await queue_message(registry, task.task_id, "deliver me")
    successor = await finish_turn(registry, task, turn, "initial")
    next_turn = await provider.next_turn()
    assert next_turn.request['prompt'] == 'deliver me'
    assert await finish_turn(registry, successor, next_turn, 'done') is None
    assert [m['status'] for m in runtime.ledger_for(registry, task).read()['messages']] == ['cancelled', 'consumed']


@pytest.mark.parametrize('profile', ['unqualified', 'capped'])
async def test_unqualified_profiles_refuse_before_enrollment_or_admission(conversation, tmp_path, monkeypatch, profile):
    import uuid
    from harflow_mcp import conversation_runtime as runtime
    from harflow_mcp.conversation_documents import ConversationError
    registry, provider = conversation
    task = await registry.start("assignment", tmp_path, backend=provider, freedom="read_only",
        max_turns=2 if profile == 'capped' else None)
    turn = await provider.next_turn()
    if profile == 'unqualified':
        monkeypatch.setattr(provider, 'conversation_resume_supported', None)
    with pytest.raises(ConversationError, match='queue_unsupported' if profile == 'unqualified' else 'queue_cap_unsupported'):
        runtime.admit(registry, task.task_id, 'unsupported', request_id=str(uuid.uuid4()), expected_epoch=0)
    assert not (registry.state_layout.root / 'conversations').exists()
    await turn.result('done')
    await turn.settle()
    await asyncio.wait_for(task.done.wait(), 10)


async def test_lost_launch_ack_protects_exact_destination_without_replay(conversation, tmp_path, monkeypatch):
    from harflow_mcp import conversation_runtime as runtime
    registry, provider = conversation
    source = await registry.start('assignment', tmp_path, backend=provider, freedom='read_only')
    first_turn = await provider.next_turn()
    await queue_message(registry, source.task_id, 'issued exactly once')
    real_resume = registry.resume
    async def lose_ack(*args, **kwargs):
        await real_resume(*args, **kwargs)
        raise RuntimeError('simulated lost acknowledgement after actual spawn')
    monkeypatch.setattr(registry, 'resume', lose_ack)
    destination = await finish_turn(registry, source, first_turn, 'original final')
    second_turn = await provider.next_turn()
    ledger = runtime.ledger_for(registry, source)
    assert ledger.read()['messages'][0]['status'] == 'unknown'
    assert await runtime.reconcile(registry) == []
    assert len(provider.history) == 2
    monkeypatch.setattr(registry, 'resume', real_resume)
    assert await finish_turn(registry, destination, second_turn, 'correlated response') is None
    assert ledger.read()['messages'][0]['status'] == 'consumed'
    assert await runtime.reconcile(registry) == []
    assert len(provider.history) == 2


async def test_restart_reconciles_settled_source_before_boundary_hook(conversation, tmp_path, monkeypatch):
    from harflow_mcp import conversation_runtime as runtime
    registry, provider = conversation
    source = await registry.start('assignment', tmp_path, backend=provider, freedom='read_only')
    first_turn = await provider.next_turn()
    await queue_message(registry, source.task_id, 'survives owner restart')
    with monkeypatch.context() as patch:
        patch.setattr(runtime, 'schedule_settled', lambda *args: False)
        await first_turn.result('source response')
        await first_turn.settle()
        await asyncio.wait_for(source.monitor, 10)
    fresh = TaskRegistry(log_dir=registry.log_dir)
    started = await runtime.reconcile(fresh)
    assert len(started) == 1
    resumed = fresh.get(started[0])
    registry._tasks[resumed.task_id] = resumed  # Share fixture cleanup ownership.
    next_turn = await provider.next_turn()
    assert next_turn.request == dict(prompt='survives owner restart', session_id=source.session_id, resumed=True)
    assert await finish_turn(fresh, resumed, next_turn, 'recovered response') is None
    assert await runtime.reconcile(fresh) == []


async def test_legacy_followups_are_never_automatically_consumed(conversation, tmp_path):
    from harflow_mcp import followups
    registry, provider = conversation
    source = await registry.start('assignment', tmp_path, backend=provider, freedom='read_only')
    turn = await provider.next_turn()
    await asyncio.to_thread(followups.send, registry.log_dir, source.task_id, 'legacy manual followup', allow_live=True, by=None)
    await turn.result('done')
    await turn.settle()
    await asyncio.wait_for(source.monitor, 10)
    jobs = list(getattr(registry, '_continuations', ()))
    if jobs:
        await asyncio.wait_for(asyncio.gather(*jobs), 10)
    assert len(provider.history) == 1
    assert followups.view(registry.log_dir, source.task_id)['items'][0]['status'] == 'queued'


async def test_public_prepare_send_replays_exact_request_after_head_advances(conversation, tmp_path):
    import uuid
    from harflow_mcp import conversation_api as api
    registry, provider = conversation
    source = await registry.start('assignment', tmp_path, backend=provider, freedom='read_only')
    initial = await provider.next_turn()
    prepared = await api.prepare_task(registry, source.task_id)
    command = dict(contract=prepared['contract'], request_id=str(uuid.uuid4()),
        delivery='queue', text='  preserve  caller\tprose\n',
        target=dict(kind='task', task_id=source.task_id), expected=prepared['expected'])
    receipt = await api.send_task(registry, command)
    assert receipt['status'] == 'queued'
    successor = await finish_turn(registry, source, initial, 'initial response')
    resumed = await provider.next_turn()
    assert resumed.request['prompt'] == command['text']
    replay = await api.send_task(registry, command)
    assert replay['message_id'] == receipt['message_id']
    assert replay['status'] == 'claimed'
    assert len(provider.history) == 2
    assert await finish_turn(registry, successor, resumed, 'final response') is None


async def test_public_queue_to_already_observed_idle_source_starts_resume(conversation, tmp_path):
    import uuid
    from harflow_mcp import conversation_api as api
    registry, provider = conversation
    source = await registry.start('assignment', tmp_path, backend=provider, freedom='read_only')
    turn = await provider.next_turn()
    await turn.result('initial settled response')
    await turn.settle()
    await asyncio.wait_for(source.monitor, 10)
    prepared = await api.prepare_task(registry, source.task_id)
    command = dict(contract=prepared['contract'], request_id=str(uuid.uuid4()), delivery='queue',
        text='resume the idle context', target=dict(kind='task', task_id=source.task_id), expected=prepared['expected'])
    await api.send_task(registry, command)
    next_turn = await provider.next_turn()
    from harflow_mcp import conversation_runtime as runtime
    successor = registry.get(runtime.ledger_for(registry, source).read()['head_task_id'])
    assert next_turn.request == dict(prompt='resume the idle context', session_id=source.session_id, resumed=True)
    assert await finish_turn(registry, successor, next_turn, 'done') is None
