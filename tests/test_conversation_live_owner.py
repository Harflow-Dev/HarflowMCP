"""Durable owner plus native protocol driver; unpaid deterministic frame barriers."""
import asyncio
import json
import sys
import uuid

import pytest

from harflow_mcp import conversation_live_owner as live, state_ownership
from harflow_mcp.backends.conversation_native import ACPConversationPort
from harflow_mcp.conversation_documents import ConversationError
from harflow_mcp.conversation_transport import ConversationDriver, ConversationInput
from harflow_mcp.state_layout import resolve_layout


class Writer:
    def __init__(self):
        self.frames = asyncio.Queue()
        self.closed = False

    def write(self, frame):
        self.frames.put_nowait(json.loads(frame))

    async def drain(self):
        pass

    def close(self):
        self.closed = True


@pytest.fixture
def layout(tmp_path):
    layout = resolve_layout(root=tmp_path / 'state')
    with state_ownership.state_session(layout):
        yield layout


async def start(layout, *, caller_approvals=False):
    task_id = str(uuid.uuid4())
    port = ACPConversationPort(dict(backend='cursor', version='2026.10.22-27dcf5f', cwd='/repo', mode='agent', caller_approvals=caller_approvals))
    owner = live.ConversationOwner(layout, task_id, 'initial assignment', port)
    writer = Writer()
    driver = ConversationDriver(port, writer, owner,
        initial_input=ConversationInput(task_id, 'initial assignment'),
        idle_timeout=.05, operation_timeout=2, control_interval=.01)
    job = driver.start()
    init = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe(dict(id=init['id'], result=dict(protocolVersion=1)))
    new = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe(dict(id=new['id'], result=dict(sessionId='native-context',
        modes=dict(currentModeId='agent', availableModes=[dict(id='agent')]))))
    first = await asyncio.wait_for(writer.frames.get(), 1)
    return owner, driver, writer, job, first


async def send(owner, text, mode='queue'):
    state = await asyncio.to_thread(owner.ledger.read)
    return await asyncio.to_thread(live.admit, owner, text, request_id=str(uuid.uuid4()),
        mode=mode, expected={key: state[key] for key in (
            'conversation_id', 'head_task_id', 'generation', 'turn_id', 'ledger_epoch')})


async def test_live_fifo_uses_same_task_context_and_records_consumption_after_terminal(layout):
    owner, driver, writer, job, first = await start(layout)
    try:
        await send(owner, 'second input')
        await send(owner, 'third input')
        assert writer.frames.empty()
        driver.observe(dict(id=first['id'], result=dict(stopReason='end_turn')))
        second = await asyncio.wait_for(writer.frames.get(), 1)
        assert second['params']['prompt'][0]['text'] == 'second input'
        state = owner.ledger.read()
        assert state['generation'] == 2 and state['head_task_id'] == owner.task_id
        assert [m['status'] for m in state['messages']] == ['claimed', 'queued']
        driver.observe(dict(id=second['id'], result=dict(stopReason='end_turn')))
        third = await asyncio.wait_for(writer.frames.get(), 1)
        assert third['params']['prompt'][0]['text'] == 'third input'
        assert owner.ledger.read()['messages'][0]['status'] == 'consumed'
        driver.observe(dict(id=third['id'], result=dict(stopReason='end_turn')))
        await asyncio.wait_for(job, 2)
        state = owner.ledger.read()
        assert [m['status'] for m in state['messages']] == ['consumed', 'consumed']
        assert state['closed'] and writer.closed
        with pytest.raises(ConversationError, match='target_stale'):
            await send(owner, 'too late')
    finally:
        await driver.close()


async def test_durable_now_is_polled_without_in_memory_wake_and_beats_older_queue(layout):
    owner, driver, writer, job, first = await start(layout)
    try:
        detached = live.ledger_binding(layout, owner.task_id)
        await send(detached, 'older queued input')
        state = detached.ledger.read()
        expected = {key: state[key] for key in ('conversation_id', 'head_task_id', 'generation', 'turn_id', 'ledger_epoch')}
        script = '''import json,sys,uuid
from harflow_mcp import conversation_live_owner as live
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session
layout=resolve_layout(root=sys.argv[1])
with state_session(layout):
    binding=live.ledger_binding(layout,sys.argv[2])
    live.admit(binding,'selected now',request_id=str(uuid.uuid4()),mode='now',expected=json.loads(sys.argv[3]))
'''
        child = await asyncio.create_subprocess_exec(sys.executable, '-c', script, str(layout.root),
            owner.task_id, json.dumps(expected), stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            output, error = await asyncio.wait_for(child.communicate(), 5)
        finally:
            if child.returncode is None:
                child.kill()
                await asyncio.wait_for(child.wait(), 5)
        assert child.returncode == 0, error.decode()
        with pytest.raises(ConversationError, match='now_busy'):
            await send(owner, 'competing now', 'now')
        # Deliberately no driver.wake(): the detached owner observes the durable ledger.
        cancel = await asyncio.wait_for(writer.frames.get(), 1)
        assert cancel['method'] == 'session/cancel'
        selected = next(m for m in owner.ledger.read()['messages'] if m['delivery'] == 'now')
        assert selected['interrupt_intent']['session_id'] == 'native-context'
        assert selected['interrupt_intent']['generation'] == 1
        assert selected['interrupt_intent']['logical_turn'] == owner.task_id
        assert writer.frames.empty()
        driver.observe(dict(id=first['id'], result=dict(stopReason='cancelled')))
        now = await asyncio.wait_for(writer.frames.get(), 1)
        assert now['params']['prompt'][0]['text'] == 'selected now'
        driver.observe(dict(id=now['id'], result=dict(stopReason='end_turn')))
        older = await asyncio.wait_for(writer.frames.get(), 1)
        assert older['params']['prompt'][0]['text'] == 'older queued input'
        driver.observe(dict(id=older['id'], result=dict(stopReason='end_turn')))
        await asyncio.wait_for(job, 2)
        assert all(m['status'] == 'consumed' for m in owner.ledger.read()['messages'])
    finally:
        await driver.close()


def test_second_owner_cannot_replay_initial_issue_after_crash(layout):
    task_id = str(uuid.uuid4())
    profile = dict(backend='cursor', version='2026.10.22-27dcf5f', cwd='/repo', mode='agent')
    live.ConversationOwner(layout, task_id, 'initial', ACPConversationPort(profile))
    with pytest.raises(ConversationError, match='conversation_owner_exists'):
        live.ConversationOwner(layout, task_id, 'initial', ACPConversationPort(profile))


async def test_known_busy_tool_refuses_now_but_queue_waits_for_response_boundary(layout):
    owner, driver, writer, job, first = await start(layout)
    try:
        tool_seen, tool_finished = asyncio.Event(), asyncio.Event()
        original_observe = owner.observe
        async def observe(observation):
            await original_observe(observation)
            if observation.kind == 'tool_pending': tool_seen.set()
            if observation.kind == 'tool_terminal': tool_finished.set()
        owner.observe = observe
        driver.observe(dict(method='session/update', params=dict(sessionId='native-context',
            update=dict(sessionUpdate='tool_call', toolCallId='owned-tool', status='running'))))
        await asyncio.wait_for(tool_seen.wait(), 1)
        with pytest.raises(ConversationError, match='tool_interruption_unqualified'):
            await send(owner, 'must not interrupt busy tool', 'now')
        assert not owner.ledger.read()['messages'] and writer.frames.empty()
        await send(owner, 'after normal response', 'queue')
        driver.observe(dict(method='session/update', params=dict(sessionId='native-context',
            update=dict(sessionUpdate='tool_call_update', toolCallId='owned-tool', status='completed'))))
        await asyncio.wait_for(tool_finished.wait(), 1)
        assert writer.frames.empty() and not owner.ledger.read()['boundary']
        driver.observe(dict(id=first['id'], result=dict(stopReason='end_turn')))
        resumed = await asyncio.wait_for(writer.frames.get(), 1)
        assert resumed['method'] == 'session/prompt'
        assert resumed['params']['prompt'][0]['text'] == 'after normal response'
        driver.observe(dict(id=resumed['id'], result=dict(stopReason='end_turn')))
        await asyncio.wait_for(job, 2)
        assert owner.ledger.read()['messages'][0]['status'] == 'consumed'
    finally:
        await driver.close()


async def test_known_pending_approval_refuses_now_without_cancel_or_grant(layout):
    owner, driver, writer, job, first = await start(layout, caller_approvals=True)
    try:
        permission_seen = asyncio.Event()
        original_observe = owner.observe
        async def observe(observation):
            await original_observe(observation)
            if observation.kind == 'approval_pending': permission_seen.set()
        owner.observe = observe
        driver.observe(dict(id='old-permission', method='session/request_permission',
            params=dict(sessionId='native-context', options=[dict(optionId='allow-once', kind='allow_once')])))
        await asyncio.wait_for(permission_seen.wait(), 1)
        with pytest.raises(ConversationError, match='tool_interruption_unqualified'):
            await send(live.ledger_binding(layout, owner.task_id), 'must not interrupt approval', 'now')
        assert not owner.ledger.read()['messages'] and writer.frames.empty()
        await send(owner, 'new response', 'queue')
        # An explicit private approval denial is separate from Now: no grant and
        # no native session/cancel is sent during the pending permission phase.
        driver.port._cancel_approvals()
        await driver._flush()
        denied = await asyncio.wait_for(writer.frames.get(), 1)
        assert denied == dict(jsonrpc='2.0', id='old-permission', result=dict(outcome=dict(outcome='cancelled')))
        driver.observe(dict(id=first['id'], result=dict(stopReason='end_turn')))
        next_response = await asyncio.wait_for(writer.frames.get(), 1)
        assert next_response['method'] == 'session/prompt'
        assert next_response['params']['prompt'][0]['text'] == 'new response'
        driver.observe(dict(id=next_response['id'], result=dict(stopReason='end_turn')))
        await asyncio.wait_for(job, 2)
        assert owner.ledger.read()['messages'][0]['status'] == 'consumed'
    finally:
        await driver.close()


async def test_native_eof_before_response_pins_fault_even_without_queued_messages(layout):
    from harflow_mcp.conversation_delivery import protects_retention
    owner, driver, writer, job, first = await start(layout)
    driver.eof()
    await asyncio.wait_for(job, 2)
    state = owner.ledger.read()
    assert state['runtime_fault'] == dict(code='native_runtime_unknown', generation=1, turn_id=owner.task_id)
    assert protects_retention(state)
    assert not state['boundary']
    with pytest.raises(ConversationError, match='conversation_unknown'):
        await send(live.ledger_binding(layout, owner.task_id), 'no automatic replay')


async def test_cancel_wins_before_interrupt_bytes_without_faulting_active_response(layout):
    from harflow_mcp import conversation_delivery as delivery
    owner, driver, writer, job, first = await start(layout)
    stale_seen = asyncio.Event()
    original_observe = owner.observe
    async def observe(observation):
        if observation.kind == 'interrupt_intent':
            request_id = observation.payload['request_id']
            await asyncio.to_thread(owner.ledger.update, lambda state: (delivery.cancel(state, request_id), None))
        await original_observe(observation)
        if observation.kind == 'interrupt_stale':
            stale_seen.set()
    owner.observe = observe
    try:
        await send(owner, 'withdraw before native interrupt', 'now')
        await asyncio.wait_for(stale_seen.wait(), 1)
        assert writer.frames.empty()
        assert not driver.port.interrupt_requested
        assert owner.ledger.read()['runtime_fault'] is None
        driver.observe(dict(id=first['id'], result=dict(stopReason='end_turn')))
        await asyncio.wait_for(job, 2)
        assert owner.ledger.read()['messages'][0]['status'] == 'cancelled'
    finally:
        await driver.close()
