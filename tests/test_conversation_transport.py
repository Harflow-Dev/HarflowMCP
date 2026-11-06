"""Scripted transport barriers; no real subprocesses or arbitrary sleeps."""
import asyncio
import json

import pytest

from harflow_mcp.backends.conversation_native import ACPConversationPort
from harflow_mcp.conversation_transport import ConversationDriver, ConversationInput


class Owner:
    def __init__(self):
        self.events = []
        self.queued = []
        self.issued = asyncio.Queue()
        self.terminal = asyncio.Queue()
        self.sealed = False
        self.claims = []

    async def poll_control(self, identity):
        return None

    async def before_issue(self, candidate, previous):
        self.claims.append(candidate.logical_turn)

    async def next_input(self, previous):
        return self.queued.pop(0) if self.queued else None

    async def before_close(self, previous):
        if self.queued:
            return self.queued.pop(0)
        self.sealed = True
        return None

    async def observe(self, observation):
        self.events.append(observation)
        if observation.kind == "input_issued":
            self.issued.put_nowait(observation)
        if observation.kind == "response_terminal":
            self.terminal.put_nowait(observation)


class Writer:
    def __init__(self):
        self.frames = asyncio.Queue()
        self.closed = False
        self.fail = False

    def write(self, data):
        if self.fail:
            raise OSError("uncertain partial write")
        self.frames.put_nowait(json.loads(data))

    async def drain(self):
        return None

    def close(self):
        self.closed = True


async def ready_driver():
    port = ACPConversationPort({"backend": "cursor", "version": "2026.10.22-27dcf5f", "cwd": "/repo", "mode": "agent"})
    owner, writer = Owner(), Writer()
    driver = ConversationDriver(port, writer, owner, initial_input=ConversationInput("first", "one"), idle_timeout=.01, operation_timeout=2)
    task = driver.start()
    init = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe({"id": init["id"], "result": {"protocolVersion": 1}})
    new = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe({"id": new["id"], "result": {"sessionId": "session-a", "modes": {"currentModeId": "agent", "availableModes": [{"id": "agent"}]}}})
    first = await asyncio.wait_for(writer.frames.get(), 1)
    await asyncio.wait_for(owner.issued.get(), 1)
    return driver, owner, writer, task, first


@pytest.mark.asyncio
async def test_queue_response_boundary_fifo_and_finite_idle_close():
    driver, owner, writer, task, first = await ready_driver()
    owner.queued.extend([ConversationInput("second", "two"), ConversationInput("third", "three")])
    driver.wake()
    # No input may be written while the original prompt RPC is outstanding.
    assert writer.frames.empty()
    driver.observe({"id": first["id"], "result": {"stopReason": "end_turn"}})
    second = await asyncio.wait_for(writer.frames.get(), 1)
    assert second["params"]["prompt"][0]["text"] == "two"
    driver.observe({"id": second["id"], "result": {"stopReason": "end_turn"}})
    third = await asyncio.wait_for(writer.frames.get(), 1)
    assert third["params"]["prompt"][0]["text"] == "three"
    driver.observe({"id": third["id"], "result": {"stopReason": "end_turn"}})
    await asyncio.wait_for(task, 1)
    assert writer.closed and owner.sealed and driver.closed
    assert owner.claims == ["first", "second", "third"]
    assert not any(event.kind == "input_consumed" for event in owner.events)
    assert owner.events[-1].payload["process_settlement"] == "unknown"


@pytest.mark.asyncio
async def test_interrupt_writes_while_prompt_outstanding_and_waits_terminal():
    driver, owner, writer, task, first = await ready_driver()
    owner.queued.append(ConversationInput("selected-now", "now"))
    await driver.interrupt(driver.port.identity)
    interrupt = await asyncio.wait_for(writer.frames.get(), 1)
    assert interrupt["method"] == "session/cancel"
    assert writer.frames.empty()
    driver.observe({"id": first["id"], "result": {"stopReason": "cancelled"}})
    selected = await asyncio.wait_for(writer.frames.get(), 1)
    assert selected["params"]["prompt"][0]["text"] == "now"
    driver.observe({"id": selected["id"], "result": {"stopReason": "end_turn"}})
    await asyncio.wait_for(task, 1)
    kinds = [e.kind for e in owner.events]
    assert kinds.index("interrupt_intent") < kinds.index("response_terminal")


@pytest.mark.asyncio
async def test_partial_write_never_replays_and_marks_unknown():
    driver, owner, writer, task, first = await ready_driver()
    writer.fail = True
    owner.queued.append(ConversationInput("second", "two"))
    driver.observe({"id": first["id"], "result": {"stopReason": "end_turn"}})
    await asyncio.wait_for(task, 1)
    assert driver.port.unknown
    assert owner.claims == ["first", "second"]
    assert not driver.port.outgoing_frames()
    assert any(e.kind == "unknown" for e in owner.events)
    assert writer.closed


@pytest.mark.asyncio
async def test_durable_control_poll_interrupts_without_process_local_wake():
    from harflow_mcp.conversation_transport import ConversationInterrupt
    driver, owner, writer, task, first = await ready_driver()
    intent = ConversationInterrupt(driver.port.identity, 'durable-now-request')
    async def poll_control(identity):
        return intent
    owner.poll_control = poll_control
    # No driver.wake(): a different ctl process can only update durable state.
    cancel = await asyncio.wait_for(writer.frames.get(), 1)
    assert cancel['method'] == 'session/cancel'
    owner.poll_control = lambda identity: no_control()
    driver.observe({'id': first['id'], 'result': {'stopReason': 'cancelled'}})
    await asyncio.wait_for(task, 1)
    assert sum(event.kind == 'interrupt_intent' for event in owner.events) == 1


async def no_control():
    return None


@pytest.mark.asyncio
async def test_cancelled_durable_now_before_io_keeps_active_conversation():
    from harflow_mcp.backends.conversation import ConversationRefused
    from harflow_mcp.conversation_transport import ConversationInterrupt
    driver, owner, writer, task, first = await ready_driver()
    requested = asyncio.Event()
    original_observe = owner.observe
    async def observe(observation):
        if observation.kind == 'interrupt_intent':
            requested.set()
            raise ConversationRefused('message cancelled before interrupt issuance')
        await original_observe(observation)
    owner.observe = observe
    intent = ConversationInterrupt(driver.port.identity, 'cancelled-now-request')
    async def poll_control(identity):
        return intent
    owner.poll_control = poll_control
    await asyncio.wait_for(requested.wait(), 1)
    assert driver.port.unknown is None and driver.port.interrupt_requested is False
    assert writer.frames.empty()
    owner.poll_control = lambda identity: no_control()
    driver.observe({'id': first['id'], 'result': {'stopReason': 'end_turn'}})
    await asyncio.wait_for(task, 1)
    assert driver.port.classify(0) == 'completed'


@pytest.mark.asyncio
async def test_advertised_native_close_ack_precedes_stdin_close():
    profile = {'backend': 'vibe', 'version': '3.1.0', 'cwd': '/repo', 'mode': 'ask',
               'model': 'mistral-medium-3.5', 'effort': 'high'}
    port, owner, writer = ACPConversationPort(profile), Owner(), Writer()
    driver = ConversationDriver(port, writer, owner, initial_input=ConversationInput('first', 'one'), idle_timeout=.01, operation_timeout=2)
    task = driver.start()
    init = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe({'id': init['id'], 'result': {'protocolVersion': 1, 'agentCapabilities': {'sessionCapabilities': {'close': {}}}}})
    new = await asyncio.wait_for(writer.frames.get(), 1)
    options = [{'id': key, 'currentValue': value, 'options': [{'value': value}]} for key, value in
               [('mode', 'ask'), ('model', 'mistral-medium-3.5'), ('thinking', 'high')]]
    driver.observe({'id': new['id'], 'result': {'sessionId': 's', 'configOptions': options, '_meta': {'workspace_trust': {'status': 'session'}}}})
    mode = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe({'id': mode['id'], 'result': {'configOptions': options}})
    prompt = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe({'id': prompt['id'], 'result': {'stopReason': 'end_turn'}})
    close = await asyncio.wait_for(writer.frames.get(), 1)
    assert close['method'] == 'session/close'
    assert not writer.closed and not task.done()
    driver.observe({'id': close['id'], 'result': {}})
    await asyncio.wait_for(task, 1)
    assert writer.closed and port.close_acknowledged


@pytest.mark.asyncio
@pytest.mark.parametrize('arrival', ['interrupt_intent', 'write_started'])
async def test_tool_arriving_before_cancel_bytes_defers_same_durable_intent(arrival):
    from harflow_mcp.conversation_transport import ConversationInterrupt
    driver, owner, writer, task, first = await ready_driver()
    identity = driver.port.identity
    intent = ConversationInterrupt(identity, 'now-request')
    deferred = asyncio.Event()
    injected = False
    original_observe = owner.observe
    async def observe(observation):
        nonlocal injected
        await original_observe(observation)
        if observation.kind == 'interrupt_deferred': deferred.set()
        if (not injected and observation.kind == arrival
                and (arrival != 'write_started' or observation.payload.get('control') == 'interrupt')):
            injected = True
            driver.observe({'method': 'session/update', 'params': {'sessionId': 'session-a',
                'update': {'sessionUpdate': 'tool_call', 'toolCallId': 'late-tool', 'status': 'in_progress'}}})
    owner.observe = observe
    async def poll_control(current): return intent
    owner.poll_control = poll_control
    owner.queued.append(ConversationInput('now-response', 'selected now'))
    await asyncio.wait_for(deferred.wait(), 1)
    assert not driver.port.interrupt_requested and driver.port.unknown is None
    assert driver._operation_deadline is None  # Busy deferral has sent no native cancel.
    assert writer.frames.empty() and owner.claims == ['first']
    assert owner.queued == [ConversationInput('now-response', 'selected now')]
    driver.observe({'method': 'session/update', 'params': {'sessionId': 'session-a',
        'update': {'sessionUpdate': 'tool_call_update', 'toolCallId': 'late-tool', 'status': 'completed'}}})
    cancel = await asyncio.wait_for(writer.frames.get(), 1)
    assert cancel['method'] == 'session/cancel'  # Same intent retried; it was not deduplicated as sent.
    driver.observe({'id': first['id'], 'result': {'stopReason': 'cancelled'}})
    selected = await asyncio.wait_for(writer.frames.get(), 1)
    assert selected['params']['prompt'][0]['text'] == 'selected now'
    driver.observe({'id': selected['id'], 'result': {'stopReason': 'end_turn'}})
    await asyncio.wait_for(task, 1)


@pytest.mark.asyncio
async def test_driver_known_busy_tool_refuses_cancel_before_intent():
    from harflow_mcp.conversation_transport import ConversationInterruptDeferred
    driver, owner, writer, task, first = await ready_driver()
    # The drainer captured this frame, but the driver has not processed it yet.
    driver.observe({'method': 'session/update', 'params': {'sessionId': 'session-a',
        'update': {'sessionUpdate': 'tool_call', 'toolCallId': 'known-tool', 'status': 'in_progress'}}})
    with pytest.raises(ConversationInterruptDeferred, match='not qualified'):
        await driver.interrupt(driver.port.identity)
    assert not any(event.kind == 'interrupt_intent' for event in owner.events)
    assert not driver.port.interrupt_requested and writer.frames.empty()
    driver.observe({'method': 'session/update', 'params': {'sessionId': 'session-a',
        'update': {'sessionUpdate': 'tool_call_update', 'toolCallId': 'known-tool', 'status': 'completed'}}})
    driver.observe({'id': first['id'], 'result': {'stopReason': 'end_turn'}})
    await asyncio.wait_for(task, 1)


@pytest.mark.asyncio
async def test_tool_racing_after_cancel_bytes_still_blocks_successor_as_unknown():
    from harflow_mcp.conversation_transport import ConversationInterrupt
    driver, owner, writer, task, first = await ready_driver()
    identity = driver.port.identity
    intent = ConversationInterrupt(identity, 'racing-now')
    async def poll_control(current): return intent
    owner.poll_control = poll_control
    owner.queued.append(ConversationInput('selected', 'selected now'))
    original_write = writer.write
    def write(data):
        original_write(data)
        if json.loads(data).get('method') == 'session/cancel':
            driver.observe({'method': 'session/update', 'params': {'sessionId': 'session-a',
                'update': {'sessionUpdate': 'tool_call', 'toolCallId': 'racing-tool', 'status': 'in_progress'}}})
    writer.write = write
    cancel = await asyncio.wait_for(writer.frames.get(), 1)
    assert cancel['method'] == 'session/cancel'
    driver.observe({'id': first['id'], 'result': {'stopReason': 'cancelled'}})
    await asyncio.wait_for(owner.terminal.get(), 1)
    assert driver.port.unsettled_effects == {'racing-tool'}
    assert writer.frames.empty() and owner.claims == ['first']
    driver.port.release_tool('racing-tool', identity=identity, trusted_settlement=True)
    driver.wake()
    selected = await asyncio.wait_for(writer.frames.get(), 1)
    assert selected['params']['prompt'][0]['text'] == 'selected now'
    driver.observe({'id': selected['id'], 'result': {'stopReason': 'end_turn'}})
    await asyncio.wait_for(task, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize('running_tool', [False, True])
async def test_healthy_response_and_tools_have_no_transport_wallclock_limit(monkeypatch, running_tool):
    from harflow_mcp import conversation_transport as transport
    clock = [0.0]
    monkeypatch.setattr(transport, 'monotonic', lambda: clock[0])
    driver, owner, writer, task, first = await ready_driver()
    checkpoints = asyncio.Queue()
    async def poll(identity):
        checkpoints.put_nowait(True)
        return None
    owner.poll_control = poll
    if running_tool:
        driver.observe({'method':'session/update','params':{'sessionId':'session-a','update':{
            'sessionUpdate':'tool_call','toolCallId':'healthy-tool','status':'in_progress'}}})
    clock[0] = 10000.0
    driver.wake()
    await asyncio.wait_for(checkpoints.get(), 1)
    assert not task.done() and driver.port.unknown is None and not writer.closed
    assert driver._operation_deadline is None
    if running_tool: assert driver.port.pending_tools == {'healthy-tool'}
    await asyncio.wait_for(driver.close(), 1)


@pytest.mark.asyncio
async def test_late_now_starts_fresh_native_settlement_deadline(monkeypatch):
    from harflow_mcp import conversation_transport as transport
    clock = [0.0]
    monkeypatch.setattr(transport, 'monotonic', lambda: clock[0])
    driver, owner, writer, task, first = await ready_driver()
    clock[0] = 10000.0
    await driver.interrupt(driver.port.identity)
    cancel = await asyncio.wait_for(writer.frames.get(), 1)
    assert cancel['method'] == 'session/cancel'
    assert driver._operation_deadline == 10002.0
    assert driver.port.unknown is None
    clock[0] = 10003.0
    driver.wake()
    await asyncio.wait_for(task, 1)
    assert driver.port.unknown and writer.closed
    assert owner.claims == ['first'] and writer.frames.empty()


@pytest.mark.asyncio
async def test_unready_handshake_deadline_is_finite(monkeypatch):
    from harflow_mcp import conversation_transport as transport
    clock = [0.0]
    monkeypatch.setattr(transport, 'monotonic', lambda: clock[0])
    port = ACPConversationPort({'backend':'cursor','version':'2026.10.22-27dcf5f','cwd':'/repo','mode':'agent'})
    owner, writer = Owner(), Writer()
    driver = ConversationDriver(port, writer, owner, initial_input=ConversationInput('first','one'), operation_timeout=2)
    task = driver.start()
    initialize = await asyncio.wait_for(writer.frames.get(), 1)
    assert initialize['method'] == 'initialize'
    clock[0] = 3.0
    driver.wake()
    await asyncio.wait_for(task, 1)
    assert port.unknown and writer.closed and not owner.claims


@pytest.mark.asyncio
@pytest.mark.parametrize('blocked_method', ['session/prompt','session/cancel'])
async def test_blocked_pipe_drain_times_out_unknown_without_reissue(monkeypatch, blocked_method):
    from harflow_mcp import conversation_transport as transport
    entered, expire = asyncio.Event(), asyncio.Event()
    class BlockingWriter(Writer):
        def write(self, data):
            super().write(data)
            self.current_method = json.loads(data).get('method')
        async def drain(self):
            return None  # Timeout is injected at the actual wait_for boundary below.
    writer = BlockingWriter()
    class AsyncioProxy:
        def __getattr__(self, name): return getattr(asyncio, name)
        async def wait_for(self, awaitable, timeout):
            if (getattr(awaitable, 'cr_code', None) is BlockingWriter.drain.__code__
                    and writer.current_method == blocked_method):
                assert timeout == 2
                entered.set()
                try:
                    await expire.wait()
                finally:
                    awaitable.close()
                raise asyncio.TimeoutError
            return await asyncio.wait_for(awaitable, timeout)
    monkeypatch.setattr(transport, 'asyncio', AsyncioProxy())
    port, owner = ACPConversationPort({'backend':'cursor','version':'2026.10.22-27dcf5f','cwd':'/repo','mode':'agent'}), Owner()
    driver = ConversationDriver(port, writer, owner, initial_input=ConversationInput('first','one'), operation_timeout=2)
    task = driver.start()
    initialize = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe({'id':initialize['id'],'result':{'protocolVersion':1}})
    new = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe({'id':new['id'],'result':{'sessionId':'session-a','modes':{
        'currentModeId':'agent','availableModes':[{'id':'agent'}]}}})
    prompt = await asyncio.wait_for(writer.frames.get(), 1)
    assert prompt['method'] == 'session/prompt'
    interrupt = None
    if blocked_method == 'session/cancel':
        interrupt = asyncio.create_task(driver.interrupt(port.identity))
        cancel = await asyncio.wait_for(writer.frames.get(), 1)
        assert cancel['method'] == 'session/cancel'
    await asyncio.wait_for(entered.wait(), 1)
    owner.queued.append(ConversationInput('second','must not replay'))
    expire.set()
    if interrupt is not None:
        with pytest.raises(asyncio.TimeoutError): await asyncio.wait_for(interrupt, 1)
    await asyncio.wait_for(task, 1)
    assert port.unknown and writer.closed and driver.closed
    assert owner.claims == ['first'] and writer.frames.empty()
    assert not port.outgoing_frames()


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel_method', ['session/prompt','session/cancel'])
async def test_cancelled_partial_write_closes_before_blocked_unknown_bookkeeping(cancel_method):
    drained, unknown_entered, release_unknown = asyncio.Event(), asyncio.Event(), asyncio.Event()
    class BlockedWriter(Writer):
        def write(self, data):
            super().write(data)
            self.method = json.loads(data).get('method')
        async def drain(self):
            if self.method == cancel_method:
                drained.set()
                await asyncio.Future()
    port = ACPConversationPort({'backend':'cursor','version':'2026.10.22-27dcf5f','cwd':'/repo','mode':'agent'})
    owner, writer = Owner(), BlockedWriter()
    original_observe = owner.observe
    async def observe(observation):
        if observation.kind == 'unknown':
            unknown_entered.set()
            await release_unknown.wait()
        await original_observe(observation)
    owner.observe = observe
    driver = ConversationDriver(port, writer, owner, initial_input=ConversationInput('first','one'), operation_timeout=2)
    task = driver.start()
    initialize = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe({'id':initialize['id'],'result':{'protocolVersion':1}})
    new = await asyncio.wait_for(writer.frames.get(), 1)
    driver.observe({'id':new['id'],'result':{'sessionId':'session-a','modes':{
        'currentModeId':'agent','availableModes':[{'id':'agent'}]}}})
    prompt = await asyncio.wait_for(writer.frames.get(), 1)
    assert prompt['method'] == 'session/prompt'
    operation = task
    if cancel_method == 'session/cancel':
        operation = asyncio.create_task(driver.interrupt(port.identity))
        cancel = await asyncio.wait_for(writer.frames.get(), 1)
        assert cancel['method'] == 'session/cancel'
    await asyncio.wait_for(drained.wait(), 1)
    owner.queued.append(ConversationInput('second','must not reissue'))
    operation.cancel()
    await asyncio.wait_for(unknown_entered.wait(), 1)
    assert writer.closed and port.unknown
    assert owner.claims == ['first'] and writer.frames.empty()
    release_unknown.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(operation, 1)
    if operation is not task:
        await asyncio.wait_for(task, 1)
    assert driver.closed and writer.closed
    assert owner.claims == ['first'] and not port.outgoing_frames()
