"""Native driver attached to the real registry/monitor with a stdio fake peer."""
import asyncio
import hashlib
import json
import sys
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from harflow_mcp import backends, state_ownership, store
from harflow_mcp.backends.base import Invocation, Enforcement
from harflow_mcp.backends.claude import ClaudeBackend
from harflow_mcp.backends.conversation import ConversationLaunch, ConversationRefused
from harflow_mcp.backends.conversation_native import ACPConversationPort
from harflow_mcp.conversation_task_transport import ConversationAttachment, qualify_saved_source
from harflow_mcp.conversation_transport import ConversationInput
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry


@pytest.fixture
def tmp_path(tmp_path_factory):
    root = tmp_path_factory.mktemp('native-conversation')
    with state_ownership.state_session(resolve_layout(root=root / 'state')):
        (root / 'state' / 'tasks').mkdir()
        yield root


class Backend(ClaudeBackend):
    name = 'stdio-conversation-fixture'
    binary = sys.executable
    capabilities = ClaudeBackend.capabilities._replace(chooses_session_id=False, supports_live_input=False)

    def build_start_argv(self, prompt, **kwargs):
        return Invocation([sys.executable, str(Path(__file__).with_name('conversation_native_process.py'))])

    def assert_safe(self, invocation, freedom, network=None):
        assert invocation.argv == [sys.executable, str(Path(__file__).with_name('conversation_native_process.py'))]

    def with_system_guidance(self, invocation, text, **kwargs):
        return invocation

    def enforcement(self, freedom, network=None):
        return Enforcement(freedom=freedom, mechanism='stdio fixture', os_enforced=False, writes_confined=False)


class Owner:
    def __init__(self, task):
        self.events = []
        self.queue = [ConversationInput('second', 'two'), ConversationInput('third', 'three')]
        self.terminal = asyncio.Queue()
        self.close_barrier = asyncio.Event()
        self.claims = []
        self.task = task
        task.conversation = dict(contract='conversation_delivery_v1', conversation_id=str(uuid.uuid4()),
                                 transport='acp', profile_fingerprint=hashlib.sha256(b'fixture').hexdigest())

    async def before_issue(self, candidate, previous):
        self.claims.append(candidate)

    async def next_input(self, previous):
        return self.queue.pop(0) if self.queue else None

    async def poll_control(self, identity):
        return None

    async def before_close(self, previous):
        await self.close_barrier.wait()
        return None

    async def observe(self, observation):
        self.events.append(observation)
        if observation.kind == 'response_terminal':
            self.terminal.put_nowait(observation)


async def test_registry_native_three_responses_one_process_and_physical_settlement(tmp_path, monkeypatch):
    backend = Backend()
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    owners = []
    def factory(backend, original, policy):
        profile = dict(v=1, backend=backend.name, version='fixture', transport='acp', cwd=str(tmp_path),
                       freedom=policy['freedom'], caller_approvals=False, mode='agent',
                       initial_prompt=policy['prompt'], argv=original.argv, env=dict(original.env))
        launch = ConversationLaunch(replace(original, conversation_context=profile), ACPConversationPort(profile))
        def owner_factory(task):
            owner = Owner(task)
            from harflow_mcp.backends.conversation_qualification import fingerprint
            owner.task.conversation["profile_fingerprint"] = fingerprint(profile)
            owners.append(owner)
            return owner
        return ConversationAttachment(launch, owner_factory, idle_timeout=.01, operation_timeout=2)
    registry = TaskRegistry(log_dir=tmp_path / 'state' / 'tasks', conversation_launch_factory=factory)
    task = await registry.start('one', tmp_path, backend=backend, freedom='read_only')
    owner = owners[0]
    try:
        try:
            terminals = [await asyncio.wait_for(owner.terminal.get(), 5) for _ in range(3)]
        except TimeoutError:
            pytest.fail(repr([(o.kind, o.payload) for o in owner.events]) + repr(task.bridge_notices))
        assert [o.identity.logical_turn for o in terminals] == [task.task_id, 'second', 'third']
        assert task.proc.returncode is None and not task.done.is_set()
        assert task.conversation_responses_issued == 3
        assert task.acc.is_error is False
        record = store.read(registry.log_dir, task.task_id)
        assert record.conversation == task.conversation
        assert qualify_saved_source(registry, record)
        assert not qualify_saved_source(registry, replace(record, pid=record.pid + 1))
        assert not task.live_input
        assert not any(e.kind == 'input_consumed' for e in owner.events)
        owner.close_barrier.set()
        await asyncio.wait_for(task.done.wait(), 5)
        assert task.exit_code == 0 and task.output_drained and task.status == 'completed'
        settled = store.read(registry.log_dir, task.task_id)
        # This low-level fixture deliberately has no durable delivery ledger.
        # Readback loss blocks continuation but must retain physical settlement.
        assert settled.status == 'completed' and settled.exit_code == 0 and settled.output_drained
        assert any('Conversation state unavailable' in notice for notice in settled.bridge_notices)
        assert task.acc.summary == 'three'
        from harflow_mcp import events
        page = events.read_page(events.events_path(registry.log_dir, task.task_id), limit=100,
                                kinds=list(events.EVENT_KINDS))
        delivered = [event for event in page.events
                     if event['kind'] == 'user_message' and event.get('source') == 'conversation_delivery']
        assert [(event['text'], event['conversation_generation'], event['conversation_turn_id'],
                 event['semantic_consumption']) for event in delivered] == [
            ('two', 2, 'second', 'unknown'), ('three', 3, 'third', 'unknown')]
        responses = [event for event in page.events if event['kind'] == 'assistant_delta']
        assert {(event['conversation_generation'], event['conversation_turn_id'])
                for event in responses} == {(1, task.task_id), (2, 'second'), (3, 'third')}
        assert not qualify_saved_source(registry, record)
    finally:
        owner.close_barrier.set()
        if task.proc.returncode is None:
            task.proc.kill()
            await task.proc.wait()
        if task.monitor:
            await asyncio.wait_for(asyncio.shield(task.monitor), 5)


async def test_serialized_context_cannot_bypass_launch_owner_qualification(tmp_path):
    registry = TaskRegistry(log_dir=tmp_path / 'state' / 'tasks')
    original = Invocation(['never-execute'], conversation_context={'v': 1})
    with pytest.raises(ConversationRefused, match='not launch authority'):
        await registry._spawn(original, repo_path=tmp_path)


async def test_owner_binding_failure_supervises_spawned_process_to_actual_exit(tmp_path, monkeypatch):
    backend = Backend()
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    captured = []
    def factory(backend, original, policy):
        profile = dict(v=1, backend=backend.name, version='fixture', transport='acp', cwd=str(tmp_path),
                       freedom=policy['freedom'], caller_approvals=False, mode='agent',
                       initial_prompt=policy['prompt'], argv=original.argv, env=dict(original.env))
        launch = ConversationLaunch(replace(original, conversation_context=profile), ACPConversationPort(profile))
        def refuse(task):
            captured.append(task)
            raise ConversationRefused('durable owner already exists')
        return ConversationAttachment(launch, refuse)
    registry = TaskRegistry(log_dir=tmp_path / 'state' / 'tasks', conversation_launch_factory=factory)
    with pytest.raises(ConversationRefused, match='already exists'):
        await registry.start('must never issue', tmp_path, backend=backend, freedom='read_only')
    task, = captured
    assert registry._tasks[task.task_id] is task
    assert task.monitor is not None
    await asyncio.wait_for(task.done.wait(), 5)
    assert task.proc.returncode is not None and task.output_drained
    assert task.status == 'failed'
    assert not task.acc.result_count
    assert not task.log_path.read_text().strip()
