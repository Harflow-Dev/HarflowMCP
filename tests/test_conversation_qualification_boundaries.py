"""Detached source qualification is blocking discovery, never a leaf mutation."""
import asyncio
from datetime import datetime, timezone
import threading
import uuid

import pytest

from harflow_mcp import conversation_api as api, conversation_live_owner as live
from harflow_mcp import conversation_task_transport as transport, state_ownership, store
from harflow_mcp.backends.conversation_native import ACPConversationPort
from harflow_mcp.conversation_documents import CONTRACT
from harflow_mcp.conversation_store import ConversationStore
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry


@pytest.fixture
def fixture(tmp_path):
    layout = resolve_layout(root=tmp_path / 'state')
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir()
        registry = TaskRegistry(log_dir=layout.tasks_directory)
        task_id = str(uuid.uuid4())
        owner = live.ConversationOwner(layout, task_id, 'initial', ACPConversationPort(dict(
            backend='cursor', version='2026.10.22-27dcf5f', cwd=str(tmp_path), mode='agent', session_id='native')))
        descriptor = dict(contract=CONTRACT, conversation_id=owner.conversation_id,
            transport='acp', profile_fingerprint='a' * 64)
        record = store.TaskRecord(task_id=task_id, backend='claude', session_id='native',
            repo_path=str(tmp_path), status='running', started_at=datetime.now(timezone.utc).isoformat(),
            conversation=descriptor)
        assert store.write_landed(registry.log_dir, record)
        yield registry, owner


@pytest.mark.parametrize('operation', ['prepare', 'send', 'promote_now'])
async def test_source_discovery_runs_in_worker_outside_leaf_and_keeps_event_loop_responsive(fixture, monkeypatch, operation):
    registry, owner = fixture
    state = owner.ledger.read()
    expected = {key: state[key] for key in ('conversation_id', 'head_task_id', 'generation', 'turn_id', 'ledger_epoch')}
    message_id = str(uuid.uuid4())
    if operation == 'promote_now':
        live.admit(owner, 'queued', request_id=message_id, mode='queue', expected=expected)
    entered, release = threading.Event(), threading.Event()
    local = threading.local()
    main_thread = threading.get_ident()
    original_update = ConversationStore.update
    def update(self, mutation):
        def guarded(state):
            previous = getattr(local, 'inside_leaf', False)
            local.inside_leaf = True
            try:
                return mutation(state)
            finally:
                local.inside_leaf = previous
        return original_update(self, guarded)
    monkeypatch.setattr(ConversationStore, 'update', update)
    def qualify(registry_, source):
        assert registry_ is registry and source.task_id == owner.task_id
        assert threading.get_ident() != main_thread, 'blocking runtime discovery ran on event loop'
        assert not getattr(local, 'inside_leaf', False), 'runtime discovery ran inside conversation CAS'
        entered.set()
        assert release.wait(3), 'event loop did not release source qualification barrier'
        return True
    monkeypatch.setattr(transport, 'qualify_saved_source', qualify)
    if operation == 'prepare':
        action = api.prepare_task(registry, owner.task_id)
    elif operation == 'send':
        action = api.send_task(registry, dict(contract=CONTRACT, request_id=message_id, delivery='queue',
            text='next input', target=dict(kind='task', task_id=owner.task_id), expected=expected))
    else:
        action = api.control_task(registry, dict(contract=CONTRACT, request_id=str(uuid.uuid4()),
            operation='promote_now', message_request_id=message_id,
            target=dict(kind='task', task_id=owner.task_id), expected=expected))
    pending = asyncio.create_task(action)
    try:
        assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3)
        progressed = asyncio.Event()
        async def unrelated_work():
            progressed.set()
        await asyncio.wait_for(unrelated_work(), 1)
        assert progressed.is_set() and not pending.done()
        release.set()
        result = await asyncio.wait_for(pending, 3)
        assert result['contract'] == CONTRACT
    finally:
        release.set()
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


async def test_large_regular_profile_refuses_before_native_write_and_owned_process_is_reaped(tmp_path, monkeypatch):
    from dataclasses import replace
    from harflow_mcp import backends
    from harflow_mcp.backends.base import Invocation, Enforcement
    from harflow_mcp.backends.claude import ClaudeBackend
    from harflow_mcp.backends.conversation import ConversationLaunch, ConversationRefused
    from harflow_mcp.backends.conversation_qualification import ConversationQualification
    import sys
    class Backend(ClaudeBackend):
        name = 'profile-capacity-native-fixture'
        capabilities = ClaudeBackend.capabilities._replace(chooses_session_id=False, supports_live_input=False)
        def build_start_argv(self, prompt, **kwargs):
            return Invocation([sys.executable, '-c', 'import sys; sys.stdin.buffer.read()'])
        def assert_safe(self, invocation, freedom, network=None):
            assert invocation.argv[0] == sys.executable
        def with_system_guidance(self, invocation, text, **kwargs): return invocation
        def enforcement(self, freedom, network=None):
            return Enforcement(freedom=freedom, mechanism='fixture', os_enforced=False, writes_confined=False)
    backend = Backend()
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    captured = []
    writes = []
    def factory(backend, original, policy):
        profile = dict(v=1, backend=backend.name, version='fixture', transport='acp', cwd=str(tmp_path),
            freedom=policy['freedom'], caller_approvals=False, mode='agent',
            initial_prompt=policy['prompt'], argv=original.argv, env=dict(original.env))
        port = ACPConversationPort(profile)
        def owner(task):
            captured.append(task)
            task.conversation = dict(contract=CONTRACT, conversation_id=str(uuid.uuid4()),
                transport='acp', profile_fingerprint='a'*64)
            return object()
        return transport.ConversationAttachment(
            ConversationLaunch(replace(original, conversation_context=profile), port), owner,
            original_invocation=replace(original, initial_input=policy['prompt'].encode()),
            qualification_receipt=ConversationQualification('r','p','l','a'*64,'fixture'))
    layout = resolve_layout(root=tmp_path/'state')
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir()
        registry = TaskRegistry(log_dir=layout.tasks_directory, conversation_launch_factory=factory)
        monkeypatch.setattr(transport, 'ConversationDriver', lambda *a, **kw: writes.append(True))
        with pytest.raises(ConversationRefused, match='capacity'):
            await registry.start('x'*(200*1024), tmp_path, backend=backend, freedom='read_only')
        task, = captured
        assert not writes and not hasattr(task, '_conversation_attachment')
        await asyncio.wait_for(task.done.wait(), 5)
        assert task.proc.returncode is not None and task.output_drained
        assert task.status == 'failed' and not task.acc.result_count
        assert not task.log_path.read_text().strip()
