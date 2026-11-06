"""Bounded conversation status views: read-only, exact-identity, never prose."""
import json
import os
import uuid

import pytest

from harflow_mcp import conversation_delivery as delivery
from harflow_mcp import conversation_workflows as binding
from harflow_mcp import conversation_views, state_ownership, store, workflows
from harflow_mcp import workflow_steering as steering
from harflow_mcp.conversation_documents import CONTRACT
from harflow_mcp.conversation_live_owner import conversation_id as live_conversation_id
from harflow_mcp.conversation_store import ConversationStore
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import StateOwnershipError


def uid() -> str:
    return str(uuid.uuid4())


def descriptor(conversation_id):
    return dict(contract=CONTRACT, conversation_id=conversation_id,
                transport='claude-stream-json', profile_fingerprint='0' * 64)


def source_record(task_id, conversation, session_id='native-a'):
    return store.TaskRecord(task_id=task_id, backend='claude', session_id=session_id,
        repo_path='/tmp/repo', started_at='2026-11-06T00:00:00+00:00', freedom='read_only',
        markers=[], status='running', conversation=conversation)


@pytest.fixture
def layout(tmp_path):
    selected = resolve_layout(root=tmp_path / 'state')
    with state_ownership.state_session(selected):
        selected.tasks_directory.mkdir()
        yield selected


def create_standalone(layout, task_id, *, session_id='native-a', generation=1):
    conversation_id = live_conversation_id(task_id)
    state = delivery.new_state(conversation_id, task_id, session_id, generation, task_id)
    ledger = ConversationStore(layout, conversation_id)
    ledger.update(lambda old: (state, None))
    return ledger, conversation_id


def test_view_without_descriptor_is_none(layout, tmp_path):
    task_id = uid()
    store.write(layout.tasks_directory, source_record(task_id, None))
    with state_ownership.state_session(layout):
        result = conversation_views.describe(layout.tasks_directory,
                                             store.read(layout.tasks_directory, task_id))
    assert result is None


def test_standalone_view_returns_bounded_summary(layout):
    task_id = uid()
    ledger, conversation_id = create_standalone(layout, task_id)
    big_text = 'sensitive marker ' * 1800  # just over 30 KB, larger than any summary
    with state_ownership.state_session(layout):
        first = uid()
        ledger.update(lambda old: delivery.admit(old, request_id=first,
            text='first caller prose', delivery='queue', target_task_id=task_id,
            expected_epoch=old['ledger_epoch']))
        ledger.update(lambda old: delivery.admit(old, request_id=uid(),
            text=big_text, delivery='queue', target_task_id=task_id,
            expected_epoch=old['ledger_epoch']))
        ledger.update(lambda old: (delivery.cancel(old, first), None))
        ledger.update(lambda old: (delivery.record_progress(old, generation=old['generation'],
            logical_turn=old['turn_id'], phase='waiting_tools', pending_tools=1), None))
    with state_ownership.state_session(layout):
        result = conversation_views.describe(layout.tasks_directory,
            source_record(task_id, descriptor(conversation_id)))
    assert set(result) == {'contract', 'available', 'conversation_id', 'expected', 'closed',
                          'progress', 'accounting', 'runtime_fault', 'message_counts',
                          'outstanding_count'}
    assert result['contract'] == CONTRACT
    assert result['available'] is True
    assert result['conversation_id'] == conversation_id
    assert result['expected'] == dict(conversation_id=conversation_id, head_task_id=task_id,
        generation=1, turn_id=task_id, ledger_epoch=0)
    assert result['closed'] is False
    assert result['progress'] == dict(generation=1, logical_turn=task_id, phase='waiting_tools',
        native_terminal=None, pending_tools=1, pending_approvals=0, unsettled_effects=0)
    assert result['accounting']['settled_responses'] == 0
    assert result['runtime_fault'] is None
    assert result['message_counts'] == dict(queued=1, claimed=0, consumed=0, unknown=0,
                                             cancelled=1)
    assert result['outstanding_count'] == 1
    # Bounded: no message body, small fixed shape regardless of queued prose.
    encoded = json.dumps(result)
    assert 'first caller prose' not in encoded and 'sensitive marker' not in encoded
    assert len(encoded) < 4096


def test_standalone_missing_and_corrupt_ledgers_are_unavailable_with_status_preserved(layout):
    task_id = uid()
    _, conversation_id = create_standalone(layout, task_id)
    store.write(layout.tasks_directory, source_record(task_id, descriptor(conversation_id)))
    ledger_path = layout.root / 'conversations' / (conversation_id + '.json')
    with state_ownership.state_session(layout):
        assert ledger_path.exists()
        ledger_path.unlink()
        result = conversation_views.describe(layout.tasks_directory,
            store.read(layout.tasks_directory, task_id))
        assert result == dict(contract=CONTRACT, available=False, code='conversation_missing',
                              conversation_id=conversation_id)
        create_standalone(layout, task_id)  # recreate, then corrupt
        ledger_path.write_text('{"contract":')
        result = conversation_views.describe(layout.tasks_directory,
            store.read(layout.tasks_directory, task_id))
        assert result['available'] is False
        assert result['code'] == 'ledger_unavailable'
        # Readback loss never rewrites the ledger or the physical record.
        assert ledger_path.read_text() == '{"contract":'
        assert store.read(layout.tasks_directory, task_id).status == 'running'


def test_exact_source_mismatch_fails_readback_without_redirecting(layout):
    owned_id, foreign_id = uid(), uid()
    with state_ownership.state_session(layout):
        _, owned_conversation = create_standalone(layout, owned_id)
        foreign = conversation_views.describe(layout.tasks_directory,
            source_record(foreign_id, descriptor(owned_conversation), session_id='native-b'))
        assert foreign == dict(contract=CONTRACT, available=False,
            code='native_owner_mismatch', conversation_id=owned_conversation)
        session_mismatch = conversation_views.describe(layout.tasks_directory,
            source_record(owned_id, descriptor(owned_conversation), session_id='native-zz'))
        assert session_mismatch['available'] is False
        assert session_mismatch['code'] == 'native_owner_mismatch'
        # The owned source still reads its own exact ledger.
        assert conversation_views.describe(layout.tasks_directory,
            source_record(owned_id, descriptor(owned_conversation)))['available'] is True


def test_fifo_ledger_refuses_without_blocking_status(layout, monkeypatch):
    task_id = uid()
    ledger, conversation_id = create_standalone(layout, task_id)
    ledger.path.unlink()
    os.mkfifo(ledger.path)
    original_open = os.open
    def checked_open(path, flags, *args, **kwargs):
        if path == ledger.path:
            # Fail deterministically before an old blocking implementation can hang.
            assert flags & os.O_NONBLOCK
        return original_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(os, 'open', checked_open)
    result = conversation_views.describe(layout.tasks_directory,
        source_record(task_id, descriptor(conversation_id)))
    assert result == dict(contract=CONTRACT, available=False,
                         code='ledger_unavailable', conversation_id=conversation_id)


@pytest.fixture
def workflow_binding(tmp_path):
    root = tmp_path / 'workflow-state'
    storage = workflows.WorkflowStore(root)
    definition = {'name': 'example', 'orchestrator': {'backend': 'claude'}, 'nodes': [],
                  'connections': []}
    run = storage.create_run(definition, 'Build', tmp_path, kind='builder',
                             freedom='read_only', interaction_owner='monitor')
    task_id, conversation_id, session_id = uid(), uid(), 'native-a'
    def setup(r):
        r.update(status='running', activations=[dict(id='activation-a', node_id='builder',
            role='builder', status='running', created_at=1,
            tasks=[dict(task_id=task_id, status='running')])])
        binding.enroll(r, target=steering.target(r),
            state=delivery.new_state(conversation_id, task_id, session_id, 1, task_id))
        binding._bindings(r)['activation-a']['native_owner'] = dict(
            task_id=task_id, transport='claude-stream-json', profile_fingerprint='0' * 64)
    storage.update_run(run['workflow_run_id'], setup, 'test_enrollment')
    return storage, run['workflow_run_id'], task_id, conversation_id


def test_workflow_facade_view_returns_the_binding_summary(workflow_binding):
    storage, rid, task_id, conversation_id = workflow_binding
    log_dir = storage.root / 'tasks'
    log_dir.mkdir(exist_ok=True)
    result = conversation_views.describe(log_dir,
        source_record(task_id, descriptor(conversation_id)))
    assert result['available'] is True
    assert result['conversation_id'] == conversation_id
    assert result['expected'] == dict(conversation_id=conversation_id, head_task_id=task_id,
        generation=1, turn_id=task_id, ledger_epoch=0)
    assert result['message_counts'] == dict(queued=0, claimed=0, consumed=0, unknown=0,
                                            cancelled=0)
    assert result['outstanding_count'] == 0


def test_workflow_namespace_mismatch_never_redirects(workflow_binding):
    storage, rid, task_id, _ = workflow_binding
    log_dir = storage.root / 'tasks'
    log_dir.mkdir(exist_ok=True)
    result = conversation_views.describe(log_dir,
        source_record(task_id, descriptor(uid())))
    assert result['available'] is False
    assert result['code'] == 'context_conflict'


def test_retired_workflow_binding_reports_request_expired(workflow_binding):
    storage, rid, task_id, conversation_id = workflow_binding
    def retire(r):
        del binding._bindings(r)['activation-a']
        r['activations'][0]['conversation_checkpoint'] = dict(
            conversation_id=conversation_id, head_task_id=task_id, native_context_id='native-a',
            generation=1, turn_id=task_id, closed_epoch=1, ordinary_source_accepted=True,
            retired_sequence=1)
    storage.update_run(rid, retire, 'test_retirement')
    log_dir = storage.root / 'tasks'
    log_dir.mkdir(exist_ok=True)
    result = conversation_views.describe(log_dir,
        source_record(task_id, descriptor(conversation_id)))
    assert result == dict(contract=CONTRACT, available=False, code='request_expired',
                          conversation_id=conversation_id)


def test_invalid_descriptor_is_unavailable_not_accepted(layout):
    task_id = uid()  # malformed linkage only ever reaches the view in memory;
    # the store itself refuses to persist or serve such a record.
    with state_ownership.state_session(layout):
        bad_id = conversation_views.describe(layout.tasks_directory, source_record(task_id,
            dict(contract=CONTRACT, conversation_id='not-a-uuid',
                 transport='claude-stream-json', profile_fingerprint='0' * 64)))
        assert bad_id == dict(contract=CONTRACT, available=False,
            code='invalid_request_id', conversation_id=None)
        bad_shape = conversation_views.describe(layout.tasks_directory,
            source_record(task_id, dict(contract=CONTRACT, conversation_id=uid(),
                transport='claude-stream-json', profile_fingerprint='0' * 64, extra=1)))
        assert bad_shape == dict(contract=CONTRACT, available=False,
            code='invalid_conversation_descriptor', conversation_id=None)


def test_state_ownership_error_propagates(tmp_path):
    root = tmp_path / 'unmarked'
    tasks = root / 'tasks'
    tasks.mkdir(parents=True)
    (tasks / 'stray.txt').touch()  # unmarked nonempty root: ownership must refuse
    task_id = uid()
    with pytest.raises(StateOwnershipError):
        conversation_views.describe(tasks, source_record(task_id, descriptor(uid())))
