"""Real retention/admission locking and conversation pin qualification."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event
import uuid

import pytest

from harflow_mcp import conversation_delivery as delivery
from harflow_mcp import conversation_runtime as runtime
from harflow_mcp import retention, state_ownership, store
from harflow_mcp.backends.claude import ClaudeBackend
from harflow_mcp.conversation_documents import ConversationError
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    root = tmp_path / 'state'
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / 'tasks').mkdir()
        registry = TaskRegistry(log_dir=root / 'tasks')
        monkeypatch.setattr(ClaudeBackend, 'conversation_resume_supported', lambda self, source: True, raising=False)
        old = (datetime.now(timezone.utc) - timedelta(days=40)).isoformat()
        record = store.TaskRecord(task_id=str(uuid.uuid4()), backend='claude', session_id='retention-context',
            repo_path=str(tmp_path), started_at=old, finished_at=old, status='completed',
            exit_code=0, output_drained=True, prompt='assignment')
        assert store.write_landed(registry.log_dir, record)
        yield registry, record


def admit(registry, record):
    witness = runtime.enroll(registry, record.task_id)
    return runtime.admit(registry, record.task_id, 'retain this conversation',
        request_id=str(uuid.uuid4()), expected_epoch=witness['ledger_epoch'],
        expected_head_task_id=witness['head_task_id'], expected_generation=witness['generation'],
        expected_turn_id=witness['turn_id'])


def test_admission_after_sweep_snapshot_is_rechecked_under_session_lock(fixture, monkeypatch):
    registry, record = fixture
    before_session_lock, proceed = Event(), Event()
    real_delete = retention._delete_task
    def barrier(*args, **kwargs):
        before_session_lock.set()
        assert proceed.wait(10), 'admission did not release deletion barrier'
        return real_delete(*args, **kwargs)
    monkeypatch.setattr(retention, '_delete_task', barrier)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(retention.sweep, registry.log_dir, 30, datetime.now(timezone.utc))
        try:
            assert before_session_lock.wait(10), 'sweep never selected expired source'
            receipt = admit(registry, record)
            assert receipt['status'] == 'queued'
        finally:
            proceed.set()
        result = future.result(timeout=10)
    assert result['deleted_tasks'] == 0
    assert result['kept_locked'] == 1
    assert store.read(registry.log_dir, record.task_id) is not None


@pytest.mark.parametrize('status', ['queued', 'unknown'])
def test_pending_delivery_pins_source_and_pre_reserved_destination(fixture, status):
    registry, record = fixture
    admit(registry, record)
    ledger = runtime.ledger_for(registry, record)
    destination = str(uuid.uuid4())
    if status == 'unknown':
        def reserve(state):
            state = delivery.observe_boundary(state, generation=state['generation'], turn_id=state['turn_id'])
            claim_id = str(uuid.uuid4())
            state, item = delivery.claim_next(state, claim_id=claim_id,
                destination_task_id=destination, destination_turn_id=str(uuid.uuid4()))
            return delivery.mark_unknown(state, claim_id=claim_id), item
        ledger.update(reserve)
    pins = runtime.protected_task_ids(registry.state_layout)
    assert record.task_id in pins
    if status == 'unknown':
        assert destination in pins
    assert retention.sweep(registry.log_dir, 30, datetime.now(timezone.utc))['deleted_tasks'] == 0
    assert store.read(registry.log_dir, record.task_id) is not None


def test_cancelled_final_delivery_does_not_prevent_age_retention(fixture):
    registry, record = fixture
    receipt = admit(registry, record)
    runtime.cancel(registry, record.task_id, receipt['message_id'])
    assert record.task_id not in runtime.protected_task_ids(registry.state_layout)
    assert retention.sweep(registry.log_dir, 30, datetime.now(timezone.utc))['deleted_tasks'] == 1
    assert store.read(registry.log_dir, record.task_id) is None


def test_corrupt_conversation_aborts_retention_before_deletion(fixture):
    registry, record = fixture
    admit(registry, record)
    runtime.ledger_for(registry, record).path.write_text('{}')
    with pytest.raises(ConversationError):
        retention.sweep(registry.log_dir, 30, datetime.now(timezone.utc))
    assert store.read(registry.log_dir, record.task_id) is not None
