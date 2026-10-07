import asyncio
import threading
import time

import pytest

from polybridge import workflows as w
from test_workflows import definition


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, 'is_installed', lambda _: True)
    return w.WorkflowStore(tmp_path)


def seed(storage, repo, *, status='completed', task_status='completed'):
    run = storage.create_run(w.validate_definition(definition()), 'work', repo)
    storage.update_run(run['workflow_run_id'], lambda r: r.update(status=status, activations=[{'id': 'a', 'node_id': 'work', 'role': 'node', 'status': 'failed', 'tasks': [{'task_id': run['workflow_run_id'], 'status': task_status, 'freedom': 'write_in_repo'}]}]), 'fixture')
    return run


def ready(storage):
    for _ in range(5):
        if not storage.list_run_page().get('bootstrap_pending'):
            return
    pytest.fail('catalog did not settle')


def test_checkout_uses_exact_repo_and_terminal_unresolved_ownership(storage, tmp_path, monkeypatch):
    other = tmp_path / 'other'
    other.mkdir()
    for _ in range(120):
        seed(storage, other, status='running', task_status='running')
    target = seed(storage, tmp_path, status='completed', task_status='uncertain')
    ready(storage)
    monkeypatch.setattr(storage, 'list_runs', lambda: pytest.fail('unbounded retained run scan'))
    monkeypatch.setattr(storage, 'get_run', lambda *args, **kwargs: pytest.fail('full run decode on warm lease'))
    lease = w.CheckoutLease(storage, str(tmp_path), True)
    assert lease._orphan_owner() == {'workflow_run_id': target['workflow_run_id'], 'task_id': target['workflow_run_id']}
    empty = tmp_path / 'empty'
    empty.mkdir()
    assert w.CheckoutLease(storage, str(empty), True)._orphan_owner() is None


def test_long_repo_prefix_collision_does_not_share_orphan_owner(storage, tmp_path):
    shared = tmp_path / ('p' * 100) / ('q' * 100)
    left, right = shared / 'left', shared / 'right'
    left.mkdir(parents=True)
    right.mkdir()
    seed(storage, left, task_status='uncertain')
    ready(storage)
    assert w.CheckoutLease(storage, str(left), False)._orphan_owner() is not None
    assert w.CheckoutLease(storage, str(right), False)._orphan_owner() is None


def test_completed_tasks_do_not_hold_checkout_and_excess_matching_owners_fail_closed(storage, tmp_path):
    seed(storage, tmp_path)
    ready(storage)
    assert w.CheckoutLease(storage, str(tmp_path), True)._orphan_owner() is None
    for _ in range(101):
        seed(storage, tmp_path, task_status='uncertain')
    ready(storage)
    assert w.CheckoutLease(storage, str(tmp_path), True)._orphan_owner()['task_id'] == 'too_many_checkout_owners'


async def test_checkout_lookup_runs_off_event_loop(storage, tmp_path, monkeypatch):
    lease = w.CheckoutLease(storage, str(tmp_path), True)
    started = threading.Event()
    def lookup():
        started.set()
        time.sleep(.05)
        return None
    monkeypatch.setattr(lease, '_orphan_owner', lookup)
    pending = asyncio.create_task(lease.__aenter__())
    for _ in range(20):
        if started.is_set():
            break
        await asyncio.sleep(.001)
    assert started.is_set() and not pending.done()
    await pending
    await lease.__aexit__()


def test_checkout_checks_exact_task_identity_without_prompt_or_stream_replay(storage, tmp_path, monkeypatch):
    from polybridge import identity, store
    run = seed(storage, tmp_path, task_status='uncertain')
    ready(storage)
    task = store.TaskRecord(run['workflow_run_id'], 'codex', 'session', str(tmp_path), 'now', status='running', pid=123, markers=['worker'], start_time='start')
    calls = []
    def read(directory, identifier, **kwargs):
        calls.append((identifier, kwargs))
        assert not kwargs['include_prompt'] and kwargs['metadata_byte_limit'] == 4 * 1024 * 1024
        return task
    monkeypatch.setattr(w.task_store, 'read', read)
    monkeypatch.setattr(w.task_store, 'resolve_status', lambda *args, **kwargs: pytest.fail('unbounded stream replay'))
    monkeypatch.setattr(identity, 'check_detail', lambda value: ('dead', 'gone'))
    lease = w.CheckoutLease(storage, str(tmp_path), True)
    assert lease._orphan_owner() is None
    monkeypatch.setattr(identity, 'check_detail', lambda value: ('undecidable', 'unknown'))
    assert lease._orphan_owner()['task_id'] == task.task_id
    monkeypatch.setattr(identity, 'check_detail', lambda value: ('alive', None))
    assert lease._orphan_owner()['task_id'] == task.task_id
    assert [entry[0] for entry in calls] == [task.task_id] * 3


async def test_checkout_cancel_during_worker_lookup_closes_descriptor(storage, tmp_path, monkeypatch):
    lease = w.CheckoutLease(storage, str(tmp_path), True)
    started, release = threading.Event(), threading.Event()
    def lookup():
        started.set()
        release.wait(1)
        return None
    monkeypatch.setattr(lease, '_orphan_owner', lookup)
    pending = asyncio.create_task(lease.__aenter__())
    try:
        for _ in range(100):
            if started.is_set():
                break
            await asyncio.sleep(.001)
        assert started.is_set()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert lease.handle.closed
    finally:
        release.set()


def test_removed_run_prunes_checkout_ownership_after_bounded_bootstrap(storage, tmp_path):
    run = seed(storage, tmp_path, task_status='uncertain')
    ready(storage)
    (storage.runs / f"{run['workflow_run_id']}.json").unlink()
    lease = w.CheckoutLease(storage, str(tmp_path), True)
    # Verified suffix discovery can prune a removed source without a decode batch.
    assert lease._orphan_owner() is None
    with storage._ownership_catalog().connect() as db:
        assert db.execute('SELECT count(*) FROM checkout_tasks').fetchone() == (0,)


def test_changed_matching_run_refreshes_before_trusting_cached_supervisor(storage, tmp_path):
    import json
    run = seed(storage, tmp_path, task_status='uncertain')
    ready(storage)
    path = storage.runs / f"{run['workflow_run_id']}.json"
    value = json.loads(path.read_text())
    value['activations'][0]['tasks'][0]['status'] = 'completed'
    path.write_text(json.dumps(value))
    lease = w.CheckoutLease(storage, str(tmp_path), True)
    assert lease._orphan_owner()['task_id'] == 'ownership_refreshed'
    assert lease._orphan_owner() is None
