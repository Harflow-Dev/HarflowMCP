import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from polybridge.detached import _wait_started, _release_started, error_doc
from polybridge import detached, store, tasks
from polybridge.extra_paths import normalize_paths, serialize_paths
from test_extra_path_tasks import Certified, _state_logs


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


@pytest.fixture
def backend(monkeypatch):
    backend = Certified()
    monkeypatch.setattr(tasks, 'get_backend', lambda _: backend)
    monkeypatch.setattr(store, 'get_backend', lambda _: backend)
    return backend


def _granted_record(task_id, repo, backend, *, certification):
    records = serialize_paths(normalize_paths([str(repo)]))
    return store.TaskRecord(
        task_id=task_id, backend=backend.name, session_id='session', repo_path=str(repo),
        started_at='2026-01-01T00:00:00+00:00',
        extra_writable_paths=records, extra_path_certification=certification,
        enforcement={'extra_writable_paths': records, 'extra_path_certification': certification},
    )


def test_error_doc_preserves_more_specific_errors_and_maps_marked_value_errors():
    from polybridge import backends
    marked = ValueError('refused before any process could exist')
    marked.polybridge_not_started = True
    assert error_doc(marked) == {'code': 'invalid_params', 'message': 'refused before any process could exist'}
    assert error_doc(ValueError('late failure'))['code'] == 'start_failed'
    for exc, code in [
        (OSError('exec failed'), 'spawn_failed'),
        (tasks.SessionBusyError('busy'), 'session_busy'),
        (backends.UnsupportedCapability('nope'), 'unsupported'),
        (backends.UnknownBackend('mystery'), 'unknown_backend'),
    ]:
        exc.polybridge_not_started = True
        assert error_doc(exc)['code'] == code


async def test_run_with_incompatible_saved_grants_reports_invalid_params(tmp_path, monkeypatch, backend):
    from unittest.mock import AsyncMock
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    store.write_landed(registry.log_dir, _granted_record('source', tmp_path, backend, certification='other-v1'))
    with pytest.raises(ValueError, match='certification') as error:
        await registry.start('Task', tmp_path, backend=backend, continuation_of_task_id='source')
    assert error.value.polybridge_not_started
    assert detached.error_doc(error.value)['code'] == 'invalid_params'


async def test_resume_with_incompatible_saved_grants_reports_invalid_params(tmp_path, monkeypatch, backend):
    from unittest.mock import AsyncMock
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    record = _granted_record('source', tmp_path, backend, certification='other-v1')
    with pytest.raises(ValueError, match='certification') as error:
        await registry.resume(record, 'Follow up')
    assert error.value.polybridge_not_started
    assert detached.error_doc(error.value)['code'] == 'invalid_params'


async def test_continue_with_incompatible_saved_grants_reports_invalid_params(tmp_path, monkeypatch, backend):
    import json
    from polybridge import task_continuation
    from unittest.mock import AsyncMock
    monkeypatch.setattr(task_continuation, 'get_backend', lambda _: backend)
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    record = replace(
        _granted_record('source', tmp_path, backend, certification='other-v1'),
        status='completed', exit_code=0, finished_at='2026-01-02T00:00:00+00:00',
        output_drained=True, prompt='Original assignment',
    )
    store.write_landed(registry.log_dir, record)
    with pytest.raises(ValueError, match='certification') as error:
        await registry.continue_task('source')
    assert error.value.polybridge_not_started
    assert detached.error_doc(error.value)['code'] == 'invalid_params'
    reservation = json.loads((registry.log_dir / 'source.continuation.json').read_text())
    assert reservation['state'] == 'not_started'


async def test_unmarked_pre_spawn_refusal_remains_start_failed(tmp_path, monkeypatch, backend):
    from unittest.mock import AsyncMock
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    record = _granted_record('source', tmp_path, backend, certification='fixture-v1')
    with pytest.raises(ValueError, match='native workflows') as error:
        await registry.resume(record, 'Follow up', native_subagent=True)
    assert not getattr(error.value, 'polybridge_not_started', False)
    assert detached.error_doc(error.value)['code'] == 'start_failed'
