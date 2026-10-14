"""MCP notification authorization and pre-spawn durability; no model calls."""
import asyncio
import json
import time
import threading
from types import SimpleNamespace

import pytest
from mcp import MCPError

from polybridge import backends, notifications, server, store


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'hook': {'url': 'https://example.com/hook'}}))
    monkeypatch.setattr(server, '_start_notification_worker', lambda _: None)


async def test_start_reserves_before_spawn(configured, monkeypatch, git_repo, fake_backend_clis):
    seen = []
    async def start(prompt, path, **kwargs):
        task_id = kwargs['task_id']
        rows = notifications.status(server._reg().log_dir, task_id)
        assert rows[0]['state'] == 'pending'
        seen.append(task_id)
        return SimpleNamespace(task_id=task_id, enforcement={}, brief=lambda: {'task_id': task_id})
    monkeypatch.setattr(server._reg(), 'start', start)
    response = await server.start_task('preserve this prose', str(git_repo), backend='codex', notify='hook')
    assert response['task_id'] == seen[0]
    assert response['notification']['task_id'] == seen[0]


async def test_failed_reservation_never_spawns(configured, monkeypatch, git_repo, fake_backend_clis):
    monkeypatch.setattr(notifications, 'subscribe', lambda *a, **k: (_ for _ in ()).throw(OSError('secret')))
    monkeypatch.setattr(server._reg(), 'start', lambda *a, **k: pytest.fail('spawned'))
    with pytest.raises(MCPError, match='no task was started'):
        await server.start_task('test', str(git_repo), backend='codex', notify='hook')


@pytest.mark.parametrize('url', [123, 1.5, True, None, [], {}], ids=['int', 'float', 'bool', 'null', 'list', 'dict'])
async def test_start_rejects_non_string_notification_url_before_spawn(configured, monkeypatch, git_repo, fake_backend_clis, url):
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'hook': {'url': url}}))
    monkeypatch.setattr(server._reg(), 'start', lambda *a, **k: pytest.fail('spawned'))
    monkeypatch.setattr(notifications, 'subscribe', lambda *a, **k: pytest.fail('reserved'))
    with pytest.raises(MCPError, match='notification destination unavailable or invalid') as error:
        await server.start_task('test', str(git_repo), backend='codex', notify='hook')
    assert error.value.code == -32602


@pytest.mark.parametrize('scheme,allow_private', [('https', False), ('http', True)])
async def test_start_rejects_port_zero_before_reservation_or_spawn(configured, monkeypatch, git_repo, fake_backend_clis, scheme, allow_private):
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({
        'hook': {'url': f'{scheme}://receiver.example:0/hook', 'allow_private': allow_private}
    }))
    monkeypatch.setattr(server._reg(), 'start', lambda *a, **k: pytest.fail('spawned'))
    monkeypatch.setattr(notifications, 'subscribe', lambda *a, **k: pytest.fail('reserved'))
    with pytest.raises(MCPError, match='notification destination unavailable or invalid') as error:
        await server.start_task('test', str(git_repo), backend='codex', notify='hook')
    assert error.value.code == -32602


@pytest.mark.parametrize('scheme,allow_private', [('https', False), ('http', True)])
async def test_subscribe_rejects_port_zero_before_persistence(configured, monkeypatch, scheme, allow_private):
    logs = server._reg().log_dir
    task_id = 'port-zero-task'
    store.write(logs, store.TaskRecord(task_id=task_id, backend='codex', session_id=None,
        repo_path='/tmp/repo', started_at='2026-10-14T00:00:00Z'))
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({
        'hook': {'url': f'{scheme}://receiver.example:0/hook', 'allow_private': allow_private}
    }))
    with pytest.raises(MCPError, match='notification destination unavailable or invalid') as error:
        await server.subscribe(task_id, 'hook')
    assert error.value.code == -32602
    assert not (logs.parent / 'notifications.sqlite3').exists()


@pytest.mark.parametrize('url', [123, 1.5, True, None, [], {}], ids=['int', 'float', 'bool', 'null', 'list', 'dict'])
async def test_subscribe_rejects_non_string_notification_url_before_persistence(configured, monkeypatch, url):
    logs = server._reg().log_dir
    task_id = 'malformed-url-task'
    store.write(logs, store.TaskRecord(task_id=task_id, backend='codex', session_id=None,
        repo_path='/tmp/repo', started_at='2026-10-14T00:00:00Z'))
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'hook': {'url': url}}))
    with pytest.raises(MCPError, match='notification destination unavailable or invalid') as error:
        await server.subscribe(task_id, 'hook')
    assert error.value.code == -32602
    assert not (logs.parent / 'notifications.sqlite3').exists()


@pytest.mark.parametrize('method,args', [(server.subscribe, ('id', 'hook')),
    (server.unsubscribe, ('id',)), (server.get_task_notifications, ('id',))])
async def test_delegated_caller_cannot_manage_egress(configured, monkeypatch, method, args):
    async def caller():
        return object()
    monkeypatch.setattr(server, '_verified_workflow_caller', caller)
    with pytest.raises(MCPError, match='direct caller'):
        await method(*args)


async def test_subscribe_lifecycle(configured):
    logs = server._reg().log_dir
    store.write(logs, store.TaskRecord(task_id='id', backend='codex', session_id=None,
        repo_path='/tmp/repo', started_at='2026-10-14T00:00:00Z'))
    first = await server.subscribe('id', 'hook')
    assert (await server.subscribe('id', 'hook'))['id'] == first['id']
    assert (await server.get_task_notifications('id'))[0]['state'] == 'pending'
    assert (await server.unsubscribe('id', 'hook'))[0]['state'] == 'unsubscribed'

async def test_rejected_start_releases_reservation(configured, monkeypatch, git_repo, fake_backend_clis):
    seen = []
    async def reject(*args, **kwargs):
        seen.append(kwargs['task_id'])
        exc = OSError('cannot spawn')
        exc.polybridge_not_started = True
        raise exc
    monkeypatch.setattr(server._reg(), 'start', reject)
    with pytest.raises(MCPError, match='inspect task_id'):
        await server.start_task('test', str(git_repo), backend='codex', notify='hook')
    assert notifications.status(server._reg().log_dir, seen[0]) == []

async def test_cleanup_failure_keeps_recovery_identity(configured, monkeypatch, git_repo, fake_backend_clis):
    seen = []
    async def reject(*args, **kwargs):
        seen.append(kwargs['task_id'])
        exc = OSError('cannot spawn')
        exc.polybridge_not_started = True
        raise exc
    monkeypatch.setattr(server._reg(), 'start', reject)
    monkeypatch.setattr(notifications, 'discard_unstarted', lambda *a: (_ for _ in ()).throw(OSError('disk error')))
    with pytest.raises(MCPError, match='inspect task_id') as error:
        await server.start_task('test', str(git_repo), backend='codex', notify='hook')
    assert seen[0] in str(error.value)
    assert notifications.status(server._reg().log_dir, seen[0])[0]['state'] == 'pending'


async def test_vibe_large_whitespace_refusal_survives_cleanup(configured, monkeypatch, git_repo, fake_backend_clis):
    seen = []
    register = notifications.subscribe
    def reserve(*args, **kwargs):
        seen.append(args[1])
        return register(*args, **kwargs)
    monkeypatch.setattr(notifications, 'subscribe', reserve)
    async def no_spawn(*args, **kwargs):
        pytest.fail('capability refusal must happen before spawning a CLI')
    monkeypatch.setattr(server._reg(), '_spawn', no_spawn)
    with pytest.raises(MCPError, match='Vibe trims outer whitespace'):
        await server.start_task(' ' + 'x' * (33 * 1024), str(git_repo), backend='vibe', notify='hook')
    assert len(seen) == 1
    assert server._reg().get(seen[0]) is None
    assert not store.record_path(server._reg().log_dir, seen[0]).exists()
    assert notifications.status(server._reg().log_dir, seen[0]) == []


async def test_codex_turn_cap_refused_before_reservation(configured, monkeypatch, git_repo, fake_backend_clis):
    monkeypatch.setattr(notifications, 'subscribe', lambda *a, **k: pytest.fail('reserved'))
    with pytest.raises(MCPError, match='max_turns'):
        await server.start_task('test', str(git_repo), backend='codex', max_turns=1, notify='hook')


@pytest.mark.parametrize('uncertainty', ['cleanup_failure', 'unknown_result', 'durable', 'live'])
async def test_capability_refusal_keeps_uncertain_identity(configured, monkeypatch, git_repo, fake_backend_clis, uncertainty):
    seen = []
    async def reject(*args, **kwargs):
        task_id = kwargs['task_id']
        seen.append(task_id)
        if uncertainty == 'durable':
            store.write(server._reg().log_dir, store.TaskRecord(task_id=task_id, backend='codex',
                session_id=None, repo_path=str(git_repo), started_at='2026-10-14T00:00:00Z'))
        elif uncertainty == 'live':
            server._reg()._tasks[task_id] = object()
        exc = backends.UnsupportedCapability('specific refusal')
        exc.polybridge_not_started = True
        raise exc
    monkeypatch.setattr(server._reg(), 'start', reject)
    if uncertainty == 'cleanup_failure':
        monkeypatch.setattr(notifications, 'discard_unstarted', lambda *a: (_ for _ in ()).throw(OSError('disk error')))
    elif uncertainty == 'unknown_result':
        monkeypatch.setattr(notifications, 'discard_unstarted', lambda *a: None)
    with pytest.raises(MCPError, match='inspect task_id') as error:
        await server.start_task('test', str(git_repo), backend='codex', notify='hook')
    assert seen[0] in str(error.value)
    assert notifications.status(server._reg().log_dir, seen[0])[0]['state'] == 'pending'


async def test_pristine_configless_registry_starts_no_notification_worker(monkeypatch):
    registry = server._reg()
    monkeypatch.delenv('PB_NOTIFY_DESTINATIONS', raising=False)
    monkeypatch.setattr(server, '_registry', None)
    monkeypatch.setattr(server, 'TaskRegistry', lambda: registry)
    monkeypatch.setattr(registry, 'start_maintenance', lambda: None)
    assert server._reg() is registry
    assert getattr(registry, '_notification_worker', None) is None
    assert not (registry.log_dir.parent / 'notifications.sqlite3').exists()


async def test_configured_registry_starts_notification_worker_once(monkeypatch):
    registry = server._reg()
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'hook': {'url': 'https://example.com/hook'}}))
    started = []
    async def worker(logs):
        started.append(logs)
        await asyncio.Future()
    monkeypatch.setattr(notifications, 'worker', worker)
    monkeypatch.setattr(server, '_registry', None)
    monkeypatch.setattr(server, 'TaskRegistry', lambda: registry)
    monkeypatch.setattr(registry, 'start_maintenance', lambda: None)
    try:
        server._reg()
        first = registry._notification_worker
        server._start_notification_worker(registry)
        await asyncio.sleep(0)
        assert registry._notification_worker is first
        assert started == [registry.log_dir]
    finally:
        registry._notification_worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await registry._notification_worker


async def test_existing_database_cleanup_resumes_without_configuration(monkeypatch):
    registry = server._reg()
    logs = registry.log_dir
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'hook': {'url': 'https://example.com/hook'}}))
    for task_id in ('expired', 'final', 'unexpired'):
        store.write(logs, store.TaskRecord(task_id=task_id, backend='codex', session_id=None,
            repo_path='/tmp/repo', started_at='2026-10-14T00:00:00Z',
            status='completed', exit_code=0, finished_at='2026-10-14T00:00:00Z'))
        notifications.subscribe(logs, task_id, 'hook')
    old = time.time() - notifications.RETENTION_SECONDS - 60
    with notifications._db(logs) as db:
        db.execute("UPDATE subscriptions SET created=? WHERE task_id='expired'", (old,))
        db.execute("UPDATE subscriptions SET state='delivered',updated=? WHERE task_id='final'", (old,))
    assert 'expired' in notifications.pinned_tasks(logs)
    monkeypatch.delenv('PB_NOTIFY_DESTINATIONS')
    monkeypatch.setattr(notifications, '_deliver', lambda *a: pytest.fail('delivery without current configuration'))
    monkeypatch.setattr(notifications, '_resolve', lambda *a: pytest.fail('network without current configuration'))
    ticked = asyncio.Event()
    async def tick(log_dir):
        notifications._tick(log_dir)
        ticked.set()
        await asyncio.Future()
    monkeypatch.setattr(notifications, 'tick', tick)
    monkeypatch.setattr(server, '_registry', None)
    monkeypatch.setattr(server, 'TaskRegistry', lambda: registry)
    monkeypatch.setattr(registry, 'start_maintenance', lambda: None)
    try:
        server._reg()
        await asyncio.wait_for(ticked.wait(), 2)
        assert notifications.status(logs, 'expired')[0]['state'] == 'expired'
        assert 'expired' not in notifications.pinned_tasks(logs)
        assert notifications.status(logs, 'final') == []
        row = notifications.status(logs, 'unexpired')[0]
        assert row['state'] == 'retry'
        assert row['attempts'] == 1
    finally:
        worker = getattr(registry, '_notification_worker', None)
        if worker is not None:
            worker.cancel()
            with pytest.raises(asyncio.CancelledError):
                await worker


@pytest.mark.parametrize('error_type', [OSError, ValueError, backends.UnsupportedCapability])
async def test_unmarked_start_error_keeps_reservation(configured, monkeypatch, git_repo, fake_backend_clis, error_type):
    seen = []
    async def reject(*args, **kwargs):
        seen.append(kwargs['task_id'])
        raise error_type('unproven refusal')
    monkeypatch.setattr(server._reg(), 'start', reject)
    monkeypatch.setattr(notifications, 'discard_unstarted', lambda *a: pytest.fail('ambiguous cleanup'))
    with pytest.raises(MCPError, match='inspect task_id') as error:
        await server.start_task('test', str(git_repo), backend='codex', notify='hook')
    assert seen[0] in str(error.value)
    assert 'unproven refusal' not in str(error.value)
    assert notifications.status(server._reg().log_dir, seen[0])[0]['state'] == 'pending'


async def test_spawned_process_persist_failure_keeps_reservation(configured, monkeypatch, git_repo, fake_backend_clis):
    registry = server._reg()
    spawned = []
    reserved = []
    register = notifications.subscribe
    def reserve(*args, **kwargs):
        reserved.append(args[1])
        return register(*args, **kwargs)
    async def fake_exec(*args, **kwargs):
        proc = SimpleNamespace(pid=424242)
        spawned.append(proc)
        return proc
    def fail_persist(task):
        assert task.proc is spawned[0]
        raise OSError('storage unavailable after spawn')
    monkeypatch.setattr(notifications, 'subscribe', reserve)
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', fake_exec)
    monkeypatch.setattr(registry, 'persist', fail_persist)
    monkeypatch.setattr(notifications, 'discard_unstarted', lambda *a: pytest.fail('live process cleanup'))
    with pytest.raises(MCPError, match='inspect task_id') as error:
        await server.start_task('test', str(git_repo), backend='codex', notify='hook')
    assert len(spawned) == 1
    assert reserved[0] in str(error.value)
    assert 'no task was started' not in str(error.value)
    assert registry.get(reserved[0]) is None
    assert not store.record_path(registry.log_dir, reserved[0]).exists()
    assert notifications.status(registry.log_dir, reserved[0])[0]['state'] == 'pending'


async def test_notification_status_absent_does_not_create_storage_or_worker(monkeypatch):
    registry = server._reg()
    monkeypatch.delenv('PB_NOTIFY_DESTINATIONS', raising=False)
    assert await server.get_task_notifications('absent') == []
    assert not (registry.log_dir.parent / 'notifications.sqlite3').exists()
    assert getattr(registry, '_notification_worker', None) is None


async def test_unsubscribe_absent_does_not_create_storage_or_future_worker(monkeypatch):
    registry = server._reg()
    monkeypatch.delenv('PB_NOTIFY_DESTINATIONS', raising=False)
    assert await server.unsubscribe('absent') == []
    assert not (registry.log_dir.parent / 'notifications.sqlite3').exists()
    monkeypatch.setattr(server, '_registry', None)
    monkeypatch.setattr(server, 'TaskRegistry', lambda: registry)
    monkeypatch.setattr(registry, 'start_maintenance', lambda: None)
    assert server._reg() is registry
    assert getattr(registry, '_notification_worker', None) is None
    assert not (registry.log_dir.parent / 'notifications.sqlite3').exists()


async def test_cancel_during_reservation_waits_for_commit_then_cleans(configured, monkeypatch, git_repo, fake_backend_clis):
    registry = server._reg()
    entered = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    seen = []
    register = notifications.subscribe
    def blocked_register(*args, **kwargs):
        seen.append(args[1])
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return register(*args, **kwargs)
    monkeypatch.setattr(notifications, 'subscribe', blocked_register)
    monkeypatch.setattr(registry, 'start', lambda *a, **k: pytest.fail('spawned'))
    pending = asyncio.create_task(server.start_task('test', str(git_repo), backend='codex', notify='hook'))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        pending.cancel()
        await asyncio.sleep(0)
        pending.cancel()  # A repeated cancellation cannot abandon the committing thread.
        await asyncio.sleep(0)
        assert not pending.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert notifications.status(registry.log_dir, seen[0]) == []
    assert seen[0] not in notifications.pinned_tasks(registry.log_dir)


@pytest.mark.parametrize('marked', [True, False])
async def test_cancel_during_start_cleans_only_proven_unstarted(configured, monkeypatch, git_repo, fake_backend_clis, marked):
    registry = server._reg()
    entered = asyncio.Event()
    seen = []
    async def interrupted_start(*args, **kwargs):
        seen.append(kwargs['task_id'])
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError as error:
            if marked:
                error.polybridge_not_started = True
            raise
    monkeypatch.setattr(registry, 'start', interrupted_start)
    if not marked:
        monkeypatch.setattr(notifications, 'discard_unstarted', lambda *a: pytest.fail('ambiguous cleanup'))
    pending = asyncio.create_task(server.start_task('test', str(git_repo), backend='codex', notify='hook'))
    await asyncio.wait_for(entered.wait(), 2)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    rows = notifications.status(registry.log_dir, seen[0])
    if marked:
        assert rows == []
    else:
        assert rows[0]['state'] == 'pending'


async def test_repeated_cancellation_waits_for_reservation_cleanup(configured, monkeypatch, git_repo, fake_backend_clis):
    registry = server._reg()
    entered = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    seen = []
    discard = notifications.discard_unstarted
    def blocked_discard(*args):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return discard(*args)
    async def reject(*args, **kwargs):
        seen.append(kwargs['task_id'])
        error = asyncio.CancelledError()
        error.polybridge_not_started = True
        raise error
    monkeypatch.setattr(registry, 'start', reject)
    monkeypatch.setattr(notifications, 'discard_unstarted', blocked_discard)
    pending = asyncio.create_task(server.start_task('test', str(git_repo), backend='codex', notify='hook'))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        pending.cancel()
        await asyncio.sleep(0)
        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert notifications.status(registry.log_dir, seen[0]) == []
