"""MCP notification authorization and pre-spawn durability; no model calls."""
import json
from types import SimpleNamespace

import pytest
from mcp import MCPError

from polybridge import notifications, server, store


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
        raise OSError('cannot spawn')
    monkeypatch.setattr(server._reg(), 'start', reject)
    with pytest.raises(MCPError, match='inspect task_id'):
        await server.start_task('test', str(git_repo), backend='codex', notify='hook')
    assert notifications.status(server._reg().log_dir, seen[0]) == []

async def test_cleanup_failure_keeps_recovery_identity(configured, monkeypatch, git_repo, fake_backend_clis):
    seen = []
    async def reject(*args, **kwargs):
        seen.append(kwargs['task_id'])
        raise OSError('cannot spawn')
    monkeypatch.setattr(server._reg(), 'start', reject)
    monkeypatch.setattr(notifications, 'discard_unstarted', lambda *a: (_ for _ in ()).throw(OSError('disk error')))
    with pytest.raises(MCPError, match='inspect task_id') as error:
        await server.start_task('test', str(git_repo), backend='codex', notify='hook')
    assert seen[0] in str(error.value)
    assert notifications.status(server._reg().log_dir, seen[0])[0]['state'] == 'pending'
