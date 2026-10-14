"""Caller-visible request validation and read-only permission previews."""
import json
from unittest.mock import AsyncMock

import pytest
from mcp import Client, MCPError

from polybridge import backends, ctl, server
from polybridge.backends.codex import EXTRA_PATHS_VERSION
from polybridge.backends.base import nested_enforcement_violation
from polybridge.extra_paths import normalize_paths


@pytest.fixture
def qualified(monkeypatch, fake_backend_clis):
    # Qualification still resolves/fingerprints a CLI, even with its version stubbed.
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: EXTRA_PATHS_VERSION)
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'darwin')


async def test_preview_exposes_effective_scopes_without_task_or_scratch(git_repo, tmp_path, qualified):
    grant = tmp_path / 'output'
    grant.mkdir()
    registry = server._reg()
    result = await server.preview_task_permissions(str(git_repo), backend='codex', extra_writable_paths=[str(grant)])
    assert result['extra_writable_paths'][0]['requested'] == str(grant)
    assert result['extra_writable_paths'][0]['path'] == str(grant.resolve())
    assert result['enforcement']['writable_roots'] == ['the working directory', str(grant.resolve())]
    assert result['enforcement']['network_access'] == 'blocked'
    assert result['enforcement']['publish_attempts_allowed_by_polybridge'] is False
    assert not registry._tasks
    assert not registry.log_dir.exists()


async def test_start_passes_explicit_request_to_registry(git_repo, tmp_path, monkeypatch, fake_backend_clis):
    request = [str(tmp_path)]
    task = type('Task', (), {'enforcement': {}, 'brief': lambda self: {'task_id': 'fixture'}})()
    start = AsyncMock(return_value=task)
    monkeypatch.setattr(server._reg(), 'start', start)
    await server.start_task('Build', str(git_repo), backend='codex', extra_writable_paths=request)
    assert start.call_args.kwargs['extra_writable_paths'] == request


async def test_workflow_rejects_grants_before_dispatch(git_repo, monkeypatch):
    launch = AsyncMock()
    monkeypatch.setattr(server, 'start_workflow', launch)
    with pytest.raises(MCPError, match='unavailable for workflow'):
        await server.start_task('Build', str(git_repo), workflow='fixture', extra_writable_paths=[])
    launch.assert_not_called()


async def test_mcp_refuses_non_string_paths(git_repo):
    async with Client(server.mcp) as client:
        result = await client.call_tool('preview_task_permissions', {
            'repo_path': str(git_repo), 'backend': 'codex', 'extra_writable_paths': [42],
        })
    assert result.is_error


async def test_unsupported_preview_is_specific(git_repo, fake_backend_clis):
    with pytest.raises(MCPError, match='not certified'):
        await server.preview_task_permissions(str(git_repo), backend='claude', extra_writable_paths=[str(git_repo)])


def test_cli_preview_and_repeated_grants(git_repo, tmp_path, qualified, capsys):
    grant = tmp_path / 'output'
    grant.mkdir()
    code = ctl.main(['permission-preview', '--repo', str(git_repo), '--backend', 'codex',
                     '--extra-writable-path', str(grant), '--extra-writable-path', str(git_repo), '--json'])
    assert code == 0
    result = json.loads(capsys.readouterr().out)['result']
    assert [item['requested'] for item in result['extra_writable_paths']] == [str(grant), str(git_repo)]


def test_cli_workflow_rejects_grants_without_detach(git_repo, monkeypatch, capsys):
    def forbidden(*args):
        pytest.fail('unsupported request dispatched a workflow')
    monkeypatch.setattr(ctl, '_cmd_workflow', forbidden)
    assert ctl.main(['run', '--repo', str(git_repo), '--workflow', 'fixture', '--prompt', 'Build',
                     '--extra-writable-path', str(git_repo), '--json']) == 1
    assert 'unavailable for workflow' in capsys.readouterr().out


def test_nested_enforcement_reports_actual_roots_and_allows_typed_narrowing(git_repo, tmp_path, qualified):
    backend = backends.get('codex')
    directory = tmp_path / 'shared'
    directory.mkdir()
    exact = directory / 'file'
    exact.write_text('x')
    parent = backend.extra_path_enforcement(backend.enforcement('write_in_repo'), normalize_paths([str(directory)]), git_repo)
    child = backend.extra_path_enforcement(backend.enforcement('write_in_repo'), normalize_paths([str(exact)]), git_repo)
    arguments = dict(parent_backend='codex', child_backend='codex', parent_repo=str(git_repo), child_repo=str(git_repo))
    assert nested_enforcement_violation(parent.as_dict(), child, **arguments) is None
    assert nested_enforcement_violation(child.as_dict(), parent, **arguments)[0] == 'extra_writable_paths'
    assert str(directory.resolve()) in parent.writable_roots


def test_nested_enforcement_rejects_legacy_temp_widening(git_repo, tmp_path, qualified):
    backend = backends.get('codex')
    parent = backend.extra_path_enforcement(backend.enforcement('write_in_repo'), normalize_paths([str(tmp_path)]), git_repo)
    violation = nested_enforcement_violation(parent.as_dict(), backend.enforcement('write_in_repo'),
        parent_backend='codex', child_backend='codex', parent_repo=str(git_repo), child_repo=str(git_repo))
    assert violation[0] == 'writable_roots'


@pytest.mark.parametrize('notify', [None, 'hook'])
@pytest.mark.parametrize('invalid', ['relative', 'missing', 'duplicate', 'oversized'])
async def test_start_invalid_grants_are_actionable_before_spawn(
    git_repo, tmp_path, monkeypatch, fake_backend_clis, qualified, notify, invalid,
):
    from polybridge import notifications
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'hook': {'url': 'https://example.com/hook'}}))
    monkeypatch.setattr(server, '_start_notification_worker', lambda _: None)
    registry = server._reg()
    spawn = AsyncMock()
    monkeypatch.setattr(registry, '_spawn', spawn)
    requests = {'relative': ['relative'], 'missing': [str(tmp_path / 'missing')],
                'duplicate': [str(tmp_path), str(tmp_path)], 'oversized': [str(tmp_path)] * 17}
    messages = {'relative': 'absolute strings', 'missing': 'must exist',
                'duplicate': 'duplicate canonical', 'oversized': 'at most 16'}
    with pytest.raises(MCPError, match=messages[invalid]) as error:
        await server.start_task('Build', str(git_repo), backend='codex',
                                extra_writable_paths=requests[invalid], notify=notify)
    assert error.value.code == -32602
    spawn.assert_not_called()
    assert not registry._tasks
    assert not notifications.pinned_tasks(registry.log_dir)


async def test_start_validation_cleanup_failure_preserves_reserved_identity(
    git_repo, monkeypatch, fake_backend_clis, qualified,
):
    from polybridge import notifications
    monkeypatch.setenv('PB_NOTIFY_DESTINATIONS', json.dumps({'hook': {'url': 'https://example.com/hook'}}))
    monkeypatch.setattr(server, '_start_notification_worker', lambda _: None)
    reserved = []
    subscribe = notifications.subscribe
    def reserve(*args, **kwargs):
        reserved.append(args[1])
        return subscribe(*args, **kwargs)
    monkeypatch.setattr(notifications, 'subscribe', reserve)
    monkeypatch.setattr(notifications, 'discard_unstarted', lambda *args: False)
    with pytest.raises(MCPError, match='inspect task_id') as error:
        await server.start_task('Build', str(git_repo), backend='codex',
                                extra_writable_paths=['relative'], notify='hook')
    assert reserved[0] in str(error.value)
    assert notifications.status(server._reg().log_dir, reserved[0])[0]['state'] == 'pending'
    assert not server._reg()._tasks


@pytest.mark.parametrize('recovered', [False, True])
@pytest.mark.parametrize('alter', ['replaced', 'deleted', 'certificate', 'enforcement'])
async def test_resume_saved_authority_failure_is_actionable_before_spawn(
    git_repo, tmp_path, monkeypatch, qualified, recovered, alter,
):
    from dataclasses import replace
    from types import SimpleNamespace
    from polybridge import store
    from polybridge.extra_paths import serialize_paths
    registry = server._reg()
    backend = backends.get('codex')
    grant = tmp_path / 'exact'
    grant.write_text('original')
    grants = normalize_paths([str(grant)])
    enforcement = backend.extra_path_enforcement(backend.enforcement('write_in_repo'), grants, git_repo).as_dict()
    record = store.TaskRecord(task_id='parent', backend='codex', session_id='session',
        repo_path=str(git_repo), started_at='2026-10-14T00:00:00Z', status='completed', exit_code=0,
        extra_writable_paths=serialize_paths(grants), extra_path_certification=backend.extra_path_support()['certification'],
        enforcement=enforcement)
    if alter == 'replaced':
        replacement = tmp_path / 'replacement'
        replacement.write_text('new')
        replacement.replace(grant)
    elif alter == 'deleted':
        grant.unlink()
    elif alter == 'certificate':
        record = replace(record, extra_path_certification='corrupt')
    else:
        record = replace(record, enforcement={})
    saved = record.extra_writable_paths.copy()
    if recovered:
        store.write_landed(registry.log_dir, record)
    else:
        registry._tasks['parent'] = SimpleNamespace(**(record.__dict__ | {'repo_path': git_repo, 'finished': True}))
    spawn = AsyncMock()
    monkeypatch.setattr(registry, '_spawn', spawn)
    with pytest.raises(MCPError, match='extra writable path') as error:
        await server.resume_task('parent', 'Continue')
    assert error.value.code == -32602
    spawn.assert_not_called()
    assert record.extra_writable_paths == saved
    assert set(registry._tasks) == (set() if recovered else {'parent'})


@pytest.mark.parametrize('recovered', [False, True])
async def test_resume_unmarked_value_error_is_not_translated(git_repo, monkeypatch, recovered):
    from types import SimpleNamespace
    from polybridge import store
    registry = server._reg()
    record = store.TaskRecord(task_id='parent', backend='codex', session_id='session',
        repo_path=str(git_repo), started_at='2026-10-14T00:00:00Z', status='completed', exit_code=0)
    if recovered:
        store.write_landed(registry.log_dir, record)
    else:
        registry._tasks['parent'] = SimpleNamespace(**(record.__dict__ | {'finished': True}))
    monkeypatch.setattr(registry, 'resume_record' if recovered else 'resume', AsyncMock(side_effect=ValueError('late failure')))
    with pytest.raises(ValueError, match='late failure'):
        await server.resume_task('parent', 'Continue')
