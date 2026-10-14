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
def qualified(monkeypatch):
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


async def test_start_passes_explicit_request_to_registry(git_repo, tmp_path, monkeypatch):
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


async def test_unsupported_preview_is_specific(git_repo):
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
