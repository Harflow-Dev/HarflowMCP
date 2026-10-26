"""Canonical publication intent and unrestricted extra-path disclosure."""
from dataclasses import replace
from types import SimpleNamespace

import pytest

from harflow_mcp.backends.base import Enforcement, nested_enforcement_violation
from harflow_mcp.task_documents import qualify_enforcement
from harflow_mcp import tasks, server
from harflow_mcp.extra_paths import normalize_paths, serialize_paths
from test_extra_path_tasks import Certified, _state_logs, owned_state

LEGACY = 'publish_attempts_allowed_by_' + 'poly' + 'bridge'


def comparison(parent, child):
    return nested_enforcement_violation(parent, child, parent_backend='same', child_backend='same', parent_repo='/repo', child_repo='/repo')


def test_publication_alias_compatibility_and_conflicts():
    enforcement = Certified().enforcement('write_in_repo')
    output = enforcement.as_dict()
    assert output['publish_attempts_allowed'] == output[LEGACY] == enforcement.publish_attempts_allowed
    old = {k: v for k, v in output.items() if k != 'publish_attempts_allowed'}
    new = {k: v for k, v in output.items() if k != LEGACY}
    for value in (old, new, output):
        qualify_enforcement(value)
        assert comparison(value, value) is None
    bad = {**output, 'publish_attempts_allowed': not output[LEGACY]}
    with pytest.raises(ValueError, match='Contradictory'):
        qualify_enforcement(bad)
    assert comparison(bad, output)[0] == 'publish_attempts_allowed'
    assert comparison(output, bad)[0] == 'publish_attempts_allowed'


async def test_unrestricted_paths_preview_skips_certification(tmp_path, monkeypatch, owned_state):
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    backend = Certified()
    def unexpected(*args, **kwargs):
        raise AssertionError('bounded certification must not run')
    monkeypatch.setattr(backend, 'qualify_extra_writable_paths', unexpected)
    monkeypatch.setattr(backend, 'extra_path_enforcement', unexpected)
    result = await registry.preview_permissions(tmp_path, backend=backend, freedom='unrestricted', extra_writable_paths=[str(tmp_path)])
    enforcement = result['enforcement']
    assert result['extra_writable_paths'] == serialize_paths(normalize_paths([str(tmp_path)]))
    assert result['extra_path_certification'] is None
    assert enforcement['extra_path_certification'] is None
    assert enforcement['writable_roots'] == list(backend.enforcement('unrestricted').writable_roots)
    assert any('no-op' in caveat for caveat in enforcement['caveats'])
    saved = SimpleNamespace(freedom='unrestricted', extra_writable_paths=result['extra_writable_paths'], extra_path_certification=None, enforcement=enforcement)
    assert registry._saved_extra_paths(saved, backend) == normalize_paths([str(tmp_path)])
    saved.freedom = 'write_in_repo'
    with pytest.raises(ValueError, match='certification'):
        registry._saved_extra_paths(saved, backend)
    with pytest.raises(ValueError):
        await registry.preview_permissions(tmp_path, backend=backend, freedom='unrestricted', extra_writable_paths=[str(tmp_path / 'missing')])


@pytest.mark.parametrize('name', ['_ON_DEFAULT_BRANCH_PUBLISH_NOTICE', '_ON_DEFAULT_BRANCH_NETWORK_BLOCKED_NOTICE'])
def test_branch_notices_disclose_checkout_only(name):
    notice = getattr(tasks, name)
    assert 'eventual commit or publication branch is not established' in notice.replace('"', '')
    assert 'directly against it' not in notice


async def test_mcp_discovery_has_extra_path_capabilities():
    from mcp import Client
    from harflow_mcp import backends
    async with Client(server.mcp) as client:
        result = await client.call_tool('list_backends', {})
    payload = result.structured_content["result"]
    for item in payload:
        assert item['capabilities']['extra_writable_paths'] == backends.BACKENDS[item['backend']].extra_path_support()


async def test_unrestricted_launch_paths_are_noop_and_cannot_weaken_parent(tmp_path, monkeypatch, owned_state):
    from harflow_mcp.backends.base import Invocation
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    backend = Certified()
    def unexpected(*args, **kwargs):
        raise AssertionError('bounded qualifier must not run')
    monkeypatch.setattr(backend, 'qualify_extra_writable_paths', unexpected)
    monkeypatch.setattr(backend, 'assert_extra_writable_paths', unexpected)
    monkeypatch.setattr(tasks, '_branch_disclosure_warranted', lambda _: False)
    monkeypatch.setattr(tasks, 'get_backend', lambda _: backend)
    grants = normalize_paths([str(tmp_path)])
    task = await registry._spawn(Invocation(['/bin/echo', '{}']), extra_path_grants=grants,
        backend=backend, prompt='Task', repo_path=tmp_path, session_id=None,
        freedom='unrestricted', max_turns=None, model=None, reasoning_effort=None)
    await task.done.wait()
    assert task.status == 'completed'
    assert task.extra_path_certification is None
    assert registry._saved_extra_paths(task, backend) == grants
    parent = Enforcement(freedom='write_in_repo', mechanism='bounded', os_enforced=True,
                         writes_confined=True, writable_roots=('the working directory',)).as_dict()
    assert comparison(parent, task.enforcement) is not None


async def test_recovered_unrestricted_resume_keeps_noop_paths(tmp_path, monkeypatch, owned_state):
    from harflow_mcp import backends, store
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    backend = backends.get('claude')
    grants = normalize_paths([str(tmp_path)])
    enforcement = registry._extra_enforcement(backend, backend.enforcement('unrestricted'), grants, tmp_path)
    source = store.TaskRecord(task_id='unrestricted-source', backend='claude', session_id='conversation',
        repo_path=str(tmp_path), freedom='unrestricted', status='completed', exit_code=0,
        started_at='2026-10-26T00:00:00+00:00', extra_writable_paths=serialize_paths(grants),
        enforcement=enforcement.as_dict())
    store.write(registry.log_dir, source)
    def unexpected(*args, **kwargs):
        pytest.fail('bounded certification must not run for recovered unrestricted paths')
    monkeypatch.setattr(backend, 'extra_path_enforcement', unexpected)
    async def launch(**kwargs):
        assert kwargs['grants'] == grants
        assert kwargs['session_id'] == source.session_id
        return kwargs
    monkeypatch.setattr(registry, '_launch_resume', launch)
    result = await registry.resume_record(source, 'continue', task_id='unrestricted-successor')
    assert result['parent_task_id'] == source.task_id
