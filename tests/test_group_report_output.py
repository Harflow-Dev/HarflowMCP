"""Engine wiring for qualified group-scoped report output (backend security is tested separately)."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import json
import sys

import pytest
from harflow_mcp import backends, group_report_output as reports, shared_scratch, store, tasks
from harflow_mcp.backends.base import Invocation, UnsupportedCapability
from test_extra_path_tasks import Certified, _state_logs, owned_state


class ReportBackend(Certified):
    def __init__(self):
        self.configured = []

    def group_report_output_support(self):
        return {'supported': True}

    def qualify_group_report_output(self):
        return {'ok': True, 'feature': 'group_report_output'}

    def preview_group_report_output(self, invocation, path, freedom, network=None):
        assert freedom == "read_only"

    def configure_group_report_output(self, invocation, path, freedom, network=None):
        assert freedom == 'read_only' and path.is_dir()
        self.configured.append(path)
        return replace(invocation, group_report_directory=str(path), group_report_qualification=self.qualify_group_report_output())

    def assert_group_report_output(self, invocation, path, freedom, network=None):
        assert invocation.group_report_directory == str(path)
        assert invocation.group_report_qualification['ok']

    def group_report_output_enforcement(self, value, path):
        # This mock verifies engine reporting, not a vendor filesystem boundary.
        return replace(value, writable_roots=(str(path),), writes_confined=True, os_enforced=True, group_report_output=True)


@pytest.fixture
def report_backend(monkeypatch):
    backend = ReportBackend()
    monkeypatch.setattr(tasks, 'get_backend', lambda _: backend)
    monkeypatch.setattr(tasks, '_branch_disclosure_warranted', lambda _: False)
    monkeypatch.setattr('harflow_mcp.git_environment.auth_status', lambda: 'unknown')
    return backend


async def test_preview_names_exact_group_without_allocating(tmp_path, owned_state, report_backend):
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    result = await registry.preview_permissions(tmp_path, backend=report_backend, freedom='read_only',
        group='review / ../ sibling', group_report_output=True)
    path = shared_scratch.directory(registry.log_dir, 'group', 'review / ../ sibling')
    assert result['group_report_directory'] == str(path)
    assert result['enforcement']['writable_roots'] == [str(path)]
    assert result['enforcement']['group_report_output'] is True
    assert not path.exists()
    assert report_backend.configured == []


async def test_shared_allocator_grants_only_group_and_child_can_create_update_report(tmp_path, owned_state, report_backend):
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    # The child directly verifies environment/assignment delivery. This is not a sandbox probe.
    code = "import os,pathlib; p=pathlib.Path(os.environ['HARFLOW_GROUP_SCRATCH'])/('report-'+os.environ['HARFLOW_TASK_ID']+'.md'); p.write_text('first'); p.write_text('updated'); print('{}')"
    task = await registry._spawn(Invocation([sys.executable, '-c', code]), backend=report_backend,
        prompt='Verbatim  caller prose', repo_path=tmp_path, session_id=None, freedom='read_only',
        max_turns=None, model=None, reasoning_effort=None, group='reviews', group_report_output=True)
    await task.done.wait()
    path = shared_scratch.directory(registry.log_dir, 'group', 'reviews')
    assert task.prompt == 'Verbatim  caller prose'
    assert task.enforcement['writable_roots'] == [str(path)]
    assert (path / f'report-{task.task_id}.md').read_text() == 'updated'
    assert task.scratch['access']['group'] == shared_scratch.WRITABLE
    assert task.scratch['access']['chain'] == shared_scratch.SANDBOX_READ_ONLY
    assert task.scratch.get('task') is None
    saved = store.read(registry.log_dir, task.task_id)
    assert reports.effective(None, saved)
    assert saved.group == 'reviews'


@pytest.mark.parametrize('override', [None, False])
async def test_ordinary_grouped_read_only_does_not_receive_write_exception(tmp_path, owned_state, report_backend, override):
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    task = await registry._spawn(Invocation(['/bin/echo', '{}']), backend=report_backend,
        prompt='Review', repo_path=tmp_path, session_id=None, freedom='read_only',
        max_turns=None, model=None, reasoning_effort=None, group='reviews', group_report_output=override)
    await task.done.wait()
    assert report_backend.configured == []
    assert 'group_report_output' not in task.enforcement


@pytest.mark.parametrize('changes,match', [
    ({'group': None}, 'nonempty group'),
    ({'freedom': 'write_in_repo'}, 'read_only'),
    ({'caller_approvals': True}, 'without caller approvals'),
    ({'native': True}, 'without caller approvals'),
    ({'builder': True}, 'without caller approvals'),
    ({'extra_paths': True}, 'without caller approvals'),
])
def test_invalid_combinations_fail_explicitly(tmp_path, report_backend, changes, match):
    kwargs = dict(backend=report_backend, log_dir=_state_logs(tmp_path), repo=tmp_path, group='reviews', freedom='read_only')
    kwargs.update(changes)
    with pytest.raises(ValueError, match=match):
        reports.validate(True, **kwargs)


@pytest.mark.parametrize('name', ['claude', 'opencode', 'vibe', 'antigravity'])
def test_unqualified_backends_refuse_report_grant(tmp_path, name):
    with pytest.raises(UnsupportedCapability, match='Omit the option'):
        reports.validate(True, backend=backends.get(name), log_dir=_state_logs(tmp_path), repo=tmp_path, group='reviews', freedom='read_only')


def test_group_path_refuses_repo_overlap_and_existing_symlink(tmp_path):
    logs = _state_logs(tmp_path)
    path = shared_scratch.directory(logs, 'group', 'reviews')
    with pytest.raises(ValueError, match='overlap'):
        reports.path_for(logs, 'reviews', path)
    path.parent.mkdir(parents=True)
    path.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match='symlink'):
        reports.path_for(logs, 'reviews', tmp_path)


async def test_confined_caller_without_exact_group_scope_cannot_receive_grant(tmp_path, owned_state, report_backend, monkeypatch):
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    monkeypatch.setattr(registry, '_caller_allows_shared', lambda *args: False)
    with pytest.raises(ValueError, match='Caller authority'):
        await registry._spawn(Invocation(['/bin/echo', '{}']), backend=report_backend,
            prompt='Review', repo_path=tmp_path, session_id=None, freedom='read_only', max_turns=None,
            model=None, reasoning_effort=None, group='reviews', group_report_output=True, spawned_by='parent')
    assert report_backend.configured == []
    assert registry.list() == []


@pytest.mark.parametrize('override,expected', [(None, True), (False, False)])
async def test_recovered_resume_preserves_or_disables_opt_in(tmp_path, owned_state, report_backend, monkeypatch, override, expected):
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    source = store.TaskRecord(task_id='source', backend=report_backend.name, session_id='conversation',
        repo_path=str(tmp_path), freedom='read_only', group='reviews', status='completed', exit_code=0,
        started_at='2026-10-28T00:00:00+00:00', enforcement={**report_backend.enforcement('read_only').as_dict(), 'group_report_output': True})
    store.write(registry.log_dir, source)
    launch = AsyncMock(side_effect=lambda **kwargs: kwargs)
    monkeypatch.setattr(registry, '_launch_resume', launch)
    result = await registry.resume_record(source, 'Continue review', group_report_output=override)
    assert result['group_report_output'] is expected
    assert result['group'] == 'reviews'


def test_effective_setting_is_strict_and_old_records_stay_off():
    assert not reports.effective(None, SimpleNamespace(enforcement={}))
    assert reports.effective(None, SimpleNamespace(enforcement={'group_report_output': True}))
    assert not reports.effective(False, SimpleNamespace(enforcement={'group_report_output': True}))
    with pytest.raises(ValueError, match='boolean'):
        reports.effective('true')


def test_supervision_preserves_nested_observations_at_json_boundary():
    from harflow_mcp.task_supervision import invocation_request
    from harflow_mcp.task_supervisor import restore_group_report_observations
    original = Invocation(['/bin/echo', '{}'], group_report_directory='/scope',
        group_report_identity=('/scope', 1, 2), group_report_runtime=('cli', (3, 4)),
        group_report_configuration=(('/config', ('missing',)), ('/other', (1, (2, 3)))),
        group_report_qualification={'ok': True, 'feature': 'group_report_output'})
    prepared = json.loads(json.dumps(invocation_request(original)))
    restore_group_report_observations(prepared)
    for field in ('group_report_identity', 'group_report_runtime', 'group_report_configuration'):
        assert prepared[field] == getattr(original, field)
    assert prepared['group_report_qualification'] == original.group_report_qualification


async def test_preview_rejects_launch_incompatible_configuration(tmp_path, owned_state, report_backend, monkeypatch):
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    def incompatible(*args):
        raise UnsupportedCapability('inherited legacy sandbox selector is unsupported')
    monkeypatch.setattr(report_backend, 'preview_group_report_output', incompatible)
    with pytest.raises(UnsupportedCapability, match='legacy sandbox'):
        await registry.preview_permissions(tmp_path, backend=report_backend, freedom='read_only',
            group='reviews', group_report_output=True)
    assert report_backend.configured == []


@pytest.mark.parametrize('changes,match', [({'network': True}, 'network disabled'), ({'group': None}, 'nonempty group')])
async def test_preview_cached_only_does_not_hide_invalid_request_shape(tmp_path, owned_state, report_backend, changes, match):
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    kwargs = dict(backend=report_backend, freedom='read_only', group='reviews', group_report_output=True, qualification_only=True)
    kwargs.update(changes)
    with pytest.raises(ValueError, match=match):
        await registry.preview_permissions(tmp_path, **kwargs)


async def test_continuation_start_inherits_saved_opt_in_and_group(tmp_path, owned_state, report_backend, monkeypatch):
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    source = store.TaskRecord(task_id='continuation-source', backend=report_backend.name, session_id=None,
        repo_path=str(tmp_path), freedom='read_only', group='reviews', status='completed', exit_code=0,
        started_at='2026-10-28T00:00:00+00:00', enforcement={**report_backend.enforcement('read_only').as_dict(), 'group_report_output': True})
    store.write(registry.log_dir, source)
    capture = AsyncMock(side_effect=lambda invocation, **kwargs: kwargs)
    monkeypatch.setattr(registry, '_spawn', capture)
    result = await registry.start('Continue', tmp_path, backend=report_backend, freedom='read_only',
        group='reviews', continuation_of_task_id=source.task_id)
    assert result['group_report_output'] is True
    assert result['group'] == 'reviews'
    with pytest.raises(ValueError, match='cannot change'):
        await registry.start('Continue', tmp_path, backend=report_backend, freedom='read_only',
            group='different-group', continuation_of_task_id=source.task_id)


@pytest.mark.parametrize('entrypoint', ['prepare', 'attach', 'locked'])
async def test_takeover_refuses_scoped_output_without_opening_attempt(tmp_path, owned_state, monkeypatch, entrypoint):
    from harflow_mcp import control, takeover
    logs = _state_logs(tmp_path)
    source = store.TaskRecord(task_id='takeover-source', backend='codex', session_id='conversation',
        repo_path=str(tmp_path), freedom='read_only', group='reviews', status='completed', exit_code=0,
        started_at='2026-10-28T00:00:00+00:00', enforcement={**backends.get('codex').enforcement('read_only').as_dict(), 'group_report_output': True})
    store.write(logs, source)
    monkeypatch.setattr(takeover, '_refuse_agents', lambda *args: None)
    with pytest.raises(control.TakeoverRefused, match='headless resume or continue') as error:
        if entrypoint == 'prepare':
            await takeover.take_over(logs, source.task_id, registry_factory=lambda: None)
        elif entrypoint == 'attach':
            takeover.attach(logs, source.task_id, pid=123456)
        else:
            takeover._open_attempt(logs, source.task_id, {}, source.session_id)
    assert error.value.code == 'group_report_output_unsupported'
    assert not list(logs.glob('*.takeover.*'))


def test_scoped_output_suppresses_ambient_resume_command(tmp_path, owned_state):
    from test_registry import make_task
    task = make_task(tmp_path, 'scoped-source', log_dir=_state_logs(tmp_path), finished=True)
    task.enforcement['group_report_output'] = True
    assert task.snapshot()['resume_command'] is None


def test_cached_qualification_discovery_includes_group_output():
    from harflow_mcp.qualification_plan import required_features
    assert required_features(group_report_output=True) == ('group_report_output',)
    assert required_features(group_report_output=False) == ()


def test_stored_scoped_output_suppresses_ambient_resume_command(tmp_path, owned_state):
    logs = _state_logs(tmp_path)
    source = store.TaskRecord(task_id='stored-scoped-source', backend='codex', session_id='conversation',
        repo_path=str(tmp_path), freedom='read_only', group='reviews', status='completed', exit_code=0,
        started_at='2026-10-28T00:00:00+00:00', enforcement={**backends.get('codex').enforcement('read_only').as_dict(), 'group_report_output': True})
    store.write(logs, source)
    assert store.snapshot(logs, source)['resume_command'] is None
