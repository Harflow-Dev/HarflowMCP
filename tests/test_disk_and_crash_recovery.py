"""Dead detached owners release only verified conversation locks; low disk refuses before spawn."""
import errno
from types import SimpleNamespace

import pytest

from harflow_mcp import disk_space, identity, state_ownership, store, task_supervision
from harflow_mcp.backends import get
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import SessionBusyError, TaskRegistry


@pytest.fixture
def registry(tmp_path):
    with state_ownership.state_session(resolve_layout(root=tmp_path / 'state')):
        result = TaskRegistry(log_dir=tmp_path / 'state' / 'tasks')
        result.log_dir.mkdir(exist_ok=True)
        yield result


def crashed_record(registry, tmp_path, index):
    record = store.TaskRecord(task_id=f'crash-{index}', backend='claude', session_id=f'session-{index}',
        repo_path=str(tmp_path), started_at='2026-10-26T00:00:00+00:00',
        freedom='read_only', status='running', pid=7000 + index,
        owner={'pid': 8000 + index, 'start_time': 'owner', 'markers': []},
        start_time='agent', markers=['test-agent'], enforcement=get('claude').enforcement('read_only').as_dict())
    store.write(registry.log_dir, record)
    info = registry.log_dir.stat()
    task_supervision._write(registry.log_dir / f'{record.task_id}.launch.json', {
        'format': task_supervision.FORMAT, 'product': 'harflow', 'v': 1,
        'product_version': task_supervision.PRODUCT_VERSION, 'task_id': record.task_id,
        'generation': 'a' * 32, 'request_sha256': 'b' * 64, 'backend': 'claude',
        'root': str(registry.log_dir.parent), 'directory_identity': [info.st_dev, info.st_ino],
        'state': 'spawned', 'owner': record.owner, 'session_id': record.session_id})
    return record


async def test_three_crashed_runs_resume_original_conversations(registry, tmp_path, monkeypatch):
    sources = [crashed_record(registry, tmp_path, i) for i in range(3)]
    monkeypatch.setattr(identity, 'identity_check', lambda _identity: 'dead')
    launches = []
    async def launch(**kwargs):
        launches.append(kwargs)
        return kwargs
    monkeypatch.setattr(registry, '_launch_resume', launch)
    for source in sources:
        # Observation is read-only: dead generations remain claimed until a mutation explicitly
        # admits recovery under the session lock.
        assert registry.session_has_live_run(source.session_id)
        assert task_supervision.read_launch(registry.log_dir, source.task_id)['state'] == 'spawned'
        await registry.resume_record(source, 'continue', task_id=f'resume-{source.task_id}')
    assert [value['session_id'] for value in launches] == [source.session_id for source in sources]
    assert [value['parent_task_id'] for value in launches] == [source.task_id for source in sources]
    assert all(task_supervision.read_launch(registry.log_dir, source.task_id)['state'] == 'settled' for source in sources)


@pytest.mark.parametrize('owner,agent', [('dead', 'alive'), ('dead', 'undecidable'), ('undecidable', 'dead'), ('alive', 'dead')])
async def test_live_or_uncertain_crashed_session_remains_busy(registry, tmp_path, monkeypatch, owner, agent):
    source = crashed_record(registry, tmp_path, 1)
    monkeypatch.setattr(identity, 'identity_check', lambda value: owner if value and value.get('pid') == source.owner['pid'] else agent)
    with pytest.raises(SessionBusyError, match='unverified launch/process liveness'):
        await registry.resume_record(source, 'continue', task_id='attempt')


def test_space_threshold_and_unknown_filesystem(tmp_path, monkeypatch):
    monkeypatch.setattr(disk_space.shutil, 'disk_usage', lambda _: SimpleNamespace(free=disk_space.MIN_FREE_BYTES))
    disk_space.check_launch_space(tmp_path / 'state', tmp_path)
    monkeypatch.setattr(disk_space.shutil, 'disk_usage', lambda _: SimpleNamespace(free=disk_space.MIN_FREE_BYTES - 1))
    with pytest.raises(disk_space.DiskSpaceError, match='Disk nearly full.*no agent was started'):
        disk_space.check_launch_space(tmp_path / 'state', tmp_path)
    def unavailable(_):
        raise OSError(errno.EIO, 'unavailable')
    monkeypatch.setattr(disk_space.shutil, 'disk_usage', unavailable)
    with pytest.raises(disk_space.DiskSpaceError, match='Cannot check available disk space'):
        disk_space.check_launch_space(tmp_path, tmp_path)


@pytest.mark.parametrize('mode', ['local', 'supervised'])
async def test_low_disk_start_and_resume_leave_no_launch_reservation(registry, tmp_path, monkeypatch, mode):
    registry.execution_mode = mode
    monkeypatch.setattr(disk_space.shutil, 'disk_usage', lambda _: SimpleNamespace(free=1))
    monkeypatch.setattr(task_supervision.subprocess, 'Popen', lambda *_args, **_kwargs: pytest.fail('must not spawn'))
    with pytest.raises(disk_space.DiskSpaceError, match='Disk nearly full'):
        await registry.start('inspect', tmp_path, backend=get('claude'), freedom='read_only', task_id='low-start')
    source = crashed_record(registry, tmp_path, 1)
    monkeypatch.setattr(identity, 'identity_check', lambda _: 'dead')
    with pytest.raises(disk_space.DiskSpaceError, match='Disk nearly full'):
        await registry.resume_record(source, 'continue', task_id='low-resume')
    assert task_supervision.read_launch(registry.log_dir, 'low-start') is None
    assert task_supervision.read_launch(registry.log_dir, 'low-resume') is None


def test_disk_exhaustion_preserves_last_usable_metadata(registry, tmp_path, monkeypatch):
    from dataclasses import replace
    source = crashed_record(registry, tmp_path, 1)
    def full(*args, **kwargs):
        raise OSError(errno.ENOSPC, 'No space left on device')
    monkeypatch.setattr(store.tempfile, 'NamedTemporaryFile', full)
    assert not store.write_landed(registry.log_dir, replace(source, status='failed'))
    assert store.last_write_error().code == 'task_unavailable'
    assert store.read(registry.log_dir, source.task_id).session_id == source.session_id
    assert store.read(registry.log_dir, source.task_id).status == 'running'


def test_missing_temporary_storage_is_actionable_and_not_started(tmp_path, monkeypatch):
    def missing():
        raise FileNotFoundError(errno.ENOENT, 'No usable temporary directory')
    monkeypatch.setattr(disk_space.tempfile, 'gettempdir', missing)
    with pytest.raises(disk_space.DiskSpaceError, match='free disk space') as error:
        disk_space.check_launch_space(tmp_path, tmp_path)
    assert error.value.harflow_not_started


@pytest.mark.parametrize('verdict,allowed', [('dead', True), ('alive', False), ('undecidable', False)])
async def test_public_resume_of_attached_crash(registry, tmp_path, monkeypatch, verdict, allowed):
    from harflow_mcp import server
    from mcp import MCPError
    monkeypatch.setattr(server, '_registry', registry)
    source = crashed_record(registry, tmp_path, 1)
    registry.attach(source.task_id)
    monkeypatch.setattr(identity, 'identity_check', lambda _: verdict)
    monkeypatch.setattr(store, 'record_process_alive', lambda _: verdict != 'dead')
    launched = []
    class Successor:
        enforcement = {}
        def brief(self):
            return {'task_id': 'resumed', 'session_id': source.session_id, 'parent_task_id': source.task_id}
    async def launch(**kwargs):
        launched.append(kwargs)
        return Successor()
    monkeypatch.setattr(registry, '_launch_resume', launch)
    if allowed:
        result = await server.resume_task(source.task_id, 'continue')
        assert result['session_id'] == source.session_id
        assert result['parent_task_id'] == source.task_id
        assert launched[0]['session_id'] == source.session_id
    else:
        with pytest.raises(MCPError, match='still'):
            await server.resume_task(source.task_id, 'continue')
        assert launched == []


@pytest.mark.parametrize('mismatch', ['missing', 'owner', 'session', 'reserved'])
def test_ambiguous_crash_launch_keeps_session_busy(registry, tmp_path, monkeypatch, mismatch):
    source = crashed_record(registry, tmp_path, 1)
    launch = task_supervision.read_launch(registry.log_dir, source.task_id)
    if mismatch == 'missing':
        store.record_path(registry.log_dir, source.task_id).unlink()
    else:
        changes = {'state': 'reserved'} if mismatch == 'reserved' else {mismatch: None} if mismatch == 'owner' else {'session_id': 'different-conversation'}
        task_supervision.mark(registry.log_dir, source.task_id, launch['generation'], **changes)
    monkeypatch.setattr(identity, 'identity_check', lambda _: 'dead')
    session = source.session_id if mismatch != 'session' else 'different-conversation'
    assert registry.session_has_live_run(session)


@pytest.mark.parametrize('low_index', [0, 1, 2])
def test_each_required_filesystem_is_checked(tmp_path, monkeypatch, low_index):
    import os
    from pathlib import Path
    paths = [tmp_path / name for name in ('state', 'checkout', 'temporary')]
    for path in paths:
        path.mkdir()
    devices = {str(path): 100 + index for index, path in enumerate(paths)}
    original_stat = Path.stat
    def stat(self, *args, **kwargs):
        value = original_stat(self, *args, **kwargs)
        if str(self) not in devices:
            return value
        fields = list(value)
        fields[2] = devices[str(self)]
        return os.stat_result(fields)
    monkeypatch.setattr(Path, 'stat', stat)
    monkeypatch.setattr(disk_space.tempfile, 'gettempdir', lambda: str(paths[2]))
    seen = []
    def usage(path):
        seen.append(path)
        return SimpleNamespace(free=0 if path == paths[low_index] else disk_space.MIN_FREE_BYTES)
    monkeypatch.setattr(disk_space.shutil, 'disk_usage', usage)
    with pytest.raises(disk_space.DiskSpaceError, match='Disk nearly full'):
        disk_space.check_launch_space(paths[0], paths[1])
    assert seen == paths[:low_index + 1]
