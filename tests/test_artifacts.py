"""Artifact references never read or own target contents."""
from dataclasses import replace
import json
import os
from pathlib import Path
import pytest
from polybridge import artifacts, identity, lineage, store, workflow_hooks


@pytest.fixture
def setup(tmp_path, monkeypatch):
    logs = tmp_path / 'bridge' / 'tasks'
    logs.mkdir(parents=True)
    repo = tmp_path / 'repo'
    repo.mkdir()
    record = store.TaskRecord('owner', 'codex', None, str(repo), '2026-01-01T00:00:00+00:00', pid=123, pgid=456, start_time='start')
    store.write(logs, record)
    monkeypatch.setattr(lineage, '_default_process_table', lambda: {os.getpid(): 123, 123: 1, 1: 0})
    monkeypatch.setattr(identity, 'identity_check', lambda _: 'alive')
    monkeypatch.delenv('PB_TASK_ID', raising=False)
    monkeypatch.delenv('PB_ROOT_TASK_ID', raising=False)
    return logs, repo, record


def test_reference_idempotency_and_observation(setup):
    logs, repo, record = setup
    target = repo / 'report.txt'
    target.write_text('first')
    item = artifacts.register(logs, 'report.txt', 'report', ' A\n note ')
    assert item == artifacts.register(logs, str(target), 'report', ' A\n note ')
    assert item['note'] == ' A\n note '
    page = artifacts.get(logs, record.task_id)
    assert page['items'][0]['availability']['state'] == 'available'
    target.write_text('second bigger')
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'changed'
    assert artifacts.register(logs, str(target), 'report', ' A\n note ')['artifact_id'] != item['artifact_id']
    target.unlink()
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'deleted'


def test_pre_epoch_file_timestamp_remains_readable(setup):
    logs, repo, record = setup
    target = repo / 'historical-report'
    target.write_text('preserved historical timestamp')
    os.utime(target, ns=(-1_000_000_000, -1_000_000_000))
    receipt = artifacts.register(logs, str(target), 'report')
    page = artifacts.get(logs, record.task_id)
    assert page['state'] == 'available'
    assert page['items'][0]['artifact_id'] == receipt['artifact_id']
    assert page['items'][0]['availability']['state'] == 'available'


def test_path_authority(setup):
    logs, repo, record = setup
    outside = repo.parent / 'outside'
    outside.write_text('secret')
    (repo / 'escape').symlink_to(outside)
    for path in ('missing', 'escape', str(outside), str(repo)):
        with pytest.raises((OSError, ValueError)):
            artifacts.register(logs, path, 'other')
    protected = repo / '.codex'
    protected.mkdir()
    (protected / 'config.toml').write_text('secret')
    with pytest.raises(ValueError, match='protected'):
        artifacts.register(logs, '.codex/config.toml', 'other')


def test_manifest_corrupt_symlink_fifo(setup):
    logs, repo, record = setup
    target = repo / 'report'
    target.write_text('hello')
    manifest = artifacts._path(logs, record.task_id)
    manifest.write_text('{')
    assert artifacts.get(logs, record.task_id)['state'] == 'unavailable'
    with pytest.raises(ValueError):
        artifacts.register(logs, str(target), 'report')
    manifest.unlink()
    manifest.symlink_to(target)
    assert artifacts.get(logs, record.task_id)['state'] == 'unavailable'
    with pytest.raises(OSError):
        artifacts.register(logs, str(target), 'report')
    manifest.unlink()
    os.mkfifo(manifest)
    assert artifacts.get(logs, record.task_id)['state'] == 'unavailable'


def test_paging_cursor_and_bounds(setup):
    logs, repo, record = setup
    target = repo / 'report'
    target.write_text('hello')
    for n in range(4):
        artifacts.register(logs, str(target), 'report', str(n))
    page = artifacts.get(logs, record.task_id, limit=2)
    assert page['has_more'] and len(page['items']) == 2
    assert len(artifacts.get(logs, record.task_id, page['next_cursor'], 2)['items']) == 2
    artifacts.register(logs, str(target), 'report', 'new')
    with pytest.raises(ValueError, match='stale'):
        artifacts.get(logs, record.task_id, page['next_cursor'])
    with pytest.raises(ValueError):
        artifacts.get(logs, record.task_id, limit=51)
    with pytest.raises(ValueError):
        artifacts.register(logs, str(target), 'other', 'x' * 4097)


def test_terminal_workflow_spoof_and_human(setup, monkeypatch):
    logs, repo, record = setup
    target = repo / 'report'
    target.write_text('hello')
    monkeypatch.setenv('PB_TASK_ID', 'another')
    with pytest.raises(ValueError, match='unverifiable'):
        artifacts.register(logs, str(target), 'report')
    monkeypatch.delenv('PB_TASK_ID')
    monkeypatch.setattr(artifacts, '_workflow_owner', lambda *a, **k: {'role': 'worker'})
    with pytest.raises(ValueError, match='Managed'):
        artifacts.get(logs, record.task_id)
    monkeypatch.setattr(artifacts, '_workflow_owner', lambda *a, **k: None)
    store.write(logs, replace(record, status='completed', exit_code=0))
    with pytest.raises(ValueError):
        artifacts.register(logs, str(target), 'report')
    assert artifacts.get(logs, record.task_id)['count'] == 0


def test_snapshot_brief_and_retention(setup):
    logs, repo, record = setup
    target = repo / 'report'
    target.write_text('hello')
    artifacts.register(logs, str(target), 'report')
    assert store.snapshot(logs, record)['artifact_summary']['count'] == 1
    from polybridge.task_reads import brief_status
    assert brief_status(store.snapshot(logs, record))['artifact_summary']['count'] == 1
    from polybridge.retention import _delete_task_files_locked
    assert _delete_task_files_locked(logs, record.task_id)
    assert target.read_text() == 'hello'
    assert not artifacts._path(logs, record.task_id).exists()


def test_nested_nearest_and_ambiguous_session(setup, monkeypatch):
    logs, repo, record = setup
    parent = replace(record, task_id='parent', pid=999, pgid=888)
    store.write(logs, parent)
    store.write(logs, replace(record, spawned_by='parent', parent_task_id='parent'))
    monkeypatch.setattr(lineage, '_default_process_table', lambda: {os.getpid(): 123, 123: 999, 999: 1, 1: 0})
    assert artifacts._caller(logs).task_id == record.task_id
    rival = replace(record, task_id='rival', pid=777, pgid=os.getsid(0))
    store.write(logs, rival)
    with pytest.raises(ValueError, match='ambiguous'):
        artifacts._caller(logs)


def test_extra_path_revocation_and_changed_symlink(setup):
    logs, repo, record = setup
    from polybridge.extra_paths import normalize_paths, serialize_paths
    outside = repo.parent / 'grant'
    outside.mkdir()
    target = outside / 'report'
    target.write_text('hello')
    store.write(logs, replace(record, extra_writable_paths=serialize_paths(normalize_paths([str(outside)]))))
    artifacts.register(logs, str(target), 'report')
    outside.rename(outside.with_name('old-grant'))
    outside.mkdir()
    target.write_text('new')
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'unavailable'
    store.write(logs, record)
    target = repo / 'local'
    target.write_text('hello')
    artifacts.register(logs, str(target), 'report')
    target.unlink()
    target.symlink_to(outside / 'report')
    assert artifacts.get(logs, record.task_id)['items'][1]['availability']['state'] == 'changed'


async def test_mcp_cli_parity_and_no_registry_launch(setup, monkeypatch, capsys):
    logs, repo, record = setup
    from polybridge import ctl, server, tasks
    monkeypatch.setattr(tasks, 'default_log_dir', lambda: logs)
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: logs)
    monkeypatch.setattr(server, '_registry', None)
    monkeypatch.setattr(server, '_reg', lambda: pytest.fail('must not launch registry'))
    target = repo / 'report'
    target.write_text('hello')
    item = await server.register_artifact(str(target), 'report', ' exact note ')
    assert ctl.main(['task-artifacts', record.task_id, '--json']) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['v'] == 12
    assert result['result']['items'][0]['artifact_id'] == item['artifact_id']
    assert ctl.main(['register-artifact', str(target), '--kind', 'report', '--note', ' exact note ', '--json']) == 0
    assert json.loads(capsys.readouterr().out)['result']['artifact_id'] == item['artifact_id']
    assert (await server.get_task_artifacts(record.task_id))['count'] == 1


def test_concurrent_registration_preserves_records(setup):
    logs, repo, record = setup
    from concurrent.futures import ThreadPoolExecutor
    target = repo / 'report'
    target.write_text('hello')
    with ThreadPoolExecutor(max_workers=8) as pool:
        entries = list(pool.map(lambda n: artifacts.register(logs, str(target), 'report', str(n)), range(20)))
    assert len({item['artifact_id'] for item in entries}) == 20
    assert artifacts.get(logs, record.task_id)['count'] == 20
    assert store.read(logs, record.task_id) == record


def test_manifest_provenance_and_duplicate_id_rejected(setup):
    logs, repo, record = setup
    target = repo / 'report'
    target.write_text('hello')
    artifacts.register(logs, str(target), 'report')
    path = artifacts._path(logs, record.task_id)
    data = json.loads(path.read_text())
    data['items'][0]['provenance']['task_id'] = 'spoof'
    path.write_text(json.dumps(data))
    assert artifacts.summary(logs, record.task_id)['state'] == 'unavailable'


def test_own_scratch_and_sibling_refusal(setup):
    logs, repo, record = setup
    from polybridge import scratch
    own = scratch.create(logs, record.task_id)
    sibling = scratch.create(logs, 'sibling')
    (own / 'capture').write_text('own')
    (sibling / 'capture').write_text('sibling')
    store.write(logs, replace(record, enforcement={'writable_roots': [str(own)]}))
    assert artifacts.register(logs, str(own / 'capture'), 'screenshot')['path'] == str(own / 'capture')
    with pytest.raises(ValueError):
        artifacts.register(logs, str(sibling / 'capture'), 'screenshot')


def test_underlock_terminal_and_process_change(setup, monkeypatch):
    logs, repo, record = setup
    from contextlib import contextmanager
    target = repo / 'report'
    target.write_text('hello')
    @contextmanager
    def lock(*args):
        store.write(logs, replace(record, status='completed', exit_code=0))
        yield
    monkeypatch.setattr(artifacts, '_task_lock', lock)
    with pytest.raises(ValueError, match='no longer running'):
        artifacts.register(logs, str(target), 'report')
    store.record_path(logs, record.task_id).unlink()
    store.write(logs, record)
    @contextmanager
    def changed(*args):
        store.write(logs, replace(record, start_time='different'))
        yield
    monkeypatch.setattr(artifacts, '_task_lock', changed)
    with pytest.raises(ValueError, match='identity changed'):
        artifacts.register(logs, str(target), 'report')


def test_manifest_bounds_special_shapes_and_no_target_content_read(setup, monkeypatch):
    logs, repo, record = setup
    target = repo / 'report'
    target.write_text('hello')
    artifacts.register(logs, str(target), 'report')
    manifest = artifacts._path(logs, record.task_id)
    valid = json.loads(manifest.read_text())
    for shape in (None, 4, [], {'v': 1, 'task_id': record.task_id, 'items': [valid['items'][0] | {'registered_observation': 1}]}, {'v': 1, 'task_id': record.task_id, 'items': [valid['items'][0] | {'registered_observation': ['device', 'inode', 'size', 'mtime_ns']}]}):
        manifest.write_text(json.dumps(shape))
        assert artifacts.get(logs, record.task_id)['state'] == 'unavailable'
    manifest.write_text('[' * 2000 + '0' + ']' * 2000)
    assert artifacts.get(logs, record.task_id)['state'] == 'unavailable'
    manifest.write_bytes(b'x' * (artifacts.MAX_BYTES + 1))
    assert artifacts.get(logs, record.task_id)['state'] == 'unavailable'
    manifest.unlink()
    # On Linux canonical is resolve-only; macOS canonical needs O_EVTONLY, so inspect read APIs
    # separately rather than forbidding canonical metadata descriptors on that platform.
    monkeypatch.setattr(Path, 'read_bytes', lambda path: pytest.fail('no artifact content reads'))
    artifacts.register(logs, str(target), 'report')
    monkeypatch.setattr(os, 'access', lambda *args: False)
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'unavailable'


def test_inherited_workflow_and_cyclic_ancestry(setup, monkeypatch):
    logs, repo, record = setup
    parent = replace(record, task_id='parent', pid=999, pgid=888)
    store.write(logs, parent)
    store.write(logs, replace(record, parent_task_id='parent'))
    monkeypatch.setattr(artifacts, '_workflow_owner', lambda directory, task_id, strict: {'role': 'worker'} if task_id == 'parent' else None)
    with pytest.raises(ValueError, match='Managed'):
        artifacts.get(logs, record.task_id)
    monkeypatch.setattr(artifacts, '_workflow_owner', lambda *a, **k: None)
    store.write(logs, replace(parent, parent_task_id=record.task_id))
    with pytest.raises(ValueError, match='cyclic'):
        artifacts.get(logs, record.task_id)


def test_cross_process_registration_and_retention_flock(setup):
    logs, repo, record = setup
    import subprocess
    import sys
    from collections import defaultdict
    from datetime import datetime, timezone
    target = repo / 'report'
    target.write_text('hello')
    script = '''
import sys
from pathlib import Path
from polybridge import artifacts, identity, store, workflow_hooks
logs = Path(sys.argv[1])
artifacts._caller = lambda directory: store.read(directory, 'owner', include_prompt=False)
identity.identity_check = lambda value: 'alive'
artifacts._workflow_owner = lambda *args, **kwargs: None
for index in range(10):
    artifacts.register(logs, sys.argv[2], 'report', sys.argv[3] + '-' + str(index))
'''
    processes = [subprocess.Popen([sys.executable, '-c', script, str(logs), str(target), str(index)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for index in range(4)]
    for process in processes:
        output, error = process.communicate(timeout=30)
        assert process.returncode == 0, error
    assert artifacts.get(logs, record.task_id, limit=50)['count'] == 40
    assert store.read(logs, record.task_id) == record
    lock_script = '''
import fcntl, sys
with open(sys.argv[1], 'a') as stream:
    fcntl.flock(stream, fcntl.LOCK_EX)
    print('locked', flush=True)
    sys.stdin.readline()
'''
    process = subprocess.Popen([sys.executable, '-c', lock_script, str(logs / 'owner.lock')], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == 'locked'
        terminal = replace(record, status='completed', exit_code=0)
        store.write(logs, terminal)
        from polybridge.retention import _delete_task
        stats = defaultdict(int)
        _delete_task(logs, terminal, stats, datetime.now(timezone.utc), frozenset({'owner'}), {'owner': terminal})
        assert stats['kept_locked'] == 1
        assert artifacts._path(logs, 'owner').exists()
    finally:
        process.communicate('\n', timeout=10)
    assert target.read_text() == 'hello'


def test_replaced_target_symlink_after_authority_check_is_refused(setup, monkeypatch):
    logs, repo, record = setup
    target = repo / 'report'
    outside = repo.parent / 'secret'
    target.write_text('hello')
    outside.write_text('secret')
    regular_stat = artifacts._regular_stat
    def replace_before_stat(path):
        target.unlink()
        target.symlink_to(outside)
        return regular_stat(path)
    monkeypatch.setattr(artifacts, '_regular_stat', replace_before_stat)
    with pytest.raises((OSError, ValueError)):
        artifacts.register(logs, str(target), 'report')
    assert not artifacts._path(logs, record.task_id).exists()


def test_summary_brief_cap_and_lists_do_not_load_manifest(setup, monkeypatch):
    logs, repo, record = setup
    target = repo / 'report'
    target.write_text('hello')
    artifacts.register(logs, str(target), 'report', '界' * 1000)
    from polybridge import task_reads
    full = store.snapshot(logs, record)
    brief = task_reads.brief_status(full | {'summary': '界' * 10000, 'note': '界' * 10000})
    assert len(json.dumps(brief).encode()) <= task_reads.BRIEF_BYTES
    assert brief['artifact_summary']['count'] == 1
    monkeypatch.setattr(artifacts, '_load', lambda *args: pytest.fail('list must not read manifests'))
    assert store.brief(logs, record)['task_id'] == record.task_id


def test_artifact_cli_usage_error_has_json_envelope(capsys):
    from polybridge import ctl
    with pytest.raises(SystemExit) as exited:
        ctl.main(['register-artifact', 'path', '--kind', 'invalid', '--json'])
    assert exited.value.code == 2
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'usage'


def test_missing_repository_preserves_independent_scratch_and_grant_authority(setup):
    logs, repo, record = setup
    from polybridge import scratch
    from polybridge.extra_paths import normalize_paths, serialize_paths
    own = scratch.create(logs, record.task_id)
    scratch_target = own / 'capture'
    scratch_target.write_text('scratch')
    grant = repo.parent / 'external-report'
    grant.write_text('grant')
    with_grant = replace(record, enforcement={'writable_roots': [str(own)]},
                         extra_writable_paths=serialize_paths(normalize_paths([str(grant)])))
    store.write(logs, with_grant)
    artifacts.register(logs, str(scratch_target), 'screenshot')
    artifacts.register(logs, str(grant), 'report')
    repo.rmdir()
    assert [item['availability']['state'] for item in artifacts.get(logs, record.task_id)['items']] == ['available', 'available']
    # Replacement invalidates exact-file grant identity while the artifact path still exists.
    grant.rename(grant.with_name('retained-old-grant'))
    grant.write_text('replacement')
    assert artifacts.get(logs, record.task_id)['items'][1]['availability']['state'] == 'unavailable'
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'available'


def test_missing_authority_is_unavailable_not_deleted(setup, monkeypatch):
    logs, repo, record = setup
    target = repo / 'report'
    target.write_text('hello')
    artifacts.register(logs, str(target), 'report')
    def missing_authority(*args):
        raise FileNotFoundError('saved authority root is missing')
    monkeypatch.setattr(artifacts, '_target', missing_authority)
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'unavailable'
    target.unlink()
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'deleted'


def test_inline_other_backend_configuration_does_not_block_references(setup, monkeypatch):
    logs, repo, record = setup
    target = repo / 'report'
    target.write_text('hello')
    monkeypatch.setenv('OPENCODE_CONFIG_CONTENT', '{"model":"some-model"}')
    assert artifacts.register(logs, str(target), 'report')['kind'] == 'report'
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'available'
    # Redirected file configuration remains excluded even when inline content is set.
    protected = repo / 'custom-config.json'
    protected.write_text('{}')
    monkeypatch.setenv('OPENCODE_CONFIG', str(protected))
    with pytest.raises(ValueError, match='protected backend configuration'):
        artifacts.register(logs, str(protected), 'other')
    from polybridge.backends import get
    with pytest.raises(ValueError, match='inline backend execution'):
        get('opencode').execution_configuration_namespaces(repo)


def test_concurrent_initial_lock_creation_uses_one_inode(setup):
    logs, _, _ = setup
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    # Several fresh names exercise initial creation without a timing-dependent soak.
    for index in range(8):
        gate = Barrier(8)
        def acquire(_):
            gate.wait(timeout=5)
            with artifacts._task_lock(logs, f'fresh-{index}') as directory_fd:
                info = os.stat(f'fresh-{index}.lock', dir_fd=directory_fd, follow_symlinks=False)
                return info.st_dev, info.st_ino
        with ThreadPoolExecutor(max_workers=8) as pool:
            identities = list(pool.map(acquire, range(8)))
        assert len(set(identities)) == 1


def test_replaced_lock_inode_is_refused(setup, monkeypatch):
    logs, _, _ = setup
    real_flock = artifacts.fcntl.flock
    def replaced(descriptor, operation):
        real_flock(descriptor, operation)
        if not operation & artifacts.fcntl.LOCK_NB:
            return  # Only replace the task lock, never the catalog lock.
        lock = logs / 'owner.lock'
        lock.rename(logs / 'old-owner.lock')
        lock.write_text('replacement')
    monkeypatch.setattr(artifacts.fcntl, 'flock', replaced)
    with pytest.raises(ValueError, match='lock identity changed'):
        with artifacts._task_lock(logs, 'owner'):
            pytest.fail('must not enter with split task lock')


@pytest.mark.parametrize('grant_kind', ['file', 'directory'])
def test_only_covering_grants_need_revalidation(setup, grant_kind):
    logs, repo, record = setup
    from polybridge.extra_paths import normalize_paths, serialize_paths
    authority = repo.parent / 'authority-a'
    authority.mkdir()
    target = authority / 'report'
    target.write_text('report')
    other = repo.parent / 'authority-b'
    other.write_text('unrelated')
    grant_a = target if grant_kind == 'file' else authority
    grants = serialize_paths(normalize_paths([str(grant_a), str(other)]))
    store.write(logs, replace(record, extra_writable_paths=grants))
    artifacts.register(logs, str(target), 'report')
    other.rename(other.with_name('old-b'))
    other.write_text('replacement')
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'available'
    artifacts.register(logs, str(target), 'report', 'independent grant remains valid')
    other.unlink()
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'available'
    grant_a.rename(grant_a.with_name('old-a'))
    if grant_kind == 'directory':
        grant_a.mkdir()
        target.write_text('replacement')
    else:
        target.write_text('replacement')
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'unavailable'
    with pytest.raises(ValueError):
        artifacts.register(logs, str(target), 'report')


def test_overlapping_covering_grants_are_independent_but_structure_is_strict(setup):
    logs, repo, record = setup
    from polybridge.extra_paths import normalize_paths, serialize_paths
    authority = repo.parent / 'authority'
    authority.mkdir()
    target = authority / 'report'
    target.write_text('first')
    other = repo.parent / 'other'
    other.write_text('other')
    grants = serialize_paths(normalize_paths([str(target), str(authority), str(other)]))
    store.write(logs, replace(record, extra_writable_paths=grants))
    artifacts.register(logs, str(target), 'report')
    target.rename(authority / 'old-report')
    target.write_text('replacement through still valid directory grant')
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'changed'
    artifacts.register(logs, str(target), 'report', 'broad grant')
    grants[-1]['inode'] = 'malformed unrelated grant'
    store.write(logs, replace(record, extra_writable_paths=grants))
    assert artifacts.get(logs, record.task_id)['items'][0]['availability']['state'] == 'unavailable'
    with pytest.raises(ValueError):
        artifacts.register(logs, str(target), 'report')


@pytest.mark.parametrize('case', ['builder', 'worker', 'ancestor', 'owner_error', 'missing_parent', 'cycle'])
def test_unsupported_summary_is_shared_by_live_recovered_and_brief(setup, monkeypatch, case):
    logs, repo, record = setup
    from datetime import datetime, timezone
    from polybridge import task_reads
    from polybridge.tasks import Task
    if case == 'builder':
        record = replace(record, workflow_builder=True)
    elif case in ('ancestor', 'missing_parent', 'cycle'):
        record = replace(record, spawned_by='parent')
        if case != 'missing_parent':
            store.write(logs, replace(record, task_id='parent', spawned_by='owner' if case == 'cycle' else None))
    store.write(logs, record)
    if case in ('worker', 'ancestor'):
        managed = 'owner' if case == 'worker' else 'parent'
        monkeypatch.setattr(artifacts, '_workflow_owner', lambda directory, identifier, strict:
                            {'role': 'worker'} if identifier == managed else None)
    elif case == 'owner_error':
        def fail(*args, **kwargs):
            raise RuntimeError('ownership lookup unavailable')
        monkeypatch.setattr(artifacts, '_workflow_owner', fail)
    monkeypatch.setattr(artifacts, '_load', lambda *args: pytest.fail('unsupported manifest must not open'))
    monkeypatch.setattr(artifacts, '_caller', lambda *args: pytest.fail('summary must not detect callers'))
    live = Task(task_id=record.task_id, backend='codex', session_id=None, repo_path=repo,
                prompt='', max_turns=None, log_path=logs / 'owner.jsonl',
                started_at=datetime.now(timezone.utc), workflow_builder=record.workflow_builder,
                spawned_by=record.spawned_by)
    expected = {'count': 0, 'state': 'unavailable'}
    assert artifacts.summary(logs, record.task_id) == expected
    for snapshot in (live.snapshot(), store.snapshot(logs, record)):
        assert snapshot['artifact_summary'] == expected
        assert task_reads.brief_status(snapshot)['artifact_summary'] == expected


def test_supported_empty_corrupt_and_unknown_summary_references(setup, monkeypatch):
    logs, _, record = setup
    from polybridge import task_reads
    monkeypatch.setattr(artifacts, '_caller', lambda *args: pytest.fail('summary must not detect callers'))
    reference = {'tool': 'get_task_artifacts', 'task_id': record.task_id}
    assert artifacts.summary(logs, record.task_id) == {'count': 0, 'state': 'available', 'detail_ref': reference}
    artifacts._path(logs, record.task_id).write_text('{')
    summary = artifacts.summary(logs, record.task_id)
    assert summary == {'count': 0, 'state': 'unavailable', 'detail_ref': reference}
    assert task_reads.brief_status({'task_id': record.task_id, 'artifact_summary': summary})['artifact_summary'] == summary
    assert artifacts.summary(logs, 'unknown') == {'count': 0, 'state': 'unavailable'}


@pytest.mark.parametrize('case', ['builder', 'managed', 'transitive', 'mixed', 'missing', 'cycle'])
def test_continuation_ancestry_denies_artifact_access(setup, monkeypatch, case):
    logs, repo, record = setup
    (repo / 'report').write_text('report')
    # A persisted successor can lack the builder marker carried by its source.
    owner = replace(record, continuation_of_task_id='source')
    source = replace(record, task_id='source', pid=999, pgid=888,
                     workflow_builder=case == 'builder',
                     continuation_of_task_id='owner' if case == 'cycle' else 'managed' if case == 'transitive' else None,
                     spawned_by='managed' if case == 'mixed' else None)
    if case != 'missing':
        store.write(logs, source)
    if case in ('mixed', 'transitive'):
        store.write(logs, replace(source, task_id='managed', spawned_by=None, continuation_of_task_id=None))
    store.write(logs, owner)
    monkeypatch.setattr(artifacts, '_workflow_owner', lambda directory, identifier, strict:
                        {'role': 'worker'} if identifier == ('managed' if case in ('mixed', 'transitive') else 'source')
                        and case in ('managed', 'mixed', 'transitive') else None)
    reason = {'missing': 'Task ancestry is unavailable', 'cycle': 'Task ancestry is cyclic'}.get(
        case, 'Managed workflow artifact access is not supported')
    with pytest.raises(ValueError, match=reason):
        artifacts.register(logs, 'report', 'report')
    with pytest.raises(ValueError, match=reason):
        artifacts.get(logs, 'owner')
    assert artifacts.summary(logs, 'owner') == {'count': 0, 'state': 'unavailable'}


def test_standalone_continuation_artifact_access(setup):
    logs, repo, record = setup
    (repo / 'report').write_text('report')
    store.write(logs, replace(record, task_id='source', pid=999, pgid=888))
    store.write(logs, replace(record, continuation_of_task_id='source', chain_root_task_id='owner'))
    artifacts.register(logs, 'report', 'report')
    assert artifacts.get(logs, 'owner')['count'] == 1
    assert artifacts.summary(logs, 'owner')['state'] == 'available'


def test_ready_caller_does_not_deserialize_unrelated_history(setup, monkeypatch):
    logs, _, record = setup
    for number in range(120):
        store.write(logs, replace(record, task_id=f'unrelated-{number}', pid=10000 + number, pgid=20000))
    while store.bootstrap_catalog(logs):
        pass
    original = store.read
    reads = []
    def read(directory, task_id, **kwargs):
        reads.append(task_id)
        assert not task_id.startswith('unrelated-')
        return original(directory, task_id, **kwargs)
    monkeypatch.setattr(store, 'read', read)
    assert artifacts._caller(logs).task_id == record.task_id
    assert reads == ['owner']


def test_caller_cold_catalog_advances_boundedly_and_retries(setup, monkeypatch):
    logs, _, record = setup
    for number in range(205):
        store.write(logs, replace(record, task_id=f'unrelated-{number:03}', pid=10000 + number, pgid=20000))
    for path in logs.glob('.listing.v9.sqlite3*'):
        path.unlink()
    original = store.read
    reads = []
    def read(*args, **kwargs):
        reads.append(args[1])
        return original(*args, **kwargs)
    monkeypatch.setattr(store, 'read', read)
    for expected in (100, 200):
        with pytest.raises(ValueError, match='preparation is incomplete'):
            artifacts._caller(logs)
        assert len(reads) == expected
    assert artifacts._caller(logs).task_id == record.task_id
    assert len(reads) == 207  # Six remaining metadata records and selected full owner.


def test_caller_related_overflow_fails_closed(setup):
    logs, _, record = setup
    for number in range(100):
        store.write(logs, replace(record, task_id=f'rival-{number}', pid=123))
    while store.bootstrap_catalog(logs):
        pass
    with pytest.raises(ValueError, match='More than 100 related'):
        artifacts._caller(logs)


def test_caller_observes_late_uncooperative_metadata_replacement(setup):
    logs, _, record = setup
    rival = replace(record, task_id='rival', pid=777, pgid=888)
    store.write(logs, rival)
    assert artifacts._caller(logs).task_id == record.task_id
    source = logs / ('rival' + store.RECORD_SUFFIX)
    data = json.loads(source.read_text())
    data['pgid'] = os.getsid(0)
    replacement = logs / 'replacement'
    replacement.write_text(json.dumps(data))
    replacement.replace(source)
    with pytest.raises(ValueError, match='ambiguous'):
        artifacts._caller(logs)


@pytest.mark.parametrize('hint', ['forged', 'owner'])
def test_caller_unknown_identity_and_forged_hints_fail_closed(setup, monkeypatch, hint):
    logs, _, _ = setup
    monkeypatch.setenv('PB_TASK_ID', hint)
    if hint == 'owner':
        monkeypatch.setattr(identity, 'identity_check', lambda _: 'undecidable')
    with pytest.raises(ValueError, match='identity|ambiguous'):
        artifacts._caller(logs)


def test_caller_blocked_catalog_does_not_authorize_human(setup):
    logs, _, _ = setup
    (logs / ('broken' + store.RECORD_SUFFIX)).write_text('{')
    with pytest.raises(ValueError, match='caller identity is unavailable'):
        artifacts._caller(logs)


def test_caller_rechecks_selected_full_record_identity(setup, monkeypatch):
    logs, _, record = setup
    original = lineage.related_catalog_candidates
    def candidates(*args):
        result = original(*args)
        store.write(logs, replace(record, start_time='replacement-process'))
        return result
    monkeypatch.setattr(lineage, 'related_catalog_candidates', candidates)
    with pytest.raises(ValueError, match='identity changed'):
        artifacts._caller(logs)


def test_registration_identity_checks_share_preparation_budget(setup, monkeypatch):
    from polybridge import catalog
    logs, repo, _ = setup
    (repo / 'report').write_text('observed')
    original = lineage.related_catalog_candidates
    budgets = []
    def candidates(*args):
        budgets.append(catalog._request_budget.get())
        return original(*args)
    monkeypatch.setattr(lineage, 'related_catalog_candidates', candidates)
    artifacts.register(logs, 'report', 'report')
    assert len(budgets) == 2
    assert budgets[0] is budgets[1]
    assert budgets[0] is not None
    assert budgets[0].metadata_decodes <= 100


def test_artifact_boundaries_never_scan_workflow_history(setup, monkeypatch):
    from polybridge.workflows import WorkflowStore
    logs, repo, record = setup
    monkeypatch.setattr(WorkflowStore, 'list_runs', lambda *a, **k: pytest.fail('unbounded workflow history'))
    (repo / 'report').write_text('report')
    artifacts.register(logs, 'report', 'report')
    assert artifacts.get(logs, record.task_id)['count'] == 1
    assert artifacts.summary(logs, record.task_id)['count'] == 1


def test_artifact_workflow_preparation_is_bounded_and_summary_unavailable(setup, monkeypatch):
    from polybridge.workflows import WorkflowStore
    logs, _, record = setup
    storage = WorkflowStore(logs.parent)
    for n in range(205):
        run = {'workflow_run_id': f'run-{n:03}', 'created_at': n, 'status': 'completed',
               'repo_path': str(logs.parent), 'activations': []}
        (storage.runs / f'run-{n:03}.json').write_text(json.dumps(run))
    original = WorkflowStore.get_run
    reads = []
    def get_run(self, run_id, **kwargs):
        reads.append(run_id)
        return original(self, run_id, **kwargs)
    monkeypatch.setattr(WorkflowStore, 'get_run', get_run)
    assert artifacts.summary(logs, record.task_id) == {'count': 0, 'state': 'unavailable'}
    assert len(reads) == 99  # The owner consumed one shared decode.
    with pytest.raises(ValueError, match='preparation is incomplete'):
        artifacts.get(logs, record.task_id)
    assert len(reads) == 198
    assert artifacts.summary(logs, record.task_id)['state'] == 'available'
    assert len(reads) == 205


@pytest.mark.parametrize('ownership', ['association', 'receipt', 'malformed', 'dangling'])
def test_real_inherited_workflow_ownership_denies_artifacts(setup, ownership):
    from polybridge.workflows import WorkflowStore
    logs, repo, record = setup
    storage = WorkflowStore(logs.parent)
    parent = replace(record, task_id='managed-parent', pid=777, pgid=888)
    store.write(logs, parent)
    store.write(logs, replace(record, spawned_by=parent.task_id))
    if ownership == 'association':
        run = {'workflow_run_id': 'run', 'created_at': 1, 'status': 'completed',
               'repo_path': str(repo), 'activations': [{'id': 'activation', 'node_id': 'node',
               'role': 'worker', 'tasks': [{'task_id': parent.task_id, 'status': 'completed'}]}]}
        (storage.runs / 'run.json').write_text(json.dumps(run))
    else:
        receipt = storage.owners / f'{parent.task_id}.json'
        if ownership == 'dangling':
            receipt.symlink_to(storage.owners / 'missing')
        else:
            receipt.write_text('{' if ownership == 'malformed' else json.dumps({'workflow_run_id': 'run'}))
    (repo / 'report').write_text('report')
    for action in (lambda: artifacts.register(logs, 'report', 'report'), lambda: artifacts.get(logs, 'owner')):
        with pytest.raises(ValueError, match='Managed'):
            action()
    assert artifacts.summary(logs, 'owner') == {'count': 0, 'state': 'unavailable'}


@pytest.mark.parametrize('edge', ['spawned_by', 'parent_task_id', 'continuation_of_task_id', None])
def test_artifact_authority_rejects_oversized_owner_or_ancestor(setup, monkeypatch, edge):
    logs, _, record = setup
    monkeypatch.setattr(artifacts, '_caller', lambda _: None)
    monkeypatch.setattr(artifacts, '_workflow_owner', lambda *args, **kwargs: None)
    oversized = record.task_id
    if edge:
        oversized = 'ancestor'
        store.write(logs, replace(record, **{edge: oversized}))
        store.write(logs, replace(record, task_id=oversized))
    path = store.record_path(logs, oversized)
    path.write_bytes(path.read_bytes() + b' ' * (4 * 1024 * 1024))
    assert artifacts.summary(logs, record.task_id) == {'count': 0, 'state': 'unavailable'}
    with pytest.raises(ValueError):
        artifacts.get(logs, record.task_id)


def test_manifest_and_repeated_owner_reads_share_nested_budget(setup, monkeypatch):
    from polybridge.catalog import current_metadata_budget, metadata_request
    logs, _, record = setup
    monkeypatch.setattr(artifacts, '_workflow_owner', lambda *args, **kwargs: None)
    manifest = artifacts._path(logs, record.task_id)
    manifest.write_text(json.dumps({'v': 1, 'task_id': record.task_id, 'items': []}))
    owner_size = store.record_path(logs, record.task_id).stat().st_size
    with metadata_request():
        budget = current_metadata_budget()
        budget.metadata_limit = owner_size + manifest.stat().st_size
        assert artifacts.summary(logs, record.task_id)['state'] == 'available'
        assert budget.metadata_bytes == budget.metadata_limit
        assert budget.metadata_decodes == 2
        assert artifacts.summary(logs, record.task_id)['state'] == 'unavailable'
        assert current_metadata_budget() is budget
    assert artifacts.summary(logs, record.task_id)['state'] == 'available'


def test_registration_budget_exhaustion_preserves_manifest(setup):
    from polybridge.catalog import current_metadata_budget, metadata_request
    logs, repo, record = setup
    (repo / 'report').write_text('report')
    artifacts.register(logs, 'report', 'report')
    manifest = artifacts._path(logs, record.task_id)
    saved = manifest.read_bytes()
    with metadata_request():
        current_metadata_budget().metadata_limit = 0
        with pytest.raises(ValueError):
            artifacts.register(logs, 'report', 'report', 'new')
    assert manifest.read_bytes() == saved


def test_valid_near_file_limit_and_shared_ancestor_byte_exhaustion(setup, monkeypatch):
    from polybridge.catalog import METADATA_BYTES
    logs, _, record = setup
    monkeypatch.setattr(artifacts, '_workflow_owner', lambda *args, **kwargs: None)
    monkeypatch.setattr(artifacts, '_caller', lambda _: None)
    def padded(task):
        store.write(logs, task)
        path = store.record_path(logs, task.task_id)
        raw = path.read_bytes()
        path.write_bytes(raw + b' ' * (METADATA_BYTES - len(raw)))
    padded(record)
    assert artifacts.get(logs, record.task_id)['state'] == 'available'
    padded(replace(record, spawned_by='parent'))
    padded(replace(record, task_id='parent', parent_task_id='grandparent'))
    store.write(logs, replace(record, task_id='grandparent'))
    assert artifacts.summary(logs, record.task_id)['state'] == 'unavailable'
    with pytest.raises(ValueError):
        artifacts.get(logs, record.task_id)


def test_registration_exhaustion_after_locked_rechecks_preserves_manifest(setup):
    from polybridge.catalog import current_metadata_budget, metadata_request
    logs, repo, record = setup
    (repo / 'report').write_text('report')
    artifacts.register(logs, 'report', 'report')
    manifest = artifacts._path(logs, record.task_id)
    saved = manifest.read_bytes()
    owner_size = store.record_path(logs, record.task_id).stat().st_size
    with metadata_request():
        budget = current_metadata_budget()
        budget.metadata_limit = 3 * owner_size + len(saved) - 1
        with pytest.raises(ValueError):
            artifacts.register(logs, 'report', 'report', 'new')
        assert budget.metadata_decodes == 3
        assert budget.metadata_bytes == 3 * owner_size
    assert manifest.read_bytes() == saved


def test_artifact_directory_mutations_share_catalog_lock_without_flock_inversion(setup, monkeypatch):
    from contextlib import contextmanager
    from polybridge.catalog import catalog_lock
    logs, repo, record = setup
    (repo / 'report').write_text('report')
    depth = 0
    mutations = []
    @contextmanager
    def tracked_lock(directory):
        nonlocal depth
        with catalog_lock(directory):
            depth += 1
            try:
                yield
            finally:
                depth -= 1
    original_open, original_replace, original_unlink = os.open, os.replace, os.unlink
    original_flock = artifacts.fcntl.flock
    def opened(path, flags, *args, **kwargs):
        if flags & os.O_CREAT and str(path).startswith(record.task_id):
            assert depth > 0
            mutations.append('create')
        return original_open(path, flags, *args, **kwargs)
    def replaced(source, destination, *args, **kwargs):
        if str(destination) == f'{record.task_id}.artifacts.json':
            assert depth > 0
            mutations.append('replace')
        return original_replace(source, destination, *args, **kwargs)
    def unlinked(path, *args, **kwargs):
        if str(path).startswith(f'{record.task_id}.artifacts-'):
            assert depth > 0
            mutations.append('cleanup')
        return original_unlink(path, *args, **kwargs)
    def flocked(fd, operation):
        if operation & artifacts.fcntl.LOCK_NB:
            assert depth == 0  # Never wait for a task flock while holding catalog lock.
        return original_flock(fd, operation)
    monkeypatch.setattr(artifacts, 'catalog_lock', tracked_lock)
    monkeypatch.setattr(os, 'open', opened)
    monkeypatch.setattr(os, 'replace', replaced)
    monkeypatch.setattr(os, 'unlink', unlinked)
    monkeypatch.setattr(artifacts.fcntl, 'flock', flocked)
    artifacts.register(logs, 'report', 'report')
    assert mutations == ['create', 'create', 'replace', 'cleanup']


def test_initial_task_lock_creation_waits_for_catalog_discovery(setup, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from contextlib import contextmanager
    from threading import Event
    from polybridge.catalog import catalog_lock
    logs, _, record = setup
    attempted = Event()
    @contextmanager
    def observed_lock(directory):
        attempted.set()
        with catalog_lock(directory):
            yield
    monkeypatch.setattr(artifacts, 'catalog_lock', observed_lock)
    def create():
        with artifacts._task_lock(logs, record.task_id):
            pass
    with ThreadPoolExecutor(max_workers=1) as pool:
        with catalog_lock(logs):
            future = pool.submit(create)
            assert attempted.wait(5)
            assert not (logs / f'{record.task_id}.lock').exists()
            assert not future.done()
        future.result(timeout=5)
    assert (logs / f'{record.task_id}.lock').is_file()
