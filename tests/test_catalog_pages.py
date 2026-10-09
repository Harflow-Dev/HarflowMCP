import base64
import json
import os
from pathlib import Path
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest

from polybridge import catalog, ctl, lineage, server, store, workflows
from polybridge.workflow_inspection import managed_page_reader


def record(identifier, *, stamp='2026-10-01T00:00:00+00:00', status='completed', **kwargs):
    return store.TaskRecord(identifier, 'codex', 'session', '/repo', stamp, status=status, exit_code=0 if status == 'completed' else None, **kwargs)


def legacy_tasks(directory, count):
    directory.mkdir(parents=True, exist_ok=True)
    for number in range(count):
        task = record(f't{number:04}', stamp=f'2026-10-01T00:{number // 60:02}:{number % 60:02}+00:00')
        (directory / f'{task.task_id}.meta.json').write_text(json.dumps(asdict(task)))


def human_authority(monkeypatch):
    monkeypatch.delenv(lineage.ENV_TASK_ID, raising=False)
    monkeypatch.setattr(lineage, '_default_process_table', lambda: {os.getpid(): 1, 1: 0})
    monkeypatch.setattr(store, 'read_all', lambda *a, **k: pytest.fail('unbounded task metadata scan'))
    monkeypatch.setattr(workflows.WorkflowStore, 'list_runs', lambda *a, **k: pytest.fail('unbounded workflow record scan'))


def test_bootstrap_is_resumable_bounded_and_does_not_modify_legacy_records(tmp_path, monkeypatch):
    directory = tmp_path / 'tasks'
    legacy_tasks(directory, 205)
    originals = {p.name: p.read_bytes() for p in directory.glob('*.meta.json')}
    calls = []
    read = store.read
    monkeypatch.setattr(store, 'read', lambda *args, **kwargs: (calls.append(args[1]), read(*args, **kwargs))[1])
    for expected in (100, 100, 5):
        calls.clear()
        page = store.list_page(directory)
        assert len(calls) == expected
        assert page['bootstrap_pending'] == (expected == 100)
        if expected == 100:
            assert page['items'] == []
    assert page['items'][0]['task_id'] == 't0204'
    calls.clear()
    second = store.list_page(directory, cursor=page['next_cursor'])
    assert len(second['items']) == 100 and not calls
    assert originals == {p.name: p.read_bytes() for p in directory.glob('*.meta.json')}


def test_stable_cursor_ignores_updates_and_newer_insertions_with_equal_timestamp_ids(tmp_path):
    for identifier in ('a', 'b', 'c', 'd'):
        store.write(tmp_path, record(identifier, status='running'))
    first = store.list_page(tmp_path, limit=2)
    assert [item['task_id'] for item in first['items']] == ['d', 'c']
    store.write(tmp_path, record('z', stamp='2026-10-02T00:00:00+00:00'))
    store.write(tmp_path, record('b'))
    second = store.list_page(tmp_path, limit=2, cursor=first['next_cursor'])
    assert [item['task_id'] for item in second['items']] == ['b', 'a']
    assert second['items'][0]['status'] == 'completed'
    assert not second['has_more']
    active = store.list_page(tmp_path, active_only=True)
    assert active['total_active_count'] is None and not active['counts_complete']


def test_session_and_exact_id_lookup_never_decodes_unrequested_history(tmp_path, monkeypatch):
    store.write(tmp_path, record('a'))
    store.write(tmp_path, replace(record('b'), session_id='other'))
    store.list_page(tmp_path)
    monkeypatch.setattr(store, 'read', lambda *a, **k: pytest.fail('indexed page decoded metadata'))
    assert [i['task_id'] for i in store.list_page(tmp_path, session_id='other')['items']] == ['b']
    assert [i['task_id'] for i in store.list_page(tmp_path, task_ids=['a'])['items']] == ['a']


@pytest.mark.parametrize('value', ['', 'garbage', 'e30=', 'W10=', 'bnVsbA==', 1, []])
def test_invalid_cursors_refused(tmp_path, value):
    with pytest.raises(ValueError, match='cursor'):
        store.list_page(tmp_path, cursor=value)


def test_cursor_cannot_cross_store_or_filter(tmp_path):
    for identifier in ('a', 'b'):
        store.write(tmp_path, record(identifier))
    token = store.list_page(tmp_path, limit=1)['next_cursor']
    with pytest.raises(ValueError, match='cursor'):
        store.list_page(tmp_path, cursor=token, active_only=True)
    with pytest.raises(ValueError, match='cursor'):
        store.list_page(tmp_path / 'other', cursor=token)


def test_huge_unicode_headers_are_bounded_and_have_no_private_caller_payload(tmp_path):
    for number in range(100):
        store.write(tmp_path, replace(record(f't{number}'), title='界' * 10000, group='界' * 10000, repo_path='界' * 10000))
    page = store.list_page(tmp_path, task_ids=[f't{number}' for number in range(100)])
    assert len(page['items']) == 100
    assert len(json.dumps(page, ensure_ascii=True).encode()) <= catalog.PAGE_BYTES
    assert all('_caller' not in item for item in page['items'])


def definition():
    return workflows.validate_definition({'name': 'test', 'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'work', 'type': 'agent', 'agent': {'backend': 'codex'}}, {'id': 'end', 'type': 'end'}], 'connections': [{'id': 'a', 'source': 'start', 'target': 'work'}, {'id': 'b', 'source': 'work', 'target': 'end'}], 'orchestrator': {'backend': 'codex'}})


def test_workflow_headers_counts_links_and_status_batch_do_not_read_runs(tmp_path, monkeypatch):
    storage = workflows.WorkflowStore(tmp_path)
    parent = storage.create_run(definition(), 'Task', tmp_path)
    child = storage.create_run(definition(), 'Child', tmp_path)
    storage.update_run(child['workflow_run_id'], lambda r: r.update(parent_link={'workflow_run_id': parent['workflow_run_id'], 'root_workflow_run_id': parent['workflow_run_id']}, orchestrator_session_owner_run_id=parent['workflow_run_id']), 'child')
    storage.update_run(parent['workflow_run_id'], lambda r: r.update(status='needs_input'), 'question')
    storage.list_run_page()
    monkeypatch.setattr(storage, 'get_run', lambda *a, **k: pytest.fail('indexed header decoded run'))
    page = storage.list_run_page(active_only=True)
    assert page['total_active_count'] == 2
    assert page['total_active_root_count'] == page['total_attention_root_count'] == 1
    related = storage.related_run_headers([child['workflow_run_id']])
    assert {r['workflow_run_id'] for r in related} == {parent['workflow_run_id'], child['workflow_run_id']}
    assert storage.list_run_page(run_ids=[child['workflow_run_id']])['items'][0]['orchestrator_session_owner_run_id'] == parent['workflow_run_id']
    assert all('definition' not in r and 'activations' not in r for r in page['items'])


def test_whole_cli_page_including_authority_decodes_at_most_100_metadata(tmp_path, monkeypatch, capsys):
    directory = tmp_path / 'tasks'
    legacy_tasks(directory, 205)
    human_authority(monkeypatch)
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: directory)
    calls, original = [], store.read
    monkeypatch.setattr(store, 'read', lambda *a, **k: (calls.append(a[1]), original(*a, **k))[1])
    for expected in (100, 100, 5, 0):
        calls.clear()
        assert ctl.main(['task-list-page', '--json']) == 0
        response = json.loads(capsys.readouterr().out)['result']
        assert len(calls) == expected
        assert response['bootstrap_pending'] == (expected > 0)
        if expected > 0:
            assert response['items'] == []


async def test_whole_server_page_has_bounded_authority_and_no_legacy_scan(tmp_path, monkeypatch):
    directory = tmp_path / 'tasks'
    legacy_tasks(directory, 103)
    human_authority(monkeypatch)
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=directory))
    calls, original = [], store.read
    monkeypatch.setattr(store, 'read', lambda *a, **k: (calls.append(a[1]), original(*a, **k))[1])
    page = await server.list_task_page()
    assert page['bootstrap_pending'] and page['items'] == [] and len(calls) == 100
    calls.clear()
    page = await server.list_task_page()
    assert page['bootstrap_pending'] and page['items'] == [] and len(calls) == 3
    calls.clear()
    page = await server.list_task_page()
    assert not page['bootstrap_pending'] and len(page['items']) == 100 and not calls


def test_catalog_authority_preserves_uncertain_and_spoofed_caller_refusal(tmp_path):
    store.write(tmp_path, replace(record('caller', status='running'), pid=20, pgid=20))
    store.bootstrap_catalog(tmp_path)
    args = {'getpid': lambda: 30, 'getsid': lambda pid: 30, 'process_table': lambda: {30: 20, 20: 1, 1: 0}}
    uncertain = lineage.detect_catalog_caller(tmp_path, environ={}, check=lambda value: 'undecidable', **args)
    assert uncertain.undecidable
    found = lineage.detect_catalog_caller(tmp_path, environ={}, check=lambda value: 'alive', **args)
    assert found.caller.record.task_id == 'caller'
    spoof = lineage.detect_catalog_caller(tmp_path, environ={lineage.ENV_TASK_ID: 'missing'}, check=lambda value: 'dead', **args)
    assert spoof.caller is None and spoof.undecidable


def test_nested_caller_inherits_workflow_authority_without_historical_scans(tmp_path, monkeypatch):
    directory = tmp_path / 'tasks'
    storage = workflows.WorkflowStore(tmp_path)
    run = storage.create_run(definition(), 'Assignment', tmp_path)
    storage.update_run(run['workflow_run_id'], lambda r: r.update(activations=[{'id': 'execution', 'node_id': 'work', 'role': 'node', 'status': 'running', 'tasks': [{'task_id': 'worker', 'status': 'running'}]}]), 'worker')
    store.write(directory, replace(record('worker', status='running'), pid=20, pgid=20))
    store.write(directory, replace(record('nested', status='running'), pid=30, pgid=30, spawned_by='worker'))
    original = lineage.detect_catalog_caller
    monkeypatch.setattr(lineage, 'detect_catalog_caller', lambda logdir: original(logdir, environ={}, getpid=lambda: 40, getsid=lambda pid: 40, process_table=lambda: {40: 30, 30: 20, 20: 1, 1: 0}, check=lambda value: 'alive'))
    monkeypatch.setattr(store, 'read_all', lambda *a, **k: pytest.fail('legacy task scan'))
    monkeypatch.setattr(storage, 'list_runs', lambda *a, **k: pytest.fail('legacy workflow scan'))
    # Already-projected metadata needs no artificial bootstrap barrier.
    ready, managed = managed_page_reader(directory)
    assert ready and managed[0]['role'] == 'node'
    assert managed[0]['activation_id'] == 'execution'
    assert managed[1]['workflow_run_id'] == run['workflow_run_id']


def test_out_of_band_addition_is_not_masked_by_modern_write(tmp_path):
    store.write(tmp_path, record('initial'))
    store.list_page(tmp_path)
    legacy = record('legacy', stamp='2026-10-03T00:00:00+00:00')
    (tmp_path / 'legacy.meta.json').write_text(json.dumps(asdict(legacy)))
    store.write(tmp_path, record('modern'))
    page = store.list_page(tmp_path)
    assert not page['bootstrap_pending']
    assert {item['task_id'] for item in page['items']} == {'legacy', 'initial', 'modern'}


def test_external_replacement_and_deletion_refresh_headers_and_counts(tmp_path):
    storage = workflows.WorkflowStore(tmp_path)
    run = storage.create_run(definition(), 'Run', tmp_path)
    storage.list_run_page()
    replacement = dict(run, status='completed')
    (storage.runs / f"{run['workflow_run_id']}.json").write_text(json.dumps(replacement))
    assert storage.get_run_header(run['workflow_run_id'])['status'] == 'completed'
    page = storage.list_run_page()
    assert page['items'][0]['status'] == 'completed'
    assert page['total_active_count'] == page['total_active_root_count'] == 0
    (storage.runs / f"{run['workflow_run_id']}.json").unlink()
    with pytest.raises(FileNotFoundError):
        storage.get_run_header(run['workflow_run_id'])
    assert storage.list_run_page()['items'] == []
    assert storage.list_run_page(run_ids=[run['workflow_run_id']])['items'] == []


def test_child_page_contains_unloaded_task_ancestor_header(tmp_path):
    store.write(tmp_path, record('parent', status='running'))
    store.write(tmp_path, replace(record('child', stamp='2026-10-03T00:00:00+00:00'), spawned_by='parent', root_task_id='parent'))
    page = store.list_page(tmp_path, limit=1)
    assert [item['task_id'] for item in page['items']] == ['child']
    assert [item['task_id'] for item in page['related_headers']] == ['parent']
    (tmp_path / 'parent.meta.json').write_text(json.dumps(asdict(record('parent'))))
    assert store.list_page(tmp_path, limit=1)['related_headers'][0]['status'] == 'completed'


@pytest.mark.parametrize('stamp', [True, float('nan'), float('inf')])
def test_nonfinite_or_boolean_cursor_timestamp_is_rejected(tmp_path, stamp):
    token = base64.urlsafe_b64encode(json.dumps({'version': 1, 'scope': [catalog.Catalog(tmp_path, store.RECORD_SUFFIX).identity, False, None], 'stamp': stamp, 'id': 'a'}).encode()).decode()
    with pytest.raises(ValueError, match='cursor'):
        store.list_page(tmp_path, cursor=token)


def test_retention_deletion_removes_task_and_caller_index(tmp_path):
    from polybridge import retention
    task = record('old')
    store.write(tmp_path, task)
    store.list_page(tmp_path)
    assert retention._delete_task_files(tmp_path, task.task_id)
    with catalog.Catalog(tmp_path, store.RECORD_SUFFIX).connect() as db:
        assert db.execute('SELECT id FROM entries').fetchall() == []
        assert db.execute('SELECT id FROM callers').fetchall() == []


async def test_managed_task_page_cannot_leak_unrelated_ancestor_headers(tmp_path, monkeypatch, capsys):
    from polybridge import workflow_inspection
    directory = tmp_path / 'tasks'
    store.write(directory, record('secret-parent'))
    store.write(directory, replace(record('secret-child'), spawned_by='secret-parent'))
    store.list_page(directory)
    managed = ({'role': 'node', 'activation_id': 'own'}, {'activations': [{'id': 'own', 'role': 'node', 'tasks': [{'task_id': 'allowed'}]}]})
    monkeypatch.setattr(workflow_inspection, 'managed_page_reader', lambda path: (True, managed))
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=directory))
    page = await server.list_task_page(task_ids=['secret-child'])
    assert page['items'] == page['related_headers'] == []
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: directory)
    assert ctl.main(['task-list-page', '--task-ids', 'secret-child', '--json']) == 0
    result = json.loads(capsys.readouterr().out)['result']
    assert result['items'] == result['related_headers'] == []
    page = await server.list_task_page(limit=1)
    assert page['items'] == page['related_headers'] == []
    assert page['next_cursor'] is None and not page['has_more']
    assert ctl.main(['task-list-page', '--limit', '1', '--json']) == 0
    result = json.loads(capsys.readouterr().out)['result']
    assert result['next_cursor'] is None and not result['has_more']


def test_many_related_run_seeds_prioritize_the_shared_root(tmp_path):
    storage = workflows.WorkflowStore(tmp_path)
    root = storage.create_run(definition(), 'Root', tmp_path)
    seeds = []
    for number in range(40):
        child = storage.create_run(definition(), f'Child {number}', tmp_path)
        storage.update_run(child['workflow_run_id'], lambda r: r.update(parent_link={'workflow_run_id': root['workflow_run_id'], 'root_workflow_run_id': root['workflow_run_id']}), 'link')
        seeds.append(child['workflow_run_id'])
    related = storage.related_run_headers(seeds)
    assert len(related) <= 32
    assert related[1]['workflow_run_id'] == root['workflow_run_id']


def test_many_unloaded_task_parents_prioritize_shared_root(tmp_path):
    store.write(tmp_path, record('root'))
    for number in range(40):
        parent = f'parent{number:02}'
        store.write(tmp_path, replace(record(parent), spawned_by='root'))
        store.write(tmp_path, replace(record(f'child{number:02}', stamp='2026-10-03T00:00:00+00:00'), spawned_by=parent))
    page = store.list_page(tmp_path, limit=40)
    assert all(item['task_id'].startswith('child') for item in page['items'])
    assert page['related_headers'][1]['task_id'] == 'root'


@pytest.mark.parametrize('persisted', ['failed', 'cancelled', 'completed', 'timed_out'])
@pytest.mark.parametrize('identity_state', ['alive', 'undecidable', 'dead'])
def test_unobserved_terminal_headers_reconcile_indexed_identity_without_metadata_or_stream_read(tmp_path, monkeypatch, persisted, identity_state):
    task = replace(record('old'), status=persisted, exit_code=None, pid=123, start_time='captured', markers=['codex'])
    store.write(tmp_path, task)
    store.bootstrap_catalog(tmp_path)
    monkeypatch.setattr(store.identity, 'identity_check', lambda value: identity_state)
    monkeypatch.setattr(store, 'read', lambda *a, **k: pytest.fail('indexed reconciliation read metadata'))
    monkeypatch.setattr(store, 'replay_log', lambda *a, **k: pytest.fail('listing replayed stream'))
    page = store.list_page(tmp_path, active_only=True)
    item = page['items'][0]
    assert item['persisted_status'] == persisted and not item['observed_exit']
    assert item['process_identity_state'] == ('uncertain' if identity_state == 'undecidable' else identity_state)
    if identity_state != 'dead':
        assert item['status'] == 'running' and item['needs_reconciliation']
        assert page['total_active_count'] == (None if identity_state == 'undecidable' else 1)
    else:
        assert item['status'] == ('unknown' if persisted == 'failed' else persisted)
        assert item['needs_reconciliation'] == (persisted == 'failed')
        assert page['total_active_count'] == 0


def test_late_observed_completion_replaces_unobserved_running_projection(tmp_path, monkeypatch):
    task = replace(record('old'), status='failed', exit_code=None, pid=123)
    store.write(tmp_path, task)
    monkeypatch.setattr(store.identity, 'identity_check', lambda value: 'alive')
    assert store.list_page(tmp_path, active_only=True)['items'][0]['status'] == 'running'
    store.write(tmp_path, replace(task, status='completed', exit_code=0))
    item = store.list_page(tmp_path, task_ids=['old'])['items'][0]
    assert item['status'] == 'completed' and item['observed_exit'] and not item['needs_reconciliation']
    assert store.list_page(tmp_path, active_only=True)['total_active_count'] == 0


def test_selected_snapshot_caches_dead_stream_outcome_without_rewriting_metadata(tmp_path, monkeypatch):
    task = replace(record('old'), status='failed', exit_code=None, pid=123)
    store.write(tmp_path, task)
    original = (tmp_path / 'old.meta.json').read_bytes()
    monkeypatch.setattr(store.identity, 'identity_check', lambda value: 'dead')
    assert store.list_page(tmp_path)['items'][0]['needs_reconciliation']
    from polybridge.backends import Accumulator
    monkeypatch.setattr(store, '_resolve', lambda *a, **k: ('completed', 'Terminal stream proves completion', Accumulator(), [], False))
    assert store.snapshot(tmp_path, task)['status'] == 'completed'
    item = store.list_page(tmp_path, task_ids=['old'])['items'][0]
    assert item['status'] == 'completed' and not item['needs_reconciliation'] and item['status_reconciled']
    assert (tmp_path / 'old.meta.json').read_bytes() == original


def test_rolling_active_identity_inventory_converges_and_detects_unloaded_death(tmp_path, monkeypatch):
    for number in range(250):
        store.write(tmp_path, replace(record(f'active{number:03}'), status='running', exit_code=None, pid=1000 + number, markers=['codex']))
    while store.bootstrap_catalog(tmp_path):
        pass
    dead = set()
    checks = []
    def identity(value):
        checks.append(value['pid'])
        return 'dead' if value['pid'] in dead else 'alive'
    monkeypatch.setattr(store.identity, 'identity_check', identity)
    monkeypatch.setattr(store, 'read', lambda *a, **k: pytest.fail('rolling inventory read metadata'))
    monkeypatch.setattr(store, 'replay_log', lambda *a, **k: pytest.fail('rolling inventory replayed stream'))
    for _ in range(3):
        checks.clear()
        page = store.list_page(tmp_path, active_only=True)
        assert len(checks) <= 200
    assert page['counts_complete'] and page['total_active_count'] == 250
    assert 'active000' not in {item['task_id'] for item in page['items']}
    dead.add(1000)
    for _ in range(4):
        checks.clear()
        page = store.list_page(tmp_path, active_only=True)
        assert len(checks) <= 200
    assert page['counts_complete'] and page['total_active_count'] == 249
    item = store.list_page(tmp_path, task_ids=['active000'])['items'][0]
    assert item['status'] == 'unknown' and item['needs_reconciliation']


def test_oversized_legacy_metadata_is_discoverable_without_reading_or_claiming_complete_history(tmp_path, monkeypatch):
    from polybridge.catalog import Catalog, METADATA_BYTES
    (tmp_path / 'oversized.meta.json').write_bytes(b' ' * (METADATA_BYTES + 1))
    store.write(tmp_path, record('known'))
    original_read = store.read
    def read(*args, **kwargs):
        assert args[1] != 'oversized', 'oversized metadata must not be opened'
        return original_read(*args, **kwargs)
    monkeypatch.setattr(store, 'read', read)
    page = store.list_page(tmp_path)
    assert not page['bootstrap_pending'] and page['history_incomplete']
    assert not page['counts_complete'] and page['total_active_count'] is None
    items = {item['task_id']: item for item in page['items']}
    assert set(items) == {'known', 'oversized'}
    assert items['oversized']['needs_direct_lookup'] and items['oversized']['status'] == 'unknown'
    assert not Catalog(tmp_path, '.meta.json').ready()
    monkeypatch.setattr(store, 'read', original_read)
    store.write(tmp_path, record('oversized'))
    assert not store.list_page(tmp_path)['history_incomplete']


def test_listing_metadata_batch_budget_bounds_decoding(tmp_path):
    from polybridge.catalog import Catalog, METADATA_BATCH_BYTES
    size = 1024 * 1024
    for number in range(12):
        (tmp_path / f'{number:02}.meta.json').write_bytes(b' ' * size)
    calls = []
    def load(identifier):
        calls.append(identifier)
        return {'task_id': identifier, 'status': 'completed'}, float(int(identifier)), False
    page = Catalog(tmp_path, '.meta.json').page(load)
    assert len(calls) * size <= METADATA_BATCH_BYTES
    assert page['bootstrap_pending'] and not page['history_incomplete']
    assert page['catalog_state']['status'] == 'preparing' and not page['counts_complete']
    assert page['items'] == []
    final = Catalog(tmp_path, '.meta.json').page(load)
    assert not final['bootstrap_pending'] and final['catalog_state']['status'] == 'ready'


def test_oversized_run_header_defers_decode_until_selected_detail(tmp_path, monkeypatch):
    from polybridge.catalog import METADATA_BYTES
    storage = workflows.WorkflowStore(tmp_path)
    run = storage.create_run(definition(), 'Large historical run', tmp_path)
    identifier = run['workflow_run_id']
    path = storage.runs / f'{identifier}.json'
    run['legacy_large_field'] = 'x' * (METADATA_BYTES + 1)
    path.write_text(json.dumps(run))
    original = path.read_bytes()
    read = storage.get_run
    monkeypatch.setattr(storage, 'get_run', lambda *a, **k: pytest.fail('oversized listing opened workflow metadata'))
    page = storage.list_run_page()
    assert page['history_incomplete'] and not page['counts_complete']
    assert page['items'][0]['needs_direct_lookup']
    monkeypatch.setattr(storage, 'get_run', read)
    assert storage.get_run(identifier)['workflow_run_id'] == identifier
    assert not storage.list_run_page()['history_incomplete']
    assert path.read_bytes() == original


def test_oversized_task_authority_fails_closed_until_direct_snapshot_without_rewriting_metadata(tmp_path, monkeypatch):
    from polybridge.catalog import METADATA_BYTES
    from polybridge.workflow_inspection import page_indexing_response
    from polybridge.backends import Accumulator
    directory = tmp_path / 'tasks'
    directory.mkdir()
    task = replace(record('oversized'), prompt='x' * (METADATA_BYTES + 1))
    path = directory / 'oversized.meta.json'
    path.write_text(json.dumps(asdict(task)))
    original = path.read_bytes()
    human_authority(monkeypatch)
    assert managed_page_reader(directory) == (False, None)
    pending = page_indexing_response(directory)
    assert pending['authority_incomplete'] and pending['history_incomplete']
    assert pending['items'] == pending['related_headers'] == []
    assert not pending['bootstrap_pending']
    monkeypatch.setattr(store, '_resolve', lambda *a, **k: ('completed', 'Completed', Accumulator(), [], False))
    assert store.snapshot(directory, store.read(directory, 'oversized'))['status'] == 'completed'
    assert managed_page_reader(directory) == (True, None)
    assert path.read_bytes() == original


def test_batch_budget_placeholder_refresh_restores_creation_order_automatically(tmp_path):
    from polybridge.catalog import Catalog
    for number in range(3):
        (tmp_path / f'{number}.meta.json').write_bytes(b' ' * (3 * 1024 * 1024))
    def load(identifier):
        return {'task_id': identifier, 'status': 'completed'}, float(int(identifier) + 1), False
    first = Catalog(tmp_path, '.meta.json').page(load)
    assert first['bootstrap_pending'] and not first['history_incomplete']
    assert first['items'] == []
    assert first['catalog_state']['pending_records'] == 1
    Catalog(tmp_path, '.meta.json').page(load)  # Normal refresh, no direct inspection.
    final = Catalog(tmp_path, '.meta.json').page(load)
    assert not final['history_incomplete']
    assert [item['task_id'] for item in final['items']] == ['2', '1', '0']


def test_oversized_owner_receipts_are_bounded_without_json_decode_and_authority_fails_closed(tmp_path, monkeypatch):
    from polybridge import bounded_io
    directory = tmp_path / 'tasks'
    owners = tmp_path / 'workflow-owners'
    owners.mkdir()
    store.write(directory, record('parent'))
    store.write(directory, replace(record('child', stamp='2026-10-03T00:00:00+00:00'), spawned_by='parent'))
    for identifier in ('parent', 'child'):
        (owners / f'{identifier}.json').write_bytes(b'x' * (bounded_io.RECEIPT_BYTES + 1))
    monkeypatch.setattr(bounded_io, 'json', SimpleNamespace(loads=lambda value: pytest.fail('oversized receipt decoded JSON')))
    page = store.list_page(directory, limit=1)
    assert page['ownership_incomplete'] and page['history_incomplete'] and not page['counts_complete']
    assert all('workflow_run_id' not in item for item in page['items'] + page['related_headers'])
    storage = workflows.WorkflowStore(tmp_path)
    monkeypatch.setattr(storage, 'get_run', lambda *a, **k: pytest.fail('oversized authority receipt opened workflow'))
    with pytest.raises(bounded_io.ReadLimit):
        storage.task_owner('child', strict=True, metadata_byte_limit=4 * 1024 * 1024)


def test_actual_opened_metadata_bytes_respect_batch_budget_when_path_stat_is_stale(tmp_path, monkeypatch):
    from pathlib import Path
    from polybridge.catalog import Catalog, METADATA_BATCH_BYTES
    original_stat = Path.stat
    for number in range(3):
        task = replace(record(str(number)), prompt='x' * (3 * 1024 * 1024))
        (tmp_path / f'{number}.meta.json').write_text(json.dumps(asdict(task)))
    def stale_stat(path, *args, **kwargs):
        result = original_stat(path, *args, **kwargs)
        if path.name.endswith('.meta.json'):
            return SimpleNamespace(st_size=1, st_mtime_ns=result.st_mtime_ns)
        return result
    monkeypatch.setattr(Path, 'stat', stale_stat)
    catalog = Catalog(tmp_path, '.meta.json')
    def load(identifier, *, _metadata_budget=None):
        task = store.read(tmp_path, identifier, include_prompt=False, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=_metadata_budget)
        return store.listing_header(task)
    load.bounded_metadata = True
    page = catalog.page(load)
    assert catalog.metadata_bytes <= METADATA_BATCH_BYTES
    assert page['bootstrap_pending'] and not page['history_incomplete']
    assert page['catalog_state']['pending_records'] == 1
    assert page['items'] == []


def test_cached_caller_projection_omits_large_nonidentity_fields_and_refuses_oversized_identity(tmp_path):
    from polybridge.catalog import Catalog
    task = replace(record('small'), repo_path='x' * (3 * 1024 * 1024), owner={'large': 'y' * (3 * 1024 * 1024)}, markers=['codex'])
    store.write(tmp_path, task)
    with Catalog(tmp_path, '.meta.json').connect() as db:
        payload = db.execute("SELECT payload FROM callers WHERE id='small'").fetchone()[0]
        assert len(payload.encode()) <= 8 * 1024
        caller = json.loads(payload)
        assert caller['repo_path'] == '' and 'owner' not in caller and 'group' not in caller
        assert caller['markers'] == ['codex']
    store.write(tmp_path, replace(record('hugeidentity'), markers=['z' * (9 * 1024)]))
    with Catalog(tmp_path, '.meta.json').connect() as db:
        assert db.execute("SELECT 1 FROM callers WHERE id='hugeidentity'").fetchone() is None
        header = json.loads(db.execute("SELECT payload FROM entries WHERE id='hugeidentity'").fetchone()[0])
        assert header['needs_direct_lookup'] and header['status'] == 'unknown'
    assert not Catalog(tmp_path, '.meta.json').ready()


def test_many_legacy_receipt_decorations_share_one_workflow_metadata_budget(tmp_path, monkeypatch):
    from polybridge import bounded_io
    from polybridge.catalog import METADATA_BATCH_BYTES
    storage = workflows.WorkflowStore(tmp_path)
    run_ids = []
    for number in range(5):
        run = storage.create_run(definition(), str(number), tmp_path)
        run['large_legacy_field'] = 'x' * (3 * 1024 * 1024)
        (storage.runs / f"{run['workflow_run_id']}.json").write_text(json.dumps(run))
        run_ids.append(run['workflow_run_id'])
    for number in range(100):
        identifier = f't{number:03}'
        store.write(tmp_path / 'tasks', record(identifier))
        (storage.owners / f'{identifier}.json').write_text(json.dumps({'workflow_run_id': run_ids[number % 5]}))
    consumed = []
    read = bounded_io.read_json
    def measured(path, limit, *, budget=None):
        before = budget.metadata_bytes if budget is not None else 0
        try:
            return read(path, limit, budget=budget)
        finally:
            if path.parent == storage.runs and budget is not None:
                consumed.append(budget.metadata_bytes - before)
    monkeypatch.setattr(bounded_io, 'read_json', measured)
    page = store.list_page(tmp_path / 'tasks')
    assert sum(consumed) <= METADATA_BATCH_BYTES
    assert page['bootstrap_pending'] and not page.get('ownership_incomplete', False)
    assert page['catalog_state']['status'] == 'preparing' and not page['counts_complete']


@pytest.mark.parametrize('backend,title,current_start,expected_state', [
    ('vibe', 'Vibe CLI', 'Wed Sep 24 10:00:00 2026', 'alive'),
    ('vibe', 'Unrelated CLI', 'Wed Sep 24 10:00:00 2026', 'uncertain'),
    ('vibe', 'Vibe CLI', 'Wed Sep 24 11:00:00 2026', 'dead'),
    ('codex', 'Vibe CLI', 'Wed Sep 24 10:00:00 2026', 'uncertain'),
])
def test_indexed_active_inventory_uses_adapter_titles_only_with_matching_identity(tmp_path, monkeypatch, backend, title, current_start, expected_state):
    task = replace(record('retitled'), backend=backend, status='running', exit_code=None, pid=123,
                   start_time='Wed Sep 24 10:00:00 2026', markers=[backend, '/repo'])
    store.write(tmp_path, task)
    monkeypatch.setattr(store.identity, '_run_ps', lambda pid: SimpleNamespace(returncode=0, stdout=current_start + ' ' + title + '\n', stderr=''))
    monkeypatch.setattr(store, 'read', lambda *a, **k: pytest.fail('warm indexed identity loaded metadata'))
    page = store.list_page(tmp_path, active_only=True)
    item = page['items'][0]
    assert item['process_identity_state'] == expected_state
    if expected_state == 'alive':
        assert page['counts_complete'] and page['total_active_count'] == 1
        assert item['status'] == 'running' and not item['needs_reconciliation']
    elif expected_state == 'uncertain':
        assert not page['counts_complete'] and page['total_active_count'] is None
        assert item['needs_reconciliation']
    else:
        assert page['counts_complete'] and page['total_active_count'] == 0
        assert item['status'] == 'unknown' and item['needs_reconciliation']


async def test_selected_workflow_polling_never_scans_retained_task_history(tmp_path, monkeypatch):
    human_authority(monkeypatch)
    directory = tmp_path / 'tasks'
    legacy_tasks(directory, 250)
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=directory))
    storage = workflows.WorkflowStore(root=tmp_path)
    monkeypatch.setattr(workflows, 'WorkflowStore', lambda **kwargs: storage)
    monkeypatch.setattr(storage, 'get_run', lambda identifier, **kwargs: {'workflow_run_id': identifier, 'status': 'completed', 'definition': {}})
    for _ in range(3):
        pending = await server._workflow_call('status', run_id='selected', _bounded_read=True)
        assert pending['catalog_state']['status'] == 'preparing'
        assert 'workflow_run_id' not in pending
    for _ in range(4):
        assert (await server._workflow_call('status', run_id='selected', _bounded_read=True))['workflow_run_id'] == 'selected'
        assert (await server._workflow_call('detail', run_id='selected', view='definition', _bounded_read=True))['chunk'] == '{}'
        assert await server._bounded_workflow_caller() is None
    monkeypatch.setenv(lineage.ENV_TASK_ID, 'unverified-task')
    with pytest.raises(Exception, match='cannot be verified'):
        await server._workflow_call('status', run_id='selected', _bounded_read=True)
    with pytest.raises(Exception, match='cannot be verified'):
        await server._bounded_workflow_caller()


@pytest.mark.parametrize('role', ['orchestrator', 'node'])
async def test_bounded_selected_read_preserves_managed_owner_authority(tmp_path, monkeypatch, role):
    from polybridge import workflow_inspection
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=tmp_path / 'tasks'))
    managed = ({'role': role}, {'workflow_run_id': 'owned', 'definition': {}, 'activations': []})
    monkeypatch.setattr(workflow_inspection, 'managed_page_reader', lambda path: (True, managed))
    if role == 'node':
        with pytest.raises(Exception, match='Worker nodes'):
            await server._workflow_call('status', run_id='owned', _bounded_read=True)
    else:
        assert (await server._workflow_call('status', run_id='owned', _bounded_read=True))['workflow_run_id'] == 'owned'
        with pytest.raises(Exception, match='own workflow run'):
            await server._workflow_call('detail', run_id='unrelated', view='definition', _bounded_read=True)
        with pytest.raises(Exception, match='settled execution'):
            await server._workflow_call('detail', run_id='owned', view='executions', _bounded_read=True)


def test_placeholder_readiness_uses_partial_index_and_tracks_updates(tmp_path):
    (tmp_path / 'oversized.meta.json').write_text('{}')
    index = catalog.Catalog(tmp_path, store.RECORD_SUFFIX)
    with index.connect() as db:
        db.execute("INSERT OR REPLACE INTO state VALUES ('complete','1')")
        query = "SELECT 1 FROM entries WHERE json_extract(payload,'$.needs_direct_lookup')=1 LIMIT 1"
        plan = db.execute('EXPLAIN QUERY PLAN ' + query).fetchall()
        assert any('incomplete_headers' in row[3] for row in plan)
        index._put(db, {'task_id': 'oversized', 'needs_direct_lookup': True}, 0, True, 'oversized')
    # Directory fingerprint is independent of placeholder readiness.
    with index.connect() as db:
        db.execute("INSERT OR REPLACE INTO state VALUES ('directory_mtime',?)", (str(tmp_path.stat().st_mtime_ns),))
    assert not index.ready()
    with index.connect() as db:
        index._put(db, {'task_id': 'oversized', 'status': 'completed'}, 1, False, 'oversized')
    assert index.ready()
    with index.connect() as db:
        index._put(db, {'task_id': 'oversized', 'needs_direct_lookup': True}, 0, True, 'oversized')
    assert not index.ready()
    index.remove('oversized', previous_directory_mtime=tmp_path.stat().st_mtime_ns)
    with index.connect() as db:
        db.execute("INSERT OR REPLACE INTO state VALUES ('complete','1')")
        db.execute("INSERT OR REPLACE INTO state VALUES ('directory_mtime',?)", (str(tmp_path.stat().st_mtime_ns),))
    assert index.ready()


def test_schema_three_cache_rebuild_is_bounded_without_decoding_old_headers(tmp_path, monkeypatch):
    directory = tmp_path / 'tasks'
    legacy_tasks(directory, 205)
    originals = {p.name: p.read_bytes() for p in directory.glob('*.meta.json')}
    index = catalog.Catalog(directory, store.RECORD_SUFFIX)
    with index.connect() as db:
        db.execute('DROP INDEX incomplete_headers')
        db.execute("UPDATE state SET value='3' WHERE key='schema_version'")
        # Invalid old payload proves partial-index construction never evaluates it.
        db.executemany('INSERT INTO entries VALUES (?,0,0,NULL,?)', [(f'old{i}', 'invalid JSON') for i in range(250)])
    reads = []
    original = store.read
    monkeypatch.setattr(store, 'read', lambda *args, **kwargs: (reads.append(args[1]), original(*args, **kwargs))[1])
    page = store.list_page(directory)
    assert page['bootstrap_pending'] and len(reads) == 100
    with index.connect() as db:
        assert db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone() == ('8',)
        assert db.execute("SELECT count(*) FROM entries WHERE id LIKE 'old%'").fetchone() == (0,)
    assert originals == {p.name: p.read_bytes() for p in directory.glob('*.meta.json')}


def test_monitor_definition_list_uses_bounded_authority_but_public_first_read_is_unchanged(tmp_path, monkeypatch, capsys):
    from unittest.mock import Mock
    from polybridge import workflow_inspection
    human_authority(monkeypatch)
    directory = tmp_path / 'tasks'
    legacy_tasks(directory, 250)
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: directory)
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=directory))
    storage = workflows.WorkflowStore(root=tmp_path)
    monkeypatch.setattr(workflows, 'WorkflowStore', lambda **kwargs: storage)
    monkeypatch.setattr(storage, 'list', lambda: [{'name': 'saved'}])
    legacy = Mock(return_value=None)
    monkeypatch.setattr(workflow_inspection, 'managed_reader', legacy)
    # Ordinary CLI still succeeds on the first call with a cold task catalog.
    assert ctl.main(['workflow-list', '--json']) == 0
    assert json.loads(capsys.readouterr().out)['result']['workflows'] == [{'name': 'saved'}]
    assert legacy.call_count == 1
    for _ in range(3):
        assert ctl.main(['workflow-list', '--monitor-view', '--json']) == 0
        pending = json.loads(capsys.readouterr().out)['result']
        assert pending['catalog_state']['status'] == 'preparing'
        assert pending['workflows'] == []
    for _ in range(4):
        assert ctl.main(['workflow-list', '--monitor-view', '--json']) == 0
        assert json.loads(capsys.readouterr().out)['result']['workflows'] == [{'name': 'saved'}]
    assert legacy.call_count == 1
    monkeypatch.setenv(lineage.ENV_TASK_ID, 'unknown-task')
    assert ctl.main(['workflow-list', '--monitor-view', '--json']) != 0
    assert 'cannot be verified' in capsys.readouterr().out


@pytest.mark.parametrize('role', ['orchestrator', 'builder', 'node'])
async def test_monitor_definition_list_retains_managed_scope(tmp_path, monkeypatch, role):
    from polybridge import workflow_inspection
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=tmp_path / 'tasks'))
    managed = ({'role': role}, {'workflow_run_id': 'owned', 'definition': {'name': 'owned'}})
    monkeypatch.setattr(workflow_inspection, 'managed_page_reader', lambda path: (True, managed))
    if role == 'orchestrator':
        assert await server._workflow_call('list', _bounded_read=True) == [{'name': 'owned'}]
    else:
        with pytest.raises(Exception, match='Builders may only|Worker nodes'):
            await server._workflow_call('list', _bounded_read=True)


async def test_public_definition_list_does_not_require_catalog_bootstrap(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock
    from polybridge import workflow_inspection
    monkeypatch.setattr(server, '_managed_workflow_reader', AsyncMock(return_value=None))
    monkeypatch.setattr(workflow_inspection, 'managed_page_reader', lambda *args: pytest.fail('private Monitor reader'))
    storage = workflows.WorkflowStore(root=tmp_path)
    monkeypatch.setattr(workflows, 'WorkflowStore', lambda **kwargs: storage)
    monkeypatch.setattr(storage, 'list', lambda: [{'name': 'public'}])
    assert await server.list_workflows() == [{'name': 'public'}]


def test_discovery_queue_survives_unrelated_churn_and_new_earlier_names(tmp_path, monkeypatch):
    legacy_tasks(tmp_path, 350)
    calls, original = [], store.read
    monkeypatch.setattr(store, 'read', lambda *a, **k: (calls.append(a[1]), original(*a, **k))[1])
    for number, expected in enumerate((100, 100, 100, 51)):
        calls.clear()
        (tmp_path / f'churn{number}.jsonl').write_text('noise')
        (tmp_path / f'.temporary{number}').write_text('noise')
        if number == 1:
            (tmp_path / '000-earlier.meta.json').write_text(json.dumps(asdict(record('000-earlier'))))
        page = store.list_page(tmp_path)
        assert len(calls) == expected
        assert page['bootstrap_pending'] == (number < 3)
    assert catalog.Catalog(tmp_path, store.RECORD_SUFFIX).ready()
    assert len(set(calls)) == 51


def test_warm_catalog_readiness_does_not_enumerate_unchanged_history(tmp_path, monkeypatch):
    legacy_tasks(tmp_path, 2)
    store.list_page(tmp_path)
    index = catalog.Catalog(tmp_path, store.RECORD_SUFFIX)
    assert index.ready()
    monkeypatch.setattr(os, 'scandir', lambda *a: pytest.fail('warm discovery scan'))
    assert index.ready()
    assert not store.list_page(tmp_path)['bootstrap_pending']


def test_replaced_same_size_and_mtime_metadata_is_reindexed(tmp_path):
    import os
    old = record('replace')
    store.write(tmp_path, old)
    store.list_page(tmp_path)
    path = tmp_path / 'replace.meta.json'
    stat = path.stat()
    replacement = tmp_path / '.replacement'
    raw = path.read_text().replace('Completed', 'Different')
    replacement.write_text(raw)
    os.utime(replacement, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    os.replace(replacement, path)
    index = catalog.Catalog(tmp_path, store.RECORD_SUFFIX)
    assert not index.ready()
    assert store.list_page(tmp_path)['catalog_state']['status'] == 'ready'
    with index.connect() as db:
        assert db.execute('SELECT inode FROM sources WHERE id=?', ('replace',)).fetchone()[0] == path.stat().st_ino


def test_projection_failure_retains_committed_invalidation_and_recovers(tmp_path, monkeypatch):
    import sqlite3
    store.write(tmp_path, record('interrupted'))
    store.list_page(tmp_path)
    original = catalog.Catalog._put
    monkeypatch.setattr(catalog.Catalog, '_put', lambda *a, **k: (_ for _ in ()).throw(sqlite3.OperationalError('interrupted projection')))
    assert store.write_landed(tmp_path, replace(record('interrupted'), title='updated'))
    assert not catalog.Catalog(tmp_path, store.RECORD_SUFFIX).ready()
    monkeypatch.setattr(catalog.Catalog, '_put', original)
    page = store.list_page(tmp_path)
    assert page['catalog_state']['status'] == 'ready'
    assert page['items'][0]['title'] == 'updated'


def test_changed_discovery_snapshot_does_not_prune_or_claim_ready(tmp_path, monkeypatch):
    store.write(tmp_path, record('removed'))
    store.write(tmp_path, record('retained'))
    store.list_page(tmp_path)
    (tmp_path / 'removed.meta.json').unlink()
    original = catalog.Catalog._changed
    def changing(index, db, identifier):
        (tmp_path / 'arriving.meta.json').write_text(json.dumps(asdict(record('arriving'))))
        return original(index, db, identifier)
    monkeypatch.setattr(catalog.Catalog, '_changed', changing)
    index = catalog.Catalog(tmp_path, store.RECORD_SUFFIX)
    assert not index.ready()
    with index.connect() as db:
        assert db.execute("SELECT 1 FROM entries WHERE id='removed'").fetchone()
    monkeypatch.setattr(catalog.Catalog, '_changed', original)
    page = store.list_page(tmp_path)
    assert page['catalog_state']['status'] == 'ready'
    assert {item['task_id'] for item in page['items']} == {'arriving', 'retained'}


def test_unsupported_caller_identity_is_persistent_without_redecode(tmp_path, monkeypatch):
    task = replace(record('blocked'), markers=['x' * 9000])
    store.write(tmp_path, task)
    calls, original = [], store.read
    monkeypatch.setattr(store, 'read', lambda *a, **k: (calls.append(a[1]), original(*a, **k))[1])
    for number in range(3):
        (tmp_path / f'{number}.jsonl').write_text('noise')
        page = store.list_page(tmp_path)
        assert page['catalog_state']['status'] == 'blocked'
        assert not page['bootstrap_pending']
    assert not calls
    store.write(tmp_path, record('blocked'))
    assert store.list_page(tmp_path)['catalog_state']['status'] == 'ready'


def test_old_live_catalog_schema_is_untouched_and_old_writer_replacement_recovers(tmp_path):
    import sqlite3
    legacy = tmp_path / '.listing.sqlite3'
    with sqlite3.connect(legacy) as db:
        db.execute('CREATE TABLE sources(id TEXT PRIMARY KEY,mtime INTEGER,size INTEGER)')
        db.execute('CREATE TABLE state(key TEXT PRIMARY KEY,value TEXT)')
        db.execute("INSERT INTO state VALUES ('schema_version','6')")
    store.write(tmp_path, record('oldwriter', title='first'))
    store.list_page(tmp_path)
    # Simulate a still-running old process: authoritative atomic replacement and
    # its original derivative schema remain usable during the rolling install.
    path = tmp_path / 'oldwriter.meta.json'
    temporary = tmp_path / '.oldwriter-temp'
    temporary.write_text(json.dumps(asdict(record('oldwriter', title='second'))))
    os.replace(temporary, path)
    with sqlite3.connect(legacy) as db:
        db.execute('INSERT INTO sources VALUES (?,?,?)', ('oldwriter', path.stat().st_mtime_ns, path.stat().st_size))
        assert db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone() == ('6',)
    page = store.list_page(tmp_path)
    assert page['items'][0]['title'] == 'second'
    assert page['catalog_state']['status'] == 'ready'


def test_catalog_reader_waits_for_supported_writer_projection(tmp_path, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    store.write(tmp_path, record('serialized', status='running'))
    store.list_page(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original = catalog.Catalog.record
    def held(index, *args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(index, *args, **kwargs)
    monkeypatch.setattr(catalog.Catalog, 'record', held)
    with ThreadPoolExecutor(max_workers=2) as pool:
        writer = pool.submit(store.write_landed, tmp_path, record('serialized', title='terminal'))
        assert entered.wait(5)
        reader = pool.submit(store.list_page, tmp_path)
        assert not reader.done()
        release.set()
        assert writer.result(timeout=5)
        page = reader.result(timeout=5)
    assert page['catalog_state']['status'] == 'ready'
    assert page['items'][0]['title'] == 'terminal'
    assert page['items'][0]['status'] == 'completed'


def test_foreign_writer_overlapping_projection_cannot_acknowledge_stale_header(tmp_path, monkeypatch):
    store.write(tmp_path, record('overlap', title='initial'))
    store.list_page(tmp_path)
    original = catalog.Catalog.record
    def overlapping(index, header, *args, **kwargs):
        path = tmp_path / 'overlap.meta.json'
        foreign = tmp_path / '.foreign-temp'
        foreign.write_text(json.dumps(asdict(record('overlap', title='foreign'))))
        os.replace(foreign, path)
        return original(index, header, *args, **kwargs)
    monkeypatch.setattr(catalog.Catalog, 'record', overlapping)
    assert store.write_landed(tmp_path, record('overlap', title='newwriter'))
    assert not catalog.Catalog(tmp_path, store.RECORD_SUFFIX).ready()
    monkeypatch.setattr(catalog.Catalog, 'record', original)
    page = store.list_page(tmp_path)
    assert page['items'][0]['title'] == 'foreign'
    assert page['catalog_state']['status'] == 'ready'


@pytest.mark.parametrize('corrupted_cache', [False, True])
def test_catalog_advisory_lock_coordinates_a_separate_writer_process(tmp_path, corrupted_cache):
    import subprocess
    import sys
    from concurrent.futures import ThreadPoolExecutor
    store.write(tmp_path, record('processwriter', title='old'))
    store.list_page(tmp_path)
    if corrupted_cache:
        corrupt_derivative(tmp_path)
    script = '''
import json, os, sys
from pathlib import Path
from polybridge import catalog, store
root = Path(sys.argv[1])
with catalog.catalog_lock(root):
    index = catalog.Catalog(root, store.RECORD_SUFFIX)
    index.invalidate('processwriter')
    path = root / 'processwriter.meta.json'
    payload = json.loads(path.read_text())
    payload['title'] = 'new'
    temporary = root / '.processwriter-temp'
    temporary.write_text(json.dumps(payload))
    os.replace(temporary, path)
    print('metadata_replaced', flush=True)
    sys.stdin.readline()
    header, stamp, active = store.listing_header(store.read(root, 'processwriter'))
    index.record(header, stamp, active, 'processwriter')
'''
    child = subprocess.Popen([sys.executable, '-c', script, str(tmp_path)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == 'metadata_replaced'
        with ThreadPoolExecutor(max_workers=1) as pool:
            reader = pool.submit(store.list_page, tmp_path)
            assert not reader.done()
            child.stdin.write('finish\n')
            child.stdin.flush()
            page = reader.result(timeout=5)
        assert child.wait(timeout=5) == 0, child.stderr.read()
        assert page['catalog_state']['status'] == 'ready'
        assert page['items'][0]['title'] == 'new'
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)


def test_stale_selected_snapshot_cannot_stamp_a_newer_task_source(tmp_path, monkeypatch):
    from polybridge.backends import Accumulator
    stale = record('repair', title='old')
    store.write(tmp_path, stale)
    store.list_page(tmp_path)
    store.write(tmp_path, replace(stale, title='new'))
    monkeypatch.setattr(store, '_resolve', lambda *a, **k: ('completed', 'Completed', Accumulator(), [], False))
    store.snapshot(tmp_path, stale)
    page = store.list_page(tmp_path)
    assert page['items'][0]['title'] == 'new'
    assert page['catalog_state']['status'] == 'ready'


def test_direct_workflow_repair_retains_the_source_identity_it_decoded(tmp_path, monkeypatch):
    storage = workflows.WorkflowStore(tmp_path)
    run = storage.create_run(definition(), 'old', tmp_path)
    identifier = run['workflow_run_id']
    run['large_legacy_field'] = 'x' * (catalog.METADATA_BYTES + 1)
    path = storage.runs / f'{identifier}.json'
    path.write_text(json.dumps(run))
    assert storage.list_run_page()['catalog_state']['status'] == 'blocked'
    original = catalog.Catalog.record
    def replace_during_repair(index, header, *args, **kwargs):
        newer = dict(run)
        newer.pop('large_legacy_field')
        newer['name'] = 'new'
        temporary = storage.runs / '.newer-run'
        temporary.write_text(json.dumps(newer))
        os.replace(temporary, path)
        return original(index, header, *args, **kwargs)
    monkeypatch.setattr(catalog.Catalog, 'record', replace_during_repair)
    assert storage.get_run(identifier)['name'] == run['name']
    assert not storage._ownership_catalog().ready()
    monkeypatch.setattr(catalog.Catalog, 'record', original)
    page = storage.list_run_page()
    assert page['items'][0]['name'] == 'new'
    assert page['catalog_state']['status'] == 'ready'


def test_managed_ownership_decode_and_page_share_one_aggregate_budget(tmp_path, monkeypatch):
    from polybridge import bounded_io
    directory = tmp_path / 'tasks'
    storage = workflows.WorkflowStore(tmp_path)
    run = storage.create_run(definition(), 'x' * (3 * 1024 * 1024), tmp_path)
    storage.update_run(run['workflow_run_id'], lambda value: value.update(activations=[{'id': 'execution', 'node_id': 'work', 'role': 'orchestrator', 'status': 'completed', 'tasks': [{'task_id': 'caller', 'status': 'completed'}]}]), 'seed')
    task = record('caller')
    store.write(directory, task)
    store.list_page(directory)
    monkeypatch.setattr(lineage, 'detect_catalog_caller', lambda *a, **k: SimpleNamespace(caller=SimpleNamespace(record=task), undecidable=None))
    original_read = bounded_io.read_json
    consumed, run_reads = [], []
    def measured(path, limit, *, budget=None):
        result = original_read(path, limit, budget=budget)
        if path.name.endswith('.meta.json') or path.parent == storage.runs:
            assert budget is not None
            consumed.append(path.stat().st_size)
            if path.parent == storage.runs:
                run_reads.append(path.name)
        return result
    monkeypatch.setattr(bounded_io, 'read_json', measured)
    with catalog.metadata_request():
        ready, managed = managed_page_reader(directory)
        assert ready and managed[0]['role'] == 'orchestrator'
        for number in range(100):
            historical = record(f'new{number:03}', prompt='x' * (100 * 1024))
            (directory / f'{historical.task_id}.meta.json').write_text(json.dumps(asdict(historical)))
        page = store.list_page(directory)
        assert page['bootstrap_pending']
        assert sum(consumed) <= catalog.METADATA_BATCH_BYTES
        assert len(consumed) <= catalog.BOOTSTRAP_LIMIT
        assert run_reads == [f"{run['workflow_run_id']}.json"]


def test_active_page_cursor_does_not_skip_a_byte_budget_deferred_row(tmp_path):
    sizes = 1024 * 1024
    index = catalog.Catalog(tmp_path, '.meta.json')
    for number, identifier in enumerate(('a', 'b', 'c'), start=1):
        (tmp_path / f'{identifier}.meta.json').write_bytes(b' ' * sizes)
        index.record({'task_id': identifier, 'status': 'running'}, number, True, identifier)
    def load(identifier):
        return {'task_id': identifier, 'status': 'running'}, float(ord(identifier) - ord('a') + 1), True
    assert not catalog.Catalog(tmp_path, '.meta.json').page(load, active_only=True)['bootstrap_pending']
    # These source rewrites do not change the directory namespace. One refresh fits
    # the remainder; the following row must stay after the returned cursor.
    for identifier in ('a', 'b'):
        (tmp_path / f'{identifier}.meta.json').write_bytes(b'x' * sizes)
    index = catalog.Catalog(tmp_path, '.meta.json')
    index.metadata_bytes = catalog.METADATA_BATCH_BYTES - sizes - 10
    first = index.page(load, active_only=True)
    assert [item['task_id'] for item in first['items']] == ['c', 'b']
    assert first['bootstrap_pending'] and first['catalog_state']['status'] == 'preparing'
    boundary = json.loads(base64.urlsafe_b64decode(first['next_cursor']))
    assert boundary['id'] == 'b'
    second = catalog.Catalog(tmp_path, '.meta.json').page(load, active_only=True, cursor=first['next_cursor'])
    assert [item['task_id'] for item in second['items']] == ['a']


def test_active_page_never_publishes_a_changed_row_after_decode_budget_is_exhausted(tmp_path):
    index = catalog.Catalog(tmp_path, '.meta.json')
    path = tmp_path / 'changed.meta.json'
    path.write_text('old')
    index.record({'task_id': 'changed', 'status': 'running', 'title': 'old'}, 1, True, 'changed')
    def load(identifier):
        return {'task_id': identifier, 'status': 'completed', 'title': 'new'}, 1, False
    catalog.Catalog(tmp_path, '.meta.json').page(load, active_only=True)
    path.write_text('new')
    index = catalog.Catalog(tmp_path, '.meta.json')
    index.metadata_decodes = catalog.BOOTSTRAP_LIMIT
    deferred = index.page(load, active_only=True)
    assert deferred['items'] == []
    assert deferred['bootstrap_pending'] and deferred['catalog_state']['status'] == 'preparing'
    assert not deferred['counts_complete']


@pytest.mark.parametrize('kind', ['tasks', 'runs'])
def test_explicit_changed_headers_defer_neutrally_after_one_ownership_decode(tmp_path, kind):
    from polybridge.bounded_io import read_json
    directory = tmp_path / 'tasks'
    if kind == 'tasks':
        identifiers = [f'changed{number:03}' for number in range(100)]
        for identifier in identifiers:
            store.write(directory, record(identifier, title='old'))
        query = lambda: store.list_page(directory, task_ids=identifiers)
        paths = [directory / f'{identifier}.meta.json' for identifier in identifiers]
        change = lambda payload: payload.update(title='new')
    else:
        storage = workflows.WorkflowStore(tmp_path)
        runs = [storage.create_run(definition(), 'request', tmp_path) for _ in range(100)]
        identifiers = [run['workflow_run_id'] for run in runs]
        query = lambda: storage.list_run_page(run_ids=identifiers)
        paths = [storage.runs / f'{identifier}.json' for identifier in identifiers]
        change = lambda payload: payload.update(name='new')
    assert len(query()['items']) == 100
    for path in paths:
        payload = json.loads(path.read_text())
        change(payload)
        path.write_text(json.dumps(payload))
    ownership = tmp_path / 'owned.json'
    ownership.write_text('{}')
    with catalog.metadata_request():
        budget = catalog.Catalog(tmp_path, '.authority')
        read_json(ownership, catalog.METADATA_BYTES, budget=budget)
        first = query()
        assert budget.metadata_decodes == catalog.BOOTSTRAP_LIMIT
        assert first['bootstrap_pending'] and first['catalog_state']['status'] == 'preparing'
        assert first['catalog_state']['pending_records'] == 1
        assert first['items'] == []
        assert not first['counts_complete']
    second = query()
    assert not second['bootstrap_pending']
    assert len(second['items']) == 100
    key = 'title' if kind == 'tasks' else 'name'
    assert all(item[key] == 'new' for item in second['items'])
    assert all(item['status'] != 'unknown' for item in second['items'])


@pytest.mark.parametrize('transport', ['server', 'cli'])
async def test_managed_explicit_header_deferral_keeps_counts_unavailable_and_recovers(tmp_path, monkeypatch, capsys, transport):
    directory = tmp_path / 'tasks'
    storage = workflows.WorkflowStore(tmp_path)
    run = storage.create_run(definition(), 'request', tmp_path)
    identifiers = [f'member{number:03}' for number in range(100)]
    storage.update_run(run['workflow_run_id'], lambda value: value.update(activations=[
        {'id': 'orchestrator', 'node_id': 'orchestrator', 'role': 'orchestrator', 'status': 'completed', 'tasks': [{'task_id': 'caller', 'status': 'completed'}]},
        {'id': 'execution', 'node_id': 'work', 'role': 'node', 'status': 'completed', 'node_result': {'status': 'succeeded', 'result': {}, 'evidence': []}, 'tasks': [{'task_id': identifier, 'status': 'completed'} for identifier in identifiers]}
    ]), 'seed')
    caller = record('caller')
    store.write(directory, caller)
    for identifier in identifiers:
        store.write(directory, record(identifier, title='old'))
    store.list_page(directory)
    monkeypatch.setattr(lineage, 'detect_catalog_caller', lambda *a, **k: SimpleNamespace(caller=SimpleNamespace(record=caller), undecidable=None))
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=directory))
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: directory)
    for identifier in identifiers:
        path = directory / f'{identifier}.meta.json'
        payload = json.loads(path.read_text())
        payload['title'] = 'new'
        path.write_text(json.dumps(payload))
    async def query():
        if transport == 'server':
            return await server.list_task_page(task_ids=identifiers)
        assert ctl.main(['task-list-page', '--task-ids', ','.join(identifiers), '--json']) == 0
        return json.loads(capsys.readouterr().out)['result']
    first = await query()
    assert first['bootstrap_pending'] and first['catalog_state']['status'] == 'preparing'
    assert first['items'] == [] and first['total_active_count'] is None
    assert not first['counts_complete']
    # One read processes the queued last source; the following read establishes
    # caller ownership using the fresh projection and returns the full requested set.
    await query()
    recovered = await query()
    assert not recovered['bootstrap_pending']
    assert len(recovered['items']) == 100
    assert all(item['title'] == 'new' and item['status'] == 'completed' for item in recovered['items'])
    assert recovered['total_active_count'] == 0


@pytest.mark.parametrize('read_kind', ['page', 'headers'])
def test_metadata_disappearing_during_decode_defers_without_a_listing_error(tmp_path, read_kind):
    path = tmp_path / 'disappearing.meta.json'
    path.write_text('{}')
    index = catalog.Catalog(tmp_path, '.meta.json')
    def load(identifier):
        path.unlink()
        return {'task_id': identifier, 'status': 'completed'}, 1, False
    first = index.page(load) if read_kind == 'page' else index.header_page(['disappearing'], load)
    assert first['items'] == []
    assert first['bootstrap_pending'] and first['catalog_state']['status'] == 'preparing'
    recovered = catalog.Catalog(tmp_path, '.meta.json').page(load)
    assert recovered['items'] == [] and not recovered['bootstrap_pending']
    assert recovered['catalog_state']['status'] == 'ready'


async def test_concurrent_caller_preparation_survives_worker_hops_without_shared_diagnostics(tmp_path, monkeypatch):
    import asyncio
    from contextvars import ContextVar
    from polybridge.bounded_io import read_json
    from polybridge.workflow_inspection import page_indexing_response
    directory = tmp_path / 'tasks'
    storage = workflows.WorkflowStore(tmp_path)
    callers = {name: record(name) for name in ('blocked-caller', 'preparing-caller')}
    for name, caller in callers.items():
        prompt = 'x' * catalog.METADATA_BYTES if name == 'blocked-caller' else 'request'
        run = storage.create_run(definition(), prompt, tmp_path)
        storage.update_run(run['workflow_run_id'], lambda value, name=name: value.update(activations=[
            {'id': 'orchestrator', 'node_id': 'orchestrator', 'role': 'orchestrator', 'status': 'completed', 'tasks': [{'task_id': name, 'status': 'completed'}]}
        ]), 'seed')
        store.write(directory, caller)
    store.list_page(directory)
    request_caller = ContextVar('request_caller')
    monkeypatch.setattr(lineage, 'detect_catalog_caller', lambda *a, **k: SimpleNamespace(caller=SimpleNamespace(record=callers[request_caller.get()]), undecidable=None))
    prior_metadata = tmp_path / 'prior-metadata.json'
    prior_metadata.write_text('"' + 'x' * (2 * 1024 * 1024 - 2) + '"')
    blocked_prepared, other_done = asyncio.Event(), asyncio.Event()
    async def blocked_request():
        with catalog.metadata_request():
            request_caller.set('blocked-caller')
            assert await asyncio.to_thread(managed_page_reader, directory) == (False, None)
            blocked_prepared.set()
            await asyncio.wait_for(other_done.wait(), timeout=5)
            # The response runs in another worker, as server._workflow_call does.
            return await asyncio.to_thread(page_indexing_response, directory)
    async def preparing_request():
        await asyncio.wait_for(blocked_prepared.wait(), timeout=5)
        with catalog.metadata_request():
            request_caller.set('preparing-caller')
            budget = catalog.Catalog(directory, store.RECORD_SUFFIX)
            for _ in range(4):
                await asyncio.to_thread(read_json, prior_metadata, catalog.METADATA_BYTES, budget=budget)
            assert await asyncio.to_thread(managed_page_reader, directory) == (False, None)
            result = await asyncio.to_thread(page_indexing_response, directory)
            other_done.set()
            return result
    blocked, preparing = await asyncio.gather(blocked_request(), preparing_request())
    assert blocked['catalog_state']['status'] == 'blocked'
    assert blocked['catalog_state']['source'] == 'caller_authority'
    assert blocked['catalog_state']['blocker_types'] == ['oversized_metadata']
    assert blocked['authority_incomplete'] and not blocked['bootstrap_pending']
    assert preparing['catalog_state']['status'] == 'preparing'
    assert preparing['bootstrap_pending'] and not preparing['authority_incomplete']
    assert blocked['items'] == preparing['items'] == []
    with catalog.Catalog(directory, store.RECORD_SUFFIX).connect() as db:
        assert db.execute("SELECT 1 FROM state WHERE key='authority_preparation'").fetchone() is None


@pytest.mark.parametrize('kind', ['task', 'workflow'])
def test_explicit_oversized_headers_are_blocked_without_unknown_rows(tmp_path, kind):
    if kind == 'task':
        directory = tmp_path / 'tasks'
        store.write(directory, record('selected'))
        path = directory / 'selected.meta.json'
        read_page = lambda: store.list_page(directory, task_ids=['selected'])
    else:
        storage = workflows.WorkflowStore(tmp_path)
        run = storage.create_run(definition(), 'Selected', tmp_path)
        identifier = run['workflow_run_id']
        path = storage.runs / f'{identifier}.json'
        read_page = lambda: storage.list_run_page(run_ids=[identifier])
    original = path.read_text()
    payload = json.loads(original)
    payload['prompt'] = 'x' * (catalog.METADATA_BYTES + 1)
    path.write_text(json.dumps(payload))
    for _ in range(2):
        page = read_page()
        assert page['items'] == []
        assert not page['bootstrap_pending'] and not page['counts_complete']
        assert page['catalog_state']['status'] == 'blocked'
        assert page['catalog_state']['blocker_types'] == ['oversized_metadata']
        assert page['history_incomplete']
    path.write_text(original)
    assert len(read_page()['items']) == 1


def large_ancestry(storage, tmp_path):
    runs = [storage.create_run(definition(), str(i), tmp_path) for i in range(3)]
    for i, run in enumerate(runs):
        if i < 2:
            run['parent_link'] = {'workflow_run_id': runs[i + 1]['workflow_run_id']}
        run['large_legacy_field'] = 'x' * (3 * 1024 * 1024)
        (storage.runs / f"{run['workflow_run_id']}.json").write_text(json.dumps(run))
    return [run['workflow_run_id'] for run in runs]


def test_workflow_ancestry_budget_preparation_recovers_next_request(tmp_path):
    storage = workflows.WorkflowStore(tmp_path)
    identifiers = large_ancestry(storage, tmp_path)
    with catalog.metadata_request():
        budget = catalog._request_budget.get()
        headers = storage.related_run_headers(identifiers[:1])
        assert budget.metadata_bytes <= catalog.METADATA_BATCH_BYTES
    assert len(headers) == 2
    assert headers.catalog_state['status'] == 'preparing'
    with catalog.metadata_request():
        budget = catalog._request_budget.get()
        recovered = storage.related_run_headers(identifiers[:1])
        assert budget.metadata_bytes <= catalog.METADATA_BATCH_BYTES
    assert [header['workflow_run_id'] for header in recovered] == identifiers
    assert recovered.catalog_state is None


@pytest.mark.asyncio
@pytest.mark.parametrize('route', ['related', 'page'])
async def test_workflow_related_transport_retries_deferred_ancestry(tmp_path, monkeypatch, route):
    human_authority(monkeypatch)
    directory = tmp_path / 'tasks'
    directory.mkdir()
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=directory))
    storage = workflows.WorkflowStore(tmp_path)
    identifiers = large_ancestry(storage, tmp_path)
    monkeypatch.setattr(workflows, 'WorkflowStore', lambda: storage)
    request = {'related_run_id': identifiers[0]} if route == 'related' else {'run_ids': identifiers[:1]}
    page = await server.list_workflow_run_page(**request)
    assert len(page['related_headers']) == (2 if route == 'related' else 1)
    assert page['bootstrap_pending'] and not page['counts_complete']
    assert page['catalog_state']['status'] == 'preparing'
    recovered = await server.list_workflow_run_page(**request)
    assert len(recovered['related_headers']) == (3 if route == 'related' else 2)
    assert not recovered['bootstrap_pending']


@pytest.mark.asyncio
async def test_workflow_page_propagates_blocked_parent_headers(tmp_path, monkeypatch):
    human_authority(monkeypatch)
    storage = workflows.WorkflowStore(tmp_path)
    parent = storage.create_run(definition(), 'Parent', tmp_path)
    child = storage.create_run(definition(), 'Child', tmp_path)
    identifier = child['workflow_run_id']
    child['parent_link'] = {'workflow_run_id': parent['workflow_run_id']}
    (storage.runs / f"{identifier}.json").write_text(json.dumps(child))
    parent['large_legacy_field'] = 'x' * (catalog.METADATA_BYTES + 1)
    (storage.runs / f"{parent['workflow_run_id']}.json").write_text(json.dumps(parent))
    directory = tmp_path / 'tasks'
    directory.mkdir()
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=directory))
    monkeypatch.setattr(workflows, 'WorkflowStore', lambda: storage)
    for _ in range(2):
        page = await server.list_workflow_run_page(run_ids=[identifier])
        assert page['items'][0]['workflow_run_id'] == identifier
        assert page['related_headers'] == []
        assert page['catalog_state']['status'] == 'blocked'
        assert not page['bootstrap_pending'] and not page['counts_complete']


def test_task_page_blocked_receipt_and_ancestor_never_publish_unknown_authority(tmp_path):
    storage = workflows.WorkflowStore(tmp_path)
    run = storage.create_run(definition(), 'Workflow', tmp_path)
    directory = tmp_path / 'tasks'
    store.write(directory, record('parent'))
    store.write(directory, record('child', parent_task_id='parent'))
    (storage.owners / 'child.json').write_text(json.dumps({'workflow_run_id': run['workflow_run_id']}))
    run['large_legacy_field'] = 'x' * (catalog.METADATA_BYTES + 1)
    (storage.runs / f"{run['workflow_run_id']}.json").write_text(json.dumps(run))
    payload = asdict(record('parent', prompt='x' * (catalog.METADATA_BYTES + 1)))
    (directory / 'parent.meta.json').write_text(json.dumps(payload))
    for _ in range(2):
        page = store.list_page(directory, task_ids=['child'])
        assert 'workflow_status' not in page['items'][0]
        assert page['related_headers'] == []
        assert page['catalog_state']['status'] == 'blocked'
        assert not page['counts_complete'] and not page['bootstrap_pending']


def test_explicit_unsupported_caller_first_projection_is_blocked(tmp_path):
    directory = tmp_path / 'tasks'
    directory.mkdir()
    (directory / 'unsupported.meta.json').write_text(json.dumps(asdict(record('unsupported', markers=['x' * 5000]))))
    page = store.list_page(directory, task_ids=['unsupported'])
    assert page['items'] == []
    assert page['catalog_state']['status'] == 'blocked'
    assert page['catalog_state']['blocker_types'] == ['unsupported_caller_identity']


def test_mixed_read_states_preserve_retry_without_hiding_persistent_blocker():
    blocked = {'status': 'blocked', 'source': 'task_catalog', 'pending_records': 0, 'blocked_records': 1, 'blocker_types': ['oversized_metadata']}
    preparing = {'status': 'preparing', 'source': 'workflow_catalog', 'pending_records': 1, 'blocked_records': 0}
    for states in ((blocked, preparing, blocked), (preparing, blocked, blocked)):
        page = {'bootstrap_pending': False, 'total_active_count': 4}
        for state in states:
            catalog.apply_read_state(page, state)
        assert page['catalog_state']['status'] == 'blocked'
        assert page['catalog_state']['pending_records'] == 1
        assert page['bootstrap_pending'] and page['total_active_count'] is None
    pure_blocked = {'bootstrap_pending': False}
    catalog.apply_read_state(pure_blocked, blocked)
    assert not pure_blocked['bootstrap_pending']


def test_blocked_ancestry_seed_does_not_hide_other_seed_budget_recovery(tmp_path):
    storage = workflows.WorkflowStore(tmp_path)
    blocked = storage.create_run(definition(), 'Blocked', tmp_path)
    blocked['large_legacy_field'] = 'x' * (catalog.METADATA_BYTES + 1)
    (storage.runs / f"{blocked['workflow_run_id']}.json").write_text(json.dumps(blocked))
    chain = large_ancestry(storage, tmp_path)
    seeds = [blocked['workflow_run_id'], chain[0]]
    with catalog.metadata_request():
        first = storage.related_run_headers(seeds)
    assert len(first) == 2
    assert first.catalog_state['status'] == 'blocked'
    assert first.catalog_state['pending_records'] == 1
    with catalog.metadata_request():
        recovered = storage.related_run_headers(seeds)
    assert len(recovered) == 3
    assert recovered.catalog_state['status'] == 'blocked'
    assert recovered.catalog_state['pending_records'] == 0


def test_explicit_task_page_loads_unindexed_parent_and_reports_blocker(tmp_path):
    directory = tmp_path / 'tasks'
    store.write(directory, record('child', parent_task_id='legacy-parent'))
    parent = record('legacy-parent', prompt='x' * (catalog.METADATA_BYTES + 1))
    (directory / 'legacy-parent.meta.json').write_text(json.dumps(asdict(parent)))
    page = store.list_page(directory, task_ids=['child'])
    assert page['related_headers'] == []
    assert page['catalog_state']['status'] == 'blocked'
    assert not page['counts_complete']


def test_foreign_insertion_between_writer_prestamp_and_replacement_requires_discovery(tmp_path, monkeypatch):
    store.write(tmp_path, record('known', title='original'))
    store.list_page(tmp_path)
    index = catalog.Catalog(tmp_path, store.RECORD_SUFFIX)
    original_read = store.read
    original_discover = catalog.Catalog._discover
    inserted = False
    discovery_calls = []

    def foreign_during_guard(directory, identifier, *args, **kwargs):
        nonlocal inserted
        result = original_read(directory, identifier, *args, **kwargs)
        if identifier == 'known' and not inserted:
            inserted = True
            temporary = directory / '.foreign-temp'
            temporary.write_text(json.dumps(asdict(record('foreign', title='older process insertion'))))
            os.replace(temporary, directory / 'foreign.meta.json')
        return result

    monkeypatch.setattr(store, 'read', foreign_during_guard)
    assert store.write_landed(tmp_path, record('known', title='updated'))
    monkeypatch.setattr(store, 'read', original_read)
    # Inspect readiness before reconciliation: the writer has projected its own
    # record, but the prestamp cannot account for this unrelated insertion.
    with index.connect() as db:
        assert index.state(db)['status'] == 'preparing'
        assert db.execute("SELECT COUNT(*) FROM entries WHERE id='foreign'").fetchone()[0] == 0

    def tracked_discovery(current, db):
        discovery_calls.append(current.directory)
        return original_discover(current, db)

    monkeypatch.setattr(catalog.Catalog, '_discover', tracked_discovery)
    assert not index.ready()
    page = store.list_page(tmp_path)
    assert discovery_calls
    assert page['catalog_state']['status'] == 'ready'
    assert {item['task_id'] for item in page['items']} == {'known', 'foreign'}
    assert next(item for item in page['items'] if item['task_id'] == 'known')['title'] == 'updated'


def test_removal_after_projection_validation_cannot_commit_source_less_authority(tmp_path, monkeypatch):
    path = tmp_path / 'removed.meta.json'
    path.write_text('{}')
    index = catalog.Catalog(tmp_path, '.meta.json')
    original_bound = catalog.bound_header
    removed = False
    def remove_during_projection(header):
        nonlocal removed
        result = original_bound(header)
        if not removed:
            removed = True
            path.unlink()
        return result
    def load(identifier):
        return {'task_id': identifier, 'status': 'completed',
                '_caller': asdict(record(identifier)),
                '_associations': {'worker': {'workflow_run_id': identifier}},
                '_checkout': [{'task_id': 'worker', 'repo_path': '/repo'}]}, 1, False
    monkeypatch.setattr(catalog, 'bound_header', remove_during_projection)
    first = index.page(load)
    assert first['items'] == []
    assert first['bootstrap_pending']
    assert first['catalog_state']['status'] == 'preparing'
    with index.connect() as db:
        for table in ('entries', 'callers', 'associations', 'checkout_tasks', 'sources'):
            assert db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] == 0
    recovered = catalog.Catalog(tmp_path, '.meta.json').page(load)
    assert recovered['items'] == []
    assert recovered['catalog_state']['status'] == 'ready'


def test_verified_discovery_prunes_preexisting_source_less_projections(tmp_path):
    path = tmp_path / 'orphan.meta.json'
    path.write_text('{}')
    index = catalog.Catalog(tmp_path, '.meta.json')
    def load(identifier):
        return {'task_id': identifier, 'status': 'completed',
                '_caller': asdict(record(identifier)),
                '_associations': {'worker': {'workflow_run_id': identifier}},
                '_checkout': [{'task_id': 'worker', 'repo_path': '/repo'}]}, 1, False
    assert index.page(load)['catalog_state']['status'] == 'ready'
    # Reproduce the durable derivative state left by the earlier projection race.
    with index.connect() as db:
        db.execute('DELETE FROM sources')
    path.unlink()
    assert index.ready()
    with index.connect() as db:
        for table in ('entries', 'callers', 'associations', 'checkout_tasks', 'sources'):
            assert db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] == 0
    page = catalog.Catalog(tmp_path, '.meta.json').page(load)
    assert page['items'] == []
    assert page['catalog_state']['status'] == 'ready'
    with index.connect() as db:
        for table in ('entries', 'callers', 'associations', 'checkout_tasks', 'sources'):
            assert db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] == 0


def test_upgrade_rebuilds_previously_acknowledged_source_less_authority(tmp_path):
    orphan = tmp_path / 'orphan.meta.json'
    survivor = tmp_path / 'retained.meta.json'
    orphan.write_text('{}')
    survivor.write_text('{}')
    original = survivor.read_bytes()
    index = catalog.Catalog(tmp_path, '.meta.json')
    calls = []
    def load(identifier):
        calls.append(identifier)
        return {'task_id': identifier, 'status': 'completed',
                '_caller': asdict(record(identifier)),
                '_associations': {f'{identifier}-worker': {'workflow_run_id': identifier}},
                '_checkout': [{'task_id': f'{identifier}-worker', 'repo_path': '/repo'}]}, 1, False
    assert index.page(load)['catalog_state']['status'] == 'ready'
    orphan.unlink()
    with index.connect() as db:
        # Old code could acknowledge this missing namespace while orphaned
        # entries/callers survived because no sources row existed to prune.
        db.execute("DELETE FROM sources WHERE id='orphan'")
        db.execute("INSERT OR REPLACE INTO state VALUES ('complete','1')")
        db.execute("INSERT OR REPLACE INTO state VALUES ('directory_mtime',?)", (str(tmp_path.stat().st_mtime_ns),))
        db.execute("INSERT OR REPLACE INTO state VALUES ('schema_version','7')")
    calls.clear()
    upgraded = catalog.Catalog(tmp_path, '.meta.json')
    assert not upgraded.ready()
    assert not calls
    with upgraded.connect() as db:
        for table in ('entries', 'callers', 'associations', 'checkout_tasks', 'sources'):
            assert db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] == 0
        assert db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone() == ('8',)
    page = upgraded.page(load)
    assert page['catalog_state']['status'] == 'ready'
    assert [item['task_id'] for item in page['items']] == ['retained']
    assert calls == ['retained']
    assert survivor.read_bytes() == original


def test_live_schema_seven_runtime_cannot_reset_schema_eight_discovery_progress(tmp_path, monkeypatch):
    import sqlite3
    legacy_tasks(tmp_path, 205)
    originals = {path.name: path.read_bytes() for path in tmp_path.glob('*.meta.json')}
    legacy = tmp_path / '.listing.v7.sqlite3'
    with sqlite3.connect(legacy) as db:
        db.execute('CREATE TABLE state(key TEXT PRIMARY KEY,value TEXT)')
        db.execute("INSERT INTO state VALUES ('schema_version','7')")
        db.execute("INSERT INTO state VALUES ('legacy_runtime','preserved')")
    original_read = store.read
    calls = []
    monkeypatch.setattr(store, 'read', lambda *args, **kwargs: (calls.append(args[1]), original_read(*args, **kwargs))[1])
    for expected in (100, 100, 5):
        calls.clear()
        page = store.list_page(tmp_path)
        assert len(calls) == expected
        assert page['bootstrap_pending'] == (expected == 100)
        # Model the old initializer reconnecting between new-version requests.
        # A shared filename would rebuild these tables back to schema 7 and
        # erase the new persistent discovery queue on every iteration.
        with sqlite3.connect(legacy) as db:
            version = db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone()
            if version != ('7',):
                tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                for table in ('entries', 'callers', 'associations', 'checkout_tasks', 'sources', 'pending', 'seen', 'state'):
                    if table in tables:
                        db.execute(f'DELETE FROM {table}')
                db.execute("INSERT INTO state VALUES ('schema_version','7')")
            assert db.execute("SELECT value FROM state WHERE key='legacy_runtime'").fetchone() == ('preserved',)
    assert page['catalog_state']['status'] == 'ready'
    assert (tmp_path / '.listing.v8.sqlite3').exists()
    assert originals == {path.name: path.read_bytes() for path in tmp_path.glob('*.meta.json')}


def corrupt_derivative(directory):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / '.listing.v8.sqlite3').write_bytes(b'corrupt derivative database' * 20)


@pytest.mark.parametrize('operation', ['task_create', 'task_terminal', 'workflow_create', 'workflow_update'])
def test_corrupt_derivative_does_not_prevent_authoritative_metadata_writes(tmp_path, operation):
    if operation.startswith('task'):
        directory = tmp_path / 'tasks'
        store.write(directory, record('historical'))
        historical = (directory / 'historical.meta.json').read_bytes()
        if operation == 'task_terminal':
            store.write(directory, record('target', status='running'))
        corrupt_derivative(directory)
        assert store.write_landed(directory, record('target', title='persisted terminal'))
        payload = json.loads((directory / 'target.meta.json').read_text())
        assert payload['status'] == 'completed' and payload['title'] == 'persisted terminal'
        assert (directory / 'historical.meta.json').read_bytes() == historical
    else:
        storage = workflows.WorkflowStore(tmp_path)
        historical_run = storage.create_run(definition(), 'Historical', tmp_path)
        historical_path = storage.runs / f"{historical_run['workflow_run_id']}.json"
        historical = historical_path.read_bytes()
        if operation == 'workflow_update':
            target = storage.create_run(definition(), 'Target', tmp_path)
        corrupt_derivative(storage.runs)
        if operation == 'workflow_create':
            target = storage.create_run(definition(), 'Created after corruption', tmp_path)
        else:
            target = storage.update_run(target['workflow_run_id'], lambda run: run.update(status='completed'), 'finished')
        payload = json.loads((storage.runs / f"{target['workflow_run_id']}.json").read_text())
        assert payload['status'] == ('starting' if operation == 'workflow_create' else 'completed')
        assert historical_path.read_bytes() == historical


def test_cache_recovery_commits_invalidation_before_interrupted_projection(tmp_path, monkeypatch):
    import sqlite3
    store.write(tmp_path, record('interrupted', title='original'))
    corrupt_derivative(tmp_path)
    original = catalog.Catalog._put
    monkeypatch.setattr(catalog.Catalog, '_put', lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.OperationalError('interrupted projection')))
    assert store.write_landed(tmp_path, record('interrupted', title='updated'))
    index = catalog.Catalog(tmp_path, store.RECORD_SUFFIX)
    assert not index.ready()
    with index.connect() as db:
        assert db.execute('SELECT COUNT(*) FROM entries').fetchone()[0] == 0
        assert db.execute('SELECT COUNT(*) FROM callers').fetchone()[0] == 0
        assert db.execute('SELECT id FROM pending').fetchall() == [('interrupted',)]
    monkeypatch.setattr(catalog.Catalog, '_put', original)
    page = store.list_page(tmp_path)
    assert page['catalog_state']['status'] == 'ready'
    assert page['items'][0]['title'] == 'updated'


def test_invalidation_transaction_error_rebuilds_cache_before_metadata_replace(tmp_path):
    store.write(tmp_path, record('target', status='running'))
    index = catalog.Catalog(tmp_path, store.RECORD_SUFFIX)
    with index.connect() as db:
        db.execute("CREATE TRIGGER broken_invalidation BEFORE INSERT ON pending BEGIN SELECT RAISE(ABORT,'broken derivative invalidation'); END")
    assert store.write_landed(tmp_path, record('target', title='terminal saved'))
    assert json.loads((tmp_path / 'target.meta.json').read_text())['title'] == 'terminal saved'
    with index.connect() as db:
        assert db.execute("SELECT name FROM sqlite_master WHERE name='broken_invalidation'").fetchone() is None


def test_derivative_recovery_only_removes_current_cache_and_sidecars(tmp_path):
    store.write(tmp_path, record('historical'))
    historical = (tmp_path / 'historical.meta.json').read_bytes()
    preserved = {}
    for name in ('.listing.sqlite3', '.listing.v7.sqlite3', '.listing.v7.sqlite3-journal', 'unrelated.tmp'):
        path = tmp_path / name
        path.write_bytes(b'preserve this unrelated source')
        preserved[path] = path.read_bytes()
    corrupt_derivative(tmp_path)
    corrupt_sidecar = b'corrupt derivative sidecar' * 20
    for suffix in ('-journal', '-wal', '-shm'):
        (tmp_path / f'.listing.v8.sqlite3{suffix}').write_bytes(corrupt_sidecar)
    assert store.write_landed(tmp_path, record('target'))
    assert historical == (tmp_path / 'historical.meta.json').read_bytes()
    assert all(path.read_bytes() == original for path, original in preserved.items())
    for suffix in ('-journal', '-wal', '-shm'):
        path = tmp_path / f'.listing.v8.sqlite3{suffix}'
        assert not path.exists() or path.read_bytes() != corrupt_sidecar


@pytest.mark.parametrize('failure', ['remove', 'initialize', 'invalidate'])
def test_failed_derivative_recovery_preserves_authoritative_metadata(tmp_path, monkeypatch, failure):
    import sqlite3
    store.write(tmp_path, record('target', status='running', title='original'))
    path = tmp_path / 'target.meta.json'
    original = path.read_bytes()
    attempts = []
    if failure == 'invalidate':
        def failed_invalidation(self, identifier):
            attempts.append(identifier)
            raise sqlite3.OperationalError('persistent invalidation failure')
        monkeypatch.setattr(catalog.Catalog, '_invalidate_locked', failed_invalidation)
    else:
        corrupt_derivative(tmp_path)
        if failure == 'remove':
            def failed_remove(self):
                attempts.append('remove')
                raise PermissionError('cannot remove unusable derivative')
            monkeypatch.setattr(catalog.Catalog, '_discard_derivative_locked', failed_remove)
        else:
            def failed_initialize(self, db):
                attempts.append('initialize')
                raise sqlite3.OperationalError('persistent cache initialization failure')
            monkeypatch.setattr(catalog.Catalog, '_initialize', failed_initialize)
    assert not store.write_landed(tmp_path, record('target', title='must not publish'))
    assert path.read_bytes() == original
    assert 1 <= len(attempts) <= 4


def test_corrupt_cache_does_not_bypass_existing_terminal_guard(tmp_path):
    store.write(tmp_path, record('terminal', title='final'))
    path = tmp_path / 'terminal.meta.json'
    original = path.read_bytes()
    corrupt_derivative(tmp_path)
    assert not store.write_landed(tmp_path, record('terminal', status='running', title='stale'))
    assert path.read_bytes() == original


def test_catalog_reader_waits_for_locked_cache_recovery(tmp_path, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    store.write(tmp_path, record('target', status='running'))
    corrupt_derivative(tmp_path)
    original = catalog.Catalog._discard_derivative_locked
    entered, release = threading.Event(), threading.Event()
    def held_recovery(self):
        entered.set()
        assert release.wait(5)
        return original(self)
    monkeypatch.setattr(catalog.Catalog, '_discard_derivative_locked', held_recovery)
    with ThreadPoolExecutor(max_workers=2) as pool:
        writer = pool.submit(store.write_landed, tmp_path, record('target', title='recovered terminal'))
        assert entered.wait(5)
        reader = pool.submit(store.list_page, tmp_path)
        assert not reader.done()
        release.set()
        assert writer.result(timeout=5)
        page = reader.result(timeout=5)
    assert page['catalog_state']['status'] == 'ready'
    assert page['items'][0]['title'] == 'recovered terminal'
