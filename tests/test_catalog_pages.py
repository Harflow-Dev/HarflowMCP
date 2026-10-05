import base64
import json
import os
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
    assert managed_page_reader(directory)[0] is False
    assert managed_page_reader(directory)[0] is False
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
    assert page['history_incomplete'] and not page['counts_complete']


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
    assert first['history_incomplete']
    assert first['items'][0]['task_id'] == '1'
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
    assert page['history_incomplete']
    assert sum(bool(item.get('needs_direct_lookup')) for item in page['items']) == 1


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
    assert page['ownership_incomplete'] and page['history_incomplete'] and not page['counts_complete']


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
        with pytest.raises(Exception, match='indexing is incomplete'):
            await server._workflow_call('status', run_id='selected', _bounded_read=True)
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
        assert db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone() == ('4',)
        assert db.execute("SELECT count(*) FROM entries WHERE id LIKE 'old%'").fetchone() == (0,)
    assert originals == {p.name: p.read_bytes() for p in directory.glob('*.meta.json')}


def test_monitor_definition_list_uses_bounded_authority_but_public_first_read_is_unchanged(tmp_path, monkeypatch, capsys):
    from unittest.mock import AsyncMock
    human_authority(monkeypatch)
    directory = tmp_path / 'tasks'
    legacy_tasks(directory, 250)
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: directory)
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=directory))
    storage = workflows.WorkflowStore(root=tmp_path)
    monkeypatch.setattr(workflows, 'WorkflowStore', lambda **kwargs: storage)
    monkeypatch.setattr(storage, 'list', lambda: [{'name': 'saved'}])
    legacy = AsyncMock(return_value=None)
    monkeypatch.setattr(server, '_managed_workflow_reader', legacy)
    # Ordinary CLI still succeeds on the first call with a cold task catalog.
    assert ctl.main(['workflow-list', '--json']) == 0
    assert json.loads(capsys.readouterr().out)['result']['workflows'] == [{'name': 'saved'}]
    assert legacy.await_count == 1
    for _ in range(3):
        assert ctl.main(['workflow-list', '--monitor-view', '--json']) != 0
        assert 'indexing is incomplete' in capsys.readouterr().out
    for _ in range(4):
        assert ctl.main(['workflow-list', '--monitor-view', '--json']) == 0
        assert json.loads(capsys.readouterr().out)['result']['workflows'] == [{'name': 'saved'}]
    assert legacy.await_count == 1
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
