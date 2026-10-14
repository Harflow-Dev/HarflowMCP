import json
import time
from dataclasses import replace

import pytest

from polybridge import store, task_reads


def record(identifier, **kw):
    return store.TaskRecord(task_id=identifier, backend='codex', session_id=identifier,
        repo_path='/tmp/repo', started_at='2026-10-14T00:00:00+00:00',
        status='completed', exit_code=0, **kw)


def test_brief_bounds_and_does_not_mutate_prose():
    payload = {'task_id': 'one', 'status': 'unknown', 'needs_reconciliation': True,
        'summary': '界' * 100000, 'outcome': 'a ' * 100000,
        'recent_activity': [str(i) * 2000 for i in range(10)]}
    before = json.dumps(payload)
    brief = task_reads.brief_status(payload)
    assert len(json.dumps(brief).encode()) <= task_reads.BRIEF_BYTES
    assert len(brief['recent_activity']) == 3
    assert brief['status'] == 'unknown' and brief['needs_reconciliation']
    assert 'summary' in brief['truncated_fields']
    assert brief['detail_refs']['events']['tool'] == 'get_task_events'
    assert json.dumps(payload) == before


@pytest.mark.parametrize('kw', [dict(limit=0), dict(limit=101), dict(fields=['prompt']),
    dict(since='yesterday'), dict(since='2026-01-01'), dict(status='success'),
    dict(group='x' * 201), dict(backend='missing')])
def test_invalid_requests(tmp_path, kw):
    with pytest.raises(ValueError):
        task_reads.list_tasks(tmp_path, **kw)


def test_filtered_paging_and_scope(tmp_path, monkeypatch):
    for i in range(12):
        store.write(tmp_path, record(f'task-{i:02}', title='Fix Widget', group='target' if i % 2 else 'other'))
    def no_read(*args, **kwargs):
        raise AssertionError('settled indexed history must not load metadata or logs')
    monkeypatch.setattr(store, 'read', no_read)
    page = task_reads.list_tasks(tmp_path, group='target', title_contains='widget', repo='/tmp/repo',
        status='completed', since='2026-10-14T00:00:00Z', limit=2, fields=['title'])
    assert [i['task_id'] for i in page['items']] == ['task-11', 'task-09']
    assert set(page['items'][0]) <= task_reads.REQUIRED_FIELDS | {'title'}
    second = task_reads.list_tasks(tmp_path, group='target', title_contains='widget', repo='/tmp/repo',
        status='completed', since='2026-10-14T00:00:00Z', limit=2, cursor=page['next_cursor'])
    assert [i['task_id'] for i in second['items']] == ['task-07', 'task-05']
    with pytest.raises(ValueError, match='cursor'):
        task_reads.list_tasks(tmp_path, cursor=page['next_cursor'], group='other')


def test_reproducible_large_history(tmp_path, monkeypatch, capsys):
    # 200 settled tasks, 20 MiB of raw history. Warm index must touch none of it.
    for i in range(200):
        store.write(tmp_path, record(f'task-{i:03}', title='history', group='wanted' if i % 10 == 0 else 'other'))
        store.log_path(tmp_path, f'task-{i:03}').write_bytes(b'x' * 100_000)
    monkeypatch.setattr(store, 'read', lambda *a, **k: pytest.fail('metadata materialized'))
    start = time.perf_counter()
    result = task_reads.list_tasks(tmp_path, group='wanted', limit=5)
    elapsed = time.perf_counter() - start
    encoded = len(json.dumps(result).encode())
    assert len(result['items']) == 5 and encoded < 16_000
    print(f'200 tasks; 20 MB logs; page=5; response={encoded} bytes; elapsed={elapsed:.6f}s; metadata reads=0; log reads=0')


def test_maximum_unicode_filters_round_trip_cursor(tmp_path):
    from polybridge.catalog import Catalog
    value = 'é' * 100  # Each of the three filters is exactly 200 UTF-8 bytes.
    catalog = Catalog(tmp_path, store.RECORD_SUFFIX)
    for i in range(3):
        identifier = f'unicode-{i}'
        store.record_path(tmp_path, identifier).write_text('{}')
        catalog.record({'task_id': identifier, 'status': 'completed',
                        'group': value, 'title': value, 'repo_path': value}, i, False, identifier)
    def no_load(*args, **kwargs):
        pytest.fail('indexed header unexpectedly reloaded')
    filters = dict(group=value, title_contains=value, repo=value)
    page = catalog.page(no_load, limit=1, filters=filters)
    assert [item['task_id'] for item in page['items']] == ['unicode-2']
    assert len(page['next_cursor']) < 2048
    second = catalog.page(no_load, limit=1, cursor=page['next_cursor'],
                          filters=dict(reversed(list(filters.items()))))
    assert [item['task_id'] for item in second['items']] == ['unicode-1']
    with pytest.raises(ValueError, match='cursor'):
        catalog.page(no_load, limit=1, cursor=page['next_cursor'],
                     filters={**filters, 'group': 'different'})


def test_status_filter_retries_after_unloaded_process_dies(tmp_path, monkeypatch):
    task = replace(record('dies'), status='running', exit_code=None, pid=1234, markers=['codex'])
    store.write(tmp_path, task)
    alive = True
    monkeypatch.setattr(store.identity, 'identity_check', lambda _: 'alive' if alive else 'dead')
    assert task_reads.list_tasks(tmp_path, status='running')['items'][0]['task_id'] == 'dies'
    original = store.record_path(tmp_path, 'dies').read_bytes()
    monkeypatch.setattr(store, 'read', lambda *a, **k: pytest.fail('metadata read'))
    monkeypatch.setattr(store, 'replay_log', lambda *a, **k: pytest.fail('log replay'))
    alive = False
    first = task_reads.list_tasks(tmp_path, status='unknown')
    assert first['items'] == [] and not first['has_more']
    assert first['status_reconciliation_pending'] and first['history_incomplete']
    assert not first['counts_complete']
    assert 'same filters without a cursor' in first['polling_guidance']
    second = task_reads.list_tasks(tmp_path, status='unknown')
    assert second['items'][0]['task_id'] == 'dies'
    assert not second['status_reconciliation_pending'] and not second['history_incomplete']
    assert store.record_path(tmp_path, 'dies').read_bytes() == original


def test_status_filter_reports_unchecked_active_inventory(tmp_path, monkeypatch):
    for number in range(205):
        store.write(tmp_path, replace(record(f'live-{number:03}'), status='running',
            exit_code=None, pid=1000 + number, markers=['codex']))
    while store.bootstrap_catalog(tmp_path):
        pass
    checks = []
    monkeypatch.setattr(store.identity, 'identity_check', lambda value: checks.append(value['pid']) or 'alive')
    monkeypatch.setattr(store, 'read', lambda *a, **k: pytest.fail('metadata read'))
    monkeypatch.setattr(store, 'replay_log', lambda *a, **k: pytest.fail('log replay'))
    for _ in range(3):
        checks.clear()
        page = task_reads.list_tasks(tmp_path, status='unknown')
        assert 0 < len(checks) <= 100
        assert page['items'] == [] and not page['has_more']
        assert page['status_reconciliation_pending'] and page['history_incomplete']
        assert not page['counts_complete']

async def test_mcp_compact_projection_and_page(tmp_path):
    from polybridge import server
    from polybridge.tasks import Task
    from datetime import datetime, timezone
    task = Task(task_id='compact', backend='codex', session_id=None, repo_path=tmp_path,
        prompt='exact caller prose', max_turns=None, log_path=tmp_path / 'compact.jsonl',
        started_at=datetime.now(timezone.utc))
    task.acc.summary = '界' * 10000
    server._reg()._tasks[task.task_id] = task
    brief = await server.get_task_status(task.task_id, brief=True, include_tail=True)
    assert len(json.dumps(brief).encode()) <= task_reads.BRIEF_BYTES
    assert 'last_output_tail' not in brief
    assert task.acc.summary == '界' * 10000
    store.write(server._reg().log_dir, record('history', group='review'))
    page = await server.list_tasks(group='review', limit=1, fields=['title'])
    assert page['items'][0]['task_id'] == 'history'
