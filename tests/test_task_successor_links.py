"""Reverse resume/continuation links use bounded, authorized persisted headers."""
import pytest

from harflow_mcp import server, state_ownership, store, task_reads
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry


@pytest.fixture
def task_directory(tmp_path, monkeypatch):
    with state_ownership.state_session(resolve_layout(root=tmp_path / 'state')):
        directory = tmp_path / 'state' / 'tasks'
        directory.mkdir()
        monkeypatch.setattr(server, '_registry', TaskRegistry(log_dir=directory))
        yield directory


def task(identifier, **kwargs):
    return store.TaskRecord(task_id=identifier, backend='claude', session_id='conversation',
        repo_path='/tmp/repo', started_at='2026-10-26T00:00:00+00:00',
        status='completed', exit_code=0, **kwargs)


async def test_reverse_links_are_paged_and_scoped(task_directory):
    for value in [task('source'), task('resume', parent_task_id='source'),
                  task('continue', continuation_of_task_id='source'),
                  task('unrelated'), task('other', parent_task_id='unrelated')]:
        store.write(task_directory, value)
    first = await server.list_task_page(limit=1, predecessor_task_id='source')
    second = await server.list_task_page(limit=1, predecessor_task_id='source', cursor=first['next_cursor'])
    assert {row['task_id'] for page in (first, second) for row in page['items']} == {'resume', 'continue'}
    with pytest.raises(Exception, match='cursor'):
        await server.list_task_page(limit=1, predecessor_task_id='unrelated', cursor=first['next_cursor'])
    status = await server.get_task_status('source')
    assert set(status['successor_tasks']['task_ids']) == {'resume', 'continue'}
    assert status['successor_tasks']['history_incomplete'] is False
    assert (await server.get_task_status('resume'))['parent_task_id'] == 'source'
    brief = task_reads.brief_status(status)
    assert brief['successor_tasks'] == status['successor_tasks']


async def test_successor_page_filters_managed_visibility(task_directory, monkeypatch):
    from harflow_mcp import workflow_inspection
    store.write(task_directory, task('source'))
    store.write(task_directory, task('hidden', parent_task_id='source'))
    monkeypatch.setattr(workflow_inspection, 'filter_task_reads', lambda entries, managed: [])
    result = await server._with_successor_links({'task_id': 'source'})
    assert result['successor_tasks']['task_ids'] == []


async def test_managed_truncation_is_not_reported_as_complete(task_directory, monkeypatch):
    from harflow_mcp import workflow_inspection
    store.write(task_directory, task('source'))
    for index in range(21):
        store.write(task_directory, task(f'child-{index:02}', parent_task_id='source'))
    monkeypatch.setattr(server, '_guard_task_read', _no_guard)
    monkeypatch.setattr(workflow_inspection, 'managed_page_reader', lambda _: (True, ({}, {})))
    monkeypatch.setattr(workflow_inspection, 'filter_task_reads', lambda entries, managed: entries)
    result = await server._with_successor_links({'task_id': 'source'})
    assert len(result['successor_tasks']['task_ids']) == 20
    assert result['successor_tasks']['history_incomplete'] is True


async def _no_guard(*args, **kwargs):
    return None
