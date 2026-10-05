"""Cursor pages bound actual reads/decodes and preserve an append-only snapshot."""
import json

import pytest

from polybridge import event_history as history
from polybridge.events import PAGE_BUDGET_BYTES


def write(path, count=250, text="hello"):
    path.write_bytes(b"".join((json.dumps({"seq": i, "kind": "assistant_text", "text": text}) + "\n").encode()
                            for i in range(count)))


def all_pages(path, **kwargs):
    pages = []
    cursor = None
    for _ in range(100):
        page = history.read_cursor_page(path, cursor=cursor, **kwargs)
        pages.append(page)
        assert page.bytes_read <= history.SCAN_BYTES + 256
        assert page.decoded_records <= kwargs.get("limit", 100)
        if not page.has_more:
            return pages
        assert page.next_cursor and page.next_cursor != cursor
        cursor = page.next_cursor
    raise AssertionError("Cursor did not reach end")


def test_three_pages_live_insert_and_deduplication(tmp_path):
    path = tmp_path / 'events'
    write(path)
    first = history.read_cursor_page(path)
    assert [e['seq'] for e in first.events] == list(range(150, 250))
    with path.open('ab') as handle:
        handle.write(b'{"seq":250,"kind":"assistant_text","text":"new"}\n')
    second = history.read_cursor_page(path, cursor=first.next_cursor)
    third = history.read_cursor_page(path, cursor=second.next_cursor)
    assert [e['seq'] for e in second.events] == list(range(50, 150))
    assert [e['seq'] for e in third.events] == list(range(50))
    assert not third.has_more
    assert second.snapshot_end == third.snapshot_end == first.snapshot_end
    assert history.read_cursor_page(path).events[-1]['seq'] == 250


@pytest.mark.parametrize('mutation', ['replace', 'truncate', 'regrow', 'other_path'])
def test_stale_generation_requires_explicit_reload(tmp_path, mutation):
    path = tmp_path / 'events'
    write(path)
    page = history.read_cursor_page(path)
    if mutation == 'replace':
        replacement = tmp_path / 'replacement'; write(replacement); replacement.replace(path)
    elif mutation == 'truncate':
        write(path, 2)
    elif mutation == 'regrow':
        write(path, 1000, 'changed')
    else:
        path = tmp_path / 'other'; write(path)
    with pytest.raises(history.StaleEventCursor):
        history.read_cursor_page(path, cursor=page.next_cursor)


def test_partial_final_line_and_page_boundary_tool_pair(tmp_path):
    path = tmp_path / 'events'
    write(path, 101)
    with path.open('ab') as handle:
        handle.write(b'{"seq":101,"kind":"tool_call","call_id":"pair"}\n')
        offset = handle.tell()
        handle.write(b'{"seq":102,"kind":"tool_result","call_id":"pair"')
    first = history.read_cursor_page(path, limit=1)
    assert first.events[0]['kind'] == 'tool_call' and first.live_offset == offset
    second = history.read_cursor_page(path, cursor=first.next_cursor, limit=1)
    assert second.live_offset == offset and second.events[0]['seq'] == 100
    with path.open('ab') as handle:
        handle.write(b'}\n')
    newest = history.read_cursor_page(path, limit=1)
    assert newest.events[0]['kind'] == 'tool_result' and newest.events[0]['call_id'] == first.events[0]['call_id']


def test_large_fixture_bounded_reads_decodes_and_serialized_bytes(tmp_path, monkeypatch):
    path = tmp_path / 'events'
    write(path, 2000, '🧠' * 3000)
    parsed = 0
    original = history._parse_event_line
    opened = type(path).open
    actual_bytes = 0
    class TrackedFile:
        def __init__(self, handle): self.handle = handle
        def __getattr__(self, name): return getattr(self.handle, name)
        def __enter__(self): return self
        def __exit__(self, *args): self.handle.close()
        def read(self, *args):
            nonlocal actual_bytes
            data = self.handle.read(*args)
            actual_bytes += len(data)
            return data
    def open_file(target, *args, **kwargs):
        handle = opened(target, *args, **kwargs)
        return TrackedFile(handle) if target == path else handle
    def parse(raw):
        nonlocal parsed
        parsed += 1
        return original(raw)
    monkeypatch.setattr(history, '_parse_event_line', parse)
    monkeypatch.setattr(type(path), 'open', open_file)
    page = history.read_cursor_page(path)
    assert parsed <= 100
    assert page.bytes_read <= history.SCAN_BYTES + 256
    assert actual_bytes == page.bytes_read
    assert len(json.dumps(page.events, ensure_ascii=False).encode()) < PAGE_BUDGET_BYTES
    assert page.has_more and page.next_cursor


def test_oversized_invalid_and_filtered_windows_keep_progress(tmp_path):
    path = tmp_path / 'events'
    path.write_bytes(b'{"seq":0,"kind":"notice","text":"old"}\n' + b'x' * (history.SCAN_BYTES * 3) + b'\n'
                     + b'{"seq":1,"kind":{},"text":"invalid"}\n'
                     + b'{"seq":2,"kind":"assistant_text","text":"new"}\n')
    pages = all_pages(path, kinds=['notice'])
    assert [e['seq'] for p in reversed(pages) for e in p.events] == [0]
    assert any(not p.events and p.has_more for p in pages)


def test_byte_budget_does_not_drop_candidate_between_pages(tmp_path):
    path = tmp_path / 'events'; write(path, 120, '🧠' * 2000)
    pages = all_pages(path)
    assert [e['seq'] for p in reversed(pages) for e in p.events] == list(range(120))


@pytest.mark.parametrize('cursor', ['', 'not-base64', 'x' * 2049])
def test_malformed_cursor_rejected(tmp_path, cursor):
    with pytest.raises(ValueError, match='cursor'):
        history.read_cursor_page(tmp_path / 'events', cursor=cursor)


def test_filter_change_requires_deliberate_reset(tmp_path):
    path = tmp_path / 'events'; write(path)
    page = history.read_cursor_page(path)
    with pytest.raises(ValueError, match='filter changed'):
        history.read_cursor_page(path, cursor=page.next_cursor, kinds=['notice'])


def test_unreadable_missing_and_hostile_envelope(tmp_path):
    path = tmp_path / 'events'
    assert not history.read_cursor_page(path).has_more
    path.write_text(json.dumps({'seq': 0, 'kind': 'notice', 'observed_at': 'x' * 500000}) + '\n')
    page = history.read_cursor_page(path)
    assert page.events == [{'seq': 0, 'kind': 'notice', 'observed_at': None}]


async def test_server_page_indexing_discloses_no_records(monkeypatch):
    from polybridge import server, workflow_inspection
    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda _: (False, None))
    monkeypatch.setattr(history, "read_cursor_page", lambda *a, **k: pytest.fail("read while indexing"))
    result = await server.get_task_event_page("task-1")
    assert result["indexing"] is True and result["events"] == []


async def test_server_page_preserves_managed_read_authority(monkeypatch):
    from polybridge import server, workflow_inspection
    from mcp import MCPError
    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda _: (True, ({}, {})))
    def deny(*args):
        raise ValueError("outside assigned execution")
    monkeypatch.setattr(workflow_inspection, "guard_task_read", deny)
    monkeypatch.setattr(history, "read_cursor_page", lambda *a, **k: pytest.fail("unauthorized read"))
    with pytest.raises(MCPError, match="outside assigned execution"):
        await server.get_task_event_page("task-1")


async def test_server_page_reads_exact_task_without_legacy_scan(tmp_path, monkeypatch):
    from polybridge import server, workflow_inspection, store
    from polybridge.events import events_path
    monkeypatch.setattr(workflow_inspection, 'managed_page_reader', lambda _: (True, None))
    monkeypatch.setattr(store, 'read', lambda *a, **k: object())
    monkeypatch.setattr(server, '_guard_task_read', lambda *a: pytest.fail('legacy authority scan'))
    monkeypatch.setattr(server._reg(), '_log_dir', tmp_path)
    write(events_path(tmp_path, 'task-1'), 250)
    first = await server.get_task_event_page('task-1')
    second = await server.get_task_event_page('task-1', cursor=first['next_cursor'])
    assert len(first['events']) == len(second['events']) == 100
    assert first['events'][-1]['seq'] == 249 and second['events'][-1]['seq'] == 149
    assert first['decoded_records'] == 100 and first['indexing'] is False


def test_same_size_rewrite_during_read_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / 'events'; write(path)
    original_open = type(path).open
    class MutatingFile:
        def __init__(self, handle): self.handle = handle; self.mutated = False
        def __enter__(self): return self
        def __exit__(self, *args): self.handle.close()
        def __getattr__(self, name): return getattr(self.handle, name)
        def read(self, count=-1):
            data = self.handle.read(count)
            if count > 64 and not self.mutated:
                self.mutated = True
                with original_open(path, 'r+b') as writer:
                    writer.seek(0); writer.write(b'X')
            return data
    monkeypatch.setattr(type(path), 'open', lambda current, *a, **k: MutatingFile(original_open(current, *a, **k)))
    with pytest.raises(history.StaleEventCursor, match='during read'):
        history.read_cursor_page(path)


def test_blank_records_obey_physical_attempt_budget(tmp_path):
    path = tmp_path / 'events'; path.write_bytes(b'\n' * 1000000)
    page = history.read_cursor_page(path)
    assert page.events == [] and page.decoded_records == 100
    assert page.has_more and page.next_cursor
    assert history._decode(page.next_cursor)['end'] == 999900


async def test_server_event_page_explains_authority_incomplete_without_ids(monkeypatch):
    from polybridge import server, workflow_inspection
    monkeypatch.setattr(workflow_inspection, 'managed_page_reader', lambda _: (False, None))
    monkeypatch.setattr(workflow_inspection, 'page_indexing_response', lambda _: {
        'bootstrap_pending': False, 'history_incomplete': True, 'authority_incomplete': True,
        'counts_complete': False, 'note': 'Inspect known tasks with direct status.'})
    result = await server.get_task_event_page('task-1')
    assert result['events'] == [] and result['authority_incomplete']
    assert not result['indexing'] and not result['has_more']
    assert 'items' not in result and 'related_headers' not in result


async def test_server_event_page_caps_direct_metadata_read(tmp_path, monkeypatch):
    from polybridge import server, workflow_inspection, store
    from polybridge.catalog import METADATA_BYTES
    monkeypatch.setattr(workflow_inspection, 'managed_page_reader', lambda _: (True, None))
    monkeypatch.setattr(server._reg(), '_log_dir', tmp_path)
    seen = []
    def bounded(*args, **kwargs):
        seen.append(kwargs)
        return object()
    monkeypatch.setattr(store, 'read', bounded)
    await server.get_task_event_page('task-1')
    assert seen == [{'include_prompt': False, 'metadata_byte_limit': METADATA_BYTES}]
