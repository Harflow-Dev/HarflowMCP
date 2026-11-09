"""Read-only marker qualification, deterministic races and ctl-only boundaries."""
import copy
from dataclasses import replace
import importlib.util
import json
import os
from pathlib import Path

import pytest

from harflow_mcp import state_access, state_ownership, store, task_continuation_stop as stop
from harflow_mcp.catalog import current_metadata_budget, metadata_request
from harflow_mcp.state_layout import resolve_layout

STAMP = '2026-10-15T00:00:00+00:00'


@pytest.fixture
def source(tmp_path, monkeypatch):
    directory = tmp_path / 'state' / 'tasks'
    record = store.TaskRecord('chain-source', 'claude', None, str(tmp_path), STAMP)
    store.write(directory, record)
    monkeypatch.setattr(stop, '_now', lambda: STAMP)
    return directory, record


def test_chain_identity_not_dispatch_ancestry(source):
    directory, record = source
    stop.marker_path(directory, record.task_id).write_text(json.dumps({'cancelled_task_id': 'successor'}))
    successor = replace(record, task_id='successor', chain_root_task_id=record.task_id,
                        root_task_id='unrelated', spawned_by='another', group='other-group')
    expected = dict(state='stopped', chain_root_task_id=record.task_id, reason='chain_cancelled',
                    cancelled_task_id='successor', observed_at=STAMP)
    assert stop.observe(directory, record) == stop.observe(directory, successor) == expected


@pytest.mark.parametrize('root', ['', '../escape', '/escape', 42])
def test_invalid_explicit_root_has_no_fallback(source, root, monkeypatch):
    directory, record = source
    record = replace(record, chain_root_task_id=root)
    monkeypatch.setattr(stop, '_inspect', lambda *a: pytest.fail('invalid root must not touch paths'))
    assert stop.observe(directory, record) == dict(state='unknown', chain_root_task_id=None,
        reason=None, cancelled_task_id=None, observed_at=STAMP)


def test_projected_identity_requires_valid_source_task(source, monkeypatch):
    directory, _ = source
    monkeypatch.setattr(stop, '_inspect', lambda *a: pytest.fail('invalid projected source must not inspect paths'))
    assert stop.observe_identity(directory, task_id=None, chain_root_task_id='valid-chain') == dict(
        state='unknown', chain_root_task_id=None, reason=None, cancelled_task_id=None, observed_at=STAMP)


@pytest.mark.parametrize('status', ['running', 'cancelled', 'completed'])
def test_absence_is_diagnostic_without_mutation(source, status):
    directory, record = source
    record = replace(record, status=status)
    before = {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}
    original = copy.deepcopy(record)
    assert stop.observe(directory, record)['state'] == 'not_stopped'
    assert record == original
    assert {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()} == before
    assert not list(directory.glob('*continuation*'))


@pytest.mark.parametrize('raw, cancelled', [
    (b'{"cancelled_task_id":"valid-task"}', 'valid-task'), (b'{}', None),
    (b'{"cancelled_task_id":null}', None), (b'{"cancelled_task_id":1}', None),
    (b'{"cancelled_task_id":"../escape"}', None), (b'[]', None),
    (b'', None), (b'{', None), (b'\xff', None),
    (b'[' * 1500 + b']' * 1500, None),
    (b'{"cancelled_task_id":"valid-task"}' + b' ' * (4096 - len(b'{"cancelled_task_id":"valid-task"}')), 'valid-task'),
    (b'x' * 4097, None),
])
def test_attribution_does_not_override_presence(source, raw, cancelled):
    directory, record = source
    stop.marker_path(directory, record.task_id).write_bytes(raw)
    observed = stop.observe(directory, record)
    assert observed['state'] == 'stopped'
    assert observed['cancelled_task_id'] == cancelled


@pytest.mark.parametrize('kind', ['symlink', 'dangling', 'directory', 'fifo', 'unreadable'])
def test_unsafe_or_unreadable_is_unknown(source, kind, monkeypatch):
    directory, record = source
    path = stop.marker_path(directory, record.task_id)
    if kind in ('symlink', 'dangling'):
        path.symlink_to(directory / ('chain-source.json' if kind == 'symlink' else 'missing'))
    elif kind == 'directory':
        path.mkdir()
    elif kind == 'fifo':
        os.mkfifo(path)
    else:
        path.write_text('{}')
        original = stop.open_descriptor
        def fail(selected, *args, **kwargs):
            if selected == path:
                raise PermissionError('controlled access refusal')
            return original(selected, *args, **kwargs)
        monkeypatch.setattr(stop, 'open_descriptor', fail)
    assert stop.observe(directory, record)['state'] == 'unknown'


@pytest.mark.parametrize('race', ['replace', 'modify', 'disappear', 'parent'])
def test_source_and_parent_races(source, race, monkeypatch):
    directory, record = source
    path = stop.marker_path(directory, record.task_id)
    path.write_text('{"cancelled_task_id":"valid"}')
    original = stop.os.read
    marker_inode = path.stat().st_ino
    fired = False
    def racing_read(fd, count):
        nonlocal fired
        raw = original(fd, count)
        if not fired and os.fstat(fd).st_ino == marker_inode:
            fired = True
            if race == 'replace':
                replacement = directory / 'replacement'
                replacement.write_text('{}')
                replacement.replace(path)
            elif race == 'modify':
                path.write_text('{"changed":true}')
            elif race == 'disappear':
                path.unlink()
            else:
                directory.rename(directory.with_name('old-tasks'))
                directory.mkdir()
        return raw
    monkeypatch.setattr(stop.os, 'read', racing_read)
    assert stop.observe(directory, record)['state'] == 'unknown'
    assert fired


@pytest.mark.parametrize('race', ['appearance', 'parent'])
def test_repeated_absence_check(source, race, monkeypatch):
    directory, record = source
    path = stop.marker_path(directory, record.task_id)
    original = Path.lstat
    calls = 0
    def racing_lstat(selected):
        nonlocal calls
        if selected == path:
            calls += 1
            if calls == 2:
                if race == 'appearance':
                    path.write_text('{}')
                else:
                    directory.rename(directory.with_name('old-tasks'))
                    directory.mkdir()
        return original(selected)
    monkeypatch.setattr(Path, 'lstat', racing_lstat)
    assert stop.observe(directory, record)['state'] == 'unknown'
    assert calls == 2


def test_bounded_reads_budget_and_descriptor_cleanup(source, monkeypatch):
    directory, record = source
    path = stop.marker_path(directory, record.task_id)
    raw = b'{"cancelled_task_id":"valid"}'
    path.write_bytes(raw + b' ' * (4096 - len(raw)))
    original = stop.os.read
    marker_inode = path.stat().st_ino
    requests = []
    def short_read(fd, count):
        if os.fstat(fd).st_ino != marker_inode:
            return original(fd, count)
        requests.append(count)
        return original(fd, min(count, 100))
    monkeypatch.setattr(stop.os, 'read', short_read)
    descriptors = set(state_access._descriptors)
    with metadata_request():
        budget = current_metadata_budget()
        before = budget.metadata_bytes
        assert stop.observe(directory, record)['cancelled_task_id'] == 'valid'
        assert budget.metadata_bytes - before == 4096
    assert requests[0] == 4096 and max(requests) <= 4096
    assert set(state_access._descriptors) == descriptors
    requests.clear()
    path.write_bytes(b'x' * 4097)
    assert stop.observe(directory, record)['state'] == 'stopped'
    assert requests == []
    with metadata_request():
        budget = current_metadata_budget()
        budget.metadata_limit = 0
        assert stop.observe(directory, record)['cancelled_task_id'] is None
        assert budget.metadata_bytes == 0


@pytest.mark.parametrize('race', ['replace', 'disappear', 'parent'])
def test_identity_change_before_open_is_unknown_and_closes_descriptors(source, race, monkeypatch):
    directory, record = source
    path = stop.marker_path(directory, record.task_id)
    path.write_text('{}')
    original = stop.open_descriptor
    descriptors = set(state_access._descriptors)
    def racing_open(selected, *args, **kwargs):
        if selected == path:
            if race == 'replace':
                replacement = directory / 'replacement'
                replacement.write_text('{}')
                replacement.replace(path)
            elif race == 'disappear':
                path.unlink()
            else:
                directory.rename(directory.with_name('old-tasks'))
                directory.mkdir()
                (directory / path.name).write_text('{}')
        return original(selected, *args, **kwargs)
    monkeypatch.setattr(stop, 'open_descriptor', racing_open)
    assert stop.observe(directory, record)['state'] == 'unknown'
    assert set(state_access._descriptors) == descriptors


def test_present_observation_is_read_only_and_creates_no_lock(source, monkeypatch):
    from harflow_mcp import control, followups
    directory, record = source
    path = stop.marker_path(directory, record.task_id)
    path.write_text('{"cancelled_task_id":"valid"}')
    before = {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}
    def forbidden(*a, **kw):
        pytest.fail('diagnostic must not lock, reserve or consume')
    monkeypatch.setattr(control, 'session_lock', forbidden)
    monkeypatch.setattr(followups, 'begin', forbidden)
    original = stop.open_descriptor
    def readonly(selected, flags, *a, **kw):
        assert flags & (os.O_CREAT | os.O_WRONLY | os.O_RDWR | os.O_TRUNC) == 0
        assert flags & os.O_NONBLOCK and flags & os.O_NOFOLLOW
        return original(selected, flags, *a, **kw)
    monkeypatch.setattr(stop, 'open_descriptor', readonly)
    assert stop.observe(directory, record)['state'] == 'stopped'
    assert {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()} == before


def test_ordinary_failure_isolated_but_ownership_refusal_preserved(source, monkeypatch):
    directory, record = source
    def fail(*args):
        raise RuntimeError('optional reader failed')
    monkeypatch.setattr(stop, '_inspect', fail)
    assert stop.observe(directory, record)['state'] == 'unknown'
    def refuse(*args):
        raise state_ownership.StateOwnershipError('lost ownership')
    monkeypatch.setattr(stop, '_inspect', refuse)
    with pytest.raises(state_ownership.StateOwnershipError, match='lost ownership'):
        stop.observe(directory, record)


def test_generator_reproduces_all_owned_fixtures():
    root = Path(__file__).parents[1]
    spec = importlib.util.spec_from_file_location('continuation_stop_fixtures', root / 'scripts/generate-continuation-stop-fixtures.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    generated = module.generate()
    assert generated == module.generate()
    assert all(path.read_bytes() == content for path, content in generated.items())


def test_mcp_status_and_store_snapshot_do_not_read_diagnostic(source, monkeypatch):
    from harflow_mcp import server
    directory, record = source
    stop.marker_path(directory, record.task_id).write_text('{}')
    monkeypatch.setattr(stop, 'observe', lambda *a: pytest.fail('MCP must not observe chain stop'))
    monkeypatch.setattr(stop, 'observe_identity', lambda *a, **kw: pytest.fail('MCP must not observe projected chain stop'))
    with state_ownership.state_session(resolve_layout(log_dir=directory)):
        snapshot = store.snapshot(directory, record)
        payload = server._status_payload(snapshot, include_tail=False, log_dir=directory, task_id=record.task_id)
    assert 'continuation_stop' not in snapshot
    assert 'continuation_stop' not in payload
