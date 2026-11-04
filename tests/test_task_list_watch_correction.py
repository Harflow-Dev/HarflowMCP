"""Observable regressions for watcher continuity and publication ownership.

All ordering uses explicit commands, barriers or injected clocks. Child processes never
invoke agents or touch state outside the test's qualified temporary root.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import threading
import time

import pytest

from harflow_mcp import catalog, state_ownership, store, task_list_watch
from harflow_mcp.state_layout import resolve_layout
from test_task_list_watch import record, run, state  # noqa: F401 (shared fixture)


def _watch(layout, cursor=None, wait=0):
    return task_list_watch.watch(layout, cursor=cursor, wait_seconds=wait)


def _catalog(layout):
    return catalog.Catalog(layout.tasks_directory, store.RECORD_SUFFIX, layout=layout)


def _project(layout, item, publication=None):
    header, stamp, active = store.listing_header(item)
    if publication is None:
        _catalog(layout).record(header, stamp, active, item.task_id)
    else:
        _catalog(layout).record(header, stamp, active, item.task_id, publication=publication)


def _publish_metadata(layout, item):
    """The durable publication seam, independent of watch bookkeeping."""
    path = store.record_path(layout.tasks_directory, item.task_id)
    payload = json.loads(path.read_text())
    payload['title'] = item.title
    temporary = path.with_suffix('.replacement')
    temporary.write_text(json.dumps(payload))
    temporary.replace(path)
    observed = store.read(layout.tasks_directory, item.task_id)
    assert observed.title == item.title
    return observed


_CHILD = r'''
import json,os,sys,types
from pathlib import Path
from harflow_mcp import catalog,state_ownership,store
from harflow_mcp.state_layout import resolve_layout
layout=resolve_layout(root=Path(sys.argv[1]))
with state_ownership.state_session(layout):
 c=catalog.Catalog(layout.tasks_directory,store.RECORD_SUFFIX,layout=layout)
 token=None
 print('ready',flush=True)
 for command in sys.stdin:
  command=command.strip()
  if command=='begin':
   token=c.invalidate('watch-task-1')
  elif command=='observe':
   item=store.read(layout.tasks_directory,'watch-task-1')
   h,s,a=store.listing_header(item); c.record(h,s,a,item.task_id)
  elif command=='publish':
   path=store.record_path(layout.tasks_directory,'watch-task-1')
   data=json.loads(path.read_text()); data['title']='published-by-child'
   temporary=path.with_suffix('.replacement'); temporary.write_text(json.dumps(data)); temporary.replace(path)
   item=store.read(layout.tasks_directory,'watch-task-1')
   h,s,a=store.listing_header(item); c.record(h,s,a,item.task_id,publication=token)
  elif command=='lock':
   import fcntl
   handle=open(layout.tasks_directory/'.listing.lock','a+')
   fcntl.flock(handle,fcntl.LOCK_EX)
  elif command=='exit':
   break
  elif command=='crash':
   os._exit(0)
  print('done',flush=True)
'''


def _line(process):
    ready, _, _ = select.select([process.stdout], [], [], 10)
    assert ready, 'child did not reach synchronization gate'
    line = process.stdout.readline().strip()
    assert line, process.stderr.read() if process.poll() is not None else 'child closed pipe'
    return line


@contextmanager
def _child(layout):
    process = subprocess.Popen(
        [sys.executable, '-u', '-c', _CHILD, str(layout.root)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        assert _line(process) == 'ready'
        yield process
    finally:
        if process.poll() is None:
            process.stdin.write('exit\n'); process.stdin.flush()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=10)
        assert process.returncode == 0, process.stderr.read()
        process.stdin.close(); process.stdout.close(); process.stderr.close()


def _command(process, command):
    process.stdin.write(command + '\n'); process.stdin.flush()
    assert _line(process) == 'done'


@pytest.mark.parametrize('observer_in_child', [False, True])
def test_observation_never_consumes_publication(state, observer_in_child):
    before = record()
    assert store.write_landed(state.tasks_directory, before)
    publication = _catalog(state).invalidate(before.task_id)
    if observer_in_child:
        with _child(state) as process:
            _command(process, 'observe')
    else:
        # This is the catalog projection performed by store.snapshot, without
        # depending on unrelated transcript and resource observations.
        _project(state, before)
    boundary = _watch(state)
    assert store.read(state.tasks_directory, before.task_id).title == 'one'
    after = _publish_metadata(state, replace(before, title='two'))
    _project(state, after, publication)
    result = _watch(state, boundary['cursor'])
    assert result['state'] == 'changed'
    assert _watch(state, result['cursor'])['state'] == 'timeout'


def test_same_pid_nested_publishers_retain_each_boundary(state):
    first, second = record(), record('watch-task-2')
    assert store.write_landed(state.tasks_directory, first)
    assert store.write_landed(state.tasks_directory, second)
    outer = _catalog(state).invalidate(first.task_id)
    inner = _catalog(state).invalidate(second.task_id)
    _project(state, second)  # Unpaired observation must close neither operation.
    boundary = _watch(state)
    inner_value = _publish_metadata(state, replace(second, title='inner'))
    _project(state, inner_value, inner)
    observed = _watch(state, boundary['cursor'])
    assert observed['state'] == 'changed'
    assert store.read(state.tasks_directory, first.task_id).title == 'one'
    outer_value = _publish_metadata(state, replace(first, title='outer'))
    _project(state, outer_value, outer)
    finished = _watch(state, observed['cursor'])
    assert finished['state'] == 'changed'
    assert _watch(state, finished['cursor'])['state'] == 'timeout'


@pytest.mark.parametrize('publish_before_crash', [False, True])
def test_dead_writer_recovery_retains_live_writer(state, publish_before_crash):
    item = record()
    assert store.write_landed(state.tasks_directory, item)
    live_token = _catalog(state).invalidate(item.task_id)
    boundary = _watch(state)
    with _child(state) as process:
        _command(process, 'begin')
        if publish_before_crash:
            # Simulate death after durable source replace but before its post-bump.
            item = _publish_metadata(state, replace(item, title='before-crash'))
        process.stdin.write('crash\n'); process.stdin.flush()
        process.wait(timeout=10)
    recovered = _watch(state, boundary['cursor'])
    assert recovered['state'] in {'changed', 'reset'}
    assert _watch(state, recovered['cursor'])['state'] == 'changed'
    final_item = _publish_metadata(state, replace(item, title='live-finished'))
    _project(state, final_item, live_token)
    finished = _watch(state, recovered['cursor'])
    assert finished['state'] == 'changed'
    assert store.read(state.tasks_directory, item.task_id).title == 'live-finished'
    assert _watch(state, finished['cursor'])['state'] == 'timeout'


def test_response_loss_resume_and_change_revert(state):
    item = record()
    assert store.write_landed(state.tasks_directory, item)
    boundary = _watch(state)
    assert store.write_landed(state.tasks_directory, replace(item, title='temporary'))
    lost_response = _watch(state, boundary['cursor'])
    assert lost_response['state'] == 'changed'
    assert store.write_landed(state.tasks_directory, item)
    assert store.read(state.tasks_directory, item.task_id).title == 'one'
    # Retry the retained cursor, not the response that never reached the caller.
    retry = _watch(state, boundary['cursor'])
    assert retry['state'] == 'changed'
    assert _watch(state, lost_response['cursor'])['state'] == 'changed'


@pytest.mark.parametrize('value', [[], None, 1, 'revision', True])
def test_revision_json_nonobjects_are_typed_unavailable(state, capsys, value):
    run([], capsys)
    task_list_watch.revision_path(state.tasks_directory).write_text(json.dumps(value))
    result = run([], capsys)
    assert result == {'state': 'unavailable', 'cursor': None, 'reason': 'observation_unavailable'}


@pytest.mark.parametrize('field,value', [
    ('v', True), ('revision', True), ('revision', -1), ('revision', 2**100),
    ('open', True), ('open', -1), ('open', 2**100), ('epoch', ''), ('epoch', 'x' * 2048),
    ('fence_pids', [True]), ('fence_pids', [-1]),
])
def test_revision_known_scalars_are_strict_and_bounded(state, capsys, field, value):
    run([], capsys)
    path = task_list_watch.revision_path(state.tasks_directory)
    payload = json.loads(path.read_text())
    payload[field] = value
    path.write_text(json.dumps(payload))
    result = run([], capsys)
    assert result['state'] == 'unavailable'
    assert result['reason'] == 'observation_unavailable'


@pytest.mark.parametrize('field,value', [('v', True), ('revision', True), ('revision', 2**100), ('epoch', '')])
def test_cursor_known_scalars_are_strict(state, capsys, field, value):
    boundary = run([], capsys)
    payload = json.loads(base64.urlsafe_b64decode(boundary['cursor']))
    payload[field] = value
    cursor = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    result = run(['--cursor', cursor, '--wait-seconds', '0'], capsys, expect=1)
    assert result['code'] == 'task_list_watch_cursor_invalid'


def test_future_cursor_cannot_establish_continuity(state, capsys):
    boundary = run([], capsys)
    payload = json.loads(base64.urlsafe_b64decode(boundary['cursor']))
    payload['revision'] += 1
    cursor = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    result = run(['--cursor', cursor, '--wait-seconds', '0'], capsys)
    assert result['state'] in {'reset', 'unavailable'}


def test_external_catalog_lock_is_bounded(state, capsys):
    boundary = run([], capsys)
    with _child(state) as process:
        _command(process, 'lock')
        start = time.monotonic()
        result = run(['--cursor', boundary['cursor'], '--wait-seconds', '0'], capsys)
        elapsed = time.monotonic() - start
        assert result['state'] == 'unavailable'
        assert elapsed < task_list_watch.SETUP_OVERHEAD_SECONDS + 1


@pytest.mark.parametrize('after_source', [False, True])
def test_aborted_publication_closes_its_own_fence(state, monkeypatch, after_source):
    item = record()
    assert store.write_landed(state.tasks_directory, item)
    boundary = _watch(state)
    if after_source:
        def fail_projection(*_args, **_kwargs):
            raise OSError('injected post-publication failure')
        monkeypatch.setattr(catalog.Catalog, 'record', fail_projection)
    else:
        real_replace = os.replace
        def fail_metadata(source, target):
            if Path(target).name == item.task_id + store.RECORD_SUFFIX:
                raise OSError('injected pre-publication failure')
            return real_replace(source, target)
        monkeypatch.setattr(store.os, 'replace', fail_metadata)
    assert not store.write_landed(state.tasks_directory, replace(item, title='attempted'))
    assert store.read(state.tasks_directory, item.task_id).title == ('attempted' if after_source else 'one')
    observed = _watch(state, boundary['cursor'])
    assert observed['state'] == 'changed'
    assert _watch(state, observed['cursor'])['state'] == 'timeout'


def test_in_process_catalog_lock_is_bounded(state, capsys):
    boundary = run([], capsys)
    acquired, release = threading.Event(), threading.Event()
    def hold():
        with catalog.catalog_lock(state.tasks_directory, layout=state):
            acquired.set()
            assert release.wait(timeout=10)
    holder = threading.Thread(target=hold)
    holder.start()
    try:
        assert acquired.wait(timeout=5)
        started = time.monotonic()
        result = run(['--cursor', boundary['cursor'], '--wait-seconds', '0'], capsys)
        assert result['state'] == 'unavailable'
        assert time.monotonic() - started < task_list_watch.SETUP_OVERHEAD_SECONDS + 1
    finally:
        release.set(); holder.join(timeout=5)
        assert not holder.is_alive()


def test_wait_cancellation_releases_resources(state, monkeypatch):
    from types import SimpleNamespace
    boundary = _watch(state)
    def cancel(_seconds):
        raise KeyboardInterrupt('injected cancellation at polling boundary')
    with monkeypatch.context() as patch:
        patch.setattr(task_list_watch, 'time', SimpleNamespace(monotonic=time.monotonic, sleep=cancel))
        with pytest.raises(KeyboardInterrupt):
            _watch(state, boundary['cursor'], wait=30)
    assert _watch(state, boundary['cursor'])['state'] == 'timeout'
    assert store.write_landed(state.tasks_directory, record())
    assert _watch(state, boundary['cursor'])['state'] == 'changed'


def test_busy_snapshot_retains_invalidation_and_timeout_reconciles(state):
    # Baseline precedes the first public snapshot. Refreshes retain the returned
    # boundary before reading, so a writer during a busy refresh stays pending.
    item = record()
    assert store.write_landed(state.tasks_directory, item)
    cursor = _watch(state)['cursor']
    assert store.read(state.tasks_directory, item.task_id).title == 'one'
    assert store.write_landed(state.tasks_directory, replace(item, title='two'))
    cursor = _watch(state, cursor)['cursor']
    snapshot = store.read(state.tasks_directory, item.task_id)
    assert snapshot.title == 'two'
    assert store.write_landed(state.tasks_directory, replace(item, title='three'))
    pending = _watch(state, cursor)
    assert pending['state'] == 'changed'
    cursor = pending['cursor']
    assert store.read(state.tasks_directory, item.task_id).title == 'three'
    timed = _watch(state, cursor)
    assert timed['state'] == 'timeout'
    # A timeout is followed by a lifecycle reconciliation read; it is not a
    # statement about execution or display stability.
    assert store.read(state.tasks_directory, item.task_id).title == 'three'


def test_root_recreation_resets_retained_cursor(tmp_path, monkeypatch, capsys):
    from harflow_mcp import takeover
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(takeover, 'caller_refusal', lambda *_: None)
    layout = resolve_layout(root=tmp_path / '.harflow')
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
    boundary = run([], capsys)
    layout.root.rename(tmp_path / 'retired-state')
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
    result = run(['--cursor', boundary['cursor'], '--wait-seconds', '0'], capsys)
    assert result['state'] == 'reset'
    assert result['cursor'] != boundary['cursor']


def test_ctl_root_lock_uses_the_same_setup_deadline(tmp_path, monkeypatch, capsys):
    from harflow_mcp import takeover
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(takeover, 'caller_refusal', lambda *_: None)
    layout = resolve_layout(root=tmp_path / '.harflow')
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
    boundary = run([], capsys)
    child = subprocess.Popen(
        [sys.executable, '-u', '-c',
         "import fcntl,sys; handle=open(sys.argv[1],'r+'); "
         "fcntl.flock(handle,fcntl.LOCK_EX); print('ready',flush=True); sys.stdin.read()",
         str(layout.root / state_ownership.LOCK_NAME)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        assert _line(child) == 'ready'
        started = time.monotonic()
        error = run(['--cursor', boundary['cursor'], '--wait-seconds', '0'], capsys, expect=1)
        assert error['code'] == 'state_busy'
        assert time.monotonic() - started < task_list_watch.SETUP_OVERHEAD_SECONDS + 1
    finally:
        child.stdin.close()
        child.wait(timeout=10)
        assert child.returncode == 0, child.stderr.read()
        child.stdout.close(); child.stderr.close()


@pytest.mark.parametrize('source_kind', ['oversize', 'symlink'])
def test_revision_resource_bounds_and_symlink_refusal(state, capsys, source_kind):
    run([], capsys)
    path = task_list_watch.revision_path(state.tasks_directory)
    if source_kind == 'oversize':
        path.write_bytes(b' ' * (task_list_watch.MAX_DOCUMENT_BYTES + 1))
    else:
        target = state.root / 'outside-revision'
        target.write_text('[]')
        path.unlink()
        path.symlink_to(target)
    result = run([], capsys)
    assert result['state'] == 'unavailable'
    assert result['reason'] == 'observation_unavailable'
    if source_kind == 'symlink':
        assert target.read_text() == '[]'


def test_child_publication_survives_parent_observation(state):
    item = record()
    assert store.write_landed(state.tasks_directory, item)
    with _child(state) as process:
        _command(process, 'begin')
        _project(state, item)
        boundary = _watch(state)
        assert store.read(state.tasks_directory, item.task_id).title == 'one'
        _command(process, 'publish')
        assert store.read(state.tasks_directory, item.task_id).title == 'published-by-child'
        finished = _watch(state, boundary['cursor'])
        assert finished['state'] == 'changed'
        assert _watch(state, finished['cursor'])['state'] == 'timeout'


def test_failed_invalidation_closes_its_publication(state, monkeypatch):
    import sqlite3
    item = record()
    assert store.write_landed(state.tasks_directory, item)
    boundary = _watch(state)
    def fail_invalidate(*_args, **_kwargs):
        raise sqlite3.OperationalError('injected invalidation failure')
    monkeypatch.setattr(catalog.Catalog, '_invalidate_locked', fail_invalidate)
    monkeypatch.setattr(catalog.Catalog, '_discard_derivative_locked', lambda *_: None)
    assert not store.write_landed(state.tasks_directory, replace(item, title='attempted'))
    assert store.read(state.tasks_directory, item.task_id).title == 'one'
    changed = _watch(state, boundary['cursor'])
    assert changed['state'] == 'changed'
    assert _watch(state, changed['cursor'])['state'] == 'timeout'


def test_swallowed_catalog_projection_failure_closes_publication(state, monkeypatch):
    item = record()
    assert store.write_landed(state.tasks_directory, item)
    boundary = _watch(state)
    def fail_projection(*_args, **_kwargs):
        raise OSError('injected derivative projection failure')
    monkeypatch.setattr(catalog.Catalog, '_put', fail_projection)
    # The authoritative source landed; a failed derivative projection does not
    # undo it or leave the publication open in this live process.
    assert store.write_landed(state.tasks_directory, replace(item, title='landed'))
    assert store.read(state.tasks_directory, item.task_id).title == 'landed'
    changed = _watch(state, boundary['cursor'])
    assert changed['state'] == 'changed'
    assert _watch(state, changed['cursor'])['state'] == 'timeout'


def test_open_publication_capacity_fails_closed_and_recovers(state):
    tokens = []
    try:
        for _ in range(task_list_watch.MAX_OPEN_PUBLICATIONS):
            tokens.append(task_list_watch.begin_publication(state.tasks_directory, layout=state))
        with pytest.raises(state_ownership.StateOwnershipError) as raised:
            task_list_watch.begin_publication(state.tasks_directory, layout=state)
        assert raised.value.reason == 'observation_unavailable'
        boundary = _watch(state)
        assert _watch(state, boundary['cursor'])['state'] == 'changed'
    finally:
        for token in tokens:
            task_list_watch.end_publication(state.tasks_directory, token, layout=state)
    closed = _watch(state, boundary['cursor'])
    assert closed['state'] == 'changed'
    assert _watch(state, closed['cursor'])['state'] == 'timeout'
    assert store.write_landed(state.tasks_directory, record())
    assert _watch(state, closed['cursor'])['state'] == 'changed'


def test_token_from_retired_root_cannot_close_new_root_publication(tmp_path):
    layout = resolve_layout(root=tmp_path / 'selected')
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
        old = task_list_watch.begin_publication(layout.tasks_directory, layout=layout)
    layout.root.rename(tmp_path / 'retired')
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
        current = task_list_watch.begin_publication(layout.tasks_directory, layout=layout)
        boundary = _watch(layout)
        task_list_watch.end_publication(layout.tasks_directory, old, layout=layout)
        assert _watch(layout, boundary['cursor'])['state'] == 'changed'
        task_list_watch.end_publication(layout.tasks_directory, current, layout=layout)
        closed = _watch(layout, boundary['cursor'])
        assert closed['state'] == 'changed'
        assert _watch(layout, closed['cursor'])['state'] == 'timeout'


def test_malformed_utf8_cursor_is_public_typed_refusal(state, capsys):
    error = run(['--cursor', '\udcff', '--wait-seconds', '0'], capsys, expect=1)
    assert error['code'] == 'task_list_watch_cursor_invalid'


@pytest.mark.parametrize('has_live_fence', [False, True])
def test_released_pid_only_revision_is_conservative(state, capsys, has_live_fence):
    baseline = run([], capsys)
    path = task_list_watch.revision_path(state.tasks_directory)
    payload = json.loads(path.read_text())
    payload.pop('publication_tokens', None)
    if has_live_fence:
        payload['open'] = 1
        payload['fence_pids'] = [os.getpid()]
    path.write_text(json.dumps(payload))
    result = run(['--cursor', baseline['cursor'], '--wait-seconds', '0'], capsys)
    if has_live_fence:
        assert result['state'] == 'unavailable'
        assert result['reason'] == 'observation_unavailable'
    else:
        assert result['state'] == 'timeout'
