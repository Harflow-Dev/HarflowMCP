"""Monitor paging is a frozen transport snapshot, not a changing run read."""
import json
import os
import time

import pytest

from polybridge import workflow_responses


def test_snapshot_pages_remain_lossless_when_live_run_changes(tmp_path):
    run = {"workflow_run_id": "run", "status": "running", "summary": "x" * 400000, "activations": [{"id": "a", "status": "running"}]}
    first = workflow_responses.monitor_snapshot(run, "run", tmp_path)
    run["activations"][0]["status"] = "completed"
    text, cursor = first["chunk"], first["next_cursor"]
    while cursor:
        page = workflow_responses.monitor_snapshot(None, "run", tmp_path, cursor)
        assert len(json.dumps(page).encode()) < 280 * 1024
        assert page["content_sha256"] == first["content_sha256"]
        text += page["chunk"]
        cursor = page["next_cursor"]
    restored = json.loads(text)
    assert restored["summary"] == "x" * 400000
    assert restored["activations"][0]["status"] == "running"


def test_snapshot_cursor_cannot_read_other_runs_or_arbitrary_paths(tmp_path):
    first = workflow_responses.monitor_snapshot({"workflow_run_id": "run", "summary": "x" * 400000}, "run", tmp_path)
    with pytest.raises(ValueError):
        workflow_responses.monitor_snapshot(None, "other", tmp_path, first["next_cursor"])
    with pytest.raises(ValueError):
        workflow_responses.monitor_snapshot(None, "run", tmp_path, "../../etc/passwd")
    assert (next(tmp_path.iterdir()).stat().st_mode & 0o777) == 0o600


def test_snapshot_expiry_removes_cache_and_requires_refresh(tmp_path):
    first = workflow_responses.monitor_snapshot({"workflow_run_id": "run", "summary": "x" * 400000}, "run", tmp_path)
    path = next(tmp_path.iterdir())
    os.utime(path, (time.time() - 1000, time.time() - 1000))
    with pytest.raises(ValueError, match="expired"):
        workflow_responses.monitor_snapshot(None, "run", tmp_path, first["next_cursor"])
    assert not path.exists()


def test_forged_cursor_run_identity_cannot_relabel_snapshot(tmp_path):
    import base64
    page = workflow_responses.monitor_snapshot({"workflow_run_id": "run", "summary": "x" * 400000}, "run", tmp_path)
    cursor = json.loads(base64.urlsafe_b64decode(page["next_cursor"]))
    cursor["run"] = "other"
    forged = base64.urlsafe_b64encode(json.dumps(cursor).encode()).decode()
    with pytest.raises(ValueError):
        workflow_responses.monitor_snapshot(None, "other", tmp_path, forged)


def test_finished_paging_removes_snapshot_file(tmp_path):
    page = workflow_responses.monitor_snapshot({"workflow_run_id": "run", "summary": "x" * 400000}, "run", tmp_path)
    while page["next_cursor"]:
        page = workflow_responses.monitor_snapshot(None, "run", tmp_path, page["next_cursor"])
    assert not list(tmp_path.iterdir())


def test_cli_snapshot_pages_do_not_reload_changing_run(monkeypatch, tmp_path, capsys):
    from polybridge import ctl, server
    from unittest.mock import AsyncMock
    monkeypatch.setattr(ctl, "default_log_dir", lambda: tmp_path / "tasks")
    monkeypatch.setattr(server, "_verified_workflow_caller", AsyncMock(return_value=None))
    read = AsyncMock(return_value={"workflow_run_id": "run", "summary": "x" * 400000})
    monkeypatch.setattr(server, "_workflow_call", read)
    assert ctl.main(["workflow-status", "run", "--monitor-view", "--snapshot", "--json"]) == 0
    first = json.loads(capsys.readouterr().out)["result"]
    assert ctl.main(["workflow-status", "run", "--monitor-view", "--snapshot", "--cursor", first["next_cursor"], "--json"]) == 0
    assert read.await_count == 1


def test_managed_cli_snapshot_is_refused_before_any_content(monkeypatch, tmp_path, capsys):
    from polybridge import ctl, server
    from unittest.mock import AsyncMock
    monkeypatch.setattr(ctl, "default_log_dir", lambda: tmp_path / "tasks")
    monkeypatch.setattr(server, "_verified_workflow_caller", AsyncMock(return_value=object()))
    read = AsyncMock()
    monkeypatch.setattr(server, "_workflow_call", read)
    assert ctl.main(["workflow-status", "run", "--monitor-view", "--snapshot", "--json"]) != 0
    assert "only available to the local Monitor" in capsys.readouterr().out
    assert read.await_count == 0


@pytest.mark.parametrize('change', ['replace', 'truncate', 'rewrite_preserve_mtime', 'hashes', 'receipt'])
def test_snapshot_changed_cache_is_rejected(change, tmp_path):
    first = workflow_responses.monitor_snapshot({'workflow_run_id': 'run', 'summary': 'x' * 400000}, 'run', tmp_path)
    payload = next(tmp_path.glob('*.json'))
    original_stat = payload.stat()
    if change == 'replace':
        replacement = tmp_path / 'replacement'
        replacement.write_bytes(payload.read_bytes())
        replacement.replace(payload)
    elif change == 'truncate':
        with payload.open('r+b') as stream:
            stream.truncate(200000)
    elif change == 'rewrite_preserve_mtime':
        with payload.open('r+b') as stream:
            stream.seek(150000)
            stream.write(b'y')
        os.utime(payload, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    else:
        sidecar = payload.with_suffix('.sha' if change == 'hashes' else '.meta')
        with sidecar.open('r+b') as stream:
            stream.write(b'y')
    with pytest.raises(ValueError, match='refresh'):
        workflow_responses.monitor_snapshot(None, 'run', tmp_path, first['next_cursor'])


def test_fresh_snapshot_provider_reads_only_requested_chunk_and_bounded_receipt(tmp_path, monkeypatch):
    import importlib
    from pathlib import Path
    from polybridge import workflow_monitor_snapshot as provider
    first = provider.snapshot({'workflow_run_id': 'run', 'summary': 'x' * (provider.CHUNK_SIZE * 40)}, 'run', tmp_path)
    read_bytes = []
    original_open = Path.open
    class Counting:
        def __init__(self, stream):
            self.stream = stream
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return self.stream.__exit__(*args)
        def __getattr__(self, name):
            return getattr(self.stream, name)
        def read(self, size=-1):
            assert 0 <= size <= provider.CHUNK_SIZE
            value = self.stream.read(size)
            read_bytes.append(len(value))
            return value
    def open(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        return Counting(stream) if path.parent == tmp_path and args and args[0] == 'rb' else stream
    monkeypatch.setattr(Path, 'open', open)
    cursor, total, pages = first['next_cursor'], len(first['chunk']), 0
    while cursor:
        provider = importlib.reload(provider)  # No provider memory survives a CLI process.
        before = sum(read_bytes)
        page = provider.snapshot(None, 'run', tmp_path, cursor)
        assert sum(read_bytes) - before <= provider.CHUNK_SIZE + provider.RECEIPT_BYTES + 33
        total += len(page['chunk'])
        pages += 1
        cursor = page['next_cursor']
    assert total == first['total_characters']
    assert sum(read_bytes) <= total + pages * (provider.RECEIPT_BYTES + 33)


def test_monitor_detail_cli_freezes_only_view_and_never_reloads_on_continuations(monkeypatch, tmp_path, capsys):
    from polybridge import ctl, server
    from unittest.mock import AsyncMock
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: tmp_path / 'tasks')
    guard = AsyncMock(return_value=None)
    monkeypatch.setattr(server, '_verified_workflow_caller', guard)
    run = {'workflow_run_id': 'run', 'summary': 'x' * 400000, 'definition': {'name': 'untouched'}}
    read = AsyncMock(return_value=run)
    monkeypatch.setattr(server, '_workflow_call', read)
    args = ['workflow-detail', 'run', '--view', 'summary', '--monitor-view', '--json']
    assert ctl.main(args) == 0
    first = json.loads(capsys.readouterr().out)['result']
    assert first['view'] == 'summary' and len(first['chunk']) == 128 * 1024
    text, cursor, pages = first['chunk'], first['next_cursor'], 1
    run['summary'] = 'new live summary'
    while cursor:
        assert ctl.main([*args, '--cursor', cursor]) == 0
        page = json.loads(capsys.readouterr().out)['result']
        assert page['content_sha256'] == first['content_sha256']
        assert len(json.dumps(page).encode()) < 280 * 1024
        text += page['chunk']
        cursor = page['next_cursor']
        pages += 1
    assert json.loads(text) == 'x' * 400000
    assert read.await_count == 1 and guard.await_count == pages


def test_monitor_detail_cursor_cannot_change_view_or_bypass_managed_guard(monkeypatch, tmp_path, capsys):
    from polybridge import ctl, server
    from unittest.mock import AsyncMock
    page = workflow_responses.monitor_detail({'workflow_run_id': 'run', 'summary': 'x' * 400000}, 'run', 'summary', tmp_path)
    with pytest.raises(ValueError):
        workflow_responses.monitor_detail(None, 'run', 'definition', tmp_path, page['next_cursor'])
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: tmp_path / 'tasks')
    monkeypatch.setattr(server, '_verified_workflow_caller', AsyncMock(return_value=object()))
    read = AsyncMock()
    monkeypatch.setattr(server, '_workflow_call', read)
    assert ctl.main(['workflow-detail', 'run', '--view', 'summary', '--monitor-view', '--cursor', page['next_cursor'], '--json']) != 0
    assert 'only available to the local Monitor' in capsys.readouterr().out
    assert read.await_count == 0


def test_default_workflow_detail_cli_keeps_public_cursor_semantics(monkeypatch, capsys):
    from polybridge import ctl, server
    from unittest.mock import AsyncMock
    public = AsyncMock(return_value={'legacy': True})
    monkeypatch.setattr(server, 'get_workflow_run_detail', public)
    assert ctl.main(['workflow-detail', 'run', '--view', 'summary', '--cursor', 'public-cursor', '--json']) == 0
    assert json.loads(capsys.readouterr().out)['result'] == {'legacy': True}
    public.assert_awaited_once_with('run', 'summary', 'public-cursor')
