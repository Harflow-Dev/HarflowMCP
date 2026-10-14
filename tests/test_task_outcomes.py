import subprocess
from pathlib import Path

from polybridge.task_outcomes import baseline, capture, observe
from polybridge import store


def git(path, *args):
    return subprocess.run(['git', '-C', str(path), *args], check=True, capture_output=True, text=True).stdout.strip()


def repository(tmp_path):
    git(tmp_path, 'init')
    git(tmp_path, 'config', 'user.email', 'test@example.com')
    git(tmp_path, 'config', 'user.name', 'Test')
    (tmp_path / 'file').write_text('first\n')
    git(tmp_path, 'add', '.')
    git(tmp_path, 'commit', '-m', 'baseline')
    return tmp_path


def test_dirty_baseline_and_committed_outcome(tmp_path):
    repo = repository(tmp_path)
    (repo / 'preexisting').write_text('dirty')
    start = baseline(repo)
    assert 'preexisting' in start['working_tree_status']
    (repo / 'file').write_text('second\n')
    git(repo, 'add', 'file')
    git(repo, 'commit', '-m', 'next')
    result = capture(repo, start, {})
    assert result['base_commit'] == start['head']
    assert result['head_commit'] != start['head']
    assert result['new_commits'] == [git(repo, 'rev-parse', 'HEAD')]
    assert 'file' in result['committed_diff_stat']
    assert 'preexisting' in result['terminal']['working_tree_status']
    assert result['pushed'] is None
    assert result['local_upstream'] is None


def test_repository_filters_never_execute(tmp_path):
    repo = repository(tmp_path)
    marker = tmp_path / 'executed'
    git(repo, 'config', 'filter.hostile.clean', f'touch {marker}; cat')
    git(repo, 'config', 'filter.hostile.process', f'touch {marker}')
    git(repo, 'config', 'filter.hostile.required', 'true')
    git(repo, 'config', 'core.fsmonitor', f'touch {marker}')
    (repo / '.gitattributes').write_text('file filter=hostile\n')
    (repo / 'file').write_text('changed\n')
    start = baseline(repo)
    assert start['working_tree_status'] is not None
    capture(repo, start, {})
    assert not marker.exists()


def test_nested_repository_directory_uses_real_worktree_root(tmp_path):
    repo = repository(tmp_path)
    nested = repo / 'sub'
    nested.mkdir()
    (nested / 'inner').write_text('tracked\n')
    git(repo, 'add', 'sub/inner')
    git(repo, 'commit', '-m', 'nested file')
    assert baseline(nested)['working_tree_status'] == ''
    (nested / 'inner').write_text('changed\n')
    status = baseline(nested)['working_tree_status']
    assert ' D ' not in status
    assert 'inner' in status


def test_missing_git_and_non_git_are_unavailable(tmp_path, monkeypatch):
    assert baseline(tmp_path)['head'] is None
    monkeypatch.setenv('PATH', str(tmp_path))
    start = baseline(tmp_path)
    assert start['error']
    assert capture(tmp_path, start, {})['new_commits'] is None


def test_structured_evidence_requires_correlated_result(tmp_path):
    evidence = {}
    observe(evidence, 'tool_call', {'call_id': 'a', 'command': 'uv run pytest tests/test_a.py'})
    observe(evidence, 'tool_result', {'call_id': 'a', 'ok': False, 'exit_code': 2})
    assert evidence['last_build_test']['exit_code'] == 2
    observe(evidence, 'tool_result', {'call_id': 'unknown', 'ok': True, 'exit_code': 0})
    assert evidence['last_build_test']['exit_code'] == 2
    observe(evidence, 'tool_call', {'call_id': 'b', 'tool': 'write', 'category': 'edit', 'path': str(tmp_path.parent / 'external')}, tmp_path)
    observe(evidence, 'tool_result', {'call_id': 'b', 'ok': True}, tmp_path)
    assert evidence['external_writes'][0]['path'] == str(tmp_path.parent / 'external')
    observe(evidence, 'tool_call', {'call_id': 'c', 'tool': 'write', 'category': 'edit', 'path': 'inside'}, tmp_path)
    observe(evidence, 'tool_result', {'call_id': 'c', 'ok': True}, tmp_path)
    assert len(evidence['external_writes']) == 1


def test_nested_dispatch_edit_elsewhere_in_repository_is_not_external(tmp_path):
    nested = tmp_path / 'sub'
    nested.mkdir()
    evidence = {}
    observe(evidence, 'tool_call', {'call_id': 'a', 'category': 'edit', 'path': '../file'}, nested, tmp_path)
    observe(evidence, 'tool_result', {'call_id': 'a', 'ok': True}, nested, tmp_path)
    assert not evidence.get('external_writes')


def test_recovered_outcome_is_persisted_not_reprobed(tmp_path):
    record = store.TaskRecord(task_id='a' * 32, backend='codex', session_id=None,
                              repo_path=str(tmp_path), started_at='2026-10-14T00:00:00+00:00',
                              status='completed', exit_code=0,
                              outcome={'head_commit': 'observed'}, git_baseline={'head': 'start'})
    store.write(tmp_path, record)
    recovered = store.read(tmp_path, record.task_id)
    assert recovered.outcome == record.outcome
    assert store.snapshot(tmp_path, recovered)['outcome'] == record.outcome
