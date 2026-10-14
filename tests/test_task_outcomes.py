import subprocess
import pytest
from pathlib import Path

from polybridge.task_outcomes import baseline, capture, observe, GIT_LOCAL_ENV
from polybridge import store


@pytest.mark.parametrize('command', ['rg pytest tests', 'echo pytest', 'echo "hi; pytest"',
    'echo ";" pytest', r'echo \; pytest', 'echo $(pytest)', 'echo "&&" pytest', 'echo " ; " pytest', 'echo " && " pytest'])
def test_arguments_are_not_build_test_commands(command):
    evidence = {}
    observe(evidence, 'tool_call', {'call_id': 'a', 'command': command})
    observe(evidence, 'tool_result', {'call_id': 'a', 'ok': True, 'exit_code': 0})
    assert 'last_build_test' not in evidence


@pytest.mark.parametrize('command', ['pytest tests', 'uv run pytest tests', 'swift test',
    'swift build', 'xcodebuild -project App', 'cargo test', 'make build', 'npm test',
    'npm run build', 'cd repo && pytest', 'false || pytest', 'pytest; echo done'])
def test_supported_command_starts(command):
    evidence = {}
    observe(evidence, 'tool_call', {'call_id': 'a', 'command': command})
    observe(evidence, 'tool_result', {'call_id': 'a', 'exit_code': 1})
    assert evidence['last_build_test']['command'] == command
    assert evidence['last_build_test']['exit_code_scope'] == 'entire tool invocation'


def test_retained_evidence_has_utf8_byte_budgets(tmp_path):
    huge = '界' * 10000
    evidence = {}
    for index in range(101):
        observe(evidence, 'tool_call', {'call_id': huge + str(index), 'command': 'pytest ' + huge,
            'path': '../' + huge, 'tool': huge, 'category': 'edit'}, tmp_path)
    assert len(evidence['pending']) == 100
    for key, call in evidence['pending'].items():
        assert len(key) == 64
        assert len(call['command'].encode()) <= 8192
        assert len(call['tool'].encode()) <= 8192
        assert call['path'] is None
    observe(evidence, 'tool_result', {'call_id': huge + '100', 'ok': True, 'exit_code': 0}, tmp_path)
    assert not evidence.get('external_writes')
    assert 'last_build_test' not in evidence


def test_git_observation_ignores_ambient_repository_selectors(tmp_path, monkeypatch):
    wanted = tmp_path / 'wanted'
    other = tmp_path / 'other'
    wanted.mkdir()
    other.mkdir()
    repository(wanted)
    repository(other)
    git(other, 'commit', '--allow-empty', '-m', 'other head')
    start = baseline(wanted)
    git(wanted, 'update-ref', 'refs/remotes/origin/main', start['head'])
    git(wanted, 'config', 'branch.' + start['branch'] + '.remote', 'origin')
    git(wanted, 'config', 'branch.' + start['branch'] + '.merge', 'refs/heads/main')
    git(wanted, 'config', 'remote.origin.fetch', '+refs/heads/*:refs/remotes/origin/*')
    (wanted / 'file').write_text('new\n')
    git(wanted, 'add', 'file')
    git(wanted, 'commit', '-m', 'wanted change')
    expected = git(wanted, 'rev-parse', 'HEAD')
    (wanted / 'wanted-dirty').write_text('dirty')
    (other / 'other-dirty').write_text('dirty')
    for key, value in {'GIT_DIR': str(other / '.git'), 'GIT_WORK_TREE': str(other),
        'GIT_COMMON_DIR': str(other / '.git'), 'GIT_INDEX_FILE': str(other / '.git/index'),
        'GIT_OBJECT_DIRECTORY': str(other / '.git/objects'), 'GIT_CONFIG_COUNT': '1',
        'GIT_CONFIG_KEY_0': 'core.bare', 'GIT_CONFIG_VALUE_0': 'true'}.items():
        monkeypatch.setenv(key, value)
    result = capture(wanted, start, {})
    assert result['head_commit'] == expected
    assert result['new_commits'] == [expected]
    assert 'wanted-dirty' in result['terminal']['working_tree_status']
    assert 'other-dirty' not in result['terminal']['working_tree_status']
    assert result['local_upstream']['ahead_behind'] == '1\t0'


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


def test_local_git_environment_contract_is_covered():
    local_names = subprocess.run(['git', 'rev-parse', '--local-env-vars'], check=True,
        capture_output=True, text=True).stdout.splitlines()
    assert set(local_names) <= GIT_LOCAL_ENV
