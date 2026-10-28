import subprocess
import pytest
from pathlib import Path
from harflow_mcp import state_ownership
from harflow_mcp.state_layout import resolve_layout

from harflow_mcp.task_outcomes import baseline, capture, observe, GIT_LOCAL_ENV
from harflow_mcp import store


@pytest.fixture
def tmp_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("task-outcomes") / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir()
        yield root / "tasks"


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
    from harflow_mcp.task_outcomes import COMMIT_ATTRIBUTION
    projected = store.snapshot(tmp_path, recovered)['outcome']
    assert projected == {**record.outcome, 'attribution': COMMIT_ATTRIBUTION}
    assert recovered.outcome == record.outcome == {'head_commit': 'observed'}
    assert store.read(tmp_path, record.task_id).outcome == record.outcome


def test_local_git_environment_contract_is_covered():
    local_names = subprocess.run(['git', 'rev-parse', '--local-env-vars'], check=True,
        capture_output=True, text=True).stdout.splitlines()
    assert set(local_names) <= GIT_LOCAL_ENV


def test_large_packed_refs_are_bounded_inputs_not_retained_output(tmp_path):
    from harflow_mcp.task_outcomes import MAX_OUTPUT
    repo = repository(tmp_path)
    head = git(repo, 'rev-parse', 'HEAD')
    packed = '# pack-refs with: peeled fully-peeled sorted\n' + ''.join(
        f'{head} refs/tags/representative-{index:06d}\n' for index in range(5000))
    assert len(packed.encode()) > MAX_OUTPUT
    (repo / '.git' / 'packed-refs').write_text(packed)
    result = baseline(repo)
    assert result['working_tree_status'] == ''
    assert result['observation_status'] == 'success'
    assert result['error'] is None
    labels = [item['probe'] for item in result['diagnostics']['probes']]
    assert 'rev-parse --verify HEAD' in labels
    assert 'rev-parse --show-toplevel' in labels
    assert 'rev-parse --absolute-git-dir' in labels
    assert 'rev-parse --path-format=absolute --git-common-dir' in labels
    assert 'rev-parse --show-object-format' in labels


def test_git_probe_failure_error_and_output_limit_are_distinct(tmp_path, monkeypatch):
    import harflow_mcp.task_outcomes as outcomes
    probe = outcomes.GitProbe(tmp_path)
    assert probe.read('rev-parse', '--verify', 'HEAD') is None
    assert probe.diagnostics[-1]['status'] == 'git_failure'
    assert probe.diagnostics[-1]['returncode'] != 0
    monkeypatch.setattr(outcomes, 'MAX_OUTPUT', 2)
    assert probe.read('--version') is None
    assert probe.diagnostics[-1]['status'] == 'output_limit'
    def unavailable(*args, **kwargs):
        raise FileNotFoundError('not retained')
    monkeypatch.setattr(outcomes.subprocess, 'Popen', unavailable)
    assert probe.read('--version') is None
    assert probe.diagnostics[-1]['status'] == 'error'
    assert probe.diagnostics[-1]['error_type'] == 'FileNotFoundError'
    assert 'not retained' not in str(probe.diagnostics)


def test_git_probe_exhausted_budget_does_not_spawn(tmp_path, monkeypatch):
    import harflow_mcp.task_outcomes as outcomes
    probe = outcomes.GitProbe(tmp_path)
    probe.deadline = 0
    def forbidden(*args, **kwargs):
        pytest.fail('Git must not start after budget exhaustion')
    monkeypatch.setattr(outcomes.subprocess, 'Popen', forbidden)
    assert probe.read('--version') is None
    assert probe.diagnostics[-1]['status'] == 'skipped_budget_exhausted'


def test_git_probe_rechecks_budget_after_temporary_file_setup(tmp_path, monkeypatch):
    import harflow_mcp.task_outcomes as outcomes
    probe = outcomes.GitProbe(tmp_path)
    original = outcomes.tempfile.TemporaryFile
    def delayed_setup(*args, **kwargs):
        probe.deadline = 0
        return original(*args, **kwargs)
    monkeypatch.setattr(outcomes.tempfile, 'TemporaryFile', delayed_setup)
    def forbidden(*args, **kwargs):
        pytest.fail('Git must not start after setup consumed budget')
    monkeypatch.setattr(outcomes.subprocess, 'Popen', forbidden)
    assert probe.read('--version') is None
    assert probe.diagnostics[-1]['status'] == 'skipped_budget_exhausted'


def test_git_probe_timeout_then_skipped_preserves_bounded_diagnostics(tmp_path, monkeypatch):
    import sys
    import time
    import harflow_mcp.task_outcomes as outcomes
    original = subprocess.Popen
    def slow(*args, **kwargs):
        return original([sys.executable, '-c', 'import time; time.sleep(5)'], **kwargs)
    monkeypatch.setattr(outcomes.subprocess, 'Popen', slow)
    probe = outcomes.GitProbe(tmp_path)
    probe.deadline = time.monotonic() + 0.05
    started = time.monotonic()
    assert probe.read('--version') is None
    assert probe.read('--version') is None
    assert time.monotonic() - started < 1
    assert [item['status'] for item in probe.diagnostics] == ['time_limit', 'skipped_budget_exhausted']


def test_partial_terminal_status_keeps_committed_range(tmp_path, monkeypatch):
    import harflow_mcp.task_outcomes as outcomes
    repo = repository(tmp_path)
    start = baseline(repo)
    (repo / 'file').write_text('next\n')
    git(repo, 'add', 'file')
    git(repo, 'commit', '-m', 'next')
    def unavailable_status(self, worktree=None):
        self.diagnostics.append({'probe': 'status', 'status': 'time_limit', 'elapsed_ms': 1500})
        return None
    monkeypatch.setattr(outcomes.GitProbe, 'status', unavailable_status)
    result = capture(repo, start, {})
    assert result['terminal']['observation_status'] == 'partial'
    assert result['terminal']['diagnostics']['phase'] == 'terminal'
    assert result['terminal']['working_tree_status'] is None
    assert result['terminal']['head'] == git(repo, 'rev-parse', 'HEAD')
    assert result['new_commits'] == [result['head_commit']]
    assert 'file' in result['committed_diff_stat']
    assert result['diagnostics']['phase'] == 'capture'
    assert 'time_limit' in result['terminal']['error']


def test_outer_timeout_freezes_partial_without_later_worker_mutation():
    from harflow_mcp.task_outcomes import empty_baseline, interrupted_baseline
    partial = empty_baseline()
    partial.update(head='observed', diagnostics={'phase': 'baseline', 'probes': [
        {'probe': 'rev-parse --verify HEAD', 'status': 'success'}]})
    result = interrupted_baseline(partial, 'outer_timeout', 1.7)
    partial['head'] = 'later'
    partial['diagnostics']['probes'].append({'probe': 'status', 'status': 'success'})
    assert result['head'] == 'observed'
    assert result['observation_status'] == 'partial'
    assert len(result['diagnostics']['probes']) == 1
    assert result['diagnostics']['outer_status'] == 'outer_timeout'
    assert result['diagnostics']['outer_budget_ms'] == 1700


def test_non_git_and_missing_git_report_baseline_terminal_and_capture_failure(tmp_path, monkeypatch):
    start = baseline(tmp_path)
    assert start['observation_status'] == 'unavailable'
    assert all(item['status'] == 'git_failure' for item in start['diagnostics']['probes'])
    monkeypatch.setenv('PATH', str(tmp_path))
    result = capture(tmp_path, start, {})
    assert result['terminal']['observation_status'] == 'unavailable'
    assert all(item['status'] == 'error' for item in result['terminal']['diagnostics']['probes'])
    assert result['terminal']['error'] == 'working tree observation unavailable: error'
    assert result['diagnostics']['probes'][0]['error_type'] == 'FileNotFoundError'
    assert result['baseline'] is start
    assert result['head_commit'] is None
    assert result['new_commits'] is None


def test_exhausted_baseline_reports_skipped_probes_and_retains_provenance(tmp_path, monkeypatch):
    import harflow_mcp.task_outcomes as outcomes
    monkeypatch.setattr(outcomes, 'BUDGET', 0)
    result = baseline(tmp_path)
    assert result['observation_status'] == 'unavailable'
    assert result['diagnostics']['budget_ms'] == 0
    assert all(item['status'] == 'skipped_budget_exhausted' for item in result['diagnostics']['probes'])
    assert result['provenance'] == 'local Git observation with executable filters disabled'


@pytest.mark.parametrize('scenario', ['checkout', 'checkout_ff', 'same_ff', 'commit', 'detached', 'external', 'roundtrip', 'truncated'])
def test_new_commits_are_reachability_observations(tmp_path, scenario):
    from harflow_mcp.task_outcomes import COMMIT_ATTRIBUTION
    repo = repository(tmp_path)
    git(repo, 'branch', '-M', 'master')
    initial = git(repo, 'rev-parse', 'HEAD')
    git(repo, 'checkout', '-b', 'target')
    count = 102 if scenario == 'truncated' else 2
    for i in range(count):
        git(repo, 'commit', '--allow-empty', '-m', f'imported {i}')
    imported = git(repo, 'rev-parse', 'HEAD')
    git(repo, 'checkout', 'master')
    if scenario in ('checkout_ff', 'roundtrip'):
        git(repo, 'checkout', '-b', 'feature')
    start = baseline(repo)
    if scenario in ('checkout', 'truncated'):
        git(repo, 'checkout', 'target')
    elif scenario == 'detached':
        git(repo, 'checkout', '--detach', imported)
    elif scenario == 'external':
        # Another actor writes through the same checkout; capture cannot identify it.
        subprocess.run(['git', '-C', str(repo), 'merge', '--ff-only', 'target'], check=True, capture_output=True)
    else:
        if scenario in ('checkout_ff', 'roundtrip'):
            git(repo, 'checkout', 'master')
        git(repo, 'merge', '--ff-only', 'target')
        if scenario == 'roundtrip':
            git(repo, 'checkout', 'feature')
            git(repo, 'merge', '--ff-only', 'master')
    if scenario == 'commit':
        git(repo, 'commit', '--allow-empty', '-m', 'actual new commit')
    result = capture(repo, start, {})
    expected = git(repo, 'rev-list', f'{initial}..HEAD').splitlines()
    assert result['base_commit'] == initial
    assert result['new_commits'] == expected[:100]
    assert result['new_commits_truncated'] == (len(expected) > 100)
    assert result['baseline']['branch'] == start['branch']
    assert result['terminal']['branch'] == (None if scenario == 'detached' else git(repo, 'branch', '--show-current'))
    assert result['attribution'] == COMMIT_ATTRIBUTION
    assert len(COMMIT_ATTRIBUTION.encode()) <= 160
