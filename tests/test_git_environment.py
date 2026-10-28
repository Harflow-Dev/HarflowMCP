"""Process-local GitHub transport and sanitized authentication observations."""
import os
import json
import subprocess
import sys
from types import SimpleNamespace

import pytest
from harflow_mcp import git_environment as git
from harflow_mcp import tasks, store
from harflow_mcp.backends.base import Invocation
from test_extra_path_tasks import Certified, _state_logs, owned_state


def test_append_preserves_existing_config_and_source():
    source = {'GIT_CONFIG_COUNT': '1', 'GIT_CONFIG_KEY_0': 'credential.helper',
              'GIT_CONFIG_VALUE_0': 'sensitive-helper', 'SSH_AUTH_SOCK': '/tmp/agent'}
    result = git.configure(source, True)
    assert result['GIT_CONFIG_COUNT'] == '3'
    assert result['GIT_CONFIG_VALUE_0'] == 'sensitive-helper'
    assert result['GIT_CONFIG_VALUE_1'] == 'git@github.com:'
    assert result['GIT_CONFIG_VALUE_2'] == 'ssh://git@ssh.github.com:443/'
    assert source['GIT_CONFIG_COUNT'] == '1'
    assert result['SSH_AUTH_SOCK'] == source['SSH_AUTH_SOCK']
    assert git.configure(source, False) == source


@pytest.mark.parametrize('count', ['secret-value', '-1', '1', '100000', '١'])
def test_malformed_config_refuses_without_values(count):
    with pytest.raises(ValueError) as error:
        git.configure({'GIT_CONFIG_COUNT': count}, True)
    assert count not in str(error.value)


@pytest.mark.parametrize('url', ['git@github.com:example/repo.git', 'ssh://git@ssh.github.com:443/example/repo.git'])
def test_real_git_resolves_url_without_writing_configuration(tmp_path, url):
    env = {**os.environ, 'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull}
    for key in list(env):
        if key == 'GIT_CONFIG_COUNT' or key.startswith(('GIT_CONFIG_KEY_', 'GIT_CONFIG_VALUE_')):
            env.pop(key)
    subprocess.run(['git', 'init', '-q', str(tmp_path)], env=env, check=True)
    subprocess.run(['git', '-C', str(tmp_path), 'remote', 'add', 'origin', url], env=env, check=True)
    config = (tmp_path / '.git/config').read_bytes()
    resolved = subprocess.run(['git', '-C', str(tmp_path), 'remote', 'get-url', 'origin'],
                              env=git.configure(env, True), capture_output=True, text=True, check=True)
    assert resolved.stdout.strip() == 'https://github.com/example/repo.git'
    assert (tmp_path / '.git/config').read_bytes() == config


def test_effective_inheritance_and_server_override(monkeypatch):
    monkeypatch.setenv(git.REWRITE_ENV, 'false')
    assert not git.inherited(None, None)
    source = SimpleNamespace(enforcement={'git_environment': {'github_https_rewrite': True}})
    assert git.inherited(None, source)
    assert not git.inherited(False, source)
    for value in (None, {}, {'git_environment': None}, {'git_environment': []}):
        assert not git.inherited(None, SimpleNamespace(enforcement=value))
    monkeypatch.setenv(git.REWRITE_ENV, 'true')
    source.enforcement = {'git_environment': {'github_https_rewrite': False}}
    assert not git.inherited(None, source)


def test_auth_probe_discards_output_and_is_cached(monkeypatch):
    calls = []
    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout='!/usr/bin/gh auth git-credential\n')
    monkeypatch.setattr(git.shutil, 'which', lambda _: '/usr/bin/gh')
    monkeypatch.setattr(git.subprocess, 'run', run)
    git.auth_status.cache_clear()
    try:
        assert git.auth_status() == git.auth_status() == 'gh_authenticated_helper_declaration_observed'
        assert len(calls) == 2
        assert calls[0][1]['stdout'] == subprocess.DEVNULL
        assert calls[0][1]['stderr'] == subprocess.DEVNULL
        assert 'stdout' not in git.disclosure(True, git.auth_status())
    finally:
        git.auth_status.cache_clear()


@pytest.mark.parametrize('backend_name', ['claude', 'codex', 'opencode', 'vibe', 'antigravity'])
@pytest.mark.parametrize('enabled', [True, False])
async def test_every_backend_uses_shared_spawn_environment(tmp_path, monkeypatch, owned_state, backend_name, enabled):
    backend = Certified()
    backend.name = backend_name
    monkeypatch.setattr(tasks, 'get_backend', lambda _: backend)
    monkeypatch.setattr(tasks, '_branch_disclosure_warranted', lambda _: False)
    monkeypatch.setattr(git, 'auth_status', lambda: 'unknown')
    monkeypatch.delenv('GIT_CONFIG_COUNT', raising=False)
    monkeypatch.setenv('HARFLOW_TEST_SECRET_SENTINEL', 'private-environment-sentinel')
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    output = tmp_path / "child-environment.json"
    code = "import os,json,sys; open(sys.argv[1], 'w').write(json.dumps({'count':os.environ.get('GIT_CONFIG_COUNT')})); print('{}')"
    invocation = Invocation([sys.executable, '-c', code, str(output)])
    task = await registry._spawn(invocation, backend=backend, prompt='Task', repo_path=tmp_path,
        session_id=None, freedom='unrestricted', max_turns=None, model=None, reasoning_effort=None,
        git_https_rewrite=enabled)
    await task.done.wait()
    assert task.status == 'completed'
    assert task.enforcement['git_environment']['github_https_rewrite'] is enabled
    record = store.read(registry.log_dir, task.task_id)
    assert git.inherited(None, record) is enabled
    assert any('Git:' in item for item in task.bridge_notices)
    assert json.loads(output.read_text()) == {'count': '2' if enabled else None}
    assert 'private-environment-sentinel' not in task.log_path.read_text()
    assert 'private-environment-sentinel' not in store.record_path(registry.log_dir, task.task_id).read_text()
    assert 'private-environment-sentinel' not in json.dumps(task.enforcement)


@pytest.mark.parametrize('helper,code,expected', [
    ('', 0, 'gh_authenticated_helper_not_observed'),
    ('cache', 0, 'gh_authenticated_helper_not_observed'),
    ('', 1, 'gh_authenticated_helper_not_observed'),
    ('sensitive-value', 128, 'unknown'),
])
def test_auth_helper_reset_or_probe_failure_is_not_claimed_available(monkeypatch, helper, code, expected):
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(returncode=0 if len(calls) == 1 else code, stdout=helper)
    monkeypatch.setattr(git.shutil, 'which', lambda _: '/usr/bin/gh')
    monkeypatch.setattr(git.subprocess, 'run', run)
    git.auth_status.cache_clear()
    try:
        assert git.auth_status() == expected
        assert calls[-1] == ['git', 'config', '--get-urlmatch', 'credential.helper', 'https://github.com']
        assert 'sensitive-value' not in git.notice(True, expected)
    finally:
        git.auth_status.cache_clear()


async def test_malformed_config_refuses_before_scratch_allocation(tmp_path, monkeypatch, owned_state):
    backend = Certified()
    monkeypatch.setattr(git, 'auth_status', lambda: 'unknown')
    monkeypatch.setattr(tasks, '_branch_disclosure_warranted', lambda _: False)
    monkeypatch.setenv('GIT_CONFIG_COUNT', 'invalid-sensitive-value')
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    with pytest.raises(ValueError, match='invalid inherited'):
        await registry._spawn(Invocation(['/bin/echo', '{}']), backend=backend, prompt='Task', repo_path=tmp_path,
            session_id=None, freedom='unrestricted', max_turns=None, model=None, reasoning_effort=None)
    assert not list((registry.log_dir.parent / 'scratch').glob('*'))
    assert not list((registry.log_dir.parent / 'shared-scratch').glob('*/*'))
    assert not registry.list()


@pytest.mark.parametrize('backend_name', ['claude', 'codex', 'opencode', 'vibe', 'antigravity'])
@pytest.mark.parametrize('override,expected', [(None, False), (True, True)])
async def test_recovered_resume_passes_effective_saved_setting(tmp_path, monkeypatch, owned_state, backend_name, override, expected):
    from harflow_mcp import backends
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    enforcement = backends.get(backend_name).enforcement('unrestricted').as_dict()
    enforcement['git_environment'] = git.disclosure(False, 'unknown')
    source = store.TaskRecord(task_id='saved-source', backend=backend_name, session_id='conversation',
        repo_path=str(tmp_path), freedom='unrestricted', status='completed', exit_code=0,
        started_at='2026-10-26T00:00:00+00:00', enforcement=enforcement)
    store.write(registry.log_dir, source)
    async def launch(**kwargs):
        return kwargs
    monkeypatch.setattr(registry, '_launch_resume', launch)
    result = await registry.resume_record(source, 'continue', git_https_rewrite=override)
    assert result['git_https_rewrite'] is expected


def test_urlmatch_observation_is_only_a_declaration_after_later_generic_reset(tmp_path, monkeypatch):
    """Real Git can select a declaration that a later credential-chain reset defeats."""
    config = tmp_path / 'gitconfig'
    config.write_text('[credential "https://github.com"]\n\thelper = !gh auth git-credential\n[credential]\n\thelper =\n')
    monkeypatch.setenv('GIT_CONFIG_NOSYSTEM', '1')
    monkeypatch.setenv('GIT_CONFIG_GLOBAL', str(config))
    monkeypatch.delenv('GIT_CONFIG_COUNT', raising=False)
    monkeypatch.chdir(tmp_path)
    real_run = subprocess.run
    def run(argv, **kwargs):
        if argv[:3] == ['gh', 'auth', 'status']:
            return SimpleNamespace(returncode=0)
        return real_run(argv, **kwargs)
    monkeypatch.setattr(git.shutil, 'which', lambda _: '/usr/bin/gh')
    monkeypatch.setattr(git.subprocess, 'run', run)
    git.auth_status.cache_clear()
    try:
        assert git.auth_status() == 'gh_authenticated_helper_declaration_observed'
        assert git.disclosure(True, git.auth_status())['reachability_verified'] is False
    finally:
        git.auth_status.cache_clear()
