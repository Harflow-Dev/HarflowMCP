"""Canonical scoped native profiles and fail-closed request-local qualification."""
from dataclasses import replace
from pathlib import Path

import pytest

from harflow_mcp.backends import get
from harflow_mcp.backends.base import UnsupportedCapability
from harflow_mcp.backends.codex import UnsafeInvocationError
from harflow_mcp.backends import codex_group_reports as reports
from test_extra_path_backends import qualified  # noqa: F401


@pytest.fixture
def scoped(qualified, monkeypatch, tmp_path):
    monkeypatch.setattr(reports, 'qualify', lambda backend: {'ok': True, 'feature': 'group_report_output', 'identity': {'version': '0.165.0', 'platform': 'darwin'}})
    repo, group = tmp_path / 'repo', tmp_path / 'group'
    repo.mkdir(); group.mkdir()
    return qualified, repo, group


def build(backend, repo, resume=False):
    method = backend.build_resume_argv if resume else backend.build_start_argv
    return method('review', repo=repo, freedom='read_only', network=False, session_id='session' if resume else None,
                  model=None, max_turns=None, reasoning_effort=None)


@pytest.mark.parametrize('resume', [False, True])
def test_exact_profile_start_and_resume(scoped, resume):
    backend, repo, group = scoped
    inv = backend.configure_group_report_output(build(backend, repo, resume), group, 'read_only', False)
    backend.assert_safe(inv, 'read_only', False)
    backend.assert_group_report_output(inv, group, 'read_only', False)
    opts = inv.argv[:inv.argv.index('--')]
    assert '-s' not in opts and '--add-dir' not in opts
    assert 'sandbox_mode' not in str(opts)
    assert all(value in opts for value in reports.settings(group))
    assert inv.argv[inv.argv.index('-C') + 1] == str(repo)
    result = backend.group_report_output_enforcement(backend.enforcement('read_only', False), group)
    assert result.writable_roots == (str(group),)
    assert result.os_enforced and result.writes_confined and result.group_report_output


@pytest.mark.parametrize('mutate', [
    lambda i: replace(i, group_report_directory=None),
    lambda i: replace(i, group_report_identity=None),
    lambda i: replace(i, group_report_runtime=None),
    lambda i: replace(i, group_report_configuration=None),
    lambda i: replace(i, group_report_qualification=None),
    lambda i: replace(i, argv=[*i.argv[:2], '-s', 'danger-full-access', *i.argv[2:]]),
    lambda i: replace(i, argv=[*i.argv[:2], '-c', 'sandbox_mode="danger-full-access"', *i.argv[2:]]),
    lambda i: replace(i, argv=[v.replace('":root"="read"', '":root"="write"') for v in i.argv]),
    lambda i: replace(i, argv=[*i.argv[:2], '--add-dir', '/tmp', *i.argv[2:]]),
    lambda i: replace(i, caller_approvals=True),
    lambda i: replace(i, native_subagent=True),
])
def test_forged_scope_or_argv_refused(scoped, mutate):
    backend, repo, group = scoped
    inv = backend.configure_group_report_output(build(backend, repo), group, 'read_only')
    with pytest.raises((UnsafeInvocationError, UnsupportedCapability)):
        backend.assert_safe(mutate(inv), 'read_only')


def test_directory_replacement_and_config_changes_refuse(scoped, monkeypatch):
    backend, repo, group = scoped
    inv = backend.configure_group_report_output(build(backend, repo), group, 'read_only')
    group.rename(group.with_name('old')); group.mkdir()
    with pytest.raises(UnsafeInvocationError, match='identity'):
        backend.assert_safe(inv, 'read_only')
    inv = backend.configure_group_report_output(build(backend, repo), group, 'read_only')
    (Path(__import__('os').environ['CODEX_HOME']) / 'config.toml').write_text('model="different"')
    with pytest.raises(UnsafeInvocationError, match='configuration'):
        backend.assert_safe(inv, 'read_only')


@pytest.mark.parametrize('name', ['claude', 'opencode', 'vibe', 'antigravity'])
def test_unqualified_backends_fail_explicitly(name, tmp_path):
    backend = get(name)
    assert backend.group_report_output_support()['supported'] is False
    with pytest.raises(UnsupportedCapability, match='task result'):
        backend.qualify_group_report_output()


@pytest.mark.parametrize('relation', ['inside', 'ancestor', 'equal', 'alias'])
def test_repo_overlap_or_alias_refused(scoped, relation):
    backend, repo, group = scoped
    if relation == 'inside':
        group = repo / 'reports'; group.mkdir()
    elif relation == 'ancestor':
        group = repo.parent
    elif relation == 'equal':
        group = repo
    else:
        alias = group.with_name('alias'); alias.symlink_to(group); group = alias
    with pytest.raises(ValueError):
        backend.configure_group_report_output(build(backend, repo), group, 'read_only')


def test_prospective_preview_is_read_only_and_checks_configuration(scoped):
    backend, repo, group = scoped
    future = group / 'not-created' / 'yet'
    backend.preview_group_report_output(build(backend, repo), future, 'read_only')
    assert not future.exists()
    (Path(__import__('os').environ['CODEX_HOME']) / 'config.toml').write_text('sandbox_mode="workspace-write"')
    with pytest.raises(ValueError, match='legacy sandbox'):
        backend.preview_group_report_output(build(backend, repo), future, 'read_only')


def test_prospective_preview_refuses_unsafe_missing_namespace(scoped):
    backend, repo, group = scoped
    # An absent descendant of the runtime package namespace still overlaps it.
    import harflow_mcp
    unsafe = Path(harflow_mcp.__file__).parent.resolve() / 'absent-output'
    with pytest.raises(UnsafeInvocationError, match='runtime'):
        backend.preview_group_report_output(build(backend, repo), unsafe, 'read_only')
    alias = group / 'alias'; alias.symlink_to(group, target_is_directory=True)
    with pytest.raises(ValueError, match='symlink'):
        backend.preview_group_report_output(build(backend, repo), alias / 'absent', 'read_only')
    with pytest.raises(UnsupportedCapability, match='network disabled'):
        backend.preview_group_report_output(build(backend, repo), group / 'absent', 'read_only', True)


def test_forged_outside_group_field_and_receipt_refused(scoped):
    backend, repo, group = scoped
    inv = backend.configure_group_report_output(build(backend, repo), group, 'read_only')
    other = group.parent / 'unrelated'; other.mkdir()
    with pytest.raises(UnsafeInvocationError, match='identity'):
        backend.assert_safe(replace(inv, group_report_directory=str(other)), 'read_only')
    with pytest.raises(UnsafeInvocationError, match='qualification changed'):
        backend.assert_safe(replace(inv, group_report_qualification={**inv.group_report_qualification, 'extra': 'forged'}), 'read_only')


def test_unsupported_versions_and_platforms_have_no_auto_probe():
    backend = get('codex')
    spec = backend.qualification_features()['group_report_output']
    assert set(spec.seeds) == {'0.165.0'}
    assert spec.platforms == spec.seed_platforms == ('darwin',)
    assert spec.probe is None


def test_group_enforcement_is_truthful_and_ordinary_unchanged(scoped):
    backend, repo, group = scoped
    ordinary = backend.enforcement('read_only')
    assert 'group_report_output' not in ordinary.as_dict()
    scoped_value = backend.group_report_output_enforcement(ordinary, group)
    assert 'the sandbox rejects writes outright' not in scoped_value.caveats
    assert any('Repository files remain read-only' in caveat for caveat in scoped_value.caveats)


@pytest.mark.parametrize('name', ['claude', 'opencode', 'vibe', 'antigravity'])
def test_other_backend_invocations_cannot_carry_forged_report_authority(name, tmp_path):
    from harflow_mcp.backends.base import InvocationEnvironmentError
    backend = get(name)
    invocation = backend.build_start_argv('review', repo=tmp_path, freedom='read_only', session_id='session' if backend.capabilities.chooses_session_id else None,
                                         model=None, max_turns=None, reasoning_effort=None)
    with pytest.raises(InvocationEnvironmentError, match='unsupported'):
        backend.assert_safe(replace(invocation, group_report_directory=str(tmp_path)), 'read_only')


def test_runtime_checks_only_effective_group_write_root(scoped, monkeypatch):
    backend, repo, group = scoped
    from harflow_mcp import runtime_paths
    original = runtime_paths.assert_runtime_outside_writes
    checked = []
    def observe(scopes, **kwargs):
        checked.extend(scopes)
        return original(scopes, **kwargs)
    monkeypatch.setattr(runtime_paths, 'assert_runtime_outside_writes', observe)
    backend.configure_group_report_output(build(backend, repo), group, 'read_only')
    assert checked == [(group, 'directory')]


def test_runtime_swap_during_configuration_scan_refuses(scoped, monkeypatch):
    backend, repo, group = scoped
    invocation = backend.configure_group_report_output(build(backend, repo), group, 'read_only')
    from harflow_mcp.backends import codex_config
    original = codex_config.qualify_configuration
    changed = False
    def change_after_snapshot(*args, **kwargs):
        nonlocal changed
        result = original(*args, **kwargs)
        if not changed:
            changed = True
            launcher = Path(invocation.group_report_runtime[1])
            launcher.write_bytes(launcher.read_bytes() + b'-replaced-during-scan')
        return result
    monkeypatch.setattr(codex_config, 'qualify_configuration', change_after_snapshot)
    with pytest.raises(UnsafeInvocationError, match='runtime changed'):
        backend.assert_safe(invocation, 'read_only')


def test_directory_replacement_during_configuration_scan_refuses(scoped, monkeypatch):
    backend, repo, group = scoped
    invocation = backend.configure_group_report_output(build(backend, repo), group, 'read_only')
    from harflow_mcp.backends import codex_config
    original = codex_config.qualify_configuration
    changed = False
    def change_after_snapshot(*args, **kwargs):
        nonlocal changed
        result = original(*args, **kwargs)
        if not changed:
            changed = True
            group.rename(group.with_name('original-directory')); group.mkdir()
        return result
    monkeypatch.setattr(codex_config, 'qualify_configuration', change_after_snapshot)
    with pytest.raises(UnsafeInvocationError, match='directory identity changed'):
        backend.assert_safe(invocation, 'read_only')


def test_configuration_change_during_later_baseline_validation_refuses(scoped, monkeypatch):
    backend, repo, group = scoped
    invocation = backend.configure_group_report_output(build(backend, repo), group, 'read_only')
    original = backend.assert_safe
    def change_after_baseline(candidate, *args, **kwargs):
        result = original(candidate, *args, **kwargs)
        if candidate.group_report_directory is None:
            (Path(__import__('os').environ['CODEX_HOME']) / 'config.toml').write_text('model="changed-during-validation"')
        return result
    monkeypatch.setattr(backend, 'assert_safe', change_after_baseline)
    with pytest.raises(UnsafeInvocationError, match='configuration changed'):
        original(invocation, 'read_only')
