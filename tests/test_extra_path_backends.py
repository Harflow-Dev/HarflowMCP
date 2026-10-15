from dataclasses import replace
import pytest
from polybridge import backends
from polybridge.backends.codex import EXTRA_PATHS_VERSION, EXTRA_PATH_PAIRS, UnsafeInvocationError
from polybridge.backends.base import Invocation, UnsupportedCapability
from polybridge.extra_paths import normalize_paths


def build(backend, repo, freedom='write_in_repo', network=None, resume=False):
    method = backend.build_resume_argv if resume else backend.build_start_argv
    return method('hello', repo=repo, freedom=freedom, network=network, session_id='session' if resume else None,
                  model=None, max_turns=None, reasoning_effort=None)


@pytest.fixture
def qualified(monkeypatch, tmp_path_factory):
    # Exercise the real executable fingerprint without requiring an installed CLI.
    # The version probe is stubbed below; invoking the stand-in would fail loudly.
    monkeypatch.setattr('polybridge.backends.codex_config._mdm_absence', lambda: ('managed_preferences_absent',))
    from test_execution_config import isolate_configuration_environment
    isolate_configuration_environment(monkeypatch, tmp_path_factory.mktemp('backend-home'))
    monkeypatch.setenv('CODEX_HOME', str(tmp_path_factory.mktemp('qualified-config')))
    launcher = tmp_path_factory.mktemp('qualified-launcher') / 'codex'
    launcher.write_bytes(b'\x7fELF-native-fixture')
    launcher.chmod(0o755)
    monkeypatch.setenv('PATH', str(launcher.parent))
    # Native-shaped stand-ins exercise namespace/identity guards, not native loaders.
    monkeypatch.setattr('polybridge.runtime_loaders.qualify_loaders',
                        lambda paths: (set(paths), set()))
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'darwin')
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: EXTRA_PATHS_VERSION)
    return backends.get('codex')


def test_monitor_auto_open_is_outside_the_qualified_profile(qualified, monkeypatch, tmp_path):
    monkeypatch.setenv('PB_OPEN_MONITOR', '1')
    calls = []
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: calls.append(1))
    repo, grant = tmp_path / 'repo', tmp_path / 'grant'
    repo.mkdir()
    grant.mkdir()
    with pytest.raises(UnsafeInvocationError, match='auto-open disabled'):
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(grant)]), 'write_in_repo')
    assert calls == []


@pytest.mark.parametrize('freedom', ['write_in_repo', 'publish'])
@pytest.mark.parametrize('network', [None, False, True])
@pytest.mark.parametrize('resume', [False, True])
def test_bounded_shapes(qualified, tmp_path, freedom, network, resume):
    grants = normalize_paths([str(tmp_path)])
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path, freedom, network, resume), grants, freedom, network)
    qualified.assert_extra_writable_paths(invocation, grants, freedom, network)
    assert all(invocation.argv.count(pair) == 1 for pair in EXTRA_PATH_PAIRS)
    assert invocation.argv.index('--add-dir') < invocation.argv.index('--')
    if resume:
        assert invocation.argv.index('--add-dir') < invocation.argv.index('resume')
    enforcement = qualified.extra_path_enforcement(qualified.enforcement(freedom, network), grants, tmp_path)
    assert enforcement.writable_roots == ('the working directory', grants[0].path)
    assert enforcement.as_dict()['extra_path_certification'] == qualified.extra_path_support()['certification']


@pytest.mark.parametrize('name', ['claude', 'opencode', 'vibe', 'antigravity'])
def test_other_harnesses_reject(name, tmp_path):
    backend = backends.get(name)
    grants = normalize_paths([str(tmp_path)])
    with pytest.raises(UnsupportedCapability):
        backend.with_extra_writable_paths(Invocation([]), grants, 'write_in_repo', False)
    with pytest.raises(UnsupportedCapability):
        backend.assert_extra_writable_paths(Invocation([]), grants, 'write_in_repo', False)
    with pytest.raises(UnsupportedCapability):
        backend.extra_path_enforcement(backend.enforcement('write_in_repo'), grants, tmp_path)


def test_version_platform_native_and_freedom_rejected(qualified, monkeypatch, tmp_path):
    grants = normalize_paths([str(tmp_path)])
    invocation = build(qualified, tmp_path)
    for freedom in ['read_only', 'unrestricted']:
        with pytest.raises(UnsupportedCapability):
            qualified.with_extra_writable_paths(invocation, grants, freedom)
    with pytest.raises(UnsupportedCapability):
        qualified.with_extra_writable_paths(replace(invocation, native_subagent=True), grants, 'write_in_repo')
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: 'codex-cli other')
    with pytest.raises(UnsupportedCapability):
        qualified.with_extra_writable_paths(invocation, grants, 'write_in_repo')
    # Empty grants never query runtime qualification.
    assert qualified.with_extra_writable_paths(invocation, (), 'write_in_repo') is invocation
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: EXTRA_PATHS_VERSION)
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'linux')
    with pytest.raises(UnsupportedCapability):
        qualified.with_extra_writable_paths(invocation, grants, 'write_in_repo')


def test_tampering_and_scratch(qualified, tmp_path):
    grants = normalize_paths([str(tmp_path)])
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path), grants, 'write_in_repo')
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    invocation = qualified.with_writable_directory(invocation, scratch, 'write_in_repo')
    qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')
    with pytest.raises(UnsafeInvocationError):
        qualified.assert_extra_writable_paths(invocation, (), 'write_in_repo')
    for pair in EXTRA_PATH_PAIRS:
        argv = list(invocation.argv)
        index = argv.index(pair)
        del argv[index-1:index+1]
        with pytest.raises(UnsafeInvocationError):
            qualified.assert_safe(replace(invocation, argv=argv), 'write_in_repo')
    argv = list(invocation.argv)
    argv[argv.index('--'):argv.index('--')] = ['--add-dir', '/etc']
    with pytest.raises(UnsafeInvocationError):
        qualified.assert_safe(replace(invocation, argv=argv), 'write_in_repo')
    with pytest.raises(UnsafeInvocationError):
        qualified.assert_safe(replace(invocation, extra_writable_paths=()), 'write_in_repo')


@pytest.mark.parametrize('name', ['claude', 'opencode', 'vibe', 'antigravity'])
def test_direct_unsupported_validator_rejects_metadata(name):
    with pytest.raises(UnsupportedCapability):
        backends.get(name).assert_safe(Invocation([], extra_writable_paths=('/etc',)), 'write_in_repo')


def test_launch_rechecks_version_and_identity(qualified, monkeypatch, tmp_path):
    grantdir = tmp_path / 'grant'
    grantdir.mkdir()
    grants = normalize_paths([str(grantdir)])
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path), grants, 'write_in_repo')
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: 'changed')
    with pytest.raises(UnsupportedCapability):
        qualified.qualify_extra_writable_paths(invocation, grants, 'write_in_repo')
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: EXTRA_PATHS_VERSION)
    grantdir.rmdir()
    grantdir.write_text('changed')
    with pytest.raises(ValueError):
        qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')


def test_one_probe_and_pure_final_runtime_check(qualified, monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(backends, 'version', lambda _, **kwargs: calls.append(1) or EXTRA_PATHS_VERSION)
    monkeypatch.setattr(qualified, '_assert_launcher_outside_writes', lambda *args: None)
    runtime = ['/original']
    monkeypatch.setattr(qualified, '_extra_path_runtime', lambda: tuple(runtime))
    grants = normalize_paths([str(tmp_path)])
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path), grants, 'write_in_repo')
    qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')
    assert calls == [1]
    runtime[0] = '/replacement'
    with pytest.raises(UnsafeInvocationError, match='runtime identity'):
        qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')
    assert calls == [1]


def test_runtime_changed_during_version_probe_refused(qualified, monkeypatch, tmp_path):
    monkeypatch.setattr(qualified, '_assert_launcher_outside_writes', lambda *args: None)
    runtime = ['/original']
    monkeypatch.setattr(qualified, '_extra_path_runtime', lambda: tuple(runtime))
    def probe(_, **kwargs):
        runtime[0] = '/replacement'
        return EXTRA_PATHS_VERSION
    monkeypatch.setattr(backends, 'version', probe)
    with pytest.raises(UnsafeInvocationError, match='during qualification'):
        qualified.with_extra_writable_paths(build(qualified, tmp_path), normalize_paths([str(tmp_path)]), 'write_in_repo')


def test_runtime_snapshot_tracks_path_and_symlink_target(qualified, monkeypatch, tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    first = tmp_path / 'first'
    second = tmp_path / 'second'
    for executable in (first, second):
        executable.write_text('#!/bin/sh\n')
        executable.chmod(0o755)
    link = bin_dir / qualified.binary
    link.symlink_to(first)
    monkeypatch.setenv('PATH', str(bin_dir))
    initial = qualified._extra_path_runtime()
    link.unlink()
    link.symlink_to(second)
    assert qualified._extra_path_runtime() != initial
    unchanged_target = qualified._extra_path_runtime()
    second.write_text('#!/bin/sh\n# replacement content\n')
    assert qualified._extra_path_runtime() != unchanged_target


@pytest.mark.parametrize('backend_name', ['codex', 'claude', 'opencode', 'vibe', 'agy', 'git', 'ps', 'open'])
@pytest.mark.parametrize('container', [None, 'pyvenv.cfg', 'package.json'])
def test_selected_backend_scripts_refused_before_probe(qualified, monkeypatch, tmp_path, backend_name, container):
    import os
    bin_dir = tmp_path / 'unqualified-bin'; bin_dir.mkdir()
    if container:
        (bin_dir / container).write_text('{}')
    external = tmp_path / 'external-source'; external.mkdir()
    source = external / 'implementation.sh'; source.write_text('exit 99')
    selected = bin_dir / backend_name
    selected.write_text('#!/bin/sh\n. "' + str(source) + '"\n')
    selected.chmod(0o755)
    monkeypatch.setenv('PATH', str(bin_dir) + os.pathsep + os.environ['PATH'])
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: pytest.fail('unqualified script was executed'))
    repo = tmp_path / 'repo'; repo.mkdir()
    with pytest.raises(UnsafeInvocationError, match='script runtime'):
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(external)]), 'write_in_repo')
    plain = build(qualified, repo)
    assert qualified.with_extra_writable_paths(plain, (), 'write_in_repo') is plain


def test_probe_uses_selected_absolute_binary_without_mutating_backend(qualified, monkeypatch, tmp_path):
    monkeypatch.setattr(qualified, '_assert_launcher_outside_writes', lambda *args: None)
    selected = str(tmp_path / 'codex-alias')
    monkeypatch.setattr(qualified, '_extra_path_runtime', lambda: (selected,))
    observed = []
    monkeypatch.setattr(backends, 'version', lambda backend, **kwargs: observed.append(kwargs['executable']) or EXTRA_PATHS_VERSION)
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path), normalize_paths([str(tmp_path)]), 'write_in_repo')
    assert observed == [selected]
    assert invocation.argv[0] == selected
    assert qualified.binary == 'codex'


@pytest.mark.parametrize('scope', ['alias', 'target', 'hop', 'repo', 'scratch', 'hardlink', 'direct', 'directory-link'])
def test_launcher_overlap_refused_before_probe(qualified, monkeypatch, tmp_path, scope):
    import os
    root = tmp_path / 'launcher'
    root.mkdir()
    targetdir = root / 'targetdir'
    targetdir.mkdir()
    target = targetdir / 'executable'
    target.write_text('#!/bin/sh\nexit 99\n')
    target.chmod(0o755)
    hopdir = root / 'hopdir'
    hopdir.mkdir()
    hop = hopdir / 'hop'
    hop.symlink_to(target)
    aliasdir = root / 'aliasdir'
    aliasdir.mkdir()
    alias = aliasdir / 'codex'
    alias.symlink_to(hop)
    if scope == 'direct':
        alias.unlink()
        alias.write_text('#!/bin/sh\nexit 99\n')
        alias.chmod(0o755)
    if scope == 'directory-link':
        directory_alias = root / 'directory-alias'
        directory_alias.symlink_to(aliasdir, target_is_directory=True)
        monkeypatch.setenv('PATH', str(directory_alias))
    else:
        monkeypatch.setenv('PATH', str(aliasdir))
    calls = []
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: calls.append(1) or EXTRA_PATHS_VERSION)
    repo = tmp_path / 'repo'
    repo.mkdir()
    grant = tmp_path / 'grant'
    grant.mkdir()
    if scope in ('alias', 'target', 'hop'):
        grant = {'alias': aliasdir, 'target': targetdir, 'hop': hopdir}[scope]
    if scope == 'direct':
        grant = alias
    if scope == 'directory-link':
        grant = root
    if scope == 'repo':
        repo = root
    if scope == 'hardlink':
        grant = tmp_path / 'linked-executable'
        os.link(target, grant)
    invocation = build(qualified, repo)
    if scope == 'scratch':
        invocation = qualified.with_writable_directory(invocation, hopdir, 'write_in_repo')
    with pytest.raises(UnsafeInvocationError, match='launcher'):
        qualified.with_extra_writable_paths(invocation, normalize_paths([str(grant)]), 'write_in_repo')
    assert calls == []


def test_final_revalidation_rejects_new_launcher_hardlink(qualified, tmp_path):
    import os
    repo = tmp_path / 'repo'
    repo.mkdir()
    grant = tmp_path / 'grant'
    grant.mkdir()
    grants = normalize_paths([str(grant)])
    invocation = qualified.with_extra_writable_paths(build(qualified, repo), grants, 'write_in_repo')
    os.link(invocation.extra_path_runtime[1], grant / 'indirect-hardlink')
    with pytest.raises(UnsafeInvocationError, match='hardlinks'):
        qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')


def test_final_revalidation_rejects_added_launcher_scratch(qualified, tmp_path):
    from pathlib import Path
    repo = tmp_path / 'repo'
    repo.mkdir()
    grants = normalize_paths([str(repo)])
    invocation = qualified.with_extra_writable_paths(build(qualified, repo), grants, 'write_in_repo')
    scratch = qualified.with_writable_directory(invocation, Path(invocation.extra_path_runtime[0]).parent, 'write_in_repo')
    with pytest.raises(UnsafeInvocationError, match='overlaps'):
        qualified.assert_extra_writable_paths(scratch, grants, 'write_in_repo')


@pytest.mark.parametrize('relative', [False, True])
def test_parent_directory_symlink_alias_overlap(qualified, monkeypatch, tmp_path, relative):
    safe = tmp_path / 'safe' / 'bin'
    safe.mkdir(parents=True)
    executable = safe / 'codex'
    executable.write_text('#!/bin/sh\nexit 99\n')
    executable.chmod(0o755)
    aliasdir = tmp_path / 'aliases'
    aliasdir.mkdir()
    (aliasdir / 'hop').symlink_to('../safe/bin' if relative else safe, target_is_directory=True)
    monkeypatch.setenv('PATH', str(aliasdir / 'hop'))
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: pytest.fail('unsafe version probe'))
    repo = tmp_path / 'repo'
    repo.mkdir()
    with pytest.raises(UnsafeInvocationError, match='overlaps'):
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(aliasdir)]), 'write_in_repo')


@pytest.mark.parametrize('scope', ['git-file', 'path-directory', 'target', 'hop', 'missing-helper', 'repo', 'scratch', 'hardlink'])
def test_server_helper_authority_refused_before_probe(qualified, monkeypatch, tmp_path, scope):
    import os
    bin_dir = tmp_path / 'helpers'
    bin_dir.mkdir()
    target_dir = tmp_path / 'helper-target'
    target_dir.mkdir()
    target = target_dir / 'real-git'
    target.write_text('#!/bin/sh\nexit 99\n')
    target.chmod(0o755)
    hop_dir = tmp_path / 'helper-hop'
    hop_dir.mkdir()
    hop = hop_dir / 'git-hop'
    hop.symlink_to(target)
    helper = bin_dir / 'git'
    helper.symlink_to(hop)
    earlier = tmp_path / 'earlier-path'
    earlier.mkdir()
    monkeypatch.setenv('PATH', os.pathsep.join([str(earlier), str(bin_dir), os.environ['PATH']]))
    calls = []
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: calls.append(1) or EXTRA_PATHS_VERSION)
    repo = tmp_path / 'repo'
    repo.mkdir()
    safe = tmp_path / 'safe'
    safe.mkdir()
    grant = {'git-file': helper, 'path-directory': bin_dir, 'target': target,
             'hop': hop_dir, 'missing-helper': earlier, 'repo': safe,
             'scratch': safe, 'hardlink': safe}[scope]
    if scope == 'repo':
        repo = earlier
    if scope == 'hardlink':
        os.link(target, safe / 'hidden-alias')
    invocation = build(qualified, repo)
    if scope == 'scratch':
        invocation = qualified.with_writable_directory(invocation, earlier, 'write_in_repo')
    with pytest.raises(UnsafeInvocationError, match='runtime'):
        qualified.with_extra_writable_paths(invocation, normalize_paths([str(grant)]), 'write_in_repo')
    assert calls == []


def test_monitor_bundle_namespace_protected_regardless_of_open_monitor(qualified, monkeypatch):
    """The fixed Monitor bundle is a launch target of this server's `open` helper, so its
    whole namespace — absent names included — stays outside writable authority whether or
    not `PB_OPEN_MONITOR` asks for a launch; a later ordinary launch must stay safe."""
    from pathlib import Path
    from polybridge.runtime_paths import MONITOR_BUNDLE, assert_runtime_outside_writes
    for env in (None, "0", "1"):
        monkeypatch.delenv("PB_OPEN_MONITOR", raising=False)
        if env is not None:
            monkeypatch.setenv("PB_OPEN_MONITOR", env)
        for scope in (MONITOR_BUNDLE, Path("/Applications")):
            with pytest.raises(ValueError, match="auto-open disabled" if env == "1" else "runtime"):
                assert_runtime_outside_writes([(scope, "directory")])


def test_unrelated_grant_preserves_helper_namespace(qualified, monkeypatch, tmp_path):
    import os
    helpers = tmp_path / 'helpers'
    helpers.mkdir()
    git = helpers / 'git'
    git.write_bytes(b'\x7fELF-native-helper-fixture')
    git.chmod(0o755)
    monkeypatch.setenv('PATH', str(helpers) + os.pathsep + os.environ['PATH'])
    repo = tmp_path / 'repo'
    repo.mkdir()
    grant = tmp_path / 'grant'
    grant.mkdir()
    calls = []
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: calls.append(1) or EXTRA_PATHS_VERSION)
    invocation = qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(grant)]), 'write_in_repo')
    assert calls == [1]
    qualified.assert_extra_writable_paths(invocation, normalize_paths([str(grant)]), 'write_in_repo')


def test_final_revalidation_rejects_new_path_namespace(qualified, monkeypatch, tmp_path):
    import os
    repo = tmp_path / 'repo'
    repo.mkdir()
    grant = tmp_path / 'grant'
    grant.mkdir()
    grants = normalize_paths([str(grant)])
    invocation = qualified.with_extra_writable_paths(build(qualified, repo), grants, 'write_in_repo')
    monkeypatch.setenv('PATH', str(grant) + os.pathsep + os.environ['PATH'])
    with pytest.raises(UnsafeInvocationError, match='runtime'):
        qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')


@pytest.mark.parametrize('readonly', [False, True])
def test_helper_hardlinks_require_readonly_filesystem(qualified, monkeypatch, tmp_path, readonly):
    import os
    from types import SimpleNamespace
    helpers = tmp_path / 'helpers'
    helpers.mkdir()
    helper = helpers / 'git'
    helper.write_bytes(b'\x7fELF-native-helper-fixture')
    helper.chmod(0o755)
    os.link(helper, tmp_path / 'hidden-helper-alias')
    monkeypatch.setenv('PATH', str(helpers) + os.pathsep + os.environ['PATH'])
    monkeypatch.setattr('polybridge.runtime_paths.os.statvfs', lambda path: SimpleNamespace(f_flag=os.ST_RDONLY if readonly else 0))
    repo = tmp_path / 'repo'
    repo.mkdir()
    grant = tmp_path / 'grant'
    grant.mkdir()
    calls = []
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: calls.append(1) or EXTRA_PATHS_VERSION)
    if readonly:
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(grant)]), 'write_in_repo')
        assert calls == [1]
    else:
        with pytest.raises(UnsafeInvocationError, match='hardlinks'):
            qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(grant)]), 'write_in_repo')
        assert calls == []


@pytest.mark.parametrize('path_entry', ['', '.', 'helpers', 'missing'])
def test_relative_and_absent_helper_namespaces(qualified, monkeypatch, tmp_path, path_entry):
    import os
    server_cwd = tmp_path / 'server'
    server_cwd.mkdir()
    helpers = server_cwd / 'helpers'
    helpers.mkdir()
    monkeypatch.chdir(server_cwd)
    monkeypatch.setenv('PATH', path_entry + os.pathsep + os.environ['PATH'])
    repo = tmp_path / 'repo'
    repo.mkdir()
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: pytest.fail('unsafe version probe'))
    # No writable scope overlaps the server-relative helper directory: refusal
    # must come from cwd ambiguity itself, not incidental namespace overlap.
    grant = tmp_path / 'independent-grant'
    grant.mkdir()
    (repo / 'helpers').mkdir()
    with pytest.raises(UnsafeInvocationError, match='absolute nonempty PATH'):
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(grant)]), 'write_in_repo')
    plain = build(qualified, repo)
    assert qualified.with_extra_writable_paths(plain, (), 'write_in_repo') is plain


@pytest.mark.parametrize('root_kind', ['prefix', 'import'])
def test_python_execution_roots_refused_before_probe(qualified, monkeypatch, tmp_path, root_kind):
    import sys
    root = tmp_path / 'python-runtime'
    root.mkdir()
    if root_kind == 'prefix':
        monkeypatch.setattr('polybridge.runtime_paths.sys.prefix', str(root))
    else:
        monkeypatch.setattr('polybridge.runtime_paths.sys.path', [*sys.path, str(root)])
    grant = root / 'poison.py'
    grant.touch()
    repo = tmp_path / 'repo'
    repo.mkdir()
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: pytest.fail('unsafe version probe'))
    with pytest.raises(UnsafeInvocationError, match='runtime'):
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(grant)]), 'write_in_repo')


def test_readonly_helper_alias_still_refuses_direct_grant(qualified, monkeypatch, tmp_path):
    import os
    from types import SimpleNamespace
    helpers = tmp_path / 'helpers'
    helpers.mkdir()
    helper = helpers / 'git'
    helper.write_text('#!/bin/sh\nexit 99\n')
    helper.chmod(0o755)
    alias = tmp_path / 'alias'
    os.link(helper, alias)
    monkeypatch.setenv('PATH', str(helpers) + os.pathsep + os.environ['PATH'])
    monkeypatch.setattr('polybridge.runtime_paths.os.statvfs', lambda path: SimpleNamespace(f_flag=os.ST_RDONLY))
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: pytest.fail('unsafe version probe'))
    repo = tmp_path / 'repo'
    repo.mkdir()
    with pytest.raises(UnsafeInvocationError, match='runtime'):
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(alias)]), 'write_in_repo')


@pytest.mark.parametrize('marker', ['pyvenv.cfg', 'package.json'])
def test_selected_script_package_container_refused_before_probe(qualified, monkeypatch, tmp_path, marker):
    import os
    env = tmp_path / 'script-environment'
    bin_dir = env / 'bin'
    bin_dir.mkdir(parents=True)
    (env / marker).write_text('{}')
    script = bin_dir / 'vibe'
    script.write_text('#!/bin/sh\nexit 99\n')
    script.chmod(0o755)
    dependencies = env / 'lib' / 'site-packages'
    dependencies.mkdir(parents=True)
    poison = dependencies / 'module.py'
    poison.touch()
    monkeypatch.setenv('PATH', str(bin_dir) + os.pathsep + os.environ['PATH'])
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: pytest.fail('unsafe version probe'))
    repo = tmp_path / 'repo'
    repo.mkdir()
    with pytest.raises(UnsafeInvocationError, match='runtime'):
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(poison)]), 'write_in_repo')


def test_shebang_interpreter_environment_refused_before_probe(qualified, monkeypatch, tmp_path):
    import os
    wrapper_bin = tmp_path / 'wrappers'
    wrapper_bin.mkdir()
    environment = tmp_path / 'hidden-env'
    interpreter_dir = environment / 'bin'
    interpreter_dir.mkdir(parents=True)
    (environment / 'pyvenv.cfg').write_text('home = elsewhere')
    interpreter = interpreter_dir / 'python'
    interpreter.symlink_to('/bin/sh')
    wrapper = wrapper_bin / 'vibe'
    wrapper.write_text('#!' + str(interpreter) + '\nexit 99\n')
    wrapper.chmod(0o755)
    dependency = environment / 'lib' / 'poison.py'
    dependency.parent.mkdir()
    dependency.touch()
    monkeypatch.setenv('PATH', str(wrapper_bin) + os.pathsep + os.environ['PATH'])
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: pytest.fail('unsafe version probe'))
    repo = tmp_path / 'repo'
    repo.mkdir()
    with pytest.raises(UnsafeInvocationError, match='runtime'):
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(dependency)]), 'write_in_repo')


def test_lexical_package_does_not_hide_target_environment(qualified, monkeypatch, tmp_path):
    import os
    package = tmp_path / 'package'
    package.mkdir()
    (package / 'package.json').write_text('{}')
    environment = tmp_path / 'environment'
    environment.mkdir()
    (environment / 'pyvenv.cfg').write_text('home = elsewhere')
    script = environment / 'bin' / 'vibe'
    script.parent.mkdir()
    script.write_text('#!/bin/sh\nexit 99\n')
    script.chmod(0o755)
    (package / 'vibe').symlink_to(script)
    poison = environment / 'lib' / 'poison.py'
    poison.parent.mkdir()
    poison.touch()
    monkeypatch.setenv('PATH', str(package) + os.pathsep + os.environ['PATH'])
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: pytest.fail('unsafe version probe'))
    repo = tmp_path / 'repo'
    repo.mkdir()
    with pytest.raises(UnsafeInvocationError, match='runtime'):
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(poison)]), 'write_in_repo')


def test_virtualenv_external_python_installation_refused_before_probe(qualified, monkeypatch, tmp_path):
    import os
    installation = tmp_path / 'standalone-python'
    python = installation / 'bin' / 'python3.12'
    python.parent.mkdir(parents=True)
    # Stand-in is never executed; retain a normal header for safe qualification.
    python.write_text('#!/bin/sh\nexit 99\n')
    python.chmod(0o755)
    environment = tmp_path / 'vibe-env'
    environment.mkdir()
    (environment / 'pyvenv.cfg').write_text('home = ' + str(python.parent))
    interpreter = environment / 'bin' / 'python'
    interpreter.parent.mkdir()
    interpreter.symlink_to(python)
    script = environment / 'bin' / 'vibe'
    script.write_text('#!' + str(interpreter) + '\nexit 99\n')
    script.chmod(0o755)
    site = installation / 'lib' / 'python3.12' / 'site.py'
    site.parent.mkdir(parents=True)
    site.touch()
    monkeypatch.setenv('PATH', str(script.parent) + os.pathsep + os.environ['PATH'])
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: pytest.fail('unsafe version probe'))
    repo = tmp_path / 'repo'
    repo.mkdir()
    with pytest.raises(UnsafeInvocationError, match='runtime'):
        qualified.with_extra_writable_paths(build(qualified, repo), normalize_paths([str(site)]), 'write_in_repo')


@pytest.mark.parametrize('scope', ['repo', 'scratch', 'directory', 'file'])
def test_existing_writable_hardlink_refused_before_version(qualified, monkeypatch, tmp_path, scope):
    import os
    repo = tmp_path / 'repo'
    repo.mkdir()
    grant = tmp_path / 'grant'
    grant.mkdir()
    scratch = tmp_path / 'scratch'
    scratch.mkdir()
    source = tmp_path / 'outside-source.py'
    source.write_text('original')
    target = {'repo': repo, 'scratch': scratch, 'directory': grant, 'file': grant}[scope] / 'alias.py'
    os.link(source, target)
    grants = normalize_paths([str(target if scope == 'file' else grant)])
    invocation = build(qualified, repo)
    if scope == 'scratch':
        invocation = qualified.with_writable_directory(invocation, scratch, 'write_in_repo')
    calls = []
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: calls.append(1) or EXTRA_PATHS_VERSION)
    with pytest.raises(UnsafeInvocationError, match='multiply-linked'):
        qualified.with_extra_writable_paths(invocation, grants, 'write_in_repo')
    assert calls == []
    assert source.read_text() == 'original'


def test_final_revalidation_rejects_new_writable_file_alias(qualified, tmp_path):
    import os
    repo = tmp_path / 'repo'
    repo.mkdir()
    grant = tmp_path / 'grant'
    grant.mkdir()
    source = tmp_path / 'outside.py'
    source.write_text('x')
    grants = normalize_paths([str(grant)])
    invocation = qualified.with_extra_writable_paths(build(qualified, repo), grants, 'write_in_repo')
    os.link(source, repo / 'later-alias.py')
    with pytest.raises(UnsafeInvocationError, match='multiply-linked'):
        qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')


def test_final_recheck_rejects_grant_replaced_during_final_scan(qualified, monkeypatch, tmp_path):
    """An exact-file grant atomically replaced while the final launcher scan runs is
    refused by the closing saved-identity recheck; an unchanged grant still passes
    and qualification keeps exactly two full scans — the recheck adds none."""
    import os
    from polybridge import runtime_paths
    repo = tmp_path / 'repo'
    repo.mkdir()
    grant = tmp_path / 'grant-file'
    grant.write_text('original')
    grants = normalize_paths([str(grant)])
    scans = []
    real_scan = runtime_paths.assert_single_link_writable_files
    monkeypatch.setattr(runtime_paths, 'assert_single_link_writable_files',
                        lambda scopes: scans.append(1) or real_scan(scopes))
    invocation = qualified.with_extra_writable_paths(build(qualified, repo), grants, 'write_in_repo')
    assert len(scans) == 2
    qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')
    assert len(scans) == 3
    real_launcher_scan = qualified._assert_launcher_outside_writes
    def replace_grant_mid_scan(inv, gr, runtime):
        result = real_launcher_scan(inv, gr, runtime)
        replacement = tmp_path / 'replacement'
        replacement.write_text('replaced')
        os.replace(replacement, grant)
        return result
    monkeypatch.setattr(qualified, '_assert_launcher_outside_writes', replace_grant_mid_scan)
    with pytest.raises(ValueError, match='identity or target changed'):
        qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')
    assert len(scans) == 4


def test_active_configuration_refused_before_version_probe(qualified, monkeypatch, tmp_path):
    from pathlib import Path
    home = Path(__import__('os').environ['CODEX_HOME'])
    (home / 'config.toml').write_text('notify=["secret-command"]')
    calls = []
    monkeypatch.setattr(backends, 'version', lambda *args, **kwargs: calls.append(1) or EXTRA_PATHS_VERSION)
    with pytest.raises(UnsafeInvocationError, match='disabled'):
        qualified.with_extra_writable_paths(build(qualified, tmp_path), normalize_paths([str(tmp_path)]), 'write_in_repo')
    assert not calls
    assert qualified.with_extra_writable_paths(build(qualified, tmp_path), (), 'write_in_repo').extra_path_configuration is None


def test_configuration_changed_before_final_refused(qualified, tmp_path):
    from pathlib import Path
    home = Path(__import__('os').environ['CODEX_HOME'])
    config = home / 'config.toml'
    config.write_text('model="first"')
    grants = normalize_paths([str(tmp_path)])
    invocation = qualified.with_extra_writable_paths(build(qualified, tmp_path), grants, 'write_in_repo')
    config.write_text('model="second"')
    with pytest.raises(UnsafeInvocationError, match='configuration changed'):
        qualified.assert_extra_writable_paths(invocation, grants, 'write_in_repo')


def test_native_alias_binding_rejects_executable_substitution(qualified, monkeypatch, tmp_path):
    launcher_dir=tmp_path/'native-bin';launcher_dir.mkdir()
    target=launcher_dir/'native-target';target.write_bytes(b'\x7fELF-native-fixture');target.chmod(0o755)
    alias=launcher_dir/'codex';alias.symlink_to(target)
    monkeypatch.setenv('PATH',str(launcher_dir))
    repo=tmp_path/'repo';repo.mkdir()
    grants=normalize_paths([str(repo)])
    invocation=qualified.with_extra_writable_paths(build(qualified,repo),grants,'write_in_repo')
    assert invocation.argv[0]==str(alias)
    assert invocation.extra_path_runtime[1]==str(target)
    for binary in ('codex',str(repo/'codex'),str(target)):
        with pytest.raises(UnsafeInvocationError):
            qualified.assert_extra_writable_paths(replace(invocation,argv=[binary,*invocation.argv[1:]]),grants,'write_in_repo')


def test_other_backend_config_grant_refused_before_version_and_ordinary_unchanged(qualified,monkeypatch,tmp_path):
    config=tmp_path/'claude-config';config.mkdir()
    settings=config/'settings.json';settings.write_text('{}')
    monkeypatch.setenv('CLAUDE_CONFIG_DIR',str(config))
    monkeypatch.setattr(backends,'version',lambda *args,**kwargs: pytest.fail('unqualified config reached version'))
    repo=tmp_path/'repo';repo.mkdir()
    with pytest.raises(UnsafeInvocationError,match='execution configuration namespace'):
        qualified.with_extra_writable_paths(build(qualified,repo),normalize_paths([str(settings)]),'write_in_repo')
    plain=build(qualified,repo)
    assert qualified.with_extra_writable_paths(plain,(),'write_in_repo') is plain


def test_qualify_runs_one_preflight_and_one_final_full_scan(qualified, monkeypatch, tmp_path):
    """The private decorator must not duplicate qualify's final validation: a fresh
    qualification performs one preflight scan plus one final assert (two full
    filesystem/runtime scans, not three), and an already-decorated qualify performs
    the same two."""
    repo = tmp_path / 'repo'; repo.mkdir()
    grant = tmp_path / 'grant'; grant.mkdir()
    grants = normalize_paths([str(grant)])
    validate_calls, assert_calls, scans = [], [], []
    from polybridge import runtime_paths
    real_scan = runtime_paths.assert_single_link_writable_files
    monkeypatch.setattr(runtime_paths, 'assert_single_link_writable_files',
                        lambda scopes: scans.append(1) or real_scan(scopes))
    real_validate = qualified._validate_extra_paths
    real_assert = qualified.assert_extra_writable_paths
    monkeypatch.setattr(qualified, '_validate_extra_paths',
                        lambda inv, gr, fr, net=None: validate_calls.append(1) or real_validate(inv, gr, fr, net))
    monkeypatch.setattr(qualified, 'assert_extra_writable_paths',
                        lambda inv, gr, fr, net=None: assert_calls.append(1) or real_assert(inv, gr, fr, net))
    qualified.qualify_extra_writable_paths(build(qualified, repo), grants, 'write_in_repo')
    assert len(validate_calls) == 2
    assert len(assert_calls) == 1
    assert len(scans) == 2
    decorated = qualified.with_extra_writable_paths(build(qualified, repo), grants, 'write_in_repo')
    assert len(validate_calls) == 4
    assert len(assert_calls) == 2
    assert len(scans) == 4
    validate_calls.clear()
    assert_calls.clear()
    scans.clear()
    qualified.qualify_extra_writable_paths(decorated, grants, 'write_in_repo')
    assert len(validate_calls) == 2
    assert len(assert_calls) == 1
    assert len(scans) == 2


@pytest.mark.parametrize('mutate', ['config', 'runtime'])
def test_qualify_rejects_postversion_configuration_and_runtime_mutation(qualified, monkeypatch, tmp_path, mutate):
    """Removing the decorator's duplicate assert must not weaken qualify: a mutation
    landing after the version probe is still refused — runtime identity at the
    qualification check, configuration by qualify's final assertion."""
    repo = tmp_path / 'repo'; repo.mkdir()
    grant = tmp_path / 'grant'; grant.mkdir()
    grants = normalize_paths([str(grant)])
    if mutate == 'runtime':
        monkeypatch.setattr(qualified, '_assert_launcher_outside_writes', lambda *args: None)
        runtime = ['/original']
        monkeypatch.setattr(qualified, '_extra_path_runtime', lambda: tuple(runtime))
        def probe(_, **kwargs):
            runtime[0] = '/mutated'
            return EXTRA_PATHS_VERSION
        monkeypatch.setattr(backends, 'version', probe)
        with pytest.raises(UnsafeInvocationError, match='during qualification'):
            qualified.qualify_extra_writable_paths(build(qualified, repo), grants, 'write_in_repo')
    else:
        import os
        from pathlib import Path
        config = Path(os.environ['CODEX_HOME']) / 'config.toml'
        config.write_text('model="first"')
        def probe(_, **kwargs):
            config.write_text('model="mutated"')
            return EXTRA_PATHS_VERSION
        monkeypatch.setattr(backends, 'version', probe)
        with pytest.raises(UnsafeInvocationError, match='configuration changed'):
            qualified.qualify_extra_writable_paths(build(qualified, repo), grants, 'write_in_repo')
