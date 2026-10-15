"""Actual case-alias protections, measured against the real macOS kernel.

`Path.resolve` keeps a caller's supplied spelling, so on a case-insensitive filesystem a
grant requested as `CONFIG.TOML` resolved "successfully" to the protected `config.toml`
under a spelling no guard recognized. These integration tests pin the closed gap: grants,
repositories, scratch directories and protected namespaces are all compared in the
kernel's actual spelling. They run only where `F_GETPATH` spellings are available and the
filesystem really is case-insensitive; the fallback behaviour is pure unit
(tests/test_canonical_paths.py) and the platform-neutral guards are covered by the
existing suite.
"""
from pathlib import Path

import pytest

from polybridge import backends
from polybridge.backends.codex import UnsafeInvocationError
from polybridge.bridge_state import assert_state_outside_writes
from polybridge.canonical_paths import kernel_canonical_supported
from polybridge.extra_paths import normalize_paths, revalidate_paths
from polybridge.execution_config import assert_execution_config_outside_writes

from test_execution_config import isolate_configuration_environment
from test_extra_path_backends import build as build_invocation, qualified  # noqa: F401


@pytest.fixture(autouse=True)
def requires_case_insensitive_kernel(tmp_path):
    if not kernel_canonical_supported():
        pytest.skip("kernel path spellings are unavailable on this platform")
    probe = tmp_path / "Case-Alias-Probe"
    probe.mkdir()
    aliased = (tmp_path / "case-alias-probe").is_dir()
    probe.rmdir()
    if not aliased:
        pytest.skip("filesystem is case-sensitive; differently-spelled aliases do not exist here")


def test_directory_grant_records_the_actual_case(tmp_path):
    real = tmp_path / "RealDir"
    real.mkdir()
    grants = normalize_paths([str(tmp_path / "REALDIR")])
    assert grants[0].kind == "directory"
    assert grants[0].path == str(real)


def test_unicode_normalization_aliases_share_one_grant(tmp_path):
    import unicodedata
    from polybridge.canonical_paths import canonical

    real = tmp_path / 'Caf\u00e9'
    real.mkdir()
    alias = tmp_path / unicodedata.normalize('NFD', real.name)
    if not alias.is_dir():
        pytest.skip('filesystem does not normalize Unicode aliases')
    assert normalize_paths([str(alias)])[0].path == str(canonical(real))
    with pytest.raises(ValueError, match='duplicate canonical'):
        normalize_paths([str(real), str(alias)])


def test_exact_file_grant_records_the_actual_case(tmp_path):
    real = tmp_path / "Config.toml"
    real.write_text("x")
    grants = normalize_paths([str(tmp_path / "CONFIG.TOML")])
    assert grants[0].kind == "file"
    assert grants[0].path == str(real)
    revalidate_paths(grants)


def test_case_alias_duplicates_the_real_grant(tmp_path):
    real = tmp_path / "RealDir"
    real.mkdir()
    with pytest.raises(ValueError, match="duplicate"):
        normalize_paths([str(real), str(tmp_path / "REALDIR")])


def test_codex_config_case_alias_grant_refused_before_probe(qualified, monkeypatch, tmp_path):
    from polybridge.backends.codex import EXTRA_PATHS_VERSION

    config_home = tmp_path / "Codex-Home"
    config_home.mkdir()
    (config_home / "config.toml").write_text("")
    monkeypatch.setenv("CODEX_HOME", str(config_home))
    calls = []
    monkeypatch.setattr(backends, "version", lambda *args, **kwargs: calls.append(1) or EXTRA_PATHS_VERSION)
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(UnsafeInvocationError, match="Codex configuration namespace"):
        qualified.with_extra_writable_paths(
            build_invocation(qualified, repo),
            normalize_paths([str(config_home / "CONFIG.TOML")]),
            "write_in_repo",
        )
    assert calls == []


def test_state_namespace_case_alias_refused(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    (home / ".polybridge" / "tasks").mkdir(parents=True)
    logs = tmp_path / "logs"
    logs.mkdir()
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(ValueError, match="bridge task/control state"):
        assert_state_outside_writes(logs, repo, normalize_paths([str(home / ".POLYBRIDGE")]))


def test_implicit_repo_case_alias_cannot_bypass_state(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    logs = tmp_path / "State" / "records"
    logs.mkdir(parents=True)
    repo = tmp_path / "STATE"
    grant = tmp_path / "grant"
    grant.mkdir()
    with pytest.raises(ValueError, match="bridge task/control state"):
        assert_state_outside_writes(logs, repo, normalize_paths([str(grant)]))


def test_execution_config_case_alias_refused(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    repo = tmp_path / "repo"
    repo.mkdir()
    grant = tmp_path / "grant"
    grant.mkdir()
    isolate_configuration_environment(monkeypatch, home)
    config = home / "custom"
    config.mkdir()
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config))
    with pytest.raises(ValueError, match="execution configuration"):
        assert_execution_config_outside_writes(repo, normalize_paths([str(home / "CUSTOM")]))


def test_implicit_repo_case_alias_cannot_bypass_execution_config(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    grant = tmp_path / "grant"
    grant.mkdir()
    isolate_configuration_environment(monkeypatch, home)
    config = home / "custom"
    config.mkdir()
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config))
    with pytest.raises(ValueError, match="execution configuration"):
        assert_execution_config_outside_writes(tmp_path / "HOME", normalize_paths([str(grant)]))


@pytest.mark.parametrize("spelling", ["/Applications", "/applications"])
def test_monitor_bundle_namespace_case_alias_refused_before_probe(qualified, monkeypatch, spelling, tmp_path):
    if not Path(spelling).exists():
        pytest.skip("the Monitor bundle parent is not present under this spelling")
    from polybridge.backends.codex import EXTRA_PATHS_VERSION

    calls = []
    monkeypatch.setattr(backends, "version", lambda *args, **kwargs: calls.append(1) or EXTRA_PATHS_VERSION)
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(UnsafeInvocationError, match="runtime"):
        qualified.with_extra_writable_paths(
            build_invocation(qualified, repo),
            normalize_paths([spelling]),
            "write_in_repo",
        )
    assert calls == []


@pytest.mark.parametrize('existing', [False, True])
def test_data_firmlink_cannot_hide_existing_or_missing_codex_configuration(tmp_path, monkeypatch, existing):
    from polybridge.backends import codex_config
    from polybridge.canonical_paths import canonical

    repo = tmp_path / 'repo'
    home = tmp_path / 'configuration'
    repo.mkdir()
    home.mkdir()
    alias = Path('/System/Volumes/Data') / home.relative_to('/')
    if not alias.is_dir() or canonical(alias) != canonical(home):
        pytest.skip('Data firmlink alias is unavailable')
    config = home / 'config.toml'
    if existing:
        config.write_text('')
    monkeypatch.setenv('CODEX_HOME', str(alias))
    monkeypatch.setattr(codex_config, '_mdm_absence', lambda: ('managed_preferences_absent',))
    scope = canonical(config if existing else home)
    with pytest.raises(ValueError, match='configuration namespace'):
        codex_config.qualify_configuration(repo, [(repo, 'directory'),
                                                (scope, 'file' if existing else 'directory')])
