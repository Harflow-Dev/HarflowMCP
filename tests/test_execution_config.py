from pathlib import Path
import pytest
from polybridge import backends
from polybridge.execution_config import assert_execution_config_outside_writes
from polybridge.extra_paths import normalize_paths

ENVIRONMENT_INPUTS = ('CLAUDE_CONFIG_DIR', 'OPENCODE_CONFIG', 'OPENCODE_CONFIG_DIR',
    'OPENCODE_CONFIG_CONTENT', 'OPENCODE_TEST_HOME', 'OPENCODE_TUI_CONFIG',
    'OPENCODE_TEST_MANAGED_CONFIG_DIR', 'VIBE_HOME', 'XDG_CONFIG_HOME',
    'XDG_DATA_HOME', 'XDG_CACHE_HOME', 'XDG_STATE_HOME')


def isolate_configuration_environment(monkeypatch, home):
    monkeypatch.setenv('HOME', str(home))
    for name in ENVIRONMENT_INPUTS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def layout(tmp_path, monkeypatch):
    home=tmp_path/'home';home.mkdir()
    repo=tmp_path/'repo';repo.mkdir()
    grant=tmp_path/'grant';grant.mkdir()
    isolate_configuration_environment(monkeypatch, home)
    return home,repo,grant


@pytest.mark.parametrize('environment,name', [
    ('CLAUDE_CONFIG_DIR','settings.json'), ('OPENCODE_CONFIG_DIR','opencode.json'),
    ('VIBE_HOME','config.toml'), ('XDG_CONFIG_HOME','opencode/opencode.json')])
@pytest.mark.parametrize('kind',['file','directory','ancestor','symlink'])
def test_external_execution_config_grants_refused(layout,monkeypatch,environment,name,kind):
    home,repo,grant=layout
    config=home/'custom';config.mkdir()
    monkeypatch.setenv(environment,str(config))
    target=config/name;target.parent.mkdir(exist_ok=True,parents=True)
    if kind=='file': target.write_text('{}');requested=target
    elif kind=='directory':requested=target.parent
    elif kind=='ancestor':requested=config.parent
    else:
        alias=grant/'alias';alias.symlink_to(config,target_is_directory=True);requested=alias
    with pytest.raises(ValueError,match='execution configuration'):
        assert_execution_config_outside_writes(repo,normalize_paths([str(requested)]))


@pytest.mark.parametrize('backend',['claude','opencode','vibe','antigravity'])
def test_latent_default_config_without_selected_binary_is_protected(layout,backend):
    home,repo,grant=layout
    namespace=backends.get(backend).execution_configuration_namespaces(repo)['external'][0]
    namespace.parent.mkdir(parents=True,exist_ok=True)
    with pytest.raises(ValueError,match='namespace'):
        assert_execution_config_outside_writes(repo,normalize_paths([str(namespace.parent)]))


@pytest.mark.parametrize('name',['.claude','.config/opencode','.vibe','.gemini'])
def test_existing_unqualified_config_refused_even_outside_grants(layout,name):
    home,repo,grant=layout
    (home/name).mkdir(parents=True)
    with pytest.raises(ValueError,match='existing backend'):
        assert_execution_config_outside_writes(repo,normalize_paths([str(grant)]))


@pytest.mark.parametrize('name',['.claude','.opencode','.vibe','.gemini'])
def test_explicit_project_config_refused_but_inherited_repository_preserved(layout,name):
    _,repo,grant=layout
    assert_execution_config_outside_writes(repo,normalize_paths([str(grant)]))
    assert_execution_config_outside_writes(repo,normalize_paths([str(repo)]))
    project=repo/name;project.mkdir()
    with pytest.raises(ValueError,match='namespace'):
        assert_execution_config_outside_writes(repo,normalize_paths([str(project)]))


def test_inline_config_and_relative_environment_refused_without_values(layout,monkeypatch):
    _,repo,grant=layout
    monkeypatch.setenv('OPENCODE_CONFIG_CONTENT','secret-inline-contents')
    with pytest.raises(ValueError,match='inline') as exc:
        assert_execution_config_outside_writes(repo,normalize_paths([str(grant)]))
    assert 'secret' not in str(exc.value)
    monkeypatch.delenv('OPENCODE_CONFIG_CONTENT')
    monkeypatch.setenv('CLAUDE_CONFIG_DIR','relative-secret')
    with pytest.raises(ValueError,match='relative') as exc:
        assert_execution_config_outside_writes(repo,normalize_paths([str(grant)]))
    assert 'secret' not in str(exc.value)


def test_granted_output_is_requalified_when_selected_as_repository(layout):
    _, repo, output = layout
    grants = normalize_paths([str(output)])
    assert_execution_config_outside_writes(repo, grants)
    project = output / '.claude'
    project.mkdir()
    (project / 'settings.json').write_text('{}')
    # Authorized output is not an input of repo A. Selecting B creates a new
    # execution context, whose existing project configuration is not qualified.
    assert_execution_config_outside_writes(repo, grants)
    with pytest.raises(ValueError, match='existing backend execution configuration'):
        assert_execution_config_outside_writes(output, normalize_paths([str(repo)]))


def test_external_implicit_repository_overlap_refused(layout,monkeypatch):
    _,repo,grant=layout
    monkeypatch.setenv('CLAUDE_CONFIG_DIR',str(repo))
    with pytest.raises(ValueError,match='namespace'):
        assert_execution_config_outside_writes(repo,normalize_paths([str(grant)]))


def test_relative_home_refused_with_absolute_codex_home(layout,monkeypatch):
    home,repo,grant=layout
    monkeypatch.setenv('CODEX_HOME',str(home/'codex'))
    monkeypatch.setenv('HOME','relative-home')
    with pytest.raises(ValueError,match='absolute|relative'):
        assert_execution_config_outside_writes(repo,normalize_paths([str(grant)]))


def test_new_registered_backend_must_declare_execution_configuration(layout,monkeypatch):
    from polybridge.backends.base import ExtraPathsUnsupported, UnsupportedCapability
    _,repo,grant=layout
    monkeypatch.setitem(backends.BACKENDS,'unqualified-new-backend',ExtraPathsUnsupported())
    with pytest.raises(UnsupportedCapability,match='namespaces are not qualified'):
        assert_execution_config_outside_writes(repo,normalize_paths([str(grant)]))


def test_allocated_scratch_overlapping_config_refused_on_final_check(layout,monkeypatch):
    home,repo,grant=layout
    allocated=home/'bridge-scratch';allocated.mkdir()
    monkeypatch.setenv('CLAUDE_CONFIG_DIR',str(allocated/'missing-config'))
    grants=normalize_paths([str(grant)])
    assert_execution_config_outside_writes(repo,grants)
    with pytest.raises(ValueError,match='namespace'):
        assert_execution_config_outside_writes(repo,grants,str(allocated))


def test_config_symlink_target_outside_namespace_protected(layout,monkeypatch):
    home,repo,grant=layout
    root=home/'claude-override';root.symlink_to(grant,target_is_directory=True)
    monkeypatch.setenv('CLAUDE_CONFIG_DIR',str(root))
    with pytest.raises(ValueError,match='namespace'):
        assert_execution_config_outside_writes(repo,normalize_paths([str(grant)]))


@pytest.mark.parametrize('name',['OPENCODE_TUI_CONFIG','OPENCODE_TEST_MANAGED_CONFIG_DIR'])
def test_opencode_additional_override_namespaces_protected(layout,monkeypatch,name):
    home,repo,_=layout
    path=home/'managed-input'
    if name.endswith('_DIR'):path.mkdir()
    else:path.write_text('{}')
    monkeypatch.setenv(name,str(path))
    with pytest.raises(ValueError,match='namespace'):
        assert_execution_config_outside_writes(repo,normalize_paths([str(path)]))
