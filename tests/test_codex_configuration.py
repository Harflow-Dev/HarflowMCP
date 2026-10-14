from pathlib import Path
import pytest
from polybridge.backends.codex_config import qualify_configuration, MAX_BYTES

@pytest.fixture
def context(tmp_path, monkeypatch):
    home = tmp_path / 'home'; home.mkdir()
    repo = tmp_path / 'repo'; repo.mkdir()
    grant = tmp_path / 'grant'; grant.mkdir()
    monkeypatch.setattr('polybridge.backends.codex_config._mdm_absence', lambda: ('managed_preferences_absent',))
    monkeypatch.setenv('CODEX_HOME', str(home))
    return home, repo, grant

def qualify(context):
    home, repo, grant = context
    return qualify_configuration(repo, [(repo, 'directory'), (grant, 'directory')])

@pytest.mark.parametrize('text', ['', 'notify=[]\n', '[mcp_servers.disabled]\nenabled=false\n', '[plugins.example]\nenabled=false\n', 'hooks={}\n'])
def test_disabled_profile(context, text):
    (context[0]/'config.toml').write_text(text)
    assert qualify(context)

@pytest.mark.parametrize('text', ['notify=["python", "secret"]', '[mcp_servers.active]\ncommand="secret"', '[plugins.example]\nenabled=true', 'hooks=["secret"]', '[profiles.test]\nnotify=["secret"]'])
def test_execution_rejected_without_disclosing_values(context, text):
    (context[0]/'config.toml').write_text(text)
    with pytest.raises(ValueError, match='disabled') as exc:
        qualify(context)
    assert 'secret' not in str(exc.value)

@pytest.mark.parametrize('exists', [False, True])
def test_global_config_writable_even_missing(context, exists):
    home, repo, grant = context
    path = home/'config.toml'
    if exists: path.write_text('')
    with pytest.raises(ValueError, match='namespace'):
        qualify_configuration(repo, [(home, 'directory')])

def test_worktrees_under_home_allowed(context):
    home, _, grant = context
    repo=home/'worktrees'/'repo'; repo.mkdir(parents=True)
    assert qualify_configuration(repo, [(repo,'directory'),(grant,'directory')])

def test_referenced_role_protected(context):
    home, repo, grant=context
    role=grant/'role.toml'; role.write_text('')
    (home/'config.toml').write_text(f'[agents.default]\nconfig_file="{role}"')
    with pytest.raises(ValueError, match='namespace'): qualify(context)

def test_referenced_role_execution_and_missing(context):
    home, repo, grant=context
    (home/'config.toml').write_text('[agents.default]\nconfig_file="role.toml"')
    with pytest.raises(ValueError, match='missing'): qualify(context)
    (home/'role.toml').write_text('notify=["x"]')
    with pytest.raises(ValueError, match='disabled'): qualify(context)

def test_project_presence_and_introduction(context):
    initial=qualify(context)
    project=context[1]/'.codex'; project.mkdir()
    (project/'config.toml').write_text('')
    with pytest.raises(ValueError, match='project'): qualify(context)
    (project/'config.toml').unlink()
    project.rmdir()
    assert qualify(context)==initial

def test_config_symlink_scope(context):
    home, repo, grant=context
    target=grant/'config.toml'; target.write_text('')
    (home/'config.toml').symlink_to(target)
    with pytest.raises(ValueError, match='namespace'): qualify(context)

@pytest.mark.parametrize('text', ['[broken', 'x="'+('a'*MAX_BYTES)+'"'])
def test_malformed_or_bounded(context,text):
    (context[0]/'config.toml').write_text(text)
    with pytest.raises(ValueError): qualify(context)

def test_revalidation_detects_content(context):
    home, _, _=context
    (home/'config.toml').write_text('model="a"')
    initial=qualify(context)
    (home/'config.toml').write_text('model="b"')
    assert qualify(context)!=initial


def test_direct_project_namespace_grant_refused(context):
    _, repo, _ = context
    directory=repo/'.codex'; directory.mkdir()
    with pytest.raises(ValueError, match='explicit writable'):
        qualify_configuration(repo, [(repo,'directory'),(directory,'directory')], [(directory,'directory')])
    assert qualify_configuration(repo, [(repo,'directory')], [(repo,'directory')])


def test_project_directory_alias_refused(context):
    _, repo, grant = context
    (repo/'.codex').symlink_to(grant, target_is_directory=True)
    with pytest.raises(ValueError, match='aliases'): qualify(context)


def test_sibling_and_scratch_metadata_do_not_invalidate_configuration(context):
    home, repo, grant=context
    (home/'config.toml').write_text('model="test"')
    before=qualify(context)
    (home/'unrelated-cache').mkdir()
    (repo.parent/'scratch').mkdir()
    assert qualify(context)==before


@pytest.mark.parametrize('name', ['hooks/hooks.json','hooks.json','managed_config.toml','requirements.toml'])
def test_auxiliary_inputs_presence_and_missing_namespace(context,name):
    home,repo,_=context
    path=home/name
    with pytest.raises(ValueError,match='namespace'):
        qualify_configuration(repo,[(home,'directory')])
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text('{}')
    with pytest.raises(ValueError,match='auxiliary'): qualify(context)


def test_codex_hook_feature_refused(context):
    (context[0]/'config.toml').write_text('[features]\ncodex_hooks=true')
    with pytest.raises(ValueError,match='hook feature'): qualify(context)


def test_configuration_symlink_identity_change(context):
    home,_,_=context
    first=home/'first.toml'; first.write_text('')
    second=home/'second.toml'; second.write_text('')
    config=home/'config.toml'; config.symlink_to(first)
    before=qualify(context)
    config.unlink(); config.symlink_to(second)
    assert qualify(context)!=before


def test_empty_plugin_directories_allowed_but_state_refused(context):
    root=context[0]/'plugins'; (root/'cache'/'generated').mkdir(parents=True)
    assert qualify(context)
    (root/'cache'/'plugin.json').write_text('{}')
    with pytest.raises(ValueError,match='nonempty Codex plugin'): qualify(context)


def test_plugin_namespace_protected_when_missing(context):
    home, repo, _=context
    root=home/'plugins'
    with pytest.raises(ValueError,match='plugin namespace'):
        qualify_configuration(repo,[(root/'cache','directory')])


@pytest.mark.parametrize('present,forced', [(False,False),(True,False),(False,True)])
def test_effective_managed_preferences_presence_and_owned_release(monkeypatch,present,forced):
    import ctypes
    from polybridge.backends.codex_config import _mdm_absence
    class Function:
        def __init__(self,call): self.call=call
        def __call__(self,*args): return self.call(*args)
    class Library: pass
    library=Library(); released=[]; created=[]
    def create(_,name,encoding):
        created.append(name); return len(created)
    library.CFStringCreateWithCString=Function(create)
    library.CFPreferencesCopyAppValue=Function(lambda *args: 100 if present else None)
    library.CFPreferencesAppValueIsForced=Function(lambda *args: forced)
    library.CFRelease=Function(lambda ref: released.append(ref))
    monkeypatch.setattr(ctypes,'CDLL',lambda path: library)
    if present or forced:
        with pytest.raises(ValueError,match='managed preferences'): _mdm_absence()
    else:
        assert _mdm_absence()==('managed_preferences_absent',)
    assert set(range(1,len(created)+1)).issubset(released)
    assert (100 in released)==present


def test_managed_preferences_unreadable_fails_closed(monkeypatch):
    import ctypes
    from polybridge.backends.codex_config import _mdm_absence
    def unavailable(path): raise OSError('sensitive implementation detail')
    monkeypatch.setattr(ctypes,'CDLL',unavailable)
    with pytest.raises(ValueError,match='cannot be qualified') as exc: _mdm_absence()
    assert 'sensitive' not in str(exc.value)
