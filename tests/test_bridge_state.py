from pathlib import Path
import pytest
from polybridge.bridge_state import assert_state_outside_writes
from polybridge.extra_paths import normalize_paths
from polybridge import tasks, scratch

@pytest.fixture
def layout(tmp_path,monkeypatch):
    home=tmp_path/'home';home.mkdir();monkeypatch.setenv('HOME',str(home))
    repo=tmp_path/'repo';repo.mkdir()
    state=tmp_path/'state';state.mkdir()
    logs=state/'records';logs.mkdir()
    grant=tmp_path/'grant';grant.mkdir()
    return home,repo,logs,grant

@pytest.mark.parametrize('which',['default','custom'])
@pytest.mark.parametrize('kind',['directory','file','repo'])
def test_state_authority_refused(layout,monkeypatch,which,kind):
    home,repo,logs,grant=layout
    root={'default':home/'.polybridge','custom':logs.parent}[which]
    root.mkdir(exist_ok=True)
    target=root/'tasks';target.mkdir(exist_ok=True)
    if kind=='file':target=target/'task.meta.json';target.write_text('{}')
    grants=normalize_paths([str(grant if kind=='repo' else target)])
    with pytest.raises(ValueError,match='bridge task/control state'):
        assert_state_outside_writes(logs,target if kind=='repo' else repo,grants)


def test_state_aliases_and_ancestors(layout):
    home,repo,logs,grant=layout
    outside=grant/'records';outside.mkdir()
    logs.rmdir();logs.symlink_to(outside,target_is_directory=True)
    with pytest.raises(ValueError,match='state'):
        assert_state_outside_writes(logs,repo,normalize_paths([str(outside)]))
    with pytest.raises(ValueError,match='state'):
        assert_state_outside_writes(logs,repo,normalize_paths([str(home)]))


def test_default_task_alias_refused(layout):
    home,repo,logs,grant=layout
    default=home/'.polybridge';default.mkdir()
    (default/'tasks').symlink_to(grant,target_is_directory=True)
    with pytest.raises(ValueError,match='state'):
        assert_state_outside_writes(logs,repo,normalize_paths([str(grant)]))


def test_only_separately_allocated_scratch_allowed(layout):
    _,repo,logs,grant=layout
    allocated=scratch.create(logs,'task')
    assert_state_outside_writes(logs,repo,normalize_paths([str(grant)]),str(allocated))
    with pytest.raises(ValueError,match='state'):
        assert_state_outside_writes(logs,repo,normalize_paths([str(allocated)]))
    with pytest.raises(ValueError,match='state'):
        assert_state_outside_writes(logs,allocated,normalize_paths([str(grant)]))


def test_registry_ordinary_request_bypasses_guard(layout):
    _,_,logs,_=layout
    registry=tasks.TaskRegistry(log_dir=logs,open_monitor=False)
    registry._assert_extra_state(logs,())


async def test_registry_preview_refuses_state_before_backend_probe(layout,monkeypatch):
    from test_extra_path_tasks import Certified
    _,repo,logs,_=layout
    backend=Certified(); calls=[]
    monkeypatch.setattr(backend,'qualify_extra_writable_paths',lambda *args: calls.append(1))
    registry=tasks.TaskRegistry(log_dir=logs,open_monitor=False)
    with pytest.raises(ValueError,match='bridge task/control state'):
        await registry.preview_permissions(repo,backend=backend,extra_writable_paths=[str(logs)])
    assert not calls


def test_revalidated_state_alias_refuses_existing_authority(layout):
    _,repo,logs,grant=layout
    grants=normalize_paths([str(grant)])
    assert_state_outside_writes(logs,repo,grants)
    logs.rmdir();logs.symlink_to(grant,target_is_directory=True)
    with pytest.raises(ValueError,match='state'):
        assert_state_outside_writes(logs,repo,grants)


def test_scratch_alias_to_control_refused(layout):
    _,repo,logs,grant=layout
    controls=logs.parent/'workflow-runs';controls.mkdir()
    (controls/'task').mkdir()
    (logs.parent/'scratch').symlink_to(controls,target_is_directory=True)
    with pytest.raises(ValueError,match='scratch identity'):
        assert_state_outside_writes(logs,repo,normalize_paths([str(grant)]),str(controls/'task'))
