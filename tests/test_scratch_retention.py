from polybridge import scratch, retention

def test_task_retention_removes_scratch_without_following_symlinks(tmp_path):
    log=tmp_path/'tasks';log.mkdir();(log/'worker.meta.json').write_text('{}')
    path=scratch.create(log,'worker');(path/'payload').write_text('review')
    assert retention._delete_task_files(log,'worker')
    assert not path.exists()
    outside=tmp_path/'outside';outside.mkdir();(outside/'keep').write_text('safe')
    path.symlink_to(outside,target_is_directory=True)
    assert retention._delete_task_files(log,'worker')
    assert (outside/'keep').read_text()=='safe'


import asyncio
import pytest
from polybridge import tasks
from polybridge.backends.base import Invocation
from test_registry import _TrivialBackend


async def dispatch(registry,backend,repo):
    return await registry._spawn(Invocation(['/bin/echo','{}']),backend=backend,prompt='Task',repo_path=repo,session_id=None,freedom='write_in_repo',max_turns=None,model=None,reasoning_effort=None,task_id='scratch-worker')


@pytest.mark.parametrize('probe',['_publish_branch_notice','_git_baseline'])
async def test_cancel_before_spawn_does_not_orphan_scratch(tmp_path,monkeypatch,probe):
    registry=tasks.TaskRegistry(log_dir=tmp_path/'tasks',open_monitor=False)
    entered=asyncio.Event()
    async def thread(function,*args):
        if function.__name__==probe:
            entered.set()
            await asyncio.Event().wait()
        return (None,None) if function.__name__=='_git_baseline' else None
    monkeypatch.setattr(tasks.asyncio,'to_thread',thread)
    monkeypatch.setattr(tasks,'_branch_disclosure_warranted',lambda _:True)
    async def spawn(*a,**k):raise AssertionError('must not spawn')
    monkeypatch.setattr(tasks.asyncio,'create_subprocess_exec',spawn)
    pending=asyncio.create_task(dispatch(registry,_TrivialBackend(),tmp_path))
    await asyncio.wait_for(entered.wait(),1)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):await pending
    assert not scratch.directory(registry._log_dir,'scratch-worker').exists()
    assert not (registry._log_dir/'scratch-worker.meta.json').exists()


async def test_non_cancel_probe_error_does_not_orphan_scratch(tmp_path,monkeypatch):
    registry=tasks.TaskRegistry(log_dir=tmp_path/'tasks',open_monitor=False)
    async def thread(function,*args):raise RuntimeError('probe failed before spawn')
    monkeypatch.setattr(tasks.asyncio,'to_thread',thread)
    monkeypatch.setattr(tasks,'_branch_disclosure_warranted',lambda _:True)
    with pytest.raises(RuntimeError,match='probe failed'):await dispatch(registry,_TrivialBackend(),tmp_path)
    assert not scratch.directory(registry._log_dir,'scratch-worker').exists()


async def test_writable_configuration_error_cleans_new_scratch(tmp_path,monkeypatch):
    registry=tasks.TaskRegistry(log_dir=tmp_path/'tasks',open_monitor=False)
    async def thread(function,*args):return (None,None)
    monkeypatch.setattr(tasks.asyncio,'to_thread',thread)
    monkeypatch.setattr(tasks,'_branch_disclosure_warranted',lambda _:False)
    class Invalid(_TrivialBackend):
        def with_writable_directory(self,*a):raise RuntimeError('invalid scratch configuration')
    with pytest.raises(RuntimeError,match='invalid scratch'):await dispatch(registry,Invalid(),tmp_path)
    assert not scratch.directory(registry._log_dir,'scratch-worker').exists()


@pytest.mark.parametrize('uncertain',[True,False])
async def test_spawn_cleanup_preserves_ambiguous_attempt_artifacts(tmp_path,monkeypatch,uncertain):
    registry=tasks.TaskRegistry(log_dir=tmp_path/'tasks',open_monitor=False)
    async def thread(function,*args):return (None,None)
    monkeypatch.setattr(tasks.asyncio,'to_thread',thread)
    monkeypatch.setattr(tasks,'_branch_disclosure_warranted',lambda _:False)
    path=scratch.directory(registry._log_dir,'scratch-worker')
    async def spawn(*a,**k):
        assert path.exists()
        (path/'possibly-running-worker-artifact').write_text('keep if ambiguous')
        if uncertain:raise asyncio.CancelledError()
        raise FileNotFoundError('exec never started')
    monkeypatch.setattr(tasks.asyncio,'create_subprocess_exec',spawn)
    with pytest.raises(asyncio.CancelledError if uncertain else FileNotFoundError):
        await dispatch(registry,_TrivialBackend(),tmp_path)
    assert path.exists() is uncertain
    if uncertain:assert (path/'possibly-running-worker-artifact').read_text()=='keep if ambiguous'
