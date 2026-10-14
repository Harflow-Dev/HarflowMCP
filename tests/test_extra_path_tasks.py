from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import asyncio
import pytest
from polybridge import tasks, store, scratch, takeover, control
from polybridge.extra_paths import normalize_paths, serialize_paths, revalidate_paths
from polybridge.backends.base import Invocation
from test_registry import _TrivialBackend


class Certified(_TrivialBackend):
    def qualify_extra_writable_paths(self, invocation, grants, freedom, network=None):
        if not invocation.extra_writable_paths:
            invocation = self.with_extra_writable_paths(invocation, grants, freedom, network)
        self.assert_extra_writable_paths(invocation, grants, freedom, network)
        return invocation
    def extra_path_support(self):
        return {'certification': 'fixture-v1'}
    def with_extra_writable_paths(self, invocation, grants, freedom, network=None):
        return replace(invocation, extra_writable_paths=tuple(g.path for g in grants))
    def assert_extra_writable_paths(self, invocation, grants, freedom, network=None):
        assert invocation.extra_writable_paths == tuple(g.path for g in grants)
        revalidate_paths(grants)
        self.assert_safe(invocation, freedom, network)
    def extra_path_enforcement(self, enforcement, grants, repo):
        return replace(enforcement, writable_roots=('the working directory',), extra_writable_paths=tuple(serialize_paths(grants)), extra_path_certification='fixture-v1')


def owner(tmp_path, grants):
    records=serialize_paths(grants)
    return store.TaskRecord(task_id='owner', backend='codex', session_id='session', repo_path=str(tmp_path), started_at='2026-01-01T00:00:00+00:00', extra_writable_paths=records, extra_path_certification='fixture-v1', enforcement={'extra_writable_paths': records, 'extra_path_certification': 'fixture-v1'})


@pytest.fixture
def backend(monkeypatch):
    backend=Certified()
    monkeypatch.setattr(tasks,'get_backend',lambda _:backend)
    monkeypatch.setattr(store,'get_backend',lambda _:backend)
    return backend


def test_nested_omission_inherits_and_explicit_empty_refuses(tmp_path, backend):
    outside=tmp_path/'shared';outside.mkdir()
    grants=normalize_paths([str(outside)])
    registry=tasks.TaskRegistry(log_dir=tmp_path/'logs')
    caller=SimpleNamespace(record=owner(tmp_path,grants))
    assert registry._resolve_extra_paths(caller,None,backend)==grants
    with pytest.raises(ValueError,match='ambient'):
        registry._resolve_extra_paths(caller,[],backend)


def test_nested_narrowing_and_widening(tmp_path, backend):
    repo=tmp_path/'repo';repo.mkdir()
    shared=tmp_path/'shared';shared.mkdir()
    child=shared/'file';child.write_text('x')
    other=tmp_path/'other';other.mkdir()
    registry=tasks.TaskRegistry(log_dir=tmp_path/'logs')
    caller=SimpleNamespace(record=owner(repo,normalize_paths([str(shared)])))
    assert registry._resolve_extra_paths(caller,[str(child)],backend)[0].kind=='file'
    with pytest.raises(ValueError,match='widen'):
        registry._resolve_extra_paths(caller,[str(other)],backend)


@pytest.mark.parametrize('alter',['certificate','records','enforcement'])
def test_saved_malformed_authority_refused(tmp_path,backend,alter):
    record=owner(tmp_path,normalize_paths([str(tmp_path)]))
    if alter=='certificate':record=replace(record,extra_path_certification=None)
    elif alter=='records':record=replace(record,extra_writable_paths=[{}])
    else:record=replace(record,enforcement={})
    with pytest.raises(ValueError):tasks.TaskRegistry(log_dir=tmp_path/'logs')._saved_extra_paths(record,backend)


def test_continuation_cannot_substitute_paths(tmp_path,backend):
    source=owner(tmp_path,normalize_paths([str(tmp_path)]))
    registry=tasks.TaskRegistry(log_dir=tmp_path/'logs')
    assert registry._resolve_extra_paths(None,None,backend,source)==normalize_paths([str(tmp_path)])
    with pytest.raises(ValueError,match='substitution'):
        registry._resolve_extra_paths(None,[str(tmp_path)],backend,source)


@pytest.mark.parametrize('kind',['directory','file'])
async def test_identity_replaced_during_bookkeeping_refuses_and_cleans_scratch(tmp_path,monkeypatch,backend,kind):
    path=tmp_path/'authorized'
    path.mkdir() if kind=='directory' else path.write_text('x')
    grants=normalize_paths([str(path)])
    registry=tasks.TaskRegistry(log_dir=tmp_path/'logs',open_monitor=False)
    async def thread(function,*args):
        if function.__name__=='baseline':
            path.rename(tmp_path/'old')
            path.mkdir() if kind=='directory' else path.write_text('replacement')
            return {'head':None,'working_tree_status':None}
        return function(*args)
    monkeypatch.setattr(tasks.asyncio,'to_thread',thread)
    monkeypatch.setattr(tasks,'_branch_disclosure_warranted',lambda _:False)
    async def spawn(*args,**kwargs):raise AssertionError('must not launch')
    monkeypatch.setattr(tasks.asyncio,'create_subprocess_exec',spawn)
    invocation=backend.with_extra_writable_paths(Invocation(['/bin/echo','{}']),grants,'write_in_repo')
    with pytest.raises(ValueError,match='identity') as error:
        await registry._spawn(invocation,extra_path_grants=grants,backend=backend,prompt='Task',repo_path=tmp_path,session_id=None,freedom='write_in_repo',max_turns=None,model=None,reasoning_effort=None,task_id='worker')
    assert error.value.polybridge_not_started
    assert not scratch.directory(registry.log_dir,'worker').exists()


async def test_takeover_refused_before_attempt(tmp_path,monkeypatch,backend):
    record=owner(tmp_path,normalize_paths([str(tmp_path)]))
    store.write_landed(tmp_path,record)
    monkeypatch.setattr(takeover,'_refuse_agents',lambda *args:None)
    with pytest.raises(control.TakeoverRefused,match='cannot preserve'):
        await takeover.take_over(tmp_path,record.task_id,registry_factory=lambda:None)
    assert control.takeover_attempt(tmp_path,record.task_id) is None


def test_indexed_caller_retains_authority(tmp_path,backend):
    record=owner(tmp_path,normalize_paths([str(tmp_path)]))
    projected=store.listing_header(record)[0]['_caller']
    assert projected['extra_writable_paths']==record.extra_writable_paths
    assert projected['enforcement']==record.enforcement


@pytest.mark.parametrize('recovered',[False,True])
async def test_resume_inherits_saved_identities_and_refuses_native(tmp_path,monkeypatch,backend,recovered):
    from unittest.mock import AsyncMock
    grants=normalize_paths([str(tmp_path)])
    record=owner(tmp_path,grants)
    store.write_landed(tmp_path/'logs',record)
    registry=tasks.TaskRegistry(log_dir=tmp_path/'logs',open_monitor=False)
    monkeypatch.setattr(registry,'_mutation_caller',AsyncMock(return_value=None))
    monkeypatch.setattr(registry,'session_has_live_run',lambda _:False)
    capture=AsyncMock(return_value=SimpleNamespace(task_id='child'))
    monkeypatch.setattr(registry,'_spawn',capture)
    parent=record if recovered else SimpleNamespace(**record.__dict__,record_landed=True)
    if not recovered:parent.repo_path=tmp_path
    resume=registry.resume_record if recovered else registry.resume
    await resume(parent,'Continue',task_id='child')
    assert capture.call_args.kwargs['extra_path_grants']==grants
    assert capture.call_args.args[0].extra_writable_paths==()
    with pytest.raises(ValueError,match='native workflows'):
        await resume(parent,'Continue',task_id='native-child',native_subagent=True)


async def test_workflow_builder_rejects_grants_before_spawn(tmp_path,monkeypatch,backend):
    from unittest.mock import AsyncMock
    registry=tasks.TaskRegistry(log_dir=tmp_path/'logs')
    monkeypatch.setattr(registry,'_mutation_caller',AsyncMock(return_value=None))
    capture=AsyncMock()
    monkeypatch.setattr(registry,'_spawn',capture)
    with pytest.raises(ValueError,match='workflow builders'):
        await registry.start('Task',tmp_path,backend=backend,workflow_builder=True,extra_writable_paths=[str(tmp_path)])
    capture.assert_not_awaited()


async def test_spawn_persists_grants_and_scratch_coexists(tmp_path,monkeypatch,backend):
    grants=normalize_paths([str(tmp_path)])
    registry=tasks.TaskRegistry(log_dir=tmp_path/'logs',open_monitor=False)
    monkeypatch.setattr(tasks,'_branch_disclosure_warranted',lambda _:False)
    invocation=backend.with_extra_writable_paths(Invocation(['/bin/echo','{}']),grants,'write_in_repo')
    task=await registry._spawn(invocation,extra_path_grants=grants,backend=backend,prompt='Task',repo_path=tmp_path,session_id='session',freedom='write_in_repo',max_turns=None,model=None,reasoning_effort=None,task_id='worker')
    await asyncio.wait_for(task.done.wait(),5)
    record=store.read(registry.log_dir,task.task_id)
    assert record.extra_writable_paths==serialize_paths(grants)
    assert record.extra_path_certification=='fixture-v1'
    assert task.snapshot()['resume_command'] is None
    assert store.snapshot(registry.log_dir,record)['resume_command'] is None
    assert scratch.directory(registry.log_dir,task.task_id).is_dir()


async def test_preview_matches_grants_without_allocating_scratch(tmp_path,monkeypatch,backend):
    from unittest.mock import AsyncMock
    registry=tasks.TaskRegistry(log_dir=tmp_path/'logs')
    monkeypatch.setattr(registry,'_mutation_caller',AsyncMock(return_value=None))
    result=await registry.preview_permissions(tmp_path,backend=backend,extra_writable_paths=[str(tmp_path)])
    assert result['extra_writable_paths']==serialize_paths(normalize_paths([str(tmp_path)]))
    assert result['extra_path_certification']=='fixture-v1'
    assert not registry.log_dir.exists()


async def test_granted_cancellation_during_probe_is_not_started_and_leaves_no_scratch(tmp_path,monkeypatch,backend):
    grants=normalize_paths([str(tmp_path)])
    registry=tasks.TaskRegistry(log_dir=tmp_path/'logs')
    entered=asyncio.Event()
    async def thread(function,*args):
        entered.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(tasks.asyncio,'to_thread',thread)
    monkeypatch.setattr(tasks,'_branch_disclosure_warranted',lambda _:False)
    invocation=backend.with_extra_writable_paths(Invocation(['/bin/echo','{}']),grants,'write_in_repo')
    pending=asyncio.create_task(registry._spawn(invocation,extra_path_grants=grants,backend=backend,prompt='Task',repo_path=tmp_path,session_id=None,freedom='write_in_repo',max_turns=None,model=None,reasoning_effort=None,task_id='worker'))
    await asyncio.wait_for(entered.wait(),1)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError) as error:await pending
    assert error.value.polybridge_not_started
    assert not scratch.directory(registry.log_dir,'worker').exists()


def test_saved_certificate_change_refuses_resume_authority(tmp_path,backend,monkeypatch):
    source=owner(tmp_path,normalize_paths([str(tmp_path)]))
    monkeypatch.setattr(backend,'extra_path_support',lambda:{'certification':'fixture-v2'})
    with pytest.raises(ValueError,match='incompatible'):
        tasks.TaskRegistry(log_dir=tmp_path/'logs')._saved_extra_paths(source,backend)


async def test_slow_qualification_keeps_loop_responsive_and_cancels_before_scratch(tmp_path, monkeypatch):
    import threading
    from polybridge import backends
    from polybridge.backends.codex import CodexBackend, EXTRA_PATHS_VERSION
    backend = CodexBackend()
    entered = threading.Event()
    release = threading.Event()
    def probe(_, **kwargs):
        entered.set()
        assert release.wait(3)
        return EXTRA_PATHS_VERSION
    monkeypatch.setattr(backends, 'version', probe)
    monkeypatch.setattr(backend, '_extra_path_runtime', lambda: ('/fixture',))
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'darwin')
    registry = tasks.TaskRegistry(log_dir=tmp_path/'logs', open_monitor=False)
    invocation = backend.build_start_argv('Task', repo=tmp_path, freedom='write_in_repo', session_id=None, model=None, max_turns=None, reasoning_effort=None)
    async def spawn(*args, **kwargs):
        raise AssertionError('must not launch')
    monkeypatch.setattr(tasks.asyncio, 'create_subprocess_exec', spawn)
    pending = asyncio.create_task(registry._spawn(invocation, extra_path_grants=normalize_paths([str(tmp_path)]), backend=backend, prompt='Task', repo_path=tmp_path, session_id=None, freedom='write_in_repo', max_turns=None, model=None, reasoning_effort=None, task_id='worker'))
    try:
        for _ in range(100):
            if entered.is_set():
                break
            await asyncio.sleep(.005)
        assert entered.is_set()
        assert not pending.done()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError) as error:
            await pending
        assert error.value.polybridge_not_started
        assert not scratch.directory(registry.log_dir, 'worker').exists()
    finally:
        release.set()


async def test_runtime_replaced_during_bookkeeping_refuses_and_cleans_scratch(tmp_path, monkeypatch):
    from polybridge import backends
    from polybridge.backends.codex import CodexBackend, EXTRA_PATHS_VERSION
    backend = CodexBackend()
    runtime = ['/original']
    monkeypatch.setattr(backends, 'version', lambda _, **kwargs: EXTRA_PATHS_VERSION)
    monkeypatch.setattr(backend, '_extra_path_runtime', lambda: tuple(runtime))
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'darwin')
    registry = tasks.TaskRegistry(log_dir=tmp_path/'logs', open_monitor=False)
    async def thread(function, *args):
        if function.__name__ == 'baseline':
            runtime[0] = '/replacement'
            return {'head': None, 'working_tree_status': None}
        return function(*args)
    monkeypatch.setattr(tasks.asyncio, 'to_thread', thread)
    monkeypatch.setattr(tasks, '_branch_disclosure_warranted', lambda _: False)
    async def spawn(*args, **kwargs):
        raise AssertionError('must not launch')
    monkeypatch.setattr(tasks.asyncio, 'create_subprocess_exec', spawn)
    invocation = backend.build_start_argv('Task', repo=tmp_path, freedom='write_in_repo', session_id=None, model=None, max_turns=None, reasoning_effort=None)
    from polybridge.backends.codex import UnsafeInvocationError
    with pytest.raises(UnsafeInvocationError, match='runtime identity') as error:
        await registry._spawn(invocation, extra_path_grants=normalize_paths([str(tmp_path)]), backend=backend, prompt='Task', repo_path=tmp_path, session_id=None, freedom='write_in_repo', max_turns=None, model=None, reasoning_effort=None, task_id='worker')
    assert error.value.polybridge_not_started
    assert not scratch.directory(registry.log_dir, 'worker').exists()
