from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import asyncio
import pytest
from polybridge import tasks, store, scratch, takeover, control
from polybridge.extra_paths import normalize_paths, serialize_paths, revalidate_paths
from polybridge.backends.base import Invocation
from test_registry import _TrivialBackend


async def test_live_send_malformed_enforcement_refuses_before_inbox_mutation(tmp_path, monkeypatch, backend):
    from unittest.mock import AsyncMock
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    task = SimpleNamespace(task_id='malformed-live', backend='codex', live_input=True,
        extra_writable_paths=[], extra_path_certification=None,
        enforcement={'extra_writable_paths': False}, chain_root_task_id=None)
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    locked = AsyncMock(side_effect=AssertionError('inbox mutated before authority validation'))
    monkeypatch.setattr(tasks.inbox, 'lock_async', locked)
    with pytest.raises(ValueError, match='authority is missing'):
        await registry.send_message(task, 'Follow up')
    locked.assert_not_awaited()


async def test_non_live_task_send_revalidates_authority_before_queueing(tmp_path, monkeypatch, backend):
    # The saturated pool fails the revalidation; the follow-up ledger must not be touched.
    import threading
    from unittest.mock import AsyncMock
    from polybridge import followups
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    task = SimpleNamespace(task_id='ordinary-worker', backend='codex', live_input=False,
        extra_writable_paths=serialize_paths(normalize_paths([str(tmp_path)])),
        extra_path_certification='fixture-v1', enforcement={}, chain_root_task_id=None)
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    monkeypatch.setattr(tasks, '_EXTRA_PATH_SLOTS', threading.BoundedSemaphore(1))
    assert tasks._EXTRA_PATH_SLOTS.acquire(blocking=False)
    def sent(*args, **kwargs):
        raise AssertionError('queued before authority was revalidated')
    monkeypatch.setattr(followups, 'send', sent)
    with pytest.raises(ValueError, match='capacity'):
        await registry.send_message(task, 'Follow up')


async def test_non_live_record_send_revalidates_authority_before_queueing(tmp_path, monkeypatch, backend):
    import threading
    from unittest.mock import AsyncMock
    from polybridge import followups
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    record = replace(owner(tmp_path, normalize_paths([str(tmp_path)])), backend=backend.name)
    store.write_landed(registry.log_dir, record)
    assert store.read(registry.log_dir, record.task_id) is not None
    assert not store.read(registry.log_dir, record.task_id).live_input
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    monkeypatch.setattr(tasks, '_EXTRA_PATH_SLOTS', threading.BoundedSemaphore(1))
    assert tasks._EXTRA_PATH_SLOTS.acquire(blocking=False)
    def sent(*args, **kwargs):
        raise AssertionError('queued before authority was revalidated')
    monkeypatch.setattr(followups, 'send', sent)
    with pytest.raises(ValueError, match='capacity'):
        await registry.send_to_record(record.task_id, 'Follow up')


def _state_logs(repo):
    # State is independently located, outside the repository/grant fixture.
    return repo.parent / (repo.name + '-bridge-state') / 'logs'


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
    registry=tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    caller=SimpleNamespace(record=owner(tmp_path,grants))
    assert registry._resolve_extra_paths(caller,None,backend)==grants
    with pytest.raises(ValueError,match='ambient'):
        registry._resolve_extra_paths(caller,[],backend)


def test_nested_narrowing_and_widening(tmp_path, backend):
    repo=tmp_path/'repo';repo.mkdir()
    shared=tmp_path/'shared';shared.mkdir()
    child=shared/'file';child.write_text('x')
    other=tmp_path/'other';other.mkdir()
    registry=tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
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
    with pytest.raises(ValueError):tasks.TaskRegistry(log_dir=_state_logs(tmp_path))._saved_extra_paths(record,backend)


def test_continuation_cannot_substitute_paths(tmp_path,backend):
    source=owner(tmp_path,normalize_paths([str(tmp_path)]))
    registry=tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    assert registry._resolve_extra_paths(None,None,backend,source)==normalize_paths([str(tmp_path)])
    with pytest.raises(ValueError,match='substitution'):
        registry._resolve_extra_paths(None,[str(tmp_path)],backend,source)


@pytest.mark.parametrize('kind',['directory','file'])
async def test_identity_replaced_during_bookkeeping_refuses_and_cleans_scratch(tmp_path,monkeypatch,backend,kind):
    path=tmp_path/'authorized'
    path.mkdir() if kind=='directory' else path.write_text('x')
    grants=normalize_paths([str(path)])
    registry=tasks.TaskRegistry(log_dir=_state_logs(tmp_path),open_monitor=False)
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
    store.write_landed(_state_logs(tmp_path),record)
    registry=tasks.TaskRegistry(log_dir=_state_logs(tmp_path),open_monitor=False)
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
    registry=tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    monkeypatch.setattr(registry,'_mutation_caller',AsyncMock(return_value=None))
    capture=AsyncMock()
    monkeypatch.setattr(registry,'_spawn',capture)
    with pytest.raises(ValueError,match='workflow builders'):
        await registry.start('Task',tmp_path,backend=backend,workflow_builder=True,extra_writable_paths=[str(tmp_path)])
    capture.assert_not_awaited()


async def test_spawn_persists_grants_and_scratch_coexists(tmp_path,monkeypatch,backend):
    grants=normalize_paths([str(tmp_path)])
    registry=tasks.TaskRegistry(log_dir=_state_logs(tmp_path),open_monitor=False)
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
    registry=tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    monkeypatch.setattr(registry,'_mutation_caller',AsyncMock(return_value=None))
    result=await registry.preview_permissions(tmp_path,backend=backend,extra_writable_paths=[str(tmp_path)])
    assert result['extra_writable_paths']==serialize_paths(normalize_paths([str(tmp_path)]))
    assert result['extra_path_certification']=='fixture-v1'
    assert not registry.log_dir.exists()


async def test_granted_cancellation_during_probe_is_not_started_and_leaves_no_scratch(tmp_path,monkeypatch,backend):
    grants=normalize_paths([str(tmp_path)])
    registry=tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
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
        tasks.TaskRegistry(log_dir=_state_logs(tmp_path))._saved_extra_paths(source,backend)


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
    monkeypatch.setattr(backend, '_assert_launcher_outside_writes', lambda *args: None)
    monkeypatch.setattr(backend, '_extra_path_configuration', lambda *args: ('fixture',))
    monkeypatch.setattr(backend, '_assert_execution_configuration', lambda *args: None)
    monkeypatch.setattr(backend, '_extra_path_runtime', lambda: ('/fixture',))
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'darwin')
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
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
    monkeypatch.setattr(backend, '_assert_launcher_outside_writes', lambda *args: None)
    monkeypatch.setattr(backend, '_extra_path_configuration', lambda *args: ('fixture',))
    monkeypatch.setattr(backend, '_assert_execution_configuration', lambda *args: None)
    monkeypatch.setattr(backend, '_extra_path_runtime', lambda: tuple(runtime))
    monkeypatch.setattr('polybridge.backends.codex.sys.platform', 'darwin')
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
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


# Exercise records decoded from disk, including old records without top-level markers.
AUTHORITY_MARKERS = [
    {"enforcement": {"extra_writable_paths": [{"path": "/saved"}]}},
    {"enforcement": {"extra_path_certification": "fixture-v1"}},
    {"extra_writable_paths": {}},
    {"extra_path_certification": False},
    {"extra_path_certification": ""},
    {"enforcement": False},
    {"enforcement": []},
    {"enforcement": {"extra_writable_paths": False}},
    {"enforcement": {"extra_writable_paths": {}}},
    {"enforcement": {"extra_writable_paths": None}},
    {"enforcement": {"extra_path_certification": False}},
    {"enforcement": {"extra_path_certification": ""}},
]
LEGACY_MARKERS = [{}, {"enforcement": None}, {"enforcement": {}},
                  {"extra_writable_paths": [], "extra_path_certification": None,
                   "enforcement": {"extra_writable_paths": [], "extra_path_certification": None}}]


def persisted_markers(tmp_path, markers):
    import json
    record = store.TaskRecord(task_id="saved", backend="codex", session_id="session",
                              repo_path=str(tmp_path), started_at="2026-01-01T00:00:00+00:00")
    store.write_landed(tmp_path, record)
    path = store.record_path(tmp_path, record.task_id)
    payload = json.loads(path.read_text())
    for key in ("extra_writable_paths", "extra_path_certification", "enforcement"):
        payload.pop(key, None)
    payload.update(markers)
    path.write_text(json.dumps(payload))
    return store.read(tmp_path, record.task_id)


@pytest.mark.parametrize("markers,blocked", [(m, True) for m in AUTHORITY_MARKERS] + [(m, False) for m in LEGACY_MARKERS])
def test_live_and_recovered_snapshot_saved_markers(tmp_path, monkeypatch, markers, blocked):
    from datetime import datetime, timezone
    record = persisted_markers(tmp_path, markers)
    monkeypatch.setattr(store, "resume_command", lambda *args: "ordinary resume")
    monkeypatch.setattr(tasks, "resume_command", lambda *args: "ordinary resume")
    live = tasks.Task(task_id="saved", backend="codex", session_id="session", repo_path=tmp_path,
                      prompt="Task", max_turns=1, log_path=tmp_path/"saved.log",
                      started_at=datetime.now(timezone.utc), extra_writable_paths=record.extra_writable_paths,
                      extra_path_certification=record.extra_path_certification, enforcement=record.enforcement)
    expected = None if blocked else "ordinary resume"
    assert live.snapshot()["resume_command"] == expected
    assert store.snapshot(tmp_path, record)["resume_command"] == expected


@pytest.mark.parametrize("markers", AUTHORITY_MARKERS)
async def test_saved_markers_refuse_resume_and_takeover_before_mutation(tmp_path, monkeypatch, backend, markers):
    record = persisted_markers(tmp_path, markers)
    from unittest.mock import AsyncMock
    registry = tasks.TaskRegistry(log_dir=tmp_path)
    monkeypatch.setattr(registry, "_mutation_caller", AsyncMock(return_value=None))
    spawn = AsyncMock()
    monkeypatch.setattr(registry, "_spawn", spawn)
    with pytest.raises(ValueError):
        await registry.resume_record(record, "Continue", task_id="child")
    spawn.assert_not_awaited()
    monkeypatch.setattr(takeover, "_refuse_agents", lambda *args: None)
    def mutation(*args, **kwargs):
        raise AssertionError("authority must be checked before control mutation")
    monkeypatch.setattr(control, "begin_takeover", mutation)
    with pytest.raises(control.TakeoverRefused) as error:
        await takeover.take_over(tmp_path, record.task_id, registry_factory=mutation)
    assert error.value.code == "extra_paths_unsupported"
    with pytest.raises(control.TakeoverRefused) as error:
        takeover._open_attempt(tmp_path, record.task_id, {}, record.session_id)
    assert error.value.code == "extra_paths_unsupported"
    with pytest.raises(control.TakeoverRefused) as error:
        takeover.attach(tmp_path, record.task_id, pid=12345)
    assert error.value.code == "extra_paths_unsupported"
    assert control.takeover_attempt(tmp_path, record.task_id) is None


@pytest.mark.parametrize("markers", LEGACY_MARKERS)
def test_legacy_saved_markers_keep_resume_authority(tmp_path, backend, markers):
    record = persisted_markers(tmp_path, markers)
    assert tasks.TaskRegistry(log_dir=tmp_path)._saved_extra_paths(record, backend) == ()


@pytest.mark.parametrize('operation', ['start', 'preview'])
async def test_slow_path_resolution_times_out_without_dispatch(tmp_path, monkeypatch, backend, operation):
    import threading
    from unittest.mock import AsyncMock
    entered, release = threading.Event(), threading.Event()
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    def resolve(*args):
        entered.set()
        assert release.wait(3)
        return ()
    monkeypatch.setattr(registry, '_resolve_extra_paths', resolve)
    monkeypatch.setattr(tasks, 'EXTRA_PATH_CHECK_BUDGET_SECONDS', .1)
    spawn = AsyncMock(side_effect=AssertionError('must not launch'))
    monkeypatch.setattr(tasks.asyncio, 'create_subprocess_exec', spawn)
    kwargs = dict(backend=backend, extra_writable_paths=[str(tmp_path)])
    pending = asyncio.create_task(registry.start('Task', tmp_path, **kwargs) if operation == 'start' else registry.preview_permissions(tmp_path, **kwargs))
    try:
        for _ in range(100):
            if entered.is_set(): break
            await asyncio.sleep(.001)
        assert entered.is_set() and not pending.done()
        # This timer must advance while the filesystem worker remains blocked.
        await asyncio.sleep(.01)
        assert not release.is_set()
        with pytest.raises(ValueError, match='filesystem availability') as error:
            await pending
        if operation == 'start': assert error.value.polybridge_not_started
        spawn.assert_not_awaited()
        assert not registry.log_dir.exists()
    finally:
        release.set()


@pytest.mark.parametrize('exit_mode', ['timeout', 'cancel', 'request_budget'])
async def test_final_path_validation_block_removes_scratch(tmp_path, monkeypatch, backend, exit_mode):
    import threading
    entered, release = threading.Event(), threading.Event()
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
    grants = normalize_paths([str(tmp_path)])
    invocation = backend.with_extra_writable_paths(Invocation(['/bin/echo','{}']), grants, 'write_in_repo')
    original = backend.assert_extra_writable_paths
    calls = 0
    def validate(*args):
        nonlocal calls
        calls += 1
        # Qualification asserts its own result; the next assertion is the post-scratch
        # final validation.
        if calls == 2:
            entered.set()
            assert release.wait(3)
        return original(*args)
    monkeypatch.setattr(backend, 'assert_extra_writable_paths', validate)
    monkeypatch.setattr(tasks, 'EXTRA_PATH_CHECK_BUDGET_SECONDS', 5 if exit_mode == 'request_budget' else .2)
    monkeypatch.setattr(tasks, '_branch_disclosure_warranted', lambda _: False)
    async def spawn(*args, **kwargs): raise AssertionError('must not launch')
    monkeypatch.setattr(tasks.asyncio, 'create_subprocess_exec', spawn)
    async def launch():
        with tasks.authority_check_budget(seconds=1 if exit_mode == 'request_budget' else tasks.AUTHORITY_CHECK_REQUEST_BUDGET_SECONDS):
            return await registry._spawn(invocation, extra_path_grants=grants, backend=backend, prompt='Task', repo_path=tmp_path, session_id=None, freedom='write_in_repo', max_turns=None, model=None, reasoning_effort=None, task_id='worker')
    pending = asyncio.create_task(launch())
    try:
        for _ in range(200):
            if entered.is_set(): break
            await asyncio.sleep(.002)
        assert entered.is_set() and not pending.done()
        assert scratch.directory(registry.log_dir, 'worker').exists()
        await asyncio.sleep(.01)
        if exit_mode == 'cancel': pending.cancel()
        with pytest.raises(asyncio.CancelledError if exit_mode == 'cancel' else ValueError) as error:
            await pending
        assert error.value.polybridge_not_started
        assert not scratch.directory(registry.log_dir, 'worker').exists()
        assert registry.get('worker') is None
        assert store.read(registry.log_dir, 'worker') is None
    finally:
        release.set()


@pytest.mark.parametrize('operation', ['resume', 'resume_record', 'continuation', 'send_message', 'send_to_record'])
async def test_saved_authority_resolution_timeout_prevents_effects(tmp_path, monkeypatch, backend, operation):
    import threading
    from unittest.mock import AsyncMock
    entered, release = threading.Event(), threading.Event()
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
    record = replace(owner(tmp_path, normalize_paths([str(tmp_path)])), backend=backend.name, live_input=True)
    store.write_landed(registry.log_dir, record)
    parent = SimpleNamespace(**record.__dict__, record_landed=True)
    parent.live_input = True
    parent.repo_path = tmp_path
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    def resolve(*args):
        entered.set()
        assert release.wait(3)
        return ()
    monkeypatch.setattr(registry, '_resolve_extra_paths', resolve)
    monkeypatch.setattr(tasks, 'EXTRA_PATH_CHECK_BUDGET_SECONDS', .1)
    spawn = AsyncMock(side_effect=AssertionError('must not spawn successor'))
    monkeypatch.setattr(registry, '_spawn', spawn)
    inbox_lock = AsyncMock(side_effect=AssertionError('must not mutate inbox'))
    monkeypatch.setattr(tasks.inbox, 'lock_async', inbox_lock)
    def inbox_send(*args, **kwargs): raise AssertionError('must not send recorded message')
    monkeypatch.setattr(tasks.inbox, 'send_to_record', inbox_send)
    if operation == 'resume': call = registry.resume(parent, 'Follow up', task_id='child')
    elif operation == 'resume_record': call = registry.resume_record(record, 'Follow up', task_id='child')
    elif operation == 'continuation':
        call = registry.start('Follow up', tmp_path, backend=backend, freedom=record.freedom,
                              continuation_of_task_id=record.task_id, task_id='child')
    elif operation == 'send_message': call = registry.send_message(parent, 'Follow up')
    else: call = registry.send_to_record(record.task_id, 'Follow up')
    pending = asyncio.create_task(call)
    try:
        for _ in range(100):
            if entered.is_set(): break
            await asyncio.sleep(.001)
        assert entered.is_set() and not pending.done()
        await asyncio.sleep(.01)
        with pytest.raises(ValueError, match='filesystem availability') as error:
            await pending
        if operation in ('resume', 'resume_record', 'continuation'):
            assert error.value.polybridge_not_started
        spawn.assert_not_awaited()
        inbox_lock.assert_not_awaited()
        assert store.read(registry.log_dir, 'child') is None
        assert not scratch.directory(registry.log_dir, 'child').exists()
    finally:
        release.set()


async def test_abandoned_path_workers_keep_capacity_and_do_not_starve_shared_executor(tmp_path, monkeypatch, backend):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='test-extra-path')
    slots = threading.BoundedSemaphore(2)
    release = threading.Event()
    entered = [threading.Event(), threading.Event()]
    monkeypatch.setattr(tasks, '_EXTRA_PATH_EXECUTOR', executor)
    monkeypatch.setattr(tasks, '_EXTRA_PATH_SLOTS', slots)
    monkeypatch.setattr(tasks, 'EXTRA_PATH_CHECK_BUDGET_SECONDS', .1)
    def blocked(index):
        entered[index].set()
        assert release.wait(3)
    pending = [asyncio.create_task(tasks._bounded_extra_path_check(blocked, i)) for i in range(2)]
    try:
        for _ in range(100):
            if all(event.is_set() for event in entered): break
            await asyncio.sleep(.001)
        assert all(event.is_set() for event in entered)
        for check in pending:
            with pytest.raises(ValueError, match='timed out'):
                await check
        # Running futures survived waiter cancellation: neither slot may be reused.
        assert not slots.acquire(blocking=False)
        ran = threading.Event()
        with pytest.raises(ValueError, match='capacity is exhausted'):
            await tasks._bounded_extra_path_check(ran.set)
        assert not ran.is_set()
        from unittest.mock import AsyncMock
        registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
        monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
        result = await registry.preview_permissions(tmp_path, backend=backend)
        assert result['extra_writable_paths'] == []
        capture = AsyncMock(return_value='ordinary-start')
        monkeypatch.setattr(registry, '_spawn', capture)
        assert await registry.start('Ordinary', tmp_path, backend=backend, extra_writable_paths=[]) == 'ordinary-start'
        assert capture.call_args.kwargs['extra_path_grants'] == ()
        assert await asyncio.wait_for(asyncio.to_thread(lambda: 'unrelated'), .5) == 'unrelated'
    finally:
        release.set()
        # Joining is safe only after releasing the workers; do not leak blocked test threads.
        await asyncio.to_thread(executor.shutdown, wait=True)
        await asyncio.gather(*pending, return_exceptions=True)
    # Both actual futures finished and returned their slots, even after asyncio timed out.
    assert slots.acquire(blocking=False)
    assert slots.acquire(blocking=False)
    assert not slots.acquire(blocking=False)
    slots.release()
    slots.release()


async def test_request_budget_caps_each_wait_at_the_remaining_budget():
    import time
    def slow():
        time.sleep(0.5)
    with tasks.authority_check_budget(seconds=0.05):
        with pytest.raises(ValueError, match='validation timed out'):
            await tasks._bounded_extra_path_check(slow)


async def test_expired_request_budget_refuses_before_submitting():
    def never():
        raise AssertionError('authority read submitted after the request budget expired')
    with tasks.authority_check_budget(seconds=0.0):
        with pytest.raises(ValueError, match='request budget expired'):
            await tasks._bounded_extra_path_check(never)
    # Outside any request budget the same check runs under the flat per-check budget again.
    assert await tasks._bounded_extra_path_check(lambda: None) is None


def test_shared_authority_budget_leaves_headroom_inside_detached_handshake():
    # The shared authority deadline plus the bounded publish-branch and git-baseline
    # probes must fit inside detached's handshake window with at least 5 seconds to
    # spare, so a fully-spent authority budget cannot expire the handshake on its own.
    from polybridge import detached
    assert tasks.EXTRA_PATH_CHECK_BUDGET_SECONDS == 20.0
    spent = (
        tasks.AUTHORITY_CHECK_REQUEST_BUDGET_SECONDS
        + tasks.GIT_PROBE_BUDGET_SECONDS * 2
        + tasks.GIT_BASELINE_BUDGET_SECONDS
        + 0.2  # _spawn's wait_for allowance around the baseline thread
    )
    assert tasks.AUTHORITY_CHECK_REQUEST_BUDGET_SECONDS <= detached.HANDSHAKE_TIMEOUT_SECONDS / 2
    assert detached.HANDSHAKE_TIMEOUT_SECONDS - spent >= 5


async def test_background_request_does_not_reuse_its_creators_expired_budget():
    gate = asyncio.Event()
    seen = []

    @tasks._with_authority_check_budget
    async def later_request():
        seen.append(tasks.authority_check_deadline())
        return await tasks._bounded_extra_path_check(lambda: 'fresh')

    async def background():
        await gate.wait()
        return await later_request()

    with tasks.authority_check_budget(seconds=0):
        expired = tasks.authority_check_deadline()
        child = asyncio.create_task(background())
    gate.set()
    assert await child == 'fresh'
    assert seen[0] > expired
    assert tasks.authority_check_deadline() is None


async def test_request_budget_failcloses_across_individually_in_budget_checks(monkeypatch):
    # Deterministic replacement for the real-sleep version: a loaded runner could take the
    # 0.06 s first check past the whole 0.10 s budget. The deadline is an absolute wall-clock
    # moment that only the checks consult, so a controlled clock plus an intercepted
    # `wait_for` proves the same three facts without any real timing: the first check runs
    # inside the budget, the second is handed the remainder (not a fresh per-check cap) and
    # times out, and the third is refused before submission once the budget has expired.
    import threading
    now = {'monotonic': 1000.0}
    def advance(seconds):
        now['monotonic'] += seconds
    # Only the tasks module's own binding is redirected; stdlib time (and asyncio's use of
    # it) keeps the real clock, so the event loop is untouched.
    monkeypatch.setattr(tasks, 'time', SimpleNamespace(monotonic=lambda: now['monotonic']))
    calls = []
    submitted = threading.Event()
    def first():
        calls.append('first')
        return 'first-ok'
    def second():
        calls.append('second')
        submitted.set()
        return 'second-ok'
    def third():
        raise AssertionError('authority read submitted after the request budget expired')
    timeouts = []
    async def controlled_wait_for(awaitable, timeout=None):
        timeouts.append(timeout)
        if len(timeouts) == 1:
            return await awaitable  # the in-budget first check genuinely completes
        await awaitable  # settle the real worker before injecting the timeout outcome
        raise TimeoutError()  # the remainder handed to the second check has run out
    # Again a module-local binding: asyncio.wait_for itself is never patched globally, so
    # unrelated awaits in the same process are unaffected.
    monkeypatch.setattr(tasks, 'asyncio', SimpleNamespace(
        current_task=asyncio.current_task,
        wrap_future=asyncio.wrap_future,
        wait_for=controlled_wait_for,
    ))
    with tasks.authority_check_budget(seconds=0.10):
        assert await tasks._bounded_extra_path_check(first) == 'first-ok'
        # The first check (and any other startup work) consumed part of the shared allowance.
        advance(0.06)
        with pytest.raises(ValueError, match='Extra writable path validation'):
            await tasks._bounded_extra_path_check(second)
        assert submitted.is_set()
        advance(0.06)  # past the 0.10 s deadline
        with pytest.raises(ValueError, match='request budget expired'):
            await tasks._bounded_extra_path_check(third)
    assert calls == ['first', 'second']
    # The second check received the 0.04 s remainder of the shared budget, not a fresh
    # EXTRA_PATH_CHECK_BUDGET_SECONDS cap, and the first received the full opening budget.
    assert timeouts[0] == pytest.approx(0.10)
    assert timeouts[1] == pytest.approx(0.04)
    assert len(timeouts) == 2


async def test_timeout_retains_real_worker_slot_until_the_worker_finishes(monkeypatch):
    import threading
    import time
    monkeypatch.setattr(tasks, '_EXTRA_PATH_SLOTS', threading.BoundedSemaphore(1))
    def slow():
        time.sleep(0.4)
        return 'slow-ok'
    with tasks.authority_check_budget(seconds=0.05):
        with pytest.raises(ValueError, match='validation timed out'):
            await tasks._bounded_extra_path_check(slow)
    # The abandoned worker still occupies the pool's only slot...
    with pytest.raises(ValueError, match='capacity is exhausted'):
        await tasks._bounded_extra_path_check(lambda: None)
    # ...and releases it once the real work finishes, not before.
    for _ in range(40):
        try:
            assert await tasks._bounded_extra_path_check(lambda: 'later-ok') == 'later-ok'
            break
        except ValueError:
            await asyncio.sleep(0.05)
    else:
        pytest.fail('the abandoned worker never released its slot')


async def test_root_request_sets_and_resets_the_shared_deadline(tmp_path, monkeypatch, backend):
    from unittest.mock import AsyncMock
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path))
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    seen = {}
    def resolve(*args, **kwargs):
        seen['deadline'] = tasks.authority_check_deadline()
        return normalize_paths([str(tmp_path)])
    monkeypatch.setattr(registry, '_resolve_extra_paths', resolve)
    with pytest.raises(ValueError, match='workflow builders'):
        await registry.start('Task', tmp_path, backend=backend, workflow_builder=True, extra_writable_paths=[str(tmp_path)])
    assert seen['deadline'] is not None
    assert tasks.authority_check_deadline() is None


async def test_nested_entry_reuses_the_caller_deadline_and_fails_closed_before_spawn(tmp_path, monkeypatch, backend):
    from unittest.mock import AsyncMock
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    def resolve(*args, **kwargs):
        raise AssertionError('authority read submitted under an expired request budget')
    monkeypatch.setattr(registry, '_resolve_extra_paths', resolve)
    async def launch(*args, **kwargs):
        raise AssertionError('must not launch after the request budget expired')
    monkeypatch.setattr(tasks.asyncio, 'create_subprocess_exec', launch)
    with tasks.authority_check_budget(seconds=0.0):
        with pytest.raises(ValueError, match='request budget expired') as error:
            await registry.start('Task', tmp_path, backend=backend, extra_writable_paths=[str(tmp_path)])
    assert error.value.polybridge_not_started
    assert not (registry.log_dir.exists() and any(registry.log_dir.rglob('*')))


async def test_expired_request_budget_leaves_ordinary_no_extra_dispatch_available(tmp_path, monkeypatch, backend):
    from unittest.mock import AsyncMock
    registry = tasks.TaskRegistry(log_dir=_state_logs(tmp_path), open_monitor=False)
    monkeypatch.setattr(registry, '_mutation_caller', AsyncMock(return_value=None))
    monkeypatch.setattr(tasks, '_branch_disclosure_warranted', lambda _: False)
    with tasks.authority_check_budget(seconds=0.0):
        task = await registry.start('Task', tmp_path, backend=backend)
    await asyncio.wait_for(task.done.wait(), 5)
    assert task.status == 'completed'
