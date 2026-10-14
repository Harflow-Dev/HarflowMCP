import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from polybridge import store
from polybridge.backends import get
from polybridge.backends.base import Accumulator
from polybridge.task_continuation import context, continue_task, validate_budget


@pytest.mark.parametrize('value', [-1, 11, True, 1.0, '1', None])
def test_budget_refuses_noncanonical(value):
    with pytest.raises(ValueError):
        validate_budget(value)


@pytest.mark.parametrize('value', [0, 1, 10])
def test_budget_valid(value):
    validate_budget(value)


def test_cutoff_is_backend_owned_and_narrow():
    acc = Accumulator(terminal={'subtype': 'error_max_turns'})
    assert get('claude').continuation_reason(acc, 0) == 'turn_cap'
    assert get('claude').continuation_reason(acc, None) is None
    acc.denials.append({'command': 'write'})
    assert get('claude').continuation_reason(acc, 0) is None
    for name in ('codex', 'opencode', 'antigravity'):
        assert get(name).continuation_reason(Accumulator(), 0) is None


def record(tmp_path, **kwargs):
    source = store.TaskRecord(task_id='a' * 32, backend='claude', session_id='old', repo_path=str(tmp_path), started_at='2026-01-01T00:00:00+00:00', prompt='Original  prose\n preserved.', status='completed', exit_code=0, finished_at='2026-01-01T00:01:00+00:00', output_drained=True, **kwargs)
    assert store.write_landed(tmp_path, source)
    return source


class Registry:
    def __init__(self, root):
        self.log_dir = root
        self._mutation_caller = AsyncMock(return_value=None)
        self.start = AsyncMock(side_effect=self.spawn)
        self.children = {}

    async def spawn(self, prompt, path, **kwargs):
        child = SimpleNamespace(task_id=kwargs['task_id'], spawned_by=None, root_task_id=None, depth=0, max_depth=None)
        self.children[child.task_id] = child
        return child

    def get(self, task_id):
        return self.children.get(task_id)

    def persist(self, child):
        pass


@pytest.mark.asyncio
async def test_manual_preserves_settings_and_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path, model='model', reasoning_effort='high', network=False, freedom='read_only', group='group', title='title', spawned_by='b' * 32, root_task_id='c' * 32, depth=2)
    registry = Registry(tmp_path)
    child = await continue_task(registry, source.task_id, 'More  prose\n')
    assert await continue_task(registry, source.task_id, 'More  prose\n') is child
    with pytest.raises(ValueError, match='different caller request'):
        await continue_task(registry, source.task_id)
    assert registry.start.await_count == 1
    args, kwargs = registry.start.call_args
    assert source.prompt in args[0] and 'More  prose\n' in args[0]
    for field in ('model', 'reasoning_effort', 'network', 'freedom', 'group', 'title'):
        assert kwargs[field] == getattr(source, field)
    assert kwargs['continuation_of_task_id'] == source.task_id
    assert kwargs['chain_root_task_id'] == source.task_id
    assert child.spawned_by == source.spawned_by


@pytest.mark.asyncio
async def test_missing_drain_evidence_refuses_old_record(tmp_path, monkeypatch):
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path)
    payload = json.loads(store.record_path(tmp_path, source.task_id).read_text())
    payload.pop('output_drained')
    store.record_path(tmp_path, source.task_id).write_text(json.dumps(payload))
    registry = Registry(tmp_path)
    with pytest.raises(ValueError, match='fully drained'):
        await continue_task(registry, source.task_id)
    registry.start.assert_not_awaited()


@pytest.mark.asyncio
async def test_interrupted_reservation_refuses_duplicate(tmp_path, monkeypatch):
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path)
    (tmp_path / f'{source.task_id}.continuation.json').write_text(json.dumps({'task_id': 'd' * 32, 'state': 'reserved', 'request_fingerprint': __import__('hashlib').sha256(json.dumps({'prompt': context(Registry(tmp_path), source, None, None), 'automatic': False}, ensure_ascii=False).encode()).hexdigest()}))
    registry = Registry(tmp_path)
    with pytest.raises(ValueError, match='unsettled'):
        await continue_task(registry, source.task_id)
    registry.start.assert_not_awaited()


def test_oversized_notes_are_referenced_and_prose_preserved(tmp_path):
    source = record(tmp_path)
    notes = tmp_path / 'notes.txt'
    notes.write_text('x' * 70000)
    prompt = context(Registry(tmp_path), source, None, str(notes))
    assert source.prompt in prompt
    assert str(notes) in prompt
    assert 'x' * 70000 not in prompt


@pytest.mark.asyncio
async def test_concurrent_identical_callers_get_one_successor(tmp_path, monkeypatch):
    import asyncio
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path)
    registry = Registry(tmp_path)
    a, b = await asyncio.gather(continue_task(registry, source.task_id), continue_task(registry, source.task_id))
    assert a is b
    assert registry.start.await_count == 1


@pytest.mark.asyncio
async def test_proven_not_started_can_retry_same_reserved_id(tmp_path, monkeypatch):
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path)
    registry = Registry(tmp_path)
    failure = OSError('no executable')
    failure.polybridge_not_started = True
    registry.start.side_effect = failure
    with pytest.raises(OSError):
        await continue_task(registry, source.task_id)
    reservation = json.loads((tmp_path / f'{source.task_id}.continuation.json').read_text())
    assert reservation['state'] == 'not_started'
    registry.start.side_effect = registry.spawn
    child = await continue_task(registry, source.task_id)
    assert child.task_id == reservation['task_id']


@pytest.mark.asyncio
@pytest.mark.parametrize('legacy', [False, True])
async def test_manual_recovers_failed_automatic_preflight_once(tmp_path, monkeypatch, legacy):
    import asyncio
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path, auto_continue=2)
    registry = Registry(tmp_path)
    done = asyncio.Event()
    done.set()
    registry.children[source.task_id] = SimpleNamespace(done=done, proc=None, cancel_requested=False,
        acc=Accumulator(terminal={'subtype': 'error_max_turns'}), snapshot=lambda: {'summary': 'Stopped on cap'})
    failure = OSError('no executable')
    failure.polybridge_not_started = True
    registry.start.side_effect = failure
    with pytest.raises(OSError):
        await continue_task(registry, source.task_id, automatic=True)
    path = tmp_path / f'{source.task_id}.continuation.json'
    reservation = json.loads(path.read_text())
    assert reservation['state'] == 'not_started'
    if not legacy:
        reservation['automatic'] = True
        path.write_text(json.dumps(reservation))
    else:
        assert 'automatic' not in reservation
    registry.start.side_effect = registry.spawn
    a, b = await asyncio.gather(continue_task(registry, source.task_id), continue_task(registry, source.task_id))
    assert a is b
    assert a.task_id == reservation['task_id']
    assert registry.start.await_count == 2  # one failed preflight, one actual successor
    assert registry.start.call_args.kwargs['auto_continue'] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('changed', ['prose', 'notes', 'uncertain', 'started', 'live_child', 'durable_child'])
async def test_manual_cannot_take_over_different_or_launched_automatic_request(tmp_path, monkeypatch, changed):
    import hashlib
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path, auto_continue=2)
    registry = Registry(tmp_path)
    identifier = 'd' * 32
    fingerprint = hashlib.sha256(json.dumps({'prompt': context(registry, source, None, None), 'automatic': True}, ensure_ascii=False).encode()).hexdigest()
    state = 'reserved' if changed == 'uncertain' else 'started' if changed == 'started' else 'not_started'
    (tmp_path / f'{source.task_id}.continuation.json').write_text(json.dumps({'task_id': identifier, 'state': state, 'request_fingerprint': fingerprint}))
    if changed == 'live_child':
        registry.children[identifier] = SimpleNamespace(task_id=identifier)
    if changed == 'durable_child':
        from dataclasses import replace
        assert store.write_landed(tmp_path, replace(source, task_id=identifier))
    notes = tmp_path / 'notes.txt'
    notes.write_text('Additional notes')
    with pytest.raises(ValueError, match='different caller request'):
        await continue_task(registry, source.task_id, 'Changed prose' if changed == 'prose' else None,
                            str(notes) if changed == 'notes' else None)
    registry.start.assert_not_awaited()


@pytest.mark.asyncio
async def test_uncertain_spawn_failure_cannot_repeat(tmp_path, monkeypatch):
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path)
    registry = Registry(tmp_path)
    registry.start.side_effect = RuntimeError('after process creation')
    with pytest.raises(RuntimeError):
        await continue_task(registry, source.task_id)
    with pytest.raises(ValueError, match='unsettled'):
        await continue_task(registry, source.task_id)
    assert registry.start.await_count == 1


@pytest.mark.asyncio
async def test_special_notes_file_refused_without_blocking(tmp_path):
    import os
    source = record(tmp_path)
    fifo = tmp_path / 'fifo'
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match='regular file'):
        context(Registry(tmp_path), source, None, str(fifo))


def test_vibe_cutoff_requires_exact_live_turn_envelope():
    backend = get('vibe')
    for text, expected in [('<vibe_stop_event>Turn limit of 2 reached</vibe_stop_event>', 'turn_cap'), ('<vibe_stop_event>Token limit of 2 reached</vibe_stop_event>', None), ('Quoted: <vibe_stop_event>Turn limit of 2 reached</vibe_stop_event>', None)]:
        acc = Accumulator()
        backend.ingest({'type': 'message', 'role': 'user', 'sessionId': 'session', 'turnId': 'turn', 'source': 'turn_start', 'content': [{'type': 'text', 'text': 'request'}]}, acc)
        backend.ingest({'type': 'message', 'role': 'assistant', 'sessionId': 'session', 'turnId': 'turn', 'source': None, 'content': [{'type': 'text', 'text': text}]}, acc)
        assert backend.continuation_reason(acc, 1) == expected


@pytest.mark.asyncio
async def test_automatic_decrements_budget_and_requires_cutoff(tmp_path, monkeypatch):
    import asyncio
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path, auto_continue=2)
    registry = Registry(tmp_path)
    done = asyncio.Event()
    done.set()
    source_live = SimpleNamespace(done=done, proc=None, cancel_requested=False, acc=Accumulator(terminal={'subtype': 'error_max_turns'}), snapshot=lambda: {'summary': 'Stopped on cap'})
    registry.children[source.task_id] = source_live
    child = await continue_task(registry, source.task_id, automatic=True)
    assert registry.start.call_args.kwargs['auto_continue'] == 1
    assert child.task_id != source.task_id


@pytest.mark.asyncio
@pytest.mark.parametrize('excluded', ['cancelled', 'denial', 'usage', 'generic', 'zero_budget'])
async def test_automatic_exclusions(tmp_path, monkeypatch, excluded):
    import asyncio
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path, auto_continue=0 if excluded == 'zero_budget' else 2)
    registry = Registry(tmp_path)
    done = asyncio.Event()
    done.set()
    acc = Accumulator(terminal={'subtype': 'error_max_turns'})
    if excluded == 'denial':
        acc.denials.append({'command': 'denied'})
    if excluded == 'usage':
        acc.failure_diagnostic = {'reason': 'limit reached'}
    if excluded == 'generic':
        acc.terminal = {'subtype': 'error'}
    live = SimpleNamespace(done=done, proc=None, cancel_requested=excluded == 'cancelled', acc=acc, snapshot=lambda: {'summary': None})
    registry.children[source.task_id] = live
    with pytest.raises(ValueError):
        await continue_task(registry, source.task_id, automatic=True)
    registry.start.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_before_reserved_spawn_prevents_restart(tmp_path, monkeypatch):
    from polybridge.task_continuation import stop_chain
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path, auto_continue=1)
    registry = Registry(tmp_path)
    await stop_chain(registry, source.task_id)
    with pytest.raises(ValueError, match='chain was cancelled'):
        await continue_task(registry, source.task_id)
    registry.start.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_waits_for_inflight_spawn_then_blocks_successors(tmp_path, monkeypatch):
    import asyncio
    from polybridge.task_continuation import stop_chain
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    source = record(tmp_path, auto_continue=1)
    registry = Registry(tmp_path)
    spawning = asyncio.Event()
    release = asyncio.Event()
    async def spawn(prompt, path, **kwargs):
        spawning.set()
        await release.wait()
        return await registry.spawn(prompt, path, **kwargs)
    registry.start.side_effect = spawn
    continuation = asyncio.create_task(continue_task(registry, source.task_id))
    await spawning.wait()
    cancellation = asyncio.create_task(stop_chain(registry, source.task_id))
    await asyncio.sleep(0.02)
    assert not cancellation.done()
    release.set()
    await continuation
    await cancellation
    with pytest.raises(ValueError, match='chain was cancelled'):
        await continue_task(registry, source.task_id)
    assert registry.start.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('end_to_end', [False, True])
async def test_cancel_waits_for_other_process_first_manual_reservation(tmp_path, monkeypatch, end_to_end):
    import asyncio
    import sys
    from polybridge import control
    from polybridge.task_continuation import stop_chain
    from polybridge.tasks import TaskRegistry
    source = record(tmp_path)  # No automatic budget or reservation yet.
    lock = control.session_lock_path(tmp_path, 'continuation-' + source.task_id)
    lock.parent.mkdir(parents=True, exist_ok=True)
    reservation = tmp_path / f'{source.task_id}.continuation.json'
    script = '''import fcntl,json,sys
from dataclasses import replace
from pathlib import Path
from polybridge import store
with open(sys.argv[1], 'a') as lock:
 fcntl.flock(lock, fcntl.LOCK_EX)
 print('locked', flush=True)
 sys.stdin.readline()
 with open(sys.argv[2], 'w') as file: json.dump({'task_id':'d'*32,'state':'started'},file)
 directory = Path(sys.argv[2]).parent
 source = store.read(directory, 'a'*32)
 assert store.write_landed(directory, replace(source, task_id='d'*32, continuation_of_task_id=source.task_id, chain_root_task_id=source.task_id))
'''
    proc = await asyncio.create_subprocess_exec(sys.executable, '-c', script, str(lock), str(reservation),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
    try:
        assert await proc.stdout.readline() == b'locked\n'
        registry = TaskRegistry(log_dir=tmp_path, open_monitor=False) if end_to_end else Registry(tmp_path)
        observed_targets = []
        if end_to_end:
            triage = registry._triage_target
            def observe_target(identifier):
                observed_targets.append(identifier)
                return triage(identifier)
            monkeypatch.setattr(registry, '_triage_target', observe_target)
            assert registry._cascade_targets(source.task_id, []) == {source.task_id}
        cancellation = asyncio.create_task(registry.cancel_cascade(source.task_id) if end_to_end
                                           else stop_chain(registry, source.task_id))
        await asyncio.sleep(0.05)
        assert not cancellation.done()
        proc.stdin.write(b'continue\n')
        await proc.stdin.drain()
        await proc.wait()
        result = await cancellation
        assert (tmp_path / f'{source.task_id}.continuation-stopped.json').exists()
        assert 'd' * 32 in TaskRegistry._cascade_targets(SimpleNamespace(_log_dir=tmp_path), source.task_id, [])
        if end_to_end:
            assert set(observed_targets) == {source.task_id, 'd' * 32}
            assert result['rounds'] == 2
            assert not result['cascade_incomplete']
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


@pytest.mark.asyncio
async def test_cancel_ordinary_task_does_not_create_chain_stop(tmp_path):
    from polybridge.task_continuation import stop_chain
    source = record(tmp_path)
    await stop_chain(Registry(tmp_path), source.task_id)
    assert not (tmp_path / f'{source.task_id}.continuation-stopped.json').exists()


@pytest.mark.asyncio
async def test_fake_executable_certifies_fresh_cap_successor_and_durable_drain(tmp_path, monkeypatch):
    import asyncio
    import sys
    from polybridge.backends import Invocation
    from polybridge.backends.claude import ClaudeBackend
    from polybridge.tasks import TaskRegistry
    from polybridge.detached import _wait_started
    counter = tmp_path / 'counter'
    class FixtureBackend(ClaudeBackend):
        def build_start_argv(self, prompt, **kwargs):
            session = kwargs['session_id']
            program = f'''import json
from pathlib import Path
p=Path({str(counter)!r})
n=int(p.read_text()) if p.exists() else 0
p.write_text(str(n+1))
print(json.dumps({{"type":"system","subtype":"init","session_id":{session!r}}}))
print(json.dumps({{"type":"result","subtype":"error_max_turns" if n==0 else "success","result":"observed result","session_id":{session!r},"is_error":n==0}}))
'''
            return Invocation([sys.executable, '-c', program])
        def assert_safe(self, *args):
            pass
    backend = FixtureBackend()
    monkeypatch.setattr('polybridge.tasks.get_backend', lambda name: backend)
    monkeypatch.setattr('polybridge.task_continuation.get_backend', lambda name: backend)
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    registry = TaskRegistry(log_dir=tmp_path / 'tasks')
    registry._mutation_caller = AsyncMock(return_value=None)
    source = await registry.start('Preserve  assignment\n', tmp_path, backend=backend, max_turns=1, auto_continue=1)
    await asyncio.wait_for(source.done.wait(), 5)
    await asyncio.wait_for(_wait_started(registry), 5)
    successors = [task for task in registry.list() if task.continuation_of_task_id == source.task_id]
    assert len(successors) == 1
    child = successors[0]
    assert child.session_id != source.session_id
    assert child.status == 'completed'
    assert child.auto_continue == 0
    assert counter.read_text() == '2'
    assert store.read(registry.log_dir, source.task_id).output_drained
    assert store.read(registry.log_dir, child.task_id).output_drained


@pytest.mark.parametrize('exit_code', [None, -9, 137, 1, 255])
def test_claude_cap_rejects_unmeasured_or_signal_exit(exit_code):
    acc = Accumulator(terminal={'subtype': 'error_max_turns'})
    assert get('claude').continuation_reason(acc, exit_code) is None


@pytest.mark.parametrize('unfinished', ['turn_open', 'background_open', 'background_abandoned'])
def test_claude_cap_rejects_later_or_unfinished_work(unfinished):
    acc = Accumulator(terminal={'subtype': 'error_max_turns'})
    setattr(acc, unfinished, {'pending'} if unfinished == 'background_open' else True)
    assert get('claude').continuation_reason(acc, 0) is None


@pytest.mark.parametrize('exit_code', [None, -9, 137, 0, 255])
def test_vibe_cap_rejects_unmeasured_or_signal_exit(exit_code):
    acc = Accumulator()
    acc.stream_state['continuation_turn_cap'] = True
    assert get('vibe').continuation_reason(acc, exit_code) is None


@pytest.mark.parametrize('error', ['is_error', 'error_result_seen'])
def test_vibe_cap_rejects_unrelated_error(error):
    acc = Accumulator()
    acc.stream_state['continuation_turn_cap'] = True
    setattr(acc, error, True)
    assert get('vibe').continuation_reason(acc, 1) is None


@pytest.mark.asyncio
async def test_abandoned_drainer_does_not_establish_complete_evidence(tmp_path, monkeypatch):
    import asyncio
    from datetime import datetime, timezone
    from polybridge.tasks import Task, _finish_draining
    task = Task(task_id='a' * 32, backend='claude', session_id='session', repo_path=tmp_path, prompt='request', max_turns=1, log_path=tmp_path / 'raw.jsonl', started_at=datetime.now(timezone.utc))
    task.watchers = [asyncio.create_task(asyncio.sleep(100))]
    monkeypatch.setattr('polybridge.tasks.DRAIN_GRACE_SECONDS', 0.001)
    assert await _finish_draining(task) is False
    assert not task.output_drained


@pytest.mark.asyncio
@pytest.mark.parametrize('failure_point', ['argv', 'caller', 'lineage', 'assert_safe', 'scratch', 'writable_assert_safe', 'baseline_cancel'])
async def test_real_registry_preflight_failure_retries_reserved_successor(tmp_path, monkeypatch, failure_point):
    import asyncio
    import sys
    from polybridge.backends import Invocation
    from polybridge.backends.claude import ClaudeBackend
    from polybridge.tasks import TaskRegistry

    counter = tmp_path / 'launches'
    class FixtureBackend(ClaudeBackend):
        def build_start_argv(self, prompt, **kwargs):
            program = f"from pathlib import Path; p=Path({str(counter)!r}); p.write_text(str(int(p.read_text())+1) if p.exists() else '1')"
            return Invocation([sys.executable, '-c', program])
        def assert_safe(self, *args):
            pass

    backend = FixtureBackend()
    registry = TaskRegistry(log_dir=tmp_path / 'tasks')
    source = record(registry.log_dir)
    registry._mutation_caller = AsyncMock(return_value=None)
    monkeypatch.setattr('polybridge.task_continuation.get_backend', lambda name: backend)
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    with monkeypatch.context() as fault:
        if failure_point == 'argv':
            fault.setattr(backend, 'build_start_argv', lambda *a, **k: (_ for _ in ()).throw(ValueError('argv')))
        elif failure_point == 'caller':
            registry._mutation_caller = AsyncMock(side_effect=[None, asyncio.CancelledError()])
        elif failure_point == 'lineage':
            fault.setattr(registry, '_resolve_lineage', lambda *a, **k: (_ for _ in ()).throw(ValueError('lineage')))
        elif failure_point == 'assert_safe':
            fault.setattr(backend, 'assert_safe', lambda *a, **k: (_ for _ in ()).throw(ValueError('unsafe')))
        elif failure_point == 'writable_assert_safe':
            from unittest.mock import Mock
            fault.setattr(backend, 'with_writable_directory', lambda invocation, *a: invocation, raising=False)
            fault.setattr(backend, 'assert_safe', Mock(side_effect=[None, ValueError('unsafe writable invocation')]))
        elif failure_point == 'scratch':
            fault.setattr('polybridge.scratch.create', lambda *a: (_ for _ in ()).throw(OSError('scratch')))
        else:
            fault.setattr('polybridge.task_outcomes.baseline', lambda *a: (_ for _ in ()).throw(asyncio.CancelledError()))
        with pytest.raises((ValueError, OSError, asyncio.CancelledError)):
            await registry.continue_task(source.task_id)
    reservation = json.loads((registry.log_dir / f'{source.task_id}.continuation.json').read_text())
    assert reservation['state'] == 'not_started'
    from polybridge.scratch import directory
    assert not directory(registry.log_dir, reservation['task_id']).exists()
    assert not counter.exists()
    registry._mutation_caller = AsyncMock(return_value=None)
    child = await registry.continue_task(source.task_id)
    assert child.task_id == reservation['task_id']
    await asyncio.wait_for(child.done.wait(), 5)
    assert counter.read_text() == '1'
    assert await registry.continue_task(source.task_id) is child


@pytest.mark.asyncio
@pytest.mark.parametrize('failure_point', ['spawn_cancel', 'spawn_uncertain', 'postspawn'])
async def test_real_registry_ambiguous_launch_keeps_reservation(tmp_path, monkeypatch, failure_point):
    import asyncio
    import sys
    from polybridge.backends import Invocation
    from polybridge.backends.claude import ClaudeBackend
    from polybridge.tasks import TaskRegistry

    class FixtureBackend(ClaudeBackend):
        def build_start_argv(self, prompt, **kwargs):
            return Invocation([sys.executable, '-c', 'pass'])
        def assert_safe(self, *args):
            pass
    backend = FixtureBackend()
    registry = TaskRegistry(log_dir=tmp_path / 'tasks')
    source = record(registry.log_dir)
    registry._mutation_caller = AsyncMock(return_value=None)
    monkeypatch.setattr('polybridge.task_continuation.get_backend', lambda name: backend)
    monkeypatch.setattr('polybridge.workflow_hooks.refuse_managed', lambda *args: None)
    real_spawn = asyncio.create_subprocess_exec
    processes = []
    async def spawn(*args, **kwargs):
        if failure_point == 'spawn_cancel':
            raise asyncio.CancelledError()
        if failure_point == 'spawn_uncertain':
            raise RuntimeError('uncertain transport')
        proc = await real_spawn(*args, **kwargs)
        processes.append(proc)
        return proc
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', spawn)
    if failure_point == 'postspawn':
        monkeypatch.setattr(registry, 'persist', lambda task: (_ for _ in ()).throw(RuntimeError('postspawn')))
    try:
        with pytest.raises((RuntimeError, asyncio.CancelledError)):
            await registry.continue_task(source.task_id)
        reservation = json.loads((registry.log_dir / f'{source.task_id}.continuation.json').read_text())
        assert reservation['state'] == 'reserved'
        with pytest.raises(ValueError, match='unsettled'):
            await registry.continue_task(source.task_id)
        assert len(processes) == (1 if failure_point == 'postspawn' else 0)
    finally:
        for proc in processes:
            await proc.communicate()
