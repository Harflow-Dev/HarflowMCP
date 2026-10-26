"""Plan metadata contention must not hold stdout or task outcome hostage."""
import asyncio
import json
import shlex
import sys
import threading
from types import SimpleNamespace

import pytest

from harflow_mcp import artifacts, backends
from harflow_mcp.plan_observations import PlanObservations, MAX_FRAMES
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session
from harflow_mcp.tasks import TaskRegistry
from test_claude_plan_files import events
from test_events import _RecordingBackend


async def test_blocked_artifact_write_does_not_block_stdout_or_change_outcome(tmp_path, monkeypatch):
    tmp_path = tmp_path.resolve()
    repo = tmp_path / 'repo'; repo.mkdir()
    config = tmp_path / 'config'
    monkeypatch.setenv('CLAUDE_CONFIG_DIR', str(config))
    plan = config / 'plans/actual.md'; plan.parent.mkdir(parents=True)
    call, response = events(plan)
    terminal = {'type': 'result', 'subtype': 'success', 'is_error': False, 'result': 'drained terminal'}
    rows = [call, response, *({'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': 'x' * 4096}]}} for _ in range(40)), terminal]
    payload = "".join(json.dumps(row) + "\n" for row in rows)
    assert len(payload.encode("utf-8")) >= 160 * 1024
    script = f"from pathlib import Path; Path({str(plan)!r}).write_text('plan'); import sys; sys.stdout.write({payload!r}); sys.stdout.flush()"
    class Backend(_RecordingBackend):
        name = 'plan-contention-double'
        def ingest(self, event, acc):
            backends.get('claude').ingest(event, acc)
        def plan_file_candidates(self, event):
            return backends.get('claude').plan_file_candidates(event)
        def observe_plan_candidates(self, frames, state, repo):
            return backends.get('claude').observe_plan_candidates(frames, state, repo)
        def builtin_plan_path(self, path, repo):
            return backends.get('claude').builtin_plan_path(path, repo)
    # Keep the pipe-drain pressure without exceeding Linux's per-argument limit.
    script_path = tmp_path / 'emit_plan_events.py'
    script_path.write_text(script, encoding='utf-8')
    backend = Backend(shlex.join([sys.executable, str(script_path)]))
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    entered, release, returned = threading.Event(), threading.Event(), threading.Event()
    original = artifacts.record_backend_plan
    def blocked(*args):
        entered.set()
        try:
            assert release.wait(10)
            return original(*args)
        finally:
            returned.set()
    monkeypatch.setattr(artifacts, 'record_backend_plan', blocked)
    with state_session(resolve_layout(root=tmp_path / 'state')):
        registry = TaskRegistry(log_dir=tmp_path / 'state/tasks')
        task = await registry.start('plan', repo, backend=backend, freedom='read_only')
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            await asyncio.wait_for(task.done.wait(), 5)
            assert task.acc.result_count == 1
            assert 'drained terminal' in task.log_path.read_text()
            assert task.status == 'completed' and task.output_drained
            assert any('settlement budget' in notice for notice in task.bridge_notices)
        finally:
            release.set()
            assert await asyncio.to_thread(returned.wait, 3)


async def test_queue_overflow_and_shutdown_are_bounded(tmp_path, monkeypatch):
    notices = []
    backend = SimpleNamespace(plan_file_candidates=lambda event: ({'id': str(event), 'path': '/plans/p.md'},))
    worker = PlanObservations(backend, tmp_path, tmp_path, 'queue', notices.append)
    # Queue admission is synchronous: a burst cannot wait on filesystem work or a worker turn.
    for index in range(MAX_FRAMES + 10):
        worker.enqueue(index)
    assert worker.queue.qsize() == MAX_FRAMES
    assert any('queue is full' in notice for notice in notices)
    await worker.finish(timeout=0)
    assert worker.queue.empty() and worker.bytes == 0
    assert worker.worker.done()


async def test_non_candidates_never_start_thread_worker(tmp_path, monkeypatch):
    worker = PlanObservations(backends.get('claude'), tmp_path, tmp_path, 'plain', lambda _: None)
    def forbidden(*args, **kwargs):
        pytest.fail('non-candidate event dispatched thread work')
    monkeypatch.setattr(asyncio, 'to_thread', forbidden)
    for _ in range(100):
        worker.enqueue({'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': 'hello'}]}})
    assert worker.worker is None
    await worker.finish()
