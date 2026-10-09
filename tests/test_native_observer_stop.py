"""Invalid native observation stops its transport without blocking stdout drain."""
import asyncio
import json
import signal
import sys
from dataclasses import replace

import pytest

from polybridge import tasks, store, workflows as w
from test_cancel import make_task, OWNER
from test_workflow_delegation import Task
from test_workflow_native import NativeRegistry, native_graph


async def failing_transport(tmp_path, task_id, backend, event, observer, monkeypatch, *, storage_failure=False):
    monkeypatch.setattr(tasks, "SIGKILL_GRACE_SECONDS", 0.05)
    registry = tasks.TaskRegistry(log_dir=tmp_path, owner=OWNER, open_monitor=False)
    registry._workflow_native_observers = {task_id: observer}
    if storage_failure:
        monkeypatch.setattr(registry, "_deliver_local_cancel", lambda _: (_ for _ in ()).throw(OSError("cancel storage failed")))
    script = "import signal,time\nsignal.signal(signal.SIGTERM, signal.SIG_IGN)\nprint(" + repr(json.dumps(event)) + ", flush=True)\nwhile True: time.sleep(0.01)\n"
    proc = await asyncio.create_subprocess_exec(sys.executable, "-c", script, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True)
    task = make_task(tmp_path, task_id, backend=backend)
    task.proc, task.pgid = proc, proc.pid
    registry._tasks[task_id] = task
    registry.persist(task)
    task.watchers = [asyncio.create_task(tasks._drain_stdout(task, registry)), asyncio.create_task(tasks._drain_stderr(task))]
    task.monitor = asyncio.create_task(tasks._monitor(task, registry))
    try:
        await asyncio.wait_for(task.done.wait(), 2)
        await task.monitor
        await asyncio.wait_for(task.termination, 2)
        assert proc.returncode == -signal.SIGKILL
        assert task.cancel_requested and not task.drain_failed
        assert task.status == "cancelled"
        assert task_id not in registry._workflow_native_observers
        return registry._workflow_native_failures[task_id]
    finally:
        if proc.returncode is None:
            tasks._signal_group(task, signal.SIGKILL)
        await proc.wait()
        await asyncio.gather(task.monitor, *task.watchers, return_exceptions=True)


@pytest.mark.parametrize("backend", ["claude", "codex"])
@pytest.mark.parametrize("storage_failure", [False, True])
async def test_observer_failure_terminates_still_running_transport(tmp_path, monkeypatch, backend, storage_failure):
    def fail(event):
        raise ValueError("Native control evidence rejected")
    failure = await failing_transport(tmp_path, "transport", backend, {"type": "notice"}, fail, monkeypatch, storage_failure=storage_failure)
    assert failure == "Native control evidence rejected"


async def test_unissued_parent_tool_stops_transport_and_keeps_native_uncertain(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda _: True)
    monkeypatch.setattr(w.backends, "version", lambda _: "2.1.295 (Claude Code)")
    storage = w.WorkflowStore(tmp_path)

    class InvalidRegistry(NativeRegistry):
        async def resume(self, previous, prompt, **kwargs):
            if not kwargs.get("native_subagent"):
                return await super().resume(previous, prompt, **kwargs)
            self.native_calls.append(kwargs)
            record = store.read(self._log_dir, previous.task_id)
            event = {"type": "assistant", "session_id": record.session_id, "message": {"content": [{"type": "tool_use", "name": "Bash", "id": "unissued", "input": {"command": "echo unissued"}}]}}
            failure = await failing_transport(self._log_dir, kwargs["task_id"], "claude", event, self._workflow_native_observers[kwargs["task_id"]], monkeypatch)
            self._workflow_native_failures = {kwargs["task_id"]: failure}
            task = Task(kwargs["task_id"], {"status": "cancelled", "summary": "Native transport stopped", "backend": "claude", "session_id": record.session_id})
            task.kwargs, task.repo = previous.kwargs, previous.repo
            self.tasks[task.task_id] = task
            store.write(self._log_dir, replace(record, task_id=task.task_id, status="cancelled"))
            return task

    registry = InvalidRegistry(tmp_path)
    run = storage.create_run(w.validate_definition(native_graph()), "Review", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "needs_attention"
    worker = next(a for a in observed["activations"] if a["role"] == "node")
    assert len(worker["tasks"]) == 1 and worker["tasks"][0]["status"] == "uncertain"
    assert worker["tasks"][0]["execution_kind"] == "native_subagent"
    assert len(registry.native_calls) == 1
    assert all(" · work" not in kwargs.get("title", "") for _, kwargs in registry.calls)
