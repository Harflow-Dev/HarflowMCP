"""Acceptance coverage for recovering a detached workflow scheduler around a live worker."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import signal
import sys
import time

import pytest

from harflow_mcp import identity, workflows as w


def _definition() -> dict:
    return {
        "name": "detached-recovery",
        "orchestrator": {"backend": "claude"},
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "work", "type": "agent", "role": "task", "instructions": "Do the work", "agent": {"backend": "claude"}},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "work"},
            {"id": "finish", "source": "work", "target": "end"},
        ],
    }


def _install_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    executable = bin_dir / "claude"
    helper = Path(__file__).with_name("fake_workflow_claude.py")
    executable.write_text(f"#!/bin/sh\nexec {sys.executable} {helper} \"$@\"\n", encoding="utf-8")
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")


async def _wait_for(predicate, timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        await asyncio.sleep(0.05)
    raise AssertionError("timed out waiting for detached workflow state")


@pytest.mark.asyncio
async def test_detached_scheduler_recovery_reuses_live_worker_without_duplicate_execution(tmp_path, monkeypatch):
    _install_cli(tmp_path, monkeypatch)
    state = tmp_path / "state"
    ready = tmp_path / "worker-ready.json"
    release = tmp_path / "worker-release"
    monkeypatch.setenv("HARFLOW_WORKFLOW_WORKER_READY", str(ready))
    monkeypatch.setenv("HARFLOW_WORKFLOW_WORKER_RELEASE", str(release))
    monkeypatch.setenv("HARFLOW_FAKE_WORKFLOW_SESSION", "workflow-session")
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(state))
    monkeypatch.setattr(w.backends, "is_installed", lambda _backend: True)

    storage = w.WorkflowStore(state)
    storage.save("detached-recovery", _definition())
    run = await w.start_workflow("detached-recovery", "Finish the requested work", tmp_path, root=state)
    run_id = run["workflow_run_id"]
    scheduler_pid = await _wait_for(lambda: storage.get_run(run_id, refresh_atomic=True).get("supervisor_pid"))
    await _wait_for(lambda: ready.exists())
    worker = json.loads(ready.read_text(encoding="utf-8"))
    worker_pid = worker["pid"]
    assert scheduler_pid != worker_pid
    assert os.kill(worker_pid, 0) is None

    os.kill(scheduler_pid, signal.SIGKILL)
    interrupted = storage.get_run(run_id, refresh_atomic=True)
    await _wait_for(lambda: identity.identity_check(interrupted["supervisor_identity"]) == "dead")
    assert interrupted["status"] in {"running", "starting"}
    assert interrupted["supervisor_pid"] == scheduler_pid

    recovered = storage.control(run_id, "recover", instructions="Confirmed scheduler death; continue the live worker")
    assert recovered["workflow_run_id"] == run_id
    release.touch()
    final = await _wait_for(lambda: storage.get_run(run_id, refresh_atomic=True) if storage.get_run(run_id, refresh_atomic=True)["status"] == "completed" else None)

    worker_tasks = [task for activation in final["activations"] for task in activation["tasks"] if task.get("role") == "node" or activation.get("role") == "node"]
    assert len(worker_tasks) == 1
    assert worker_tasks[0]["status"] == "completed"
    assert final["decisions"]
    assert final.get("scheduler_recovery", {}).get("reason") == "Confirmed scheduler death; continue the live worker"
