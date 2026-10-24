"""Fresh-interpreter driver for detached supervision restart probes."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from harflow_mcp.backends import get
from harflow_mcp import identity, store
from harflow_mcp.tasks import TaskRegistry


async def start(root: Path, ready: Path, task_id: str) -> None:
    registry = TaskRegistry(log_dir=root / "state" / "tasks", execution_mode="supervised")
    registry._mutation_caller = lambda: asyncio.sleep(0, result=None)
    task = await registry.start(
        "tool 1 active",
        root,
        backend=get("claude"),
        freedom="read_only",
        task_id=task_id,
        background_idle_seconds=0.1,
    )
    for _ in range(200):
        if task.acc.turn_open:
            break
        await asyncio.sleep(0.05)
    ready.write_text(json.dumps({"task_id": task.task_id, "caller_pid": os.getpid()}))
    await asyncio.sleep(30)

async def start_continuation(root: Path, ready: Path, task_id: str) -> None:
    registry = TaskRegistry(log_dir=root / "state" / "tasks", execution_mode="supervised")
    registry._mutation_caller = lambda: asyncio.sleep(0, result=None)
    task = await registry.start("cap", root, backend=get("claude"), freedom="read_only", auto_continue=1, task_id=task_id)
    ready.write_text(json.dumps({"task_id": task.task_id, "caller_pid": os.getpid()}))
    await asyncio.sleep(30)


async def observe(root: Path, task_id: str) -> None:
    registry = TaskRegistry(log_dir=root / "state" / "tasks", execution_mode="supervised")
    registry._mutation_caller = lambda: asyncio.sleep(0, result=None)
    task = registry.attach(task_id)
    if task is None:
        raise RuntimeError("task was not attachable")
    # The macOS sandbox used for this test can deny ps to the observer. Keep the production
    # identity guard intact, but inject a positive verdict only for the exact recorded owner so
    # this probe still exercises the cross-process inbox path.
    record = store.read(registry.log_dir, task_id)
    owner = record.owner if record is not None else None
    real_identity_check = identity.identity_check
    identity.identity_check = lambda candidate: "alive" if candidate == owner else real_identity_check(candidate)
    print(json.dumps({"status": task.snapshot()["status"], "turn_open": task.acc.turn_open}), flush=True)
    # Attach and input delivery happen in a new interpreter after the original caller is killed.
    queued = await registry.send_message(task, "reply after reconnect")
    for _ in range(200):
        if task.finished:
            break
        await asyncio.sleep(0.05)
    snapshot = task.snapshot()
    print(json.dumps({"queued": queued["status"], "status": snapshot["status"], "summary": snapshot.get("summary"), "output_drained": snapshot.get("output_drained"), "result_count": task.acc.result_count}), flush=True)


def main() -> None:
    mode, root, task_id = sys.argv[1:4]
    if mode == "start":
        asyncio.run(start(Path(root), Path(sys.argv[4]), task_id))
    elif mode == "start_continuation":
        asyncio.run(start_continuation(Path(root), Path(sys.argv[4]), task_id))
    elif mode == "observe":
        asyncio.run(observe(Path(root), task_id))
    else:
        raise SystemExit(f"unknown mode: {mode}")


if __name__ == "__main__":
    main()
