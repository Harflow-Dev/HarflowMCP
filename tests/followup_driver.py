"""Fresh-interpreter caller for follow-up hand-off tests: resumes a task with its queued follow-ups,
reports the pending launch, then idles until the test kills it (a caller that goes away mid-wait)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from harflow_mcp import store
from harflow_mcp.tasks import TaskRegistry


async def resume(root: Path, source_id: str, ready: Path) -> None:
    registry = TaskRegistry(log_dir=root / "state" / "tasks", execution_mode="supervised")
    registry._mutation_caller = lambda: asyncio.sleep(0, result=None)
    pending = await registry.resume_record(store.read(registry.log_dir, source_id), None, consume_followups=True)
    ready.write_text(json.dumps({"task_id": pending.task_id, "caller_pid": os.getpid()}))
    await asyncio.sleep(120)


if __name__ == "__main__":
    asyncio.run(resume(Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])))
