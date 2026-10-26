"""Test-only hooks for a detached task owner, enabled by putting this directory on PYTHONPATH.

`HARFLOW_TEST_HOOK=<point>:<directory>` makes the owner pause at `<point>` until `<directory>/release`
exists, after writing `<directory>/reached`, so a test can act at an exact moment of the launch that
a real owner passes through in milliseconds. `HARFLOW_TEST_FAIL=rebuild` makes the worktree rebuild
raise; `cancel_at_persist` writes a pending-cancel request right after the first spawn. Nothing happens unless one of the variables is set.
"""
import os
import time

_hook = os.environ.get("HARFLOW_TEST_HOOK")
_fail = os.environ.get("HARFLOW_TEST_FAIL")


def _pause(directory):
    from pathlib import Path
    root = Path(directory)
    (root / "reached").write_text("1")
    deadline = time.monotonic() + 60
    while not (root / "release").exists() and time.monotonic() < deadline:
        time.sleep(0.02)


if _hook:
    point, directory = _hook.split(":", 1)
    if point == "worktree_create":
        from harflow_mcp import worktrees
        _original = worktrees.create

        def _create(*args, **kwargs):
            _pause(directory)
            return _original(*args, **kwargs)

        worktrees.create = _create
    elif point == "baseline":
        from harflow_mcp import task_outcomes
        _baseline = task_outcomes.baseline

        def _slow_baseline(*args, **kwargs):
            _pause(directory)
            return _baseline(*args, **kwargs)

        task_outcomes.baseline = _slow_baseline

if _fail == "rebuild":
    from harflow_mcp.tasks import TaskRegistry

    async def _boom(self, *args, **kwargs):
        raise ValueError("rebuild failed for the test")

    TaskRegistry._rebuild_invocation = _boom

if _fail == "cancel_at_persist":
    from harflow_mcp import task_resources
    from harflow_mcp.tasks import TaskRegistry

    _persist = TaskRegistry.persist
    _requested = []

    def _persist_after_cancel(self, task, *args, **kwargs):
        # The first persist is right after the spawn: a cancel landing here missed the last pre-spawn look.
        if not _requested:
            _requested.append(1)
            task_resources.request_cancel(self._log_dir, task.task_id)
        return _persist(self, task, *args, **kwargs)

    TaskRegistry.persist = _persist_after_cancel
