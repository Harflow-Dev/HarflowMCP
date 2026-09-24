"""Runs `polybridge-ctl` in a fresh interpreter for the fork tests in `test_ctl_control.py`.

`run`/`resume` fork. The real CLI forks from a single-threaded process; pytest's own process carries
idle worker threads by then, and forking a multi-threaded process can deadlock the child. So those
tests run this script as a subprocess instead: it registers a fake backend, stubs caller detection,
and calls the real `ctl.main` (mode `ctl`) or `detached.run_detached` with a test action (modes
`hang`, `start_then_hang`), printing the outcome as JSON.

Environment: `PB_FAKE_SCRIPT` (the fake agent's `sh -c` script), `PB_FAKE_CALLER` (a TaskRecord as
JSON to report as the detected caller; unset = none).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from polybridge import backends, ctl, detached, lineage, store
from polybridge.backends import Enforcement, Invocation
from polybridge.tasks import TaskRegistry, default_log_dir


class FakeBackend:
    """Runs `/bin/sh -c <script>` — installed everywhere, and nothing reads its output."""

    name = "fake"
    binary = "/bin/sh"
    capabilities = backends.get("claude").capabilities._replace(
        chooses_session_id=False, supports_live_input=False
    )

    def __init__(self, script: str = "sleep 0.3") -> None:
        self.script = script

    def build_start_argv(self, prompt, **kwargs):
        return Invocation([self.binary, "-c", self.script])

    def build_resume_argv(self, prompt, **kwargs):
        return Invocation([self.binary, "-c", self.script])

    def assert_safe(self, invocation, freedom, network=None):
        assert isinstance(invocation, Invocation)

    def encode_live_message(self, text):
        raise backends.UnsupportedCapability("no live input")

    def interactive_resume_argv(self, session_id, repo_path):
        return [self.binary, "--resume", session_id]

    def enforcement(self, freedom, network=None):
        return Enforcement(freedom=freedom, mechanism="none", os_enforced=False, writes_confined=False)

    def ingest(self, event, acc):
        return None

    def normalize(self, event, acc):
        return []

    def classify(self, acc, exit_code):
        return "completed" if exit_code == 0 else "failed"


def main() -> int:
    fake = FakeBackend(os.environ.get("PB_FAKE_SCRIPT", "sleep 0.3"))
    backends.BACKENDS[fake.name] = fake
    raw_caller = os.environ.get("PB_FAKE_CALLER")
    caller = lineage.Caller(store.TaskRecord(**json.loads(raw_caller)), "pb_task_id") if raw_caller else None
    lineage.detect_caller = lambda *args, **kwargs: caller

    mode, *rest = sys.argv[1:]
    if mode == "ctl":
        return ctl.main(rest)

    timeout = float(rest[0])
    if mode == "hang":

        async def action(registry):
            await asyncio.sleep(3600)

    elif mode == "start_then_hang":
        repo = Path(rest[1])

        async def action(registry):
            await registry.start("hi", repo, backend=fake)
            await asyncio.sleep(3600)

    else:
        raise SystemExit(f"unknown mode {mode!r}")

    log_dir = default_log_dir()
    outcome = detached.run_detached(
        action,
        log_path=log_dir.parent / "ctl.log",
        registry_factory=lambda: TaskRegistry(log_dir=log_dir, open_monitor=False),
        timeout=timeout,
    )
    print(json.dumps({"kind": outcome.kind, "payload": outcome.payload, "child_pid": outcome.child_pid}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
