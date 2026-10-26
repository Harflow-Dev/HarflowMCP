"""Runs `ResourceLedger` workloads in a fresh interpreter for the contention tests in `test_resources.py`.

Modes (state root in `HARFLOW_STATE_ROOT`):

* `unique COUNT TAG`: request COUNT distinct names, never releasing, so a lost update is visible.
* `exclusive COUNT MARKER`: acquire the one name `shared`, COUNT times, creating MARKER exclusively
  inside the critical section; a second holder at the same time makes `O_EXCL` fail and is reported.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

from harflow_mcp import resources


def main() -> int:
    ledger = resources.ResourceLedger(os.environ["HARFLOW_STATE_ROOT"])
    mode, count, argument = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    owner = {"kind": "task", "task_id": f"t-{os.getpid()}"}
    if mode == "unique":
        for index in range(count):
            ledger.request([f"{argument}-{index}"], owner)
        return 0
    if mode == "exclusive":
        violations = 0

        async def run() -> None:
            nonlocal violations
            for _ in range(count):
                entry = await ledger.acquire(["shared"], owner, timeout=60, poll_initial=0.01, poll_max=0.05)
                try:
                    descriptor = os.open(argument, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                except FileExistsError:
                    violations += 1
                else:
                    os.close(descriptor)
                    await asyncio.sleep(0.005)
                    os.unlink(argument)
                ledger.release(entry.reservation_id)

        asyncio.run(run())
        print(json.dumps({"violations": violations}))
        return 0
    raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    sys.exit(main())
