"""Fresh-process fixture for genuine task-lineage phase-slot CLI calls."""
from __future__ import annotations

import json
import io
import os
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path

from harflow_mcp import ctl


def main() -> int:
    root = Path(os.environ["HARFLOW_STATE_ROOT"])
    gate = Path(sys.argv[1])
    deadline = time.monotonic() + 10
    while not gate.exists() and time.monotonic() < deadline:
        time.sleep(.01)
    if len(sys.argv) > 2 and sys.argv[2] == "hold":
        tag, stop = sys.argv[3], Path(sys.argv[4])
        def emit(kind: str, view: dict) -> None:
            print(json.dumps({"task": tag, "kind": kind, "lease": view, "at": time.time_ns()}), flush=True)
        def ctl_result(argv: list[str]) -> dict:
            output = io.StringIO()
            with redirect_stdout(output):
                code = ctl.main(argv + ["--json"])
            if code != 0:
                raise RuntimeError(output.getvalue())
            return json.loads(output.getvalue())
        until = time.monotonic() + 20
        view = ctl_result(["slot", "acquire", "fixture", "--wait-seconds", "20"])["result"]
        while view["state"] != "held" and time.monotonic() < until:
            time.sleep(.03)
            view = ctl_result(["slot", "acquire", "fixture", "--wait-seconds", "20"])["result"]
        if view["state"] != "held":
            raise RuntimeError("phase lease did not become held")
        emit("held", view)
        until = time.monotonic() + 30
        while not stop.exists() and time.monotonic() < until:
            time.sleep(.02)
        return 0
    if len(sys.argv) > 2:
        tag = sys.argv[2]
        def emit(kind: str) -> None:
            print(json.dumps({"task": tag, "kind": kind, "at": time.time_ns()}), flush=True)
        def ctl_result(argv: list[str]) -> dict:
            output = io.StringIO()
            with redirect_stdout(output):
                code = ctl.main(argv + ["--json"])
            if code != 0:
                raise RuntimeError(output.getvalue())
            return json.loads(output.getvalue())
        emit("code_start")
        time.sleep(.5)
        emit("code_end")
        view = ctl_result(["slot", "acquire", "fixture", "--wait-seconds", "30"])["result"]
        until = time.monotonic() + 10
        while view["state"] != "held" and time.monotonic() < until:
            time.sleep(.02)
            view = ctl_result(["slot", "acquire", "fixture", "--wait-seconds", "30"])["result"]
        if view["state"] != "held":
            raise RuntimeError("phase lease did not become held")
        emit("heavy_start")
        time.sleep(.2)
        emit("heavy_end")
        ctl_result(["slot", "release", "--lease-id", view["lease_id"]])
        return 0
    try:
        ctl.main(["slot", "acquire", "fixture", "--wait-seconds", "0", "--json"])
    except SystemExit as exc:
        return int(exc.code or 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
