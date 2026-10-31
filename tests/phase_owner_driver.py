"""Process-generation fixture for phase-slot owner-death recovery tests."""
from __future__ import annotations

import sys
import time
from pathlib import Path


def main() -> int:
    ready, finish = Path(sys.argv[1]), Path(sys.argv[2])
    deadline = time.monotonic() + 30
    while not ready.exists() and time.monotonic() < deadline:
        time.sleep(.02)
    while not finish.exists() and time.monotonic() < deadline:
        time.sleep(.02)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
