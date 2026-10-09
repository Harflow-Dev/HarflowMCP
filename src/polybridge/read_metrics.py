"""Opt-in, content-free timings for workflow read boundaries.

Only fixed stage names and elapsed durations are emitted. Never pass record,
caller, command, prompt, or answer data to this module. Catalog records bootstrap
work; projection includes disk reads and thread scheduling as well as response
projection. Catalog may be nested inside authority or projection, so stage totals
are not additive.
"""
from contextlib import contextmanager
import json
import os
import sys
import time
from typing import Iterator

STAGES = frozenset({"import", "authority", "catalog", "projection", "serialization"})


def enabled() -> bool:
    return os.environ.get("PB_WORKFLOW_READ_METRICS") == "1"


@contextmanager
def span(stage: str) -> Iterator[None]:
    """Measure a fixed stage without affecting the wrapped operation's result."""
    if stage not in STAGES:
        raise ValueError("Unknown workflow read timing stage")
    if not enabled():
        yield
        return
    start = time.perf_counter()
    try:
        yield
    finally:
        try:
            print(json.dumps({"workflow_read_metric_version": 1, "stage": stage,
                              "duration_ms": (time.perf_counter() - start) * 1000}),
                  file=sys.stderr)
        except (OSError, ValueError):
            # Diagnostics must not change an otherwise successful read contract.
            pass
