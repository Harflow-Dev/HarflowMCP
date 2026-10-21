"""Strict task observations for lifecycle tests that poll concurrent writers."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

from harflow_mcp import store


def read_task_until(
    log_dir: Path,
    task_id: str,
    predicate: Callable[[store.TaskRecord | None], bool],
    *,
    deadline: float,
    interval: float = 0.05,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> store.TaskRecord | None:
    """Return an accepted fresh sample, retrying only concurrent source replacement."""
    while clock() < deadline:
        try:
            record = store.read(log_dir, task_id)
        except store.TaskDocumentError as error:
            if getattr(error, "_source_changed_during_read", None) is not True:
                raise
        else:
            if predicate(record):
                return record
        sleep(interval)
    return None
