"""Bounded, read-only observation of a requested task set."""
from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any
from types import SimpleNamespace

from . import store
from .tasks import TERMINAL_STATUSES, TaskRegistry

MAX_TASKS = 32
POLL_SECONDS = 1.0
PROGRESS_SECONDS = 5.0
METADATA_BYTES = 256 * 1024
OBSERVATION_IO_SECONDS = 2.0


def _record_exists(directory, task_id):
    try:
        store.record_path(directory, task_id).stat()
    except FileNotFoundError:
        return False
    return True


def _recovered(directory, task_id):
    record = store.read(directory, task_id, include_prompt=False, metadata_byte_limit=METADATA_BYTES)
    if record is None:
        return None
    header, _, unobserved = store.listing_header(record)
    store._project_indexed_status(header, record)
    status, note = header["status"], header.get("note")
    if unobserved and header.get("process_identity_state") != "alive":
        status, note = "unknown", "Process outcome unobserved; inspect task diagnostics."
    return record, status, note, "recovered_liveness" if unobserved else "recovered_record"



def validate(task_ids: list[str], mode: str, timeout_seconds: int) -> None:
    if not isinstance(task_ids, list) or not 1 <= len(task_ids) <= MAX_TASKS:
        raise ValueError("task_ids must contain between 1 and 32 identifiers")
    if any(not isinstance(value, str) or not value or len(json.dumps(value).encode("utf-8")) > 128 for value in task_ids):
        raise ValueError("task_ids must be nonempty strings of at most 128 JSON-encoded bytes")
    for task_id in task_ids:
        store.validate_task_id(task_id)
    if len(set(task_ids)) != len(task_ids):
        raise ValueError("task_ids must not contain duplicates")
    if mode not in {"any", "all"}:
        raise ValueError("mode must be any or all")
    if type(timeout_seconds) is not int or not 0 <= timeout_seconds <= 300:
        raise ValueError("timeout_seconds must be between 0 and 300")


def _text(value: Any, size: int) -> str | None:
    if value is None:
        return None
    text = str(value)[:size]
    while len(json.dumps(text).encode("utf-8")) > size:
        text = text[: max(0, len(text) - max(1, (len(json.dumps(text).encode("utf-8")) - size) // 6))]
    return text


async def wait_for_tasks(
    registry: TaskRegistry,
    task_ids: list[str],
    mode: str = "any",
    timeout_seconds: int = 30,
    progress: Callable[..., Awaitable[Any]] | None = None,
) -> dict[str, Any]:
    """Observe only: cancellation cleans up our event waiters, never task monitors."""
    validate(task_ids, mode, timeout_seconds)
    effective = min(timeout_seconds, 45)
    loop = asyncio.get_running_loop()
    sources = []
    io_deadline = loop.time() + OBSERVATION_IO_SECONDS
    live_tasks = {task_id: registry.get(task_id) for task_id in task_ids}
    recovered_ids = [task_id for task_id, task in live_tasks.items() if task is None]
    # Complete existence validation before metadata can consume the shared budget.
    # stat errors and timeouts are undecidable, never evidence of a missing ID.
    if recovered_ids:
        try:
            existence = await asyncio.wait_for(
                asyncio.gather(*(asyncio.to_thread(_record_exists, registry.log_dir, task_id)
                                 for task_id in recovered_ids), return_exceptions=True),
                max(0, io_deadline - loop.time()),
            )
        except asyncio.TimeoutError:
            raise ValueError("Unable to validate task existence within the bounded I/O budget; retry.") from None
        for task_id, exists in zip(recovered_ids, existence):
            if isinstance(exists, BaseException):
                raise ValueError(f"Unable to validate task existence for task_id: {task_id}; retry or inspect task diagnostics.") from None
            if not exists:
                raise ValueError(f"unknown task_id: {task_id}")

    def unavailable():
        return (SimpleNamespace(backend=None, session_id=None, title=None), "unavailable", "Bounded metadata observation unavailable; retry or inspect task diagnostics.", "unavailable")

    async def read_recovered(task_id):
        remaining = io_deadline - loop.time()
        try:
            if remaining <= 0:
                raise asyncio.TimeoutError
            return await asyncio.wait_for(asyncio.to_thread(_recovered, registry.log_dir, task_id), remaining)
        except (asyncio.TimeoutError, OSError, ValueError):
            return unavailable()

    # Resolve the complete set before creating observers; no partial wait on missing ids.
    for task_id in task_ids:
        task = live_tasks[task_id]
        record = None if task is not None else await read_recovered(task_id)
        if task is None and record is None:
            record = unavailable()
        sources.append((task_id, task, record))
    # Initial validation/materialization has its own bounded I/O budget; it must
    # not consume the caller's requested observation interval.
    deadline = loop.time() + effective
    next_progress = loop.time() + PROGRESS_SECONDS
    observers = {asyncio.create_task(task.done.wait()) for _, task, _ in sources if task is not None and not task.done.is_set()}
    try:
        while True:
            io_deadline = loop.time() + OBSERVATION_IO_SECONDS
            rows = []
            for task_id, task, record in sources:
                if task is not None:
                    status, summary = task.status, task.acc.summary
                    settling = status in TERMINAL_STATUSES and not task.done.is_set()
                    if settling:
                        status = "running"
                    source = task
                    note = None
                    status_source = "live_settling" if settling else "live_task"
                else:
                    fresh = await read_recovered(task_id)
                    if fresh is None:
                        source, status, note, status_source = record[0], "unavailable", "Task metadata disappeared or became unreadable while waiting.", "unavailable"
                    else:
                        source, status, note, status_source = fresh
                    summary = None
                rows.append({"task_id": task_id, "backend": _text(source.backend, 80), "session_id": _text(source.session_id, 128), "title": _text(source.title, 192), "status": status, "recovered": task is None, "status_source": status_source, "note": _text(note, 192), "summary": _text(summary, 512), "diagnostic_reference": {"tool": "get_task_status", "task_id": task_id}})
            terminal = [row["task_id"] for row in rows if row["status"] in TERMINAL_STATUSES]
            satisfied = bool(terminal) if mode == "any" else len(terminal) == len(rows)
            if satisfied or loop.time() >= deadline:
                return {"mode": mode, "tasks": rows, "terminal_task_ids": terminal, "requested_timeout_seconds": timeout_seconds, "effective_timeout_seconds": effective, "timed_out": not satisfied, "polling_guidance": "Call wait_for_tasks again while tasks remain active; waits never cancel tasks."}
            if progress is not None and loop.time() >= next_progress:
                try:
                    await asyncio.wait_for(progress(len(terminal), total=len(rows), message=f"{len(terminal)}/{len(rows)} requested tasks terminal"), .25)
                except Exception:
                    pass
                next_progress = loop.time() + PROGRESS_SECONDS
            pending = {observer for observer in observers if not observer.done()}
            interval = min(POLL_SECONDS, max(0, deadline - loop.time()))
            if pending:
                await asyncio.wait(pending, timeout=interval, return_when=asyncio.FIRST_COMPLETED)
            else:
                await asyncio.sleep(interval)
    finally:
        for observer in observers:
            observer.cancel()
        if observers:
            await asyncio.gather(*observers, return_exceptions=True)
