"""Bounded, read-only observation of a requested task set."""
from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any
from types import SimpleNamespace

from . import identity, store, wait_policy
from .tasks import TERMINAL_STATUSES, TaskRegistry

MAX_TASKS = 32
POLL_SECONDS = 1.0
METADATA_BYTES = 256 * 1024
OBSERVATION_IO_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 30
FAILURE_CATEGORY_BYTES = 48
FAILURE_REASON_BYTES = 160


def _now() -> float:
    """The clock a wait measures with; a seam so tests can drive time deterministically."""
    return asyncio.get_running_loop().time()


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _record_exists(directory, task_id):
    try:
        store.record_path(directory, task_id).stat()
    except FileNotFoundError:
        return False
    return True


def _owner_alive(record):
    try:
        return identity.identity_check(record.owner) == "alive"
    except Exception:
        return False


def _recovered(directory, task_id):
    record = store.read(directory, task_id, include_prompt=False, metadata_byte_limit=METADATA_BYTES)
    if record is None:
        return None
    header, _, unobserved = store.listing_header(record)
    store._project_indexed_status(header, record)
    status, note = header["status"], header.get("note")
    source = "recovered_liveness" if unobserved else "recovered_record"
    if unobserved and header.get("process_identity_state") != "alive":
        status, note = "unknown", "Process outcome unobserved; inspect task diagnostics."
        # Only a confirmed-dead process is settled for observation; an uncertain identity is not.
        if header.get("process_identity_state") == "dead":
            # Another live polybridge server owning the run is still persisting its outcome;
            # the wait loop promotes this to recovered_dead after a bounded grace.
            source = "recovered_owner_draining" if _owner_alive(record) else "recovered_dead"
    return record, status, note, source



def validate(task_ids: list[str], mode: str, timeout_seconds: int | None, until: str = "timeout") -> None:
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
    wait_policy.WaitPlan.validate(timeout_seconds, until, DEFAULT_TIMEOUT_SECONDS)


def _text(value: Any, size: int) -> str | None:
    if value is None:
        return None
    text = str(value)[:size]
    while len(json.dumps(text).encode("utf-8")) > size:
        text = text[: max(0, len(text) - max(1, (len(json.dumps(text).encode("utf-8")) - size) // 6))]
    return text


def _failure_diagnostic(source: Any) -> dict[str, Any] | None:
    """Category and reason only, bounded; the rest stays behind get_task_status."""
    diagnostic = getattr(source, "failure_diagnostic", None)
    if not isinstance(diagnostic, dict):
        return None
    return {"category": _text(diagnostic.get("category"), FAILURE_CATEGORY_BYTES),
            "reason": _text(diagnostic.get("reason"), FAILURE_REASON_BYTES)}


async def wait_for_tasks(
    registry: TaskRegistry,
    task_ids: list[str],
    mode: str = "any",
    timeout_seconds: int | None = None,
    progress: Callable[..., Awaitable[Any]] | None = None,
    until: str = "timeout",
) -> dict[str, Any]:
    """Observe only: cancellation cleans up our event waiters, never task monitors."""
    validate(task_ids, mode, timeout_seconds, until)
    plan = wait_policy.WaitPlan.validate(timeout_seconds, until, DEFAULT_TIMEOUT_SECONDS)
    sources = []
    io_deadline = _now() + OBSERVATION_IO_SECONDS
    live_tasks = {task_id: registry.get(task_id) for task_id in task_ids}
    recovered_ids = [task_id for task_id, task in live_tasks.items() if task is None]
    # Complete existence validation before metadata can consume the shared budget.
    # stat errors and timeouts are undecidable, never evidence of a missing ID.
    if recovered_ids:
        try:
            existence = await asyncio.wait_for(
                asyncio.gather(*(asyncio.to_thread(_record_exists, registry.log_dir, task_id)
                                 for task_id in recovered_ids), return_exceptions=True),
                max(0, io_deadline - _now()),
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
        remaining = io_deadline - _now()
        try:
            if remaining <= 0:
                raise asyncio.TimeoutError
            return await asyncio.wait_for(asyncio.to_thread(_recovered, registry.log_dir, task_id), remaining)
        except (asyncio.TimeoutError, OSError, ValueError, TypeError):
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
    started = _now()
    deadline = started + plan.effective
    next_progress = started + wait_policy.PROGRESS_INTERVAL_SECONDS
    unavailable_rounds = {task_id: 0 for task_id in task_ids}
    draining_since: dict[str, float] = {}
    observers = {asyncio.create_task(task.done.wait()) for _, task, _ in sources if task is not None and not task.done.is_set()}
    try:
        while True:
            io_deadline = _now() + OBSERVATION_IO_SECONDS
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
                row = {"task_id": task_id, "backend": _text(source.backend, 80), "session_id": _text(source.session_id, 128), "title": _text(source.title, 192), "status": status, "recovered": task is None, "status_source": status_source, "note": _text(note, 192), "summary": _text(summary, 512), "diagnostic_reference": {"tool": "get_task_status", "task_id": task_id}}
                if getattr(source, "caller_approvals", False):
                    from . import approvals
                    row["approval_summary"] = await asyncio.to_thread(approvals.summary, registry.log_dir, task_id)
                diagnostic = _failure_diagnostic(task.acc if task is not None else source)
                if diagnostic is not None:
                    row["failure_diagnostic"] = diagnostic
                if status_source == "recovered_owner_draining":
                    first_seen = draining_since.setdefault(task_id, _now())
                    if _now() - first_seen >= wait_policy.OWNER_DRAIN_GRACE_SECONDS:
                        row["status_source"] = "recovered_dead"
                else:
                    draining_since.pop(task_id, None)
                rows.append(row)
            terminal = [row["task_id"] for row in rows if row["status"] in TERMINAL_STATUSES]
            undecidable = [row["task_id"] for row in rows if row["status_source"] == "recovered_dead"]
            # A process confirmed gone without an observed exit is settled for observation in
            # every mode, exactly as wait_for_task treats it.
            settled = set(terminal) | set(undecidable)
            for row in rows:
                unavailable_rounds[row["task_id"]] = unavailable_rounds[row["task_id"]] + 1 if row["status"] == "unavailable" else 0
            persistent = [row["task_id"] for row in rows if unavailable_rounds[row["task_id"]] >= wait_policy.UNAVAILABLE_ROUNDS]
            unsettled = [row["task_id"] for row in rows if row["task_id"] not in settled]
            satisfied = bool(settled) if mode == "any" else not unsettled
            stop_reason = None
            if any(row.get("approval_summary", {}).get("interaction_state") for row in rows):
                stop_reason = wait_policy.STOP_ATTENTION
            elif satisfied:
                stop_reason = wait_policy.STOP_TERMINAL
            elif plan.terminal_mode and persistent and (mode == "all" or all(task_id in persistent for task_id in unsettled)):
                stop_reason = wait_policy.STOP_UNAVAILABLE
            elif _now() >= deadline:
                stop_reason = plan.expiry_reason
            if stop_reason is not None:
                guidance = None
                if stop_reason == wait_policy.STOP_TERMINAL:
                    gone = [task_id for task_id in undecidable if task_id in settled]
                    others = [task_id for task_id in unsettled]
                    if gone:
                        guidance = (
                            f"Stopped because {len(gone)} requested task(s) have a confirmed-gone process with "
                            "no observed outcome (see undecidable_task_ids); their final status is unknown, so "
                            "inspect them with get_task_status."
                        )
                        if others:
                            guidance += " Other requested tasks are still active; call wait_for_tasks again for them."
                    elif others:
                        guidance = (
                            "Satisfied (mode=any) while other requested tasks are still active; call "
                            "wait_for_tasks again to keep waiting for them. Waits never cancel tasks."
                        )
                return {"mode": mode, "tasks": rows, "terminal_task_ids": terminal, "undecidable_task_ids": undecidable,
                        **wait_policy.result_fields(plan, stop_reason, _now() - started, "wait_for_tasks", "get_task_status", guidance)}
            if progress is not None and _now() >= next_progress:
                await wait_policy.report_progress(progress, len(terminal), len(rows), f"{len(terminal)}/{len(rows)} requested tasks terminal")
                next_progress = _now() + wait_policy.PROGRESS_INTERVAL_SECONDS
            pending = {observer for observer in observers if not observer.done()}
            interval = min(POLL_SECONDS, max(0, deadline - _now()))
            if pending:
                await asyncio.wait(pending, timeout=interval, return_when=asyncio.FIRST_COMPLETED)
            else:
                await _sleep(interval)
    finally:
        for observer in observers:
            observer.cancel()
        if observers:
            await asyncio.gather(*observers, return_exceptions=True)
