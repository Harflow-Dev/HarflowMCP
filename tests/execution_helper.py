"""Small bounded execution harnesses used by workflow lifecycle tests."""

from __future__ import annotations

import asyncio
import time
import math
import inspect
import reprlib
from collections import Counter
from itertools import islice
from typing import Any, Awaitable, Callable


CLEANUP_BUDGET_SECONDS = 2.0
SNAPSHOT_ITEMS = 20


def _safe_repr(value: Any) -> str:
    try:
        formatter = reprlib.Repr()
        formatter.maxstring = 500
        formatter.maxother = 500
        formatter.maxlist = formatter.maxtuple = formatter.maxdict = 20
        return formatter.repr(value)[:12000]
    except Exception as exc:
        return "diagnostic formatting failed: " + type(exc).__name__


def _snapshot(task: asyncio.Task[Any], diagnostic: Callable[[], Any] | None) -> str:
    """Collect diagnostics synchronously; a broken diagnostic must not hide the timeout."""
    values: dict[str, Any] = {"task_done": task.done(), "task_cancelled": task.cancelled()}
    if diagnostic is not None:
        try:
            values["execution"] = diagnostic()
        except Exception as exc:  # pragma: no cover - exercised by focused tests
            values["diagnostic_error"] = _safe_repr(exc)
    try:
        frames = task.get_stack(limit=3)
        values["stack"] = [f"{frame.f_code.co_name}:{frame.f_lineno}" for frame in frames]
    except Exception as exc:  # pragma: no cover - defensive diagnostic boundary
        values["stack_error"] = _safe_repr(exc)
    return _safe_repr(values)


async def _bounded_cleanup(task: asyncio.Task[Any]) -> list[str]:
    """Cancel a task with two finite waits and retrieve any finished exception."""
    if task.done():
        try:
            task.result()
        except BaseException:
            pass
        return []
    errors: list[str] = []
    task.cancel()
    done, pending = await asyncio.wait({task}, timeout=CLEANUP_BUDGET_SECONDS)
    if pending:
        task.cancel()
        done, pending = await asyncio.wait({task}, timeout=CLEANUP_BUDGET_SECONDS)
    if pending:
        stacks = []
        for item in pending:
            try:
                stacks.extend(f"{frame.f_code.co_name}:{frame.f_lineno}" for frame in item.get_stack(limit=3))
            except Exception as exc:
                stacks.append(_safe_repr(exc))
        errors.append("execution cleanup incomplete: " + _safe_repr(stacks))
    if done:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except BaseException as exc:
            errors.append("execution raised during cancellation cleanup: " + _safe_repr(exc))
    return errors


async def run_bounded(
    awaitable: Awaitable[Any],
    budget: float,
    *,
    label: str = "execution",
    diagnostic: Callable[[], Any] | None = None,
) -> Any:
    """Run *awaitable* within a finite budget, preserving its real exception type.

    Timeout diagnostics are collected without awaiting. Every exit path performs bounded
    cancellation cleanup, including cancellation of the caller.
    """
    if not math.isfinite(budget) or budget <= 0:
        if inspect.iscoroutine(awaitable):
            awaitable.close()
        raise ValueError("execution budget must be positive and finite")
    task = asyncio.create_task(awaitable, name=f"bounded-{label}")
    started = time.monotonic()
    primary: BaseException | None = None
    try:
        done, pending = await asyncio.wait({task}, timeout=budget)
        if pending:
            elapsed = time.monotonic() - started
            detail = _snapshot(task, diagnostic)
            exc = TimeoutError(f"{label} exceeded {budget:g}s (elapsed {elapsed:.3f}s)")
            exc.add_note(detail)
            primary = exc
            raise exc
        return task.result()
    except BaseException as exc:
        primary = exc
        raise
    finally:
        cleanup_errors = await _bounded_cleanup(task)
        if cleanup_errors and primary is not None:
            for message in cleanup_errors:
                primary.add_note(message)
        elif cleanup_errors:
            raise AssertionError("; ".join(cleanup_errors))


def workflow_snapshot(storage: Any, run_id: str, registry: Any) -> dict[str, Any]:
    """Return a bounded, best-effort workflow snapshot for timeout diagnostics."""
    run = storage.get_run(run_id)
    pending = run.get("pending", [])
    activations = run.get("activations", [])
    joins = run.get("joins", {})
    def identity(item):
        result = {key: item.get(key) for key in ("id", "node_id", "generation", "generation_id", "activation_generation", "execution_activation_id", "visit_id", "status") if key in item}
        for key in ("stack", "branch_ids"):
            if key in item:
                result[key] = collection(item[key])
        tasks = item.get("tasks", [])
        result["task_count"] = len(tasks)
        result["tasks"] = [{key: task.get(key) for key in ("task_id", "status")} for task in tasks[-SNAPSHOT_ITEMS:]]
        return result

    def collection(value):
        if value is None:
            return None
        if isinstance(value, dict):
            return {"count": len(value), "items": list(islice(value.items(), SNAPSHOT_ITEMS))}
        return {"count": len(value), "items": list(value[-SNAPSHOT_ITEMS:])}

    terminal = {"completed", "succeeded", "failed", "cancelled", "timed_out", "skipped"}
    def needs_inspection(item):
        return item.get("status") not in terminal or any(
            task.get("status") in {"reserved", "running", "uncertain"}
            for task in item.get("tasks", [])
        )

    nonterminal = [item for item in activations if needs_inspection(item)]
    selected = nonterminal[:SNAPSHOT_ITEMS]
    if len(selected) < SNAPSHOT_ITEMS:
        selected += [item for item in activations[-SNAPSHOT_ITEMS:] if not needs_inspection(item)][-(SNAPSHOT_ITEMS - len(selected)):]

    return {
        "run_id": run_id,
        "status": run.get("status"),
        "sequence": run.get("sequence"),
        "transitions": collection(run.get("transitions")) if isinstance(run.get("transitions"), (list, dict)) else run.get("transitions"),
        "pending_count": len(pending),
        "pending": [identity(item) for item in pending[:SNAPSHOT_ITEMS]],
        "join_count": len(joins),
        "joins": [{"id": key, **{field: value.get(field) for field in ("join_id", "split_id", "generation", "generation_id", "expected") if field in value}, "arrived_count": len(value.get("arrived", []))} for key, value in islice(joins.items(), SNAPSHOT_ITEMS)],
        "activation_count": len(activations),
        "activation_status_counts": dict(Counter(item.get("status") for item in activations)),
        "nonterminal_activation_count": len(nonterminal),
        "activations": [identity(item) for item in selected],
        "registry_calls": collection(getattr(registry, "calls", None)),
        "contexts": collection(getattr(registry, "contexts", None)),
        "worker_counts": collection(getattr(registry, "worker_counts", None)),
    }
