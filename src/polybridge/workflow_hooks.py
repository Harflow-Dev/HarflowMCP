"""Workflow associations at existing dispatch and human-control boundaries."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any


def owner(log_dir: Path, task_id: str, *, strict: bool = False) -> dict[str, Any] | None:
    from .workflows import WorkflowStore
    return WorkflowStore(root=log_dir.parent).task_owner(task_id, strict=strict)


def pause_for_task(log_dir: Path, task_id: str, reason: str) -> None:
    """Human task control must survive unavailable optional workflow bookkeeping."""
    try:
        association = owner(log_dir, task_id)
        if association is not None and association.get("status") not in {"completed", "failed", "cancelled", "cancelling"}:
            from .workflows import WorkflowStore
            run_id = association.get("workflow_run_id") or association.get("run_id")
            if run_id:
                WorkflowStore(root=log_dir.parent).control(run_id, "pause", instructions=reason)
    except (OSError, ValueError, KeyError, TypeError):
        logging.getLogger(__name__).warning("Workflow pause bookkeeping unavailable for %s", task_id, exc_info=True)


def refuse_takeover(log_dir: Path, task_id: str) -> None:
    from . import control
    from .workflows import WorkflowStore, TERMINAL
    try:
        association = owner(log_dir, task_id, strict=True)
        if association is None:
            return
        run = WorkflowStore(root=log_dir.parent).get_run(association["workflow_run_id"])
        if run.get("kind") == "builder" or run["status"] not in TERMINAL or run.get("settling"):
            raise control.TakeoverRefused("workflow_active", "Workflow tasks can only be taken over after the entire workflow has finished and settled")
    except control.TakeoverRefused:
        raise
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise control.TakeoverRefused("workflow_ownership_unknown", "Cannot establish workflow ownership safely") from exc


def refuse_managed(log_dir: Path, task_id: str) -> None:
    if owner(log_dir, task_id, strict=True) is not None:
        from .backends.base import NestedDispatchRefused
        raise NestedDispatchRefused(
            "Workflow-managed agents report results to their supervisor; direct dispatch and workflow mutations are refused",
            rule="workflow_managed",
        )
