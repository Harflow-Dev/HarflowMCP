"""Workflow associations at existing dispatch and human-control boundaries."""
from __future__ import annotations

from pathlib import Path
from typing import Any


def owner(log_dir: Path, task_id: str) -> dict[str, Any] | None:
    from .workflows import WorkflowStore
    return WorkflowStore(root=log_dir.parent).task_owner(task_id)


def pause_for_task(log_dir: Path, task_id: str, reason: str) -> None:
    association = owner(log_dir, task_id)
    if association is not None and association.get("status") not in {"completed", "failed", "cancelled"}:
        from .workflows import WorkflowStore
        run_id = association.get("workflow_run_id") or association.get("run_id")
        if run_id:
            WorkflowStore(root=log_dir.parent).control(run_id, "pause", instructions=reason)


def refuse_managed(log_dir: Path, task_id: str) -> None:
    if owner(log_dir, task_id) is not None:
        from .backends.base import NestedDispatchRefused
        raise NestedDispatchRefused(
            "Workflow-managed agents report results to their supervisor; direct dispatch and workflow mutations are refused",
            rule="workflow_managed",
        )
