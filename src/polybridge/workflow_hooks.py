"""Workflow associations at existing dispatch and human-control boundaries."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any


def _tree_gate(log_dir: Path, association: dict[str, Any]) -> dict[str, Any] | None:
    """Root status and settling for the run owning a task; None when unavailable."""
    try:
        from .workflows import WorkflowStore
        store = WorkflowStore(root=log_dir.parent)
        run = store.get_run(association["workflow_run_id"])
        from .workflow_invocation import tree_state
        return tree_state(store, run)
    except (OSError, ValueError, KeyError, TypeError):
        logging.getLogger(__name__).warning("Workflow tree state unavailable for %s", association.get("workflow_run_id"), exc_info=True)
        return None


def owner(log_dir: Path, task_id: str, *, strict: bool = False) -> dict[str, Any] | None:
    from .workflows import WorkflowStore
    return WorkflowStore(root=log_dir.parent).task_owner(task_id, strict=strict)


def pause_for_task(log_dir: Path, task_id: str, reason: str) -> None:
    """Human task control must survive unavailable optional workflow bookkeeping.

    A child run has no public controls of its own: pausing pauses the root.
    """
    try:
        association = owner(log_dir, task_id)
        if association is not None and association.get("status") not in {"completed", "failed", "cancelled", "cancelling"}:
            from .workflows import WorkflowStore
            store = WorkflowStore(root=log_dir.parent)
            run = store.get_run(association["workflow_run_id"])
            link = run.get("parent_link") or {}
            run_id = link.get("root_workflow_run_id") or association.get("workflow_run_id") or association.get("run_id")
            if run_id:
                store.control(run_id, "pause", instructions=reason)
    except (OSError, ValueError, KeyError, TypeError):
        logging.getLogger(__name__).warning("Workflow pause bookkeeping unavailable for %s", task_id, exc_info=True)


def refuse_takeover(log_dir: Path, task_id: str) -> None:
    from . import control
    from .workflows import WorkflowStore, TERMINAL
    try:
        association = owner(log_dir, task_id, strict=True)
        if association is None:
            return
        store = WorkflowStore(root=log_dir.parent)
        run = store.get_run(association["workflow_run_id"])
        if run.get("kind") == "builder" or run["status"] not in TERMINAL or run.get("settling"):
            raise control.TakeoverRefused("workflow_active", "Workflow tasks can only be taken over after the entire workflow has finished and settled")
        state = _tree_gate(log_dir, association)
        if state is not None and (state["root_status"] not in TERMINAL or state["settling"]):
            raise control.TakeoverRefused("workflow_tree_active", "Workflow tasks can only be taken over after the entire workflow tree has finished and settled")
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


def refuse_direct_message(log_dir: Path, task_id: str) -> None:
    """Managed execution assignments can only change through the runner."""
    from .inbox import SendRefused
    from .workflows import TERMINAL
    try:
        association = owner(log_dir, task_id, strict=True)
        if association is None or association.get("role") == "builder":
            return
        if association.get("status") not in TERMINAL or association.get("workflow_settling"):
            raise SendRefused("This task belongs to an active workflow; respond through the workflow caller", code="workflow_active")
        state = _tree_gate(log_dir, association)
        if state is not None and (state["root_status"] not in TERMINAL or state["settling"]):
            raise SendRefused("This task belongs to an active workflow tree; respond through the workflow caller", code="workflow_active")
    except SendRefused:
        raise
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise SendRefused("Cannot establish workflow ownership safely", code="workflow_ownership_unknown") from exc


def refuse_message_caller(log_dir: Path, task_id: str) -> None:
    """Public message entry points cannot let managed agents change assignments."""
    from .inbox import SendRefused
    try:
        if owner(log_dir, task_id, strict=True) is not None:
            raise SendRefused("Workflow-managed agents cannot send direct task messages", code="workflow_managed")
    except SendRefused:
        raise
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise SendRefused("Cannot establish caller workflow ownership safely", code="workflow_ownership_unknown") from exc
