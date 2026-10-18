"""Shared `inspect`/`assigned-input` workflow node reads for the MCP server and `polybridge-ctl`.

Both transports resolve managed-caller authority themselves — the server through its
registry-backed reader, ctl through `workflow_inspection.managed_reader` — and hand the resolved
`managed` tuple here, so the per-role refusals, request validation and payload assembly cannot
drift between the two surfaces. Nothing here constructs a registry, so importing this module never
imports `server` (with its MCP stack), never starts retention and never starts the notification
worker.
"""

from __future__ import annotations

from typing import Any


def _guard_inspect(managed: tuple[dict[str, Any], dict[str, Any]] | None, run_id: str) -> None:
    """The per-role refusals the server applies to a managed `inspect` caller."""
    if managed is None:
        return
    association, owned_run = managed
    if association["role"] == "builder":
        if run_id != owned_run["workflow_run_id"]:
            raise ValueError("Builders may only inspect their own workflow run")
        raise ValueError("Builders may only inspect their own draft and builder run")
    if association["role"] != "orchestrator":
        raise ValueError("Worker nodes cannot inspect workflow context")
    if run_id != owned_run["workflow_run_id"]:
        raise ValueError("Orchestrators may only inspect their own workflow run")


def error_payload(exc: Exception) -> tuple[str, dict[str, Any] | None]:
    """The message and data payload both transports report for a refused node read.

    Mirrors `server._workflow_call`'s exception mapping, including `missing_references` data, so
    the CLI reports the same refusal the MCP tool does.
    """
    data = None
    if hasattr(exc, "missing_references"):
        data = {
            "missing_references": exc.missing_references,
            "missing_references_truncated": exc.missing_references_truncated,
        }
    return str(exc), data


def inspect_node(
    managed: tuple[dict[str, Any], dict[str, Any]] | None,
    run_id: str,
    *,
    execution_id: str,
    task_id: str | None = None,
    view: str = "result",
    cursor: str | None = None,
    limit: int | None = None,
    before_seq: int | None = None,
    after_seq: int | None = None,
) -> dict[str, Any]:
    """One settled node execution's result or activity page, for either transport."""
    # The same pre-flight the MCP tool applies, so a bad request is refused before any role check.
    if view == "result" and (before_seq is not None or after_seq is not None):
        raise ValueError("Result view uses cursor, not event sequence cursors")
    _guard_inspect(managed, run_id)
    from .workflow_inspection import inspect_request
    from .workflows import WorkflowStore
    store_ = WorkflowStore()
    run = store_.get_run(run_id)
    request = {
        "execution_id": execution_id,
        "task_id": task_id,
        "view": view,
        "cursor": cursor,
        "limit": limit if limit is not None else (50 if view == "activity" else 16000),
        "before_seq": before_seq,
        "after_seq": after_seq,
    }
    return inspect_request(run, store_.root, request)


def assigned_input_node(
    managed: tuple[dict[str, Any], dict[str, Any]] | None,
    run_id: str,
    execution_id: str,
    *,
    source_run_id: str | None = None,
    task_id: str | None = None,
    cursor: str | None = None,
    limit: int = 16000,
) -> dict[str, Any]:
    """One digest-bound immutable input page, for a verified managed worker on either transport."""
    from .workflow_inspection import assigned_input_page
    from .workflows import WorkflowStore
    return assigned_input_page(
        managed,
        WorkflowStore().root,
        run_id,
        execution_id,
        source_run_id=source_run_id,
        task_id=task_id,
        cursor=cursor,
        limit=limit,
    )


def workflow_artifacts_page(
    managed: tuple[dict[str, Any], dict[str, Any]] | None,
    human: bool,
    log_dir: Any,
    run_id: str,
    *,
    cursor: str | None = None,
    limit: int = 20,
    execution_id: str | None = None,
    task_id: str | None = None,
) -> dict[str, Any]:
    """Run-level artifact reads, shared by the MCP tool and ctl so their authority cannot drift.

    A verified human reads any run. A delegation orchestrator reads its own run and the linked
    descendants of it. Worker nodes and native transports read only their own task through
    ``get_task_artifacts``; standalone task callers, builders and other managed callers are refused.
    """
    from . import artifacts
    if not human:
        if managed is None or managed[0].get("role") != "orchestrator":
            raise ValueError("Run-level artifact reads are available to a human caller or the run's orchestrator")
        _, owned_run = managed
        if run_id != owned_run["workflow_run_id"]:
            from .workflow_inspection import _authorized_linked_run
            _authorized_linked_run(owned_run, log_dir.parent, run_id)
    return artifacts.workflow_page(log_dir, run_id, cursor=cursor, limit=limit, execution_id=execution_id, task_id=task_id)
