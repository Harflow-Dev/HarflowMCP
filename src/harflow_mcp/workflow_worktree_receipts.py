"""Small durable lookup receipts for root workflow worktrees.

These receipts locate paths only; execution and removal authority always comes
from qualified workflow run documents. Orphan lookup receipts remain conservative.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
from typing import Any

from .state_access import task_operation
from .state_compatibility import persisted_compatible
from .wire_contracts import PRODUCT, PRODUCT_VERSION

FORMAT = "harflow.workflow.worktree"
NAMESPACE = "workflow-worktrees"


def _directory(log_dir: Path, *, create: bool = False) -> Path:
    from .state_access import task_layout, require_state_path
    layout = task_layout(log_dir)
    directory = layout.root / NAMESPACE
    require_state_path(layout, directory)
    if create:
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            pass
        else:
            descriptor = os.open(layout.root, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    try:
        info = directory.lstat()
    except FileNotFoundError:
        return directory
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError("Workflow worktree receipt namespace is not a directory")
    return directory


def _document(log_dir: Path, run_id: str, worktree: dict[str, Any], *, canonicalize: bool = True) -> dict[str, Any]:
    from .state_access import task_layout
    from .workflow_run_documents import identifier
    identifier(run_id)
    root = task_layout(log_dir).root
    def qualified_path(value: Any) -> Path:
        if type(value) is not str or not os.path.isabs(value) or "\0" in value:
            raise ValueError("Invalid workflow worktree receipt path")
        if canonicalize:
            return Path(value).resolve()
        # Persisted paths describe the registration-time location. An agent can
        # replace a checkout with a symlink without changing engine metadata.
        if os.path.normpath(value) != value or str(Path(value)) != value:
            raise ValueError("Workflow worktree receipt path is not normalized")
        return Path(value)
    path = qualified_path(worktree["path"])
    worktree_root = qualified_path(worktree["root"])
    source = qualified_path(worktree["source_repo_path"])
    if path != worktree_root / run_id:
        raise ValueError("Workflow worktree path does not match its root and run identity")
    if root == path or root.is_relative_to(path) or path.is_relative_to(root):
        raise ValueError("Workflow worktree path overlaps engine state")
    if source == path or source.is_relative_to(path) or path.is_relative_to(source):
        raise ValueError("Workflow worktree path overlaps source repository")
    return {"product": PRODUCT, "product_version": PRODUCT_VERSION, "format": FORMAT, "v": 1,
            "state_root": str(root),
            "workflow_run_id": run_id, "path": str(path), "worktree_root": str(worktree_root),
            "source_repo_path": str(source)}


def _read(log_dir: Path, path: Path) -> dict[str, Any]:
    from .workflow_receipt_io import read
    def qualify(document: Any, run_id: str) -> dict[str, Any]:
        if type(document) is not dict or document.get("product") != PRODUCT or document.get("format") != FORMAT:
            raise ValueError("Foreign workflow worktree receipt")
        if type(document.get("v")) is not int or document["v"] != 1 or not persisted_compatible(document.get("product_version"), FORMAT):
            raise ValueError("Incompatible workflow worktree receipt")
        if document.get("workflow_run_id") != run_id:
            raise ValueError("Workflow worktree receipt identity mismatch")
        for field in ("state_root", "path", "worktree_root", "source_repo_path"):
            if type(document.get(field)) is not str or not Path(document[field]).is_absolute():
                raise ValueError("Invalid workflow worktree receipt path")
        expected = _document(log_dir, run_id, {"path": document["path"], "root": document["worktree_root"], "source_repo_path": document["source_repo_path"]}, canonicalize=False)
        for field in ("state_root", "workflow_run_id", "path", "worktree_root", "source_repo_path"):
            if document.get(field) != expected[field]:
                raise ValueError("Workflow worktree receipt qualification mismatch: " + field)
        return document
    return read(path, decode_document=qualify)[0]


@task_operation
def register(log_dir: Path, run_id: str, worktree: dict[str, Any]) -> None:
    """Persist lookup before run publication or Git; caller holds this path's usage lock."""
    from .bounded_io import RECEIPT_BYTES
    from .state_access import task_layout
    from .workflows import _write
    document = _document(log_dir, run_id, worktree)
    if len(json.dumps(document).encode()) > RECEIPT_BYTES:
        raise ValueError("Workflow worktree receipt exceeds read bound")
    path = _directory(log_dir, create=True) / (run_id + ".json")
    try:
        existing = _read(log_dir, path)
    except FileNotFoundError:
        existing = None
    if existing is not None:
        if any(existing.get(field) != value for field, value in document.items() if field != "product_version"):
            raise ValueError("Conflicting workflow worktree lookup receipt")
        return
    # The usage lock serializes same-path registration. A run-id conflict on a
    # different path is also checked under the workflow catalog lock by create_run.
    _write(path, document, layout=task_layout(log_dir), before_replace=lambda: _require_absent(path))


def _require_absent(path: Path) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    raise ValueError("Workflow worktree lookup receipt appeared during registration")


@task_operation
def paths(log_dir: Path) -> list[Path]:
    """Read only small lookup receipts, never the workflow catalog or run documents."""
    from .worktrees import WorktreeError
    try:
        directory = _directory(log_dir)
        try:
            entries = list(directory.iterdir())
        except FileNotFoundError:
            return []
        return [Path(_read(log_dir, path)["path"]) for path in sorted(entries) if path.suffix == ".json"]
    except Exception as exc:
        from .state_ownership import StateOwnershipError
        if isinstance(exc, StateOwnershipError):
            raise
        raise WorktreeError("Workflow worktree lookup metadata is unavailable: " + str(exc), code="worktree_lookup_unavailable") from exc
