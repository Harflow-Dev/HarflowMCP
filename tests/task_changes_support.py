"""Shared helpers for the `task-changes` capture and reader tests: isolated repositories and records."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

from harflow_mcp import store
from harflow_mcp import task_changes_document as document_shape
from harflow_mcp.wire_contracts import PRODUCT, PRODUCT_VERSION

DATE = "2027-01-01T00:00:00+00:00"
TASK = "changes-task-1"


def pin_git(monkeypatch, home: Path) -> None:
    """A deterministic identity and no inherited configuration for every Git process of the test."""
    home.mkdir(parents=True, exist_ok=True)
    for key in [key for key in os.environ if key.startswith("GIT_")]:
        monkeypatch.delenv(key)
    for key, value in {"HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "GIT_CONFIG_GLOBAL": "/dev/null",
                       "GIT_CONFIG_NOSYSTEM": "1", "GIT_AUTHOR_NAME": "Fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
                       "GIT_COMMITTER_NAME": "Fixture", "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
                       "GIT_AUTHOR_DATE": DATE, "GIT_COMMITTER_DATE": DATE}.items():
        monkeypatch.setenv(key, value)


def git(cwd: Path, *args: str, check: bool = True, input: bytes | None = None) -> str:
    done = subprocess.run(["git", *args], cwd=cwd, check=check, capture_output=True, input=input)
    return done.stdout.decode("utf-8", "replace").strip()


def init_repo(path: Path, *, object_format: str = "sha1") -> Path:
    path.mkdir(parents=True)
    git(path, "init", "-q", "-b", "main", f"--object-format={object_format}")
    return path


def commit(repo: Path, message: str = "change", **files: str | bytes | None) -> str:
    """Write (or with None delete) files, stage everything and commit; returns the new head."""
    for name, content in files.items():
        target = repo / name
        if content is None:
            target.unlink()
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content if isinstance(content, bytes) else content.encode())
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "--allow-empty", "-m", message)
    return head(repo)


def head(repo: Path) -> str:
    return git(repo, "rev-parse", "HEAD")


def entry(repo: Path, base: str | None, head_commit: str | None, *, status: str | None = "", provenance: str = "dispatch",
          worktree: Path | None = None, common: str | None = None) -> dict[str, Any]:
    """An `outcome.repositories[]` entry as `task_outcomes.capture` persists it."""
    return {"repository_root": str(worktree or repo), "worktree": str(worktree or repo),
            "git_common_dir": common or git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir"),
            "base_commit": base, "head_commit": head_commit, "base_provenance": provenance,
            "baseline": None if status is None else {"working_tree_status": status}}


def seed_task(tasks: Path, repositories: list[dict[str, Any]] | None, *, task_id: str = TASK, status: str = "completed",
              exit_code: int | None = 0, finished_at: str | None = "2027-01-01T00:00:10+00:00", **extra: Any) -> store.TaskRecord:
    outcome = None if repositories is None else {"repositories": repositories}
    record = store.TaskRecord(task_id=task_id, backend="claude", session_id="s1", markers=["s1"], repo_path="/repo",
                              started_at="2027-01-01T00:00:00+00:00", status=status, exit_code=exit_code,
                              finished_at=finished_at, outcome=outcome, **extra)
    store.write(tasks, record)
    return store.read(tasks, task_id, include_prompt=False)


def blank_scope(name: str, rows: list[dict[str, Any]], *, state: str = "available", reason: str | None = None,
                truncated: bool = False) -> dict[str, Any]:
    scope = {"state": state, "reason": reason, "row_count": len(rows), "rows_truncated": truncated,
             "observed_at": DATE, "rows": rows}
    if name == "working_tree":
        scope["counts_and_patch_separate"] = True
        scope["unmerged_unknown"] = False
        scope["reported_conflicts"] = {"names": [], "truncated": False}
    return scope


def make_row(path: str | bytes, text: bytes | None = None, *, status: str = "modified", state: str | None = None,
             reason: str | None = None) -> dict[str, Any]:
    import base64
    raw = path if isinstance(path, bytes) else path.encode()
    try:
        shown, encoded = raw.decode("utf-8"), None
    except UnicodeDecodeError:
        shown, encoded = None, base64.b64encode(raw).decode()
    content_state = state or ("available" if text else "not_captured")
    row = {"path": shown, "path_bytes_b64": encoded, "path_truncated": False, "old_path": None, "status": status,
           "additions": 1, "deletions": 0, "binary": False,
           "content": {"state": content_state, "reason": reason, "bytes": len(text) if text else 0}}
    if text:
        row["text_b64"] = base64.b64encode(text).decode()
    return row


def make_repository(root: str, committed: list[dict[str, Any]], working: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {"repository_root": root, "worktree": root, "git_common_dir": root + "/.git", "base_commit": "a" * 40,
            "base_provenance": "dispatch", "head_commit": "b" * 40, "observed_at": DATE, "baseline_dirty": False, "fields_truncated": False,
            "attribution": document_shape.ATTRIBUTION, "state": "available", "reason": None,
            "scopes": {"committed_range": blank_scope("committed_range", committed),
                       "working_tree": blank_scope("working_tree", working or [])}}


def make_document(repositories: list[dict[str, Any]], *, task_id: str = TASK, capture: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"product": PRODUCT, "product_version": PRODUCT_VERSION, "format": document_shape.FORMAT, "v": 1, "task_id": task_id,
            "captured_at": DATE, "capture": capture or {"state": "available", "reason": None}, "repositories": repositories}


def write_document(tasks: Path, document: dict[str, Any], *, task_id: str = TASK) -> Path:
    path = tasks / f"{task_id}{document_shape.SUFFIX}"
    path.write_text(json.dumps(document))
    os.chmod(path, 0o600)
    return path
