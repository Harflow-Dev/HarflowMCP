"""Linked Git worktrees for tasks: planning, creation, listing and guarded removal.

A task started with `worktree` runs in `<worktree_root>/<task_id>`, a linked worktree of the
repository it named, so it can write without touching the caller's checkout or index. This module
is the part that talks to Git and the filesystem; the launch receipt, the owner and the CLI use it
and nothing here knows about tasks beyond an id. It is written to be reused for a workflow run's
single worktree.

Safety rules, each enforced here rather than hoped for:

* **Location.** `HARFLOW_WORKTREE_ROOT` (default `~/.harflow-worktrees`) must lie outside every
  protected Harflow state root and outside the source repository, in both directions (neither
  contains the other); otherwise the request is refused.
* **No repository code runs.** Git is invoked with `core.hooksPath=/dev/null` and
  `core.fsmonitor=false`, so no hook (including `post-checkout`) and no fsmonitor program executes
  during creation, and a repository that configures clean/smudge/process filter drivers (Git LFS
  included) is refused, because checking out or inspecting such a tree would run them.
* **Nothing is forced.** Never `--force`, never a reset, never a branch deletion. The caller's
  checkout and index are untouched; Git's own refusal to check a branch out in two places stands.
* **Removal is explicit.** Only a person (ctl, humans-only gate) removes a worktree, and only when
  no unsettled or uncertain task uses it and `git status` is clean, under the per-worktree usage
  lock that every launch into the path also takes.
"""
from __future__ import annotations

import errno
import fcntl
import os
import re
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

ROOT_ENV = "HARFLOW_WORKTREE_ROOT"
DEFAULT_ROOT_NAME = ".harflow-worktrees"
LOCK_DIRECTORY = ".locks"
GIT_TIMEOUT_SECONDS = 120

# `core.hooksPath=/dev/null` disables every hook; `core.fsmonitor=false` stops a configured
# monitor program; `GIT_NO_LAZY_FETCH` stops a promisor remote from running a transport helper.
GIT_SAFE_ARGS = ("-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false")
GIT_SAFE_ENV = {"GIT_NO_LAZY_FETCH": "1", "GIT_TERMINAL_PROMPT": "0"}
FILTER_PATTERN = r"^filter\..*\.(process|smudge|clean)$"


class WorktreeError(ValueError):
    """A refused or failed worktree operation; `code` is the stable reason."""

    code = "worktree_error"

    def __init__(self, message: str, *, code: str | None = None, partial_path: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code
        self.partial_path = partial_path


def worktree_root(environ: dict[str, str] | None = None) -> Path:
    """The configured root, not yet validated against a repository."""
    env = os.environ if environ is None else environ
    raw = env.get(ROOT_ENV)
    if raw is None:
        return Path.home() / DEFAULT_ROOT_NAME
    if not raw or "\0" in raw or not os.path.isabs(raw):
        raise WorktreeError(f"{ROOT_ENV} must be a nonempty absolute path", code="worktree_root_invalid")
    return Path(os.path.abspath(raw))


def _canonical(path: Path) -> Path:
    """Resolve what exists and keep the rest lexical, so a not-yet-created root compares correctly."""
    path = Path(os.path.abspath(path))
    existing = path
    tail: list[str] = []
    while not existing.exists() and existing != existing.parent:
        tail.append(existing.name)
        existing = existing.parent
    return Path(os.path.realpath(existing)).joinpath(*reversed(tail))


def _overlaps(a: Path, b: Path) -> bool:
    return a == b or a.is_relative_to(b) or b.is_relative_to(a)


def validate_root(root: Path, source_repo: Path, log_dir: Path) -> Path:
    """The canonical root, or `WorktreeError` when it overlaps protected state or the repository."""
    from .state_layout import protected_roots
    canonical = _canonical(root)
    for protected in protected_roots(log_dir=log_dir):
        if _overlaps(canonical, _canonical(protected)):
            raise WorktreeError(
                f"{ROOT_ENV} ({canonical}) overlaps Harflow state ({protected}); choose a separate directory",
                code="worktree_root_protected")
    if _overlaps(canonical, _canonical(source_repo)):
        raise WorktreeError(
            f"{ROOT_ENV} ({canonical}) overlaps the source repository ({source_repo}); choose a directory outside it",
            code="worktree_root_in_repository")
    return canonical


def validate_spec(spec: Any) -> dict[str, Any]:
    """A normalized `{base_ref, branch, detach}` or `WorktreeError`."""
    if spec is None or spec is False:
        raise WorktreeError("worktree must be an object", code="worktree_invalid")
    if spec is True:
        spec = {}
    if not isinstance(spec, dict) or set(spec) - {"base_ref", "branch", "detach"}:
        raise WorktreeError("worktree accepts only base_ref, branch and detach", code="worktree_invalid")
    base_ref, branch, detach = spec.get("base_ref"), spec.get("branch"), spec.get("detach", False)
    if type(detach) is not bool:
        raise WorktreeError("worktree.detach must be a boolean", code="worktree_invalid")
    for name, value in (("base_ref", base_ref), ("branch", branch)):
        if value is not None and (type(value) is not str or not value or value.startswith("-") or len(value) > 255
                                  or any(ord(c) < 32 or c == "\x7f" for c in value)):
            raise WorktreeError(f"worktree.{name} must be a nonempty ref name that does not start with '-'", code="worktree_invalid")
    if detach and branch is not None:
        raise WorktreeError("worktree.detach and worktree.branch are mutually exclusive", code="worktree_invalid")
    return {"base_ref": base_ref, "branch": branch, "detach": detach}


def plan(task_id: str, source_repo: Path, spec: Any, log_dir: Path, environ: dict[str, str] | None = None) -> dict[str, Any]:
    """The deterministic plan for `task_id`: where the worktree will be and under which name.

    Nothing is created and no Git command runs, so the parent can build the invocation (argv,
    identity markers, working directory) against `path` before the owner makes it exist.
    """
    normalized = validate_spec(spec)
    root = validate_root(worktree_root(environ), source_repo, log_dir)
    branch = None if normalized["detach"] else (normalized["branch"] or f"harflow/{task_id}")
    return {"path": str(root / task_id), "root": str(root), "source_repo_path": str(source_repo),
            "base_ref": normalized["base_ref"], "branch": branch, "detached": normalized["detach"], "state": "planned"}


def is_worktree_path(path: Path, environ: dict[str, str] | None = None) -> bool:
    try:
        return _canonical(Path(path)).parent == _canonical(worktree_root(environ))
    except (WorktreeError, OSError):
        return False


def _git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            ["git", *GIT_SAFE_ARGS, "-C", str(repo), *args],
            env={**os.environ, **GIT_SAFE_ENV}, capture_output=True, text=True,
            timeout=GIT_TIMEOUT_SECONDS, check=False)
    except subprocess.TimeoutExpired:
        raise WorktreeError(f"git {args[0]} timed out", code="worktree_git_timeout") from None
    except OSError as exc:
        raise WorktreeError(f"git is unavailable: {exc}", code="worktree_git_unavailable") from None
    if check and result.returncode != 0:
        raise WorktreeError(f"git {args[0]} failed: {(result.stderr or result.stdout).strip()[:500]}", code="worktree_git_failed")
    return result


def check_filters(repo: Path) -> None:
    """Refuse a repository that configures filter drivers; they would run on checkout and status."""
    result = _git(repo, "config", "--get-regexp", FILTER_PATTERN, check=False)
    if result.returncode == 0 and result.stdout.strip():
        names = sorted({line.split()[0] for line in result.stdout.splitlines() if line.strip()})
        raise WorktreeError(
            "the repository configures Git filter drivers (" + ", ".join(names[:4]) + "), which would run code "
            "while the worktree is checked out; worktrees do not support Git LFS or other filters",
            code="worktree_filters_unsupported")


def create(plan_document: dict[str, Any], *, on_creating=None) -> dict[str, Any]:
    """Create the planned worktree and return the plan with `base_commit` and `state: created`.

    The base ref is resolved here, after any resource grant, so a task that waited for hours is
    based on what the ref names now. `on_creating(plan)` is called after every check passed and
    immediately before the one Git command that writes, so a receipt can say `creating` and a crash
    inside it is reported as `unknown`. Refusals before that raise `WorktreeError` with nothing
    created; a failure of the add itself carries `partial_path` when anything was left behind.
    """
    repo, path = Path(plan_document["source_repo_path"]), Path(plan_document["path"])
    check_filters(repo)
    base_ref = plan_document.get("base_ref") or "HEAD"
    resolved = _git(repo, "rev-parse", "--verify", "--end-of-options", f"{base_ref}^{{commit}}", check=False)
    if resolved.returncode != 0:
        raise WorktreeError(f"worktree base_ref {base_ref!r} does not name a commit in {repo}", code="worktree_base_ref_unknown")
    sha = resolved.stdout.strip()
    branch = plan_document.get("branch")
    if branch is not None:
        if _git(repo, "check-ref-format", "--branch", branch, check=False).returncode != 0:
            raise WorktreeError(f"{branch!r} is not a valid branch name", code="worktree_branch_invalid")
        if _git(repo, "show-ref", "--verify", "--quiet", f"refs/heads/{branch}", check=False).returncode == 0:
            raise WorktreeError(f"branch {branch!r} already exists; choose another name or use detach", code="worktree_branch_exists")
    if os.path.lexists(path):
        raise WorktreeError(f"{path} already exists", code="worktree_path_exists")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if on_creating is not None:
        on_creating({**plan_document, "base_commit": sha, "state": "creating"})
    args = ["worktree", "add", "-b", branch, str(path), sha] if branch is not None else ["worktree", "add", "--detach", str(path), sha]
    try:
        _git(repo, *args)
    except WorktreeError as exc:
        raise WorktreeError(str(exc), code=exc.code, partial_path=str(path) if os.path.lexists(path) else None) from None
    return {**plan_document, "base_commit": sha, "state": "created"}


# ---- usage lock --------------------------------------------------------------------------------


def lock_path(path: Path) -> Path:
    return Path(path).parent / LOCK_DIRECTORY / f"{Path(path).name}.lock"


@contextmanager
def usage_lock(path: Path, *, timeout: float = 10.0) -> Iterator[None]:
    """The per-worktree usage lock: held by removal for its whole decision and by every launch into
    the path just long enough to write its receipt, so a launch can never slip between the
    "nothing uses it" check and the removal. Non-blocking retry, never an indefinite wait."""
    target = lock_path(path)
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(target, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                    raise
                if time.monotonic() >= deadline:
                    raise WorktreeError(f"the usage lock of {path} is held by another operation", code="worktree_busy") from None
                time.sleep(0.01)
        yield
    finally:
        os.close(descriptor)


# ---- listing and removal -----------------------------------------------------------------------


def _same(a: str | None, b: Path) -> bool:
    return a is not None and _canonical(Path(a)) == _canonical(b)


def users(log_dir: Path, path: Path) -> list[dict[str, Any]]:
    """Every task or pending launch that uses `path` and is not provably finished.

    Unsettled and uncertain both count: removal is a control decision, so only a conclusively
    settled run, or a launch that provably never started, stops being a user.
    """
    from . import retention, store
    from .task_supervision import reservations
    found: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in store.read_all(log_dir):
        if not _same(record.repo_path, path):
            continue
        status = store.resolve_status(log_dir, record, detail=False)[0]
        seen.add(record.task_id)
        if not retention._conclusively_settled(record, status):
            found.append({"task_id": record.task_id, "kind": "task", "status": status})
    for receipt in reservations(log_dir):
        if receipt["task_id"] in seen or not _same(receipt.get("repo_path"), path):
            continue
        if receipt["state"] not in ("settled", "not_started"):
            found.append({"task_id": receipt["task_id"], "kind": "launch", "status": receipt["state"]})
    return found


def inventory(log_dir: Path) -> list[dict[str, Any]]:
    """The worktrees Harflow knows of, from launch receipts and task records, with their users."""
    from . import store
    from .task_supervision import reservations
    entries: dict[str, dict[str, Any]] = {}
    for receipt in reservations(log_dir):
        info = receipt.get("worktree")
        if isinstance(info, dict) and info.get("path"):
            entries[info["path"]] = {**info, "task_id": receipt["task_id"], "launch_state": receipt["state"]}
    for record in store.read_all(log_dir):
        info = getattr(record, "worktree", None)
        if isinstance(info, dict) and info.get("path") and record.task_id == Path(info["path"]).name:
            entries.setdefault(info["path"], {**info, "task_id": record.task_id})
    result = []
    for path, entry in sorted(entries.items()):
        entry["exists"] = os.path.isdir(path)
        entry["users"] = users(log_dir, Path(path))
        result.append(entry)
    return result


def remove(log_dir: Path, task_id: str) -> dict[str, Any]:
    """Remove the worktree created for `task_id`, or refuse with the reason. Blocking; humans only.

    Refuses while any user is unsettled or uncertain and while the tree has any tracked or
    untracked change. `git worktree remove` runs without `--force`; the branch stays.
    """
    entry = next((e for e in inventory(log_dir) if e.get("task_id") == task_id), None)
    if entry is None:
        raise WorktreeError(f"no worktree is recorded for task {task_id}", code="worktree_unknown")
    path, source = Path(entry["path"]), Path(entry["source_repo_path"])
    with usage_lock(path):
        busy = users(log_dir, path)
        if busy:
            raise WorktreeError(
                "the worktree is still used by " + ", ".join(f"{u['task_id']} ({u['status']})" for u in busy)
                + "; wait for them to settle or settle them first", code="worktree_in_use")
        if not path.is_dir():
            raise WorktreeError(f"{path} no longer exists; run `git worktree prune` in {source} if Git still lists it", code="worktree_missing")
        status = _git(path, "status", "--porcelain", "--untracked-files=all")
        if status.stdout.strip():
            raise WorktreeError("the worktree has uncommitted or untracked changes; commit, stash or delete them first",
                                code="worktree_dirty")
        if not source.is_dir():
            raise WorktreeError(f"the source repository {source} is unavailable", code="worktree_source_missing")
        _git(source, "worktree", "remove", str(path))
    return {"task_id": task_id, "path": str(path), "removed": True, "branch": entry.get("branch"),
            "note": "The branch, if any, was kept."}
