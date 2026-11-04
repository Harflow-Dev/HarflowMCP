"""Bounded public task-list invalidation wait (`task-list-watch.v1`).

Producer-owned fence+revision under the task catalog lock. One ctl invocation returns one
document; there is no daemon, transcript stream or MCP surface. Authority and root qualification
precede cursor decode and watch-source inspection.
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import catalog
from .state_layout import StateLayout
from .state_ownership import StateOwnershipError, assert_owned

log = logging.getLogger(__name__)

FORMAT = "harflow.task-list-revision"
REVISION_NAME = ".task-list-revision.v1.json"
CURSOR_SCOPE = "task-list-watch.v1"
MAX_CURSOR_BYTES = 2048
MAX_DOCUMENT_BYTES = 16 * 1024
MAX_WAIT_SECONDS = 30
DEFAULT_WAIT_SECONDS = 30
POLL_SECONDS = 0.05
# Bounded setup/cancel overhead outside the wait argument (transport margin for consumers).
SETUP_OVERHEAD_SECONDS = 2.0

REASON_EPOCH_CHANGED = "epoch_changed"
REASON_RECLAIMED = "open_reclaimed"
REASON_STATE_MISSING = "state_missing"
REASON_OBSERVATION_UNAVAILABLE = "observation_unavailable"
REASON_DISCONTINUITY = "discontinuity"


class TaskListWatchCursorInvalid(Exception):
    """Malformed or wrong-scope cursor."""

    code = "task_list_watch_cursor_invalid"


@dataclass(frozen=True)
class RevisionState:
    epoch: str
    revision: int
    open: int
    fence_pids: tuple[int, ...] = ()


def root_token(layout: StateLayout) -> str:
    """Opaque selected-root identity; reveals no filesystem path."""
    return hashlib.sha256(str(layout.root.resolve()).encode()).hexdigest()[:32]


def revision_path(tasks_directory: Path) -> Path:
    return tasks_directory / REVISION_NAME


def _encode_cursor(layout: StateLayout, state: RevisionState) -> str:
    payload = {
        "v": 1,
        "scope": CURSOR_SCOPE,
        "root": root_token(layout),
        "epoch": state.epoch,
        "revision": state.revision,
    }
    raw = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()).decode()
    if len(raw.encode()) > MAX_CURSOR_BYTES:
        raise TaskListWatchCursorInvalid("Encoded cursor exceeds 2048 bytes")
    return raw


def _decode_cursor(layout: StateLayout, cursor: str) -> RevisionState:
    if not isinstance(cursor, str) or not cursor or len(cursor.encode()) > MAX_CURSOR_BYTES:
        raise TaskListWatchCursorInvalid("Cursor must be a UTF-8 string of at most 2048 bytes")
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()))
    except (ValueError, json.JSONDecodeError) as exc:
        raise TaskListWatchCursorInvalid("Cursor is not valid base64 JSON") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("v") != 1
        or payload.get("scope") != CURSOR_SCOPE
        or not isinstance(payload.get("epoch"), str)
        or not isinstance(payload.get("revision"), int)
        or payload["revision"] < 0
        or not isinstance(payload.get("root"), str)
    ):
        raise TaskListWatchCursorInvalid("Cursor fields are malformed")
    if payload["root"] != root_token(layout):
        raise TaskListWatchCursorInvalid("Cursor belongs to another selected root")
    return RevisionState(epoch=payload["epoch"], revision=payload["revision"], open=0)


def _read_unlocked(path: Path) -> RevisionState | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if not (info.st_mode & 0o170000) == 0o100000:  # regular file
        raise StateOwnershipError("task-list revision must be a regular file", reason="observation_unavailable")
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        raw = os.read(fd, MAX_DOCUMENT_BYTES + 1)
    finally:
        os.close(fd)
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise StateOwnershipError("task-list revision is oversized", reason="observation_unavailable")
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise StateOwnershipError("task-list revision is invalid", reason="observation_unavailable") from exc
    fence_pids = value.get("fence_pids", ())
    # Accept legacy single fence_pid from earlier drafts of this release candidate.
    if not fence_pids and isinstance(value.get("fence_pid"), int):
        fence_pids = (value["fence_pid"],) if value.get("open", 0) else ()
    if (
        not isinstance(value, dict)
        or value.get("product") != "harflow"
        or value.get("format") != FORMAT
        or value.get("v") != 1
        or not isinstance(value.get("epoch"), str)
        or not isinstance(value.get("revision"), int)
        or value["revision"] < 0
        or not isinstance(value.get("open"), int)
        or value["open"] < 0
        or not isinstance(fence_pids, (list, tuple))
        or any(type(pid) is not int for pid in fence_pids)
        or len(fence_pids) != value["open"]
    ):
        raise StateOwnershipError("task-list revision is incompatible", reason="observation_unavailable")
    return RevisionState(epoch=value["epoch"], revision=value["revision"], open=value["open"], fence_pids=tuple(fence_pids))


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _write_unlocked(path: Path, state: RevisionState) -> None:
    payload = json.dumps(
        {
            "product": "harflow",
            "format": FORMAT,
            "v": 1,
            "epoch": state.epoch,
            "revision": state.revision,
            "open": state.open,
            "fence_pids": list(state.fence_pids),
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode() + b"\n"
    directory = path.parent
    temporary = directory / f".{REVISION_NAME}.{secrets.token_hex(8)}"
    fd = -1
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb", closefd=False) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(temporary, path)
        dir_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        if fd >= 0:
            os.close(fd)
        temporary.unlink(missing_ok=True)
        raise


def _ensure_locked(tasks_directory: Path) -> RevisionState:
    """Return the durable revision, creating a zero baseline when absent."""
    path = revision_path(tasks_directory)
    current = _read_unlocked(path)
    if current is not None:
        return current
    state = RevisionState(epoch=secrets.token_hex(16), revision=0, open=0, fence_pids=())
    _write_unlocked(path, state)
    return state


def begin_locked(tasks_directory: Path) -> None:
    """Pre-bump; caller must hold `catalog_lock` on `tasks_directory`."""
    current = _ensure_locked(tasks_directory)
    pid = os.getpid()
    _write_unlocked(
        revision_path(tasks_directory),
        RevisionState(current.epoch, current.revision + 1, current.open + 1, current.fence_pids + (pid,)),
    )


def end_locked(tasks_directory: Path) -> None:
    """Close one open fence. No-op when nothing is open (observation/blocker catalog writes)."""
    current = _ensure_locked(tasks_directory)
    if current.open <= 0:
        return
    pid = os.getpid()
    pids = list(current.fence_pids)
    if pid in pids:
        pids.remove(pid)
    elif pids:
        pids.pop()
    _write_unlocked(
        revision_path(tasks_directory),
        RevisionState(current.epoch, current.revision + 1, len(pids), tuple(pids)),
    )


def begin_publication(tasks_directory: Path, *, layout: StateLayout | None = None) -> None:
    """Pre-bump before durable task-list publication (catalog invalidate)."""
    from .state_layout import resolve_layout
    selected = layout if layout is not None else resolve_layout()
    assert_owned(selected)
    with catalog.catalog_lock(tasks_directory, layout=selected):
        begin_locked(tasks_directory)


def end_publication(tasks_directory: Path, *, layout: StateLayout | None = None) -> None:
    """Post-bump after Catalog.record/remove; solo-bumps when no fence is open."""
    from .state_layout import resolve_layout
    selected = layout if layout is not None else resolve_layout()
    assert_owned(selected)
    with catalog.catalog_lock(tasks_directory, layout=selected):
        end_locked(tasks_directory)


def _snapshot(layout: StateLayout) -> tuple[RevisionState, str | None]:
    """Read or establish revision; reclaim only openers whose processes are dead."""
    tasks = layout.tasks_directory
    with catalog.catalog_lock(tasks, layout=layout):
        current = _ensure_locked(tasks)
        if current.open == 0:
            return current, None
        live = tuple(pid for pid in current.fence_pids if _pid_alive(pid))
        if len(live) == current.open:
            return current, None
        if live:
            # Drop dead openers only; keep the live writers' fence count.
            state = RevisionState(current.epoch, current.revision + 1, len(live), live)
            _write_unlocked(revision_path(tasks), state)
            return state, None
        state = RevisionState(epoch=secrets.token_hex(16), revision=0, open=0, fence_pids=())
        _write_unlocked(revision_path(tasks), state)
        return state, REASON_RECLAIMED


def _result(state_name: str, cursor: str | None, *, reason: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"state": state_name, "cursor": cursor}
    if reason is not None:
        result["reason"] = reason
    raw = json.dumps(result, separators=(",", ":"), sort_keys=True).encode()
    if len(raw) > MAX_DOCUMENT_BYTES:
        return {"state": "unavailable", "cursor": None, "reason": REASON_OBSERVATION_UNAVAILABLE}
    return result


def watch(layout: StateLayout, *, cursor: str | None, wait_seconds: int) -> dict[str, Any]:
    """Establish or wait on a task-list invalidation boundary for the selected root."""
    assert_owned(layout)
    if cursor is None:
        state, reclaim_reason = _snapshot(layout)
        encoded = _encode_cursor(layout, state)
        if reclaim_reason is not None:
            return _result("reset", encoded, reason=reclaim_reason)
        return _result("baseline", encoded)

    bound = _decode_cursor(layout, cursor)
    expected_root = root_token(layout)
    deadline = time.monotonic() + wait_seconds
    while True:
        assert_owned(layout)
        if root_token(layout) != expected_root:
            state, _ = _snapshot(layout)
            return _result("reset", _encode_cursor(layout, state), reason=REASON_DISCONTINUITY)
        state, reclaim_reason = _snapshot(layout)
        if reclaim_reason is not None or state.epoch != bound.epoch:
            return _result("reset", _encode_cursor(layout, state), reason=reclaim_reason or REASON_EPOCH_CHANGED)
        if state.revision > bound.revision or state.open > 0:
            return _result("changed", _encode_cursor(layout, state))
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return _result("timeout", _encode_cursor(layout, state))
        time.sleep(min(POLL_SECONDS, remaining))


def unavailable(reason: str = REASON_STATE_MISSING) -> dict[str, Any]:
    return _result("unavailable", None, reason=reason)


def env_agent_refusal(environ: Any = None) -> tuple[str, str] | None:
    """Cheap agent-env refusal that never opens the state root."""
    from . import lineage
    env = os.environ if environ is None else environ
    if any(key in env for key in (lineage.ENV_TASK_ID, "PB_TASK_ID", "HARFLOW_DEPTH", "PB_DEPTH")):
        return (
            "agent_caller",
            "HARFLOW_TASK_ID, PB_TASK_ID or a worker depth is set; unset stale HARFLOW_TASK_ID, PB_TASK_ID, HARFLOW_DEPTH and PB_DEPTH in a human shell",
        )
    return None
