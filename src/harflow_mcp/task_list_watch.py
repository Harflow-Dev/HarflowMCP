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
import re
import time
from dataclasses import dataclass
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
MAX_REVISION = (1 << 63) - 1
MAX_OPEN_PUBLICATIONS = 64
_ID = re.compile(r"[0-9a-f]{32}\Z")
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
class PublicationToken:
    epoch: str
    pid: int
    operation: str | None


@dataclass(frozen=True)
class RevisionState:
    epoch: str
    revision: int
    open: int
    fence_pids: tuple[int, ...] = ()
    publication_tokens: tuple[PublicationToken, ...] = ()


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
    try:
        cursor_bytes = cursor.encode() if isinstance(cursor, str) else b""
    except UnicodeError as exc:
        raise TaskListWatchCursorInvalid("Cursor is not UTF-8") from exc
    if not isinstance(cursor, str) or not cursor or len(cursor_bytes) > MAX_CURSOR_BYTES:
        raise TaskListWatchCursorInvalid("Cursor must be a UTF-8 string of at most 2048 bytes")
    try:
        payload = json.loads(base64.b64decode(cursor.encode(), altchars=b"-_", validate=True))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise TaskListWatchCursorInvalid("Cursor is not valid base64 JSON") from exc
    if (
        not isinstance(payload, dict)
        or type(payload.get("v")) is not int
        or payload.get("v") != 1
        or payload.get("scope") != CURSOR_SCOPE
        or not isinstance(payload.get("epoch"), str)
        or not _ID.fullmatch(payload["epoch"])
        or type(payload.get("revision")) is not int
        or not 0 <= payload["revision"] <= MAX_REVISION
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
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        opened = os.fstat(fd)
        if not (opened.st_mode & 0o170000) == 0o100000:
            raise StateOwnershipError("task-list revision must be a regular file", reason="observation_unavailable")
        if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
            raise StateOwnershipError("task-list revision changed while opening", reason="observation_unavailable")
        raw = os.read(fd, MAX_DOCUMENT_BYTES + 1)
    finally:
        os.close(fd)
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise StateOwnershipError("task-list revision is oversized", reason="observation_unavailable")
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise StateOwnershipError("task-list revision is invalid", reason="observation_unavailable") from exc
    if not isinstance(value, dict):
        raise StateOwnershipError("task-list revision is incompatible", reason="observation_unavailable")
    fence_pids = value.get("fence_pids")
    tokens = value.get("publication_tokens")
    if "publication_tokens" not in value and type(value.get("open")) is int and 0 <= value["open"] <= MAX_OPEN_PUBLICATIONS:
        tokens = [None] * value["open"]
    if (
        value.get("product") != "harflow" or value.get("format") != FORMAT
        or type(value.get("v")) is not int or value["v"] != 1
        or not isinstance(value.get("epoch"), str) or not _ID.fullmatch(value["epoch"])
        or type(value.get("revision")) is not int or not 0 <= value["revision"] <= MAX_REVISION
        or type(value.get("open")) is not int or not 0 <= value["open"] <= MAX_OPEN_PUBLICATIONS
        or not isinstance(fence_pids, list) or len(fence_pids) != value["open"]
        or any(type(pid) is not int or not 0 < pid <= (1 << 31) - 1 for pid in fence_pids)
        or not isinstance(tokens, list) or len(tokens) != value["open"]
        or any(token is not None and (not isinstance(token, str) or not _ID.fullmatch(token)) for token in tokens)
        or len({token for token in tokens if token is not None}) != sum(token is not None for token in tokens)
    ):
        # Released PID-only fences cannot establish exact publication ownership.
        raise StateOwnershipError("task-list revision is incompatible", reason="observation_unavailable")
    publications = tuple(PublicationToken(value["epoch"], pid, token) for pid, token in zip(fence_pids, tokens))
    return RevisionState(value["epoch"], value["revision"], value["open"], tuple(fence_pids), publications)


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
            "publication_tokens": ([token.operation for token in state.publication_tokens]
                                   if state.publication_tokens else [None] * state.open),
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


def begin_locked(tasks_directory: Path) -> PublicationToken:
    """Pre-bump an exactly owned publication under the catalog lock."""
    current = _ensure_locked(tasks_directory)
    if current.open >= MAX_OPEN_PUBLICATIONS or current.revision + current.open + 2 > MAX_REVISION:
        raise StateOwnershipError("task-list publication capacity exhausted", reason="observation_unavailable")
    token = PublicationToken(current.epoch, os.getpid(), secrets.token_hex(16))
    try:
        _write_unlocked(revision_path(tasks_directory), RevisionState(
            current.epoch, current.revision + 1, current.open + 1,
            current.fence_pids + (token.pid,), current.publication_tokens + (token,)))
    except BaseException:
        # Atomic replacement may have succeeded before directory fsync failed.
        try:
            end_locked(tasks_directory, token)
        except Exception:
            log.warning("Task-list fence setup abort unavailable", exc_info=True)
        raise
    return token


def end_locked(tasks_directory: Path, publication: PublicationToken | None = None) -> None:
    """Close only the explicit matching operation; observations never close fences."""
    if publication is None:
        return
    current = _ensure_locked(tasks_directory)
    if publication.operation is None or publication.pid != os.getpid() or publication not in current.publication_tokens:
        return
    tokens = tuple(token for token in current.publication_tokens if token != publication)
    if current.revision >= MAX_REVISION:
        raise StateOwnershipError("task-list revision capacity exhausted", reason="observation_unavailable")
    _write_unlocked(revision_path(tasks_directory), RevisionState(
        current.epoch, current.revision + 1, len(tokens), tuple(token.pid for token in tokens), tokens))


def begin_publication(tasks_directory: Path, *, layout: StateLayout | None = None) -> PublicationToken:
    from .state_layout import resolve_layout
    selected = layout if layout is not None else resolve_layout()
    assert_owned(selected)
    with catalog.catalog_lock(tasks_directory, layout=selected):
        return begin_locked(tasks_directory)


def end_publication(tasks_directory: Path, publication: PublicationToken | None = None, *, layout: StateLayout | None = None) -> None:
    if publication is None:
        return
    from .state_layout import resolve_layout
    selected = layout if layout is not None else resolve_layout()
    assert_owned(selected)
    with catalog.catalog_lock(tasks_directory, layout=selected):
        end_locked(tasks_directory, publication)


def _snapshot_unavailable_checked(layout: StateLayout, *, deadline: float | None = None) -> tuple[RevisionState, str | None]:
    """Read or establish revision; reclaim only openers whose processes are dead."""
    tasks = layout.tasks_directory
    with catalog.catalog_lock(tasks, layout=layout, deadline=deadline):
        current = _ensure_locked(tasks)
        if current.open == 0:
            return current, None
        live_tokens = tuple(token for token in current.publication_tokens if _pid_alive(token.pid))
        live = tuple(token.pid for token in live_tokens)
        if any(token.operation is None for token in live_tokens):
            raise StateOwnershipError("legacy task-list publication ownership is unresolved", reason="observation_unavailable")
        if len(live) == current.open:
            return current, None
        if live:
            if current.revision >= MAX_REVISION:
                raise StateOwnershipError("task-list revision capacity exhausted", reason="observation_unavailable")
            # Drop dead openers only; keep the live writers' fence count.
            state = RevisionState(current.epoch, current.revision + 1, len(live), live, live_tokens)
            _write_unlocked(revision_path(tasks), state)
            return state, None
        state = RevisionState(epoch=secrets.token_hex(16), revision=0, open=0, fence_pids=())
        _write_unlocked(revision_path(tasks), state)
        return state, REASON_RECLAIMED


def _snapshot(layout: StateLayout, *, deadline: float | None = None) -> tuple[RevisionState, str | None]:
    try:
        return _snapshot_unavailable_checked(layout, deadline=deadline)
    except OSError as exc:
        raise StateOwnershipError("task-list observation infrastructure unavailable", reason=REASON_OBSERVATION_UNAVAILABLE) from exc


def _result(state_name: str, cursor: str | None, *, reason: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"state": state_name, "cursor": cursor}
    if reason is not None:
        result["reason"] = reason
    raw = json.dumps(result, separators=(",", ":"), sort_keys=True).encode()
    if len(raw) > MAX_DOCUMENT_BYTES:
        return {"state": "unavailable", "cursor": None, "reason": REASON_OBSERVATION_UNAVAILABLE}
    return result


def watch(layout: StateLayout, *, cursor: str | None, wait_seconds: int, deadline: float | None = None) -> dict[str, Any]:
    """Establish or wait on a task-list invalidation boundary for the selected root."""
    assert_owned(layout)
    if type(wait_seconds) is not int or not 0 <= wait_seconds <= MAX_WAIT_SECONDS:
        raise ValueError("wait_seconds must be an integer from 0 to 30")
    lock_deadline = deadline if deadline is not None else time.monotonic() + (wait_seconds if cursor is not None else 0) + SETUP_OVERHEAD_SECONDS
    if cursor is None:
        state, reclaim_reason = _snapshot(layout, deadline=lock_deadline)
        encoded = _encode_cursor(layout, state)
        if reclaim_reason is not None:
            return _result("reset", encoded, reason=reclaim_reason)
        return _result("baseline", encoded)

    bound = _decode_cursor(layout, cursor)
    expected_root = root_token(layout)
    wait_deadline = min(lock_deadline, time.monotonic() + wait_seconds)
    while True:
        assert_owned(layout)
        if root_token(layout) != expected_root:
            state, _ = _snapshot(layout, deadline=lock_deadline)
            return _result("reset", _encode_cursor(layout, state), reason=REASON_DISCONTINUITY)
        state, reclaim_reason = _snapshot(layout, deadline=lock_deadline)
        if reclaim_reason is not None or state.epoch != bound.epoch:
            return _result("reset", _encode_cursor(layout, state), reason=reclaim_reason or REASON_EPOCH_CHANGED)
        if state.revision < bound.revision:
            return _result("reset", _encode_cursor(layout, state), reason=REASON_DISCONTINUITY)
        if state.revision > bound.revision or state.open > 0:
            return _result("changed", _encode_cursor(layout, state))
        remaining = wait_deadline - time.monotonic()
        if remaining <= 0:
            return _result("timeout", _encode_cursor(layout, state))
        time.sleep(min(POLL_SECONDS, remaining))


def unavailable(reason: str = REASON_STATE_MISSING) -> dict[str, Any]:
    return _result("unavailable", None, reason=reason)


def env_agent_refusal(environ: Any = None) -> tuple[str, str] | None:
    """Cheap agent-env refusal that never opens the state root."""
    from . import takeover
    return takeover.env_agent_refusal(environ)
