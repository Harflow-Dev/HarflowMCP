"""The `harflow-ctl task-changes` result: a bounded view of a task's stored repository differences.

Two modes share one cursor format. Listing mode returns the capture state, every repository's
provenance and scope metadata, and a page of rows (content state and byte size, no text). Text mode
returns one chunk of the stored patch of one row. Authority and id resolution are decided by the
caller (`ctl`), as for `task-events`; these functions take the resolved record and raise typed
refusals carrying the stable wire codes.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import stat as stat_module
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, NamedTuple

from . import identity, store
from . import task_changes_document as document_shape
from .state_access import open_descriptor, task_layout
from .task_event_read import SETTLE_GRACE_SECONDS
from .wire_contracts import CTL_FORMAT, WIRE_VERSION, envelope

GENERATION = "py-changes-1"
DEFAULT_LIMIT = 200
MAX_LIMIT = 500
DOCUMENT_MAX_BYTES = 262_144
FIT_RESERVE_BYTES = 512
CURSOR_CHARS = 2048
SCOPE_ORDER = document_shape.SCOPES


class ChangesRefusal(Exception):
    """A refusal with a stable wire `code` and optional `data`."""

    code = "changes_refused"

    def __init__(self, message: str, data: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.data = data


class ChangesCursorInvalid(ChangesRefusal):
    code = "changes_cursor_invalid"


class ChangesCursorExpired(ChangesRefusal):
    code = "changes_cursor_expired"

    def __init__(self, message: str) -> None:
        super().__init__(message, {"reload": "first"})


class ChangesEntryNotFound(ChangesRefusal):
    code = "changes_entry_not_found"


class Selector(NamedTuple):
    """The row addressed by `--repository-root`, `--scope` and `--path-b64` (already decoded)."""

    repository_root: str
    scope: str
    path: bytes


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _document_bytes(result: dict[str, Any]) -> int:
    """The size of the bytes `harflow-ctl --json` prints for this result."""
    return len(json.dumps(envelope(CTL_FORMAT, {"v": WIRE_VERSION, "result": result}))) + 1


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


# --- stored document --------------------------------------------------------------------------


@dataclass
class Stored:
    state: str  # document | missing | invalid
    document: dict[str, Any] | None = None
    digest: str | None = None


def load_document(log_dir: Path, task_id: str) -> Stored:
    """Read and strictly validate the stored document without following a final symlink."""
    path = log_dir / (store.validate_task_id(task_id) + document_shape.SUFFIX)
    try:
        descriptor = open_descriptor(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, layout=task_layout(log_dir))
    except FileNotFoundError:
        return Stored("missing")
    except OSError:
        return Stored("invalid")
    try:
        if not stat_module.S_ISREG(os.fstat(descriptor).st_mode):
            return Stored("invalid")
        chunks: list[bytes] = []
        total = 0
        while total <= document_shape.MAX_DOCUMENT_BYTES:
            chunk = os.read(descriptor, min(1_048_576, document_shape.MAX_DOCUMENT_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        if total > document_shape.MAX_DOCUMENT_BYTES:
            return Stored("invalid")
        data = b"".join(chunks)
    except OSError:
        return Stored("invalid")
    finally:
        descriptor.close()
    try:
        document = json.loads(data)
        document_shape.validate(document, task_id)
    except (ValueError, RecursionError, UnicodeError):
        return Stored("invalid")
    return Stored("document", document, _digest(data))


# --- capture state when no document exists ----------------------------------------------------


def _within_grace(finished_at: str | None) -> bool:
    try:
        finished = datetime.fromisoformat(finished_at) if finished_at else None
    except ValueError:
        return False
    if finished is None:
        return False
    finished = finished if finished.tzinfo else finished.replace(tzinfo=timezone.utc)
    return 0 <= (_now() - finished).total_seconds() < SETTLE_GRACE_SECONDS


def _supervisor_owner(log_dir: Path, task_id: str) -> str:
    """`alive`, `dead` or `undecidable` for the supervisor named by the launch receipt (absent: undecidable)."""
    from .task_supervision import read_launch
    try:
        receipt = read_launch(log_dir, task_id)
    except Exception:
        return "undecidable"
    owner = receipt.get("owner") if isinstance(receipt, dict) else None
    if not isinstance(owner, dict):
        return "undecidable"
    return identity.identity_check(owner)


def derive_capture_state(log_dir: Path, record: store.TaskRecord) -> dict[str, Any]:
    """`{state, reason}` for a task whose changes document does not exist, from the evidence available."""
    if record.status not in store.TERMINAL_RECORD_STATUSES:
        return {"state": "pending", "reason": "task_running"}
    if store.outcome_unobserved(record):
        return {"state": "not_captured", "reason": "outcome_unobserved"}
    repositories = (record.outcome or {}).get("repositories")
    if not isinstance(repositories, list) or not repositories:
        return {"state": "available", "reason": "no_repository"}
    owner = _supervisor_owner(log_dir, record.task_id)
    if owner == "dead":
        return {"state": "not_captured", "reason": "capture_missing"}
    if owner == "alive":
        return {"state": "pending", "reason": "capture_settling"}
    if _within_grace(record.finished_at):
        return {"state": "pending", "reason": "capture_settling"}
    return {"state": "not_captured", "reason": "capture_missing"}


def _capture_of(log_dir: Path, record: store.TaskRecord, stored: Stored) -> dict[str, Any]:
    if stored.state == "document":
        return dict(stored.document["capture"])
    if stored.state == "invalid":
        return {"state": "unavailable", "reason": "changes_document_invalid"}
    return derive_capture_state(log_dir, record)


# --- cursors ----------------------------------------------------------------------------------


def _encode_cursor(mode: str, task_id: str, digest: str, entry: tuple[int, int, int], offset: int) -> str:
    payload = {"g": GENERATION, "m": mode, "t": _digest(task_id.encode()), "d": digest, "e": list(entry), "o": offset}
    return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode().rstrip("=")


@dataclass
class _Cursor:
    mode: str
    digest: str
    entry: tuple[int, int, int]
    offset: int


def parse_cursor(value: str, task_id: str) -> _Cursor:
    """Raises `ChangesCursorInvalid` for a malformed cursor or one issued for another task."""
    if not isinstance(value, str) or not value or len(value) > CURSOR_CHARS:
        raise ChangesCursorInvalid("Malformed cursor")
    try:
        payload = json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
    except (binascii.Error, ValueError, UnicodeError):
        raise ChangesCursorInvalid("Malformed cursor") from None
    if not isinstance(payload, dict) or set(payload) != {"g", "m", "t", "d", "e", "o"}:
        raise ChangesCursorInvalid("Malformed cursor")
    entry = payload["e"]
    if not (isinstance(payload["g"], str) and payload["m"] in ("list", "text") and isinstance(payload["t"], str)
            and isinstance(payload["d"], str) and isinstance(entry, list) and len(entry) == 3
            and all(type(part) is int and part >= 0 for part in entry) and type(payload["o"]) is int and payload["o"] >= 0):
        raise ChangesCursorInvalid("Malformed cursor")
    if payload["t"] != _digest(task_id.encode()):
        raise ChangesCursorInvalid("Cursor belongs to another task")
    if payload["g"] != GENERATION:
        raise ChangesCursorExpired("Cursor was issued by another generation")
    return _Cursor(payload["m"], payload["d"], (entry[0], entry[1], entry[2]), payload["o"])


# --- listing ----------------------------------------------------------------------------------


def _row_view(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key != "text_b64"}


def _positions(document: dict[str, Any], start: tuple[int, int, int]) -> Iterator[tuple[tuple[int, int, int], dict[str, Any]]]:
    repository_index, scope_index, row_index = start
    for r in range(repository_index, len(document["repositories"])):
        scopes = document["repositories"][r]["scopes"]
        for s in range(scope_index if r == repository_index else 0, len(SCOPE_ORDER)):
            rows = scopes[SCOPE_ORDER[s]]["rows"]
            for i in range(row_index if (r, s) == (repository_index, scope_index) else 0, len(rows)):
                yield (r, s, i), rows[i]


def _valid_start(document: dict[str, Any], entry: tuple[int, int, int]) -> bool:
    r, s, i = entry
    if r >= len(document["repositories"]) or s >= len(SCOPE_ORDER):
        return False
    return i < len(document["repositories"][r]["scopes"][SCOPE_ORDER[s]]["rows"])


def _repositories_view(document: dict[str, Any], included: list[tuple[tuple[int, int, int], dict[str, Any]]]) -> list[dict[str, Any]]:
    paged: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for (r, s, _), row in included:
        paged.setdefault((r, s), []).append(_row_view(row))
    view = []
    for r, repository in enumerate(document["repositories"]):
        scopes = {}
        for s, name in enumerate(SCOPE_ORDER):
            scope = repository["scopes"][name]
            scopes[name] = {**{key: value for key, value in scope.items() if key != "rows"}, "rows": paged.get((r, s), [])}
        view.append({**{key: value for key, value in repository.items() if key != "scopes"}, "scopes": scopes})
    return view


def _listing(task_id: str, capture: dict[str, Any], stored: Stored, start: tuple[int, int, int], limit: int) -> dict[str, Any]:
    document = stored.document
    items = []
    for item in _positions(document, start):
        items.append(item)
        if len(items) > limit:
            break
    included = items[:limit]

    def build() -> dict[str, Any]:
        following = items[len(included)][0] if len(items) > len(included) else None
        return {"task_id": task_id, "mode": "list", "capture": capture,
                "repositories": _repositories_view(document, included), "limit": limit,
                "has_more": following is not None,
                "next_cursor": _encode_cursor("list", task_id, stored.digest, following, 0) if following else None,
                "observed_at": _now().isoformat()}

    while True:
        result = build()
        excess = _document_bytes(result) + FIT_RESERVE_BYTES - DOCUMENT_MAX_BYTES
        if excess <= 0 or len(included) <= 1:
            return result
        while excess > 0 and len(included) > 1:
            excess -= len(json.dumps(included.pop()[1])) + 2


# --- text -------------------------------------------------------------------------------------


def _find(document: dict[str, Any], selector: Selector) -> tuple[int, int, int]:
    if selector.scope not in SCOPE_ORDER:
        raise ChangesEntryNotFound("No such scope")
    s = SCOPE_ORDER.index(selector.scope)
    for r, repository in enumerate(document["repositories"]):
        if repository["repository_root"] != selector.repository_root:
            continue
        for i, row in enumerate(repository["scopes"][selector.scope]["rows"]):
            if document_shape.row_path_bytes(row) == selector.path:
                return r, s, i
    raise ChangesEntryNotFound("No such repository, scope or path in the stored changes")


def _text_result(task_id: str, capture: dict[str, Any], stored: Stored, entry: tuple[int, int, int], offset: int) -> dict[str, Any]:
    r, s, i = entry
    repository = stored.document["repositories"][r]
    scope = repository["scopes"][SCOPE_ORDER[s]]
    row = scope["rows"][i]
    data = document_shape.decode_b64(row["text_b64"]) if row.get("text_b64") else b""
    total = len(data)
    if offset > total or (offset == total and total > 0):
        raise ChangesCursorInvalid("Cursor offset is outside the stored text")
    try:
        data.decode("utf-8")
        valid = True
    except UnicodeDecodeError:
        valid = False
    if valid and offset < total and data[offset] & 0xC0 == 0x80:
        raise ChangesCursorInvalid("Cursor offset is not on a character boundary")

    def chunk(size: int) -> tuple[int, str | None, str | None]:
        end = min(total, offset + size)
        if valid and end < total:
            while end > offset and data[end] & 0xC0 == 0x80:
                end -= 1
            if end == offset:  # one character wider than the allowance: take it whole
                end = offset + 1
                while end < total and data[end] & 0xC0 == 0x80:
                    end += 1
        part = data[offset:end]
        return end, (part.decode("utf-8") if valid else None), (None if valid else base64.b64encode(part).decode("ascii"))

    def build(size: int) -> dict[str, Any]:
        end, text, encoded = chunk(size)
        more = end < total
        return {"task_id": task_id, "mode": "text", "capture": capture,
                "repository_root": repository["repository_root"], "scope": SCOPE_ORDER[s],
                **{key: row[key] for key in ("path", "path_bytes_b64", "path_truncated", "status", "additions", "deletions", "binary", "content")},
                "text": text, "text_b64": encoded, "offset": offset, "returned_bytes": end - offset, "total_bytes": total,
                "has_more": more, "next_cursor": _encode_cursor("text", task_id, stored.digest, entry, end) if more else None,
                "observed_at": _now().isoformat()}

    size = max(total, 1)
    while True:
        result = build(size)
        excess = _document_bytes(result) + FIT_RESERVE_BYTES - DOCUMENT_MAX_BYTES
        if excess <= 0 or result["returned_bytes"] <= 1:
            return result
        size = max(1, result["returned_bytes"] // 2)


# --- entry point ------------------------------------------------------------------------------


def task_changes_result(log_dir: Path, record: store.TaskRecord, *, cursor: str | None = None, limit: int = DEFAULT_LIMIT,
                        selector: Selector | None = None) -> dict[str, Any]:
    """A listing page, or with `selector` (or a text `cursor`) a chunk of one row's stored patch.

    The caller validates arguments (`limit` 1..`MAX_LIMIT`; selector with neither cursor nor limit).
    Raises `ChangesCursorInvalid`, `ChangesCursorExpired` (stored document changed, gone or unreadable)
    and `ChangesEntryNotFound`.
    """
    task_id = record.task_id
    issued = parse_cursor(cursor, task_id) if cursor is not None else None
    stored = load_document(log_dir, task_id)
    if issued is not None and (stored.state != "document" or stored.digest != issued.digest):
        raise ChangesCursorExpired("The stored changes changed or are no longer available")
    if issued is not None and not _valid_start(stored.document, issued.entry):
        raise ChangesCursorInvalid("Cursor does not address a stored row")
    capture = _capture_of(log_dir, record, stored)
    if selector is not None or (issued is not None and issued.mode == "text"):
        if stored.state != "document":
            raise ChangesEntryNotFound("No stored changes to read")
        entry = _find(stored.document, selector) if selector is not None else issued.entry
        return _text_result(task_id, capture, stored, entry, issued.offset if issued is not None else 0)
    if stored.state != "document":
        return {"task_id": task_id, "mode": "list", "capture": capture, "repositories": [], "limit": limit,
                "has_more": False, "next_cursor": None, "observed_at": _now().isoformat()}
    return _listing(task_id, capture, stored, issued.entry if issued is not None else (0, 0, 0), limit)
