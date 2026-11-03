"""The stored `{task_id}.changes.json` document: its constants, shape and strict validation.

Written once by `task_changes_capture` after a task settles and read by `task_changes_read`. The
validator is the single definition of what a readable document is, so a reader never serves a file
that is not exactly this shape.
"""
from __future__ import annotations

import base64
import binascii
import json
from typing import Any

FORMAT = "harflow.task-changes"
VERSION = 1
SUFFIX = ".changes.json"
ATTRIBUTION = "Observed repository differences; not evidence of task authorship."

MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
MAX_REPOSITORIES = 8
MAX_ROWS = 500
MAX_PATH_BYTES = 4096
MAX_TEXT_BYTES = 64 * 1024
MAX_FIELD_CHARS = 8192
MAX_REPOSITORY_FIELD = 4096  # JSON-escaped (ASCII) bytes; bounds the worst-case listing page

SCOPES = ("committed_range", "working_tree")
CAPTURE_STATES = frozenset({"available", "incomplete", "unavailable"})
REPOSITORY_STATES = frozenset({"available", "incomplete", "unavailable", "not_captured"})
SCOPE_STATES = REPOSITORY_STATES
CONTENT_STATES = frozenset({"available", "truncated", "binary", "omitted_size", "not_captured"})
ROW_STATUSES = frozenset({"added", "modified", "deleted", "typechange", "unmerged", "submodule", "untracked", "unknown"})


class DocumentInvalid(ValueError):
    """The stored document is not a valid `harflow.task-changes` v1 document."""


def row_path_bytes(row: dict[str, Any]) -> bytes:
    """The (possibly truncated) path bytes a row carries."""
    if row["path"] is not None:
        return row["path"].encode("utf-8")
    return base64.b64decode(row["path_bytes_b64"], validate=True)


def decode_b64(value: str) -> bytes:
    return base64.b64decode(value, validate=True)


def escaped_size(value: str) -> int:
    """Bytes of `value` as `harflow-ctl --json` prints it (ASCII-escaped), without the quotes."""
    return len(json.dumps(value)) - 2


def bound_field(value: Any) -> str | None:
    """`value` cut so that its escaped form is at most `MAX_REPOSITORY_FIELD` bytes (None for a non-string)."""
    if not isinstance(value, str):
        return None
    if escaped_size(value) <= MAX_REPOSITORY_FIELD:
        return value
    low, high = 0, len(value)
    while low < high:
        middle = (low + high + 1) // 2
        if escaped_size(value[:middle]) <= MAX_REPOSITORY_FIELD:
            low = middle
        else:
            high = middle - 1
    return value[:low]


def _require(condition: bool, what: str) -> None:
    if not condition:
        raise DocumentInvalid(what)


def _is_string(value: Any, *, nullable: bool = False) -> bool:
    return (value is None and nullable) or isinstance(value, str)


def _string(value: Any, what: str, *, nullable: bool = False, escaped: bool = False) -> None:
    _require(_is_string(value, nullable=nullable), what)
    if value is not None:
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            raise DocumentInvalid(what) from None
        _require(escaped_size(value) <= MAX_REPOSITORY_FIELD if escaped else len(value) <= MAX_FIELD_CHARS, what)


def _count(value: Any, what: str, *, nullable: bool = False) -> None:
    _require((value is None and nullable) or (type(value) is int and value >= 0), what)


def _member(value: Any, allowed: frozenset[str], what: str) -> None:
    _require(isinstance(value, str) and value in allowed, what)


def _dict(value: Any, keys: set[str], what: str) -> None:
    _require(isinstance(value, dict) and set(value) == keys, what)


def _row(row: Any) -> None:
    _require(isinstance(row, dict), "row")
    expected = {"path", "path_bytes_b64", "path_truncated", "old_path", "status", "additions", "deletions", "binary", "content"}
    _require(set(row) - {"text_b64"} == expected, "row keys")
    _string(row["path"], "row path", nullable=True)
    _string(row["path_bytes_b64"], "row path_bytes_b64", nullable=True)
    _require((row["path"] is None) != (row["path_bytes_b64"] is None), "row path")
    try:
        raw = row_path_bytes(row)
    except (binascii.Error, ValueError, UnicodeError):
        raise DocumentInvalid("row path encoding") from None
    _require(len(raw) <= MAX_PATH_BYTES, "row path size")
    _require(type(row["path_truncated"]) is bool and type(row["binary"]) is bool, "row flags")
    _require(row["old_path"] is None, "row old_path")
    _member(row["status"], ROW_STATUSES, "row status")
    _count(row["additions"], "row additions", nullable=True)
    _count(row["deletions"], "row deletions", nullable=True)
    content = row["content"]
    _dict(content, {"state", "reason", "bytes"}, "row content")
    _member(content["state"], CONTENT_STATES, "content state")
    _string(content["reason"], "content reason", nullable=True)
    _count(content["bytes"], "content bytes")
    text = row.get("text_b64")
    if text is None:
        _require("text_b64" not in row and content["bytes"] == 0, "row text missing")
        return
    _require(isinstance(text, str) and content["state"] in {"available", "truncated"}, "row text state")
    try:
        data = decode_b64(text)
    except (binascii.Error, ValueError):
        raise DocumentInvalid("row text encoding") from None
    _require(len(data) == content["bytes"] and 0 < len(data) <= MAX_TEXT_BYTES, "row text size")


def _scope(name: str, scope: Any) -> None:
    expected = {"state", "reason", "row_count", "rows_truncated", "observed_at", "rows"}
    if name == "working_tree":
        expected = expected | {"counts_and_patch_separate"}
    _dict(scope, expected, "scope")
    _member(scope["state"], SCOPE_STATES, "scope state")
    _string(scope["reason"], "scope reason", nullable=True, escaped=True)
    _require(type(scope["rows_truncated"]) is bool, "scope rows_truncated")
    _string(scope["observed_at"], "scope observed_at", nullable=True, escaped=True)
    if name == "working_tree":
        _require(scope["counts_and_patch_separate"] is True, "counts_and_patch_separate")
    rows = scope["rows"]
    _require(isinstance(rows, list) and len(rows) <= MAX_ROWS, "scope rows")
    _require(type(scope["row_count"]) is int and scope["row_count"] == len(rows), "scope row_count")
    for row in rows:
        _row(row)


def _repository(entry: Any) -> None:
    _dict(entry, {"repository_root", "worktree", "git_common_dir", "base_commit", "base_provenance", "head_commit",
                  "observed_at", "baseline_dirty", "fields_truncated", "attribution", "state", "reason", "scopes"}, "repository")
    for key in ("repository_root", "worktree", "git_common_dir", "base_commit", "base_provenance", "head_commit", "observed_at", "reason"):
        _string(entry[key], "repository " + key, nullable=True, escaped=True)
    _require(entry["baseline_dirty"] is True or entry["baseline_dirty"] is False or entry["baseline_dirty"] == "unknown", "baseline_dirty")
    _require(type(entry["fields_truncated"]) is bool, "fields_truncated")
    _require(entry["attribution"] == ATTRIBUTION, "attribution")
    _member(entry["state"], REPOSITORY_STATES, "repository state")
    scopes = entry["scopes"]
    _require(isinstance(scopes, dict) and set(scopes) == set(SCOPES), "scopes")
    for name in SCOPES:
        _scope(name, scopes[name])


def validate(document: Any, task_id: str) -> None:
    """Raises `DocumentInvalid` unless `document` is a well-formed `harflow.task-changes` v1 document."""
    try:
        _validate(document, task_id)
    except DocumentInvalid:
        raise
    except (TypeError, AttributeError, KeyError, ValueError, RecursionError, UnicodeError):
        raise DocumentInvalid("malformed document") from None


def _validate(document: Any, task_id: str) -> None:
    _dict(document, {"product", "product_version", "format", "v", "task_id", "captured_at", "capture", "repositories"}, "document")
    _require(document["format"] == FORMAT and type(document["v"]) is int and document["v"] == VERSION
             and document["task_id"] == task_id, "document identity")
    _string(document["product"], "product")
    _string(document["product_version"], "product_version")
    _string(document["captured_at"], "captured_at")
    capture = document["capture"]
    _dict(capture, {"state", "reason"}, "capture")
    _member(capture["state"], CAPTURE_STATES, "capture state")
    _string(capture["reason"], "capture reason", nullable=True, escaped=True)
    repositories = document["repositories"]
    _require(isinstance(repositories, list) and len(repositories) <= MAX_REPOSITORIES, "repositories")
    for entry in repositories:
        _repository(entry)
