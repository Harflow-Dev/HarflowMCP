"""Pure qualification of persisted task metadata."""
from __future__ import annotations

from typing import Any, get_args, get_origin, get_type_hints
import types
import math
import os
from pathlib import Path
import unicodedata

from .wire_contracts import PRODUCT, PRODUCT_VERSION, compatible_line

TASK_FORMAT = "harflow.task"


class TaskDocumentError(Exception):
    """An existing task source cannot provide trusted task identity."""

    def __init__(self, task_id: str, reason: str, *, code: str = "task_incompatible"):
        self.task_id, self.reason, self.code = task_id, reason, code
        super().__init__(f"{task_id}: {reason}")

    def as_dict(self) -> dict[str, str]:
        return {"task_id": self.task_id, "code": self.code, "reason": self.reason}


def _matches(value: Any, annotation: Any) -> bool:
    if annotation is Any:
        return True
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is types.UnionType:
        return any(_matches(value, arg) for arg in args)
    if origin is list:
        return isinstance(value, list) and all(_matches(item, args[0]) for item in value)
    if origin is dict:
        return isinstance(value, dict) and all(_matches(k, args[0]) and _matches(v, args[1]) for k, v in value.items())
    return type(value) is annotation and (annotation is not float or math.isfinite(value))


def _shape(value: dict, shape: dict[str, Any], name: str) -> None:
    for key, annotation in shape.items():
        if key in value and not _matches(value[key], annotation):
            raise ValueError(f"Malformed {name}.{key}")


def _grants(values: list) -> None:
    if len(values) > 16:
        raise ValueError("Too many path grants")
    for grant in values:
        if not isinstance(grant, dict):
            raise ValueError("Malformed path grant")
        shape = {"requested": str, "path": str, "kind": str, "device": int, "inode": int}
        if not shape.keys() <= grant.keys():
            raise ValueError("Incomplete path grant")
        _shape(grant, shape, "path grant")
        if grant["kind"] not in {"file", "directory"} or grant["device"] < 0 or grant["inode"] < 0:
            raise ValueError("Malformed path grant identity")
        for key in ("requested", "path"):
            value = grant[key]
            if not value or not Path(value).is_absolute() or len(value.encode("utf-8")) > 4096 or any(unicodedata.category(c) == "Cc" for c in value):
                raise ValueError("Malformed absolute path grant")
        path = grant["path"]
        if os.path.normpath(path) != path or str(Path(path)) != path or path == Path(path).anchor:
            raise ValueError("Malformed canonical path grant")
    if len({value["path"] for value in values}) != len(values) or sum(len(value["requested"].encode("utf-8")) for value in values) > 16384:
        raise ValueError("Malformed path grant bounds")


BASELINE = {key: str | None for key in ("head", "branch", "repository_root", "working_tree_status", "provenance", "error")}


def _nested(record: Any) -> None:
    if record.git_baseline is not None:
        _shape(record.git_baseline, BASELINE, "git_baseline")
    if record.failure_diagnostic is not None:
        _shape(record.failure_diagnostic, {"category": str, "reason": str, "source": str, "reset_at": str | int | float}, "failure_diagnostic")
    outcome = record.outcome
    if outcome is None:
        return
    _shape(outcome, {"baseline": dict[str, Any] | None, "terminal": dict[str, Any] | None,
        "base_commit": str | None, "head_commit": str | None, "new_commits": list[str] | None,
        "new_commits_truncated": bool, "committed_diff_stat": str | None, "local_upstream": dict[str, Any] | None,
        "pushed": bool | None, "publication_evidence": dict[str, Any] | None, "attribution": str,
        "external_writes": list[dict[str, Any]] | None, "last_build_test": dict[str, Any] | None,
        "evidence_provenance": str, "structured_evidence_complete": bool}, "outcome")
    for key in ("baseline", "terminal"):
        if outcome.get(key) is not None:
            _shape(outcome[key], BASELINE, "outcome." + key)
    if outcome.get("local_upstream") is not None:
        _shape(outcome["local_upstream"], {"ref": str, "ahead_behind": str | None, "provenance": str}, "local_upstream")
    if outcome.get("last_build_test") is not None:
        _shape(outcome["last_build_test"], {"command": str, "command_truncated": bool, "exit_code": int | None,
            "ok": bool | None, "call_id": str, "call_id_encoding": str, "exit_code_scope": str, "provenance": str}, "last_build_test")
    for item in outcome.get("external_writes") or []:
        _shape(item, {"path": str, "tool": str | None, "tool_truncated": bool, "call_id": str,
            "call_id_encoding": str, "provenance": str}, "external_write")


def decode(document: Any, task_id: str, record_type: type) -> Any:
    """Validate identity and every supplied known field without filesystem access."""
    try:
        if not isinstance(document, dict) or document.get("product") != PRODUCT or document.get("format") != TASK_FORMAT:
            raise ValueError("Missing or foreign task product/format")
        if type(document.get("v")) is not int or document["v"] != 1:
            raise ValueError("Unsupported task document version")
        if not compatible_line(document.get("product_version")):
            raise ValueError("Incompatible task product major/minor line")
        return qualify_payload(document, task_id, record_type)
    except (TypeError, ValueError) as exc:
        raise TaskDocumentError(task_id, str(exc)) from exc


def qualify_payload(payload: Any, task_id: str, record_type: type) -> Any:
    """Qualify an embedded task payload without inventing a document envelope."""
    try:
        if type(payload) is not dict:
            raise ValueError("Task payload must be an object")
        hints = get_type_hints(record_type)
        payload = {key: value for key, value in payload.items() if key in hints}
        _shape(payload, hints, "task")
        record = record_type(**payload)
        if record.task_id != task_id:
            raise ValueError("Task document id does not match source id")
        if record.owner is not None:
            if not {"pid", "start_time", "markers"} <= record.owner.keys():
                raise ValueError("Incomplete process owner")
            _shape(record.owner, {"pid": int, "start_time": str | None, "markers": list[str]}, "owner")
            if record.owner["pid"] <= 0:
                raise ValueError("Invalid process owner pid")
        for key in ("pid", "pgid", "max_turns"):
            if getattr(record, key) is not None and getattr(record, key) <= 0:
                raise ValueError(f"Invalid task.{key}")
        for key in ("depth", "auto_continue", "input_after_result", "max_depth"):
            if getattr(record, key) is not None and getattr(record, key) < 0:
                raise ValueError(f"Invalid task.{key}")
        _grants(record.extra_writable_paths)
        if record.enforcement is not None:
            qualify_enforcement(record.enforcement, require_core=True)
        _nested(record)
        return record
    except (TypeError, ValueError) as exc:
        raise TaskDocumentError(task_id, str(exc)) from exc



def qualify_enforcement(value: Any, *, require_core: bool = False, path: str = "enforcement") -> None:
    """Validate supplied enforcement fields without changing saved grants or authority.

    Full task records require the stable contract core; stored workflow result
    projections retain their existing supplied-field qualification.
    """
    if type(value) is not dict:
        raise ValueError(f"Malformed {path}")
    if require_core and not {"freedom", "mechanism", "os_enforced", "writes_confined"} <= value.keys():
        raise ValueError("Incomplete enforcement contract")
    _shape(value, {"freedom": str, "mechanism": str, "os_enforced": bool, "writes_confined": bool, "writable_roots": list[str], "commit_push_blocked": bool, "direct_commit_commands_denied": bool, "publish_attempts_allowed_by_polybridge": bool, "network_access": str, "caveats": list[str], "extra_writable_paths": list[dict[str, Any]], "extra_path_certification": str | None}, path)
    _grants(value.get("extra_writable_paths", []))


def encode(payload: dict[str, Any]) -> dict[str, Any]:
    return {**payload, "product": PRODUCT, "format": TASK_FORMAT, "v": 1, "product_version": PRODUCT_VERSION}
