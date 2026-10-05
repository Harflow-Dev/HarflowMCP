"""Bounded reverse pages over immutable prefixes of a normalized task log.

The sequence-based reader remains available to existing callers. This interface
seeks directly to an issued byte boundary and never scans the complete log.
"""
from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .events import EVENT_KINDS, EVENT_MAX_BYTES, PAGE_BUDGET_BYTES, _parse_event_line, _truncate_value

DEFAULT_LIMIT = 100
SCAN_BYTES = 1_000_000
ANCHOR_BYTES = 64
CURSOR_BYTES = 2048


class StaleEventCursor(ValueError):
    """The cursor's file or immutable snapshot prefix no longer exists."""


@dataclass
class CursorEventPage:
    events: list[dict[str, Any]]
    has_more: bool
    next_cursor: str | None
    snapshot_end: int
    live_offset: int
    bytes_read: int
    decoded_records: int


def _encode(value: dict[str, Any]) -> str:
    return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode()


def _decode(cursor: str) -> dict[str, Any]:
    if not isinstance(cursor, str) or not cursor or len(cursor) > CURSOR_BYTES:
        raise ValueError("Invalid event cursor")
    try:
        raw = base64.b64decode(cursor, altchars=b"-_", validate=True)
        value = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Invalid event cursor") from exc
    if not isinstance(value, dict) or type(value.get("v")) is not int or value.get("v") != 1:
        raise ValueError("Invalid event cursor version")
    for key in ("dev", "ino", "snapshot_end", "end", "live_offset"):
        if type(value.get(key)) is not int or not 0 <= value[key] < 2**63:
            raise ValueError("Invalid event cursor boundary")
    if value["end"] > value["snapshot_end"] or value["live_offset"] > value["snapshot_end"]:
        raise ValueError("Invalid event cursor boundary")
    for key in ("path", "head", "tail", "filter"):
        if not isinstance(value.get(key), str) or len(value[key]) != 64:
            raise ValueError("Invalid event cursor anchor")
    return value


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _anchor(handle: Any, end: int, *, head: bool = False) -> bytes:
    begin = 0 if head else max(0, end - ANCHOR_BYTES)
    handle.seek(begin)
    return handle.read(min(ANCHOR_BYTES, end - begin))


def _output(event: dict[str, Any]) -> dict[str, Any]:
    fields, truncated = _truncate_value({k: v for k, v in event.items()
        if k not in {"v", "seq", "kind", "observed_at", "source_ts", "raw_offset", "task_id"}})
    observed = event.get("observed_at")
    observed = observed if isinstance(observed, str) and len(observed) <= 128 else None
    result = {**fields, "seq": event["seq"], "kind": event["kind"], "observed_at": observed}
    if truncated:
        result["truncated"] = True
    size = len(json.dumps(result, ensure_ascii=False, default=str).encode())
    if size > EVENT_MAX_BYTES:
        return {"seq": event["seq"], "kind": event["kind"], "observed_at": observed,
                "truncated": True, "oversized": True, "bytes": size}
    return result


def read_cursor_page(path: Path, *, cursor: str | None = None, limit: int = DEFAULT_LIMIT,
                     kinds: list[str] | None = None) -> CursorEventPage:
    """Newest page, or one older page from ``next_cursor``, oldest first.

    Each call reads at most 1 MiB plus four 64-byte integrity anchors and decodes
    at most ``limit`` records. Kind filters may yield an empty page with a
    continuation; malformed/oversized lines never prevent cursor progress.
    Append-only growth leaves an issued snapshot unchanged. Replacement,
    truncation or changed anchors raise ``StaleEventCursor`` so callers can
    deliberately reload the newest page. A partial final line remains available
    to a live tail beginning at ``live_offset``.
    """
    if type(limit) is not int or not 1 <= limit <= 200:
        raise ValueError("limit must be 1..200")
    if kinds is not None and (not isinstance(kinds, list) or not kinds or
            any(not isinstance(k, str) or k not in EVENT_KINDS for k in kinds)):
        raise ValueError("kinds must be a nonempty list of known event kinds")
    allowed = set(kinds) if kinds is not None else EVENT_KINDS - {"assistant_delta"}
    filter_key = _digest(json.dumps(sorted(allowed)).encode())
    issued = _decode(cursor) if cursor is not None else None
    if issued and issued["filter"] != filter_key:
        raise ValueError("Event cursor filter changed; reload its newest page")
    path_key = _digest(str(path.absolute()).encode("utf-8", errors="surrogatepass"))
    try:
        handle = path.open("rb")
    except FileNotFoundError:
        if issued is not None:
            raise StaleEventCursor("Event log disappeared; reload its newest page") from None
        return CursorEventPage([], False, None, 0, 0, 0, 0)
    with handle:
        import os
        stat = os.fstat(handle.fileno())
        snapshot_end = issued["snapshot_end"] if issued else stat.st_size
        if issued and (issued["path"] != path_key or issued["dev"] != stat.st_dev or
                       issued["ino"] != stat.st_ino or stat.st_size < snapshot_end):
            raise StaleEventCursor("Event log changed; reload its newest page")
        head = _anchor(handle, snapshot_end, head=True)
        tail = _anchor(handle, snapshot_end)
        if issued and (issued["head"] != _digest(head) or issued["tail"] != _digest(tail)):
            raise StaleEventCursor("Event log snapshot changed; reload its newest page")
        identity = issued or {"v": 1, "dev": stat.st_dev, "ino": stat.st_ino,
            "path": path_key, "snapshot_end": snapshot_end, "head": _digest(head), "tail": _digest(tail), "filter": filter_key}
        end = issued["end"] if issued else snapshot_end
        start = max(0, end - SCAN_BYTES)
        handle.seek(start)
        window = handle.read(end - start)
        if os.fstat(handle.fileno()).st_size < snapshot_end:
            raise StaleEventCursor("Event log truncated during read; reload its newest page")
        checked_head = _anchor(handle, snapshot_end, head=True)
        checked_tail = _anchor(handle, snapshot_end)
        if head != checked_head or tail != checked_tail:
            raise StaleEventCursor("Event log changed during read; reload its newest page")
    bytes_read = len(head) + len(tail) + len(checked_head) + len(checked_tail) + len(window)
    last_newline = window.rfind(b"\n")
    live_offset = issued["live_offset"] if issued else (
        start + last_newline + 1 if last_newline >= 0 else snapshot_end if start else 0)
    identity["live_offset"] = live_offset
    first = window.find(b"\n") + 1 if start else 0
    boundary = start + first
    result: list[dict[str, Any]] = []
    decoded = 0
    output_bytes = 0
    # Only newline-terminated, whole lines enter parsing. A window entirely
    # inside an oversized line advances one bounded scan window without decode.
    if last_newline < first:
        boundary = start
    else:
        line_end = last_newline + 1
        boundary = start + line_end
        while line_end > first and decoded < limit:
            previous = window.rfind(b"\n", first, line_end - 1)
            line_start = previous + 1 if previous >= first else first
            raw = window[line_start:line_end - 1]
            try:
                event = _parse_event_line(raw) if raw else None
            except (ValueError, RecursionError):
                event = None
            decoded += 1
            if event is not None and type(event.get("seq")) is int and isinstance(event.get("kind"), str) and event["kind"] in allowed:
                try:
                    output = _output(event)
                    size = len(json.dumps(output, ensure_ascii=False, default=str).encode()) + 1
                except (ValueError, UnicodeError, RecursionError):
                    pass  # A malformed record is consumed without blocking older history.
                else:
                    if output_bytes + size > PAGE_BUDGET_BYTES - 4096:
                        break  # This candidate remains reachable from the last boundary.
                    result.append(output)
                    output_bytes += size
            boundary = start + line_start
            line_end = line_start
        if not result and boundary == end and start:
            boundary = start
    result.reverse()
    more = boundary > 0
    next_cursor = _encode({**identity, "end": boundary}) if more else None
    return CursorEventPage(result, more, next_cursor, snapshot_end, live_offset, bytes_read, decoded)
