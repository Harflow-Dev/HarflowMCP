"""The `harflow-ctl task-events` result: one bounded page, the log's lifecycle evidence and a change token."""
from __future__ import annotations

import json
import os
import stat as stat_module
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import event_pages, events, store, supervision
from .state_access import open_descriptor, task_layout
from .wire_contracts import CTL_FORMAT, WIRE_VERSION, envelope

DOCUMENT_MAX_BYTES = 262_144
SETTLE_GRACE_SECONDS = 30
FIT_RESERVE_BYTES = 512


def _now() -> datetime:
    return datetime.now(timezone.utc)


def effective_kinds(kinds: list[str] | None, all_kinds: bool) -> tuple[frozenset[str], str | list[str]]:
    if kinds is not None:
        return frozenset(kinds), sorted(set(kinds))
    return (events.EVENT_KINDS, "all") if all_kinds else (event_pages.DEFAULT_KINDS, "default")


def _open_log(log_dir: Path, task_id: str) -> int | None:
    """A no-follow, non-blocking descriptor on a regular log within the state root, else None."""
    try:
        fd = open_descriptor(events.events_path(log_dir, task_id), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             layout=task_layout(log_dir))
    except OSError:
        return None
    try:
        if not stat_module.S_ISREG(os.fstat(fd).st_mode):
            fd.close()
            return None
    except OSError:
        fd.close()
        return None
    return fd


def _finished_seq(fd: int, size: int) -> int | None:
    """The seq of a `task_finished` line that ends the log exactly at EOF, whatever the page filter."""
    begin = max(0, size - event_pages.SCAN_WINDOW)
    tail = event_pages._pread(fd, begin, size - begin)
    if len(tail) != size - begin or not tail.endswith(b"\n"):
        return None
    body = tail[:-1]
    newline = body.rfind(b"\n")
    if newline < 0 and begin > 0:
        return None  # the last line is longer than the window: finality cannot be proven
    try:
        event = events._parse_event_line(body[newline + 1:])
    except (ValueError, RecursionError):
        return None  # a pathologically nested line is no evidence of finality
    if event is not None and event.get("kind") == "task_finished" and type(event.get("seq")) is int:
        return event["seq"]
    return None


def _within_grace(finished_at: str | None) -> bool:
    try:
        finished = datetime.fromisoformat(finished_at) if finished_at else None
    except ValueError:
        return False
    if finished is None:
        return False
    finished = finished if finished.tzinfo else finished.replace(tzinfo=timezone.utc)
    return 0 <= (_now() - finished).total_seconds() < SETTLE_GRACE_SECONDS  # a future stamp (clock skew) is not grace


def log_object(log_dir: Path, record: store.TaskRecord, fd: int | None) -> dict[str, Any]:
    """Whether more events can still arrive. `final` means no further appends, not complete history."""
    observed_terminal = record.status in store.TERMINAL_RECORD_STATUSES and not store.outcome_unobserved(record)
    capture = supervision.evidence(record, store.log_path(log_dir, record.task_id),
                                   observed_terminal=observed_terminal)["capture_state"]
    # The recorded status: resolving it would replay the whole raw stream on every poll, and the
    # lifecycle is already carried by `capture_state`.
    status = record.status
    final_seq = None
    if fd is None:
        state = "unavailable"
    elif capture == "live":
        state = "open"
    elif capture == "lost_with_owner":
        state = "incomplete"
    elif capture != "observed_terminal":
        state = "unknown"
    else:
        if record.output_drained:
            final_seq = _finished_seq(fd, os.fstat(fd).st_size)
        state = ("final" if final_seq is not None else
                 "settling" if record.output_drained and _within_grace(record.finished_at) else "incomplete")
    return {"state": state, "task_status": status, "output_drained": record.output_drained,
            "capture_state": capture, "final_seq": final_seq}


def _fingerprint(path: Path | None, *, mtime: bool) -> list[int] | None:
    if path is None:
        return None
    try:
        info = os.stat(path)
    except OSError:
        return None
    return [*event_pages.identity_of(info), info.st_size, *([info.st_mtime_ns] if mtime else [])]


def _change_token(log_dir: Path, task_id: str, log: dict[str, Any], fd: int | None) -> str:
    log_print = None
    if fd is not None:
        info = os.fstat(fd)
        log_print = [*event_pages.identity_of(info), info.st_size]
    payload = [event_pages.GENERATION, _fingerprint(store.record_path(log_dir, task_id), mtime=True), log_print, log]
    return event_pages.digest(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())


def _document(result: dict[str, Any]) -> bytes:
    """The bytes `harflow-ctl --json` prints for this result."""
    return (json.dumps(envelope(CTL_FORMAT, {"v": WIRE_VERSION, "result": result})) + "\n").encode()


def task_events_result(log_dir: Path, record: store.TaskRecord, *, cursor: str | None, from_oldest: bool,
                       limit: int, kinds: list[str] | None, all_kinds: bool) -> dict[str, Any]:
    """Raises `CursorInvalid` or `CursorExpired`; an unreadable log answers an `unavailable` page."""
    task_id = record.task_id
    allowed, label = effective_kinds(kinds, all_kinds)
    issued = event_pages.parse_cursor(cursor, task_id, event_pages.filter_digest(allowed)) if cursor is not None else None
    fd = _open_log(log_dir, task_id)
    try:
        page = None
        if fd is not None:
            try:
                page = event_pages.read_event_page(fd, task_id, issued=issued, from_oldest=from_oldest, limit=limit,
                                                   allowed=allowed)
            except OSError:
                fd.close()
                fd = None
        if page is None and issued is not None:
            raise event_pages.CursorExpired("Event log is unavailable")
        # The lifecycle is judged from the record as it is after the log was read.
        fresh = store.read(log_dir, task_id, include_prompt=False) or record
        log = log_object(log_dir, fresh, fd)
        token = _change_token(log_dir, task_id, log, fd)
        empty = {"events": [], "older_cursor": None, "newer_cursor": None, "has_older": False, "has_newer": False,
                 "span": None, "skipped": {"malformed": 0, "oversized": 0}, "gaps": []}

        def build(fields: dict[str, Any]) -> dict[str, Any]:
            return {"task_id": task_id, "order": "ascending", "kinds": label, **fields, "log": log,
                    "change_token": token, "observed_at": _now().isoformat()}
        if page is None:
            return build(empty)
        return build(_fit(page, lambda fields: len(_document(build(fields)))))
    finally:
        if fd is not None:
            fd.close()


def _fit(page: event_pages.PageRead, measure: Any) -> dict[str, Any]:
    """Render the page, dropping lines from its far side until the whole ctl document fits the bound.

    The reserve covers the `resolved_from` echo a prefix id adds after this measurement.
    """
    while True:
        fields = page.render()
        excess = measure(fields) + FIT_RESERVE_BYTES - DOCUMENT_MAX_BYTES
        if excess <= 0 or len(page.lines) <= 1:
            return fields
        # Drop by measured event size so a page of large events needs few re-renders.
        while excess > 0:
            line = page.lines[0] if page.direction == "older" else page.lines[-1]
            size = len(json.dumps(line.output)) + 2 if line.output is not None else 0
            if not page.trim():
                return fields
            excess -= size
