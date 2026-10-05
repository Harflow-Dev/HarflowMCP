"""Shared provider-envelope checks; adapters select their authoritative envelopes."""
from __future__ import annotations

import re
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

OUTAGES = {500, 502, 503, 504, 529}
QUOTA_CODES = {"usage_limit_reached", "insufficient_quota", "model_not_found", "rate_limit_exceeded"}
TRANSPORT_CODES = {"overloaded_error", "api_connection_error", "APITimeoutError", "APIConnectionError", "service_unavailable"}


def stderr_availability(diagnostic: str, *, quota_patterns: tuple[str, ...] = ()) -> str | None:
    if any(re.search(pattern, diagnostic) for pattern in quota_patterns):
        return "backend availability rejected"
    for line in diagnostic.splitlines():
        match = re.match(r"(?i)^(?:API[ _]Error|Provider[ _]Error|HTTP[ _]Error|APIConnectionError|APITimeoutError|ConnectError|ConnectionError)\s*:?\s*(.*)$", line)
        if not match:
            continue
        body = match.group(1)
        status = re.match(r"(?i)^(?:(?:HTTP(?:\s+status)?|status(?:\s*code)?)\s*[:=]?\s*)?([1-5][0-9]{2})\b", body)
        if status:
            if int(status.group(1)) in OUTAGES:
                return "provider server unavailable"
            continue
        if re.search(r"(?i)\b(?:overloaded_error|api_connection_error|service unavailable|connection (?:reset|refused)|provider timeout|timed out)\b", body):
            return "provider transport unavailable"
    return None


def provider_error(event: dict[str, Any], *, event_type: str, extra_transport_codes: tuple[str, ...] = (), quota_reason: str | None = None) -> str | None:
    if event.get("type") != event_type:
        return None
    error = event.get("error")
    if not isinstance(error, dict):
        return None
    data = error.get("data") if isinstance(error.get("data"), dict) else {}
    statuses = [error.get("status"), error.get("status_code"), error.get("statusCode"), data.get("statusCode")]
    # Explicit client/security status wins over suggestive provider prose/codes.
    if any(type(status) is int and 400 <= status < 500 and status != 429 for status in statuses):
        return None
    if any(type(status) is int and status in OUTAGES for status in statuses):
        return "provider server unavailable"
    codes = [error.get("type"), error.get("code"), error.get("name")]
    if any(isinstance(code, str) and code in TRANSPORT_CODES | set(extra_transport_codes) for code in codes):
        return "provider transport unavailable"
    if quota_reason and any(isinstance(code, str) and code in QUOTA_CODES for code in codes):
        return quota_reason
    return None


STREAM_WINDOW_BYTES = 256 * 1024
STREAM_WINDOW_EVENTS = 256


@lru_cache(maxsize=16)
def _stream_events(path: str, inode: int, size: int, modified_ns: int) -> tuple[dict[str, Any], ...]:
    """Inspect bounded startup/terminal windows once per immutable log revision."""
    with Path(path).open("rb") as stream:
        head = stream.read(min(size, STREAM_WINDOW_BYTES))
        if size > len(head):
            head = head.rsplit(b"\n", 1)[0] if b"\n" in head else b""
            stream.seek(max(0, size - STREAM_WINDOW_BYTES))
            tail = stream.read(STREAM_WINDOW_BYTES)
            tail = tail.split(b"\n", 1)[1] if b"\n" in tail else b""
        else:
            tail = b""
    if size <= STREAM_WINDOW_BYTES:
        complete = head.splitlines()
        # Keep both ends of a dense small stream, without overlapping entries.
        lines = complete if len(complete) <= 2 * STREAM_WINDOW_EVENTS else complete[:STREAM_WINDOW_EVENTS] + complete[-STREAM_WINDOW_EVENTS:]
    else:
        lines = head.splitlines()[:STREAM_WINDOW_EVENTS] + tail.splitlines()[-STREAM_WINDOW_EVENTS:]
    events = []
    for line in lines:
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError):
            continue
        if isinstance(event, dict):
            events.append(event)
    return tuple(events)


def stream_events(path: str | Path) -> tuple[dict[str, Any], ...]:
    """No assistant/tool text is promoted to evidence; adapters choose envelopes."""
    try:
        source = Path(path)
        stat = source.stat()
        return _stream_events(str(source), stat.st_ino, stat.st_size, stat.st_mtime_ns)
    except OSError:
        return ()
