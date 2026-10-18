"""Shared provider-envelope checks; adapters select their authoritative envelopes."""
from __future__ import annotations

import re
import json
import math
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

OUTAGES = {500, 502, 503, 504, 529}
MODEL_CODES = {"model_not_found"}
TRANSPORT_CODES = {"overloaded_error", "api_connection_error", "APITimeoutError", "APIConnectionError", "service_unavailable"}


def stderr_blocks_availability(diagnostic: str) -> bool:
    """Quota stderr vetoes automatic replacement without asserting terminal usage authority.

    Recognized plain native forms and error-prefixed codes block the whole artifact;
    warnings and quoted prose do not become evidence.
    """
    quota = r"(?:usage_limit_reached|insufficient_quota|rate_limit_exceeded|rate_limit_error)"
    error_prefix = r"(?:API[ _]Error|Provider[ _]Error|HTTP[ _]Error|Error|RateLimitError|APIConnectionError|APITimeoutError|ConnectError|ConnectionError)"
    return re.search(r"(?im)^(?:" + error_prefix + r"\b[^\n]*\b" + quota + r"\b|" + quota + r"\s*$|You've hit your limit\b[^\n]*|Credit balance is too low\b[^\n]*)", diagnostic) is not None


def stderr_availability(diagnostic: str, *, model_patterns: tuple[str, ...] = ()) -> str | None:
    if stderr_blocks_availability(diagnostic):
        return None
    if any(re.search(pattern, diagnostic) for pattern in model_patterns):
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


def provider_error(event: dict[str, Any], *, event_type: str, extra_transport_codes: tuple[str, ...] = (), model_reason: str | None = None) -> str | None:
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
    if any(isinstance(code, str) and code in USAGE_CODES for code in codes):
        return None  # Quota evidence never becomes an automatic transport fallback.
    if any(isinstance(code, str) and code in TRANSPORT_CODES | set(extra_transport_codes) for code in codes):
        return "provider transport unavailable"
    if model_reason and any(isinstance(code, str) and code in MODEL_CODES for code in codes):
        return model_reason
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


USAGE_CODES = {"usage_limit_reached", "insufficient_quota", "rate_limit_exceeded", "rate_limit_error"}
USAGE_LIMIT_REASON = "Provider rejected this turn because its usage limit was reached."

def usage_limit(event: dict[str, Any], *, envelope: str, claude: bool = False, agy: bool = False) -> dict[str, Any] | None:
    """Only authoritative provider error envelopes establish a terminal quota failure."""
    info = event.get("rate_limit_info")
    if claude and event.get("type") == "rate_limit_event" and isinstance(info, dict) and info.get("status") == "rejected":
        result = {"category": "usage_limit", "reason": USAGE_LIMIT_REASON, "source": "stream:rate_limit_event"}
        if valid_reset_value(info.get("resetsAt")):
            result["reset_at"] = info["resetsAt"]
        return result
    if agy:
        result = event.get("result")
        if event.get("event") != "result" or not isinstance(result, dict) or result.get("status") != "ERROR" or result.get("denied_actions"):
            return None
        error = result.get("error")
    else:
        if event.get("type") != envelope:
            return None
        error = event.get("error")
    if agy and isinstance(error, str):
        if not re.search(r"\b(?:401|403|model_not_found)\b", error) and re.search(r"\b(?:usage_limit_reached|insufficient_quota|rate_limit_exceeded|rate_limit_error)\b", error):
            return {"category": "usage_limit", "reason": USAGE_LIMIT_REASON, "source": "stream:result"}
        return None
    if not isinstance(error, dict):
        return None
    data = error.get("data") if isinstance(error.get("data"), dict) else {}
    if any(type(value) is int and 400 <= value < 500 and value != 429 for value in (error.get("status"), error.get("status_code"), error.get("statusCode"), data.get("statusCode"))):
        return None
    if any(type(value) is int and value in OUTAGES for value in (error.get("status"), error.get("status_code"), error.get("statusCode"), data.get("statusCode"))):
        return None
    if not any(error.get(key) in USAGE_CODES for key in ("type", "code", "name") if isinstance(error.get(key), str)):
        return None
    diagnostic = {"category": "usage_limit", "reason": USAGE_LIMIT_REASON, "source": "stream:" + ("result" if agy else envelope)}
    reset = error.get("reset_at", data.get("reset_at"))
    if valid_reset_value(reset):
        diagnostic["reset_at"] = reset
    return diagnostic

def stderr_usage_limit(line: str, *, claude: bool = False) -> dict[str, Any] | None:
    # Error-prefixed harness stderr only; arbitrary prose mentioning a quota is not evidence.
    prefix = r"(?i)^(?:API[ _]Error|Provider[ _]Error|HTTP[ _]Error|Error|RateLimitError)\s*:?\s*"
    if not re.match(prefix, line):
        return None
    if re.search(r"\b(?:401|403|500|502|503|504|529|model_not_found)\b", line):
        return None
    if not re.search(r"\b(?:usage_limit_reached|insufficient_quota|rate_limit_exceeded|rate_limit_error)\b", line) and not (claude and re.search(r"You've hit your limit|Credit balance is too low", line)):
        return None
    return {"category": "usage_limit", "reason": USAGE_LIMIT_REASON, "source": "stderr"}


def valid_reset_value(value: Any) -> bool:
    if type(value) in (int, float):
        try:
            return math.isfinite(value)
        except OverflowError:
            return False
    if not isinstance(value, str) or len(value) > 100 or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)", value
    ):
        return False
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).utcoffset() is not None
    except ValueError:
        return False
