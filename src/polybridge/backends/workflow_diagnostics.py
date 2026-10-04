"""Shared provider-envelope checks; adapters select their authoritative envelopes."""
from __future__ import annotations

import re
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
