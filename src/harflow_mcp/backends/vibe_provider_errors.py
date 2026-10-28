"""Vibe 3's measured HTTPX payment error; never retain URLs or provider prose."""
from __future__ import annotations

import re

from .base import Accumulator
from .workflow_diagnostics import USAGE_LIMIT_REASON

PAYMENT_REASON = "Provider rejected this turn with HTTP 402 (Payment Required). Check the provider's billing and usage limits; the precise cause was not established."
PAYMENT_SOURCE = "stderr:vibe_http_error"
PAYMENT_LINE = re.compile(r"^Error: Client error '402 Payment Required' for url '[^\r\n]+'$")
DETAIL_PREFIX = "For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/402: "


def structured_payment_error(event: dict) -> dict | None:
    error = event.get("error")
    if event.get("type") != "error" or not isinstance(error, dict):
        return None
    statuses = [error.get(key) for key in ("status", "status_code", "statusCode")]
    if any(type(value) is int and value == 402 for value in statuses) and all(value is None or type(value) is int and value == 402 for value in statuses):
        return {"category": "provider_error", "reason": PAYMENT_REASON,
                "source": "stream:error", "http_status": 402}
    return None


def observe_stderr(acc: Accumulator, line: str) -> None:
    # Only the immediately following line belongs to the captured HTTPX error.
    # Retain one boolean, never a request URL, headers or untrusted response body.
    pending = acc.stream_state.pop("vibe_payment_detail_pending", False)
    previous = acc.failure_diagnostic
    if pending and line == DETAIL_PREFIX + "Usage limit exceeded":
        if previous and previous.get("category") == "provider_error" and previous.get("source") == PAYMENT_SOURCE:
            acc.failure_diagnostic = {"category": "usage_limit", "reason": USAGE_LIMIT_REASON,
                                      "source": PAYMENT_SOURCE, "http_status": 402}
        return
    if PAYMENT_LINE.fullmatch(line):
        acc.stream_state["vibe_payment_detail_pending"] = True
        if previous is None:
            acc.failure_diagnostic = {"category": "provider_error", "reason": PAYMENT_REASON,
                                      "source": PAYMENT_SOURCE, "http_status": 402}
            acc.error_result_seen = True
