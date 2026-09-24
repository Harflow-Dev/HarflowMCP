"""Shared helpers for translating a backend's raw stream into monitor events.

Nothing here branches on a backend's name — that is the whole point of the seam. Each backend's own
`normalize` method decides which of its native events map to which of these builders; this module
only knows how to build a well-formed monitor event dict and how to truncate untrusted text safely.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any

from .base import Accumulator

INPUT_PREVIEW_BYTES = 2 * 1024
EDIT_TEXT_BYTES = 8 * 1024
OUTPUT_TAIL_BYTES = 4 * 1024

CATEGORIES = ("read", "search", "edit", "write", "shell", "mcp", "web", "other")


def truncate_head(text: str, limit_bytes: int) -> str:
    """Keep the first `limit_bytes` of `text`'s UTF-8 encoding, never splitting a codepoint."""
    encoded = text.encode("utf-8")
    if len(encoded) <= limit_bytes:
        return text
    return encoded[:limit_bytes].decode("utf-8", errors="ignore")


def truncate_tail(text: str, limit_bytes: int) -> str:
    """Keep the LAST `limit_bytes` of `text`'s UTF-8 encoding, never splitting a codepoint."""
    encoded = text.encode("utf-8")
    if len(encoded) <= limit_bytes:
        return text
    return encoded[-limit_bytes:].decode("utf-8", errors="ignore")


def preview(value: Any) -> str:
    """A short, head-truncated rendering of an arbitrary tool input: a str stays as is, anything
    else is rendered as sorted, non-ASCII-escaped JSON so it reads the same on every run."""
    text = (
        value
        if isinstance(value, str)
        else json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)
    )
    return truncate_head(text, INPUT_PREVIEW_BYTES)


def iso_from_epoch_ms(value: Any) -> str | None:
    """An epoch-milliseconds timestamp as ISO-8601, or None for anything not a genuine timestamp.

    `bool` is excluded even though it is an `int` subclass (`True`/`False` are not timestamps), and
    `NaN`/`Infinity` are excluded even though `isinstance(..., float)` accepts them (a stream we do
    not control could carry either, per json's own permissive defaults).
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or value < 0:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def iso_string(value: Any) -> str | None:
    """A timestamp a backend already reports as a string, or None."""
    return value if isinstance(value, str) and value else None


def _coerce_id(value: Any) -> str:
    if isinstance(value, str):
        return value
    return str(value) if value is not None else ""


def tool_call(
    *,
    call_id: Any,
    tool: Any,
    category: str,
    input: Any,
    path: str | None = None,
    command: str | None = None,
    edit: tuple[str, str] | None = None,
    source_ts: str | None = None,
) -> dict[str, Any]:
    """Build a `tool_call` monitor event. `edit` is an `(old, new)` pair of str, each independently
    truncated so one huge side of an edit cannot crowd out the other."""
    event: dict[str, Any] = {
        "kind": "tool_call",
        "call_id": _coerce_id(call_id),
        "tool": _coerce_id(tool),
        "category": category if category in CATEGORIES else "other",
        "input_preview": preview(input),
    }
    if isinstance(path, str) and path:
        event["path"] = path
    if isinstance(command, str) and command:
        event["command"] = command
    if edit is not None:
        old, new = edit
        event["edit"] = {
            "old": truncate_head(old, EDIT_TEXT_BYTES),
            "new": truncate_head(new, EDIT_TEXT_BYTES),
        }
    if source_ts is not None:
        event["source_ts"] = source_ts
    return event


def tool_result(
    *,
    call_id: Any,
    ok: Any,
    output: Any,
    exit_code: int | None = None,
    source_ts: str | None = None,
) -> dict[str, Any]:
    """Build a `tool_result` monitor event. A non-str `output` is rendered as JSON first (no head
    truncation — only the tail matters here) and then tail-truncated, so the end of a long output
    (often where the interesting part is) survives rather than the start."""
    raw = output or ""
    text = raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False, default=str, sort_keys=True)
    event: dict[str, Any] = {
        "kind": "tool_result",
        "call_id": _coerce_id(call_id),
        "ok": bool(ok),
        "output_tail": truncate_tail(text, OUTPUT_TAIL_BYTES),
    }
    if isinstance(exit_code, int) and not isinstance(exit_code, bool):
        event["exit_code"] = exit_code
    if source_ts is not None:
        event["source_ts"] = source_ts
    return event


def assistant_text(text: str, source_ts: str | None = None) -> dict[str, Any]:
    event: dict[str, Any] = {"kind": "assistant_text", "text": text}
    if source_ts is not None:
        event["source_ts"] = source_ts
    return event


def user_message(text: str, source: str, source_ts: str | None = None) -> dict[str, Any]:
    event: dict[str, Any] = {"kind": "user_message", "text": text, "source": source}
    if source_ts is not None:
        event["source_ts"] = source_ts
    return event


def notice(text: str, source_ts: str | None = None) -> dict[str, Any]:
    event: dict[str, Any] = {"kind": "notice", "text": text}
    if source_ts is not None:
        event["source_ts"] = source_ts
    return event


def usage(acc: Accumulator, source_ts: str | None = None) -> dict[str, Any]:
    """The accumulator's cumulative usage, read after `ingest` — so this means the same thing
    across backends, whatever shape each one's own stream reports it in."""
    event: dict[str, Any] = {
        "kind": "usage",
        "usage": acc.usage,
        "total_cost_usd": acc.total_cost_usd,
        "num_turns": acc.num_turns,
    }
    if source_ts is not None:
        event["source_ts"] = source_ts
    return event
