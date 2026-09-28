"""Per-task normalized event log: `<task_id>.events.jsonl`.

Where the raw stream log (`<task_id>.jsonl`) is whatever bytes the backend's CLI produced, this is
the bridge's own normalized view of the same run — one JSON object per line, with envelope fields
that mean the same thing across every backend (see `backends/base.py`'s `normalize` contract for
what a backend hands back). Written only by the server process that owns the task: single-writer by
design, so no locking is needed here the way `retention.py` needs it for cross-process deletion.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import deque
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from . import store

log = logging.getLogger(__name__)

EVENTS_SUFFIX = ".events.jsonl"
EVENT_LOG_VERSION = 1

# Constants for bounded reading
MAX_LINE_BYTES = 1_000_000  # 1 MiB - lines larger than this are skipped
CHUNK_SIZE = 65_536  # 64 KiB chunks for reading
RECENT_ACTIVITY_MAX_BYTES = 1_000_000  # 1 MiB total cap for recent_activity scan
RECENT_ACTIVITY_LIMIT = 5  # Maximum number of recent_activity entries
RECENT_ACTIVITY_LINE_LIMIT = 160  # Maximum chars per recent_activity line
MAX_TRUNCATED_STRING = 2000  # Max chars for string fields before truncation
MAX_LIST_ITEMS = 50  # Max items in lists before truncation
MAX_NESTING_DEPTH = 4  # Max dict nesting depth before truncation
PAGE_BUDGET_BYTES = 262_144
# One event's serialized ceiling after truncation: `_truncate_value` bounds strings, lists and
# depth, but not how many keys a dict has, so a wide one is replaced by a stub past this.
EVENT_MAX_BYTES = 16_384  # 256 KiB budget for a page of events

# The closed set of `kind` values in a v1 events log — the Monitor app (Stage C) switches on these,
# so adding one is a contract change: update this set, README.md's list, and the app's
# `TaskEvent.Kind` (macos/PbCore/MonitorCore/Sources/MonitorCore/Events.swift) together.
# Found by reading every emit site: the bridge's own writes in `tasks.py` (`task_started`,
# `task_finished`, `user_message`, `notice`, `undelivered`) and the helpers every backend's
# `normalize` builds events with (`backends/normalize.py`). Pinned by `tests/test_events.py`.
EVENT_KINDS = frozenset(
    {
        "task_started",
        "assistant_text",
        "assistant_delta",
        "tool_call",
        "tool_result",
        "user_message",
        "usage",
        "notice",
        "task_finished",
        "undelivered",
    }
)

# Kinds that contribute to recent_activity (meaningful events)
# Skip assistant_delta, usage, task_started, task_finished per plan
MEANINGFUL_KINDS = frozenset(
    {
        "tool_call",
        "tool_result",
        "assistant_text",
        "user_message",
        "notice",
        "undelivered",
    }
)


class UnknownEventKind(ValueError):
    """A write named a `kind` outside `EVENT_KINDS` — a programming error, not an I/O failure."""


def events_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{store.validate_task_id(task_id)}{EVENTS_SUFFIX}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventLog:
    """Append-only writer for one task's normalized event stream.

    Every failure mode disables the log rather than raising: a normalized event is a convenience
    view over the raw stream, and losing it must never touch a run's outcome (CLAUDE.md:
    bookkeeping must never change an outcome).
    """

    def __init__(self, path: Path, task_id: str) -> None:
        self._task_id = task_id
        self._seq = 0
        self._handle = None
        try:
            self._handle = path.open("ab")
        except OSError:
            log.warning(
                "task %s: cannot open %s; events will not be recorded", task_id, path, exc_info=True
            )

    def write(
        self,
        kind: str,
        fields: dict[str, Any],
        *,
        raw_offset: int | None = None,
        source_ts: str | None = None,
    ) -> bool:
        """Append one envelope. False if the log is disabled or the write failed. Raises
        `UnknownEventKind` for a `kind` outside `EVENT_KINDS` — the only thing it ever raises.

        Envelope keys win over any same-named key in `fields`, so a caller can pass a backend's own
        normalized dict straight through without stripping fields that happen to collide.
        """
        if kind not in EVENT_KINDS:
            # Raised rather than written, so an unlisted kind never reaches a reader of the frozen
            # v1 schema. Every call site already guards its write, so this cannot change a run's
            # outcome; it fails the test that exercises it (see `tests/conftest.py`).
            raise UnknownEventKind(f"{kind!r} is not a v1 event kind; see events.EVENT_KINDS")
        if self._handle is None:
            return False
        envelope: dict[str, Any] = {
            "v": EVENT_LOG_VERSION,
            "seq": self._seq,
            "observed_at": _now_iso(),
            "source_ts": source_ts,
            "raw_offset": raw_offset,
            "task_id": self._task_id,
            "kind": kind,
        }
        envelope.update((key, value) for key, value in fields.items() if key not in envelope)
        try:
            line = json.dumps(envelope, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            # One unserializable event is dropped; the log itself stays usable for the next.
            log.warning("task %s: dropping an unserializable %s event", self._task_id, kind)
            return False
        try:
            self._handle.write((line + "\n").encode("utf-8"))
            self._handle.flush()
        except OSError:
            log.warning(
                "task %s: events log write failed; disabling it", self._task_id, exc_info=True
            )
            self.close()
            return False
        self._seq += 1
        return True

    def close(self) -> None:
        if self._handle is not None:
            try:
                self._handle.close()
            except OSError:
                pass
            self._handle = None


# --- Bounded readers for events.log -----------------------------------------------------------


@dataclass
class PageResult:
    """Result of reading a page of events."""

    events: list[dict[str, Any]]
    has_more: bool
    next_before_seq: int | None = None
    next_after_seq: int | None = None
    skipped_oversized: int = 0


def _truncate_value(value: Any, depth: int = 0) -> tuple[Any, bool]:
    """Recursively truncate values that exceed bounds.

    Strings > 2000 chars are cut with "..."
    Lists > 50 items are cut.
    Dict nesting deeper than 4 is replaced with "..."
    Returns (truncated_value, was_truncated).
    """
    if depth > MAX_NESTING_DEPTH:
        return "...", True

    if isinstance(value, str):
        if len(value) > MAX_TRUNCATED_STRING:
            return value[: MAX_TRUNCATED_STRING] + "...", True
        return value, False

    if isinstance(value, list):
        truncated = False
        if len(value) > MAX_LIST_ITEMS:
            value = value[:MAX_LIST_ITEMS]
            truncated = True
        new_list = []
        for item in value:
            item_truncated, item_changed = _truncate_value(item, depth + 1)
            new_list.append(item_truncated)
            truncated = truncated or item_changed
        return new_list, truncated

    if isinstance(value, dict):
        truncated = False
        new_dict: dict[str, Any] = {}
        for k, v in value.items():
            val_truncated, val_changed = _truncate_value(v, depth + 1)
            new_dict[k] = val_truncated
            truncated = truncated or val_changed
        return new_dict, truncated

    return value, False


def _parse_event_line(line_bytes: bytes) -> dict[str, Any] | None:
    """Parse a single line from the events log.

    Returns the event dict with all fields, or None if unparsable or oversized.
    Lines > 1 MiB are not parsed (per round 2: skipped, no seq/kind recovery).
    Never raises.
    """
    if len(line_bytes) > MAX_LINE_BYTES:
        return None  # Skip oversized lines

    try:
        text = line_bytes.decode("utf-8")
        event = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None  # Skip unparsable lines

    # Validate required envelope fields
    if not isinstance(event, dict):
        return None
    if "seq" not in event or "kind" not in event:
        return None

    return event


def _iter_events_forward(path: Path, skipped: list[int]) -> Iterator[dict[str, Any]]:
    """Parsed events oldest first, up to the size the file had when opened.

    Stopping at that size keeps a busy writer from stretching the scan, and only newline-terminated
    lines count, so an event still being appended is left for the next read. A line longer than
    `MAX_LINE_BYTES` is discarded as it streams past — never buffered whole — and counted in
    `skipped[0]`. Stops quietly on an I/O error.
    """
    try:
        with path.open("rb") as handle:
            yield from _iter_open_log(handle, skipped)
    except OSError:
        return


def _iter_open_log(handle: Any, skipped: list[int]) -> Iterator[dict[str, Any]]:
    """`_iter_events_forward`'s loop over an open log — split out so one `try` covers the whole
    life of the file, closing included."""
    try:
        remaining = handle.seek(0, 2)
        handle.seek(0)
    except OSError:
        return
    fragment = b""
    discarding = False
    while remaining > 0:
        try:
            chunk = handle.read(min(CHUNK_SIZE, remaining))
        except OSError:
            return
        if not chunk:
            return
        remaining -= len(chunk)
        pieces = chunk.split(b"\n")
        for index, piece in enumerate(pieces):
            is_last = index == len(pieces) - 1
            if discarding:
                if not is_last:
                    discarding = False
                continue
            fragment += piece
            if is_last:
                if len(fragment) > MAX_LINE_BYTES:
                    fragment = b""
                    discarding = True
                    skipped[0] += 1
                continue
            line, fragment = fragment, b""
            if not line:
                continue
            if len(line) > MAX_LINE_BYTES:
                skipped[0] += 1
                continue
            if (event := _parse_event_line(line)) is not None:
                yield event


def _stream_events_backward(path: Path) -> list[dict[str, Any]]:
    """The events in the last `RECENT_ACTIVITY_MAX_BYTES` of the log, newest first.

    One bounded read of the tail rather than chunked seeks: at ≤ 1 MiB it is cheap, and a single
    buffer has no chunk boundaries for a line to straddle. Only whole lines count — the partial
    line the window starts inside is older than everything returned, and the bytes after the last
    newline are a line the owner is still appending. Splitting at `\n` bytes never cuts a UTF-8
    sequence. Never raises.
    """
    try:
        with path.open("rb") as handle:
            end = handle.seek(0, 2)
            start = max(0, end - RECENT_ACTIVITY_MAX_BYTES)
            handle.seek(start)
            window = handle.read(end - start)
    except OSError:
        return []
    last_newline = window.rfind(b"\n")
    if last_newline < 0:
        return []
    window = window[: last_newline + 1]
    if start > 0:
        first_newline = window.find(b"\n")
        window = window[first_newline + 1 :]
    events = []
    for line in reversed(window.split(b"\n")):
        if line and len(line) <= MAX_LINE_BYTES and (event := _parse_event_line(line)) is not None:
            events.append(event)
    return events


def _identity(event: dict[str, Any]) -> tuple[str, int] | None:
    """A streamed text block's identity, `(message_id, block_index)` — only when both are present,
    so a delta with a missing half is never merged into some other block's text."""
    message_id = event.get("message_id")
    block_index = event.get("block_index")
    if isinstance(message_id, str) and message_id and isinstance(block_index, int):
        return (message_id, block_index)
    return None


def _streaming_text(events: list[dict[str, Any]]) -> tuple[int, str] | None:
    """The block still being streamed, if any: `(position of its last delta, its text so far)`.

    Only the newest identified block counts, and only while no `assistant_text` for that same block
    has arrived after it — once it has, the finished text speaks for itself.
    """
    finished: set[tuple[str, int]] = set()
    for event in events:
        if event.get("kind") == "assistant_text" and (identity := _identity(event)) is not None:
            finished.add(identity)
    for position in range(len(events) - 1, -1, -1):
        event = events[position]
        if event.get("kind") != "assistant_delta":
            continue
        identity = _identity(event)
        if identity is None:
            continue
        if identity in finished:
            return None
        chunks = [
            e.get("text", "")
            for e in events[: position + 1]
            if e.get("kind") == "assistant_delta" and _identity(e) == identity
        ]
        return position, "".join(c for c in chunks if isinstance(c, str))
    return None


def _one_line(text: Any) -> str:
    return " ".join(str(text).split()) if text else ""


def _format_single_event(event: dict[str, Any], tool_names: dict[str, str]) -> str | None:
    """One `recent_activity` line (≤ `RECENT_ACTIVITY_LINE_LIMIT` chars), or None when the event
    says nothing an orchestrator needs (a successful tool result — its call already said it)."""
    kind = event.get("kind")
    if kind == "tool_call":
        detail = event.get("command") or event.get("path") or event.get("input_preview") or ""
        line = f"{event.get('category') or 'tool'}  {event.get('tool') or ''}  {_one_line(detail)}"
    elif kind == "tool_result":
        if event.get("ok", True):
            return None
        name = tool_names.get(str(event.get("call_id", "")), "a tool call")
        output = event.get("output_tail") or ""
        first = next((ln for ln in str(output).splitlines() if ln.strip()), "")
        line = f"failed  {name}: {_one_line(first)}" if first else f"failed  {name}"
    elif kind == "assistant_text":
        line = f"text  {_one_line(event.get('text'))}"
    elif kind == "streaming":
        # The newest words are the progress; the head of a long block is old news.
        text = _one_line(event.get("text"))
        room = RECENT_ACTIVITY_LINE_LIMIT - len("text…  …")
        line = f"text…  …{text[-room:]}" if len(text) > room else f"text…  {text}"
    elif kind == "user_message":
        line = f"user  {_one_line(event.get('text'))}"
    elif kind == "notice":
        line = f"notice  {_one_line(event.get('text'))}"
    elif kind == "undelivered":
        line = f"undelivered  {_one_line(event.get('text') or event.get('reason'))}"
    else:
        return None
    line = line.rstrip()
    if len(line) > RECENT_ACTIVITY_LINE_LIMIT:
        line = line[: RECENT_ACTIVITY_LINE_LIMIT - 1] + "…"
    return line


def read_recent(path: Path, limit: int = RECENT_ACTIVITY_LIMIT) -> list[str]:
    """The last `limit` meaningful events as one-line strings, oldest first.

    Scans the log backwards (`_stream_events_backward`: 64 KiB chunks, ≤ 1 MiB, whole lines only,
    oversized and unparsable lines skipped). A block still being streamed shows as one `text…`
    line at the position of its newest delta. Never raises: a missing or unreadable log is `[]`.
    """
    if limit <= 0:
        return []
    try:
        events = list(reversed(_stream_events_backward(path)))
        tool_names = {
            str(e["call_id"]): str(e.get("tool") or "a tool call")
            for e in events
            if e.get("kind") == "tool_call" and e.get("call_id")
        }
        streaming = _streaming_text(events)
        timeline: list[dict[str, Any]] = []
        for position, event in enumerate(events):
            if event.get("kind") == "assistant_delta":
                if streaming is not None and position == streaming[0]:
                    timeline.append({"kind": "streaming", "text": streaming[1]})
                continue
            if event.get("kind") in MEANINGFUL_KINDS:
                timeline.append(event)
        lines = [line for e in timeline if (line := _format_single_event(e, tool_names))]
        return lines[-limit:]
    except Exception:  # bookkeeping must never change an outcome — a status call still answers
        log.debug("could not summarise recent activity from %s", path, exc_info=True)
        return []


def read_page(
    path: Path,
    limit: int,
    before_seq: int | None = None,
    after_seq: int | None = None,
    kinds: list[str] | None = None,
) -> PageResult:
    """Read a page of events from the events log.

    Default behavior (no before_seq, no after_seq): newest page.
    before_seq: exclusive upper bound - read events with seq < before_seq (older than).
    after_seq: exclusive lower bound - read events with seq > after_seq (newer than).
    Both at once -> raises ValueError.
    kinds=[] or unknown kind -> raises ValueError.
    limit must be 1..200 else raises ValueError.

    Each returned event is truncated recursively:
    - strings > 2000 chars cut with "..."
    - lists > 50 items cut
    - dict nesting > 4 replaced with "..."
    Events that were truncated get "truncated": true.

    Lines > 1 MiB are skipped (not parsed) and counted in skipped_oversized.
    Cursors are the seq of parsed events, so a skipped line never stalls paging.

    Page stops early once its serialized size would pass 256 KiB
    (has_more true, cursors still valid).
    A page always includes at least its first event.

    Missing file returns empty list, has_more=False.
    Never raises on I/O problems.
    """
    # Validate parameters
    if limit < 1 or limit > 200:
        raise ValueError(f"limit must be 1..200, got {limit}")

    if before_seq is not None and after_seq is not None:
        raise ValueError("both before_seq and after_seq cannot be specified")

    if kinds is not None:
        if not kinds:
            raise ValueError("kinds cannot be empty")
        for kind in kinds:
            if kind not in EVENT_KINDS:
                raise ValueError(
                    f"unknown kind {kind!r}; must be one of {sorted(EVENT_KINDS)}"
                )

    # Build kind filter set
    kind_filter: set[str] | None = None
    if kinds is not None:
        kind_filter = set(kinds)


    # If no kind filter, exclude assistant_delta by default
    if kind_filter is None:
        kind_filter = EVENT_KINDS - {"assistant_delta"}

    def matches(event: dict[str, Any]) -> bool:
        if event.get("kind") not in kind_filter:
            return False
        seq = event.get("seq")
        if not isinstance(seq, int):
            return False
        if before_seq is not None and seq >= before_seq:
            return False
        if after_seq is not None and seq <= after_seq:
            return False
        return True

    # Only the candidate page is ever held: forward keeps the first `limit` matches and stops at the
    # next one (that is `has_more`); backward keeps a sliding window of the last `limit`, counting
    # the rest.
    skipped = [0]
    matching = 0
    if after_seq is not None:
        events_to_return: list[dict[str, Any]] = []
        for event in _iter_events_forward(path, skipped):
            if matches(event):
                matching += 1
                if len(events_to_return) == limit:
                    break
                events_to_return.append(event)
    else:
        window: deque[dict[str, Any]] = deque(maxlen=limit)
        for event in _iter_events_forward(path, skipped):
            if matches(event):
                matching += 1
                window.append(event)
        events_to_return = list(window)
    skipped_oversized = skipped[0]

    # The budget trims the end farthest from the cursor, so whatever is left out stays reachable
    # from the returned page's own cursor: a newer-first fill for the default/`before_seq`
    # direction, an older-first fill for `after_seq`.
    newest_first = after_seq is None
    ordered = list(reversed(events_to_return)) if newest_first else events_to_return
    final_events: list[dict[str, Any]] = []
    page_size = 0
    for event in ordered:
        output_event: dict[str, Any] = {
            "seq": event.get("seq"),
            "kind": event.get("kind", ""),
            "observed_at": event.get("observed_at"),
        }
        data_fields = {
            k: v
            for k, v in event.items()
            if k not in ("v", "seq", "observed_at", "source_ts", "raw_offset", "task_id", "kind")
        }
        truncated_data, was_truncated = _truncate_value(data_fields)
        if was_truncated:
            output_event["truncated"] = True
        output_event.update(truncated_data)
        size = len(json.dumps(output_event, ensure_ascii=False, default=str).encode("utf-8"))
        if size > EVENT_MAX_BYTES:
            output_event = {
                "seq": output_event["seq"],
                "kind": output_event["kind"],
                "observed_at": output_event["observed_at"],
                "truncated": True,
                "oversized": True,
                "bytes": size,
            }
            size = len(json.dumps(output_event, default=str).encode("utf-8"))
        if final_events and page_size + size >= PAGE_BUDGET_BYTES:
            break
        final_events.append(output_event)
        page_size += size
    if newest_first:
        final_events.reverse()

    has_more = bool(final_events) and matching > len(final_events)

    # Set cursors
    next_before_seq = final_events[0].get("seq") if final_events else None
    next_after_seq = final_events[-1].get("seq") if final_events else None

    return PageResult(
        events=final_events,
        has_more=has_more,
        next_before_seq=next_before_seq,
        next_after_seq=next_after_seq,
        skipped_oversized=skipped_oversized,
    )
