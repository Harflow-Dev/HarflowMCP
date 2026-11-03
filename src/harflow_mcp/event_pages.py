"""Bidirectional bounded pages over a task's normalized event log (`task-events`).

`event_history.read_cursor_page` stays the reverse-only reader behind the MCP tool. This module adds
the ctl page: ascending events, an older and a newer cursor on every page, forward polling from a
byte boundary, and accounting for every consumed line so a client can prove continuity.

Boundaries are byte offsets just after a newline. The one exception is an oversized line (longer than
the scan window), which has no boundary of its own: a mid-line boundary carries `resync` and a
forward read discards bytes up to the next newline. Each call reads one window of at most
`SCAN_WINDOW` bytes plus a few 64-byte anchors and decodes at most `limit` lines.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import Any

from .event_history import _output
from .events import EVENT_KINDS, MAX_LINE_BYTES, _parse_event_line

GENERATION = "py-events-1"
CURSOR_VERSION = 2
DEFAULT_LIMIT = 100
MAX_LIMIT = 200
SCAN_WINDOW = 1_048_576  # a decodable line (MAX_LINE_BYTES) and its newline always fit
ANCHOR_BYTES = 64
CURSOR_CHARS = 2048
DEFAULT_KINDS = EVENT_KINDS - {"assistant_delta"}


class CursorInvalid(ValueError):
    """Malformed cursor, a cursor for another task or filter, or an unknown cursor version."""


class CursorExpired(ValueError):
    """A well-formed cursor that no longer applies: other generation, replaced or changed log."""


def identity_of(stat: os.stat_result) -> tuple[int, int]:
    """File identity that cursors bind to; one place to control it for deterministic fixtures."""
    return stat.st_dev, stat.st_ino


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def filter_digest(allowed: frozenset[str] | set[str]) -> str:
    return digest(json.dumps(sorted(allowed)).encode())


def _pread(fd: int, offset: int, length: int) -> bytes:
    chunks: list[bytes] = []
    while length > 0:
        chunk = os.pread(fd, length, offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
        length -= len(chunk)
    return b"".join(chunks)


def _anchors(fd: int, boundary: int) -> tuple[str, str, int]:
    """sha256 of the 64 bytes ending at `boundary`, of the first `min(64, boundary)` bytes, and that length."""
    length = min(ANCHOR_BYTES, boundary)
    return digest(_pread(fd, boundary - length, length)), digest(_pread(fd, 0, length)), length


@dataclass
class Line:
    """One consumed line (or an oversized span) of the log, in file order."""

    start: int
    end: int
    kind: str  # event | filtered | blank | malformed | oversized
    seq: int | None = None
    output: dict[str, Any] | None = None
    open_start: bool = False  # `start` may be inside a line
    open_end: bool = False  # `end` may be inside a line


@dataclass
class PageRead:
    fd: int
    task_id: str
    direction: str  # "older" (backward) or "newer" (forward)
    identity: tuple[int, int]
    size: int
    filter_key: str
    lines: list[Line]
    fallback: int  # older boundary when no line was consumed
    fallback_open: bool
    newer_fallback: int  # newer boundary when no line was consumed
    newer_fallback_open: bool
    has_newer: bool
    neighbour_seq: int | None  # accounted seq on the cursor's side, from the cursor this page was read with
    neighbour_skip: bool  # skipped lines lay between that seq and the boundary
    limit: int
    _anchor_cache: dict[int, tuple[str, str, int]] = field(default_factory=dict)

    @property
    def older_boundary(self) -> int:
        return self.lines[0].start if self.lines else self.fallback

    @property
    def newer_boundary(self) -> int:
        return self.lines[-1].end if self.lines else self.newer_fallback

    def trim(self) -> bool:
        """Drop the line farthest from the page's reading origin; False when none is left to drop.

        An older page gives up its oldest lines, a newer page its newest; the dropped lines stay
        reachable because the matching cursor boundary moves with them.
        """
        if len(self.lines) <= 1:
            return False  # one line is always kept: it carries the page's progress
        if self.direction == "older":
            self.lines.pop(0)
        else:
            self.lines.pop()
            self.has_newer = True
        return True

    def _cursor(self, boundary: int, direction: str, resync: bool, seq: int | None, skip: bool) -> str:
        if boundary not in self._anchor_cache:
            self._anchor_cache[boundary] = _anchors(self.fd, boundary)
        anchor, head, head_length = self._anchor_cache[boundary]
        value = {"v": CURSOR_VERSION, "gen": GENERATION, "dir": direction, "task": digest(self.task_id.encode()),
                 "dev": self.identity[0], "ino": self.identity[1], "path": path_digest(self.task_id),
                 "boundary": boundary, "anchor": anchor, "head": head, "hlen": head_length,
                 "filter": self.filter_key, "resync": resync, "seq": seq, "skip": skip}
        return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode()

    def render(self) -> dict[str, Any]:
        """The page's fields, recomputed from the lines currently held."""
        accounted = [line for line in self.lines if line.seq is not None]
        older_seq = accounted[0].seq if accounted else self.neighbour_seq
        newer_seq = accounted[-1].seq if accounted else self.neighbour_seq
        # Whether skipped lines lie between the cursor's seq and its boundary.
        hidden = [line.kind in {"malformed", "oversized"} for line in self.lines]
        indexes = [index for index, line in enumerate(self.lines) if line.seq is not None]
        inherit = lambda side: self.neighbour_skip and self.direction == side
        older_skip = any(hidden[:indexes[0]]) if indexes else any(hidden) or inherit("older")
        newer_skip = any(hidden[indexes[-1] + 1:]) if indexes else any(hidden) or inherit("newer")
        older, newer = self.older_boundary, self.newer_boundary
        older_open = self.lines[0].open_start if self.lines else self.fallback_open
        newer_open = self.lines[-1].open_end if self.lines else self.newer_fallback_open
        skipped = {"malformed": sum(line.kind == "malformed" for line in self.lines),
                   "oversized": sum(line.kind == "oversized" for line in self.lines)}
        return {
            "events": [line.output for line in self.lines if line.kind == "event"],
            "older_cursor": self._cursor(older, "older", older_open, older_seq, older_skip) if older > 0 else None,
            "newer_cursor": self._cursor(newer, "newer", newer_open, newer_seq, newer_skip),
            "has_older": older > 0,
            "has_newer": self.has_newer,
            "span": {"first_seq": accounted[0].seq, "last_seq": accounted[-1].seq} if accounted else None,
            "skipped": skipped,
            "gaps": self._gaps(),
        }

    def _gaps(self) -> list[dict[str, Any]]:
        gaps: list[dict[str, Any]] = []
        previous = self.neighbour_seq if self.direction == "newer" else None
        skipped = self.neighbour_skip and self.direction == "newer"
        for line in self.lines:
            if line.kind in {"malformed", "oversized"}:
                skipped = True
            elif line.seq is not None:
                if previous is not None and line.seq > previous + 1:
                    gaps.append({"after_seq": previous, "before_seq": line.seq,
                                 "reason": "unreadable_records" if skipped else "unknown"})
                previous, skipped = line.seq, False
        if self.direction == "older" and self.neighbour_seq is not None and previous is not None \
                and self.neighbour_seq > previous + 1:
            gaps.append({"after_seq": previous, "before_seq": self.neighbour_seq,
                         "reason": "unreadable_records" if skipped or self.neighbour_skip else "unknown"})
        return gaps[:self.limit]


def path_digest(task_id: str) -> str:
    return digest(f"{task_id}.events.jsonl".encode())


def parse_cursor(cursor: str, task_id: str, filter_key: str) -> dict[str, Any]:
    """Validate a cursor's shape, task and filter without touching the log."""
    if not isinstance(cursor, str) or not cursor or len(cursor) > CURSOR_CHARS:
        raise CursorInvalid("Invalid event cursor")
    try:
        value = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True))
    except (ValueError, UnicodeError, binascii.Error, RecursionError) as exc:
        raise CursorInvalid("Invalid event cursor") from exc
    if not isinstance(value, dict) or type(value.get("v")) is not int or value["v"] != CURSOR_VERSION:
        raise CursorInvalid("Unsupported event cursor version")
    for key in ("dev", "ino", "boundary"):
        if type(value.get(key)) is not int or not 0 <= value[key] < 2**64:
            raise CursorInvalid("Invalid event cursor boundary")
    if type(value.get("hlen")) is not int or not 0 <= value["hlen"] <= ANCHOR_BYTES:
        raise CursorInvalid("Invalid event cursor anchor")
    for key in ("task", "path", "anchor", "head", "filter"):
        if not isinstance(value.get(key), str) or len(value[key]) != 64:
            raise CursorInvalid("Invalid event cursor anchor")
    seq = value.get("seq", False)
    if not isinstance(value.get("dir"), str) or value["dir"] not in {"older", "newer"} \
            or type(value.get("resync")) is not bool or type(value.get("skip")) is not bool \
            or not isinstance(value.get("gen"), str) or len(value["gen"]) > 32 \
            or (seq is not None and (type(seq) is not int or not 0 <= seq < 2**63)):
        raise CursorInvalid("Invalid event cursor")
    if value["task"] != digest(task_id.encode()):
        raise CursorInvalid("Event cursor belongs to another task")
    if value["filter"] != filter_key:
        raise CursorInvalid("Event cursor filter changed")
    return value


def _verify(fd: int, task_id: str, issued: dict[str, Any], stat: os.stat_result) -> None:
    if issued["gen"] != GENERATION:
        raise CursorExpired("Event cursor generation is no longer served")
    if issued["path"] != path_digest(task_id) or (issued["dev"], issued["ino"]) != identity_of(stat):
        raise CursorExpired("Event log was replaced")
    boundary = issued["boundary"]
    if stat.st_size < boundary:
        raise CursorExpired("Event log was truncated")
    length = min(ANCHOR_BYTES, boundary)
    if issued["hlen"] != length or digest(_pread(fd, boundary - length, length)) != issued["anchor"] \
            or digest(_pread(fd, 0, length)) != issued["head"]:
        raise CursorExpired("Event log content changed")


def _classify(raw: bytes, start: int, end: int, allowed: frozenset[str]) -> Line:
    if not raw:
        return Line(start, end, "blank")
    if len(raw) > MAX_LINE_BYTES:
        return Line(start, end, "oversized")
    try:
        event = _parse_event_line(raw)
    except (ValueError, RecursionError):
        event = None
    seq = event.get("seq") if event else None
    if event is None or type(seq) is not int or seq < 0 or not isinstance(event.get("kind"), str):
        return Line(start, end, "malformed")
    if event["kind"] not in allowed:
        return Line(start, end, "filtered", seq)
    try:
        output = _output(event)
        json.dumps(output, ensure_ascii=False, default=str)
    except (ValueError, UnicodeError, RecursionError):
        return Line(start, end, "malformed")
    return Line(start, end, "event", seq, output)


def _backward(fd: int, end: int, limit: int, allowed: frozenset[str], *, resync: bool) -> list[Line]:
    """Whole lines ending at or before `end`, in file order.

    Bytes after the last newline are a fragment and are ignored: the tail of an oversized line when
    `resync` (a mid-line boundary), otherwise a partial trailing line.
    """
    start = max(0, end - SCAN_WINDOW)
    window = _pread(fd, start, end - start)
    if len(window) != end - start:
        raise OSError("event log shrank during read")
    last = window.rfind(b"\n")
    if last < 0:
        if start == 0:
            return []  # an incomplete first line only
        return [Line(start, end, "oversized", open_start=True, open_end=True)]
    line_end = last + 1
    first = window.find(b"\n") + 1 if start else 0
    if first >= line_end:
        if line_end == len(window):
            # The line ending at `end` is longer than the window: skip it a window at a time.
            return [Line(start, start + line_end, "oversized", open_start=True)]
        # The one newline ends a line that began before the window, and may be short. Consume only the
        # fragment after it so the next page, ending at that aligned boundary, decodes the line.
        if resync:
            return [Line(start + line_end, end, "oversized", open_end=True)]
        return [Line(start + line_end, start + line_end, "blank")]
    lines: list[Line] = []
    decoded = 0
    while line_end > first and decoded < limit:
        previous = window.rfind(b"\n", first, line_end - 1)
        line_start = previous + 1 if previous >= first else first
        item = _classify(window[line_start:line_end - 1], start + line_start, start + line_end, allowed)
        decoded += item.kind != "oversized"
        lines.append(item)
        line_end = line_start
    lines.reverse()
    return lines


def _forward(fd: int, begin: int, size: int, resync: bool, limit: int, allowed: frozenset[str]) -> tuple[list[Line], bool]:
    """Whole lines from `begin`; and whether complete lines remain beyond them."""
    stop = min(size, begin + SCAN_WINDOW)
    window = _pread(fd, begin, stop - begin)
    if len(window) != stop - begin:
        raise OSError("event log shrank during read")
    at_eof = stop == size
    lines: list[Line] = []
    position = 0
    if resync:
        newline = window.find(b"\n")
        if newline < 0:
            return ([Line(begin, stop, "oversized", open_start=True, open_end=True)] if window else []), not at_eof
        lines.append(Line(begin, begin + newline + 1, "oversized", open_start=True))
        position = newline + 1
    decoded = 0
    while position < len(window) and decoded < limit:
        newline = window.find(b"\n", position)
        if newline < 0:
            break
        item = _classify(window[position:newline], begin + position, begin + newline + 1, allowed)
        decoded += item.kind != "oversized"
        lines.append(item)
        position = newline + 1
    if not lines and not at_eof and b"\n" not in window:
        return [Line(begin, stop, "oversized", open_end=True)], True
    return lines, window.find(b"\n", position) >= 0 or not at_eof


def _complete_line_after(fd: int, boundary: int, size: int) -> bool:
    """Whether a newline lies within one scan window after `boundary`; a longer single line reads as absent."""
    length = min(SCAN_WINDOW, size - boundary)
    return length > 0 and b"\n" in _pread(fd, boundary, length)


def read_event_page(fd: int, task_id: str, *, issued: dict[str, Any] | None, from_oldest: bool, limit: int,
                    allowed: frozenset[str]) -> PageRead:
    """One page: newest (default), oldest-forward, or the page a validated cursor names.

    Raises `CursorExpired` when a cursor no longer applies, `OSError` when the log cannot be read
    consistently (the caller reports an unavailable log).
    """
    stat = os.fstat(fd)
    size = stat.st_size
    if issued is not None:
        _verify(fd, task_id, issued, stat)
    backward = issued["dir"] == "older" if issued is not None else not from_oldest
    boundary = issued["boundary"] if issued is not None else (size if backward else 0)
    resync = issued["resync"] if issued is not None else False
    if backward:
        lines = _backward(fd, boundary, limit, allowed, resync=resync)
        has_newer = issued is not None and _complete_line_after(fd, boundary, size)
        # With nothing consumed an older page still reports where it was asked to start.
        fallback, fallback_open = 0, False
        newer_fallback, newer_open = (boundary, resync) if issued is not None else (0, False)
    else:
        lines, has_newer = _forward(fd, boundary, size, resync, limit, allowed)
        fallback, fallback_open = boundary, resync
        newer_fallback, newer_open = boundary, resync
    # Anything an issued cursor vouched for must be unchanged after the read.
    after = os.fstat(fd)
    if identity_of(after) != identity_of(stat) or after.st_size < size:
        raise CursorExpired("Event log changed during read") if issued is not None else OSError("event log changed during read")
    if issued is not None:
        _verify(fd, task_id, issued, after)
    return PageRead(fd, task_id, "older" if backward else "newer", identity_of(stat), size, filter_digest(allowed),
                    lines, fallback, fallback_open, newer_fallback, newer_open, has_newer,
                    issued["seq"] if issued is not None else None, issued["skip"] if issued is not None else False, limit)
