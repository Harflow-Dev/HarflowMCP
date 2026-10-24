"""Backend-neutral bounds for dispatch-time `allowed_commands`; each backend owns its own grammar."""
from __future__ import annotations

import unicodedata
from collections.abc import Iterable

from .backends.base import UnsupportedCapability

MAX_PATTERNS = 16
MAX_PATTERN_BYTES = 256
MAX_TOTAL_BYTES = 2048


class CommandGrantError(UnsupportedCapability):
    """A refused `allowed_commands` request; the message names the pattern and the reason."""


def check_bounds(patterns: Iterable[object]) -> tuple[str, ...]:
    """Validate shape and size only: at most 16 patterns, 256 bytes each and 2048 in total, no control characters,
    no outer whitespace, no empty entries and no duplicates."""
    if isinstance(patterns, (str, bytes)) or not isinstance(patterns, (list, tuple)):
        raise CommandGrantError("allowed_commands must be a list of strings")
    if len(patterns) > MAX_PATTERNS:
        raise CommandGrantError(f"allowed_commands accepts at most {MAX_PATTERNS} patterns, got {len(patterns)}")
    seen: set[str] = set()
    total = 0
    for pattern in patterns:
        if type(pattern) is not str:
            raise CommandGrantError("allowed_commands entries must be strings")
        if not pattern:
            raise CommandGrantError("allowed_commands entries must not be empty")
        if pattern != pattern.strip():
            raise CommandGrantError(f"allowed_commands pattern {pattern!r} has leading or trailing whitespace")
        if any(unicodedata.category(ch).startswith("C") or unicodedata.category(ch) in ("Zl", "Zp") for ch in pattern):
            raise CommandGrantError(f"allowed_commands pattern {pattern!r} contains a control character")
        size = len(pattern.encode("utf-8"))
        if size > MAX_PATTERN_BYTES:
            raise CommandGrantError(f"allowed_commands pattern {pattern[:32]!r}... is {size} bytes; the limit is {MAX_PATTERN_BYTES}")
        if pattern in seen:
            raise CommandGrantError(f"allowed_commands pattern {pattern!r} is duplicated")
        seen.add(pattern)
        total += size
    if total > MAX_TOTAL_BYTES:
        raise CommandGrantError(f"allowed_commands total size is {total} bytes; the limit is {MAX_TOTAL_BYTES}")
    return tuple(patterns)


def normalize(patterns: Iterable[object] | None) -> tuple[str, ...] | None:
    """None stays None (inherit); a list becomes a validated tuple (replace)."""
    return None if patterns is None else check_bounds(patterns)  # type: ignore[arg-type]
