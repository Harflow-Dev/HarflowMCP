"""Just enough JSONC to *read* opencode's config: comments and trailing commas removed, then `json`.

Read-only by design. Writing JSONC back would mean preserving the comments this throws away, which is
exactly why registration goes through `opencode mcp add` instead of an edit here.

A small tokenizer rather than a regex, because the one thing that has to be right is strings: `//`
inside `"https://…"` is not a comment, a `/*` inside a string opens nothing, and a `,` before `}` inside
a string is content. Anything malformed that the tokenizer can see (an unterminated comment or string)
raises `ValueError`; everything else is left for `json.loads` to reject.
"""

from __future__ import annotations

import json
from typing import Any


def strip(text: str) -> str:
    """`text` with comments and trailing commas removed, and every other character kept."""
    out: list[str] = []
    pending_comma: int | None = None
    i = 0
    n = len(text)
    if text.startswith("﻿"):
        i = 1

    while i < n:
        char = text[i]

        if char == '"':
            end = _string_end(text, i)
            out.append(text[i:end])
            pending_comma = None
            i = end
            continue

        if text.startswith("//", i):
            newline = text.find("\n", i)
            i = n if newline == -1 else newline
            continue

        if text.startswith("/*", i):
            close = text.find("*/", i + 2)
            if close == -1:
                raise ValueError(f"unterminated block comment at offset {i}")
            # A space, not nothing: `1/**/2` must not become `12`.
            out.append(" ")
            i = close + 2
            continue

        if char in " \t\r\n":
            out.append(char)
            i += 1
            continue

        if char in "}]" and pending_comma is not None:
            out[pending_comma] = ""
        if char == ",":
            pending_comma = len(out)
        else:
            pending_comma = None
        out.append(char)
        i += 1

    return "".join(out)


def loads(text: str) -> Any:
    return json.loads(strip(text))


def _string_end(text: str, start: int) -> int:
    """Index just past the string opening at `start`, honouring backslash escapes."""
    i = start + 1
    while i < len(text):
        if text[i] == "\\":
            i += 2
            continue
        if text[i] == '"':
            return i + 1
        i += 1
    raise ValueError(f"unterminated string at offset {start}")
