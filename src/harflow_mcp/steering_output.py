"""Bounded, whole-final steering grammar. No transcript or substring commands."""
from __future__ import annotations

import json
import math
import re
from typing import Any

START = "HARFLOW_COMMIT_V1"
END = "END_HARFLOW_COMMIT_V1"
ASCII = " \t\r\n\v\f"
MAX_FINAL_BYTES = 64 * 1024


def strict_loads(text: str) -> Any:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result
    def nonfinite(value):
        raise ValueError("Nonfinite JSON number")
    def floating(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("Nonfinite JSON number")
        return parsed
    return json.loads(text, object_pairs_hook=pairs, parse_constant=nonfinite, parse_float=floating)


def _shaped(value):
    return isinstance(value, dict) and bool(set(value) & {"decision_id", "action", "definition", "episode_id", "turn_id", "commit_target_ref", "steering_revision", "interaction_epoch", "checkpoint_revision", "goal_revision", "role", "command"} or "status" in value and ("result" in value or "evidence" in value))


def classify_final(text: str) -> dict[str, Any]:
    """Return conversation/command/refused; preserve caller's raw text separately."""
    def refuse(code):
        return {"kind": "refused", "code": code}
    if not isinstance(text, str):
        return refuse("capture_unknown")
    try:
        if len(text.encode("utf-8", errors="strict")) > MAX_FINAL_BYTES:
            return refuse("output_too_large")
    except UnicodeError:
        return refuse("ambiguous_output")
    body = text.strip("\r\n\v\f")
    body = body.rstrip(" \t")
    # Surrounding whitespace cannot turn an indented delimiter into an exact frame.
    if body.startswith(("\t" + START, "    " + START)):
        return refuse("commit_frame_required")
    if re.match(r"^ {1,3}HARFLOW_COMMIT", body):
        return refuse("malformed_command")
    body = body.lstrip(" \t")
    if not body:
        return refuse("empty_output")
    lines = body.replace("\r\n", "\n").split("\n")
    if lines[0] == START and lines[-1] == END:
        if sum(line == START or line == END for line in lines) != 2:
            return refuse("conflicting_command")
        try:
            value = strict_loads("\n".join(lines[1:-1]))
            if type(value) is not dict:
                return refuse("malformed_command")
        except (ValueError, TypeError, RecursionError):
            return refuse("malformed_command")
        return {"kind": "command", "command": value}
    # Whole fenced/indented command documents need an explicit commit frame too.
    fenced = re.fullmatch(r" {0,3}(`{3,}|~{3,})[^\n]*\n([\s\S]*?)\n {0,3}([`~]+) *", body)
    if fenced and fenced[1][0] == fenced[3][0] and len(fenced[3]) >= len(fenced[1]):
        inner = fenced[2].strip(ASCII)
        if inner.startswith((START, END, "{" , "[")):
            if inner.startswith((START, END)):
                return refuse("commit_frame_required")
            try:
                parsed = strict_loads(inner)
            except (ValueError, RecursionError):
                return refuse("malformed_command")
            if _shaped(parsed):
                return refuse("commit_frame_required")
    if body.startswith(("{", "[")):
        try:
            parsed = strict_loads(body)
        except (ValueError, RecursionError):
            return refuse("malformed_command")
        if _shaped(parsed):
            return refuse("commit_frame_required")
    fence = None
    ambiguous = False
    reserved = []
    for line in lines:
        opening = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
        if fence:
            if re.fullmatch(r" {0,3}" + re.escape(fence[0]) + "{" + str(fence[1]) + r",} *", line):
                fence = None
            continue
        if opening and not (opening[1][0] == "`" and "`" in opening[2]):
            fence = (opening[1][0], len(opening[1]))
            continue
        if re.match(r"^( {0,3}>|\t| {4})", line):
            continue
        # Strip only positively closed inline spans. An unclosed span is ambiguous.
        out = ""
        pos = 0
        for match in re.finditer(r"`+", line):
            if match.start() < pos:
                continue
            out += line[pos:match.start()]
            close = re.search(r"(?<!`)" + re.escape(match[0]) + r"(?!`)", line[match.end():])
            if close is None:
                ambiguous = True
                out += line[match.start():]
                pos = len(line)
                break
            pos = match.end() + close.end()
        out += line[pos:]
        if re.match(r"^ {0,3}(?:HARFLOW_COMMIT|END_HARFLOW_COMMIT)", out):
            reserved.append(out)
    if (fence or ambiguous) and ("HARFLOW_COMMIT" in body or re.search(r'"(?:definition|decision_id|command)"\s*:', body)):
        return refuse("ambiguous_output")
    if reserved:
        return refuse("conflicting_command" if len(reserved) > 2 else "malformed_command")
    return {"kind": "conversation"}
