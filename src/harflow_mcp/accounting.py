"""Bounded execution accounting; unknown evidence never becomes a zero."""
from __future__ import annotations

import json
import math
from typing import Any

CATEGORIES = ("input_tokens", "output_tokens", "cache_read_tokens", "cache_creation_tokens")
MAX_COUNT = 2**63 - 1


def contribution_id(task_id: str) -> str:
    return "task:" + task_id


def unavailable(task_id: str, reason: str = "unavailable", *, state: str = "unavailable") -> dict:
    return {"version": 1, "contribution_id": contribution_id(task_id), "state": state,
            "scope": "unknown", "additive": False, "categories": dict.fromkeys(CATEGORIES),
            "total_tokens": None, "qualified_wait_intervals": [], "diagnostics": [str(reason)[:256]]}


def validate(value: Any) -> dict:
    """Validate a complete v1 snapshot without coercion or unbounded nested payloads."""
    if type(value) is not dict or len(json.dumps(value, allow_nan=False).encode()) > 16384:
        raise ValueError("Malformed accounting bounds")
    required = {"version", "contribution_id", "state", "scope", "additive", "categories",
                "total_tokens", "qualified_wait_intervals", "diagnostics"}
    if set(value) != required or type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("Unsupported accounting shape/version")
    if type(value["contribution_id"]) is not str or not 1 <= len(value["contribution_id"]) <= 256:
        raise ValueError("Malformed contribution identity")
    if type(value["state"]) is not str or type(value["scope"]) is not str or value["state"] not in {"available", "pending", "unavailable"} or value["scope"] not in {"execution", "unknown"}:
        raise ValueError("Malformed accounting qualification")
    if type(value["additive"]) is not bool:
        raise ValueError("Malformed additive qualification")
    categories = value["categories"]
    if type(categories) is not dict or set(categories) != set(CATEGORIES):
        raise ValueError("Malformed token categories")
    for number in [*categories.values(), value["total_tokens"]]:
        if number is not None and (type(number) is not int or not 0 <= number <= MAX_COUNT):
            raise ValueError("Malformed token count")
    total = value["total_tokens"]
    if total is not None and (not value["additive"] or value["scope"] != "execution"
                             or any(v is None for v in categories.values()) or total != sum(categories.values())):
        raise ValueError("Unqualified comparable token total")
    if value["additive"] and (value["scope"] != "execution" or value["state"] != "available"):
        raise ValueError("Unqualified additive accounting")
    diagnostics = value["diagnostics"]
    if type(diagnostics) is not list or len(diagnostics) > 16 or any(type(v) is not str or len(v) > 256 for v in diagnostics):
        raise ValueError("Malformed accounting diagnostics")
    intervals = value["qualified_wait_intervals"]
    if type(intervals) is not list or len(intervals) > 128:
        raise ValueError("Malformed wait intervals")
    for interval in intervals:
        if type(interval) is not dict or set(interval) != {"start", "end", "execution_id", "provenance", "qualified"}:
            raise ValueError("Malformed wait interval")
        if interval["qualified"] is not True or not all(_number(interval[k]) for k in ("start", "end")) or interval["end"] < interval["start"]:
            raise ValueError("Unqualified wait interval")
        if any(type(interval[k]) is not str or not 1 <= len(interval[k]) <= 256 for k in ("execution_id", "provenance")):
            raise ValueError("Malformed wait provenance")
    return value


def normalize(task_id: str, usage: Any, *, fields: dict, scope: str = "unknown",
              disjoint: bool = False, diagnostics=()) -> dict:
    """Map backend-qualified fields. Adapters own category and accumulation semantics."""
    result = unavailable(task_id)
    result["scope"] = scope
    result["diagnostics"] = [str(v)[:256] for v in diagnostics][:16]
    usage = usage if type(usage) is dict else {}
    for category in CATEGORIES:
        field = fields.get(category)
        value = usage.get(field) if field else None
        if type(value) is int and 0 <= value <= MAX_COUNT:
            result["categories"][category] = value
    result["state"] = "available" if any(v is not None for v in result["categories"].values()) else "unavailable"
    result["additive"] = scope == "execution" and result["state"] == "available"
    if disjoint and result["additive"] and all(v is not None for v in result["categories"].values()):
        total = sum(result["categories"].values())
        if total <= MAX_COUNT:
            result["total_tokens"] = total
    return validate(result)


def snapshot(backend: Any, acc: Any, context: dict) -> dict:
    """Accounting is bookkeeping: evidence errors cannot prevent terminal persistence."""
    task_id = context["task_id"]
    if not context.get("observed", True):
        return unavailable(task_id, "outcome_unobserved")
    try:
        evidence = validate(backend.accounting_evidence(acc, context))
        if evidence["contribution_id"] != contribution_id(task_id):
            raise ValueError("Contribution belongs to another execution")
        if not context.get("terminal") and evidence["state"] == "unavailable":
            return unavailable(task_id, "normalization_pending", state="pending")
        return evidence
    except Exception:
        return unavailable(task_id, "accounting_evidence_error")


def _number(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def duration(start: float, end: float, intervals=()) -> dict:
    """Union closed proven pauses, clamp them to the execution and retain elapsed fallback."""
    if not _number(start) or not _number(end) or end < start:
        return {"elapsed_seconds": None, "execution_seconds": None, "wait_seconds": None,
                "time_qualification": "unavailable"}
    spans = []
    for interval in list(intervals)[:128]:
        if not isinstance(interval, dict) or interval.get("qualified") is not True or not interval.get("execution_id") or not interval.get("provenance"):
            continue
        a, b = interval.get("start"), interval.get("end")
        if _number(a) and _number(b) and b >= a:
            a, b = max(start, a), min(end, b)
            if b > a:
                spans.append((a, b))
    merged = []
    for a, b in sorted(spans):
        if merged and a <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    wait = sum(b - a for a, b in merged)
    return {"elapsed_seconds": end - start, "execution_seconds": end - start - wait,
            "wait_seconds": wait, "time_qualification": "qualified_waits" if merged else "elapsed_may_include_waits"}
