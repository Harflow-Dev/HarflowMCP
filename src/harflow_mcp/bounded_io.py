"""Bounded JSON reads for derived listing metadata and ownership receipts."""
from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any

RECEIPT_BYTES = 16 * 1024


class ReadLimit(ValueError):
    pass


def read_json(path: Path, limit: int, *, budget: Any = None) -> Any:
    if budget is not None and getattr(budget, 'metadata_decodes', 0) >= 100:
        raise ReadLimit('Metadata decode request limit reached')
    available = limit if budget is None else min(limit, max(0, budget.metadata_limit - budget.metadata_bytes))
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ReadLimit('JSON source must be a regular file')
        if info.st_size > available:
            raise ReadLimit('JSON exceeds bounded read budget')
        with os.fdopen(fd, 'rb', buffering=0, closefd=False) as source:
            content = source.read(available)
        grew = os.fstat(fd).st_size > available
    finally:
        os.close(fd)
    if budget is not None:
        budget.metadata_bytes += len(content)
    if grew:
        raise ReadLimit('JSON exceeds bounded read budget')
    if budget is not None and hasattr(budget, 'metadata_decodes'):
        budget.metadata_decodes += 1
    try:
        return json.loads(content)
    except RecursionError as exc:
        raise ReadLimit('JSON nesting exceeds decoder bounds') from exc


def read_receipt(path: Path) -> dict[str, Any]:
    value = read_json(path, RECEIPT_BYTES)
    if not isinstance(value, dict):
        raise ValueError('Ownership receipt must be an object')
    return value
