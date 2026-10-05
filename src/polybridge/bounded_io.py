"""Bounded JSON reads for derived listing metadata and ownership receipts."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

RECEIPT_BYTES = 16 * 1024


class ReadLimit(ValueError):
    pass


def read_json(path: Path, limit: int, *, budget: Any = None) -> Any:
    available = limit if budget is None else min(limit, max(0, budget.metadata_limit - budget.metadata_bytes))
    with path.open('rb') as source:
        if os.fstat(source.fileno()).st_size > available:
            raise ReadLimit('JSON exceeds bounded read budget')
        content = source.read(available)
        grew = os.fstat(source.fileno()).st_size > available
    if budget is not None:
        budget.metadata_bytes += len(content)
    if grew:
        raise ReadLimit('JSON exceeds bounded read budget')
    return json.loads(content)


def read_receipt(path: Path) -> dict[str, Any]:
    value = read_json(path, RECEIPT_BYTES)
    if not isinstance(value, dict):
        raise ValueError('Ownership receipt must be an object')
    return value
