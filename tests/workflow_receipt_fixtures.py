"""Helpers for persisted ownership-receipt fixtures.

Positive persisted receipts must go through the production envelope codec so
tests exercise the same qualification boundary as the store writer. Tests
that intentionally cover malformed, dangling, oversized, or public inner
receipts should continue constructing those values directly.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from harflow_mcp import workflow_receipt_documents


def qualified_receipt(task_id: str, workflow_run_id: str, **fields: Any) -> dict[str, Any]:
    """Return a qualified persisted document for ``task_id``."""

    receipt = {"workflow_run_id": workflow_run_id, **copy.deepcopy(fields)}
    return workflow_receipt_documents.encode(receipt, task_id)


def persist_receipt(path: Path, task_id: str, workflow_run_id: str, **fields: Any) -> None:
    """Write a qualified persisted receipt fixture at ``path``."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(qualified_receipt(task_id, workflow_run_id, **fields)),
        encoding="utf-8",
    )
