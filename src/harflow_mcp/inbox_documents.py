"""Pure qualification of private persisted live-input inbox rows."""
from __future__ import annotations

import copy
import re
from typing import Any

from .wire_contracts import PRODUCT, PRODUCT_VERSION, compatible_line
from .workflow_run_documents import _json

INBOX_FORMAT = "harflow.task.inbox"
_TASK = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")


class InboxDocumentError(Exception):
    """An inbox row cannot provide qualified message or seal payload."""


def _kind(payload: Any) -> str:
    if type(payload) is not dict:
        raise ValueError("Inbox payload must be an object")
    if payload.get("closed") is True and "id" not in payload:
        return "seal"
    if isinstance(payload.get("id"), str) and isinstance(payload.get("text"), str):
        return "message"
    raise ValueError("Invalid inbox payload")


def decode(document: Any, task_id: str) -> dict[str, Any]:
    """Return a qualified inner row, preserving optional and additive finite JSON."""
    try:
        if type(task_id) is not str or _TASK.fullmatch(task_id) is None:
            raise ValueError("Invalid inbox task identifier")
        if type(document) is not dict or document.get("product") != PRODUCT or document.get("format") != INBOX_FORMAT:
            raise ValueError("Missing or foreign inbox product/format")
        if type(document.get("v")) is not int or document["v"] != 1 or not compatible_line(document.get("product_version")):
            raise ValueError("Incompatible inbox version")
        if document.get("task_id") != task_id:
            raise ValueError("Inbox task mismatch")
        if document.get("kind") != _kind(document.get("payload")):
            raise ValueError("Inbox kind mismatch")
        _json(document)
        return copy.deepcopy(document["payload"])
    except (ValueError, TypeError, RecursionError) as exc:
        raise InboxDocumentError("Inbox document failed qualification") from exc


def encode(payload: dict[str, Any], task_id: str) -> dict[str, Any]:
    """Build a qualified candidate without modifying or coercing supplied JSON values."""
    try:
        document = dict(product=PRODUCT, format=INBOX_FORMAT, v=1, product_version=PRODUCT_VERSION,
                        task_id=task_id, kind=_kind(payload), payload=copy.deepcopy(payload))
        decode(document, task_id)
        return document
    except (ValueError, TypeError, RecursionError) as exc:
        raise InboxDocumentError("Inbox candidate failed qualification") from exc
