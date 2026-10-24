"""Pure qualification of persisted next-continuation follow-up ledgers."""
from __future__ import annotations

import copy
import re
from typing import Any

from .state_compatibility import persisted_compatible
from .wire_contracts import PRODUCT, PRODUCT_VERSION
from .workflow_run_documents import _json

FOLLOWUP_FORMAT = "harflow.task.followups"
STATUSES = frozenset({"queued", "reserved", "consumed", "cancelled", "undeliverable", "delivery_unknown"})


class FollowupDocumentError(Exception):
    """A document cannot provide qualified follow-up authority."""

    def __init__(self, task_id: str, reason: str):
        self.task_id, self.reason = task_id, reason
        super().__init__(f"{task_id}: {reason}")


def validate(ledger: Any) -> None:
    """Validate the existing inner ledger shape without normalizing caller data."""
    if not isinstance(ledger, dict) or type(ledger.get("v")) is not int or ledger["v"] != 1:
        raise ValueError("Invalid follow-up ledger version")
    seq, omitted = ledger.get("next_seq"), ledger.get("omitted_final")
    if type(seq) is not int or seq < 1 or type(omitted) is not int or omitted < 0:
        raise ValueError("Invalid follow-up ledger counters")
    if ledger.get("sealed_by") is not None and type(ledger["sealed_by"]) is not str:
        raise ValueError("Invalid follow-up ledger seal")
    if type(ledger.get("items")) is not list:
        raise ValueError("Invalid follow-up ledger items")
    seen = set()
    for item in ledger["items"]:
        if type(item) is not dict:
            raise ValueError("Invalid follow-up item")
        key, status = item.get("id"), item.get("status")
        if type(key) is not str or key in seen or type(item.get("seq")) is not int:
            raise ValueError("Invalid follow-up item identity")
        seen.add(key)
        if type(status) is not str or status not in STATUSES:
            raise ValueError("Invalid follow-up item status")
        if status not in {"consumed", "cancelled"} and type(item.get("text")) is not str:
            raise ValueError("Invalid follow-up item text")
        for field in ("text", "queued_at", "at", "reason", "successor_task_id"):
            if field in item and item[field] is not None and type(item[field]) is not str:
                raise ValueError("Invalid optional follow-up text field")
        for field in ("owner", "by"):
            if field in item and item[field] is not None and type(item[field]) is not dict:
                raise ValueError("Invalid optional follow-up identity field")
    _json(dict(ledger))


def decode(document: Any, task_id: str) -> dict[str, Any]:
    """Return the qualified inner ledger with additive data preserved."""
    try:
        if type(task_id) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", task_id) is None:
            raise ValueError("Invalid follow-up task identifier")
        if type(document) is not dict or document.get("product") != PRODUCT or document.get("format") != FOLLOWUP_FORMAT:
            raise ValueError("Missing or foreign follow-up product/format")
        if type(document.get("v")) is not int or document["v"] != 1:
            raise ValueError("Unsupported follow-up version")
        if not persisted_compatible(document.get("product_version"), FOLLOWUP_FORMAT):
            raise ValueError("Incompatible follow-up product major/minor line")
        if document.get("task_id") != task_id:
            raise ValueError("Follow-up task_id does not match its filename")
        validate(document.get("ledger"))
        _json(document)
        return copy.deepcopy(document["ledger"])
    except (ValueError, TypeError, RecursionError) as exc:
        raise FollowupDocumentError(task_id, "Follow-up document failed qualification") from exc


def encode(ledger: dict[str, Any], task_id: str, *, envelope: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        document = copy.deepcopy(envelope) if envelope is not None else {}
        document.update(product=PRODUCT, format=FOLLOWUP_FORMAT, v=1, product_version=PRODUCT_VERSION,
                        task_id=task_id, ledger=copy.deepcopy(dict(ledger)))
        decode(document, task_id)
        return document
    except (ValueError, TypeError, RecursionError) as exc:
        raise FollowupDocumentError(task_id, "Follow-up candidate failed qualification") from exc
