"""Pure qualification of persisted workflow ownership receipts."""
from __future__ import annotations

import copy
from typing import Any

from .state_compatibility import persisted_compatible
from .wire_contracts import PRODUCT, PRODUCT_VERSION
from .workflow_run_documents import identifier, _json

RECEIPT_FORMAT = "harflow.workflow.receipt"


class WorkflowReceiptDocumentError(Exception):
    """An existing receipt cannot provide qualified ownership authority."""

    def __init__(self, task_id: str, reason: str, *, code: str = "workflow_receipt_incompatible", workflow_run_id: str | None = None):
        self.task_id, self.reason, self.code = task_id, reason, code
        self.workflow_run_id = workflow_run_id
        super().__init__(f"{task_id}: {reason}")

    def as_dict(self) -> dict[str, Any]:
        result = {"task_id": self.task_id, "code": self.code, "reason": self.reason}
        if self.workflow_run_id is not None:
            result["workflow_run_id"] = self.workflow_run_id
        return result


def _payload(value: Any) -> None:
    if type(value) is not dict:
        raise ValueError("receipt must be an object")
    try:
        _json(value)
    except (ValueError, RecursionError):
        raise ValueError("receipt must contain finite JSON values") from None
    try:
        identifier(value.get("workflow_run_id"))
    except ValueError:
        raise ValueError("Invalid receipt.workflow_run_id") from None
    for field in ("workflow_node_id", "workflow_execution_id", "workflow_role", "workflow_name", "workflow_status", "execution_contract", "interaction_owner"):
        if field in value and value[field] is not None and type(value[field]) is not str:
            raise ValueError(f"Invalid receipt.{field}")
    for field in ("root_workflow_run_id", "workflow_session_owner_run_id"):
        if field in value and value[field] is not None:
            try:
                identifier(value[field])
            except ValueError:
                raise ValueError(f"Invalid receipt.{field}") from None


def decode(document: Any, task_id: str) -> dict[str, Any]:
    """Return qualified inner ownership without adding optional fields."""
    try:
        identifier(task_id)
        if type(document) is not dict or document.get("product") != PRODUCT or document.get("format") != RECEIPT_FORMAT:
            raise ValueError("Missing or foreign receipt product/format")
        if type(document.get("v")) is not int or document["v"] != 1:
            raise ValueError("Unsupported receipt version")
        if not persisted_compatible(document.get("product_version"), RECEIPT_FORMAT):
            raise ValueError("Incompatible receipt product major/minor line")
        if document.get("task_id") != task_id:
            raise ValueError("Receipt task_id does not match its filename")
        _payload(document.get("receipt"))
        _json(document)
        return copy.deepcopy(document["receipt"])
    except (ValueError, RecursionError) as exc:
        reason = str(exc) if isinstance(exc, ValueError) else "Receipt nesting exceeds qualification bounds"
        raise WorkflowReceiptDocumentError(task_id, reason) from exc


def encode(receipt: dict[str, Any], task_id: str) -> dict[str, Any]:
    document = {"product": PRODUCT, "format": RECEIPT_FORMAT, "v": 1, "product_version": PRODUCT_VERSION, "task_id": task_id, "receipt": copy.deepcopy(receipt)}
    decode(document, task_id)
    return document
