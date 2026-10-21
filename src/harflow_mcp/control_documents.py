"""Pure qualification of private persisted control-phase documents."""
from __future__ import annotations

import copy
import re
from typing import Any

from .wire_contracts import PRODUCT, PRODUCT_VERSION, compatible_line
from .workflow_run_documents import _json

CONTROL_FORMAT = "harflow.task.control"
_COMPONENT = re.compile(r"[A-Za-z0-9_-]+")
_TASK = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")


class ControlDocumentError(Exception):
    """A phase document cannot provide qualified payload authority."""


def validate_identity(task_id: str, family: str, attempt: int, phase: str) -> None:
    """Accept safe generic names and positive attempts, independently of discovery limits."""
    if type(task_id) is not str or _TASK.fullmatch(task_id) is None:
        raise ValueError("Invalid control task identifier")
    if any(type(value) is not str or _COMPONENT.fullmatch(value) is None for value in (family, phase)):
        raise ValueError("Invalid control filename component")
    if type(attempt) is not int or attempt < 1:
        raise ValueError("Invalid control attempt")


def validate(payload: Any, family: str, phase: str) -> None:
    """Check fields belonging to known phases; generic payloads remain opaque JSON."""
    if type(payload) is not dict:
        raise ValueError("Control payload must be an object")
    fields: dict[str, tuple[type, ...]] = {}
    if family == "cancel":
        if phase == "req" or re.fullmatch(r"join-[0-9a-f]{32}", phase):
            fields = {"at": (str,), "by": (dict, type(None)), "lease_seconds": (int, float)}
        elif phase == "sig":
            fields = {"at": (str,), "leader_alive": (bool,)}
        elif phase == "failed" or re.fullmatch(r"nosig-[0-9a-f]{32}", phase):
            fields = {"at": (str,), "reason": (str,)}
    elif family == "takeover":
        if phase == "req":
            fields = {"at": (str,), "by": (dict, type(None)), "lease_seconds": (int, float), "session_id": (str, type(None))}
        elif phase == "ready":
            fields = {"at": (str,)}
        elif phase == "failed":
            fields = {"at": (str,), "reason": (str,)}
        elif phase == "attach":
            fields = {"at": (str,), "pid": (int,), "start_time": (str, type(None)), "markers": (list,)}
    for field, kinds in fields.items():
        if field in payload and type(payload[field]) not in kinds:
            raise ValueError("Invalid supplied control field")
    if "pid" in fields and "pid" in payload and payload["pid"] < 1:
        raise ValueError("Invalid control process identifier")
    if "markers" in fields and "markers" in payload and any(type(item) is not str for item in payload["markers"]):
        raise ValueError("Invalid control process markers")
    _json(payload)


def decode(document: Any, task_id: str, family: str, attempt: int, phase: str) -> dict[str, Any]:
    """Return the inner payload, preserving additive finite JSON data."""
    try:
        validate_identity(task_id, family, attempt, phase)
        if type(document) is not dict or document.get("product") != PRODUCT or document.get("format") != CONTROL_FORMAT:
            raise ValueError("Missing or foreign control product/format")
        if type(document.get("v")) is not int or document["v"] != 1 or not compatible_line(document.get("product_version")):
            raise ValueError("Incompatible control version")
        if (document.get("task_id"), document.get("family"), document.get("attempt"), document.get("phase")) != (task_id, family, attempt, phase) or type(document.get("attempt")) is not int:
            raise ValueError("Control identity mismatch")
        validate(document.get("payload"), family, phase)
        _json(document)
        return copy.deepcopy(document["payload"])
    except (ValueError, TypeError, RecursionError) as exc:
        raise ControlDocumentError("Control document failed qualification") from exc


def encode(payload: dict[str, Any], task_id: str, family: str, attempt: int, phase: str) -> dict[str, Any]:
    """Build a qualified candidate without changing the supplied payload."""
    try:
        document = dict(product=PRODUCT, format=CONTROL_FORMAT, v=1, product_version=PRODUCT_VERSION,
                        task_id=task_id, family=family, attempt=attempt, phase=phase, payload=copy.deepcopy(payload))
        decode(document, task_id, family, attempt, phase)
        return document
    except (ValueError, TypeError, RecursionError) as exc:
        raise ControlDocumentError("Control candidate failed qualification") from exc
