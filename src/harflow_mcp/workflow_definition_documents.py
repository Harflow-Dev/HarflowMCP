"""Pure qualification of saved workflow definition documents."""
from __future__ import annotations

import copy
import math
from typing import Any

from .state_compatibility import persisted_compatible
from .wire_contracts import PRODUCT, PRODUCT_VERSION

DEFINITION_FORMAT = "harflow.workflow.definition"


class WorkflowDefinitionDocumentError(Exception):
    """An existing source cannot provide a qualified saved definition."""

    def __init__(self, name: str, reason: str, *, code: str = "workflow_definition_incompatible"):
        self.name, self.reason, self.code = name, reason, code
        super().__init__(f"{name}: {reason}")

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "code": self.code, "reason": self.reason}


def _shape(value: dict, shape: dict, path: str) -> None:
    for key, types in shape.items():
        if key in value and type(value[key]) not in (types if isinstance(types, tuple) else (types,)):
            raise ValueError(f"Malformed {path}.{key}")


def _json(value: Any) -> None:
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float and math.isfinite(value):
        return
    if isinstance(value, list):
        for item in value:
            _json(item)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for item in value.values():
            _json(item)
        return
    raise ValueError("Definition must contain finite JSON values")


def _candidate(value: dict) -> None:
    _shape(value, {"backend": str, "model": (str, type(None)), "reasoning_effort": (str, type(None)), "max_turns": (int, type(None)), "background_idle_seconds": (int, float, type(None)), "caller_approvals": bool, "caller_write_approvals": bool, "allowed_commands": list, "fallbacks": list, "instructions": str}, "agent")
    for item in value.get("fallbacks", []):
        if not isinstance(item, dict):
            raise ValueError("Malformed agent fallback")
        _candidate(item)


def decode(document: Any, name: str) -> dict[str, Any]:
    """Qualify supplied types and source name without migrating graph semantics."""
    from .workflows import _workflow_name, WorkflowError
    try:
        if not isinstance(document, dict) or document.get("product") != PRODUCT or document.get("format") != DEFINITION_FORMAT:
            raise ValueError("Missing or foreign workflow definition product/format")
        if type(document.get("v")) is not int or document["v"] != 1:
            raise ValueError("Unsupported workflow definition document version")
        if not persisted_compatible(document.get("product_version"), DEFINITION_FORMAT):
            raise ValueError("Incompatible workflow definition product major/minor line")
        value = document.get("definition")
        if not isinstance(value, dict):
            raise ValueError("Definition must be an object")
        _json(value)
        if _workflow_name(value.get("name")) != _workflow_name(name):
            raise ValueError("Definition name does not match source name")
        shape = {key: str for key in ("name", "workflow_id", "description", "routing_mode", "context_delivery", "execution_contract")}
        shape.update({key: int for key in ("schema_version", "revision", "max_parallel", "max_transitions", "max_decision_attempts", "max_inspections", "max_on_demand_calls")})
        shape.update(updated_at=(int, float), orchestrator=dict, nodes=list, connections=list, on_demand_nodes=list)
        _shape(value, shape, "definition")
        if "orchestrator" in value:
            _candidate(value["orchestrator"])
        for node in value.get("nodes", []) + value.get("on_demand_nodes", []):
            if not isinstance(node, dict):
                raise ValueError("Malformed definition node")
            fields = {key: str for key in ("id", "type", "title", "purpose", "instructions", "prompt", "role", "freedom", "session_mode", "execution_mode", "branch_mode", "branch_selection", "selection_guidance", "parallel_group_id", "join_id", "workflow_name", "orchestrator_mode", "child_session_policy", "call_condition")}
            fields.update({key: bool for key in ("optional", "require_technical_plan", "require_tasks", "always_route")})
            fields.update({key: int for key in ("max_attempts", "max_context_questions")})
            fields.update(network=(bool, type(None)), timeout_seconds=(int, type(None)), agent=dict, position=dict, workflow_ref=dict)
            _shape(node, fields, "node")
            if "agent" in node:
                _candidate(node["agent"])
            if "workflow_ref" in node:
                ref = node["workflow_ref"]
                if not isinstance(ref.get("workflow_id"), str) or not ref["workflow_id"].strip():
                    raise ValueError("Malformed workflow reference identity")
            if "position" in node:
                _shape(node["position"], {"x": (int, float), "y": (int, float)}, "position")
        for edge in value.get("connections", []):
            if not isinstance(edge, dict):
                raise ValueError("Malformed definition connection")
            _shape(edge, {"id": str, "source": str, "target": str, "condition": str, "default": bool, "backward": bool, "max_retries": int}, "connection")
        return copy.deepcopy(value)
    except (ValueError, TypeError, RecursionError, WorkflowError) as exc:
        raise WorkflowDefinitionDocumentError(name, str(exc)) from exc


def definition_warnings(definition: dict[str, Any]) -> list[str]:
    """Advisories do not reject or rewrite existing workflow definitions."""
    return [("Agent " if node["type"] == "agent" else "Workflow ") + node["id"] + " has no purpose; context uses a bounded instructions excerpt"
            for node in definition.get("nodes", [])
            if node.get("type") in {"agent", "workflow"} and not node.get("purpose")]


def encode(definition: dict[str, Any]) -> dict[str, Any]:
    """Wrap authored payload without changing its fields."""
    return {"product": PRODUCT, "format": DEFINITION_FORMAT, "v": 1, "product_version": PRODUCT_VERSION, "definition": copy.deepcopy(definition)}
