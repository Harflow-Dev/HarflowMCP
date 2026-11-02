"""Denials evidenced by the programmatic stream measured on Vibe 3.0.3.

The stream has no runtime version field. Matching this schema is evidence of a
refusal, not a claim about the installed version or permission authority.
"""
from __future__ import annotations

import re
from typing import Any

REMEDIES = {
    "deny_rule": "A Vibe command denylist refused this action. Inspect inherited tool configuration. Vibe caller approvals and allowed_commands are unsupported; configure permissions, then start a new resume or continue attempt.",
    "policy_denied": "The tool was refused by an approval policy. Inspect inherited tool configuration; this evidence does not identify the policy's source. Vibe caller approvals and allowed_commands are unsupported; configure permissions, then start a new resume or continue attempt.",
    "approval_required": "The tool requested approval which programmatic mode denied. Inspect the required permission and inherited tool configuration. Vibe caller approvals and allowed_commands are unsupported. Configure permissions, then start a new resume or continue attempt.",
    "unclassified": "Inspect the refusal text. No remedy is inferred.",
}


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def effect_error(event: dict[str, Any]) -> dict[str, Any] | None:
    state = event.get("state")
    if event.get("type") != "effect" or not isinstance(state, dict) or state.get("status") != "failed":
        return None
    error = state.get("error")
    return error if isinstance(error, dict) and error.get("code") == "tool_denied" else None


def metadata(event: dict[str, Any]) -> dict[str, Any]:
    detail = event.get("detail")
    detail = detail if isinstance(detail, dict) else {}
    result: dict[str, Any] = {}
    if event.get("type") == "effect":
        error = effect_error(event)
        if error is not None:
            result["error_code"] = error["code"]
            if _text(error.get("message")):
                result["refusal_text"] = error["message"]
    permissions = detail.get("requiredPermissions")
    if isinstance(permissions, list) and permissions:
        result["required_permissions"] = permissions
    tool = detail.get("effect") if event.get("type") == "callback" else detail
    if isinstance(tool, dict) and isinstance(tool.get("input"), dict):
        # Legacy callbacks without the new schema retain their exact raw payload.
        if result or event.get("type") == "effect":
            result["tool_input"] = tool["input"]
    return result


def identity(event: dict[str, Any]) -> str | None:
    detail = event.get("detail")
    detail = detail if isinstance(detail, dict) else {}
    value = event.get("id") if event.get("type") == "effect" else event.get("relatedEntryId") or detail.get("relatedEntryId")
    # Conflicting structural references cannot establish identity.
    if event.get("type") == "callback" and event.get("relatedEntryId") and detail.get("relatedEntryId") and event["relatedEntryId"] != detail["relatedEntryId"]:
        return None
    return value if _text(value) else None


def classify_one(denial: dict[str, Any]) -> dict[str, Any]:
    category, reason = "unclassified", "insufficient_evidence"
    text = denial.get("refusal_text")
    if denial.get("error_code") == "tool_denied":
        if text == "Tool execution denied by approval policy":
            category = "policy_denied"
        elif text == "Tool execution denied by approval callback":
            category = "approval_required"
        elif isinstance(text, str) and any(re.fullmatch(pattern, text, re.DOTALL) for pattern in (
            r"Command denied: '.+' matches denylist pattern '.+'\. Do not attempt to run this command\.",
            r"Command denied: '.+' is not allowed as a standalone command\. Do not attempt to run this command\.",
        )):
            category = "deny_rule"
        else:
            reason = "unrecognized_text"
    permissions = denial.get("required_permissions")
    if category == "unclassified" and text is None and isinstance(permissions, list) and permissions and all(
        isinstance(p, dict) and isinstance(p.get("scope"), str) and p.get("scope") in {"command_pattern", "file_pattern", "outside_directory"} and _text(p.get("invocationPattern")) and _text(p.get("sessionPattern"))
        for p in permissions
    ):
        category = "approval_required"
    basis = ({"evidence": "structured_tool_denied_and_exact_text" if text else "approval_callback_required_permissions", "measured_versions": ["3.0.3"]}
             if category != "unclassified" else {"reason": reason})
    return {**denial, "denial_category": category, "denial_remedy": REMEDIES[category], "classification_basis": basis}
