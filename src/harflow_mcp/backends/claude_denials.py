"""Claude permission-denial classification from the exact texts measured on Claude Code 2.1.310.

A denial is classified only when a result event's `permission_denials` names its `tool_use_id`; the
matching `tool_result` text is evidence for the reason, never for the existence of a denial, so an
agent printing a refusal sentence from its own Bash output is at most `stream_only`.
"""
from __future__ import annotations

import re
from typing import Any

from . import claude_qualification as qualification

_FLAGS = re.DOTALL
_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("deny_rule", re.compile(r"Permission to use (\S+) with command (.+) has been denied\.", _FLAGS)),
    ("approval_required", re.compile(r"This command requires approval")),
    ("chained_command", re.compile(r"This Bash command contains multiple operations\. The following part requires approval: (.+)", _FLAGS)),
    ("unverifiable_command", re.compile(r"A nested command in this command can't be checked before it runs")),
    ("path_safety", re.compile(r"Claude requested permissions to read from (.+), which contains a suspicious Windows path pattern that requires manual approval\.", _FLAGS)),
    ("outside_working_directory", re.compile(r"Claude requested permissions to write to (.+), but you haven't granted it yet\.", _FLAGS)),
    ("caller_rejected", re.compile(r"Caller rejected this exact action")),
    ("harflow_policy_rejected", re.compile(r"Harflow refused this action: ([a-z][a-z0-9_]*)")),
)
# Detection of a refusal in the stream only. Looser than classification on purpose: the legacy
# substrings keep recording denials whose wording was never measured, as `unrecognized_text`.
_LEGACY_MARKERS = ("this command requires approval", "contains brace with quote character")

REMEDIES = {
    "deny_rule": "A deny rule refused this. It may come from Harflow's access level, user or project settings, or managed policy. Allow rules and `allowed_commands` cannot override it.",
    "approval_required": "A matching allow rule, `allowed_commands` on a new or resumed run, or caller approvals.",
    "chained_command": "Run each part as a separate command. Each part needs its own permission.",
    "unverifiable_command": "Avoid command substitution, or run the inner command separately.",
    "path_safety": "A built-in path check. Allow rules cannot satisfy it. Use caller approvals (Read, on qualified versions) or rename the path.",
    "outside_working_directory": "Work inside the repository, or on a qualified version start the task with caller_approvals and caller_write_approvals so a caller can approve an out-of-repo Write or Edit one exact action at a time.",
    "caller_rejected": "The caller rejected this action.",
    "harflow_policy_rejected": "Harflow's own policy refused this action; the stated reason names the policy.",
    "unclassified": "Inspect the refusal text. No remedy is inferred.",
}
_EXTRA = {"chained_command": "part", "harflow_policy_rejected": "policy_reason"}


def match(text: Any) -> tuple[str, str | None] | None:
    """The category an exact measured text belongs to, plus any captured detail."""
    if not isinstance(text, str):
        return None
    for category, pattern in _PATTERNS:
        found = pattern.fullmatch(text)
        if found:
            return category, (found.group(found.lastindex) if found.lastindex else None)
    return None


def is_refusal(text: Any) -> bool:
    return isinstance(text, str) and (match(text) is not None or any(marker in text.lower() for marker in _LEGACY_MARKERS))


def unclassified(reason: str, version: str | None = None) -> dict[str, Any]:
    basis = {"reason": reason, **({"claude_code_version": version} if version else {})}
    return {"denial_category": "unclassified", "denial_remedy": REMEDIES["unclassified"], "classification_basis": basis}


def _denied_by_harflow(denial: dict[str, Any], deny_prefixes: tuple[str, ...]) -> bool:
    tool_input = denial.get("tool_input")
    command = tool_input.get("command") if isinstance(tool_input, dict) else None
    return denial.get("tool_name") == "Bash" and isinstance(command, str) and any(command == p or command.startswith(p + " ") for p in deny_prefixes)


def classify_one(denial: dict[str, Any], state: dict[str, Any], deny_prefixes: tuple[str, ...]) -> dict[str, Any]:
    version = state.get("claude_code_version")
    version = version if isinstance(version, str) else None
    tool_id = denial.get("tool_use_id")
    if version is None:
        return unclassified("unknown_version")
    if not qualification.supports(version, "denial_texts"):
        return unclassified("unmeasured_version", version)
    if not (isinstance(tool_id, str) and tool_id in state.get("denial_result_ids", ())):
        return unclassified("stream_only", version)
    text = state.get("tool_error_text", {}).get(tool_id)
    if text is None:
        return unclassified("no_refusal_text", version)
    found = match(text)
    if found is None:
        return unclassified("unrecognized_text", version)
    category, detail = found
    out: dict[str, Any] = {"denial_category": category, "denial_remedy": REMEDIES[category],
                           "classification_basis": {"claude_code_version": version, "evidence": "result_event_and_tool_result_text"}}
    if category in _EXTRA and detail is not None:
        out[_EXTRA[category]] = detail
    if category == "deny_rule" and _denied_by_harflow(denial, deny_prefixes):
        out["harflow_access_deny"] = True
    return out


def classify(denials: list[Any], state: dict[str, Any], deny_prefixes: tuple[str, ...] = ()) -> list[Any]:
    return [{**d, **classify_one(d, state, deny_prefixes)} if isinstance(d, dict) else d for d in denials]
