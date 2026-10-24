"""Qualified Claude stdio permission controls; never reusable permission grants."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from typing import Any

from .base import ApprovalRefused, Invocation, UnsupportedCapability
from . import claude_qualification as qualification
from .claude import UnsafeInvocationError

READ_KEYS = {"file_path": str, "offset": int, "limit": int, "pages": str}
WRITE_SCHEMAS = {"Write": ({"file_path": str, "content": str}, {"file_path", "content"}),
                 "Edit": ({"file_path": str, "old_string": str, "new_string": str, "replace_all": bool}, {"file_path", "old_string", "new_string"})}


def _versions() -> list[str]:
    return [v for v, row in qualification.MEASURED.items() if row.get("caller_approval_tools")]


def support() -> dict[str, Any]:
    versions = _versions()
    by_version = {v: list(qualification.approval_tools(v, write=True)) for v in versions}
    return {"supported": True, "versions": versions, "tools": list(dict.fromkeys(t for tools in by_version.values() for t in tools)),
            "tools_by_version": by_version, "write_tools_by_version": {v: list(qualification.MEASURED[v].get("caller_write_tools", ())) for v in versions},
            "path_safety_prompt_versions": [v for v in versions if qualification.supports(v, "path_safety_prompts")],
            "freedoms": ["write_in_repo", "publish"], "scope": "exact_action_once", "native": False, "turn_caps": False}


def validate(enabled: bool, freedom: str, max_turns: int | None, native: bool = False) -> None:
    if type(enabled) is not bool:
        raise ValueError("caller_approvals must be a boolean")
    if enabled and (freedom not in {"write_in_repo", "publish"} or max_turns is not None or native):
        raise UnsupportedCapability("Caller approvals require uncapped non-native Claude stream input at write_in_repo or publish")


def validate_write(write: bool, enabled: bool, version: str | None = None) -> None:
    if type(write) is not bool:
        raise ValueError("caller_write_approvals must be a boolean")
    if not write:
        return
    if not enabled:
        raise ValueError("caller_write_approvals requires caller_approvals")
    if version is not None and not qualification.measured(version).get("caller_write_tools"):
        raise UnsupportedCapability(f"caller_write_approvals is not qualified on Claude Code {version}")


def qualify(backend: Any) -> str:
    """The exact installed version, which must be measured for caller approvals; probed before every spawn."""
    return qualification.qualify(backend, "caller_approval_tools")


def configure(invocation: Invocation, freedom: str, max_turns: int | None, version: str | None = None, write: bool = False) -> Invocation:
    validate(True, freedom, max_turns, invocation.native_subagent)
    validate_write(write, True, version)
    if not invocation.live_input:
        raise UnsupportedCapability("Caller approvals require live stream input")
    return replace(invocation, argv=[*invocation.argv, "--permission-prompt-tool", "stdio", "--permission-prompts", "host"], caller_approvals=True, caller_approval_version=version, caller_write_approvals=write)


def assert_safe(invocation: Invocation, freedom: str, seen: dict[str, list[str]], live: bool) -> None:
    flags = (seen.get("--permission-prompt-tool", []), seen.get("--permission-prompts", []))
    if invocation.caller_approvals:
        validate(True, freedom, None if not seen.get("--max-turns") else 1, invocation.native_subagent)
        if not live or flags != (["stdio"], ["host"]):
            raise UnsafeInvocationError("Caller approval transport must use exact stdio/host controls")
    elif any(flags):
        raise UnsafeInvocationError("Permission control flags require caller_approvals opt-in")


def _strict(tool: str, action_input: Any) -> None:
    if not isinstance(action_input, dict):
        raise ValueError("Permission input must be an object")
    if tool == "Bash":
        if not isinstance(action_input.get("command"), str) or not action_input["command"].strip():
            raise ValueError("Missing exact Bash input")
        if action_input.get("dangerouslyDisableSandbox") not in {None, False}:
            raise ValueError("Caller approvals cannot disable the harness sandbox")
        return
    types, required = (READ_KEYS, {"file_path"}) if tool == "Read" else WRITE_SCHEMAS[tool]
    if set(action_input) - set(types) or required - set(action_input):
        raise ValueError(f"Unsupported or missing {tool} input keys")
    for key, value in action_input.items():
        if type(value) is not types[key]:
            raise ValueError(f"Invalid {tool} input field {key}")
    path = action_input["file_path"]
    if not path or "\x00" in path or not path.startswith("/"):
        raise ValueError(f"Invalid {tool} file_path")


def _qualified_version(qualified: dict[str, Any] | None) -> tuple[str, bool]:
    feature = (qualified or {}).get("feature", "caller approvals")
    version = (qualified or {}).get("version")
    if version is None:
        raise ApprovalRefused(f"{feature} qualification is missing; stopping execution without approval")
    observed = (qualified or {}).get("observed")
    if observed != version:
        raise ApprovalRefused(f"Claude Code reported version {observed or 'none before this request'} but {feature} were qualified on {version}; stopping execution without approval")
    return version, bool((qualified or {}).get("write"))


def check_event(event: dict[str, Any], qualified: dict[str, Any] | None) -> None:
    """Raise `ApprovalRefused` unless the run's version is the one qualified before launch.

    Checked at the init event itself, and for any tool use or control request that arrives without a valid init version first.
    `qualified["feature"]` names what was qualified ("caller approvals", or "allowed_commands" for a run carrying command grants)."""
    version = (qualified or {}).get("version")
    kind = event.get("type")
    if kind == "system" and event.get("subtype") == "init":
        reported = event.get("claude_code_version")
        if version is None or reported != version:
            raise ApprovalRefused(f"Claude Code reported version {reported or 'none'} but {(qualified or {}).get('feature', 'caller approvals')} were qualified on {version}; stopping execution without approval")
        return
    content = (event.get("message") or {}).get("content") if kind == "assistant" and isinstance(event.get("message"), dict) else None
    if kind == "control_request" or (isinstance(content, list) and any(isinstance(b, dict) and b.get("type") == "tool_use" for b in content)):
        _qualified_version(qualified)


def decode_oversize(event: dict[str, Any], qualified: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """A bounded, rejection-only audit row for an over-budget Read, Write or Edit whose identity is still valid.

    Stores ids, the tool, a short `file_path`, the original size and the SHA-256 of the canonical original action; never content."""
    import hashlib
    request = event.get("request")
    native_id = event.get("request_id")
    if event.get("type") != "control_request" or not isinstance(request, dict) or not isinstance(native_id, str) or not native_id or len(native_id) > 256:
        return None
    tool, tool_id = request.get("tool_name"), request.get("tool_use_id")
    if request.get("subtype") != "can_use_tool" or tool not in {"Read", "Write", "Edit"} or not isinstance(tool_id, str) or not tool_id or len(tool_id) > 256:
        return None
    if any(request.get(k) or event.get(k) for k in ("agent_id", "agentId", "parent_tool_use_id", "parentToolUseId")):
        return None
    version, _write = _qualified_version(qualified)
    if tool not in qualification.approval_tools(version, write=True):
        return None
    action = {"tool_name": tool, "input": request.get("input")}
    path = (request.get("input") or {}).get("file_path") if isinstance(request.get("input"), dict) else None
    raw = json.dumps(action, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")
    return {"native_request_id": native_id, "tool_use_id": tool_id, "action": {"tool_name": tool, "input": {}}, "system_rejection": "request_too_large",
            "oversize_bytes": len(json.dumps(event, ensure_ascii=False, default=str).encode("utf-8")), "oversize_action_digest": hashlib.sha256(raw).hexdigest(),
            **({"file_path": path} if isinstance(path, str) and len(path.encode("utf-8")) < 4096 else {})}


def decode(event: dict[str, Any], qualified: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """`qualified` is `{"version", "observed", "write"}`: the pre-spawn version, the init-event version and the write opt-in."""
    if event.get("type") != "control_request":
        return None
    request = event.get("request")
    native_id = event.get("request_id")
    if not isinstance(native_id, str) or not native_id or len(native_id) > 256 or not isinstance(request, dict):
        raise ValueError("Uncorrelated permission control request")
    tool_id = request.get("tool_use_id")
    tool = request.get("tool_name")
    if request.get("subtype") != "can_use_tool" or not isinstance(tool, str):
        raise ValueError("Unsupported permission tool or control subtype")
    version, write = _qualified_version(qualified)
    if tool not in qualification.approval_tools(version, write=True):
        raise ValueError("Unsupported permission tool or control subtype")
    # Child context is intentionally unsupported, including unknown nonempty child fields.
    if any(request.get(k) or event.get(k) for k in ("agent_id", "agentId", "parent_tool_use_id", "parentToolUseId")):
        raise ValueError("Native child permission requests are unsupported")
    if not isinstance(tool_id, str) or not tool_id or len(tool_id) > 256:
        raise ValueError("Missing permission tool identity")
    action_input = request.get("input")
    _strict(tool, action_input)
    reason = request.get("decision_reason")
    kind = request.get("decision_reason_type")
    decoded = {"native_request_id": native_id, "tool_use_id": tool_id, "action": {"tool_name": tool, "input": copy.deepcopy(action_input)},
               "decision_reason": reason.encode("utf-8")[:1024].decode("utf-8", "ignore") if isinstance(reason, str) else None,
               "decision_reason_type": kind[:64] if isinstance(kind, str) else None}
    if tool in qualification.MEASURED[version].get("caller_write_tools", ()) and not write:
        decoded["system_rejection"] = "write_approvals_not_enabled"
    return decoded


SYSTEM_REASONS = ("write_approvals_not_enabled", "protected_path", "request_too_large", "approval_expired")


def encode(request: dict[str, Any], decision: str) -> bytes:
    if decision not in {"approve", "reject"}:
        raise ValueError("decision must be approve or reject")
    system = request.get("system_rejection") or ("approval_expired" if request.get("expiry_rejection") else None)
    if system is not None and (decision != "reject" or system not in SYSTEM_REASONS):
        raise ValueError("A Harflow system rejection can only be delivered as a rejection")
    if system is not None:
        response = {"behavior": "deny", "message": f"Harflow refused this action: {system}"}
    else:
        response = {"behavior": "allow", "updatedInput": copy.deepcopy(request["action"]["input"])} if decision == "approve" else {"behavior": "deny", "message": "Caller rejected this exact action"}
    return (json.dumps({"type": "control_response", "response": {"subtype": "success", "request_id": request["native_request_id"], "response": response}}, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def outcome(event: dict[str, Any]) -> list[tuple[str, str]]:
    results = []
    if event.get("type") == "user":
        for block in (event.get("message") or {}).get("content", []):
            if isinstance(block, dict) and block.get("type") == "tool_result" and isinstance(block.get("tool_use_id"), str):
                results.append((block["tool_use_id"], "failed" if block.get("is_error") else "succeeded"))
    return results
