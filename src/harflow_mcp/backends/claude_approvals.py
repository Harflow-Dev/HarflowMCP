"""Qualified Claude stdio permission controls; never reusable permission grants."""
from __future__ import annotations

import copy
import json
import re
from dataclasses import replace
from typing import Any

from .base import Invocation, UnsupportedCapability
from .claude import UnsafeInvocationError

VERSION = "2.1.302"


def support() -> dict[str, Any]:
    return {"supported": True, "versions": [VERSION], "tools": ["Bash"], "freedoms": ["write_in_repo", "publish"], "scope": "exact_action_once", "native": False, "turn_caps": False}


def validate(enabled: bool, freedom: str, max_turns: int | None, native: bool = False) -> None:
    if type(enabled) is not bool:
        raise ValueError("caller_approvals must be a boolean")
    if enabled and (freedom not in {"write_in_repo", "publish"} or max_turns is not None or native):
        raise UnsupportedCapability("Caller approvals require uncapped non-native Claude stream input at write_in_repo or publish")


def qualify(backend: Any) -> None:
    from . import version
    observed = version(backend)
    if observed is None or re.fullmatch(r"2\.1\.302(?: \(Claude Code\))?", observed) is None:
        raise UnsupportedCapability(f"Caller approvals require Claude Code {VERSION}; observed version is not qualified")


def configure(invocation: Invocation, freedom: str, max_turns: int | None) -> Invocation:
    validate(True, freedom, max_turns, invocation.native_subagent)
    if not invocation.live_input:
        raise UnsupportedCapability("Caller approvals require live stream input")
    return replace(invocation, argv=[*invocation.argv, "--permission-prompt-tool", "stdio", "--permission-prompts", "host"], caller_approvals=True)


def assert_safe(invocation: Invocation, freedom: str, seen: dict[str, list[str]], live: bool) -> None:
    flags = (seen.get("--permission-prompt-tool", []), seen.get("--permission-prompts", []))
    if invocation.caller_approvals:
        validate(True, freedom, None if not seen.get("--max-turns") else 1, invocation.native_subagent)
        if not live or flags != (["stdio"], ["host"]):
            raise UnsafeInvocationError("Caller approval transport must use exact stdio/host controls")
    elif any(flags):
        raise UnsafeInvocationError("Permission control flags require caller_approvals opt-in")


def decode(event: dict[str, Any]) -> dict[str, Any] | None:
    if event.get("type") != "control_request":
        return None
    request = event.get("request")
    native_id = event.get("request_id")
    if not isinstance(native_id, str) or not native_id or len(native_id) > 256 or not isinstance(request, dict):
        raise ValueError("Uncorrelated permission control request")
    tool_id = request.get("tool_use_id")
    if request.get("subtype") != "can_use_tool" or request.get("tool_name") != "Bash":
        raise ValueError("Unsupported permission tool or control subtype")
    # Child context is intentionally unsupported, including unknown nonempty child fields.
    if any(request.get(k) or event.get(k) for k in ("agent_id", "agentId", "parent_tool_use_id", "parentToolUseId")):
        raise ValueError("Native child permission requests are unsupported")
    if not isinstance(tool_id, str) or not tool_id or len(tool_id) > 256:
        raise ValueError("Missing permission tool identity")
    action_input = request.get("input")
    if not isinstance(action_input, dict) or not isinstance(action_input.get("command"), str) or not action_input["command"].strip():
        raise ValueError("Missing exact Bash input")
    if action_input.get("dangerouslyDisableSandbox") not in {None, False}:
        raise ValueError("Caller approvals cannot disable the harness sandbox")
    return {"native_request_id": native_id, "tool_use_id": tool_id, "action": {"tool_name": "Bash", "input": copy.deepcopy(action_input)}}


def encode(request: dict[str, Any], decision: str) -> bytes:
    if decision not in {"approve", "reject"}:
        raise ValueError("decision must be approve or reject")
    response = {"behavior": "allow", "updatedInput": copy.deepcopy(request["action"]["input"])} if decision == "approve" else {"behavior": "deny", "message": "Caller rejected this exact action"}
    return (json.dumps({"type": "control_response", "response": {"subtype": "success", "request_id": request["native_request_id"], "response": response}}, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def outcome(event: dict[str, Any]) -> list[tuple[str, str]]:
    results = []
    if event.get("type") == "user":
        for block in (event.get("message") or {}).get("content", []):
            if isinstance(block, dict) and block.get("type") == "tool_result" and isinstance(block.get("tool_use_id"), str):
                results.append((block["tool_use_id"], "failed" if block.get("is_error") else "succeeded"))
    return results
