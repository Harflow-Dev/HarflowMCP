"""Codex permission-denial classification, from evidence Harflow itself holds.

Nothing here reads agent prose or command output. Three sources, each measured on codex-cli 0.163.0:

* `codex app-server` item denials (a `declined` command or file change) are classified from the answer Harflow wrote
  for that exact ledger identity: a caller's rejection, or one of Harflow's own (a system rejection or expiry).
  A decline Harflow did not write (a cancelled turn) stays unclassified.
* `codex app-server` stderr, while a turn is active: the router line for an exec-policy `forbidden` rule, in the one
  exact format measured. Tool processes hold pipes to the native process, never the server's stderr, so command
  output cannot forge it.
* `codex exec` stderr: the router line for an escalation refused under `approval_policy="never"`, in its one measured format.

An MCP tool call rejected with `user rejected MCP tool call` is Harflow's only when exactly one elicitation Harflow
answered with an error matches that call's server, tool and arguments; anything less is unclassified. Sandbox
denials (`Operation not permitted` in command output, no item) are never classified.
"""
from __future__ import annotations

import json
import re
from typing import Any

ANSI = re.compile(r"\x1b\[[0-9;]*m")
FORBIDDEN = re.compile(r'(?:\S+ +)?ERROR codex_core::tools::router: error=exec_command failed: CreateProcess \{ message: "Rejected\(\\"`(?P<command>.+)` rejected: (?P<justification>[^"\\]*)\\"\)" \}')
APPROVAL_POLICY_NEVER = re.compile(r"(?:\S+ +)?ERROR codex_core::tools::router: error=approval policy is Never; reject command — you cannot ask for escalated permissions if the approval policy is Never")
MCP_REJECTED = "user rejected MCP tool call"
MCP_TOOL = re.compile(r'run tool "([^"]+)"')
MAX_DENIALS = 64

REMEDIES = {
    "caller_rejected": "The caller rejected this action.",
    "harflow_policy_rejected": "Harflow's own policy refused this action; the stated reason names the policy.",
    "policy_denied": "An exec-policy rule with a forbidden decision refused this command. Change the rule; approvals cannot override it.",
    "approval_policy_never": "This run could not ask for escalation (approval policy never). Start the task with caller_approvals (write_in_repo or publish, macOS, a qualified Codex version) so a caller can approve the exact command, or use a freedom that needs none.",
    "unclassified": "Inspect the refusal text. No remedy is inferred.",
}


def canonical(value: Any) -> str:
    """Type-preserving canonical JSON: `true` and `1` differ, key order does not matter."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def call_key(server: Any, tool: Any, arguments: Any) -> str:
    return canonical([server, tool, arguments])


def record_call(state: Any, item: dict[str, Any]) -> None:
    """Every mcpToolCall of the active turn, so an elicitation is only associated with a call that is the only one of its kind."""
    if item.get("type") == "mcpToolCall" and isinstance(item.get("id"), str):
        state.mcp_calls[item["id"]] = call_key(item.get("server"), item.get("tool"), item.get("arguments"))


def unclassified(reason: str) -> dict[str, Any]:
    return {"denial_category": "unclassified", "denial_remedy": REMEDIES["unclassified"], "classification_basis": {"reason": reason}}


def strip(line: str) -> str:
    return ANSI.sub("", line)


def _add(acc: Any, denial: dict[str, Any]) -> None:
    if len(acc.denials) < MAX_DENIALS:
        acc.denials.append(denial)


def observe_stderr(acc: Any, line: str) -> None:
    """One stderr line: a denial only in an exact measured format and context."""
    text = strip(line).rstrip()
    state = acc.stream_state.get("app_server")
    if state is not None:
        if state.turn_id is None or state.terminal is not None or state.failed is not None:
            return
        found = FORBIDDEN.fullmatch(text)
        if found:
            _add(acc, {"tool_use_id": None, "tool_name": "commandExecution", "tool_input": {"command": found["command"][:500]}, "source": "stderr",
                       "stderr_kind": "forbidden", "justification": found["justification"][:300]})
        return
    if APPROVAL_POLICY_NEVER.fullmatch(text):
        _add(acc, {"tool_use_id": None, "tool_name": "escalation", "tool_input": {}, "source": "stderr", "stderr_kind": "approval_policy_never"})


def note_answer(state: Any, row: dict[str, Any], effective: str, written: bool) -> None:
    """Remember what Harflow wrote for a ledger identity, once the bytes were actually written."""
    if not written:
        return
    if row.get("native_method") == "mcpServer/elicitation/request":
        action = (row.get("action") or {}).get("input") or {}
        meta = action.get("_meta") if isinstance(action.get("_meta"), dict) else {}
        found = MCP_TOOL.search(action.get("message") or "")
        tool = found.group(1) if found else None
        arguments = meta.get("tool_params")
        state.mcp_errors.append({"key": call_key(action.get("serverName"), tool, arguments), "has_arguments": arguments is not None,
                                 "thread": action.get("threadId"), "turn": action.get("turnId"), "used": False})
        return
    key = row.get("tool_use_id")
    if not isinstance(key, str):
        return
    if effective == "approve":
        state.answers[key] = {"answer": "approved"}
    elif row.get("expiry_rejection") or row.get("system_rejection"):
        state.answers[key] = {"answer": "harflow", "reason": row.get("system_rejection") or "approval_expired"}
    elif row.get("decided_by") == "caller":
        state.answers[key] = {"answer": "caller_rejected"}


def observe_item(acc: Any, params: dict[str, Any]) -> None:
    """A completed item that is a refusal: record it once; `classify` decides the category from Harflow's own evidence."""
    item = params.get("item")
    if not isinstance(item, dict) or not isinstance(item.get("id"), str):
        return
    seen = acc.stream_state.setdefault("denied_items", set())
    kind, item_id = item.get("type"), item["id"]
    if item_id in seen:
        return
    state = acc.stream_state.get("app_server")
    if kind == "commandExecution" and item.get("status") == "declined":
        denial = {"tool_use_id": item_id, "tool_name": "commandExecution", "tool_input": {"command": str(item.get("command"))[:500]}, "source": "item"}
    elif kind == "fileChange" and item.get("status") == "declined":
        paths = [c.get("path") for c in item.get("changes") or [] if isinstance(c, dict) and isinstance(c.get("path"), str)][:10]
        denial = {"tool_use_id": item_id, "tool_name": "fileChange", "tool_input": {"paths": paths}, "source": "item"}
    elif kind == "mcpToolCall" and item.get("status") == "failed" and isinstance(item.get("error"), dict) and item["error"].get("message") == MCP_REJECTED:
        denial = {"tool_use_id": item_id, "tool_name": f"mcp__{item.get('server')}__{item.get('tool')}", "tool_input": {}, "source": "item", "mcp": "none"}
        if state is not None:
            # Same active thread and turn, same server, tool and non-null arguments compared as canonical JSON (so true and 1 differ),
            # and the call must be the only one of its kind in the turn: one elicitation cannot say which of two identical calls it was for.
            record_call(state, item)
            key = call_key(item.get("server"), item.get("tool"), item.get("arguments"))
            matches = [e for e in state.mcp_errors if not e["used"] and e["thread"] == state.thread_id and e["turn"] == state.turn_id and e["has_arguments"] and e["key"] == key]
            denial["mcp_key"] = key
            # Provisional: the elicitation matched uniquely now; whether the *call* is the only one of its kind is rechecked
            # against every call recorded by the time of classification.
            denial["mcp"] = "associated" if len(matches) == 1 else "ambiguous" if matches else "none"
            if len(matches) == 1:
                matches[0]["used"] = True
    else:
        return
    seen.add(item_id)
    _add(acc, denial)


def classify_one(denial: dict[str, Any], acc: Any) -> dict[str, Any]:
    state = acc.stream_state.get("app_server")
    if denial.get("source") == "stderr":
        category = {"forbidden": "policy_denied", "approval_policy_never": "approval_policy_never"}.get(denial.get("stderr_kind"))
        if category is None:
            return unclassified("unmeasured_stderr")
        extra = {"justification": denial["justification"]} if category == "policy_denied" and "justification" in denial else {}
        return {"denial_category": category, "denial_remedy": REMEDIES[category], "classification_basis": {"evidence": "stderr_router_line"}, **extra}
    if denial.get("source") != "item":
        return unclassified("backend_unmeasured")
    if state is None:
        return unclassified("no_ledger_evidence")
    if str(denial.get("tool_name", "")).startswith("mcp__"):
        if denial.get("mcp") == "associated" and sum(1 for other in state.mcp_calls.values() if other == denial.get("mcp_key")) == 1:
            return {"denial_category": "harflow_policy_rejected", "denial_remedy": REMEDIES["harflow_policy_rejected"], "policy_reason": "mcp_approval_unsupported",
                    "classification_basis": {"evidence": "elicitation_answered_with_error"}}
        return unclassified("mcp_association_unclear" if denial.get("mcp") in {"ambiguous", "associated"} else "no_matching_elicitation")
    key = denial.get("tool_use_id")
    if denial.get("tool_name") == "fileChange":
        key = f"fileChange:{key}"
    answer = state.answers.get(key) if isinstance(key, str) else None
    if answer is None:
        return unclassified("no_harflow_answer")
    if answer["answer"] == "caller_rejected":
        return {"denial_category": "caller_rejected", "denial_remedy": REMEDIES["caller_rejected"], "classification_basis": {"evidence": "ledger_answer"}}
    if answer["answer"] == "harflow":
        return {"denial_category": "harflow_policy_rejected", "denial_remedy": REMEDIES["harflow_policy_rejected"], "policy_reason": answer["reason"], "classification_basis": {"evidence": "ledger_answer"}}
    return unclassified("approved_but_declined")


def classify(denials: list[Any], acc: Any) -> list[Any]:
    return [{**d, **classify_one(d, acc)} if isinstance(d, dict) else d for d in denials]
