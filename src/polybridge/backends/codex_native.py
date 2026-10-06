"""Pinned Codex foreground read-only native child execution.

The exec stream omits child lifecycle and tool activity. Child evidence comes
from correlated CLI rollouts, never inferred from argv or parent narration.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import replace
from itertools import islice
from pathlib import Path
from typing import Any

from .base import Invocation

CERTIFIED_VERSION = "0.160.1"
CERTIFIED_MODEL = "gpt-6.1-sol"
PROFILE_PATH = str(Path(__file__).with_suffix(".toml").resolve())
# Use the complete certified table; observed child configuration is still required
# because CLI configuration precedence is not established by argv alone.
NATIVE_PAIRS = (
    "features.multi_agent=true",
    "features.multi_agent_v2=false",
    'agents={max_concurrent_threads_per_session=1,max_depth=1,default={config_file=' + json.dumps(PROFILE_PATH) + ',description="Polybridge inherited read-only worker"}}',
)
NATIVE_ARGS = [token for pair in NATIVE_PAIRS for token in ("-c", pair)]


def validate_native(invocation: Invocation, freedom: str) -> Invocation:
    """Strip only the exact native suffix, leaving ordinary safety checks intact."""
    from .codex import UnsafeInvocationError
    if not invocation.native_subagent:
        return invocation
    argv = list(invocation.argv)
    if "--" not in argv:
        raise UnsafeInvocationError("Native invocation needs the ordinary positional separator")
    index = argv.index("--")
    if index and argv[index - 1] == "resume":
        index -= 1
    if freedom != "read_only" or argv[index-len(NATIVE_ARGS):index] != NATIVE_ARGS:
        raise UnsafeInvocationError("Native invocation needs the exact certified read-only configuration")
    del argv[index-len(NATIVE_ARGS):index]
    # Ordinary validator rejects every additional native override after stripping.
    if "-m" not in argv or argv[argv.index("-m") + 1] != CERTIFIED_MODEL:
        raise UnsafeInvocationError("Native invocation requires the certified explicit model")
    if any(pair.startswith("model_reasoning_effort=") for pair in argv[:argv.index("--")]):
        raise UnsafeInvocationError("Native effort overrides are not certified")
    return replace(invocation, argv=argv, native_subagent=False)


def _fail(state: dict[str, Any], reason: str) -> None:
    state["invalid"] = reason
    raise ValueError(reason)


def child_configuration(child_id: str, state: dict[str, Any]) -> dict[str, Any]:
    """Read bounded evidence from exactly one CLI-produced child rollout."""
    if not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", child_id):
        raise ValueError("Invalid native child session identity")
    home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    # Rollouts use one fixed year/month/day hierarchy. No arbitrary recursive walk.
    files = list(islice((home / "sessions").glob(f"*/*/*/rollout-*-{child_id}.jsonl"), 2))
    if len(files) != 1 or files[0].is_symlink():
        raise ValueError("Missing or ambiguous native child configuration evidence")
    records: dict[str, Any] = {}
    budget = 2 * 1024 * 1024
    with files[0].open("rb") as stream:
        while budget > 0:
            line = stream.readline(budget + 1)
            if not line:
                break
            budget -= len(line)
            if budget < 0 or not line.endswith(b"\n"):
                raise ValueError("Native child configuration exceeds evidence limit")
            event = json.loads(line)
            kind = event.get("type")
            if kind in {"session_meta", "turn_context"}:
                records[kind] = event.get("payload", {})
            if "session_meta" in records and "turn_context" in records:
                break
    meta, context = records.get("session_meta", {}), records.get("turn_context", {})
    source = meta.get("source", {})
    spawn = source.get("subagent", {}).get("thread_spawn", {}) if isinstance(source, dict) else {}
    if meta.get("id") != child_id or meta.get("cli_version") != CERTIFIED_VERSION or spawn.get("parent_thread_id") != state.get("owner_session_id") or spawn.get("depth") != 1:
        raise ValueError("Native child configuration has foreign or uncertified provenance")
    expected_repo = state.get("expected_repo")
    if not expected_repo or context.get("cwd") != expected_repo or meta.get("cwd") != expected_repo:
        raise ValueError("Native child repository differs from its owning workflow")
    sandbox = context.get("sandbox_policy", {})
    if context.get("model") != CERTIFIED_MODEL or context.get("approval_policy") != "never" or sandbox.get("type") != "read-only" or sandbox.get("network_access", False) is not False:
        raise ValueError("Native child model or access differs from certified configuration")
    return {"model": context["model"], "approval_policy": context["approval_policy"], "sandbox_policy": sandbox, "network_access": False, "repo_path": expected_repo, "cli_version": meta["cli_version"], "parent_session_id": spawn["parent_thread_id"], "depth": spawn["depth"], "settings_source": "codex_child_rollout"}


class CodexNativeAdapter:
    activity_level = "limited"

    def configure(self, invocation: Invocation) -> Invocation:
        argv = list(invocation.argv)
        index = argv.index("--")
        if argv[index - 1] == "resume":
            index -= 1
        argv[index:index] = NATIVE_ARGS
        return replace(invocation, argv=argv, native_subagent=True)

    def eligible(self, parent: Any, candidate: dict[str, Any], settings: dict[str, Any]) -> str | None:
        from . import get, version
        measured = version(get(parent.backend))
        if measured != f"codex-cli {CERTIFIED_VERSION}":
            return f"Native execution is certified only for Codex CLI {CERTIFIED_VERSION}"
        if parent.freedom != "read_only" or settings["freedom"] != "read_only":
            return "Only read-only native workers are certified"
        if parent.network != settings["network"] or parent.network is True:
            return "Native worker must inherit its owning orchestrator blocked network request"
        if parent.model != CERTIFIED_MODEL or candidate.get("model") != parent.model:
            return f"Native execution requires the inherited explicit model {CERTIFIED_MODEL}"
        if parent.reasoning_effort is not None or candidate.get("reasoning_effort") is not None:
            return "Native effort overrides are not certified"
        if parent.max_turns is not None or candidate.get("max_turns") is not None:
            return "Codex does not support turn caps"
        return None

    def prompt(self, assignment: str, nonce: str) -> str:
        args = self.spawn_arguments(assignment, nonce)
        return "Polybridge issued one native assignment. Invoke collaboration.spawn_agent exactly once using the exact Spawn arguments below. Do not dispatch any other work or override model, effort or fork settings. Call collaboration.wait_agent until the child has completed, then return ONLY " + json.dumps({"native_dispatch_nonce": nonce, "settled": True}) + ". Do not return a workflow decision. The CLI closes the settled child runtime when this control process exits.\nSpawn arguments:\n" + json.dumps(args)

    @staticmethod
    def spawn_arguments(assignment: str, nonce: str) -> dict[str, str]:
        return {"task_name": "pb_" + nonce.replace("-", "_").lower(), "message": CodexNativeAdapter.child_prompt(assignment, nonce), "fork_turns": "none"}

    @staticmethod
    def child_prompt(assignment: str, nonce: str) -> str:
        return f"Polybridge native assignment nonce: {nonce}\nExecute only this assignment, read-only. Do not delegate, start workflows, or run other agents. Return only the requested worker JSON envelope.\n\n{assignment}"

    def observe(self, event: dict[str, Any], nonce: str, state: dict[str, Any]) -> list[dict[str, Any]]:
        # This installed CLI exposes child activity in rollouts, not exec JSONL.
        if state.get("invalid"):
            raise ValueError(state["invalid"])
        kind = event.get("type")
        if kind == "thread.started":
            if event.get("thread_id") != state.get("owner_session_id"):
                _fail(state, "Stale or foreign parent session")
            state["parent_seen"] = True
        elif kind in {"turn.failed", "error"}:
            _fail(state, "Native owning parent failed")
        elif kind == "turn.completed":
            state["parent_done"] = True
        elif kind == "item.completed" and event.get("item", {}).get("type") == "agent_message":
            try:
                ack = json.loads(event["item"].get("text", ""))
            except (ValueError, TypeError):
                return []
            if ack == {"native_dispatch_nonce": nonce, "settled": True}:
                state["ack"] = True
        return []

    def finalize(self, nonce: str, state: dict[str, Any]) -> list[dict[str, Any]]:
        """Verify persisted evidence only after the control process has exited."""
        if state.get("invalid"):
            raise ValueError(state["invalid"])
        if state.get("terminal"):
            return []
        if not state.get("parent_seen") or not state.get("parent_done") or not state.get("ack"):
            _fail(state, "Parent completed without native settlement acknowledgement")
        records = rollout_records(state["owner_session_id"])
        meta = next((r["payload"] for r in records if r.get("type") == "session_meta"), {})
        if meta.get("id") != state["owner_session_id"] or meta.get("cli_version") != CERTIFIED_VERSION:
            _fail(state, "Uncertified native parent provenance")
        expected = self.spawn_arguments(state["assignment"], nonce)
        matches = []
        for record in records:
            payload = record.get("payload", {})
            if record.get("type") == "response_item" and payload.get("type") == "function_call" and payload.get("namespace") == "collaboration" and payload.get("name") == "spawn_agent":
                try:
                    args = json.loads(payload.get("arguments", ""))
                except (ValueError, TypeError):
                    continue
                if args == expected:
                    matches.append(payload)
        if len(matches) != 1:
            _fail(state, "Missing or duplicate nonce-correlated native assignment")
        spawn = matches[0]
        turn = spawn.get("internal_chat_message_metadata_passthrough", {}).get("turn_id")
        call_id = spawn.get("call_id")
        if not turn or not call_id:
            _fail(state, "Native assignment lacks owning turn and tool identities")
        calls = [r["payload"] for r in records if r.get("type") == "response_item" and r["payload"].get("type") == "function_call" and r["payload"].get("internal_chat_message_metadata_passthrough", {}).get("turn_id") == turn and r["payload"].get("namespace") == "collaboration"]
        if any(c.get("name") not in {"spawn_agent", "wait_agent"} for c in calls) or sum(c.get("name") == "spawn_agent" for c in calls) != 1 or not any(c.get("name") == "wait_agent" for c in calls):
            _fail(state, "Unexpected or missing native child lifecycle calls")
        activities = [r["payload"]["item"] for r in records if r.get("type") == "event_msg" and r["payload"].get("type") == "item_completed" and r["payload"].get("turn_id") == turn and r["payload"].get("thread_id") == state["owner_session_id"] and r["payload"].get("item", {}).get("type") == "SubAgentActivity"]
        started = [a for a in activities if a.get("kind") == "started"]
        if len(started) != 1 or started[0].get("id") != call_id:
            _fail(state, "Native launch acknowledgement does not match one child")
        child, path = started[0].get("agent_thread_id"), started[0].get("agent_path")
        if path != "/root/" + expected["task_name"]:
            _fail(state, "Native child path differs from its assignment")
        outputs = [r["payload"] for r in records if r.get("type") == "response_item" and r["payload"].get("type") == "function_call_output" and r["payload"].get("call_id") == call_id]
        if len(outputs) != 1 or json.loads(outputs[0].get("output", "")) != {"task_name": path}:
            _fail(state, "Native spawn output differs from its launch acknowledgement")
        child_records = rollout_records(child)
        child_meta = next((r["payload"] for r in child_records if r.get("type") == "session_meta"), {})
        child_spawn = child_meta.get("source", {}).get("subagent", {}).get("thread_spawn", {})
        if child_spawn.get("agent_path") != path:
            _fail(state, "Native child rollout path differs from its launch")
        if not isinstance(meta.get("model_provider"), str) or not meta["model_provider"] or child_meta.get("model_provider") != meta["model_provider"]:
            _fail(state, "Native child model provider did not inherit its owner")
        complete = [r["payload"] for r in child_records if r.get("type") == "event_msg" and r["payload"].get("type") == "task_complete"]
        if len(complete) != 1:
            _fail(state, "Native child has no single terminal worker turn")
        child_terminal = complete[0]
        error = child_terminal.get("error")
        terminal_positions = [index for index, r in enumerate(records) if r.get("type") == "event_msg" and r["payload"].get("type") == "item_completed" and r["payload"].get("turn_id") == turn and r["payload"].get("thread_id") == state["owner_session_id"] and r["payload"].get("item", {}).get("type") == "SubAgentActivity" and r["payload"].get("item", {}).get("kind") == "completed" and r["payload"].get("item", {}).get("agent_thread_id") == child and r["payload"].get("item", {}).get("agent_path") == path]
        status = "completed"
        summary = child_terminal.get("last_agent_message")
        if error is not None:
            if child_terminal.get("last_agent_message", "missing") is not None or not isinstance(error, dict) or not isinstance(error.get("message"), str) or not error["message"] or not error.get("codex_error_info"):
                _fail(state, "Native child error terminal is incomplete")
            # A child's structured terminal error is authoritative only when the
            # owning harness also delivered that exact child's error notification.
            prefix = f"Message Type: FINAL_ANSWER\nTask name: /root\nSender: {path}\nPayload:\nAgent errored: {error['message']}\n\nThis agent's turn failed."
            terminal_positions = [index for index, r in enumerate(records) if r.get("type") == "response_item" and r["payload"].get("type") == "agent_message" and r["payload"].get("author") == path and r["payload"].get("recipient") == "/root" and r["payload"].get("internal_chat_message_metadata_passthrough", {}).get("turn_id") == turn and any(part.get("type") == "input_text" and isinstance(part.get("text"), str) and part["text"].startswith(prefix) for part in r["payload"].get("content", []))]
            status, summary = "failed", error["message"]
        elif not isinstance(summary, str):
            _fail(state, "Native child has no completed worker report")
        if not terminal_positions:
            _fail(state, "Native child has no correlated terminal notification")
        parent_completions = [(index, r["payload"]) for index, r in enumerate(records) if r.get("type") == "event_msg" and r["payload"].get("type") == "task_complete" and r["payload"].get("turn_id") == turn]
        if len(parent_completions) != 1 or max(terminal_positions) >= parent_completions[0][0]:
            _fail(state, "Owning turn acknowledged before child terminal evidence")
        try:
            completion_ack = json.loads(parent_completions[0][1].get("last_agent_message", ""))
        except (ValueError, TypeError):
            completion_ack = None
        if completion_ack != {"native_dispatch_nonce": nonce, "settled": True}:
            _fail(state, "Owning turn terminal acknowledgement does not match its assignment")
        metadata = child_configuration(child, state)
        parent_contexts = [r["payload"] for r in records if r.get("type") == "turn_context" and r["payload"].get("turn_id") == turn]
        child_contexts = [r["payload"] for r in child_records if r.get("type") == "turn_context"]
        if len(parent_contexts) != 1:
            _fail(state, "Missing or ambiguous owning turn configuration")
        context = parent_contexts[0]
        sandbox = context.get("sandbox_policy", {})
        if context.get("model") != CERTIFIED_MODEL or context.get("approval_policy") != "never" or context.get("cwd") != state.get("expected_repo") or sandbox.get("type") != "read-only" or sandbox.get("network_access", False) is not False:
            _fail(state, "Owning turn model or access differs from certified configuration")
        # The certified explicit model defaults to low. The parent omits that
        # implicit default from its context while a child records the resolved value.
        if len(child_contexts) != 1 or (context.get("effort") or "low") != (child_contexts[0].get("effort") or "low"):
            _fail(state, "Native effort did not inherit its owning turn")
        metadata["reasoning_effort"] = child_contexts[0].get("effort")
        metadata["model_provider"] = child_meta["model_provider"]
        metadata["cleanup_source"] = "settled_child_and_control_process_exit"
        state.update(native_child_id=child, terminal=True)
        return [{"native_update": "started", "native_child_id": child}, {"native_update": "settled", "status": status, "summary": summary, "observed_model": metadata["model"], "observed_metadata": metadata}]


def rollout_records(session_id: str) -> list[dict[str, Any]]:
    """Bounded CLI evidence; historical parent turns cannot satisfy a fresh nonce."""
    if not isinstance(session_id, str) or not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", session_id):
        raise ValueError("Invalid native session identity")
    home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    files = list(islice((home / "sessions").glob(f"*/*/*/rollout-*-{session_id}.jsonl"), 2))
    if len(files) != 1 or files[0].is_symlink() or files[0].stat().st_size > 16 * 1024 * 1024:
        raise ValueError("Missing, ambiguous or oversized native rollout evidence")
    with files[0].open("rb") as stream:
        content = stream.read(16 * 1024 * 1024 + 1)
    if len(content) > 16 * 1024 * 1024 or content and not content.endswith(b"\n"):
        raise ValueError("Oversized or incomplete native rollout evidence")
    records = [json.loads(line) for line in content.splitlines() if line.strip()]
    return records
