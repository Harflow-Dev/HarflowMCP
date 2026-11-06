"""Version-pinned native frame ports and explicit launch profile preparation.

Profile construction is not qualification. The launch owner must separately
qualify the actual executable/configuration and retain its receipt; serialized
context is descriptive metadata, never permission authority.
"""
from __future__ import annotations

import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Any

from .base import Invocation
from .conversation import (ConversationIdentity, ConversationLaunch, ConversationPort,
                           ConversationRefused, rpc_key)

VERSIONS = {"codex": "0.166.0", "claude": "2.1.319", "vibe": "3.1.0",
            "antigravity": "1.5.1", "cursor": "2026.10.22-27dcf5f"}
SUPPORTED_PROTOCOL_VERSIONS = {name: frozenset((version,)) for name, version in VERSIONS.items()}
SUPPORTED_PROTOCOL_VERSIONS["claude"] = frozenset(("2.1.318", "2.1.319"))


def _result(frame: dict[str, Any]) -> dict[str, Any] | None:
    value = frame.get("result")
    return value if type(value) is dict else None


class CodexConversationPort(ConversationPort):
    transport = "codex-app-server"

    def normalize_observation(self, observation, acc):
        from .codex import CodexBackend
        frame = observation.payload.get("frame") or observation.payload.get("native")
        if observation.kind not in {"native_event", "response_terminal"} or type(frame) is not dict:
            return []
        if frame.get("method") == "item/agentMessage/delta":
            # The legacy exec translator only exposes completed items. Live
            # conversation consumers need the already-correlated streamed text
            # while this native turn is still active.
            from . import normalize as nz
            params = frame.get("params", {})
            delta, item_id = params.get("delta"), params.get("itemId")
            return [nz.assistant_delta(delta, message_id=item_id, block_index=0)] if (
                type(delta) is str and delta and type(item_id) is str and item_id) else []
        backend = CodexBackend()
        if frame.get("method") == "thread/tokenUsage/updated":
            from . import normalize as nz
            native = frame.get("params", {}).get("tokenUsage", {}).get("total", {})
            keys = {"inputTokens": "input_tokens", "cachedInputTokens": "cached_input_tokens",
                    "outputTokens": "output_tokens", "reasoningOutputTokens": "reasoning_output_tokens"}
            if native and all(type(native.get(key)) is int and native[key] >= 0 for key in keys):
                # Native `total` is a cumulative snapshot, never a delta to sum.
                acc.usage = {target: native[source] for source, target in keys.items()}
                return [nz.usage(acc)]
            return []
        previous_usage = acc.usage
        backend.ingest(frame, acc)
        if observation.kind == "response_terminal":
            # Legacy translate emits an empty terminal usage map. Absence is
            # unknown; do not erase the correlated native cumulative snapshot.
            acc.usage = previous_usage
        normalized = backend.normalize(frame, acc)
        if frame.get('method') == 'item/completed':
            item = frame.get('params', {}).get('item', {})
            if type(item) is dict and item.get('type') == 'agentMessage':
                item_id = item.get('id')
                if type(item_id) is str and item_id:
                    for event in normalized:
                        if event.get('kind') == 'assistant_text':
                            event.update(message_id=item_id, block_index=0)
        return normalized

    def __init__(self, profile: dict[str, Any]) -> None:
        super().__init__(session_id=profile.get("session_id"))
        self.profile = profile
        self._request("initialize", {"clientInfo": {"name": "harflow", "version": "1"},
                                     "capabilities": {"experimentalApi": True}})

    def _start_session(self):
        fields = {"cwd": self.profile["cwd"], "sandbox": self.profile["sandbox"],
                  "approvalPolicy": self.profile["approval_policy"], "approvalsReviewer": "user"}
        if self.profile.get("model"):
            fields["model"] = self.profile["model"]
        if self.profile.get("config"):
            fields["config"] = self.profile["config"]
        if self.session_id:
            fields["threadId"] = self.session_id
        if self.profile.get('runtime_workspace_roots') is not None:
            fields['runtimeWorkspaceRoots'] = self.profile['runtime_workspace_roots']
        self._request("thread/resume" if self.session_id else "thread/start", fields, "session")

    def _issue(self, text: str) -> None:
        self._request("turn/start", {"threadId": self.session_id,
                                     "input": [{"type": "text", "text": text}]}, f"prompt:{self._generation}")

    def _interrupt(self) -> None:
        if self.identity is None or not self.identity.turn_id:
            raise ConversationRefused("Codex interrupt awaits exact native turn id")
        self._request("turn/interrupt", {"threadId": self.session_id,
                                        "turnId": self.identity.turn_id}, f"interrupt:{self._generation}")

    def _cancel_approvals(self) -> None:
        for key, request_id in list(self.pending_approvals.items()):
            self._outbox.append({"jsonrpc": "2.0", "id": request_id, "result": {"decision": "cancel"}})
            del self.pending_approvals[key]
            self._emit("approval_expired", request_id=request_id)

    def _observe(self, frame: dict[str, Any]) -> None:
        method, params = frame.get("method"), frame.get("params", {})
        if type(params) is not dict:
            self.mark_unknown("invalid Codex params")
            return
        if method is None:
            pending = self._pending.pop(rpc_key(frame.get("id")), None)
            if pending is None:
                self._emit("historical_frame", frame=frame)
                return
            purpose, sent = pending
            result = _result(frame)
            if "error" in frame or result is None:
                self.mark_unknown("native RPC failed: " + purpose)
                return
            if purpose == "initialize":
                agent = str(result.get("userAgent", ""))
                if not re.search(r"(?<![\d.])" + re.escape(self.profile["version"]) + r"(?![\d.])", agent):
                    self.mark_unknown("Codex initialize version mismatch")
                    return
                self._notify("initialized", {})
                if self.profile.get('configuration_witness'):
                    self._request('config/read', {'cwd': self.profile['cwd'], 'includeLayers': False}, 'configuration')
                else:
                    self._start_session()
            elif purpose == 'configuration':
                from .conversation_codex_config import filter_configuration
                try:
                    observed = filter_configuration(result)
                except ConversationRefused:
                    self.mark_unknown('Codex effective configuration malformed before thread')
                    return
                if observed != self.profile['configuration_witness']['configuration']:
                    self.mark_unknown('Codex effective configuration changed before thread')
                    return
                self._start_session()
            elif purpose == "session":
                thread = result.get("thread", {})
                sandbox = result.get("sandbox", {})
                expected_type = {"read-only": "readOnly", "workspace-write": "workspaceWrite", "danger-full-access": "dangerFullAccess"}[self.profile["sandbox"]]
                if (type(thread) is not dict or not isinstance(thread.get("id"), str)
                        or (self.session_id and thread["id"] != self.session_id)
                        or result.get("approvalPolicy") != self.profile["approval_policy"]
                        or result.get("approvalsReviewer") != "user"
                        or type(sandbox) is not dict or sandbox.get("type") != expected_type):
                    self.mark_unknown("Codex session permission/identity echo mismatch")
                    return
                if expected_type == "workspaceWrite" and (type(sandbox.get("writableRoots")) is not list
                        or any(type(root) is not str for root in sandbox["writableRoots"])
                        or type(sandbox.get("networkAccess")) is not bool
                        or sandbox.get("networkAccess") != self.profile["network"]
                        or {self.profile["cwd"], *sandbox.get("writableRoots", [])} != set(self.profile["writable_roots"])):
                    self.mark_unknown("Codex writable roots/network echo mismatch")
                    return
                if self.profile.get('runtime_workspace_roots') is not None:
                    runtime_roots = result.get('runtimeWorkspaceRoots')
                    if (type(runtime_roots) is not list
                            or any(type(root) is not str for root in runtime_roots)
                            or set(runtime_roots) != set(self.profile['runtime_workspace_roots'])):
                        self.mark_unknown('Codex runtime workspace roots replacement echo mismatch')
                        return
                if expected_type == 'workspaceWrite' and self.profile.get('configuration_witness'):
                    workspace = self.profile['configuration_witness']['configuration']['workspace']
                    if (sandbox.get('excludeSlashTmp') != workspace['exclude_slash_tmp']
                            or sandbox.get('excludeTmpdirEnvVar') != workspace['exclude_tmpdir_env_var']):
                        self.mark_unknown('Codex sandbox exclusion echo mismatch')
                        return
                if self.profile.get("model") and result.get("model") != self.profile["model"]:
                    self.mark_unknown("Codex model echo mismatch")
                    return
                self.session_id = thread["id"]
                self.ready = True
                self._emit("session_ready", session_id=self.session_id)
            elif purpose == f"prompt:{self._generation}":
                turn = result.get("turn", {})
                if type(turn) is dict and type(turn.get("id")) is str:
                    self._accept(turn["id"])
                else:
                    self.mark_unknown("Codex start lacks turn identity")
            elif purpose == f"interrupt:{self._generation}":
                self.interrupt_acknowledged = True
                self._emit("interrupt_acknowledged", native=result)
            return
        if method.startswith("thread/") and method != "thread/tokenUsage/updated":
            return
        if params.get("threadId") != self.session_id:
            self._emit("historical_frame", frame=frame)
            return
        turn = params.get("turn", {})
        tid = params.get("turnId") or (turn.get("id") if type(turn) is dict else None)
        if method == "turn/started" and self.identity and self.identity.turn_id is None:
            self._accept(tid)
        if not self.identity or tid != self.identity.turn_id:
            self._emit("historical_frame", frame=frame)
            return
        if "id" in frame:
            if method in {"item/commandExecution/requestApproval", "item/fileChange/requestApproval"}:
                key = rpc_key(frame["id"])
                if key is None:
                    self.mark_unknown("invalid approval id")
                else:
                    self.pending_approvals[key] = frame["id"]
                    self._emit("approval_pending", request_id=frame["id"], native=frame)
                    # Approval opt-in belongs to the existing approval subsystem. This
                    # port installs no grant, including when a config asks for one.
                    if not self.profile.get("caller_approvals") or self.interrupt_requested:
                        self._cancel_approvals()
            else:
                self.mark_unknown("unsupported Codex server request")
            return
        if method == "turn/completed":
            status = turn.get("status")
            if status not in {"completed", "interrupted", "failed"}:
                self.mark_unknown("unknown Codex terminal status")
            else:
                self._finish(status, frame)
        elif method in {"item/started", "item/completed"}:
            item = params.get("item", {})
            # Only known presentation/bookkeeping items are non-tools. New or
            # unrecognized native operation types fail closed for interruption,
            # including web/image/collaboration operations in the pinned schema.
            non_tool_types = {'userMessage', 'hookPrompt', 'agentMessage', 'functionCallOutput',
                              'plan', 'reasoning', 'enteredReviewMode', 'exitedReviewMode', 'contextCompaction'}
            if type(item) is not dict or type(item.get('type')) is not str:
                self.mark_unknown('Codex item lacks native type identity')
            elif item['type'] not in non_tool_types:
                self._tool(item.get("id"), "completed" if method == "item/completed" else "running")
            self._emit("native_event", frame=frame)
        else:
            self._emit("native_event", frame=frame)


class ACPConversationPort(ConversationPort):
    transport = "acp"

    def normalize_observation(self, observation, acc):
        from . import normalize as nz
        identity = observation.identity
        # ACP has no assistant message/block identity. This is a bridge display
        # block for one logical response, never native delivery/settlement proof.
        if identity is None:
            return []
        message_id = 'harflow-acp:' + json.dumps(
            [identity.logical_turn, identity.generation], separators=(',', ':'))
        if observation.kind == 'response_terminal':
            return [nz.assistant_text(acc.summary, message_id=message_id, block_index=0)] if (
                type(acc.summary) is str and acc.summary) else []
        if observation.kind != "native_event":
            return []
        frame = observation.payload["frame"]
        update = frame.get("params", {}).get("update", {})
        kind = update.get("sessionUpdate")
        if kind == "agent_message_chunk":
            content = update.get("content", {})
            if content.get("type") == "text" and type(content.get("text")) is str:
                return [nz.assistant_delta(content["text"], message_id=message_id, block_index=0)]
        if kind == "tool_call":
            return [nz.tool_call(call_id=update.get("toolCallId"), tool=update.get("title", "ACP tool"),
                                 category="other", input=update.get("rawInput", {}))]
        if kind == "tool_call_update" and update.get("status") in {"completed", "failed", "cancelled"}:
            return [nz.tool_result(call_id=update.get("toolCallId"), ok=update["status"] == "completed",
                                   output=update.get("rawOutput", update.get("content", [])))]
        return []

    def __init__(self, profile: dict[str, Any]) -> None:
        super().__init__(session_id=profile.get("session_id"))
        self.profile = profile
        self._selections: list[tuple[str, str, str]] = []
        cap = profile.get("max_turns")
        if cap is not None and (type(cap) is not int or cap < 1):
            raise ConversationRefused("ACP conversation cap requires a positive integer")
        self._selection_expected: tuple[str, str] | None = None
        self._selection_echo = False
        self._selection_ack = False
        self._supports_close = False
        self._request("initialize", {"protocolVersion": 1, "clientCapabilities": {},
                                     "clientInfo": {"name": "harflow", "version": "1"}})

    def _issue(self, text: str) -> None:
        self._request("session/prompt", {"sessionId": self.session_id,
                                          "prompt": [{"type": "text", "text": text}]}, f"prompt:{self._generation}")
        # ACP has no native turn id: the exact outstanding prompt RPC is its identity.
        request_id = self._outbox[-1]["id"]
        self.identity = ConversationIdentity(self.session_id, request_id, self._generation, self.identity.logical_turn)

    def _interrupt(self) -> None:
        self._notify("session/cancel", {"sessionId": self.session_id})

    def begin_close(self) -> bool:
        if not self._supports_close or not self.can_issue or not self.session_id:
            return False
        self._request("session/close", {"sessionId": self.session_id}, "close")
        return True

    def _cancel_approvals(self) -> None:
        for key, request_id in list(self.pending_approvals.items()):
            self._outbox.append({"jsonrpc": "2.0", "id": request_id,
                                 "result": {"outcome": {"outcome": "cancelled"}}})
            del self.pending_approvals[key]
            self._emit("approval_expired", request_id=request_id)

    def _select_next(self) -> None:
        if self._selections:
            method, key, value = self._selections.pop(0)
            self._selection_expected = (key, value)
            self._selection_echo = self._selection_ack = False
            fields = {"sessionId": self.session_id, key: value}
            if key.startswith("config:"):
                fields = {"sessionId": self.session_id, "configId": key[7:], "value": value}
            self._request(method, fields, "select:" + key)
        else:
            self.ready = True
            self._emit("session_ready", session_id=self.session_id)

    def _observe(self, frame: dict[str, Any]) -> None:
        method, params = frame.get("method"), frame.get("params", {})
        if type(params) is not dict:
            self.mark_unknown("invalid ACP params")
            return
        if method is None:
            pending = self._pending.pop(rpc_key(frame.get("id")), None)
            if pending is None:
                self._emit("historical_frame", frame=frame)
                return
            purpose, sent = pending
            result = _result(frame)
            if "error" in frame or result is None:
                self.mark_unknown("native RPC failed: " + purpose)
                return
            if purpose == "initialize":
                info = result.get("agentInfo", {})
                if result.get("protocolVersion") != 1 or (type(info) is dict and info.get("version") not in {None, self.profile["version"]}):
                    self.mark_unknown("ACP protocol/runtime version mismatch")
                    return
                capabilities = result.get("agentCapabilities", {}).get("sessionCapabilities", {})
                self._supports_close = type(capabilities) is dict and "close" in capabilities
                if self.session_id:
                    self._request("session/load", {"sessionId": self.session_id, "cwd": self.profile["cwd"], "mcpServers": []}, "session")
                else:
                    self._request("session/new", {"cwd": self.profile["cwd"], "mcpServers": []}, "session")
            elif purpose == "session":
                sid = result.get("sessionId", self.session_id)
                if type(sid) is not str or not sid or (self.session_id and sid != self.session_id):
                    self.mark_unknown("ACP session identity mismatch")
                    return
                self.session_id = sid
                if self.profile["backend"] == "vibe":
                    trust = result.get("_meta", {}).get("workspace_trust")
                    if type(trust) is not dict or trust.get("status") not in {"trusted", "session", "untrusted"}:
                        self.mark_unknown("Vibe workspace trust metadata missing")
                        return
                    if trust["status"] == "untrusted" and trust.get("details") is not None:
                        self.mark_unknown("workspace_trust_unqualified: native trust decision would persist configuration")
                        return
                self._configure(result)
            elif purpose.startswith("select:"):
                key = purpose.removeprefix("select:")
                if self.profile.get("backend") == "vibe":
                    observed_options = {option.get("id"): option.get("currentValue")
                                        for option in result.get("configOptions", []) if type(option) is dict}
                    for setting, wanted in (("mode", self.profile["mode"]), ("model", self.profile.get("model")),
                                            ("thinking", self.profile.get("effort"))):
                        if wanted is not None and observed_options.get(setting) != wanted:
                            self.mark_unknown("Vibe configuration changed during selection: " + setting)
                            return
                if key == "modeId":
                    observed = result.get("currentModeId") or result.get("modes", {}).get("currentModeId")
                elif key == "modelId":
                    observed = result.get("currentModelId") or result.get("models", {}).get("currentModelId")
                elif key.startswith("config:"):
                    observed = next((o.get("currentValue") for o in result.get("configOptions", [])
                                     if type(o) is dict and o.get("id") == key[7:]), None)
                    if key == "config:max_turns" and self.profile.get("backend") == "vibe":
                        # The pinned ACP implementation applies this session-only
                        # setting before replying, but omits it from configOptions.
                        # Acceptance of this profile still requires a separately
                        # measured native cap-stop qualification row.
                        self._selection_ack = True
                        self._selection_expected = None
                        self._emit("setting_acknowledged", setting="max_turns", value=sent["value"],
                                   evidence="pinned session-only setter returned success; no native value echo")
                        self._select_next()
                        return
                else:
                    observed = None
                expected = sent.get("value", sent.get(key))
                if observed is not None and observed != expected:
                    self.mark_unknown("ACP selection lacks matching echo: " + key)
                    return
                self._selection_ack = True
                self._selection_echo = self._selection_echo or observed == expected
                if self._selection_echo:
                    self._selection_expected = None
                    self._select_next()
            elif purpose == f"prompt:{self._generation}":
                status = result.get("stopReason")
                if status not in {"end_turn", "cancelled", "max_tokens", "max_turn_requests", "refusal"}:
                    self.mark_unknown("unknown ACP terminal reason")
                    return
                self._finish(status, frame)
            elif purpose == "close":
                self.close_acknowledged = True
                self._emit("native_close_acknowledged")
            return
        if params.get("sessionId") == self.session_id and method == "session/update" and self._selection_expected:
            update = params.get("update", {})
            key, expected = self._selection_expected
            observed = None
            if update.get("sessionUpdate") == "current_mode_update" and key == "modeId":
                observed = update.get("currentModeId")
            elif update.get("sessionUpdate") == "current_model_update" and key == "modelId":
                observed = update.get("currentModelId")
            if observed is not None:
                if observed != expected:
                    self.mark_unknown("ACP asynchronous selection echo mismatch")
                    return
                self._selection_echo = True
                if self._selection_ack:
                    self._selection_expected = None
                    self._select_next()
                return
        if params.get("sessionId") != self.session_id or self.identity is None:
            self._emit("historical_frame", frame=frame)
            return
        if method == "session/request_permission" and "id" in frame:
            key = rpc_key(frame["id"])
            if key is None:
                self.mark_unknown("invalid ACP approval id")
                return
            self.pending_approvals[key] = frame["id"]
            self._emit("approval_pending", request_id=frame["id"], native=frame)
            if not self.profile.get("caller_approvals") or self.interrupt_requested or self.terminal:
                self._cancel_approvals()
        elif method == "session/update":
            update = params.get("update", {})
            if type(update) is not dict:
                self.mark_unknown("invalid ACP update")
                return
            if self.terminal is not None:
                self._emit("historical_frame", frame=frame)
                return
            kind = update.get("sessionUpdate")
            if kind == "current_mode_update" and update.get("currentModeId") != self.profile["mode"]:
                self.mark_unknown("ACP native permission mode changed")
                return
            if (kind == "current_model_update" and self.profile.get("model") is not None
                    and update.get("currentModelId") != self.profile["model"]):
                self.mark_unknown("ACP native model changed")
                return
            if kind in {"tool_call", "tool_call_update"}:
                self._tool(update.get("toolCallId"), update.get("status"))
            self._emit("native_event", frame=frame)
        elif "id" in frame:
            self.mark_unknown("unsupported ACP server request")

    def _configure(self, result: dict[str, Any]) -> None:
        options = {o.get("id"): o for o in result.get("configOptions", []) if type(o) is dict}
        if self.profile["backend"] == "vibe":
            for key, value in (("mode", self.profile["mode"]), ("model", self.profile.get("model")),
                               ("thinking", self.profile.get("effort"))):
                if value is None:
                    continue
                option = options.get(key, {})
                if value not in {o.get("value") for o in option.get("options", []) if type(o) is dict}:
                    self.mark_unknown("ACP requested config option is not advertised: " + key)
                    return
                if key == "mode":
                    # Installed 3.1.0: agent.switch changes the session profile
                    # layer and session pins, not the global configuration.
                    self._selections.append(("session/set_config_option", "config:" + key, value))
                elif option.get("currentValue") != value:
                    # Model/thinking setters write native configuration. Child
                    # environment pins must already have produced these values.
                    self.mark_unknown("Vibe child-only model/thinking profile mismatch: " + key)
                    return
            if self.profile.get("max_turns") is not None:
                self._selections.append(("session/set_config_option", "config:max_turns", str(self.profile["max_turns"])))
            self._select_next()
            return
        modes = result.get("modes", {})
        mode = self.profile["mode"]
        available = {entry.get("id") for entry in modes.get("availableModes", []) if type(entry) is dict}
        if mode not in available:
            self.mark_unknown("ACP requested mode is not advertised")
            return
        if modes.get("currentModeId") != mode:
            self._selections.append(("session/set_mode", "modeId", mode))
        model = self.profile.get("model")
        if model:
            models = result.get("models", {})
            if model not in {entry.get("modelId") for entry in models.get("availableModels", []) if type(entry) is dict}:
                self.mark_unknown("ACP requested model is not advertised")
                return
            if models.get("currentModelId") != model:
                self.mark_unknown("ACP model differs; setter persistence is not qualified")
                return
        self._select_next()


class ClaudeConversationPort(ConversationPort):
    transport = "claude-stream-json"

    def normalize_observation(self, observation, acc):
        from .claude import ClaudeBackend
        frame = observation.payload.get("frame") or observation.payload.get("native")
        if observation.kind not in {"native_event", "response_terminal"} or type(frame) is not dict:
            return []
        backend = ClaudeBackend()
        backend.ingest(frame, acc)
        return backend.normalize(frame, acc)

    def __init__(self, profile: dict[str, Any]) -> None:
        super().__init__(session_id=profile.get("session_id"))
        self.profile = profile
        self.ready = True
        self._message_ids: set[str] = set()
        self._active_message_ids: set[str] = set()
        self._result_ids: set[str] = set()
        self._fresh_message: str | None = None
        self._interrupt_id: str | None = None

    def _issue(self, text: str) -> None:
        self._fresh_message = None
        self._active_message_ids.clear()
        self._outbox.append({"type": "user", "uuid": self.identity.logical_turn,
                             "session_id": self.session_id, "parent_tool_use_id": None,
                             "message": {"role": "user", "content": text}})

    def _interrupt(self) -> None:
        if not self.identity.turn_id:
            raise ConversationRefused("Claude interrupt awaits fresh native message_start")
        self._counter += 1
        self._interrupt_id = f"hf-interrupt-{self._counter}"
        self._outbox.append({"type": "control_request", "request_id": self._interrupt_id,
                             "request": {"subtype": "interrupt"}})

    def _cancel_approvals(self) -> None:
        for key, request_id in list(self.pending_approvals.items()):
            self._outbox.append({"type": "control_response", "response": {"subtype": "success",
                                 "request_id": request_id, "response": {"behavior": "deny", "message": "Request expired"}}})
            del self.pending_approvals[key]
            self._emit("approval_expired", request_id=request_id)

    def _observe(self, frame: dict[str, Any]) -> None:
        kind = frame.get("type")
        if kind == "system" and frame.get("subtype") == "init":
            expected_version = self.profile.get("version")
            if expected_version is not None and frame.get("claude_code_version") != expected_version:
                self.mark_unknown("Claude native initialization version mismatch")
                return
            sid = frame.get("session_id")
            if self.session_id and sid != self.session_id:
                self.mark_unknown("Claude session changed")
            elif type(sid) is str and sid:
                self.session_id = sid
                if self.identity:
                    self.identity = replace(self.identity, session_id=sid)
                self._emit("session_ready", session_id=sid)
                self._emit("native_event", frame=frame)
            return
        if kind == "control_response":
            response = frame.get("response", {})
            if response.get("request_id") == self._interrupt_id and response.get("subtype") == "success":
                self.interrupt_acknowledged = True
                self._emit("interrupt_acknowledged", native=frame)
            return
        if kind == "control_request":
            request_id = frame.get("request_id")
            key = rpc_key(request_id)
            if key is None:
                self.mark_unknown("invalid Claude approval id")
                return
            self.pending_approvals[key] = request_id
            self._emit("approval_pending", request_id=request_id, native=frame)
            if not self.profile.get("caller_approvals") or self.interrupt_requested or self.terminal:
                self._cancel_approvals()
            return
        if not self.identity or frame.get("session_id") != self.session_id:
            self._emit("historical_frame", frame=frame)
            return
        if kind == "system":
            from .claude import BACKGROUND_TERMINAL_STATUSES
            task_id, subtype = frame.get("task_id"), frame.get("subtype")
            patch = frame.get("patch", {})
            if type(task_id) is str and task_id:
                tool_id = "background:" + task_id
                if ((subtype == "task_started" and frame.get("is_backgrounded") is True)
                        or (subtype == "task_updated" and patch.get("is_backgrounded") is True)):
                    self._tool(tool_id, "running")
                status = patch.get("status") if subtype == "task_updated" else frame.get("status")
                if subtype in {"task_updated", "task_notification"} and status in BACKGROUND_TERMINAL_STATUSES:
                    self._tool(tool_id, "cancelled" if status in {"killed", "stopped", "cancelled"} else "completed")
            self._emit("native_event", frame=frame)
            return
        if kind == "stream_event":
            event = frame.get("event", {})
            if event.get("type") == "message_start":
                mid = event.get("message", {}).get("id")
                if type(mid) is str and mid and mid not in self._message_ids:
                    if self.terminal is not None:
                        self.mark_unknown("unsolicited Claude response after logical terminal")
                        return
                    self._message_ids.add(mid)
                    self._active_message_ids.add(mid)
                    if self._fresh_message is None:
                        self._fresh_message = mid
                        self._accept(mid)
            if self._fresh_message is None or self.terminal:
                self._emit("historical_frame", frame=frame)
                return
            block = event.get("content_block", {})
            if event.get("type") == "content_block_start" and block.get("type") in {"tool_use", "server_tool_use"}:
                self._tool(block.get("id"), "running")
            self._emit("native_event", frame=frame)
        elif kind in {"assistant", "user"}:
            message = frame.get("message", {})
            if self._fresh_message is None or self.terminal is not None or (
                    message.get("id") in self._message_ids and message.get("id") not in self._active_message_ids):
                self._emit("historical_frame", frame=frame)
                return
            for block in message.get("content", []) if type(message.get("content")) is list else []:
                if type(block) is dict and block.get("type") == "tool_use":
                    self._tool(block.get("id"), "running")
                elif type(block) is dict and block.get("type") == "tool_result":
                    self._tool(block.get("tool_use_id"), "completed")
            self._emit("native_event", frame=frame)
        elif kind == "result":
            result_id = frame.get('uuid')
            if type(result_id) is not str or not result_id:
                self.mark_unknown('Claude result lacks unique terminal identity')
                return
            if result_id in self._result_ids:
                self._emit('historical_frame', frame=frame)
                return
            self._result_ids.add(result_id)
            # A result without a fresh assistant id cannot cross a generation fence.
            if self._fresh_message is None:
                self.mark_unknown("Claude result lacks fresh response identity")
                return
            reason = frame.get("terminal_reason")
            self._finish("aborted_streaming" if reason == "aborted_streaming" else
                         "failed" if frame.get("is_error") else "completed", frame)
        else:
            self._emit("native_event", frame=frame)


def _option(argv: list[str], flag: str) -> str | None:
    values = [argv[i + 1] for i, value in enumerate(argv[:-1]) if value == flag]
    if len(values) > 1:
        raise ConversationRefused("ambiguous native option " + flag)
    return values[0] if values else None


def _prompt(invocation: Invocation, backend: str) -> str:
    if invocation.control_context:
        value = invocation.control_context.get("prompt")
        if type(value) is str:
            return value
    if invocation.initial_input is not None:
        value = invocation.initial_input.decode("utf-8")
        if invocation.stdin_mode == "pipe_once":
            return value
        frame = json.loads(value)
        if backend == "claude":
            content = frame["message"]["content"]
            return content if type(content) is str else content[0]["text"]
        if backend == "antigravity":
            return frame["message"]["content"]
    if backend == "vibe":
        return invocation.argv[-1].removeprefix("--prompt=")
    return invocation.argv[-1]


def conversation_profile(backend: Any, invocation: Invocation, *, version: str, freedom: str,
                         network: bool | None = None, profile_name: str | None = None,
                         configuration_witness=None) -> dict[str, Any]:
    """Describe a proposed exact profile after validating the original invocation.

    This method performs no process/network calls and grants nothing. Unsupported
    grant/configuration seams refuse rather than omit settings during conversion.
    """
    name = backend.name
    if version not in SUPPORTED_PROTOCOL_VERSIONS.get(name, ()):
        raise ConversationRefused("native conversation version is not qualified")
    if invocation.conversation_context is not None:
        raise ConversationRefused("invocation already has a conversation transport")
    backend.assert_safe(invocation, freedom, network)
    if invocation.native_subagent:
        raise ConversationRefused("native child conversation delivery is not qualified")
    if invocation.control_pipe:
        # Existing approval contexts are version-specific and one-turn. Their
        # complete policy must be preserved by a separately qualified conversion.
        raise ConversationRefused("existing one-turn control profile needs explicit conversation qualification")
    profile: dict[str, Any] = {"v": 1, "backend": name, "version": version, "freedom": freedom,
                               "caller_approvals": invocation.caller_approvals,
                               "profile_name": profile_name, "initial_prompt": _prompt(invocation, name)}
    if not profile["initial_prompt"].strip():
        raise ConversationRefused("native conversation requires nonempty initial prompt")
    if name == "claude":
        argv = list(invocation.argv)
        cap = _option(argv, "--max-turns")
        if cap is not None:
            # Preserve the exact configured response cap and every permission
            # flag; only the input transport changes from the validated classic
            # prompt boundary to bridge-owned stream JSON.
            if "--" in argv:
                argv = argv[:argv.index("--")]
            if "--input-format" in argv:
                argv[argv.index("--input-format") + 1] = "stream-json"
            else:
                argv.extend(["--input-format", "stream-json"])
            profile["max_turns"] = int(cap)
        elif invocation.stdin_mode != "pipe":
            raise ConversationRefused("Claude conversation requires its validated stream input shape")
        profile.update(transport="claude-stream-json", session_id=_option(invocation.argv, "--session-id") or
                       _option(invocation.argv, "--resume"), argv=argv, env=dict(invocation.env))
        return profile
    # Additional filesystem grants, scratch guidance and command grants are never
    # dropped when moving from print/exec to ACP. Refuse until mapped+qualified.
    if any((invocation.allowed_commands, invocation.native_settings, invocation.group_report_directory)) or (
            name != "codex" and any((invocation.extra_writable_paths, invocation.scratch_directory,
                                     invocation.shared_scratch_directories, invocation.system_guidance))):
        raise ConversationRefused("native conversation grant/configuration profile needs qualification")
    if name == "codex":
        argv = invocation.argv
        cwd, sandbox = _option(argv, "-C"), _option(argv, "-s")
        config: dict[str, Any] = {}
        pins: list[tuple[str, Any]] = []
        for i, token in enumerate(argv[:-1]):
            if token != "-c":
                continue
            key, _, value = argv[i + 1].partition("=")
            try:
                decoded = json.loads(value)
            except ValueError:
                raise ConversationRefused("unmapped Codex config pin: " + key) from None
            pins.append((key, decoded))
            node = config
            *parents, leaf = key.split(".")
            for parent in parents:
                node = node.setdefault(parent, {})
            node[leaf] = decoded
        policy = config.get("approval_policy")
        if policy not in {"never", "on-request"} or (policy == "on-request" and not invocation.caller_approvals):
            raise ConversationRefused("Codex approval policy conversion is not authorized")
        separator = argv.index("--")
        session = argv[separator + 1] if "resume" in argv[:separator] else None
        resolved_network = backend._resolve_network(freedom, network)
        additional_roots = [argv[i + 1] for i, token in enumerate(argv[:-1]) if token == "--add-dir"]
        configured_roots = list(config.get("sandbox_workspace_write", {}).get("writable_roots", []))
        if any(type(path) is not str or not Path(path).is_absolute() for path in configured_roots):
            raise ConversationRefused("configured Codex roots require validated absolute paths")
        if configuration_witness is not None:
            if sandbox != 'workspace-write' or configuration_witness.get('cwd') != cwd:
                raise ConversationRefused('Codex configuration witness scope mismatch')
            from .conversation_codex_config import FIELDS
            workspace = configuration_witness['configuration']['workspace']
            if workspace['network_access'] != resolved_network:
                raise ConversationRefused('Codex effective network differs from source policy')
            configured_roots = list(workspace['writable_roots'])
            config['sandbox_workspace_write'] = {key: workspace[key] for key in FIELDS}
            profile['configuration_witness'] = configuration_witness
        combined_roots = [*configured_roots, *additional_roots]
        if sandbox == "workspace-write":
            config.setdefault("sandbox_workspace_write", {})["network_access"] = resolved_network
            config["sandbox_workspace_write"]["writable_roots"] = combined_roots
            pins = [(key, value) for key, value in pins if key not in {
                "sandbox_workspace_write.network_access", "sandbox_workspace_write.writable_roots"}]
            pins.append(("sandbox_workspace_write.network_access", resolved_network))
            pins.append(("sandbox_workspace_write.writable_roots", combined_roots))
        native_argv = [argv[0], "app-server", "--listen", "stdio://"]
        # Keys are raw TOML namespace paths; quoting the key changes its meaning.
        for key, value in pins:
            native_argv.extend(["-c", key + "=" + json.dumps(value)])
        native_argv.extend(["-c", "sandbox_mode=" + json.dumps(sandbox), "-c", 'approvals_reviewer="user"'])
        if configuration_witness is not None:
            from .conversation_codex_config import baseline_argv
            native_argv = baseline_argv(invocation, sandbox)
        profile.update(transport="codex-app-server", cwd=cwd, sandbox=sandbox,
                       approval_policy=policy, model=_option(argv, "-m"), network=resolved_network,
                       writable_roots=[cwd, *combined_roots] if sandbox == "workspace-write" else [],
                       config=config, session_id=session, argv=native_argv, env=dict(invocation.env))
        if sandbox == "workspace-write":
            profile["configured_writable_roots"] = configured_roots
            if session is not None:
                # Resume must replace roots retained by the previous process.
                # Only current explicit authority is sent, never echoed old grants.
                profile['runtime_workspace_roots'] = [cwd, *combined_roots]
        return profile
    mode_maps = {"vibe": {"read_only": "ask", "write_in_repo": "accept-edits", "publish": "auto-approve", "unrestricted": "auto-approve"},
                 "antigravity": {"read_only": "plan", "write_in_repo": "accept-edits"},
                 "cursor": {"read_only": "plan", "write_in_repo": "agent", "publish": "agent", "unrestricted": "agent"}}
    mode = mode_maps[name].get(freedom)
    if mode is None:
        raise ConversationRefused("ACP has no qualified matching permission mode for this freedom")
    argv = invocation.argv
    cwd = _option(argv, "--workdir" if name == "vibe" else "--add-dir" if name == "antigravity" else "--workspace")
    if not cwd or not Path(cwd).is_absolute():
        raise ConversationRefused("ACP requires exact absolute working directory")
    effort = _option(argv, "--effort")
    if effort:
        raise ConversationRefused("ACP effort mapping requires a separately qualified profile")
    model = _option(argv, "--model")
    env = dict(invocation.env)
    if name == "vibe":
        cap = _option(argv, "--max-turns")
        if cap is not None:
            profile["max_turns"] = int(cap)
        if profile_name != "shipped-medium35-high":
            raise ConversationRefused("Vibe default max profile failed; explicit high profile required")
        model, effort = "mistral-medium-3.5", "high"
        profile["print_trust_flag"] = "--trust" in argv
        env["VIBE_MODELS"] = json.dumps({model: {"name": "mistral-vibe-cli-latest", "provider": "mistral", "alias": model, "thinking": "high"}}, separators=(",", ":"))
        env["VIBE_ACTIVE_MODEL"] = model
        native_argv = ["vibe-acp"]
    elif name == "antigravity":
        native_argv = [argv[0], "acp", "--stdio"]
    else:
        native_argv = [argv[0], "acp"]
    session = _option(argv, "--resume") or _option(argv, "--conversation")
    profile.update(transport="acp", mode=mode, cwd=cwd, model=model, effort=effort,
                   session_id=session, argv=native_argv, env=env)
    return profile


def restore_conversation_port(context: dict[str, Any], *, validated_profile: dict[str, Any]) -> ConversationPort:
    """Reconstruct only after the owner independently revalidates the exact profile.

    Equality is a guard against mixing records, not authentication. A persisted
    mapping by itself is never sufficient authority for a launch or a grant.
    """
    if type(context) is not dict or context != validated_profile:
        raise ConversationRefused("conversation context does not match independently validated profile")
    name = context.get("backend")
    if context.get("v") != 1 or context.get("version") not in SUPPORTED_PROTOCOL_VERSIONS.get(name, ()):
        raise ConversationRefused("unqualified conversation profile")
    if name == "claude":
        return ClaudeConversationPort(context)
    if name == "codex":
        return CodexConversationPort(context)
    return ACPConversationPort(context)


def configure_conversation(backend: Any, invocation: Invocation, *, version: str, freedom: str,
                           network: bool | None = None, profile_name: str | None = None,
                           qualified: bool = False) -> ConversationLaunch:
    """Prepare a launch-selected native transport. Never upgrades a running task.

    `qualified` is the launch owner's explicit source/profile qualification
    decision. It must not be populated from serialized conversation metadata.
    Initial stdin fields remain intact for provenance; the conversation owner
    issues context.initial_prompt through the port, never the legacy stdin pump.
    """
    profile = conversation_profile(backend, invocation, version=version, freedom=freedom,
                                   network=network, profile_name=profile_name)
    if qualified is not True:
        raise ConversationRefused("native conversation profile requires explicit owner qualification")
    port = restore_conversation_port(profile, validated_profile=profile)
    converted = replace(invocation, argv=profile["argv"], env=profile["env"],
                        conversation_context=profile, identity_markers=tuple(profile["argv"][:2]))
    return ConversationLaunch(converted, port)
