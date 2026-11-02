"""Vibe 3.0.3 ACP transport. Each task owns one process and one session/prompt.

Approval input comes from a correlated tool_call update, never the display title.
Only allow_once/reject_once are emitted: no session or permanent grant is installed.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from .base import ApprovalRefused, Invocation, UnsupportedCapability, InvocationEnvironmentError, validate_invocation_env
from . import normalize as nz

BINARY = "vibe-acp"
TRANSPORT = "vibe-acp"
VERSION = "3.0.3"
IDENTITY_MARKERS = (BINARY,)
MODES = {"read_only": "ask", "write_in_repo": "accept-edits", "publish": "auto-approve", "unrestricted": "auto-approve"}
REQUEST = "session/request_permission"
MAX_KEY = 256
BOOTSTRAP_TIMEOUT_SECONDS = 30.0
WRITE_TOOLS = frozenset({"write_file", "edit"})
TOOLS = frozenset({"bash", "read_file", *WRITE_TOOLS})


def typed_key(value: Any) -> str | None:
    if type(value) is int:
        return f"i:{value}"
    if isinstance(value, str) and 0 < len(value) <= MAX_KEY:
        return "s:" + value
    return None


def untyped(value: str) -> int | str:
    if not isinstance(value, str) or len(value) < 3:
        raise ValueError("Invalid typed ACP id")
    if value.startswith("i:"):
        parsed = int(value[2:])
        if typed_key(parsed) == value:
            return parsed
    elif value.startswith("s:") and typed_key(value[2:]) == value:
        return value[2:]
    raise ValueError("Invalid typed ACP id")


def _line(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()


def runtime_identity(binary: str | None = None) -> dict[str, Any]:
    """Fingerprint the actual ACP launcher, Python and shipped ACP/harness code.

    The ordinary Rust `vibe` executable is a different transport and is not used
    as identity evidence for this Python ACP entrypoint.
    """
    found = binary or shutil.which(BINARY)
    if not found:
        raise UnsupportedCapability("vibe-acp is not installed")
    launcher = Path(found).absolute()
    try:
        first = launcher.read_bytes().splitlines()[0].decode()
        if not first.startswith("#!/") or " " in first[2:]:
            raise ValueError("ACP requires an absolute Python interpreter shebang")
        interpreter = Path(first[2:])
        roots = list((interpreter.parent.parent / "lib").glob("python*/site-packages"))
        if len(roots) != 1:
            raise ValueError("ACP runtime packages could not be identified")
        packages = [roots[0] / name for name in ("vibe", "acp", "mistralai_vibe_local_harness")]
        if not all(path.is_dir() for path in packages):
            raise ValueError("ACP or local harness package is unavailable")
        files = [launcher, interpreter.resolve()]
        for package in packages:
            files.extend(sorted(path for path in package.rglob("*") if path.is_file() and path.suffix in {".py", ".so", ".dylib", ".pyd"}))
        return {"launcher": str(launcher), "files": [[str(path), hashlib.sha256(path.read_bytes()).hexdigest()] for path in files]}
    except (OSError, UnicodeError, ValueError) as exc:
        raise UnsupportedCapability(f"Cannot establish vibe-acp runtime identity: {exc}") from exc


def validate_context(ctx: Any) -> dict[str, Any]:
    if not isinstance(ctx, dict) or ctx.get("transport") != TRANSPORT or ctx.get("version") != VERSION:
        raise ValueError("Invalid or unqualified Vibe ACP context")
    for name in ("cwd", "prompt", "mode"):
        if not isinstance(ctx.get(name), str) or not ctx[name]:
            raise ValueError(f"Invalid Vibe ACP {name}")
    if not os.path.isabs(ctx["cwd"]) or ctx.get("freedom") not in MODES or ctx["mode"] != MODES[ctx["freedom"]]:
        raise ValueError("Invalid Vibe ACP cwd or freedom/mode")
    for name in ("model", "session_id"):
        if ctx.get(name) is not None and (not isinstance(ctx[name], str) or not ctx[name] or "\x00" in ctx[name]):
            raise ValueError(f"Invalid Vibe ACP {name}")
    if ctx.get("max_turns") is not None and (type(ctx["max_turns"]) is not int or ctx["max_turns"] < 1):
        raise ValueError("Invalid Vibe ACP turn cap")
    for name in ("write", "caller_approvals"):
        if type(ctx.get(name, False)) is not bool:
            raise ValueError(f"Invalid Vibe ACP {name}")
    if ctx.get("trust") not in {None, "trust_cwd"}:
        raise ValueError("Invalid Vibe ACP trust decision")
    patterns = ctx.get("allowed_patterns", [])
    if not isinstance(patterns, list) or not all(isinstance(p, str) and p for p in patterns):
        raise ValueError("Invalid Vibe ACP allowed patterns")
    identity = ctx.get("runtime_identity")
    if identity is not None and (not isinstance(identity, dict) or not isinstance(identity.get("launcher"), str) or not isinstance(identity.get("files"), list)):
        raise ValueError("Invalid Vibe ACP runtime identity")
    allowed = {"transport", "version", "cwd", "prompt", "freedom", "mode", "model", "session_id", "max_turns", "write", "caller_approvals", "trust", "allowed_patterns", "runtime_identity", "vibe_commands"}
    if set(ctx) - allowed:
        raise ValueError("Unknown Vibe ACP context fields")
    return ctx


def argv(ctx: dict[str, Any]) -> list[str]:
    identity = ctx.get("runtime_identity")
    return [identity["launcher"] if identity else BINARY]


def assert_safe(invocation: Invocation, freedom: str) -> str | None:
    try:
        ctx = validate_context(invocation.control_context)
        expected_env = {"VIBE_CLI": "python", **({"VIBE_ACTIVE_MODEL": ctx["model"]} if ctx.get("model") else {})}
        validate_invocation_env(invocation, expected_env)
    except (ValueError, InvocationEnvironmentError) as exc:
        return str(exc)
    if (invocation.argv != argv(ctx) or ctx["freedom"] != freedom or not invocation.control_pipe
            or invocation.stdin_mode != "devnull" or invocation.initial_input is not None
            or invocation.identity_markers != IDENTITY_MARKERS or invocation.native_subagent
            or invocation.extra_writable_paths or invocation.system_guidance is not None
            or invocation.system_guidance_root is not None or invocation.scratch_directory is not None
            or invocation.shared_scratch_directories
            or not isinstance(invocation.native_settings or {}, dict)
            or set(invocation.native_settings or {}) - {"vibe_commands"}
            or invocation.allowed_commands_version != (VERSION if invocation.allowed_commands else None)
            or invocation.caller_approvals is not ctx.get("caller_approvals", False)
            or invocation.caller_write_approvals is not ctx.get("write", False)
            or tuple(ctx.get("allowed_patterns", ())) != invocation.allowed_commands
            or ctx.get("vibe_commands") != (invocation.native_settings or {}).get("vibe_commands")):
        return "Vibe ACP invocation is not the exact qualified control shape"
    if invocation.caller_approval_version != (VERSION if ctx.get("caller_approvals") else None):
        return "Vibe ACP caller approval version mismatch"
    if invocation.allowed_commands:
        from .vibe_commands import assert_safe as assert_commands
        try:
            assert_commands(invocation, freedom)
        except (ValueError, UnsupportedCapability) as exc:
            return str(exc)
        if ctx["vibe_commands"].get("repo") != ctx["cwd"] or ctx["vibe_commands"].get("version") != ctx["version"]:
            return "Vibe ACP command grant repository or version differs from control context"
    try:
        if ctx.get("runtime_identity") is None or runtime_identity(ctx["runtime_identity"]["launcher"]) != ctx["runtime_identity"]:
            return "Vibe ACP runtime changed since qualification"
    except UnsupportedCapability as exc:
        return str(exc)
    return None


def prepare(invocation: Invocation, parts: dict[str, Any], freedom: str, version: str | None = VERSION,
            write: bool = False, environ: Any = None, *, caller_approvals: bool | None = None) -> Invocation:
    if version != VERSION:
        raise UnsupportedCapability("Vibe ACP is qualified only on 3.0.3")
    enabled = invocation.caller_approvals if caller_approvals is None else caller_approvals
    validate(enabled, freedom, parts.get("max_turns"), invocation.native_subagent)
    if invocation.extra_writable_paths:
        raise UnsupportedCapability("Vibe ACP extra writable paths are not qualified")
    ctx = {"transport": TRANSPORT, "version": VERSION, "cwd": os.path.realpath(parts["repo"]),
           "prompt": parts["prompt"], "freedom": freedom, "mode": MODES[freedom], "model": parts.get("model"),
           "session_id": parts.get("session_id", parts.get("thread_id")), "max_turns": parts.get("max_turns"),
           "write": write, "caller_approvals": enabled, "allowed_patterns": list(invocation.allowed_commands),
           "runtime_identity": runtime_identity(), "trust": "trust_cwd",
           **({"vibe_commands": copy.deepcopy(invocation.native_settings["vibe_commands"])} if (invocation.native_settings or {}).get("vibe_commands") else {})}
    validate_context(ctx)
    return replace(invocation, argv=argv(ctx), stdin_mode="devnull", initial_input=None, control_pipe=True,
                   control_context=ctx, identity_markers=IDENTITY_MARKERS, caller_approvals=enabled,
                   caller_write_approvals=write, caller_approval_version=VERSION if enabled else None,
                   env={"VIBE_CLI": "python", **({"VIBE_ACTIVE_MODEL": ctx["model"]} if ctx.get("model") else {})})


def validate(enabled: bool, freedom: str, max_turns: int | None, native: bool = False) -> None:
    if type(enabled) is not bool:
        raise ValueError("caller_approvals must be a boolean")
    if freedom not in MODES or native:
        raise UnsupportedCapability("Vibe ACP requires a non-native task and a supported freedom")
    if max_turns is not None and (type(max_turns) is not int or max_turns < 1):
        raise ValueError("max_turns must be a positive integer")


def support() -> dict[str, Any]:
    return {"supported": True, "transport": TRANSPORT, "versions": [VERSION], "tools": sorted(TOOLS),
            "freedoms": list(MODES), "scope": "exact_action_once", "native": False, "turn_caps": True,
            "write_approvals": True, "write_tools": sorted(WRITE_TOOLS), "workflows": False}


class State:
    def __init__(self, ctx: dict[str, Any]):
        self.ctx = copy.deepcopy(validate_context(ctx))
        self.started = False
        self.bootstrap_deadline: float | None = None
        self.next_id = 1
        self.pending: dict[str, tuple[str, Any]] = {}
        self.outbox: list[bytes] = []
        self.observed_version: str | None = None
        self.session_id: str | None = None
        self.verified = False
        self.trust_reloaded = False
        self.prompt_started = False
        self.terminal: dict[str, Any] | None = None
        self.failed: str | None = None
        self.tools: dict[str, dict[str, Any]] = {}
        self.ambiguous: set[str] = set()
        self.admitted: dict[str, dict[str, Any]] = {}
        self.seen_requests: set[str] = set()
        self.responded_requests: set[str] = set()
        self.answers: dict[str, dict[str, Any]] = {}
        self.normalized_calls: set[str] = set()
        self.normalized_results: set[str] = set()
        self.decoded_requests: dict[str, dict[str, Any]] = {}
        self.config_queue: list[tuple[str, str]] = []
        self.text = ""
        self.usage: dict[str, Any] | None = None
        self.effects_seen: set[str] = set()

    @property
    def admissible(self) -> bool:
        return self.verified and self.prompt_started and self.session_id is not None and self.terminal is None and self.failed is None


def state_for(acc: Any) -> State | None:
    state = acc.stream_state.get("vibe_acp")
    ctx = acc.stream_state.get("control_context")
    if state is None and isinstance(ctx, dict) and ctx.get("transport") == TRANSPORT:
        state = acc.stream_state["vibe_acp"] = State(ctx)
    return state


def request_frame(state: State, method: str, params: dict[str, Any], check: Any = None) -> bytes:
    allowed = {"initialize": {"protocolVersion", "clientCapabilities", "clientInfo"},
               "session/new": {"cwd", "mcpServers"}, "session/load": {"sessionId", "cwd", "mcpServers"},
               "session/close": {"sessionId"}, "session/set_config_option": {"sessionId", "configId", "value"},
               "session/prompt": {"sessionId", "prompt"}, "_trust/decision": {"sessionId", "cwd", "decision"}}
    if method not in allowed or set(params) != allowed[method]:
        raise ValueError("Unapproved Vibe ACP request fields")
    if method == "session/set_config_option":
        # The native model setter persists the user's default. Models travel
        # only through VIBE_ACTIVE_MODEL and are verified in returned options.
        expected = {"mode": state.ctx["mode"], "max_turns": str(state.ctx.get("max_turns"))}
        if params["configId"] not in expected or params["value"] != expected[params["configId"]]:
            raise ValueError("Unqualified or persistent Vibe ACP configuration mutation")
    number = state.next_id
    state.next_id += 1
    state.pending[typed_key(number)] = (method, check)
    return _line({"jsonrpc": "2.0", "id": number, "method": method, "params": params})


def initial_frames(acc: Any) -> list[bytes]:
    state = state_for(acc)
    if state is None or state.started:
        return []
    state.started = True
    state.bootstrap_deadline = time.monotonic() + BOOTSTRAP_TIMEOUT_SECONDS
    return [request_frame(state, "initialize", {"protocolVersion": 1, "clientCapabilities": {}, "clientInfo": {"name": "harflow", "version": "0"}})]


def turn_frames(acc: Any) -> list[bytes]:
    state = state_for(acc)
    if state is None:
        return []
    if (not state.prompt_started and state.bootstrap_deadline is not None
            and time.monotonic() >= state.bootstrap_deadline and state.failed is None):
        state.failed = "Vibe ACP initialization/configuration timed out before prompt"
        state.outbox.clear()
        raise ApprovalRefused(state.failed)
    if state.failed is not None or state.terminal is not None:
        state.outbox.clear()
        return []
    frames, state.outbox = state.outbox, []
    return frames


def outgoing_frames(phase: str, acc: Any) -> list[bytes]:
    return initial_frames(acc) if phase == "initial" else turn_frames(acc) if phase == "turn" else []


def finished(acc: Any) -> bool:
    state = state_for(acc)
    return state is not None and (state.terminal is not None or state.failed is not None)


def _expect(value: Any, message: str) -> None:
    if not value:
        raise ApprovalRefused("Vibe ACP verification failed: " + message)


def _current(options: Any, name: str) -> Any:
    if not isinstance(options, list):
        return None
    found = [o for o in options if isinstance(o, dict) and o.get("id") == name]
    return found[0].get("currentValue") if len(found) == 1 else None


def _next_config(state: State) -> None:
    if state.config_queue:
        key, value = state.config_queue.pop(0)
        state.outbox.append(request_frame(state, "session/set_config_option", {"sessionId": state.session_id, "configId": key, "value": value}, (key, value)))
    else:
        state.verified = True
        state.prompt_started = True
        state.outbox.append(request_frame(state, "session/prompt", {"sessionId": state.session_id, "prompt": [{"type": "text", "text": state.ctx["prompt"]}]}))


def _configure(state: State) -> None:
    state.config_queue = [("mode", state.ctx["mode"])]
    if state.ctx.get("max_turns") is not None:
        state.config_queue.append(("max_turns", str(state.ctx["max_turns"])))
    _next_config(state)


def step(state: State, event: dict[str, Any]) -> None:
    """Idempotent when generic approval observation and ingest see the same frame."""
    if state.failed is not None:
        return
    try:
        _step(state, event)
    except (ApprovalRefused, ValueError, TypeError) as exc:
        state.failed = str(exc)
        if isinstance(exc, ApprovalRefused):
            raise
        raise ApprovalRefused("Malformed Vibe ACP frame: " + str(exc)) from exc


def _step(state: State, event: dict[str, Any]) -> None:
    if event.get("method") is None and "id" in event:
        expected = state.pending.pop(typed_key(event["id"]), None)
        if expected is None:
            return
        method, check = expected
        if not state.prompt_started:
            state.bootstrap_deadline = time.monotonic() + BOOTSTRAP_TIMEOUT_SECONDS
        _expect("error" not in event, f"{method} returned an error: {event.get('error')}")
        result = event.get("result")
        _expect(isinstance(result, dict), f"{method} result missing")
        if method == "initialize":
            info = result.get("agentInfo")
            state.observed_version = info.get("version") if isinstance(info, dict) else None
            _expect(state.observed_version == state.ctx["version"] and isinstance(info, dict) and info.get("name") == "@mistralai/mistral-vibe"
                    and type(result.get("protocolVersion")) is int and result["protocolVersion"] == 1, "unqualified runtime identity, version or protocol")
            resumed = state.ctx.get("session_id")
            params = {"cwd": state.ctx["cwd"], "mcpServers": []}
            if resumed:
                params["sessionId"] = resumed
            state.outbox.append(request_frame(state, "session/load" if resumed else "session/new", params))
        elif method in {"session/new", "session/load"}:
            session = (state.session_id or state.ctx.get("session_id")) if method == "session/load" else result.get("sessionId")
            _expect(isinstance(session, str) and bool(session), "session id missing")
            if method == "session/load" and "sessionId" in result:
                _expect(result["sessionId"] == session, "loaded session mismatch")
            state.session_id = session
            meta = result.get("_meta")
            _expect(isinstance(meta, dict), "workspace trust metadata missing")
            trust = meta.get("workspace_trust")
            _expect(isinstance(trust, dict) and trust.get("status") in {"trusted", "session", "untrusted"}, "workspace trust metadata missing")
            if trust.get("status") == "untrusted" and trust.get("details") is not None:
                _expect(state.ctx.get("trust") == "trust_cwd", "workspace is untrusted; ACP has no ephemeral --trust equivalent")
                state.outbox.append(request_frame(state, "_trust/decision", {"sessionId": session, "cwd": state.ctx["cwd"], "decision": "trust_cwd"}))
            else:
                _configure(state)
        elif method == "_trust/decision":
            _expect(result.get("trust_status") == "trusted", "workspace trust decision was not applied")
            _expect(not state.trust_reloaded, "workspace trust did not survive session reload")
            state.trust_reloaded = True
            state.outbox.append(request_frame(state, "session/close", {"sessionId": state.session_id}))
        elif method == "session/close":
            params = {"cwd": state.ctx["cwd"], "mcpServers": []}
            if state.ctx.get("session_id"):
                params["sessionId"] = state.ctx["session_id"]
                state.outbox.append(request_frame(state, "session/load", params))
            else:
                state.session_id = None
                state.outbox.append(request_frame(state, "session/new", params))
        elif method == "session/set_config_option":
            name, wanted = check
            # Vibe 3.0.3 applies max_turns but doesn't include it in configOptions;
            # source inspection plus capped execution measures that seam separately.
            if name != "max_turns":
                _expect(_current(result.get("configOptions"), name) == wanted, f"{name} was not applied")
            _expect(_current(result.get("configOptions"), "mode") == state.ctx["mode"], "permission mode changed")
            if state.ctx.get("model"):
                _expect(_current(result.get("configOptions"), "model") == state.ctx["model"], "selected model changed")
            _next_config(state)
        elif method == "session/prompt":
            reason = result.get("stopReason")
            _expect(reason in {"end_turn", "cancelled", "max_tokens", "max_turn_requests", "refusal"}, "unknown terminal stop reason")
            state.terminal = {"stop_reason": reason, "stopReason": reason, "status": "completed" if reason == "end_turn" else "failed"}
            # A queued one-shot grant cannot outlive its active prompt.
            state.outbox.clear()
            usage = result.get("usage")
            if isinstance(usage, dict):
                fields = {"inputTokens": "input_tokens", "outputTokens": "output_tokens", "totalTokens": "total_tokens", "cachedReadTokens": "cached_input_tokens"}
                state.usage = {target: usage[source] for source, target in fields.items() if type(usage.get(source)) is int and usage[source] >= 0} or None
        return
    if event.get("method") != "session/update":
        return
    params = event.get("params")
    _expect(isinstance(params, dict), "session update missing parameters")
    # session/load replays historical updates before its response; ignore all
    # updates until the current prompt has actually been emitted.
    if not state.prompt_started or state.terminal is not None:
        return
    _expect(params.get("sessionId") == state.session_id, "foreign session update")
    update = params.get("update")
    _expect(isinstance(update, dict), "session update missing update")
    kind = update.get("sessionUpdate")
    if kind == "config_option_update":
        _expect(_current(update.get("configOptions"), "mode") == state.ctx["mode"], "permission mode changed during prompt")
        if state.ctx.get("model"):
            _expect(_current(update.get("configOptions"), "model") == state.ctx["model"], "model changed during prompt")
    elif kind in {"tool_call", "tool_call_update"}:
        tool_id = update.get("toolCallId")
        _expect(isinstance(tool_id, str) and 0 < len(tool_id) <= MAX_KEY, "tool call id missing")
        _expect(update.get("_meta") is None or isinstance(update.get("_meta"), dict), "malformed tool metadata")
        _expect(update.get("kind") is None or isinstance(update.get("kind"), str), "malformed tool kind")
        _expect(update.get("status") is None or isinstance(update.get("status"), str), "malformed tool status")
        prior = state.tools.get(tool_id)
        raw = update.get("rawInput")
        if prior is not None:
            before_meta = prior.get("_meta") or {}
            after_meta = update.get("_meta") or {}
            changed_identity = (("kind" in update and update["kind"] is not None and update["kind"] != prior.get("kind"))
                                or (isinstance(after_meta, dict) and "tool_name" in after_meta
                                    and (not isinstance(before_meta, dict) or after_meta["tool_name"] != before_meta.get("tool_name"))))
            if changed_identity:
                state.ambiguous.add(tool_id)
                if tool_id in state.admitted:
                    raise ApprovalRefused("Vibe ACP changed tool identity after approval admission")
        if prior is not None and raw is not None and prior.get("rawInput") is not None and raw != prior["rawInput"]:
            state.ambiguous.add(tool_id)
            if tool_id in state.admitted:
                raise ApprovalRefused("Vibe ACP changed tool input after approval admission")
        if kind == "tool_call" and prior is not None and any(
                key in update and key in prior and update[key] != prior[key]
                for key in ("rawInput", "kind", "_meta")):
            state.ambiguous.add(tool_id)
        merged = {**(prior or {}), **{k: copy.deepcopy(v) for k, v in update.items() if v is not None}}
        state.tools[tool_id] = merged


def is_frame(event: Any) -> bool:
    return isinstance(event, dict) and event.get("jsonrpc") == "2.0"


def frame_kind(event: dict[str, Any]) -> str | None:
    return "request" if isinstance(event.get("method"), str) and "id" in event else None


def request_key(event: dict[str, Any]) -> str | None:
    return typed_key(event.get("id"))


def _permissions(options: list[Any]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] | None = None
    for option in options:
        meta = option.get("_meta") if isinstance(option, dict) else None
        permissions = meta.get("required_permissions") if isinstance(meta, dict) else None
        if permissions is not None:
            _expect(isinstance(permissions, list) and all(isinstance(p, dict) for p in permissions), "malformed permission metadata")
            _expect(found is None or found == permissions, "conflicting permission metadata")
            found = permissions
    return copy.deepcopy(found or [])


def decode(event: dict[str, Any], qualified: dict[str, Any] | None) -> dict[str, Any]:
    state = (qualified or {}).get("state")
    _expect(isinstance(state, State) and state.admissible, "permission requested outside verified active prompt")
    key, params = request_key(event), event.get("params")
    cached = getattr(state, "cached_decoded", None)
    if cached is not None and cached[0] == key:
        state.cached_decoded = None
        _expect(cached[2] == event, "permission request identity changed")
        return copy.deepcopy(cached[1])
    _expect(key is not None and isinstance(params, dict), "permission request has no typed identity")
    base = {"native_request_id": key, "native_method": event.get("method"), "tool_use_id": f"request:{key}",
            "action": {"tool_name": "request", "input": {}}, "may_run_outside_sandbox": True}
    if event.get("method") != REQUEST:
        return {**base, "system_rejection": "unsupported_request"}
    _expect(params.get("sessionId") == state.session_id, "permission request belongs to another session")
    options = params.get("options")
    _expect(isinstance(options, list), "permission options missing")
    _expect(all(isinstance(o, dict) and isinstance(o.get("optionId"), str) and bool(o["optionId"])
                and isinstance(o.get("kind"), str) and bool(o["kind"]) for o in options), "malformed permission options")
    ids = [o["optionId"] for o in options]
    _expect(len(set(ids)) == len(ids), "duplicate permission option identities")
    pairs = [(o["optionId"], o["kind"]) for o in options]
    _expect(pairs.count(("allow_once", "allow_once")) == 1 and pairs.count(("reject_once", "reject_once")) == 1, "one-shot allow/reject options missing or ambiguous")
    tool = params.get("toolCall")
    tool_id = tool.get("toolCallId") if isinstance(tool, dict) else None
    _expect(isinstance(tool_id, str) and 0 < len(tool_id) <= MAX_KEY, "permission tool id missing")
    base.update(tool_use_id=tool_id, allow_option_id="allow_once", reject_option_id="reject_once", required_permissions=_permissions(options))
    update = state.tools.get(tool_id)
    if update is None:
        return {**base, "system_rejection": "tool_call_not_started"}
    if not isinstance(update.get("status"), str) or update["status"] not in {"pending", "in_progress"}:
        return {**base, "system_rejection": "tool_call_not_active"}
    if any(field in tool and tool[field] is not None and tool[field] != update.get(field) for field in ("rawInput", "_meta", "kind")):
        return {**base, "system_rejection": "tool_call_changed"}
    meta = update.get("_meta")
    tool_name = meta.get("tool_name") if isinstance(meta, dict) else None
    raw = update.get("rawInput")
    base["action"] = {"tool_name": tool_name if isinstance(tool_name, str) else "request", "input": copy.deepcopy(raw) if isinstance(raw, dict) else {}}
    if key in state.seen_requests or tool_id in state.admitted or tool_id in state.ambiguous:
        return {**base, "system_rejection": "ambiguous_tool_call"}
    state.seen_requests.add(key)
    if not isinstance(tool_name, str) or tool_name not in TOOLS or not isinstance(raw, dict):
        return {**base, "system_rejection": "unsupported_tool"}
    if tool_name == "bash" and (not isinstance(raw.get("command"), str) or not raw["command"].strip()):
        return {**base, "system_rejection": "missing_command"}
    if tool_name != "bash" and (not isinstance(raw.get("filePath"), str) or not raw["filePath"]):
        return {**base, "system_rejection": "invalid_path"}
    # Freeze full original tool input. A later differing update trips verification.
    state.admitted[tool_id] = copy.deepcopy(base)
    if tool_name in WRITE_TOOLS and not state.ctx.get("write"):
        return {**base, "system_rejection": "write_approval_disabled"}
    return base


def encode(request: dict[str, Any], decision: str) -> bytes:
    if decision not in {"approve", "reject", "cancel"}:
        raise ValueError("Vibe ACP decision must be approve, reject or cancel")
    if request.get("system_rejection") or request.get("expiry_rejection"):
        if decision != "reject":
            raise ValueError("A refused Vibe ACP request cannot be approved")
    wire_id = untyped(request["native_request_id"])
    if request.get("native_method") != REQUEST:
        return _line({"jsonrpc": "2.0", "id": wire_id, "error": {"code": -32601, "message": "Harflow refused unsupported ACP request"}})
    if decision == "cancel":
        outcome = {"outcome": "cancelled"}
    else:
        option = request.get("allow_option_id" if decision == "approve" else "reject_option_id")
        _expect(option == ("allow_once" if decision == "approve" else "reject_once"), "unqualified response option")
        outcome = {"outcome": "selected", "optionId": option}
    return _line({"jsonrpc": "2.0", "id": wire_id, "result": {"outcome": outcome}})


def cancel_frame(state: State) -> bytes:
    _expect(state.session_id is not None, "cannot cancel before session creation")
    return _line({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": state.session_id}})


def outcomes(event: dict[str, Any], state: State | None) -> list[tuple[str, str]]:
    if state is None or not state.admissible or event.get("method") != "session/update":
        return []
    params = event.get("params")
    if not isinstance(params, dict) or params.get("sessionId") != state.session_id:
        return []
    update = params.get("update")
    if not isinstance(update, dict):
        return []
    tool_id, status = update.get("toolCallId"), update.get("status")
    if tool_id not in state.admitted or tool_id in state.ambiguous or status not in {"completed", "failed"}:
        return []
    return [(tool_id, "succeeded" if status == "completed" else "failed")]


def change_paths(action: dict[str, Any]) -> list[str]:
    raw = action.get("input")
    path = raw.get("filePath") if isinstance(raw, dict) else None
    return [path] if action.get("tool_name") in {*WRITE_TOOLS, "read_file"} and isinstance(path, str) and path else []


def protected_paths(repo: Any, environ: Any = None) -> list[Path]:
    env = os.environ if environ is None else environ
    home = Path(env.get("HOME") or Path.home())
    found = [Path(env.get("VIBE_HOME") or home / ".vibe")]
    cwd = Path(os.path.realpath(repo))
    for directory in (cwd, *cwd.parents):
        found.extend([directory / ".vibe", directory / ".agents"])
    return found


def ingest(event: dict[str, Any], acc: Any) -> None:
    state = state_for(acc)
    if state is None:
        return
    step(state, event)
    if (frame_kind(event) == "request" and not state.ctx.get("caller_approvals")
            and request_key(event) not in state.responded_requests):
        request = decode(event, {"state": state})
        state.outbox.append(encode(request, "reject"))
        state.responded_requests.add(request_key(event))
        note_answer(acc, {**request, "system_rejection": request.get("system_rejection") or "caller_approvals_disabled"}, "reject", True)
    params = event.get("params")
    if (state.admissible and event.get("method") == "session/update" and isinstance(params, dict)
            and params.get("sessionId") == state.session_id):
        update = params.get("update")
        if isinstance(update, dict) and update.get("sessionUpdate") == "agent_message_chunk":
            content = update.get("content")
            if isinstance(content, dict) and content.get("type") == "text" and isinstance(content.get("text"), str):
                _clear_refusal(acc)
                state.text += content["text"]
        if isinstance(update, dict) and update.get("status") == "failed":
            raw = update.get("rawOutput")
            error = raw.get("error", raw) if isinstance(raw, dict) else None
            if isinstance(error, dict) and error.get("code") == "tool_denied":
                tool_id = update.get("toolCallId")
                full = state.tools.get(tool_id, {})
                meta = full.get("_meta") or {}
                denied = {"tool_use_id": tool_id, "tool": meta.get("tool_name"), "tool_input": copy.deepcopy(full.get("rawInput") or {}),
                          "error_code": "tool_denied", "refusal_text": error.get("message")}
                _record_denial(acc, state, denied)
    if state.usage:
        acc.usage = copy.deepcopy(state.usage)
    # A trust decision recreates the pre-prompt runtime. Its provisional id is
    # not a resumable conversation; expose only the verified final session,
    # before the queued prompt is flushed to the process.
    if state.session_id and state.verified:
        acc.session_id = state.session_id
    acc.summary = state.text or None
    acc.saw_final_message = bool(state.text)
    if state.terminal:
        acc.terminal = copy.deepcopy(state.terminal)


def normalize(event: dict[str, Any], acc: Any) -> list[dict[str, Any]]:
    state = state_for(acc)
    if state is None or not state.prompt_started or event.get("method") != "session/update":
        return []
    params = event.get("params")
    if not isinstance(params, dict) or params.get("sessionId") != state.session_id:
        return []
    update = params.get("update")
    if not isinstance(update, dict):
        return []
    kind = update.get("sessionUpdate")
    content = update.get("content")
    if kind in {"agent_message_chunk", "agent_thought_chunk"} and isinstance(content, dict) and content.get("type") == "text" and isinstance(content.get("text"), str):
        return [nz.assistant_text(content["text"])] if kind == "agent_message_chunk" else []
    tool_id = update.get("toolCallId")
    full = state.tools.get(tool_id)
    events = []
    if kind in {"tool_call", "tool_call_update"} and isinstance(full, dict):
        meta = full.get("_meta") or {}
        raw = full.get("rawInput") or {}
        tool = meta.get("tool_name", "tool")
        if tool_id not in state.normalized_calls:
            state.normalized_calls.add(tool_id)
            events.append(nz.tool_call(call_id=tool_id, tool=tool, category={"bash": "shell", "read_file": "read", "write_file": "write", "edit": "edit"}.get(tool, "other"),
                                       input=raw, path=raw.get("filePath") if isinstance(raw, dict) else None,
                                       command=raw.get("command") if isinstance(raw, dict) else None))
        if update.get("status") in {"completed", "failed"} and tool_id not in state.normalized_results:
            state.normalized_results.add(tool_id)
            events.append(nz.tool_result(call_id=tool_id, ok=update["status"] == "completed", output=update.get("rawOutput")))
        return events
    return []


def translate(event: dict[str, Any], acc: Any) -> list[dict[str, Any]]:
    return normalize(event, acc)


def consume_granted(event: dict[str, Any], qualified: dict[str, Any] | None) -> bool:
    """Answer matching dispatch grants once; leave unmatched requests to caller admission."""
    from .vibe_commands import automatic_decision
    state = (qualified or {}).get("state")
    if not isinstance(state, State) or not state.ctx.get("vibe_commands"):
        return False
    request = decode(event, qualified)
    if not request.get("system_rejection") and automatic_decision(request, state.ctx["vibe_commands"]):
        state.outbox.append(encode(request, "approve"))
        state.responded_requests.add(request_key(event))
        return True
    # decode freezes admission; if the caller ledger will handle this same request,
    # return the original decoded record there rather than admitting it twice.
    state.cached_decoded = (request_key(event), request, copy.deepcopy(event))
    return False


def _clear_refusal(acc: Any) -> None:
    warning = acc.stream_state.pop("vibe_acp_refusal_warning", None)
    if warning in acc.notices:
        acc.notices.remove(warning)


def _record_denial(acc: Any, state: State, denial: dict[str, Any]) -> None:
    tool_id = denial.get("tool_use_id")
    for existing in acc.denials:
        if isinstance(tool_id, str) and existing.get("tool_use_id") == tool_id:
            existing.update({k: v for k, v in denial.items() if k not in existing})
            return
    acc.denials.append(denial)
    _clear_refusal(acc)
    state.text = ""
    acc.summary = None
    acc.saw_final_message = False
    raw = denial.get("tool_input") or {}
    description = raw.get("command") or raw.get("filePath") or denial.get("tool") or "an action"
    warning = f"An action was refused (`{description}`) and no assistant response followed it. A clean exit is reported completed despite this; for an editing task, check the worktree — the requested changes may not all have landed."
    acc.notices.append(warning)
    acc.stream_state["vibe_acp_refusal_warning"] = warning


def note_answer(acc: Any, row: dict[str, Any], effective: str, written: bool) -> None:
    state = state_for(acc)
    key = row.get("tool_use_id")
    if state is None or not written or not isinstance(key, str):
        return
    if effective == "approve":
        state.answers[key] = {"answer": "approved"}
        return
    reason = row.get("system_rejection") or ("approval_expired" if row.get("expiry_rejection") else None)
    answer = ({"answer": "harflow", "reason": reason} if reason else
              {"answer": "caller_rejected"} if row.get("decided_by") in {"caller", "monitor"} else {"answer": "unknown"})
    state.answers[key] = answer
    action = row.get("action") or {}
    _record_denial(acc, state, {"tool_use_id": key, "tool": action.get("tool_name"), "tool_input": copy.deepcopy(action.get("input") or {}),
                               "required_permissions": copy.deepcopy(row.get("required_permissions") or [])})


def classify_denials(denials: list[Any], acc: Any) -> list[Any]:
    from .vibe_denials import classify_one
    state = state_for(acc)
    result = []
    for denial in denials:
        if not isinstance(denial, dict):
            result.append(denial)
            continue
        answer = state.answers.get(denial.get("tool_use_id")) if state is not None else None
        category = None
        if answer is not None and answer.get("answer") == "caller_rejected":
            category = "caller_rejected"
        elif answer is not None and answer.get("answer") == "harflow":
            category = "harflow_policy_rejected"
        if category is None:
            result.append(classify_one(denial))
        else:
            result.append({**denial, "denial_category": category,
                           "denial_remedy": "The caller rejected this exact action." if category == "caller_rejected" else "Harflow refused this exact action; the policy reason identifies why.",
                           "classification_basis": {"evidence": "ledger_answer", "measured_versions": [VERSION]},
                           **({"policy_reason": answer["reason"]} if category == "harflow_policy_rejected" else {})})
    return result
