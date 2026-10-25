"""Caller approvals through `codex app-server` (stdio JSON-RPC), measured on codex-cli 0.163.0.

The adapter is a state machine over the run's own accumulator, driven through the generic approval seams:
`outgoing_frames` writes what Harflow originates, `check_approval_event` is the per-frame step (it verifies
and advances, and refuses by raising `ApprovalRefused`), `approval_frame_kind`/`decode`/`encode` carry
server requests through the approval ledger, and `translate` maps the measured notifications onto the
`codex exec` event shapes so `CodexBackend.normalize` is reused.

Lifecycle, one process per task or resume (never a second `thread/resume` in a loaded process, which
ignores new pins):

    initialize -> (userAgent version check) -> initialized + thread/start|resume -> (policy, sandbox,
    roots, thread.cliVersion verified) -> hooks/list (argv pin: none) -> turn/start -> ... -> turn/completed
    -> stdin closed.

Everything Harflow sends is built by `request_frame`, which accepts only an explicit field allowlist per
method: `turn/start` carries a thread id and the prompt and nothing else, because turn-level
`approvalPolicy`, `approvalsReviewer`, `sandboxPolicy` and `permissions` silently override the pins.
JSON-RPC ids are typed keys (`i:<n>` / `s:<text>`); Harflow's requests and Codex's server requests live in
separate tables, and a frame is a response only if it matches an outstanding Harflow id.
"""
from __future__ import annotations

import ast
import copy
import json
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import replace
from pathlib import Path
from typing import Any

from . import codex_qualification as qualification
from .base import ApprovalRefused, Invocation, UnsupportedCapability

TRANSPORT = "codex-app-server"
BINARY = "codex"
IDENTITY_MARKERS = (BINARY, "app-server")
FREEDOM_SANDBOX = {"write_in_repo": "workspace-write", "publish": "workspace-write"}
CLIENT_NAME = "harflow"

COMMAND_REQUEST = "item/commandExecution/requestApproval"
FILE_CHANGE_REQUEST = "item/fileChange/requestApproval"
NATIVE_TOOL = {COMMAND_REQUEST: "commandExecution", FILE_CHANGE_REQUEST: "fileChange"}
RESOLVED = "serverRequest/resolved"
MAX_KEY = 256
MAX_REASON = 1024

# Fields Harflow may send per method. Anything else is a programming error, never a pass-through.
ALLOWED_PARAMS: dict[str, frozenset[str]] = {
    "initialize": frozenset({"clientInfo", "capabilities"}),
    "thread/start": frozenset({"cwd", "approvalPolicy", "approvalsReviewer", "sandbox", "config", "model"}),
    "thread/resume": frozenset({"threadId", "cwd", "approvalPolicy", "approvalsReviewer", "sandbox", "config", "model"}),
    "hooks/list": frozenset({"cwds"}),
    "turn/start": frozenset({"threadId", "input"}),
}
# Never on `turn/start`, whatever a caller or a config merge asks for (measured: they replace the pins).
TURN_FORBIDDEN = frozenset({"approvalPolicy", "approvalsReviewer", "sandboxPolicy", "permissions"})

# Notifications that mean an approval was, or could be, answered by something other than the caller.
TRIPWIRE_PREFIXES = ("hook/", "item/autoApprovalReview/")
TRIPWIRE_METHODS = frozenset({"guardianWarning", "autoApprovalReview/strictReviewRequired"})

SYSTEM_REASONS = ("not_active_turn", "unsupported_kind", "missing_command", "additional_permissions", "network_context",
                  "unsupported_request", "file_change_unsupported", "grant_root", "file_change_not_started", "file_change_ambiguous", "invalid_path",
                  "request_too_large", "approval_expired", "protected_path", "path_changed_since_admission")
MAX_FROZEN_BYTES = 32 * 1024

_PLAIN_STRING = re.compile(r"""\A(?:"[^"\\\n]*"|'[^'\\\n]*')\Z""")
RULE_KEYWORDS = frozenset({"pattern", "decision", "justification", "match", "not_match"})
RULE_DECISIONS = frozenset({"prompt", "forbidden"})
MAX_RULE_BYTES = 256 * 1024
MAX_RULE_FILES = 64


# --- typed JSON-RPC ids ---------------------------------------------------------------------------------

def typed_key(value: Any) -> str | None:
    """`i:<n>` for an integer id, `s:<text>` for a string id; None for anything else (bool, float, null)."""
    if type(value) is int:
        return f"i:{value}"
    if isinstance(value, str) and 0 < len(value) <= MAX_KEY:
        return "s:" + value
    return None


def untyped(key: str) -> int | str:
    """The wire id a typed key stands for, with its original JSON type."""
    kind, _, text = key.partition(":")
    if kind == "i":
        return int(text)
    if kind == "s":
        return text
    raise ValueError("Invalid typed request key")


# --- context, pins and argv -------------------------------------------------------------------------------

def _toml(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml(item) for item in value) + "]"
    raise ValueError(f"unsupported pin value: {value!r}")


def _pins(ctx: dict[str, Any]) -> list[tuple[str, Any]]:
    from .codex import EFFORT_KEY
    pins = [("approval_policy", "on-request"), ("approvals_reviewer", "user"), ("sandbox_mode", FREEDOM_SANDBOX[ctx["freedom"]]),
            ("sandbox_workspace_write.writable_roots", []), ("sandbox_workspace_write.network_access", ctx["network"]),
            ("features.hooks", False), ("features.request_permissions_tool", False), ("features.exec_permission_approvals", False),
            ("shell_environment_policy.inherit", "core")]
    if ctx.get("effort"):
        pins.append((EFFORT_KEY, ctx["effort"]))
    return pins


def argv(ctx: dict[str, Any]) -> list[str]:
    """The one exact app-server argv: the full pin set on `-c`, then the listen transport."""
    result = [BINARY, "app-server"]
    for key, value in _pins(ctx):
        result += ["-c", f"{key}={_toml(value)}"]
    return [*result, "--listen", "stdio://"]


def trust_directories(cwd: str, git_root: str, linked: Any = ()) -> list[str]:
    """Every directory from the canonical cwd up to the filesystem root, plus the Git root, each of which gets an
    explicit untrusted override: whichever of them Codex keys project trust on, a trusted entry for it cannot apply."""
    found: list[str] = []
    for start in (cwd, *linked):
        found.append(start)
        parent = os.path.dirname(start)
        while parent != found[-1]:
            found.append(parent)
            parent = os.path.dirname(parent)
    return list(dict.fromkeys([*found, git_root]))


def config_map(ctx: dict[str, Any]) -> dict[str, Any]:
    """The `thread/start` `config` map: the same pins, nested, plus the untrusted-project overrides (R6-2).

    The overrides ignore project config, rules and hooks and, unlike the argv form, write nothing to CODEX_HOME."""
    nested: dict[str, Any] = {}
    for key, value in _pins(ctx):
        node = nested
        *parents, leaf = key.split(".")
        for part in parents:
            node = node.setdefault(part, {})
        node[leaf] = value
    nested["projects"] = {directory: {"trust_level": "untrusted"} for directory in trust_directories(ctx["cwd"], ctx["git_root"], ctx.get("linked_roots", ()))}
    return nested


def validate_context(ctx: Any) -> dict[str, Any]:
    if not isinstance(ctx, dict) or ctx.get("transport") != TRANSPORT:
        raise ValueError("Invalid app-server control context")
    expected = {"version": str, "cwd": str, "git_root": str, "freedom": str, "network": bool, "prompt": str}
    for key, kind in expected.items():
        if type(ctx.get(key)) is not kind:
            raise ValueError(f"Invalid app-server control context field {key}")
    if ctx["freedom"] not in FREEDOM_SANDBOX or not ctx["prompt"].strip():
        raise ValueError("Invalid app-server control context")
    for key in ("model", "effort", "thread_id"):
        if ctx.get(key) is not None and (not isinstance(ctx[key], str) or not ctx[key]):
            raise ValueError(f"Invalid app-server control context field {key}")
    linked = ctx.get("linked_roots", [])
    if not isinstance(linked, list) or not all(isinstance(r, str) and os.path.isabs(r) for r in linked):
        raise ValueError("Invalid app-server control context field linked_roots")
    if type(ctx.get("write", False)) is not bool:
        raise ValueError("Invalid app-server control context field write")
    if set(ctx) - {"transport", "model", "effort", "thread_id", "write", "linked_roots", *expected}:
        raise ValueError("Unknown app-server control context field")
    return ctx


def assert_safe(invocation: Invocation, freedom: str) -> str | None:
    """Why `invocation` is not the one exact app-server shape, or None."""
    try:
        ctx = validate_context(invocation.control_context)
    except ValueError as exc:
        return str(exc)
    if invocation.argv != argv(ctx):
        return "app-server argv is not the exact qualified shape"
    if (not invocation.control_pipe or invocation.stdin_mode != "devnull" or invocation.initial_input is not None or not invocation.caller_approvals
            or invocation.caller_write_approvals is not ctx.get("write", False) or invocation.native_subagent or invocation.allowed_commands or invocation.extra_writable_paths
            or invocation.caller_approval_version != ctx["version"] or invocation.identity_markers != IDENTITY_MARKERS):
        return "app-server invocation carries an unexpected control field"
    if ctx["freedom"] != freedom:
        return "app-server freedom does not match the requested freedom"
    return None


# --- outgoing frames --------------------------------------------------------------------------------------

def _line(obj: dict[str, Any]) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")


def request_frame(state: "State", method: str, params: dict[str, Any]) -> bytes:
    allowed = ALLOWED_PARAMS.get(method)
    extra = set(params) - allowed if allowed is not None else {"*"}
    if allowed is None or extra or (method == "turn/start" and set(params) & TURN_FORBIDDEN):
        raise ValueError(f"app-server request {method} carries fields outside its allowlist: {sorted(extra)}")
    number = state.next_id
    state.next_id += 1
    state.pending[f"i:{number}"] = method
    return _line({"id": number, "method": method, "params": params})


def notification_frame(method: str) -> bytes:
    if method != "initialized":
        raise ValueError("only the initialized notification is ever sent")
    return _line({"method": method})


class State:
    """Per-run protocol state, held in `Accumulator.stream_state` (never on the shared backend instance)."""

    def __init__(self, ctx: dict[str, Any]) -> None:
        self.ctx = validate_context(ctx)
        self.started = False
        self.next_id = 1
        self.pending: dict[str, str] = {}
        self.outbox: list[bytes] = []
        self.observed_version: str | None = None
        self.thread_id: str | None = None
        self.turn_id: str | None = None
        self.thread_verified = False
        self.verified = False
        self.terminal: dict[str, Any] | None = None
        self.failed: str | None = None
        self.seen_items: set[str] = set()
        self.duplicates: set[str] = set()
        # fileChange items seen in `item/started` for the active thread and turn (itemId -> normalized changes),
        # the ones whose started item was ambiguous, the ones frozen at admission, and the ones whose completion
        # contradicted what was approved.
        self.file_started: dict[str, list[str]] = {}
        self.file_ambiguous: set[str] = set()
        self.file_frozen: dict[str, list[str]] = {}
        self.file_mismatched: set[str] = set()
        # What Harflow answered per ledger identity, for denial classification.
        self.answers: dict[str, dict[str, Any]] = {}
        self.mcp_errors: list[dict[str, Any]] = []
        self.mcp_calls: dict[str, str] = {}   # mcpToolCall item id -> canonical (server, tool, arguments), started or completed in the active turn

    @property
    def admissible(self) -> bool:
        """Requests are admitted only between turn/start and the terminal turn, and only while nothing has refused."""
        return self.verified and self.turn_id is not None and self.terminal is None and self.failed is None


def state_for(acc: Any) -> State | None:
    state = acc.stream_state.get("app_server")
    if state is None and isinstance(acc.stream_state.get("control_context"), dict):
        state = acc.stream_state["app_server"] = State(acc.stream_state["control_context"])
    return state


def initial_frames(acc: Any) -> list[bytes]:
    state = state_for(acc)
    if state is None or state.started:
        return []
    state.started = True
    return [request_frame(state, "initialize", {"clientInfo": {"name": CLIENT_NAME, "title": "Harflow", "version": "0"}, "capabilities": {"experimentalApi": True}})]


def turn_frames(acc: Any) -> list[bytes]:
    state = state_for(acc)
    if state is None or not state.outbox:
        return []
    frames, state.outbox = state.outbox, []
    return frames


def finished(acc: Any) -> bool:
    state = state_for(acc)
    return state is not None and (state.terminal is not None or state.failed is not None)


# --- the per-frame step -----------------------------------------------------------------------------------

def _expect(condition: bool, what: str, observed: Any = None, wanted: Any = None) -> None:
    if not condition:
        detail = f" (reported {observed!r}, expected {wanted!r})" if wanted is not None or observed is not None else ""
        raise ApprovalRefused(f"Codex app-server verification failed: {what}{detail}; stopping execution without approval")


def _canonical(path: Any) -> str | None:
    return os.path.realpath(path) if isinstance(path, str) and os.path.isabs(path) else None


def _verify_thread(state: State, result: dict[str, Any], resumed: bool) -> None:
    ctx = state.ctx
    thread = result.get("thread")
    _expect(isinstance(thread, dict) and isinstance(thread.get("id"), str) and bool(thread["id"]), "thread id missing")
    _expect(thread.get("cliVersion") == ctx["version"], "thread.cliVersion", thread.get("cliVersion"), ctx["version"])
    if resumed:
        _expect(thread["id"] == ctx["thread_id"], "resumed thread id", thread["id"], ctx["thread_id"])
    _expect(result.get("approvalPolicy") == "on-request", "approvalPolicy", result.get("approvalPolicy"), "on-request")
    _expect(result.get("approvalsReviewer") == "user", "approvalsReviewer", result.get("approvalsReviewer"), "user")
    sandbox = result.get("sandbox")
    _expect(isinstance(sandbox, dict) and sandbox.get("type") == "workspaceWrite", "sandbox type", (sandbox or {}).get("type") if isinstance(sandbox, dict) else sandbox, "workspaceWrite")
    _expect(sandbox.get("writableRoots") == [], "sandbox.writableRoots", sandbox.get("writableRoots"), [])
    _expect(sandbox.get("networkAccess") is ctx["network"], "sandbox.networkAccess", sandbox.get("networkAccess"), ctx["network"])
    roots = result.get("runtimeWorkspaceRoots")
    _expect(isinstance(roots, list) and [_canonical(root) for root in roots] == [ctx["cwd"]], "runtimeWorkspaceRoots", roots, [ctx["cwd"]])
    _expect(_canonical(result.get("cwd")) == ctx["cwd"], "cwd", result.get("cwd"), ctx["cwd"])


def _thread_params(state: State) -> dict[str, Any]:
    ctx = state.ctx
    params: dict[str, Any] = {"cwd": ctx["cwd"], "approvalPolicy": "on-request", "approvalsReviewer": "user",
                              "sandbox": FREEDOM_SANDBOX[ctx["freedom"]], "config": config_map(ctx)}
    if ctx.get("model"):
        params["model"] = ctx["model"]
    if ctx.get("thread_id"):
        params["threadId"] = ctx["thread_id"]
    return params


def _on_initialize(state: State, result: dict[str, Any]) -> None:
    observed = qualification.parse_user_agent(result.get("userAgent"))
    state.observed_version = observed
    if observed != state.ctx["version"]:
        raise ApprovalRefused(f"Codex app-server reported version {observed or 'none'} but caller approvals were qualified on {state.ctx['version']}; stopping execution without approval")
    state.outbox.append(notification_frame("initialized"))
    method = "thread/resume" if state.ctx.get("thread_id") else "thread/start"
    state.outbox.append(request_frame(state, method, _thread_params(state)))


def _on_thread(state: State, result: dict[str, Any], resumed: bool) -> None:
    _verify_thread(state, result, resumed)
    state.thread_id = result["thread"]["id"]
    state.thread_verified = True
    state.outbox.append(request_frame(state, "hooks/list", {"cwds": [state.ctx["cwd"]]}))


def _on_hooks(state: State, result: dict[str, Any]) -> None:
    """`hooks/list` must cover the canonical cwd, be schema-complete, and report no hooks and no errors."""
    data = result.get("data")
    _expect(isinstance(data, list) and bool(data), "hooks/list returned no entries")
    covered = False
    for entry in data:
        _expect(isinstance(entry, dict) and isinstance(entry.get("cwd"), str) and all(isinstance(entry.get(k), list) for k in ("hooks", "errors", "warnings")), "hooks/list entry is malformed")
        _expect(entry["hooks"] == [], "hooks/list reported hooks under the hooks pin", len(entry["hooks"]), 0)
        _expect(entry["errors"] == [], "hooks/list reported errors", len(entry["errors"]), 0)
        covered = covered or _canonical(entry["cwd"]) == state.ctx["cwd"]
    _expect(covered, "hooks/list did not cover the task's working directory", wanted=state.ctx["cwd"])
    state.verified = True
    state.outbox.append(request_frame(state, "turn/start", {"threadId": state.thread_id, "input": [{"type": "text", "text": state.ctx["prompt"]}]}))


def _on_turn_start(state: State, result: dict[str, Any]) -> None:
    turn = result.get("turn")
    _expect(isinstance(turn, dict) and isinstance(turn.get("id"), str) and bool(turn["id"]), "turn id missing")
    state.turn_id = turn["id"]


def _error_text(error: Any) -> str:
    if isinstance(error, dict):
        return f"code {error.get('code')}: {str(error.get('message'))[:300]}"
    return str(error)[:300]


def step(state: State, event: dict[str, Any]) -> None:
    """Verify and advance on one non-request frame. Raises `ApprovalRefused`; once refused, nothing advances."""
    if state.failed is not None:
        return
    try:
        _step(state, event)
    except ApprovalRefused as exc:
        state.failed = str(exc)
        raise


def _step(state: State, event: dict[str, Any]) -> None:
    method = event.get("method")
    if method is None and "id" in event and ("result" in event or "error" in event):
        key = typed_key(event["id"])
        name = state.pending.pop(key, None) if key is not None else None
        if name is None:
            return  # not an answer to anything Harflow asked
        if "error" in event:
            raise ApprovalRefused(f"Codex app-server returned an error for {name}: {_error_text(event['error'])}; stopping execution without approval")
        result = event.get("result")
        _expect(isinstance(result, dict), f"{name} result is malformed")
        if name == "initialize":
            _on_initialize(state, result)
        elif name in {"thread/start", "thread/resume"}:
            _on_thread(state, result, name == "thread/resume")
        elif name == "hooks/list":
            _on_hooks(state, result)
        elif name == "turn/start":
            _on_turn_start(state, result)
        return
    if not isinstance(method, str):
        return
    if method.startswith(TRIPWIRE_PREFIXES) or method in TRIPWIRE_METHODS:
        raise ApprovalRefused(f"Codex reported {method}: an approval could be answered by something other than the caller; stopping execution without approval", audit=f"tripwire {method}"[:120])
    params = event.get("params") if isinstance(event.get("params"), dict) else {}
    if (method.startswith("item/") or method.startswith("turn/")) and not state.verified:
        raise ApprovalRefused(f"Codex sent {method} before the version, thread and policy checks passed; stopping execution without approval")
    if method in {"item/started", "item/completed"}:
        _track_file_change(state, method, params)
    if method == "turn/completed":
        turn = params.get("turn") if isinstance(params.get("turn"), dict) else {}
        if params.get("threadId") == state.thread_id and turn.get("id") == state.turn_id and state.terminal is None:
            state.terminal = {"status": turn.get("status"), "error": turn.get("error")}


# --- file changes ---------------------------------------------------------------------------------------

def _norm_changes(changes: Any) -> list[str] | None:
    """Order-insensitive canonical form of an item's complete change objects (any extra field counts as a difference),
    or None when malformed."""
    if not isinstance(changes, list) or not all(isinstance(change, dict) for change in changes):
        return None
    return sorted(json.dumps(change, sort_keys=True, ensure_ascii=False) for change in changes)


def _track_file_change(state: State, method: str, params: dict[str, Any]) -> None:
    """Remember fileChange items of the active thread and turn as they start, and compare a completion with what was approved."""
    item = params.get("item")
    if not isinstance(item, dict) or item.get("type") != "fileChange" or params.get("threadId") != state.thread_id or params.get("turnId") != state.turn_id:
        return
    item_id = item.get("id")
    if not isinstance(item_id, str) or not item_id:
        return
    if method == "item/started":
        if item_id in state.file_started:
            state.file_ambiguous.add(item_id)
        elif _norm_changes(item.get("changes")) is None:
            state.file_ambiguous.add(item_id)
        else:
            state.file_started[item_id] = copy.deepcopy(item["changes"])
        return
    frozen = state.file_frozen.get(item_id)
    if frozen is not None and _norm_changes(item.get("changes")) != frozen:
        state.file_mismatched.add(item_id)
        raise ApprovalRefused(f"Codex completed file change {item_id} with changes that differ from the ones approved; stopping execution",
                              audit="file change mismatch", outcomes=((f"fileChange:{item_id}", "mismatch"),))


def change_paths(action: dict[str, Any]) -> list[str]:
    """Every source and destination path of a frozen fileChange action, in first-seen order."""
    found: list[str] = []
    changes = (action.get("input") or {}).get("changes") if action.get("tool_name") == "fileChange" else None
    for change in changes if isinstance(changes, list) else []:
        kind = change.get("kind") if isinstance(change, dict) else None
        for path in (change.get("path") if isinstance(change, dict) else None, kind.get("move_path") if isinstance(kind, dict) else None):
            if isinstance(path, str) and path not in found:
                found.append(path)
    return found


def _decode_file_change(state: State, params: dict[str, Any], base: dict[str, Any], digest_input: dict[str, Any], key: str) -> dict[str, Any]:
    item_id = params.get("itemId")
    valid = isinstance(item_id, str) and 0 < len(item_id) <= MAX_KEY
    identity = f"fileChange:{item_id}" if valid else f"{FILE_CHANGE_REQUEST}:{key}"
    decoded = {**base, "tool_use_id": identity[:MAX_KEY], "action": {"tool_name": "fileChange", "input": digest_input}}
    if not state.ctx.get("write", False):
        return _rejection(decoded, "file_change_unsupported")
    if not valid:
        raise ValueError("Missing file change item identity")
    if params.get("threadId") != state.thread_id or params.get("turnId") != state.turn_id:
        return _rejection(decoded, "not_active_turn")
    if params.get("grantRoot") is not None:
        return _rejection(decoded, "grant_root")
    changes = state.file_started.get(item_id)
    if item_id in state.file_ambiguous:
        return _rejection(decoded, "file_change_ambiguous")
    if changes is None:
        return _rejection(decoded, "file_change_not_started")
    frozen_bytes = json.dumps(changes, sort_keys=True, ensure_ascii=False).encode("utf-8")
    if len(frozen_bytes) > MAX_FROZEN_BYTES:
        import hashlib
        decoded["action"] = {"tool_name": "fileChange", "input": {"itemId": item_id, "change_count": len(changes), "changes_sha256": hashlib.sha256(frozen_bytes).hexdigest()}}
        return _rejection(decoded, "request_too_large")
    decoded["action"] = {"tool_name": "fileChange", "input": {**digest_input, "changes": copy.deepcopy(changes)}}
    if not all(isinstance(p, str) and os.path.isabs(p) and "\x00" not in p for p in change_paths(decoded["action"])) or not change_paths(decoded["action"]):
        return _rejection(decoded, "invalid_path")
    state.file_frozen[item_id] = _norm_changes(changes) or []
    return decoded


# --- server requests --------------------------------------------------------------------------------------

def frame_kind(event: dict[str, Any]) -> str | None:
    method = event.get("method")
    if not isinstance(method, str):
        return None
    if method == RESOLVED:
        return "resolved"
    return "request" if "id" in event else None


def request_key(event: dict[str, Any]) -> str | None:
    if event.get("method") == RESOLVED:
        params = event.get("params")
        return typed_key(params.get("requestId")) if isinstance(params, dict) else None
    return typed_key(event.get("id"))


def _rejection(decoded: dict[str, Any], reason: str) -> dict[str, Any]:
    decoded["system_rejection"] = reason
    return decoded


def decode(event: dict[str, Any], qualified: dict[str, Any] | None) -> dict[str, Any]:
    """Admission for one server request. Structural problems raise; request-level problems become Harflow
    rejections (declined, never shown to the caller as a decision); a missing `accept`/`cancel` pair or a request
    before verification stops the run."""
    state = (qualified or {}).get("state")
    if not isinstance(state, State):
        raise ApprovalRefused("Codex app-server state is unavailable; stopping execution without approval")
    if state.failed is not None or not state.admissible:
        when = "after the turn ended" if state.terminal is not None else "before the version, thread and policy checks passed"
        raise ApprovalRefused(f"Codex sent a request {when}; stopping execution without approval")
    method, params, key = event.get("method"), event.get("params"), request_key(event)
    if not isinstance(method, str) or key is None or not isinstance(params, dict):
        raise ValueError("Uncorrelated app-server request")
    digest_input = {k: copy.deepcopy(v) for k, v in params.items() if k != "startedAtMs"}
    item_id = params.get("itemId")
    base = {"native_request_id": key, "native_method": method}
    if method == FILE_CHANGE_REQUEST:
        return _decode_file_change(state, params, base, digest_input, key)
    if method != COMMAND_REQUEST:
        label = NATIVE_TOOL.get(method, method)
        identity = f"{label}:{item_id}" if isinstance(item_id, str) and item_id and len(item_id) <= MAX_KEY and method == FILE_CHANGE_REQUEST else f"{method}:{key}"
        decoded = {**base, "tool_use_id": identity[:MAX_KEY], "action": {"tool_name": label, "input": digest_input}}
        return _rejection(decoded, "file_change_unsupported" if method == FILE_CHANGE_REQUEST else "unsupported_request")
    if not isinstance(item_id, str) or not item_id or len(item_id) > MAX_KEY:
        raise ValueError("Missing command item identity")
    if item_id in state.seen_items:
        state.duplicates.add(item_id)
    state.seen_items.add(item_id)
    decisions = params.get("availableDecisions")
    if not (isinstance(decisions, list) and "accept" in decisions and "cancel" in decisions):
        raise ApprovalRefused("Codex offered no accept and cancel decisions for a command request; stopping execution without approval")
    reason = params.get("reason")
    decoded = {**base, "tool_use_id": item_id, "action": {"tool_name": "commandExecution", "input": digest_input},
               "decision_reason": reason.encode("utf-8")[:MAX_REASON].decode("utf-8", "ignore") if isinstance(reason, str) else None,
               "decision_reason_type": params.get("kind") if isinstance(params.get("kind"), str) else None,
               "may_run_outside_sandbox": True}
    command = params.get("command")
    if params.get("threadId") != state.thread_id or params.get("turnId") != state.turn_id:
        return _rejection(decoded, "not_active_turn")
    if params.get("kind") != "command":
        return _rejection(decoded, "unsupported_kind")
    if not isinstance(command, str) or not command.strip():
        return _rejection(decoded, "missing_command")
    if params.get("additionalPermissions") is not None:
        return _rejection(decoded, "additional_permissions")
    if params.get("networkApprovalContext") is not None or params.get("proposedNetworkPolicyAmendments") is not None:
        return _rejection(decoded, "network_context")
    return decoded


def decode_oversize(event: dict[str, Any], qualified: dict[str, Any] | None) -> dict[str, Any] | None:
    """A bounded rejection-only audit row for an over-budget request whose identity is still valid."""
    import hashlib
    state = (qualified or {}).get("state")
    key, params = request_key(event), event.get("params")
    if not isinstance(state, State) or not state.admissible or key is None or not isinstance(params, dict) or not isinstance(event.get("method"), str):
        return None
    item_id = params.get("itemId")
    raw = json.dumps(event, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    state.seen_items.add(item_id) if isinstance(item_id, str) else None
    return {"native_request_id": key, "native_method": event["method"], "tool_use_id": item_id if isinstance(item_id, str) and 0 < len(item_id) <= MAX_KEY else f"{event['method']}:{key}"[:MAX_KEY],
            "action": {"tool_name": NATIVE_TOOL.get(event["method"], "request"), "input": {}}, "system_rejection": "request_too_large",
            "oversize_bytes": len(raw), "oversize_action_digest": hashlib.sha256(raw).hexdigest()}


def encode(request: dict[str, Any], decision: str) -> bytes:
    if decision not in {"approve", "reject"}:
        raise ValueError("decision must be approve or reject")
    system = request.get("system_rejection") or ("approval_expired" if request.get("expiry_rejection") else None)
    if system is not None and (decision != "reject" or system not in SYSTEM_REASONS):
        raise ValueError("A Harflow system rejection can only be delivered as a rejection")
    wire_id = untyped(request["native_request_id"])
    method = request.get("native_method")
    if method == COMMAND_REQUEST:
        return _line({"id": wire_id, "result": {"decision": "accept" if decision == "approve" else "decline"}})
    if method == FILE_CHANGE_REQUEST:
        return _line({"id": wire_id, "result": {"decision": "accept" if decision == "approve" else "decline"}})
    # Permissions, elicitation, user-input and every unmeasured method: a JSON-RPC error grants nothing (measured).
    return _line({"id": wire_id, "error": {"code": -32000, "message": f"Harflow refused this request: {system or 'unsupported_request'}"}})


def outcomes(event: dict[str, Any], state: State | None) -> list[tuple[str, str]]:
    """`(itemId, outcome)` from an `item/completed` command item of the active thread and turn, for a unique admitted item."""
    # After any refusal (a tripwire above all) nothing completed on the stream is attributed to an approval.
    if state is None or state.failed is not None or event.get("method") != "item/completed" or not isinstance(event.get("params"), dict):
        return []
    params = event["params"]
    item = params.get("item")
    if isinstance(item, dict) and item.get("type") == "fileChange" and params.get("threadId") == state.thread_id and params.get("turnId") == state.turn_id:
        file_id = item.get("id")
        if isinstance(file_id, str) and file_id in state.file_frozen and file_id not in state.file_mismatched:
            return [(f"fileChange:{file_id}", "succeeded" if item.get("status") == "completed" else "failed")]
        return []
    if not isinstance(item, dict) or item.get("type") != "commandExecution" or params.get("threadId") != state.thread_id or params.get("turnId") != state.turn_id:
        return []
    item_id = item.get("id")
    if not isinstance(item_id, str) or item_id not in state.seen_items or item_id in state.duplicates:
        return []
    exit_code = item.get("exitCode")
    clean = item.get("status") == "completed" and type(exit_code) is int and exit_code == 0
    return [(item_id, "succeeded" if clean else "failed")]


# --- translation to exec-shaped events --------------------------------------------------------------------

def is_frame(event: Any) -> bool:
    return isinstance(event, dict) and "type" not in event and (isinstance(event.get("method"), str) or ("id" in event and ("result" in event or "error" in event)))


def _exec_item(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    kind, item_id = item.get("type"), item.get("id")
    status = {"declined": "failed", "inProgress": "in_progress"}.get(item.get("status"), item.get("status"))
    if kind == "commandExecution":
        return {"type": "command_execution", "id": item_id, "command": item.get("command"), "status": status,
                "exit_code": item.get("exitCode"), "aggregated_output": item.get("aggregatedOutput")}
    if kind == "agentMessage":
        return {"type": "agent_message", "id": item_id, "text": item.get("text")}
    if kind == "fileChange":
        return {"type": "file_change", "id": item_id, "changes": item.get("changes"), "status": status}
    if kind == "mcpToolCall":
        return {"type": "mcp_tool_call", "id": item_id, "server": item.get("server"), "tool": item.get("tool"), "arguments": item.get("arguments"),
                "status": status, "result": item.get("result"), "error": item.get("error")}
    return None


def item_correlated(event: dict[str, Any], state: State) -> bool:
    """Item frames count only for the active thread and turn; historical or foreign items never feed the summary,
    the final message or the event log. Other frames (responses, errors, turn terminals) have their own checks."""
    if event.get("method") not in {"item/started", "item/completed"}:
        return True
    params = event.get("params")
    return isinstance(params, dict) and params.get("threadId") == state.thread_id and params.get("turnId") == state.turn_id and state.turn_id is not None


def translate(event: dict[str, Any]) -> list[dict[str, Any]]:
    """Measured app-server frames as the `codex exec` events `CodexBackend` already ingests and normalizes."""
    method, params = event.get("method"), event.get("params")
    params = params if isinstance(params, dict) else {}
    if method is None:
        result = event.get("result")
        thread = result.get("thread") if isinstance(result, dict) else None
        if isinstance(thread, dict) and isinstance(thread.get("id"), str) and thread.get("cliVersion") is not None:
            return [{"type": "thread.started", "thread_id": thread["id"]}]
        return []
    if method in {"item/started", "item/completed"}:
        item = _exec_item(params.get("item"))
        return [{"type": "item.started" if method == "item/started" else "item.completed", "item": item}] if item is not None else []
    if method == "turn/completed":
        turn = params.get("turn") if isinstance(params.get("turn"), dict) else {}
        status = turn.get("status")
        if status == "completed":
            return [{"type": "turn.completed", "usage": {}}]
        error = turn.get("error")
        message = error.get("message") if isinstance(error, dict) else None
        return [{"type": "turn.failed", "message": message if isinstance(message, str) and message else f"Codex turn ended with status {status}"}]
    if method == "error":
        error = params.get("error") if isinstance(params.get("error"), dict) else {}
        message = error.get("message") if isinstance(error.get("message"), str) else None
        if message is None:
            return []
        if params.get("willRetry") is True:
            return [{"type": "item.completed", "item": {"type": "error", "message": message}}]
        return [{"type": "error", "message": message}]
    return []


# --- dispatch-time preparation ------------------------------------------------------------------------------

def git_root(directory: Path) -> str:
    """The Git root Codex keys project trust on, resolved with repository-controlled configuration neutralised.

    Repository config (`core.worktree`), replace refs and `GIT_*` environment could make `git` report a different
    root than the filesystem shows, so they are switched off, and the answer must be a real ancestor-or-self of the
    canonical directory that holds `.git` and is the nearest such ancestor; anything else refuses."""
    canonical = os.path.realpath(directory)
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null", GIT_OPTIONAL_LOCKS="0")
    try:
        # Git still honours `core.worktree` from the repository's own config, so the ancestor check below, not a flag,
        # is the protection against a redirected root.
        result = subprocess.run(["git", "--no-replace-objects", "-c", "core.fsmonitor=false", "-C", canonical, "rev-parse", "--show-toplevel"],
                                capture_output=True, text=True, timeout=5, env=env, check=False)
    except (OSError, subprocess.SubprocessError, UnicodeError) as exc:
        raise UnsupportedCapability("Codex caller approvals need a Git repository, and its root could not be resolved") from exc
    reported = result.stdout.strip()
    if result.returncode != 0 or not reported:
        raise UnsupportedCapability("Codex caller approvals need a Git repository: the project trust override is keyed by the Git root, and none was found")
    root = os.path.realpath(reported)
    nearest = None
    probe = Path(canonical)
    for candidate in (probe, *probe.parents):
        if (candidate / ".git").exists():
            nearest = str(candidate)
            break
    if nearest is None or root != nearest or not _inside(Path(canonical), Path(root)):
        raise UnsupportedCapability("Codex caller approvals refuse a repository whose Git root cannot be confirmed from the filesystem (a redirected worktree, or a root that is not the nearest ancestor holding .git)")
    return root


def linked_roots(root: str) -> list[str]:
    """Extra roots Codex may key project trust on when `root` is a linked worktree (measured: a trusted main checkout
    applies to its linked worktrees unless the worktree itself has an explicit entry).

    `<root>/.git` is then a file naming `<common>/worktrees/<name>`; that line is repository content, so it is
    verified against Git's own back-link and layout, and anything else (a submodule's `modules/` gitdir, a gitdir
    outside a `worktrees` directory, a missing or mismatching back-link) refuses. A normal checkout has none."""
    git = Path(root) / ".git"
    if git.is_dir():
        return []
    bad = UnsupportedCapability("Codex caller approvals refuse a linked worktree or submodule whose .git file does not point into a verifiable <common>/worktrees/<name> layout")
    try:
        text = git.read_text(encoding="utf-8") if git.is_file() and git.stat().st_size < 4096 else ""
        found = re.fullmatch(r"gitdir: (.+)\n?", text)
        if found is None:
            raise bad
        gitdir = Path(os.path.realpath(found.group(1) if os.path.isabs(found.group(1)) else os.path.join(root, found.group(1))))
        common_line = (gitdir / "commondir").read_text(encoding="utf-8").strip()
        common = Path(os.path.realpath(common_line if os.path.isabs(common_line) else os.path.join(gitdir, common_line)))
        back = Path(os.path.realpath((gitdir / "gitdir").read_text(encoding="utf-8").strip()))
    except (OSError, UnicodeError) as exc:
        raise bad from exc
    if gitdir.parent.name != "worktrees" or gitdir.parent.parent != common or back != Path(os.path.realpath(git)):
        raise bad
    roots = [str(common.parent)] if common.name == ".git" else [str(common)]
    return list(dict.fromkeys([*roots, str(common)]))


def codex_home(environ: Any = None) -> Path:
    env = os.environ if environ is None else environ
    return Path(os.path.realpath(env.get("CODEX_HOME") or Path(env.get("HOME") or Path.home()) / ".codex"))


def _inside(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def rules_problem(source: str) -> str | None:
    """Why a rules file is outside the accepted grammar, or None.

    Accepted: comments, blank lines and calls of `prefix_rule(...)` whose `pattern` is a list of plain string
    literals (or lists of them) and whose `decision` is a plain literal `"prompt"` or `"forbidden"`; optional
    `justification` (a plain string) and `match`/`not_match` (lists of the same pattern shape). Everything else
    refuses: a missing decision (the default is allow, measured), concatenation, escapes, variables, other calls,
    other statements. Codex's default decision for `prefix_rule` is `allow`, and `"al" + "low"` and `"\\x61llow"`
    both evaluate to it, so a scan for the word cannot be the test."""
    try:
        tree = ast.parse(source, mode="exec")
    except (SyntaxError, ValueError, RecursionError):
        return "is not parseable"

    def plain(node: ast.AST) -> bool:
        return isinstance(node, ast.Constant) and isinstance(node.value, str) and _PLAIN_STRING.match(ast.get_source_segment(source, node) or "") is not None

    def tokens(node: ast.AST) -> bool:
        return plain(node) or (isinstance(node, ast.List) and bool(node.elts) and all(plain(e) for e in node.elts))

    def token_list(node: ast.AST) -> bool:
        return isinstance(node, ast.List) and bool(node.elts) and all(tokens(e) for e in node.elts)

    for statement in tree.body:
        call = statement.value if isinstance(statement, ast.Expr) else None
        if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == "prefix_rule" and not call.args):
            return "contains something other than prefix_rule(...) calls"
        keywords = {kw.arg: kw.value for kw in call.keywords}
        if None in keywords or len(keywords) != len(call.keywords) or set(keywords) - RULE_KEYWORDS:
            return "uses an unexpected prefix_rule argument"
        decision = keywords.get("decision")
        if "pattern" not in keywords or not token_list(keywords["pattern"]):
            return "has a prefix_rule pattern that is not a list of plain string literals"
        if decision is None or not plain(decision) or decision.value not in RULE_DECISIONS:
            return 'has a prefix_rule without an explicit literal decision of "prompt" or "forbidden"'
        if "justification" in keywords and not plain(keywords["justification"]):
            return "has a non-literal justification"
        for key in ("match", "not_match"):
            if key in keywords and not token_list(keywords[key]):
                return f"has a {key} that is not a list of plain string literals"
    return None


def check_rules_dir(rules: Path, label: str) -> None:
    """Every rules file Codex loads from `rules` must conform to the restricted grammar; symlinks refuse.

    Measured on 0.163.0: regular files directly in `rules/` named `*.rules` (hidden ones too) load; other
    extensions, subdirectories, an upper-case extension and symlinked files or directories do not."""
    try:
        if not rules.exists() and not rules.is_symlink():
            return
        if rules.is_symlink():
            raise UnsupportedCapability(f"Codex caller approvals refuse a symlinked {label} rules directory")
        entries = sorted(rules.iterdir())
        for path in entries:
            if path.is_symlink():
                raise UnsupportedCapability(f"Codex caller approvals refuse a symlink in {label} rules ({path.name})")
        for path in (p for p in rules.rglob("*") if p.is_symlink()):
            raise UnsupportedCapability(f"Codex caller approvals refuse a symlink in {label} rules ({path.name})")
        candidates = [p for p in entries if p.is_file() and p.name.lower().endswith(".rules")]
        if len(candidates) > MAX_RULE_FILES:
            raise UnsupportedCapability(f"Codex caller approvals refuse a {label} rules directory with too many rule files")
        for path in candidates:
            data = path.read_bytes()
            if len(data) > MAX_RULE_BYTES:
                raise UnsupportedCapability(f"Codex caller approvals refuse an oversized rules file ({path.name})")
            problem = rules_problem(data.decode("utf-8"))
            if problem is not None:
                raise UnsupportedCapability(f"Codex caller approvals refuse a {label} rules file that does not conform ({path.name} {problem}): only prefix_rule calls with a literal prompt or forbidden decision are accepted, because allow rules run commands without approval and outside the sandbox")
    except UnsupportedCapability:
        raise
    except (OSError, UnicodeError) as exc:
        raise UnsupportedCapability(f"Codex caller approvals refuse an unreadable or unparseable {label} rules directory") from exc


def check_rules(home: Path) -> None:
    check_rules_dir(home / "rules", "CODEX_HOME")


def project_directories(cwd: str, root: str) -> list[Path]:
    """Directories from the Git root down to the working directory, inclusive."""
    chain = [Path(cwd)]
    while str(chain[-1]) != root and chain[-1] != chain[-1].parent:
        chain.append(chain[-1].parent)
    return list(reversed(chain))


def check_project_rules(cwd: str, root: str, linked: Any = ()) -> None:
    """Belt and braces: the untrusted override ignores project rules, but any `.codex/rules` from the Git root down to
    the working directory must conform as well, so a failed override can never turn into a silent allow."""
    for directory in [*project_directories(cwd, root), *(Path(r) for r in linked)]:
        check_rules_dir(directory / ".codex" / "rules", "project")


def check_home(home: Path, cwd: str, root: str, environ: Any = None) -> None:
    """Refusals that pins cannot clear, evaluated before every spawn by reading files only (R6-5)."""
    env = os.environ if environ is None else environ
    roots = {Path(cwd), Path(root), Path(os.path.realpath(env.get("TMPDIR") or tempfile.gettempdir())), Path(os.path.realpath("/tmp"))}
    if any(_inside(home, writable) for writable in roots):
        raise UnsupportedCapability("Codex caller approvals refuse a CODEX_HOME inside a sandbox-writable location (the workspace, $TMPDIR or /tmp), where a sandboxed command could rewrite Codex's own configuration")
    check_rules(home)
    config = user_config(home)
    if "profile" in config:
        raise UnsupportedCapability("Codex caller approvals refuse a profile set in CODEX_HOME/config.toml")


def user_config(home: Path) -> dict[str, Any]:
    path = home / "config.toml"
    try:
        return tomllib.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise UnsupportedCapability("Codex caller approvals refuse an unreadable or invalid CODEX_HOME/config.toml") from exc


def ambient(environ: Any = None) -> dict[str, list[str]]:
    """User-level MCP servers and shell environment `set` entries that pins cannot remove (names only)."""
    try:
        config = user_config(codex_home(environ))
    except UnsupportedCapability:
        return {"mcp_servers": [], "shell_environment_set": []}
    servers = config.get("mcp_servers")
    shell = config.get("shell_environment_policy")
    shell_set = shell.get("set") if isinstance(shell, dict) else None
    return {"mcp_servers": sorted(servers) if isinstance(servers, dict) else [], "shell_environment_set": sorted(shell_set) if isinstance(shell_set, dict) else []}


def prepare(invocation: Invocation, parts: dict[str, Any], freedom: str, version: str | None, write: bool, environ: Any = None) -> Invocation:
    """The app-server invocation for an exec-shaped one, after every pre-spawn refusal."""
    validate(True, freedom, None, invocation.native_subagent)
    if write and not qualification.supports(version, "caller_write_tools"):
        raise UnsupportedCapability("caller_write_approvals is not qualified on this Codex version")
    if version is None or not qualification.platform_supported(version):
        raise UnsupportedCapability("Codex caller approvals are qualified only on a measured version and platform")
    if invocation.extra_writable_paths or invocation.allowed_commands:
        raise UnsupportedCapability("Codex caller approvals cannot be combined with extra writable paths or allowed_commands")
    cwd = os.path.realpath(parts["repo"])
    root = git_root(Path(cwd))
    check_home(codex_home(environ), cwd, root, environ)
    linked = linked_roots(root)
    check_project_rules(cwd, root, linked)
    ctx = {"transport": TRANSPORT, "version": version, "cwd": cwd, "git_root": root, "freedom": freedom, "network": parts["network"],
           "model": parts.get("model"), "effort": parts.get("effort"), "prompt": parts["prompt"], "thread_id": parts.get("thread_id"), "write": write, **({"linked_roots": linked} if linked else {})}
    validate_context(ctx)
    return replace(invocation, argv=argv(ctx), stdin_mode="devnull", initial_input=None, control_pipe=True, control_context=ctx, identity_markers=IDENTITY_MARKERS,
                   caller_approvals=True, caller_approval_version=version, caller_write_approvals=write)


def validate(enabled: bool, freedom: str, max_turns: int | None, native: bool = False) -> None:
    if type(enabled) is not bool:
        raise ValueError("caller_approvals must be a boolean")
    if enabled and (freedom not in FREEDOM_SANDBOX or max_turns is not None or native):
        raise UnsupportedCapability("Codex caller approvals require an uncapped non-native task at write_in_repo or publish")


def support() -> dict[str, Any]:
    return {"supported": True, "transport": "codex app-server", "versions": qualification.versions(), "platforms": ["darwin"], "tools": ["commandExecution"],
            "freedoms": list(FREEDOM_SANDBOX), "scope": "exact_action_once", "native": False, "turn_caps": False, "write_approvals": True, "write_tools": ["fileChange"], "workflows": False}


def protected_paths(repo: Any, environ: Any = None) -> list[Path]:
    """Locations a caller may never approve a file change into: the resolved CODEX_HOME, and `.codex` and `.agents` in
    every directory from the Git root down to the working directory and in each linked-worktree root. (`.git` hooks and
    config are covered by the generic protected targets.) The Codex sandbox itself protects `.git`, `.codex` and `.agents`
    at the Git root and the working directory (measured); the intermediate directories and linked roots are a conservative extension."""
    found = [codex_home(environ)]
    try:
        cwd = os.path.realpath(repo)
        root = git_root(Path(cwd))
        directories = [*project_directories(cwd, root), *(Path(r) for r in linked_roots(root))]
    except UnsupportedCapability:
        directories = [Path(os.path.realpath(repo))]
    for directory in directories:
        found += [directory / ".codex", directory / ".agents"]
    return found


def enforcement_caveats(environ: Any = None, write: bool = False) -> tuple[str, ...]:
    notes = ambient(environ)
    caveats = [
        "Caller approvals run Codex through `codex app-server` with approval policy on-request, the approvals reviewer pinned to the caller and project trust forced to untrusted: this repository's own .codex config, rules and hooks do not apply to the task.",
        "An approved command may run outside the OS sandbox (Codex does not say which approvals leave it), and command approvals are not path-checked: an approved command can modify the repository's Git config and hooks, CODEX_HOME and Harflow's own state. Each approval covers one exact command shown to the caller.",
        ("File changes Codex asks about (patches outside the workspace) can be approved one exact patch at a time; the paths and diff are frozen at approval, a completion that differs stops the run, and writes to CODEX_HOME, Git config and hooks and every .codex and .agents from the Git root to the working directory are refused; the paths are re-checked just before the approval is written, and a changed or newly protected path is refused instead. "
         if write else "File changes outside the workspace are not approvable without caller_write_approvals: those requests are declined. ")
        + "MCP tool calls that need approval are always rejected.",
    ]
    if notes["mcp_servers"]:
        caveats.append("User MCP servers in CODEX_HOME/config.toml stay in effect: " + ", ".join(notes["mcp_servers"]) + ".")
    if notes["shell_environment_set"]:
        caveats.append("shell_environment_policy.set entries in CODEX_HOME/config.toml stay in effect: " + ", ".join(notes["shell_environment_set"]) + ".")
    return tuple(caveats)


_LIMITS_HEAD = ("Exact original command only, through `codex app-server`; an approved command may run outside the OS sandbox and can modify Git config and hooks, CODEX_HOME and Harflow state. "
                "This repository's .codex config is ignored. ")
_LIMITS_TAIL = (" macOS only. Dispatch can still refuse: no confirmable Git root, a CODEX_HOME or project rules file that is not only prompt or forbidden prefix_rule calls, "
                "a profile in CODEX_HOME/config.toml, or a CODEX_HOME inside a sandbox-writable location (the workspace, $TMPDIR or /tmp).")


def limitations(write: bool) -> str:
    middle = ("File changes are approvable one exact patch at a time (caller_write_approvals); MCP approvals are declined."
              if write else "File-change (without caller_write_approvals) and MCP approvals are declined.")
    return _LIMITS_HEAD + middle + _LIMITS_TAIL


LIMITATIONS = limitations(False)


def mechanism_suffix() -> str:
    return " + codex app-server caller approvals (approval_policy on-request, approvals reviewer user)"
