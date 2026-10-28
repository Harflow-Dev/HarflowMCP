"""The contract every coding agent must satisfy, and nothing beyond it.

Backends differ in ways that cannot be papered over: only Claude has a turn cap, only Claude lets
us choose the session id, only Codex has an OS sandbox, Codex, vibe and antigravity report no
dollar cost, and only Claude and antigravity take live input — a mid-turn message folds into the
running turn on claude, while every written line starts a turn of its own on antigravity. Rather
than pretending otherwise, each backend declares its `Capabilities` and reports the `Enforcement`
its flags actually deliver. Everything outside this package works on the normalised view and never
branches on a backend's name.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, NamedTuple, Protocol, runtime_checkable

from .mcp_approval import MCPApprovalPolicy

Freedom = Literal["read_only", "write_in_repo", "publish", "unrestricted"]

FREEDOMS: tuple[Freedom, ...] = ("read_only", "write_in_repo", "publish", "unrestricted")
DEFAULT_FREEDOM: Freedom = "write_in_repo"

Status = Literal["running", "completed", "failed", "timed_out", "cancelled"]

StdinMode = Literal["devnull", "pipe", "pipe_once"]
STDIN_DEVNULL: StdinMode = "devnull"
STDIN_PIPE: StdinMode = "pipe"


@dataclass(frozen=True)
class Invocation:
    """Everything `_spawn` needs to launch one run: the argv, how its stdin is wired, and the first
    bytes to write to it, and the exact backend-approved environment contribution for the child.

    Returned by the argv builders instead of a bare argv, so the stdin mode comes from the run that
    was actually built — never from the backend's name or its static capability. A live-input run
    (`stdin_mode="pipe"`) carries its prompt as `initial_input`, because a CLI reading its input as a
    stream ignores a positional prompt (measured on claude). Every other run is `"devnull"` with no
    initial input. Backends may also support "pipe_once": a large prompt is sent as
    initial bytes and stdin is immediately closed, without a live-input pump. `env` defaults to
    empty; each backend validates its complete allowed mapping in `assert_safe`.
    """

    argv: list[str]
    stdin_mode: StdinMode = STDIN_DEVNULL
    initial_input: bytes | None = None
    extra_writable_paths: tuple[str, ...] = ()
    # Request-local runtime observation, never persisted as grant authority. The execution
    # boundary refreshes it before any scratch allocation; final checks only inspect identity.
    extra_path_runtime: tuple[Any, ...] | None = None
    extra_path_configuration: tuple[Any, ...] | None = None
    group_report_directory: str | None = None
    group_report_identity: tuple[Any, ...] | None = None
    group_report_runtime: tuple[Any, ...] | None = None
    group_report_configuration: tuple[Any, ...] | None = None
    group_report_qualification: dict[str, Any] | None = None
    scratch_directory: str | None = None
    # The shared chain and group folders granted to this run, in that order, after `scratch_directory`
    # and before any extra writable paths in a backend's `--add-dir` list.
    shared_scratch_directories: tuple[str, ...] = ()
    # The version id of the static scratch-folder note this argv carries through a system-prompt
    # seam (`system_guidance.VERSION`), or None when it carries none.
    system_guidance: str | None = None
    # The state root whose `guidance` directory holds the instructions file a per-run seam points at; a
    # backend that reads the file by path accepts it only from there.
    system_guidance_root: str | None = None
    native_subagent: bool = False
    caller_approvals: bool = False
    caller_write_approvals: bool = False
    caller_approval_version: str | None = None
    # A stdin pipe the bridge owns for protocol frames (approval answers, Harflow-originated
    # frames), independent of user-message live input. `live_input` stays false for it: no inbox
    # forwarding, no `send_message` delivery. Claude's live-input runs never set it.
    control_pipe: bool = False
    # Backend-private, JSON-safe settings for the frames a control-pipe run's backend originates (seeded into
    # the run's accumulator before the first frame is written). Meaningful only with `control_pipe`.
    control_context: dict[str, Any] | None = None
    # Strings that must appear on the process's command line for its identity to match; None keeps the
    # dispatcher's default (binary plus session id or repository path).
    identity_markers: tuple[str, ...] | None = None
    allowed_commands: tuple[str, ...] = ()
    allowed_commands_version: str | None = None
    native_settings: dict[str, Any] | None = None
    env: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.control_context is not None and not self.control_pipe:
            raise ValueError("control_context requires control_pipe")
        if self.control_pipe and (self.stdin_mode != STDIN_DEVNULL or self.initial_input is not None):
            raise ValueError("control_pipe requires stdin_mode 'devnull' and no initial input: it is a bridge-owned frame pipe, not live input")

    @property
    def live_input(self) -> bool:
        return self.stdin_mode == STDIN_PIPE


def validate_invocation_env(invocation: Any, expected: Mapping[str, str], *, allow_group_report_output: bool = False) -> None:
    """Require the invocation's complete environment contribution to match its backend contract."""
    # Leave shape diagnostics to each backend's existing Invocation validation. In particular,
    # preserve the established "expected an Invocation" error for bare argv lists.
    if not isinstance(invocation, Invocation):
        return
    if not allow_group_report_output and any(getattr(invocation, name, None) is not None for name in (
            "group_report_directory", "group_report_identity", "group_report_runtime",
            "group_report_configuration", "group_report_qualification")):
        raise InvocationEnvironmentError("group report authority is unsupported by this backend invocation")
    env = getattr(invocation, "env", None)
    if not isinstance(env, Mapping) or any(type(key) is not str or type(value) is not str for key, value in env.items()):
        raise InvocationEnvironmentError("invocation env must be a string-to-string mapping")
    if dict(env) != dict(expected):
        raise InvocationEnvironmentError(f"unrecognised invocation env: {dict(env)!r}")


class InvocationEnvironmentError(RuntimeError):
    """The invocation requested an environment contribution outside its backend contract."""


def classic_invocation_problem(invocation: Any) -> str | None:
    """Why `invocation` is not a plain devnull-stdin run, or None if it is — for backends with no
    live input. A bare argv list is refused too: a caller that skipped the Invocation could not have
    said how the process's stdin is wired."""
    if not isinstance(invocation, Invocation):
        return f"expected an Invocation, got {type(invocation).__name__}: {invocation!r}"
    if invocation.control_pipe:
        return "control_pipe is set, but this backend has no control pipe"
    if invocation.stdin_mode != STDIN_DEVNULL:
        return f"stdin_mode is {invocation.stdin_mode!r}, but this backend has no live input"
    if invocation.initial_input is not None:
        return "initial_input is set, but this backend has no live input to write it to"
    return None

# Stopping at xhigh is deliberate, not an oversight — but "every model measured accepts" is only
# true of codex (per ~/.codex/models_cache.json), where no canonical level can produce a
# model-dependent failure. It is not true of opencode: --variant support is per model there, e.g.
# ling-3.0-flash-fin accepts only low/medium/high (no xhigh), and several models declare no
# `variants` at all and silently ignore the flag — see OpencodeBackend's reasoning_effort caveat.
# antigravity refuses xhigh loudly before any model call (exit 1, `invalid --effort "xhigh"`,
# measured), so it declares only low/medium/high and check_reasoning_effort turns the request
# away up front. Each backend's own ceiling is therefore left unreachable on purpose — claude
# `max`, codex `max`/`ultra`, antigravity `max` — so a caller cannot spend it by accident. The
# tiers below `low` (codex and opencode `minimal`, codex `none`) are unreachable too, for no
# reason beyond keeping one vocabulary shared by the four backends that accept an effort at all —
# vibe accepts none, so it is outside this vocabulary rather than a fifth member of it.
EFFORTS: tuple[str, ...] = ("low", "medium", "high", "xhigh")


class ReasoningEffort(NamedTuple):
    """What a backend does with the shared four-level effort vocabulary in `EFFORTS`.

    The vocabulary is passed through verbatim — each of the four level *names* (`low`, `medium`,
    `high`, `xhigh`) is spelled the same way, and accepted, on claude, codex and opencode's own
    flag, per each CLI's `--help` text and model metadata. That is a claim about spelling only:
    it says nothing about the levels behaving identically, or producing an equivalent ladder,
    across backends — see `accepted_in_real_run` and `levels_change_behaviour` for what was
    actually measured about behaviour.
    """

    accepts_parameter: bool
    levels: tuple[str, ...]
    """Canonical levels (a subset of `EFFORTS`) this backend accepts. Acceptance is not the same
    as taking effect — see `levels_change_behaviour` and this backend's own caveats."""

    native_flag: str
    """The flag/option this backend's effort is carried on. Empty when accepts_parameter is False."""

    # Split the way `commit_push_blocked`/`direct_commit_commands_denied` are: a single
    # `runtime_verified` boolean claimed "a real run showed the flag changing behaviour" for all
    # three backends, but only opencode has a cross-level behavioural measurement — claude's
    # evidence is acceptance without degradation, and codex's own stream reports zero reasoning
    # output tokens at every level, so no cross-level difference is even observable there. A
    # caveat cannot repair a boolean that says something untrue, so the claim is split into what
    # was actually measured.
    accepted_in_real_run: bool
    """A real run accepted this flag: the CLI neither refused it nor silently degraded it to a
    default. Acceptance only — says nothing about whether different levels change behaviour."""

    levels_change_behaviour: bool
    """Stronger than acceptance: a real run measured different behaviour between two levels of the
    shared vocabulary. False does not mean the levels are inert — only that no such comparison was
    made, or (codex) that the signal to make one is not present in the stream at all."""

    caveats: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "accepts_parameter": self.accepts_parameter,
            "levels": list(self.levels),
            "native_flag": self.native_flag,
            "accepted_in_real_run": self.accepted_in_real_run,
            "levels_change_behaviour": self.levels_change_behaviour,
            "caveats": list(self.caveats),
        }


class NetworkControl(NamedTuple):
    """Whether harflow_mcp can raise or lower its own network barrier, per freedom.

    Two freedom tuples rather than a pair of global booleans because support is non-rectangular
    on codex: `read_only` cannot enable (measured — the `sandbox_workspace_write.network_access`
    key is inert under the read-only sandbox even when set true), `workspace-write` takes either
    direction, and `unrestricted` cannot block (danger-full-access disables the sandbox, so no
    barrier remains to raise). A backend with no network-controlling mechanism at all declares
    every freedom in `can_enable` — `network=True` means "impose no barrier of your own", which
    having none to impose genuinely delivers — and nothing in `can_block`, because
    `network=False` means "impose one", which it genuinely cannot. Every claim here is about
    harflow_mcp's own barrier only, never about reachability.
    """

    can_enable: tuple[Freedom, ...]
    """Freedoms where network=True is accepted: harflow_mcp can lift, or need not raise, its own barrier."""

    can_block: tuple[Freedom, ...]
    """Freedoms where network=False is accepted: harflow_mcp can impose its own barrier."""

    caveats: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "can_enable": list(self.can_enable),
            "can_block": list(self.can_block),
            "caveats": list(self.caveats),
        }


class Capabilities(NamedTuple):
    """What a backend can and cannot do, so callers are told rather than surprised."""

    chooses_session_id: bool
    """Whether we can assign the session id up front, or must wait for the run to disclose it."""

    supports_turn_cap: bool
    reports_cost_usd: bool
    os_sandbox: bool
    """Whether restrictions are enforced by the operating system rather than by the agent itself."""

    per_command_deny: bool
    """Whether individual commands (e.g. `git commit`) can be denied."""

    supports_model_selection: bool
    """Whether this backend has a flag to choose the model at all. False means `model` is refused
    outright rather than silently ignored — see `reject_model`. vibe is the first backend where this
    is False: model choice is config-only there, with no CLI flag."""

    reasoning_effort: ReasoningEffort

    network_control: NetworkControl

    supports_live_input: bool
    """Whether a run can take further messages on stdin while it works (`send_message`). A capability
    of the backend, not a promise about every run: claude falls back to the classic one-shot shape
    when `max_turns` is set (that combination is unmeasured), so what a *task* got is its own
    `live_input` field, never this one."""

    tracks_background_tasks: bool = False
    """Whether normalized stream evidence supports live background idle waiting."""

    supports_auto_continue: bool = False
    """Whether a structured recoverable cutoff has a certified continuation classifier."""

    live_input_message_is_turn: bool = False
    """Whether each message written on stdin starts a turn of its own, even mid-turn. agy does
    (measured: two queued lines, EOF at the first result, three results), so a run that received N
    messages is idle only after N further results — see `tasks._note_input_written`. claude does
    not: it folds a mid-turn message into the running turn (measured, claude 2.1.281), so however
    many messages landed, exactly one further result settles the run. Default False so the
    folding semantics stay the behaviour of every backend that does not say otherwise."""

    supports_caller_approvals: bool = False
    """Whether a version-qualified exact-action caller control transport exists."""

    system_guidance: str | None = None
    """How the backend takes Harflow's static scratch-folder note through a system-prompt seam that
    grants nothing else: `first_turn_sticky` (read when the session starts, kept for the session),
    `per_run` (read on every launch) or None (no such seam was measured)."""

    resume_may_start_fresh: bool = False
    """Whether an unavailable conversation can silently create a new one on resume.

    Such a CLI cannot provide strict conversation inheritance without an
    authoritative session-existence check. Ordinary task resume retains its
    existing backend behavior; strict child workflow reuse refuses it upfront.
    """

    def as_dict(self) -> dict[str, Any]:
        # `_asdict()` does not recurse into a nested NamedTuple — it would serialize as a bare
        # JSON list and lose its field names — so each nested block is expanded explicitly.
        data: dict[str, Any] = dict(self._asdict())
        data["reasoning_effort"] = self.reasoning_effort.as_dict()
        data["network_control"] = self.network_control.as_dict()
        return data


@dataclass(frozen=True)
class Enforcement:
    """What a run's restrictions actually amount to.

    Reported with every task so a uniform `freedom` value never implies a guarantee the chosen
    backend cannot make. Each boolean is a strict claim: it is True only when the thing it names
    genuinely cannot happen. Where a restriction is real but weaker than the name suggests, that
    belongs in the *other* fields, not in a caveat attached to a True — a caveat cannot repair a
    boolean that says something untrue.
    """

    freedom: str
    mechanism: str

    os_enforced: bool
    """Whether the operating system imposes this, rather than the agent policing itself."""

    writes_confined: bool
    """Whether writes are restricted at all. See `writable_roots` for where they may still land."""

    writable_roots: tuple[str, ...] = ()
    """Where writes remain possible when confined — often wider than just the repository."""

    commit_push_blocked: bool = False
    """Whether committing or pushing is genuinely prevented. Rarely true; see the next field."""

    direct_commit_commands_denied: bool = False
    """Whether the obvious `git commit` / `git push` invocations are refused, which is weaker."""

    publish_attempts_allowed_by_polybridge: bool = False
    """True only at freedoms where harflow_mcp authorized an *attempt* to commit, push or open a
    PR. An authorization claim and nothing more: it does not say the mechanism prevents the
    attempt elsewhere — codex at `write_in_repo` with `network=True` can mechanically reach a
    remote while this field is false, the difference being recorded intent, not an enforced
    barrier — nor that publication will actually succeed — credentials, remote permissions,
    branch protection, hooks, an unauthenticated `gh`, and the agent's own behaviour are all
    outside harflow_mcp's control. Deliberately not named `publishing_permitted`: that name would
    be read as a promise this field cannot make. (An earlier docstring also claimed harflow_mcp
    "configured no publish-specific barrier of its own" here; that conjunction became untrue in
    both directions once network was requestable — true where this is false — so it was removed
    rather than caveated, per the rule that a caveat cannot repair an untrue claim.)"""

    network_access: str = "not_controlled"
    """One of "blocked" | "enabled" | "unrestricted" | "not_controlled". "not_controlled" means
    harflow_mcp imposes nothing of its own here and the surrounding environment decides — which is
    NOT the same as "blocked"."""

    caveats: tuple[str, ...] = ()
    extra_writable_paths: tuple[dict[str, Any], ...] = ()
    extra_path_certification: str | None = None
    caller_approval_tools: tuple[str, ...] = ()
    """Tools a caller may be asked to approve for this run; empty (and omitted from `as_dict`) without caller approvals."""
    git_environment: dict[str, Any] | None = None
    """Observed process Git configuration; never a network or credential access guarantee."""
    group_report_output: bool | None = None
    extra_allowed_commands: tuple[str, ...] = ()
    """Dispatch-time command patterns granted to this run; empty (and omitted from `as_dict`) without `allowed_commands`."""

    @property
    def publish_attempts_allowed(self) -> bool:
        """Canonical publication intent; the legacy constructor field remains compatible."""
        return self.publish_attempts_allowed_by_polybridge

    def as_dict(self) -> dict[str, Any]:
        return {
            **({"git_environment": self.git_environment} if self.git_environment is not None else {}),
            **({"caller_approval_tools": list(self.caller_approval_tools)} if self.caller_approval_tools else {}),
            **({"extra_allowed_commands": list(self.extra_allowed_commands)} if self.extra_allowed_commands else {}),
            "extra_writable_paths": list(self.extra_writable_paths),
            "extra_path_certification": self.extra_path_certification,
            "freedom": self.freedom,
            "mechanism": self.mechanism,
            "os_enforced": self.os_enforced,
            "writes_confined": self.writes_confined,
            "writable_roots": list(self.writable_roots),
            **({"group_report_output": self.group_report_output} if self.group_report_output is not None else {}),
            "commit_push_blocked": self.commit_push_blocked,
            "direct_commit_commands_denied": self.direct_commit_commands_denied,
            "publish_attempts_allowed": self.publish_attempts_allowed,
            # Deprecated alias retained for one compatibility version.
            "publish_attempts_allowed_by_polybridge": self.publish_attempts_allowed,
            "network_access": self.network_access,
            "caveats": [*self.caveats, *(
                ["Read-only describes agent tool permissions; the harness may still persist session history, logs and configuration in its own user state directory."]
                if self.freedom == "read_only" else []
            )],
        }


@dataclass
class Accumulator:
    """The normalised picture of a run, filled in by whichever backend produced the stream."""

    failure_diagnostic: dict[str, Any] | None = None
    session_id: str | None = None
    summary: str | None = None
    is_error: bool | None = None
    num_turns: int | None = None
    total_cost_usd: float | None = None
    usage: dict[str, Any] | None = None
    denials: list[dict[str, Any]] = field(default_factory=list)
    mcp_servers: list[dict[str, Any]] = field(default_factory=list)
    available_tool_count: int | None = None

    terminal: dict[str, Any] | None = None
    """The backend's own end-of-run event, if it emits one. Used for classification."""

    saw_final_message: bool = False
    """Whether the agent produced a closing message — for backends with no terminal event."""

    notices: list[str] = field(default_factory=list)
    """Non-fatal messages the run emitted. Informational: these do not mean the run failed."""

    event_count: int = 0
    unparsable_lines: int = 0

    normalize_errors: int = 0
    """How many stream events the monitor normalizer failed on. Counted by the drain path, never
    read by `classify`, so a normalizer bug can never change a run's reported outcome."""

    result_count: int = 0
    """How many end-of-turn results the stream has carried. One on a classic run; one per turn on
    a live-input run, which is why the per-result counters are accumulated rather than assigned."""

    turn_open: bool = False
    """A turn is running: the agent has shown activity since the last result. Distinguishes a run
    waiting only on background tasks (the idle bound applies) from one that is simply working."""

    latest_result: dict[str, Any] | None = None
    background_details: dict[str, dict[str, Any]] = field(default_factory=dict)
    background_open: set[str] = field(default_factory=set)
    """Ids of background tasks the agent started and the stream has not yet reported finished. A
    live-input run is not idle while any are open: closing its stdin would kill them (measured)."""

    awaiting_input: bool = False
    """A live-input run is idle: a result has arrived, no turn is running, and no background task
    is open. The input pump closes stdin once this holds with nothing queued. Set by `ingest`,
    cleared by `ingest` on turn activity and by the pump when it writes a message."""

    error_result_seen: bool = False
    """A result reported an error (or a non-success subtype). Sticky: the input pump stops
    forwarding and closes stdin, and every message still queued is reported undelivered."""

    background_abandoned: bool = False
    """Set by the input pump when a run waited on background tasks alone, with no output, for the
    idle bound, and stdin was closed anyway — which kills them. A backend's `classify` reports that
    as a failure, never a clean completion."""

    stream_state: dict[str, Any] = field(default_factory=dict)
    """Backend-private scratch space for an ingest algorithm that needs memory across events (e.g.
    vibe's current-turn tracking). Lives here, per task, rather than on the backend instance:
    `BACKENDS` holds one shared instance per backend, so state on `self` would corrupt concurrent
    tasks. Never read by anything outside the backend that wrote it."""


def classified_denials(backend: Any, acc: Accumulator, freedom: str | None) -> tuple[list[Any], list[str]]:
    """The backend's classified denials and their distinct categories; raw denials if classification fails."""
    try:
        denials = backend.classify_denials(acc.denials, acc, freedom)
    except Exception:
        return acc.denials, []
    return denials, list(dict.fromkeys(d["denial_category"] for d in denials if isinstance(d, dict) and isinstance(d.get("denial_category"), str)))


class ApprovalRefused(ValueError):
    """A refusal whose message is safe and useful to show the caller as a bridge notice.

    `audit`, when given, is a short bounded reason the runtime also records as a system audit entry in the approval ledger;
    `outcomes` are `(tool_use_id, outcome)` pairs the runtime records on the matching rows before it stops the run."""

    def __init__(self, message: str, audit: str | None = None, outcomes: tuple[tuple[str, str], ...] = ()) -> None:
        super().__init__(message)
        self.audit = audit
        self.outcomes = tuple(outcomes)


class UnsupportedCapability(ValueError):
    """A request a backend cannot honour, which must fail rather than be silently dropped."""


class NestedDispatchRefused(UnsupportedCapability):
    """A nested dispatch (one task spawning another via harflow_mcp) that would be weaker than its
    parent task on some enforcement field, or that would exceed the parent's depth budget.

    This is a best-effort cap, not a sandbox boundary — see `check_nested_enforcement` and
    `check_nested_depth` for what it actually compares and why it can be wrong. `rule` names which
    single check failed (a field name from `POLICY_FIELDS`, or `"backend"`, `"repo"`,
    `"parent_enforcement_unrecorded"`, `"depth"`), so a caller can log or test on the failure
    reason without parsing the message.
    """

    def __init__(self, message: str, *, rule: str) -> None:
        super().__init__(message)
        self.rule = rule


def control_pipe_problem(backend: Any) -> str | None:
    """Why `backend` may not run a control-pipe invocation, or None: it must override
    `control_pipe_finished`, because a pipe with no lifetime rule would never close."""
    method = getattr(type(backend), "control_pipe_finished", None)
    if method is None or method is ExtraPathsUnsupported.control_pipe_finished:
        return f"{getattr(backend, 'name', 'this backend')} declares no control pipe lifetime (control_pipe_finished)"
    return None


class ExtraPathsUnsupported:
    """Default fail-closed seams for uncertified optional execution capabilities."""

    def verify_launch_identity(self) -> None:
        """Last check before spawn. The default accepts the selected binary."""
        return None

    def qualification_features(self):
        """Versioned unsupported features; no probe runs for ordinary dispatch."""
        from .qualification import FeatureSpec
        return {name: FeatureSpec(name, "unsupported-v1") for name in
                ("caller_approval_tools", "caller_write_tools", "path_safety_prompts", "denial_texts", "allowed_commands")}

    def group_report_output_support(self):
        return {"supported": False, "reason": "This backend has no qualified scoped read-only group report output; return the report in your task result"}

    def qualify_group_report_output(self):
        raise UnsupportedCapability(self.group_report_output_support()["reason"])

    def preview_group_report_output(self, invocation, path, freedom, network=None):
        raise UnsupportedCapability(self.group_report_output_support()["reason"])

    def configure_group_report_output(self, invocation, path, freedom, network=None):
        raise UnsupportedCapability(self.group_report_output_support()["reason"])

    def assert_group_report_output(self, invocation, path, freedom, network=None):
        raise UnsupportedCapability(self.group_report_output_support()["reason"])

    def group_report_output_enforcement(self, enforcement, path):
        raise UnsupportedCapability(self.group_report_output_support()["reason"])

    def qualification_version(self, observed):
        return observed.strip() if isinstance(observed, str) and observed.strip() else None

    def accounting_evidence(self, acc, context):
        from ..accounting import unavailable
        return unavailable(context["task_id"], "backend_accounting_unqualified")

    def configure_dispatch_context(self, invocation, background_idle_seconds):
        return invocation

    def recovery_activity(self, record, cutoff, cursor=None, limit=50):
        return {"source": "backend_transcript", "display_only": True, "events": [],
                "next_cursor": None, "reset": False, "has_more": False,
                "notices": ["Backend transcript recovery is unsupported"]}

    def caller_approval_support(self) -> dict[str, Any]:
        return {"supported": False}

    def classify_denials(self, denials, acc, freedom=None):
        """Backends without measured refusal texts leave every denial unclassified; non-dict entries pass through."""
        return [{**d, "denial_category": "unclassified", "denial_remedy": "Inspect the refusal text. No remedy is inferred.",
                 "classification_basis": {"reason": "backend_unmeasured"}} if isinstance(d, dict) else d for d in denials]

    def validate_caller_approvals(self, enabled, freedom, max_turns, native=False):
        if type(enabled) is not bool:
            raise ValueError("caller_approvals must be a boolean")
        if enabled:
            raise UnsupportedCapability("This backend has no qualified caller approval transport")

    def qualify_caller_approvals(self):
        raise UnsupportedCapability("This backend has no qualified caller approval transport")

    def validate_caller_write_approvals(self, write, enabled, version=None):
        if type(write) is not bool:
            raise ValueError("caller_write_approvals must be a boolean")
        if write:
            raise UnsupportedCapability("This backend has no qualified caller write approvals")

    def configure_caller_approvals(self, invocation, freedom, max_turns, version=None, write=False):
        raise UnsupportedCapability("This backend has no qualified caller approval transport")

    def approval_stream_version(self, acc):
        return None

    def allowed_commands_support(self) -> dict[str, Any]:
        return {"supported": False, "reason": f"allowed_commands is not qualified for the {self.name} backend"}

    def validate_allowed_commands(self, patterns, freedom, repo):
        if patterns:
            raise UnsupportedCapability(self.allowed_commands_support()["reason"])

    def qualify_allowed_commands(self):
        raise UnsupportedCapability(self.allowed_commands_support()["reason"])

    def configure_allowed_commands(self, invocation, patterns, freedom, repo, version=None):
        if patterns:
            raise UnsupportedCapability(self.allowed_commands_support()["reason"])
        return invocation

    def allowed_commands_enforcement(self, enforcement, patterns):
        if patterns:
            raise UnsupportedCapability(self.allowed_commands_support()["reason"])
        return enforcement

    def approval_enforcement(self, enforcement, version, enabled, write):
        return enforcement

    def decode_approval_request(self, event, qualification=None):
        return None

    def decode_oversize_approval(self, event, qualification=None):
        return None

    def check_approval_event(self, event, qualification=None):
        return None

    def encode_approval_response(self, request, decision):
        raise UnsupportedCapability("This backend has no qualified caller approval transport")

    def approval_outcomes(self, event):
        return []

    # Generic approval seams. Every default is fail-closed: a backend that declares none of them
    # routes no frame as an approval, extracts no paths, claims no transport and writes nothing.
    def approval_frame_kind(self, event):
        """Classify one parsed stdout event: "request" (a permission request to decide),
        "cancellation" or "resolved" (the harness closed or answered a request itself), else None."""
        return None

    def approval_request_key(self, event):
        """The string stored as a ledger row's `native_request_id` for a request, cancellation or
        resolved event, or None. Typed by the backend so that, for example, an integer and a string
        id can never collide."""
        return None

    def approval_paths(self, decoded):
        """Absolute paths a decoded request would touch (a list, so one action may touch several).

        There is no safe default: a backend that routes approval requests must say which paths they touch
        (an empty list for an action that touches none). Raising here makes `observe` cancel and audit."""
        raise UnsupportedCapability(f"{self.name} does not declare the paths its approval requests touch")

    def approval_write_tools(self):
        """Tool names whose approval can write, which are checked against the protected targets.

        No safe default either: an undeclared set would skip the protected-target checks."""
        raise UnsupportedCapability(f"{self.name} does not declare which approval tools can write")

    def protected_config_paths(self, repo):
        """Extra locations a caller may never approve writes to, beyond Harflow's own and Git's."""
        return []

    def approval_transport(self):
        """The name of the approval transport reported by `preview_permissions`, or None."""
        return None

    def approval_limitations(self, write):
        """Backend-specific limitation text reported by `preview_permissions`."""
        return "Caller approvals are not available for this backend."

    def approval_context(self, acc):
        """Backend state handed to `decode_approval_request` and `check_approval_event` as `qualification` entries."""
        return {}

    def observed_approval_outcomes(self, event, acc):
        """`approval_outcomes` for a backend whose outcome correlation needs the run's state."""
        return self.approval_outcomes(event)

    def workflow_caller_approvals_supported(self):
        """Whether a saved workflow candidate may carry `caller_approvals` on this backend."""
        return False

    def note_approval_answer(self, acc, row, effective, written):
        """Called after the pump wrote (or failed to write) a ledger row's answer, with the run's accumulator."""
        return None

    def observe_stderr(self, acc, line):
        """Called with every non-empty stderr line of a run, for stderr evidence a backend measured."""
        return None

    def approval_scratch(self):
        """Whether a task with caller approvals still gets a task-owned scratch directory."""
        return True

    def approval_notice(self, write):
        """The bridge notice a task with caller approvals carries."""
        return "Caller approvals use a version-qualified control transport for one exact action. Existing harness permissions remain authoritative. No reusable permission grant is installed."

    def control_pipe_finished(self, acc):
        """Whether a control-pipe-only run (no live input) should now have its stdin closed."""
        return False

    def outgoing_frames(self, phase, acc):
        """Harflow-originated frames for `phase` ("initial" after spawn, "turn" on each pump pass),
        written in order through the bridge's single stdin writer. Interface only; none by default."""
        return []

    def execution_configuration_namespaces(self, repo):
        raise UnsupportedCapability("backend execution configuration namespaces are not qualified")

    def plan_file_candidates(self, event):
        """Bounded stream metadata; this synchronous seam must perform no I/O."""
        return ()

    def observe_plan_candidates(self, candidates, state, repo):
        """Worker-only metadata observation using exclusively worker-owned state."""
        return []

    def builtin_plan_path(self, path, repo):
        return False

    def observed_plan_files(self, event, acc, repo):
        """Successful built-in plan writes observed by this backend; metadata only."""
        return []

    def artifact_configuration_namespaces(self, repo):
        """Configuration namespaces excluded from metadata artifact registration."""
        groups = self.execution_configuration_namespaces(repo)
        return tuple(path for group in groups.values() for path in group)

    def extra_path_support(self) -> dict[str, Any]:
        return {"supported": False, "certification": None, "reason": "bounded writable paths are not certified for this backend"}

    def with_extra_writable_paths(self, invocation, grants, freedom, network=None):
        if grants:
            raise UnsupportedCapability(self.extra_path_support()["reason"])
        return invocation

    def assert_extra_writable_paths(self, invocation, grants, freedom, network=None):
        if grants or invocation.extra_writable_paths:
            raise UnsupportedCapability(self.extra_path_support()["reason"])
        self.assert_safe(invocation, freedom, network)

    def qualify_extra_writable_paths(self, invocation, grants, freedom, network=None):
        if invocation.extra_writable_paths:
            self.assert_extra_writable_paths(invocation, grants, freedom, network)
            return invocation
        return self.with_extra_writable_paths(invocation, grants, freedom, network)

    def extra_path_enforcement(self, enforcement, grants, repo):
        if grants:
            raise UnsupportedCapability(self.extra_path_support()["reason"])
        return enforcement


@runtime_checkable
class Backend(Protocol):
    def verify_launch_identity(self) -> None: ...

    name: str
    binary: str
    capabilities: Capabilities
    mcp_approval: MCPApprovalPolicy

    def accounting_evidence(self, acc: Accumulator, context: dict[str, Any]) -> dict[str, Any]: ...

    def execution_configuration_namespaces(self, repo: Path) -> dict[str, Any]: ...

    def classify_denials(self, denials: list[Any], acc: Accumulator, freedom: str | None = None) -> list[Any]: ...

    def artifact_configuration_namespaces(self, repo: Path) -> tuple[Path, ...]: ...
    def extra_path_support(self) -> dict[str, Any]: ...
    def with_extra_writable_paths(self, invocation, grants, freedom, network=None) -> Invocation: ...
    def assert_extra_writable_paths(self, invocation, grants, freedom, network=None) -> None: ...
    def qualify_extra_writable_paths(self, invocation, grants, freedom, network=None) -> Invocation: ...
    def extra_path_enforcement(self, enforcement, grants, repo) -> Enforcement: ...

    def caller_approval_support(self) -> dict[str, Any]: ...
    def validate_caller_approvals(self, enabled: bool, freedom: str, max_turns: int | None, native: bool = False) -> None: ...
    def qualify_caller_approvals(self) -> None: ...
    def validate_caller_write_approvals(self, write: bool, enabled: bool, version: str | None = None) -> None: ...
    def configure_caller_approvals(self, invocation: Invocation, freedom: str, max_turns: int | None, version: str | None = None, write: bool = False) -> Invocation: ...
    def approval_stream_version(self, acc: Accumulator) -> str | None: ...
    def allowed_commands_support(self) -> dict[str, Any]: ...
    def validate_allowed_commands(self, patterns: tuple[str, ...], freedom: str, repo: Path | None) -> None: ...
    def qualify_allowed_commands(self) -> str: ...
    def configure_allowed_commands(self, invocation: Invocation, patterns: tuple[str, ...], freedom: str, repo: Path | None, version: str | None = None) -> Invocation: ...
    def allowed_commands_enforcement(self, enforcement: Enforcement, patterns: tuple[str, ...]) -> Enforcement: ...
    def approval_enforcement(self, enforcement: Enforcement, version: str | None, enabled: bool, write: bool) -> Enforcement: ...
    def decode_approval_request(self, event: dict[str, Any], qualification: dict[str, Any] | None = None) -> dict[str, Any] | None: ...
    def decode_oversize_approval(self, event: dict[str, Any], qualification: dict[str, Any] | None = None) -> dict[str, Any] | None: ...
    def check_approval_event(self, event: dict[str, Any], qualification: dict[str, Any] | None = None) -> None: ...
    def encode_approval_response(self, request: dict[str, Any], decision: str) -> bytes: ...
    def approval_outcomes(self, event: dict[str, Any]) -> list[tuple[str, str]]: ...
    def approval_frame_kind(self, event: dict[str, Any]) -> str | None: ...
    def approval_request_key(self, event: dict[str, Any]) -> str | None: ...
    def group_report_output_support(self) -> dict[str, Any]: ...
    def qualify_group_report_output(self) -> dict[str, Any]: ...
    def preview_group_report_output(self, invocation: Invocation, path: Path, freedom: Freedom, network: bool | None = None) -> None: ...
    def configure_group_report_output(self, invocation: Invocation, path: Path, freedom: Freedom, network: bool | None = None) -> Invocation: ...
    def assert_group_report_output(self, invocation: Invocation, path: Path, freedom: Freedom, network: bool | None = None) -> None: ...
    def group_report_output_enforcement(self, enforcement: Enforcement, path: Path) -> Enforcement: ...
    def approval_paths(self, decoded: dict[str, Any]) -> list[str]: ...
    def approval_write_tools(self) -> frozenset[str]: ...
    def protected_config_paths(self, repo: Path) -> list[Path]: ...
    def approval_transport(self) -> str | None: ...
    def approval_limitations(self, write: bool) -> str: ...
    def approval_context(self, acc: Accumulator) -> dict[str, Any]: ...
    def observed_approval_outcomes(self, event: dict[str, Any], acc: Accumulator) -> list[tuple[str, str]]: ...
    def workflow_caller_approvals_supported(self) -> bool: ...
    def note_approval_answer(self, acc: Accumulator, row: dict[str, Any], effective: str, written: bool) -> None: ...
    def observe_stderr(self, acc: Accumulator, line: str) -> None: ...
    def approval_scratch(self) -> bool: ...
    def approval_notice(self, write: bool) -> str: ...
    def control_pipe_finished(self, acc: Accumulator) -> bool: ...
    def outgoing_frames(self, phase: str, acc: Accumulator | None) -> list[bytes]: ...

    def usage_limit_diagnostic(self, event: dict[str, Any]) -> dict[str, Any] | None: ...

    def stderr_usage_limit_diagnostic(self, line: str) -> dict[str, Any] | None: ...

    def build_start_argv(
        self,
        prompt: str,
        *,
        repo: Path,
        freedom: Freedom,
        session_id: str | None,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
        network: bool | None = None,
    ) -> Invocation: ...

    def build_resume_argv(
        self,
        prompt: str,
        *,
        repo: Path,
        freedom: Freedom,
        session_id: str,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
        network: bool | None = None,
    ) -> Invocation: ...

    def assert_safe(
        self, invocation: Invocation, freedom: Freedom, network: bool | None = None
    ) -> None:
        """Raise unless the invocation still carries this backend's required guarantees.

        The whole `Invocation`, not just its argv: a live-input argv paired with a devnull stdin (or
        a one-shot argv paired with a pipe) is a mismatch between what the CLI is told and how it is
        wired, and must be refused here like any other unsafe shape. A bare list is refused too.

        `freedom` and `network` together are the authorization the caller actually asked for —
        neither is, nor can be, derived from `argv` itself. An argv built for `read_only` can be
        just as internally consistent as one built for `unrestricted`: checking that the
        mode/agent token is *one of* the backend's known values only proves the argv is
        well-formed, not that it is well-formed for what the caller requested. So every
        implementation must check the mode/agent token against the exact value this freedom maps
        to (e.g. `PERMISSION_MODES[freedom]`), not merely against the set of values it could
        take — and, where the backend carries its network setting in argv, that the network
        token matches the mechanism this (freedom, network) pair *resolves to*.

        That is deliberately weaker than this docstring once claimed: it can no longer promise
        the argv is well-formed for the authorization tuple itself, because two authorizations
        can resolve to the same argv — codex `write_in_repo` with `network=True` is
        byte-identical to `publish`'s default, and codex `publish` with `network=False` to
        `write_in_repo`'s default. Where that happens, assert_safe genuinely cannot refuse the
        crossed claim; there is no argv difference to detect. That is a loss of provenance, not
        a sandbox escape — the collapsed argv already had identical powers — and the collapses
        are pinned by tests rather than left as silent gaps.
        """
        ...

    def enforcement(self, freedom: Freedom, network: bool | None = None) -> Enforcement: ...

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        """Fold one stream event into the normalised view."""
        ...

    def usage_limit_diagnostic(self, event: dict[str, Any]) -> dict[str, Any] | None:
        """Recognize an authoritative terminal quota rejection, never assistant/tool prose.

        Returns category, reason, source, and optional provider-supplied reset_at. Ingest latches
        the first diagnostic so later success cannot erase it. Unknown signal shapes return None.
        """
        ...

    def stderr_usage_limit_diagnostic(self, line: str) -> dict[str, Any] | None:
        """Recognize a harness-owned stderr quota diagnostic, or None when unverified."""
        ...

    def workflow_stderr_blocks_availability_failure(self, diagnostic: str) -> bool:
        """Veto automatic availability fallback for an entire failed-process artifact.

        This does not assert a structured terminal failure. A backend may recognize
        unsupported plain quota forms conservatively while rejecting warnings/prose.
        """
        return False

    def normalize(self, event: dict[str, Any], acc: Accumulator) -> list[dict[str, Any]]:
        """Translate one stream event into zero or more monitor events for `<task_id>.events.jsonl`.

        Called AFTER `ingest` has folded the same event into `acc`, so it may read `acc` (e.g.
        vibe's current turn, cumulative usage). It must not mutate `acc` except for
        `acc.stream_state` keys prefixed `normalize_`. Each returned dict carries `kind` plus that
        kind's fields, and optionally `source_ts` (an ISO-8601 string, the backend's own timestamp
        for the event) which the writer lifts into the envelope. It never returns `task_started` or
        `task_finished` — the bridge writes those itself. A raise is caught and counted by the
        caller, but implementations should still be defensive: streams are untrusted input.
        """
        ...

    def continuation_reason(self, acc: Accumulator, exit_code: int | None) -> str | None:
        """Observed recoverable cutoff, or None when no certified signal exists."""
        return None

    def classify(self, acc: Accumulator, exit_code: int | None) -> Status:
        """Decide the terminal status from this backend's own signals."""
        ...

    def encode_live_message(self, text: str) -> bytes:
        """One message for a live-input run's stdin, in this CLI's own input format, newline
        included. Raises `UnsupportedCapability` on a backend without live input — the pump in
        `tasks.py` calls this so it never needs to know any backend's wire format."""
        ...

    def interactive_resume_argv(self, session_id: str, repo_path: Path) -> list[str] | None:
        """The command that resumes `session_id` in this CLI's own interactive UI, for a human
        taking a task over. `argv[0]` is the bare `binary`; the caller resolves it. Built at call
        time and never stored. None when no safe command exists — see
        `interactive_session_id_ok`."""
        ...


# Every measured interactive resume takes the session id as an *optional* option value (claude
# `--resume [value]`, vibe `--resume [SESSION_ID]`, agy `--conversation <id>`) or a positional
# (codex `resume [SESSION_ID]`), so an id beginning with `-` would be parsed as an option. Session
# ids come out of the agent's own stream, which is untrusted, so anything outside the shapes the
# CLIs actually mint is refused.
_INTERACTIVE_SESSION_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}")


def interactive_session_id_ok(session_id: str | None, repo_path: Path) -> bool:
    return (
        isinstance(session_id, str)
        and _INTERACTIVE_SESSION_ID_RE.fullmatch(session_id) is not None
        and Path(repo_path).is_absolute()
    )


def check_freedom(freedom: str) -> Freedom:
    if freedom not in FREEDOMS:
        raise UnsupportedCapability(
            f"unknown freedom {freedom!r}; expected one of {list(FREEDOMS)}"
        )
    return freedom  # type: ignore[return-value]


def reject_turn_cap(backend: Backend, max_turns: int | None) -> None:
    """Fail loudly when a turn cap was asked for and cannot be delivered."""
    if max_turns is not None and not backend.capabilities.supports_turn_cap:
        raise UnsupportedCapability(
            f"the {backend.name} CLI has no turn cap, so max_turns={max_turns} cannot be honoured; "
            "omit it, or use a backend whose capabilities report supports_turn_cap"
        )


def reject_model(backend: Backend, model: str | None) -> None:
    """Fail loudly when a model was asked for and this backend has no flag to choose one."""
    if model is not None and not backend.capabilities.supports_model_selection:
        raise UnsupportedCapability(
            f"the {backend.name} CLI has no model selection flag, so model={model!r} cannot be "
            "honoured; omit it, or use a backend whose capabilities report "
            "supports_model_selection"
        )


def check_reasoning_effort(backend: Backend, reasoning_effort: str | None) -> None:
    """Fail loudly when an effort was asked for and this backend cannot honour it verbatim."""
    if reasoning_effort is None:
        return
    if reasoning_effort not in EFFORTS:
        raise UnsupportedCapability(
            f"unknown reasoning_effort {reasoning_effort!r}; expected one of {list(EFFORTS)}"
        )
    effort_caps = backend.capabilities.reasoning_effort
    if not effort_caps.accepts_parameter:
        raise UnsupportedCapability(
            f"the {backend.name} CLI has no reasoning effort control, so "
            f"reasoning_effort={reasoning_effort!r} cannot be honoured; omit it, or use a backend "
            "whose capabilities report reasoning_effort.accepts_parameter"
        )
    if reasoning_effort not in effort_caps.levels:
        raise UnsupportedCapability(
            f"the {backend.name} CLI does not accept reasoning_effort={reasoning_effort!r}; "
            f"it accepts {list(effort_caps.levels)}"
        )


def check_network(backend: Backend, freedom: Freedom, network: bool | None) -> None:
    """Fail loudly when a network request cannot be honoured at this freedom.

    Driven purely by the declared `NetworkControl` tuples, so it holds no backend-specific
    branch — codex's non-rectangular support lives in its declaration, not here. `None` always
    passes: it is the historical default, and keeping every pre-existing caller byte-for-byte
    unchanged is the point of the parameter. The request governs harflow_mcp's own network
    barrier only, never reachability, which is why a backend with no barrier at all refuses
    `False` rather than pretending to impose one.
    """
    check_freedom(freedom)
    if network is None:
        return
    control = backend.capabilities.network_control
    if network and freedom not in control.can_enable:
        raise UnsupportedCapability(
            f"the {backend.name} backend cannot enable network at freedom {freedom!r}; "
            f"network=True is accepted only at {list(control.can_enable)} — see its "
            f"network_control caveats for why"
        )
    if network is False and freedom not in control.can_block:
        if not control.can_block:
            raise UnsupportedCapability(
                f"the {backend.name} backend has no network barrier of its own to raise, so "
                f"network=False cannot be honoured; omit it, or use a backend whose "
                f"capabilities report network_control.can_block"
            )
        raise UnsupportedCapability(
            f"the {backend.name} backend cannot block network at freedom {freedom!r}; "
            f"network=False is accepted only at {list(control.can_block)}"
        )


# --- Nested-dispatch caps -----------------------------------------------------------------------
#
# A task dispatched *through harflow_mcp itself* (an agent running under one harflow_mcp task that
# then calls `start_task`/`resume_task` again) can, without a cap, ask for something stronger than
# the parent task it is running inside of — e.g. a `read_only` claude run spawning an `unrestricted`
# codex run. `nested_enforcement_violation` compares a parent's recorded `Enforcement` against a
# proposed child's and says whether the child would be weaker (in the sense of "the parent's own
# restrictions would not hold for it"). This is advisory, not a sandbox: it is only ever checked
# when a caller is *detected* (see `lineage.detect_caller`), detection is itself best-effort, and
# nothing stops an agent from dispatching outside harflow_mcp entirely. Every message this module
# raises says so.

POLICY_FIELDS: tuple[str, ...] = (
    "os_enforced",
    "writes_confined",
    "commit_push_blocked",
    "direct_commit_commands_denied",
    "publish_attempts_allowed_by_polybridge",
    "network_access",
    "writable_roots",
)
"""Every `Enforcement` field this cap compares. A parent record missing any of these (a legacy
record predating the field, or an `enforcement=None` record from before A1) cannot be compared at
all — see `nested_enforcement_violation`'s `parent_enforcement_unrecorded` rule."""

# Ranked so "child rank >= parent rank" means "at least as strict". An unrecognised value on either
# side is treated conservatively: a parent with a value this table does not know is assumed as
# strict as possible (rank 2, "blocked"-equivalent) so an unfamiliar parent claim can never be
# under-compared away; a child with an unrecognised value is assumed as lax as possible (rank 0,
# "unrestricted"-equivalent) so it can never slip past the check by reporting nonsense.
NETWORK_STRICTNESS: dict[str, int] = {
    "blocked": 2,
    "enabled": 1,
    "not_controlled": 1,
    "unrestricted": 0,
}

# True on the parent means "restricted"; a child that is not is weaker. Checked in this exact
# order — the first field that fails is the one reported, so a caller only ever sees one cause.
_MONOTONIC_TRUE_RESTRICTS: tuple[str, ...] = (
    "os_enforced",
    "writes_confined",
    "commit_push_blocked",
    "direct_commit_commands_denied",
)


def _field(obj: Enforcement | Mapping[str, Any], name: str) -> Any:
    """Read one enforcement field whether `obj` is a live `Enforcement` or a plain dict (e.g. a
    `TaskRecord.enforcement` loaded back off disk)."""
    if isinstance(obj, Mapping):
        return obj.get(name)
    return getattr(obj, name, None)


def _violation(rule: str, parent_value: Any, child_value: Any) -> tuple[str, str]:
    return (
        rule,
        f"nested dispatch refused: the child task would be weaker than its parent on {rule} "
        f"(parent={parent_value!r}, child={child_value!r}); this cap is best-effort",
    )


def nested_enforcement_violation(
    parent: Mapping[str, Any],
    child: Enforcement | Mapping[str, Any],
    *,
    parent_backend: str,
    child_backend: str,
    parent_repo: str,
    child_repo: str,
) -> tuple[str, str] | None:
    """The first way `child` would be weaker than `parent`, or None if it never is.

    Checked in this fixed order — `parent_enforcement_unrecorded`, `os_enforced`,
    `writes_confined`, `commit_push_blocked`, `direct_commit_commands_denied`,
    `publish_attempts_allowed_by_polybridge`, `network_access`, `backend`, `writable_roots`,
    `repo` — so only ever the first violated rule is reported. `extra_allowed_commands` and
    `caller_approval_tools` are narrowing-only and are checked before these. `freedom`, `mechanism` and
    `caveats` are deliberately never compared: they describe *how* a level is achieved, not how
    strict it is, and two different mechanisms can enforce the same strength.

    Pure on its inputs except for `os.path.realpath` on the two repo paths (needed to resolve a
    symlink pointing outside the parent's repo).
    """
    parent = dict(parent or {})
    legacy = "publish_attempts_allowed_by_polybridge"
    canonical = "publish_attempts_allowed"
    for label, value in (("parent", parent), ("child", child)):
        if isinstance(value, Mapping):
            if canonical in value and legacy in value and value[canonical] != value[legacy]:
                return _violation(canonical, f"{label} contradictory aliases", "cannot compare")
            if canonical in value and type(value[canonical]) is not bool:
                return _violation(canonical, f"{label} invalid intent", "cannot compare")
            if legacy in value and type(value[legacy]) is not bool:
                return _violation(canonical, f"{label} invalid intent", "cannot compare")
    if canonical in parent:
        parent[legacy] = parent[canonical]
    missing = [f for f in POLICY_FIELDS if f not in parent]
    if missing:
        return _violation("parent_enforcement_unrecorded", missing[0], "n/a (parent unrecorded)")

    # Typed additional scopes can narrow from a directory to a descendant/file.
    # Keep them separate from symbolic base roots and the trusted scratch grant.
    from ..extra_paths import restore_paths
    try:
        parent_extra = restore_paths(parent.get("extra_writable_paths", []))
        child_extra = restore_paths(list(_field(child, "extra_writable_paths") or ()))
    except (ValueError, TypeError, AttributeError):
        return _violation("extra_writable_paths", "invalid recorded scope", "cannot compare")
    owner_repo = Path(os.path.realpath(parent_repo))
    for grant in child_extra:
        target = Path(grant.path)
        if target.is_relative_to(owner_repo):
            continue
        if not any((owner.kind == "directory" and target.is_relative_to(Path(owner.path))) or
                   (owner.kind == "file" and grant.kind == "file" and grant.path == owner.path)
                   for owner in parent_extra):
            return _violation("extra_writable_paths", "owner scope", grant.path)

    # Write and Edit approvals are narrowing-only: a child may offer them only if its parent did.
    child_writes = {t for t in (_field(child, "caller_approval_tools") or ()) if t in {"Write", "Edit"}}
    if child_writes - set(parent.get("caller_approval_tools") or ()):
        return _violation("caller_approval_tools", list(parent.get("caller_approval_tools") or ()), sorted(child_writes))

    # Command grants are narrowing-only: exact strings the caller holds, unless the caller is unrestricted.
    child_commands = tuple(_field(child, "extra_allowed_commands") or ())
    if child_commands and parent.get("freedom") != "unrestricted" and not set(child_commands) <= set(parent.get("extra_allowed_commands") or ()):
        return _violation("extra_allowed_commands", list(parent.get("extra_allowed_commands") or ()), list(child_commands))

    for field_name in _MONOTONIC_TRUE_RESTRICTS:
        parent_value = parent[field_name]
        child_value = _field(child, field_name)
        if parent_value and not child_value:
            return _violation(field_name, parent_value, child_value)

    # publish_attempts_allowed_by_polybridge runs the other way: True means "permitted to try
    # publishing", so a parent that was *not* authorized (False) must not have a child that is.
    parent_publish = parent["publish_attempts_allowed_by_polybridge"]
    child_publish = _field(child, "publish_attempts_allowed")
    if child_publish is None:
        child_publish = _field(child, "publish_attempts_allowed_by_polybridge")
    if type(child_publish) is not bool:
        return _violation(canonical, "child intent missing", "cannot compare")
    if not parent_publish and child_publish:
        return _violation(
            "publish_attempts_allowed_by_polybridge", parent_publish, child_publish
        )

    parent_network = parent["network_access"]
    child_network = _field(child, "network_access")
    parent_rank = NETWORK_STRICTNESS.get(parent_network, 2)
    child_rank = NETWORK_STRICTNESS.get(child_network, 0)
    if child_rank < parent_rank:
        return _violation("network_access", parent_network, child_network)

    # The remaining three rules only bite when the parent itself is confined at all — an
    # unconfined parent (claude/opencode/vibe at any freedom, codex at `unrestricted`) imposes no
    # backend, root, or repo constraint on what it spawns.
    if parent["writes_confined"]:
        if child_backend != parent_backend:
            return _violation("backend", parent_backend, child_backend)

        parent_roots = set(parent["writable_roots"]) - {grant.path for grant in parent_extra}
        child_roots = set(_field(child, "writable_roots") or ()) - {grant.path for grant in child_extra}
        if not child_roots <= parent_roots:
            return _violation(
                "writable_roots", parent["writable_roots"], _field(child, "writable_roots")
            )

        parent_real = os.path.realpath(parent_repo)
        child_real = os.path.realpath(child_repo)
        if child_real != parent_real and os.path.commonpath([parent_real, child_real]) != parent_real:
            return _violation("repo", parent_repo, child_repo)

    return None


def check_nested_enforcement(
    parent: Mapping[str, Any],
    child: Enforcement | Mapping[str, Any],
    *,
    parent_backend: str,
    child_backend: str,
    parent_repo: str,
    child_repo: str,
) -> None:
    """Raise `NestedDispatchRefused` for the first way `child` would be weaker than `parent`."""
    violation = nested_enforcement_violation(
        parent,
        child,
        parent_backend=parent_backend,
        child_backend=child_backend,
        parent_repo=parent_repo,
        child_repo=child_repo,
    )
    if violation is not None:
        rule, message = violation
        raise NestedDispatchRefused(message, rule=rule)


def check_nested_depth(parent_depth: int, max_depth: int) -> None:
    """Raise `NestedDispatchRefused` when the child's depth (`parent_depth + 1`) would exceed the
    parent's own depth budget. Kept separate from `check_nested_enforcement`: depth is a property
    of the dispatch chain, not of either task's `Enforcement`."""
    child_depth = parent_depth + 1
    if child_depth > max_depth:
        raise NestedDispatchRefused(
            "nested dispatch refused: it would exceed the dispatch chain's depth budget "
            f"(parent depth={parent_depth!r}, child depth={child_depth!r} > "
            f"max_depth={max_depth!r}); this cap is best-effort",
            rule="depth",
        )
