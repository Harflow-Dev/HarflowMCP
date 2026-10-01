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

Freedom = Literal["read_only", "write_in_repo", "publish", "unrestricted"]

FREEDOMS: tuple[Freedom, ...] = ("read_only", "write_in_repo", "publish", "unrestricted")
DEFAULT_FREEDOM: Freedom = "write_in_repo"

Status = Literal["running", "completed", "failed", "timed_out", "cancelled"]

StdinMode = Literal["devnull", "pipe"]
STDIN_DEVNULL: StdinMode = "devnull"
STDIN_PIPE: StdinMode = "pipe"


@dataclass(frozen=True)
class Invocation:
    """Everything `_spawn` needs to launch one run: the argv, how its stdin is wired, and the first
    bytes to write to it.

    Returned by the argv builders instead of a bare argv, so the stdin mode comes from the run that
    was actually built — never from the backend's name or its static capability. A live-input run
    (`stdin_mode="pipe"`) carries its prompt as `initial_input`, because a CLI reading its input as a
    stream ignores a positional prompt (measured on claude). Every other run is `"devnull"` with no
    initial input: codex and vibe block forever reading an open stdin, so DEVNULL is mandatory there.
    """

    argv: list[str]
    stdin_mode: StdinMode = STDIN_DEVNULL
    initial_input: bytes | None = None

    @property
    def live_input(self) -> bool:
        return self.stdin_mode == STDIN_PIPE


def classic_invocation_problem(invocation: Any) -> str | None:
    """Why `invocation` is not a plain devnull-stdin run, or None if it is — for backends with no
    live input. A bare argv list is refused too: a caller that skipped the Invocation could not have
    said how the process's stdin is wired."""
    if not isinstance(invocation, Invocation):
        return f"expected an Invocation, got {type(invocation).__name__}: {invocation!r}"
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
    """Whether polybridge can raise or lower its own network barrier, per freedom.

    Two freedom tuples rather than a pair of global booleans because support is non-rectangular
    on codex: `read_only` cannot enable (measured — the `sandbox_workspace_write.network_access`
    key is inert under the read-only sandbox even when set true), `workspace-write` takes either
    direction, and `unrestricted` cannot block (danger-full-access disables the sandbox, so no
    barrier remains to raise). A backend with no network-controlling mechanism at all declares
    every freedom in `can_enable` — `network=True` means "impose no barrier of your own", which
    having none to impose genuinely delivers — and nothing in `can_block`, because
    `network=False` means "impose one", which it genuinely cannot. Every claim here is about
    polybridge's own barrier only, never about reachability.
    """

    can_enable: tuple[Freedom, ...]
    """Freedoms where network=True is accepted: polybridge can lift, or need not raise, its own barrier."""

    can_block: tuple[Freedom, ...]
    """Freedoms where network=False is accepted: polybridge can impose its own barrier."""

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

    live_input_message_is_turn: bool = False
    """Whether each message written on stdin starts a turn of its own, even mid-turn. agy does
    (measured: two queued lines, EOF at the first result, three results), so a run that received N
    messages is idle only after N further results — see `tasks._note_input_written`. claude does
    not: it folds a mid-turn message into the running turn (measured, claude 2.1.281), so however
    many messages landed, exactly one further result settles the run. Default False so the
    folding semantics stay the behaviour of every backend that does not say otherwise."""

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
    """True only at freedoms where polybridge authorized an *attempt* to commit, push or open a
    PR. An authorization claim and nothing more: it does not say the mechanism prevents the
    attempt elsewhere — codex at `write_in_repo` with `network=True` can mechanically reach a
    remote while this field is false, the difference being recorded intent, not an enforced
    barrier — nor that publication will actually succeed — credentials, remote permissions,
    branch protection, hooks, an unauthenticated `gh`, and the agent's own behaviour are all
    outside polybridge's control. Deliberately not named `publishing_permitted`: that name would
    be read as a promise this field cannot make. (An earlier docstring also claimed polybridge
    "configured no publish-specific barrier of its own" here; that conjunction became untrue in
    both directions once network was requestable — true where this is false — so it was removed
    rather than caveated, per the rule that a caveat cannot repair an untrue claim.)"""

    network_access: str = "not_controlled"
    """One of "blocked" | "enabled" | "unrestricted" | "not_controlled". "not_controlled" means
    polybridge imposes nothing of its own here and the surrounding environment decides — which is
    NOT the same as "blocked"."""

    caveats: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "freedom": self.freedom,
            "mechanism": self.mechanism,
            "os_enforced": self.os_enforced,
            "writes_confined": self.writes_confined,
            "writable_roots": list(self.writable_roots),
            "commit_push_blocked": self.commit_push_blocked,
            "direct_commit_commands_denied": self.direct_commit_commands_denied,
            "publish_attempts_allowed_by_polybridge": self.publish_attempts_allowed_by_polybridge,
            "network_access": self.network_access,
            "caveats": list(self.caveats),
        }


@dataclass
class Accumulator:
    """The normalised picture of a run, filled in by whichever backend produced the stream."""

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


class UnsupportedCapability(ValueError):
    """A request a backend cannot honour, which must fail rather than be silently dropped."""


class NestedDispatchRefused(UnsupportedCapability):
    """A nested dispatch (one task spawning another via polybridge) that would be weaker than its
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


@runtime_checkable
class Backend(Protocol):
    name: str
    binary: str
    capabilities: Capabilities

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
    unchanged is the point of the parameter. The request governs polybridge's own network
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
# A task dispatched *through polybridge itself* (an agent running under one polybridge task that
# then calls `start_task`/`resume_task` again) can, without a cap, ask for something stronger than
# the parent task it is running inside of — e.g. a `read_only` claude run spawning an `unrestricted`
# codex run. `nested_enforcement_violation` compares a parent's recorded `Enforcement` against a
# proposed child's and says whether the child would be weaker (in the sense of "the parent's own
# restrictions would not hold for it"). This is advisory, not a sandbox: it is only ever checked
# when a caller is *detected* (see `lineage.detect_caller`), detection is itself best-effort, and
# nothing stops an agent from dispatching outside polybridge entirely. Every message this module
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
    `repo` — so only ever the first violated rule is reported. `freedom`, `mechanism` and
    `caveats` are deliberately never compared: they describe *how* a level is achieved, not how
    strict it is, and two different mechanisms can enforce the same strength.

    Pure on its inputs except for `os.path.realpath` on the two repo paths (needed to resolve a
    symlink pointing outside the parent's repo).
    """
    parent = parent or {}
    missing = [f for f in POLICY_FIELDS if f not in parent]
    if missing:
        return _violation("parent_enforcement_unrecorded", missing[0], "n/a (parent unrecorded)")

    for field_name in _MONOTONIC_TRUE_RESTRICTS:
        parent_value = parent[field_name]
        child_value = _field(child, field_name)
        if parent_value and not child_value:
            return _violation(field_name, parent_value, child_value)

    # publish_attempts_allowed_by_polybridge runs the other way: True means "permitted to try
    # publishing", so a parent that was *not* authorized (False) must not have a child that is.
    parent_publish = parent["publish_attempts_allowed_by_polybridge"]
    child_publish = _field(child, "publish_attempts_allowed_by_polybridge")
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

        parent_roots = set(parent["writable_roots"])
        child_roots = set(_field(child, "writable_roots") or ())
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
