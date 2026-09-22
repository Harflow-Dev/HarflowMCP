"""The contract every coding agent must satisfy, and nothing beyond it.

Backends differ in ways that cannot be papered over: only Claude has a turn cap, only Claude lets us
choose the session id, only Codex has an OS sandbox, and Codex alone reports no dollar cost. Rather
than pretending otherwise, each backend declares its `Capabilities` and reports the `Enforcement` its
flags actually deliver. Everything outside this package works on the normalised view and never
branches on a backend's name.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, NamedTuple, Protocol, runtime_checkable

Freedom = Literal["read_only", "write_in_repo", "publish", "unrestricted"]

FREEDOMS: tuple[Freedom, ...] = ("read_only", "write_in_repo", "publish", "unrestricted")
DEFAULT_FREEDOM: Freedom = "write_in_repo"

Status = Literal["running", "completed", "failed", "timed_out", "cancelled"]

# Stopping at xhigh is deliberate, not an oversight — but "every model measured accepts" is only
# true of codex (per ~/.codex/models_cache.json), where no canonical level can produce a
# model-dependent failure. It is not true of opencode: --variant support is per model there, e.g.
# ling-3.0-flash-fin accepts only low/medium/high (no xhigh), and several models declare no
# `variants` at all and silently ignore the flag — see OpencodeBackend's reasoning_effort caveat.
# Each backend's own ceiling is therefore left unreachable on purpose — claude `max`, codex
# `max`/`ultra` — so a caller cannot spend it by accident. The tiers below `low` (codex and opencode
# `minimal`, codex `none`) are unreachable too, for no reason beyond keeping one vocabulary shared by
# the three backends that accept an effort at all — vibe accepts none, so it is outside this
# vocabulary rather than a fourth member of it.
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

    stream_state: dict[str, Any] = field(default_factory=dict)
    """Backend-private scratch space for an ingest algorithm that needs memory across events (e.g.
    vibe's current-turn tracking). Lives here, per task, rather than on the backend instance:
    `BACKENDS` holds one shared instance per backend, so state on `self` would corrupt concurrent
    tasks. Never read by anything outside the backend that wrote it."""


class UnsupportedCapability(ValueError):
    """A request a backend cannot honour, which must fail rather than be silently dropped."""


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
    ) -> list[str]: ...

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
    ) -> list[str]: ...

    def assert_safe(self, argv: list[str], freedom: Freedom, network: bool | None = None) -> None:
        """Raise unless the argv still carries this backend's required guarantees.

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

    def classify(self, acc: Accumulator, exit_code: int | None) -> Status:
        """Decide the terminal status from this backend's own signals."""
        ...


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
