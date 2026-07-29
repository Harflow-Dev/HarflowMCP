"""The contract every coding agent must satisfy, and nothing beyond it.

Backends differ in ways that cannot be papered over: Codex has no turn cap and will not let us
choose a session id, Claude has no OS sandbox, and only Claude reports a dollar cost. Rather than
pretending otherwise, each backend declares its `Capabilities` and reports the `Enforcement` its
flags actually deliver. Everything outside this package works on the normalised view and never
branches on a backend's name.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, NamedTuple, Protocol, runtime_checkable

Freedom = Literal["read_only", "write_in_repo", "unrestricted"]

FREEDOMS: tuple[Freedom, ...] = ("read_only", "write_in_repo", "unrestricted")
DEFAULT_FREEDOM: Freedom = "write_in_repo"

Status = Literal["running", "completed", "failed", "timed_out", "cancelled"]


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

    def as_dict(self) -> dict[str, bool]:
        return dict(self._asdict())


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
    ) -> list[str]: ...

    def assert_safe(self, argv: list[str]) -> None:
        """Raise unless the argv still carries this backend's required guarantees."""
        ...

    def enforcement(self, freedom: Freedom) -> Enforcement: ...

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        """Fold one stream event into the normalised view."""
        ...

    def classify(self, acc: Accumulator, exit_code: int) -> Status:
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
