"""Backend registry: name -> implementation, plus what is actually installed."""

from __future__ import annotations

import logging
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Any

from .base import (
    DEFAULT_FREEDOM,
    EFFORTS,
    FREEDOMS,
    NETWORK_STRICTNESS,
    POLICY_FIELDS,
    Accumulator,
    Backend,
    Capabilities,
    Enforcement,
    Freedom,
    Invocation,
    NestedDispatchRefused,
    NetworkControl,
    ReasoningEffort,
    STDIN_DEVNULL,
    STDIN_PIPE,
    Status,
    UnsupportedCapability,
    check_freedom,
    classic_invocation_problem,
    check_nested_depth,
    check_nested_enforcement,
    check_network,
    check_reasoning_effort,
    interactive_session_id_ok,
    nested_enforcement_violation,
    reject_model,
    reject_turn_cap,
)
from .antigravity import AntigravityBackend
from .claude import ClaudeBackend
from .codex import CodexBackend
from .opencode import OpencodeBackend
from .vibe import VibeBackend

log = logging.getLogger(__name__)

BACKENDS: dict[str, Backend] = {
    ClaudeBackend.name: ClaudeBackend(),
    CodexBackend.name: CodexBackend(),
    OpencodeBackend.name: OpencodeBackend(),
    VibeBackend.name: VibeBackend(),
    AntigravityBackend.name: AntigravityBackend(),
}

DEFAULT_BACKEND = ClaudeBackend.name


class UnknownBackend(ValueError):
    """A backend name that is not registered."""


def get(name: str) -> Backend:
    try:
        return BACKENDS[name]
    except KeyError:
        raise UnknownBackend(
            f"unknown backend {name!r}; expected one of {sorted(BACKENDS)}"
        ) from None


def is_installed(backend: Backend) -> bool:
    return shutil.which(backend.binary) is not None


def version(backend: Backend) -> str | None:
    """The backend CLI's version, or None if it is absent or unresponsive."""
    if not is_installed(backend):
        return None
    try:
        result = subprocess.run(
            [backend.binary, "--version"], capture_output=True, text=True, check=False, timeout=15
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip().splitlines()[0] if result.stdout.strip() else None


def describe(backend: Backend) -> dict[str, Any]:
    """Everything a caller needs to choose a backend deliberately."""
    installed = is_installed(backend)
    return {
        "backend": backend.name,
        "binary": backend.binary,
        "installed": installed,
        "version": version(backend) if installed else None,
        "capabilities": backend.capabilities.as_dict(),
        "freedoms": {
            freedom: backend.enforcement(freedom).as_dict()  # type: ignore[arg-type]
            for freedom in FREEDOMS
        },
    }


def describe_all() -> list[dict[str, Any]]:
    return [describe(backend) for backend in BACKENDS.values()]


def resume_command(backend_name: str, session_id: str | None, repo_path: str | Path) -> str | None:
    """The command a human pastes into their own terminal to resume `session_id`: `cd <repo> &&
    <the backend's interactive resume argv>`. Computed fresh for a snapshot (live or recovered),
    never stored on a record — see `Task.snapshot`/`store.snapshot`.

    Assumes the string is pasted into a POSIX shell (bash/zsh); it is not `cmd.exe`/PowerShell
    syntax. `argv[0]` is the bare binary name, exactly as `interactive_resume_argv` returns it — no
    PATH lookup here, unlike `takeover.py`'s `_interactive_command`, since this string is meant to
    be pasted into a shell that will do its own lookup.

    None whenever no safe command exists: an unknown backend; a NUL byte in `session_id` or
    `repo_path` (checked here rather than trusting the resulting argv alone — claude's and codex's
    interactive argv never carry the repo path as an operand at all, so a NUL-bearing `repo_path`
    would otherwise slip straight past that check and into the `cd` operand); `
    interactive_resume_argv` itself returning None (an unsafe/option-like id, a relative repo, or a
    backend with no safe interactive command); or a NUL surviving into the argv it returned.

    Never raises: this feeds directly into a snapshot, and bookkeeping must never change an
    outcome, so any unexpected failure here is swallowed into `None` rather than allowed to break
    the snapshot that carries it.
    """
    try:
        repo_str = str(repo_path)
        if (session_id is not None and "\0" in session_id) or "\0" in repo_str:
            return None
        backend = get(backend_name)
        argv = backend.interactive_resume_argv(session_id, Path(repo_path))  # type: ignore[arg-type]
        if argv is None or any("\0" in part for part in argv):
            return None
        return f"cd {shlex.quote(repo_str)} && {shlex.join(argv)}"
    except Exception:
        return None


__all__ = [
    "BACKENDS",
    "DEFAULT_BACKEND",
    "DEFAULT_FREEDOM",
    "EFFORTS",
    "FREEDOMS",
    "NETWORK_STRICTNESS",
    "POLICY_FIELDS",
    "Accumulator",
    "Backend",
    "Capabilities",
    "Enforcement",
    "Freedom",
    "Invocation",
    "NestedDispatchRefused",
    "NetworkControl",
    "ReasoningEffort",
    "STDIN_DEVNULL",
    "STDIN_PIPE",
    "Status",
    "UnknownBackend",
    "UnsupportedCapability",
    "check_freedom",
    "classic_invocation_problem",
    "check_nested_depth",
    "check_nested_enforcement",
    "check_network",
    "check_reasoning_effort",
    "describe",
    "describe_all",
    "get",
    "interactive_session_id_ok",
    "is_installed",
    "nested_enforcement_violation",
    "reject_model",
    "reject_turn_cap",
    "resume_command",
    "version",
]
