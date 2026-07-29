"""Backend registry: name -> implementation, plus what is actually installed."""

from __future__ import annotations

import logging
import shutil
import subprocess
from typing import Any

from .base import (
    DEFAULT_FREEDOM,
    FREEDOMS,
    Accumulator,
    Backend,
    Capabilities,
    Enforcement,
    Freedom,
    Status,
    UnsupportedCapability,
    check_freedom,
    reject_turn_cap,
)
from .claude import ClaudeBackend
from .codex import CodexBackend

log = logging.getLogger(__name__)

BACKENDS: dict[str, Backend] = {
    ClaudeBackend.name: ClaudeBackend(),
    CodexBackend.name: CodexBackend(),
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


__all__ = [
    "BACKENDS",
    "DEFAULT_BACKEND",
    "DEFAULT_FREEDOM",
    "FREEDOMS",
    "Accumulator",
    "Backend",
    "Capabilities",
    "Enforcement",
    "Freedom",
    "Status",
    "UnknownBackend",
    "UnsupportedCapability",
    "check_freedom",
    "describe",
    "describe_all",
    "get",
    "is_installed",
    "reject_turn_cap",
    "version",
]
