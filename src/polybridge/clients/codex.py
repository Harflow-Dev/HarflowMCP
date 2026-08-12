"""Codex — `mcp add` overwrites, so the default policy in `CliClient` is enough.

Measured (codex-cli 0.145.0): re-adding an existing name replaced the entry and left comments and
formatting elsewhere in `~/.codex/config.toml` intact. That preservation is a property of the
version measured, not a guarantee we can make on Codex's behalf.
"""

from __future__ import annotations

from dataclasses import dataclass

from .base import CliClient, Registration


@dataclass(frozen=True)
class CodexClient(CliClient):
    key: str = "codex"
    label: str = "Codex"
    binary: str = "codex"
    config_hint: str = "~/.codex/config.toml"

    def env_flag(self, registration: Registration) -> list[str]:
        return ["--env", f"PATH={registration.path_env}"]
