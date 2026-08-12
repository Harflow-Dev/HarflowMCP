"""opencode — `mcp add` overwrites, so the default policy in `CliClient` is enough.

Measured (opencode 1.18.3): re-adding an existing name replaced the entry and preserved JSONC
comments in `~/.config/opencode/opencode.jsonc`. It stores the entry in its own shape — `type:
"local"`, `command` as an array, and the environment under `environment` rather than `env` — which
is exactly the reason to let it write its own config rather than doing it here.
"""

from __future__ import annotations

from dataclasses import dataclass

from .base import CliClient, Registration


@dataclass(frozen=True)
class OpencodeClient(CliClient):
    key: str = "opencode"
    label: str = "opencode"
    binary: str = "opencode"
    config_hint: str = "~/.config/opencode/opencode.jsonc"

    def env_flag(self, registration: Registration) -> list[str]:
        return ["--env", f"PATH={registration.path_env}"]
