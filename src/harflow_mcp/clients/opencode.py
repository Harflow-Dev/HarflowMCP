"""opencode — `mcp add` overwrites, so the default policy in `CliClient` is enough.

Measured (opencode 1.18.3): re-adding an existing name replaced the entry and preserved JSONC
comments in `~/.config/opencode/opencode.jsonc`. It stores the entry in its own shape — `type:
"local"`, `command` as an array, and the environment under `environment` rather than `env` — which
is exactly the reason to let it write its own config rather than doing it here.

It has no `mcp remove`, and `mcp list` is human-readable only (and connects to each server to report
its status), so neither can serve inspect or remove. `inspect` reads the config files itself, through
the read-only tokenizer in `jsonc.py`; `remove` only says what to delete by hand.

Which file `add` writes depends on what exists (measured, opencode 1.18.32, sandboxed
`XDG_CONFIG_HOME`): `opencode.jsonc` in an empty directory, but `opencode.json` when that exists, and
also when only the legacy `config.json` does. So all three candidates are read. Which of them wins
when more than one carries our entry is not measured, so an entry that differs between files reports
no single command rather than guessing a precedence.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import jsonc
from .base import (
    CliClient,
    Inspection,
    Registration,
    Result,
    Runner,
    SetupError,
    display_command,
)

CANDIDATES = ("config.json", "opencode.json", "opencode.jsonc")


def config_dir() -> Path:
    """`$XDG_CONFIG_HOME/opencode`, else `~/.config/opencode`. A relative XDG value is ignored, per
    the XDG spec."""
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg and os.path.isabs(xdg) else Path.home() / ".config"
    return base / "opencode"


def entries_for(key: str, directory: Path) -> tuple[list[Path], list[tuple[Path, Any]]]:
    """(files read, (file, entry) for each file carrying `key`). Raises SetupError on a bad file."""
    read: list[Path] = []
    found: list[tuple[Path, Any]] = []
    for name in CANDIDATES:
        path = directory / name
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        read.append(path)
        if not raw.strip():
            continue
        try:
            config = jsonc.loads(raw)
        except ValueError as exc:
            raise SetupError(f"{path} could not be parsed ({exc})") from None
        if not isinstance(config, dict):
            raise SetupError(f"{path} does not contain a JSON object")
        servers = config.get("mcp")
        if servers is None:
            servers = {}
        if not isinstance(servers, dict):
            raise SetupError(f"'mcp' in {path} is not a JSON object")
        if key in servers:
            found.append((path, servers[key]))
    return read, found


def _launch(entry: Any) -> tuple[tuple[str, ...] | None, str | None]:
    """(argv, PATH) an opencode entry launches. A non-local entry launches no command."""
    if not isinstance(entry, dict) or entry.get("type", "local") != "local":
        return None, None
    command = entry.get("command")
    if isinstance(command, list) and command and all(isinstance(part, str) for part in command):
        argv: tuple[str, ...] | None = tuple(command)
    else:
        argv = None
    environment = entry.get("environment")
    path_env = environment.get("PATH") if isinstance(environment, dict) else None
    return argv, path_env if isinstance(path_env, str) else None


@dataclass(frozen=True)
class OpencodeClient(CliClient):
    key: str = "opencode"
    label: str = "Opencode"
    binary: str = "opencode"
    config_hint: str = "~/.config/opencode/opencode.jsonc"

    def env_flag(self, registration: Registration) -> list[str]:
        return ["--env", f"PATH={registration.path_env}"]

    def inspect(self, key: str, run: Runner) -> Inspection:
        """Read-only; `run` is unused. A parse failure raises and is reported as an error."""
        directory = config_dir()
        read, found = entries_for(key, directory)
        if not found:
            where = ", ".join(map(str, read)) if read else f"no config in {directory}"
            return Inspection(self.key, False, notes=(f"read {where}",))

        notes = tuple(f"entry in {path}" for path, _ in found)
        launches = {_launch(entry) for _, entry in found}
        if len(launches) > 1:
            return Inspection(
                self.key, True, notes=(*notes, "the entries differ between files")
            )
        argv, path_env = launches.pop()
        return Inspection(
            self.key,
            True,
            command=display_command(argv),
            path_env=path_env,
            notes=notes,
            argv=argv,
        )

    def remove(self, key: str, run: Runner) -> Result:
        """opencode has no `mcp remove`, so this says what to delete rather than deleting it.

        `skipped`, not `removed`: nothing was changed. Absence is still reported as `not_installed`,
        since there is then nothing for the user to do either.
        """
        _, found = entries_for(key, config_dir())
        if not found:
            return Result(self.key, "not_installed", "nothing registered")
        files = ", ".join(str(path) for path, _ in found)
        return Result(
            self.key,
            "skipped",
            f"opencode has no `mcp remove`; remove `mcp.{key}` manually from {files}",
        )
