"""Cursor: register Harflow in the global `~/.cursor/mcp.json`.

There is no `mcp add`. `agent mcp` only lists, enables, disables, and logs in.
This client merges one server entry the way the desktop client does. It refuses a
file it cannot round-trip with `json.loads`: Cursor accepts JSONC, and `mcp list`
does not rewrite it, so stripping comments and writing JSON back would destroy
the file.

`agent mcp enable` approves the server for the current working directory only.
Setup runs it from the home directory, so a project that still says the server
needs approval has to run `agent mcp enable <name>` from that project. Disable
does not delete the mcp.json entry, so uninstall edits the file.

The Cursor editor being signed in does not sign in this CLI. Headless use needs
`agent login` or `CURSOR_API_KEY`, checked with `agent status`.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from ..cursor_cli import find_executable
from .base import Availability, Inspection, Registration, Result, Runner, SetupError, entry_inspection
from .desktop import back_up, load_config, merge_entry, read_raw, server_entry, servers_of, write_config


def cursor_config_path() -> Path:
    return Path.home() / ".cursor" / "mcp.json"


@dataclass(frozen=True)
class CursorClient:
    """Edits only the global MCP file. A missing CLI is a skip, not a created directory."""

    key: str = "cursor"
    label: str = "Cursor"
    post_apply_note: str = (
        "A new Cursor session reads the global ~/.cursor/mcp.json. "
        "Setup approves the server only for the home directory; a project that still "
        "says it needs approval needs `agent mcp enable` for this server, run from that project. "
        "Signing into the Cursor editor does not sign in the agent CLI: run `agent login` "
        "or set CURSOR_API_KEY, then confirm `agent status` reports an access token. "
        "That check uses the current HOME."
    )
    config_path: Path | None = None

    def resolve_path(self) -> Path:
        path = self.config_path or cursor_config_path()
        return Path(os.path.realpath(path)) if path.is_symlink() else path

    def availability(self) -> Availability:
        found = find_executable()
        if found is None:
            return Availability(
                False,
                reason="Cursor Agent CLI was not found (`cursor-agent` or `agent` with a Cursor version line)",
            )
        return Availability(True, where=found)

    def preview(self, registration: Registration) -> Result:
        path = self.resolve_path()
        merged = merge_entry(load_config(path), registration.key, server_entry(registration))
        return Result(
            self.key,
            "previewed",
            f"would write {path}",
            diagnostics=tuple(json.dumps(merged, indent=2).splitlines()),
        )

    def apply(self, registration: Registration, run: Runner) -> Result:
        binary = find_executable()
        if binary is None:
            return Result(
                self.key,
                "failed",
                "Cursor Agent CLI was not found; nothing was written",
            )
        path = self.resolve_path()
        before = read_raw(path)
        merged = merge_entry(load_config(path), registration.key, server_entry(registration))
        if read_raw(path) != before:
            return Result(
                self.key,
                "failed",
                f"{path} changed while it was being edited — nothing was written",
            )
        backup = back_up(path)
        write_config(path, merged)
        enabled = run([binary, "mcp", "enable", registration.key])
        wrote = f"wrote {path}"
        backup_note = (f"backed up previous config to {backup}",) if backup else ()
        if not enabled.ok:
            return Result(
                self.key,
                "failed",
                f"{wrote}, but enabling the server failed",
                diagnostics=(*backup_note, enabled.tail),
            )
        return Result(self.key, "applied", wrote, diagnostics=backup_note)

    def inspect(self, key: str, run: Runner) -> Inspection:
        path = self.resolve_path()
        servers = servers_of(load_config(path))
        if key not in servers:
            return Inspection(self.key, False, notes=(f"read {path}",))
        return entry_inspection(self.key, servers[key], str(path))

    def remove(self, key: str, run: Runner) -> Result:
        path = self.resolve_path()
        before = read_raw(path)
        config = load_config(path)
        servers = servers_of(config)
        if key not in servers:
            return Result(self.key, "not_installed", f"no {key} entry in {path}")
        updated = {**config, "mcpServers": {name: value for name, value in servers.items() if name != key}}
        if read_raw(path) != before:
            return Result(
                self.key,
                "failed",
                f"{path} changed while it was being edited — nothing was written",
            )
        backup = back_up(path)
        write_config(path, updated)
        return Result(
            self.key,
            "removed",
            f"removed {key} from {path}",
            diagnostics=(f"backed up previous config to {backup}",) if backup else (),
        )
