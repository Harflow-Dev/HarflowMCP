"""Antigravity — `mcp add` overwrites, so the default policy in `CliClient` is enough.

Measured (agy 1.2.14, sandboxed with a temp `HOME`; its config is `~/.gemini/config/mcp_config.json`,
plain JSON `{"mcpServers": {"<name>": {"command", "args"?, "env", "disabled"}}}`):

    agy mcp add --env K=V <name> -- <cmd>  -> "Added MCP server \\"<name>\\" (stdio)"   exit 0
    agy mcp add <same name, new or old>    -> same message, and the entry is replaced exit 0
    agy mcp remove <existing>              -> "Removed MCP server \\"<name>\\""         exit 0
    agy mcp remove <absent>                -> "Error: MCP server \\"<name>\\" not found" exit 1

Re-adding an identical entry and re-adding with different settings both exit 0 and replace the
stored entry — like codex, unlike Claude Code — so `CliClient`'s default overwrite policy reports
the truth and no `apply` override is needed. `agy mcp list` prints a human table, not JSON (measured),
so `inspect` reads the JSON file instead — the same shape `agy mcp add` itself writes.

**Flags must come before the name** (agy's own help says so), which is why `add_argv` is overridden
outright rather than assembling the default `CliClient` shape.

Each remove outcome requires a *complete line* of the output, the name delimited by its quotes, per
CLAUDE.md's narrow-signature rule: `Removed MCP server "polybridge"` must not match a longer name
that merely contains ours, and an exit-0 wording we do not recognise is `unknown`, never `removed`.
"""

from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from pathlib import Path

from .base import (
    CLI_TIMEOUT_SECONDS,
    CliClient,
    Inspection,
    Registration,
    Result,
    RunResult,
    Runner,
    entry_inspection,
)


def config_path() -> Path:
    """`~/.gemini/config/mcp_config.json`. agy honours no config-directory override (measured), so
    the sandbox for a test is a temp `HOME`, not an environment variable."""
    return Path.home() / ".gemini" / "config" / "mcp_config.json"


def _whole_line(result: RunResult, expected_lower: str) -> bool:
    """True if some complete line of `result.output`, stripped and lower-cased, equals
    `expected_lower` exactly.

    A substring check would also match a sentence that merely contains the expected wording — and,
    worse, a *different server's* name that contains ours. The quotes around the name in both
    measured signatures are what keep a collision like `"not-polybridge"` from reading as ours.
    """
    return any(line.strip().lower() == expected_lower for line in result.output.splitlines())


def says_removed(result: RunResult, key: str) -> bool:
    """Measured, a whole line, punctuation included: `Removed MCP server "<key>"`, exit 0."""
    return result.ok and _whole_line(result, f'removed mcp server "{key}"')


def says_not_found(result: RunResult, key: str) -> bool:
    """Measured, a whole line: `Error: MCP server "<key>" not found` — the exit-1 absence signature."""
    return result.returncode == 1 and _whole_line(result, f'error: mcp server "{key}" not found')


@dataclass(frozen=True)
class AntigravityClient(CliClient):
    key: str = "antigravity"
    label: str = "Google Antigravity CLI"
    binary: str = "agy"
    config_hint: str = "~/.gemini/config/mcp_config.json"

    def add_argv(self, registration: Registration) -> list[str]:
        # Flags precede the name (agy's own requirement, measured), so the default CliClient shape
        # — name first, then flags — cannot be reused.
        return [
            self.binary,
            "mcp",
            "add",
            *self.env_flag(registration),
            registration.key,
            "--",
            registration.command,
        ]

    def env_flag(self, registration: Registration) -> list[str]:
        return ["--env", f"PATH={registration.path_env}"]

    def remove_argv(self, key: str) -> list[str]:
        return [self.binary, "mcp", "remove", key]

    def inspect(self, key: str, run: Runner) -> Inspection:
        """Read-only; `run` is unused — `agy mcp list` is a human table (measured), not JSON."""
        path = config_path()
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return Inspection(self.key, False, notes=(f"no config at {path}",))
        except OSError as exc:
            return Inspection(self.key, None, error=f"{path} could not be read ({exc})")
        try:
            config = json.loads(raw)
        except ValueError as exc:
            return Inspection(self.key, None, error=f"{path} is not valid JSON ({exc})")
        if not isinstance(config, dict):
            return Inspection(self.key, None, error=f"{path} does not contain a JSON object")
        servers = config.get("mcpServers")
        if servers is None:
            return Inspection(self.key, False, notes=(f"read {path} (no 'mcpServers' object)",))
        if not isinstance(servers, dict):
            return Inspection(self.key, None, error=f"'mcpServers' in {path} is not a JSON object")
        if key not in servers:
            return Inspection(self.key, False, notes=(f"read {path}",))
        return entry_inspection(self.key, servers[key], str(path))

    def remove(self, key: str, run: Runner) -> Result:
        removed = run(self.remove_argv(key))
        steps = (shlex.join(removed.argv),)
        if removed.timed_out:
            return Result(
                self.key,
                "unknown",
                f"timed out after {CLI_TIMEOUT_SECONDS:.0f}s; the entry may or may not still be there",
                steps=steps,
            )
        if removed.ok:
            if says_removed(removed, key):
                return Result(
                    self.key,
                    "removed",
                    f"remove command succeeded ({self.config_hint})",
                    steps=steps,
                )
            return Result(
                self.key,
                "unknown",
                "remove exited 0 but did not report a recognised outcome; the entry may or may not "
                "still be there",
                steps=steps,
                diagnostics=(removed.tail,) if removed.tail else (),
            )
        if says_not_found(removed, key):
            return Result(self.key, "not_installed", "nothing registered", steps=steps)
        return Result(
            self.key,
            "failed",
            f"remove command failed (exit {removed.returncode})",
            steps=steps,
            diagnostics=(removed.tail,) if removed.tail else (),
        )
