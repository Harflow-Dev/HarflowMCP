"""Codex — `mcp add` overwrites, so the default policy in `CliClient` is enough.

Measured (codex-cli 0.145.0): re-adding an existing name replaced the entry and left comments and
formatting elsewhere in `~/.codex/config.toml` intact. That preservation is a property of the
version measured, not a guarantee we can make on Codex's behalf.

Measured (codex-cli 0.154.0, sandboxed `CODEX_HOME`), for inspect and remove:

    codex mcp list --json          -> `[]`, or a JSON array of entries with `name` and a
                                      `transport` of `{type, command, args, env, …}`     exit 0
    codex mcp remove <existing>    -> "Removed global MCP server '<name>'."             exit 0
    codex mcp remove <absent>      -> "No MCP server named '<name>' found."             exit 0

**`remove` exits 0 both ways**, so — as with vibe — the exit code cannot tell a removal from a no-op
and the message is the signal. Each check requires a whole line to match, the name delimited by its
quotes, and anything unrecognised at exit 0 is `unknown`, never `removed`. None of these three
commands wrote a `[projects.…]` trust entry (measured); only `codex exec` does that.
"""

from __future__ import annotations

import json
import shlex
from dataclasses import dataclass

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


def _says(result: RunResult, expected_lower: str) -> bool:
    """A zero exit and some complete line equal to `expected_lower`, case-insensitively."""
    return result.ok and any(
        line.strip().lower() == expected_lower for line in result.output.splitlines()
    )


def says_removed(result: RunResult, key: str) -> bool:
    return _says(result, f"removed global mcp server '{key}'.")


def says_not_found(result: RunResult, key: str) -> bool:
    return _says(result, f"no mcp server named '{key}' found.")


@dataclass(frozen=True)
class CodexClient(CliClient):
    key: str = "codex"
    label: str = "Codex"
    binary: str = "codex"
    config_hint: str = "~/.codex/config.toml"

    def env_flag(self, registration: Registration) -> list[str]:
        return ["--env", f"PATH={registration.path_env}"]

    def list_argv(self) -> list[str]:
        return [self.binary, "mcp", "list", "--json"]

    def remove_argv(self, key: str) -> list[str]:
        return [self.binary, "mcp", "remove", key]

    def inspect(self, key: str, run: Runner) -> Inspection:
        listed = run(self.list_argv())
        where = shlex.join(listed.argv)
        if listed.timed_out:
            return Inspection(self.key, None, error=f"`{where}` timed out")
        if not listed.ok:
            return Inspection(
                self.key, None, error=f"`{where}` failed (exit {listed.returncode}): {listed.tail}"
            )
        try:
            entries = json.loads(listed.stdout if listed.stdout is not None else listed.output)
        except ValueError as exc:
            return Inspection(self.key, None, error=f"`{where}` did not print JSON ({exc})")
        if not isinstance(entries, list):
            return Inspection(self.key, None, error=f"`{where}` did not print a JSON array")

        matching = [e for e in entries if isinstance(e, dict) and e.get("name") == key]
        if not matching:
            return Inspection(self.key, False, notes=(f"read {where}",))
        transport = matching[0].get("transport")
        if isinstance(transport, dict) and transport.get("type") not in (None, "stdio"):
            return Inspection(
                self.key,
                True,
                notes=(f"read {where}", f"registered with a {transport.get('type')} transport"),
            )
        return entry_inspection(self.key, transport, where)

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
        if not removed.ok:
            return Result(
                self.key,
                "failed",
                f"remove command failed (exit {removed.returncode})",
                steps=steps,
                diagnostics=(removed.tail,) if removed.tail else (),
            )
        if says_removed(removed, key):
            return Result(
                self.key, "removed", f"remove command succeeded ({self.config_hint})", steps=steps
            )
        if says_not_found(removed, key):
            return Result(self.key, "not_installed", "nothing registered", steps=steps)
        return Result(
            self.key,
            "unknown",
            "remove exited 0 but did not report a recognised outcome; the entry may or may not "
            "still be there",
            steps=steps,
            diagnostics=(removed.tail,) if removed.tail else (),
        )
