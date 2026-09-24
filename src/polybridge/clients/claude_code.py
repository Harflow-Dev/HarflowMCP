"""Claude Code — the only client whose `mcp add` refuses to overwrite.

Measured (claude 2.1.220): re-adding an existing name exits 1 with
`MCP server <name> already exists in user config` and writes nothing, and there is no
`--force`/`--replace` on `add` or `add-json`. So updating an entry means removing it first, which
opens a window where the user has no registration at all.

Two things keep that honest. Both signatures are matched narrowly, including the server's own name,
so an unrelated future error mentioning something else that "already exists" cannot send us on to
`remove`. And if the replacement `add` then fails, the report says to assume nothing is registered
and prints the exact command that restores it. The entry at risk is only ever our own key; no other
server is touched.

`claude mcp get` is deliberately not used to probe first: it prints human prose rather than JSON, and
it *launches the server* to health-check it (measured). So `inspect` reads the config file itself —
top-level `mcpServers` in `~/.claude.json`, or `$CLAUDE_CONFIG_DIR/.claude.json` when that is set
(measured, claude 2.1.281: `mcp add -s user` writes exactly there). Only user scope: the entries under
`projects[*]` are other scopes, and `add` never writes them.

`remove` is `claude mcp remove <key> -s user`, whose exit code is meaningful (measured): 0 with
`Removed MCP server <key> from user config`, or 1 with the not-found signature below when absent.
"""

from __future__ import annotations

import json
import os
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


def user_config_path() -> Path:
    """Where `claude mcp add -s user` writes: `$CLAUDE_CONFIG_DIR/.claude.json`, else `~/.claude.json`."""
    override = os.environ.get("CLAUDE_CONFIG_DIR")
    if override:
        return Path(os.path.expanduser(override)) / ".claude.json"
    return Path.home() / ".claude.json"


def says_already_exists(result: RunResult, key: str) -> bool:
    """The measured collision, whole: `MCP server <key> already exists in user config`.

    The leading `mcp server ` is what makes this safe. Matching `<key> already exists` alone would
    also match a *different* server whose name ends in ours — `not-polybridge already exists` — and
    then remove the entry we were asked to protect.
    """
    signature = f"mcp server {key} already exists in".lower()
    return result.returncode == 1 and signature in result.output.lower()


def says_not_found(result: RunResult, key: str) -> bool:
    """The measured absence, whole: `No MCP server named "<key>" in user scope`.

    Quoted for the same reason: the name has to be delimited, or a longer name containing ours (or a
    stray mention in trailing diagnostics) reads as our own absence.
    """
    signature = f'no mcp server named "{key}"'.lower()
    return result.returncode == 1 and signature in result.output.lower()


@dataclass(frozen=True)
class ClaudeCodeClient(CliClient):
    key: str = "claude-code"
    label: str = "Claude Code"
    binary: str = "claude"
    config_hint: str = "~/.claude.json, user scope"
    # User scope, so the registration is not confined to whichever directory setup ran in.
    add_flags: tuple[str, ...] = ("-s", "user")

    def env_flag(self, registration: Registration) -> list[str]:
        return ["-e", f"PATH={registration.path_env}"]

    def remove_argv(self, key: str) -> list[str]:
        return [self.binary, "mcp", "remove", key, "-s", "user"]

    def inspect(self, key: str, run: Runner) -> Inspection:
        """Read-only, and never through `claude mcp get`, which launches the server. `run` is unused."""
        path = user_config_path()
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return Inspection(self.key, False, notes=(f"no config at {path}",))
        try:
            config = json.loads(raw)
        except ValueError as exc:
            return Inspection(self.key, None, error=f"{path} is not valid JSON ({exc})")
        if not isinstance(config, dict):
            return Inspection(self.key, None, error=f"{path} does not contain a JSON object")
        servers = config.get("mcpServers", {})
        if not isinstance(servers, dict):
            return Inspection(self.key, None, error=f"'mcpServers' in {path} is not a JSON object")
        if key not in servers:
            return Inspection(self.key, False, notes=(f"read {path}",))
        return entry_inspection(self.key, servers[key], str(path))

    def remove(self, key: str, run: Runner) -> Result:
        removed = run(self.remove_argv(key))
        steps = (shlex.join(removed.argv),)
        if removed.ok:
            return Result(
                self.key, "removed", f"remove command succeeded ({self.config_hint})", steps=steps
            )
        if says_not_found(removed, key):
            return Result(self.key, "not_installed", "nothing registered at user scope", steps=steps)
        if removed.timed_out:
            return Result(
                self.key,
                "unknown",
                f"timed out after {CLI_TIMEOUT_SECONDS:.0f}s; the entry may or may not still be there",
                steps=steps,
            )
        return Result(
            self.key,
            "failed",
            f"remove command failed (exit {removed.returncode})",
            steps=steps,
            diagnostics=(removed.tail,) if removed.tail else (),
        )

    def apply(self, registration: Registration, run: Runner) -> Result:
        added = run(self.add_argv(registration))
        if not says_already_exists(added, registration.key):
            return self.judge(added)
        return self.replace(registration, run, first_add=added)

    def replace(self, registration: Registration, run: Runner, *, first_add: RunResult) -> Result:
        restore = shlex.join(self.add_argv(registration))
        removed = run(self.remove_argv(registration.key))
        steps = (shlex.join(first_add.argv), shlex.join(removed.argv))

        if removed.timed_out:
            return Result(
                self.key,
                "unknown",
                "an entry already existed; removing it timed out, so it may or may not still be there",
                steps=steps,
                diagnostics=(f"if it is gone, restore it with: {restore}",),
            )
        if not removed.ok and not says_not_found(removed, registration.key):
            return Result(
                self.key,
                "failed",
                f"an entry already existed and could not be removed (exit {removed.returncode}); "
                "no replacement was attempted",
                steps=steps,
                diagnostics=(removed.tail,) if removed.tail else (),
            )

        # `remove` reporting the name was absent means the collision was over something we never saw.
        # Every sentence below has to respect that, or it invents an entry and a deletion.
        gone = "the previous entry was removed" if removed.ok else "the reported entry was already gone"

        readded = run(self.add_argv(registration))
        steps = (*steps, shlex.join(readded.argv))
        if readded.ok:
            what = "replaced the existing entry" if removed.ok else gone
            return Result(
                self.key,
                "applied",
                f"{what}; add command succeeded ({self.config_hint})",
                steps=steps,
            )
        if readded.timed_out:
            return Result(
                self.key,
                "unknown",
                f"{gone} and the replacement timed out, so it may or may not have been written",
                steps=steps,
                diagnostics=(f"if nothing is registered, run: {restore}",),
            )
        return Result(
            self.key,
            "failed",
            f"{gone} and the replacement failed (exit {readded.returncode})"
            " — assume polybridge is NOT registered with Claude Code",
            steps=steps,
            diagnostics=(f"restore it with: {restore}", *((readded.tail,) if readded.tail else ())),
        )
