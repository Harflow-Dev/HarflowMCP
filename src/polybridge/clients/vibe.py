"""vibe — `mcp add` neither overwrites (codex, opencode) nor refuses (Claude Code): measured against
a sandboxed `VIBE_HOME` (vibe 2.25.1), it silently **skips at exit 0** when the name is already
configured, and writes nothing. Inheriting `CliClient`'s default overwrite policy would therefore
report success while leaving a stale command in place — the exact overclaim CLAUDE.md forbids.

So `apply` here is add-first, mirroring `clients/claude_code.py`: it runs `add` first, and only when
that reports the entry already existed does it fall back to remove-then-add. That confines the
destructive window (removing before knowing the add will succeed) to the one case where an entry
genuinely existed, rather than paying it on every call regardless of whether one did. There is no
`--force`/`--replace` on `add` to shortcut that with.

Measured (vibe 2.25.1, against a sandboxed `VIBE_HOME`):

    vibe mcp add <new>          -> "Added MCP server `<name>`."                                exit 0
    vibe mcp add <identical>    -> "MCP server `<name>` is already configured."   exit 0, writes nothing
    vibe mcp add <same name,    -> usage: ... / "vibe mcp: error: MCP server name `<name>` is
                 differing>        already configured."                           exit 2, writes nothing
    vibe mcp remove <existing>  -> "Removed MCP server `<name>`."                               exit 0
    vibe mcp remove <absent>    -> "MCP server `<name>` is not configured in the user config."  exit 0

**The collision has two shapes, and only one of them exits 0.** Re-adding a byte-for-byte identical
entry is an idempotent no-op at exit 0; re-adding the *same name with different settings* — which is
exactly what an update is — fails at exit 2 through argparse, with the extra word "name" in the
message and a `usage:` block. An earlier measurement of this client only re-added an identical entry
and wrongly generalised "always skips at exit 0" from it; a real `cli_integration` run against the
binary is what caught that. Both signatures are treated as a collision and route to remove-then-add,
so the stored entry always ends up matching what was asked for rather than merely already existing.

Remove-then-add updates correctly (verified: the stored command changed). Because **every case but
the differing-settings conflict exits 0, the exit code alone cannot tell success from a no-op** —
the stdout message is the signal, and the exit code only ever narrows which message to expect.
Every check below requires a *complete line* of the output, after stripping and lower-casing, to
equal one of these sentences verbatim (punctuation included), with the name delimited by its
backticks per CLAUDE.md's rule about narrow signature matching — a longer name ending or starting
with ours must not be mistaken for our own, and neither must a superficially similar sentence that
merely contains the expected wording as a fragment (e.g. a hypothetical "Not added MCP server
`<name>`." or "Added MCP server `<name>` but configuration was not written"). Anything unmatched —
including exit 0 with unrecognised wording — is reported `unknown`, never `applied`.

If a confirmed removal (the entry existed and `remove` said so, or a race meant it was already gone)
is followed by an `add` that then times out or fails, the result says to assume polybridge is **not**
registered with vibe and gives the exact command to restore it — the same discipline
`clients/claude_code.py` applies after its own replace step, and for the same reason: a failed `add`
is not proof it wrote nothing.

Also measured: `vibe mcp add` **rewrites the whole `config.toml` and destroys comments** — a
`# hand-written comment` above an unrelated `[[mcp_servers]]` entry was gone afterwards, and that
entry's `args` array was reformatted from an inline list to one spread over multiple lines. The
entry's own data survived; only the comment and formatting were lost. That is unlike codex and
opencode, which were measured to preserve comments and formatting elsewhere in their config files —
do not let that expectation leak into how this client's success is described.

`NAME` is a positional argument here, not `--name`, and there is no `--` separator: the server
command goes behind `--command`. So `CliClient.add_argv`'s trailing `"--", registration.command`
shape does not apply and is overridden outright.

`inspect` reads `config.toml` itself with `tomllib` — vibe has no machine-readable listing — from
`$VIBE_HOME` when set, else `~/.vibe` (`VIBE_HOME` is honoured, measured). `remove` is
`vibe mcp remove`, which **also strips every comment** in `config.toml` (measured, vibe 2.25.5: a
trailing `# c2` was gone after removing our entry), so the file is backed up with a timestamp first
and the result names the backup. Removing an absent name left the file byte-identical (measured), but
the backup is taken regardless, before vibe is asked: a guess about which case applies is not worth
the comments it would cost if wrong.
"""

from __future__ import annotations

import os
import shlex
import tomllib
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
    cli_relative,
    entry_inspection,
)
from .desktop import back_up


def config_path() -> Path:
    override = os.environ.get("VIBE_HOME")
    base = cli_relative(override) if override else Path.home() / ".vibe"
    return base / "config.toml"


def _exact_line(result: RunResult, expected_lower: str) -> bool:
    """True if some complete line of `result.output`, stripped and lower-cased, equals
    `expected_lower` exactly.

    A substring check would also match a sentence that merely contains the expected wording as a
    fragment — a hypothetical "Not added MCP server `<key>`." contains "added mcp server `<key>`.",
    and "Added MCP server `<key>` but configuration was not written" contains the whole confirmation
    as a prefix. Requiring the *entire line* to match is what rules those out.
    """
    if not result.ok:
        return False
    return any(line.strip().lower() == expected_lower for line in result.output.splitlines())


def says_added(result: RunResult, key: str) -> bool:
    """Measured, a whole line, punctuation included: "Added MCP server `<key>`.\""""
    return _exact_line(result, f"added mcp server `{key}`.")


def says_already_configured(result: RunResult, key: str) -> bool:
    """Measured, a whole line: "MCP server `<key>` is already configured." exit 0, writes nothing."""
    return _exact_line(result, f"mcp server `{key}` is already configured.")


def says_removed(result: RunResult, key: str) -> bool:
    """Measured, a whole line: "Removed MCP server `<key>`.\""""
    return _exact_line(result, f"removed mcp server `{key}`.")


def says_not_configured(result: RunResult, key: str) -> bool:
    """Measured, a whole line: "MCP server `<key>` is not configured in the user config.\""""
    return _exact_line(result, f"mcp server `{key}` is not configured in the user config.")


def says_name_conflict(result: RunResult, key: str) -> bool:
    """The *other* collision: same name, different settings — argparse-level, exit 2.

    Distinct from `says_already_configured`, which is the exit-0 idempotent no-op for an entry that
    already matches byte for byte. This one carries the extra word "name" and an argparse `usage:`
    block, so the two signatures cannot be confused for one another:

        vibe mcp: error: MCP server name `<key>` is already configured.

    Not gated on the exit code here because the wording is what identifies it; the branch that
    consults it has already established the call failed.
    """
    return any(
        line.strip().lower() == f"vibe mcp: error: mcp server name `{key}` is already configured."
        for line in result.output.splitlines()
    )


@dataclass(frozen=True)
class VibeClient(CliClient):
    key: str = "vibe"
    label: str = "Mistral Vibe"
    binary: str = "vibe"
    config_hint: str = "~/.vibe/config.toml"

    def add_argv(self, registration: Registration) -> list[str]:
        return [
            self.binary,
            "mcp",
            "add",
            registration.key,
            "--transport",
            "stdio",
            "--command",
            registration.command,
            *self.env_flag(registration),
        ]

    def remove_argv(self, key: str) -> list[str]:
        return [self.binary, "mcp", "remove", key]

    def env_flag(self, registration: Registration) -> list[str]:
        return ["--env", f"PATH={registration.path_env}"]

    def inspect(self, key: str, run: Runner) -> Inspection:
        """Read-only; `run` is unused."""
        path = config_path()
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return Inspection(self.key, False, notes=(f"no config at {path}",))
        try:
            config = tomllib.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            return Inspection(self.key, None, error=f"{path} could not be parsed ({exc})")
        servers = config.get("mcp_servers", [])
        if not isinstance(servers, list):
            return Inspection(self.key, None, error=f"'mcp_servers' in {path} is not an array")

        if not all(isinstance(e, dict) and isinstance(e.get("name"), str) for e in servers):
            # One entry we cannot read could be ours, so absence would be a guess.
            return Inspection(
                self.key, None, error=f"'mcp_servers' in {path} has an entry with no name"
            )
        matching = [e for e in servers if e["name"] == key]
        if not matching:
            return Inspection(self.key, False, notes=(f"read {path}",))
        inspections = [entry_inspection(self.key, entry, str(path)) for entry in matching]
        if len({(i.command, i.path_env) for i in inspections}) > 1:
            return Inspection(
                self.key,
                True,
                notes=(f"read {path}", f"{len(matching)} differing entries named `{key}`"),
            )
        return inspections[0]

    def remove(self, key: str, run: Runner) -> Result:
        """Back up, then `vibe mcp remove`. A failed backup raises before vibe is ever run."""
        path = config_path()
        backup = back_up(path)
        kept = (f"backed up {path} to {backup} first (vibe mcp remove strips comments)",)
        diagnostics = kept if backup else ()

        removed = run(self.remove_argv(key))
        steps = (shlex.join(removed.argv),)
        if removed.timed_out:
            return Result(
                self.key,
                "unknown",
                f"timed out after {CLI_TIMEOUT_SECONDS:.0f}s; the entry may or may not still be there",
                steps=steps,
                diagnostics=diagnostics,
            )
        if not removed.ok:
            return Result(
                self.key,
                "failed",
                f"remove command failed (exit {removed.returncode})",
                steps=steps,
                diagnostics=(*((removed.tail,) if removed.tail else ()), *diagnostics),
            )
        if says_removed(removed, key):
            return Result(
                self.key,
                "removed",
                f"remove command succeeded ({self.config_hint})",
                steps=steps,
                diagnostics=diagnostics,
            )
        if says_not_configured(removed, key):
            return Result(
                self.key, "not_installed", "nothing registered", steps=steps, diagnostics=diagnostics
            )
        return Result(
            self.key,
            "unknown",
            "remove exited 0 but did not report a recognised outcome; the entry may or may not "
            "still be there",
            steps=steps,
            diagnostics=(*((removed.tail,) if removed.tail else ()), *diagnostics),
        )

    def apply(self, registration: Registration, run: Runner) -> Result:
        key = registration.key
        added = run(self.add_argv(registration))

        if added.timed_out:
            return Result(
                self.key,
                "unknown",
                f"timed out after {CLI_TIMEOUT_SECONDS:.0f}s; the config may have changed",
                steps=(shlex.join(added.argv),),
            )
        if not added.ok:
            if says_name_conflict(added, key):
                # The collision that actually matters for an update: the name exists with different
                # settings, which vibe reports as an argparse error rather than the exit-0 no-op.
                return self.replace(registration, run, first_add=added)
            return Result(
                self.key,
                "failed",
                f"add command failed (exit {added.returncode})",
                steps=(shlex.join(added.argv),),
                diagnostics=(added.tail,) if added.tail else (),
            )
        if says_added(added, key):
            return Result(
                self.key,
                "applied",
                f"add command succeeded ({self.config_hint})",
                steps=(shlex.join(added.argv),),
            )
        if says_already_configured(added, key):
            return self.replace(registration, run, first_add=added)

        # Exit 0 with wording this client does not recognise: pressing on to remove-then-add would
        # be a guess dressed up as a fact, not the confirmed collision that justifies it.
        return Result(
            self.key,
            "unknown",
            "add exited 0 but did not report a recognised confirmation; the config may have changed",
            steps=(shlex.join(added.argv),),
            diagnostics=(added.tail,) if added.tail else (),
        )

    def replace(self, registration: Registration, run: Runner, *, first_add: RunResult) -> Result:
        """`add` reported the entry already existed: clear it, then add again.

        Mirrors `clients/claude_code.py`'s `replace` — the destructive `remove` only happens here,
        after a confirmed collision, never unconditionally on every `apply`.
        """
        key = registration.key
        restore = shlex.join(self.add_argv(registration))
        removed = run(self.remove_argv(registration.key))
        steps = (shlex.join(first_add.argv), shlex.join(removed.argv))

        if removed.timed_out:
            return Result(
                self.key,
                "unknown",
                "an entry already existed; removing it timed out, so it may or may not still be "
                "there",
                steps=steps,
                diagnostics=(f"if it is gone, restore it with: {restore}",),
            )
        if not removed.ok:
            return Result(
                self.key,
                "failed",
                f"an entry already existed and could not be removed (exit {removed.returncode}); "
                "no replacement was attempted, so assume polybridge may no longer be registered "
                "with vibe",
                steps=steps,
                # Nothing measured establishes that a failing `remove` cannot have written first, so
                # the restoring command is given rather than assuming the entry survived.
                diagnostics=(
                    *((removed.tail,) if removed.tail else ()),
                    f"if it is gone, restore it with: {restore}",
                ),
            )
        if not (says_removed(removed, key) or says_not_configured(removed, key)):
            # `remove` always exits 0, so an unrecognised message here is the same kind of signal an
            # unrecognised `add` message is: vibe's wording may have moved out from under this
            # client, and pressing on to `add` regardless would be a guess dressed as a fact.
            return Result(
                self.key,
                "unknown",
                "clearing the existing entry exited 0 but did not report a recognised confirmation; "
                "the replacement was not attempted, so assume polybridge may no longer be "
                "registered with vibe",
                steps=steps,
                # If vibe changed its success wording *after* actually removing the entry, saying
                # only "the config may have changed" leaves the caller with no registration and no
                # way back.
                diagnostics=(
                    *((removed.tail,) if removed.tail else ()),
                    f"if it is gone, restore it with: {restore}",
                ),
            )

        # `remove` reporting the name was absent means the collision was over something we never
        # saw (a race). Every sentence below has to respect that, or it invents an entry and a
        # deletion that never happened.
        gone = (
            "the previous entry was removed"
            if says_removed(removed, key)
            else "no entry was found to remove"
        )

        readded = run(self.add_argv(registration))
        steps = (*steps, shlex.join(readded.argv))

        if readded.ok and says_added(readded, key):
            return Result(
                self.key,
                "applied",
                f"{gone}; add command succeeded ({self.config_hint})",
                steps=steps,
            )
        if readded.timed_out:
            return Result(
                self.key,
                "unknown",
                f"{gone} and the replacement timed out — assume polybridge is NOT registered with "
                "vibe until confirmed otherwise",
                steps=steps,
                diagnostics=(f"if nothing is registered, run: {restore}",),
            )
        if not readded.ok:
            return Result(
                self.key,
                "failed",
                f"{gone} and the replacement failed (exit {readded.returncode})"
                " — assume polybridge is NOT registered with vibe",
                steps=steps,
                diagnostics=(
                    f"restore it with: {restore}", *((readded.tail,) if readded.tail else ())
                ),
            )

        # Exit 0 without the confirmation this client relies on — e.g. `add` reporting "already
        # configured" right after a remove that reported success. Should not happen, but the whole
        # point of overriding `apply` is to never assume success from an exit code alone.
        return Result(
            self.key,
            "unknown",
            f"{gone} and the replacement exited 0 but did not report a recognised confirmation — "
            "assume polybridge is NOT registered with vibe until confirmed otherwise",
            steps=steps,
            diagnostics=(
                f"if nothing is registered, run: {restore}", *((readded.tail,) if readded.tail else ())
            ),
        )
