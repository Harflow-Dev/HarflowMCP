"""Claude Code backend.

CLI facts established by measurement, not assumption:

* `-p --output-format stream-json` refuses to start without `--verbose`.
* `--disallowedTools` is variadic, so its patterns must be one comma-separated value or the option
  swallows whatever follows it. `--allowedTools` is variadic the same way.
* `--max-turns` works but is undocumented in `--help`.
* Deny rules *do* take precedence over `bypassPermissions`, and the matcher decomposes `&&` chains —
  but not `git -C … commit` or `bash -c 'git commit'`. There is no OS sandbox at all.
* `-p`/`--print` is a *boolean* flag, not one that takes the prompt as its argument — `--help`'s own
  usage line is `claude [options] [command] [prompt]`, with `prompt` a separate declared positional.
  So a prompt token that happens to exactly equal a real claude option name (e.g.
  `--dangerously-skip-permissions`) is parsed by claude as that option, not as text, if nothing marks
  where options end. Confirmed live (killed within seconds, `--permission-mode plan`): with no `--`,
  that shape reached claude's own option parser and the run aborted for lack of a prompt; with `--`
  inserted before the prompt, claude correctly read the following token as literal prompt text and
  started a normal turn. `assert_safe` and the argv builders both rely on `--` for this now, the same
  way opencode and codex already did — a prior version of this backend assumed the prompt was simply
  positional at a fixed index, which this measurement showed was not a safe assumption to make.

**`publish` (measured, and it refuted an earlier plan for this level).** Dropping the deny patterns
alone is not enough: with `acceptEdits` and no denies, `git commit` was still refused with "This
command requires approval", because `acceptEdits` auto-approves *edits*, not arbitrary Bash, and
headless `-p` mode has nobody to give that approval. The lever that actually works is an allow-list:
`--permission-mode acceptEdits --allowedTools "Bash(git commit:*),Bash(git push:*)"` let both commands
succeed with an empty `permission_denials`, and the commit landed in a bare remote. So at `publish`
the deny patterns are dropped and `--allowedTools "Bash(git commit:*),Bash(git push:*),Bash(gh pr
create:*)"` is added instead — deny and allow are never combined, since deny beats allow and would be
self-defeating. Everything else still needs approval, which a headless run cannot give, so it is
still refused — `publish` is a genuine middle tier here, not a fallback to `bypassPermissions`. Also
measured: the approval layer refuses a *chained* command citing each part separately (e.g.
`git commit -am wip, echo "EXIT: $?"`), so an allow-listed command bundled with another is still
refused.

**Compatibility break at `unrestricted`.** The git deny patterns used to apply at every freedom,
`unrestricted` included, so `git commit`/`git push` were refused even there. They are now dropped at
`unrestricted` too — `bypassPermissions` with no denies — because adding `publish` as a middle tier
between `write_in_repo` and `unrestricted` only makes sense if `unrestricted` is at least as
permissive. Measured (Gate D, not just inferred from "bypassPermissions auto-approves everything"):
`--permission-mode bypassPermissions` with no `--disallowedTools`, against a scratch repo with a
local bare remote — `git commit` and `git push` both succeeded, `permission_denials` was empty, and
the commit landed in both the working repo and the remote.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .base import (
    EFFORTS,
    Accumulator,
    Capabilities,
    Enforcement,
    Freedom,
    ReasoningEffort,
    Status,
    check_freedom,
    check_reasoning_effort,
)

BINARY = "claude"

# One comma-separated value on purpose: the option is variadic.
DISALLOWED_TOOLS = "Bash(git commit:*),Bash(git push:*)"

# The freedoms at which the deny patterns above are applied. Deliberately excludes `publish` and
# `unrestricted`: deny beats allow, so keeping the denies at `publish` would defeat its own
# allow-list, and `unrestricted` drops them as the (documented) compatibility break above.
DENY_FREEDOMS: frozenset[str] = frozenset({"read_only", "write_in_repo"})

# One comma-separated value, same reason as DISALLOWED_TOOLS. Measured as the mechanism that
# actually lets `publish` commit/push/open-a-PR headlessly — see the module docstring.
ALLOWED_TOOLS: dict[str, str] = {
    "publish": "Bash(git commit:*),Bash(git push:*),Bash(gh pr create:*)",
}

# Flags that would cut a dispatched agent off from the user's own MCP servers, settings, hooks and
# CLAUDE.md. That inheritance is the point — a dispatched agent should be as capable as one the user
# runs themselves — so these are refused rather than merely unused.
FORBIDDEN_FLAGS = ("--strict-mcp-config", "--setting-sources", "--safe-mode", "--bare")

# Options this backend itself ever writes — nothing more. `assert_safe` walks the option region
# against exactly these instead of searching/counting it, because a search is what let non-canonical
# spellings through while claude still honoured them. Measured evading the old search/count check:
# the attached `--permission-mode=bypassPermissions`, `--dangerously-skip-permissions` (caught
# separately, below, for a clearer message — see the dedicated check in assert_safe), and the
# attached alias form `--disallowed-tools=...`. Long aliases (`--allowed-tools`,
# `--disallowed-tools`) are deliberately absent even though claude accepts them: this backend never
# writes them, so admitting them here would reopen the same hole under a different spelling.
BOOLEAN_FLAGS = ("--verbose",)
VALUE_FLAGS = (
    "--output-format",
    "--permission-mode",
    "--disallowedTools",
    "--allowedTools",
    "--max-turns",
    "--model",
    "--effort",
    "--session-id",
    "--resume",
)

PERMISSION_MODES: dict[str, str] = {
    "read_only": "plan",
    "write_in_repo": "acceptEdits",
    "publish": "acceptEdits",
    "unrestricted": "bypassPermissions",
}

_EFFORT_ACCEPTANCE_CAVEAT = (
    "acceptance evidence is a real --effort xhigh run that produced no \"Unknown --effort value\" "
    "warning on stderr and completed normally; no cross-level comparison was made, so this does not "
    "show xhigh behaving differently from low"
)
_EFFORT_DEGRADE_CAVEAT = (
    "an --effort value outside low/medium/high/xhigh/max is silently ignored by the CLI itself "
    "(stderr warning, default effort used, measured) — polybridge's own validation is what stops "
    "an unsupported value reaching it, not the CLI"
)

_NO_SANDBOX_CAVEAT = (
    "no OS sandbox: the agent can read and write outside repo_path, which is only its working "
    "directory"
)
_DENY_PATTERN_CAVEAT = (
    "the commit/push deny patterns are evaded by `git -C <path> commit` and `bash -c 'git commit'` "
    "(measured), so they stop ordinary work, not deliberate circumvention"
)
_PUBLISH_ALLOWLIST_CAVEAT = (
    "the deny patterns are dropped and replaced with --allowedTools "
    f'"{ALLOWED_TOOLS["publish"]}" (measured: dropping the denies alone was NOT sufficient — with '
    "acceptEdits and no denies, git commit was still refused with \"This command requires "
    "approval\"; the allow-list is the mechanism that actually works). Everything not allow-listed "
    "still needs approval, which a headless run cannot give, so it is still refused — this is a "
    "genuine middle tier, not a fallback to bypassPermissions"
)
_CHAINED_COMMAND_CAVEAT = (
    "claude's approval layer refuses a chained command citing each part separately (measured: "
    "\"git commit -am wip, echo $?\" was refused even though git commit alone is allow-listed), so "
    "bundling an allow-listed command with another still gets the whole thing refused"
)
_UNRESTRICTED_COMPAT_BREAK_CAVEAT = (
    "compatibility break: the deny patterns used to apply at unrestricted too, so ordinary git "
    "commit/push were refused even there; they are now dropped, same as at publish. Measured "
    "(Gate D): bypassPermissions with no --disallowedTools, against a scratch repo with a local "
    "bare remote — git commit and git push both succeeded, permission_denials was empty, and the "
    "commit landed in both the working repo and the remote"
)

# Per-mode detail. `writes_confined_to_repo` stays False throughout because none of this is enforced
# by the OS — but what the permission layer does add is worth stating rather than leaving implied.
_MODE_CAVEATS: dict[str, tuple[str, ...]] = {
    "read_only": (
        "read-only is plan mode: an agent-level restriction, so it holds only as long as the agent "
        "respects it",
    ),
    "write_in_repo": (
        "acceptEdits does additionally gate edits outside the working directory through the "
        "permission layer, which a headless run cannot approve (observed: such a write was denied) "
        "— useful in practice, but still not an OS boundary",
    ),
    "publish": (
        _PUBLISH_ALLOWLIST_CAVEAT,
        _CHAINED_COMMAND_CAVEAT,
    ),
    "unrestricted": (
        "bypassPermissions removes even that gate: writes anywhere the user can write will succeed",
        _UNRESTRICTED_COMPAT_BREAK_CAVEAT,
    ),
}


class UnsafeInvocationError(RuntimeError):
    """An argv was assembled without this backend's required guarantees."""


class ClaudeBackend:
    name = "claude"
    binary = BINARY
    capabilities = Capabilities(
        chooses_session_id=True,
        supports_turn_cap=True,
        reports_cost_usd=True,
        os_sandbox=False,
        per_command_deny=True,
        supports_model_selection=True,
        reasoning_effort=ReasoningEffort(
            accepts_parameter=True,
            levels=EFFORTS,
            native_flag="--effort",
            accepted_in_real_run=True,
            levels_change_behaviour=False,
            caveats=(_EFFORT_ACCEPTANCE_CAVEAT, _EFFORT_DEGRADE_CAVEAT),
        ),
    )

    def build_start_argv(
        self,
        prompt: str,
        *,
        repo: Path,
        freedom: Freedom,
        session_id: str | None,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
    ) -> list[str]:
        if session_id is None:
            raise ValueError("claude accepts a chosen session id, so one must be supplied")
        argv = self._common(freedom, model, max_turns, reasoning_effort)
        argv += ["--session-id", session_id]
        # `--` then the prompt: last, and explicitly not parsed as an option however it looks —
        # see the note in assert_safe about why this is load-bearing for claude specifically.
        argv += ["--", self._check_prompt(prompt)]
        self.assert_safe(argv, freedom)
        return argv

    def build_resume_argv(
        self,
        prompt: str,
        *,
        repo: Path,
        freedom: Freedom,
        session_id: str,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
    ) -> list[str]:
        argv = self._common(freedom, model, max_turns, reasoning_effort)
        # --resume and --session-id conflict, so never both.
        argv += ["--resume", session_id]
        argv += ["--", self._check_prompt(prompt)]
        self.assert_safe(argv, freedom)
        return argv

    def _common(
        self,
        freedom: Freedom,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
    ) -> list[str]:
        # Re-checked here, the one place both start and resume funnel through, so no caller of this
        # method can reach the CLI with an effort it would silently degrade instead of honour.
        check_reasoning_effort(self, reasoning_effort)
        argv = [
            BINARY,
            "-p",
            "--output-format",
            "stream-json",
            # Required, not stylistic: the CLI refuses to start without it.
            "--verbose",
            "--permission-mode",
            PERMISSION_MODES[freedom],
        ]
        if freedom in DENY_FREEDOMS:
            argv += ["--disallowedTools", DISALLOWED_TOOLS]
        if freedom in ALLOWED_TOOLS:
            # Never combined with the deny patterns above: deny beats allow, which would defeat
            # this freedom's own allow-list.
            argv += ["--allowedTools", ALLOWED_TOOLS[freedom]]
        if max_turns is not None:
            argv += ["--max-turns", str(max_turns)]
        if model:
            argv += ["--model", model]
        if reasoning_effort:
            argv += ["--effort", reasoning_effort]
        return argv

    @staticmethod
    def _check_prompt(prompt: str) -> str:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        return prompt

    def assert_safe(self, argv: list[str], freedom: Freedom) -> None:
        # Rejects an unknown freedom outright rather than letting PERMISSION_MODES[freedom] raise a
        # bare KeyError below.
        check_freedom(freedom)
        if argv[:2] != [BINARY, "-p"]:
            raise UnsafeInvocationError(f"unrecognised claude argv layout: {argv!r}")

        # Only the option region is inspected. Everything after `--` is the prompt — caller text
        # that happens to contain a flag name must never be able to satisfy a safety check, nor be
        # read by claude itself as anything but text. An earlier version of this method assumed the
        # prompt was a fixed positional right after `-p` (index 2), reasoning that `-p <value>`
        # takes its argument the way most flags do. That assumption was wrong: measured against
        # `claude --help`, `-p`/`--print` is a *boolean* flag and `prompt` is a separate declared
        # positional (`Usage: claude [options] [command] [prompt]`) — so a prompt token that
        # happened to exactly equal a real claude option name (e.g.
        # `--dangerously-skip-permissions`) was parsed by claude as that option, not as prompt text,
        # with no separator to stop it. Confirmed live, with `--permission-mode plan` and killed
        # within seconds: without `--`, that exact shape reached claude's own parser; with `--`
        # inserted before the prompt, claude correctly treated the token after it as literal prompt
        # text and started a normal turn. So, as with opencode/codex, `--` now pins the boundary
        # explicitly rather than relying on argv position.
        if "--" not in argv:
            raise UnsafeInvocationError(
                f"refusing to run claude without a `--` separator before the prompt, which stops "
                f"prompt text being parsed as options: {argv!r}"
            )
        options = argv[2 : argv.index("--")]

        # Positional arity, not just separator presence: claude declares exactly one positional
        # (the prompt), so anything other than exactly one token after `--` is not a shape this
        # backend ever writes.
        positionals = argv[argv.index("--") + 1 :]
        if len(positionals) != 1:
            raise UnsafeInvocationError(
                f"expected exactly one positional argument (the prompt) after `--`, found "
                f"{len(positionals)}: {argv!r}"
            )

        # Kept for the clearer message even though the allowlist below would refuse this token too
        # (as unrecognised) — this is the one form worth naming explicitly: a full permission
        # bypass, not merely a flag this backend happens not to write.
        if "--dangerously-skip-permissions" in options:
            raise UnsafeInvocationError(
                f"--dangerously-skip-permissions discards the permission layer this backend's "
                f"freedom mapping depends on entirely: {argv!r}"
            )

        # Likewise kept ahead of the allowlist for their own clearer message.
        for flag in FORBIDDEN_FLAGS:
            if flag in options:
                raise UnsafeInvocationError(
                    f"{flag} would cut the dispatched agent off from the user's MCP servers and "
                    f"settings, which it is meant to inherit: {argv!r}"
                )

        seen = self._parse_options(options, argv)

        self._exactly_one(seen, "--verbose", argv)

        fmt = self._exactly_one(seen, "--output-format", argv)
        if fmt != "stream-json":
            raise UnsafeInvocationError(
                f"--output-format was {fmt!r}, but only stream-json can be parsed into events: "
                f"{argv!r}"
            )

        deny_values = seen.get("--disallowedTools", [])
        allow_values = seen.get("--allowedTools", [])

        # Structurally impossible for both to survive the walk, whatever spelling arrived and
        # whatever freedom is in play: deny beats allow, so both present would silently neuter an
        # allow-list freedom rather than error loudly. Checked before either per-freedom branch
        # below, so this is the invariant itself — not merely a consequence of DENY_FREEDOMS and
        # ALLOWED_TOOLS happening not to share a freedom today.
        if deny_values and allow_values:
            raise UnsafeInvocationError(
                f"--disallowedTools and --allowedTools both present: deny beats allow, which "
                f"would silently neuter this freedom's allow-list: {argv!r}"
            )

        if freedom in DENY_FREEDOMS:
            denied = self._exactly_one(seen, "--disallowedTools", argv)
            if denied != DISALLOWED_TOOLS:
                raise UnsafeInvocationError(
                    f"--disallowedTools was {denied!r}, expected the deny list: {argv!r}"
                )
        elif deny_values:
            raise UnsafeInvocationError(
                f"--disallowedTools present at freedom {freedom!r}, which must not deny git "
                f"commit/push: {argv!r}"
            )

        expected_allowed = ALLOWED_TOOLS.get(freedom)
        if expected_allowed is not None:
            allowed = self._exactly_one(seen, "--allowedTools", argv)
            if allowed != expected_allowed:
                raise UnsafeInvocationError(
                    f"--allowedTools was {allowed!r}, expected {expected_allowed!r} for freedom "
                    f"{freedom!r}: {argv!r}"
                )
        elif allow_values:
            raise UnsafeInvocationError(
                f"--allowedTools present at freedom {freedom!r}, which this backend does not use "
                f"there: {argv!r}"
            )

        mode = self._exactly_one(seen, "--permission-mode", argv)
        expected_mode = PERMISSION_MODES[freedom]
        if mode != expected_mode:
            raise UnsafeInvocationError(
                f"--permission-mode was {mode!r}, expected {expected_mode!r} for freedom "
                f"{freedom!r}: {argv!r}"
            )

        # Optional, unlike the flags above — but a second one, or a non-canonical value, would
        # either win silently or reach a CLI that degrades it without telling anyone.
        effort_values = seen.get("--effort", [])
        if len(effort_values) > 1:
            raise UnsafeInvocationError(f"--effort appears {len(effort_values)} times: {argv!r}")
        if effort_values and effort_values[0] not in EFFORTS:
            raise UnsafeInvocationError(f"unexpected --effort value {effort_values[0]!r}: {argv!r}")

        # Optional too, and caller-supplied — a second value would win silently.
        model_values = seen.get("--model", [])
        if len(model_values) > 1:
            raise UnsafeInvocationError(f"--model appears {len(model_values)} times: {argv!r}")

        # A second --max-turns would win silently, same reasoning as --effort/--model above.
        turns_values = seen.get("--max-turns", [])
        if len(turns_values) > 1:
            raise UnsafeInvocationError(f"--max-turns appears {len(turns_values)} times: {argv!r}")

        # Exactly one session flag, counted across both spellings: never both --session-id and
        # --resume, never neither, and it must actually name a session — a resume that silently
        # continued the wrong conversation is worse than one that fails outright.
        session_values = seen.get("--session-id", []) + seen.get("--resume", [])
        if len(session_values) != 1:
            raise UnsafeInvocationError(
                f"expected exactly one of --session-id/--resume, found {len(session_values)}: "
                f"{argv!r}"
            )
        if not session_values[0].strip():
            raise UnsafeInvocationError(f"session flag with no session id: {argv!r}")

    @staticmethod
    def _parse_options(options: list[str], argv: list[str]) -> dict[str, list[str]]:
        """Walk the option region strictly, refusing any token this backend would not have written.

        Mirrors OpencodeBackend._parse_options and exists for the same reason: searching
        `"--permission-mode" in flags` or counting `flags.count(...)` is not enough, because claude
        also honours `--flag=value` and documented long aliases (`--allowed-tools`,
        `--disallowed-tools`) this backend never emits — a search sees the canonical form it wrote
        and passes while claude applies the non-canonical one that rode along. Measured evading the
        old search/count check: the attached `--permission-mode=bypassPermissions`,
        `--dangerously-skip-permissions` (caught separately, above, for a clearer message), and the
        attached alias form `--disallowed-tools=...`. So every token not in canonical
        space-separated form is refused rather than skipped over.
        """
        seen: dict[str, list[str]] = {}
        index = 0
        while index < len(options):
            token = options[index]
            if token in BOOLEAN_FLAGS:
                seen.setdefault(token, []).append("")
                index += 1
            elif token in VALUE_FLAGS:
                if index + 1 >= len(options):
                    raise UnsafeInvocationError(f"{token} has no value: {argv!r}")
                value = options[index + 1]
                # A value that looks like an option is not a value: claude's parser would read it
                # as the next flag. `model` and the session id are caller-supplied and land here,
                # so a model named `--dangerously-skip-permissions` would otherwise smuggle an
                # option into the region this method exists to police.
                if value.startswith("-"):
                    raise UnsafeInvocationError(
                        f"{token} was given {value!r}, which claude would parse as an option "
                        f"rather than a value: {argv!r}"
                    )
                seen.setdefault(token, []).append(value)
                index += 2
            else:
                raise UnsafeInvocationError(
                    f"unrecognised option token {token!r}: this backend writes only canonical "
                    f"space-separated options, and `--flag=value` or an alias spelling would apply "
                    f"unnoticed: {argv!r}"
                )
        return seen

    @staticmethod
    def _exactly_one(seen: dict[str, list[str]], flag: str, argv: list[str]) -> str:
        values = seen.get(flag, [])
        if len(values) != 1:
            raise UnsafeInvocationError(f"{flag} appears {len(values)} times: {argv!r}")
        return values[0]

    def enforcement(self, freedom: Freedom) -> Enforcement:
        mode = PERMISSION_MODES[freedom]
        denies = freedom in DENY_FREEDOMS
        allowed = ALLOWED_TOOLS.get(freedom)
        if denies:
            mechanism = f"claude --permission-mode {mode} + deny patterns ({DISALLOWED_TOOLS})"
        elif allowed is not None:
            mechanism = f"claude --permission-mode {mode} + allow-list ({allowed}), no deny patterns"
        else:
            mechanism = f"claude --permission-mode {mode}, no deny patterns"

        caveats: tuple[str, ...] = (_NO_SANDBOX_CAVEAT,)
        if denies:
            caveats += (_DENY_PATTERN_CAVEAT,)
        caveats += _MODE_CAVEATS[freedom]

        return Enforcement(
            freedom=freedom,
            mechanism=mechanism,
            # Claude's restrictions are applied by the agent's own permission layer.
            os_enforced=False,
            writes_confined=False,
            writable_roots=(),
            # Deliberately False. The deny patterns refuse the obvious invocations but are evaded by
            # `git -C` and `bash -c` (measured), so committing is discouraged, not prevented — and a
            # boolean that says "blocked" would be a promise this cannot keep.
            commit_push_blocked=False,
            direct_commit_commands_denied=denies,
            # publish and unrestricted are the two freedoms where polybridge configures no barrier
            # of its own against a commit/push/PR attempt — see the field's own docstring for what
            # this does and does not promise.
            publish_attempts_allowed_by_polybridge=freedom in ("publish", "unrestricted"),
            # No sandbox at all, so nothing here ever controls network reachability.
            network_access="not_controlled",
            caveats=caveats,
        )

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        session_id = event.get("session_id")
        if acc.session_id is None and isinstance(session_id, str) and session_id:
            acc.session_id = session_id

        event_type = event.get("type")
        if event_type == "system" and event.get("subtype") == "init":
            servers = event.get("mcp_servers")
            if isinstance(servers, list):
                acc.mcp_servers = servers
            tools = event.get("tools")
            if isinstance(tools, list):
                acc.available_tool_count = len(tools)
        elif event_type == "result":
            acc.terminal = event
            acc.saw_final_message = True
            text = event.get("result")
            acc.summary = text if isinstance(text, str) else None
            acc.is_error = bool(event.get("is_error"))
            turns = event.get("num_turns")
            acc.num_turns = turns if isinstance(turns, int) else None
            cost = event.get("total_cost_usd")
            acc.total_cost_usd = float(cost) if isinstance(cost, (int, float)) else None
            usage = event.get("usage")
            acc.usage = usage if isinstance(usage, dict) else None
            denials = event.get("permission_denials")
            acc.denials = denials if isinstance(denials, list) else []

    def classify(self, acc: Accumulator, exit_code: int | None) -> Status:
        if acc.terminal is None:
            return "failed"
        if acc.terminal.get("subtype") == "error_max_turns":
            return "timed_out"
        if acc.is_error:
            return "failed"
        # `exit_code is None` means nothing observed the process exit (a recovered run). Claude is
        # the one backend that does not need an observed exit: its `result` event is a real terminal
        # event, so a successful one is evidence in its own right. Only an *observed* non-zero exit
        # overrules it — `exit_code != 0` alone would read None as failure and throw that evidence
        # away.
        if exit_code is not None and exit_code != 0:
            return "failed"
        return "completed"
