"""Claude Code backend.

CLI facts established by measurement, not assumption:

* `-p --output-format stream-json` refuses to start without `--verbose`.
* `--disallowedTools` is variadic, so its patterns must be one comma-separated value or the option
  swallows whatever follows it. `--allowedTools` is variadic the same way.
* `--max-turns` works but is undocumented in `--help`.
* Deny rules *do* take precedence over `bypassPermissions`, and the matcher decomposes `&&` chains —
  but not `git -C … commit` or `bash -c 'git commit'`. There is no OS sandbox at all.

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
        argv = self._common(prompt, freedom, model, max_turns, reasoning_effort)
        argv += ["--session-id", session_id]
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
        argv = self._common(prompt, freedom, model, max_turns, reasoning_effort)
        # --resume and --session-id conflict, so never both.
        argv += ["--resume", session_id]
        self.assert_safe(argv, freedom)
        return argv

    def _common(
        self,
        prompt: str,
        freedom: Freedom,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
    ) -> list[str]:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        # Re-checked here, the one place both start and resume funnel through, so no caller of this
        # method can reach the CLI with an effort it would silently degrade instead of honour.
        check_reasoning_effort(self, reasoning_effort)
        argv = [
            BINARY,
            "-p",
            prompt,
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

    def assert_safe(self, argv: list[str], freedom: Freedom) -> None:
        # Rejects an unknown freedom outright rather than letting PERMISSION_MODES[freedom] raise a
        # bare KeyError below.
        check_freedom(freedom)
        # The prompt is arbitrary caller text that may itself look like a flag, so only the option
        # region is inspected. Checked rather than assumed, so a reorder fails loudly.
        if len(argv) <= 3 or argv[0] != BINARY or argv[1] != "-p":
            raise UnsafeInvocationError(f"unrecognised claude argv layout: {argv!r}")
        flags = argv[3:]

        for flag in ("--verbose", "--permission-mode"):
            count = flags.count(flag)
            if count != 1:
                raise UnsafeInvocationError(f"{flag} appears {count} times: {argv!r}")

        deny_count = flags.count("--disallowedTools")
        if freedom in DENY_FREEDOMS:
            if deny_count != 1:
                raise UnsafeInvocationError(
                    f"--disallowedTools appears {deny_count} times: {argv!r}"
                )
            denied = flags[flags.index("--disallowedTools") + 1]
            if denied != DISALLOWED_TOOLS:
                raise UnsafeInvocationError(
                    f"--disallowedTools was {denied!r}, expected the deny list"
                )
        elif deny_count != 0:
            raise UnsafeInvocationError(
                f"--disallowedTools present at freedom {freedom!r}, which must not deny git "
                f"commit/push: {argv!r}"
            )

        allow_count = flags.count("--allowedTools")
        expected_allowed = ALLOWED_TOOLS.get(freedom)
        if expected_allowed is not None:
            if allow_count != 1:
                raise UnsafeInvocationError(f"--allowedTools appears {allow_count} times: {argv!r}")
            allowed = flags[flags.index("--allowedTools") + 1]
            if allowed != expected_allowed:
                raise UnsafeInvocationError(
                    f"--allowedTools was {allowed!r}, expected {expected_allowed!r} for freedom "
                    f"{freedom!r}: {argv!r}"
                )
        elif allow_count != 0:
            raise UnsafeInvocationError(
                f"--allowedTools present at freedom {freedom!r}, which this backend does not use "
                f"there: {argv!r}"
            )

        mode = flags[flags.index("--permission-mode") + 1]
        expected_mode = PERMISSION_MODES[freedom]
        if mode != expected_mode:
            raise UnsafeInvocationError(
                f"--permission-mode was {mode!r}, expected {expected_mode!r} for freedom "
                f"{freedom!r}: {argv!r}"
            )

        # Optional, unlike the flags above — but a second one, or a non-canonical value, would
        # either win silently or reach a CLI that degrades it without telling anyone.
        effort_count = flags.count("--effort")
        if effort_count > 1:
            raise UnsafeInvocationError(f"--effort appears {effort_count} times: {argv!r}")
        if effort_count == 1:
            effort = flags[flags.index("--effort") + 1]
            if effort not in EFFORTS:
                raise UnsafeInvocationError(f"unexpected --effort value {effort!r}: {argv!r}")

        for flag in FORBIDDEN_FLAGS:
            if flag in flags:
                raise UnsafeInvocationError(
                    f"{flag} would cut the dispatched agent off from the user's MCP servers and "
                    f"settings, which it is meant to inherit: {argv!r}"
                )

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
