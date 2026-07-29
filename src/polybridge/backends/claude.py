"""Claude Code backend.

CLI facts established by measurement, not assumption:

* `-p --output-format stream-json` refuses to start without `--verbose`.
* `--disallowedTools` is variadic, so its patterns must be one comma-separated value or the option
  swallows whatever follows it.
* `--max-turns` works but is undocumented in `--help`.
* Deny rules *do* take precedence over `bypassPermissions`, and the matcher decomposes `&&` chains —
  but not `git -C … commit` or `bash -c 'git commit'`. There is no OS sandbox at all.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .base import Accumulator, Capabilities, Enforcement, Freedom, Status

BINARY = "claude"

# One comma-separated value on purpose: the option is variadic.
DISALLOWED_TOOLS = "Bash(git commit:*),Bash(git push:*)"

# Flags that would cut a dispatched agent off from the user's own MCP servers, settings, hooks and
# CLAUDE.md. That inheritance is the point — a dispatched agent should be as capable as one the user
# runs themselves — so these are refused rather than merely unused.
FORBIDDEN_FLAGS = ("--strict-mcp-config", "--setting-sources", "--safe-mode", "--bare")

PERMISSION_MODES: dict[str, str] = {
    "read_only": "plan",
    "write_in_repo": "acceptEdits",
    "unrestricted": "bypassPermissions",
}

_NO_SANDBOX_CAVEAT = (
    "no OS sandbox: the agent can read and write outside repo_path, which is only its working "
    "directory"
)
_DENY_PATTERN_CAVEAT = (
    "the commit/push deny patterns are evaded by `git -C <path> commit` and `bash -c 'git commit'` "
    "(measured), so they stop ordinary work, not deliberate circumvention"
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
    "unrestricted": (
        "bypassPermissions removes even that gate: writes anywhere the user can write will succeed",
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
    ) -> list[str]:
        if session_id is None:
            raise ValueError("claude accepts a chosen session id, so one must be supplied")
        argv = self._common(prompt, freedom, model, max_turns)
        argv += ["--session-id", session_id]
        self.assert_safe(argv)
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
    ) -> list[str]:
        argv = self._common(prompt, freedom, model, max_turns)
        # --resume and --session-id conflict, so never both.
        argv += ["--resume", session_id]
        self.assert_safe(argv)
        return argv

    def _common(
        self, prompt: str, freedom: Freedom, model: str | None, max_turns: int | None
    ) -> list[str]:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
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
            "--disallowedTools",
            DISALLOWED_TOOLS,
        ]
        if max_turns is not None:
            argv += ["--max-turns", str(max_turns)]
        if model:
            argv += ["--model", model]
        return argv

    def assert_safe(self, argv: list[str]) -> None:
        # The prompt is arbitrary caller text that may itself look like a flag, so only the option
        # region is inspected. Checked rather than assumed, so a reorder fails loudly.
        if len(argv) <= 3 or argv[0] != BINARY or argv[1] != "-p":
            raise UnsafeInvocationError(f"unrecognised claude argv layout: {argv!r}")
        flags = argv[3:]

        for flag in ("--verbose", "--disallowedTools", "--permission-mode"):
            count = flags.count(flag)
            if count != 1:
                raise UnsafeInvocationError(f"{flag} appears {count} times: {argv!r}")

        denied = flags[flags.index("--disallowedTools") + 1]
        if denied != DISALLOWED_TOOLS:
            raise UnsafeInvocationError(f"--disallowedTools was {denied!r}, expected the deny list")

        mode = flags[flags.index("--permission-mode") + 1]
        if mode not in set(PERMISSION_MODES.values()):
            raise UnsafeInvocationError(f"unexpected --permission-mode {mode!r}")

        for flag in FORBIDDEN_FLAGS:
            if flag in flags:
                raise UnsafeInvocationError(
                    f"{flag} would cut the dispatched agent off from the user's MCP servers and "
                    f"settings, which it is meant to inherit: {argv!r}"
                )

    def enforcement(self, freedom: Freedom) -> Enforcement:
        mode = PERMISSION_MODES[freedom]
        return Enforcement(
            freedom=freedom,
            mechanism=f"claude --permission-mode {mode} + deny patterns ({DISALLOWED_TOOLS})",
            # Claude's restrictions are applied by the agent's own permission layer.
            os_enforced=False,
            writes_confined=False,
            writable_roots=(),
            # Deliberately False. The deny patterns refuse the obvious invocations but are evaded by
            # `git -C` and `bash -c` (measured), so committing is discouraged, not prevented — and a
            # boolean that says "blocked" would be a promise this cannot keep.
            commit_push_blocked=False,
            direct_commit_commands_denied=True,
            caveats=(_NO_SANDBOX_CAVEAT, _DENY_PATTERN_CAVEAT, *_MODE_CAVEATS[freedom]),
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

    def classify(self, acc: Accumulator, exit_code: int) -> Status:
        if acc.terminal is None:
            return "failed"
        if acc.terminal.get("subtype") == "error_max_turns":
            return "timed_out"
        if acc.is_error or exit_code != 0:
            return "failed"
        return "completed"
