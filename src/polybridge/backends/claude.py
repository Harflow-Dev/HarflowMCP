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
* **Live input** (claude 2.1.281, `tests/test_live_input_real.py`): with `--input-format stream-json`
  a positional prompt is silently ignored, so a live run carries its prompt as the first stdin line
  and has no `--` and no positional at all. Mid-turn messages fold into the running turn; after a
  `result` the process idles until more input or EOF. Every run is live except one with `max_turns`
  (an unmeasured combination), which keeps the classic `-- <prompt>` shape on stdin DEVNULL.
* **Partial messages** (claude 2.1.283, real captures in `tests/fixtures/claude_partial_*.jsonl`):
  `--include-partial-messages` adds `stream_event` lines to the stream-json output in both the
  classic and the live shape, and on resume. Each message opens with a `message_start` carrying the
  message id; each content block with a `content_block_start` (its `index`) and
  `content_block_delta`s — `text_delta` carries a text chunk, while `thinking_delta`,
  `signature_delta` and `input_json_delta` never carry surfaced text. Claude still emits one
  `assistant` event *per content block*, after that block's deltas and before its
  `content_block_stop`, so a streamed text chunk's identity is `(message id, block index)`.

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

import json
from pathlib import Path
from typing import Any

from . import normalize as nz
from .base import (
    EFFORTS,
    FREEDOMS,
    Accumulator,
    Capabilities,
    Enforcement,
    STDIN_DEVNULL,
    STDIN_PIPE,
    Freedom,
    Invocation,
    NetworkControl,
    ReasoningEffort,
    Status,
    check_freedom,
    check_network,
    check_reasoning_effort,
    interactive_session_id_ok,
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
# Review uses the same narrow command-prefix mechanism; validated locally,
# without a paid harness run. Raw gh api remains unapproved: method/endpoint combinations
# cannot be safely represented as one command-prefix rule.
ALLOWED_TOOLS: dict[str, str] = {
    "publish": "Bash(git commit:*),Bash(git push:*),Bash(gh pr create:*),Bash(gh pr review:*)",
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
BOOLEAN_FLAGS = ("--verbose", "--include-partial-messages")
VALUE_FLAGS = (
    "--output-format",
    "--input-format",
    "--permission-mode",
    "--disallowedTools",
    "--allowedTools",
    "--max-turns",
    "--model",
    "--effort",
    "--session-id",
    "--resume",
)

# The one `--input-format` value this backend writes, and only on a live-input run. Measured (claude
# 2.1.281): with it, a positional prompt is silently ignored (exit 0, no model call), so a live run
# carries its prompt as the first stdin line and has no `--` and no positional at all.
LIVE_INPUT_FORMAT = "stream-json"

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

# No OS sandbox and no network-controlling mechanism at all: network=True ("impose no barrier of
# our own") is accepted at every freedom because having nothing to impose genuinely delivers it,
# while network=False ("impose one") is refused outright — a silently-ignored block would look
# exactly like an enforced one. enforcement.network_access stays "not_controlled" either way:
# the surrounding environment, not polybridge, decides reachability here.
_NETWORK_CONTROL_CAVEAT = (
    "network=True is accepted as 'impose no barrier of our own', which this backend genuinely "
    "delivers by having none to impose — not a claim of reachability: "
    "enforcement.network_access stays not_controlled, and the surrounding environment decides. "
    "network=False is refused outright: there is no barrier this backend could raise, and a "
    "silently-ignored block would be indistinguishable from an enforced one"
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
    "genuine middle tier, not a fallback to bypassPermissions. Narrow gh pr review "
    "prefix is also configured; gh pr comment, raw gh api requests, merges, closes and repository mutations "
    "are not auto-approved by Polybridge. Inherited user rules can grant additional commands, "
    "and command-prefix approval is not an OS sandbox"
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


# Statuses that end a background task, on `system/task_updated`'s `patch.status` and on
# `system/task_notification`'s `status`. Measured: "completed" (both), "killed" (task_updated) and
# "stopped" (notification) on 2.1.281; the rest are the conservative remainder of the obvious
# vocabulary. An unrecognised status keeps the task open — the input pump's idle bound is what
# ends a wait that never resolves, never a guess here.
BACKGROUND_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "killed", "stopped", "cancelled", "error"}
)


# `usage` fields that count tokens for one result, so a live run's total is their sum. Every other
# `usage` field (service tier, nested breakdowns, …) takes the latest result's value.
USAGE_TOKEN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cache_creation_input_tokens",
    "cache_read_input_tokens",
)


def _is_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


# Tool name -> monitor category. `mcp__`-prefixed names (any MCP server's tool) are matched by
# prefix below rather than listed here, since the set of MCP tools is unbounded.
_CATEGORY_BY_TOOL: dict[str, str] = {
    "Read": "read",
    "NotebookRead": "read",
    "Grep": "search",
    "Glob": "search",
    "LS": "search",
    "Edit": "edit",
    "MultiEdit": "edit",
    "NotebookEdit": "edit",
    "Write": "write",
    "Bash": "shell",
    "BashOutput": "shell",
    "KillShell": "shell",
    "KillBash": "shell",
    "WebFetch": "web",
    "WebSearch": "web",
}


def _tool_category(name: Any) -> str:
    if not isinstance(name, str):
        return "other"
    if name.startswith("mcp__"):
        return "mcp"
    return _CATEGORY_BY_TOOL.get(name, "other")


def _tool_path(input_dict: dict[str, Any]) -> str | None:
    for key in ("file_path", "notebook_path", "path"):
        value = input_dict.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _tool_command(input_dict: dict[str, Any]) -> str | None:
    value = input_dict.get("command")
    return value if isinstance(value, str) and value else None


def _tool_edit(name: Any, input_dict: dict[str, Any]) -> tuple[str, str] | None:
    if name != "Edit":
        return None
    old = input_dict.get("old_string")
    new = input_dict.get("new_string")
    return (old, new) if isinstance(old, str) and isinstance(new, str) else None


def _tool_result_text(content: Any) -> Any:
    """A `tool_result` block's `content` is either a str or a list of `{"type":"text","text":…}`
    blocks — join the latter into one string. Anything else is passed through for `tool_result`'s
    own JSON-preview fallback rather than dropped."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        texts = [
            block.get("text")
            for block in content
            if isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
        ]
        return "".join(texts)
    return content


class UnsafeInvocationError(RuntimeError):
    """An argv was assembled without this backend's required guarantees."""


class ClaudeBackend:
    @staticmethod
    def workflow_observed_metadata(snapshot: dict[str, Any]) -> dict[str, Any] | None:
        path = snapshot.get("raw_stream_log")
        if not path:
            return None
        try:
            with Path(path).open(encoding="utf-8") as stream:
                for number, line in enumerate(stream):
                    if number >= 1000:
                        break
                    try:
                        event = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    if isinstance(event, dict) and event.get("type") == "system" and event.get("subtype") == "init" and isinstance(event.get("model"), str) and event["model"]:
                        return {"observed": {"model": event["model"]}, "provenance": "harness_initialization_event", "verification_status": "observed"}
        except (OSError, UnicodeError):
            pass
        return None

    @staticmethod
    def workflow_stderr_availability_failure(diagnostic: str) -> str | None:
        from .workflow_diagnostics import stderr_availability
        return stderr_availability(diagnostic, quota_patterns=("(?im)^.*(?:You've hit your limit|Credit balance is too low|rate_limit_error|model_not_found).*$",))

    @staticmethod
    def workflow_availability_failure(event: dict[str, Any]) -> str | None:
        from .workflow_diagnostics import provider_error
        info = event.get("rate_limit_info")
        if event.get("type") == "rate_limit_event" and isinstance(info, dict) and info.get("status") == "rejected":
            return "claude quota rejected"
        return provider_error(event, event_type="error", extra_transport_codes=("api_error",))

    @staticmethod
    def workflow_failure_diagnostic(event: dict[str, Any]) -> str | None:
        return None

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
        network_control=NetworkControl(
            can_enable=FREEDOMS,
            can_block=(),
            caveats=(_NETWORK_CONTROL_CAVEAT,),
        ),
        supports_live_input=True,
        # Explicit, not just the default: claude folds a mid-turn message into the running turn
        # (measured, 2.1.281 — one result however many messages landed), so one further result
        # settles the run regardless of how many messages were queued. agy is the contrast case.
        live_input_message_is_turn=False,
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
        network: bool | None = None,
    ) -> Invocation:
        if session_id is None:
            raise ValueError("claude accepts a chosen session id, so one must be supplied")
        invocation = self._invocation(
            prompt,
            ["--session-id", session_id],
            freedom=freedom,
            model=model,
            max_turns=max_turns,
            reasoning_effort=reasoning_effort,
            network=network,
        )
        self.assert_safe(invocation, freedom, network)
        return invocation

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
        network: bool | None = None,
    ) -> Invocation:
        # --resume and --session-id conflict, so never both.
        invocation = self._invocation(
            prompt,
            ["--resume", session_id],
            freedom=freedom,
            model=model,
            max_turns=max_turns,
            reasoning_effort=reasoning_effort,
            network=network,
        )
        self.assert_safe(invocation, freedom, network)
        return invocation

    def _invocation(
        self,
        prompt: str,
        session_flags: list[str],
        *,
        freedom: Freedom,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
        network: bool | None,
    ) -> Invocation:
        """Live input unless a turn cap was asked for.

        `--max-turns` together with `--input-format stream-json` has never been measured, so a
        capped run keeps the classic one-shot shape (`--` then the prompt as the sole positional,
        stdin DEVNULL) and cannot take `send_message`. Everything else is live: the prompt becomes
        the first stdin line and the pipe stays open until the pump closes it.
        """
        argv = self._common(freedom, model, max_turns, reasoning_effort, network)
        prompt = self._check_prompt(prompt)
        if max_turns is not None:
            # `--` then the prompt: last, and explicitly not parsed as an option however it looks —
            # see the note in assert_safe about why this is load-bearing for claude specifically.
            return Invocation([*argv, *session_flags, "--", prompt])
        return Invocation(
            [*argv, "--input-format", LIVE_INPUT_FORMAT, *session_flags],
            stdin_mode=STDIN_PIPE,
            initial_input=self.encode_live_message(prompt),
        )

    def encode_live_message(self, text: str) -> bytes:
        """One stream-json user line (measured shape). `json.dumps` escapes any newline in `text`,
        so a message can never split into two lines on the wire."""
        line = {
            "type": "user",
            "message": {"role": "user", "content": [{"type": "text", "text": text}]},
        }
        return (json.dumps(line, ensure_ascii=False) + "\n").encode("utf-8")


    def interactive_resume_argv(self, session_id: str, repo_path: Path) -> list[str] | None:
        """`claude --resume <id>`, run in the repository (measured: it loads the prior
        conversation; an untrusted folder shows claude's own trust dialog first)."""
        if not interactive_session_id_ok(session_id, repo_path):
            return None
        return [self.binary, "--resume", session_id]

    def _common(
        self,
        freedom: Freedom,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
        network: bool | None = None,
    ) -> list[str]:
        # Re-checked here, the one place both start and resume funnel through, so no caller of this
        # method can reach the CLI with an effort it would silently degrade instead of honour.
        check_reasoning_effort(self, reasoning_effort)
        # Validated here for the same reason: no caller can reach the CLI with a network request
        # this backend would silently ignore (False). True needs no argv change at all — there is
        # nothing to impose — which is exactly why the enforcement block, not the argv, is where
        # its acceptance is disclosed.
        check_network(self, freedom, network)
        argv = [
            BINARY,
            "-p",
            "--output-format",
            "stream-json",
            # Required, not stylistic: the CLI refuses to start without it.
            "--verbose",
            # Streams text as it arrives (measured, 2.1.283): adds `stream_event` lines in both
            # the classic and the live shape, and on resume. No other stream content changes.
            "--include-partial-messages",
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

    def assert_safe(
        self, invocation: Invocation, freedom: Freedom, network: bool | None = None
    ) -> None:
        # Rejects an unknown freedom outright rather than letting PERMISSION_MODES[freedom] raise a
        # bare KeyError below. The network request is validated here too — this is the final
        # execution seam, so an unhonourable request must fail loudly even if every earlier check
        # was bypassed. network leaves no trace in this backend's argv (there is nothing to
        # impose), so there is nothing further to check for it beyond the request itself.
        check_freedom(freedom)
        check_network(self, freedom, network)
        if not isinstance(invocation, Invocation):
            raise UnsafeInvocationError(
                f"expected an Invocation, got {type(invocation).__name__}: {invocation!r}"
            )
        argv = invocation.argv
        if argv[:2] != [BINARY, "-p"]:
            raise UnsafeInvocationError(f"unrecognised claude argv layout: {argv!r}")

        # Exactly two shapes, told apart by the separator and each validated completely — never a
        # mixture. Classic: `--` then exactly one positional (the prompt), stdin DEVNULL. Live: no
        # `--` and no positional at all, `--input-format stream-json`, a stdin pipe, and the prompt
        # as the one stream-json line in `initial_input`.
        #
        # Classic, the reason for `--`: only the option region is inspected, and everything after
        # `--` is the prompt — caller text that happens to contain a flag name must never be able to
        # satisfy a safety check, nor be read by claude itself as anything but text. An earlier
        # version of this method assumed the prompt was a fixed positional right after `-p`
        # (index 2), reasoning that `-p <value>` takes its argument the way most flags do. That
        # assumption was wrong: measured against `claude --help`, `-p`/`--print` is a *boolean* flag
        # and `prompt` is a separate declared positional (`Usage: claude [options] [command]
        # [prompt]`) — so a prompt token that happened to exactly equal a real claude option name
        # (e.g. `--dangerously-skip-permissions`) was parsed by claude as that option, not as prompt
        # text, with no separator to stop it. Confirmed live, with `--permission-mode plan` and
        # killed within seconds: without `--`, that exact shape reached claude's own parser; with
        # `--` inserted before the prompt, claude correctly treated the token after it as literal
        # prompt text and started a normal turn. So, as with opencode/codex, `--` pins the boundary
        # explicitly rather than relying on argv position.
        #
        # Live: the prompt never touches argv, so there is no boundary to pin — and a positional
        # would be silently ignored by claude under `--input-format stream-json` (measured), so one
        # is refused rather than tolerated. The strict option walk below does that: in the live
        # shape every token must be a known option or its value.
        live = "--" not in argv
        if live:
            options = argv[2:]
            self._check_live_wiring(invocation)
        else:
            options = argv[2 : argv.index("--")]
            # Positional arity, not just separator presence: claude declares exactly one
            # positional (the prompt), so anything other than exactly one token after `--` is not a
            # shape this backend ever writes.
            positionals = argv[argv.index("--") + 1 :]
            if len(positionals) != 1:
                raise UnsafeInvocationError(
                    f"expected exactly one positional argument (the prompt) after `--`, found "
                    f"{len(positionals)}: {argv!r}"
                )
            if invocation.stdin_mode != STDIN_DEVNULL or invocation.initial_input is not None:
                raise UnsafeInvocationError(
                    f"a one-shot claude argv (prompt after `--`) must run with stdin DEVNULL and no "
                    f"initial input, got stdin_mode={invocation.stdin_mode!r}: {argv!r}"
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
        # Exactly once, like --verbose: a second copy would be a shape this backend never writes,
        # and a missing one silently loses streaming without any error to notice.
        self._exactly_one(seen, "--include-partial-messages", argv)

        input_values = seen.get("--input-format", [])
        if live:
            input_format = self._exactly_one(seen, "--input-format", argv)
            if input_format != LIVE_INPUT_FORMAT:
                raise UnsafeInvocationError(
                    f"--input-format was {input_format!r}, expected {LIVE_INPUT_FORMAT!r} on a "
                    f"live-input run: {argv!r}"
                )
            # Never written together: --max-turns with live input is unmeasured, so a capped run
            # is built classic (see `_invocation`).
            if seen.get("--max-turns"):
                raise UnsafeInvocationError(
                    f"--max-turns on a live-input run, a combination this backend never builds: "
                    f"{argv!r}"
                )
        elif input_values:
            raise UnsafeInvocationError(
                f"--input-format on a one-shot argv, where claude would ignore the positional "
                f"prompt: {argv!r}"
            )

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
    def _check_live_wiring(invocation: Invocation) -> None:
        """A live argv must be wired to a pipe, with exactly one well-formed user message queued."""
        argv = invocation.argv
        if invocation.stdin_mode != STDIN_PIPE:
            raise UnsafeInvocationError(
                f"a live-input claude argv needs a stdin pipe, got stdin_mode="
                f"{invocation.stdin_mode!r}: {argv!r}"
            )
        data = invocation.initial_input
        if not isinstance(data, bytes) or not data.endswith(b"\n") or data.count(b"\n") != 1:
            raise UnsafeInvocationError(
                f"a live-input run needs its prompt as exactly one newline-terminated stdin line, "
                f"got {data!r}: {argv!r}"
            )
        try:
            message = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise UnsafeInvocationError(
                f"initial_input is not one JSON line: {data!r}"
            ) from None
        body = message.get("message") if isinstance(message, dict) else None
        content = body.get("content") if isinstance(body, dict) else None
        block = content[0] if isinstance(content, list) and len(content) == 1 else None
        well_formed = (
            isinstance(message, dict)
            and set(message) == {"type", "message"}
            and message["type"] == "user"
            and isinstance(body, dict)
            and set(body) == {"role", "content"}
            and body["role"] == "user"
            and isinstance(block, dict)
            and set(block) == {"type", "text"}
            and block["type"] == "text"
            and isinstance(block["text"], str)
            and block["text"].strip() != ""
        )
        if not well_formed:
            raise UnsafeInvocationError(
                f"initial_input is not a single non-empty stream-json user message: {data!r}"
            )

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

    def enforcement(self, freedom: Freedom, network: bool | None = None) -> Enforcement:
        # Validated here too, not only in the argv builders: `enforcement` is part of the widened
        # Backend contract, and reporting `not_controlled` for a request this backend documents as
        # an error would make the contract internally inconsistent for any caller that asks it
        # directly rather than going through the tool surface.
        check_network(self, freedom, network)
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
            # publish and unrestricted are the two freedoms that authorize a publish attempt —
            # see the field's own docstring for what that does and does not promise. A network
            # request changes nothing here: this backend has no barrier of its own either way.
            publish_attempts_allowed_by_polybridge=freedom in ("publish", "unrestricted"),
            # No sandbox at all, so nothing here ever controls network reachability — whatever
            # the caller asked for, the environment decides.
            network_access="not_controlled",
            caveats=caveats,
        )

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        session_id = event.get("session_id")
        if acc.session_id is None and isinstance(session_id, str) and session_id:
            acc.session_id = session_id

        event_type = event.get("type")
        if event_type == "system":
            self._ingest_system(event, acc)
        elif event_type in ("assistant", "user"):
            # Turn activity: whatever happened before, the agent is working now — including the
            # follow-up turn claude starts by itself when a background task finishes. A subagent's
            # own events (non-empty `parent_tool_use_id`) are not the main thread's turn: after a
            # result they can only come from a background subagent, which `background_open`
            # already tracks — letting them open the turn would keep a stalled one from ever
            # reaching the idle bound.
            parent = event.get("parent_tool_use_id")
            if not (isinstance(parent, str) and parent):
                acc.turn_open = True
                acc.awaiting_input = False
        elif event_type == "stream_event":
            # Stream activity counts as turn activity: summary, `saw_final_message`, cost and
            # result counts still come only from `assistant`/`result`, so classification is
            # unchanged — but a main-thread `stream_event` proves a turn is being produced right
            # now. Without this, a follow-up turn that starts streaming after a result reads as
            # idle to the live-input pump, and a recovered run whose new message never finished
            # would read as the earlier success. A subagent's own stream (non-empty
            # `parent_tool_use_id`) is not the main thread's turn, same rule as above.
            parent = event.get("parent_tool_use_id")
            if not (isinstance(parent, str) and parent):
                acc.turn_open = True
                acc.awaiting_input = False
        elif event_type == "result":
            self._ingest_result(event, acc)

    @staticmethod
    def _ingest_system(event: dict[str, Any], acc: Accumulator) -> None:
        subtype = event.get("subtype")
        task_id = event.get("task_id")
        task_id = task_id if isinstance(task_id, str) and task_id else None
        if subtype == "init":
            servers = event.get("mcp_servers")
            if isinstance(servers, list):
                acc.mcp_servers = servers
            tools = event.get("tools")
            if isinstance(tools, list):
                acc.available_tool_count = len(tools)
        elif subtype == "task_started":
            # A foreground Bash emits task_started too (is_backgrounded false, measured); only a
            # backgrounded one outlives its turn.
            if task_id is not None and event.get("is_backgrounded") is True:
                acc.background_open.add(task_id)
        elif subtype == "task_updated":
            patch = event.get("patch")
            patch = patch if isinstance(patch, dict) else {}
            if task_id is None:
                return
            # Unmeasured, and conservative: a task moved to the background later is open too.
            if patch.get("is_backgrounded") is True:
                acc.background_open.add(task_id)
            if patch.get("status") in BACKGROUND_TERMINAL_STATUSES:
                ClaudeBackend._close_background(task_id, acc)
        elif subtype == "task_notification":
            if task_id is not None and event.get("status") in BACKGROUND_TERMINAL_STATUSES:
                ClaudeBackend._close_background(task_id, acc)

    @staticmethod
    def _close_background(task_id: str, acc: Accumulator) -> None:
        if task_id not in acc.background_open:
            return
        acc.background_open.discard(task_id)
        # The last open background task finishing after a result makes the run idle, unless a
        # turn is running. Claude may still start a follow-up turn for it (measured); closing stdin
        # now does not stop that turn, which finishes and emits its own result (also measured).
        if not acc.background_open and acc.result_count and not acc.turn_open:
            acc.awaiting_input = True

    def _ingest_result(self, event: dict[str, Any], acc: Accumulator) -> None:
        acc.result_count += 1
        acc.turn_open = False
        acc.saw_final_message = True
        text = event.get("result")
        acc.summary = text if isinstance(text, str) else None

        is_error = bool(event.get("is_error"))
        bad = is_error or event.get("subtype") != "success"
        # The worst outcome is sticky: once a result reported an error, a later clean one (a
        # follow-up turn for a background task, say) must not overwrite what `classify` sees.
        if not acc.error_result_seen:
            acc.terminal = event
            acc.is_error = is_error
        if bad:
            acc.error_result_seen = True

        # A live-input run emits one result per turn. Measured: `total_cost_usd` is cumulative per
        # process, so it takes the latest value; `num_turns`, `usage` and `permission_denials` are
        # per result, so they accumulate. A classic run has exactly one result, which makes every
        # rule here the plain assignment it always was.
        turns = event.get("num_turns")
        if _is_count(turns):
            acc.num_turns = (acc.num_turns or 0) + turns
        cost = event.get("total_cost_usd")
        if isinstance(cost, (int, float)) and not isinstance(cost, bool):
            acc.total_cost_usd = float(cost)
        usage = event.get("usage")
        if isinstance(usage, dict):
            acc.usage = self._merge_usage(acc.usage, usage)
        denials = event.get("permission_denials")
        if isinstance(denials, list):
            acc.denials = self._union_denials(acc.denials, denials)

        acc.awaiting_input = not acc.background_open

    @staticmethod
    def _merge_usage(previous: dict[str, Any] | None, usage: dict[str, Any]) -> dict[str, Any]:
        merged = dict(previous or {})
        for key, value in usage.items():
            if key in USAGE_TOKEN_FIELDS and _is_count(value):
                prior = merged.get(key)
                merged[key] = (prior if _is_count(prior) else 0) + value
            else:
                merged[key] = value
        return merged

    @staticmethod
    def _union_denials(
        previous: list[dict[str, Any]], denials: list[Any]
    ) -> list[dict[str, Any]]:
        """Every distinct denial across results, in first-seen order."""
        union = list(previous)
        seen = {json.dumps(entry, sort_keys=True, default=str) for entry in union}
        for entry in denials:
            key = json.dumps(entry, sort_keys=True, default=str)
            if key not in seen:
                seen.add(key)
                union.append(entry)
        return union

    def normalize(self, event: dict[str, Any], acc: Accumulator) -> list[dict[str, Any]]:
        if not isinstance(event, dict):
            return []
        source_ts = nz.iso_string(event.get("timestamp"))
        parent = event.get("parent_tool_use_id")
        subagent = isinstance(parent, str) and parent != ""

        event_type = event.get("type")
        if event_type == "assistant":
            return self._normalize_assistant(event, acc, source_ts, subagent)
        if event_type == "user":
            return self._normalize_user(event, source_ts, subagent)
        if event_type == "stream_event":
            return self._normalize_stream_event(event, acc, source_ts, subagent)
        if event_type == "result":
            return [nz.usage(acc, source_ts=source_ts)]
        return []

    @staticmethod
    def _normalize_stream_event(
        event: dict[str, Any],
        acc: Accumulator,
        source_ts: str | None,
        subagent: bool,
    ) -> list[dict[str, Any]]:
        """`--include-partial-messages` stream events. Only a main-thread `text_delta` produces
        anything — one `assistant_delta` per chunk — and only main-thread events may touch the
        stream state, so a subagent's own stream can neither read as the main thread speaking nor
        overwrite the message id/block index its text blocks are identified by."""
        inner = event.get("event")
        if not isinstance(inner, dict) or subagent:
            return []
        inner_type = inner.get("type")
        if inner_type == "message_start":
            message = inner.get("message")
            message_id = message.get("id") if isinstance(message, dict) else None
            if isinstance(message_id, str) and message_id:
                acc.stream_state["normalize_stream_message_id"] = message_id
                # Block indices restart per message; a stale index from the previous one must not
                # label this message's blocks.
                acc.stream_state.pop("normalize_stream_block_index", None)
                acc.stream_state.pop("normalize_stream_block_type", None)
            return []
        if inner_type == "content_block_start":
            index = inner.get("index")
            if _is_count(index):
                block = inner.get("content_block")
                acc.stream_state["normalize_stream_block_index"] = index
                acc.stream_state["normalize_stream_block_type"] = (
                    block.get("type") if isinstance(block, dict) else None
                )
            return []
        if inner_type == "content_block_delta":
            delta = inner.get("delta")
            if not (isinstance(delta, dict) and delta.get("type") == "text_delta"):
                # thinking/signature/input_json deltas carry nothing polybridge surfaces.
                return []
            text = delta.get("text")
            if not isinstance(text, str):
                return []
            index = inner.get("index")
            message_id = acc.stream_state.get("normalize_stream_message_id")
            return [
                nz.assistant_delta(
                    text,
                    message_id=message_id if isinstance(message_id, str) else None,
                    block_index=index if _is_count(index) else None,
                    source_ts=source_ts,
                )
            ]
        return []

    @staticmethod
    def _normalize_assistant(
        event: dict[str, Any],
        acc: Accumulator,
        source_ts: str | None,
        subagent: bool,
    ) -> list[dict[str, Any]]:
        message = event.get("message")
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, list):
            return []

        # With partial streaming (measured, claude 2.1.283), `message_start` and
        # `content_block_start` preceded this event, so the message id and the index of the block
        # this event carries are known. Claude emits one `assistant` event per content block, so the
        # recorded index is unambiguous only when this event carries exactly one block; a message
        # with no preceding stream state (no flag, or a replayed capture) keeps today's shape —
        # no `message_id`, no `block_index`.
        state = acc.stream_state
        message_id = message.get("id") if isinstance(message, dict) else None
        identified = (
            isinstance(message_id, str)
            and message_id
            and message_id == state.get("normalize_stream_message_id")
        )
        block_index = None
        if identified and len(content) == 1:
            candidate = state.get("normalize_stream_block_index")
            block_index = candidate if _is_count(candidate) else None

        events: list[dict[str, Any]] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "text":
                # A subagent's own prose must not read as the main thread speaking.
                if subagent:
                    continue
                text = block.get("text")
                if isinstance(text, str):
                    events.append(
                        nz.assistant_text(
                            text,
                            source_ts=source_ts,
                            message_id=message_id if identified else None,
                            block_index=block_index,
                        )
                    )
            elif block_type == "tool_use":
                name = block.get("name")
                tool_input = block.get("input")
                input_dict = tool_input if isinstance(tool_input, dict) else {}
                events.append(
                    nz.tool_call(
                        call_id=block.get("id"),
                        tool=name,
                        category=_tool_category(name),
                        input=tool_input,
                        path=_tool_path(input_dict),
                        command=_tool_command(input_dict),
                        edit=_tool_edit(name, input_dict),
                        source_ts=source_ts,
                    )
                )
            # `thinking` blocks, and anything else: never surfaced — no thinking text, ever.
        return events

    @staticmethod
    def _normalize_user(
        event: dict[str, Any], source_ts: str | None, subagent: bool
    ) -> list[dict[str, Any]]:
        message = event.get("message")
        content = message.get("content") if isinstance(message, dict) else None

        # A str `message.content` is itself a user text, same isSynthetic/subagent gating as a
        # `text` block below.
        if isinstance(content, str):
            if subagent or event.get("isSynthetic"):
                return []
            return [nz.user_message(content, source="initial", source_ts=source_ts)]

        if not isinstance(content, list):
            return []

        events: list[dict[str, Any]] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "tool_result":
                events.append(
                    nz.tool_result(
                        call_id=block.get("tool_use_id"),
                        ok=not block.get("is_error"),
                        output=_tool_result_text(block.get("content")),
                        source_ts=source_ts,
                    )
                )
            elif block_type == "text":
                # Skill bodies injected by the harness (isSynthetic), and a subagent's own prompt
                # text, must never read as the main thread's user speaking.
                if subagent or event.get("isSynthetic"):
                    continue
                text = block.get("text")
                if isinstance(text, str):
                    events.append(nz.user_message(text, source="initial", source_ts=source_ts))
        return events

    def classify(self, acc: Accumulator, exit_code: int | None) -> Status:
        if acc.terminal is None:
            return "failed"
        # The input pump closed stdin on a run still waiting on background tasks (the idle bound),
        # which kills them: whatever the last result said, the work it started never finished.
        if acc.background_abandoned:
            return "failed"
        subtype = acc.terminal.get("subtype")
        if subtype == "error_max_turns":
            return "timed_out"
        # A non-success subtype is a failure even without `is_error` — the same test `ingest`
        # uses for "a result reported an error", so what stopped the input pump can never read
        # as a clean completion.
        if acc.is_error or subtype != "success":
            return "failed"
        # A live run can hold several results. Activity after the last one means a later turn
        # never finished, so that earlier success is stale evidence, whatever the exit code. With
        # no observed exit (a recovered run), open background work is unfinished for the same
        # reason; an observed exit has already had claude report them killed or stopped.
        if acc.turn_open:
            return "failed"
        if exit_code is None and acc.background_open:
            return "failed"
        # `exit_code is None` means nothing observed the process exit (a recovered run). Claude is
        # the one backend that does not need an observed exit: its `result` event is a real terminal
        # event, so a successful one is evidence in its own right. Only an *observed* non-zero exit
        # overrules it — `exit_code != 0` alone would read None as failure and throw that evidence
        # away.
        if exit_code is not None and exit_code != 0:
            return "failed"
        return "completed"
