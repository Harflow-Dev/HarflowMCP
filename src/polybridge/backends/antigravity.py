"""Antigravity (`agy`) backend.

CLI facts established by measurement (agy 1.2.14, 2026-10-01; real captures in
`tests/fixtures/antigravity_*.jsonl`), not assumption:

* `-p/--print` takes the prompt as its *value* — unlike claude's boolean `-p` — and a positional
  prompt, with or without `--`, is refused outright (exit 2, "Prompts are read only from
  -p/--print, -i/--prompt-interactive, or stdin"). There is therefore no separator to pin a prompt
  behind, and this backend builds no classic one-shot shape at all: every run is live, carrying
  its prompt as the first stdin line. The parser is strict — an unknown or mis-split token exits 2
  with "flags provided but not defined" — which is what the strict option walk in `assert_safe`
  mirrors.
* **Live input** (`--input-format stream-json`, one line per message:
  `{"event":"user","message":{"content":"<text>"}}`): a `--print` alongside it is refused by the CLI
  itself (exit 2), a claude-shaped line is refused (`missing the "event" field`), and **each written
  line is its own turn, even mid-turn** — unlike claude, which folds a mid-turn message into the
  running turn. `result.usage` and `num_turns` are cumulative per process (the second result's
  usage is roughly twice the first's), so `ingest` assigns them rather than summing. EOF after a
  result exits 0 in ~0.2 s; lines already written when stdin hits EOF still run, one turn each.
* `--output-format stream-json` events: `init` (carries `conversation_id` on line one), one
  `step_update` per step state change (`user_input` / `agent_response` / `tool`), and a terminal
  `result` with `status` (`SUCCESS`/`ERROR`), `response`, `num_turns`, `usage`, optional `error`
  and `denied_actions`. **No dollar cost.** Exit 0 on success, 1 on an `ERROR` result.
* **A tool step's `state` does not show a denial; `result.denied_actions` does** (a plan-mode
  `write_to_file` step read `DONE` though no file was written; only `denied_actions` said so). A
  denied command step does read `ERROR` with a permission-check message.
* **A headless denial ends the run as a success**: the first permission prompt it cannot show is
  auto-denied, the agent stops, and the run reports `status: SUCCESS`, often an empty `response`,
  exit 0. polybridge follows vibe's policy for this — completed, with a warning naming what was
  denied.
* `--conversation <id>` resumes the same id (verified), but **an unknown id is not refused**: a
  new conversation with a different id starts at exit 0. The id cannot be chosen up front
  (`--conversation <fresh-uuid>` is just the not-found case), so `chooses_session_id=False` and
  `--add-dir <repo>` puts the repository on the command line as the identity marker for a fresh
  run. agy keeps its argv as its process title, so the markers keep matching.
* `--effort low|medium|high|max`: `xhigh` is refused loudly (exit 1, `invalid --effort "xhigh"`),
  and `--effort` is refused at startup for a model id that encodes its own level or for
  `claude-sonnet-4-6` — polybridge cannot know which model ids take it, so both gaps are caveats,
  not checks.
* Freedom mapping, measured against a scratch repo with a local bare remote: default
  (`request-review`) and `--mode plan` deny workspace writes and mutating commands, while
  read-only commands run unapproved; `--mode accept-edits` auto-approves workspace writes but still
  denies mutating commands and writes outside the workspace; `--dangerously-skip-permissions`
  permitted file write, `git commit`, `git push` to the remote, `curl` (HTTP 200) and a write
  outside the repo. `--sandbox` is OS-level but command-only and measured not usable for real
  work, so no freedom maps to it.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from . import normalize as nz
from .base import (
    FREEDOMS,
    Accumulator,
    Capabilities,
    Enforcement,
    Freedom,
    Invocation,
    NetworkControl,
    ReasoningEffort,
    STDIN_PIPE,
    Status,
    UnsupportedCapability,
    check_freedom,
    check_network,
    check_reasoning_effort,
    interactive_session_id_ok,
)

BINARY = "agy"

# read_only and write_in_repo are carried by an explicit --mode; publish and unrestricted by
# skipping the permission layer entirely. The two upper freedoms deliberately share one mechanism:
# --dangerously-skip-permissions is the only thing agy offers that permits a publish (measured —
# commit and push both landed), and it removes every other restriction with it, so `publish` is a
# relabelling of `unrestricted` here, like vibe's. That collapse is pinned by a test.
MODES: dict[str, str | None] = {
    "read_only": "plan",
    "write_in_repo": "accept-edits",
    "publish": None,
    "unrestricted": None,
}

SKIP_PERMISSIONS: frozenset[str] = frozenset({"publish", "unrestricted"})

LIVE_INPUT_FORMAT = "stream-json"

# Options this backend itself ever writes — nothing more. `assert_safe` walks the whole argv
# against exactly these (agy has no positional at all on a live run) instead of searching it,
# for the same reason as claude/opencode: a search sees the canonical form this backend wrote and
# passes while agy honours a non-canonical one that rode along.
VALUE_FLAGS = (
    "--output-format",
    "--input-format",
    "--add-dir",
    "--mode",
    "--model",
    "--effort",
    "--conversation",
)
BOOLEAN_FLAGS = ("--dangerously-skip-permissions",)

# Flags that would break a guarantee this backend makes: -p/--print/--prompt put the prompt on the
# command line (and --print alongside --input-format is refused by the CLI itself, exit 2,
# measured); -i/--prompt-interactive opens a UI nothing is here to drive; -c/--continue resumes
# "the most recent conversation", which races with anything else the user is running;
# --remote-control/--new-project/--project/--agent/--json-schema change what is executed or where;
# --sandbox is deliberately unused (see _SANDBOX_UNUSED_CAVEAT); --log-file/--print-timeout/
# --disable-slash-commands are shapes this backend never writes.
REJECTED_FLAGS = (
    "-p",
    "--print",
    "--prompt",
    "-i",
    "--prompt-interactive",
    "-c",
    "--continue",
    "--remote-control",
    "--new-project",
    "--project",
    "--sandbox",
    "--agent",
    "--json-schema",
    "--log-file",
    "--print-timeout",
    "--disable-slash-commands",
)

_EFFORT_CAVEATS = (
    "measured on the default model only: three paired runs gave 'low' 0/70/0 and 'high' "
    "145/134/138 thinking tokens — non-overlapping, so the levels change behaviour there",
    "agy itself refuses a level it does not know before any model call (exit 1, "
    '`invalid --effort "xhigh"`), which is why xhigh is outside this backend\'s vocabulary',
    "--effort is refused at startup (exit 1) for a model id that encodes its own level "
    "(e.g. gemini-3.8-flash-low conflicts with --effort) and for claude-sonnet-4-6 (--effort is "
    "not supported for model) — polybridge cannot know which model ids take it, so a dispatch "
    "combining --model with --effort can still die at startup; base family names "
    "(gemini-3.8-flash, gemini-3.1-pro) accept it",
)

# No OS sandbox and no network-controlling mechanism at all: network=True ("impose no barrier of
# our own") is accepted at every freedom because having nothing to impose genuinely delivers it,
# while network=False ("impose one") is refused outright — a silently-ignored block would look
# exactly like an enforced one. enforcement.network_access stays "not_controlled" either way: the
# surrounding environment, not polybridge, decides reachability here. (agy's own permission layer
# did not stop `curl` at any freedom, so "blocked" would overclaim in the other direction too.)
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
_SETTINGS_CAVEAT = (
    "all of this is agy's own permission layer plus the user's settings.json allow-rules, which "
    "can widen it — nothing here is OS-enforced"
)
_SANDBOX_UNUSED_CAVEAT = (
    "--sandbox is deliberately unused: it is OS-level but only for commands, and measured not "
    "usable for real work (commands could not write inside the repo nor read ~/.gitconfig, yet "
    "curl reached the network and the file-writing tool wrote outside the repo; measured from "
    "inside a nested sandbox, so the in-repo denial may partly reflect that)"
)
_HEADLESS_DENIAL_CAVEAT = (
    "a headless denial ends the run as status SUCCESS at exit 0 — polybridge reports completed "
    "with a warning naming what was denied (vibe's policy). The tool step itself still reads "
    "DONE; only result.denied_actions says so, so the warning is often the only signal"
)

_PLAN_CAVEAT = (
    "read_only is agy's plan mode: workspace file writes and mutating commands (touch, git "
    "commit) are denied by agy's own permission layer, read-only commands (git status) run "
    "without approval, and plan artifacts are written under ~/.gemini/antigravity-cli/brain/"
    "<id>/ — outside the repo, and allowed"
)
_ACCEPT_EDITS_CAVEAT = (
    "accept-edits auto-approves workspace writes; a write to a path outside the workspace was "
    "denied, and mutating commands are still denied — so tests/builds cannot run unless the "
    "user's agy settings.json allow-rules permit them"
)
_SKIP_CAVEAT = (
    "--dangerously-skip-permissions removed every measured restriction: file write, git commit, "
    "git push to a local bare remote, curl (HTTP 200) and a write outside the repo all succeeded"
)
_PUBLISH_COLLAPSE_CAVEAT = (
    "identical mechanism to unrestricted (--dangerously-skip-permissions): assert_safe cannot "
    "tell these two freedoms apart from argv alone, and that collapse is deliberate"
)

_MODE_CAVEATS: dict[str, tuple[str, ...]] = {
    "read_only": (_PLAN_CAVEAT,),
    "write_in_repo": (_ACCEPT_EDITS_CAVEAT,),
    "publish": (_SKIP_CAVEAT, _PUBLISH_COLLAPSE_CAVEAT),
    "unrestricted": (_SKIP_CAVEAT,),
}


class UnsafeInvocationError(RuntimeError):
    """An argv was assembled without this backend's required guarantees."""


def _is_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _denial_notice(entry: Any) -> str:
    """The warning for one denied action — the notice is the only signal a headless denial leaves
    (the run still reports SUCCESS at exit 0), so it names what was denied."""
    if isinstance(entry, dict):
        action = entry.get("action")
        display = entry.get("display_name")
        if isinstance(action, str) and action:
            named = f"{action} ({display})" if isinstance(display, str) and display else action
            return f"agy auto-denied {named}: headless mode cannot prompt for it"
        return f"agy denied an action: {json.dumps(entry, ensure_ascii=False, default=str)}"
    return "agy denied an action: headless mode cannot prompt for it"


def _result_notices(result: dict[str, Any]) -> list[str]:
    """The notices one `result` event earns — shared by `ingest` and `normalize` so the monitor's
    notice says exactly what `ingest` already decided was worth surfacing."""
    notices: list[str] = []
    error = result.get("error")
    if result.get("status") != "SUCCESS" and isinstance(error, str) and error:
        notices.append(error)
    denied = result.get("denied_actions")
    if isinstance(denied, list):
        notices.extend(_denial_notice(entry) for entry in denied)
    return notices


# Tool name -> monitor category, per agy's own tool names. `browser_*` tools are matched by
# prefix below, since the set of browser tools is open-ended.
_CATEGORY_BY_TOOL: dict[str, str] = {
    "view_file": "read",
    "read_url_content": "read",
    "read_resource": "read",
    "grep_search": "search",
    "find_by_name": "search",
    "list_dir": "search",
    "write_to_file": "write",
    "replace_file_content": "edit",
    "multi_replace_file_content": "edit",
    "sed_file": "edit",
    "notebook_edit": "edit",
    "run_command": "shell",
    "send_command_input": "shell",
    "command_status": "shell",
    "search_web": "web",
    "open_browser_url": "web",
}


# Tools whose DONE state does not prove the change landed (see `_normalize_tool`).
_DEFERRED_CATEGORIES = frozenset({"write", "edit"})


def _tool_category(name: Any) -> str:
    if not isinstance(name, str):
        return "other"
    if name in _CATEGORY_BY_TOOL:
        return _CATEGORY_BY_TOOL[name]
    if name.startswith("browser_"):
        return "web"
    return "other"


def _tool_path(parameters: dict[str, Any]) -> str | None:
    for key in ("TargetFile", "AbsolutePath", "Path", "File"):
        value = parameters.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _tool_command(parameters: dict[str, Any]) -> str | None:
    value = parameters.get("CommandLine")
    return value if isinstance(value, str) and value else None


class AntigravityBackend:
    @staticmethod
    def workflow_availability_failure(event: dict[str, Any]) -> str | None:
        """Interpret only agy's terminal provider error, never response/tool prose."""
        result = event.get("result")
        if event.get("event") != "result" or not isinstance(result, dict) or result.get("status") != "ERROR" or result.get("denied_actions"):
            return None
        error = result.get("error")
        codes = {"insufficient_quota", "model_not_found", "rate_limit_exceeded", "usage_limit_reached"}
        outages = {"overloaded_error", "api_connection_error", "APITimeoutError", "APIConnectionError", "service_unavailable"}
        if isinstance(error, dict):
            if any(type(error.get(key)) is int and error[key] in {401, 403} for key in ("status", "status_code", "statusCode")):
                return None
            if any(error.get(key) in codes for key in ("type", "code", "name") if isinstance(error.get(key), str)):
                return "backend availability rejected"
            if any(type(error.get(key)) is int and error[key] in {500, 502, 503, 504, 529} for key in ("status", "status_code", "statusCode")):
                return "provider server unavailable"
            if any(error.get(key) in outages for key in ("type", "code", "name") if isinstance(error.get(key), str)):
                return "provider transport unavailable"
        elif isinstance(error, str):
            if re.match(r"(?i)^(?:API[ _]Error|Provider[ _]Error|HTTP[ _]Error)\s*:?\s*(?:HTTP\s*)?(?:401|403)\b", error):
                return None
            if re.search(r"\b(?:insufficient_quota|model_not_found|rate_limit_exceeded|usage_limit_reached)\b", error):
                return "backend availability rejected"
            if re.match(r"(?i)^(?:API[ _]Error|Provider[ _]Error|HTTP[ _]Error)\s*:?\s*(?:HTTP\s*)?(?:500|502|503|504|529)\b", error):
                return "provider server unavailable"
        return None

    name = "antigravity"
    binary = BINARY
    capabilities = Capabilities(
        # agy mints its conversation id itself and only discloses it on the first stream line;
        # --conversation with a fresh id is just the not-found case (a new conversation, measured).
        chooses_session_id=False,
        # No --max-turns on agy 1.2.14.
        supports_turn_cap=False,
        # Only token counts, no dollar cost (measured).
        reports_cost_usd=False,
        os_sandbox=False,
        per_command_deny=False,
        supports_model_selection=True,
        reasoning_effort=ReasoningEffort(
            accepts_parameter=True,
            levels=("low", "medium", "high"),
            native_flag="--effort",
            accepted_in_real_run=True,
            levels_change_behaviour=True,
            caveats=_EFFORT_CAVEATS,
        ),
        network_control=NetworkControl(
            can_enable=FREEDOMS,
            can_block=(),
            caveats=(_NETWORK_CONTROL_CAVEAT,),
        ),
        supports_live_input=True,
        # Measured: each written line is its own turn, even mid-turn — the pump must wait for one
        # result per message before it may consider the run idle (see tasks._note_input_written).
        live_input_message_is_turn=True,
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
        if session_id is not None:
            raise ValueError("antigravity mints its own conversation id; one cannot be supplied")
        self._reject_turn_cap(max_turns)
        prompt = self._check_prompt(prompt)
        argv = self._options(repo, freedom, model, reasoning_effort, network)
        # The only shape this backend builds is live: agy refuses a positional prompt outright,
        # so the prompt rides as the first stdin line and the pipe stays open until the pump
        # closes it.
        invocation = Invocation(
            argv,
            stdin_mode=STDIN_PIPE,
            initial_input=self.encode_live_message(prompt),
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
        if not session_id:
            raise ValueError("resuming antigravity needs the conversation id its first run reported")
        self._reject_turn_cap(max_turns)
        prompt = self._check_prompt(prompt)
        argv = [
            *self._options(repo, freedom, model, reasoning_effort, network),
            "--conversation",
            session_id,
        ]
        invocation = Invocation(
            argv,
            stdin_mode=STDIN_PIPE,
            initial_input=self.encode_live_message(prompt),
        )
        self.assert_safe(invocation, freedom, network)
        return invocation

    def _options(
        self,
        repo: Path,
        freedom: Freedom,
        model: str | None,
        reasoning_effort: str | None,
        network: bool | None = None,
    ) -> list[str]:
        # Re-checked here, the one place start and resume both funnel through, so no caller can
        # reach the CLI with an effort it would refuse at startup (agy exits 1 on `xhigh`) or a
        # network request this backend would silently ignore.
        check_reasoning_effort(self, reasoning_effort)
        check_network(self, freedom, network)
        argv = [
            BINARY,
            "--output-format",
            "stream-json",
            "--input-format",
            LIVE_INPUT_FORMAT,
            # --add-dir duplicates the spawn cwd on purpose: it is what puts the repository on
            # the command line, the only identity marker for a backend that cannot carry its
            # conversation id in argv on a fresh run. See tasks._identity_markers.
            "--add-dir",
            str(repo),
        ]
        mode = MODES[freedom]
        if mode is not None:
            argv += ["--mode", mode]
        elif freedom in SKIP_PERMISSIONS:
            argv.append("--dangerously-skip-permissions")
        if model:
            argv += ["--model", model]
        if reasoning_effort:
            argv += ["--effort", reasoning_effort]
        return argv

    def _reject_turn_cap(self, max_turns: int | None) -> None:
        if max_turns is not None:
            raise UnsupportedCapability(
                f"the antigravity CLI has no turn cap, so max_turns={max_turns} cannot be "
                "honoured; omit it rather than have it silently ignored"
            )

    @staticmethod
    def _check_prompt(prompt: str) -> str:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        return prompt

    def encode_live_message(self, text: str) -> bytes:
        """One stream-json user line (measured shape). `json.dumps` escapes any newline in `text`,
        so a message can never split into two lines on the wire — each line being its own turn is
        exactly why that matters here."""
        line = {"event": "user", "message": {"content": self._check_prompt(text)}}
        return (json.dumps(line, ensure_ascii=False) + "\n").encode("utf-8")

    def interactive_resume_argv(self, session_id: str, repo_path: Path) -> list[str] | None:
        """`agy --conversation <id>`, run in the repository (measured: resumes the same
        conversation). An unknown id is *not* refused by the CLI — it silently starts a new
        conversation — which is why the drainer reports a resumed run whose stream discloses a
        different id (see tasks._drain_stdout)."""
        if not interactive_session_id_ok(session_id, repo_path):
            return None
        return [self.binary, "--conversation", session_id]

    def assert_safe(
        self, invocation: Invocation, freedom: Freedom, network: bool | None = None
    ) -> None:
        # Rejects an unknown freedom outright rather than letting MODES[freedom] raise a bare
        # KeyError below. The network request is validated here too — this is the final execution
        # seam, so an unhonourable request must fail loudly even if every earlier check was
        # bypassed. network leaves no trace in this backend's argv (there is nothing to impose).
        check_freedom(freedom)
        check_network(self, freedom, network)
        if not isinstance(invocation, Invocation):
            raise UnsafeInvocationError(
                f"expected an Invocation, got {type(invocation).__name__}: {invocation!r}"
            )
        argv = invocation.argv
        if not argv or argv[0] != BINARY:
            raise UnsafeInvocationError(f"unrecognised antigravity argv layout: {argv!r}")

        # Every run is live, so the wiring is part of the shape, not an extra: an argv that looks
        # right but runs on DEVNULL would never receive the prompt its argv implies.
        self._check_live_wiring(invocation)

        # The whole argv is the option region — agy takes no positional on a live run, and a
        # `--print=` token (the classic shape this backend never builds) is refused by the walk
        # below as unrecognised, on top of the CLI refusing it alongside --input-format (exit 2,
        # measured).
        seen = self._parse_options(argv[1:], argv)

        fmt = self._exactly_one(seen, "--output-format", argv)
        if fmt != "stream-json":
            raise UnsafeInvocationError(
                f"--output-format was {fmt!r}, but only stream-json can be parsed into events: "
                f"{argv!r}"
            )
        input_format = self._exactly_one(seen, "--input-format", argv)
        if input_format != LIVE_INPUT_FORMAT:
            raise UnsafeInvocationError(
                f"--input-format was {input_format!r}, expected {LIVE_INPUT_FORMAT!r} — the only "
                f"shape this backend builds: {argv!r}"
            )
        if not self._exactly_one(seen, "--add-dir", argv).strip():
            raise UnsafeInvocationError(f"--add-dir names no directory: {argv!r}")

        # --mode must equal this freedom's mode exactly, and is absent exactly where the freedom
        # is carried by --dangerously-skip-permissions instead — never more, never less. Checking
        # against the exact value (not merely the set of known modes) is what stops an argv built
        # for read_only passing a check told write_in_repo.
        mode_values = seen.get("--mode", [])
        if len(mode_values) > 1:
            raise UnsafeInvocationError(f"--mode appears {len(mode_values)} times: {argv!r}")
        expected_mode = MODES[freedom]
        if expected_mode is None:
            if mode_values:
                raise UnsafeInvocationError(
                    f"--mode was {mode_values[0]!r}, but freedom {freedom!r} is carried by "
                    f"--dangerously-skip-permissions instead: {argv!r}"
                )
        elif not mode_values:
            raise UnsafeInvocationError(
                f"--mode is missing, but freedom {freedom!r} requires it: {argv!r}"
            )
        elif mode_values[0] != expected_mode:
            raise UnsafeInvocationError(
                f"--mode was {mode_values[0]!r}, expected {expected_mode!r} for freedom "
                f"{freedom!r}: {argv!r}"
            )

        skip_values = seen.get("--dangerously-skip-permissions", [])
        if len(skip_values) > 1:
            raise UnsafeInvocationError(
                f"--dangerously-skip-permissions appears {len(skip_values)} times: {argv!r}"
            )
        should_skip = freedom in SKIP_PERMISSIONS
        if bool(skip_values) != should_skip:
            raise UnsafeInvocationError(
                f"--dangerously-skip-permissions "
                f"{'is missing but freedom' if should_skip else 'contradicts freedom'} "
                f"{freedom!r}: {argv!r}"
            )

        # Optional, and caller-supplied — a second one would win silently.
        model_values = seen.get("--model", [])
        if len(model_values) > 1:
            raise UnsafeInvocationError(f"--model appears {len(model_values)} times: {argv!r}")

        # Optional, but a non-canonical value is one agy would refuse at startup (exit 1,
        # `invalid --effort "xhigh"`, measured) — polybridge must refuse it first, with its own
        # message.
        effort_values = seen.get("--effort", [])
        if len(effort_values) > 1:
            raise UnsafeInvocationError(f"--effort appears {len(effort_values)} times: {argv!r}")
        if effort_values and effort_values[0] not in self.capabilities.reasoning_effort.levels:
            raise UnsafeInvocationError(
                f"unexpected --effort value {effort_values[0]!r}: {argv!r}"
            )

        # Optional, and only ever written on a resume: it must actually name a conversation.
        conversation_values = seen.get("--conversation", [])
        if len(conversation_values) > 1:
            raise UnsafeInvocationError(
                f"--conversation appears {len(conversation_values)} times: {argv!r}"
            )
        if conversation_values and not conversation_values[0].strip():
            raise UnsafeInvocationError(f"--conversation names no conversation: {argv!r}")

    @staticmethod
    def _check_live_wiring(invocation: Invocation) -> None:
        """A live argv must be wired to a pipe, with exactly one well-formed agy user message
        queued — the measured line shape `{"event":"user","message":{"content":"<text>"}}`, which
        the CLI itself refuses to read in any other form."""
        argv = invocation.argv
        if invocation.stdin_mode != STDIN_PIPE:
            raise UnsafeInvocationError(
                f"every antigravity run takes live input, so its argv needs a stdin pipe, got "
                f"stdin_mode={invocation.stdin_mode!r}: {argv!r}"
            )
        data = invocation.initial_input
        if not isinstance(data, bytes) or not data.endswith(b"\n") or data.count(b"\n") != 1:
            raise UnsafeInvocationError(
                f"a live-input run needs its prompt as exactly one newline-terminated stdin "
                f"line, got {data!r}: {argv!r}"
            )
        try:
            message = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise UnsafeInvocationError(f"initial_input is not one JSON line: {data!r}") from None
        body = message.get("message") if isinstance(message, dict) else None
        well_formed = (
            isinstance(message, dict)
            and set(message) == {"event", "message"}
            and message["event"] == "user"
            and isinstance(body, dict)
            and set(body) == {"content"}
            and isinstance(body.get("content"), str)
            and body["content"].strip() != ""
        )
        if not well_formed:
            raise UnsafeInvocationError(
                f"initial_input is not a single non-empty agy user message: {data!r}"
            )

    @staticmethod
    def _parse_options(options: list[str], argv: list[str]) -> dict[str, list[str]]:
        """Walk the option region strictly, refusing any token this backend would not have written.

        Mirrors claude/opencode's walk and exists for the same reason: agy's parser is strict
        (`flags provided but not defined`, exit 2), and an `--flag=value` spelling this backend
        never emits is a shape whose meaning it did not choose. Every token is therefore either a
        known option, a known option's value, or a refusal.
        """
        seen: dict[str, list[str]] = {}
        index = 0
        while index < len(options):
            token = options[index]
            if token in REJECTED_FLAGS:
                raise UnsafeInvocationError(
                    f"{token} would break this backend's prompt, session or freedom guarantees: "
                    f"{argv!r}"
                )
            if token in BOOLEAN_FLAGS:
                seen.setdefault(token, []).append("")
                index += 1
            elif token in VALUE_FLAGS:
                if index + 1 >= len(options):
                    raise UnsafeInvocationError(f"{token} has no value: {argv!r}")
                value = options[index + 1]
                # A value that looks like an option is not a value: agy's parser would read it as
                # the next flag (it exits 2 on an unknown token). `model` is caller-supplied and
                # lands here, so a model named `--dangerously-skip-permissions` would otherwise
                # smuggle a full permission bypass into the region this method exists to police.
                if value.startswith("-"):
                    raise UnsafeInvocationError(
                        f"{token} was given {value!r}, which agy would parse as an option rather "
                        f"than a value: {argv!r}"
                    )
                seen.setdefault(token, []).append(value)
                index += 2
            else:
                raise UnsafeInvocationError(
                    f"unrecognised option token {token!r}: this backend writes only canonical "
                    f"space-separated options, and `--flag=value` (or a `--print=` prompt) is a "
                    f"shape it never builds: {argv!r}"
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
        # Backend contract, and reporting `not_controlled` for a request this backend documents
        # as an error would make the contract internally inconsistent.
        check_network(self, freedom, network)
        mode = MODES[freedom]
        mechanism = f"agy --mode {mode}" if mode is not None else "agy --dangerously-skip-permissions"
        return Enforcement(
            freedom=freedom,
            mechanism=mechanism,
            # agy's restrictions are its own permission layer plus the user's settings.json
            # allow-rules; --sandbox (the one OS-level thing it has) is deliberately unused.
            os_enforced=False,
            writes_confined=False,
            writable_roots=(),
            # No per-command deny list exists, so neither commit claim can be made.
            commit_push_blocked=False,
            direct_commit_commands_denied=False,
            # publish and unrestricted are the freedoms that authorize a publish attempt — see
            # the field's own docstring. On agy they are also one mechanism (the collapse pinned
            # by a test).
            publish_attempts_allowed_by_polybridge=freedom in ("publish", "unrestricted"),
            # Nothing here controls network reachability — the environment decides.
            network_access="not_controlled",
            caveats=(
                _NO_SANDBOX_CAVEAT,
                _SETTINGS_CAVEAT,
                _SANDBOX_UNUSED_CAVEAT,
                _HEADLESS_DENIAL_CAVEAT,
                *_MODE_CAVEATS[freedom],
            ),
        )

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        if not isinstance(event, dict):
            return
        kind = event.get("event")
        if kind == "init":
            conversation_id = event.get("conversation_id")
            if acc.session_id is None and isinstance(conversation_id, str) and conversation_id:
                acc.session_id = conversation_id
        elif kind == "step_update":
            self._ingest_step(event.get("step_update"), acc)
        elif kind == "result":
            self._ingest_result(event, acc)

    @staticmethod
    def _ingest_step(step: Any, acc: Accumulator) -> None:
        if not isinstance(step, dict):
            return
        conversation_id = step.get("conversation_id")
        if acc.session_id is None and isinstance(conversation_id, str) and conversation_id:
            acc.session_id = conversation_id
        # Any step is turn activity: a result has not arrived since it started, so an earlier
        # success is not the run's outcome and the live-input pump must not close stdin.
        acc.turn_open = True
        acc.awaiting_input = False

    def _ingest_result(self, event: dict[str, Any], acc: Accumulator) -> None:
        result = event.get("result")
        if not isinstance(result, dict):
            return
        acc.result_count += 1
        acc.terminal = event
        response = result.get("response")
        # Kept only when non-empty: a headless denial ends with an empty response (measured), and
        # an earlier turn's answer is a better summary than nothing.
        if isinstance(response, str) and response:
            acc.summary = response
        # Assigned, never accumulated: both are cumulative per process on agy (measured — the
        # second result's usage is roughly twice the first's, and a resumed live run reported
        # num_turns: 3), unlike claude/opencode where per-result values must be summed.
        turns = result.get("num_turns")
        if _is_count(turns):
            acc.num_turns = turns
        usage = result.get("usage")
        if isinstance(usage, dict):
            acc.usage = usage
        if result.get("status") != "SUCCESS":
            # A result reporting an error is terminal for the run (the CLI exits 1): sticky, so a
            # later result cannot overwrite it.
            acc.is_error = True
            acc.error_result_seen = True
        acc.notices.extend(_result_notices(result))
        denied = result.get("denied_actions")
        if isinstance(denied, list):
            acc.denials.extend(denied)
        # The turn that produced this result is closed; the run now owes input. Cleared again by
        # the next step_update, and by the pump when it writes a queued message.
        acc.turn_open = False
        acc.awaiting_input = True
        acc.saw_final_message = True

    def normalize(self, event: dict[str, Any], acc: Accumulator) -> list[dict[str, Any]]:
        if not isinstance(event, dict):
            return []
        kind = event.get("event")
        if kind == "step_update":
            return self._normalize_step(event.get("step_update"), acc)
        if kind == "result":
            return self._normalize_result(event.get("result"), acc)
        return []

    @staticmethod
    def _normalize_step(step: Any, acc: Accumulator) -> list[dict[str, Any]]:
        if not isinstance(step, dict):
            return []
        step_type = step.get("step_type")
        conversation_id = step.get("conversation_id")
        # A step's identity: agy carries no per-step id, so the conversation id plus its own
        # index is the stable key a tool_call/tool_result pair shares.
        call_id = f"{conversation_id}:{step.get('step_index')}"
        if step_type == "agent_response":
            return AntigravityBackend._normalize_response(step, acc, call_id, conversation_id)
        if step_type == "tool":
            return AntigravityBackend._normalize_tool(step, acc, call_id)
        return []

    @staticmethod
    def _normalize_response(
        step: dict[str, Any], acc: Accumulator, call_id: str, conversation_id: Any
    ) -> list[dict[str, Any]]:
        """One streamed answer block per `agent_response` step. Its deltas and its final
        `assistant_text` share `(call_id, 0)` so a consumer replaces the streaming entry instead
        of appending a second answer. The `DONE` update carries the step's last chunk (measured:
        `PONG` while ACTIVE, then `\\n` on DONE), so it is kept rather than dropped."""
        message_id = call_id if isinstance(conversation_id, str) else None
        texts = acc.stream_state.setdefault("normalize_step_text", {})
        delta = step.get("text_delta")
        events: list[dict[str, Any]] = []
        if isinstance(delta, str) and delta:
            texts[call_id] = texts.get(call_id, "") + delta
            events.append(nz.assistant_delta(delta, message_id=message_id, block_index=0))
        if step.get("state") == "DONE":
            text = texts.pop(call_id, "")
            if text.strip():
                acc.stream_state["normalize_turn_answered"] = True
                events.append(nz.assistant_text(text, message_id=message_id, block_index=0))
        return events

    @staticmethod
    def _normalize_tool(
        step: dict[str, Any], acc: Accumulator, call_id: str
    ) -> list[dict[str, Any]]:
        tool_info = step.get("tool_info")
        tool_info = tool_info if isinstance(tool_info, dict) else {}
        parameters = tool_info.get("parameters")
        parameters = parameters if isinstance(parameters, dict) else {}
        tool = step.get("tool_name") or tool_info.get("name")

        # agy re-emits the same step as its state moves ACTIVE -> DONE/ERROR, so the call fires
        # exactly once per step index and the result only on the terminal state — the same pairing
        # vibe's effects needed.
        seen_calls = acc.stream_state.setdefault("normalize_seen_calls", set())
        events: list[dict[str, Any]] = []
        if call_id not in seen_calls:
            seen_calls.add(call_id)
            events.append(
                nz.tool_call(
                    call_id=call_id,
                    tool=tool,
                    category=_tool_category(tool),
                    input=parameters,
                    path=_tool_path(parameters),
                    command=_tool_command(parameters),
                )
            )
        state = step.get("state")
        if state == "DONE" and _tool_category(tool) in _DEFERRED_CATEGORIES and not tool_info.get(
            "output"
        ):
            # A file write reads DONE even when agy denied it (measured, plan mode); only the
            # turn's `result.denied_actions` says so. Confirmed there instead — until then, and for
            # good if no result ever arrives, the call stays unconfirmed rather than claimed done.
            acc.stream_state.setdefault("normalize_pending_writes", []).append(call_id)
            return events
        if state in ("DONE", "ERROR"):
            error = tool_info.get("error")
            error = error if isinstance(error, dict) else {}
            output = tool_info.get("output")
            if not isinstance(output, str) or not output:
                message = error.get("message")
                output = message if isinstance(message, str) and message else None
            events.append(
                nz.tool_result(
                    call_id=call_id,
                    ok=state == "DONE" and not error,
                    output=output,
                )
            )
        return events

    @staticmethod
    def _normalize_result(result: Any, acc: Accumulator) -> list[dict[str, Any]]:
        if not isinstance(result, dict):
            return []
        events: list[dict[str, Any]] = []
        denied = result.get("denied_actions")
        denied_actions = {
            entry.get("action") for entry in denied if isinstance(entry, dict)
        } if isinstance(denied, list) else set()
        # denied_actions names a permission, not a step, so a turn with one denied write marks
        # every pending write in it failed. Under-reporting an edit is the safe direction: the
        # alternative is claiming a file was written when it was not. A turn that ended in an
        # error with no write denial proves neither outcome, so its writes stay unconfirmed.
        pending_writes = acc.stream_state.pop("normalize_pending_writes", [])
        if "write_file" in denied_actions:
            events.extend(
                nz.tool_result(
                    call_id=pending, ok=False, output="denied by agy's permission layer"
                )
                for pending in pending_writes
            )
        elif result.get("status") == "SUCCESS":
            events.extend(
                nz.tool_result(call_id=pending, ok=True, output=None) for pending in pending_writes
            )
        # The answer normally arrives through the agent_response steps above; the result's own
        # response is the fallback for a turn that streamed none.
        answered = acc.stream_state.pop("normalize_turn_answered", False)
        response = result.get("response")
        if not answered and isinstance(response, str) and response.strip():
            events.append(nz.assistant_text(response))
        events.extend(nz.notice(text) for text in _result_notices(result))
        events.append(nz.usage(acc))
        return events

    def classify(self, acc: Accumulator, exit_code: int | None) -> Status:
        # A result reporting an error is a real failure (the CLI exits 1 on an ERROR result).
        if acc.is_error or acc.error_result_seen:
            return "failed"
        # An *observed* non-zero exit overrules everything else; None (a recovered run) does not,
        # because agy's `result` event is a real terminal event — the same evidence claude's is.
        if exit_code is not None and exit_code != 0:
            return "failed"
        # The input pump closed stdin on a run still waiting on background tasks (the idle bound),
        # which kills them: whatever the last result said, the work it started never finished.
        if acc.background_abandoned:
            return "failed"
        if acc.result_count == 0:
            return "failed"
        # Activity after the last result means a later turn never finished, so an earlier success
        # is stale evidence, whatever the exit code.
        if acc.turn_open:
            return "failed"
        # A result carrying denied_actions and a clean exit reads completed — the notice naming
        # what was denied is the warning (vibe's policy, decided by the owner).
        return "completed"
