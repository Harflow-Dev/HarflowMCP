"""Mistral vibe backend.

CLI facts established by measurement (vibe 2.25.1), not assumption — see also the plan that
introduced this module and CLAUDE.md's per-CLI section.

**Invocation and stdin**
* `get_prompt_from_stdin()` runs unconditionally before mode dispatch and reads `sys.stdin` whenever
  it is not a tty, blocking forever on a pipe that never closes. `stdin=DEVNULL` is mandatory, same
  reason as codex.
* Programmatic mode is `-p/--prompt TEXT` with `--output streaming`. **The `PROMPT` positional is
  ignored in programmatic mode** — the CLI reads `args.prompt or stdin_prompt` — so there is no
  working `--` separator of the kind codex and opencode rely on. `-p` is `nargs="?", const=""`, so a
  prompt starting with `-` is not taken as its value and the run falls through to stdin (DEVNULL),
  dying with "No prompt provided". Hence the single canonical token `--prompt=<text>`, positioned
  last: measured — a prompt beginning `--max-turns` reached the model verbatim at exit 0, unparsed as
  a flag.
* Exit codes are clean: 0 on success; 1 for a limit breach, a teleport error, or any
  RuntimeError/ValueError, message on stderr, never in the JSON stream.
* Untrusted workspaces warn and continue in programmatic mode rather than prompting, but `--trust` is
  passed anyway so project configuration is honoured rather than silently ignored.

**The stream**
* `--output streaming` emits only `PublicHistoryEntry` objects with `generation_status == COMPLETED`.
  Observed `type`s: `message` (role `user`|`assistant`, a `content` parts list), `reasoning` (`text`),
  `checkpoint`, `effect` (a tool call), `callback` (an approval request). Unknown types are ignored,
  not fatal.
* **An auto-denied approval leaves no other trace, measured.** A `git commit` run against a repo
  whose `[tools.bash]` allowlist did not cover it emitted a `callback`
  (`detail.kind: "approval"`, carrying the tool name and the exact command), then an `effect` with
  `state.status: "cancelled"`, and then **ended with no assistant message at all, at exit 0** — so
  `classify` reports `failed` with an empty summary. The `callback` is therefore the only thing that
  can explain the failure, and `ingest` records it on `acc.denials` (surfaced as
  `permission_denials`); without that the caller is told a run failed but never why.
* No terminal event, no dollar cost, no token counts — `reports_cost_usd=False`, and
  `total_cost_usd`/`usage` stay `None`. `num_turns` also stays `None`: counting `turn_start` records
  would not correspond to what `--max-turns` actually caps, and a wrong number is worse than none.
* `sessionId` is on every entry including the first.
* **History replay on resume, measured.** A `--resume` run emits, in order: the prior user message,
  the prior `reasoning`, the prior assistant message, a `checkpoint`, and only then the live turn.
  Replayed entries carry `turnId: null` and `source: "harness"`; the live turn's user message carries
  `source: "turn_start"` with a real `turnId`, and later assistant entries in that turn share it. A
  fresh run has no replay and its first user message already has a real `turnId`. `ingest` is
  marker-first (waits for a `turn_start` before accepting anything) rather than treating `turnId is
  None` as the discriminator, so it still holds if a future vibe stamps ids on replayed entries too.
* Hitting `--max-turns`/`--max-price`/`--max-tokens` raises `ProgrammaticLimitError` → exit 1: a
  breach reads as **failed**, not a clean stop, unlike claude. **Gate 1, measured**
  (`--max-turns 1` on a prompt needing a tool call): exit 1, stderr
  `<vibe_stop_event>Turn limit of 1 reached</vibe_stop_event>` — and the stream's live-turn
  `assistant` message carries that same marker text. A naive ingest would set `saw_final_message`
  from it and report the marker as the agent's answer; `ingest` instead recognises the marker only
  when the *entire* message is the stop-event envelope (a legitimate answer that merely quotes or
  discusses the token, as this module's own docstring does, must still be preserved), and records it
  as a notice.

  Recognising it is not enough on its own. An earlier step in the same turn may already have
  recorded ordinary assistant text, so merely *declining* to add the marker leaves
  `saw_final_message` still asserting a clean close, and the marker itself would be reported as the
  agent's answer. So the evidence is **withdrawn** (`summary` cleared, `saw_final_message` reset)
  and **latched** in `stream_state`, so nothing later in the turn can re-establish it; the latch is
  turn-scoped like the rest of that state.

  This was originally load-bearing for a second reason that no longer applies: `store.py` used to
  substitute exit code `0` for a run it never saw exit, so a recovered breach would have been
  published as `completed`. `classify` now takes `int | None` and this backend requires an
  *observed* zero exit, so that path is closed at the source. The withdrawal stays because it is
  independently correct — the marker is not an answer and must not be reported as the summary — not
  because it is compensating for that bug.

  `is_error` is deliberately *not* set from this. A real breach already exits 1, so the flag adds
  nothing there, while setting it would make a false positive at exit 0 unrecoverable.

  The accepted trade-off: an assistant message whose *entire* content is exactly a stop-event
  envelope is treated as a breach even at exit 0, so a legitimate answer consisting of nothing but
  that envelope would be discarded. That is judged vanishingly unlikely against the recovery hole it
  closes — stated here rather than left implied.

**Freedom levers** (`vibe/core/agents/models.py`)
* `--agent plan` sets `write_file`/`edit` to `permission: "never"` (allowlisted only inside the plans
  dir) — a real refusal. **But `bash` is untouched**, falling back to the user's own `[tools.bash]`
  config; a user with `permission = "always"` there could still write via `bash`. So
  `writes_confined` is **False** at every freedom.
* `--agent accept-edits` auto-approves `write_file`/`edit` only. `--agent auto-approve` sets
  `bypass_tool_permissions: True`. Every approval callback is otherwise auto-denied in programmatic
  mode, so anything not pre-approved by the agent profile or the user's own config simply fails.
* **No OS sandbox**: `os_sandbox=False`, hence `os_enforced=False` at every freedom. Whether `git
  commit` is even reachable depends on the user's `[tools.bash]` allowlist, not anything polybridge
  sets — so `per_command_deny=False`, `commit_push_blocked=False`,
  `direct_commit_commands_denied=False`.

**Why model and reasoning effort are unsupported**
vibe has no `--model` flag and no effort flag at all — both are config-only
(`[[models]].thinking`, vocabulary `off/low/medium/high/max`). Resolving either safely would require
reading vibe's config-layer precedence (lowest to highest: schema defaults, GrowthBook experiments,
user TOML, **project TOML** (outranks the user's own, and `--trust` honours it), `VIBE_*` env vars,
runtime overrides, **agent-profile overrides** (always in effect here, since `--agent` is always
passed), enforced admin config) — several of which outrank an env override and are not inspectable
from here. A silent no-op — the override landing on the wrong model, or being overridden by a layer
polybridge cannot see — is exactly the failure mode this repo exists to prevent, so both are refused
outright (`supports_model_selection=False`,
`reasoning_effort=ReasoningEffort(accepts_parameter=False, ...)`) rather than attempted unsafely. A
faithful effort translation, if this is ever revisited, is `low->low, medium->medium, high->high,
xhigh->max` — vibe's own OpenAI-responses backend maps its `max` to `xhigh`.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .base import (
    Accumulator,
    Capabilities,
    Enforcement,
    Freedom,
    ReasoningEffort,
    Status,
    check_freedom,
    check_reasoning_effort,
    reject_model,
)

BINARY = "vibe"

AGENTS: dict[str, str] = {
    "read_only": "plan",
    "write_in_repo": "accept-edits",
    # Not accept-edits: measured, a `git commit` under accept-edits produced an approval `callback`
    # and was auto-denied (the user's [tools.bash] allowlist did not cover it), and no commit
    # landed. auto-approve is the only agent profile that can publish here, which makes `publish`
    # identical to `unrestricted` on vibe — see the module docstring and _MODE_CAVEATS["publish"].
    "publish": "auto-approve",
    "unrestricted": "auto-approve",
}

# Options this backend itself ever writes. The walker below refuses any token not in this canonical
# space-separated form, mirroring opencode/codex, for the same reason: a search-based check would
# miss an attached or aliased form this CLI still honours.
BOOLEAN_FLAGS = ("--trust",)
VALUE_FLAGS = ("--output", "--agent", "--workdir", "--max-turns", "--resume")

# Flags that would break a guarantee this backend makes, or widen what the run can touch.
# `-c`/`--continue` resumes whatever ran last on this machine, not this task's conversation;
# `--teleport` is hidden and pushes commits to a remote; `--worktree` moves the run off the repo and
# breaks tasks._identity_markers, which needs repo_path on the command line; `--add-dir` widens file
# access and implicitly trusts that directory; `--yolo`/`--auto-approve` are rejected so
# `--agent auto-approve` stays the one canonical spelling of unrestricted; `--smart-approve` and
# `--experimental-harness` force a different harness and silently rewrite the agent
# (`entrypoint.py:213-217`); `--legacy-harness` changes the same contract; `--setup` and
# `--check-upgrade` are unrelated CLI modes with no place in a headless dispatch.
REJECTED_FLAGS = (
    "-c",
    "--continue",
    "--teleport",
    "--worktree",
    "--add-dir",
    "--yolo",
    "--auto-approve",
    "--smart-approve",
    "--experimental-harness",
    "--legacy-harness",
    "--setup",
    "--check-upgrade",
)

_NO_SANDBOX_CAVEAT = (
    "no OS sandbox: os_sandbox=False at every freedom, and nothing here confines writes to "
    "repo_path, which is only the working directory"
)
_PLAN_CAVEAT = (
    "read_only is --agent plan: write_file and edit get permission \"never\" (allowlisted only "
    "inside the plans dir) — a real refusal, not model restraint. But bash is untouched, so it "
    "falls back to the user's own [tools.bash] config; a user with permission = \"always\" there "
    "could still write via bash"
)
_ACCEPT_EDITS_CAVEAT = (
    "write_in_repo is --agent accept-edits: write_file and edit are auto-approved, but nothing "
    "confines them to repo_path, and bash is governed by the user's own [tools.bash] config just as "
    "in plan mode"
)
_AUTO_APPROVE_CAVEAT = (
    "unrestricted is --agent auto-approve, which sets bypass_tool_permissions: True — every "
    "approval callback that programmatic mode would otherwise auto-deny is skipped instead"
)

_PUBLISH_COLLAPSE_CAVEAT = (
    "identical mechanism to unrestricted: --agent auto-approve is the only agent profile that can "
    "publish here (measured: accept-edits left a git commit auto-denied, no commit landed), so "
    "publish is not narrower than unrestricted on vibe — assert_safe cannot tell these two "
    "freedoms apart from argv alone, and that collapse is deliberate"
)

_MODE_CAVEATS: dict[str, tuple[str, ...]] = {
    "read_only": (_PLAN_CAVEAT,),
    "write_in_repo": (_ACCEPT_EDITS_CAVEAT,),
    "publish": (_AUTO_APPROVE_CAVEAT, _PUBLISH_COLLAPSE_CAVEAT),
    "unrestricted": (_AUTO_APPROVE_CAVEAT,),
}

_EFFORT_CAVEAT = (
    "effort is config-only ([[models]].thinking, off/low/medium/high/max) with no CLI flag at all. "
    "It is not exposed here because vibe's config-layer precedence — lowest to highest: schema "
    "defaults, GrowthBook experiments, user TOML, project TOML (outranks the user's own, and "
    "--trust honours it), VIBE_* env vars, runtime overrides, agent-profile overrides (always in "
    "effect here, since --agent is always passed), enforced admin config — means an env override "
    "keyed off the user's own config could silently apply to the wrong model, or be overridden by a "
    "layer polybridge cannot inspect. A faithful translation, if this is ever revisited, is "
    "low->low, medium->medium, high->high, xhigh->max — vibe's own OpenAI-responses backend maps "
    "its max to xhigh"
)


# Matches only when the *whole* stripped message is the envelope — not merely contains it — so a
# legitimate answer that quotes or discusses the token inside a longer sentence is never mistaken
# for a --max-turns breach (Gate 1).
_STOP_EVENT_PATTERN = re.compile(r"\A<vibe_stop_event>.*</vibe_stop_event>\Z", re.DOTALL)


def _is_live_turn_id(value: Any) -> bool:
    """A turn id worth trusting: real `turnId`s are non-empty strings. A dict or list would pass a
    bare truthiness check and could become — or wrongly match — the "current" live turn."""
    return isinstance(value, str) and value != ""


class UnsafeInvocationError(RuntimeError):
    """An argv was assembled without this backend's required guarantees."""


class VibeBackend:
    name = "vibe"
    binary = BINARY
    capabilities = Capabilities(
        # vibe mints its own sessionId and reports it on the first stream entry.
        chooses_session_id=False,
        # Measured (Gate 1): a breach exits 1 and the live-turn assistant message carries the same
        # <vibe_stop_event> marker `ingest` must not mistake for a real answer — see classify().
        supports_turn_cap=True,
        reports_cost_usd=False,
        os_sandbox=False,
        per_command_deny=False,
        supports_model_selection=False,
        reasoning_effort=ReasoningEffort(
            accepts_parameter=False,
            levels=(),
            native_flag="",
            accepted_in_real_run=False,
            levels_change_behaviour=False,
            caveats=(_EFFORT_CAVEAT,),
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
        if session_id is not None:
            raise ValueError("vibe mints its own session id; one cannot be supplied")
        argv = [BINARY, *self._options(repo, freedom, model, max_turns, reasoning_effort)]
        # The single canonical token, last: no `--` separator works here (see module docstring), so
        # this is the only thing standing between prompt text and being parsed as an option.
        argv.append(f"--prompt={self._check_prompt(prompt)}")
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
        if not session_id:
            raise ValueError("resuming vibe needs the session id its first run reported")
        argv = [BINARY, *self._options(repo, freedom, model, max_turns, reasoning_effort)]
        argv += ["--resume", session_id]
        argv.append(f"--prompt={self._check_prompt(prompt)}")
        self.assert_safe(argv, freedom)
        return argv

    def _options(
        self,
        repo: Path,
        freedom: Freedom,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
    ) -> list[str]:
        reject_model(self, model)
        # accepts_parameter=False turns any non-None value into UnsupportedCapability; no vibe-side
        # plumbing is needed beyond that shared check.
        check_reasoning_effort(self, reasoning_effort)
        # --workdir also puts the repo path on the command line, which is what
        # tasks._identity_markers needs for a backend that mints its own session id — the same
        # reason opencode passes --dir.
        options = [
            "--output", "streaming", "--trust", "--agent", AGENTS[freedom], "--workdir", str(repo),
        ]
        if max_turns is not None:
            options += ["--max-turns", str(max_turns)]
        return options

    @staticmethod
    def _check_prompt(prompt: str) -> str:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        return prompt

    def assert_safe(self, argv: list[str], freedom: Freedom) -> None:
        # Rejects an unknown freedom outright rather than letting AGENTS[freedom] raise a bare
        # KeyError below.
        check_freedom(freedom)
        if not argv or argv[0] != BINARY:
            raise UnsafeInvocationError(f"unrecognised vibe argv layout: {argv!r}")

        # There is no `--` separator on this backend (see module docstring): the prompt positional
        # is ignored in programmatic mode, so the only thing that stops prompt text being parsed as
        # an option is that it rides on a single trailing --prompt=<text> token, excluded below.
        # This is the final execution seam (re-run at spawn time in tasks.py), so it must require a
        # genuinely non-empty prompt itself rather than trust the argv builders' earlier check.
        prompt_token = argv[-1] if argv else ""
        prompt_value = (
            prompt_token[len("--prompt=") :] if prompt_token.startswith("--prompt=") else None
        )
        if len(argv) < 2 or prompt_value is None or not prompt_value.strip():
            raise UnsafeInvocationError(
                f"refusing to run vibe without a non-empty --prompt=<text> as the very last token: "
                f"{argv!r}"
            )
        options = argv[1:-1]
        seen = self._parse_options(options, argv)

        output = self._exactly_one(seen, "--output", argv)
        if output != "streaming":
            raise UnsafeInvocationError(
                f"--output was {output!r}, but only streaming can be parsed into events: {argv!r}"
            )

        if len(seen.get("--trust", ())) != 1:
            raise UnsafeInvocationError(
                f"expected exactly one --trust, without which project configuration may be "
                f"silently ignored: {argv!r}"
            )

        agent = self._exactly_one(seen, "--agent", argv)
        expected_agent = AGENTS[freedom]
        if agent != expected_agent:
            raise UnsafeInvocationError(
                f"--agent was {agent!r}, expected {expected_agent!r} for freedom {freedom!r}: "
                f"{argv!r}"
            )

        if not self._exactly_one(seen, "--workdir", argv).strip():
            raise UnsafeInvocationError(f"--workdir names no directory: {argv!r}")

        max_turns_values = seen.get("--max-turns", ())
        if len(max_turns_values) > 1:
            raise UnsafeInvocationError(
                f"--max-turns appears {len(max_turns_values)} times: {argv!r}"
            )
        if max_turns_values:
            value = max_turns_values[0]
            if not value.isdigit() or int(value) < 1:
                raise UnsafeInvocationError(
                    f"--max-turns must be a canonical positive integer, got {value!r}: {argv!r}"
                )

        # At most one, non-empty when present — the same shape as opencode's session flag. assert_safe
        # sees only argv, with no way to know whether the call it is guarding was meant to be a start
        # or a resume (there is no subcommand token here the way `codex exec resume` has one), so it
        # can only police well-formedness: a resume naming no session is worse than one refused
        # outright, and a duplicate would let a second value win silently.
        resumes = seen.get("--resume", ())
        if len(resumes) > 1:
            raise UnsafeInvocationError(f"--resume appears {len(resumes)} times: {argv!r}")
        if resumes and not resumes[0].strip():
            raise UnsafeInvocationError(f"--resume names no session: {argv!r}")

    @staticmethod
    def _parse_options(options: list[str], argv: list[str]) -> dict[str, list[str]]:
        """Walk the option region strictly, refusing any token this backend would not have written.

        Mirrors OpencodeBackend/CodexBackend._parse_options: a search for e.g. `"--output" in
        options` would miss an attached `--output=streaming` or an alias vibe still honours, so
        anything not in canonical space-separated form is refused rather than skipped over.
        """
        seen: dict[str, list[str]] = {}
        index = 0
        while index < len(options):
            token = options[index]
            if token in REJECTED_FLAGS:
                raise UnsafeInvocationError(
                    f"{token} would break this backend's session, trust or harness guarantees: "
                    f"{argv!r}"
                )
            if token in BOOLEAN_FLAGS:
                seen.setdefault(token, []).append("")
                index += 1
            elif token in VALUE_FLAGS:
                if index + 1 >= len(options):
                    raise UnsafeInvocationError(f"{token} has no value: {argv!r}")
                value = options[index + 1]
                # A value starting with "-" is not a value: vibe's parser would read it as the next
                # option. `model` is caller-supplied in other backends but is always rejected before
                # reaching here on vibe, so this mainly guards --resume/--workdir/--max-turns against
                # a flag eating the next flag.
                if value.startswith("-"):
                    raise UnsafeInvocationError(
                        f"{token} was given {value!r}, which vibe would parse as an option rather "
                        f"than a value: {argv!r}"
                    )
                seen.setdefault(token, []).append(value)
                index += 2
            else:
                raise UnsafeInvocationError(
                    f"unrecognised option token {token!r}: this backend writes only canonical "
                    f"space-separated options, and a non-canonical form could override one of them "
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
        return Enforcement(
            freedom=freedom,
            mechanism=f"vibe --agent {AGENTS[freedom]}",
            os_enforced=False,
            writes_confined=False,
            writable_roots=(),
            commit_push_blocked=False,
            direct_commit_commands_denied=False,
            # publish and unrestricted are the two freedoms where the agent profile in use
            # (auto-approve, identical for both here) leaves no barrier of polybridge's own
            # against a commit/push/PR attempt.
            publish_attempts_allowed_by_polybridge=freedom in ("publish", "unrestricted"),
            network_access="not_controlled",
            caveats=(_NO_SANDBOX_CAVEAT, *_MODE_CAVEATS[freedom]),
        )

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        session_id = event.get("sessionId")
        if acc.session_id is None and isinstance(session_id, str) and session_id:
            acc.session_id = session_id

        entry_type = event.get("type")
        turn_id = event.get("turnId")
        current_turn_id = acc.stream_state.get("current_turn_id")

        if entry_type == "callback":
            # Gated on an established, matching live turn — exactly like assistant messages below.
            # Without this, a callback replayed from prior history on a --resume run (no turn_start
            # yet, or one carrying a stale/foreign turnId) would be reported as a denial of the *new*
            # task rather than ignored as history.
            if current_turn_id is None or not _is_live_turn_id(turn_id) or turn_id != current_turn_id:
                return
            # Programmatic mode auto-denies every approval request, so a callback is a *refusal* that
            # already happened. Measured: `git commit` outside the user's bash allowlist produced one
            # of these and the run then ended with no assistant message at exit 0 — `failed` with no
            # summary. Without recording it the caller is told the run failed but never why, which is
            # exactly the inference CLAUDE.md says belongs in the payload instead.
            _record_denial(event, acc)
            return

        if entry_type != "message":
            # reasoning, checkpoint, effect, and anything a future vibe adds: non-fatal, ignored.
            return

        role = event.get("role")

        if role == "user" and event.get("source") == "turn_start" and _is_live_turn_id(turn_id):
            # Marker-first: this is what makes a turn "live", stronger than treating turnId is None
            # as the discriminator — it still holds if a future vibe stamps ids on replayed entries.
            if turn_id == current_turn_id:
                # Idempotent: the same marker arriving twice must not wipe out an answer already
                # recorded for this turn.
                return
            # A genuinely new turn. Reset every turn-scoped field — summary, saw_final_message,
            # is_error, notices, denials — so nothing from the turn that just ended (an error, a
            # notice, a denial) leaks into the outcome of this one. The stop-event latch is
            # turn-scoped for the same reason.
            acc.stream_state["current_turn_id"] = turn_id
            acc.stream_state.pop("stop_event_seen", None)
            acc.summary = None
            acc.saw_final_message = False
            acc.is_error = None
            acc.notices = []
            acc.denials = []
            return

        if role != "assistant":
            return

        if current_turn_id is None or not _is_live_turn_id(turn_id) or turn_id != current_turn_id:
            # No turn has started yet, or this is a replayed/stale entry from a different turn —
            # replayed entries carry turnId: null and must never leak into summary.
            return

        text = _entry_text(event)
        if text is None:
            return

        if _STOP_EVENT_PATTERN.fullmatch(text.strip()) is not None:
            # A --max-turns breach (Gate 1), not a genuine answer — but only when the *entire*
            # message is the envelope. A legitimate answer that merely quotes or discusses the token
            # inside a longer message must be preserved, not discarded.
            #
            # Neither of the two simpler options works, for different reasons. *Accepting* the
            # marker as an answer reports the envelope itself as the run's summary. *Ignoring* it
            # leaves whatever an earlier step in this turn already recorded standing as the summary,
            # with `saw_final_message` still asserting a clean close. So the evidence is withdrawn,
            # and latched, rather than either added to or merely skipped. (This once also covered a
            # recovery hole where store.py fabricated a zero exit; `classify` now takes
            # `int | None` and this backend requires an observed zero exit, so the withdrawal stands
            # on its own merits.)
            acc.stream_state["stop_event_seen"] = True
            acc.summary = None
            acc.saw_final_message = False
            acc.notices.append(text)
            return

        if acc.stream_state.get("stop_event_seen"):
            # Anything after the breach in the same turn cannot re-establish a clean close.
            return

        acc.summary = text
        acc.saw_final_message = True

    def classify(self, acc: Accumulator, exit_code: int | None) -> Status:
        # No terminal event exists, so the exit code is the authority and the closing message is
        # only corroboration — the same shape as codex, and for the same reason an *observed* zero
        # exit is mandatory: with `exit_code is None` (a recovered run nothing saw exit) a closing
        # assistant message proves the agent spoke, not that the run finished. Spelled out rather
        # than left to `!= 0` incidentally rejecting None.
        if acc.is_error:
            return "failed"
        if exit_code is None or exit_code != 0:
            return "failed"
        return "completed" if acc.saw_final_message else "failed"


def _record_denial(event: dict[str, Any], acc: Accumulator) -> None:
    """Normalise one auto-denied approval callback onto `acc.denials`.

    Only `kind: "approval"` callbacks are refusals; any other kind is a different sort of request and
    is left alone rather than reported as something the agent was stopped from doing.
    """
    detail = event.get("detail")
    detail = detail if isinstance(detail, dict) else {}
    if detail.get("kind") != "approval":
        return

    effect = detail.get("effect")
    effect = effect if isinstance(effect, dict) else {}
    tool = effect.get("toolName")
    command = effect.get("input")
    command = command.get("command") if isinstance(command, dict) else None
    title = event.get("title")

    denial: dict[str, Any] = {
        key: value
        for key, value in (("tool", tool), ("command", command), ("title", title))
        if isinstance(value, str) and value
    }
    if denial:
        acc.denials.append(denial)


def _entry_text(event: dict[str, Any]) -> str | None:
    """Join a message entry's text parts, or None if there is nothing usable."""
    content = event.get("content")
    if not isinstance(content, list):
        return None
    parts = [
        part.get("text")
        for part in content
        if isinstance(part, dict) and isinstance(part.get("text"), str)
    ]
    if not parts:
        return None
    return "".join(parts)
