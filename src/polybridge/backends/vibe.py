"""Mistral vibe backend.

CLI facts established by measurement (vibe 2.25.1), not assumption — see also the plan that
introduced this module and CLAUDE.md's per-CLI section.

**Invocation and stdin**
* `get_prompt_from_stdin()` runs unconditionally before mode dispatch and reads `sys.stdin` whenever
  it is not a tty, blocking forever on a pipe that never closes. Small prompts use DEVNULL;
  prompts over 32 KiB use one-shot UTF-8 stdin followed immediately by EOF and trailing `--prompt=`
  to select programmatic mode. Native stdin ingestion strips outer whitespace: large prompts with
  outer whitespace fail explicitly rather than being altered. Start and resume use this transport.
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
  `state.status: "cancelled"`, and then **ended with no assistant message at all, at exit 0**. The
  `callback` is therefore the only thing that can explain how the turn ended, and `ingest` records
  it on `acc.denials` (surfaced as `permission_denials`); without that the caller is never told a
  command was refused. Since 2026-09-26 such a turn no longer reads as a failure by itself: the
  withdrawal of the turn's close (a pre-refusal narration is not an answer) is accompanied by
  `stream_state["ended_on_refusal"]` and one outcome-neutral warning on `acc.notices`, and a run
  that ends this way at an *observed* zero exit reports **`completed` with that warning** rather
  than `failed` with an empty summary — the refusal says the agent was stopped, not that the run
  failed, and a real dispatch whose every edit had landed was reported `failed` only because its
  last action was refused. A later assistant message in the turn clears the flag and removes only
  that warning; a new turn resets both with the other turn-scoped fields; the denial itself stays
  on `acc.denials` either way. Every genuine failure signal still wins in `classify` — an error,
  a non-zero or unobserved exit, a turn-cap breach (checked explicitly, so a denial cannot
  complete a turn that also breached its cap). The accepted trade-off, stated so it is not
  rediscovered as a bug: this reintroduces the 2026-09-22 false success — a run that narrates, is
  refused, changes nothing and exits 0 reads `completed`; the warning is the signal, and for an
  editing task the worktree is the check.
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
import json
import os
import tomllib
from pathlib import Path
from typing import Any

from . import normalize as nz
from .mcp_approval import VibeApproval
from .base import (
    FREEDOMS,
    Accumulator,
    Capabilities,
    Enforcement,
    Freedom,
    Invocation,
    NetworkControl,
    ReasoningEffort,
    Status,
    UnsupportedCapability,
    check_freedom,
    check_network,
    check_reasoning_effort,
    interactive_session_id_ok,
    classic_invocation_problem,
    reject_model,
)

# Large prompts use the native EOF stdin path with --prompt= selecting programmatic mode.
# Installed Vibe get_prompt_from_stdin strips outer whitespace, so oversized inputs with
# such whitespace are refused rather than silently changed. Both start and resume use
# the same pipe_once transport; its stdin closes immediately after the full UTF-8 payload.
BINARY = "vibe"
ARGV_PROMPT_BYTES = 32 * 1024

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
    @staticmethod
    def workflow_observed_metadata(snapshot: dict[str, Any]) -> dict[str, Any] | None:
        """Read Vibe's own session config snapshot, never worker self-report text."""
        session_id = snapshot.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            return None
        home = Path(os.environ.get("VIBE_HOME", str(Path.home() / ".vibe"))).expanduser()
        directory = home / "logs" / "session"
        try:
            config_path = home / "config.toml"
            if config_path.is_file():
                config = tomllib.loads(config_path.read_text(encoding="utf-8"))
                logging = config.get("session_logging")
                if isinstance(logging, dict) and isinstance(logging.get("save_dir"), str) and logging["save_dir"]:
                    directory = Path(logging["save_dir"]).expanduser()
            # Current Vibe writes a unified store. Resolve only its committed generation,
            # and verify the full native session identity before trusting model metadata.
            if re.fullmatch(r"[A-Za-z0-9_-]+", session_id):
                unified = directory / "unified" / session_id
                current_path = unified / "CURRENT"
                if current_path.is_file() and current_path.stat().st_size <= 1024 * 1024:
                    current = json.loads(current_path.read_text(encoding="utf-8"))
                    generation = current.get("generation") if isinstance(current, dict) else None
                    if isinstance(current, dict) and current.get("session_id") == session_id and isinstance(generation, str) and re.fullmatch(r"[0-9]{16}", generation):
                        state_path = unified / "generations" / generation / "runtime-state.json"
                        if state_path.stat().st_size <= 8 * 1024 * 1024:
                            state = json.loads(state_path.read_text(encoding="utf-8"))
                            metadata = state.get("session_metadata", {}) if isinstance(state, dict) else {}
                            active = metadata.get("active_model") if isinstance(metadata, dict) else None
                            if isinstance(state, dict) and state.get("session_id") == session_id and isinstance(active, str) and active:
                                observed = {"active_model": active, "model": active}
                                if isinstance(metadata.get("reasoning_effort"), str):
                                    observed["reasoning_effort"] = metadata["reasoning_effort"]
                                return {"observed": observed, "provenance": "harness_session_configuration", "verification_status": "observed_configuration", "metadata_source": str(state_path)}
            # Session folder names include the first eight characters of their ID.
            # Still verify the complete native ID before accepting metadata.
            paths = sorted(directory.glob("*" + session_id[:8] + "*/meta.json"), reverse=True)[:20]
            for path in paths:
                if path.stat().st_size > 1024 * 1024:
                    continue
                metadata = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(metadata, dict) or metadata.get("session_id") != session_id:
                    continue
                config = metadata.get("config")
                active = config.get("active_model") if isinstance(config, dict) else None
                if not isinstance(active, str) or not active:
                    continue
                observed = {"active_model": active, "model": active}
                models = config.get("models", [])
                if isinstance(models, list):
                    model = next((m for m in models if isinstance(m, dict) and m.get("alias") == active), None)
                    if model:
                        if isinstance(model.get("name"), str) and model["name"]:
                            observed["model"] = model["name"]
                        if isinstance(model.get("thinking"), str):
                            observed["reasoning_effort"] = model["thinking"]
                        if isinstance(model.get("provider"), str):
                            observed["provider"] = model["provider"]
                return {"observed": observed, "provenance": "harness_session_configuration", "verification_status": "observed_configuration", "metadata_source": str(path)}
        except (OSError, ValueError, UnicodeError):
            pass
        return None

    @staticmethod
    def workflow_stderr_blocks_availability_failure(diagnostic: str) -> bool:
        from .workflow_diagnostics import stderr_blocks_availability
        return stderr_blocks_availability(diagnostic)

    @staticmethod
    def workflow_stderr_availability_failure(diagnostic: str) -> str | None:
        from .workflow_diagnostics import stderr_availability
        return stderr_availability(diagnostic, model_patterns=(r"(?im)^.*\bmodel_not_found\b.*$",))

    @staticmethod
    def workflow_availability_failure(event: dict[str, Any]) -> str | None:
        from .workflow_diagnostics import provider_error
        return provider_error(event, event_type="error")

    @staticmethod
    def usage_limit_diagnostic(event: dict[str, Any]) -> dict[str, Any] | None:
        from .workflow_diagnostics import usage_limit
        return usage_limit(event, envelope='error', claude=False, agy=False)

    @staticmethod
    def stderr_usage_limit_diagnostic(line: str) -> dict[str, Any] | None:
        from .workflow_diagnostics import stderr_usage_limit
        return stderr_usage_limit(line, claude=False)

    name = "vibe"
    mcp_approval = VibeApproval()
    binary = BINARY
    # Programmatic Vibe replaces argv with this title after startup. Caller
    # verification still requires the captured process start time and ancestry.
    caller_process_titles = ("Vibe CLI",)
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
        network_control=NetworkControl(
            can_enable=FREEDOMS,
            can_block=(),
            caveats=(_NETWORK_CONTROL_CAVEAT,),
        ),
        supports_live_input=False,
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
            raise ValueError("vibe mints its own session id; one cannot be supplied")
        argv = [
            BINARY,
            *self._options(repo, freedom, model, max_turns, reasoning_effort, network),
        ]
        # The single canonical token, last: no `--` separator works here (see module docstring), so
        # this is the only thing standing between prompt text and being parsed as an option.
        invocation = self._prompt_invocation(argv, prompt)
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
            raise ValueError("resuming vibe needs the session id its first run reported")
        argv = [
            BINARY,
            *self._options(repo, freedom, model, max_turns, reasoning_effort, network),
        ]
        argv += ["--resume", session_id]
        invocation = self._prompt_invocation(argv, prompt)
        self.assert_safe(invocation, freedom, network)
        return invocation

    def _options(
        self,
        repo: Path,
        freedom: Freedom,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
        network: bool | None = None,
    ) -> list[str]:
        reject_model(self, model)
        # accepts_parameter=False turns any non-None value into UnsupportedCapability; no vibe-side
        # plumbing is needed beyond that shared check.
        check_reasoning_effort(self, reasoning_effort)
        # Validated here so no caller can reach the CLI with a network request this backend
        # would silently ignore (False). True needs no argv change at all — there is nothing to
        # impose — which is exactly why the enforcement block, not the argv, is where its
        # acceptance is disclosed.
        check_network(self, freedom, network)
        # --workdir also puts the repo path on the command line, which is what
        # tasks._identity_markers needs for a backend that mints its own session id — the same
        # reason opencode passes --dir.
        options = [
            "--output", "streaming", "--trust", "--agent", AGENTS[freedom], "--workdir", str(repo),
        ]
        if max_turns is not None:
            options += ["--max-turns", str(max_turns)]
        return options

    def _prompt_invocation(self, argv: list[str], prompt: str) -> Invocation:
        prompt = self._check_prompt(prompt)
        if len(prompt.encode("utf-8")) > ARGV_PROMPT_BYTES:
            if prompt != prompt.strip():
                raise UnsupportedCapability("Vibe trims outer whitespace from stdin prompts. Remove outer whitespace or choose another harness for this large assignment.")
            return Invocation([*argv, "--prompt="], stdin_mode="pipe_once", initial_input=prompt.encode("utf-8"))
        return Invocation([*argv, f"--prompt={prompt}"])

    @staticmethod
    def _check_prompt(prompt: str) -> str:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        return prompt

    def assert_safe(
        self, invocation: Invocation, freedom: Freedom, network: bool | None = None
    ) -> None:
        # One-shot stdin is closed after delivery, never a live conversation pipe.
        one_shot = isinstance(invocation, Invocation) and invocation.stdin_mode == "pipe_once"
        if one_shot:
            try:
                prompt = invocation.initial_input.decode("utf-8") if isinstance(invocation.initial_input, bytes) else ""
            except UnicodeError:
                prompt = ""
            if not prompt or prompt != prompt.strip() or invocation.argv[-1:] != ["--prompt="]:
                raise UnsafeInvocationError("Vibe one-shot stdin requires exact nonempty UTF-8 input, no outer whitespace, and an empty trailing --prompt=")
            problem = None
        else:
            problem = classic_invocation_problem(invocation)
        if problem is not None:
            raise UnsafeInvocationError(problem)
        argv = invocation.argv
        # Rejects an unknown freedom outright rather than letting AGENTS[freedom] raise a bare
        # KeyError below. The network request is validated here too — this is the final
        # execution seam, so an unhonourable request must fail loudly even if every earlier check
        # was bypassed. network leaves no trace in this backend's argv (there is nothing to
        # impose), so there is nothing further to check for it beyond the request itself.
        check_freedom(freedom)
        check_network(self, freedom, network)
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
        if len(argv) < 2 or prompt_value is None or (not one_shot and not prompt_value.strip()):
            raise UnsafeInvocationError(
                f"refusing to run vibe without a non-empty --prompt=<text> as the very last token: "
                f"{argv!r}"
            )
        if not one_shot and len(prompt_value.encode("utf-8")) > ARGV_PROMPT_BYTES:
            raise UnsafeInvocationError("Vibe large assignments require one-shot stdin transport")
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

    def enforcement(self, freedom: Freedom, network: bool | None = None) -> Enforcement:
        # Validated here too, not only in the argv builders: `enforcement` is part of the widened
        # Backend contract, and reporting `not_controlled` for a request this backend documents as
        # an error would make the contract internally inconsistent for any caller that asks it
        # directly rather than going through the tool surface.
        check_network(self, freedom, network)
        return Enforcement(
            freedom=freedom,
            mechanism=f"vibe --agent {AGENTS[freedom]}",
            os_enforced=False,
            writes_confined=False,
            writable_roots=(),
            commit_push_blocked=False,
            direct_commit_commands_denied=False,
            # publish and unrestricted are the two freedoms that authorize a publish attempt —
            # see the field's own docstring; the agent profile in use (auto-approve, identical
            # for both here) is what leaves no barrier of polybridge's own against one. A
            # network request changes nothing here: this backend has no barrier either way.
            publish_attempts_allowed_by_polybridge=freedom in ("publish", "unrestricted"),
            network_access="not_controlled",
            caveats=(_NO_SANDBOX_CAVEAT, *_MODE_CAVEATS[freedom]),
        )

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        diagnostic = self.usage_limit_diagnostic(event)
        if diagnostic is not None and acc.failure_diagnostic is None:
            acc.failure_diagnostic = diagnostic
            acc.error_result_seen = True
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
            # of these and the run then ended with no assistant message at exit 0. Without recording
            # it the caller cannot tell why the run ended where it did, which is exactly the
            # inference CLAUDE.md says belongs in the payload instead.
            if not _is_approval_callback(event):
                # Not a refusal at all — a selection request, or some future callback kind. It
                # neither belongs on `denials` nor says anything about whether the run finished,
                # so it must not withdraw the close below. Sharing one predicate with
                # `_record_denial` is what keeps those two decisions from drifting apart: an
                # earlier version withdrew unconditionally here while `_record_denial` filtered,
                # so a `selection` callback after an answer produced `failed` with nothing on
                # `denials` to explain it — worse than the bug being fixed.
                return
            _record_denial(event, acc)
            # Withdraw any close this turn had already recorded, the same way the stop-event branch
            # below does — because a message that arrived *before* a refusal was narration of work
            # the agent then never got to do, not an answer. Measured on a dispatch that changed
            # zero files: it narrated its next step, had bash denied, and never spoke again, so
            # the narration must not be allowed to stand in for an answer.
            #
            # Deliberately NOT latched, which is the one way this differs from the stop-event
            # branch. A turn-cap breach is terminal, so nothing after it can re-establish a clean
            # close. A refusal is not: an agent denied one incidental command may work around it
            # and genuinely finish, and a later assistant message in this turn re-establishes the
            # close and clears the flag below. The denial itself stays on `acc.denials` either
            # way — recovery does not erase that it was refused.
            #
            # Since 2026-09-26 a turn that ends on a refusal and then exits 0 reports `completed`
            # with a warning, not `failed` with no summary: the refusal says the agent was stopped,
            # not that the run failed, and a real dispatch whose every edit had landed was reported
            # `failed` only because its last action was refused. So the withdrawal is accompanied
            # by `ended_on_refusal` and one warning on `acc.notices` (see `_record_refusal_warning`
            # for why there is at most one, and why its wording is outcome-neutral). Every genuine
            # failure signal still wins in `classify`: an error, a non-zero or unobserved exit, and
            # a turn-cap breach. The accepted trade-off: a run that narrates, is refused, changes
            # nothing and exits 0 reads `completed` again — the warning is the signal, and for an
            # editing task the worktree is the check.
            acc.summary = None
            acc.saw_final_message = False
            _record_refusal_warning(acc, event)
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
            # Turn-scoped like the latch above: a refusal in the turn that just ended must not
            # colour this one. The warning itself leaves with the rest of `notices` below.
            acc.stream_state.pop("ended_on_refusal", None)
            acc.stream_state.pop("refusal_warning", None)
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

        # A genuine answer, so the agent recovered from the refusal if there was one: this turn no
        # longer ended on it. The flag and its warning are cleared here — the warning would
        # otherwise misdescribe a clean close — while the denial itself stays on `acc.denials`.
        _clear_refusal_warning(acc)
        acc.summary = text
        acc.saw_final_message = True

    def normalize(self, event: dict[str, Any], acc: Accumulator) -> list[dict[str, Any]]:
        if not isinstance(event, dict):
            return []
        # Gate on EVERY entry type, not just messages: replayed history (turnId null, or a stale
        # id from a turn that already ended) must produce nothing — including effects and
        # callbacks — exactly like the gate `ingest` applies to assistant messages above.
        current_turn_id = acc.stream_state.get("current_turn_id")
        turn_id = event.get("turnId")
        if current_turn_id is None or not _is_live_turn_id(turn_id) or turn_id != current_turn_id:
            return []

        source_ts = nz.iso_from_epoch_ms(event.get("createdAt"))
        entry_type = event.get("type")
        if entry_type == "message":
            return self._normalize_message(event, source_ts)
        if entry_type == "effect":
            return self._normalize_effect(event, source_ts, acc)
        if entry_type == "callback":
            return self._normalize_callback(event, source_ts)
        # reasoning, checkpoint, and anything else: never surfaced.
        return []

    @staticmethod
    def _normalize_message(event: dict[str, Any], source_ts: str | None) -> list[dict[str, Any]]:
        role = event.get("role")
        text = _entry_text(event)
        if text is None:
            return []
        if role == "user":
            if event.get("source") != "turn_start":
                return []
            return [nz.user_message(text, source="initial", source_ts=source_ts)]
        if role == "assistant":
            if _STOP_EVENT_PATTERN.fullmatch(text.strip()) is not None:
                return [nz.notice(text, source_ts=source_ts)]
            return [nz.assistant_text(text, source_ts=source_ts)]
        return []

    @staticmethod
    def _normalize_effect(
        event: dict[str, Any], source_ts: str | None, acc: Accumulator
    ) -> list[dict[str, Any]]:
        """One `tool_call` the first time this effect's `id` is seen, one `tool_result` the first
        time its `state.status` is observed terminal (`completed`/`failed`/`cancelled`).

        vibe re-emits the same effect `id` as it moves from a non-terminal status to a terminal
        one (measured), so both are deduped against per-task `stream_state` sets rather than
        assumed to fire once each: the Timeline must never gain a second row for one effect, and a
        `tool_result` must never fire twice for one `call_id`. An id already terminal the first
        time it is seen yields both events, in call-then-result order.
        """
        detail = event.get("detail")
        detail = detail if isinstance(detail, dict) else {}
        tool = detail.get("toolName")
        tool_input = detail.get("input")
        input_dict = tool_input if isinstance(tool_input, dict) else {}
        kind = detail.get("kind")
        category = _EFFECT_CATEGORY_BY_KIND.get(kind, "other") if isinstance(kind, str) else "other"
        call_id = event.get("id")

        events: list[dict[str, Any]] = []
        seen_calls = acc.stream_state.setdefault("normalize_seen_effect_calls", set())
        if call_id not in seen_calls:
            seen_calls.add(call_id)
            events.append(
                nz.tool_call(
                    call_id=call_id,
                    tool=tool,
                    category=category,
                    input=tool_input,
                    path=_effect_path(input_dict),
                    command=_effect_command(input_dict),
                    edit=_effect_edit(kind, input_dict),
                    source_ts=source_ts,
                )
            )

        state = event.get("state")
        status = state.get("status") if isinstance(state, dict) else None
        if status in _TERMINAL_EFFECT_STATUSES:
            seen_results = acc.stream_state.setdefault("normalize_seen_effect_results", set())
            if call_id not in seen_results:
                seen_results.add(call_id)
                events.append(
                    nz.tool_result(
                        call_id=call_id,
                        ok=status == "completed",
                        output=_effect_result_output(state),
                        source_ts=source_ts,
                    )
                )
        return events

    @staticmethod
    def _normalize_callback(event: dict[str, Any], source_ts: str | None) -> list[dict[str, Any]]:
        if not _is_approval_callback(event):
            return []
        description = _callback_description(event)
        text = f"auto-denied: {description}" if description else "auto-denied"
        return [nz.notice(text, source_ts=source_ts)]

    def encode_live_message(self, text: str) -> bytes:
        raise UnsupportedCapability(
            f"the {self.name} backend has no live input, so a message cannot be added to a running "
            "task; continue its session with resume_task instead"
        )


    def interactive_resume_argv(self, session_id: str, repo_path: Path) -> list[str] | None:
        """`vibe --trust --workdir <repo> --resume <id>` (measured: loads the prior conversation,
        no prompts)."""
        if not interactive_session_id_ok(session_id, repo_path):
            return None
        return [self.binary, "--trust", "--workdir", str(repo_path), "--resume", session_id]

    def classify(self, acc: Accumulator, exit_code: int | None) -> Status:
        if acc.failure_diagnostic is not None:
            return "failed"
        # No terminal event exists, so the exit code is the authority and the closing message is
        # only corroboration — the same shape as codex, and for the same reason an *observed* zero
        # exit is mandatory: with `exit_code is None` (a recovered run nothing saw exit) neither a
        # closing assistant message nor a turn that ended on a refusal proves the run finished.
        # Spelled out rather than left to `!= 0` incidentally rejecting None.
        #
        # Every failure signal wins first: an error, a non-observed or non-zero exit, and the
        # turn-cap latch — checked explicitly here rather than left to the withdrawal alone, so a
        # denial in a turn that also breached its cap cannot complete it. Only then does a turn
        # that ended on a refusal complete at a clean exit (decided 2026-09-26): the refusal says
        # the agent was stopped, not that the run failed, so such a run reports `completed` with the
        # warning `_record_refusal_warning` placed on `notices`, rather than `failed` with no
        # summary. The accepted trade-off: a run that narrates, is refused, changes nothing and
        # exits 0 reads `completed` — the warning is the signal, and for an editing task the
        # worktree is the check.
        if acc.is_error:
            return "failed"
        if exit_code is None or exit_code != 0:
            return "failed"
        if acc.stream_state.get("stop_event_seen"):
            return "failed"
        if acc.saw_final_message or acc.stream_state.get("ended_on_refusal"):
            return "completed"
        return "failed"


def _is_approval_callback(event: dict[str, Any]) -> bool:
    """Whether this callback is an auto-denied approval — a refusal that already happened.

    The single source of truth for that question: `ingest` uses it to decide whether to withdraw
    the turn's close, and `_record_denial` to decide whether to report one. Any other kind is a
    different sort of request and means nothing about whether the run finished.
    """
    detail = event.get("detail")
    return isinstance(detail, dict) and detail.get("kind") == "approval"


def _record_denial(event: dict[str, Any], acc: Accumulator) -> None:
    """Normalise one auto-denied approval callback onto `acc.denials`."""
    if not _is_approval_callback(event):
        return
    detail = event.get("detail")
    detail = detail if isinstance(detail, dict) else {}

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
    # A recognised approval always lands, even with every descriptive field malformed or missing.
    # `ingest` has already withdrawn the turn's close and raised the refusal warning on the strength
    # of the same `kind`, so dropping the entry here would leave that outcome with an empty
    # `permission_denials` — the payload contradicting the signal the status was derived from. The fallback repeats only
    # what was actually observed rather than inventing a tool, command or title for it.
    acc.denials.append(denial or {"kind": "approval"})


def _refusal_warning_text(event: dict[str, Any]) -> str:
    """The one warning a live-turn refusal places on `acc.notices`.

    The description is the command the approval would have run, else the event's title, else
    "an action" — a recognised approval can carry neither command nor title, and the warning must
    not be keyed on the description's presence (`_callback_description` then returns None).

    The wording is outcome-neutral by design: it is written at refusal time, before anything
    could know how the run ends, so it can also reach running snapshots and runs that later fail
    without claiming an outcome it does not have.
    """
    description = _callback_description(event) or "an action"
    return (
        f"An action was refused (`{description}`) and no assistant response followed it. A clean "
        "exit is reported completed despite this; for an editing task, check the worktree — the "
        "requested changes may not all have landed."
    )


def _record_refusal_warning(acc: Accumulator, event: dict[str, Any]) -> None:
    """Mark the turn as ended on a refusal, keeping exactly one warning for it on `acc.notices`.

    A second trailing refusal replaces the first — the last refused action is the one that
    matters. Replacement and removal touch only that warning; unrelated notices stay, in order.
    The exact text is tracked in `stream_state` so the warning can later be removed precisely (by
    a recovering assistant message, or a new turn) without disturbing anything else in the list.
    The flag itself is a boolean, never derived from the description: a recognised approval with
    no usable metadata must still count as a refusal.
    """
    _clear_refusal_warning(acc)
    warning = _refusal_warning_text(event)
    acc.stream_state["ended_on_refusal"] = True
    acc.stream_state["refusal_warning"] = warning
    acc.notices.append(warning)


def _clear_refusal_warning(acc: Accumulator) -> None:
    """Clear the refusal flag and remove exactly the tracked warning from `acc.notices`, if any.

    The denial itself stays on `acc.denials`: clearing the warning says the turn no longer ended
    on the refusal, not that it never happened.
    """
    warning = acc.stream_state.pop("refusal_warning", None)
    acc.stream_state.pop("ended_on_refusal", None)
    if warning is not None:
        try:
            acc.notices.remove(warning)
        except ValueError:
            # A new turn already reset `notices` wholesale; there is nothing left to remove.
            pass


def _callback_description(event: dict[str, Any]) -> str | None:
    """What an auto-denied approval callback was for — the command it would have run if present,
    else the event's own `title`. Command first because the title is generic ("Allow bash?") and
    says nothing about what was refused. Same fields `_record_denial` reads, for the same reasons."""
    detail = event.get("detail")
    detail = detail if isinstance(detail, dict) else {}
    effect = detail.get("effect")
    effect = effect if isinstance(effect, dict) else {}
    command = effect.get("input")
    command = command.get("command") if isinstance(command, dict) else None
    if isinstance(command, str) and command:
        return command
    title = event.get("title")
    return title if isinstance(title, str) and title else None


# An effect's `state.status` values that mean it is done, one way or another (measured: vibe's own
# vocabulary for a settled effect). Anything else (`in_progress`, absent state) is still running.
_TERMINAL_EFFECT_STATUSES: frozenset[str] = frozenset({"completed", "failed", "cancelled"})


# An `effect`'s `detail.kind` -> monitor category.
_EFFECT_CATEGORY_BY_KIND: dict[str, str] = {
    "file_read": "read",
    "file_search": "search",
    "file_edit": "edit",
    "file_write": "write",
    "shell": "shell",
}


def _effect_path(input_dict: dict[str, Any]) -> str | None:
    for key in ("filePath", "path"):
        value = input_dict.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _effect_command(input_dict: dict[str, Any]) -> str | None:
    value = input_dict.get("command")
    return value if isinstance(value, str) and value else None


def _effect_edit(kind: Any, input_dict: dict[str, Any]) -> tuple[str, str] | None:
    if kind != "file_edit":
        return None
    old = input_dict.get("oldString")
    new = input_dict.get("newString")
    return (old, new) if isinstance(old, str) and isinstance(new, str) else None


def _effect_result_output(state: dict[str, Any]) -> Any:
    """An effect's result payload for `tool_result.output`: its `output` on success, else its
    `error` (a failed/cancelled effect carries the latter instead, measured)."""
    if "output" in state and state["output"] is not None:
        return state["output"]
    return state.get("error")


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
