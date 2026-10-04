"""opencode backend.

CLI facts established by capturing real runs, not assumption:

* `opencode run --format json` emits one JSON object per line, with **no non-JSON noise** in any run
  observed. Five event types matter::

      {"type":"step_start","sessionID":"ses_00b8…","part":{…}}
      {"type":"tool_use","sessionID":"ses_00b8…","part":{"tool":"write","state":{"status":"completed",…}}}
      {"type":"text","sessionID":"ses_00b8…","part":{"type":"text","text":"…"}}
      {"type":"step_finish","sessionID":"ses_00b8…","part":{"reason":"stop","tokens":{…},"cost":0.0148}}
      {"type":"error","sessionID":"ses_00b8…","error":{"name":"UnknownError","data":{"message":"…"}}}

* `sessionID` is on **every** event including the first, so the id is known immediately — unlike
  Codex, which discloses its thread id only once the run has started.
* **`cost` on `step_finish` is per step, not cumulative.** A three-step run reported 0.0583, 0.0149
  and 0.0148 — the run cost 0.0881. Taking the last value would understate it fourfold, so `ingest`
  accumulates. Token counts are per step for the same reason and are summed alongside.
* `step_finish.reason` is `"tool-calls"` for intermediate steps and `"stop"` for the last one, which
  is the only trustworthy end-of-run signal: a `text` part can appear at any step, so "saw some text"
  cannot distinguish a finished run from one killed mid-flight.
* A top-level `error` event comes with **exit code 1**. There is no terminal *success* event beyond
  `reason: "stop"`.
* `--` is honoured: a prompt beginning `--auto --format json --dir /etc` was passed through as text
  and changed nothing about the invocation.
* Its parser also accepts `--flag=value` and compact `-sID`, and **a later value wins**: appending
  `--format=default` to an argv that already carried `--format json` disabled the JSON stream
  entirely (measured — the output came back as coloured prose). That is why `assert_safe` walks the
  option region and refuses anything not in canonical space-separated form, rather than searching it.
* `-s <id>` resumes into the **same** session id — verified by comparing the ids across two runs.
* No grandchild process: 1.18.3 runs its server in-process, so the only pid is the CLI leader.

* Assignments over 32 KiB use one-shot UTF-8 stdin, closed immediately at EOF, with trailing
  `--` and no argv message. The installed run handler reads `Bun.stdin.text()` and preserves
  content without trimming when the argv message is empty. Start and resume use this transport;
  small assignments retain the existing separated argv form.

The freedom mapping is agent-based, and measured rather than reasoned about:

* `--agent plan` declined to write a file or run bash, and wrote nothing. It said so in its own words
  ("I'm currently in plan mode (read-only)"). That is the **model declining**, not a layer stopping
  it — nothing here proves a write would have been refused had it tried.
* `--agent build` **without** `--auto` created the file and ran `git status` unprompted. It neither
  hung nor refused, so there is no approval deadlock to guard against the way Codex needs
  `approval_policy="never"`.
* That measurement shows `--auto` is not what separates writing from not writing. It does **not**
  establish what `build` permits in general, nor that `--auto` never matters: per its own help it
  auto-approves what would otherwise be *asked*, which depends on the user's configuration. The
  caveats say only the narrow thing that was measured.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from . import normalize as nz
from .mcp_approval import OpencodeApproval
from .base import (
    EFFORTS,
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
)

BINARY = "opencode"
ARGV_PROMPT_BYTES = 32 * 1024

AGENTS: dict[str, str] = {
    "read_only": "plan",
    "write_in_repo": "build",
    # Identical to write_in_repo on purpose: nothing here was ever enforced by polybridge (see
    # _BUILD_CAVEAT), so there is no separate mechanism to give `publish` — assert_safe cannot tell
    # these two freedoms apart from argv alone, and that collapse is deliberate, not a hole.
    "publish": "build",
    "unrestricted": "build",
}

# The only freedom that adds --auto. It is not what separates writing from not writing — `build`
# already writes without it — so it is deliberately not treated as the mechanism for anything.
AUTO_APPROVE: frozenset[str] = frozenset({"unrestricted"})

SESSION_FLAGS = ("-s", "--session")

# Options this backend emits, split by whether they take a value. The safety check walks the option
# region against these rather than searching it, because opencode also accepts `--flag=value` and
# compact `-sID` forms that a search would miss while opencode still honours them — measured:
# `--format=default` appended after `--format json` silently disabled the JSON stream.
VALUE_FLAGS = ("--format", "--dir", "--agent", "-m", "--variant", *SESSION_FLAGS)
BOOLEAN_FLAGS = ("--auto",)

# Flags that would break a guarantee this backend makes. `-c`/`--continue` picks "the most recent
# session", which races with any other opencode the user is running; `--fork` mints a *new* session
# id, contradicting resume_task's promise of continuing the same conversation; `--attach` sends the
# run to another machine's server entirely; the last two discard the permission layer.
REJECTED_FLAGS = (
    "-c",
    "--continue",
    "--fork",
    "--attach",
    "--yolo",
    "--dangerously-skip-permissions",
    # Both break the headless JSONL contract rather than the session one: --interactive switches to
    # a UI nothing is here to drive, and --command replaces what is executed, turning the prompt
    # into arguments for something else.
    "-i",
    "--interactive",
    "--command",
)

_PLAN_CAVEAT = (
    "read-only is opencode's `plan` agent: the agent declines to write or run commands, so it holds "
    "only as long as the agent respects it (measured: it declined and wrote nothing — it never "
    "attempted a write, so whether a tool-layer refusal would have caught one was not exercised)"
)
_NO_SANDBOX_CAVEAT = (
    "no OS sandbox: the agent can read and write outside repo_path, which is only its working "
    "directory"
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
_EFFORT_CAVEAT = (
    "--variant support is per model, not universal: measured working on "
    "opencode/muse-spark-1.3-contributor-free, where reasoning tokens on one prompt rose "
    "monotonically with the variant over three paired runs each — 75/95/99 at the native 'minimal' "
    "(outside this vocabulary), 172/201/230 at 'low', 239/254/308 at 'xhigh'. Non-overlapping, but "
    "the low-to-xhigh margin is narrow, so treat the flag as directional rather than as a dial with "
    "a guaranteed magnitude. A model that declares no `variants` in `opencode models --verbose` "
    "(e.g. big-pickle) ignores --variant with no warning at all, and polybridge cannot know which "
    "model has variants — so levels_change_behaviour=True means 'shown to change behaviour on one "
    "capable model', not 'guaranteed on the model this run uses'"
)

_BUILD_CAVEAT = (
    "the `build` agent wrote a file and ran a shell command without --auto (measured), so --auto is "
    "not what separates writing from not writing; what else `build` permits depends on the user's "
    "opencode configuration and was not measured"
)

_MODE_CAVEATS: dict[str, tuple[str, ...]] = {
    "read_only": (_PLAN_CAVEAT,),
    "write_in_repo": (
        _BUILD_CAVEAT,
        "writes are not confined to the repository — repo_path is the working directory, nothing "
        "more",
    ),
    "publish": (
        _BUILD_CAVEAT,
        "writes are not confined to the repository — repo_path is the working directory, nothing "
        "more",
        "identical mechanism to write_in_repo: --agent build, no --auto — publish adds nothing "
        "real here, since nothing was ever enforced by this mapping. assert_safe cannot tell "
        "these two freedoms apart from argv alone, and that collapse is deliberate",
    ),
    "unrestricted": (
        _BUILD_CAVEAT,
        "--auto auto-approves only what would otherwise be *asked*: it widens nothing already "
        "permitted and overrides no deny the user has configured, so how much it adds over "
        "write_in_repo depends entirely on that configuration",
    ),
}


class UnsafeInvocationError(RuntimeError):
    """An argv was assembled without this backend's required guarantees."""


class OpencodeBackend:
    @staticmethod
    def workflow_stderr_availability_failure(diagnostic: str) -> str | None:
        from .workflow_diagnostics import stderr_availability
        return stderr_availability(diagnostic, quota_patterns=('(?im)^.*(?:insufficient_quota|model_not_found|rate_limit_exceeded).*$',))

    @staticmethod
    def workflow_availability_failure(event: dict[str, Any]) -> str | None:
        from .workflow_diagnostics import provider_error
        return provider_error(event, event_type="error")

    @staticmethod
    def workflow_failure_diagnostic(event: dict[str, Any]) -> str | None:
        error = event.get("error")
        if event.get("type") != "error" or not isinstance(error, dict) or not isinstance(error.get("name"), str):
            return None
        data = error.get("data")
        if not isinstance(data, dict):
            return None
        if data.get("statusCode") in {401, 403}:
            return f"OpenCode authentication failed (HTTP {data['statusCode']}); check the configured provider credentials."
        return data.get("message") if isinstance(data.get("message"), str) else None

    name = "opencode"
    mcp_approval = OpencodeApproval()
    binary = BINARY
    capabilities = Capabilities(
        # opencode mints `ses_…` itself and reports it on the first event.
        chooses_session_id=False,
        supports_turn_cap=False,
        # Measured: `cost` on every step_finish, summed across steps.
        reports_cost_usd=True,
        os_sandbox=False,
        per_command_deny=False,
        supports_model_selection=True,
        reasoning_effort=ReasoningEffort(
            accepts_parameter=True,
            levels=EFFORTS,
            native_flag="--variant",
            accepted_in_real_run=True,
            levels_change_behaviour=True,
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
            raise ValueError("opencode mints its own session id; one cannot be supplied")
        self._reject_turn_cap(max_turns)
        argv = [BINARY, "run", *self._options(repo, freedom, model, reasoning_effort, network)]
        # `--` then the prompt: last, and explicitly not parsed as an option however it looks.
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
            raise ValueError("resuming opencode needs the session id its first run reported")
        self._reject_turn_cap(max_turns)
        # -s, never -c: "the most recent session" is whatever ran last on this machine, which is not
        # necessarily this task's conversation.
        options = self._options(repo, freedom, model, reasoning_effort, network)
        argv = [BINARY, "run", *options, "-s", session_id]
        invocation = self._prompt_invocation(argv, prompt)
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
        check_reasoning_effort(self, reasoning_effort)
        # Validated here so no caller can reach the CLI with a network request this backend
        # would silently ignore (False). True needs no argv change at all — there is nothing to
        # impose — which is exactly why the enforcement block, not the argv, is where its
        # acceptance is disclosed.
        check_network(self, freedom, network)
        # --dir duplicates the spawn cwd on purpose: it is what puts the repository on the command
        # line, which is the only identity marker available for a backend that cannot carry its
        # session id in argv on a fresh run. See tasks._identity_markers.
        options = ["--format", "json", "--dir", str(repo), "--agent", AGENTS[freedom]]
        if freedom in AUTO_APPROVE:
            options.append("--auto")
        if model:
            options += ["-m", model]
        if reasoning_effort:
            options += ["--variant", reasoning_effort]
        return options

    def _reject_turn_cap(self, max_turns: int | None) -> None:
        if max_turns is not None:
            raise UnsupportedCapability(
                f"the opencode CLI has no turn cap, so max_turns={max_turns} cannot be honoured; "
                "omit it rather than have it silently ignored"
            )

    def _prompt_invocation(self, argv: list[str], prompt: str) -> Invocation:
        prompt = self._check_prompt(prompt)
        if len(prompt.encode("utf-8")) > ARGV_PROMPT_BYTES:
            return Invocation([*argv, "--"], stdin_mode="pipe_once", initial_input=prompt.encode("utf-8"))
        return Invocation([*argv, "--", prompt])

    @staticmethod
    def _check_prompt(prompt: str) -> str:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        return prompt

    def assert_safe(
        self, invocation: Invocation, freedom: Freedom, network: bool | None = None
    ) -> None:
        # EOF stdin is a one-shot assignment transport, never a live input pipe.
        one_shot = isinstance(invocation, Invocation) and invocation.stdin_mode == "pipe_once"
        if one_shot:
            try:
                prompt = invocation.initial_input.decode("utf-8") if isinstance(invocation.initial_input, bytes) else ""
            except UnicodeError:
                prompt = ""
            if not prompt.strip() or invocation.argv[-1:] != ["--"]:
                raise UnsafeInvocationError("OpenCode one-shot stdin requires nonempty UTF-8 input and trailing -- with no argv message")
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
        if argv[:2] != [BINARY, "run"]:
            raise UnsafeInvocationError(f"unrecognised opencode argv layout: {argv!r}")

        # Only the option region is inspected. Everything after `--` is the prompt — caller text that
        # happens to contain a flag name must never be able to satisfy a safety check.
        if "--" not in argv:
            raise UnsafeInvocationError(
                f"refusing to run opencode without a `--` separator before the prompt, which stops "
                f"prompt text being parsed as options: {argv!r}"
            )
        positionals = argv[argv.index("--") + 1:]
        if one_shot and positionals:
            raise UnsafeInvocationError("OpenCode one-shot stdin cannot also supply argv messages")
        if not one_shot and (len(positionals) != 1 or not positionals[0].strip()):
            raise UnsafeInvocationError("OpenCode requires exactly one nonempty argv assignment")
        if not one_shot and len(positionals[0].encode("utf-8")) > ARGV_PROMPT_BYTES:
            raise UnsafeInvocationError("OpenCode large assignments require one-shot stdin transport")
        seen = self._parse_options(argv[2 : argv.index("--")], argv)

        fmt = self._exactly_one(seen, "--format", argv)
        if fmt != "json":
            raise UnsafeInvocationError(
                f"--format was {fmt!r}, but only json can be parsed into events: {argv!r}"
            )

        if not self._exactly_one(seen, "--dir", argv).strip():
            raise UnsafeInvocationError(f"--dir names no directory: {argv!r}")

        agent = self._exactly_one(seen, "--agent", argv)
        expected_agent = AGENTS[freedom]
        if agent != expected_agent:
            raise UnsafeInvocationError(
                f"--agent was {agent!r}, expected {expected_agent!r} for freedom {freedom!r}: "
                f"{argv!r}"
            )

        if len(seen.get("--auto", ())) > 1:
            raise UnsafeInvocationError(f"--auto appears 2 times: {argv!r}")

        # A second -m wins, so the run would use a model other than the one the task reports.
        if len(seen.get("-m", ())) > 1:
            raise UnsafeInvocationError(f"-m appears {len(seen['-m'])} times: {argv!r}")

        # Optional, like -m: absent unless an effort was requested, but a second one would still
        # win silently, and a non-canonical value is one opencode would ignore without a warning.
        variants = seen.get("--variant", ())
        if len(variants) > 1:
            raise UnsafeInvocationError(f"--variant appears {len(variants)} times: {argv!r}")
        if variants and variants[0] not in EFFORTS:
            raise UnsafeInvocationError(f"unexpected --variant {variants[0]!r}: {argv!r}")

        # `--auto` must be present exactly when `freedom` calls for auto-approval — never more,
        # never less. The old check only refused `--auto` alongside the read_only agent; that is
        # now a special case of this rule, since read_only is never in AUTO_APPROVE.
        has_auto = "--auto" in seen
        should_auto = freedom in AUTO_APPROVE
        if has_auto and not should_auto:
            raise UnsafeInvocationError(
                f"--auto contradicts freedom {freedom!r} (agent {agent!r}), which does not call "
                f"for auto-approval: {argv!r}"
            )
        if should_auto and not has_auto:
            raise UnsafeInvocationError(
                f"--auto is missing but freedom {freedom!r} requires auto-approval: {argv!r}"
            )

        # At most one session flag, counted across both spellings, and it must actually name a
        # session: a resume that silently continued the wrong conversation is worse than one that
        # fails outright.
        sessions = [value for flag in SESSION_FLAGS for value in seen.get(flag, ())]
        if len(sessions) > 1:
            raise UnsafeInvocationError(f"expected at most one session flag: {argv!r}")
        if sessions and not sessions[0].strip():
            raise UnsafeInvocationError(f"session flag with no session id: {argv!r}")

    @staticmethod
    def _parse_options(options: list[str], argv: list[str]) -> dict[str, list[str]]:
        """Walk the option region strictly, refusing any token this backend would not have written.

        Searching for `"--format" in options` is not enough: opencode also accepts `--format=json`
        and compact `-sID`, and a *later* value wins. Measured — appending `--format=default` to an
        argv that already had `--format json` disabled the JSON stream while every search-based
        check still passed. So anything not in canonical space-separated form is refused rather than
        skipped over.
        """
        seen: dict[str, list[str]] = {}
        index = 0
        while index < len(options):
            token = options[index]
            if token in REJECTED_FLAGS:
                raise UnsafeInvocationError(
                    f"{token} would break this backend's session or permission guarantees: {argv!r}"
                )
            if token in BOOLEAN_FLAGS:
                seen.setdefault(token, []).append("")
                index += 1
            elif token in VALUE_FLAGS:
                if index + 1 >= len(options):
                    raise UnsafeInvocationError(f"{token} has no value: {argv!r}")
                value = options[index + 1]
                # A value that looks like an option is not a value: opencode's parser would read it
                # as the next flag. `model` is caller-supplied and lands here, so a model named
                # `--continue=true` would otherwise smuggle an option into the region this method
                # exists to police. Also catches `--dir --agent`, where the flag ate the next flag.
                if value.startswith("-"):
                    raise UnsafeInvocationError(
                        f"{token} was given {value!r}, which opencode would parse as an option "
                        f"rather than a value: {argv!r}"
                    )
                seen.setdefault(token, []).append(value)
                index += 2
            else:
                raise UnsafeInvocationError(
                    f"unrecognised option token {token!r}: this backend writes only canonical "
                    f"space-separated options, and `--flag=value` or `-sID` forms would override "
                    f"one of them unnoticed: {argv!r}"
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
        agent = AGENTS[freedom]
        auto = " --auto" if freedom in AUTO_APPROVE else ""
        return Enforcement(
            freedom=freedom,
            mechanism=f"opencode --agent {agent}{auto}",
            # Everything opencode applies is the agent's own policy; there is no OS boundary.
            os_enforced=False,
            writes_confined=False,
            writable_roots=(),
            # No per-command deny list exists, so neither commit claim can be made.
            commit_push_blocked=False,
            direct_commit_commands_denied=False,
            # publish and unrestricted are the freedoms that authorize a publish attempt — see
            # the field's own docstring. A network request changes nothing here: this backend
            # has no barrier of its own either way, at any freedom.
            publish_attempts_allowed_by_polybridge=freedom in ("publish", "unrestricted"),
            network_access="not_controlled",
            caveats=(_NO_SANDBOX_CAVEAT, *_MODE_CAVEATS[freedom]),
        )

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        session_id = event.get("sessionID")
        if acc.session_id is None and isinstance(session_id, str) and session_id:
            acc.session_id = session_id

        event_type = event.get("type")
        part = event.get("part")
        part = part if isinstance(part, dict) else {}

        if event_type == "text":
            text = part.get("text")
            if isinstance(text, str):
                # Later text supersedes earlier: the closing message is the one worth reporting.
                acc.summary = text

        elif event_type == "step_finish":
            acc.num_turns = (acc.num_turns or 0) + 1

            cost = _usable_number(part.get("cost"))
            if cost is not None:
                # Accumulated, never assigned: each step reports only its own cost.
                acc.total_cost_usd = (acc.total_cost_usd or 0.0) + cost

            tokens = part.get("tokens")
            if isinstance(tokens, dict):
                acc.usage = _sum_tokens(acc.usage, tokens)

            if part.get("reason") == "stop":
                # The only end-of-run signal opencode gives. Intermediate steps say "tool-calls".
                acc.saw_final_message = True
                acc.terminal = event

        elif event_type == "error":
            acc.is_error = True
            acc.terminal = event
            detail = _error_detail(event)
            if detail:
                acc.notices.append(detail)

    def normalize(self, event: dict[str, Any], acc: Accumulator) -> list[dict[str, Any]]:
        if not isinstance(event, dict):
            return []
        source_ts = nz.iso_from_epoch_ms(event.get("timestamp"))
        event_type = event.get("type")
        part = event.get("part")
        part = part if isinstance(part, dict) else {}

        if event_type == "tool_use":
            return self._normalize_tool_use(part, source_ts)
        if event_type == "text":
            text = part.get("text")
            return [nz.assistant_text(text, source_ts=source_ts)] if isinstance(text, str) else []
        if event_type == "step_finish":
            return [nz.usage(acc, source_ts=source_ts)]
        if event_type == "error":
            detail = _error_detail(event)
            return [nz.notice(detail, source_ts=source_ts)] if detail else []
        return []

    @staticmethod
    def _normalize_tool_use(part: dict[str, Any], source_ts: str | None) -> list[dict[str, Any]]:
        call_id = part.get("callID") or part.get("id")
        tool = part.get("tool")
        state = part.get("state")
        state = state if isinstance(state, dict) else {}
        tool_input = state.get("input")
        input_dict = tool_input if isinstance(tool_input, dict) else {}
        category = _tool_category(tool)

        events: list[dict[str, Any]] = [
            nz.tool_call(
                call_id=call_id,
                tool=tool,
                category=category,
                input=tool_input,
                path=_tool_path(input_dict),
                command=_tool_command(input_dict),
                edit=_tool_edit(tool, input_dict),
                source_ts=source_ts,
            )
        ]

        status = state.get("status")
        if status in ("completed", "error"):
            metadata = state.get("metadata")
            exit_code = metadata.get("exit") if isinstance(metadata, dict) else None
            output = state.get("output") if status == "completed" else state.get("error")
            events.append(
                nz.tool_result(
                    call_id=call_id,
                    ok=status == "completed",
                    exit_code=exit_code,
                    output=output,
                    source_ts=source_ts,
                )
            )
        return events

    def encode_live_message(self, text: str) -> bytes:
        raise UnsupportedCapability(
            f"the {self.name} backend has no live input, so a message cannot be added to a running "
            "task; continue its session with resume_task instead"
        )


    def interactive_resume_argv(self, session_id: str, repo_path: Path) -> list[str] | None:
        """`opencode <repo> -s <id>` (measured: loads the prior conversation, no prompts)."""
        if not interactive_session_id_ok(session_id, repo_path):
            return None
        return [self.binary, str(repo_path), "-s", session_id]

    def classify(self, acc: Accumulator, exit_code: int | None) -> Status:
        # `reason: "stop"` is a real end-of-run signal, not merely "some text arrived", so unlike
        # codex and vibe this backend does not need an observed exit code to establish success: a
        # recovered run whose stream reached `stop` did finish. Only an *observed* non-zero exit
        # overrules that, which is why None is not lumped in with it.
        if acc.is_error:
            return "failed"
        if exit_code is not None and exit_code != 0:
            return "failed"
        return "completed" if acc.saw_final_message else "failed"


def _error_detail(event: dict[str, Any]) -> str | None:
    """The notice text for an `error` event — shared by `ingest` and `normalize` so the monitor's
    notice says exactly what `ingest` already decided was worth surfacing."""
    error = event.get("error")
    if not isinstance(error, dict):
        return None
    data = error.get("data")
    message = data.get("message") if isinstance(data, dict) else None
    name = error.get("name")
    detail = message if isinstance(message, str) else None
    if isinstance(name, str):
        detail = f"{name}: {detail}" if detail else name
    return detail if detail else None


# Tool name -> monitor category, per opencode's own tool names (lowercase, unlike claude's).
_CATEGORY_BY_TOOL: dict[str, str] = {
    "read": "read",
    "grep": "search",
    "glob": "search",
    "list": "search",
    "edit": "edit",
    "multiedit": "edit",
    "patch": "edit",
    "write": "write",
    "bash": "shell",
    "webfetch": "web",
    "websearch": "web",
}


def _tool_category(tool: Any) -> str:
    if not isinstance(tool, str):
        return "other"
    return _CATEGORY_BY_TOOL.get(tool, "other")


def _tool_path(input_dict: dict[str, Any]) -> str | None:
    for key in ("filePath", "path"):
        value = input_dict.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _tool_command(input_dict: dict[str, Any]) -> str | None:
    value = input_dict.get("command")
    return value if isinstance(value, str) and value else None


def _tool_edit(tool: Any, input_dict: dict[str, Any]) -> tuple[str, str] | None:
    if tool != "edit":
        return None
    old = input_dict.get("oldString")
    new = input_dict.get("newString")
    return (old, new) if isinstance(old, str) and isinstance(new, str) else None


def _usable_number(value: Any) -> float | None:
    """A number worth adding to a running total, or None.

    Three ways a value can be unusable, all reachable from a stream we do not control: `bool` is an
    `int` in Python, so `"cost": true` would bill a dollar; `json.loads` accepts `NaN` and `Infinity`
    by default, and either would poison every later sum irrecoverably; and a negative count or cost
    is not a thing opencode can truthfully report. Dropped rather than clamped — a total that
    silently absorbed nonsense is worse than one missing a step.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    if not math.isfinite(number) or number < 0:
        return None
    return number


def _sum_tokens(current: dict[str, Any] | None, step: dict[str, Any]) -> dict[str, Any]:
    """Add one step's token counts onto the running total, nested `cache` included.

    Summed rather than replaced because each step is a separate API call billed in its own right —
    the same reason `cost` accumulates.
    """
    total: dict[str, Any] = dict(current) if current else {}
    for key, value in step.items():
        if isinstance(value, dict):
            existing = total.get(key)
            total[key] = _sum_tokens(existing if isinstance(existing, dict) else None, value)
            continue
        number = _usable_number(value)
        if number is None:
            continue
        existing = total.get(key)
        running = (existing if isinstance(existing, (int, float)) else 0) + number
        # Token counts are whole numbers and should still look like it in the reported payload.
        total[key] = int(running) if float(running).is_integer() else running
    return total
