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

from .base import (
    EFFORTS,
    Accumulator,
    Capabilities,
    Enforcement,
    Freedom,
    ReasoningEffort,
    Status,
    UnsupportedCapability,
    check_freedom,
    check_reasoning_effort,
)

BINARY = "opencode"

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
    name = "opencode"
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
            raise ValueError("opencode mints its own session id; one cannot be supplied")
        self._reject_turn_cap(max_turns)
        argv = [BINARY, "run", *self._options(repo, freedom, model, reasoning_effort)]
        # `--` then the prompt: last, and explicitly not parsed as an option however it looks.
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
        if not session_id:
            raise ValueError("resuming opencode needs the session id its first run reported")
        self._reject_turn_cap(max_turns)
        # -s, never -c: "the most recent session" is whatever ran last on this machine, which is not
        # necessarily this task's conversation.
        options = self._options(repo, freedom, model, reasoning_effort)
        argv = [BINARY, "run", *options, "-s", session_id]
        argv += ["--", self._check_prompt(prompt)]
        self.assert_safe(argv, freedom)
        return argv

    def _options(
        self, repo: Path, freedom: Freedom, model: str | None, reasoning_effort: str | None
    ) -> list[str]:
        check_reasoning_effort(self, reasoning_effort)
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

    @staticmethod
    def _check_prompt(prompt: str) -> str:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        return prompt

    def assert_safe(self, argv: list[str], freedom: Freedom) -> None:
        # Rejects an unknown freedom outright rather than letting AGENTS[freedom] raise a bare
        # KeyError below.
        check_freedom(freedom)
        if argv[:2] != [BINARY, "run"]:
            raise UnsafeInvocationError(f"unrecognised opencode argv layout: {argv!r}")

        # Only the option region is inspected. Everything after `--` is the prompt — caller text that
        # happens to contain a flag name must never be able to satisfy a safety check.
        if "--" not in argv:
            raise UnsafeInvocationError(
                f"refusing to run opencode without a `--` separator before the prompt, which stops "
                f"prompt text being parsed as options: {argv!r}"
            )
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

    def enforcement(self, freedom: Freedom) -> Enforcement:
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
            # publish and unrestricted are where polybridge configures no barrier of its own
            # against a commit/push/PR attempt — though on opencode nothing ever did, at any
            # freedom, so this is no stronger a claim than write_in_repo already implied.
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
            error = event.get("error")
            if isinstance(error, dict):
                data = error.get("data")
                message = data.get("message") if isinstance(data, dict) else None
                name = error.get("name")
                detail = message if isinstance(message, str) else None
                if isinstance(name, str):
                    detail = f"{name}: {detail}" if detail else name
                if detail:
                    acc.notices.append(detail)

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
