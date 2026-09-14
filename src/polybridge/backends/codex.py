"""Codex backend.

CLI facts established by capturing a real run, not assumption:

* `codex exec` **blocks forever reading stdin** unless stdin is closed. Spawning with
  `stdin=DEVNULL` is mandatory, not tidiness.
* An approval prompt would hang a headless run just as badly, so `approval_policy="never"` is pinned
  alongside the sandbox mode.
* The event stream is nothing like Claude's::

      {"type":"thread.started","thread_id":"019fadb8-…"}
      {"type":"turn.started"}
      {"type":"item.completed","item":{"type":"error","message":"…"}}
      {"type":"item.completed","item":{"type":"agent_message","text":"ok"}}
      {"type":"turn.completed","usage":{"input_tokens":27992,…}}

  The session id is `thread_id`. The final answer is an `agent_message` item. There is no terminal
  event carrying success or failure, and **no dollar cost** — only token counts.
* An `item.type == "error"` was observed in a *successful* run (a benign skills warning), so error
  items are notices, never proof of failure.

**`publish` (measured network table).** `-s workspace-write` alone leaves network to *the user's own
config* — measured: with `[sandbox_workspace_write] network_access = true` set there, a plain
`workspace-write` run reached the network (HTTP 200). So polybridge passes
`-c sandbox_workspace_write.network_access=false` wherever it reports `blocked`, making that claim
true of the run rather than of the machine. Network opens only with the config pair
`-c sandbox_workspace_write.network_access=true` added on top: paired runs measured curl against a
real host returning exit 6 (could not resolve host) at `read-only` and at `workspace-write` without
the pair, and HTTP 200 at `workspace-write` with the pair, and at `danger-full-access`. So `publish`
maps to `workspace-write` plus that pair. This is honestly **general** network access, not
git/gh-specific — anything the sandbox lets run can now reach the network, not only a git push or
`gh pr create`. `WRITABLE_ROOTS` at `publish` is identical to `write_in_repo` — the switch changes
network reachability only, not the writable set.
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
    UnsupportedCapability,
    check_freedom,
    check_reasoning_effort,
)

BINARY = "codex"

SANDBOX_MODES: dict[str, str] = {
    "read_only": "read-only",
    "write_in_repo": "workspace-write",
    "publish": "workspace-write",
    "unrestricted": "danger-full-access",
}

# Without this a headless run can stop dead waiting for an approval nobody can give.
NEVER_ASK = ("-c", 'approval_policy="never"')

# The `-c` key/value that turns network access on inside `workspace-write`, per the binary's own
# strings (see module docstring) — passed only at `publish`. Codex does not validate `-c` keys at
# all, so getting the literal exactly right is what stands between "network still blocked" and
# "wrong key, silently ignored".
NETWORK_KEY = "sandbox_workspace_write.network_access"
NETWORK_ENABLE_PAIR = f"{NETWORK_KEY}=true"
# Passed explicitly wherever we claim network is blocked, rather than relying on codex's default.
# Measured: with `[sandbox_workspace_write] network_access = true` in the user's own config,
# `-s workspace-write` with no `-c` pair reached the network (HTTP 200) — so "blocked" was a claim
# about this machine's config, not about the sandbox. The explicit `=false` overrides that ambient
# setting (measured: back to curl exit 6), which makes the claim true by construction.
NETWORK_DISABLE_PAIR = f"{NETWORK_KEY}=false"

# The `-c` key this backend's effort rides on. EFFORTS is a closed four-value vocabulary with no
# quote or `=` characters in it, so quoting the TOML value is a non-issue — no encoder needed.
EFFORT_KEY = "model_reasoning_effort"

_EFFORT_CAVEAT = (
    "codex does not validate model_reasoning_effort itself — an unsupported value would reach the "
    "API as a mid-run 400 rather than being rejected up front — but that gap is moot for the fixed "
    "EFFORTS vocabulary, which every model accepts (measured: a run at 'ultra', outside this "
    "vocabulary, still recorded reasoning_effort in its session rollout end to end)"
)
_EFFORT_NO_COMPARISON_CAVEAT = (
    "acceptance evidence is the run's own session rollout recording the requested "
    "model_reasoning_effort verbatim — stronger than nothing, but reasoning_output_tokens is "
    "reported as 0 at both 'low' and 'xhigh' (measured), so a cross-level behavioural comparison is "
    "not available from codex's stream at all"
)

# `workspace-write` is *not* repo-only: Codex reports its own writable set as
# `[workdir, /tmp, $TMPDIR]`, so claiming confinement to the repository would overstate it. `publish`
# shares write_in_repo's writable set exactly — the network pair changes reachability, not writes.
WRITABLE_ROOTS: dict[str, tuple[str, ...]] = {
    "read_only": (),
    "write_in_repo": ("the working directory", "/tmp", "$TMPDIR"),
    "publish": ("the working directory", "/tmp", "$TMPDIR"),
    "unrestricted": ("anywhere the user can write",),
}

_NETWORK_CAVEAT = (
    "this level additionally enables GENERAL network access "
    f"({NETWORK_ENABLE_PAIR}), not git/gh-specific: anything running inside the sandbox can reach "
    "the network, not only a git push or gh pr create. Measured per sandbox: read-only and plain "
    "workspace-write both block it (curl exit 6, could not resolve host); workspace-write plus this "
    "pair enables it (HTTP 200), same as danger-full-access"
)

_MODE_CAVEATS: dict[str, tuple[str, ...]] = {
    "read_only": ("the sandbox rejects writes outright",),
    "write_in_repo": (
        "writes are confined by the OS, but to the workspace *plus* temporary directories — not to "
        "the repository alone",
        "no per-command deny list, so the agent can freely commit inside the sandbox",
    ),
    "publish": (
        "writes are confined by the OS, but to the workspace *plus* temporary directories — not to "
        "the repository alone",
        "no per-command deny list, so the agent can freely commit inside the sandbox",
        _NETWORK_CAVEAT,
    ),
    "unrestricted": (
        "danger-full-access disables the sandbox: nothing is restricted, which is the one mode where "
        "codex is no safer than an unsandboxed agent",
    ),
}

# Options this backend itself ever writes — nothing more. `assert_safe` walks the option region
# against exactly these instead of searching it, because searching is what let a non-canonical
# spelling through while codex still honoured it. Measured evading a search-based check: the
# attached forms `--config=…` and `-capproval_policy=…`, the attached `--sandbox=danger-full-access`
# (`codex exec --json --sandbox=read-only …` runs fine, so this spelling is real), and `--add-dir
# /etc`. Long aliases (`--cd`, `--sandbox`, `--config`, `--model`) are deliberately absent even though
# codex accepts them: this backend never writes them, so admitting them here would reopen the same
# hole under a different spelling.
BOOLEAN_FLAGS = ("--json",)
VALUE_FLAGS = ("-C", "-s", "-c", "-m")

# The only `-c key=value` literals this backend ever writes, matched byte-for-byte rather than
# parsed. Measured: codex normalises whitespace around a `-c key=value` pair before applying it, and
# falls back to the raw string when the value fails to parse as TOML — so a strip()-then-compare
# check let an unbalanced quote (`model_reasoning_effort="low`, missing its close) and doubled
# quoting (`=""low""`) through as if they were the clean value. Exact-literal membership closes that
# by construction: anything not identical to one of these shapes is refused outright, with no
# parsing step for a malformed value to hide behind. Built by construction, not hardcoded loosely,
# so NETWORK_ENABLE_PAIR (the `publish` network switch) stays the single source of truth for its own
# literal.
PERMITTED_C_PAIRS: frozenset[str] = frozenset(
    {NEVER_ASK[1], NETWORK_ENABLE_PAIR, NETWORK_DISABLE_PAIR}
    | {f'{EFFORT_KEY}="{level}"' for level in EFFORTS}
)


class UnsafeInvocationError(RuntimeError):
    """An argv was assembled without this backend's required guarantees."""


class CodexBackend:
    name = "codex"
    binary = BINARY
    capabilities = Capabilities(
        # Codex mints its own thread id and reports it in the stream.
        chooses_session_id=False,
        supports_turn_cap=False,
        reports_cost_usd=False,
        os_sandbox=True,
        per_command_deny=False,
        supports_model_selection=True,
        reasoning_effort=ReasoningEffort(
            accepts_parameter=True,
            levels=EFFORTS,
            native_flag=f"-c {EFFORT_KEY}",
            accepted_in_real_run=True,
            levels_change_behaviour=False,
            caveats=(_EFFORT_CAVEAT, _EFFORT_NO_COMPARISON_CAVEAT),
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
            raise ValueError("codex mints its own session id; one cannot be supplied")
        self._reject_turn_cap(max_turns)
        argv = [BINARY, "exec", *self._options(repo, freedom, model, reasoning_effort)]
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
            raise ValueError("resuming codex needs the thread id its first run reported")
        self._reject_turn_cap(max_turns)
        argv = [BINARY, "exec", "resume", *self._options(repo, freedom, model, reasoning_effort)]
        # `codex exec resume [SESSION_ID] [PROMPT]` — both positional, after `--`.
        argv += ["--", session_id, self._check_prompt(prompt)]
        self.assert_safe(argv, freedom)
        return argv

    def _options(
        self, repo: Path, freedom: Freedom, model: str | None, reasoning_effort: str | None
    ) -> list[str]:
        check_reasoning_effort(self, reasoning_effort)
        options = ["--json", "-C", str(repo), "-s", SANDBOX_MODES[freedom], *NEVER_ASK]
        if freedom == "publish":
            options += ["-c", NETWORK_ENABLE_PAIR]
        elif SANDBOX_MODES[freedom] == "workspace-write":
            # Only meaningful for workspace-write: the key is scoped to that sandbox, and `read-only`
            # was measured immune to it even when the user's config sets it true.
            options += ["-c", NETWORK_DISABLE_PAIR]
        if model:
            options += ["-m", model]
        if reasoning_effort:
            options += ["-c", f'{EFFORT_KEY}="{reasoning_effort}"']
        return options

    def _reject_turn_cap(self, max_turns: int | None) -> None:
        if max_turns is not None:
            raise UnsupportedCapability(
                f"the codex CLI has no turn cap, so max_turns={max_turns} cannot be honoured; "
                "omit it rather than have it silently ignored"
            )

    @staticmethod
    def _check_prompt(prompt: str) -> str:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        return prompt

    def assert_safe(self, argv: list[str], freedom: Freedom) -> None:
        # Rejects an unknown freedom outright rather than letting SANDBOX_MODES[freedom] raise a
        # bare KeyError below.
        check_freedom(freedom)
        if argv[:2] != [BINARY, "exec"]:
            raise UnsafeInvocationError(f"unrecognised codex argv layout: {argv!r}")

        # Only the option region is inspected. Everything after `--` is positional — prompt text
        # that happens to contain a flag name must not be able to satisfy a safety check.
        if "--" not in argv:
            raise UnsafeInvocationError(
                f"refusing to run codex without a `--` separator before the prompt, which stops "
                f"prompt text being parsed as options: {argv!r}"
            )
        # The option region starts right after `exec`, except on a resume argv where `resume` is
        # a literal subcommand token before it — `build_start_argv`'s first option is always
        # `--json`, never the string "resume", so this is unambiguous rather than a hardcoded offset.
        start_index = 3 if argv[2:3] == ["resume"] else 2
        options = argv[start_index : argv.index("--")]

        # Kept for the clearer message even though the allowlist below would refuse this token too
        # (as unrecognised) — this is the one form worth naming explicitly.
        if "--dangerously-bypass-approvals-and-sandbox" in options:
            raise UnsafeInvocationError(
                "--dangerously-bypass-approvals-and-sandbox discards the sandbox that is this "
                "backend's main safety property"
            )

        seen = self._parse_options(options, argv)

        # Positional arity, not just separator presence. `exec` takes one positional (the prompt),
        # `exec resume` takes two (session id then prompt) — and a resume missing its prompt is the
        # dangerous shape, because codex would read the intended prompt as the SESSION_ID and
        # silently continue some other conversation. Counted, never matched: a legitimate prompt or
        # session id may itself be `--`, `resume`, or look like an option.
        positionals = argv[argv.index("--") + 1 :]
        expected = 2 if start_index == 3 else 1
        if len(positionals) != expected:
            raise UnsafeInvocationError(
                f"expected exactly {expected} positional argument(s) after `--`, found "
                f"{len(positionals)}: {argv!r}"
            )

        # Exactly one, not merely present: a duplicate is not something this backend writes, and
        # the allowlist's promise is that nothing else wrote here either.
        if len(seen.get("--json", [])) != 1:
            raise UnsafeInvocationError(f"refusing to run codex without exactly one --json: {argv!r}")

        cds = seen.get("-C", [])
        if len(cds) != 1:
            raise UnsafeInvocationError(f"expected exactly one -C: {argv!r}")
        if not cds[0].strip():
            raise UnsafeInvocationError(f"-C names no directory: {argv!r}")

        # `-s` and its long alias `--sandbox` were previously counted together as one option; now
        # `--sandbox` is refused outright by the allowlist below, so only `-s` can appear here.
        sandbox_values = seen.get("-s", [])
        if len(sandbox_values) != 1:
            raise UnsafeInvocationError(f"expected exactly one sandbox flag: {argv!r}")
        mode = sandbox_values[0]
        expected_mode = SANDBOX_MODES[freedom]
        if mode != expected_mode:
            raise UnsafeInvocationError(
                f"sandbox mode was {mode!r}, expected {expected_mode!r} for freedom {freedom!r}: "
                f"{argv!r}"
            )

        models = seen.get("-m", [])
        if len(models) > 1:
            raise UnsafeInvocationError(f"-m appears {len(models)} times: {argv!r}")
        # `_options` omits -m entirely when no model was chosen, so an empty value means something
        # other than this backend assembled the argv.
        if models and not models[0].strip():
            raise UnsafeInvocationError(f"-m names no model: {argv!r}")

        # Every `-c` value was already checked against PERMITTED_C_PAIRS while walking, so only
        # multiplicity remains: exactly one approval override, the network pair iff freedom is
        # `publish` and nowhere else, at most one effort override.
        pairs = seen.get("-c", [])
        approvals = [pair for pair in pairs if pair == NEVER_ASK[1]]
        if approvals != [NEVER_ASK[1]]:
            raise UnsafeInvocationError(
                f"codex must be given exactly one {NEVER_ASK[1]} override, which prevents it "
                f"blocking on an approval prompt no one can answer; found {approvals!r}: {argv!r}"
            )

        # The network switch is asserted in BOTH directions: `=true` exactly at `publish`, and
        # `=false` wherever the sandbox is `workspace-write` and we claim network is blocked. The
        # second half is not belt-and-braces — without it the claim depends on the user's own
        # `[sandbox_workspace_write]` config, and was measured false against a config setting it
        # true.
        enables = [pair for pair in pairs if pair == NETWORK_ENABLE_PAIR]
        disables = [pair for pair in pairs if pair == NETWORK_DISABLE_PAIR]
        if enables and disables:
            raise UnsafeInvocationError(
                f"codex was given both network overrides at once, so which one applies depends on "
                f"argument order rather than on the requested freedom: {argv!r}"
            )
        if freedom == "publish":
            if enables != [NETWORK_ENABLE_PAIR]:
                raise UnsafeInvocationError(
                    f"codex must carry exactly one {NETWORK_ENABLE_PAIR} override at freedom "
                    f"'publish': found {enables!r}: {argv!r}"
                )
        elif SANDBOX_MODES[freedom] == "workspace-write":
            if disables != [NETWORK_DISABLE_PAIR]:
                raise UnsafeInvocationError(
                    f"codex must carry exactly one {NETWORK_DISABLE_PAIR} override at freedom "
                    f"{freedom!r}, or 'network_access: blocked' would be a claim about the user's "
                    f"own config rather than this run: found {disables!r}: {argv!r}"
                )
        elif enables or disables:
            raise UnsafeInvocationError(
                f"codex carries a network override at freedom {freedom!r}, where the sandbox "
                f"({SANDBOX_MODES[freedom]}) does not take one: {argv!r}"
            )

        overrides_with_own_checks = {NEVER_ASK[1], NETWORK_ENABLE_PAIR, NETWORK_DISABLE_PAIR}
        efforts = [pair for pair in pairs if pair not in overrides_with_own_checks]
        if len(efforts) > 1:
            raise UnsafeInvocationError(f"expected at most one {EFFORT_KEY} override: {argv!r}")

    @staticmethod
    def _parse_options(options: list[str], argv: list[str]) -> dict[str, list[str]]:
        """Walk the option region strictly, refusing any token this backend would not have written.

        Mirrors OpencodeBackend._parse_options and exists for the same reason: searching for
        `"-s" in options` or `value.startswith("approval_policy")` is not enough, because codex
        also honours `--flag=value`, attached short forms (`-capproval_policy=…`), and long aliases
        this backend never emits — a search sees the canonical form it wrote and passes while codex
        applies the non-canonical one that rode along. So every token not in canonical
        space-separated form is refused rather than skipped over, and a `-c` value is checked
        byte-for-byte against PERMITTED_C_PAIRS rather than parsed, which is what closes the
        unbalanced-quote and doubled-quote holes (see PERMITTED_C_PAIRS) by construction.
        """
        seen: dict[str, list[str]] = {}
        index = 0
        while index < len(options):
            token = options[index]
            if token in BOOLEAN_FLAGS:
                seen.setdefault(token, []).append("")
                index += 1
            elif token == "-c":
                if index + 1 >= len(options):
                    raise UnsafeInvocationError(f"-c has no value: {argv!r}")
                value = options[index + 1]
                if "=" not in value:
                    # Codex still applies *something* from a key with no `=`, so this is refused
                    # outright rather than silently dropped from `seen`, which would let it slip
                    # past every check below unexamined.
                    raise UnsafeInvocationError(f"-c/--config pair has no '=': {value!r}: {argv!r}")
                if value not in PERMITTED_C_PAIRS:
                    raise UnsafeInvocationError(f"unexpected -c pair {value!r}: {argv!r}")
                seen.setdefault(token, []).append(value)
                index += 2
            elif token in VALUE_FLAGS:
                if index + 1 >= len(options):
                    raise UnsafeInvocationError(f"{token} has no value: {argv!r}")
                value = options[index + 1]
                # A value starting with "-" is not a value: codex's parser would read it as the
                # next option. `model` is caller-supplied and lands here via `-m`, so a model named
                # e.g. `--sandbox=danger-full-access` would otherwise smuggle an option into the
                # region this method exists to police.
                if value.startswith("-"):
                    raise UnsafeInvocationError(
                        f"{token} was given {value!r}, which codex would parse as an option rather "
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

    def enforcement(self, freedom: Freedom) -> Enforcement:
        mode = SANDBOX_MODES[freedom]
        unrestricted = freedom == "unrestricted"
        mechanism = f"codex sandbox: {mode}"
        # Name the override that actually carries the network claim. Without it the mechanism reads
        # as though the sandbox alone decided, which is what `blocked` used to rest on and what was
        # measured false: plain `workspace-write` defers to the user's own config.
        if freedom == "publish":
            mechanism += f" + -c {NETWORK_ENABLE_PAIR}"
        elif mode == "workspace-write":
            mechanism += f" + -c {NETWORK_DISABLE_PAIR}"
        # Measured per sandbox — see the module docstring and _NETWORK_CAVEAT for the evidence.
        network_access = {
            "read_only": "blocked",
            "write_in_repo": "blocked",
            "publish": "enabled",
            "unrestricted": "unrestricted",
        }[freedom]
        return Enforcement(
            freedom=freedom,
            mechanism=mechanism,
            # Imposed by the OS, not by the agent's own judgement — except when switched off.
            os_enforced=not unrestricted,
            writes_confined=not unrestricted,
            writable_roots=WRITABLE_ROOTS[freedom],
            # Codex has no per-command deny list at all, so neither claim can be made.
            commit_push_blocked=False,
            direct_commit_commands_denied=False,
            # publish and unrestricted are where polybridge configures no barrier of its own
            # against a commit/push/PR attempt — see the field's own docstring for what this does
            # and does not promise.
            publish_attempts_allowed_by_polybridge=freedom in ("publish", "unrestricted"),
            network_access=network_access,
            caveats=_MODE_CAVEATS[freedom],
        )

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        event_type = event.get("type")

        if event_type == "thread.started":
            thread_id = event.get("thread_id")
            if acc.session_id is None and isinstance(thread_id, str) and thread_id:
                acc.session_id = thread_id

        elif event_type == "turn.completed":
            acc.num_turns = (acc.num_turns or 0) + 1
            usage = event.get("usage")
            if isinstance(usage, dict):
                acc.usage = usage
            acc.terminal = event

        elif event_type == "item.completed":
            item = event.get("item")
            if not isinstance(item, dict):
                return
            item_type = item.get("type")
            if item_type == "agent_message":
                text = item.get("text")
                if isinstance(text, str):
                    acc.summary = text
                    acc.saw_final_message = True
            elif item_type == "error":
                # Observed in a successful run: a notice, not a failure.
                message = item.get("message")
                if isinstance(message, str):
                    acc.notices.append(message)

        elif event_type == "error" or (
            isinstance(event_type, str) and event_type.endswith(".failed")
        ):
            # Distinct from an `error` *item*, which was observed in a successful run. A top-level
            # error event is the run itself reporting failure, so it is not shrugged off.
            acc.is_error = True
            acc.terminal = event
            message = event.get("message")
            if isinstance(message, str):
                acc.notices.append(message)

    def classify(self, acc: Accumulator, exit_code: int | None) -> Status:
        # No terminal success/failure event exists, so the exit code is the authority and the closing
        # message is only corroboration. That makes an *observed* zero exit mandatory here: with
        # `exit_code is None` (a recovered run nothing saw exit) an `agent_message` proves the agent
        # spoke, not that the run finished, so completion cannot be established. Spelled out rather
        # than left to `!= 0` incidentally rejecting None.
        if acc.is_error:
            return "failed"
        if exit_code is None or exit_code != 0:
            return "failed"
        return "completed" if acc.saw_final_message else "failed"
