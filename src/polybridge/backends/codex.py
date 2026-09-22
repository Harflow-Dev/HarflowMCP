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
* **`codex exec resume` rejects `-C`/`-s`: they are global `codex exec` options, absent from `codex
  exec resume --help`.** Measured on codex-cli 0.154.0, 2026-09-16, with a bogus thread id so a
  parse success shows up as `no rollout found for thread id …` rather than an option error:
  `codex exec --json -C <dir> -s read-only -c approval_policy="never" resume -- <id> <prompt>`
  parses; `codex exec resume --json -C <dir> …` does not (`error: unexpected argument '-C' found`).
  So `build_resume_argv` places the whole option region *before* `resume`, and `resume` itself
  becomes the last token ahead of the `--` separator — the shape `assert_safe` now checks for.

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

**The `network` parameter (measured 2026-09-22, codex-cli 0.154.0, with the free `codex sandbox`
harness — a real sandbox run with no model call, so the whole matrix cost seconds).** The
measurements above reproduce exactly under that harness, which is what licenses it as a proxy
for `codex exec` on this axis. `sandbox_workspace_write.network_access` is the only working
switch: `read-only` is immune to it in both directions (set true, curl still could not resolve
the host), `workspace-write` takes either direction, `danger-full-access` has no sandbox left
to configure. The domain-allowlist machinery (`experimental_network.domains`, legacy
`allowed_domains`/`denied_domains`) is inert via `-c` — a curl to a "denied" host returned 200 —
so no domain-scoped tier is buildable today; re-measure before believing otherwise on a later
codex. Hence the resolution table: `network=None` keeps each freedom's historical setting
(read_only and unrestricted emit no pair at all), True emits the `=true` pair, False the
`=false` pair, and the two unhonourable cells — enable at read_only, block at unrestricted —
refuse loudly. Two identities follow and are pinned by tests: `write_in_repo`+True is
byte-identical to `publish`'s default, and `publish`+False to `write_in_repo`'s default. The
first means a remote is reachable although publishing was not authorized — recorded intent,
not an enforced barrier, since codex has no per-command deny list at any level — and it enables
arbitrary outbound traffic, exfiltration included. The second blocks **network-backed** push
only: measured, `git push <local bare repo> HEAD:refs/heads/main` still landed with
`network_access=false`, so no wording about that cell may claim publishing is stopped outright.
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
    NetworkControl,
    ReasoningEffort,
    Status,
    UnsupportedCapability,
    check_freedom,
    check_network,
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

# The historical network setting per freedom, pre-dating the `network` parameter: `network=None`
# resolves to exactly this, which is what makes every pre-existing caller byte-identical. Derived
# from the measured matrix in the module docstring: read-only blocks regardless (the key is
# inert there), workspace-write blocks by the explicit `=false`, publish enables by the `=true`,
# danger-full-access has no sandbox left to configure.
DEFAULT_NETWORK: dict[str, bool] = {
    "read_only": False,
    "write_in_repo": False,
    "publish": True,
    "unrestricted": True,
}

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

# Only at write_in_repo + network=True: the argv is byte-identical to publish's default, so the
# run is mechanically a publish even though publishing was never authorized. Stated as a caveat
# because the enforcement booleans stay honest — publish_attempts_allowed_by_polybridge is an
# authorization claim, and nothing was authorized here.
_NETWORK_TRUE_AT_WRITE_IN_REPO_CAVEAT = (
    "network=True at this freedom enables arbitrary outbound traffic — exfiltration included — "
    "not merely docs lookup or a git push: the argv is byte-identical to publish's default, so a "
    "remote is reachable although publishing was not authorized. The difference from publish is "
    "recorded intent, not an enforced barrier — codex has no per-command deny list at any level"
)

# _NETWORK_CAVEAT follows the *resolved* network setting (see enforcement), not the freedom
# name, so it lives outside this table: publish's default still carries it, write_in_repo with
# network=True acquires it, and both freedoms shed it whenever network is actually blocked.
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
        # Measured with the free `codex sandbox` harness (codex-cli 0.154.0 — see the module
        # docstring), every cell of the matrix rather than just the diagonal: read-only +
        # network_access=true stayed blocked (the key is inert under that sandbox) and
        # danger-full-access has no sandbox to block with, so those two directions are refused
        # rather than silently accepted as no-ops. read-only CAN be asked for network=False —
        # the sandbox already blocks there, so the request is satisfiable with no pair emitted.
        network_control=NetworkControl(
            can_enable=("write_in_repo", "publish", "unrestricted"),
            can_block=("read_only", "write_in_repo", "publish"),
            caveats=(
                "support is non-rectangular on purpose: read-only cannot enable (the "
                "sandbox_workspace_write.network_access key is inert under the read-only "
                "sandbox, measured — set true, curl still could not resolve the host) and "
                "unrestricted cannot block (danger-full-access disables the sandbox entirely, "
                "so no barrier remains to raise). Either direction is a claim about "
                "polybridge's own -c override only, never about reachability — the "
                "surrounding network can still defeat it"
            ),
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
        network: bool | None = None,
    ) -> list[str]:
        if session_id is not None:
            raise ValueError("codex mints its own session id; one cannot be supplied")
        self._reject_turn_cap(max_turns)
        argv = [BINARY, "exec", *self._options(repo, freedom, model, reasoning_effort, network)]
        # `--` then the prompt: last, and explicitly not parsed as an option however it looks.
        argv += ["--", self._check_prompt(prompt)]
        self.assert_safe(argv, freedom, network)
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
        network: bool | None = None,
    ) -> list[str]:
        if not session_id:
            raise ValueError("resuming codex needs the thread id its first run reported")
        self._reject_turn_cap(max_turns)
        # `-C`/`-s` are global `codex exec` options, absent from `codex exec resume --help` (see
        # module docstring) — so the option region goes ahead of `resume`, not after it, and
        # `resume` becomes the last token before `--`.
        argv = [
            BINARY, "exec", *self._options(repo, freedom, model, reasoning_effort, network),
            "resume",
        ]
        # `codex exec resume [SESSION_ID] [PROMPT]` — both positional, after `--`.
        argv += ["--", session_id, self._check_prompt(prompt)]
        self.assert_safe(argv, freedom, network)
        return argv

    def _options(
        self,
        repo: Path,
        freedom: Freedom,
        model: str | None,
        reasoning_effort: str | None,
        network: bool | None = None,
    ) -> list[str]:
        check_reasoning_effort(self, reasoning_effort)
        resolved = self._resolve_network(freedom, network)
        options = ["--json", "-C", str(repo), "-s", SANDBOX_MODES[freedom], *NEVER_ASK]
        if SANDBOX_MODES[freedom] == "workspace-write":
            # Only meaningful for workspace-write: the key is scoped to that sandbox, and
            # `read-only` was measured immune to it even when the user's config sets it true.
            # The direction follows the *resolved* request — at network=None that is the
            # freedom's own historical setting, which is what keeps the default byte-identical
            # to before the parameter existed.
            options += ["-c", NETWORK_ENABLE_PAIR if resolved else NETWORK_DISABLE_PAIR]
        if model:
            options += ["-m", model]
        if reasoning_effort:
            options += ["-c", f'{EFFORT_KEY}="{reasoning_effort}"']
        return options

    def _resolve_network(self, freedom: Freedom, network: bool | None) -> bool:
        """The concrete network setting this run will carry, refusing what the sandbox cannot
        honour.

        `None` resolves to the freedom's historical default (DEFAULT_NETWORK) — the
        backward-compatibility guarantee. An explicit boolean goes through the shared capability
        check, so an unhonourable request fails loudly (read-only cannot enable: the key is
        inert there, measured; unrestricted cannot block: no sandbox remains) instead of being
        silently accepted as a no-op.
        """
        if network is None:
            return DEFAULT_NETWORK[freedom]
        check_network(self, freedom, network)
        return network

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

    def assert_safe(self, argv: list[str], freedom: Freedom, network: bool | None = None) -> None:
        # Rejects an unknown freedom outright rather than letting SANDBOX_MODES[freedom] raise a
        # bare KeyError below. The network request is resolved here too — assert_safe is the
        # final execution seam, re-run at spawn time, so an unhonourable (freedom, network) pair
        # must be refused here just as it is in the builders, never waved through.
        check_freedom(freedom)
        resolved = self._resolve_network(freedom, network)
        if argv[:2] != [BINARY, "exec"]:
            raise UnsafeInvocationError(f"unrecognised codex argv layout: {argv!r}")

        # Only the option region is inspected. Everything after `--` is positional — prompt text
        # that happens to contain a flag name must not be able to satisfy a safety check.
        if "--" not in argv:
            raise UnsafeInvocationError(
                f"refusing to run codex without a `--` separator before the prompt, which stops "
                f"prompt text being parsed as options: {argv!r}"
            )
        # The option region is `argv[2:sep]` on BOTH shapes now: `-C`/`-s` are global `codex exec`
        # options that `codex exec resume` itself rejects (measured — see module docstring), so a
        # resume argv carries them ahead of `resume`, which becomes the region's own last token
        # rather than living at a fixed offset like the pre-fix `argv[2] == "resume"` layout did.
        # `_parse_options` recognises that trailing `resume` token itself, since only it knows which
        # positions are option-start (a candidate subcommand) versus a value already claimed by the
        # previous flag (e.g. `-m resume`, where "resume" is this backend's own model value).
        options = argv[2 : argv.index("--")]

        # Kept for the clearer message even though the allowlist below would refuse this token too
        # (as unrecognised) — this is the one form worth naming explicitly.
        if "--dangerously-bypass-approvals-and-sandbox" in options:
            raise UnsafeInvocationError(
                "--dangerously-bypass-approvals-and-sandbox discards the sandbox that is this "
                "backend's main safety property"
            )

        seen, is_resume = self._parse_options(options, argv)

        # Positional arity, not just separator presence. `exec` takes one positional (the prompt),
        # `exec resume` takes two (session id then prompt) — and a resume missing its prompt is the
        # dangerous shape, because codex would read the intended prompt as the SESSION_ID and
        # silently continue some other conversation. Counted, never matched: a legitimate prompt or
        # session id may itself be `--`, `resume`, or look like an option. Driven by `is_resume`,
        # which the walker above decided, rather than a fixed offset into `argv`.
        positionals = argv[argv.index("--") + 1 :]
        expected = 2 if is_resume else 1
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
        # multiplicity remains: exactly one approval override, the network pair exactly where
        # the resolved network setting calls for one, at most one effort override.
        pairs = seen.get("-c", [])
        approvals = [pair for pair in pairs if pair == NEVER_ASK[1]]
        if approvals != [NEVER_ASK[1]]:
            raise UnsafeInvocationError(
                f"codex must be given exactly one {NEVER_ASK[1]} override, which prevents it "
                f"blocking on an approval prompt no one can answer; found {approvals!r}: {argv!r}"
            )

        # The network switch is asserted in BOTH directions against the *resolved* request, not
        # against the freedom name: `=true` exactly where the resolved setting enables it,
        # `=false` exactly where it blocks, and no pair at all where the sandbox takes one
        # nowhere (read-only: the key is inert there, measured; danger-full-access: no sandbox
        # remains). The explicit `=false` half is not belt-and-braces — without it the
        # 'blocked' claim depends on the user's own `[sandbox_workspace_write]` config, and was
        # measured false against a config setting it true.
        expected_network_pair = (
            (NETWORK_ENABLE_PAIR if resolved else NETWORK_DISABLE_PAIR)
            if SANDBOX_MODES[freedom] == "workspace-write"
            else None
        )
        enables = [pair for pair in pairs if pair == NETWORK_ENABLE_PAIR]
        disables = [pair for pair in pairs if pair == NETWORK_DISABLE_PAIR]
        if enables and disables:
            raise UnsafeInvocationError(
                f"codex was given both network overrides at once, so which one applies depends on "
                f"argument order rather than on the requested freedom: {argv!r}"
            )
        if expected_network_pair is None:
            if enables or disables:
                raise UnsafeInvocationError(
                    f"codex carries a network override at freedom {freedom!r}, where the sandbox "
                    f"({SANDBOX_MODES[freedom]}) does not take one: {argv!r}"
                )
        elif expected_network_pair == NETWORK_ENABLE_PAIR:
            if enables != [NETWORK_ENABLE_PAIR]:
                raise UnsafeInvocationError(
                    f"codex must carry exactly one {NETWORK_ENABLE_PAIR} override for this run's "
                    f"resolved network setting: found {enables!r}: {argv!r}"
                )
        elif disables != [NETWORK_DISABLE_PAIR]:
            raise UnsafeInvocationError(
                f"codex must carry exactly one {NETWORK_DISABLE_PAIR} override for this run's "
                f"resolved network setting, or 'network_access: blocked' would be a claim about "
                f"the user's own config rather than this run: found {disables!r}: {argv!r}"
            )

        overrides_with_own_checks = {NEVER_ASK[1], NETWORK_ENABLE_PAIR, NETWORK_DISABLE_PAIR}
        efforts = [pair for pair in pairs if pair not in overrides_with_own_checks]
        if len(efforts) > 1:
            raise UnsafeInvocationError(f"expected at most one {EFFORT_KEY} override: {argv!r}")

    @staticmethod
    def _parse_options(options: list[str], argv: list[str]) -> tuple[dict[str, list[str]], bool]:
        """Walk the option region strictly, refusing any token this backend would not have written.

        Mirrors OpencodeBackend._parse_options and exists for the same reason: searching for
        `"-s" in options` or `value.startswith("approval_policy")` is not enough, because codex
        also honours `--flag=value`, attached short forms (`-capproval_policy=…`), and long aliases
        this backend never emits — a search sees the canonical form it wrote and passes while codex
        applies the non-canonical one that rode along. So every token not in canonical
        space-separated form is refused rather than skipped over, and a `-c` value is checked
        byte-for-byte against PERMITTED_C_PAIRS rather than parsed, which is what closes the
        unbalanced-quote and doubled-quote holes (see PERMITTED_C_PAIRS) by construction.

        Also decides `is_resume`: whether the region ends in a literal `resume` subcommand token.
        Only this walker can tell, because a candidate `resume` is examined solely when the loop is
        at an option-start position — never in a value position, which a plain string search cannot
        distinguish. That is what keeps `-m resume` (a caller-supplied model literally named
        "resume", consumed as `-m`'s value two lines below) from ever being mistaken for the
        subcommand, and even `-m resume resume` unambiguous: the first "resume" is consumed as the
        value, and only the second, trailing one is examined as a candidate subcommand token. A
        `resume` found anywhere but the region's last position is refused outright, since codex
        itself would then see it as a bare positional/unknown token, not the subcommand — which
        also refuses a duplicate, because only the first occurrence could ever be non-final.
        """
        seen: dict[str, list[str]] = {}
        is_resume = False
        index = 0
        while index < len(options):
            token = options[index]
            if token == "resume":
                if index != len(options) - 1:
                    raise UnsafeInvocationError(
                        f"'resume' must be the last token before the '--' separator on a resume "
                        f"argv; found it earlier, at index {index} of the option region: {argv!r}"
                    )
                is_resume = True
                index += 1
            elif token in BOOLEAN_FLAGS:
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
        return seen, is_resume

    def enforcement(self, freedom: Freedom, network: bool | None = None) -> Enforcement:
        mode = SANDBOX_MODES[freedom]
        unrestricted = freedom == "unrestricted"
        resolved = self._resolve_network(freedom, network)
        mechanism = f"codex sandbox: {mode}"
        # Name the override that actually carries the network claim. Without it the mechanism reads
        # as though the sandbox alone decided, which is what `blocked` used to rest on and what was
        # measured false: plain `workspace-write` defers to the user's own config.
        if mode == "workspace-write":
            mechanism += f" + -c {NETWORK_ENABLE_PAIR if resolved else NETWORK_DISABLE_PAIR}"
        # Measured per sandbox — see the module docstring and _NETWORK_CAVEAT for the evidence.
        # The *resolved* setting decides, not the freedom name: publish with network=False is
        # genuinely blocked, and write_in_repo with network=True is genuinely enabled.
        network_access = (
            "unrestricted" if unrestricted else ("enabled" if resolved else "blocked")
        )
        caveats = list(_MODE_CAVEATS[freedom])
        if mode == "workspace-write" and resolved:
            # _NETWORK_CAVEAT follows the resolved setting rather than the freedom name: it
            # described publish's default before the parameter existed (and still does), it now
            # also covers write_in_repo with network=True, and it is absent wherever network is
            # actually blocked — a caveat attached to a True-sounding claim it contradicted
            # would be exactly the overclaim this dataclass forbids.
            caveats.append(_NETWORK_CAVEAT)
            if freedom == "write_in_repo":
                caveats.append(_NETWORK_TRUE_AT_WRITE_IN_REPO_CAVEAT)
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
            # publish and unrestricted are the freedoms that authorize a publish attempt — see
            # the field's own docstring for what that does and does not promise. write_in_repo
            # with network=True keeps this False: a remote push is mechanically possible there,
            # but it was not authorized, and the field claims authorization only.
            publish_attempts_allowed_by_polybridge=freedom in ("publish", "unrestricted"),
            network_access=network_access,
            caveats=tuple(caveats),
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
